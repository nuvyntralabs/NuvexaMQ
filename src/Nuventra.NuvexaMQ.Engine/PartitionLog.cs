using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading.Channels;

namespace Nuventra.NuvexaMQ.Engine;

internal sealed class PartitionLog : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly string _directory;
    private readonly BrokerOptions _options;
    private readonly Channel<AppendJob> _channel;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<SegmentState> _segments = [];
    private readonly Task _loop;
    private SegmentState? _active;
    private long _nextOffset;
    private long _committedOffset = -1;

    private PartitionLog(string directory, BrokerOptions options)
    {
        _directory = directory;
        _options = options;
        Directory.CreateDirectory(directory);
        Recover();
        _channel = Channel.CreateBounded<AppendJob>(new BoundedChannelOptions(8192)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        _loop = Task.Run(Loop);
    }

    public long FirstOffset
    {
        get
        {
            lock (_gate)
            {
                foreach (var segment in _segments)
                {
                    if (segment.RecordCount > 0 && segment.ReadableLength > LogFormat.HeaderSize)
                        return segment.BaseOffset;
                }

                return 0;
            }
        }
    }

    public long NextOffset
    {
        get
        {
            lock (_gate)
                return _nextOffset;
        }
    }

    public long CommittedOffset => Volatile.Read(ref _committedOffset);

    public TimeSpan? MaxAge { get; set; }

    public long? MaxBytes { get; set; }

    public Action? OnCommitted { get; set; }

    public static PartitionLog Open(string directory, BrokerOptions options) => new(directory, options);

    public async ValueTask<long> AppendAsync(AppendRequest request, CancellationToken cancellationToken)
    {
        var job = new AppendJob(request);
        try
        {
            await _channel.Writer.WriteAsync(job, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            throw new NuvexaMqException(NuvexaMqError.Closed, "The partition log is closed.");
        }

        return await job.Done.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public bool TryRead(long offset, out StoredRecord? record)
    {
        record = null;
        string? path = null;
        long position = LogFormat.HeaderSize;
        long readable = 0;
        lock (_gate)
        {
            if (offset > _committedOffset || _committedOffset < 0)
                return false;

            foreach (var segment in _segments)
            {
                if (segment.RecordCount == 0 || offset < segment.BaseOffset || offset > segment.LastOffset)
                    continue;

                path = segment.LogPath;
                readable = segment.ReadableLength;
                position = IndexPosition(segment, offset);
                break;
            }
        }

        if (path is null)
            return false;

        FileStream stream;
        try
        {
            stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }
        catch (FileNotFoundException)
        {
            return false;
        }

        using (stream)
        {
            if (readable > stream.Length)
                readable = stream.Length;
            stream.Position = position;
            while (stream.Position < readable)
            {
                if (!LogFormat.TryReadRecord(stream, _options.MaxMessageBytes, out var candidate))
                    return false;
                if (candidate.Offset == offset)
                {
                    record = candidate;
                    return true;
                }

                if (candidate.Offset > offset)
                    return false;
            }
        }

        return false;
    }

    public int ReadForward(long offset, int max, List<StoredRecord> into)
    {
        var added = 0;
        while (added < max)
        {
            string? path = null;
            long position = LogFormat.HeaderSize;
            long readable = 0;
            lock (_gate)
            {
                if (offset > _committedOffset || _committedOffset < 0)
                    return added;

                foreach (var segment in _segments)
                {
                    if (segment.RecordCount == 0 || offset < segment.BaseOffset || offset > segment.LastOffset)
                        continue;

                    path = segment.LogPath;
                    readable = segment.ReadableLength;
                    position = IndexPosition(segment, offset);
                    break;
                }
            }

            if (path is null)
                return added;

            FileStream stream;
            try
            {
                stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            }
            catch (FileNotFoundException)
            {
                return added;
            }

            using (stream)
            {
                if (readable > stream.Length)
                    readable = stream.Length;
                stream.Position = position;
                var progressed = false;
                while (added < max && stream.Position < readable)
                {
                    if (!LogFormat.TryReadRecord(stream, _options.MaxMessageBytes, out var candidate))
                        return added;
                    if (candidate.Offset < offset)
                        continue;
                    if (candidate.Offset != offset)
                        return added;

                    into.Add(candidate);
                    added++;
                    offset++;
                    progressed = true;
                }

                if (!progressed)
                    return added;
            }
        }

        return added;
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        _stop.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        lock (_gate)
        {
            if (_active is not null)
            {
                LogFormat.WriteIndex(IndexPath(_active.LogPath), _active.Index);
                WriteCommitFile();
            }

            _active?.Stream?.Dispose();
            _active = null;
        }
    }

    private void Recover()
    {
        foreach (var path in Directory.GetFiles(_directory, "segment-*.log").OrderBy(Path.GetFileName, StringComparer.Ordinal))
        {
            var segment = LoadSegment(path);
            if (segment is null)
                continue;
            _segments.Add(segment);
            if (segment.RecordCount > 0)
                _nextOffset = segment.LastOffset + 1;
            else if (_segments.Count == 1)
                _nextOffset = segment.BaseOffset;
        }

        if (_segments.Count > 0)
        {
            _active = _segments[^1];
            OpenWriter(_active);
            _committedOffset = _nextOffset == 0 ? -1 : _nextOffset - 1;
            if (_segments.All(s => s.RecordCount == 0))
                _committedOffset = -1;
        }

        WriteCommitFile();
    }

    private SegmentState? LoadSegment(string path)
    {
        List<IndexEntry> index;
        long lastOffset;
        long maxTimestamp;
        int recordCount;
        long goodLength;
        long baseOffset;
        using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
        {
            index = [];
            var stop = LogFormat.Scan(stream, _options.MaxMessageBytes, index, out lastOffset, out maxTimestamp, out recordCount);
            goodLength = stop.GoodLength;
            if (goodLength < LogFormat.HeaderSize)
            {
                stream.SetLength(0);
            }
            else if (stop.Torn || stream.Length != goodLength)
            {
                stream.SetLength(goodLength);
            }

            baseOffset = 0;
            if (goodLength >= LogFormat.HeaderSize)
            {
                stream.Position = 0;
                if (!LogFormat.TryReadSegmentHeader(stream, out _, out baseOffset))
                    goodLength = 0;
            }
        }

        if (goodLength < LogFormat.HeaderSize)
        {
            File.Delete(path);
            var brokenIndex = IndexPath(path);
            if (File.Exists(brokenIndex))
                File.Delete(brokenIndex);
            return null;
        }

        LogFormat.WriteIndex(IndexPath(path), index);
        return new SegmentState(path, baseOffset)
        {
            Index = index,
            LastOffset = lastOffset,
            MaxTimestamp = maxTimestamp,
            RecordCount = recordCount,
            ReadableLength = goodLength
        };
    }

    private async Task Loop()
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                var batch = new List<AppendJob>();
                var max = Math.Max(1, _options.FlushMaxRecords);
                Drain(batch, max);
                // Publishers with one confirm in flight cannot enqueue the next record until this
                // batch is fsynced. Sleeping out FlushInterval here only delays those confirms.
                // Pick up records that are already queued, then fsync as soon as the queue is idle.
                if (_options.FlushInterval > TimeSpan.Zero && batch.Count < max && !_stop.IsCancellationRequested)
                    DrainReady(batch, max);

                if (batch.Count > 0)
                    Commit(batch);
            }
        }
        catch (Exception ex)
        {
            while (_channel.Reader.TryRead(out var job))
                job.Done.TrySetException(ex);
        }
    }

    private void Commit(List<AppendJob> batch)
    {
        try
        {
            var started = System.Diagnostics.Stopwatch.StartNew();
            foreach (var job in batch)
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                EnsureActive();
                var encodedLength = LogFormat.Encode(0, timestamp, job.Request.Key, job.Request.Subject, job.Request.Headers, job.Request.Payload).Length;
                lock (_gate)
                {
                    if (_active!.RecordCount > 0 && _active.WritePosition + encodedLength > _options.SegmentBytes)
                        Roll();
                }

                var offset = _nextOffset++;
                var bytes = LogFormat.Encode(offset, timestamp, job.Request.Key, job.Request.Subject, job.Request.Headers, job.Request.Payload);
                var segment = _active!;
                var position = segment.WritePosition;
                segment.Stream!.Write(bytes);
                segment.WritePosition += bytes.Length;
                if (segment.Index.Count == 0 || position - segment.Index[^1].Position >= LogFormat.IndexStride)
                    segment.Index.Add(new IndexEntry(offset, position));
                segment.LastOffset = offset;
                segment.RecordCount++;
                if (timestamp > segment.MaxTimestamp)
                    segment.MaxTimestamp = timestamp;
                job.Offset = offset;
            }

            DiskSync.Sync(_active!.Stream!);
            var fsyncSeconds = started.Elapsed.TotalSeconds;
            lock (_gate)
            {
                _active.ReadableLength = _active.WritePosition;
                _committedOffset = batch[^1].Offset;
                ApplyRetention();
            }

            LastFsyncSeconds = fsyncSeconds;
            OnCommitted?.Invoke();
            foreach (var job in batch)
                job.Done.TrySetResult(job.Offset);
        }
        catch (Exception ex)
        {
            foreach (var job in batch)
                job.Done.TrySetException(ex);
        }
    }

    private void Drain(List<AppendJob> batch, int max)
    {
        while (batch.Count < max && _channel.Reader.TryRead(out var job))
            batch.Add(job);
    }

    private void DrainReady(List<AppendJob> batch, int max)
    {
        var cap = _options.FlushInterval < TimeSpan.FromMilliseconds(1)
            ? _options.FlushInterval
            : TimeSpan.FromMilliseconds(1);
        var idle = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 5);
        if (idle > cap)
            idle = cap;

        var idleSince = Stopwatch.GetTimestamp();
        var limit = idleSince + (long)(cap.TotalSeconds * Stopwatch.Frequency);
        var idleTicks = (long)(idle.TotalSeconds * Stopwatch.Frequency);
        while (batch.Count < max && Stopwatch.GetTimestamp() < limit && !_stop.IsCancellationRequested)
        {
            if (_channel.Reader.TryPeek(out _))
            {
                Drain(batch, max);
                idleSince = Stopwatch.GetTimestamp();
                continue;
            }

            if (Stopwatch.GetTimestamp() - idleSince >= idleTicks)
                break;

            Thread.SpinWait(64);
        }
    }

    private void EnsureActive()
    {
        if (_active is not null)
            return;

        lock (_gate)
        {
            if (_active is not null)
                return;

            var segment = CreateSegment(_nextOffset);
            _segments.Add(segment);
            _active = segment;
        }
    }

    private void Roll()
    {
        var current = _active ?? throw new InvalidOperationException("No active segment.");
        var stream = current.Stream ?? throw new InvalidOperationException("No active segment stream.");
        DiskSync.Sync(stream);
        current.ReadableLength = current.WritePosition;
        stream.Dispose();
        current.Stream = null;
        LogFormat.WriteIndex(IndexPath(current.LogPath), current.Index);
        var segment = CreateSegment(_nextOffset);
        _segments.Add(segment);
        _active = segment;
    }

    private SegmentState CreateSegment(long baseOffset)
    {
        var path = Path.Combine(_directory, $"segment-{baseOffset:D20}.log");
        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
        LogFormat.WriteSegmentHeader(stream, _options.Epoch, baseOffset);
        stream.Flush(true);
        return new SegmentState(path, baseOffset)
        {
            Stream = stream,
            WritePosition = LogFormat.HeaderSize,
            ReadableLength = LogFormat.HeaderSize
        };
    }

    private void OpenWriter(SegmentState segment)
    {
        var stream = new FileStream(segment.LogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        stream.Position = segment.ReadableLength;
        segment.Stream = stream;
        segment.WritePosition = segment.ReadableLength;
    }

    private void ApplyRetention()
    {
        var maxBytes = MaxBytes;
        var maxAge = MaxAge;
        while (_segments.Count > 1)
        {
            var oldest = _segments[0];
            var tooBig = maxBytes is long cap && TotalBytes() > cap;
            var tooOld = false;
            if (maxAge is TimeSpan age && oldest.MaxTimestamp > 0)
            {
                var written = DateTimeOffset.FromUnixTimeMilliseconds(oldest.MaxTimestamp);
                tooOld = DateTimeOffset.UtcNow - written > age;
            }

            if (!tooBig && !tooOld)
                break;

            _segments.RemoveAt(0);
            if (oldest.Stream is not null)
            {
                oldest.Stream.Dispose();
                oldest.Stream = null;
            }

            File.Delete(oldest.LogPath);
            var index = IndexPath(oldest.LogPath);
            if (File.Exists(index))
                File.Delete(index);
        }
    }

    private long TotalBytes()
    {
        long total = 0;
        foreach (var segment in _segments)
            total += segment.WritePosition > 0 ? segment.WritePosition : segment.ReadableLength;
        return total;
    }

    private void WriteCommitFile()
    {
        var dto = new CommitDto
        {
            Epoch = _options.Epoch,
            CommittedOffset = _committedOffset
        };
        var json = JsonSerializer.Serialize(dto, EngineJsonContext.Default.CommitDto);
        AtomicFile.Write(Path.Combine(_directory, "commit.json"), json);
    }

    private static long IndexPosition(SegmentState segment, long offset)
    {
        var position = (long)LogFormat.HeaderSize;
        foreach (var entry in segment.Index)
        {
            if (entry.Offset > offset)
                break;
            position = entry.Position;
        }

        return position;
    }

    private static string IndexPath(string logPath) => Path.ChangeExtension(logPath, ".idx");

    internal double LastFsyncSeconds { get; private set; }
}

internal sealed class AppendRequest
{
    public byte[] Key { get; init; } = [];

    public string Subject { get; init; } = "";

    public IReadOnlyList<MessageHeader> Headers { get; init; } = [];

    public byte[] Payload { get; init; } = [];
}

internal sealed class AppendJob(AppendRequest request)
{
    public AppendRequest Request { get; } = request;

    public long Offset { get; set; }

    public TaskCompletionSource<long> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

internal sealed class SegmentState(string logPath, long baseOffset)
{
    public string LogPath { get; } = logPath;

    public long BaseOffset { get; } = baseOffset;

    public FileStream? Stream { get; set; }

    public List<IndexEntry> Index { get; set; } = [];

    public long WritePosition { get; set; }

    public long ReadableLength { get; set; }

    public long LastOffset { get; set; } = -1;

    public long MaxTimestamp { get; set; }

    public int RecordCount { get; set; }
}

internal static class DiskSync
{
    public static void Sync(FileStream stream)
    {
        stream.Flush(false);
        if (OperatingSystem.IsWindows())
        {
            stream.Flush(true);
            return;
        }

        // libc fsync. On macOS, FileStream.Flush(true) uses F_FULLFSYNC, which waits for the
        // drive cache and costs several milliseconds. fsync matches other brokers on this OS:
        // a confirmed write survives a process crash. A power cut can still drop it.
        var result = fsync(stream.SafeFileHandle);
        if (result != 0)
            throw new IOException($"fsync failed ({Marshal.GetLastPInvokeError()}).");
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int fsync(SafeHandle handle);
}
