using System.Text.Json;

namespace Nuventra.NuvexaMQ.Engine;

public sealed class NuvexaBroker : IAsyncDisposable
{
    public const string DeadLetterStream = "$dlq";

    private readonly object _gate = new();
    private readonly string _dataDir;
    private readonly BrokerOptions _options;
    private readonly CommitSignal _signal = new();
    private readonly Dictionary<string, StreamState> _streams = new(StringComparer.Ordinal);
    private readonly object _cursorLock = new();
    private readonly Dictionary<string, ConsumerCursor> _dirtyCursors = new(StringComparer.Ordinal);
    private readonly Task _cursorLoop;
    private int _cursorStop;
    private long _messagesIn;
    private long _messagesOut;
    private int _disposed;

    private NuvexaBroker(string dataDir, BrokerOptions options)
    {
        _dataDir = dataDir;
        _options = options;
        Directory.CreateDirectory(StreamRoot);
        foreach (var directory in Directory.GetDirectories(StreamRoot))
        {
            var metaPath = Path.Combine(directory, "stream.json");
            if (!File.Exists(metaPath))
                continue;

            var meta = JsonSerializer.Deserialize(File.ReadAllText(metaPath), EngineJsonContext.Default.StreamMetaDto)
                ?? throw new NuvexaMqException(NuvexaMqError.Invalid, $"Stream metadata in '{metaPath}' is unreadable.");
            meta.Deleted ??= [];
            var state = OpenStream(meta);
            _streams.Add(meta.Name, state);
        }

        _cursorLoop = Task.Run(FlushCursors);
    }

    private string StreamRoot => Path.Combine(_dataDir, "streams");

    public static NuvexaBroker Open(string dataDir, BrokerOptions? options = null) =>
        new(dataDir, options ?? new BrokerOptions());

    public async Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        SubjectFilter.ValidateName(spec.Name, "Stream");
        if (spec.Name.StartsWith('$') && spec.Name != DeadLetterStream && !spec.Queue)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Stream names starting with '$' are reserved.");
        if (spec.Filters.Count == 0)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "A stream needs at least one subject filter.");
        if (spec.PartitionCount is < 1 or > 256)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Partition count must be from 1 to 256.");
        foreach (var filter in spec.Filters)
            SubjectFilter.ValidateFilter(filter);

        var maxMessage = spec.MaxMessageBytes ?? _options.MaxMessageBytes;
        if (maxMessage < 1)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Max message size must be positive.");

        StreamState created;
        lock (_gate)
        {
            if (_streams.TryGetValue(spec.Name, out var existing))
            {
                if (existing.Meta.PartitionCount != spec.PartitionCount || !SameFilters(existing.Meta.Filters, spec.Filters))
                    throw new NuvexaMqException(NuvexaMqError.Conflict, $"Stream '{spec.Name}' already exists with a different layout.");

                existing.Meta.MaxAgeMs = spec.MaxAge is TimeSpan updatedAge ? (long)updatedAge.TotalMilliseconds : null;
                existing.Meta.MaxBytes = spec.MaxBytes;
                if (existing.Meta.Queue != spec.Queue)
                    throw new NuvexaMqException(NuvexaMqError.Conflict, $"Stream '{spec.Name}' already exists with a different layout.");
                existing.Meta.MaxMessageBytes = maxMessage;
                ApplyRetentionSettings(existing);
                PersistMeta(existing.Meta);
                return;
            }

            var meta = new StreamMetaDto
            {
                Name = spec.Name,
                Filters = spec.Filters.ToArray(),
                PartitionCount = spec.PartitionCount,
                MaxAgeMs = spec.MaxAge is TimeSpan createdAge ? (long)createdAge.TotalMilliseconds : null,
                MaxBytes = spec.MaxBytes,
                MaxMessageBytes = maxMessage,
                Epoch = _options.Epoch,
                Queue = spec.Queue
            };
            created = OpenStream(meta);
            _streams.Add(spec.Name, created);
            PersistMeta(meta);
        }

        await Task.CompletedTask;
    }

    public async Task<IReadOnlyList<PublishReceipt>> PublishAsync(
        string subject,
        ReadOnlyMemory<byte> payload,
        string? key = null,
        IReadOnlyList<MessageHeader>? headers = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        SubjectFilter.ValidateSubject(subject);
        var matches = Matching(subject);
        if (matches.Count == 0)
            throw new NuvexaMqException(NuvexaMqError.NotFound, $"No stream captures subject '{subject}'.");

        var receipts = new List<PublishReceipt>(matches.Count);
        foreach (var stream in matches)
        {
            if (payload.Length > stream.Meta.MaxMessageBytes)
                throw new NuvexaMqException(NuvexaMqError.TooLarge, $"Payload exceeds the {stream.Meta.MaxMessageBytes} byte limit for stream '{stream.Meta.Name}'.");

            receipts.Add(await AppendAsync(stream, subject, key, headers, payload, cancellationToken).ConfigureAwait(false));
        }

        return receipts;
    }

    public void EnsureConsumer(ConsumeSpec spec)
    {
        ThrowIfDisposed();
        SubjectFilter.ValidateName(spec.Stream, "Stream");
        SubjectFilter.ValidateName(spec.Name, "Consumer");
        if (spec.MaxDeliver < 1)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "MaxDeliver must be at least 1.");
        if (spec.MaxAckPending < 1)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "MaxAckPending must be at least 1.");
        if (spec.AckWait < TimeSpan.FromMilliseconds(1))
            throw new NuvexaMqException(NuvexaMqError.Invalid, "AckWait must be at least 1ms.");
        if (!string.IsNullOrEmpty(spec.Filter))
            SubjectFilter.ValidateFilter(spec.Filter);

        var stream = GetStream(spec.Stream);
        lock (stream.Consumers)
        {
            if (stream.Consumers.ContainsKey(spec.Name))
                return;

            var start = spec.Start ?? (spec.Ephemeral ? ConsumeStart.Tail : ConsumeStart.First);
            var cursors = new ConsumerCursor[stream.Partitions.Length];
            for (var i = 0; i < stream.Partitions.Length; i++)
            {
                var next = start switch
                {
                    ConsumeStart.Tail => Tail(stream.Partitions[i]),
                    ConsumeStart.Offset => spec.StartOffset,
                    _ => stream.Partitions[i].FirstOffset
                };
                var cursor = new ConsumerCursor
                {
                    Name = spec.Name,
                    Filter = spec.Filter ?? "",
                    AckWaitMs = (int)spec.AckWait.TotalMilliseconds,
                    MaxDeliver = spec.MaxDeliver,
                    MaxAckPending = spec.MaxAckPending,
                    NextOffset = next,
                    Ephemeral = spec.Ephemeral,
                    Path = spec.Ephemeral ? "" : ConsumerPath(spec.Stream, i, spec.Name)
                };
                cursors[i] = cursor;
                if (!spec.Ephemeral)
                    PersistCursor(cursor);
            }

            stream.Consumers.Add(spec.Name, new ConsumerGroup(spec.Name, cursors, spec.Ephemeral));
        }
    }

    public async Task<FetchResult> FetchAsync(string streamName, string consumerName, int maxMessages, TimeSpan expires, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (maxMessages < 1)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Fetch size must be at least 1.");

        var stream = GetStream(streamName);
        var group = GetConsumer(stream, consumerName);
        var until = DateTime.UtcNow + (expires < TimeSpan.Zero ? TimeSpan.Zero : expires);
        while (true)
        {
            var combined = new List<Delivery>();
            for (var step = 0; step < stream.Partitions.Length && combined.Count < maxMessages; step++)
            {
                var index = (group.Start + step) % stream.Partitions.Length;
                var part = await FetchPartitionAsync(stream, group, index, maxMessages - combined.Count, cancellationToken).ConfigureAwait(false);
                if (part.OffsetReset)
                    return part;
                combined.AddRange(part.Messages);
            }

            group.Start = (group.Start + 1) % stream.Partitions.Length;
            if (combined.Count > 0)
            {
                Interlocked.Add(ref _messagesOut, combined.Count);
                return new FetchResult { Messages = combined };
            }

            var remaining = until - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                return new FetchResult { Messages = combined };

            await _signal.WaitAsync(remaining, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task AckAsync(string streamName, string consumerName, int partition, long offset, CancellationToken cancellationToken = default)
    {
        var cursor = Cursor(streamName, consumerName, partition);
        await cursor.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var removed = cursor.Pending.Remove(offset);
            if (!removed && offset >= cursor.NextOffset)
                throw new NuvexaMqException(NuvexaMqError.Invalid, "That offset has not been delivered to this consumer.");
            PersistCursor(cursor);
        }
        finally
        {
            cursor.Gate.Release();
        }
    }

    public async Task NackAsync(string streamName, string consumerName, int partition, long offset, CancellationToken cancellationToken = default)
    {
        var cursor = Cursor(streamName, consumerName, partition);
        await cursor.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!cursor.Pending.TryGetValue(offset, out var pending))
                return;
            pending.DeliveredAtUnixMs = 0;
            PersistCursor(cursor);
        }
        finally
        {
            cursor.Gate.Release();
        }

        _signal.Pulse();
    }

    public void ResetConsumer(string streamName, string consumerName, long? offset)
    {
        var stream = GetStream(streamName);
        var group = GetConsumer(stream, consumerName);
        for (var i = 0; i < group.Cursors.Length; i++)
        {
            var cursor = group.Cursors[i];
            cursor.Gate.Wait();
            try
            {
                cursor.NextOffset = offset ?? stream.Partitions[i].FirstOffset;
                cursor.Pending.Clear();
                PersistCursor(cursor);
            }
            finally
            {
                cursor.Gate.Release();
            }
        }
    }

    public void ReleaseConsumer(string streamName, string consumerName)
    {
        var stream = GetStream(streamName);
        lock (stream.Consumers)
        {
            if (!stream.Consumers.TryGetValue(consumerName, out var group) || !group.Ephemeral)
                return;
            stream.Consumers.Remove(consumerName);
        }
    }

    public bool TryRead(string streamName, int partition, long offset, out StoredRecord? record)
    {
        var stream = GetStream(streamName);
        if (partition < 0 || partition >= stream.Partitions.Length)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Partition is out of range.");
        return stream.Partitions[partition].TryRead(offset, out record);
    }

    public BrokerSnapshot Snapshot()
    {
        var partitions = new List<PartitionSnapshot>();
        var consumers = new List<ConsumerSnapshot>();
        lock (_gate)
        {
            foreach (var stream in _streams.Values)
            {
                for (var i = 0; i < stream.Partitions.Length; i++)
                {
                    var log = stream.Partitions[i];
                    partitions.Add(new PartitionSnapshot(stream.Meta.Name, i, log.CommittedOffset, log.FirstOffset));
                }

                lock (stream.Consumers)
                {
                    foreach (var group in stream.Consumers.Values)
                    {
                        for (var i = 0; i < group.Cursors.Length; i++)
                            consumers.Add(new ConsumerSnapshot(stream.Meta.Name, group.Name, i, group.Cursors[i].Pending.Count));
                    }
                }
            }
        }

        return new BrokerSnapshot
        {
            MessagesIn = Interlocked.Read(ref _messagesIn),
            MessagesOut = Interlocked.Read(ref _messagesOut),
            LastFsyncSeconds = partitions.Count == 0 ? 0 : _streams.Values.SelectMany(s => s.Partitions).Max(p => p.LastFsyncSeconds),
            Partitions = partitions,
            Consumers = consumers
        };
    }

    public IReadOnlyList<StreamInfo> ListStreams()
    {
        lock (_gate)
        {
            return _streams.Values
                .OrderBy(stream => stream.Meta.Name, StringComparer.Ordinal)
                .Select(stream => new StreamInfo
                {
                    Name = stream.Meta.Name,
                    Filters = stream.Meta.Filters.ToArray(),
                    PartitionCount = stream.Meta.PartitionCount,
                    MaxAgeMs = stream.Meta.MaxAgeMs,
                    MaxBytes = stream.Meta.MaxBytes,
                    MaxMessageBytes = stream.Meta.MaxMessageBytes,
                    Queue = stream.Meta.Queue
                })
                .ToArray();
        }
    }

    public IReadOnlyList<ConsumerInfo> ListConsumers()
    {
        var list = new List<ConsumerInfo>();
        lock (_gate)
        {
            foreach (var stream in _streams.Values)
            {
                lock (stream.Consumers)
                {
                    foreach (var group in stream.Consumers.Values)
                    {
                        for (var i = 0; i < group.Cursors.Length; i++)
                        {
                            var cursor = group.Cursors[i];
                            list.Add(new ConsumerInfo
                            {
                                Stream = stream.Meta.Name,
                                Name = group.Name,
                                Partition = i,
                                Pending = cursor.Pending.Count,
                                NextOffset = cursor.NextOffset,
                                Filter = cursor.Filter,
                                Ephemeral = group.Ephemeral,
                                AckWaitMs = cursor.AckWaitMs,
                                MaxDeliver = cursor.MaxDeliver
                            });
                        }
                    }
                }
            }
        }

        return list;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        lock (_cursorLock)
        {
            _cursorStop = 1;
            Monitor.PulseAll(_cursorLock);
        }

        await _cursorLoop.ConfigureAwait(false);

        PartitionLog[] logs;
        lock (_gate)
            logs = _streams.Values.SelectMany(s => s.Partitions).ToArray();

        foreach (var log in logs)
            await log.DisposeAsync().ConfigureAwait(false);

        _signal.Pulse();
    }

    private async Task<FetchResult> FetchPartitionAsync(StreamState stream, ConsumerGroup group, int partition, int maxMessages, CancellationToken cancellationToken)
    {
        var cursor = group.Cursors[partition];
        var log = stream.Partitions[partition];
        await cursor.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var poisons = cursor.Pending.Values
                .Where(item => item.DeadLettering || now >= item.DeliveredAtUnixMs + cursor.AckWaitMs && item.DeliveryCount >= cursor.MaxDeliver)
                .ToList();
            foreach (var poison in poisons)
            {
                poison.DeadLettering = true;
                PersistCursor(cursor);
                if (log.TryRead(poison.Offset, out var stored) && stored is not null)
                    await PublishDeadLetterAsync(stream.Meta.Name, group.Name, partition, stored, poison.DeliveryCount, cancellationToken).ConfigureAwait(false);
                cursor.Pending.Remove(poison.Offset);
                PersistCursor(cursor);
            }

            if (cursor.NextOffset < log.FirstOffset && log.CommittedOffset >= 0 && log.FirstOffset > cursor.NextOffset)
            {
                return new FetchResult
                {
                    OffsetReset = true,
                    FirstOffset = log.FirstOffset,
                    Messages = []
                };
            }

            var deliveries = new List<Delivery>();
            foreach (var pending in cursor.Pending.Values.OrderBy(item => item.Offset).ToList())
            {
                if (IsDeleted(stream.Meta, partition, pending.Offset))
                {
                    cursor.Pending.Remove(pending.Offset);
                    continue;
                }

                if (deliveries.Count >= maxMessages)
                    break;
                if (now < pending.DeliveredAtUnixMs + cursor.AckWaitMs)
                    continue;
                if (!log.TryRead(pending.Offset, out var stored) || stored is null)
                    continue;

                pending.DeliveryCount++;
                pending.DeliveredAtUnixMs = now;
                deliveries.Add(ToDelivery(stream.Meta.Name, group.Name, partition, stored, pending.DeliveryCount));
            }

            while (deliveries.Count < maxMessages && cursor.Pending.Count < cursor.MaxAckPending)
            {
                if (cursor.NextOffset > log.CommittedOffset)
                    break;
                if (cursor.NextOffset < log.FirstOffset && log.CommittedOffset >= 0)
                {
                    return new FetchResult
                    {
                        OffsetReset = true,
                        FirstOffset = log.FirstOffset,
                        Messages = deliveries
                    };
                }

                var take = Math.Min(maxMessages - deliveries.Count, cursor.MaxAckPending - cursor.Pending.Count);
                var records = new List<StoredRecord>(take);
                if (log.ReadForward(cursor.NextOffset, take, records) == 0)
                    break;

                var stopped = false;
                foreach (var stored in records)
                {
                    if (stored.Offset != cursor.NextOffset)
                    {
                        stopped = true;
                        break;
                    }

                    var offset = cursor.NextOffset;
                    cursor.NextOffset++;
                    if (offset < stream.Meta.FloorOffset || IsDeleted(stream.Meta, partition, offset))
                        continue;
                    if (stream.Meta.MessageTtlMs is long ttl && ttl >= 0 && now - stored.TimestampUnixMs > ttl)
                    {
                        await PublishDeadLetterAsync(stream.Meta.Name, group.Name, partition, stored, 1, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    if (!Accepts(cursor.Filter, stored.Subject))
                        continue;

                    cursor.Pending[offset] = new PendingDto
                    {
                        Offset = offset,
                        DeliveryCount = 1,
                        DeliveredAtUnixMs = now
                    };
                    deliveries.Add(ToDelivery(stream.Meta.Name, group.Name, partition, stored, 1));
                }

                if (stopped)
                    break;
            }

            PersistCursor(cursor);
            return new FetchResult { Messages = deliveries };
        }
        finally
        {
            cursor.Gate.Release();
        }
    }

    public Func<string, StoredRecord, CancellationToken, Task<bool>>? OnDeadLetter { get; set; }

    public async Task<PublishReceipt> AppendStreamAsync(string streamName, string subject, ReadOnlyMemory<byte> payload, string? key = null, IReadOnlyList<MessageHeader>? headers = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var stream = GetStream(streamName);
        return await AppendAsync(stream, subject, key, headers, payload, cancellationToken).ConfigureAwait(false);
    }

    public void ConfigureQueueStream(string name, long floor, long? messageTtlMs)
    {
        lock (_gate)
        {
            if (!_streams.TryGetValue(name, out var stream))
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Stream '{name}' was not found.");
            if (floor > stream.Meta.FloorOffset)
                stream.Meta.FloorOffset = floor;
            stream.Meta.MessageTtlMs = messageTtlMs;
            PruneDeleted(stream);
            PersistMeta(stream.Meta);
        }
    }

    public void DeleteMessage(string streamName, int partition, long offset)
    {
        ThrowIfDisposed();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Stream '{streamName}' was not found.");
            if ((uint)partition >= (uint)stream.Partitions.Length)
                throw new NuvexaMqException(NuvexaMqError.Invalid, "Partition is out of range.");
            var log = stream.Partitions[partition];
            var first = Math.Max(log.FirstOffset, stream.Meta.FloorOffset);
            if (log.CommittedOffset < 0 || offset < first || offset > log.CommittedOffset)
                throw new NuvexaMqException(NuvexaMqError.NotFound, "That message is not in the log.");
            stream.Meta.Deleted ??= [];
            PruneDeleted(stream);
            if (IsDeleted(stream.Meta, partition, offset))
                return;
            stream.Meta.Deleted.Add(new DeletedMessageDto { Partition = partition, Offset = offset });
            PersistMeta(stream.Meta);
        }
    }

    public bool MessageIsDeleted(string streamName, int partition, long offset)
    {
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                return false;
            return IsDeleted(stream.Meta, partition, offset);
        }
    }

    public long FloorOffset(string streamName)
    {
        lock (_gate)
            return _streams.TryGetValue(streamName, out var stream) ? stream.Meta.FloorOffset : 0;
    }

    public long VisibleMessageCount(string streamName)
    {
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
                return 0;
            long count = 0;
            for (var i = 0; i < stream.Partitions.Length; i++)
            {
                var log = stream.Partitions[i];
                if (log.CommittedOffset < 0)
                    continue;
                var first = Math.Max(log.FirstOffset, stream.Meta.FloorOffset);
                if (log.CommittedOffset < first)
                    continue;
                var hidden = stream.Meta.Deleted?.Count(item => item.Partition == i && item.Offset >= first && item.Offset <= log.CommittedOffset) ?? 0;
                count += Math.Max(0, log.CommittedOffset - first + 1 - hidden);
            }

            return count;
        }
    }

    private static bool IsDeleted(StreamMetaDto meta, int partition, long offset) =>
        meta.Deleted is not null && meta.Deleted.Exists(item => item.Partition == partition && item.Offset == offset);

    private static void PruneDeleted(StreamState stream)
    {
        if (stream.Meta.Deleted is null || stream.Meta.Deleted.Count == 0)
            return;
        stream.Meta.Deleted.RemoveAll(item =>
        {
            if ((uint)item.Partition >= (uint)stream.Partitions.Length)
                return true;
            var log = stream.Partitions[item.Partition];
            return item.Offset < log.FirstOffset || item.Offset < stream.Meta.FloorOffset;
        });
    }

    public async Task DeleteStreamAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        if (name == DeadLetterStream)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "The dead-letter stream cannot be deleted.");
        StreamState? state;
        lock (_gate)
        {
            if (!_streams.Remove(name, out state))
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Stream '{name}' was not found.");
        }

        foreach (var log in state!.Partitions)
            await log.DisposeAsync().ConfigureAwait(false);
        var directory = Path.Combine(StreamRoot, name);
        if (Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);
    }

    public long QueueDepth(string name)
    {
        var snapshot = Snapshot().Partitions.Where(item => item.Stream == name).ToArray();
        long floor;
        lock (_gate)
        {
            if (!_streams.TryGetValue(name, out var stream))
                return 0;
            floor = stream.Meta.FloorOffset;
        }

        long depth = 0;
        foreach (var part in snapshot)
        {
            if (part.CommittedOffset < 0)
                continue;
            var first = Math.Max(part.FirstOffset, floor);
            if (part.CommittedOffset >= first)
                depth += part.CommittedOffset - first + 1;
        }

        return depth;
    }

    private async Task PublishDeadLetterAsync(string streamName, string consumerName, int partition, StoredRecord source, int deliveries, CancellationToken cancellationToken)
    {
        if (OnDeadLetter is not null)
        {
            var handled = await OnDeadLetter(streamName, source, cancellationToken).ConfigureAwait(false);
            if (handled)
                return;
        }

        await EnsureStreamAsync(new StreamSpec(DeadLetterStream, ["$dlq.>"]), cancellationToken).ConfigureAwait(false);
        var headers = new List<MessageHeader>
        {
            new("Nuvexa-Original-Subject", System.Text.Encoding.UTF8.GetBytes(source.Subject)),
            new("Nuvexa-Original-Stream", System.Text.Encoding.UTF8.GetBytes(streamName)),
            new("Nuvexa-Consumer", System.Text.Encoding.UTF8.GetBytes(consumerName)),
            new("Nuvexa-Original-Offset", System.Text.Encoding.UTF8.GetBytes($"{partition}:{source.Offset}")),
            new("Nuvexa-Deliveries", System.Text.Encoding.UTF8.GetBytes(deliveries.ToString()))
        };
        headers.AddRange(source.Headers);
        var dlq = GetStream(DeadLetterStream);
        await AppendAsync(dlq, $"$dlq.{streamName}", null, headers, source.Payload, cancellationToken).ConfigureAwait(false);
    }

    private async Task<PublishReceipt> AppendAsync(StreamState stream, string subject, string? key, IReadOnlyList<MessageHeader>? headers, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var keyBytes = SubjectFilter.EncodeKey(key);
        var partition = SubjectFilter.PartitionFor(keyBytes, stream.Partitions.Length, ref stream.RoundRobin);
        var copiedHeaders = headers is null
            ? []
            : headers.Select(header => new MessageHeader(header.Name, header.Value.ToArray())).ToArray();
        var offset = await stream.Partitions[partition].AppendAsync(new AppendRequest
        {
            Key = keyBytes.ToArray(),
            Subject = subject,
            Headers = copiedHeaders,
            Payload = payload.ToArray()
        }, cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _messagesIn);
        return new PublishReceipt(stream.Meta.Name, partition, offset);
    }

    private List<StreamState> Matching(string subject)
    {
        lock (_gate)
        {
            return _streams.Values
                .Where(stream => !stream.Meta.Queue && stream.Meta.Name != DeadLetterStream && stream.Meta.Filters.Any(filter => SubjectFilter.Matches(filter, subject)))
                .ToList();
        }
    }

    private StreamState OpenStream(StreamMetaDto meta)
    {
        var state = new StreamState(meta);
        var root = Path.Combine(StreamRoot, meta.Name);
        Directory.CreateDirectory(root);
        for (var i = 0; i < meta.PartitionCount; i++)
        {
            var directory = Path.Combine(root, "p" + i);
            var log = PartitionLog.Open(directory, _options);
            log.OnCommitted = _signal.Pulse;
            state.Partitions[i] = log;
            foreach (var file in Directory.GetFiles(directory, "consumer-*.json"))
            {
                var dto = JsonSerializer.Deserialize(File.ReadAllText(file), EngineJsonContext.Default.ConsumerDto);
                if (dto is null)
                    continue;
                foreach (var pending in dto.Pending)
                    pending.DeliveredAtUnixMs = 0;

                if (!state.Consumers.TryGetValue(dto.Name, out var group))
                {
                    var cursors = new ConsumerCursor[meta.PartitionCount];
                    group = new ConsumerGroup(dto.Name, cursors, ephemeral: false);
                    state.Consumers.Add(dto.Name, group);
                }

                group.Cursors[i] = new ConsumerCursor
                {
                    Name = dto.Name,
                    Filter = dto.Filter,
                    AckWaitMs = dto.AckWaitMs,
                    MaxDeliver = dto.MaxDeliver,
                    MaxAckPending = dto.MaxAckPending,
                    NextOffset = dto.NextOffset,
                    Pending = dto.Pending.ToDictionary(item => item.Offset),
                    Ephemeral = false,
                    Path = file
                };
            }
        }

        foreach (var group in state.Consumers.Values)
        {
            var sample = group.Cursors.FirstOrDefault(cursor => cursor is not null);
            if (sample is null)
                continue;
            for (var i = 0; i < group.Cursors.Length; i++)
            {
                if (group.Cursors[i] is not null)
                    continue;
                group.Cursors[i] = new ConsumerCursor
                {
                    Name = sample.Name,
                    Filter = sample.Filter,
                    AckWaitMs = sample.AckWaitMs,
                    MaxDeliver = sample.MaxDeliver,
                    MaxAckPending = sample.MaxAckPending,
                    NextOffset = Tail(state.Partitions[i]),
                    Ephemeral = false,
                    Path = ConsumerPath(meta.Name, i, sample.Name)
                };
            }
        }

        ApplyRetentionSettings(state);
        return state;
    }

    private void ApplyRetentionSettings(StreamState state)
    {
        TimeSpan? age = state.Meta.MaxAgeMs is long ms ? TimeSpan.FromMilliseconds(ms) : null;
        foreach (var partition in state.Partitions)
        {
            if (partition is null)
                continue;
            partition.MaxAge = age;
            partition.MaxBytes = state.Meta.MaxBytes;
        }
    }

    private void PersistMeta(StreamMetaDto meta)
    {
        var directory = Path.Combine(StreamRoot, meta.Name);
        Directory.CreateDirectory(directory);
        var json = JsonSerializer.Serialize(meta, EngineJsonContext.Default.StreamMetaDto);
        AtomicFile.Write(Path.Combine(directory, "stream.json"), json);
    }

    private void PersistCursor(ConsumerCursor cursor)
    {
        if (cursor.Ephemeral || cursor.Path.Length == 0)
            return;

        lock (_cursorLock)
        {
            _dirtyCursors[cursor.Path] = cursor;
            Monitor.Pulse(_cursorLock);
        }
    }

    private void FlushCursors()
    {
        while (true)
        {
            List<ConsumerCursor> dirty;
            var stop = false;
            lock (_cursorLock)
            {
                if (_dirtyCursors.Count == 0 && _cursorStop == 0)
                    Monitor.Wait(_cursorLock, 5);
                stop = _cursorStop == 1;
                dirty = _dirtyCursors.Values.ToList();
                _dirtyCursors.Clear();
            }

            foreach (var cursor in dirty)
                WriteCursor(cursor);

            if (!stop)
                continue;

            lock (_cursorLock)
            {
                if (_dirtyCursors.Count == 0)
                    return;
            }
        }
    }

    private static void WriteCursor(ConsumerCursor cursor)
    {
        ConsumerDto dto;
        cursor.Gate.Wait();
        try
        {
            dto = new ConsumerDto
            {
                Name = cursor.Name,
                Filter = cursor.Filter,
                AckWaitMs = cursor.AckWaitMs,
                MaxDeliver = cursor.MaxDeliver,
                MaxAckPending = cursor.MaxAckPending,
                NextOffset = cursor.NextOffset,
                Pending = cursor.Pending.Values.Select(item => new PendingDto
                {
                    Offset = item.Offset,
                    DeliveryCount = item.DeliveryCount,
                    DeliveredAtUnixMs = item.DeliveredAtUnixMs,
                    DeadLettering = item.DeadLettering
                }).ToList()
            };
        }
        finally
        {
            cursor.Gate.Release();
        }

        AtomicFile.Write(cursor.Path, JsonSerializer.Serialize(dto, EngineJsonContext.Default.ConsumerDto));
    }

    private StreamState GetStream(string name)
    {
        lock (_gate)
        {
            if (_streams.TryGetValue(name, out var stream))
                return stream;
        }

        throw new NuvexaMqException(NuvexaMqError.NotFound, $"Stream '{name}' was not found.");
    }

    private static ConsumerGroup GetConsumer(StreamState stream, string name)
    {
        lock (stream.Consumers)
        {
            if (stream.Consumers.TryGetValue(name, out var group))
                return group;
        }

        throw new NuvexaMqException(NuvexaMqError.NotFound, $"Consumer '{name}' was not found on stream '{stream.Meta.Name}'.");
    }

    private ConsumerCursor Cursor(string streamName, string consumerName, int partition)
    {
        var stream = GetStream(streamName);
        var group = GetConsumer(stream, consumerName);
        if (partition < 0 || partition >= group.Cursors.Length || group.Cursors[partition] is null)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Partition is out of range.");
        return group.Cursors[partition];
    }

    private string ConsumerPath(string stream, int partition, string consumer) =>
        Path.Combine(StreamRoot, stream, "p" + partition, "consumer-" + consumer + ".json");

    private static long Tail(PartitionLog log) => log.CommittedOffset < 0 ? 0 : log.CommittedOffset + 1;

    private static bool Accepts(string filter, string subject) =>
        filter.Length == 0 || SubjectFilter.Matches(filter, subject);

    private static bool SameFilters(IReadOnlyList<string> left, IReadOnlyList<string> right) =>
        left.Count == right.Count && left.OrderBy(x => x, StringComparer.Ordinal).SequenceEqual(right.OrderBy(x => x, StringComparer.Ordinal));

    private static Delivery ToDelivery(string stream, string consumer, int partition, StoredRecord record, int deliveryCount) =>
        new()
        {
            Stream = stream,
            Consumer = consumer,
            Partition = partition,
            Offset = record.Offset,
            TimestampUnixMs = record.TimestampUnixMs,
            DeliveryCount = deliveryCount,
            Subject = record.Subject,
            Key = record.Key,
            Headers = record.Headers,
            Payload = record.Payload
        };

    private void ThrowIfDisposed()
    {
        if (_disposed == 1)
            throw new NuvexaMqException(NuvexaMqError.Closed, "The broker is closed.");
    }

    private sealed class StreamState
    {
        public StreamState(StreamMetaDto meta)
        {
            Meta = meta;
            Partitions = new PartitionLog[meta.PartitionCount];
        }

        public StreamMetaDto Meta { get; }

        public PartitionLog[] Partitions { get; }

        public Dictionary<string, ConsumerGroup> Consumers { get; } = new(StringComparer.Ordinal);

        public int RoundRobin;
    }

    private sealed class ConsumerGroup(string name, ConsumerCursor[] cursors, bool ephemeral)
    {
        public string Name { get; } = name;

        public ConsumerCursor[] Cursors { get; } = cursors;

        public bool Ephemeral { get; } = ephemeral;

        public int Start;
    }

    private sealed class ConsumerCursor
    {
        public SemaphoreSlim Gate { get; } = new(1, 1);

        public string Name { get; init; } = "";

        public string Filter { get; init; } = "";

        public int AckWaitMs { get; init; }

        public int MaxDeliver { get; init; }

        public int MaxAckPending { get; init; }

        public long NextOffset { get; set; }

        public Dictionary<long, PendingDto> Pending { get; init; } = [];

        public bool Ephemeral { get; init; }

        public string Path { get; init; } = "";
    }
}

internal sealed class CommitSignal
{
    private readonly object _gate = new();
    private readonly List<TaskCompletionSource<bool>> _waiters = [];

    public void Pulse()
    {
        TaskCompletionSource<bool>[] pending;
        lock (_gate)
        {
            pending = _waiters.ToArray();
            _waiters.Clear();
        }

        foreach (var waiter in pending)
            waiter.TrySetResult(true);
    }

    public async Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero)
            return;

        var waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
            _waiters.Add(waiter);

        using var registration = cancellationToken.Register(() => waiter.TrySetCanceled(cancellationToken));
        var delay = Task.Delay(timeout);
        var completed = await Task.WhenAny(waiter.Task, delay).ConfigureAwait(false);
        if (completed != waiter.Task)
        {
            lock (_gate)
                _waiters.Remove(waiter);
            return;
        }

        await waiter.Task.ConfigureAwait(false);
    }
}
