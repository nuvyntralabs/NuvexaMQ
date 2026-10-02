using System.Globalization;

namespace Nuventra.NuvexaMQ.Desktop;

public sealed class HourlyLogWriter : IDisposable
{
    private readonly object _gate = new();
    private string? _root;
    private bool _enabled;
    private StreamWriter? _writer;
    private string? _path;
    private DateTime _fileHour;
    private bool _disposed;

    public void Configure(string logsDir, bool enabled)
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            var root = logsDir;
            if (_enabled == enabled && string.Equals(_root, root, StringComparison.Ordinal))
                return;
            Close();
            _root = root;
            _enabled = enabled;
            if (enabled)
                Open(DateTime.Now);
        }
    }

    public void Write(DateTime local, string line)
    {
        lock (_gate)
        {
            if (_disposed || !_enabled || _root is null)
                return;
            if (_writer is null || HourOf(local) != _fileHour)
                Open(local);
            _writer?.WriteLine(line);
        }
    }

    public void Roll(DateTime local)
    {
        lock (_gate)
        {
            if (_disposed || !_enabled || _root is null)
                return;
            if (_writer is not null && HourOf(local) == _fileHour)
            {
                _writer.Flush();
                return;
            }

            Open(local);
        }
    }

    public (int Skipped, string[] Lines) Tail(int max)
    {
        lock (_gate)
        {
            if (_path is null || !File.Exists(_path))
                return (0, []);
            _writer?.Flush();
            var ring = new Queue<string>(max);
            var seen = 0;
            foreach (var line in File.ReadLines(_path))
            {
                seen++;
                if (ring.Count == max)
                    ring.Dequeue();
                ring.Enqueue(line);
            }

            var skipped = Math.Max(0, seen - ring.Count);
            return (skipped, ring.ToArray());
        }
    }

    public static DateTime HourOf(DateTime local) =>
        new(local.Year, local.Month, local.Day, local.Hour, 0, 0, local.Kind);

    public static string FileName(DateTime local) =>
        $"log-{local.ToString("HHmm", CultureInfo.InvariantCulture)}.log";

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _enabled = false;
            Close();
        }
    }

    private void Open(DateTime local)
    {
        Close();
        var root = _root ?? throw new InvalidOperationException();
        var day = Path.Combine(root, local.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        Directory.CreateDirectory(day);
        _fileHour = HourOf(local);
        _path = ExistingHourFile(day, local) ?? Path.Combine(day, FileName(local));
        var stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        _writer = new StreamWriter(stream) { AutoFlush = false };
    }

    private static string? ExistingHourFile(string day, DateTime local)
    {
        if (!Directory.Exists(day))
            return null;
        var start = local.Hour * 100;
        var end = (local.Hour + 1) * 100;
        string? found = null;
        var foundStamp = -1;
        foreach (var path in Directory.EnumerateFiles(day, "log-*.log"))
        {
            if (!TryStamp(Path.GetFileName(path), out var stamp) || stamp < start || stamp >= end || stamp < foundStamp)
                continue;
            found = path;
            foundStamp = stamp;
        }

        return found;
    }

    private static bool TryStamp(string fileName, out int stamp)
    {
        stamp = 0;
        const string prefix = "log-";
        const string suffix = ".log";
        if (!fileName.StartsWith(prefix, StringComparison.Ordinal) || !fileName.EndsWith(suffix, StringComparison.Ordinal))
            return false;
        var body = fileName.AsSpan(prefix.Length, fileName.Length - prefix.Length - suffix.Length);
        return body.Length == 4 && int.TryParse(body, out stamp);
    }

    private void Close()
    {
        _writer?.Flush();
        _writer?.Dispose();
        _writer = null;
        _path = null;
    }
}
