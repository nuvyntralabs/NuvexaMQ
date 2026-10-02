using System.Text;
using Nuventra.NuvexaMQ.Server;

namespace Nuventra.NuvexaMQ.Desktop;

public sealed class BrokerController : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly LineWriter _writer;
    private NuvexaMqServer? _server;
    private int _installed;

    public BrokerController()
    {
        _writer = new LineWriter(line => Logged?.Invoke(line));
    }

    public bool IsRunning
    {
        get
        {
            lock (_gate)
                return _server is not null;
        }
    }

    public event Action<string>? Logged;

    public async Task StartAsync(DesktopSettings settings)
    {
        if (Interlocked.Exchange(ref _installed, 1) == 0)
            Console.SetOut(_writer);

        NuvexaMqServer server;
        try
        {
            server = await NuvexaMqServer.StartAsync(ToOptions(settings)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logged?.Invoke(ex.Message);
            throw;
        }

        lock (_gate)
        {
            if (_server is not null)
            {
                _ = server.DisposeAsync();
                return;
            }

            _server = server;
        }

        StartupLog.WriteBanner(
            ToOptions(settings),
            server.ListenPort,
            server.HealthPort,
            server.ManagementPort,
            server.ManagementHttpsPort,
            server.Tls,
            server.GeneratedHttpsCertificate);
    }

    public async Task StopAsync()
    {
        NuvexaMqServer? server;
        lock (_gate)
        {
            server = _server;
            _server = null;
        }

        if (server is null)
            return;

        await server.DisposeAsync().ConfigureAwait(false);
        Logged?.Invoke($"{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}: NuvexaMQ stopped.");
    }

    public static NuvexaMqOptions ToOptions(DesktopSettings settings) => new()
    {
        ListenAddress = "0.0.0.0",
        ListenPort = settings.BrokerPort,
        HealthAddress = "0.0.0.0",
        HealthPort = settings.HealthPort,
        ManagementAddress = "0.0.0.0",
        ManagementPort = settings.AdminHttpPort,
        ManagementHttpsPort = settings.AdminHttpsPort,
        DataDir = string.IsNullOrWhiteSpace(settings.DataDir) ? DesktopSettings.DefaultDataDir() : settings.DataDir,
        Verbose = settings.Verbose,
        ManagementUser = "guest",
        ManagementPassword = "guest"
    };

    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

    private sealed class LineWriter : TextWriter
    {
        private readonly Action<string> _onLine;
        private readonly StringBuilder _buffer = new();

        public LineWriter(Action<string> onLine) => _onLine = onLine;

        public override Encoding Encoding => Encoding.UTF8;

        public override void Write(char value) => Emit(value.ToString());

        public override void Write(string? value)
        {
            if (!string.IsNullOrEmpty(value))
                Emit(value);
        }

        public override void Write(char[] buffer, int index, int count)
        {
            if (count > 0)
                Emit(new string(buffer, index, count));
        }

        private void Emit(string value)
        {
            List<string>? lines = null;
            lock (_buffer)
            {
                foreach (var ch in value)
                {
                    if (ch == '\n')
                    {
                        lines ??= [];
                        lines.Add(_buffer.ToString().TrimEnd('\r'));
                        _buffer.Clear();
                    }
                    else
                    {
                        _buffer.Append(ch);
                    }
                }
            }

            if (lines is null)
                return;
            foreach (var line in lines)
                _onLine(line);
        }
    }
}
