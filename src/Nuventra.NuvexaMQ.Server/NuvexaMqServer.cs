using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Logging;
using Nuventra.NuvexaMQ.Engine;
using Nuventra.NuvexaMQ.Protocol;
using EngineError = Nuventra.NuvexaMQ.Engine.NuvexaMqError;
using EngineException = Nuventra.NuvexaMQ.Engine.NuvexaMqException;
using EngineStream = Nuventra.NuvexaMQ.Engine.StreamSpec;
using EngineConsume = Nuventra.NuvexaMQ.Engine.ConsumeSpec;
using EngineStart = Nuventra.NuvexaMQ.Engine.ConsumeStart;
using WireError = Nuventra.NuvexaMQ.Protocol.NuvexaMqError;

namespace Nuventra.NuvexaMQ.Server;

public sealed class NuvexaMqServer : IAsyncDisposable
{
    private readonly NuvexaMqOptions _options;
    private readonly TcpListener _listener;
    private readonly WebApplication _app;
    private readonly WebApplication _management;
    private readonly ConnectionTable _connections;
    private readonly Dictionary<TcpClient, ClientSession> _sessions = [];
    private readonly NuvexaTopology _topology;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<TcpClient> _clients = [];
    private readonly X509Certificate2? _certificate;
    private readonly X509Certificate2? _httpsCertificate;
    private readonly bool _ownsHttpsCertificate;
    private int _connectionCount;
    private Task? _accept;

    private NuvexaMqServer(NuvexaMqOptions options, NuvexaBroker broker, NuvexaTopology topology, TcpListener listener, WebApplication app, WebApplication management, ConnectionTable connections, int healthPort, int managementPort, int managementHttpsPort, X509Certificate2? certificate, X509Certificate2 httpsCertificate, bool ownsHttpsCertificate)
    {
        _options = options;
        Broker = broker;
        _topology = topology;
        _listener = listener;
        _app = app;
        _management = management;
        _connections = connections;
        HealthPort = healthPort;
        ManagementPort = managementPort;
        ManagementHttpsPort = managementHttpsPort;
        _certificate = certificate;
        _httpsCertificate = httpsCertificate;
        _ownsHttpsCertificate = ownsHttpsCertificate;
        ListenPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        Tls = certificate is not null;
        GeneratedHttpsCertificate = ownsHttpsCertificate;
    }

    public NuvexaBroker Broker { get; }

    public int ListenPort { get; }

    public int HealthPort { get; }

    public int ManagementPort { get; }

    public int ManagementHttpsPort { get; }

    public bool Tls { get; }

    public bool GeneratedHttpsCertificate { get; }

    public static async Task<NuvexaMqServer> StartAsync(NuvexaMqOptions options, CancellationToken cancellationToken = default)
    {
        var broker = NuvexaBroker.Open(options.DataDir, new BrokerOptions
        {
            FlushInterval = TimeSpan.FromMilliseconds(Math.Max(0, options.FlushIntervalMs)),
            FlushMaxRecords = Math.Max(1, options.FlushMaxRecords),
            MaxMessageBytes = Math.Max(1, options.MaxMessageBytes),
            SegmentBytes = Math.Max(64, options.SegmentBytes)
        });

        X509Certificate2? certificate = null;
        if (!string.IsNullOrWhiteSpace(options.CertificatePath))
            certificate = LoadCertificate(options.CertificatePath, options.KeyPath);
        var ownsHttps = certificate is null;
        var httpsCertificate = certificate ?? CreateDevelopmentCertificate();

        var listenIp = ParseAddress(options.ListenAddress);
        var listener = new TcpListener(listenIp, options.ListenPort);
        listener.Start();

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = [],
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseUrls($"http://{options.HealthAddress}:{options.HealthPort}");
        var app = builder.Build();
        app.MapGet("/health", () => Results.Json(new { status = "ok" }));
        app.MapGet("/metrics", () => Results.Text(Prometheus(broker.Snapshot()), "text/plain; version=0.0.4; charset=utf-8"));
        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        var healthPort = BoundPort(app);
        var listenPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        var connections = new ConnectionTable();
        var topology = NuvexaTopology.Open(broker, options.DataDir, options.ManagementUser, options.ManagementPassword);
        WebApplication management;
        try
        {
            management = await ManagementHost.StartAsync(options, broker, topology, connections, listenPort, healthPort, certificate is not null, httpsCertificate, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            listener.Stop();
            await app.StopAsync().ConfigureAwait(false);
            await app.DisposeAsync().ConfigureAwait(false);
            await broker.DisposeAsync().ConfigureAwait(false);
            if (ownsHttps)
                httpsCertificate.Dispose();
            certificate?.Dispose();
            throw;
        }

        var ports = ManagementHost.BoundPorts(management);
        var server = new NuvexaMqServer(options, broker, topology, listener, app, management, connections, healthPort, ports.Http, ports.Https, certificate, httpsCertificate, ownsHttps);
        server._accept = Task.Run(server.AcceptLoop);
        return server;
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        TcpClient[] clients;
        lock (_clients)
        {
            clients = _clients.ToArray();
            _clients.Clear();
        }

        foreach (var client in clients)
            client.Dispose();

        if (_accept is not null)
        {
            try
            {
                await _accept.ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
            }
        }

        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        await _management.StopAsync().ConfigureAwait(false);
        await _management.DisposeAsync().ConfigureAwait(false);
        await Broker.DisposeAsync().ConfigureAwait(false);
        _certificate?.Dispose();
        if (_ownsHttpsCertificate)
            _httpsCertificate?.Dispose();
    }

    private async Task AcceptLoop()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException or InvalidOperationException)
            {
                break;
            }

            client.NoDelay = true;
            if (Interlocked.Increment(ref _connectionCount) > Math.Max(1, _options.MaxConnections))
            {
                Interlocked.Decrement(ref _connectionCount);
                StartupLog.Verbose(_options.Verbose, $"New connection from {Peer(client)} on port {ListenPort} disconnected: too many connections.");
                client.Dispose();
                continue;
            }

            lock (_clients)
                _clients.Add(client);
            _connections.Add(client);
            _ = Task.Run(() => HandleAsync(client));
        }
    }

    private async Task HandleAsync(TcpClient client)
    {
        var ephemeral = new List<(string Stream, string Name)>();
        var announced = false;
        var shown = "";
        var peer = Peer(client);
        try
        {
            StartupLog.Verbose(_options.Verbose, $"New connection from {peer} on port {ListenPort}.");
            announced = true;
            Stream stream = client.GetStream();
            if (_certificate is not null)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsServerAsync(_certificate, clientCertificateRequired: false, checkCertificateRevocation: false).ConfigureAwait(false);
                stream = ssl;
            }

            var hello = await FrameCodec.ReadAsync(stream, _stop.Token).ConfigureAwait(false);
            if (hello.Type != Op.Hello)
            {
                await WriteErrorAsync(stream, hello.RequestId, WireError.Invalid, "The first frame must be hello.").ConfigureAwait(false);
                return;
            }

            var (token, name, user, password, vhost) = Payloads.ReadHello(hello.Payload);
            shown = string.IsNullOrEmpty(name) ? "<unknown>" : name;
            if (!string.IsNullOrEmpty(_options.Token) && !TokenMatches(token))
            {
                StartupLog.Verbose(_options.Verbose, "Client <unknown> disconnected: not authorised.");
                announced = false;
                await WriteErrorAsync(stream, hello.RequestId, WireError.Unauthorized, "The cluster token was rejected.").ConfigureAwait(false);
                return;
            }

            var session = new ClientSession { Unlimited = true, Vhost = "/", ClientName = shown };
            if (!string.IsNullOrEmpty(user))
            {
                var remote = client.Client.RemoteEndPoint is IPEndPoint endpoint ? endpoint.Address : null;
                if (!_topology.TryLogin(user, password, remote, management: false, out var reason))
                {
                    StartupLog.Verbose(_options.Verbose, "Client <unknown> disconnected: not authorised.");
                    announced = false;
                    await WriteErrorAsync(stream, hello.RequestId, WireError.Unauthorized, reason).ConfigureAwait(false);
                    return;
                }

                if (string.IsNullOrEmpty(vhost))
                    vhost = "/";
                if (!_topology.VhostExists(vhost))
                {
                    await WriteErrorAsync(stream, hello.RequestId, WireError.NotFound, $"Virtual host '{vhost}' was not found.").ConfigureAwait(false);
                    return;
                }

                session = new ClientSession { User = user, Vhost = vhost, ClientName = shown };
                _connections.Identify(client, user, vhost);
            }

            _sessions[client] = session;
            _connections.Open(client, shown);
            var loginUser = string.IsNullOrEmpty(session.User) ? "" : session.User;
            StartupLog.Verbose(_options.Verbose, $"New client connected from {peer} as {shown} (u'{loginUser}', v'{session.Vhost}').");

            await FrameCodec.WriteAsync(stream, new Frame(Op.HelloOk, hello.RequestId, Payloads.HelloOk(StartupLog.ProductVersion)), _stop.Token).ConfigureAwait(false);
            while (!_stop.IsCancellationRequested)
            {
                Frame request;
                try
                {
                    request = await FrameCodec.ReadAsync(stream, _stop.Token).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    break;
                }

                var response = await DispatchAsync(request, ephemeral, session, _connections.IdOf(client)).ConfigureAwait(false);
                await FrameCodec.WriteAsync(stream, response, _stop.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or InvalidDataException)
        {
        }
        finally
        {
            foreach (var (streamName, name) in ephemeral)
            {
                try
                {
                    Broker.ReleaseConsumer(streamName, name);
                }
                catch (EngineException)
                {
                }
            }

            var connectionId = _connections.IdOf(client);
            lock (_clients)
                _clients.Remove(client);
            _sessions.Remove(client);
            _connections.Remove(client);
            client.Dispose();
            Interlocked.Decrement(ref _connectionCount);
            if (announced)
                StartupLog.Verbose(_options.Verbose, $"Client {shown} disconnected.");
            if (!string.IsNullOrEmpty(connectionId))
            {
                try
                {
                    await _topology.CloseConnectionAsync(connectionId).ConfigureAwait(false);
                }
                catch (EngineException)
                {
                }
            }
        }
    }

    private async Task<Frame> DispatchAsync(Frame request, List<(string Stream, string Name)> ephemeral, ClientSession session, string? connectionId)
    {
        try
        {
            switch (request.Type)
            {
                case Op.Ping:
                    return new Frame(Op.Pong, request.RequestId, []);
                case Op.EnsureStream:
                {
                    var body = Payloads.ReadEnsureStream(request.Payload);
                    Demand(session, "configure", body.Name);
                    var spec = new EngineStream(body.Name, body.Filters)
                    {
                        PartitionCount = body.PartitionCount,
                        MaxAge = body.MaxAgeMs < 0 ? null : TimeSpan.FromMilliseconds(body.MaxAgeMs),
                        MaxBytes = body.MaxBytes < 0 ? null : body.MaxBytes,
                        MaxMessageBytes = body.MaxMessageBytes == 0 ? null : body.MaxMessageBytes
                    };
                    await Broker.EnsureStreamAsync(spec).ConfigureAwait(false);
                    return new Frame(Op.EnsureStreamOk, request.RequestId, []);
                }
                case Op.Publish:
                {
                    var body = Payloads.ReadPublish(request.Payload);
                    if (!session.Unlimited)
                    {
                        foreach (var stream in Broker.ListStreams())
                        {
                            if (stream.Queue)
                                continue;
                            if (stream.Filters.Any(filter => SubjectFilter.Matches(filter, body.Subject)))
                                Demand(session, "write", stream.Name);
                        }
                    }

                    var headers = body.Headers.Select(header => new MessageHeader(header.Name, header.Value)).ToArray();
                    var receipts = await Broker.PublishAsync(body.Subject, body.Payload, string.IsNullOrEmpty(body.Key) ? null : body.Key, headers).ConfigureAwait(false);
                    StartupLog.Verbose(_options.Verbose, $"Received PUBLISH from {session.ClientName} ('{body.Subject}', ... ({body.Payload.Length} bytes))");
                    var wire = receipts.Select(receipt => new PublishReceiptWire(receipt.Stream, receipt.Partition, receipt.Offset)).ToArray();
                    return new Frame(Op.PublishOk, request.RequestId, Payloads.PublishOk(wire));
                }
                case Op.EnsureConsumer:
                {
                    var body = Payloads.ReadEnsureConsumer(request.Payload);
                    Demand(session, "read", body.Stream);
                    var spec = new EngineConsume(body.Stream, body.Name)
                    {
                        Filter = string.IsNullOrEmpty(body.Filter) ? null : body.Filter,
                        AckWait = TimeSpan.FromMilliseconds(body.AckWaitMs),
                        MaxDeliver = body.MaxDeliver,
                        MaxAckPending = body.MaxAckPending,
                        Ephemeral = body.Ephemeral,
                        Start = (EngineStart)body.Start,
                        StartOffset = body.StartOffset
                    };
                    Broker.EnsureConsumer(spec);
                    if (body.Ephemeral && !ephemeral.Contains((body.Stream, body.Name)))
                        ephemeral.Add((body.Stream, body.Name));
                    return new Frame(Op.EnsureConsumerOk, request.RequestId, []);
                }
                case Op.Fetch:
                {
                    var body = Payloads.ReadFetch(request.Payload);
                    Demand(session, "read", body.Stream);
                    var expires = TimeSpan.FromMilliseconds(Math.Clamp(body.ExpiresMs, 0, 30_000));
                    var fetched = await Broker.FetchAsync(body.Stream, body.Consumer, Math.Max(1, body.MaxMessages), expires, _stop.Token).ConfigureAwait(false);
                    var messages = fetched.Messages.Select(message => new DeliveryWire(
                        message.Partition,
                        message.Offset,
                        message.TimestampUnixMs,
                        message.DeliveryCount,
                        message.Subject,
                        message.Key,
                        message.Headers.Select(header => new HeaderWire(header.Name, header.Value)).ToArray(),
                        message.Payload)).ToArray();
                    return new Frame(Op.FetchOk, request.RequestId, Payloads.FetchOk(fetched.OffsetReset, fetched.FirstOffset, messages));
                }
                case Op.Ack:
                {
                    var body = Payloads.ReadAck(request.Payload);
                    Demand(session, "read", body.Stream);
                    await Broker.AckAsync(body.Stream, body.Consumer, body.Partition, body.Offset).ConfigureAwait(false);
                    return new Frame(Op.AckOk, request.RequestId, []);
                }
                case Op.Nack:
                {
                    var body = Payloads.ReadAck(request.Payload);
                    Demand(session, "read", body.Stream);
                    await Broker.NackAsync(body.Stream, body.Consumer, body.Partition, body.Offset).ConfigureAwait(false);
                    return new Frame(Op.NackOk, request.RequestId, []);
                }
                case Op.ResetConsumer:
                {
                    var body = Payloads.ReadReset(request.Payload);
                    Demand(session, "read", body.Stream);
                    Broker.ResetConsumer(body.Stream, body.Consumer, body.Absolute ? body.Offset : null);
                    return new Frame(Op.ResetConsumerOk, request.RequestId, []);
                }
                case Op.ReleaseConsumer:
                {
                    var body = Payloads.ReadRelease(request.Payload);
                    Demand(session, "read", body.Stream);
                    Broker.ReleaseConsumer(body.Stream, body.Consumer);
                    ephemeral.RemoveAll(item => item.Stream == body.Stream && item.Name == body.Consumer);
                    return new Frame(Op.ReleaseConsumerOk, request.RequestId, []);
                }
                case Op.DeclareExchange:
                {
                    var body = Payloads.ReadDeclareExchange(request.Payload);
                    Demand(session, "configure", body.Name);
                    _topology.DeclareExchange(session.Vhost, body.Name, body.Type, body.Durable, body.AutoDelete);
                    return new Frame(Op.DeclareExchangeOk, request.RequestId, []);
                }
                case Op.DeclareQueue:
                {
                    var body = Payloads.ReadDeclareQueue(request.Payload);
                    Demand(session, "configure", body.Name);
                    await _topology.DeclareQueueAsync(session.Vhost, body.Name, new QueueOptions
                    {
                        Durable = body.Durable,
                        Exclusive = body.Exclusive,
                        AutoDelete = body.AutoDelete,
                        MessageTtlMs = body.MessageTtlMs < 0 ? null : body.MessageTtlMs,
                        MaxLength = body.MaxLength < 0 ? null : body.MaxLength,
                        DeadLetterExchange = body.DeadLetterExchange,
                        DeadLetterRoutingKey = body.DeadLetterRoutingKey
                    }, connectionId).ConfigureAwait(false);
                    return new Frame(Op.DeclareQueueOk, request.RequestId, []);
                }
                case Op.BindQueue:
                {
                    var body = Payloads.ReadBindQueue(request.Payload);
                    Demand(session, "write", body.Exchange);
                    Demand(session, "read", body.Queue);
                    _topology.Bind(session.Vhost, body.Exchange, body.Queue, body.RoutingKey, body.Arguments.ToDictionary(item => item.Name, item => item.Value));
                    return new Frame(Op.BindQueueOk, request.RequestId, []);
                }
                case Op.DeleteQueue:
                {
                    var name = Payloads.ReadNamed(request.Payload);
                    Demand(session, "configure", name);
                    await _topology.DeleteQueueAsync(session.Vhost, name).ConfigureAwait(false);
                    return new Frame(Op.DeleteQueueOk, request.RequestId, []);
                }
                case Op.DeleteExchange:
                {
                    var name = Payloads.ReadNamed(request.Payload);
                    Demand(session, "configure", name);
                    _topology.DeleteExchange(session.Vhost, name);
                    return new Frame(Op.DeleteExchangeOk, request.RequestId, []);
                }
                case Op.PurgeQueue:
                {
                    var name = Payloads.ReadNamed(request.Payload);
                    Demand(session, "read", name);
                    _topology.Purge(session.Vhost, name);
                    return new Frame(Op.PurgeQueueOk, request.RequestId, []);
                }
                case Op.PublishExchange:
                {
                    var body = Payloads.ReadPublishExchange(request.Payload);
                    Demand(session, "write", body.Exchange);
                    var headers = body.Headers.Select(header => new MessageHeader(header.Name, header.Value)).ToArray();
                    var receipts = await _topology.PublishAsync(session.Vhost, body.Exchange, body.RoutingKey, body.Payload, string.IsNullOrEmpty(body.Key) ? null : body.Key, headers).ConfigureAwait(false);
                    var exchange = body.Exchange.Length == 0 ? "(AMQP default)" : body.Exchange;
                    StartupLog.Verbose(_options.Verbose, $"Received PUBLISH from {session.ClientName} to {exchange} ('{body.RoutingKey}', ... ({body.Payload.Length} bytes))");
                    var wire = receipts.Select(receipt => new PublishReceiptWire(receipt.Stream, receipt.Partition, receipt.Offset)).ToArray();
                    return new Frame(Op.PublishExchangeOk, request.RequestId, Payloads.PublishOk(wire));
                }
                default:
                    return Error(request.RequestId, WireError.Invalid, "Unknown frame.", 0);
            }
        }
        catch (EngineException ex)
        {
            return Error(request.RequestId, Map(ex.Error), ex.Message, ex.Detail);
        }
        catch (InvalidDataException ex)
        {
            return Error(request.RequestId, WireError.Invalid, ex.Message, 0);
        }
    }

    private void Demand(ClientSession session, string permission, string resource)
    {
        if (session.Unlimited)
            return;
        if (!_topology.Allowed(session.User, session.Vhost, permission, resource))
            throw new EngineException(EngineError.Unauthorized, $"User '{session.User}' cannot {permission} '{resource}'.");
    }

    private bool TokenMatches(string presented)
    {
        if (string.IsNullOrEmpty(_options.Token))
            return true;

        var expected = Encoding.UTF8.GetBytes(_options.Token);
        var actual = Encoding.UTF8.GetBytes(presented);
        return expected.Length == actual.Length && CryptographicOperations.FixedTimeEquals(expected, actual);
    }

    private static async Task WriteErrorAsync(Stream stream, uint requestId, WireError error, string message)
    {
        await FrameCodec.WriteAsync(stream, Error(requestId, error, message, 0), CancellationToken.None).ConfigureAwait(false);
    }

    private static Frame Error(uint requestId, WireError error, string message, long detail) =>
        new(Op.Error, requestId, Payloads.Error(error, message, detail));

    private static WireError Map(EngineError error) => error switch
    {
        EngineError.Unauthorized => WireError.Unauthorized,
        EngineError.NotFound => WireError.NotFound,
        EngineError.Conflict => WireError.Conflict,
        EngineError.TooLarge => WireError.TooLarge,
        EngineError.OffsetReset => WireError.OffsetReset,
        EngineError.Closed => WireError.Closed,
        _ => WireError.Invalid
    };

    private static X509Certificate2 CreateDevelopmentCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        using var issued = request.Create(
            request.SubjectName,
            X509SignatureGenerator.CreateForRSA(rsa, RSASignaturePadding.Pkcs1),
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1),
            [1, 2, 3, 4]);

        // On macOS, CopyWithPrivateKey stores the key in the login keychain and exporting it asks for the keychain password.
        if (OperatingSystem.IsMacOS())
        {
            var directory = Path.Combine(Path.GetTempPath(), "nuvexamq-" + Guid.NewGuid().ToString("n"));
            Directory.CreateDirectory(directory);
            try
            {
                var certPath = Path.Combine(directory, "cert.pem");
                var keyPath = Path.Combine(directory, "key.pem");
                File.WriteAllText(certPath, issued.ExportCertificatePem());
                File.WriteAllText(keyPath, rsa.ExportPkcs8PrivateKeyPem());
                return LoadCertificateWithOpenSsl(certPath, keyPath);
            }
            finally
            {
                Directory.Delete(directory, recursive: true);
            }
        }

        using var paired = issued.CopyWithPrivateKey(rsa);
        return LoadPkcs12(paired.Export(X509ContentType.Pfx), null);
    }

    private static X509Certificate2 LoadPkcs12(byte[] pfx, string? password)
    {
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadPkcs12(pfx, password);
#else
        return new X509Certificate2(pfx, password, X509KeyStorageFlags.Exportable);
#endif
    }

    private static X509Certificate2 LoadCertificate(string certificatePath, string? keyPath)
    {
        if (string.IsNullOrWhiteSpace(keyPath))
            return LoadPkcs12(File.ReadAllBytes(certificatePath), null);

        if (OperatingSystem.IsMacOS())
            return LoadCertificateWithOpenSsl(certificatePath, keyPath);

        using var pem = X509Certificate2.CreateFromPemFile(certificatePath, keyPath);
        return LoadPkcs12(pem.Export(X509ContentType.Pfx), null);
    }

    private static X509Certificate2 LoadCertificateWithOpenSsl(string certificatePath, string keyPath)
    {
        var pfxPath = Path.Combine(Path.GetTempPath(), "nuvexamq-" + Guid.NewGuid().ToString("n") + ".pfx");
        var password = Guid.NewGuid().ToString("n");
        try
        {
            var start = new ProcessStartInfo
            {
                FileName = "openssl",
                RedirectStandardError = true,
                UseShellExecute = false
            };
            start.ArgumentList.Add("pkcs12");
            start.ArgumentList.Add("-export");
            start.ArgumentList.Add("-in");
            start.ArgumentList.Add(certificatePath);
            start.ArgumentList.Add("-inkey");
            start.ArgumentList.Add(keyPath);
            start.ArgumentList.Add("-out");
            start.ArgumentList.Add(pfxPath);
            start.ArgumentList.Add("-passout");
            start.ArgumentList.Add("pass:" + password);
            using var process = Process.Start(start) ?? throw new InvalidOperationException("openssl is required to load a PEM certificate on this runtime.");
            var error = process.StandardError.ReadToEnd();
            process.WaitForExit();
            if (process.ExitCode != 0)
                throw new InvalidOperationException(string.IsNullOrWhiteSpace(error) ? "openssl could not build a certificate." : error.Trim());

            return LoadPkcs12(File.ReadAllBytes(pfxPath), password);
        }
        finally
        {
            if (File.Exists(pfxPath))
                File.Delete(pfxPath);
        }
    }

    private static IPAddress ParseAddress(string value) =>
        value is "0.0.0.0" or "*" ? IPAddress.Any : IPAddress.Parse(value);

    private static int BoundPort(WebApplication app)
    {
        var feature = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var address = feature?.Addresses.FirstOrDefault() ?? throw new InvalidOperationException("The health port did not bind.");
        return new Uri(address).Port;
    }

    private static string Prometheus(BrokerSnapshot snapshot)
    {
        var builder = new StringBuilder();
        builder.AppendLine("# TYPE nuvexamq_messages_in_total counter");
        builder.Append("nuvexamq_messages_in_total ").Append(snapshot.MessagesIn).AppendLine();
        builder.AppendLine("# TYPE nuvexamq_messages_out_total counter");
        builder.Append("nuvexamq_messages_out_total ").Append(snapshot.MessagesOut).AppendLine();
        builder.AppendLine("# TYPE nuvexamq_fsync_seconds gauge");
        builder.Append("nuvexamq_fsync_seconds ").Append(snapshot.LastFsyncSeconds.ToString("0.######", System.Globalization.CultureInfo.InvariantCulture)).AppendLine();
        builder.AppendLine("# TYPE nuvexamq_committed_offset gauge");
        foreach (var partition in snapshot.Partitions)
        {
            builder.Append("nuvexamq_committed_offset{stream=\"").Append(Escape(partition.Stream))
                .Append("\",partition=\"").Append(partition.Partition).Append("\"} ")
                .Append(partition.CommittedOffset).AppendLine();
        }

        builder.AppendLine("# TYPE nuvexamq_consumer_pending gauge");
        foreach (var consumer in snapshot.Consumers)
        {
            builder.Append("nuvexamq_consumer_pending{stream=\"").Append(Escape(consumer.Stream))
                .Append("\",consumer=\"").Append(Escape(consumer.Consumer))
                .Append("\",partition=\"").Append(consumer.Partition).Append("\"} ")
                .Append(consumer.Pending).AppendLine();
        }

        return builder.ToString();
    }

    private static string Peer(TcpClient client) =>
        client.Client.RemoteEndPoint is IPEndPoint endpoint ? $"{endpoint.Address}:{endpoint.Port}" : "unknown";

    private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}

internal sealed class ClientSession
{
    public string User { get; init; } = "";

    public string ClientName { get; init; } = "";

    public string Vhost { get; init; } = "/";

    public bool Unlimited { get; init; }
}
