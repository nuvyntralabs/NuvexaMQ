using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nuventra.NuvexaMQ.Engine;
using EngineException = Nuventra.NuvexaMQ.Engine.NuvexaMqException;
using EngineStream = Nuventra.NuvexaMQ.Engine.StreamSpec;

namespace Nuventra.NuvexaMQ.Server;

internal static class ManagementHost
{
    private const string CookieName = "nuvexamq_mgmt";
    private static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static async Task<WebApplication> StartAsync(
        NuvexaMqOptions options,
        NuvexaBroker broker,
        NuvexaTopology topology,
        ConnectionTable connections,
        int listenPort,
        int healthPort,
        bool tls,
        X509Certificate2 httpsCertificate,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(options.ManagementUser) || string.IsNullOrEmpty(options.ManagementPassword))
            throw new InvalidOperationException("Set ManagementUser and ManagementPassword. The management site does not start without them.");

        var sessions = new SessionStore();
        var site = new SiteInfo();
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = [],
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        var managementAddress = EndpointAddress(options.ManagementAddress);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(managementAddress, options.ManagementPort);
            kestrel.Listen(managementAddress, options.ManagementHttpsPort, listen => listen.UseHttps(httpsCertificate));
        });
        var app = builder.Build();
        app.Use(async (context, next) =>
        {
            if (IsPublic(context))
            {
                await next(context).ConfigureAwait(false);
                return;
            }

            if (!TryAuthenticate(context, topology, sessions, out var reason))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                await context.Response.WriteAsJsonAsync(new { error = "not_authorized", reason }).ConfigureAwait(false);
                return;
            }

            await next(context).ConfigureAwait(false);
        });

        app.MapGet("/", () => Results.Content(ConsolePage.Html, "text/html; charset=utf-8"));
        app.MapGet("/icon.png", () => Results.Bytes(ConsolePage.Icon, "image/png"));
        app.MapPost("/api/login", (HttpContext context, LoginRequest? body) => Login(context, topology, sessions, body));
        app.MapPost("/api/logout", (HttpContext context) =>
        {
            if (context.Request.Cookies.TryGetValue(CookieName, out var token))
                sessions.Remove(token);
            context.Response.Cookies.Delete(CookieName);
            return Results.NoContent();
        });
        app.MapGet("/api/whoami", (HttpContext context) => Results.Json(new
        {
            username = CurrentUser(context, options, sessions),
            localhostOnly = string.Equals(CurrentUser(context, options, sessions), "guest", StringComparison.Ordinal)
        }));
        app.MapGet("/api/overview", () => Results.Json(Overview(options, broker, topology, connections, listenPort, healthPort, site.Port, site.HttpsPort, tls)));
        app.MapGet("/api/connections", () => Results.Json(connections.Snapshot().Select(row => new
        {
            id = row.Id,
            client = row.ClientName,
            user = row.User,
            vhost = row.Vhost,
            peer = row.Peer,
            state = row.State,
            connectedAt = row.ConnectedAt
        })));
        app.MapGet("/api/channels", () => Results.Json(connections.Snapshot().Select(row => new
        {
            name = row.Id + ".1",
            connection = row.Id,
            user = row.User,
            vhost = row.Vhost,
            state = row.State,
            prefetch = 1000
        })));
        app.MapGet("/api/streams", () => Results.Json(Streams(broker)));
        app.MapGet("/api/streams/{name}", (string name) =>
        {
            var stream = Streams(broker).FirstOrDefault(item => item.Name == name);
            return stream is null ? Results.Json(new { error = "not_found", reason = $"Stream '{name}' was not found." }, statusCode: StatusCodes.Status404NotFound) : Results.Json(stream);
        });
        app.MapPut("/api/streams/{name}", async (string name, StreamRequest? body) =>
        {
            if (body?.Filters is null || body.Filters.Length == 0)
                return Results.Json(new { error = "invalid", reason = "A stream needs at least one subject filter." }, statusCode: StatusCodes.Status400BadRequest);
            try
            {
                await broker.EnsureStreamAsync(new EngineStream(name, body.Filters.Where(filter => !string.IsNullOrWhiteSpace(filter)))
                {
                    PartitionCount = body.PartitionCount is > 0 ? body.PartitionCount.Value : 1,
                    MaxAge = body.MaxAgeMs is null ? null : TimeSpan.FromMilliseconds(body.MaxAgeMs.Value),
                    MaxBytes = body.MaxBytes,
                    MaxMessageBytes = body.MaxMessageBytes
                }).ConfigureAwait(false);
                var created = Streams(broker).First(item => item.Name == name);
                return Results.Json(created);
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/consumers", (string? stream) =>
        {
            var consumers = broker.ListConsumers();
            if (!string.IsNullOrEmpty(stream))
                consumers = consumers.Where(item => item.Stream == stream).ToArray();
            return Results.Json(consumers);
        });
        app.MapGet("/api/streams/{name}/messages", (string name, int? partition, int? count, long? offset) =>
        {
            try
            {
                return Results.Json(ReadMessages(broker, name, partition ?? 0, Math.Clamp(count ?? 20, 1, 100), offset));
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/streams/{name}/messages", (string name, int? partition, long? offset) =>
        {
            if (offset is null)
                return Results.Json(new { error = "invalid", reason = "Delete requires an offset." }, statusCode: StatusCodes.Status400BadRequest);
            try
            {
                broker.DeleteMessage(name, partition ?? 0, offset.Value);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/streams/{name}", async (string name) =>
        {
            try
            {
                await topology.DeleteManagedStreamAsync(name).ConfigureAwait(false);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapPost("/api/publish", async (PublishRequest? body) =>
        {
            if (string.IsNullOrWhiteSpace(body?.Subject))
                return Results.Json(new { error = "invalid", reason = "Publish requires a subject." }, statusCode: StatusCodes.Status400BadRequest);
            try
            {
                var receipts = await broker.PublishAsync(body.Subject, Encoding.UTF8.GetBytes(body.Payload ?? ""), string.IsNullOrEmpty(body.Key) ? null : body.Key).ConfigureAwait(false);
                return Results.Json(new
                {
                    routed = receipts.Count,
                    receipts = receipts.Select(receipt => new { stream = receipt.Stream, partition = receipt.Partition, offset = receipt.Offset })
                });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        MapTopology(app, topology);

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        var ports = BoundPorts(app);
        site.Port = ports.Http;
        site.HttpsPort = ports.Https;
        return app;
    }

    private static void MapTopology(WebApplication app, NuvexaTopology topology)
    {
        app.MapGet("/api/exchanges", (string? vhost) => Results.Json(topology.ListExchanges(vhost).Select(item => new
        {
            vhost = item.Vhost,
            name = item.Name,
            display = item.Name.Length == 0 ? "(AMQP default)" : item.Name,
            type = item.Type,
            durable = item.Durable,
            builtin = item.Builtin
        })));
        app.MapPut("/api/exchanges/{name}", (string name, ExchangeRequest? body) =>
        {
            try
            {
                topology.DeclareExchange(body?.Vhost ?? "/", name, body?.Type ?? "direct", body?.Durable ?? true, body?.AutoDelete ?? false);
                return Results.Json(new { name });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/exchanges/{name}", (string name, string? vhost) =>
        {
            try
            {
                topology.DeleteExchange(vhost ?? "/", name);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/queues", (string? vhost) => Results.Json(topology.ListQueues(vhost)));
        app.MapPut("/api/queues/{name}", async (string name, QueueRequest? body) =>
        {
            try
            {
                await topology.DeclareQueueAsync(body?.Vhost ?? "/", name, new QueueOptions
                {
                    Durable = body?.Durable ?? true,
                    Exclusive = body?.Exclusive ?? false,
                    AutoDelete = body?.AutoDelete ?? false,
                    MessageTtlMs = body?.MessageTtlMs,
                    MaxLength = body?.MaxLength,
                    DeadLetterExchange = body?.DeadLetterExchange,
                    DeadLetterRoutingKey = body?.DeadLetterRoutingKey
                }, null).ConfigureAwait(false);
                return Results.Json(new { name });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/queues/{name}", async (string name, string? vhost) =>
        {
            try
            {
                await topology.DeleteQueueAsync(vhost ?? "/", name).ConfigureAwait(false);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapPost("/api/queues/{name}/purge", (string name, string? vhost) =>
        {
            try
            {
                topology.Purge(vhost ?? "/", name);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/bindings", (string? vhost) => Results.Json(topology.ListBindings(vhost)));
        app.MapDelete("/api/bindings", (string? vhost, string? exchange, string? queue, string? routingKey) =>
        {
            topology.Unbind(vhost ?? "/", exchange ?? "", queue ?? "", routingKey ?? "");
            return Results.NoContent();
        });
        app.MapPost("/api/bindings", (BindingRequest? body) =>
        {
            try
            {
                topology.Bind(body?.Vhost ?? "/", body?.Exchange ?? "", body?.Queue ?? "", body?.RoutingKey ?? "", body?.Arguments);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapPost("/api/exchanges/publish", async (ExchangePublishRequest? body) =>
        {
            try
            {
                var receipts = await topology.PublishAsync(body?.Vhost ?? "/", body?.Exchange ?? "", body?.RoutingKey ?? "", Encoding.UTF8.GetBytes(body?.Payload ?? ""), string.IsNullOrEmpty(body?.Key) ? null : body.Key, null).ConfigureAwait(false);
                return Results.Json(new { routed = receipts.Count, receipts = receipts.Select(item => new { queue = item.Stream, partition = item.Partition, offset = item.Offset }) });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/users", () => Results.Json(topology.ListUsers().Select(item => new { name = item.Name, tags = item.Tags })));
        app.MapPut("/api/users/{name}", (string name, UserRequest? body) =>
        {
            try
            {
                topology.UpsertUser(name, body?.Password, body?.Tags ?? ["management"]);
                return Results.Json(new { name });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/users/{name}", (string name) =>
        {
            try
            {
                topology.DeleteUser(name);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/permissions", () => Results.Json(topology.ListPermissions()));
        app.MapPut("/api/permissions", (PermissionRequest? body) =>
        {
            try
            {
                topology.SetPermission(body?.User ?? "", body?.Vhost ?? "/", body?.Configure ?? "", body?.Write ?? "", body?.Read ?? "");
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/permissions", (string? user, string? vhost) =>
        {
            try
            {
                topology.ClearPermission(user ?? "", vhost ?? "/");
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/vhosts", () => Results.Json(topology.ListVhosts()));
        app.MapPut("/api/vhosts/{name}", (string name) =>
        {
            try
            {
                topology.AddVhost(name);
                return Results.Json(new { name });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/vhosts/{name}", (string name) =>
        {
            try
            {
                topology.DeleteVhost(name);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapGet("/api/policies", (string? vhost) => Results.Json(topology.ListPolicies(vhost)));
        app.MapPut("/api/policies/{name}", (string name, PolicyRequest? body) =>
        {
            try
            {
                topology.UpsertPolicy(body?.Vhost ?? "/", name, body?.Pattern ?? ".*", body?.Priority ?? 0, body?.MessageTtlMs, body?.MaxLength, body?.DeadLetterExchange, body?.DeadLetterRoutingKey);
                return Results.Json(new { name });
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
        app.MapDelete("/api/policies/{name}", (string name, string? vhost) =>
        {
            try
            {
                topology.DeletePolicy(vhost ?? "/", name);
                return Results.NoContent();
            }
            catch (EngineException ex)
            {
                return Fail(ex);
            }
        });
    }

    public static (int Http, int Https) BoundPorts(WebApplication app)
    {
        var feature = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var http = 0;
        var https = 0;
        foreach (var address in feature?.Addresses ?? [])
        {
            var uri = new Uri(address);
            if (string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase))
                https = uri.Port;
            else
                http = uri.Port;
        }

        if (http == 0 || https == 0)
            throw new InvalidOperationException("The management ports did not bind.");
        return (http, https);
    }

    private static IPAddress EndpointAddress(string value) =>
        value is "0.0.0.0" or "*" or "+" ? IPAddress.Any :
        value.Equals("localhost", StringComparison.OrdinalIgnoreCase) ? IPAddress.Loopback :
        IPAddress.Parse(value);

    private static bool IsPublic(HttpContext context)
    {
        var path = context.Request.Path.Value ?? "";
        if (HttpMethods.IsGet(context.Request.Method) && path is "/" or "" or "/icon.png")
            return true;
        return HttpMethods.IsPost(context.Request.Method) && path.Equals("/api/login", StringComparison.OrdinalIgnoreCase);
    }

    private static IResult Login(HttpContext context, NuvexaTopology topology, SessionStore sessions, LoginRequest? body)
    {
        if (!topology.TryLogin(body?.Username ?? "", body?.Password ?? "", context.Connection.RemoteIpAddress, management: true, out var reason))
            return Results.Json(new { error = "not_authorized", reason }, statusCode: StatusCodes.Status401Unauthorized);

        var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        sessions.Add(token, body!.Username!);
        context.Response.Cookies.Append(CookieName, token, new CookieOptions
        {
            HttpOnly = true,
            SameSite = SameSiteMode.Strict,
            Path = "/",
            MaxAge = TimeSpan.FromHours(8)
        });
        return Results.Json(new { username = body.Username, localhostOnly = string.Equals(body.Username, "guest", StringComparison.Ordinal) });
    }

    private static bool TryAuthenticate(HttpContext context, NuvexaTopology topology, SessionStore sessions, out string reason)
    {
        if (context.Request.Cookies.TryGetValue(CookieName, out var token) && sessions.TryGet(token, out var user))
        {
            if (string.Equals(user, "guest", StringComparison.Ordinal) && !IsLoopback(context.Connection.RemoteIpAddress))
            {
                reason = "User 'guest' can only log in via localhost";
                return false;
            }

            context.Items["user"] = user;
            reason = "";
            return true;
        }

        var header = context.Request.Headers.Authorization.ToString();
        if (header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(header["Basic ".Length..].Trim()));
                var split = decoded.IndexOf(':');
                if (split <= 0)
                {
                    reason = "Login failed";
                    return false;
                }

                if (!topology.TryLogin(decoded[..split], decoded[(split + 1)..], context.Connection.RemoteIpAddress, management: true, out reason))
                    return false;

                context.Items["user"] = decoded[..split];
                return true;
            }
            catch (FormatException)
            {
                reason = "Login failed";
                return false;
            }
        }

        reason = "Login failed";
        return false;
    }

    private static bool IsLoopback(IPAddress? address)
    {
        if (address is null)
            return false;
        if (address.IsIPv4MappedToIPv6)
            address = address.MapToIPv4();
        return IPAddress.IsLoopback(address);
    }

    private static string CurrentUser(HttpContext context, NuvexaMqOptions options, SessionStore sessions)
    {
        if (context.Items.TryGetValue("user", out var value) && value is string user)
            return user;
        return options.ManagementUser;
    }

    private static object Overview(NuvexaMqOptions options, NuvexaBroker broker, NuvexaTopology topology, ConnectionTable connections, int listenPort, int healthPort, int managementPort, int managementHttpsPort, bool tls)
    {
        var snapshot = broker.Snapshot();
        var streams = broker.ListStreams();
        return new
        {
            version = StartupLog.ProductVersion,
            node = Dns.GetHostName(),
            dataDir = options.DataDir,
            tokenRequired = !string.IsNullOrEmpty(options.Token),
            tls,
            messagesIn = snapshot.MessagesIn,
            messagesOut = snapshot.MessagesOut,
            lastFsyncSeconds = snapshot.LastFsyncSeconds,
            connections = connections.Snapshot().Count,
            exchanges = topology.ListExchanges().Count,
            queues = topology.ListQueues().Count,
            streams = streams.Count(item => !item.Queue),
            consumers = snapshot.Consumers.Select(item => item.Consumer).Distinct(StringComparer.Ordinal).Count(),
            listeners = new object[]
            {
                new { protocol = "nuvexamq", address = options.ListenAddress, port = listenPort },
                new { protocol = "health", address = options.HealthAddress, port = healthPort },
                new { protocol = "http", address = options.ManagementAddress, port = managementPort },
                new { protocol = "https", address = options.ManagementAddress, port = managementHttpsPort }
            },
            partitions = snapshot.Partitions
        };
    }

    private static IReadOnlyList<StreamView> Streams(NuvexaBroker broker)
    {
        var snapshot = broker.Snapshot();
        var consumers = broker.ListConsumers();
        return broker.ListStreams().Where(stream => !stream.Queue).Select(stream =>
        {
            var parts = snapshot.Partitions.Where(item => item.Stream == stream.Name).ToArray();
            var messages = broker.VisibleMessageCount(stream.Name);

            return new StreamView
            {
                Name = stream.Name,
                Filters = stream.Filters,
                PartitionCount = stream.PartitionCount,
                MaxAgeMs = stream.MaxAgeMs,
                MaxBytes = stream.MaxBytes,
                MaxMessageBytes = stream.MaxMessageBytes,
                Messages = messages,
                Consumers = consumers.Where(item => item.Stream == stream.Name).Select(item => item.Name).Distinct(StringComparer.Ordinal).Count(),
                Partitions = parts
            };
        }).ToArray();
    }

    private static object ReadMessages(NuvexaBroker broker, string name, int partition, int count, long? offset)
    {
        var stream = broker.ListStreams().FirstOrDefault(item => item.Name == name)
            ?? throw new EngineException(NuvexaMqError.NotFound, $"Stream '{name}' was not found.");
        if (partition < 0 || partition >= stream.PartitionCount)
            throw new EngineException(NuvexaMqError.Invalid, "Partition is out of range.");

        var part = broker.Snapshot().Partitions.First(item => item.Stream == name && item.Partition == partition);
        var floor = broker.FloorOffset(name);
        var messages = new List<object>();
        if (part.CommittedOffset >= 0)
        {
            var start = offset ?? Math.Max(part.FirstOffset, part.CommittedOffset - count + 1);
            if (start < part.FirstOffset)
                start = part.FirstOffset;
            if (start < floor)
                start = floor;
            for (var cursor = start; cursor <= part.CommittedOffset && messages.Count < count; cursor++)
            {
                if (broker.MessageIsDeleted(name, partition, cursor))
                    continue;
                if (!broker.TryRead(name, partition, cursor, out var record) || record is null)
                    continue;
                var payloadText = Text(record.Payload);
                messages.Add(new
                {
                    offset = record.Offset,
                    timestampUnixMs = record.TimestampUnixMs,
                    subject = record.Subject,
                    key = BytesText(record.Key),
                    payloadText,
                    payloadBase64 = payloadText is null ? Convert.ToBase64String(record.Payload) : null
                });
            }
        }

        return new
        {
            stream = name,
            partition,
            firstOffset = part.FirstOffset,
            committedOffset = part.CommittedOffset,
            messages
        };
    }

    private static string? Text(byte[] value)
    {
        if (value.Length == 0)
            return "";
        try
        {
            return Utf8.GetString(value);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private static string? BytesText(byte[] value) => value.Length == 0 ? null : Text(value) ?? Convert.ToBase64String(value);

    private static IResult Fail(EngineException ex)
    {
        var status = ex.Error switch
        {
            NuvexaMqError.NotFound => StatusCodes.Status404NotFound,
            NuvexaMqError.Conflict => StatusCodes.Status409Conflict,
            NuvexaMqError.TooLarge => StatusCodes.Status413PayloadTooLarge,
            NuvexaMqError.Unauthorized => StatusCodes.Status401Unauthorized,
            _ => StatusCodes.Status400BadRequest
        };
        return Results.Json(new { error = ex.Error.ToString(), reason = ex.Message }, statusCode: status);
    }

    private sealed class StreamView
    {
        public string Name { get; init; } = "";

        public IReadOnlyList<string> Filters { get; init; } = [];

        public int PartitionCount { get; init; }

        public long? MaxAgeMs { get; init; }

        public long? MaxBytes { get; init; }

        public int MaxMessageBytes { get; init; }

        public long Messages { get; init; }

        public int Consumers { get; init; }

        public IReadOnlyList<PartitionSnapshot> Partitions { get; init; } = [];
    }

    private sealed class LoginRequest
    {
        public string? Username { get; set; }

        public string? Password { get; set; }
    }

    private sealed class StreamRequest
    {
        public string[]? Filters { get; set; }

        public int? PartitionCount { get; set; }

        public long? MaxAgeMs { get; set; }

        public long? MaxBytes { get; set; }

        public int? MaxMessageBytes { get; set; }
    }

    private sealed class ExchangeRequest
    {
        public string? Vhost { get; set; }

        public string? Type { get; set; }

        public bool? Durable { get; set; }

        public bool? AutoDelete { get; set; }
    }

    private sealed class QueueRequest
    {
        public string? Vhost { get; set; }

        public bool? Durable { get; set; }

        public bool? Exclusive { get; set; }

        public bool? AutoDelete { get; set; }

        public long? MessageTtlMs { get; set; }

        public int? MaxLength { get; set; }

        public string? DeadLetterExchange { get; set; }

        public string? DeadLetterRoutingKey { get; set; }
    }

    private sealed class BindingRequest
    {
        public string? Vhost { get; set; }

        public string? Exchange { get; set; }

        public string? Queue { get; set; }

        public string? RoutingKey { get; set; }

        public Dictionary<string, string>? Arguments { get; set; }
    }

    private sealed class ExchangePublishRequest
    {
        public string? Vhost { get; set; }

        public string? Exchange { get; set; }

        public string? RoutingKey { get; set; }

        public string? Key { get; set; }

        public string? Payload { get; set; }
    }

    private sealed class UserRequest
    {
        public string? Password { get; set; }

        public List<string>? Tags { get; set; }
    }

    private sealed class PermissionRequest
    {
        public string? User { get; set; }

        public string? Vhost { get; set; }

        public string? Configure { get; set; }

        public string? Write { get; set; }

        public string? Read { get; set; }
    }

    private sealed class PolicyRequest
    {
        public string? Vhost { get; set; }

        public string? Pattern { get; set; }

        public int? Priority { get; set; }

        public long? MessageTtlMs { get; set; }

        public int? MaxLength { get; set; }

        public string? DeadLetterExchange { get; set; }

        public string? DeadLetterRoutingKey { get; set; }
    }

    private sealed class PublishRequest
    {
        public string? Subject { get; set; }

        public string? Payload { get; set; }

        public string? Key { get; set; }
    }

    private sealed class SiteInfo
    {
        public int Port { get; set; }

        public int HttpsPort { get; set; }
    }

    private sealed class SessionStore
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, (string User, DateTimeOffset Expires)> _sessions = new(StringComparer.Ordinal);

        public void Add(string token, string user)
        {
            lock (_gate)
                _sessions[token] = (user, DateTimeOffset.UtcNow.AddHours(8));
        }

        public void Remove(string token)
        {
            lock (_gate)
                _sessions.Remove(token);
        }

        public bool TryGet(string token, out string user)
        {
            lock (_gate)
            {
                var expired = _sessions.Where(item => item.Value.Expires <= DateTimeOffset.UtcNow).Select(item => item.Key).ToArray();
                foreach (var key in expired)
                    _sessions.Remove(key);
                if (_sessions.TryGetValue(token, out var session))
                {
                    user = session.User;
                    return true;
                }
            }

            user = "";
            return false;
        }
    }
}

internal static class ConsolePage
{
    public static string Html { get; } = LoadText("console.html");

    public static byte[] Icon { get; } = LoadBytes("icon.png");

    private static string LoadText(string suffix)
    {
        using var reader = new StreamReader(Open(suffix));
        return reader.ReadToEnd();
    }

    private static byte[] LoadBytes(string suffix)
    {
        using var stream = Open(suffix);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);
        return memory.ToArray();
    }

    private static Stream Open(string suffix)
    {
        var assembly = typeof(ConsolePage).Assembly;
        var name = assembly.GetManifestResourceNames().Single(item => item.EndsWith(suffix, StringComparison.Ordinal));
        return assembly.GetManifestResourceStream(name) ?? throw new InvalidOperationException("The management page is missing from the server assembly.");
    }
}
