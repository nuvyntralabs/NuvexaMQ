using System.Net.Http.Json;
using Xunit;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Nuventra.NuvexaMQ;
using Nuventra.NuvexaMQ.Protocol;
using Nuventra.NuvexaMQ.Server;

namespace Nuventra.NuvexaMQ.Tests;

public sealed class ServerTests
{
    [Fact]
    public async Task Client_and_server_publish_and_ack()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        await client.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        var receipts = await client.PublishAsync("orders.created", "hello"u8.ToArray(), key: "order-18");
        await client.EnsureConsumerAsync(new ConsumeSpec("orders", "billing"));
        var batch = await client.FetchAsync(new ConsumeSpec("orders", "billing"), 10, TimeSpan.Zero);
        Assert.Equal(receipts[0].Offset, batch[0].Offset);
        Assert.Equal("hello", batch[0].PayloadText);
        await batch[0].AckAsync();

        using var http = new HttpClient();
        var health = await http.GetFromJsonAsync<HealthDto>($"http://127.0.0.1:{server.HealthPort}/health");
        Assert.Equal("ok", health!.Status);
        var metrics = await http.GetStringAsync($"http://127.0.0.1:{server.HealthPort}/metrics");
        Assert.Contains("nuvexamq_messages_in_total", metrics, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Two_connections_on_one_durable_name_do_not_both_receive_the_same_message()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        await using var setup = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        await setup.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await setup.PublishAsync("orders.created", "alpha");
        await setup.PublishAsync("orders.created", "beta");

        await using var left = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        await using var right = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        var spec = new ConsumeSpec("orders", "billing");
        var first = await left.FetchAsync(spec, 1, TimeSpan.Zero);
        var second = await right.FetchAsync(spec, 1, TimeSpan.Zero);
        var payloads = new[] { first[0].PayloadText, second[0].PayloadText }.OrderBy(value => value).ToArray();
        Assert.Equal(["alpha", "beta"], payloads);
        Assert.NotEqual(first[0].Offset, second[0].Offset);
    }

    [Fact]
    public async Task Two_durable_names_both_receive_the_message()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        await client.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await client.PublishAsync("orders.created", "shared");
        var billing = await client.FetchAsync(new ConsumeSpec("orders", "billing"), 1, TimeSpan.Zero);
        var warehouse = await client.FetchAsync(new ConsumeSpec("orders", "warehouse"), 1, TimeSpan.Zero);
        Assert.Equal("shared", billing[0].PayloadText);
        Assert.Equal("shared", warehouse[0].PayloadText);
        await billing[0].AckAsync();
        await warehouse[0].AckAsync();
    }

    [Fact]
    public async Task Rejects_a_bad_token()
    {
        using var dir = new TempDir();
        var options = Options(dir.Path);
        options.Token = "correct-horse";
        await using var server = await NuvexaMqServer.StartAsync(options);
        var error = await Assert.ThrowsAsync<NuvexaMqException>(() =>
            NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort, new NuvexaClientOptions { Token = "nope" }));
        Assert.Equal(NuvexaMqError.Unauthorized, error.Error);
    }

    [Fact]
    public async Task Tls_connection_publishes()
    {
        using var dir = new TempDir();
        var certPath = Path.Combine(dir.Path, "cert.pem");
        var keyPath = Path.Combine(dir.Path, "key.pem");
        WriteCertificate(certPath, keyPath);
        var options = Options(dir.Path);
        options.CertificatePath = certPath;
        options.KeyPath = keyPath;
        await using var server = await NuvexaMqServer.StartAsync(options);
        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort, new NuvexaClientOptions
        {
            UseTls = true,
            CertificateValidation = (_, _, _, _) => true
        });
        await client.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        var receipts = await client.PublishAsync("orders.created", "secure");
        Assert.Equal(0, receipts[0].Offset);
    }

    [Fact]
    public async Task Management_console_requires_login_then_creates_and_reads_a_stream()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        using var anonymous = new HttpClient();
        var denied = await anonymous.GetAsync($"http://127.0.0.1:{server.ManagementPort}/api/overview");
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, denied.StatusCode);
        var page = await anonymous.GetStringAsync($"http://127.0.0.1:{server.ManagementPort}/");
        Assert.Contains("Login", page, StringComparison.Ordinal);

        using var http = new HttpClient();
        var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest"));
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", raw);
        using var wrong = new HttpClient();
        var bad = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:nope"));
        wrong.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", bad);
        var rejected = await wrong.GetAsync($"http://127.0.0.1:{server.ManagementPort}/api/overview");
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, rejected.StatusCode);

        var created = await http.PutAsJsonAsync($"http://127.0.0.1:{server.ManagementPort}/api/streams/orders", new { filters = new[] { "orders.>" }, partitionCount = 1 });
        created.EnsureSuccessStatusCode();
        var published = await http.PostAsJsonAsync($"http://127.0.0.1:{server.ManagementPort}/api/publish", new { subject = "orders.created", payload = "hello", key = "order-18" });
        published.EnsureSuccessStatusCode();
        var messages = await http.GetFromJsonAsync<MessagePage>($"http://127.0.0.1:{server.ManagementPort}/api/streams/orders/messages?count=10");
        Assert.Equal("hello", messages!.Messages[0].PayloadText);

        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort, new NuvexaClientOptions { ClientName = "billing-api" });
        var connections = await http.GetFromJsonAsync<ConnectionRow[]>($"http://127.0.0.1:{server.ManagementPort}/api/connections");
        Assert.Contains(connections!, row => row.Client == "billing-api" && row.State == "running");
    }

    [Fact]
    public async Task Exchanges_route_to_queues()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        await client.DeclareExchangeAsync("orders", "topic");
        await client.DeclareQueueAsync("eu");
        await client.DeclareQueueAsync("all");
        await client.DeclareQueueAsync("billing");
        await client.BindQueueAsync("orders", "eu", "order.*");
        await client.BindQueueAsync("orders", "all", "order.#");
        await client.PublishToExchangeAsync("orders", "order.created", "one");
        await client.PublishToExchangeAsync("orders", "order.created.eu", "two");
        await client.PublishToExchangeAsync("", "billing", "direct");

        var eu = await client.FetchAsync(new ConsumeSpec("$queue.eu", "eu"), 10, TimeSpan.Zero);
        var all = await client.FetchAsync(new ConsumeSpec("$queue.all", "all"), 10, TimeSpan.Zero);
        var direct = await client.FetchAsync(new ConsumeSpec("$queue.billing", "billing"), 10, TimeSpan.Zero);
        Assert.Equal(["one"], eu.Select(message => message.PayloadText).ToArray());
        Assert.Equal(["one", "two"], all.Select(message => message.PayloadText).ToArray());
        Assert.Equal("direct", direct[0].PayloadText);
    }

    [Fact]
    public async Task Permissions_limit_what_a_user_can_declare()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        using var http = new HttpClient();
        var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest"));
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", raw);
        (await http.PutAsJsonAsync($"http://127.0.0.1:{server.ManagementPort}/api/users/billing", new { password = "secret", tags = new[] { "management" } })).EnsureSuccessStatusCode();
        (await http.PutAsJsonAsync($"http://127.0.0.1:{server.ManagementPort}/api/permissions", new { user = "billing", vhost = "/", configure = "^orders$", write = "^orders$", read = "^orders$" })).EnsureSuccessStatusCode();

        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort, new NuvexaClientOptions { User = "billing", Password = "secret" });
        await client.DeclareExchangeAsync("orders", "direct");
        var error = await Assert.ThrowsAsync<NuvexaMqException>(() => client.DeclareExchangeAsync("payments", "direct"));
        Assert.Equal(NuvexaMqError.Unauthorized, error.Error);
    }

    [Fact]
    public async Task Management_deletes_exchanges_queues_streams_bindings_and_messages()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        using var http = new HttpClient();
        var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest"));
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", raw);
        var root = $"http://127.0.0.1:{server.ManagementPort}";

        (await http.PutAsJsonAsync(root + "/api/exchanges/orders", new { vhost = "/", type = "topic", durable = true })).EnsureSuccessStatusCode();
        (await http.PutAsJsonAsync(root + "/api/queues/billing", new { vhost = "/", durable = true })).EnsureSuccessStatusCode();
        (await http.PostAsJsonAsync(root + "/api/bindings", new { vhost = "/", exchange = "orders", queue = "billing", routingKey = "orders.#" })).EnsureSuccessStatusCode();
        var published = await http.PostAsJsonAsync(root + "/api/exchanges/publish", new { vhost = "/", exchange = "orders", routingKey = "orders.created", payload = "one" });
        published.EnsureSuccessStatusCode();
        var receipt = await published.Content.ReadFromJsonAsync<PublishBody>();
        Assert.Equal(1, receipt!.Routed);

        var kept = await http.DeleteAsync(root + "/api/exchanges/amq.topic?vhost=/");
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, kept.StatusCode);
        (await http.DeleteAsync(root + "/api/bindings?vhost=/&exchange=orders&queue=billing&routingKey=orders.%23")).EnsureSuccessStatusCode();
        (await http.PostAsync(root + "/api/queues/billing/purge?vhost=/", null)).EnsureSuccessStatusCode();
        var purged = await http.GetFromJsonAsync<MessagePage>(root + "/api/streams/$queue.billing/messages?count=10");
        Assert.Empty(purged!.Messages);
        (await http.DeleteAsync(root + "/api/queues/billing?vhost=/")).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/exchanges/orders?vhost=/")).EnsureSuccessStatusCode();

        (await http.PutAsJsonAsync(root + "/api/streams/orders", new { filters = new[] { "orders.>" }, partitionCount = 1 })).EnsureSuccessStatusCode();
        (await http.PostAsJsonAsync(root + "/api/publish", new { subject = "orders.created", payload = "alpha" })).EnsureSuccessStatusCode();
        (await http.PostAsJsonAsync(root + "/api/publish", new { subject = "orders.created", payload = "beta" })).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/streams/orders/messages?partition=0&offset=0")).EnsureSuccessStatusCode();
        var page = await http.GetFromJsonAsync<MessagePage>(root + "/api/streams/orders/messages?count=10");
        Assert.Equal(["beta"], page!.Messages.Select(item => item.PayloadText!).ToArray());

        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);
        var batch = await client.FetchAsync(new ConsumeSpec("orders", "billing"), 10, TimeSpan.Zero);
        Assert.Equal(["beta"], batch.Select(message => message.PayloadText).ToArray());
        (await http.DeleteAsync(root + "/api/streams/orders")).EnsureSuccessStatusCode();
        var missing = await http.GetAsync(root + "/api/streams/orders");
        Assert.Equal(System.Net.HttpStatusCode.NotFound, missing.StatusCode);

        (await http.PutAsJsonAsync(root + "/api/users/billing", new { password = "secret", tags = new[] { "management" } })).EnsureSuccessStatusCode();
        (await http.PutAsJsonAsync(root + "/api/permissions", new { user = "billing", vhost = "/", configure = ".*", write = ".*", read = ".*" })).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/permissions?user=billing&vhost=/")).EnsureSuccessStatusCode();
        (await http.PutAsJsonAsync(root + "/api/vhosts/production", new { })).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/vhosts/production")).EnsureSuccessStatusCode();
        (await http.PutAsJsonAsync(root + "/api/policies/limits", new { vhost = "/", pattern = "^orders" })).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/policies/limits?vhost=/")).EnsureSuccessStatusCode();
        (await http.DeleteAsync(root + "/api/users/billing")).EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task Client_declares_publishes_nacks_purges_and_deletes()
    {
        using var dir = new TempDir();
        await using var server = await NuvexaMqServer.StartAsync(Options(dir.Path));
        await using var client = await NuvexaClient.ConnectAsync("127.0.0.1", server.ListenPort);

        await client.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]) { PartitionCount = 1 });
        var published = await client.PublishAsync("orders.created", """{"id":1}""", key: "order-1", headers: new Dictionary<string, byte[]>
        {
            ["content-type"] = "application/json"u8.ToArray()
        });
        Assert.Equal(0, published[0].Offset);

        var stream = new ConsumeSpec("orders", "billing");
        var first = (await client.FetchAsync(stream, 1, TimeSpan.Zero))[0];
        Assert.Equal("application/json", System.Text.Encoding.UTF8.GetString(first.Headers["content-type"]));
        await first.AckAsync();

        await client.PublishAsync("orders.created", "retry"u8.ToArray());
        var nacked = (await client.FetchAsync(stream, 1, TimeSpan.Zero))[0];
        await nacked.NackAsync();
        var redelivered = (await client.FetchAsync(stream, 1, TimeSpan.Zero))[0];
        Assert.Equal(2, redelivered.DeliveryCount);
        Assert.Equal("retry", redelivered.PayloadText);
        await redelivered.AckAsync();

        await using (var replay = await client.ConsumeAsync(stream))
            await replay.ResetAsync();
        var replayed = await client.FetchAsync(stream, 10, TimeSpan.Zero);
        Assert.Equal(["{\"id\":1}", "retry"], replayed.Select(message => message.PayloadText).ToArray());
        foreach (var message in replayed)
            await message.AckAsync();

        await using var live = await client.ConsumeAsync(new ConsumeSpec("orders", "live") { Start = ConsumeStart.Tail });
        await client.PublishAsync("orders.created", "live");
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var seen = "";
        await foreach (var message in live.ReadAllAsync(stop.Token))
        {
            seen = message.PayloadText;
            await message.AckAsync();
            break;
        }

        Assert.Equal("live", seen);
        await using (var tail = await client.ConsumeAsync(new ConsumeSpec("orders", "tail") { Ephemeral = true, Start = ConsumeStart.Tail }))
        {
        }

        await client.DeclareExchangeAsync("orders", "topic");
        await client.DeclareQueueAsync("billing", messageTtlMs: 60_000, maxLength: 1000, deadLetterExchange: "amq.topic", deadLetterRoutingKey: "orders.dead");
        await client.BindQueueAsync("orders", "billing", "orders.#", new Dictionary<string, string> { ["x-match"] = "all" });
        var routed = await client.PublishToExchangeAsync("orders", "orders.created", "one");
        Assert.Equal("billing", routed[0].Stream);

        await client.DeclareExchangeAsync("orders.headers", "headers");
        await client.DeclareQueueAsync("audit");
        await client.BindQueueAsync("orders.headers", "audit", "", new Dictionary<string, string> { ["format"] = "json", ["x-match"] = "all" });
        var matched = await client.PublishToExchangeAsync("orders.headers", "", "headered", headers: new Dictionary<string, byte[]>
        {
            ["format"] = "json"u8.ToArray()
        });
        Assert.Equal("audit", matched[0].Stream);
        var missed = await Assert.ThrowsAsync<NuvexaMqException>(() => client.PublishToExchangeAsync("orders.headers", "", "nope"));
        Assert.Equal(NuvexaMqError.NotFound, missed.Error);

        await client.DeclareQueueAsync("fan");
        await client.DeclareQueueAsync("out");
        await client.BindQueueAsync("amq.fanout", "fan", "");
        await client.BindQueueAsync("amq.fanout", "out", "");
        var fanout = await client.PublishToExchangeAsync("amq.fanout", "", "both");
        Assert.Equal(2, fanout.Count);

        var queue = new ConsumeSpec("$queue.billing", "billing");
        var queued = await client.FetchAsync(queue, 10, TimeSpan.Zero);
        Assert.Equal(["one"], queued.Select(message => message.PayloadText).ToArray());
        await queued[0].AckAsync();

        await client.PublishToExchangeAsync("orders", "orders.created", "purge-me");
        await client.PurgeQueueAsync("billing");
        Assert.Empty(await client.FetchAsync(queue, 10, TimeSpan.Zero));

        var builtin = await Assert.ThrowsAsync<NuvexaMqException>(() => client.DeleteExchangeAsync("amq.topic"));
        Assert.Equal(NuvexaMqError.Invalid, builtin.Error);
        await client.DeleteQueueAsync("billing");
        await client.DeleteQueueAsync("audit");
        await client.DeleteQueueAsync("fan");
        await client.DeleteQueueAsync("out");
        await client.DeleteExchangeAsync("orders");
        await client.DeleteExchangeAsync("orders.headers");
        var gone = await Assert.ThrowsAsync<NuvexaMqException>(() => client.PublishToExchangeAsync("orders", "orders.created", "after"));
        Assert.Equal(NuvexaMqError.NotFound, gone.Error);
    }

    [Fact]
    public void Startup_urls_use_a_concrete_address()
    {
        var plain = StartupLog.ListenerUrls("0.0.0.0", 5761, "0.0.0.0", 5762, "0.0.0.0", 5763, 5764, tls: false);
        Assert.Equal(
            ["tcp://127.0.0.1:5761", "http://127.0.0.1:5762/", "http://127.0.0.1:5763/", "https://127.0.0.1:5764/"],
            plain);
        var secure = StartupLog.ListenerUrls("127.0.0.1", 5761, "127.0.0.1", 5762, "127.0.0.1", 5763, 5764, tls: true);
        Assert.Equal("tls://127.0.0.1:5761", secure[0]);
        Assert.Equal("https://127.0.0.1:5764/", secure[3]);
    }

    private static NuvexaMqOptions Options(string path) => new()
    {
        ListenAddress = "127.0.0.1",
        ListenPort = 0,
        HealthAddress = "127.0.0.1",
        HealthPort = 0,
        ManagementAddress = "127.0.0.1",
        ManagementPort = 0,
        ManagementHttpsPort = 0,
        DataDir = path,
        FlushIntervalMs = 0,
        Token = ""
    };

    private static void WriteCertificate(string certPath, string keyPath)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=nuvexamq-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(System.Net.IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        using var cert = request.Create(
            request.SubjectName,
            X509SignatureGenerator.CreateForRSA(rsa, RSASignaturePadding.Pkcs1),
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddDays(2),
            [1, 2, 3, 4, 5, 6, 7, 8]);
        File.WriteAllText(certPath, cert.ExportCertificatePem());
        File.WriteAllText(keyPath, rsa.ExportPkcs8PrivateKeyPem());
    }

    private sealed class HealthDto
    {
        public string Status { get; set; } = "";
    }

    private sealed class PublishBody
    {
        public int Routed { get; set; }
    }

    private sealed class MessagePage
    {
        public MessageItem[] Messages { get; set; } = [];
    }

    private sealed class MessageItem
    {
        public string? PayloadText { get; set; }
    }

    private sealed class ConnectionRow
    {
        public string Client { get; set; } = "";

        public string State { get; set; } = "";
    }
}
