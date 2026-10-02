using System.Text.Json;
using Nuventra.NuvexaMQ.Server;

var parsed = CliArgs.Parse(args);
if (parsed.Command == "path")
    return Cli.InstallPath(parsed);
if (parsed.Command is "stream" or "pub" or "consume")
    return await Cli.RunAsync(parsed);
if (BrokerService.ShouldRunAsService())
    return await BrokerService.RunAsync(args);

var options = Cli.LoadOptions(parsed);
await using var server = await NuvexaMqServer.StartAsync(options);
StartupLog.WriteBanner(options, server.ListenPort, server.HealthPort, server.ManagementPort, server.ManagementHttpsPort, server.Tls, server.GeneratedHttpsCertificate);
var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stop.Cancel();
};
try
{
    await Task.Delay(Timeout.Infinite, stop.Token);
}
catch (OperationCanceledException)
{
}

return 0;

internal sealed class CliArgs
{
    public string Command { get; init; } = "serve";

    public string? Subcommand { get; init; }

    public Dictionary<string, List<string>> Parsed { get; init; } = new(StringComparer.Ordinal);

    public string? Get(string name) => Parsed.TryGetValue(name, out var values) ? values.LastOrDefault() : null;

    public IReadOnlyList<string> GetAll(string name) => Parsed.TryGetValue(name, out var values) ? values : [];

    public static CliArgs Parse(string[] args)
    {
        string? command = null;
        string? sub = null;
        var options = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg is "-v" or "--verbose")
            {
                Add(options, "verbose", "true");
                continue;
            }

            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                var name = arg[2..];
                if (name == "verbose")
                {
                    Add(options, "verbose", "true");
                    continue;
                }

                if (i + 1 >= args.Length)
                    throw new InvalidOperationException($"Missing value for --{name}.");
                if (!options.TryGetValue(name, out var list))
                {
                    list = [];
                    options[name] = list;
                }

                list.Add(args[++i]);
                continue;
            }

            if (command is null)
                command = arg;
            else if (sub is null)
                sub = arg;
        }

        return new CliArgs
        {
            Command = command ?? "serve",
            Subcommand = sub,
            Parsed = options
        };

        static void Add(Dictionary<string, List<string>> options, string name, string value)
        {
            if (!options.TryGetValue(name, out var list))
            {
                list = [];
                options[name] = list;
            }

            list.Add(value);
        }
    }
}

internal static class Cli
{
    public static NuvexaMqOptions LoadOptions(CliArgs args)
    {
        var options = new NuvexaMqOptions();
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (File.Exists(path))
        {
            using var document = JsonDocument.Parse(File.ReadAllText(path));
            if (document.RootElement.TryGetProperty("NuvexaMQ", out var section))
            {
                options.ListenAddress = String(section, "ListenAddress") ?? options.ListenAddress;
                options.ListenPort = Int(section, "ListenPort") ?? options.ListenPort;
                options.HealthAddress = String(section, "HealthAddress") ?? options.HealthAddress;
                options.HealthPort = Int(section, "HealthPort") ?? options.HealthPort;
                options.ManagementAddress = String(section, "ManagementAddress") ?? options.ManagementAddress;
                options.ManagementPort = Int(section, "ManagementPort") ?? options.ManagementPort;
                options.ManagementHttpsPort = Int(section, "ManagementHttpsPort") ?? options.ManagementHttpsPort;
                if (String(section, "ManagementUser") is string managementUser)
                    options.ManagementUser = managementUser;
                if (String(section, "ManagementPassword") is string managementPassword)
                    options.ManagementPassword = managementPassword;
                options.DataDir = String(section, "DataDir") ?? options.DataDir;
                options.Token = String(section, "Token");
                options.CertificatePath = String(section, "CertificatePath");
                options.KeyPath = String(section, "KeyPath");
                options.FlushIntervalMs = Int(section, "FlushIntervalMs") ?? options.FlushIntervalMs;
                options.FlushMaxRecords = Int(section, "FlushMaxRecords") ?? options.FlushMaxRecords;
                options.MaxMessageBytes = Int(section, "MaxMessageBytes") ?? options.MaxMessageBytes;
                options.MaxConnections = Int(section, "MaxConnections") ?? options.MaxConnections;
                options.SegmentBytes = Int(section, "SegmentBytes") ?? options.SegmentBytes;
            }
        }

        if (args.Get("data") is string data)
            options.DataDir = data;
        if (args.Get("token") is string token)
            options.Token = token;
        if (args.Get("port") is string port)
            options.ListenPort = int.Parse(port);
        if (args.Get("health-port") is string health)
            options.HealthPort = int.Parse(health);
        if (args.Get("management-port") is string managementPortArg)
            options.ManagementPort = int.Parse(managementPortArg);
        if (args.Get("management-https-port") is string managementHttpsPortArg)
            options.ManagementHttpsPort = int.Parse(managementHttpsPortArg);
        if (args.Get("management-user") is string managementUserArg)
            options.ManagementUser = managementUserArg;
        if (args.Get("management-password") is string managementPasswordArg)
            options.ManagementPassword = managementPasswordArg;
        if (args.Get("flush-ms") is string flush)
            options.FlushIntervalMs = int.Parse(flush);
        if (args.Get("cert") is string cert)
            options.CertificatePath = cert;
        if (args.Get("cert-key") is string certKey)
            options.KeyPath = certKey;
        options.Verbose = args.Get("verbose") is "true";
        return options;
    }

    public static int InstallPath(CliArgs args)
    {
        if (args.Subcommand != "install")
        {
            Console.Error.WriteLine("Commands: path install");
            return 1;
        }

        if (CommandPath.IsDevelopmentOutput(AppContext.BaseDirectory))
        {
            Console.Error.WriteLine("PATH was not changed. This is a build output folder. Install the published NuvexaMQ first.");
            return 1;
        }

        var result = CommandPath.Install(AppContext.BaseDirectory);
        if (result.Message.Length > 0)
            Console.WriteLine(result.Message);
        return result.Ready ? 0 : 1;
    }

    public static async Task<int> RunAsync(CliArgs args)
    {
        var host = args.Get("host") ?? "localhost";
        var port = int.Parse(args.Get("port") ?? "5761");
        var token = args.Get("token");
        await using var client = await Nuventra.NuvexaMQ.NuvexaClient.ConnectAsync(host, port, new Nuventra.NuvexaMQ.NuvexaClientOptions { Token = token });
        switch (args.Command)
        {
            case "stream" when args.Subcommand == "add":
                var filters = args.GetAll("filter");
                if (filters.Count == 0 || args.Get("name") is not string name)
                    throw new InvalidOperationException("stream add requires --name and --filter.");
                await client.EnsureStreamAsync(new Nuventra.NuvexaMQ.StreamSpec(name, filters));
                Console.WriteLine($"stream {name}");
                return 0;
            case "pub":
                if (args.Get("subject") is not string subject)
                    throw new InvalidOperationException("pub requires --subject.");
                var receipts = await client.PublishAsync(subject, args.Get("body") ?? "", key: args.Get("key"));
                foreach (var receipt in receipts)
                    Console.WriteLine($"{receipt.Stream} p{receipt.Partition} {receipt.Offset}");
                return 0;
            case "consume":
                if (args.Get("stream") is not string stream || args.Get("durable") is not string durable)
                    throw new InvalidOperationException("consume requires --stream and --durable.");
                var count = int.Parse(args.Get("count") ?? "1");
                await client.EnsureConsumerAsync(new Nuventra.NuvexaMQ.ConsumeSpec(stream, durable));
                var got = 0;
                while (got < count)
                {
                    var batch = await client.FetchAsync(new Nuventra.NuvexaMQ.ConsumeSpec(stream, durable), count - got, TimeSpan.FromSeconds(2));
                    if (batch.Count == 0)
                        break;
                    foreach (var message in batch)
                    {
                        Console.WriteLine($"{message.Offset} {message.Subject} {message.PayloadText}");
                        await message.AckAsync();
                        got++;
                    }
                }

                return 0;
            default:
                Console.Error.WriteLine("Commands: serve | stream add | pub | consume | path install");
                Console.Error.WriteLine("serve accepts --verbose (-v) for connection and publish logs.");
                return 1;
        }
    }

    private static string? String(JsonElement section, string name) =>
        section.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static int? Int(JsonElement section, string name) =>
        section.TryGetProperty(name, out var value) && value.TryGetInt32(out var number) ? number : null;
}
