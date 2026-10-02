namespace Nuventra.NuvexaMQ.Server;

public static class StartupLog
{
    public static readonly string ProductVersion = Format(typeof(StartupLog).Assembly.GetName().Version);

    private static string Format(Version? version) =>
        version is null ? "0.0.0" : $"{version.Major}.{version.Minor}.{Math.Max(version.Build, 0)}";

    public static string Advertise(string address)
    {
        if (string.IsNullOrWhiteSpace(address) || address is "0.0.0.0" or "*" or "+" or "::" or "[::]")
            return "127.0.0.1";
        if (address.Equals("localhost", StringComparison.OrdinalIgnoreCase))
            return "127.0.0.1";
        return address.Trim().Trim('[', ']');
    }

    public static IReadOnlyList<string> ListenerUrls(string listenAddress, int listenPort, string healthAddress, int healthPort, string managementAddress, int managementPort, int managementHttpsPort, bool tls)
    {
        var broker = Host(Advertise(listenAddress));
        var health = Host(Advertise(healthAddress));
        var management = Host(Advertise(managementAddress));
        return
        [
            tls ? $"tls://{broker}:{listenPort}" : $"tcp://{broker}:{listenPort}",
            $"http://{health}:{healthPort}/",
            $"http://{management}:{managementPort}/",
            $"https://{management}:{managementHttpsPort}/"
        ];
    }

    public static void WriteBanner(NuvexaMqOptions options, int listenPort, int healthPort, int managementPort, int managementHttpsPort, bool tls, bool generatedHttps)
    {
        var stamp = UnixNow();
        Console.WriteLine($"{stamp}: NuvexaMQ version {ProductVersion} starting");
        Console.WriteLine($"{stamp}: Opening {(tls ? "tls" : "ipv4")} listen socket on port {listenPort}.");
        Console.WriteLine($"{stamp}: Opening http listen socket on port {healthPort}.");
        Console.WriteLine($"{stamp}: Opening http listen socket on port {managementPort}.");
        Console.WriteLine($"{stamp}: Opening https listen socket on port {managementHttpsPort}.");
        Console.WriteLine($"{stamp}: NuvexaMQ version {ProductVersion} running");
        foreach (var url in ListenerUrls(options.ListenAddress, listenPort, options.HealthAddress, healthPort, options.ManagementAddress, managementPort, managementHttpsPort, tls))
            Console.WriteLine(url);
        if (generatedHttps)
            Console.WriteLine($"{stamp}: Warning: HTTPS is using a generated certificate. Browsers warn until CertificatePath is set.");
        if (string.Equals(options.ManagementUser, "guest", StringComparison.Ordinal))
            Console.WriteLine($"{stamp}: Warning: management user guest is accepted only from localhost.");
    }

    public static void Verbose(bool enabled, string message)
    {
        if (!enabled)
            return;
        Console.WriteLine($"{UnixNow()}: {message}");
    }

    private static string Host(string address) => address.Contains(':') ? $"[{address}]" : address;

    private static long UnixNow() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();
}
