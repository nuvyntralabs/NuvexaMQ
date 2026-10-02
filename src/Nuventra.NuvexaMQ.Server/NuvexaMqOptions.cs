namespace Nuventra.NuvexaMQ.Server;

public sealed class NuvexaMqOptions
{
    public string ListenAddress { get; set; } = "0.0.0.0";

    public int ListenPort { get; set; } = 5761;

    public string HealthAddress { get; set; } = "0.0.0.0";

    public int HealthPort { get; set; } = 5762;

    public string ManagementAddress { get; set; } = "0.0.0.0";

    public int ManagementPort { get; set; } = 5763;

    public int ManagementHttpsPort { get; set; } = 5764;

    public string ManagementUser { get; set; } = "guest";

    public string ManagementPassword { get; set; } = "guest";

    public string DataDir { get; set; } = "./data";

    public string? Token { get; set; }

    public string? CertificatePath { get; set; }

    public string? KeyPath { get; set; }

    public int FlushIntervalMs { get; set; } = 10;

    public int FlushMaxRecords { get; set; } = 256;

    public int MaxMessageBytes { get; set; } = 1024 * 1024;

    public int MaxConnections { get; set; } = 10_000;

    public int SegmentBytes { get; set; } = 64 * 1024 * 1024;

    public bool Verbose { get; set; }
}
