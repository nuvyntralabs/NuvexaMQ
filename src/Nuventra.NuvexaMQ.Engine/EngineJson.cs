using System.Text.Json.Serialization;

namespace Nuventra.NuvexaMQ.Engine;

internal static class AtomicFile
{
    public static void Write(string path, string contents)
    {
        var tmp = path + ".tmp";
        File.WriteAllText(tmp, contents);
        File.Move(tmp, path, overwrite: true);
    }
}

internal sealed class CommitDto
{
    public ulong Epoch { get; set; }

    public long CommittedOffset { get; set; }
}

internal sealed class StreamMetaDto
{
    public string Name { get; set; } = "";

    public string[] Filters { get; set; } = [];

    public int PartitionCount { get; set; } = 1;

    public long? MaxAgeMs { get; set; }

    public long? MaxBytes { get; set; }

    public int MaxMessageBytes { get; set; }

    public ulong Epoch { get; set; } = 1;

    public bool Queue { get; set; }

    public long FloorOffset { get; set; }

    public long? MessageTtlMs { get; set; }

    public List<DeletedMessageDto> Deleted { get; set; } = [];
}

internal sealed class DeletedMessageDto
{
    public int Partition { get; set; }

    public long Offset { get; set; }
}

internal sealed class PendingDto
{
    public long Offset { get; set; }

    public int DeliveryCount { get; set; }

    public long DeliveredAtUnixMs { get; set; }

    public bool DeadLettering { get; set; }
}

internal sealed class ConsumerDto
{
    public string Name { get; set; } = "";

    public string Filter { get; set; } = "";

    public int AckWaitMs { get; set; }

    public int MaxDeliver { get; set; }

    public int MaxAckPending { get; set; }

    public long NextOffset { get; set; }

    public List<PendingDto> Pending { get; set; } = [];
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(CommitDto))]
[JsonSerializable(typeof(StreamMetaDto))]
[JsonSerializable(typeof(DeletedMessageDto))]
[JsonSerializable(typeof(ConsumerDto))]
[JsonSerializable(typeof(TopologyFile))]
internal partial class EngineJsonContext : JsonSerializerContext;
