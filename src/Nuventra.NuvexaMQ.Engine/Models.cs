namespace Nuventra.NuvexaMQ.Engine;

public enum NuvexaMqError
{
    Invalid = 0,
    Unauthorized = 1,
    NotFound = 2,
    Conflict = 3,
    TooLarge = 4,
    OffsetReset = 5,
    Closed = 6
}

public sealed class NuvexaMqException : Exception
{
    public NuvexaMqException(NuvexaMqError error, string message, long detail = 0)
        : base(message)
    {
        Error = error;
        Detail = detail;
    }

    public NuvexaMqError Error { get; }

    public long Detail { get; }
}

public enum ConsumeStart
{
    First = 0,
    Tail = 1,
    Offset = 2
}

public sealed class BrokerOptions
{
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromMilliseconds(10);

    public int FlushMaxRecords { get; init; } = 256;

    public int MaxMessageBytes { get; init; } = 1024 * 1024;

    public int SegmentBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Replication epoch written into every segment header and commit file.
    /// This node is always epoch 1. A later follower wave uses the field as the term.
    /// </summary>
    public ulong Epoch { get; init; } = 1;
}

public sealed class StreamSpec
{
    public StreamSpec(string name, IEnumerable<string> filters)
    {
        Name = name;
        Filters = filters.ToArray();
    }

    public string Name { get; }

    public IReadOnlyList<string> Filters { get; }

    public int PartitionCount { get; init; } = 1;

    public TimeSpan? MaxAge { get; init; }

    public long? MaxBytes { get; init; }

    public int? MaxMessageBytes { get; init; }

    public bool Queue { get; init; }
}

public sealed class ConsumeSpec
{
    public ConsumeSpec(string stream, string name)
    {
        Stream = stream;
        Name = name;
    }

    public string Stream { get; }

    public string Name { get; }

    public string? Filter { get; init; }

    public TimeSpan AckWait { get; init; } = TimeSpan.FromSeconds(30);

    public int MaxDeliver { get; init; } = 5;

    public int MaxAckPending { get; init; } = 1000;

    public bool Ephemeral { get; init; }

    public ConsumeStart? Start { get; init; }

    public long StartOffset { get; init; }
}

public readonly record struct MessageHeader(string Name, byte[] Value);

public readonly record struct PublishReceipt(string Stream, int Partition, long Offset);

public sealed class StoredRecord
{
    public long Offset { get; init; }

    public long TimestampUnixMs { get; init; }

    public string Subject { get; init; } = "";

    public byte[] Key { get; init; } = [];

    public IReadOnlyList<MessageHeader> Headers { get; init; } = [];

    public byte[] Payload { get; init; } = [];
}

public sealed class Delivery
{
    public string Stream { get; init; } = "";

    public string Consumer { get; init; } = "";

    public int Partition { get; init; }

    public long Offset { get; init; }

    public long TimestampUnixMs { get; init; }

    public int DeliveryCount { get; init; }

    public string Subject { get; init; } = "";

    public byte[] Key { get; init; } = [];

    public IReadOnlyList<MessageHeader> Headers { get; init; } = [];

    public byte[] Payload { get; init; } = [];
}

public sealed class FetchResult
{
    public bool OffsetReset { get; init; }

    public long FirstOffset { get; init; }

    public IReadOnlyList<Delivery> Messages { get; init; } = [];
}

public sealed class BrokerSnapshot
{
    public long MessagesIn { get; init; }

    public long MessagesOut { get; init; }

    public double LastFsyncSeconds { get; init; }

    public IReadOnlyList<PartitionSnapshot> Partitions { get; init; } = [];

    public IReadOnlyList<ConsumerSnapshot> Consumers { get; init; } = [];
}

public readonly record struct PartitionSnapshot(string Stream, int Partition, long CommittedOffset, long FirstOffset);

public readonly record struct ConsumerSnapshot(string Stream, string Consumer, int Partition, int Pending);

public sealed class StreamInfo
{
    public string Name { get; init; } = "";

    public IReadOnlyList<string> Filters { get; init; } = [];

    public int PartitionCount { get; init; }

    public long? MaxAgeMs { get; init; }

    public long? MaxBytes { get; init; }

    public int MaxMessageBytes { get; init; }

    public bool Queue { get; init; }
}

public sealed class ConsumerInfo
{
    public string Stream { get; init; } = "";

    public string Name { get; init; } = "";

    public int Partition { get; init; }

    public int Pending { get; init; }

    public long NextOffset { get; init; }

    public string Filter { get; init; } = "";

    public bool Ephemeral { get; init; }

    public int AckWaitMs { get; init; }

    public int MaxDeliver { get; init; }
}
