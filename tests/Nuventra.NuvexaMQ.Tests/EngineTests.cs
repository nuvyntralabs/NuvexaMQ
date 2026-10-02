using Nuventra.NuvexaMQ.Engine;
using Xunit;

namespace Nuventra.NuvexaMQ.Tests;

using ConsumeSpec = Nuventra.NuvexaMQ.Engine.ConsumeSpec;
using StreamSpec = Nuventra.NuvexaMQ.Engine.StreamSpec;

public sealed class EngineTests
{
    [Fact]
    public async Task Read_back_is_in_offset_order()
    {
        using var dir = new TempDir();
        await using var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await broker.PublishAsync("orders.created", "one"u8.ToArray());
        await broker.PublishAsync("orders.created", "two"u8.ToArray());
        await broker.PublishAsync("orders.created", "three"u8.ToArray());
        broker.EnsureConsumer(new ConsumeSpec("orders", "billing"));
        var fetched = await broker.FetchAsync("orders", "billing", 10, TimeSpan.Zero);
        Assert.Equal(["one", "two", "three"], fetched.Messages.Select(message => System.Text.Encoding.UTF8.GetString(message.Payload)));
        Assert.Equal(fetched.Messages.OrderBy(message => message.Offset), fetched.Messages);
    }

    [Fact]
    public async Task Missing_index_is_rebuilt_and_records_remain()
    {
        using var dir = new TempDir();
        var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await broker.PublishAsync("orders.created", "kept"u8.ToArray());
        await broker.DisposeAsync();
        var index = Directory.GetFiles(Path.Combine(dir.Path, "streams", "orders", "p0"), "*.idx").Single();
        File.Delete(index);
        await using var reopened = Open(dir.Path);
        Assert.True(reopened.TryRead("orders", 0, 0, out var record));
        Assert.Equal("kept", System.Text.Encoding.UTF8.GetString(record!.Payload));
        Assert.True(File.Exists(index));
    }

    [Fact]
    public async Task Torn_tail_is_truncated_and_the_previous_record_remains()
    {
        using var dir = new TempDir();
        var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await broker.PublishAsync("orders.created", "first"u8.ToArray());
        await broker.PublishAsync("orders.created", "second"u8.ToArray());
        await broker.DisposeAsync();

        var log = Path.Combine(dir.Path, "streams", "orders", "p0", "segment-00000000000000000000.log");
        var length = new FileInfo(log).Length;
        using (var append = new FileStream(log, FileMode.Append, FileAccess.Write, FileShare.None))
            append.Write(new byte[] { 0xFF, 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });

        await using var reopened = Open(dir.Path);
        Assert.Equal(length, new FileInfo(log).Length);
        Assert.True(reopened.TryRead("orders", 0, 0, out var first));
        Assert.True(reopened.TryRead("orders", 0, 1, out var second));
        Assert.Equal("first", System.Text.Encoding.UTF8.GetString(first!.Payload));
        Assert.Equal("second", System.Text.Encoding.UTF8.GetString(second!.Payload));
    }

    [Fact]
    public async Task Acknowledged_publish_survives_reopen()
    {
        using var dir = new TempDir();
        var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        var receipts = await broker.PublishAsync("orders.created", "durable"u8.ToArray(), key: "order-18");
        await broker.DisposeAsync();

        var commit = await File.ReadAllTextAsync(Path.Combine(dir.Path, "streams", "orders", "p0", "commit.json"));
        Assert.Contains("\"epoch\":1", commit, StringComparison.Ordinal);
        Assert.Contains(receipts[0].Offset.ToString(), commit, StringComparison.Ordinal);

        await using var reopened = Open(dir.Path);
        Assert.True(reopened.TryRead("orders", 0, receipts[0].Offset, out var record));
        Assert.Equal("durable", System.Text.Encoding.UTF8.GetString(record!.Payload));
    }

    [Fact]
    public async Task Same_key_stays_on_one_partition()
    {
        using var dir = new TempDir();
        await using var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]) { PartitionCount = 4 });
        var first = await broker.PublishAsync("orders.created", "a"u8.ToArray(), key: "order-18");
        var second = await broker.PublishAsync("orders.created", "b"u8.ToArray(), key: "order-18");
        Assert.Equal(first[0].Partition, second[0].Partition);
    }

    [Fact]
    public async Task Retention_drops_an_old_segment_and_keeps_a_later_offset()
    {
        using var dir = new TempDir();
        await using var broker = NuvexaBroker.Open(dir.Path, new BrokerOptions
        {
            FlushInterval = TimeSpan.Zero,
            SegmentBytes = 64
        });
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]) { MaxBytes = 1 });
        var first = await broker.PublishAsync("orders.created", "aaa"u8.ToArray());
        await broker.PublishAsync("orders.created", "bbb"u8.ToArray());
        var third = await broker.PublishAsync("orders.created", "ccc"u8.ToArray());
        Assert.False(broker.TryRead("orders", 0, first[0].Offset, out _));
        Assert.True(broker.TryRead("orders", 0, third[0].Offset, out var kept));
        Assert.Equal("ccc", System.Text.Encoding.UTF8.GetString(kept!.Payload));
    }

    [Fact]
    public async Task Unacked_message_is_redelivered_then_dead_lettered()
    {
        using var dir = new TempDir();
        await using var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await broker.PublishAsync("orders.created", "poison"u8.ToArray());
        broker.EnsureConsumer(new ConsumeSpec("orders", "billing")
        {
            AckWait = TimeSpan.FromMilliseconds(30),
            MaxDeliver = 2
        });

        var first = await broker.FetchAsync("orders", "billing", 1, TimeSpan.Zero);
        Assert.Equal(1, first.Messages[0].DeliveryCount);
        await Task.Delay(80);
        var second = await broker.FetchAsync("orders", "billing", 1, TimeSpan.Zero);
        Assert.Equal(2, second.Messages[0].DeliveryCount);
        Assert.Equal(first.Messages[0].Offset, second.Messages[0].Offset);
        await Task.Delay(80);
        var third = await broker.FetchAsync("orders", "billing", 1, TimeSpan.Zero);
        Assert.Empty(third.Messages);

        broker.EnsureConsumer(new ConsumeSpec(NuvexaBroker.DeadLetterStream, "audit"));
        var dead = await broker.FetchAsync(NuvexaBroker.DeadLetterStream, "audit", 10, TimeSpan.Zero);
        Assert.Equal("poison", System.Text.Encoding.UTF8.GetString(dead.Messages[0].Payload));
    }

    [Fact]
    public async Task Deleted_message_is_skipped_and_the_dead_letter_stream_stays()
    {
        using var dir = new TempDir();
        await using var broker = Open(dir.Path);
        await broker.EnsureStreamAsync(new StreamSpec("orders", ["orders.>"]));
        await broker.PublishAsync("orders.created", "alpha"u8.ToArray());
        await broker.PublishAsync("orders.created", "beta"u8.ToArray());
        broker.DeleteMessage("orders", 0, 0);
        Assert.Equal(1, broker.VisibleMessageCount("orders"));

        broker.EnsureConsumer(new ConsumeSpec("orders", "billing"));
        var fetched = await broker.FetchAsync("orders", "billing", 10, TimeSpan.Zero);
        Assert.Equal(["beta"], fetched.Messages.Select(message => System.Text.Encoding.UTF8.GetString(message.Payload)));

        var blocked = await Assert.ThrowsAsync<Nuventra.NuvexaMQ.Engine.NuvexaMqException>(() => broker.DeleteStreamAsync(NuvexaBroker.DeadLetterStream));
        Assert.Equal("Invalid", blocked.Error.ToString());
        await broker.DeleteStreamAsync("orders");
        Assert.DoesNotContain(broker.ListStreams(), stream => stream.Name == "orders");
    }

    [Fact]
    public void Subject_wildcards_match_one_token_or_the_rest()
    {
        Assert.True(SubjectFilter.Matches("orders.>", "orders.created"));
        Assert.True(SubjectFilter.Matches("orders.>", "orders.created.eu"));
        Assert.False(SubjectFilter.Matches("orders.>", "orders"));
        Assert.True(SubjectFilter.Matches("orders.*", "orders.created"));
        Assert.False(SubjectFilter.Matches("orders.*", "orders.created.eu"));
    }

    private static NuvexaBroker Open(string path) =>
        NuvexaBroker.Open(path, new BrokerOptions { FlushInterval = TimeSpan.Zero });
}

public sealed class TempDir : IDisposable
{
    public TempDir()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "nuvexamq-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
