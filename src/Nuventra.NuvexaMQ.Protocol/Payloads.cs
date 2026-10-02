using System.Text;

namespace Nuventra.NuvexaMQ.Protocol;

public readonly record struct PublishReceiptWire(string Stream, int Partition, long Offset);

public readonly record struct HeaderWire(string Name, byte[] Value);

public readonly record struct DeliveryWire(
    int Partition,
    long Offset,
    long TimestampUnixMs,
    int DeliveryCount,
    string Subject,
    byte[] Key,
    IReadOnlyList<HeaderWire> Headers,
    byte[] Payload);

public static class Payloads
{
    public const byte StartFirst = 0;
    public const byte StartTail = 1;
    public const byte StartOffset = 2;

    public static byte[] Hello(string token, string clientName, string user, string password, string vhost)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(token);
        wire.WriteString(clientName);
        wire.WriteString(user);
        wire.WriteString(password);
        wire.WriteString(vhost);
        return wire.ToArray();
    }

    public static (string Token, string ClientName, string User, string Password, string Vhost) ReadHello(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var token = wire.ReadString();
        var clientName = wire.ReadString();
        if (!wire.HasRemaining)
            return (token, clientName, "", "", "/");
        var user = wire.ReadString();
        var password = wire.HasRemaining ? wire.ReadString() : "";
        var vhost = wire.HasRemaining ? wire.ReadString() : "/";
        return (token, clientName, user, password, vhost);
    }

    public static byte[] HelloOk(string version)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(version);
        return wire.ToArray();
    }

    public static string ReadHelloOk(byte[] payload) => WireBuffer.Reader(payload).ReadString();

    public static byte[] EnsureStream(string name, IReadOnlyList<string> filters, int partitionCount, long maxAgeMs, long maxBytes, int maxMessageBytes)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(name);
        wire.WriteUInt16((ushort)filters.Count);
        foreach (var filter in filters)
            wire.WriteString(filter);
        wire.WriteUInt16((ushort)partitionCount);
        wire.WriteInt64(maxAgeMs);
        wire.WriteInt64(maxBytes);
        wire.WriteInt32(maxMessageBytes);
        return wire.ToArray();
    }

    public static (string Name, string[] Filters, int PartitionCount, long MaxAgeMs, long MaxBytes, int MaxMessageBytes) ReadEnsureStream(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var name = wire.ReadString();
        var count = wire.ReadUInt16();
        var filters = new string[count];
        for (var i = 0; i < count; i++)
            filters[i] = wire.ReadString();
        return (name, filters, wire.ReadUInt16(), wire.ReadInt64(), wire.ReadInt64(), wire.ReadInt32());
    }

    public static byte[] Publish(string subject, string? key, IReadOnlyList<HeaderWire> headers, ReadOnlySpan<byte> payload)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(subject);
        wire.WriteString(key ?? "");
        wire.WriteUInt16((ushort)headers.Count);
        foreach (var header in headers)
        {
            wire.WriteString(header.Name);
            wire.WriteBytes(header.Value);
        }

        wire.WriteBytes(payload);
        return wire.ToArray();
    }

    public static (string Subject, string Key, HeaderWire[] Headers, byte[] Payload) ReadPublish(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var subject = wire.ReadString();
        var key = wire.ReadString();
        var count = wire.ReadUInt16();
        var headers = new HeaderWire[count];
        for (var i = 0; i < count; i++)
            headers[i] = new HeaderWire(wire.ReadString(), wire.ReadBytes());
        return (subject, key, headers, wire.ReadBytes());
    }

    public static byte[] PublishOk(IReadOnlyList<PublishReceiptWire> receipts)
    {
        var wire = WireBuffer.Writer();
        wire.WriteUInt16((ushort)receipts.Count);
        foreach (var receipt in receipts)
        {
            wire.WriteString(receipt.Stream);
            wire.WriteUInt16((ushort)receipt.Partition);
            wire.WriteInt64(receipt.Offset);
        }

        return wire.ToArray();
    }

    public static PublishReceiptWire[] ReadPublishOk(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var count = wire.ReadUInt16();
        var receipts = new PublishReceiptWire[count];
        for (var i = 0; i < count; i++)
            receipts[i] = new PublishReceiptWire(wire.ReadString(), wire.ReadUInt16(), wire.ReadInt64());
        return receipts;
    }

    public static byte[] EnsureConsumer(string stream, string name, string filter, int ackWaitMs, int maxDeliver, int maxAckPending, bool ephemeral, byte start, long startOffset)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(stream);
        wire.WriteString(name);
        wire.WriteString(filter);
        wire.WriteInt32(ackWaitMs);
        wire.WriteInt32(maxDeliver);
        wire.WriteInt32(maxAckPending);
        wire.WriteByte(ephemeral ? (byte)1 : (byte)0);
        wire.WriteByte(start);
        wire.WriteInt64(startOffset);
        return wire.ToArray();
    }

    public static (string Stream, string Name, string Filter, int AckWaitMs, int MaxDeliver, int MaxAckPending, bool Ephemeral, byte Start, long StartOffset) ReadEnsureConsumer(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (
            wire.ReadString(),
            wire.ReadString(),
            wire.ReadString(),
            wire.ReadInt32(),
            wire.ReadInt32(),
            wire.ReadInt32(),
            wire.ReadByte() == 1,
            wire.ReadByte(),
            wire.ReadInt64());
    }

    public static byte[] Fetch(string stream, string consumer, int maxMessages, int expiresMs)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(stream);
        wire.WriteString(consumer);
        wire.WriteUInt16((ushort)maxMessages);
        wire.WriteInt32(expiresMs);
        return wire.ToArray();
    }

    public static (string Stream, string Consumer, int MaxMessages, int ExpiresMs) ReadFetch(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadString(), wire.ReadUInt16(), wire.ReadInt32());
    }

    public static byte[] FetchOk(bool offsetReset, long firstOffset, IReadOnlyList<DeliveryWire> messages)
    {
        var wire = WireBuffer.Writer();
        wire.WriteByte(offsetReset ? (byte)1 : (byte)0);
        wire.WriteInt64(firstOffset);
        wire.WriteUInt16((ushort)messages.Count);
        foreach (var message in messages)
        {
            wire.WriteUInt16((ushort)message.Partition);
            wire.WriteInt64(message.Offset);
            wire.WriteInt64(message.TimestampUnixMs);
            wire.WriteInt32(message.DeliveryCount);
            wire.WriteString(message.Subject);
            wire.WriteBytes(message.Key);
            wire.WriteUInt16((ushort)message.Headers.Count);
            foreach (var header in message.Headers)
            {
                wire.WriteString(header.Name);
                wire.WriteBytes(header.Value);
            }

            wire.WriteBytes(message.Payload);
        }

        return wire.ToArray();
    }

    public static (bool OffsetReset, long FirstOffset, DeliveryWire[] Messages) ReadFetchOk(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var reset = wire.ReadByte() == 1;
        var first = wire.ReadInt64();
        var count = wire.ReadUInt16();
        var messages = new DeliveryWire[count];
        for (var i = 0; i < count; i++)
        {
            var partition = wire.ReadUInt16();
            var offset = wire.ReadInt64();
            var timestamp = wire.ReadInt64();
            var deliveryCount = wire.ReadInt32();
            var subject = wire.ReadString();
            var key = wire.ReadBytes();
            var headerCount = wire.ReadUInt16();
            var headers = new HeaderWire[headerCount];
            for (var h = 0; h < headerCount; h++)
                headers[h] = new HeaderWire(wire.ReadString(), wire.ReadBytes());
            messages[i] = new DeliveryWire(partition, offset, timestamp, deliveryCount, subject, key, headers, wire.ReadBytes());
        }

        return (reset, first, messages);
    }

    public static byte[] Ack(string stream, string consumer, int partition, long offset)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(stream);
        wire.WriteString(consumer);
        wire.WriteUInt16((ushort)partition);
        wire.WriteInt64(offset);
        return wire.ToArray();
    }

    public static (string Stream, string Consumer, int Partition, long Offset) ReadAck(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadString(), wire.ReadUInt16(), wire.ReadInt64());
    }

    public static byte[] Reset(string stream, string consumer, bool absolute, long offset)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(stream);
        wire.WriteString(consumer);
        wire.WriteByte(absolute ? (byte)1 : (byte)0);
        wire.WriteInt64(offset);
        return wire.ToArray();
    }

    public static (string Stream, string Consumer, bool Absolute, long Offset) ReadReset(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadString(), wire.ReadByte() == 1, wire.ReadInt64());
    }

    public static byte[] Release(string stream, string consumer)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(stream);
        wire.WriteString(consumer);
        return wire.ToArray();
    }

    public static (string Stream, string Consumer) ReadRelease(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadString());
    }

    public static byte[] Error(NuvexaMqError error, string message, long detail)
    {
        var wire = WireBuffer.Writer();
        wire.WriteUInt16((ushort)error);
        wire.WriteString(message);
        wire.WriteInt64(detail);
        return wire.ToArray();
    }

    public static (NuvexaMqError Error, string Message, long Detail) ReadError(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return ((NuvexaMqError)wire.ReadUInt16(), wire.ReadString(), wire.ReadInt64());
    }

    public static byte[] DeclareExchange(string name, string type, bool durable, bool autoDelete)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(name);
        wire.WriteString(type);
        wire.WriteByte(durable ? (byte)1 : (byte)0);
        wire.WriteByte(autoDelete ? (byte)1 : (byte)0);
        return wire.ToArray();
    }

    public static (string Name, string Type, bool Durable, bool AutoDelete) ReadDeclareExchange(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadString(), wire.ReadByte() == 1, wire.ReadByte() == 1);
    }

    public static byte[] DeclareQueue(string name, bool durable, bool exclusive, bool autoDelete, long messageTtlMs, int maxLength, string deadLetterExchange, string deadLetterRoutingKey)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(name);
        wire.WriteByte(durable ? (byte)1 : (byte)0);
        wire.WriteByte(exclusive ? (byte)1 : (byte)0);
        wire.WriteByte(autoDelete ? (byte)1 : (byte)0);
        wire.WriteInt64(messageTtlMs);
        wire.WriteInt32(maxLength);
        wire.WriteString(deadLetterExchange);
        wire.WriteString(deadLetterRoutingKey);
        return wire.ToArray();
    }

    public static (string Name, bool Durable, bool Exclusive, bool AutoDelete, long MessageTtlMs, int MaxLength, string DeadLetterExchange, string DeadLetterRoutingKey) ReadDeclareQueue(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        return (wire.ReadString(), wire.ReadByte() == 1, wire.ReadByte() == 1, wire.ReadByte() == 1, wire.ReadInt64(), wire.ReadInt32(), wire.ReadString(), wire.ReadString());
    }

    public static byte[] BindQueue(string exchange, string queue, string routingKey, IReadOnlyList<HeaderWire> arguments)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(exchange);
        wire.WriteString(queue);
        wire.WriteString(routingKey);
        wire.WriteUInt16((ushort)arguments.Count);
        foreach (var argument in arguments)
        {
            wire.WriteString(argument.Name);
            wire.WriteString(Encoding.UTF8.GetString(argument.Value));
        }

        return wire.ToArray();
    }

    public static (string Exchange, string Queue, string RoutingKey, (string Name, string Value)[] Arguments) ReadBindQueue(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var exchange = wire.ReadString();
        var queue = wire.ReadString();
        var routingKey = wire.ReadString();
        var count = wire.ReadUInt16();
        var arguments = new (string Name, string Value)[count];
        for (var i = 0; i < count; i++)
            arguments[i] = (wire.ReadString(), wire.ReadString());
        return (exchange, queue, routingKey, arguments);
    }

    public static byte[] Named(string name)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(name);
        return wire.ToArray();
    }

    public static string ReadNamed(byte[] payload) => WireBuffer.Reader(payload).ReadString();

    public static byte[] PublishExchange(string exchange, string routingKey, string? key, IReadOnlyList<HeaderWire> headers, ReadOnlySpan<byte> payload)
    {
        var wire = WireBuffer.Writer();
        wire.WriteString(exchange);
        wire.WriteString(routingKey);
        wire.WriteString(key ?? "");
        wire.WriteUInt16((ushort)headers.Count);
        foreach (var header in headers)
        {
            wire.WriteString(header.Name);
            wire.WriteBytes(header.Value);
        }

        wire.WriteBytes(payload);
        return wire.ToArray();
    }

    public static (string Exchange, string RoutingKey, string Key, HeaderWire[] Headers, byte[] Payload) ReadPublishExchange(byte[] payload)
    {
        var wire = WireBuffer.Reader(payload);
        var exchange = wire.ReadString();
        var routingKey = wire.ReadString();
        var key = wire.ReadString();
        var count = wire.ReadUInt16();
        var headers = new HeaderWire[count];
        for (var i = 0; i < count; i++)
            headers[i] = new HeaderWire(wire.ReadString(), wire.ReadBytes());
        return (exchange, routingKey, key, headers, wire.ReadBytes());
    }
}
