using System.Buffers.Binary;

namespace Nuventra.NuvexaMQ.Protocol;

public static class Op
{
    public const ushort Hello = 1;
    public const ushort HelloOk = 2;
    public const ushort EnsureStream = 3;
    public const ushort EnsureStreamOk = 4;
    public const ushort Publish = 5;
    public const ushort PublishOk = 6;
    public const ushort EnsureConsumer = 7;
    public const ushort EnsureConsumerOk = 8;
    public const ushort Fetch = 9;
    public const ushort FetchOk = 10;
    public const ushort Ack = 11;
    public const ushort AckOk = 12;
    public const ushort Nack = 13;
    public const ushort NackOk = 14;
    public const ushort ResetConsumer = 15;
    public const ushort ResetConsumerOk = 16;
    public const ushort ReleaseConsumer = 17;
    public const ushort ReleaseConsumerOk = 18;
    public const ushort Ping = 19;
    public const ushort Pong = 20;
    public const ushort Error = 21;
    public const ushort DeclareExchange = 22;
    public const ushort DeclareExchangeOk = 23;
    public const ushort DeclareQueue = 24;
    public const ushort DeclareQueueOk = 25;
    public const ushort BindQueue = 26;
    public const ushort BindQueueOk = 27;
    public const ushort DeleteQueue = 28;
    public const ushort DeleteQueueOk = 29;
    public const ushort DeleteExchange = 30;
    public const ushort DeleteExchangeOk = 31;
    public const ushort PurgeQueue = 32;
    public const ushort PurgeQueueOk = 33;
    public const ushort PublishExchange = 34;
    public const ushort PublishExchangeOk = 35;
}

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

public readonly record struct Frame(ushort Type, uint RequestId, byte[] Payload);

public static class FrameCodec
{
    public const int MaxFrameBytes = 32 * 1024 * 1024;

    public static async ValueTask WriteAsync(Stream stream, Frame frame, CancellationToken cancellationToken)
    {
        var length = 6 + frame.Payload.Length;
        var buffer = new byte[4 + length];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, length);
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(4), frame.Type);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(6), frame.RequestId);
        frame.Payload.CopyTo(buffer.AsSpan(10));
        await stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public static async ValueTask<Frame> ReadAsync(Stream stream, CancellationToken cancellationToken)
    {
        var lengthBytes = new byte[4];
        await ReadExactAsync(stream, lengthBytes, cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
        if (length < 6 || length > MaxFrameBytes)
            throw new InvalidDataException("Frame length is out of range.");

        var body = new byte[length];
        await ReadExactAsync(stream, body, cancellationToken).ConfigureAwait(false);
        var type = BinaryPrimitives.ReadUInt16LittleEndian(body);
        var requestId = BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(2));
        var payload = body.AsSpan(6).ToArray();
        return new Frame(type, requestId, payload);
    }

    private static async ValueTask ReadExactAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer[offset..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException("The broker connection closed.");
            offset += read;
        }
    }
}
