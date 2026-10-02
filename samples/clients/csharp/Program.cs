// Publish one message, then fetch and ack it.
//   dotnet run --project csharp.csproj
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
// This sample speaks the frames itself. It does not reference the client library.

using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;

if (args.Length > 0 && args[0] != "demo")
{
    FeatureSamples.Run(args[0]);
    return;
}

var host = Environment.GetEnvironmentVariable("NUVEXA_HOST") ?? "127.0.0.1";
var port = int.Parse(Environment.GetEnvironmentVariable("NUVEXA_PORT") ?? "5761");
const string language = "csharp";
var body = Encoding.UTF8.GetBytes($"hello from {language}");

using var tcp = new TcpClient(host, port);
var stream = tcp.GetStream();
var client = new Client(stream);
client.Hello("guest", "guest", "/", $"sample-{language}");
client.EnsureStream("clients", ["clients.>"]);
client.Publish("clients.created", body, language);
var consumer = $"demo-{language}";
client.EnsureConsumer("clients", consumer);
var found = false;
while (!found)
{
    var messages = client.Fetch("clients", consumer);
    if (messages.Count == 0)
        throw new InvalidOperationException("published message was not delivered");
    foreach (var message in messages)
    {
        var text = Encoding.UTF8.GetString(message.Payload);
        Console.WriteLine($"fetched {message.Offset} {text}");
        client.Ack("clients", consumer, message.Partition, message.Offset);
        found |= text == $"hello from {language}";
    }
}

sealed class Client(NetworkStream stream)
{
    private uint _nextId;

    public void Hello(string user, string password, string vhost, string name)
    {
        var writer = new Writer();
        writer.Str("");
        writer.Str(name);
        writer.Str(user);
        writer.Str(password);
        writer.Str(vhost);
        Request(Op.Hello, writer.Bytes(), Op.HelloOk);
    }

    public void EnsureStream(string name, string[] filters)
    {
        var writer = new Writer();
        writer.Str(name);
        writer.U16((ushort)filters.Length);
        foreach (var filter in filters)
            writer.Str(filter);
        writer.U16(1);
        writer.I64(-1);
        writer.I64(-1);
        writer.I32(0);
        Request(Op.EnsureStream, writer.Bytes(), Op.EnsureStreamOk);
    }

    public void Publish(string subject, byte[] payload, string key)
    {
        var writer = new Writer();
        writer.Str(subject);
        writer.Str(key);
        writer.U16(0);
        writer.Blob(payload);
        var reader = new Reader(Request(Op.Publish, writer.Bytes(), Op.PublishOk));
        if (reader.U16() < 1)
            throw new InvalidOperationException("publish was not stored");
        var streamName = reader.Str();
        var partition = reader.U16();
        var offset = reader.I64();
        Console.WriteLine($"published {streamName} partition {partition} offset {offset}");
    }

    public void EnsureConsumer(string streamName, string name)
    {
        var writer = new Writer();
        writer.Str(streamName);
        writer.Str(name);
        writer.Str("");
        writer.I32(30000);
        writer.I32(5);
        writer.I32(1000);
        writer.U8(0);
        writer.U8(0);
        writer.I64(0);
        Request(Op.EnsureConsumer, writer.Bytes(), Op.EnsureConsumerOk);
    }

    public List<Message> Fetch(string streamName, string name)
    {
        var writer = new Writer();
        writer.Str(streamName);
        writer.Str(name);
        writer.U16(32);
        writer.I32(0);
        var reader = new Reader(Request(Op.Fetch, writer.Bytes(), Op.FetchOk));
        if (reader.U8() == 1)
            throw new InvalidOperationException($"offset reset at {reader.I64()}");
        reader.I64();
        var count = reader.U16();
        var messages = new List<Message>(count);
        for (var i = 0; i < count; i++)
        {
            var partition = reader.U16();
            var offset = reader.I64();
            reader.I64();
            reader.I32();
            reader.Str();
            reader.Blob();
            var headers = reader.U16();
            for (var h = 0; h < headers; h++)
            {
                reader.Str();
                reader.Blob();
            }
            messages.Add(new Message(partition, offset, reader.Blob()));
        }
        return messages;
    }

    public void Ack(string streamName, string name, int partition, long offset)
    {
        var writer = new Writer();
        writer.Str(streamName);
        writer.Str(name);
        writer.U16((ushort)partition);
        writer.I64(offset);
        Request(Op.Ack, writer.Bytes(), Op.AckOk);
    }

    private byte[] Request(ushort op, byte[] payload, ushort expected)
    {
        _nextId++;
        var length = 6 + payload.Length;
        var frame = new byte[4 + length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, length);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(4), op);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(6), _nextId);
        payload.CopyTo(frame.AsSpan(10));
        stream.Write(frame);
        var size = ReadExact(4);
        var body = ReadExact(BinaryPrimitives.ReadInt32LittleEndian(size));
        var kind = BinaryPrimitives.ReadUInt16LittleEndian(body);
        var requestId = BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(2));
        var data = body.AsSpan(6).ToArray();
        if (requestId != _nextId)
            throw new InvalidOperationException("response id did not match the request");
        if (kind == Op.Error)
        {
            var reader = new Reader(data);
            throw new InvalidOperationException($"broker error {reader.U16()}: {reader.Str()}");
        }
        if (kind != expected)
            throw new InvalidOperationException($"unexpected frame {kind}");
        return data;
    }

    private byte[] ReadExact(int count)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = stream.Read(buffer, offset, count - offset);
            if (read == 0)
                throw new EndOfStreamException("The broker connection closed.");
            offset += read;
        }
        return buffer;
    }
}

sealed class Writer
{
    private readonly MemoryStream _buf = new();
    public void U8(int value) => _buf.WriteByte((byte)value);
    public void U16(int value)
    {
        Span<byte> raw = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(raw, (ushort)value);
        _buf.Write(raw);
    }
    public void I32(int value)
    {
        Span<byte> raw = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(raw, value);
        _buf.Write(raw);
    }
    public void I64(long value)
    {
        Span<byte> raw = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(raw, value);
        _buf.Write(raw);
    }
    public void Str(string value)
    {
        var raw = Encoding.UTF8.GetBytes(value);
        U16(raw.Length);
        _buf.Write(raw);
    }
    public void Blob(byte[] value)
    {
        I32(value.Length);
        _buf.Write(value);
    }
    public byte[] Bytes() => _buf.ToArray();
}

sealed class Reader(byte[] data)
{
    private int _i;
    private ReadOnlySpan<byte> Take(int count)
    {
        if (_i + count > data.Length)
            throw new InvalidOperationException("frame ended early");
        var chunk = data.AsSpan(_i, count);
        _i += count;
        return chunk;
    }
    public int U8() => Take(1)[0];
    public int U16() => BinaryPrimitives.ReadUInt16LittleEndian(Take(2));
    public int I32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));
    public long I64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));
    public string Str() => Encoding.UTF8.GetString(Take(U16()));
    public byte[] Blob() => Take(I32()).ToArray();
}

readonly record struct Message(int Partition, long Offset, byte[] Payload);

static class Op
{
    public const ushort Hello = 1, HelloOk = 2, EnsureStream = 3, EnsureStreamOk = 4;
    public const ushort Publish = 5, PublishOk = 6, EnsureConsumer = 7, EnsureConsumerOk = 8;
    public const ushort Fetch = 9, FetchOk = 10, Ack = 11, AckOk = 12, Error = 21;
}
