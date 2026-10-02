using System.Buffers.Binary;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

static class FeatureSamples
{
    public static void Run(string command)
    {
        var language = "csharp";
        if (command == "admin")
        {
            Admin(language);
            return;
        }

        using var client = Wire.Connect(language);
        switch (command)
        {
            case "ping":
                client.Ping();
                break;
            case "stream":
                Stream(client, language);
                break;
            case "consume":
                Consume(client, language);
                break;
            case "exchange":
                Exchange(client, language);
                break;
            default:
                throw new InvalidOperationException("Use demo, ping, stream, consume, exchange, or admin.");
        }
    }

    static void Stream(Wire client, string language)
    {
        var stream = $"catalog-{language}";
        client.EnsureStream(stream, [$"{stream}.>"], 2, 86_400_000, 1_048_576, 65_536);
        var body = """{"id":1}"""u8.ToArray();
        var headers = new[] { ("content-type", "application/json"u8.ToArray()) };
        var keyed = client.Publish($"{stream}.created", body, "alpha", headers);
        var first = client.Publish($"{stream}.created", body);
        var second = client.Publish($"{stream}.created", body);
        if (keyed.Count == 0 || first[0].Partition == second[0].Partition)
            throw new InvalidOperationException("round-robin did not use both partitions");
    }

    static void Consume(Wire client, string language)
    {
        var stream = $"mailbox-{language}";
        var durable = $"box-{language}";
        var tail = $"tail-{language}";
        client.EnsureStream(stream, [$"{stream}.>"]);
        client.EnsureConsumer(stream, durable, filter: $"{stream}.>");
        client.Publish($"{stream}.created", "ack me"u8.ToArray(), headers: [("content-type", "text/plain"u8.ToArray())]);
        client.Publish($"{stream}.created", "nack me"u8.ToArray(), headers: [("content-type", "text/plain"u8.ToArray())]);
        var found = client.Fetch(stream, durable).ToDictionary(message => Encoding.UTF8.GetString(message.Payload));
        client.Ack(stream, durable, found["ack me"]);
        Console.WriteLine("ack");
        client.Nack(stream, durable, found["nack me"]);
        Console.WriteLine("nack");
        var again = client.Fetch(stream, durable).First(message => Encoding.UTF8.GetString(message.Payload) == "nack me");
        Console.WriteLine($"redelivered {again.Attempts}");
        client.Ack(stream, durable, again);
        client.Reset(stream, durable, 0);
        var reset = client.Fetch(stream, durable, 1).First();
        Console.WriteLine($"after reset offset {reset.Offset}");
        client.Ack(stream, durable, reset);
        client.Release(stream, tail);
        client.EnsureConsumer(stream, tail, ephemeral: true, start: 1);
        client.Publish($"{stream}.created", "tail me"u8.ToArray());
        var tailed = client.Fetch(stream, tail).First(message => Encoding.UTF8.GetString(message.Payload) == "tail me");
        client.Ack(stream, tail, tailed);
        client.Release(stream, tail);
        client.EnsureConsumer(stream, $"from0-{language}", start: 2, startOffset: 0);
    }

    static void Exchange(Wire client, string language)
    {
        var queue = $"work-{language}";
        var dead = $"dead-{language}";
        var direct = $"direct-{language}";
        var fanout = $"fanout-{language}";
        var topic = $"topic-{language}";
        var headers = $"headers-{language}";
        client.DeclareExchange("amq.direct", "direct");
        client.DeclareExchange(direct, "direct");
        client.DeclareExchange(fanout, "fanout");
        client.DeclareExchange(topic, "topic");
        client.DeclareExchange(headers, "headers");
        client.DeclareQueue(dead);
        client.DeclareQueue(queue, 60_000, 100, direct, "expired");
        client.Bind(direct, dead, "expired");
        client.Bind(direct, queue, "work.created");
        client.Bind(fanout, queue, "");
        client.Bind(topic, queue, "work.*");
        client.Bind(headers, queue, "", [("format", "json"), ("x-match", "all")]);
        var body = "routed"u8.ToArray();
        var count = 0;
        count += client.PublishExchange(direct, "work.created", body, "order-1").Count;
        count += client.PublishExchange(fanout, "", body).Count;
        count += client.PublishExchange(topic, "work.created", body).Count;
        count += client.PublishExchange(headers, "", body, headers: [("format", "json"u8.ToArray())]).Count;
        count += client.PublishExchange("", queue, body).Count;
        if (count < 5)
            throw new InvalidOperationException($"expected at least 5 receipts, got {count}");
        client.Purge(queue);
        client.DeleteQueue(queue);
        client.DeleteQueue(dead);
        foreach (var name in new[] { direct, fanout, topic, headers })
            client.DeleteExchange(name);
    }

    static void Admin(string language)
    {
        using var http = new HttpClient(new HttpClientHandler { ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator });
        var user = Environment.GetEnvironmentVariable("NUVEXA_USER") ?? "guest";
        var password = Environment.GetEnvironmentVariable("NUVEXA_PASSWORD") ?? "guest";
        var host = Environment.GetEnvironmentVariable("NUVEXA_HOST") ?? "127.0.0.1";
        var health = int.Parse(Environment.GetEnvironmentVariable("NUVEXA_HEALTH_PORT") ?? "5762");
        var management = int.Parse(Environment.GetEnvironmentVariable("NUVEXA_MANAGEMENT_PORT") ?? "5763");
        var httpsPort = int.Parse(Environment.GetEnvironmentVariable("NUVEXA_MANAGEMENT_HTTPS_PORT") ?? "5764");
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}")));
        Get(http, $"http://{host}:{health}/health");
        Get(http, $"http://{host}:{health}/metrics");
        var baseUrl = $"http://{host}:{management}";
        foreach (var path in new[] { "/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies" })
            Get(http, baseUrl + path);
        var vhost = $"vh-{language}";
        var account = $"user-{language}";
        var policy = $"policy-{language}";
        Send(http, HttpMethod.Put, $"{baseUrl}/api/vhosts/{vhost}");
        Send(http, HttpMethod.Put, $"{baseUrl}/api/users/{account}", new { password = "sample-pass", tags = new[] { "management" } });
        Send(http, HttpMethod.Put, $"{baseUrl}/api/permissions", new { user = account, vhost, configure = ".*", write = ".*", read = ".*" });
        Send(http, HttpMethod.Put, $"{baseUrl}/api/policies/{policy}", new { vhost = "/", pattern = "sample-.*", priority = 1, messageTtlMs = 60000, maxLength = 100, deadLetterExchange = "", deadLetterRoutingKey = "" });
        Send(http, HttpMethod.Delete, $"{baseUrl}/api/policies/{policy}?vhost=/");
        Send(http, HttpMethod.Delete, $"{baseUrl}/api/permissions?user={account}&vhost={vhost}");
        Send(http, HttpMethod.Delete, $"{baseUrl}/api/users/{account}");
        Send(http, HttpMethod.Delete, $"{baseUrl}/api/vhosts/{vhost}");
        Get(http, $"https://{host}:{httpsPort}/api/whoami");
    }

    static void Get(HttpClient http, string url) => Send(http, HttpMethod.Get, url);

    static void Send(HttpClient http, HttpMethod method, string url, object? body = null)
    {
        var request = new HttpRequestMessage(method, url);
        if (body is not null)
            request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = http.Send(request);
        var text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        Console.WriteLine($"{method} {url} {(int)response.StatusCode} {text.Length} bytes");
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException(text);
    }
}

sealed class Wire : IDisposable
{
    const ushort OpHello = 1, OpHelloOk = 2, OpEnsureStream = 3, OpEnsureStreamOk = 4, OpPublish = 5, OpPublishOk = 6;
    const ushort OpEnsureConsumer = 7, OpEnsureConsumerOk = 8, OpFetch = 9, OpFetchOk = 10, OpAck = 11, OpAckOk = 12, OpNack = 13, OpNackOk = 14;
    const ushort OpReset = 15, OpResetOk = 16, OpRelease = 17, OpReleaseOk = 18, OpPing = 19, OpPong = 20, OpError = 21;
    const ushort OpDeclareExchange = 22, OpDeclareExchangeOk = 23, OpDeclareQueue = 24, OpDeclareQueueOk = 25, OpBindQueue = 26, OpBindQueueOk = 27;
    const ushort OpDeleteQueue = 28, OpDeleteQueueOk = 29, OpDeleteExchange = 30, OpDeleteExchangeOk = 31, OpPurgeQueue = 32, OpPurgeQueueOk = 33;
    const ushort OpPublishExchange = 34, OpPublishExchangeOk = 35;
    readonly NetworkStream _stream;
    readonly TcpClient _tcp;
    uint _next;

    Wire(TcpClient tcp)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
    }

    public static Wire Connect(string language)
    {
        var host = Environment.GetEnvironmentVariable("NUVEXA_HOST") ?? "127.0.0.1";
        var port = int.Parse(Environment.GetEnvironmentVariable("NUVEXA_PORT") ?? "5761");
        var client = new Wire(new TcpClient(host, port));
        client.Hello(language);
        return client;
    }

    public void Dispose() { _tcp.Dispose(); }

    public void Ping() { Request(OpPing, [], OpPong); Console.WriteLine("pong"); }

    public void EnsureStream(string name, string[] filters, int partitions = 1, long maxAge = -1, long maxBytes = -1, int maxMessage = 0)
    {
        var writer = new Buf();
        writer.Str(name); writer.U16(filters.Length);
        foreach (var filter in filters) writer.Str(filter);
        writer.U16(partitions); writer.I64(maxAge); writer.I64(maxBytes); writer.I32(maxMessage);
        Request(OpEnsureStream, writer.Bytes(), OpEnsureStreamOk);
        Console.WriteLine($"stream {name} partitions {partitions}");
    }

    public List<Receipt> Publish(string subject, byte[] payload, string key = "", (string Name, byte[] Value)[]? headers = null) =>
        Receipts(Request(OpPublish, Message(subject, key, headers, payload), OpPublishOk));

    public void EnsureConsumer(string stream, string name, string filter = "", bool ephemeral = false, int start = 0, long startOffset = 0)
    {
        var writer = new Buf();
        writer.Str(stream); writer.Str(name); writer.Str(filter);
        writer.I32(30000); writer.I32(5); writer.I32(1000);
        writer.U8(ephemeral ? 1 : 0); writer.U8(start); writer.I64(startOffset);
        Request(OpEnsureConsumer, writer.Bytes(), OpEnsureConsumerOk);
        Console.WriteLine($"consumer {name} on {stream} start {start}");
    }

    public List<Delivery> Fetch(string stream, string name, int max = 32)
    {
        var writer = new Buf();
        writer.Str(stream); writer.Str(name); writer.U16(max); writer.I32(0);
        var reader = new Rd(Request(OpFetch, writer.Bytes(), OpFetchOk));
        if (reader.U8() == 1) throw new InvalidOperationException($"offset reset at {reader.I64()}");
        reader.I64();
        var messages = new List<Delivery>();
        var count = reader.U16();
        for (var i = 0; i < count; i++)
        {
            var partition = reader.U16();
            var offset = reader.I64();
            reader.I64();
            var delivery = reader.I32();
            reader.Str(); reader.Blob();
            var headers = reader.U16();
            for (var h = 0; h < headers; h++) { reader.Str(); reader.Blob(); }
            messages.Add(new Delivery(partition, offset, delivery, reader.Blob()));
        }
        return messages;
    }

    public void Ack(string stream, string name, Delivery message) => Cursor(OpAck, OpAckOk, stream, name, message);
    public void Nack(string stream, string name, Delivery message) => Cursor(OpNack, OpNackOk, stream, name, message);
    public void Reset(string stream, string name, long offset)
    {
        var writer = new Buf();
        writer.Str(stream); writer.Str(name); writer.U8(1); writer.I64(offset);
        Request(OpReset, writer.Bytes(), OpResetOk);
        Console.WriteLine($"reset {name} offset {offset}");
    }
    public void Release(string stream, string name)
    {
        var writer = new Buf();
        writer.Str(stream); writer.Str(name);
        Request(OpRelease, writer.Bytes(), OpReleaseOk);
        Console.WriteLine($"release {name}");
    }
    public void DeclareExchange(string name, string type)
    {
        var writer = new Buf();
        writer.Str(name); writer.Str(type); writer.U8(1); writer.U8(0);
        Request(OpDeclareExchange, writer.Bytes(), OpDeclareExchangeOk);
        Console.WriteLine($"exchange {name} {type}");
    }
    public void DeclareQueue(string name, long ttl = -1, int max = -1, string deadLetter = "", string deadLetterKey = "")
    {
        var writer = new Buf();
        writer.Str(name); writer.U8(1); writer.U8(0); writer.U8(0); writer.I64(ttl); writer.I32(max); writer.Str(deadLetter); writer.Str(deadLetterKey);
        Request(OpDeclareQueue, writer.Bytes(), OpDeclareQueueOk);
        Console.WriteLine($"queue {name}");
    }
    public void Bind(string exchange, string queue, string routingKey, (string Name, string Value)[]? arguments = null)
    {
        var writer = new Buf();
        writer.Str(exchange); writer.Str(queue); writer.Str(routingKey);
        arguments ??= [];
        writer.U16(arguments.Length);
        foreach (var argument in arguments) { writer.Str(argument.Name); writer.Str(argument.Value); }
        Request(OpBindQueue, writer.Bytes(), OpBindQueueOk);
    }
    public List<Receipt> PublishExchange(string exchange, string routingKey, byte[] payload, string key = "", (string Name, byte[] Value)[]? headers = null)
    {
        var writer = new Buf();
        writer.Str(exchange); writer.Str(routingKey); writer.Str(key);
        headers ??= [];
        writer.U16(headers.Length);
        foreach (var header in headers) { writer.Str(header.Name); writer.Blob(header.Value); }
        writer.Blob(payload);
        var receipts = Receipts(Request(OpPublishExchange, writer.Bytes(), OpPublishExchangeOk));
        Console.WriteLine($"routed {exchange} -> {receipts.Count}");
        return receipts;
    }
    public void Purge(string name) => Named(OpPurgeQueue, OpPurgeQueueOk, name, $"purge queue {name}");
    public void DeleteQueue(string name) => Named(OpDeleteQueue, OpDeleteQueueOk, name, $"delete queue {name}");
    public void DeleteExchange(string name) => Named(OpDeleteExchange, OpDeleteExchangeOk, name, $"delete exchange {name}");

    void Hello(string language)
    {
        var writer = new Buf();
        writer.Str(Environment.GetEnvironmentVariable("NUVEXA_TOKEN") ?? "");
        writer.Str($"sample-{language}");
        writer.Str(Environment.GetEnvironmentVariable("NUVEXA_USER") ?? "guest");
        writer.Str(Environment.GetEnvironmentVariable("NUVEXA_PASSWORD") ?? "guest");
        writer.Str(Environment.GetEnvironmentVariable("NUVEXA_VHOST") ?? "/");
        var version = new Rd(Request(OpHello, writer.Bytes(), OpHelloOk)).Str();
        Console.WriteLine($"hello {version}");
    }

    static byte[] Message(string subject, string key, (string Name, byte[] Value)[]? headers, byte[] payload)
    {
        var writer = new Buf();
        writer.Str(subject); writer.Str(key);
        headers ??= [];
        writer.U16(headers.Length);
        foreach (var header in headers) { writer.Str(header.Name); writer.Blob(header.Value); }
        writer.Blob(payload);
        return writer.Bytes();
    }

    List<Receipt> Receipts(byte[] data)
    {
        var reader = new Rd(data);
        var receipts = new List<Receipt>();
        var count = reader.U16();
        for (var i = 0; i < count; i++)
        {
            var item = new Receipt(reader.Str(), reader.U16(), reader.I64());
            Console.WriteLine($"published {item.Stream} partition {item.Partition} offset {item.Offset}");
            receipts.Add(item);
        }
        return receipts;
    }

    void Cursor(ushort op, ushort expected, string stream, string name, Delivery message)
    {
        var writer = new Buf();
        writer.Str(stream); writer.Str(name); writer.U16(message.Partition); writer.I64(message.Offset);
        Request(op, writer.Bytes(), expected);
    }

    void Named(ushort op, ushort expected, string name, string label)
    {
        var writer = new Buf();
        writer.Str(name);
        Request(op, writer.Bytes(), expected);
        Console.WriteLine(label);
    }

    byte[] Request(ushort op, byte[] payload, ushort expected)
    {
        _next++;
        var frame = new byte[10 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, 6 + payload.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(4), op);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(6), _next);
        payload.CopyTo(frame.AsSpan(10));
        _stream.Write(frame);
        var size = ReadExact(4);
        var body = ReadExact(BinaryPrimitives.ReadInt32LittleEndian(size));
        var kind = BinaryPrimitives.ReadUInt16LittleEndian(body);
        var requestId = BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(2));
        if (requestId != _next) throw new InvalidOperationException("response id did not match the request");
        var data = body.AsSpan(6).ToArray();
        if (kind == OpError) { var reader = new Rd(data); throw new InvalidOperationException($"broker error {reader.U16()}: {reader.Str()}"); }
        if (kind != expected) throw new InvalidOperationException($"unexpected frame {kind}");
        return data;
    }

    byte[] ReadExact(int count)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = _stream.Read(buffer, offset, count - offset);
            if (read == 0) throw new EndOfStreamException("The broker connection closed.");
            offset += read;
        }
        return buffer;
    }
}

sealed class Buf
{
    readonly MemoryStream _buf = new();
    public void U8(int value) => _buf.WriteByte((byte)value);
    public void U16(int value) { Span<byte> raw = stackalloc byte[2]; BinaryPrimitives.WriteUInt16LittleEndian(raw, (ushort)value); _buf.Write(raw); }
    public void I32(int value) { Span<byte> raw = stackalloc byte[4]; BinaryPrimitives.WriteInt32LittleEndian(raw, value); _buf.Write(raw); }
    public void I64(long value) { Span<byte> raw = stackalloc byte[8]; BinaryPrimitives.WriteInt64LittleEndian(raw, value); _buf.Write(raw); }
    public void Str(string value) { var raw = Encoding.UTF8.GetBytes(value); U16(raw.Length); _buf.Write(raw); }
    public void Blob(byte[] value) { I32(value.Length); _buf.Write(value); }
    public byte[] Bytes() => _buf.ToArray();
}

sealed class Rd(byte[] data)
{
    int _i;
    ReadOnlySpan<byte> Take(int count)
    {
        if (_i + count > data.Length) throw new InvalidOperationException("frame ended early");
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

readonly record struct Receipt(string Stream, int Partition, long Offset);
readonly record struct Delivery(int Partition, long Offset, int Attempts, byte[] Payload);
