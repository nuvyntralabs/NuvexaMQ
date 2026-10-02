using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Nuventra.NuvexaMQ.Protocol;

namespace Nuventra.NuvexaMQ;

public enum ConsumeStart
{
    First = 0,
    Tail = 1,
    Offset = 2
}

public sealed class NuvexaClientOptions
{
    public string? Token { get; init; }

    public string? User { get; init; } = "guest";

    public string? Password { get; init; } = "guest";

    public string Vhost { get; init; } = "/";

    public string ClientName { get; init; } = "nuvexamq";

    public bool UseTls { get; init; }

    public RemoteCertificateValidationCallback? CertificateValidation { get; init; }
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

public readonly record struct PublishReceipt(string Stream, int Partition, long Offset);

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

    public long FirstOffset => Detail;
}

public sealed class NuvexaClient : IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly TcpClient _tcp;
    private readonly NuvexaClientOptions _options;
    private readonly string _host;
    private readonly int _port;
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly SemaphoreSlim _slots = new(256, 256);
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<Frame>> _waiters = new();
    private readonly CancellationTokenSource _readStop = new();
    private Task? _reader;
    private uint _nextId;

    private NuvexaClient(TcpClient tcp, Stream stream, string host, int port, NuvexaClientOptions options)
    {
        _tcp = tcp;
        _stream = stream;
        _host = host;
        _port = port;
        _options = options;
    }

    public static async Task<NuvexaClient> ConnectAsync(string host, int port, NuvexaClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new NuvexaClientOptions();
        var tcp = new TcpClient { NoDelay = true };
        await tcp.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
        Stream stream = tcp.GetStream();
        if (options.UseTls)
        {
            var ssl = new SslStream(stream, leaveInnerStreamOpen: false, options.CertificateValidation);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = host,
                RemoteCertificateValidationCallback = options.CertificateValidation
            }, cancellationToken).ConfigureAwait(false);
            stream = ssl;
        }

        var client = new NuvexaClient(tcp, stream, host, port, options);
        client.StartReader();
        await client.RequestAsync(Op.Hello, Payloads.Hello(options.Token ?? "", options.ClientName, options.User ?? "", options.Password ?? "", options.Vhost), Op.HelloOk, cancellationToken).ConfigureAwait(false);
        return client;
    }

    public Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default)
    {
        var payload = Payloads.EnsureStream(
            spec.Name,
            spec.Filters.ToArray(),
            spec.PartitionCount,
            spec.MaxAge is TimeSpan age ? (long)age.TotalMilliseconds : -1,
            spec.MaxBytes ?? -1,
            spec.MaxMessageBytes ?? 0);
        return RequestAsync(Op.EnsureStream, payload, Op.EnsureStreamOk, cancellationToken);
    }

    public async Task<IReadOnlyList<PublishReceipt>> PublishAsync(string subject, ReadOnlyMemory<byte> payload, string? key = null, IReadOnlyDictionary<string, byte[]>? headers = null, CancellationToken cancellationToken = default)
    {
        var wireHeaders = headers is null
            ? []
            : headers.Select(header => new HeaderWire(header.Key, header.Value)).ToArray();
        var response = await RequestAsync(Op.Publish, Payloads.Publish(subject, key, wireHeaders, payload.Span), Op.PublishOk, cancellationToken).ConfigureAwait(false);
        return Payloads.ReadPublishOk(response).Select(receipt => new PublishReceipt(receipt.Stream, receipt.Partition, receipt.Offset)).ToArray();
    }

    public Task<IReadOnlyList<PublishReceipt>> PublishAsync(string subject, string payload, string? key = null, IReadOnlyDictionary<string, byte[]>? headers = null, CancellationToken cancellationToken = default) =>
        PublishAsync(subject, Encoding.UTF8.GetBytes(payload), key, headers, cancellationToken);

    public Task DeclareExchangeAsync(string name, string type, bool durable = true, bool autoDelete = false, CancellationToken cancellationToken = default) =>
        RequestAsync(Op.DeclareExchange, Payloads.DeclareExchange(name, type, durable, autoDelete), Op.DeclareExchangeOk, cancellationToken);

    public Task DeclareQueueAsync(string name, bool durable = true, bool exclusive = false, bool autoDelete = false, long? messageTtlMs = null, int? maxLength = null, string? deadLetterExchange = null, string? deadLetterRoutingKey = null, CancellationToken cancellationToken = default) =>
        RequestAsync(Op.DeclareQueue, Payloads.DeclareQueue(name, durable, exclusive, autoDelete, messageTtlMs ?? -1, maxLength ?? -1, deadLetterExchange ?? "", deadLetterRoutingKey ?? ""), Op.DeclareQueueOk, cancellationToken);

    public Task BindQueueAsync(string exchange, string queue, string routingKey, IReadOnlyDictionary<string, string>? arguments = null, CancellationToken cancellationToken = default)
    {
        var headers = (arguments ?? new Dictionary<string, string>()).Select(item => new HeaderWire(item.Key, Encoding.UTF8.GetBytes(item.Value))).ToArray();
        return RequestAsync(Op.BindQueue, Payloads.BindQueue(exchange, queue, routingKey, headers), Op.BindQueueOk, cancellationToken);
    }

    public Task DeleteQueueAsync(string name, CancellationToken cancellationToken = default) =>
        RequestAsync(Op.DeleteQueue, Payloads.Named(name), Op.DeleteQueueOk, cancellationToken);

    public Task DeleteExchangeAsync(string name, CancellationToken cancellationToken = default) =>
        RequestAsync(Op.DeleteExchange, Payloads.Named(name), Op.DeleteExchangeOk, cancellationToken);

    public Task PurgeQueueAsync(string name, CancellationToken cancellationToken = default) =>
        RequestAsync(Op.PurgeQueue, Payloads.Named(name), Op.PurgeQueueOk, cancellationToken);

    public async Task<IReadOnlyList<PublishReceipt>> PublishToExchangeAsync(string exchange, string routingKey, ReadOnlyMemory<byte> payload, string? key = null, IReadOnlyDictionary<string, byte[]>? headers = null, CancellationToken cancellationToken = default)
    {
        var wireHeaders = headers?.Select(header => new HeaderWire(header.Key, header.Value)).ToArray() ?? [];
        var response = await RequestAsync(Op.PublishExchange, Payloads.PublishExchange(exchange, routingKey, key, wireHeaders, payload.Span), Op.PublishExchangeOk, cancellationToken).ConfigureAwait(false);
        return Payloads.ReadPublishOk(response).Select(receipt => new PublishReceipt(receipt.Stream, receipt.Partition, receipt.Offset)).ToArray();
    }

    public Task<IReadOnlyList<PublishReceipt>> PublishToExchangeAsync(string exchange, string routingKey, string payload, string? key = null, IReadOnlyDictionary<string, byte[]>? headers = null, CancellationToken cancellationToken = default) =>
        PublishToExchangeAsync(exchange, routingKey, Encoding.UTF8.GetBytes(payload), key, headers, cancellationToken);

    public Task EnsureConsumerAsync(ConsumeSpec spec, CancellationToken cancellationToken = default)
    {
        var start = spec.Start ?? (spec.Ephemeral ? ConsumeStart.Tail : ConsumeStart.First);
        var payload = Payloads.EnsureConsumer(
            spec.Stream,
            spec.Name,
            spec.Filter ?? "",
            (int)spec.AckWait.TotalMilliseconds,
            spec.MaxDeliver,
            spec.MaxAckPending,
            spec.Ephemeral,
            (byte)start,
            spec.StartOffset);
        return RequestAsync(Op.EnsureConsumer, payload, Op.EnsureConsumerOk, cancellationToken);
    }

    public async Task<IReadOnlyList<NuvexaMessage>> FetchAsync(ConsumeSpec spec, int maxMessages, TimeSpan expires, CancellationToken cancellationToken = default)
    {
        await EnsureConsumerAsync(spec, cancellationToken).ConfigureAwait(false);
        var response = await RequestAsync(
            Op.Fetch,
            Payloads.Fetch(spec.Stream, spec.Name, maxMessages, (int)expires.TotalMilliseconds),
            Op.FetchOk,
            cancellationToken).ConfigureAwait(false);
        var fetched = Payloads.ReadFetchOk(response);
        if (fetched.OffsetReset)
            throw new NuvexaMqException(NuvexaMqError.OffsetReset, "The consumer fell behind retention.", fetched.FirstOffset);

        return fetched.Messages.Select(message => new NuvexaMessage(this, spec.Stream, spec.Name, message)).ToArray();
    }

    public async Task<NuvexaConsumer> ConsumeAsync(ConsumeSpec spec, CancellationToken cancellationToken = default)
    {
        var connection = await ConnectAsync(_host, _port, _options, cancellationToken).ConfigureAwait(false);
        await connection.EnsureConsumerAsync(spec, cancellationToken).ConfigureAwait(false);
        return new NuvexaConsumer(connection, spec);
    }

    public async ValueTask DisposeAsync()
    {
        _readStop.Cancel();
        await _stream.DisposeAsync().ConfigureAwait(false);
        _tcp.Dispose();
        if (_reader is not null)
        {
            try
            {
                await _reader.ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException or OperationCanceledException or InvalidDataException or ObjectDisposedException)
            {
            }
        }

        FailWaiters(new ObjectDisposedException(nameof(NuvexaClient)));
        _write.Dispose();
        _slots.Dispose();
        _readStop.Dispose();
    }

    internal async Task AckAsync(string stream, string consumer, int partition, long offset, CancellationToken cancellationToken)
    {
        await RequestAsync(Op.Ack, Payloads.Ack(stream, consumer, partition, offset), Op.AckOk, cancellationToken).ConfigureAwait(false);
    }

    internal async Task NackAsync(string stream, string consumer, int partition, long offset, CancellationToken cancellationToken)
    {
        await RequestAsync(Op.Nack, Payloads.Ack(stream, consumer, partition, offset), Op.NackOk, cancellationToken).ConfigureAwait(false);
    }

    internal async Task ResetAsync(string stream, string consumer, bool absolute, long offset, CancellationToken cancellationToken)
    {
        await RequestAsync(Op.ResetConsumer, Payloads.Reset(stream, consumer, absolute, offset), Op.ResetConsumerOk, cancellationToken).ConfigureAwait(false);
    }

    internal async Task ReleaseAsync(string stream, string consumer, CancellationToken cancellationToken)
    {
        await RequestAsync(Op.ReleaseConsumer, Payloads.Release(stream, consumer), Op.ReleaseConsumerOk, cancellationToken).ConfigureAwait(false);
    }

    private void StartReader() => _reader = ReadLoop();

    private async Task ReadLoop()
    {
        try
        {
            while (!_readStop.IsCancellationRequested)
            {
                var frame = await FrameCodec.ReadAsync(_stream, _readStop.Token).ConfigureAwait(false);
                if (_waiters.TryRemove(frame.RequestId, out var waiter))
                    waiter.TrySetResult(frame);
            }
        }
        catch (Exception ex)
        {
            FailWaiters(ex);
        }
    }

    private void FailWaiters(Exception ex)
    {
        foreach (var entry in _waiters)
        {
            if (_waiters.TryRemove(entry.Key, out var waiter))
                waiter.TrySetException(ex);
        }
    }

    private async Task<byte[]> RequestAsync(ushort type, byte[] payload, ushort expected, CancellationToken cancellationToken)
    {
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var id = Interlocked.Increment(ref _nextId);
            var waiter = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters[id] = waiter;
            try
            {
                await _write.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await FrameCodec.WriteAsync(_stream, new Frame(type, id, payload), cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _write.Release();
                }

                var response = await waiter.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                if (response.Type == Op.Error)
                {
                    var error = Payloads.ReadError(response.Payload);
                    throw new NuvexaMqException(error.Error, error.Message, error.Detail);
                }

                if (response.Type != expected)
                    throw new NuvexaMqException(NuvexaMqError.Invalid, "The broker returned an unexpected frame.");
                return response.Payload;
            }
            finally
            {
                if (_waiters.TryRemove(id, out var abandoned))
                    abandoned.TrySetCanceled();
            }
        }
        finally
        {
            _slots.Release();
        }
    }
}

public sealed class NuvexaMessage
{
    private readonly NuvexaClient _client;
    private readonly string _stream;
    private readonly string _consumer;

    internal NuvexaMessage(NuvexaClient client, string stream, string consumer, DeliveryWire wire)
    {
        _client = client;
        _stream = stream;
        _consumer = consumer;
        Partition = wire.Partition;
        Offset = wire.Offset;
        TimestampUnixMs = wire.TimestampUnixMs;
        DeliveryCount = wire.DeliveryCount;
        Subject = wire.Subject;
        Key = wire.Key;
        Headers = wire.Headers.ToDictionary(header => header.Name, header => header.Value);
        Payload = wire.Payload;
    }

    public int Partition { get; }

    public long Offset { get; }

    public long TimestampUnixMs { get; }

    public int DeliveryCount { get; }

    public string Subject { get; }

    public byte[] Key { get; }

    public IReadOnlyDictionary<string, byte[]> Headers { get; }

    public byte[] Payload { get; }

    public string PayloadText => Encoding.UTF8.GetString(Payload);

    public Task AckAsync(CancellationToken cancellationToken = default) =>
        _client.AckAsync(_stream, _consumer, Partition, Offset, cancellationToken);

    public Task NackAsync(CancellationToken cancellationToken = default) =>
        _client.NackAsync(_stream, _consumer, Partition, Offset, cancellationToken);
}

public sealed class NuvexaConsumer : IAsyncDisposable
{
    private readonly NuvexaClient _client;
    private readonly ConsumeSpec _spec;

    internal NuvexaConsumer(NuvexaClient client, ConsumeSpec spec)
    {
        _client = client;
        _spec = spec;
    }

    public async IAsyncEnumerable<NuvexaMessage> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var batch = await _client.FetchAsync(_spec, 32, TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            foreach (var message in batch)
                yield return message;
        }
    }

    public Task ResetAsync(CancellationToken cancellationToken = default) =>
        _client.ResetAsync(_spec.Stream, _spec.Name, absolute: false, 0, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (_spec.Ephemeral)
        {
            try
            {
                await _client.ReleaseAsync(_spec.Stream, _spec.Name, CancellationToken.None).ConfigureAwait(false);
            }
            catch (IOException)
            {
            }
            catch (NuvexaMqException)
            {
            }
        }

        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
