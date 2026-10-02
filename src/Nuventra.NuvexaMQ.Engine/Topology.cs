using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Nuventra.NuvexaMQ.Engine;

public sealed class NuvexaTopology
{
    public const string DefaultVhost = "/";

    private readonly object _gate = new();
    private readonly NuvexaBroker _broker;
    private readonly string _path;
    private TopologyFile _file;

    private NuvexaTopology(NuvexaBroker broker, string dataDir, string administrator, string password)
    {
        _broker = broker;
        _path = Path.Combine(dataDir, "topology.json");
        if (File.Exists(_path))
        {
            _file = JsonSerializer.Deserialize(File.ReadAllText(_path), EngineJsonContext.Default.TopologyFile) ?? new TopologyFile();
        }
        else
        {
            _file = new TopologyFile();
            _file.Vhosts.Add(new VhostRecord { Name = DefaultVhost });
            _file.Users.Add(new UserRecord
            {
                Name = administrator,
                PasswordHash = PasswordHasher.Hash(password),
                Tags = ["administrator"]
            });
            _file.Permissions.Add(new PermissionRecord
            {
                User = administrator,
                Vhost = DefaultVhost,
                Configure = ".*",
                Write = ".*",
                Read = ".*"
            });
            AddBuiltins(DefaultVhost);
            Save();
        }

        _broker.OnDeadLetter = OnDeadLetterAsync;
    }

    public static NuvexaTopology Open(NuvexaBroker broker, string dataDir, string administrator = "guest", string password = "guest") =>
        new(broker, dataDir, string.IsNullOrWhiteSpace(administrator) ? "guest" : administrator, string.IsNullOrEmpty(password) ? "guest" : password);

    public bool TryLogin(string username, string password, IPAddress? remote, bool management, out string reason)
    {
        if (string.Equals(username, "guest", StringComparison.Ordinal) && !IsLoopback(remote))
        {
            reason = "User 'guest' can only log in via localhost";
            return false;
        }

        UserRecord? user;
        lock (_gate)
            user = _file.Users.FirstOrDefault(item => item.Name == username);
        if (user is null || !PasswordHasher.Verify(password, user.PasswordHash))
        {
            reason = "Login failed";
            return false;
        }

        if (management && !CanManage(user))
        {
            reason = "Not management user";
            return false;
        }

        reason = "";
        return true;
    }

    public bool Allowed(string user, string vhost, string permission, string resource)
    {
        string? pattern;
        lock (_gate)
        {
            var grant = _file.Permissions.FirstOrDefault(item => item.User == user && item.Vhost == vhost);
            pattern = grant is null
                ? null
                : permission switch
                {
                    "configure" => grant.Configure,
                    "write" => grant.Write,
                    "read" => grant.Read,
                    _ => null
                };
        }

        if (string.IsNullOrEmpty(pattern))
            return false;
        try
        {
            return Regex.IsMatch(resource, pattern, RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(50));
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    public bool VhostExists(string name)
    {
        lock (_gate)
            return _file.Vhosts.Any(item => item.Name == name);
    }

    public IReadOnlyList<string> Tags(string user)
    {
        lock (_gate)
            return _file.Users.FirstOrDefault(item => item.Name == user)?.Tags.ToArray() ?? [];
    }

    public void DeclareExchange(string vhost, string name, string type, bool durable = true, bool autoDelete = false)
    {
        RequireVhost(vhost);
        type = NormalizeType(type);
        if (name.Length == 0 || IsBuiltin(name))
        {
            var expected = name.Length == 0 || name == "amq.direct" ? "direct" : name == "amq.fanout" ? "fanout" : name == "amq.topic" ? "topic" : "headers";
            if (type != expected)
                throw new NuvexaMqException(NuvexaMqError.Conflict, $"Exchange '{DisplayExchange(name)}' already exists as {expected}.");
            return;
        }

        ValidateResource(name, "Exchange");
        lock (_gate)
        {
            var existing = FindExchange(vhost, name);
            if (existing is not null)
            {
                if (existing.Type != type)
                    throw new NuvexaMqException(NuvexaMqError.Conflict, $"Exchange '{name}' already exists as {existing.Type}.");
                return;
            }

            _file.Exchanges.Add(new ExchangeRecord
            {
                Vhost = vhost,
                Name = name,
                Type = type,
                Durable = durable,
                AutoDelete = autoDelete
            });
            Save();
        }
    }

    public async Task DeclareQueueAsync(string vhost, string name, QueueOptions options, string? connectionId, CancellationToken cancellationToken = default)
    {
        RequireVhost(vhost);
        ValidateResource(name, "Queue");
        var stream = StreamName(vhost, name);
        await _broker.EnsureStreamAsync(new StreamSpec(stream, [">"]) { Queue = true }, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            var existing = FindQueue(vhost, name);
            if (existing is null)
            {
                _file.Queues.Add(new QueueRecord
                {
                    Vhost = vhost,
                    Name = name,
                    Durable = options.Durable,
                    Exclusive = options.Exclusive,
                    AutoDelete = options.AutoDelete,
                    MessageTtlMs = options.MessageTtlMs,
                    MaxLength = options.MaxLength,
                    DeadLetterExchange = options.DeadLetterExchange ?? "",
                    DeadLetterRoutingKey = options.DeadLetterRoutingKey ?? "",
                    ConnectionId = options.Exclusive ? connectionId ?? "" : ""
                });
            }
            else
            {
                existing.MessageTtlMs = options.MessageTtlMs;
                existing.MaxLength = options.MaxLength;
                existing.DeadLetterExchange = options.DeadLetterExchange ?? existing.DeadLetterExchange;
                existing.DeadLetterRoutingKey = options.DeadLetterRoutingKey ?? existing.DeadLetterRoutingKey;
            }

            Save();
        }

        ApplyQueue(vhost, name);
    }

    public void Bind(string vhost, string exchange, string queue, string routingKey, IReadOnlyDictionary<string, string>? arguments)
    {
        RequireVhost(vhost);
        if (exchange.Length == 0)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "The default exchange delivers to the queue named by the routing key.");
        lock (_gate)
        {
            if (FindQueue(vhost, queue) is null)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Queue '{queue}' was not found.");
            if (!IsBuiltin(exchange) && FindExchange(vhost, exchange) is null)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Exchange '{exchange}' was not found.");
            if (_file.Bindings.Any(item => item.Vhost == vhost && item.Exchange == exchange && item.Queue == queue && item.RoutingKey == routingKey))
                return;
            _file.Bindings.Add(new BindingRecord
            {
                Vhost = vhost,
                Exchange = exchange,
                Queue = queue,
                RoutingKey = routingKey,
                Arguments = arguments is null ? new Dictionary<string, string>() : new Dictionary<string, string>(arguments)
            });
            Save();
        }
    }

    public async Task DeleteManagedStreamAsync(string name, CancellationToken cancellationToken = default)
    {
        if (name == NuvexaBroker.DeadLetterStream)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "The dead-letter stream cannot be deleted.");
        QueueRecord? queue;
        lock (_gate)
            queue = _file.Queues.FirstOrDefault(item => StreamName(item.Vhost, item.Name) == name);
        if (queue is not null)
        {
            await DeleteQueueAsync(queue.Vhost, queue.Name, cancellationToken).ConfigureAwait(false);
            return;
        }

        await _broker.DeleteStreamAsync(name, cancellationToken).ConfigureAwait(false);
    }

    public void Unbind(string vhost, string exchange, string queue, string routingKey)
    {
        lock (_gate)
        {
            _file.Bindings.RemoveAll(item => item.Vhost == vhost && item.Exchange == exchange && item.Queue == queue && item.RoutingKey == routingKey);
            Save();
        }
    }

    public async Task DeleteQueueAsync(string vhost, string name, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_file.Queues.RemoveAll(item => item.Vhost == vhost && item.Name == name) == 0)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Queue '{name}' was not found.");
            _file.Bindings.RemoveAll(item => item.Vhost == vhost && item.Queue == name);
            Save();
        }

        await _broker.DeleteStreamAsync(StreamName(vhost, name), cancellationToken).ConfigureAwait(false);
    }

    public void DeleteExchange(string vhost, string name)
    {
        if (IsBuiltin(name) || name.Length == 0)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "A built-in exchange cannot be deleted.");
        lock (_gate)
        {
            if (_file.Exchanges.RemoveAll(item => item.Vhost == vhost && item.Name == name) == 0)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Exchange '{name}' was not found.");
            _file.Bindings.RemoveAll(item => item.Vhost == vhost && item.Exchange == name);
            Save();
        }
    }

    public void Purge(string vhost, string queue)
    {
        var stream = StreamName(vhost, queue);
        var depth = _broker.QueueDepth(stream);
        var snapshot = _broker.Snapshot().Partitions.Where(item => item.Stream == stream).ToArray();
        var floor = snapshot.Length == 0 ? 0 : snapshot.Max(item => item.CommittedOffset) + 1;
        if (depth > 0 || floor > 0)
            _broker.ConfigureQueueStream(stream, floor, Effective(vhost, queue).MessageTtlMs);
    }

    public async Task<IReadOnlyList<PublishReceipt>> PublishAsync(string vhost, string exchange, string routingKey, ReadOnlyMemory<byte> payload, string? key, IReadOnlyList<MessageHeader>? headers, CancellationToken cancellationToken = default)
    {
        var targets = Destinations(vhost, exchange, routingKey, headers);
        if (targets.Count == 0)
            throw new NuvexaMqException(NuvexaMqError.NotFound, $"No queue is bound for '{routingKey}' on exchange '{DisplayExchange(exchange)}'.");

        var receipts = new List<PublishReceipt>(targets.Count);
        foreach (var queue in targets)
        {
            var stream = StreamName(vhost, queue);
            var settings = Effective(vhost, queue);
            var receipt = await _broker.AppendStreamAsync(stream, string.IsNullOrEmpty(routingKey) ? queue : routingKey, payload, key, headers, cancellationToken).ConfigureAwait(false);
            receipts.Add(new PublishReceipt(queue, receipt.Partition, receipt.Offset));
            if (settings.MaxLength is int max && max >= 0)
            {
                var depth = _broker.QueueDepth(stream);
                if (depth > max)
                {
                    var drop = depth - max;
                    var floor = receipt.Offset - max + 1;
                    if (drop > 0 && floor > 0)
                        _broker.ConfigureQueueStream(stream, floor, settings.MessageTtlMs);
                }
            }
        }

        return receipts;
    }

    public void UpsertUser(string name, string? password, IReadOnlyList<string> tags)
    {
        ValidateResource(name, "User");
        lock (_gate)
        {
            var existing = _file.Users.FirstOrDefault(item => item.Name == name);
            if (existing is null)
            {
                if (string.IsNullOrEmpty(password))
                    throw new NuvexaMqException(NuvexaMqError.Invalid, "A new user needs a password.");
                _file.Users.Add(new UserRecord { Name = name, PasswordHash = PasswordHasher.Hash(password), Tags = tags.ToList() });
            }
            else
            {
                if (!string.IsNullOrEmpty(password))
                    existing.PasswordHash = PasswordHasher.Hash(password);
                existing.Tags = tags.ToList();
            }

            Save();
        }
    }

    public void DeleteUser(string name)
    {
        lock (_gate)
        {
            if (_file.Users.RemoveAll(item => item.Name == name) == 0)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"User '{name}' was not found.");
            _file.Permissions.RemoveAll(item => item.User == name);
            Save();
        }
    }

    public void SetPermission(string user, string vhost, string configure, string write, string read)
    {
        RequireVhost(vhost);
        lock (_gate)
        {
            if (_file.Users.All(item => item.Name != user))
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"User '{user}' was not found.");
            _file.Permissions.RemoveAll(item => item.User == user && item.Vhost == vhost);
            _file.Permissions.Add(new PermissionRecord { User = user, Vhost = vhost, Configure = configure, Write = write, Read = read });
            Save();
        }
    }

    public void ClearPermission(string user, string vhost)
    {
        lock (_gate)
        {
            if (_file.Permissions.RemoveAll(item => item.User == user && item.Vhost == vhost) == 0)
                throw new NuvexaMqException(NuvexaMqError.NotFound, "That permission was not found.");
            Save();
        }
    }

    public void AddVhost(string name)
    {
        if (name != DefaultVhost)
            ValidateResource(name, "Virtual host");
        lock (_gate)
        {
            if (_file.Vhosts.Any(item => item.Name == name))
                return;
            _file.Vhosts.Add(new VhostRecord { Name = name });
            AddBuiltins(name);
            Save();
        }
    }

    public void DeleteVhost(string name)
    {
        if (name == DefaultVhost)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "The default virtual host cannot be deleted.");
        lock (_gate)
        {
            if (_file.Vhosts.RemoveAll(item => item.Name == name) == 0)
                throw new NuvexaMqException(NuvexaMqError.NotFound, $"Virtual host '{name}' was not found.");
            _file.Exchanges.RemoveAll(item => item.Vhost == name);
            _file.Queues.RemoveAll(item => item.Vhost == name);
            _file.Bindings.RemoveAll(item => item.Vhost == name);
            _file.Permissions.RemoveAll(item => item.Vhost == name);
            _file.Policies.RemoveAll(item => item.Vhost == name);
            Save();
        }
    }

    public void UpsertPolicy(string vhost, string name, string pattern, int priority, long? messageTtlMs, int? maxLength, string? deadLetterExchange, string? deadLetterRoutingKey)
    {
        RequireVhost(vhost);
        ValidateResource(name, "Policy");
        lock (_gate)
        {
            _file.Policies.RemoveAll(item => item.Vhost == vhost && item.Name == name);
            _file.Policies.Add(new PolicyRecord
            {
                Vhost = vhost,
                Name = name,
                Pattern = pattern,
                Priority = priority,
                MessageTtlMs = messageTtlMs,
                MaxLength = maxLength,
                DeadLetterExchange = deadLetterExchange ?? "",
                DeadLetterRoutingKey = deadLetterRoutingKey ?? ""
            });
            Save();
        }

        foreach (var queue in ListQueues(vhost))
            ApplyQueue(vhost, queue.Name);
    }

    public void DeletePolicy(string vhost, string name)
    {
        lock (_gate)
        {
            _file.Policies.RemoveAll(item => item.Vhost == vhost && item.Name == name);
            Save();
        }

        foreach (var queue in ListQueues(vhost))
            ApplyQueue(vhost, queue.Name);
    }

    public async Task CloseConnectionAsync(string connectionId, CancellationToken cancellationToken = default)
    {
        List<QueueRecord> exclusive;
        lock (_gate)
            exclusive = _file.Queues.Where(item => item.Exclusive && item.ConnectionId == connectionId).ToList();
        foreach (var queue in exclusive)
            await DeleteQueueAsync(queue.Vhost, queue.Name, cancellationToken).ConfigureAwait(false);
    }

    public IReadOnlyList<ExchangeRecord> ListExchanges(string? vhost = null)
    {
        lock (_gate)
        {
            var defaults = _file.Vhosts
                .Where(item => vhost is null || item.Name == vhost)
                .Select(item => new ExchangeRecord { Vhost = item.Name, Name = "", Type = "direct", Durable = true, Builtin = true });
            return _file.Exchanges.Where(item => vhost is null || item.Vhost == vhost).Concat(defaults).ToArray();
        }
    }

    public IReadOnlyList<QueueRecord> ListQueues(string? vhost = null)
    {
        lock (_gate)
            return _file.Queues.Where(item => vhost is null || item.Vhost == vhost).Select(Clone).ToArray();
    }

    public IReadOnlyList<BindingRecord> ListBindings(string? vhost = null)
    {
        lock (_gate)
            return _file.Bindings.Where(item => vhost is null || item.Vhost == vhost).ToArray();
    }

    public IReadOnlyList<UserRecord> ListUsers()
    {
        lock (_gate)
            return _file.Users.Select(item => new UserRecord { Name = item.Name, Tags = item.Tags.ToList(), PasswordHash = "" }).ToArray();
    }

    public IReadOnlyList<PermissionRecord> ListPermissions()
    {
        lock (_gate)
            return _file.Permissions.ToArray();
    }

    public IReadOnlyList<VhostRecord> ListVhosts()
    {
        lock (_gate)
            return _file.Vhosts.ToArray();
    }

    public IReadOnlyList<PolicyRecord> ListPolicies(string? vhost = null)
    {
        lock (_gate)
            return _file.Policies.Where(item => vhost is null || item.Vhost == vhost).ToArray();
    }

    public static bool TopicMatches(string pattern, string routingKey) => TopicMatch(pattern.Split('.'), 0, routingKey.Length == 0 ? [] : routingKey.Split('.'), 0);

    public static string StreamName(string vhost, string queue) =>
        vhost == DefaultVhost ? "$queue." + queue : "$queue." + vhost + "." + queue;

    private async Task<bool> OnDeadLetterAsync(string streamName, StoredRecord source, CancellationToken cancellationToken)
    {
        QueueRecord? queue;
        lock (_gate)
            queue = _file.Queues.FirstOrDefault(item => StreamName(item.Vhost, item.Name) == streamName);
        if (queue is null)
            return false;
        var settings = Effective(queue.Vhost, queue.Name);
        if (string.IsNullOrEmpty(settings.DeadLetterExchange))
            return false;
        var deaths = source.Headers.FirstOrDefault(header => header.Name == "Nuvexa-Deaths").Value;
        var count = deaths is null ? 0 : int.TryParse(Encoding.UTF8.GetString(deaths), out var parsed) ? parsed : 0;
        if (count >= 8)
            return false;
        var headers = source.Headers.Where(header => header.Name != "Nuvexa-Deaths").Append(new MessageHeader("Nuvexa-Deaths", Encoding.UTF8.GetBytes((count + 1).ToString()))).ToArray();
        var routingKey = string.IsNullOrEmpty(settings.DeadLetterRoutingKey) ? source.Subject : settings.DeadLetterRoutingKey;
        var targets = Destinations(queue.Vhost, settings.DeadLetterExchange, routingKey, headers);
        if (targets.Count == 0)
            return false;
        foreach (var target in targets)
            await _broker.AppendStreamAsync(StreamName(queue.Vhost, target), routingKey, source.Payload, null, headers, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private List<string> Destinations(string vhost, string exchange, string routingKey, IReadOnlyList<MessageHeader>? headers)
    {
        if (exchange.Length == 0)
        {
            lock (_gate)
                return FindQueue(vhost, routingKey) is null ? [] : [routingKey];
        }

        List<BindingRecord> bindings;
        string type;
        lock (_gate)
        {
            type = ExchangeType(vhost, exchange);
            bindings = _file.Bindings.Where(item => item.Vhost == vhost && item.Exchange == exchange).ToList();
        }

        var matched = new List<string>();
        foreach (var binding in bindings)
        {
            var hit = type switch
            {
                "fanout" => true,
                "topic" => TopicMatches(binding.RoutingKey, routingKey),
                "headers" => HeadersMatch(binding, headers),
                _ => binding.RoutingKey == routingKey
            };
            if (hit && !matched.Contains(binding.Queue))
                matched.Add(binding.Queue);
        }

        return matched;
    }

    private static bool HeadersMatch(BindingRecord binding, IReadOnlyList<MessageHeader>? headers)
    {
        var all = !binding.Arguments.TryGetValue("x-match", out var mode) || !string.Equals(mode, "any", StringComparison.OrdinalIgnoreCase);
        var required = binding.Arguments.Where(item => item.Key != "x-match").ToArray();
        if (required.Length == 0)
            return false;
        var hits = required.Count(pair => headers?.Any(header => header.Name == pair.Key && Encoding.UTF8.GetString(header.Value) == pair.Value) == true);
        return all ? hits == required.Length : hits > 0;
    }

    private QueueSettings Effective(string vhost, string queue)
    {
        lock (_gate)
        {
            var record = FindQueue(vhost, queue) ?? throw new NuvexaMqException(NuvexaMqError.NotFound, $"Queue '{queue}' was not found.");
            var settings = new QueueSettings(record.MessageTtlMs, record.MaxLength, record.DeadLetterExchange, record.DeadLetterRoutingKey);
            PolicyRecord? winner = null;
            foreach (var policy in _file.Policies.Where(item => item.Vhost == vhost))
            {
                bool matched;
                try
                {
                    matched = Regex.IsMatch(queue, policy.Pattern, RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(50));
                }
                catch (ArgumentException)
                {
                    continue;
                }

                if (!matched)
                    continue;
                if (winner is null || policy.Priority >= winner.Priority)
                    winner = policy;
            }

            if (winner is null)
                return settings;
            return new QueueSettings(
                winner.MessageTtlMs ?? settings.MessageTtlMs,
                winner.MaxLength ?? settings.MaxLength,
                string.IsNullOrEmpty(winner.DeadLetterExchange) ? settings.DeadLetterExchange : winner.DeadLetterExchange,
                string.IsNullOrEmpty(winner.DeadLetterRoutingKey) ? settings.DeadLetterRoutingKey : winner.DeadLetterRoutingKey);
        }
    }

    private void ApplyQueue(string vhost, string name)
    {
        var settings = Effective(vhost, name);
        _broker.ConfigureQueueStream(StreamName(vhost, name), 0, settings.MessageTtlMs);
    }

    private string ExchangeType(string vhost, string name)
    {
        if (name.Length == 0 || name == "amq.direct")
            return "direct";
        if (name == "amq.fanout")
            return "fanout";
        if (name == "amq.topic")
            return "topic";
        if (name == "amq.headers")
            return "headers";
        return FindExchange(vhost, name)?.Type ?? throw new NuvexaMqException(NuvexaMqError.NotFound, $"Exchange '{name}' was not found.");
    }

    private void AddBuiltins(string vhost)
    {
        foreach (var exchange in Builtins(vhost))
        {
            if (_file.Exchanges.Any(item => item.Vhost == vhost && item.Name == exchange.Name))
                continue;
            if (exchange.Name.Length == 0)
                continue;
            _file.Exchanges.Add(exchange);
        }
    }

    private static IEnumerable<ExchangeRecord> Builtins(string vhost)
    {
        yield return new ExchangeRecord { Vhost = vhost, Name = "", Type = "direct", Durable = true, Builtin = true };
        yield return new ExchangeRecord { Vhost = vhost, Name = "amq.direct", Type = "direct", Durable = true, Builtin = true };
        yield return new ExchangeRecord { Vhost = vhost, Name = "amq.fanout", Type = "fanout", Durable = true, Builtin = true };
        yield return new ExchangeRecord { Vhost = vhost, Name = "amq.topic", Type = "topic", Durable = true, Builtin = true };
        yield return new ExchangeRecord { Vhost = vhost, Name = "amq.headers", Type = "headers", Durable = true, Builtin = true };
    }

    private ExchangeRecord? FindExchange(string vhost, string name) =>
        _file.Exchanges.FirstOrDefault(item => item.Vhost == vhost && item.Name == name);

    private QueueRecord? FindQueue(string vhost, string name) =>
        _file.Queues.FirstOrDefault(item => item.Vhost == vhost && item.Name == name);

    private static QueueRecord Clone(QueueRecord item) => new()
    {
        Vhost = item.Vhost,
        Name = item.Name,
        Durable = item.Durable,
        Exclusive = item.Exclusive,
        AutoDelete = item.AutoDelete,
        MessageTtlMs = item.MessageTtlMs,
        MaxLength = item.MaxLength,
        DeadLetterExchange = item.DeadLetterExchange,
        DeadLetterRoutingKey = item.DeadLetterRoutingKey,
        ConnectionId = item.ConnectionId
    };

    private void RequireVhost(string name)
    {
        if (!VhostExists(name))
            throw new NuvexaMqException(NuvexaMqError.NotFound, $"Virtual host '{name}' was not found.");
    }

    private static void ValidateResource(string name, string kind)
    {
        if (name.Length is < 1 or > 255 || name.Any(c => !IsResourceChar(c)))
            throw new NuvexaMqException(NuvexaMqError.Invalid, $"{kind} name '{name}' is not valid.");
    }

    private static bool IsResourceChar(char c) => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-' or ':' or '/';

    private static string NormalizeType(string type) => type.ToLowerInvariant() switch
    {
        "direct" or "fanout" or "topic" or "headers" => type.ToLowerInvariant(),
        _ => throw new NuvexaMqException(NuvexaMqError.Invalid, "Exchange type must be direct, fanout, topic, or headers.")
    };

    private static bool IsBuiltin(string name) => name is "amq.direct" or "amq.fanout" or "amq.topic" or "amq.headers";

    private static string DisplayExchange(string name) => name.Length == 0 ? "(AMQP default)" : name;

    private static bool CanManage(UserRecord user) =>
        user.Tags.Any(tag => tag is "administrator" or "management");

    private static bool IsLoopback(IPAddress? address)
    {
        if (address is null)
            return false;
        if (address.IsIPv4MappedToIPv6)
            address = address.MapToIPv4();
        return IPAddress.IsLoopback(address);
    }

    private static bool TopicMatch(string[] pattern, int patternIndex, string[] key, int keyIndex)
    {
        if (patternIndex == pattern.Length)
            return keyIndex == key.Length;
        if (pattern[patternIndex] == "#")
        {
            for (var index = keyIndex; index <= key.Length; index++)
            {
                if (TopicMatch(pattern, patternIndex + 1, key, index))
                    return true;
            }

            return false;
        }

        if (keyIndex >= key.Length)
            return false;
        if (pattern[patternIndex] != "*" && pattern[patternIndex] != key[keyIndex])
            return false;
        return TopicMatch(pattern, patternIndex + 1, key, keyIndex + 1);
    }

    private void Save() => AtomicFile.Write(_path, JsonSerializer.Serialize(_file, EngineJsonContext.Default.TopologyFile));
}

public sealed class QueueOptions
{
    public bool Durable { get; init; } = true;

    public bool Exclusive { get; init; }

    public bool AutoDelete { get; init; }

    public long? MessageTtlMs { get; init; }

    public int? MaxLength { get; init; }

    public string? DeadLetterExchange { get; init; }

    public string? DeadLetterRoutingKey { get; init; }
}

public readonly record struct QueueSettings(long? MessageTtlMs, int? MaxLength, string DeadLetterExchange, string DeadLetterRoutingKey);

internal static class PasswordHasher
{
    public static string Hash(string password)
    {
        var salt = RandomNumberGenerator.GetBytes(16);
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, 10_000, HashAlgorithmName.SHA256, 32);
        return Convert.ToBase64String(salt) + "." + Convert.ToBase64String(hash);
    }

    public static bool Verify(string password, string stored)
    {
        var split = stored.IndexOf('.');
        if (split <= 0)
            return false;
        var salt = Convert.FromBase64String(stored[..split]);
        var expected = Convert.FromBase64String(stored[(split + 1)..]);
        var actual = Rfc2898DeriveBytes.Pbkdf2(password, salt, 10_000, HashAlgorithmName.SHA256, 32);
        return expected.Length == actual.Length && CryptographicOperations.FixedTimeEquals(expected, actual);
    }
}

internal sealed class TopologyFile
{
    public List<VhostRecord> Vhosts { get; set; } = [];

    public List<UserRecord> Users { get; set; } = [];

    public List<PermissionRecord> Permissions { get; set; } = [];

    public List<ExchangeRecord> Exchanges { get; set; } = [];

    public List<QueueRecord> Queues { get; set; } = [];

    public List<BindingRecord> Bindings { get; set; } = [];

    public List<PolicyRecord> Policies { get; set; } = [];
}

public sealed class VhostRecord
{
    public string Name { get; set; } = "";
}

public sealed class UserRecord
{
    public string Name { get; set; } = "";

    public string PasswordHash { get; set; } = "";

    public List<string> Tags { get; set; } = [];
}

public sealed class PermissionRecord
{
    public string User { get; set; } = "";

    public string Vhost { get; set; } = "/";

    public string Configure { get; set; } = "";

    public string Write { get; set; } = "";

    public string Read { get; set; } = "";
}

public sealed class ExchangeRecord
{
    public string Vhost { get; set; } = "/";

    public string Name { get; set; } = "";

    public string Type { get; set; } = "direct";

    public bool Durable { get; set; } = true;

    public bool AutoDelete { get; set; }

    public bool Builtin { get; set; }
}

public sealed class QueueRecord
{
    public string Vhost { get; set; } = "/";

    public string Name { get; set; } = "";

    public bool Durable { get; set; } = true;

    public bool Exclusive { get; set; }

    public bool AutoDelete { get; set; }

    public long? MessageTtlMs { get; set; }

    public int? MaxLength { get; set; }

    public string DeadLetterExchange { get; set; } = "";

    public string DeadLetterRoutingKey { get; set; } = "";

    public string ConnectionId { get; set; } = "";
}

public sealed class BindingRecord
{
    public string Vhost { get; set; } = "/";

    public string Exchange { get; set; } = "";

    public string Queue { get; set; } = "";

    public string RoutingKey { get; set; } = "";

    public Dictionary<string, string> Arguments { get; set; } = [];
}

public sealed class PolicyRecord
{
    public string Vhost { get; set; } = "/";

    public string Name { get; set; } = "";

    public string Pattern { get; set; } = ".*";

    public int Priority { get; set; }

    public long? MessageTtlMs { get; set; }

    public int? MaxLength { get; set; }

    public string DeadLetterExchange { get; set; } = "";

    public string DeadLetterRoutingKey { get; set; } = "";
}
