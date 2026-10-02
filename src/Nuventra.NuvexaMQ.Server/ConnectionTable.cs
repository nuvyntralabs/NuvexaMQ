using System.Net.Sockets;

namespace Nuventra.NuvexaMQ.Server;

internal sealed class ConnectionTable
{
    private readonly object _gate = new();
    private readonly Dictionary<TcpClient, ConnectionRow> _rows = [];
    private long _next;

    public void Add(TcpClient client)
    {
        var id = Interlocked.Increment(ref _next);
        var row = new ConnectionRow
        {
            Id = id.ToString(),
            Peer = client.Client.RemoteEndPoint?.ToString() ?? "",
            ConnectedAt = DateTimeOffset.UtcNow,
            State = "starting"
        };
        lock (_gate)
            _rows[client] = row;
    }

    public void Open(TcpClient client, string clientName)
    {
        lock (_gate)
        {
            if (_rows.TryGetValue(client, out var row))
            {
                row.ClientName = string.IsNullOrWhiteSpace(clientName) ? "nuvexamq" : clientName;
                row.State = "running";
            }
        }
    }

    public void Identify(TcpClient client, string user, string vhost)
    {
        lock (_gate)
        {
            if (_rows.TryGetValue(client, out var row))
            {
                row.User = user;
                row.Vhost = vhost;
            }
        }
    }

    public string? IdOf(TcpClient client)
    {
        lock (_gate)
            return _rows.TryGetValue(client, out var row) ? row.Id : null;
    }

    public void Remove(TcpClient client)
    {
        lock (_gate)
            _rows.Remove(client);
    }

    public IReadOnlyList<ConnectionRow> Snapshot()
    {
        lock (_gate)
            return _rows.Values.OrderBy(row => row.Id, StringComparer.Ordinal).ToArray();
    }
}

internal sealed class ConnectionRow
{
    public string Id { get; init; } = "";

    public string ClientName { get; set; } = "";

    public string Peer { get; init; } = "";

    public DateTimeOffset ConnectedAt { get; init; }

    public string State { get; set; } = "";

    public string User { get; set; } = "";

    public string Vhost { get; set; } = "/";
}
