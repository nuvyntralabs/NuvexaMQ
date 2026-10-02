# NuvexaMQ technical reference

NuvexaMQ 0.1.1 is a single-node durable message broker. One append-only log stores each message. A stream, a queue, and an exchange binding are three ways to write or read that log.

The wire protocol is NuvexaMQ's own length-prefixed TCP protocol. It is not AMQP, MQTT, or Kafka. RabbitMQ is the model for virtual hosts, users, permissions, exchanges, queues, bindings, and policies. The bytes on the socket are different.

This version does not fail over to another machine. An acknowledged publish is on disk. Segment headers and `commit.json` store an epoch and a committed offset so a later follower can use the same files. The epoch on this node is 1.

## Listeners

The process opens four listeners. A package install and the desktop app both use these defaults.

| Port | Role | Authentication |
| --- | --- | --- |
| 5761 | Data. Clients publish, fetch, and ack here. | Hello frame: optional cluster token, then user, password, and virtual host |
| 5762 | Health and metrics. `GET /health` and `GET /metrics` | None |
| 5763 | Management console and HTTP API | Cookie `nuvexamq_mgmt` or `Authorization: Basic` |
| 5764 | Same console and API over HTTPS | Same as 5763 |

`GET /health` returns `{"status":"ok"}`. `GET /metrics` returns Prometheus text.

The data port binds to `0.0.0.0` unless `ListenAddress` is changed. The banner prints `tcp://127.0.0.1:5761` for a wildcard bind. With `CertificatePath` set, that banner is `tls://` and the same frames are wrapped in TLS. Without a certificate, HTTPS on 5764 still starts with a generated certificate, and browsers warn until you replace it.

`guest` / `guest` is the seeded management user. That account is accepted only from localhost. A 401 body is JSON `{ "error", "reason" }` and does not include a `WWW-Authenticate` header.

## What you install

| System | Package | Service | Data directory |
| --- | --- | --- | --- |
| Windows | `NuvexaMQ-<version>-win-x64.msi` or `win-arm64.msi` | Windows service `NuvexaMQ` | `C:\ProgramData\NuvexaMQ\data` |
| macOS | `NuvexaMQ-<version>-osx-arm64.pkg` or `osx-x64.pkg` | launchd `com.nuventra.nuvexamq` | `/Library/Application Support/NuvexaMQ/data` |
| Linux | `nuvexamq_<version>_amd64.deb` or `_arm64.deb`, and the matching `.rpm` | systemd `nuvexamq` | `/var/lib/nuvexamq` |

The package installs the broker and the desktop app, puts `nuvexamq` on `PATH`, and starts the broker on boot. CI keeps the packages as workflow artifacts for 14 days. The tag `v<Version>` publishes them on the GitHub Release for that version. CI does not push them to NuGet.

The desktop window uses a different data directory: the user application-data folder `NuvexaMQ/data`. On macOS that is `~/Library/Application Support/NuvexaMQ/data`. Starting the desktop while the system service already owns 5761–5764 fails with "Address already in use". The window then shows **In use**, and **Stop** unloads the system service (macOS asks for an administrator password because that service runs as root and restarts if it is only killed). **Open admin portal** opens `http://127.0.0.1:5763/`.

## Process configuration

`nuvexamq` reads `appsettings.json` next to the binary, section `NuvexaMQ`, then applies flags.

| Flag | Setting | Default |
| --- | --- | --- |
| `--data` | `DataDir` | `./data` for a manual start. The package overrides this. |
| `--port` | `ListenPort` | 5761 |
| `--health-port` | `HealthPort` | 5762 |
| `--management-port` | `ManagementPort` | 5763 |
| `--management-https-port` | `ManagementHttpsPort` | 5764 |
| `--token` | `Token` | empty. An empty token accepts any hello token. |
| `--cert` / `--cert-key` | `CertificatePath` / `KeyPath` | PEM certificate and key for the data port and for HTTPS |
| `--flush-ms` | `FlushIntervalMs` | 10 |
| `--management-user` / `--management-password` | seeded administrator, only when `topology.json` does not exist yet | `guest` / `guest` |
| `-v`, `--verbose` | connection and publish lines | off |

Other settings exist only in `appsettings.json`:

| Setting | Default | Meaning |
| --- | --- | --- |
| `FlushMaxRecords` | 256 | Group-commit flushes when this many records are waiting or when the flush interval elapses |
| `MaxMessageBytes` | 1048576 | Broker-wide maximum payload. A stream can set a lower limit. |
| `MaxConnections` | 10000 | Extra connections are closed |
| `SegmentBytes` | 67108864 | Rotate the live segment after this many bytes. The minimum accepted is 64. |
| `ListenAddress`, `HealthAddress`, `ManagementAddress` | `0.0.0.0` | `localhost` binds loopback |

A publish acknowledgement means the record was inside an fsync. `FlushIntervalMs` 0 fsyncs every batch. The index file is not fsynced on the hot path. A crash can redeliver the last few milliseconds of acknowledgements because cursor writes are coalesced.

Verbose lines are a unix timestamp, then `New connection from`, `New client connected`, `Received PUBLISH`, and `Client ... disconnected`. Passwords and message bodies are not printed.

Command-line client, used by the same `nuvexamq` binary:

```bash
nuvexamq stream add --name orders --filter orders.>
nuvexamq pub --subject orders.created --body hello --key order-18
nuvexamq consume --stream orders --durable billing --count 1
nuvexamq path install
```

`path install` adds the published binary to `PATH`. It refuses a `bin/Debug` or `bin/Release` folder.

## Data model

### Streams

A stream is a named, partitioned log. `EnsureStream` creates it or, when the layout matches, updates retention.

| Field | Rule |
| --- | --- |
| Name | 1–128 characters: ASCII letters, digits, `.`, `_`, `-`, `$` |
| Filters | At least one subject filter. The stream stores a publish whose subject matches any filter. |
| Partitions | 1–256. Changing the count later is a conflict. |
| Max age | Milliseconds, or none |
| Max bytes | Byte cap, or none. The live segment is never deleted by retention. |
| Max message bytes | 0 means the broker default |

Subject filters are dot-separated. `*` is one token. `>` is the remainder and must be the last token, and the subject must have at least one token there. `orders.>` matches `orders.created` and `orders.created.eu`. `orders.*` matches `orders.created` only.

The partition is `FNV-1a(key) % count`. An empty key round-robins. The same key always lands on the same partition.

`$dlq` is the broker dead-letter stream for messages that exhaust `MaxDeliver`. It cannot be deleted.

### Consumers

A consumer is a named cursor on one stream.

| Field | Default | Meaning |
| --- | --- | --- |
| Filter | empty | Extra subject filter on top of the stream. Empty accepts the stream's messages. |
| Ack wait | 30000 ms | Minimum 1 ms. An unacked delivery is eligible again after this wait. |
| Max deliver | 5 | After this many attempts the message is appended to `$dlq` |
| Max ack pending | 1000 | Cap on outstanding deliveries |
| Ephemeral | false | An ephemeral consumer starts at the tail and keeps no checkpoint. `Release` removes only ephemeral consumers. Releasing a durable name is a no-op. Releasing a missing name succeeds. |
| Start | first | `0` first offset, `1` tail, `2` a specific offset |

If the consumer name already exists, `EnsureConsumer` returns success and ignores the new start, filter, and ephemeral flag. Create the durable name once with the settings you want. Release an ephemeral name before creating it again.

A fetched message is not delivered again until nack, or until the ack wait expires. Nack of an unknown offset succeeds. Nack marks the delivery so the next fetch can return it with a higher delivery count.

Two durable consumers with different names on the same stream are fan-out: each receives the message. Two connections that use the same durable name compete: one of them receives each delivery.

If retention deletes a segment the cursor still points at, fetch sets the offset-reset flag and reports the first offset still on disk. Reset the consumer to continue.

### Exchanges, queues, and bindings

Every virtual host has these built-in exchanges. They cannot be deleted.

| Name | Type |
| --- | --- |
| empty name, shown as `(AMQP default)` | direct. The routing key is the queue name. Do not bind it. |
| `amq.direct` | direct. Routing key must equal the binding key. |
| `amq.fanout` | fanout. Every bound queue receives the message. |
| `amq.topic` | topic. `*` is one word. `#` is the rest. |
| `amq.headers` | headers. Binding arguments are header name/value pairs. `x-match` is `all` (default) or `any`. |

Declaring the same name and type again is a no-op. Declaring a different type is a conflict. Custom exchange types are `direct`, `fanout`, `topic`, and `headers`.

A queue is a stream whose filter is `>`. On virtual host `/` the log is named `$queue.` plus the queue name. On another virtual host it is `$queue.` plus the virtual host, a dot, and the queue name. Queue options:

| Option | Effect |
| --- | --- |
| Durable | Recorded on the queue. The log itself is always durable. |
| Exclusive | Removed when the connection that declared it closes |
| Auto-delete | Stored on the queue. The console does not offer this flag. |
| Message TTL | Messages older than this are dropped from the visible range |
| Max length | After a publish, older messages are dropped so depth stays within the limit |
| Dead-letter exchange and routing key | When TTL or the delivery limit expires, the message is published there. Header `Nuvexa-Deaths` counts hops and stops at 8. |

A policy is a regular expression over queue names on one virtual host. The highest priority match wins. A later policy of equal priority replaces the earlier one. A null TTL, null max length, or empty dead-letter field on the policy keeps the queue's own value. The pattern match times out after 50 ms and then does not apply.

Permissions are three regular expressions per user and virtual host: configure, write, and read. `.*` allows every name. The resource name must match the pattern. A match that times out or a broken pattern denies the operation.

User tags `administrator` and `management` may call the management API. Other users can use the data port when their password and permissions allow it. Passwords are stored as a hash. The seeded user is created only when `topology.json` is missing, so later changes to `--management-password` do not reset an existing `guest`.

Resource names follow the same character rules as stream names. Virtual host `/` is special and cannot be deleted.

## On disk

```
{dataDir}/topology.json
{dataDir}/streams/{stream}/stream.json
{dataDir}/streams/{stream}/p{n}/segment-{baseOffset}.log
{dataDir}/streams/{stream}/p{n}/segment-{baseOffset}.idx
{dataDir}/streams/{stream}/p{n}/consumer-{name}.json
{dataDir}/streams/{stream}/p{n}/commit.json
```

Log records are little-endian and carry a CRC-32 over the bytes after the checksum. On open, a torn tail is truncated. A missing index is rebuilt from the log. `commit.json` stores the epoch and the last fsynced offset.

Desktop logs, when logging is enabled, are hourly files `{logsDir}/{yyyy-MM-dd}/log-HHmm.log`. The desktop console keeps the latest 2000 lines.

## Wire protocol

Every multi-byte integer is little-endian. A frame is:

| Field | Type |
| --- | --- |
| Length | int32. Number of bytes that follow. Equals 6 plus the payload length. |
| Type | uint16 operation code |
| Request id | uint32. The response copies it. |
| Payload | The rest of the frame |

The maximum frame is 32 MiB. A string is a uint16 byte count plus UTF-8 bytes. A blob is an int32 byte count plus bytes. An absent retention limit is int64 `-1`.

The first frame on a connection must be Hello. The server handles one request at a time on each connection.

| Code | Name | Payload |
| --- | --- | --- |
| 1 | Hello | token, client name, user, password, virtual host. If the payload ends after the client name, user and password are empty and the virtual host is `/`. |
| 2 | HelloOk | version string, `0.1.1` |
| 3 | EnsureStream | name, uint16 filter count, filters, uint16 partition count, int64 max age ms, int64 max bytes, int32 max message bytes |
| 4 | EnsureStreamOk | empty |
| 5 | Publish | subject, key, uint16 header count, each header a string name plus a blob value, then the payload blob. An empty key round-robins. |
| 6 | PublishOk | uint16 receipt count, then stream, uint16 partition, int64 offset for each receipt |
| 7 | EnsureConsumer | stream, name, filter, int32 ack wait ms, int32 max deliver, int32 max ack pending, byte ephemeral, byte start, int64 start offset |
| 8 | EnsureConsumerOk | empty |
| 9 | Fetch | stream, consumer, uint16 max messages, int32 wait ms |
| 10 | FetchOk | byte offset-reset, int64 first offset still on disk, uint16 message count, then each delivery |
| 11 | Ack | stream, consumer, uint16 partition, int64 offset |
| 12 | AckOk | empty |
| 13 | Nack | same shape as Ack |
| 14 | NackOk | empty |
| 15 | ResetConsumer | stream, consumer, byte absolute, int64 offset |
| 16 | ResetConsumerOk | empty |
| 17 | ReleaseConsumer | stream, consumer |
| 18 | ReleaseConsumerOk | empty |
| 19 | Ping | empty |
| 20 | Pong | empty |
| 21 | Error | uint16 code, message, int64 detail |
| 22 | DeclareExchange | name, type, byte durable, byte auto-delete |
| 23 | DeclareExchangeOk | empty |
| 24 | DeclareQueue | name, byte durable, byte exclusive, byte auto-delete, int64 message TTL ms, int32 max length, dead-letter exchange, dead-letter routing key |
| 25 | DeclareQueueOk | empty |
| 26 | BindQueue | exchange, queue, routing key, uint16 argument count. Each argument is two strings, not a blob. |
| 27 | BindQueueOk | empty |
| 28 | DeleteQueue | name |
| 29 | DeleteQueueOk | empty |
| 30 | DeleteExchange | name |
| 31 | DeleteExchangeOk | empty |
| 32 | PurgeQueue | name |
| 33 | PurgeQueueOk | empty |
| 34 | PublishExchange | exchange, routing key, key, headers, payload. Same header encoding as Publish. |
| 35 | PublishExchangeOk | same receipt list as PublishOk |

A delivery inside FetchOk is: uint16 partition, int64 offset, int64 timestamp unix ms, int32 delivery count, subject, key blob, uint16 header count, headers, payload blob.

Start bytes: `0` first, `1` tail, `2` offset.

Error codes:

| Code | Name | When |
| --- | --- | --- |
| 0 | Invalid | Malformed name, bad filter, or an illegal operation |
| 1 | Unauthorized | Token, password, or permission rejected |
| 2 | NotFound | Stream, queue, exchange, user, or binding target is missing |
| 3 | Conflict | Partition count, filters, or exchange type changed |
| 4 | TooLarge | Payload exceeds the maximum |
| 5 | OffsetReset | The cursor pointed at a deleted segment |
| 6 | Closed | The connection or broker is closing |

## Management HTTP

Public routes, with no login, are `GET /`, `GET /icon.png`, and `POST /api/login`. Everything else needs the cookie or Basic authentication. The cookie is HttpOnly, `SameSite=Strict`, path `/`, and lasts 8 hours.

`POST /api/login` body is `{ "username", "password" }`. Success sets the cookie and returns `{ "username", "localhostOnly" }`.

| Method | Path | Body |
| --- | --- | --- |
| POST | `/api/logout` | Clears the cookie |
| GET | `/api/whoami` | Current user |
| GET | `/api/overview` | Version, node, data directory, counters, listeners |
| GET | `/api/connections` | Open data-port sessions |
| GET | `/api/channels` | One channel per connection. Prefetch is reported as 1000. |
| GET | `/api/streams` | Streams that are not queues |
| GET | `/api/streams/{name}` | One stream |
| PUT | `/api/streams/{name}` | `{ filters, partitionCount, maxAgeMs, maxBytes, maxMessageBytes }` |
| DELETE | `/api/streams/{name}` | Deletes the stream. `$dlq` is refused. |
| GET | `/api/streams/{name}/messages?partition&count&offset` | Up to 100 messages. Count defaults to 20. |
| DELETE | `/api/streams/{name}/messages?partition&offset` | Hides that offset |
| GET | `/api/consumers?stream` | Consumer cursors |
| POST | `/api/publish` | `{ subject, key, payload }` written to matching streams |
| GET | `/api/exchanges?vhost` | |
| PUT | `/api/exchanges/{name}` | `{ vhost, type, durable, autoDelete }` |
| DELETE | `/api/exchanges/{name}?vhost` | |
| GET | `/api/queues?vhost` | |
| PUT | `/api/queues/{name}` | `{ vhost, durable, exclusive, autoDelete, messageTtlMs, maxLength, deadLetterExchange, deadLetterRoutingKey }` |
| DELETE | `/api/queues/{name}?vhost` | |
| POST | `/api/queues/{name}/purge?vhost` | Drops currently visible messages |
| GET | `/api/bindings?vhost` | |
| POST | `/api/bindings` | `{ vhost, exchange, queue, routingKey, arguments }` |
| DELETE | `/api/bindings?vhost&exchange&queue&routingKey` | |
| POST | `/api/exchanges/publish` | `{ vhost, exchange, routingKey, key, payload }` |
| GET | `/api/users` | Names and tags. Password hashes are not returned. |
| PUT | `/api/users/{name}` | `{ password, tags }`. A new user requires a password. |
| DELETE | `/api/users/{name}` | Also removes that user's permissions |
| GET | `/api/permissions` | |
| PUT | `/api/permissions` | `{ user, vhost, configure, write, read }` |
| DELETE | `/api/permissions?user&vhost` | |
| GET | `/api/vhosts` | |
| PUT | `/api/vhosts/{name}` | Creates the virtual host |
| DELETE | `/api/vhosts/{name}` | `/` cannot be deleted |
| GET | `/api/policies?vhost` | |
| PUT | `/api/policies/{name}` | `{ vhost, pattern, priority, messageTtlMs, maxLength, deadLetterExchange, deadLetterRoutingKey }` |
| DELETE | `/api/policies/{name}?vhost` | |

Failures use `{ "error", "reason" }` with 400, 401, 404, 409, or 413.

## Limits of this version

Clustering, federation, and the shovel plugin are not implemented. The data port is not AMQP. One request is handled at a time on each connection. The index is not fsynced on the publish path. Auto-delete is stored on the queue and is not applied when the last consumer leaves.
