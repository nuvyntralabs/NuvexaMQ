# Client integration

An application integrates by opening the data port and exchanging NuvexaMQ frames. The port is TCP 5761 by default. The frames are not AMQP, MQTT, or Kafka. Use one of the programs in `samples/clients` as the client, or copy that program's codec into your application.

The C# sample does not reference a NuGet package. `Nuventra.NuvexaMQ.Client` is an internal library used by the `nuvexamq` command. Ship the sample code, not that project.

SQL cannot open the data port. `samples/clients/sql/ledger.sql` is only a table for receipts you insert yourself.

## Point the application at the broker

| Variable | Default | Used for |
| --- | --- | --- |
| `NUVEXA_HOST` | `127.0.0.1` | Data port |
| `NUVEXA_PORT` | `5761` | Data port |
| `NUVEXA_HEALTH_PORT` | `5762` | `admin` sample only |
| `NUVEXA_MANAGEMENT_PORT` | `5763` | `admin` sample only |
| `NUVEXA_MANAGEMENT_HTTPS_PORT` | `5764` | `admin` sample only |
| `NUVEXA_TOKEN` | empty | Cluster token. Required only when the broker was started with `--token`. |
| `NUVEXA_USER` | `guest` | Data-port user |
| `NUVEXA_PASSWORD` | `guest` | Data-port password |
| `NUVEXA_VHOST` | `/` | Virtual host |

`guest` on `/` starts with configure, write, and read `.*`. That is enough for every sample. Create a dedicated user in the admin panel before you point a real application at the broker, and grant only the names that application uses.

The token is an extra gate on Hello. It does not replace the user and password.

## What to copy

Each language folder has a short `demo` and five feature programs. They share one broker. Feature programs use a language-specific stream or queue name so they do not collide. Do not point them at the `orders`, `bench`, or `clients` streams that the desktop already uses, except `demo`, which publishes `clients.created` on the shared `clients` stream.

| Program | What your application learns |
| --- | --- |
| `demo` | Hello, ensure the `clients` stream, publish one message, fetch, ack |
| `ping` | Hello including the token field, then ping/pong |
| `stream` | Partitions, retention, headers, and round-robin when the key is empty |
| `consume` | Filter, ack, nack and redelivery, reset, ephemeral tail, start at an offset, release |
| `exchange` | direct, fanout, topic, headers, queue TTL, max length, dead letter, bind arguments, the default exchange, purge, delete |
| `admin` | Health, metrics, and the management API: virtual host, user, permission, policy, then HTTPS |

Run them from `samples/clients`:

```bash
dotnet run --project csharp/csharp.csproj -- demo
dotnet run --project csharp/csharp.csproj -- ping
dotnet run --project csharp/csharp.csproj -- stream
dotnet run --project csharp/csharp.csproj -- consume
dotnet run --project csharp/csharp.csproj -- exchange
dotnet run --project csharp/csharp.csproj -- admin
```

The same five names exist for JavaScript, TypeScript, PHP, Python, Ruby, Java, Go, C, C++, Swift, Bash, Kotlin, Dart, Rust, R, Julia, and Lua. Commands are in `samples/clients/README.md`. Kotlin, Dart, Rust, R, Julia, and Lua need that language's compiler installed. They are not required to integrate from C# or from any language you already run.

## Session lifetime

1. Open TCP to `NUVEXA_HOST:NUVEXA_PORT`.
2. Send Hello and wait for HelloOk. The version string is `0.1.1`.
3. Declare what you need: a stream, a consumer, or an exchange and queue.
4. Publish or fetch.
5. Close the socket when the process exits. Ephemeral consumers disappear. Durable consumers remain, and the next connection that uses the same name continues from the stored cursor.

The broker handles one request at a time on the connection. Send the next frame after the response arrives. Pipelining is not required and the samples do not do it.

Hello fields, in order: token, client name, user, password, virtual host. Use a stable client name such as `billing-api` so the Connections page shows who is connected. Use a different consumer name per independent reader. Use the same consumer name when several workers should share one queue of work.

## Publish and consume a stream

This is the path the C# `demo` uses.

1. `EnsureStream` name `clients`, filter `clients.>`, 1 partition. The desktop may already have created `clients`. Ensuring it again with the same layout succeeds.
2. `Publish` subject `clients.created`, key empty or a business id, payload the UTF-8 body. Headers are optional pairs of name and raw bytes.
3. Read PublishOk. Each receipt is stream, partition, and offset. Keep the offset if you need to point a consumer at it later.
4. `EnsureConsumer` on `clients` with a durable name, ack wait 30000, max deliver 5, max ack pending 1000, ephemeral false, start at the first offset.
5. `Fetch`. For each delivery, process the payload, then `Ack` that partition and offset.
6. If processing fails, `Nack` that offset. The next fetch returns it with a higher delivery count. After max deliver attempts the broker appends it to `$dlq`.

If `EnsureConsumer` is called again with the same name, the broker keeps the original start position and filter. To force a durable consumer back to the start, send `ResetConsumer`. To recreate an ephemeral consumer, `ReleaseConsumer` it first. Release does not delete a durable consumer.

A message you just fetched stays invisible until you nack it or the ack wait expires. Ack the ones you keep, then nack the ones you want redelivered. Do not fetch a second time and expect the same message to still be in that batch.

When the key is empty, successive publishes spread across partitions. When the key is set, every message with that key stays on one partition, in order.

## Publish and consume through an exchange

Use this when the application thinks in queues rather than in logs.

1. `DeclareExchange` `orders`, type `topic`, durable.
2. `DeclareQueue` `billing`. Set message TTL, max length, and dead-letter exchange here if you need them. `-1` and empty strings mean "not set" where the sample uses those sentinels.
3. `BindQueue` exchange `orders`, queue `billing`, routing key `orders.*`. For a headers exchange, arguments are string pairs. `x-match` `all` is the usual choice.
4. `PublishExchange` to `orders` with routing key `orders.created`.
5. Consume with `EnsureConsumer` and `Fetch` on the queue's log name. On virtual host `/` that name is `$queue.` plus the queue name, so queue `billing` is `$queue.billing`. On any other virtual host it is `$queue.` plus the virtual host, a dot, and the queue name. PublishExchangeOk receipts name the queue, not this log. A negative message TTL or max length in DeclareQueue means "not set".

Fan-out is two queues bound to a fanout exchange, or two durable consumers with different names on one stream. Competing consumers are two connections using one durable consumer name.

The default exchange has an empty name. Publish with that name and set the routing key to the queue name. Do not bind the default exchange.

Deleting a missing queue or exchange is an error. Deleting a built-in exchange is an error. Declaring an existing exchange with the same type is success. Purge drops what is currently readable and leaves the queue in place.

## Management calls from an application

Prefer the data port for publish and consume. Use the management API when the application must create users, virtual hosts, or policies.

```http
GET /api/whoami HTTP/1.1
Host: 127.0.0.1:5763
Authorization: Basic Z3Vlc3Q6Z3Vlc3Q=
```

`Z3Vlc3Q6Z3Vlc3Q=` is `guest:guest`. You can also `POST /api/login` and send the `nuvexamq_mgmt` cookie. A user must be tagged `management` or `administrator`.

The `admin` sample reads overview, connections, channels, streams, exchanges, queues, bindings, users, permissions, virtual hosts, and policies. It then creates a virtual host, user, permission, and policy, deletes them, and calls `GET https://127.0.0.1:5764/api/whoami` with TLS verification disabled because the broker certificate is generated.

Health and metrics stay on port 5762 and do not use this login.

## Errors the application should surface

| Code | Name | What the application should do |
| --- | --- | --- |
| 0 | Invalid | Fix the name, filter, or arguments. Retrying the same bytes will fail again. |
| 1 | Unauthorized | Check the token, user, password, virtual host, and permission regex. |
| 2 | NotFound | Declare the stream, queue, or exchange before using it. |
| 3 | Conflict | Do not change a stream's partition count or filters. Do not change an exchange's type. |
| 4 | TooLarge | The payload exceeds the broker or stream maximum. The default maximum is 1 MiB. |
| 5 | OffsetReset | Retention deleted the segment. Reset the consumer to the first offset in the error detail. |
| 6 | Closed | Reconnect and send Hello again. |

A frame larger than 32 MiB is rejected before the operation runs.

## Names that will not collide

- One consumer name per work queue. A second process with the same name competes for messages.
- One consumer name per independent subscriber when each subscriber must see every message.
- Stream and queue names that include the application, such as `billing-orders`, so a sample and the application are not sharing a cursor by accident.
- Subjects with dots: `orders.created`, `orders.created.eu`. The filter `orders.>` covers both. The filter `orders.*` covers only one word after `orders`.

## Language map

| Language | Entry |
| --- | --- |
| C# | `dotnet run --project csharp/csharp.csproj -- <demo\|ping\|stream\|consume\|exchange\|admin>` |
| JavaScript | `node javascript/<name>.mjs` |
| TypeScript | `node --experimental-strip-types typescript/<name>.ts` |
| Python | `python3 python/<name>.py` |
| PHP | `php php/<name>.php` |
| Ruby | `ruby ruby/<name>.rb` |
| Java | `javac` the class, then `java` |
| Go | `go run . <name>` from `go/` |
| C | `cc -O2 -o /tmp/nuvexa-c-feature c/feature.c && /tmp/nuvexa-c-feature <name>` |
| C++ | `c++ -O2 -std=c++17 -o /tmp/nuvexa-cpp-feature cpp/feature.cpp && /tmp/nuvexa-cpp-feature <name>` |
| Swift | `swift swift/feature.swift <name>` |
| Bash | `bash bash/feature.sh <name>` |
| Kotlin, Dart, Rust, R, Julia, Lua | See `samples/clients/README.md`. Each feature program takes the same `ping`, `stream`, `consume`, `exchange`, or `admin` argument. |

Full frame layouts are in the [technical reference](technical-reference.md). Console steps for the objects these programs create are in the [admin manual](admin-manual.md).
