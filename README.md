# NuvexaMQ

Durable message broker. Install the server, then integrate with a client sample.

**Version:** 0.1.1  
**Author:** Niladri Prasad Padhy / Nuventra  
**License:** MIT

What you install is the server package. What you copy into an application is a program from `samples/clients`. Those programs open the broker's TCP port and publish, fetch, and ack. They do not speak AMQP, MQTT, or Kafka. Those systems are the reference for the behavior, not the wire protocol.

One append-only log stores the message. A queue, a pub/sub fan-out, and a replayable log are three ways to read that log.

This version is one node. An acknowledged publish is on disk. The segment header and `commit.json` carry an epoch and a committed offset so a later follower wave does not need a new log format. This version does not fail over to another machine.

## Install

Production does not use the `src` folder. Install the package for the machine:

| System | Package | Service |
| --- | --- | --- |
| Windows | `NuvexaMQ-<version>-win-x64.msi` or `win-arm64.msi` | Windows service `NuvexaMQ` |
| macOS | `NuvexaMQ-<version>-osx-arm64.pkg` or `osx-x64.pkg` | launchd `com.nuventra.nuvexamq` |
| Linux | `nuvexamq_<version>_amd64.deb` / `_arm64.deb`, and the matching `.rpm` | systemd `nuvexamq` |

The package installs the broker and the desktop app, puts `nuvexamq` on `PATH`, and starts the broker on boot. Data is kept in `C:\ProgramData\NuvexaMQ\data`, `/Library/Application Support/NuvexaMQ/data`, or `/var/lib/nuvexamq`.

CI on `main` builds these packages and keeps them as workflow artifacts for 14 days. Pushing the tag `v<Version>` (the version in `Directory.Build.props`) publishes those packages on the GitHub Release. A push that is not that tag does not create a release. Build one from a source checkout:

```bash
./packaging/pack.sh 0.1.1 osx-arm64 artifacts
```

Use `win-x64`, `win-arm64`, `osx-x64`, `linux-x64`, or `linux-arm64` for the other packages. Windows needs the WiX CLI (`dotnet tool install -g wix --version 5.0.2`). Linux needs `fpm` and `rpmbuild`. macOS uses `pkgbuild`.

Open a new terminal and start the broker, or use the service the package registered:

```bash
nuvexamq
nuvexamq -v
nuvexamq-desktop
```

## Run from source

```bash
dotnet run --project src/Nuventra.NuvexaMQ.Server
```

The broker listens on **5761**. Health and Prometheus text are on **5762** (`/health`, `/metrics`). The management console is on **5763** for HTTP and **5764** for HTTPS. On start the process prints those listeners as URLs:

```
tcp://127.0.0.1:5761
http://127.0.0.1:5762/
http://127.0.0.1:5763/
https://127.0.0.1:5764/
```

A wildcard bind is shown as `127.0.0.1`. With `CertificatePath` set, the protocol listener is `tls://` and HTTPS uses that certificate. Without one, HTTPS still listens with a generated certificate, and browsers warn about it. Data is written under `./data`.

`--verbose` or `-v` turns on connection and publish logs, the same idea as Mosquitto `-v`: a unix timestamp, then `New connection from`, `New client connected`, `Received PUBLISH`, and `Client ... disconnected`. The listener lines are always printed. Verbose does not print passwords or message bodies.

Open `http://127.0.0.1:5763/` or `https://127.0.0.1:5764/` and sign in as `guest` / `guest`. That user is accepted only from this machine, the same rule as RabbitMQ. The console is the broker user directory: virtual hosts, users, configure/write/read permissions, exchanges, queues, bindings, and policies. `guest` starts with permission `.*` on the `/` virtual host.

The desktop window starts and stops the same broker, shows the console log, and sets the broker, health, and admin ports:

```bash
dotnet run --project src/Nuventra.NuvexaMQ.Desktop -f net10.0
```

Defaults are broker **5761**, health **5762**, admin HTTP **5763**, and admin HTTPS **5764**. Those values are saved under the local application data folder. **Open admin portal** launches the HTTP console in the browser after the broker is running.

An application integrates by running one of the programs in `samples/clients` against the installed broker. `NUVEXA_HOST` and `NUVEXA_PORT` default to `127.0.0.1` and `5761`. Each sample signs in as `guest` / `guest` on virtual host `/`. Set a token on the connection when the broker requires the cluster token. The token is an extra gate on the TCP port. It does not replace the user. The commands for each language are in `samples/clients/README.md`.

Exchanges are `direct`, `fanout`, `topic`, and `headers`. Every virtual host has the default exchange (publish with an empty name and the queue name as the routing key), plus `amq.direct`, `amq.fanout`, `amq.topic`, and `amq.headers`. Topic routing keys use `*` for one word and `#` for the rest. A queue can set a message TTL, a max length, and a dead-letter exchange. A policy with a higher priority overrides those settings for queue names that match its regular expression.

```bash
dotnet run --project src/Nuventra.NuvexaMQ.Server -- stream add --name orders --filter orders.>
dotnet run --project src/Nuventra.NuvexaMQ.Server -- pub --subject orders.created --body hello --key order-18
dotnet run --project src/Nuventra.NuvexaMQ.Server -- consume --stream orders --durable billing --count 1
```

`FlushIntervalMs` in `appsettings.json` is the group-commit window (default 10). A publish acknowledgement means the record was inside an fsync. Set it to `0` to fsync every batch.

Set `Token` to require that string on connect. Set `CertificatePath` and `KeyPath` to PEM files to wrap the same frames in TLS. On .NET 8 for macOS the host uses `openssl` to turn that PEM pair into a certificate the runtime can serve. .NET 9 and .NET 10 load the PEM files directly.

## Documentation

- [Technical reference](docs/technical-reference.md) covers the service, ports, storage, and wire protocol.
- [Admin panel](docs/admin-manual.md) is the operator guide for the management console.
- [Client integration](docs/client-integration.md) is how an application publishes and consumes.
- [Benchmark](docs/benchmark.md) is the localhost comparison with RabbitMQ.

## Integrate

Start from `samples/clients`. The C# sample does not reference a client library. It opens the data port and writes the same frames as the JavaScript, Java, Go, and other samples. Point the sample at the broker the installer registered.

`orders.>` matches `orders.created` and `orders.created.eu`. `*` is one token. `>` is the rest of the subject and needs at least one token.

A second durable consumer named `warehouse` on `orders` is fan-out: both consumers receive the message. A second connection that uses the durable name `billing` is a competing consumer: only one of them receives each delivery.

Leave a delivery unacknowledged and it is delivered again after the ack wait. After the maximum number of attempts it is appended to the `$dlq` stream. An ephemeral consumer starts at the tail and keeps no checkpoint after disconnect.

If retention deletes a segment the consumer was still reading, fetch returns an offset-reset error. Resetting that consumer starts it at the first offset still on disk.

## On disk

```
{dataDir}/streams/{stream}/stream.json
{dataDir}/streams/{stream}/p{n}/segment-{baseOffset}.log
{dataDir}/streams/{stream}/p{n}/segment-{baseOffset}.idx
{dataDir}/streams/{stream}/p{n}/consumer-{name}.json
{dataDir}/streams/{stream}/p{n}/commit.json
```

Records are little-endian and carry a CRC-32 over the bytes after the checksum field. On open, a torn tail is truncated. A missing index is rebuilt from the log. The live segment is never deleted by retention. `commit.json` stores the epoch and the last fsynced offset.

The partition for a message is `FNV-1a(key) % count`. With no key, the broker round-robins.

## Layout

```
packaging/                     server installer: MSI, PKG, DEB, and RPM
samples/clients                 integration programs, one per language
src/Nuventra.NuvexaMQ.Server     broker that the installer publishes
src/Nuventra.NuvexaMQ.Desktop    window shipped inside the same installer
src/Nuventra.NuvexaMQ.Engine     log, streams, and consumers, linked into the server
src/Nuventra.NuvexaMQ.Protocol   frames the server reads and writes
src/Nuventra.NuvexaMQ.Client     used by the nuvexamq command inside the server
tests/Nuventra.NuvexaMQ.Tests
```

The server, engine, protocol, and command-line client are not NuGet packages. The installer is the build CI uploads.

## Not in this version

Clustering, federation, the shovel plugin, and the AMQP wire protocol. NuvexaMQ speaks its own protocol. The exchange, queue, binding, virtual host, user, and permission model matches RabbitMQ.
