# Client samples

These programs are how an application integrates with an installed NuvexaMQ broker. They open the data port directly. They are not AMQP, MQTT, or Kafka clients.

`NUVEXA_HOST` defaults to `127.0.0.1` and `NUVEXA_PORT` to `5761`. Management samples also use `NUVEXA_HEALTH_PORT` (5762), `NUVEXA_MANAGEMENT_PORT` (5763), and `NUVEXA_MANAGEMENT_HTTPS_PORT` (5764). Guest credentials and vhost `/` are the defaults. Set `NUVEXA_TOKEN` when the broker requires a cluster token.

`demo` publishes `hello from <language>` on `clients.created`, then fetches and acks it. The other programs use a language-specific resource name so they can run on one broker together:

| Program | What it exercises |
| --- | --- |
| `ping` | Hello, including the token field, then ping/pong |
| `stream` | Partitions, retention, max message bytes, headers, and unkeyed round-robin |
| `consume` | Filter, ack, nack and redelivery, reset, ephemeral tail, start-at-offset, release |
| `exchange` | direct, fanout, topic, headers, queue TTL, max length, dead letter, bind arguments, default exchange, purge, delete |
| `admin` | `/health`, `/metrics`, management reads, vhost, user, permission, policy, and the HTTPS listener |

SQL cannot open the TCP port. `sql/ledger.sql` is a receipt table for the line each sample prints.

| Area | Languages | Sample |
| --- | --- | --- |
| Web | JavaScript, TypeScript, PHP, Python, Ruby, C# | below |
| Mobile | Kotlin, Swift, Dart, C# | CLI programs that send the same frames an app would |
| Backend | Java, C#, Python, Go, Rust, TypeScript | below |
| AI / Data | Python, R, Julia | below |
| Systems | C, C++, Rust, Go | below |
| Enterprise | Java, C#, Python, TypeScript | below |
| Database | SQL | `sql/ledger.sql` |
| Game development | C++, C#, Lua | below |
| Cloud / DevOps | Go, Python, Bash, Rust | below |

Every language except SQL has `ping`, `stream`, `consume`, `exchange`, and `admin` in addition to `demo`. SQL cannot open the data port.

| Language | Run |
| --- | --- |
| JavaScript | `node javascript/demo.mjs` and the same folder's `ping.mjs`, `stream.mjs`, `consume.mjs`, `exchange.mjs`, `admin.mjs` |
| TypeScript | `node --experimental-strip-types typescript/demo.ts` and `ping.ts`, `stream.ts`, `consume.ts`, `exchange.ts`, `admin.ts` |
| PHP | `php php/demo.php` and `ping.php`, `stream.php`, `consume.php`, `exchange.php`, `admin.php` |
| Python | `python3 python/demo.py` and `ping.py`, `stream.py`, `consume.py`, `exchange.py`, `admin.py` |
| Ruby | `ruby ruby/demo.rb` and `ping.rb`, `stream.rb`, `consume.rb`, `exchange.rb`, `admin.rb` |
| C# | `dotnet run --project csharp/csharp.csproj -- demo` and `ping`, `stream`, `consume`, `exchange`, `admin` |
| Kotlin | `kotlinc kotlin/Demo.kt -include-runtime -d /tmp/nuvexa-kotlin.jar && java -jar /tmp/nuvexa-kotlin.jar`. Features: `kotlinc kotlin/Feature.kt -include-runtime -d /tmp/nuvexa-kotlin-feature.jar && java -jar /tmp/nuvexa-kotlin-feature.jar ping` |
| Swift | `swift swift/demo.swift`. Features: `swift swift/feature.swift ping` |
| Dart | `dart run dart/demo.dart`. Features: `dart run dart/feature.dart ping` |
| Java | `javac -d /tmp/nuvexa-java java/Demo.java && java -cp /tmp/nuvexa-java Demo`. Feature classes: `Client.java` with `Ping`, `Stream`, `Consume`, `Exchange`, `Admin` |
| Go | `go run .` from `go/`, or `go run . ping`, `stream`, `consume`, `exchange`, `admin` |
| Rust | `cargo run` from `rust/`. Features: `cargo run --bin feature -- ping` |
| R | `Rscript r/demo.R`. Features: `Rscript r/feature.R ping` |
| Julia | `julia julia/demo.jl`. Features: `julia julia/feature.jl ping` |
| C | `cc -O2 -o /tmp/nuvexa-c-demo c/demo.c && /tmp/nuvexa-c-demo`. Features: `cc -O2 -o /tmp/nuvexa-c-feature c/feature.c && /tmp/nuvexa-c-feature ping` |
| C++ | `c++ -O2 -std=c++17 -o /tmp/nuvexa-cpp-demo cpp/demo.cpp && /tmp/nuvexa-cpp-demo`. Features: `c++ -O2 -std=c++17 -o /tmp/nuvexa-cpp-feature cpp/feature.cpp && /tmp/nuvexa-cpp-feature ping` |
| Lua | `luarocks install luasocket` then `lua lua/demo.lua`. Features: `lua lua/feature.lua ping` |
| Bash | `bash bash/demo.sh`. Features: `bash bash/feature.sh ping` |
| SQL | load `sql/ledger.sql` in the database that should keep receipts |
