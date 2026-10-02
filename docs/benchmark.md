# Benchmark

This is a localhost comparison of NuvexaMQ 0.1.0 and RabbitMQ 4.3.6 on one Mac. It is not a published multi-node benchmark. Both brokers ran the same client shape, one after the other, on 2 October 2026.

## Machine

Apple M1 Pro, 8 CPU cores, 16 GB RAM. Both brokers were on this machine. Nothing else was publishing to the stream or queue under test.

## Workload

| Setting | Value |
| --- | --- |
| Publisher connections | 8 |
| Messages per publisher | 10,000 |
| Timed publishes | 80,000 |
| Warmup, not timed | 200 messages on one connection, before the clock starts |
| Payload | 128 bytes |
| Consumer connections | 8 |
| In flight | 256 messages per consumer |

Each publisher sends one message and waits for the broker to accept it before sending the next. The publish clock covers the 80,000 timed messages. The consume clock covers those messages plus the 200 warmup messages (80,200).

The runs did not overlap. NuvexaMQ was a Release build of the server on port 29761 with its own data directory. The desktop broker on port 5761 was not the process under test. RabbitMQ listened on port 5672.

## Result

| | Publish | Consume |
| --- | ---: | ---: |
| NuvexaMQ 0.1.0 | 16,073 msg/s (4.98 s) | 65,923 msg/s (1.22 s) |
| RabbitMQ 4.3.6 | 9,475 msg/s (8.44 s) | 50,919 msg/s (1.58 s) |

NuvexaMQ’s `bench` stream ended at offset 80,200, and the `drain` consumer finished with nothing pending.

RabbitMQ’s consume rate on this Mac moved between repeats. An earlier run of the same program reached 60,456 msg/s, and another reached 47,028 msg/s. The table uses the RabbitMQ run taken immediately beside the NuvexaMQ run.

## What each side confirmed

NuvexaMQ published to stream `bench` (filter `bench.>`, one partition). A publish acknowledgement means the record was in a batch that `fsync` had returned for. On macOS that is libc `fsync`, the same durability RabbitMQ uses here: the write survives a process crash, and a power cut can still drop the last writes. Consumers fetch up to 256 messages and acknowledge that batch together.

RabbitMQ used one durable classic queue, `nuvexa-bench`. Messages were persistent. Each publisher enabled confirms and waited for a confirm on every message. Each consumer used manual acknowledgement and a prefetch of 256. Acknowledgements were not waited on one by one. The queue was deleted after the run.

## Repeat the NuvexaMQ run

`samples/Bench` is the client used above. It leaves the `bench` stream in place.

```bash
dotnet run -c Release --project samples/Bench/Bench.csproj -- \
  --port=5761 --publishers=8 --each=10000 --size=128 --consumers=8
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Broker address |
| `--port` | `5761` | Data port |
| `--publishers` | `8` | Parallel publish connections |
| `--each` | `10000` | Messages on each publish connection |
| `--size` | `128` | Payload size in bytes |
| `--consumers` | `8` | Parallel consume connections. They share the durable consumer name `drain`. |

Point `--port` at a broker whose data directory you can fill. The sample creates stream `bench` if it is missing and appends to it if it already exists. One connection is much slower than eight, because that connection waits alone for each fsync. More connections share the flush.

## Repeat the RabbitMQ run

The RabbitMQ client is not in this repository. The measured run used `RabbitMQ.Client` 6.8.1 against `127.0.0.1:5672` as `guest` / `guest`, with the queue and confirm settings in the section above, and the same connection counts, message size, and message counts.
