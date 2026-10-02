# Stream, consume, exchange, ping, and management samples.
#   julia feature.jl ping

using Sockets

const HELLO = 1
const HELLO_OK = 2
const ENSURE_STREAM = 3
const ENSURE_STREAM_OK = 4
const PUBLISH = 5
const PUBLISH_OK = 6
const ENSURE_CONSUMER = 7
const ENSURE_CONSUMER_OK = 8
const FETCH = 9
const FETCH_OK = 10
const ACK = 11
const ACK_OK = 12
const NACK = 13
const NACK_OK = 14
const RESET = 15
const RESET_OK = 16
const RELEASE = 17
const RELEASE_OK = 18
const PING = 19
const PONG = 20
const OP_ERROR = 21
const DECLARE_EXCHANGE = 22
const DECLARE_EXCHANGE_OK = 23
const DECLARE_QUEUE = 24
const DECLARE_QUEUE_OK = 25
const BIND_QUEUE = 26
const BIND_QUEUE_OK = 27
const DELETE_QUEUE = 28
const DELETE_QUEUE_OK = 29
const DELETE_EXCHANGE = 30
const DELETE_EXCHANGE_OK = 31
const PURGE_QUEUE = 32
const PURGE_QUEUE_OK = 33
const PUBLISH_EXCHANGE = 34
const PUBLISH_EXCHANGE_OK = 35

mutable struct Writer
    buf::Vector{UInt8}
end
Writer() = Writer(UInt8[])
u8!(w, v) = push!(w.buf, UInt8(v & 0xff))
u16!(w, v) = append!(w.buf, reinterpret(UInt8, [htol(UInt16(v))]))
i32!(w, v) = append!(w.buf, reinterpret(UInt8, [htol(Int32(v))]))
i64!(w, v) = append!(w.buf, reinterpret(UInt8, [htol(Int64(v))]))
function str!(w, v)
    raw = Vector{UInt8}(v)
    u16!(w, length(raw))
    append!(w.buf, raw)
end
function blob!(w, v)
    raw = v isa String ? Vector{UInt8}(v) : v
    i32!(w, length(raw))
    append!(w.buf, raw)
end

mutable struct Reader
    data::Vector{UInt8}
    i::Int
end
function take!(r, n)
    if r.i + n - 1 > length(r.data)
        error("frame ended early")
    end
    chunk = r.data[r.i:r.i + n - 1]
    r.i += n
    chunk
end
u8(r) = Int(take!(r, 1)[1])
u16(r) = Int(ltoh(reinterpret(UInt16, take!(r, 2))[1]))
i32(r) = Int(ltoh(reinterpret(Int32, take!(r, 4))[1]))
i64(r) = Int64(ltoh(reinterpret(Int64, take!(r, 8))[1]))
str(r) = String(take!(r, u16(r)))
blob(r) = String(take!(r, i32(r)))

function readexact(io, n)
    buf = Vector{UInt8}(undef, n)
    read!(io, buf)
    buf
end

mutable struct Client
    io::TCPSocket
    next::UInt32
end

function request(client, op, payload, expected)
    client.next += 1
    body = UInt8[]
    append!(body, reinterpret(UInt8, [htol(UInt16(op))]))
    append!(body, reinterpret(UInt8, [htol(UInt32(client.next))]))
    append!(body, payload)
    write(client.io, reinterpret(UInt8, [htol(UInt32(length(body)))]))
    write(client.io, body)
    size = Int(ltoh(reinterpret(UInt32, readexact(client.io, 4))[1]))
    frame = readexact(client.io, size)
    kind = Int(ltoh(reinterpret(UInt16, frame[1:2])[1]))
    request_id = ltoh(reinterpret(UInt32, frame[3:6])[1])
    if request_id != client.next
        error("response id did not match the request")
    end
    reader = Reader(frame[7:end], 1)
    if kind == OP_ERROR
        error("broker error $(u16(reader)): $(str(reader))")
    end
    if kind != expected
        error("unexpected frame $kind")
    end
    reader
end

env(name, fallback) = let value = get(ENV, name, ""); isempty(value) ? fallback : value end

function hello!(client)
    body = Writer()
    str!(body, env("NUVEXA_TOKEN", "")); str!(body, "sample-julia")
    str!(body, env("NUVEXA_USER", "guest")); str!(body, env("NUVEXA_PASSWORD", "guest")); str!(body, env("NUVEXA_VHOST", "/"))
    println("hello $(str(request(client, HELLO, body.buf, HELLO_OK)))")
end

function receipts(reader)
    partitions = Int[]
    for _ in 1:u16(reader)
        stream = str(reader)
        partition = u16(reader)
        offset = i64(reader)
        println("published $stream partition $partition offset $offset")
        push!(partitions, partition)
    end
    partitions
end

function publish!(client, subject, payload; key="", header="", header_value="")
    body = Writer()
    str!(body, subject); str!(body, key)
    if isempty(header)
        u16!(body, 0)
    else
        u16!(body, 1); str!(body, header); blob!(body, header_value)
    end
    blob!(body, payload)
    receipts(request(client, PUBLISH, body.buf, PUBLISH_OK))
end

function ensure_stream!(client, name, filter, partitions, max_age, max_bytes, max_message)
    body = Writer()
    str!(body, name); u16!(body, 1); str!(body, filter); u16!(body, partitions); i64!(body, max_age); i64!(body, max_bytes); i32!(body, max_message)
    request(client, ENSURE_STREAM, body.buf, ENSURE_STREAM_OK)
    println("stream $name partitions $partitions")
end

function ensure_consumer!(client, stream, name, filter, ephemeral, start, offset)
    body = Writer()
    str!(body, stream); str!(body, name); str!(body, filter)
    i32!(body, 30000); i32!(body, 5); i32!(body, 1000); u8!(body, ephemeral ? 1 : 0); u8!(body, start); i64!(body, offset)
    request(client, ENSURE_CONSUMER, body.buf, ENSURE_CONSUMER_OK)
    println("consumer $name on $stream start $start")
end

function fetch!(client, stream, name, max)
    body = Writer()
    str!(body, stream); str!(body, name); u16!(body, max); i32!(body, 0)
    reader = request(client, FETCH, body.buf, FETCH_OK)
    u8(reader) == 1 && error("offset reset")
    i64(reader)
    messages = []
    for _ in 1:u16(reader)
        partition = u16(reader)
        offset = i64(reader)
        i64(reader)
        attempts = i32(reader)
        str(reader); blob(reader)
        for _ in 1:u16(reader)
            str(reader); blob(reader)
        end
        push!(messages, (partition=partition, offset=offset, attempts=attempts, payload=blob(reader)))
    end
    messages
end

function cursor!(client, op, expected, stream, name, message)
    body = Writer()
    str!(body, stream); str!(body, name); u16!(body, message.partition); i64!(body, message.offset)
    request(client, op, body.buf, expected)
end

function declare_exchange!(client, name, kind)
    body = Writer()
    str!(body, name); str!(body, kind); u8!(body, 1); u8!(body, 0)
    request(client, DECLARE_EXCHANGE, body.buf, DECLARE_EXCHANGE_OK)
    println("exchange $name $kind")
end

function declare_queue!(client, name, ttl, max, dead, dead_key)
    body = Writer()
    str!(body, name); u8!(body, 1); u8!(body, 0); u8!(body, 0); i64!(body, ttl); i32!(body, max); str!(body, dead); str!(body, dead_key)
    request(client, DECLARE_QUEUE, body.buf, DECLARE_QUEUE_OK)
    println("queue $name")
end

function bind!(client, exchange, queue, key, headers)
    body = Writer()
    str!(body, exchange); str!(body, queue); str!(body, key)
    if headers
        u16!(body, 2); str!(body, "format"); str!(body, "json"); str!(body, "x-match"); str!(body, "all")
    else
        u16!(body, 0)
    end
    request(client, BIND_QUEUE, body.buf, BIND_QUEUE_OK)
end

function publish_exchange!(client, exchange, routing, payload, key, header)
    body = Writer()
    str!(body, exchange); str!(body, routing); str!(body, key)
    if header
        u16!(body, 1); str!(body, "format"); blob!(body, "json")
    else
        u16!(body, 0)
    end
    blob!(body, payload)
    items = receipts(request(client, PUBLISH_EXCHANGE, body.buf, PUBLISH_EXCHANGE_OK))
    println("routed $exchange -> $(length(items))")
    length(items)
end

function named!(client, op, expected, name, label)
    body = Writer()
    str!(body, name)
    request(client, op, body.buf, expected)
    println(label)
end

function http(method, url, body, auth)
    cmd = `curl -sk -o /tmp/nuvexa-julia-http -w %{http_code} -X $method`
    if auth
        cmd = `$cmd -u $(env("NUVEXA_USER", "guest")):$(env("NUVEXA_PASSWORD", "guest"))`
    end
    if body !== nothing
        cmd = `$cmd -H "Content-Type: application/json" --data $body`
    end
    cmd = `$cmd $url`
    status = readchomp(cmd)
    bytes = filesize("/tmp/nuvexa-julia-http")
    println("$method $url $status $bytes bytes")
    (parse(Int, status) >= 400) && error("management request failed")
end

function run_stream(client)
    ensure_stream!(client, "catalog-julia", "catalog-julia.>", 2, 86400000, 1048576, 65536)
    keyed = publish!(client, "catalog-julia.created", "{\"id\":1}"; key="alpha", header="content-type", header_value="application/json")
    first = publish!(client, "catalog-julia.created", "{\"id\":1}")
    second = publish!(client, "catalog-julia.created", "{\"id\":1}")
    (isempty(keyed) || first[1] == second[1]) && error("round-robin did not use both partitions")
end

function run_consume(client)
    ensure_stream!(client, "mailbox-julia", "mailbox-julia.>", 1, -1, -1, 0)
    ensure_consumer!(client, "mailbox-julia", "box-julia", "mailbox-julia.>", false, 0, 0)
    publish!(client, "mailbox-julia.created", "ack me"; header="content-type", header_value="text/plain")
    publish!(client, "mailbox-julia.created", "nack me"; header="content-type", header_value="text/plain")
    batch = fetch!(client, "mailbox-julia", "box-julia", 32)
    acked = batch[findfirst(message -> message.payload == "ack me", batch)]
    nacked = batch[findfirst(message -> message.payload == "nack me", batch)]
    cursor!(client, ACK, ACK_OK, "mailbox-julia", "box-julia", acked); println("ack")
    cursor!(client, NACK, NACK_OK, "mailbox-julia", "box-julia", nacked); println("nack")
    again = fetch!(client, "mailbox-julia", "box-julia", 32)
    again = again[findfirst(message -> message.payload == "nack me", again)]
    println("redelivered $(again.attempts)")
    cursor!(client, ACK, ACK_OK, "mailbox-julia", "box-julia", again)
    body = Writer(); str!(body, "mailbox-julia"); str!(body, "box-julia"); u8!(body, 1); i64!(body, 0)
    request(client, RESET, body.buf, RESET_OK)
    println("reset box-julia offset 0")
    reset = fetch!(client, "mailbox-julia", "box-julia", 1)
    isempty(reset) && error("reset did not return a message")
    println("after reset offset $(reset[1].offset)")
    cursor!(client, ACK, ACK_OK, "mailbox-julia", "box-julia", reset[1])
end

function release!(client, stream, name)
    body = Writer(); str!(body, stream); str!(body, name)
    request(client, RELEASE, body.buf, RELEASE_OK)
    println("release $name")
end

function run_consume_tail(client)
    release!(client, "mailbox-julia", "tail-julia")
    ensure_consumer!(client, "mailbox-julia", "tail-julia", "", true, 1, 0)
    publish!(client, "mailbox-julia.created", "tail me")
    tailed = fetch!(client, "mailbox-julia", "tail-julia", 32)
    tailed = tailed[findfirst(message -> message.payload == "tail me", tailed)]
    cursor!(client, ACK, ACK_OK, "mailbox-julia", "tail-julia", tailed)
    release!(client, "mailbox-julia", "tail-julia")
    ensure_consumer!(client, "mailbox-julia", "from0-julia", "", false, 2, 0)
    println("offset consumer from0-julia")
end

function run_exchange(client)
    declare_exchange!(client, "amq.direct", "direct")
    declare_exchange!(client, "direct-julia", "direct")
    declare_exchange!(client, "fanout-julia", "fanout")
    declare_exchange!(client, "topic-julia", "topic")
    declare_exchange!(client, "headers-julia", "headers")
    declare_queue!(client, "dead-julia", -1, -1, "", "")
    declare_queue!(client, "work-julia", 60000, 100, "direct-julia", "expired")
    bind!(client, "direct-julia", "dead-julia", "expired", false)
    bind!(client, "direct-julia", "work-julia", "work.created", false)
    bind!(client, "fanout-julia", "work-julia", "", false)
    bind!(client, "topic-julia", "work-julia", "work.*", false)
    bind!(client, "headers-julia", "work-julia", "", true)
    count = 0
    count += publish_exchange!(client, "direct-julia", "work.created", "routed", "order-1", false)
    count += publish_exchange!(client, "fanout-julia", "", "routed", "", false)
    count += publish_exchange!(client, "topic-julia", "work.created", "routed", "", false)
    count += publish_exchange!(client, "headers-julia", "", "routed", "", true)
    count += publish_exchange!(client, "", "work-julia", "routed", "", false)
    count < 5 && error("expected at least 5 receipts")
    named!(client, PURGE_QUEUE, PURGE_QUEUE_OK, "work-julia", "purge queue work-julia")
    named!(client, DELETE_QUEUE, DELETE_QUEUE_OK, "work-julia", "delete queue work-julia")
    named!(client, DELETE_QUEUE, DELETE_QUEUE_OK, "dead-julia", "delete queue dead-julia")
    for name in ["direct-julia", "fanout-julia", "topic-julia", "headers-julia"]
        named!(client, DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, "delete exchange $name")
    end
end

function run_admin()
    host = env("NUVEXA_HOST", "127.0.0.1")
    health = "http://$host:$(env("NUVEXA_HEALTH_PORT", "5762"))"
    http("GET", "$health/health", nothing, false)
    http("GET", "$health/metrics", nothing, false)
    base = "http://$host:$(env("NUVEXA_MANAGEMENT_PORT", "5763"))"
    for path in ["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"]
        http("GET", base * path, nothing, true)
    end
    http("PUT", "$base/api/vhosts/vh-julia", nothing, true)
    http("PUT", "$base/api/users/user-julia", "{\"password\":\"sample-pass\",\"tags\":[\"management\"]}", true)
    http("PUT", "$base/api/permissions", "{\"user\":\"user-julia\",\"vhost\":\"vh-julia\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}", true)
    http("PUT", "$base/api/policies/policy-julia", "{\"vhost\":\"/\",\"pattern\":\"sample-.*\",\"priority\":1,\"messageTtlMs\":60000,\"maxLength\":100,\"deadLetterExchange\":\"\",\"deadLetterRoutingKey\":\"\"}", true)
    http("DELETE", "$base/api/policies/policy-julia?vhost=/", nothing, true)
    http("DELETE", "$base/api/permissions?user=user-julia&vhost=vh-julia", nothing, true)
    http("DELETE", "$base/api/users/user-julia", nothing, true)
    http("DELETE", "$base/api/vhosts/vh-julia", nothing, true)
    http("GET", "https://$host:$(env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"))/api/whoami", nothing, true)
end

command = length(ARGS) == 0 ? "ping" : ARGS[1]
if command == "admin"
    run_admin()
else
    client = Client(connect(env("NUVEXA_HOST", "127.0.0.1"), parse(Int, env("NUVEXA_PORT", "5761"))), 0)
    hello!(client)
    if command == "ping"
        request(client, PING, UInt8[], PONG)
        println("pong")
    elseif command == "stream"
        run_stream(client)
    elseif command == "consume"
        run_consume(client)
        run_consume_tail(client)
    elseif command == "exchange"
        run_exchange(client)
    else
        error("use ping, stream, consume, exchange, or admin")
    end
    close(client.io)
end
