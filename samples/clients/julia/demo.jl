# Publish one message, then fetch and ack it.
#   julia demo.jl
# NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

using Sockets

const HELLO = 0x01
const HELLO_OK = 0x02
const ENSURE_STREAM = 0x03
const ENSURE_STREAM_OK = 0x04
const PUBLISH = 0x05
const PUBLISH_OK = 0x06
const ENSURE_CONSUMER = 0x07
const ENSURE_CONSUMER_OK = 0x08
const FETCH = 0x09
const FETCH_OK = 0x0a
const ACK = 0x0b
const ACK_OK = 0x0c
const OP_ERROR = 0x15

mutable struct Writer
    buf::Vector{UInt8}
end
Writer() = Writer(UInt8[])
u8!(w, v) = push!(w.buf, UInt8(v & 0xff))
function u16!(w, v)
    append!(w.buf, reinterpret(UInt8, [htol(UInt16(v))]))
end
function i32!(w, v)
    append!(w.buf, reinterpret(UInt8, [htol(Int32(v))]))
end
function i64!(w, v)
    append!(w.buf, reinterpret(UInt8, [htol(Int64(v))]))
end
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
function str(r)
    n = u16(r)
    String(take!(r, n))
end
function blob(r)
    n = i32(r)
    String(take!(r, n))
end

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

host = get(ENV, "NUVEXA_HOST", "127.0.0.1")
port = parse(Int, get(ENV, "NUVEXA_PORT", "5761"))
language = "julia"
text = "hello from " * language
client = Client(connect(host, port), 0)
hello = Writer()
str!(hello, ""); str!(hello, "sample-julia"); str!(hello, "guest"); str!(hello, "guest"); str!(hello, "/")
request(client, HELLO, hello.buf, HELLO_OK)
stream = Writer()
str!(stream, "clients"); u16!(stream, 1); str!(stream, "clients.>"); u16!(stream, 1); i64!(stream, -1); i64!(stream, -1); i32!(stream, 0)
request(client, ENSURE_STREAM, stream.buf, ENSURE_STREAM_OK)
publish = Writer()
str!(publish, "clients.created"); str!(publish, language); u16!(publish, 0); blob!(publish, text)
published = request(client, PUBLISH, publish.buf, PUBLISH_OK)
u16(published) < 1 && error("publish was not stored")
println("published $(str(published)) partition $(u16(published)) offset $(i64(published))")
consumer = Writer()
str!(consumer, "clients"); str!(consumer, "demo-julia"); str!(consumer, "")
i32!(consumer, 30000); i32!(consumer, 5); i32!(consumer, 1000); u8!(consumer, 0); u8!(consumer, 0); i64!(consumer, 0)
request(client, ENSURE_CONSUMER, consumer.buf, ENSURE_CONSUMER_OK)
found = false
while !found
    fetch = Writer()
    str!(fetch, "clients"); str!(fetch, "demo-julia"); u16!(fetch, 32); i32!(fetch, 0)
    messages = request(client, FETCH, fetch.buf, FETCH_OK)
    u8(messages) == 1 && error("offset reset")
    i64(messages)
    count = u16(messages)
    count == 0 && error("published message was not delivered")
    for _ in 1:count
        part = u16(messages)
        offset = i64(messages)
        i64(messages); i32(messages); str(messages); blob(messages)
        for _ in 1:u16(messages)
            str(messages); blob(messages)
        end
        payload = blob(messages)
        println("fetched $offset $payload")
        ack = Writer()
        str!(ack, "clients"); str!(ack, "demo-julia"); u16!(ack, part); i64!(ack, offset)
        request(client, ACK, ack.buf, ACK_OK)
        found = found || payload == text
    end
end
close(client.io)
