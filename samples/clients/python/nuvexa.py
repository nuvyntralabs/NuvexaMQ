"""NuvexaMQ frame client used by the Python samples."""

import os
import socket
import struct

HOST = os.environ.get("NUVEXA_HOST", "127.0.0.1")
PORT = int(os.environ.get("NUVEXA_PORT", "5761"))
HEALTH_PORT = int(os.environ.get("NUVEXA_HEALTH_PORT", "5762"))
MANAGEMENT_PORT = int(os.environ.get("NUVEXA_MANAGEMENT_PORT", "5763"))
MANAGEMENT_HTTPS_PORT = int(os.environ.get("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"))
TOKEN = os.environ.get("NUVEXA_TOKEN", "")
USER = os.environ.get("NUVEXA_USER", "guest")
PASSWORD = os.environ.get("NUVEXA_PASSWORD", "guest")
VHOST = os.environ.get("NUVEXA_VHOST", "/")

HELLO, HELLO_OK = 1, 2
ENSURE_STREAM, ENSURE_STREAM_OK = 3, 4
PUBLISH, PUBLISH_OK = 5, 6
ENSURE_CONSUMER, ENSURE_CONSUMER_OK = 7, 8
FETCH, FETCH_OK = 9, 10
ACK, ACK_OK = 11, 12
NACK, NACK_OK = 13, 14
RESET, RESET_OK = 15, 16
RELEASE, RELEASE_OK = 17, 18
PING, PONG = 19, 20
ERROR = 21
DECLARE_EXCHANGE, DECLARE_EXCHANGE_OK = 22, 23
DECLARE_QUEUE, DECLARE_QUEUE_OK = 24, 25
BIND_QUEUE, BIND_QUEUE_OK = 26, 27
DELETE_QUEUE, DELETE_QUEUE_OK = 28, 29
DELETE_EXCHANGE, DELETE_EXCHANGE_OK = 30, 31
PURGE_QUEUE, PURGE_QUEUE_OK = 32, 33
PUBLISH_EXCHANGE, PUBLISH_EXCHANGE_OK = 34, 35

START_FIRST, START_TAIL, START_OFFSET = 0, 1, 2


class Writer:
    def __init__(self):
        self.buf = bytearray()

    def u8(self, value):
        self.buf.append(value & 0xFF)

    def u16(self, value):
        self.buf += struct.pack("<H", value)

    def i32(self, value):
        self.buf += struct.pack("<i", value)

    def i64(self, value):
        self.buf += struct.pack("<q", value)

    def string(self, value):
        raw = value.encode()
        self.u16(len(raw))
        self.buf += raw

    def blob(self, value):
        self.i32(len(value))
        self.buf += value

    def bytes(self):
        return bytes(self.buf)


class Reader:
    def __init__(self, data):
        self.data = data
        self.i = 0

    def take(self, count):
        end = self.i + count
        if end > len(self.data):
            raise EOFError("frame ended early")
        chunk = self.data[self.i:end]
        self.i = end
        return chunk

    def u8(self):
        return self.take(1)[0]

    def u16(self):
        return struct.unpack("<H", self.take(2))[0]

    def i32(self):
        return struct.unpack("<i", self.take(4))[0]

    def i64(self):
        return struct.unpack("<q", self.take(8))[0]

    def string(self):
        return self.take(self.u16()).decode()

    def blob(self):
        return self.take(self.i32())


def read_exact(sock, count):
    chunks = []
    remaining = count
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class Client:
    def __init__(self, host=HOST, port=PORT):
        self.sock = socket.create_connection((host, port))
        self.next_id = 0

    def close(self):
        self.sock.close()

    def request(self, op, payload, expected):
        self.next_id += 1
        body = struct.pack("<HI", op, self.next_id) + payload
        self.sock.sendall(struct.pack("<I", len(body)) + body)
        length = struct.unpack("<I", read_exact(self.sock, 4))[0]
        frame = read_exact(self.sock, length)
        kind, request_id = struct.unpack_from("<HI", frame)
        data = frame[6:]
        if request_id != self.next_id:
            raise RuntimeError("response id did not match the request")
        if kind == ERROR:
            reader = Reader(data)
            raise RuntimeError(f"broker error {reader.u16()}: {reader.string()}")
        if kind != expected:
            raise RuntimeError(f"unexpected frame {kind}")
        return data

    def hello(self, name, token=TOKEN, user=USER, password=PASSWORD, vhost=VHOST):
        writer = Writer()
        writer.string(token)
        writer.string(name)
        writer.string(user)
        writer.string(password)
        writer.string(vhost)
        version = Reader(self.request(HELLO, writer.bytes(), HELLO_OK)).string()
        print(f"hello {version} user {user} vhost {vhost}")

    def ping(self):
        self.request(PING, b"", PONG)
        print("pong")

    def ensure_stream(self, name, filters, partitions=1, max_age_ms=-1, max_bytes=-1, max_message_bytes=0):
        writer = Writer()
        writer.string(name)
        writer.u16(len(filters))
        for item in filters:
            writer.string(item)
        writer.u16(partitions)
        writer.i64(max_age_ms)
        writer.i64(max_bytes)
        writer.i32(max_message_bytes)
        self.request(ENSURE_STREAM, writer.bytes(), ENSURE_STREAM_OK)
        print(f"stream {name} partitions {partitions} filters {','.join(filters)}")

    def publish(self, subject, payload, key="", headers=None):
        writer = Writer()
        writer.string(subject)
        writer.string(key)
        headers = headers or []
        writer.u16(len(headers))
        for name, value in headers:
            writer.string(name)
            writer.blob(value)
        writer.blob(payload)
        return self._receipts(self.request(PUBLISH, writer.bytes(), PUBLISH_OK))

    def ensure_consumer(self, stream, name, filter="", ack_wait_ms=30000, max_deliver=5, max_ack_pending=1000, ephemeral=False, start=START_FIRST, start_offset=0):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.string(filter)
        writer.i32(ack_wait_ms)
        writer.i32(max_deliver)
        writer.i32(max_ack_pending)
        writer.u8(1 if ephemeral else 0)
        writer.u8(start)
        writer.i64(start_offset)
        self.request(ENSURE_CONSUMER, writer.bytes(), ENSURE_CONSUMER_OK)
        print(f"consumer {name} on {stream} start {start} ephemeral {int(ephemeral)}")

    def fetch(self, stream, name, max_messages=32, expires_ms=0):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.u16(max_messages)
        writer.i32(expires_ms)
        reader = Reader(self.request(FETCH, writer.bytes(), FETCH_OK))
        reset = reader.u8() == 1
        first = reader.i64()
        messages = []
        for _ in range(reader.u16()):
            partition = reader.u16()
            offset = reader.i64()
            timestamp = reader.i64()
            delivery = reader.i32()
            subject = reader.string()
            key = reader.blob()
            headers = [(reader.string(), reader.blob()) for _header in range(reader.u16())]
            payload = reader.blob()
            messages.append({
                "partition": partition,
                "offset": offset,
                "timestamp": timestamp,
                "delivery": delivery,
                "subject": subject,
                "key": key,
                "headers": headers,
                "payload": payload,
            })
        return reset, first, messages

    def ack(self, stream, name, partition, offset):
        self._cursor(ACK, ACK_OK, stream, name, partition, offset)

    def nack(self, stream, name, partition, offset):
        self._cursor(NACK, NACK_OK, stream, name, partition, offset)

    def reset(self, stream, name, absolute=True, offset=0):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.u8(1 if absolute else 0)
        writer.i64(offset)
        self.request(RESET, writer.bytes(), RESET_OK)
        print(f"reset {name} absolute {int(absolute)} offset {offset}")

    def release(self, stream, name):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        self.request(RELEASE, writer.bytes(), RELEASE_OK)
        print(f"release {name}")

    def declare_exchange(self, name, exchange_type, durable=True, auto_delete=False):
        writer = Writer()
        writer.string(name)
        writer.string(exchange_type)
        writer.u8(1 if durable else 0)
        writer.u8(1 if auto_delete else 0)
        self.request(DECLARE_EXCHANGE, writer.bytes(), DECLARE_EXCHANGE_OK)
        print(f"exchange {name or '(default)'} {exchange_type}")

    def declare_queue(self, name, durable=True, exclusive=False, auto_delete=False, message_ttl_ms=-1, max_length=-1, dead_letter_exchange="", dead_letter_routing_key=""):
        writer = Writer()
        writer.string(name)
        writer.u8(1 if durable else 0)
        writer.u8(1 if exclusive else 0)
        writer.u8(1 if auto_delete else 0)
        writer.i64(message_ttl_ms)
        writer.i32(max_length)
        writer.string(dead_letter_exchange)
        writer.string(dead_letter_routing_key)
        self.request(DECLARE_QUEUE, writer.bytes(), DECLARE_QUEUE_OK)
        print(f"queue {name} ttl {message_ttl_ms} max {max_length} dlx {dead_letter_exchange or '-'}")

    def bind_queue(self, exchange, queue, routing_key, arguments=None):
        writer = Writer()
        writer.string(exchange)
        writer.string(queue)
        writer.string(routing_key)
        arguments = arguments or []
        writer.u16(len(arguments))
        for name, value in arguments:
            writer.string(name)
            writer.string(value)
        self.request(BIND_QUEUE, writer.bytes(), BIND_QUEUE_OK)
        print(f"bind {exchange or '(default)'} -> {queue} key {routing_key or '-'}")

    def delete_queue(self, name):
        self._named(DELETE_QUEUE, DELETE_QUEUE_OK, name)
        print(f"delete queue {name}")

    def delete_exchange(self, name):
        self._named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name)
        print(f"delete exchange {name}")

    def purge_queue(self, name):
        self._named(PURGE_QUEUE, PURGE_QUEUE_OK, name)
        print(f"purge queue {name}")

    def publish_exchange(self, exchange, routing_key, payload, key="", headers=None):
        writer = Writer()
        writer.string(exchange)
        writer.string(routing_key)
        writer.string(key)
        headers = headers or []
        writer.u16(len(headers))
        for name, value in headers:
            writer.string(name)
            writer.blob(value)
        writer.blob(payload)
        receipts = self._receipts(self.request(PUBLISH_EXCHANGE, writer.bytes(), PUBLISH_EXCHANGE_OK))
        print(f"routed {exchange or '(default)'} key {routing_key} -> {len(receipts)}")
        return receipts

    def _receipts(self, data):
        reader = Reader(data)
        receipts = []
        for _ in range(reader.u16()):
            receipts.append((reader.string(), reader.u16(), reader.i64()))
        for stream, partition, offset in receipts:
            print(f"published {stream} partition {partition} offset {offset}")
        return receipts

    def _cursor(self, op, expected, stream, name, partition, offset):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.u16(partition)
        writer.i64(offset)
        self.request(op, writer.bytes(), expected)

    def _named(self, op, expected, name):
        writer = Writer()
        writer.string(name)
        self.request(op, writer.bytes(), expected)


def connect(language):
    client = Client()
    client.hello(f"sample-{language}")
    return client
