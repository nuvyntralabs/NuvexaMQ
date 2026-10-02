#!/usr/bin/env python3
"""Publish one message, then fetch and ack it.

    python3 demo.py

NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
"""

import os
import socket
import struct
import sys

HOST = os.environ.get("NUVEXA_HOST", "127.0.0.1")
PORT = int(os.environ.get("NUVEXA_PORT", "5761"))
HELLO, HELLO_OK = 1, 2
ENSURE_STREAM, ENSURE_STREAM_OK = 3, 4
PUBLISH, PUBLISH_OK = 5, 6
ENSURE_CONSUMER, ENSURE_CONSUMER_OK = 7, 8
FETCH, FETCH_OK = 9, 10
ACK, ACK_OK = 11, 12
ERROR = 21


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
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port))
        self.next_id = 0

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

    def hello(self, user, password, vhost, name):
        writer = Writer()
        writer.string("")
        writer.string(name)
        writer.string(user)
        writer.string(password)
        writer.string(vhost)
        self.request(HELLO, bytes(writer.buf), HELLO_OK)

    def ensure_stream(self, name, filters):
        writer = Writer()
        writer.string(name)
        writer.u16(len(filters))
        for item in filters:
            writer.string(item)
        writer.u16(1)
        writer.i64(-1)
        writer.i64(-1)
        writer.i32(0)
        self.request(ENSURE_STREAM, bytes(writer.buf), ENSURE_STREAM_OK)

    def publish(self, subject, payload, key):
        writer = Writer()
        writer.string(subject)
        writer.string(key)
        writer.u16(0)
        writer.blob(payload)
        data = self.request(PUBLISH, bytes(writer.buf), PUBLISH_OK)
        reader = Reader(data)
        count = reader.u16()
        if count < 1:
            raise RuntimeError("publish was not stored")
        return reader.string(), reader.u16(), reader.i64()

    def ensure_consumer(self, stream, name):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.string("")
        writer.i32(30000)
        writer.i32(5)
        writer.i32(1000)
        writer.u8(0)
        writer.u8(0)
        writer.i64(0)
        self.request(ENSURE_CONSUMER, bytes(writer.buf), ENSURE_CONSUMER_OK)

    def fetch(self, stream, name):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.u16(32)
        writer.i32(0)
        reader = Reader(self.request(FETCH, bytes(writer.buf), FETCH_OK))
        if reader.u8() == 1:
            raise RuntimeError(f"offset reset at {reader.i64()}")
        reader.i64()
        messages = []
        for _ in range(reader.u16()):
            partition = reader.u16()
            offset = reader.i64()
            reader.i64()
            reader.i32()
            reader.string()
            reader.blob()
            for _header in range(reader.u16()):
                reader.string()
                reader.blob()
            messages.append((partition, offset, reader.blob()))
        return messages

    def ack(self, stream, name, partition, offset):
        writer = Writer()
        writer.string(stream)
        writer.string(name)
        writer.u16(partition)
        writer.i64(offset)
        self.request(ACK, bytes(writer.buf), ACK_OK)


def main():
    language = "python"
    body = f"hello from {language}".encode()
    client = Client(HOST, PORT)
    client.hello("guest", "guest", "/", f"sample-{language}")
    client.ensure_stream("clients", ["clients.>"])
    stream, partition, offset = client.publish("clients.created", body, language)
    print(f"published {stream} partition {partition} offset {offset}")
    consumer = f"demo-{language}"
    client.ensure_consumer("clients", consumer)
    while True:
        messages = client.fetch("clients", consumer)
        if not messages:
            raise SystemExit("published message was not delivered")
        found = False
        for part, message_offset, payload in messages:
            text = payload.decode()
            print(f"fetched {message_offset} {text}")
            client.ack("clients", consumer, part, message_offset)
            found = found or text == body.decode()
        if found:
            return


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(ex, file=sys.stderr)
        raise
