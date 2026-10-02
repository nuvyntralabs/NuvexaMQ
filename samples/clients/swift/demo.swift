// Publish one message, then fetch and ack it.
//   swift demo.swift
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
// The same frames are what an iOS app would send.

import Darwin
import Foundation

let HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4
let PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8
let FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, OP_ERROR = 21

func fail(_ message: String) -> Never { fputs(message + "\n", stderr); exit(1) }

struct Writer {
    var buf = Data()
    mutating func u8(_ value: Int) { buf.append(UInt8(value & 0xff)) }
    mutating func u16(_ value: Int) { u8(value); u8(value >> 8) }
    mutating func i32(_ value: Int) { for i in 0..<4 { u8(value >> (8 * i)) } }
    mutating func i64(_ value: Int64) { for i in 0..<8 { u8(Int(truncatingIfNeeded: value >> (8 * i))) } }
    mutating func str(_ value: String) {
        let raw = Array(value.utf8)
        u16(raw.count)
        buf.append(contentsOf: raw)
    }
    mutating func blob(_ value: [UInt8]) { i32(value.count); buf.append(contentsOf: value) }
}

struct Reader {
    let data: [UInt8]
    var i = 0
    mutating func take(_ count: Int) -> ArraySlice<UInt8> {
        if i + count > data.count { fail("frame ended early") }
        let chunk = data[i..<(i + count)]
        i += count
        return chunk
    }
    mutating func u8() -> Int { Int(take(1).first!) }
    mutating func u16() -> Int { let p = Array(take(2)); return Int(p[0]) | (Int(p[1]) << 8) }
    mutating func i32() -> Int {
        let p = Array(take(4))
        var n = 0
        for shift in 0..<4 { n |= Int(p[shift]) << (8 * shift) }
        return n
    }
    mutating func i64() -> Int64 {
        let p = Array(take(8))
        var n: UInt64 = 0
        for shift in 0..<8 { n |= UInt64(p[shift]) << (8 * shift) }
        return Int64(bitPattern: n)
    }
    mutating func str() -> String { String(bytes: take(u16()), encoding: .utf8) ?? "" }
    mutating func blob() -> [UInt8] { Array(take(i32())) }
}

func readExact(_ fd: Int32, _ count: Int) -> [UInt8] {
    var stored: [UInt8] = []
    stored.reserveCapacity(count)
    while stored.count < count {
        var buf = [UInt8](repeating: 0, count: count - stored.count)
        let n = buf.withUnsafeMutableBytes { Darwin.read(fd, $0.baseAddress, $0.count) }
        if n <= 0 { fail("connection closed") }
        stored.append(contentsOf: buf.prefix(n))
    }
    return stored
}

func connectHost(_ host: String, _ port: String) -> Int32 {
    var hints = addrinfo()
    hints.ai_family = AF_UNSPEC
    hints.ai_socktype = SOCK_STREAM
    var res: UnsafeMutablePointer<addrinfo>?
    if getaddrinfo(host, port, &hints, &res) != 0 { fail("could not resolve host") }
    defer { freeaddrinfo(res) }
    var item = res
    while let current = item {
        let fd = socket(current.pointee.ai_family, current.pointee.ai_socktype, current.pointee.ai_protocol)
        if fd >= 0 && connect(fd, current.pointee.ai_addr, current.pointee.ai_addrlen) == 0 { return fd }
        if fd >= 0 { close(fd) }
        item = current.pointee.ai_next
    }
    fail("could not connect")
}

final class Client {
    let fd: Int32
    var next: UInt32 = 0
    init(_ fd: Int32) { self.fd = fd }
    func request(_ op: Int, _ payload: Writer, _ expected: Int) -> Reader {
        next += 1
        var frame = Data(count: 10 + payload.buf.count)
        let length = UInt32(6 + payload.buf.count)
        for i in 0..<4 { frame[i] = UInt8((length >> (8 * i)) & 0xff) }
        frame[4] = UInt8(op & 0xff); frame[5] = UInt8((op >> 8) & 0xff)
        for i in 0..<4 { frame[6 + i] = UInt8((next >> (8 * i)) & 0xff) }
        frame.replaceSubrange(10..<frame.count, with: payload.buf)
        let bytes = Array(frame)
        var sent = 0
        while sent < bytes.count {
            let n = bytes[sent...].withUnsafeBytes { Darwin.write(fd, $0.baseAddress, $0.count) }
            if n <= 0 { fail("write failed") }
            sent += n
        }
        let sizeRaw = readExact(fd, 4)
        let size = Int(sizeRaw[0]) | (Int(sizeRaw[1]) << 8) | (Int(sizeRaw[2]) << 16) | (Int(sizeRaw[3]) << 24)
        let body = readExact(fd, size)
        let kind = Int(body[0]) | (Int(body[1]) << 8)
        let requestId = UInt32(body[2]) | (UInt32(body[3]) << 8) | (UInt32(body[4]) << 16) | (UInt32(body[5]) << 24)
        if requestId != next { fail("response id did not match the request") }
        var reader = Reader(data: Array(body[6...]))
        if kind == OP_ERROR { fail("broker error \(reader.u16()): \(reader.str())") }
        if kind != expected { fail("unexpected frame \(kind)") }
        return reader
    }
}

let host = ProcessInfo.processInfo.environment["NUVEXA_HOST"].flatMap { $0.isEmpty ? nil : $0 } ?? "127.0.0.1"
let port = ProcessInfo.processInfo.environment["NUVEXA_PORT"].flatMap { $0.isEmpty ? nil : $0 } ?? "5761"
let language = "swift"
let body = Array("hello from \(language)".utf8)
let client = Client(connectHost(host, port))
var hello = Writer()
hello.str(""); hello.str("sample-swift"); hello.str("guest"); hello.str("guest"); hello.str("/")
_ = client.request(HELLO, hello, HELLO_OK)
var stream = Writer()
stream.str("clients"); stream.u16(1); stream.str("clients.>"); stream.u16(1); stream.i64(-1); stream.i64(-1); stream.i32(0)
_ = client.request(ENSURE_STREAM, stream, ENSURE_STREAM_OK)
var publish = Writer()
publish.str("clients.created"); publish.str(language); publish.u16(0); publish.blob(body)
var published = client.request(PUBLISH, publish, PUBLISH_OK)
if published.u16() < 1 { fail("publish was not stored") }
let streamName = published.str()
let partition = published.u16()
let offset = published.i64()
print("published \(streamName) partition \(partition) offset \(offset)")
var consumer = Writer()
consumer.str("clients"); consumer.str("demo-swift"); consumer.str("")
consumer.i32(30000); consumer.i32(5); consumer.i32(1000); consumer.u8(0); consumer.u8(0); consumer.i64(0)
_ = client.request(ENSURE_CONSUMER, consumer, ENSURE_CONSUMER_OK)
var found = false
while !found {
    var fetch = Writer()
    fetch.str("clients"); fetch.str("demo-swift"); fetch.u16(32); fetch.i32(0)
    var messages = client.request(FETCH, fetch, FETCH_OK)
    if messages.u8() == 1 { fail("offset reset") }
    _ = messages.i64()
    let count = messages.u16()
    if count == 0 { fail("published message was not delivered") }
    for _ in 0..<count {
        let part = messages.u16()
        let messageOffset = messages.i64()
        _ = messages.i64(); _ = messages.i32(); _ = messages.str(); _ = messages.blob()
        let headers = messages.u16()
        for _ in 0..<headers { _ = messages.str(); _ = messages.blob() }
        let payload = messages.blob()
        let text = String(bytes: payload, encoding: .utf8) ?? ""
        print("fetched \(messageOffset) \(text)")
        var ack = Writer()
        ack.str("clients"); ack.str("demo-swift"); ack.u16(part); ack.i64(messageOffset)
        _ = client.request(ACK, ack, ACK_OK)
        found = found || text == "hello from \(language)"
    }
}
close(client.fd)
