// Stream, consume, exchange, ping, and management samples.
//   swift feature.swift ping

import Darwin
import Foundation

let HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4
let PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8
let FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, NACK = 13, NACK_OK = 14
let RESET = 15, RESET_OK = 16, RELEASE = 17, RELEASE_OK = 18, PING = 19, PONG = 20, OP_ERROR = 21
let DECLARE_EXCHANGE = 22, DECLARE_EXCHANGE_OK = 23, DECLARE_QUEUE = 24, DECLARE_QUEUE_OK = 25
let BIND_QUEUE = 26, BIND_QUEUE_OK = 27, DELETE_QUEUE = 28, DELETE_QUEUE_OK = 29
let DELETE_EXCHANGE = 30, DELETE_EXCHANGE_OK = 31, PURGE_QUEUE = 32, PURGE_QUEUE_OK = 33
let PUBLISH_EXCHANGE = 34, PUBLISH_EXCHANGE_OK = 35

func fail(_ message: String) -> Never { fputs(message + "\n", stderr); exit(1) }
func env(_ name: String, _ fallback: String) -> String {
    guard let value = ProcessInfo.processInfo.environment[name], !value.isEmpty else { return fallback }
    return value
}

struct Writer {
    var buf = Data()
    mutating func u8(_ value: Int) { buf.append(UInt8(value & 0xff)) }
    mutating func u16(_ value: Int) { u8(value); u8(value >> 8) }
    mutating func i32(_ value: Int) { for i in 0..<4 { u8(value >> (8 * i)) } }
    mutating func i64(_ value: Int64) { for i in 0..<8 { u8(Int(truncatingIfNeeded: value >> (8 * i))) } }
    mutating func str(_ value: String) { let raw = Array(value.utf8); u16(raw.count); buf.append(contentsOf: raw) }
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
    mutating func i32() -> Int { let p = Array(take(4)); var n = 0; for shift in 0..<4 { n |= Int(p[shift]) << (8 * shift) }; return n }
    mutating func i64() -> Int64 {
        let p = Array(take(8)); var n: UInt64 = 0
        for shift in 0..<8 { n |= UInt64(p[shift]) << (8 * shift) }
        return Int64(bitPattern: n)
    }
    mutating func str() -> String { String(bytes: take(u16()), encoding: .utf8) ?? "" }
    mutating func blob() -> [UInt8] { Array(take(i32())) }
}

struct Receipt { var stream: String; var partition: Int; var offset: Int64 }
struct Delivery { var partition: Int; var offset: Int64; var attempts: Int; var payload: String }

func readExact(_ fd: Int32, _ count: Int) -> [UInt8] {
    var stored: [UInt8] = []
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
        if !payload.buf.isEmpty { frame.replaceSubrange(10..<frame.count, with: payload.buf) }
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
    func hello() {
        var body = Writer()
        body.str(env("NUVEXA_TOKEN", "")); body.str("sample-swift")
        body.str(env("NUVEXA_USER", "guest")); body.str(env("NUVEXA_PASSWORD", "guest")); body.str(env("NUVEXA_VHOST", "/"))
        var reader = request(HELLO, body, HELLO_OK)
        print("hello \(reader.str())")
    }
    func ping() { _ = request(PING, Writer(), PONG); print("pong") }
    func ensureStream(_ name: String, _ filter: String, _ partitions: Int, _ maxAge: Int64, _ maxBytes: Int64, _ maxMessage: Int) {
        var body = Writer()
        body.str(name); body.u16(1); body.str(filter); body.u16(partitions); body.i64(maxAge); body.i64(maxBytes); body.i32(maxMessage)
        _ = request(ENSURE_STREAM, body, ENSURE_STREAM_OK)
        print("stream \(name) partitions \(partitions)")
    }
    func publish(_ subject: String, _ payload: String, _ key: String, _ header: String, _ headerValue: String) -> [Receipt] {
        var body = Writer()
        body.str(subject); body.str(key)
        if header.isEmpty { body.u16(0) } else { body.u16(1); body.str(header); body.blob(Array(headerValue.utf8)) }
        body.blob(Array(payload.utf8))
        return receipts(request(PUBLISH, body, PUBLISH_OK))
    }
    func ensureConsumer(_ stream: String, _ name: String, _ filter: String, _ ephemeral: Bool, _ start: Int, _ offset: Int64) {
        var body = Writer()
        body.str(stream); body.str(name); body.str(filter)
        body.i32(30000); body.i32(5); body.i32(1000); body.u8(ephemeral ? 1 : 0); body.u8(start); body.i64(offset)
        _ = request(ENSURE_CONSUMER, body, ENSURE_CONSUMER_OK)
        print("consumer \(name) on \(stream) start \(start)")
    }
    func fetch(_ stream: String, _ name: String, _ max: Int) -> [Delivery] {
        var body = Writer()
        body.str(stream); body.str(name); body.u16(max); body.i32(0)
        var reader = request(FETCH, body, FETCH_OK)
        if reader.u8() == 1 { fail("offset reset") }
        _ = reader.i64()
        var messages: [Delivery] = []
        for _ in 0..<reader.u16() {
            let partition = reader.u16()
            let offset = reader.i64()
            _ = reader.i64()
            let attempts = reader.i32()
            _ = reader.str(); _ = reader.blob()
            for _ in 0..<reader.u16() { _ = reader.str(); _ = reader.blob() }
            let payload = String(bytes: reader.blob(), encoding: .utf8) ?? ""
            messages.append(Delivery(partition: partition, offset: offset, attempts: attempts, payload: payload))
        }
        return messages
    }
    func ack(_ stream: String, _ name: String, _ message: Delivery) { cursor(ACK, ACK_OK, stream, name, message) }
    func nack(_ stream: String, _ name: String, _ message: Delivery) { cursor(NACK, NACK_OK, stream, name, message) }
    func reset(_ stream: String, _ name: String, _ offset: Int64) {
        var body = Writer(); body.str(stream); body.str(name); body.u8(1); body.i64(offset)
        _ = request(RESET, body, RESET_OK)
        print("reset \(name) offset \(offset)")
    }
    func release(_ stream: String, _ name: String) {
        var body = Writer(); body.str(stream); body.str(name)
        _ = request(RELEASE, body, RELEASE_OK)
        print("release \(name)")
    }
    func declareExchange(_ name: String, _ type: String) {
        var body = Writer(); body.str(name); body.str(type); body.u8(1); body.u8(0)
        _ = request(DECLARE_EXCHANGE, body, DECLARE_EXCHANGE_OK)
        print("exchange \(name) \(type)")
    }
    func declareQueue(_ name: String, _ ttl: Int64, _ max: Int, _ dead: String, _ deadKey: String) {
        var body = Writer(); body.str(name); body.u8(1); body.u8(0); body.u8(0); body.i64(ttl); body.i32(max); body.str(dead); body.str(deadKey)
        _ = request(DECLARE_QUEUE, body, DECLARE_QUEUE_OK)
        print("queue \(name)")
    }
    func bind(_ exchange: String, _ queue: String, _ key: String, _ headers: Bool) {
        var body = Writer(); body.str(exchange); body.str(queue); body.str(key)
        if headers { body.u16(2); body.str("format"); body.str("json"); body.str("x-match"); body.str("all") } else { body.u16(0) }
        _ = request(BIND_QUEUE, body, BIND_QUEUE_OK)
    }
    func publishExchange(_ exchange: String, _ routing: String, _ payload: String, _ key: String, _ header: Bool) -> Int {
        var body = Writer(); body.str(exchange); body.str(routing); body.str(key)
        if header { body.u16(1); body.str("format"); body.blob(Array("json".utf8)) } else { body.u16(0) }
        body.blob(Array(payload.utf8))
        let items = receipts(request(PUBLISH_EXCHANGE, body, PUBLISH_EXCHANGE_OK))
        print("routed \(exchange) -> \(items.count)")
        return items.count
    }
    func named(_ op: Int, _ expected: Int, _ name: String, _ label: String) {
        var body = Writer(); body.str(name); _ = request(op, body, expected); print(label)
    }
    func receipts(_ readerIn: Reader) -> [Receipt] {
        var reader = readerIn
        var items: [Receipt] = []
        for _ in 0..<reader.u16() {
            let item = Receipt(stream: reader.str(), partition: reader.u16(), offset: reader.i64())
            print("published \(item.stream) partition \(item.partition) offset \(item.offset)")
            items.append(item)
        }
        return items
    }
    func cursor(_ op: Int, _ expected: Int, _ stream: String, _ name: String, _ message: Delivery) {
        var body = Writer(); body.str(stream); body.str(name); body.u16(message.partition); body.i64(message.offset)
        _ = request(op, body, expected)
    }
}

final class TrustAll: NSObject, URLSessionDelegate {
    func urlSession(_ session: URLSession, didReceive challenge: URLAuthenticationChallenge, completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
        if let trust = challenge.protectionSpace.serverTrust { completionHandler(.useCredential, URLCredential(trust: trust)) }
        else { completionHandler(.performDefaultHandling, nil) }
    }
}

func http(_ method: String, _ url: String, _ body: String?, _ auth: Bool) {
    let semaphore = DispatchSemaphore(value: 0)
    var request = URLRequest(url: URL(string: url)!)
    request.httpMethod = method
    if let body { request.httpBody = body.data(using: .utf8); request.setValue("application/json", forHTTPHeaderField: "Content-Type") }
    if auth && url.contains("/api/") {
        let token = Data("\(env("NUVEXA_USER", "guest")):\(env("NUVEXA_PASSWORD", "guest"))".utf8).base64EncodedString()
        request.setValue("Basic \(token)", forHTTPHeaderField: "Authorization")
    }
    let session = URLSession(configuration: .ephemeral, delegate: TrustAll(), delegateQueue: nil)
    var failed = ""
    session.dataTask(with: request) { data, response, error in
        if let error { failed = error.localizedDescription }
        else if let http = response as? HTTPURLResponse {
            let count = data?.count ?? 0
            print("\(method) \(url) \(http.statusCode) \(count) bytes")
            if http.statusCode >= 400 { failed = String(data: data ?? Data(), encoding: .utf8) ?? "request failed" }
        } else { failed = "no response" }
        semaphore.signal()
    }.resume()
    semaphore.wait()
    session.finishTasksAndInvalidate()
    if !failed.isEmpty { fail(failed) }
}

func runStream(_ client: Client) {
    client.ensureStream("catalog-swift", "catalog-swift.>", 2, 86_400_000, 1_048_576, 65_536)
    let keyed = client.publish("catalog-swift.created", "{\"id\":1}", "alpha", "content-type", "application/json")
    let first = client.publish("catalog-swift.created", "{\"id\":1}", "", "", "")
    let second = client.publish("catalog-swift.created", "{\"id\":1}", "", "", "")
    if keyed.isEmpty || first[0].partition == second[0].partition { fail("round-robin did not use both partitions") }
}

func runConsume(_ client: Client) {
    client.ensureStream("mailbox-swift", "mailbox-swift.>", 1, -1, -1, 0)
    client.ensureConsumer("mailbox-swift", "box-swift", "mailbox-swift.>", false, 0, 0)
    _ = client.publish("mailbox-swift.created", "ack me", "", "content-type", "text/plain")
    _ = client.publish("mailbox-swift.created", "nack me", "", "content-type", "text/plain")
    let batch = client.fetch("mailbox-swift", "box-swift", 32)
    guard let acked = batch.first(where: { $0.payload == "ack me" }), let nacked = batch.first(where: { $0.payload == "nack me" }) else { fail("expected both deliveries") }
    client.ack("mailbox-swift", "box-swift", acked); print("ack")
    client.nack("mailbox-swift", "box-swift", nacked); print("nack")
    guard let again = client.fetch("mailbox-swift", "box-swift", 32).first(where: { $0.payload == "nack me" }) else { fail("nack was not redelivered") }
    print("redelivered \(again.attempts)")
    client.ack("mailbox-swift", "box-swift", again)
    client.reset("mailbox-swift", "box-swift", 0)
    let reset = client.fetch("mailbox-swift", "box-swift", 1)
    if reset.isEmpty { fail("reset did not return a message") }
    print("after reset offset \(reset[0].offset)")
    client.ack("mailbox-swift", "box-swift", reset[0])
    client.release("mailbox-swift", "tail-swift")
    client.ensureConsumer("mailbox-swift", "tail-swift", "", true, 1, 0)
    _ = client.publish("mailbox-swift.created", "tail me", "", "", "")
    guard let tailed = client.fetch("mailbox-swift", "tail-swift", 32).first(where: { $0.payload == "tail me" }) else { fail("tail message was not delivered") }
    client.ack("mailbox-swift", "tail-swift", tailed)
    client.release("mailbox-swift", "tail-swift")
    client.ensureConsumer("mailbox-swift", "from0-swift", "", false, 2, 0)
    print("offset consumer from0-swift")
}

func runExchange(_ client: Client) {
    client.declareExchange("amq.direct", "direct")
    client.declareExchange("direct-swift", "direct")
    client.declareExchange("fanout-swift", "fanout")
    client.declareExchange("topic-swift", "topic")
    client.declareExchange("headers-swift", "headers")
    client.declareQueue("dead-swift", -1, -1, "", "")
    client.declareQueue("work-swift", 60000, 100, "direct-swift", "expired")
    client.bind("direct-swift", "dead-swift", "expired", false)
    client.bind("direct-swift", "work-swift", "work.created", false)
    client.bind("fanout-swift", "work-swift", "", false)
    client.bind("topic-swift", "work-swift", "work.*", false)
    client.bind("headers-swift", "work-swift", "", true)
    var count = 0
    count += client.publishExchange("direct-swift", "work.created", "routed", "order-1", false)
    count += client.publishExchange("fanout-swift", "", "routed", "", false)
    count += client.publishExchange("topic-swift", "work.created", "routed", "", false)
    count += client.publishExchange("headers-swift", "", "routed", "", true)
    count += client.publishExchange("", "work-swift", "routed", "", false)
    if count < 5 { fail("expected at least 5 receipts") }
    client.named(PURGE_QUEUE, PURGE_QUEUE_OK, "work-swift", "purge queue work-swift")
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "work-swift", "delete queue work-swift")
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "dead-swift", "delete queue dead-swift")
    for name in ["direct-swift", "fanout-swift", "topic-swift", "headers-swift"] {
        client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, "delete exchange \(name)")
    }
}

func runAdmin() {
    let host = env("NUVEXA_HOST", "127.0.0.1")
    let health = "http://\(host):\(env("NUVEXA_HEALTH_PORT", "5762"))"
    http("GET", "\(health)/health", nil, false)
    http("GET", "\(health)/metrics", nil, false)
    let base = "http://\(host):\(env("NUVEXA_MANAGEMENT_PORT", "5763"))"
    for path in ["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"] {
        http("GET", base + path, nil, true)
    }
    http("PUT", "\(base)/api/vhosts/vh-swift", nil, true)
    http("PUT", "\(base)/api/users/user-swift", "{\"password\":\"sample-pass\",\"tags\":[\"management\"]}", true)
    http("PUT", "\(base)/api/permissions", "{\"user\":\"user-swift\",\"vhost\":\"vh-swift\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}", true)
    http("PUT", "\(base)/api/policies/policy-swift", "{\"vhost\":\"/\",\"pattern\":\"sample-.*\",\"priority\":1,\"messageTtlMs\":60000,\"maxLength\":100,\"deadLetterExchange\":\"\",\"deadLetterRoutingKey\":\"\"}", true)
    http("DELETE", "\(base)/api/policies/policy-swift?vhost=/", nil, true)
    http("DELETE", "\(base)/api/permissions?user=user-swift&vhost=vh-swift", nil, true)
    http("DELETE", "\(base)/api/users/user-swift", nil, true)
    http("DELETE", "\(base)/api/vhosts/vh-swift", nil, true)
    http("GET", "https://\(host):\(env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"))/api/whoami", nil, true)
}

let command = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "ping"
if command == "admin" { runAdmin(); exit(0) }
let client = Client(connectHost(env("NUVEXA_HOST", "127.0.0.1"), env("NUVEXA_PORT", "5761")))
client.hello()
switch command {
case "ping": client.ping()
case "stream": runStream(client)
case "consume": runConsume(client)
case "exchange": runExchange(client)
default: fail("use ping, stream, consume, exchange, or admin")
}
close(client.fd)
