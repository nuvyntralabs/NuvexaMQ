// Stream, consume, exchange, ping, and management samples.
//   cargo run --bin feature -- ping

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;

const HELLO: u16 = 1;
const HELLO_OK: u16 = 2;
const ENSURE_STREAM: u16 = 3;
const ENSURE_STREAM_OK: u16 = 4;
const PUBLISH: u16 = 5;
const PUBLISH_OK: u16 = 6;
const ENSURE_CONSUMER: u16 = 7;
const ENSURE_CONSUMER_OK: u16 = 8;
const FETCH: u16 = 9;
const FETCH_OK: u16 = 10;
const ACK: u16 = 11;
const ACK_OK: u16 = 12;
const NACK: u16 = 13;
const NACK_OK: u16 = 14;
const RESET: u16 = 15;
const RESET_OK: u16 = 16;
const RELEASE: u16 = 17;
const RELEASE_OK: u16 = 18;
const PING: u16 = 19;
const PONG: u16 = 20;
const OP_ERROR: u16 = 21;
const DECLARE_EXCHANGE: u16 = 22;
const DECLARE_EXCHANGE_OK: u16 = 23;
const DECLARE_QUEUE: u16 = 24;
const DECLARE_QUEUE_OK: u16 = 25;
const BIND_QUEUE: u16 = 26;
const BIND_QUEUE_OK: u16 = 27;
const DELETE_QUEUE: u16 = 28;
const DELETE_QUEUE_OK: u16 = 29;
const DELETE_EXCHANGE: u16 = 30;
const DELETE_EXCHANGE_OK: u16 = 31;
const PURGE_QUEUE: u16 = 32;
const PURGE_QUEUE_OK: u16 = 33;
const PUBLISH_EXCHANGE: u16 = 34;
const PUBLISH_EXCHANGE_OK: u16 = 35;

struct Writer { buf: Vec<u8> }
impl Writer {
    fn new() -> Self { Self { buf: Vec::new() } }
    fn u8(&mut self, value: u32) { self.buf.push((value & 0xff) as u8); }
    fn u16(&mut self, value: u32) { self.buf.extend_from_slice(&(value as u16).to_le_bytes()); }
    fn i32(&mut self, value: i32) { self.buf.extend_from_slice(&value.to_le_bytes()); }
    fn i64(&mut self, value: i64) { self.buf.extend_from_slice(&value.to_le_bytes()); }
    fn str(&mut self, value: &str) { self.u16(value.len() as u32); self.buf.extend_from_slice(value.as_bytes()); }
    fn blob(&mut self, value: &[u8]) { self.i32(value.len() as i32); self.buf.extend_from_slice(value); }
}

struct Reader { data: Vec<u8>, i: usize }
impl Reader {
    fn take(&mut self, count: usize) -> Vec<u8> {
        if self.i + count > self.data.len() { panic!("frame ended early"); }
        let chunk = self.data[self.i..self.i + count].to_vec();
        self.i += count;
        chunk
    }
    fn u8(&mut self) -> u32 { self.take(1)[0] as u32 }
    fn u16(&mut self) -> u32 { u16::from_le_bytes(self.take(2).try_into().unwrap()) as u32 }
    fn i32(&mut self) -> i32 { i32::from_le_bytes(self.take(4).try_into().unwrap()) }
    fn i64(&mut self) -> i64 { i64::from_le_bytes(self.take(8).try_into().unwrap()) }
    fn str(&mut self) -> String { String::from_utf8(self.take(self.u16() as usize)).unwrap() }
    fn blob(&mut self) -> Vec<u8> { self.take(self.i32() as usize) }
}

struct Delivery { partition: u32, offset: i64, attempts: i32, payload: String }

struct Client { stream: TcpStream, next: u32 }
impl Client {
    fn request(&mut self, op: u16, payload: &[u8], expected: u16) -> Reader {
        self.next += 1;
        let mut body = Vec::with_capacity(6 + payload.len());
        body.extend_from_slice(&op.to_le_bytes());
        body.extend_from_slice(&self.next.to_le_bytes());
        body.extend_from_slice(payload);
        self.stream.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
        self.stream.write_all(&body).unwrap();
        let mut size_raw = [0u8; 4];
        self.stream.read_exact(&mut size_raw).unwrap();
        let mut frame = vec![0u8; u32::from_le_bytes(size_raw) as usize];
        self.stream.read_exact(&mut frame).unwrap();
        let kind = u16::from_le_bytes(frame[0..2].try_into().unwrap());
        let request_id = u32::from_le_bytes(frame[2..6].try_into().unwrap());
        if request_id != self.next { panic!("response id did not match the request"); }
        let mut reader = Reader { data: frame[6..].to_vec(), i: 0 };
        if kind == OP_ERROR { panic!("broker error {}: {}", reader.u16(), reader.str()); }
        if kind != expected { panic!("unexpected frame {kind}"); }
        reader
    }
    fn hello(&mut self) {
        let mut body = Writer::new();
        body.str(&env("NUVEXA_TOKEN", "")); body.str("sample-rust");
        body.str(&env("NUVEXA_USER", "guest")); body.str(&env("NUVEXA_PASSWORD", "guest")); body.str(&env("NUVEXA_VHOST", "/"));
        println!("hello {}", self.request(HELLO, &body.buf, HELLO_OK).str());
    }
    fn ping(&mut self) { self.request(PING, &[], PONG); println!("pong"); }
    fn ensure_stream(&mut self, name: &str, filter: &str, partitions: u32, max_age: i64, max_bytes: i64, max_message: i32) {
        let mut body = Writer::new();
        body.str(name); body.u16(1); body.str(filter); body.u16(partitions); body.i64(max_age); body.i64(max_bytes); body.i32(max_message);
        self.request(ENSURE_STREAM, &body.buf, ENSURE_STREAM_OK);
        println!("stream {name} partitions {partitions}");
    }
    fn publish(&mut self, subject: &str, payload: &str, key: &str, header: &str, header_value: &str) -> Vec<u32> {
        let mut body = Writer::new();
        body.str(subject); body.str(key);
        if header.is_empty() { body.u16(0); } else { body.u16(1); body.str(header); body.blob(header_value.as_bytes()); }
        body.blob(payload.as_bytes());
        self.receipts(self.request(PUBLISH, &body.buf, PUBLISH_OK))
    }
    fn ensure_consumer(&mut self, stream: &str, name: &str, filter: &str, ephemeral: bool, start: u32, offset: i64) {
        let mut body = Writer::new();
        body.str(stream); body.str(name); body.str(filter);
        body.i32(30000); body.i32(5); body.i32(1000); body.u8(if ephemeral { 1 } else { 0 }); body.u8(start); body.i64(offset);
        self.request(ENSURE_CONSUMER, &body.buf, ENSURE_CONSUMER_OK);
        println!("consumer {name} on {stream} start {start}");
    }
    fn fetch(&mut self, stream: &str, name: &str, max: u32) -> Vec<Delivery> {
        let mut body = Writer::new();
        body.str(stream); body.str(name); body.u16(max); body.i32(0);
        let mut reader = self.request(FETCH, &body.buf, FETCH_OK);
        if reader.u8() == 1 { panic!("offset reset"); }
        reader.i64();
        let mut messages = Vec::new();
        for _ in 0..reader.u16() {
            let partition = reader.u16();
            let offset = reader.i64();
            reader.i64();
            let attempts = reader.i32();
            reader.str(); reader.blob();
            for _ in 0..reader.u16() { reader.str(); reader.blob(); }
            messages.push(Delivery { partition, offset, attempts, payload: String::from_utf8(reader.blob()).unwrap() });
        }
        messages
    }
    fn ack(&mut self, stream: &str, name: &str, message: &Delivery) { self.cursor(ACK, ACK_OK, stream, name, message); }
    fn nack(&mut self, stream: &str, name: &str, message: &Delivery) { self.cursor(NACK, NACK_OK, stream, name, message); }
    fn reset(&mut self, stream: &str, name: &str, offset: i64) {
        let mut body = Writer::new();
        body.str(stream); body.str(name); body.u8(1); body.i64(offset);
        self.request(RESET, &body.buf, RESET_OK);
        println!("reset {name} offset {offset}");
    }
    fn release(&mut self, stream: &str, name: &str) {
        let mut body = Writer::new();
        body.str(stream); body.str(name);
        self.request(RELEASE, &body.buf, RELEASE_OK);
        println!("release {name}");
    }
    fn declare_exchange(&mut self, name: &str, kind: &str) {
        let mut body = Writer::new();
        body.str(name); body.str(kind); body.u8(1); body.u8(0);
        self.request(DECLARE_EXCHANGE, &body.buf, DECLARE_EXCHANGE_OK);
        println!("exchange {name} {kind}");
    }
    fn declare_queue(&mut self, name: &str, ttl: i64, max: i32, dead: &str, dead_key: &str) {
        let mut body = Writer::new();
        body.str(name); body.u8(1); body.u8(0); body.u8(0); body.i64(ttl); body.i32(max); body.str(dead); body.str(dead_key);
        self.request(DECLARE_QUEUE, &body.buf, DECLARE_QUEUE_OK);
        println!("queue {name}");
    }
    fn bind(&mut self, exchange: &str, queue: &str, key: &str, headers: bool) {
        let mut body = Writer::new();
        body.str(exchange); body.str(queue); body.str(key);
        if headers { body.u16(2); body.str("format"); body.str("json"); body.str("x-match"); body.str("all"); } else { body.u16(0); }
        self.request(BIND_QUEUE, &body.buf, BIND_QUEUE_OK);
    }
    fn publish_exchange(&mut self, exchange: &str, routing: &str, payload: &str, key: &str, header: bool) -> usize {
        let mut body = Writer::new();
        body.str(exchange); body.str(routing); body.str(key);
        if header { body.u16(1); body.str("format"); body.blob(b"json"); } else { body.u16(0); }
        body.blob(payload.as_bytes());
        let items = self.receipts(self.request(PUBLISH_EXCHANGE, &body.buf, PUBLISH_EXCHANGE_OK));
        println!("routed {exchange} -> {}", items.len());
        items.len()
    }
    fn named(&mut self, op: u16, expected: u16, name: &str, label: &str) {
        let mut body = Writer::new();
        body.str(name);
        self.request(op, &body.buf, expected);
        println!("{label}");
    }
    fn receipts(&mut self, mut reader: Reader) -> Vec<u32> {
        let mut partitions = Vec::new();
        for _ in 0..reader.u16() {
            let stream = reader.str();
            let partition = reader.u16();
            let offset = reader.i64();
            println!("published {stream} partition {partition} offset {offset}");
            partitions.push(partition);
        }
        partitions
    }
    fn cursor(&mut self, op: u16, expected: u16, stream: &str, name: &str, message: &Delivery) {
        let mut body = Writer::new();
        body.str(stream); body.str(name); body.u16(message.partition); body.i64(message.offset);
        self.request(op, &body.buf, expected);
    }
}

fn env(name: &str, fallback: &str) -> String {
    env::var(name).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| fallback.to_string())
}

fn http(method: &str, url: &str, body: Option<&str>, auth: bool) {
    let mut command = Command::new("curl");
    command.arg("-sk").arg("-o").arg("/tmp/nuvexa-rust-http").arg("-w").arg("%{http_code}").arg("-X").arg(method);
    if auth { command.arg("-u").arg(format!("{}:{}", env("NUVEXA_USER", "guest"), env("NUVEXA_PASSWORD", "guest"))); }
    if let Some(body) = body { command.arg("-H").arg("Content-Type: application/json").arg("--data").arg(body); }
    command.arg(url);
    let output = command.output().unwrap();
    let status = String::from_utf8_lossy(&output.stdout);
    let bytes = std::fs::metadata("/tmp/nuvexa-rust-http").map(|meta| meta.len()).unwrap_or(0);
    println!("{method} {url} {status} {bytes} bytes");
    if !output.status.success() || status.trim().parse::<u16>().unwrap_or(500) >= 400 { panic!("management request failed"); }
}

fn run_stream(client: &mut Client) {
    client.ensure_stream("catalog-rust", "catalog-rust.>", 2, 86_400_000, 1_048_576, 65_536);
    let keyed = client.publish("catalog-rust.created", "{\"id\":1}", "alpha", "content-type", "application/json");
    let first = client.publish("catalog-rust.created", "{\"id\":1}", "", "", "");
    let second = client.publish("catalog-rust.created", "{\"id\":1}", "", "", "");
    if keyed.is_empty() || first[0] == second[0] { panic!("round-robin did not use both partitions"); }
}

fn run_consume(client: &mut Client) {
    client.ensure_stream("mailbox-rust", "mailbox-rust.>", 1, -1, -1, 0);
    client.ensure_consumer("mailbox-rust", "box-rust", "mailbox-rust.>", false, 0, 0);
    client.publish("mailbox-rust.created", "ack me", "", "content-type", "text/plain");
    client.publish("mailbox-rust.created", "nack me", "", "content-type", "text/plain");
    let batch = client.fetch("mailbox-rust", "box-rust", 32);
    let acked = batch.iter().find(|message| message.payload == "ack me").unwrap();
    let nacked = batch.iter().find(|message| message.payload == "nack me").unwrap().clone_delivery();
    let acked = Delivery { partition: acked.partition, offset: acked.offset, attempts: acked.attempts, payload: acked.payload.clone() };
    client.ack("mailbox-rust", "box-rust", &acked);
    println!("ack");
    client.nack("mailbox-rust", "box-rust", &nacked);
    println!("nack");
    let again = client.fetch("mailbox-rust", "box-rust", 32).into_iter().find(|message| message.payload == "nack me").unwrap();
    println!("redelivered {}", again.attempts);
    client.ack("mailbox-rust", "box-rust", &again);
    client.reset("mailbox-rust", "box-rust", 0);
    let reset = client.fetch("mailbox-rust", "box-rust", 1).into_iter().next().unwrap();
    println!("after reset offset {}", reset.offset);
    client.ack("mailbox-rust", "box-rust", &reset);
    client.release("mailbox-rust", "tail-rust");
    client.ensure_consumer("mailbox-rust", "tail-rust", "", true, 1, 0);
    client.publish("mailbox-rust.created", "tail me", "", "", "");
    let tailed = client.fetch("mailbox-rust", "tail-rust", 32).into_iter().find(|message| message.payload == "tail me").unwrap();
    client.ack("mailbox-rust", "tail-rust", &tailed);
    client.release("mailbox-rust", "tail-rust");
    client.ensure_consumer("mailbox-rust", "from0-rust", "", false, 2, 0);
    println!("offset consumer from0-rust");
}

impl Delivery {
    fn clone_delivery(&self) -> Self { Self { partition: self.partition, offset: self.offset, attempts: self.attempts, payload: self.payload.clone() } }
}

fn run_exchange(client: &mut Client) {
    client.declare_exchange("amq.direct", "direct");
    client.declare_exchange("direct-rust", "direct");
    client.declare_exchange("fanout-rust", "fanout");
    client.declare_exchange("topic-rust", "topic");
    client.declare_exchange("headers-rust", "headers");
    client.declare_queue("dead-rust", -1, -1, "", "");
    client.declare_queue("work-rust", 60000, 100, "direct-rust", "expired");
    client.bind("direct-rust", "dead-rust", "expired", false);
    client.bind("direct-rust", "work-rust", "work.created", false);
    client.bind("fanout-rust", "work-rust", "", false);
    client.bind("topic-rust", "work-rust", "work.*", false);
    client.bind("headers-rust", "work-rust", "", true);
    let mut count = 0;
    count += client.publish_exchange("direct-rust", "work.created", "routed", "order-1", false);
    count += client.publish_exchange("fanout-rust", "", "routed", "", false);
    count += client.publish_exchange("topic-rust", "work.created", "routed", "", false);
    count += client.publish_exchange("headers-rust", "", "routed", "", true);
    count += client.publish_exchange("", "work-rust", "routed", "", false);
    if count < 5 { panic!("expected at least 5 receipts"); }
    client.named(PURGE_QUEUE, PURGE_QUEUE_OK, "work-rust", "purge queue work-rust");
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "work-rust", "delete queue work-rust");
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "dead-rust", "delete queue dead-rust");
    for name in ["direct-rust", "fanout-rust", "topic-rust", "headers-rust"] {
        client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, &format!("delete exchange {name}"));
    }
}

fn run_admin() {
    let host = env("NUVEXA_HOST", "127.0.0.1");
    let health = env("NUVEXA_HEALTH_PORT", "5762");
    let management = env("NUVEXA_MANAGEMENT_PORT", "5763");
    let https = env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764");
    http("GET", &format!("http://{host}:{health}/health"), None, false);
    http("GET", &format!("http://{host}:{health}/metrics"), None, false);
    let base = format!("http://{host}:{management}");
    for path in ["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"] {
        http("GET", &format!("{base}{path}"), None, true);
    }
    http("PUT", &format!("{base}/api/vhosts/vh-rust"), None, true);
    http("PUT", &format!("{base}/api/users/user-rust"), Some(r#"{"password":"sample-pass","tags":["management"]}"#), true);
    http("PUT", &format!("{base}/api/permissions"), Some(r#"{"user":"user-rust","vhost":"vh-rust","configure":".*","write":".*","read":".*"}"#), true);
    http("PUT", &format!("{base}/api/policies/policy-rust"), Some(r#"{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}"#), true);
    http("DELETE", &format!("{base}/api/policies/policy-rust?vhost=/"), None, true);
    http("DELETE", &format!("{base}/api/permissions?user=user-rust&vhost=vh-rust"), None, true);
    http("DELETE", &format!("{base}/api/users/user-rust"), None, true);
    http("DELETE", &format!("{base}/api/vhosts/vh-rust"), None, true);
    http("GET", &format!("https://{host}:{https}/api/whoami"), None, true);
}

fn main() {
    let command = env::args().nth(1).unwrap_or_else(|| "ping".to_string());
    if command == "admin" { run_admin(); return; }
    let address = format!("{}:{}", env("NUVEXA_HOST", "127.0.0.1"), env("NUVEXA_PORT", "5761"));
    let mut client = Client { stream: TcpStream::connect(address).unwrap(), next: 0 };
    client.hello();
    match command.as_str() {
        "ping" => client.ping(),
        "stream" => run_stream(&mut client),
        "consume" => run_consume(&mut client),
        "exchange" => run_exchange(&mut client),
        _ => panic!("use ping, stream, consume, exchange, or admin"),
    }
}
