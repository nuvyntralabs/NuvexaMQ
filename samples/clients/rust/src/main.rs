// Publish one message, then fetch and ack it.
//   cargo run
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;

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
const OP_ERROR: u16 = 21;

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
}

fn env(name: &str, fallback: &str) -> String {
    env::var(name).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| fallback.to_string())
}

fn main() {
    let language = "rust";
    let text = format!("hello from {language}");
    let address = format!("{}:{}", env("NUVEXA_HOST", "127.0.0.1"), env("NUVEXA_PORT", "5761"));
    let mut client = Client { stream: TcpStream::connect(address).unwrap(), next: 0 };
    let mut hello = Writer::new();
    hello.str(""); hello.str("sample-rust"); hello.str("guest"); hello.str("guest"); hello.str("/");
    client.request(HELLO, &hello.buf, HELLO_OK);
    let mut stream = Writer::new();
    stream.str("clients"); stream.u16(1); stream.str("clients.>"); stream.u16(1); stream.i64(-1); stream.i64(-1); stream.i32(0);
    client.request(ENSURE_STREAM, &stream.buf, ENSURE_STREAM_OK);
    let mut publish = Writer::new();
    publish.str("clients.created"); publish.str(language); publish.u16(0); publish.blob(text.as_bytes());
    let mut published = client.request(PUBLISH, &publish.buf, PUBLISH_OK);
    if published.u16() < 1 { panic!("publish was not stored"); }
    println!("published {} partition {} offset {}", published.str(), published.u16(), published.i64());
    let mut consumer = Writer::new();
    consumer.str("clients"); consumer.str("demo-rust"); consumer.str("");
    consumer.i32(30000); consumer.i32(5); consumer.i32(1000); consumer.u8(0); consumer.u8(0); consumer.i64(0);
    client.request(ENSURE_CONSUMER, &consumer.buf, ENSURE_CONSUMER_OK);
    let mut found = false;
    while !found {
        let mut fetch = Writer::new();
        fetch.str("clients"); fetch.str("demo-rust"); fetch.u16(32); fetch.i32(0);
        let mut messages = client.request(FETCH, &fetch.buf, FETCH_OK);
        if messages.u8() == 1 { panic!("offset reset"); }
        messages.i64();
        let count = messages.u16();
        if count == 0 { panic!("published message was not delivered"); }
        for _ in 0..count {
            let part = messages.u16();
            let offset = messages.i64();
            messages.i64(); messages.i32(); messages.str(); messages.blob();
            for _ in 0..messages.u16() { messages.str(); messages.blob(); }
            let payload = String::from_utf8(messages.blob()).unwrap();
            println!("fetched {offset} {payload}");
            let mut ack = Writer::new();
            ack.str("clients"); ack.str("demo-rust"); ack.u16(part); ack.i64(offset);
            client.request(ACK, &ack.buf, ACK_OK);
            found = found || payload == text;
        }
    }
}
