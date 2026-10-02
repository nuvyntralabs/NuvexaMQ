// Publish one message, then fetch and ack it.
//   node demo.mjs
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

import net from "node:net";

const host = process.env.NUVEXA_HOST ?? "127.0.0.1";
const port = Number(process.env.NUVEXA_PORT ?? "5761");
const HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4;
const PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8;
const FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, ERROR = 21;

class Writer {
  constructor() { this.parts = []; }
  u8(value) { this.parts.push(Buffer.from([value & 0xff])); }
  u16(value) { const b = Buffer.alloc(2); b.writeUInt16LE(value); this.parts.push(b); }
  i32(value) { const b = Buffer.alloc(4); b.writeInt32LE(value); this.parts.push(b); }
  i64(value) { const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(value)); this.parts.push(b); }
  string(value) { const raw = Buffer.from(value); this.u16(raw.length); this.parts.push(raw); }
  blob(value) { this.i32(value.length); this.parts.push(value); }
  bytes() { return Buffer.concat(this.parts); }
}

class Reader {
  constructor(buffer) { this.buffer = buffer; this.i = 0; }
  take(count) {
    if (this.i + count > this.buffer.length) throw new Error("frame ended early");
    const chunk = this.buffer.subarray(this.i, this.i + count);
    this.i += count;
    return chunk;
  }
  u8() { return this.take(1)[0]; }
  u16() { return this.take(2).readUInt16LE(0); }
  i32() { return this.take(4).readInt32LE(0); }
  i64() { return this.take(8).readBigInt64LE(0); }
  string() { return this.take(this.u16()).toString(); }
  blob() { return this.take(this.i32()); }
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.nextId = 0;
    this.pending = Buffer.alloc(0);
    this.waiters = [];
    socket.on("data", (chunk) => {
      this.pending = Buffer.concat([this.pending, chunk]);
      this.pump();
    });
    socket.on("close", () => {
      for (const waiter of this.waiters) waiter.reject(new Error("connection closed"));
      this.waiters = [];
    });
  }
  pump() {
    while (this.waiters.length && this.pending.length >= this.waiters[0].count) {
      const waiter = this.waiters.shift();
      const chunk = this.pending.subarray(0, waiter.count);
      this.pending = this.pending.subarray(waiter.count);
      waiter.resolve(chunk);
    }
  }
  readExact(count) {
    return new Promise((resolve, reject) => {
      this.waiters.push({ count, resolve, reject });
      this.pump();
    });
  }
  async request(op, payload, expected) {
    this.nextId += 1;
    const head = Buffer.alloc(6);
    head.writeUInt16LE(op, 0);
    head.writeUInt32LE(this.nextId, 2);
    const body = Buffer.concat([head, payload]);
    const length = Buffer.alloc(4);
    length.writeUInt32LE(body.length, 0);
    this.socket.write(Buffer.concat([length, body]));
    const size = (await this.readExact(4)).readUInt32LE(0);
    const frame = await this.readExact(size);
    const kind = frame.readUInt16LE(0);
    const requestId = frame.readUInt32LE(2);
    const data = frame.subarray(6);
    if (requestId !== this.nextId) throw new Error("response id did not match the request");
    if (kind === ERROR) {
      const reader = new Reader(data);
      throw new Error(`broker error ${reader.u16()}: ${reader.string()}`);
    }
    if (kind !== expected) throw new Error(`unexpected frame ${kind}`);
    return data;
  }
  hello(user, password, vhost, name) {
    const writer = new Writer();
    writer.string(""); writer.string(name); writer.string(user); writer.string(password); writer.string(vhost);
    return this.request(HELLO, writer.bytes(), HELLO_OK);
  }
  ensureStream(name, filters) {
    const writer = new Writer();
    writer.string(name); writer.u16(filters.length);
    for (const filter of filters) writer.string(filter);
    writer.u16(1); writer.i64(-1); writer.i64(-1); writer.i32(0);
    return this.request(ENSURE_STREAM, writer.bytes(), ENSURE_STREAM_OK);
  }
  async publish(subject, payload, key) {
    const writer = new Writer();
    writer.string(subject); writer.string(key); writer.u16(0); writer.blob(payload);
    const reader = new Reader(await this.request(PUBLISH, writer.bytes(), PUBLISH_OK));
    if (reader.u16() < 1) throw new Error("publish was not stored");
    return { stream: reader.string(), partition: reader.u16(), offset: reader.i64() };
  }
  ensureConsumer(stream, name) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.string("");
    writer.i32(30000); writer.i32(5); writer.i32(1000); writer.u8(0); writer.u8(0); writer.i64(0);
    return this.request(ENSURE_CONSUMER, writer.bytes(), ENSURE_CONSUMER_OK);
  }
  async fetch(stream, name) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.u16(32); writer.i32(0);
    const reader = new Reader(await this.request(FETCH, writer.bytes(), FETCH_OK));
    if (reader.u8() === 1) throw new Error(`offset reset at ${reader.i64()}`);
    reader.i64();
    const messages = [];
    const count = reader.u16();
    for (let i = 0; i < count; i++) {
      const partition = reader.u16();
      const offset = reader.i64();
      reader.i64(); reader.i32(); reader.string(); reader.blob();
      const headers = reader.u16();
      for (let h = 0; h < headers; h++) { reader.string(); reader.blob(); }
      messages.push({ partition, offset, payload: reader.blob() });
    }
    return messages;
  }
  ack(stream, name, partition, offset) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.u16(partition); writer.i64(offset);
    return this.request(ACK, writer.bytes(), ACK_OK);
  }
}

const language = "javascript";
const body = Buffer.from(`hello from ${language}`);
const socket = net.connect({ host, port });
await new Promise((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
const client = new Client(socket);
await client.hello("guest", "guest", "/", `sample-${language}`);
await client.ensureStream("clients", ["clients.>"]);
const receipt = await client.publish("clients.created", body, language);
console.log(`published ${receipt.stream} partition ${receipt.partition} offset ${receipt.offset}`);
const consumer = `demo-${language}`;
await client.ensureConsumer("clients", consumer);
let found = false;
while (!found) {
  const messages = await client.fetch("clients", consumer);
  if (messages.length === 0) throw new Error("published message was not delivered");
  for (const message of messages) {
    const text = message.payload.toString();
    console.log(`fetched ${message.offset} ${text}`);
    await client.ack("clients", consumer, message.partition, message.offset);
    found = found || text === body.toString();
  }
}
socket.end();
