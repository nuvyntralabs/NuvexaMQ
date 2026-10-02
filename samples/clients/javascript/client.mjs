import net from "node:net";

export const host = process.env.NUVEXA_HOST ?? "127.0.0.1";
export const port = Number(process.env.NUVEXA_PORT ?? "5761");
export const healthPort = Number(process.env.NUVEXA_HEALTH_PORT ?? "5762");
export const managementPort = Number(process.env.NUVEXA_MANAGEMENT_PORT ?? "5763");
export const managementHttpsPort = Number(process.env.NUVEXA_MANAGEMENT_HTTPS_PORT ?? "5764");
export const token = process.env.NUVEXA_TOKEN ?? "";
export const user = process.env.NUVEXA_USER ?? "guest";
export const password = process.env.NUVEXA_PASSWORD ?? "guest";
export const vhost = process.env.NUVEXA_VHOST ?? "/";

export const HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4;
export const PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8;
export const FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, NACK = 13, NACK_OK = 14;
export const RESET = 15, RESET_OK = 16, RELEASE = 17, RELEASE_OK = 18, PING = 19, PONG = 20, ERROR = 21;
export const DECLARE_EXCHANGE = 22, DECLARE_EXCHANGE_OK = 23, DECLARE_QUEUE = 24, DECLARE_QUEUE_OK = 25;
export const BIND_QUEUE = 26, BIND_QUEUE_OK = 27, DELETE_QUEUE = 28, DELETE_QUEUE_OK = 29;
export const DELETE_EXCHANGE = 30, DELETE_EXCHANGE_OK = 31, PURGE_QUEUE = 32, PURGE_QUEUE_OK = 33;
export const PUBLISH_EXCHANGE = 34, PUBLISH_EXCHANGE_OK = 35;
export const START_FIRST = 0, START_TAIL = 1, START_OFFSET = 2;

class Writer {
  constructor() { this.parts = []; }
  u8(value) { this.parts.push(Buffer.from([value & 0xff])); }
  u16(value) { const b = Buffer.alloc(2); b.writeUInt16LE(value); this.parts.push(b); }
  i32(value) { const b = Buffer.alloc(4); b.writeInt32LE(value); this.parts.push(b); }
  i64(value) { const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(value)); this.parts.push(b); }
  string(value) { const raw = Buffer.from(value); this.u16(raw.length); this.parts.push(raw); }
  blob(value) { this.i32(value.length); this.parts.push(Buffer.from(value)); }
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

export class Client {
  constructor(socket) {
    this.socket = socket;
    this.nextId = 0;
    this.pending = Buffer.alloc(0);
    this.waiters = [];
    socket.on("data", (chunk) => { this.pending = Buffer.concat([this.pending, chunk]); this.pump(); });
    socket.on("close", () => { for (const waiter of this.waiters) waiter.reject(new Error("connection closed")); this.waiters = []; });
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
    return new Promise((resolve, reject) => { this.waiters.push({ count, resolve, reject }); this.pump(); });
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
    if (kind === ERROR) { const reader = new Reader(data); throw new Error(`broker error ${reader.u16()}: ${reader.string()}`); }
    if (kind !== expected) throw new Error(`unexpected frame ${kind}`);
    return data;
  }
  async hello(name) {
    const writer = new Writer();
    writer.string(token); writer.string(name); writer.string(user); writer.string(password); writer.string(vhost);
    const version = new Reader(await this.request(HELLO, writer.bytes(), HELLO_OK)).string();
    console.log(`hello ${version} user ${user} vhost ${vhost}`);
  }
  async ping() { await this.request(PING, Buffer.alloc(0), PONG); console.log("pong"); }
  async ensureStream(name, filters, partitions = 1, maxAge = -1, maxBytes = -1, maxMessage = 0) {
    const writer = new Writer();
    writer.string(name); writer.u16(filters.length);
    for (const filter of filters) writer.string(filter);
    writer.u16(partitions); writer.i64(maxAge); writer.i64(maxBytes); writer.i32(maxMessage);
    await this.request(ENSURE_STREAM, writer.bytes(), ENSURE_STREAM_OK);
    console.log(`stream ${name} partitions ${partitions}`);
  }
  async publish(subject, payload, key = "", headers = []) { return this.receipts(await this.request(PUBLISH, this.message(subject, key, headers, payload), PUBLISH_OK)); }
  async ensureConsumer(stream, name, { filter = "", ackWait = 30000, maxDeliver = 5, maxPending = 1000, ephemeral = false, start = START_FIRST, startOffset = 0 } = {}) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.string(filter);
    writer.i32(ackWait); writer.i32(maxDeliver); writer.i32(maxPending);
    writer.u8(ephemeral ? 1 : 0); writer.u8(start); writer.i64(startOffset);
    await this.request(ENSURE_CONSUMER, writer.bytes(), ENSURE_CONSUMER_OK);
    console.log(`consumer ${name} on ${stream} start ${start}`);
  }
  async fetch(stream, name, maxMessages = 32, expires = 0) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.u16(maxMessages); writer.i32(expires);
    const reader = new Reader(await this.request(FETCH, writer.bytes(), FETCH_OK));
    const reset = reader.u8() === 1;
    const first = reader.i64();
    const messages = [];
    const count = reader.u16();
    for (let i = 0; i < count; i++) {
      const partition = reader.u16();
      const offset = reader.i64();
      const timestamp = reader.i64();
      const delivery = reader.i32();
      const subject = reader.string();
      const key = reader.blob();
      const headers = [];
      const headerCount = reader.u16();
      for (let h = 0; h < headerCount; h++) headers.push([reader.string(), reader.blob()]);
      messages.push({ partition, offset, timestamp, delivery, subject, key, headers, payload: reader.blob() });
    }
    return { reset, first, messages };
  }
  ack(stream, name, partition, offset) { return this.cursor(ACK, ACK_OK, stream, name, partition, offset); }
  nack(stream, name, partition, offset) { return this.cursor(NACK, NACK_OK, stream, name, partition, offset); }
  async reset(stream, name, offset = 0) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.u8(1); writer.i64(offset);
    await this.request(RESET, writer.bytes(), RESET_OK);
    console.log(`reset ${name} offset ${offset}`);
  }
  async release(stream, name) {
    const writer = new Writer();
    writer.string(stream); writer.string(name);
    await this.request(RELEASE, writer.bytes(), RELEASE_OK);
    console.log(`release ${name}`);
  }
  async declareExchange(name, type, durable = true, autoDelete = false) {
    const writer = new Writer();
    writer.string(name); writer.string(type); writer.u8(durable ? 1 : 0); writer.u8(autoDelete ? 1 : 0);
    await this.request(DECLARE_EXCHANGE, writer.bytes(), DECLARE_EXCHANGE_OK);
    console.log(`exchange ${name || "(default)"} ${type}`);
  }
  async declareQueue(name, options = {}) {
    const writer = new Writer();
    writer.string(name);
    writer.u8(options.durable === false ? 0 : 1);
    writer.u8(options.exclusive ? 1 : 0);
    writer.u8(options.autoDelete ? 1 : 0);
    writer.i64(options.ttl ?? -1);
    writer.i32(options.maxLength ?? -1);
    writer.string(options.deadLetter ?? "");
    writer.string(options.deadLetterKey ?? "");
    await this.request(DECLARE_QUEUE, writer.bytes(), DECLARE_QUEUE_OK);
    console.log(`queue ${name}`);
  }
  async bindQueue(exchange, queue, routingKey, arguments_ = []) {
    const writer = new Writer();
    writer.string(exchange); writer.string(queue); writer.string(routingKey); writer.u16(arguments_.length);
    for (const [name, value] of arguments_) { writer.string(name); writer.string(value); }
    await this.request(BIND_QUEUE, writer.bytes(), BIND_QUEUE_OK);
    console.log(`bind ${exchange || "(default)"} -> ${queue}`);
  }
  deleteQueue(name) { return this.named(DELETE_QUEUE, DELETE_QUEUE_OK, name, `delete queue ${name}`); }
  deleteExchange(name) { return this.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, `delete exchange ${name}`); }
  purgeQueue(name) { return this.named(PURGE_QUEUE, PURGE_QUEUE_OK, name, `purge queue ${name}`); }
  async publishExchange(exchange, routingKey, payload, key = "", headers = []) {
    const writer = new Writer();
    writer.string(exchange); writer.string(routingKey); writer.string(key); writer.u16(headers.length);
    for (const [name, value] of headers) { writer.string(name); writer.blob(value); }
    writer.blob(payload);
    const receipts = await this.receipts(await this.request(PUBLISH_EXCHANGE, writer.bytes(), PUBLISH_EXCHANGE_OK));
    console.log(`routed ${exchange || "(default)"} -> ${receipts.length}`);
    return receipts;
  }
  message(subject, key, headers, payload) {
    const writer = new Writer();
    writer.string(subject); writer.string(key); writer.u16(headers.length);
    for (const [name, value] of headers) { writer.string(name); writer.blob(value); }
    writer.blob(payload);
    return writer.bytes();
  }
  async receipts(data) {
    const reader = new Reader(data);
    const count = reader.u16();
    const receipts = [];
    for (let i = 0; i < count; i++) {
      const item = { stream: reader.string(), partition: reader.u16(), offset: reader.i64() };
      console.log(`published ${item.stream} partition ${item.partition} offset ${item.offset}`);
      receipts.push(item);
    }
    return receipts;
  }
  cursor(op, expected, stream, name, partition, offset) {
    const writer = new Writer();
    writer.string(stream); writer.string(name); writer.u16(partition); writer.i64(offset);
    return this.request(op, writer.bytes(), expected);
  }
  async named(op, expected, name, label) {
    const writer = new Writer();
    writer.string(name);
    await this.request(op, writer.bytes(), expected);
    console.log(label);
  }
  close() { this.socket.end(); }
}

export async function connect(language) {
  const socket = net.connect({ host, port });
  await new Promise((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
  const client = new Client(socket);
  await client.hello(`sample-${language}`);
  return client;
}
