// Publish one message, then fetch and ack it.
//   dart run demo.dart
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
// The same frames are what a Flutter app would send.

import "dart:async";
import "dart:io";
import "dart:typed_data";

const hello = 1, helloOk = 2, ensureStream = 3, ensureStreamOk = 4;
const publish = 5, publishOk = 6, ensureConsumer = 7, ensureConsumerOk = 8;
const fetch = 9, fetchOk = 10, ack = 11, ackOk = 12, opError = 21;

class Writer {
  final bytes = BytesBuilder();
  void u8(int value) => bytes.addByte(value & 0xff);
  void u16(int value) { u8(value); u8(value >> 8); }
  void i32(int value) { for (var i = 0; i < 4; i++) u8(value >> (8 * i)); }
  void i64(int value) { for (var i = 0; i < 8; i++) u8(value >> (8 * i)); }
  void str(String value) { final raw = utf8.encode(value); u16(raw.length); bytes.add(raw); }
  void blob(List<int> value) { i32(value.length); bytes.add(value); }
}

class Reader {
  Reader(this.data);
  final Uint8List data;
  var i = 0;
  Uint8List take(int count) {
    if (i + count > data.length) throw StateError("frame ended early");
    final chunk = data.sublist(i, i + count);
    i += count;
    return chunk;
  }
  int u8() => take(1)[0];
  int u16() { final p = take(2); return p[0] | (p[1] << 8); }
  int i32() {
    final p = take(4);
    var n = 0;
    for (var shift = 0; shift < 4; shift++) n |= p[shift] << (8 * shift);
    return n;
  }
  int i64() {
    final p = take(8);
    var n = 0;
    for (var shift = 0; shift < 8; shift++) n |= p[shift] << (8 * shift);
    return n;
  }
  String str() => utf8.decode(take(u16()));
  Uint8List blob() => take(i32());
}

class Client {
  Client(this.socket) {
    _sub = socket.listen((chunk) {
      _pending.add(chunk);
      final waiter = _waiter;
      if (waiter != null && !waiter.isCompleted) waiter.complete();
    }, onDone: () {
      final waiter = _waiter;
      if (waiter != null && !waiter.isCompleted) waiter.complete();
    });
  }
  final Socket socket;
  late final StreamSubscription<Uint8List> _sub;
  var next = 0;
  var _pending = BytesBuilder();
  Completer<void>? _waiter;
  Future<Reader> request(int op, Uint8List payload, int expected) async {
    next += 1;
    final body = BytesBuilder();
    body.addByte(op & 0xff); body.addByte((op >> 8) & 0xff);
    for (var i = 0; i < 4; i++) body.addByte((next >> (8 * i)) & 0xff);
    body.add(payload);
    final raw = body.toBytes();
    final frame = BytesBuilder();
    for (var i = 0; i < 4; i++) frame.addByte((raw.length >> (8 * i)) & 0xff);
    frame.add(raw);
    socket.add(frame.toBytes());
    await socket.flush();
    final sizeRaw = await take(4);
    var size = 0;
    for (var i = 0; i < 4; i++) size |= sizeRaw[i] << (8 * i);
    final response = await take(size);
    final kind = response[0] | (response[1] << 8);
    var requestId = 0;
    for (var i = 0; i < 4; i++) requestId |= response[2 + i] << (8 * i);
    if (requestId != next) throw StateError("response id did not match the request");
    final reader = Reader(Uint8List.fromList(response.sublist(6)));
    if (kind == opError) throw StateError("broker error ${reader.u16()}: ${reader.str()}");
    if (kind != expected) throw StateError("unexpected frame $kind");
    return reader;
  }

  Future<Uint8List> take(int count) async {
    while (_pending.length < count) {
      final waiter = Completer<void>();
      _waiter = waiter;
      if (_pending.length >= count) break;
      await waiter.future;
      if (_pending.length < count && _sub.isPaused) throw StateError("connection closed");
    }
    _waiter = null;
    final all = _pending.toBytes();
    if (all.length < count) throw StateError("connection closed");
    final head = all.sublist(0, count);
    _pending = BytesBuilder()..add(all.sublist(count));
    return Uint8List.fromList(head);
  }

  Future<void> close() => _sub.cancel();
}

Future<void> main() async {
  final host = Platform.environment["NUVEXA_HOST"] ?? "127.0.0.1";
  final port = int.parse(Platform.environment["NUVEXA_PORT"] ?? "5761");
  const language = "dart";
  final text = "hello from $language";
  final socket = await Socket.connect(host, port);
  final client = Client(socket);
  final hello = Writer()..str("")..str("sample-dart")..str("guest")..str("guest")..str("/");
  await client.request(hello, hello.bytes.toBytes(), helloOk);
  final stream = Writer()..str("clients")..u16(1)..str("clients.>")..u16(1)..i64(-1)..i64(-1)..i32(0);
  await client.request(ensureStream, stream.bytes.toBytes(), ensureStreamOk);
  final publishedBody = Writer()..str("clients.created")..str(language)..u16(0)..blob(utf8.encode(text));
  final published = await client.request(publish, publishedBody.bytes.toBytes(), publishOk);
  if (published.u16() < 1) throw StateError("publish was not stored");
  print("published ${published.str()} partition ${published.u16()} offset ${published.i64()}");
  final consumer = Writer()
    ..str("clients")..str("demo-dart")..str("")
    ..i32(30000)..i32(5)..i32(1000)..u8(0)..u8(0)..i64(0);
  await client.request(ensureConsumer, consumer.bytes.toBytes(), ensureConsumerOk);
  var found = false;
  while (!found) {
    final fetchBody = Writer()..str("clients")..str("demo-dart")..u16(32)..i32(0);
    final messages = await client.request(fetch, fetchBody.bytes.toBytes(), fetchOk);
    if (messages.u8() == 1) throw StateError("offset reset");
    messages.i64();
    final count = messages.u16();
    if (count == 0) throw StateError("published message was not delivered");
    for (var i = 0; i < count; i++) {
      final part = messages.u16();
      final offset = messages.i64();
      messages.i64(); messages.i32(); messages.str(); messages.blob();
      final headers = messages.u16();
      for (var h = 0; h < headers; h++) { messages.str(); messages.blob(); }
      final payload = utf8.decode(messages.blob());
      print("fetched $offset $payload");
      final ackBody = Writer()..str("clients")..str("demo-dart")..u16(part)..i64(offset);
      await client.request(ack, ackBody.bytes.toBytes(), ackOk);
      found = found || payload == text;
    }
  }
  await client.close();
  await socket.close();
}
