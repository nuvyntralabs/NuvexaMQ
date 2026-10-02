// Stream, consume, exchange, ping, and management samples.
//   dart run feature.dart ping

import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

const helloOp = 1, helloOk = 2, ensureStreamOp = 3, ensureStreamOk = 4;
const publishOp = 5, publishOk = 6, ensureConsumer = 7, ensureConsumerOk = 8;
const fetchOp = 9, fetchOk = 10, ackOp = 11, ackOk = 12, nackOp = 13, nackOk = 14;
const resetOp = 15, resetOk = 16, releaseOp = 17, releaseOk = 18, pingOp = 19, pongOp = 20, opError = 21;
const declareExchangeOp = 22, declareExchangeOk = 23, declareQueueOp = 24, declareQueueOk = 25;
const bindQueueOp = 26, bindQueueOk = 27, deleteQueueOp = 28, deleteQueueOk = 29;
const deleteExchangeOp = 30, deleteExchangeOk = 31, purgeQueueOp = 32, purgeQueueOk = 33;
const publishExchangeOp = 34, publishExchangeOk = 35;

String env(String name, String fallback) {
  final value = Platform.environment[name];
  return value == null || value.isEmpty ? fallback : value;
}

class Writer {
  final bytes = BytesBuilder();
  void u8(int value) => bytes.addByte(value & 0xff);
  void u16(int value) { u8(value); u8(value >> 8); }
  void i32(int value) { for (var i = 0; i < 4; i++) u8(value >> (8 * i)); }
  void i64(int value) { for (var i = 0; i < 8; i++) u8(value >> (8 * i)); }
  void str(String value) { final raw = utf8.encode(value); u16(raw.length); bytes.add(raw); }
  void blob(List<int> value) { i32(value.length); bytes.add(value); }
  Uint8List toBytes() => bytes.toBytes();
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
  int i32() { final p = take(4); var n = 0; for (var shift = 0; shift < 4; shift++) n |= p[shift] << (8 * shift); return n; }
  int i64() { final p = take(8); var n = 0; for (var shift = 0; shift < 8; shift++) n |= p[shift] << (8 * shift); return n; }
  String str() => utf8.decode(take(u16()));
  Uint8List blob() => take(i32());
}

class Delivery {
  Delivery(this.partition, this.offset, this.attempts, this.payload);
  final int partition;
  final int offset;
  final int attempts;
  final String payload;
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
      if (_pending.length < count) throw StateError("connection closed");
    }
    _waiter = null;
    final all = _pending.toBytes();
    if (all.length < count) throw StateError("connection closed");
    _pending = BytesBuilder()..add(all.sublist(count));
    return Uint8List.fromList(all.sublist(0, count));
  }

  Future<void> hello() async {
    final body = Writer()
      ..str(env("NUVEXA_TOKEN", ""))..str("sample-dart")
      ..str(env("NUVEXA_USER", "guest"))..str(env("NUVEXA_PASSWORD", "guest"))..str(env("NUVEXA_VHOST", "/"));
    final reader = await request(helloOp, body.toBytes(), helloOk);
    print("hello ${reader.str()}");
  }
  Future<void> ping() async { await request(pingOp, Uint8List(0), pongOp); print("pong"); }
  Future<void> ensureStream(String name, String filter, int partitions, int maxAge, int maxBytes, int maxMessage) async {
    final body = Writer()..str(name)..u16(1)..str(filter)..u16(partitions)..i64(maxAge)..i64(maxBytes)..i32(maxMessage);
    await request(ensureStreamOp, body.toBytes(), ensureStreamOk);
    print("stream $name partitions $partitions");
  }
  Future<List<int>> publish(String subject, String payload, {String key = "", String header = "", String headerValue = ""}) async {
    final body = Writer()..str(subject)..str(key);
    if (header.isEmpty) { body.u16(0); } else { body.u16(1); body.str(header); body.blob(utf8.encode(headerValue)); }
    body.blob(utf8.encode(payload));
    return receipts(await request(publishOp, body.toBytes(), publishOk));
  }
  Future<void> ensureConsumer(String stream, String name, String filter, bool ephemeral, int start, int offset) async {
    final body = Writer()
      ..str(stream)..str(name)..str(filter)
      ..i32(30000)..i32(5)..i32(1000)..u8(ephemeral ? 1 : 0)..u8(start)..i64(offset);
    await request(ensureConsumer, body.toBytes(), ensureConsumerOk);
    print("consumer $name on $stream start $start");
  }
  Future<List<Delivery>> fetch(String stream, String name, int max) async {
    final body = Writer()..str(stream)..str(name)..u16(max)..i32(0);
    final reader = await request(fetchOp, body.toBytes(), fetchOk);
    if (reader.u8() == 1) throw StateError("offset reset");
    reader.i64();
    final messages = <Delivery>[];
    final count = reader.u16();
    for (var i = 0; i < count; i++) {
      final partition = reader.u16();
      final offset = reader.i64();
      reader.i64();
      final attempts = reader.i32();
      reader.str(); reader.blob();
      final headers = reader.u16();
      for (var h = 0; h < headers; h++) { reader.str(); reader.blob(); }
      messages.add(Delivery(partition, offset, attempts, utf8.decode(reader.blob())));
    }
    return messages;
  }
  Future<void> ack(String stream, String name, Delivery message) => cursor(ackOp, ackOk, stream, name, message);
  Future<void> nack(String stream, String name, Delivery message) => cursor(nackOp, nackOk, stream, name, message);
  Future<void> reset(String stream, String name, int offset) async {
    final body = Writer()..str(stream)..str(name)..u8(1)..i64(offset);
    await request(resetOp, body.toBytes(), resetOk);
    print("reset $name offset $offset");
  }
  Future<void> release(String stream, String name) async {
    final body = Writer()..str(stream)..str(name);
    await request(releaseOp, body.toBytes(), releaseOk);
    print("release $name");
  }
  Future<void> declareExchange(String name, String type) async {
    final body = Writer()..str(name)..str(type)..u8(1)..u8(0);
    await request(declareExchangeOp, body.toBytes(), declareExchangeOk);
    print("exchange $name $type");
  }
  Future<void> declareQueue(String name, int ttl, int max, String dead, String deadKey) async {
    final body = Writer()..str(name)..u8(1)..u8(0)..u8(0)..i64(ttl)..i32(max)..str(dead)..str(deadKey);
    await request(declareQueueOp, body.toBytes(), declareQueueOk);
    print("queue $name");
  }
  Future<void> bind(String exchange, String queue, String key, bool headers) async {
    final body = Writer()..str(exchange)..str(queue)..str(key);
    if (headers) { body.u16(2); body.str("format"); body.str("json"); body.str("x-match"); body.str("all"); } else { body.u16(0); }
    await request(bindQueueOp, body.toBytes(), bindQueueOk);
  }
  Future<int> publishExchange(String exchange, String routing, String payload, String key, bool header) async {
    final body = Writer()..str(exchange)..str(routing)..str(key);
    if (header) { body.u16(1); body.str("format"); body.blob(utf8.encode("json")); } else { body.u16(0); }
    body.blob(utf8.encode(payload));
    final items = receipts(await request(publishExchangeOp, body.toBytes(), publishExchangeOk));
    print("routed $exchange -> ${items.length}");
    return items.length;
  }
  Future<void> named(int op, int expected, String name, String label) async {
    await request(op, (Writer()..str(name)).toBytes(), expected);
    print(label);
  }
  List<int> receipts(Reader reader) {
    final partitions = <int>[];
    final count = reader.u16();
    for (var i = 0; i < count; i++) {
      final stream = reader.str();
      final partition = reader.u16();
      final offset = reader.i64();
      print("published $stream partition $partition offset $offset");
      partitions.add(partition);
    }
    return partitions;
  }
  Future<void> cursor(int op, int expected, String stream, String name, Delivery message) async {
    final body = Writer()..str(stream)..str(name)..u16(message.partition)..i64(message.offset);
    await request(op, body.toBytes(), expected);
  }
  Future<void> close() => _sub.cancel();
}

Future<void> http(String method, String url, String? body, bool auth) async {
  final client = HttpClient()..badCertificateCallback = (cert, host, port) => true;
  final request = await client.openUrl(method, Uri.parse(url));
  if (auth && url.contains("/api/")) {
    final token = base64.encode(utf8.encode("${env("NUVEXA_USER", "guest")}:${env("NUVEXA_PASSWORD", "guest")}"));
    request.headers.set(HttpHeaders.authorizationHeader, "Basic $token");
  }
  if (body != null) {
    request.headers.contentType = ContentType.json;
    request.add(utf8.encode(body));
  }
  final response = await request.close();
  final text = await response.transform(utf8.decoder).join();
  print("$method $url ${response.statusCode} ${text.length} bytes");
  client.close(force: true);
  if (response.statusCode >= 400) throw StateError(text);
}

Future<void> runStream(Client client) async {
  await client.ensureStream("catalog-dart", "catalog-dart.>", 2, 86400000, 1048576, 65536);
  final keyed = await client.publish("catalog-dart.created", '{"id":1}', key: "alpha", header: "content-type", headerValue: "application/json");
  final first = await client.publish("catalog-dart.created", '{"id":1}');
  final second = await client.publish("catalog-dart.created", '{"id":1}');
  if (keyed.isEmpty || first.first == second.first) throw StateError("round-robin did not use both partitions");
}

Future<void> runConsume(Client client) async {
  await client.ensureStream("mailbox-dart", "mailbox-dart.>", 1, -1, -1, 0);
  await client.ensureConsumer("mailbox-dart", "box-dart", "mailbox-dart.>", false, 0, 0);
  await client.publish("mailbox-dart.created", "ack me", header: "content-type", headerValue: "text/plain");
  await client.publish("mailbox-dart.created", "nack me", header: "content-type", headerValue: "text/plain");
  final batch = {for (final message in await client.fetch("mailbox-dart", "box-dart", 32)) message.payload: message};
  await client.ack("mailbox-dart", "box-dart", batch["ack me"]!);
  print("ack");
  await client.nack("mailbox-dart", "box-dart", batch["nack me"]!);
  print("nack");
  final again = (await client.fetch("mailbox-dart", "box-dart", 32)).firstWhere((message) => message.payload == "nack me");
  print("redelivered ${again.attempts}");
  await client.ack("mailbox-dart", "box-dart", again);
  await client.reset("mailbox-dart", "box-dart", 0);
  final reset = (await client.fetch("mailbox-dart", "box-dart", 1)).first;
  print("after reset offset ${reset.offset}");
  await client.ack("mailbox-dart", "box-dart", reset);
  await client.release("mailbox-dart", "tail-dart");
  await client.ensureConsumer("mailbox-dart", "tail-dart", "", true, 1, 0);
  await client.publish("mailbox-dart.created", "tail me");
  final tailed = (await client.fetch("mailbox-dart", "tail-dart", 32)).firstWhere((message) => message.payload == "tail me");
  await client.ack("mailbox-dart", "tail-dart", tailed);
  await client.release("mailbox-dart", "tail-dart");
  await client.ensureConsumer("mailbox-dart", "from0-dart", "", false, 2, 0);
  print("offset consumer from0-dart");
}

Future<void> runExchange(Client client) async {
  await client.declareExchange("amq.direct", "direct");
  await client.declareExchange("direct-dart", "direct");
  await client.declareExchange("fanout-dart", "fanout");
  await client.declareExchange("topic-dart", "topic");
  await client.declareExchange("headers-dart", "headers");
  await client.declareQueue("dead-dart", -1, -1, "", "");
  await client.declareQueue("work-dart", 60000, 100, "direct-dart", "expired");
  await client.bind("direct-dart", "dead-dart", "expired", false);
  await client.bind("direct-dart", "work-dart", "work.created", false);
  await client.bind("fanout-dart", "work-dart", "", false);
  await client.bind("topic-dart", "work-dart", "work.*", false);
  await client.bind("headers-dart", "work-dart", "", true);
  var count = 0;
  count += await client.publishExchange("direct-dart", "work.created", "routed", "order-1", false);
  count += await client.publishExchange("fanout-dart", "", "routed", "", false);
  count += await client.publishExchange("topic-dart", "work.created", "routed", "", false);
  count += await client.publishExchange("headers-dart", "", "routed", "", true);
  count += await client.publishExchange("", "work-dart", "routed", "", false);
  if (count < 5) throw StateError("expected at least 5 receipts");
  await client.named(purgeQueueOp, purgeQueueOk, "work-dart", "purge queue work-dart");
  await client.named(deleteQueueOp, deleteQueueOk, "work-dart", "delete queue work-dart");
  await client.named(deleteQueueOp, deleteQueueOk, "dead-dart", "delete queue dead-dart");
  for (final name in ["direct-dart", "fanout-dart", "topic-dart", "headers-dart"]) {
    await client.named(deleteExchangeOp, deleteExchangeOk, name, "delete exchange $name");
  }
}

Future<void> runAdmin() async {
  final host = env("NUVEXA_HOST", "127.0.0.1");
  final health = "http://$host:${env("NUVEXA_HEALTH_PORT", "5762")}";
  await http("GET", "$health/health", null, false);
  await http("GET", "$health/metrics", null, false);
  final base = "http://$host:${env("NUVEXA_MANAGEMENT_PORT", "5763")}";
  for (final path in ["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"]) {
    await http("GET", "$base$path", null, true);
  }
  await http("PUT", "$base/api/vhosts/vh-dart", null, true);
  await http("PUT", "$base/api/users/user-dart", '{"password":"sample-pass","tags":["management"]}', true);
  await http("PUT", "$base/api/permissions", '{"user":"user-dart","vhost":"vh-dart","configure":".*","write":".*","read":".*"}', true);
  await http("PUT", "$base/api/policies/policy-dart", '{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}', true);
  await http("DELETE", "$base/api/policies/policy-dart?vhost=/", null, true);
  await http("DELETE", "$base/api/permissions?user=user-dart&vhost=vh-dart", null, true);
  await http("DELETE", "$base/api/users/user-dart", null, true);
  await http("DELETE", "$base/api/vhosts/vh-dart", null, true);
  await http("GET", "https://$host:${env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764")}/api/whoami", null, true);
}

Future<void> main(List<String> args) async {
  final command = args.isEmpty ? "ping" : args.first;
  if (command == "admin") { await runAdmin(); return; }
  final socket = await Socket.connect(env("NUVEXA_HOST", "127.0.0.1"), int.parse(env("NUVEXA_PORT", "5761")));
  final client = Client(socket);
  await client.hello();
  switch (command) {
    case "ping": await client.ping();
    case "stream": await runStream(client);
    case "consume": await runConsume(client);
    case "exchange": await runExchange(client);
    default: throw StateError("use ping, stream, consume, exchange, or admin");
  }
  await client.close();
  await socket.close();
}
