// Stream, consume, exchange, ping, and management samples.
//   kotlinc Feature.kt -include-runtime -d /tmp/nuvexa-kotlin-feature.jar && java -jar /tmp/nuvexa-kotlin-feature.jar ping

import java.io.ByteArrayOutputStream
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

const val HELLO = 1
const val HELLO_OK = 2
const val ENSURE_STREAM = 3
const val ENSURE_STREAM_OK = 4
const val PUBLISH = 5
const val PUBLISH_OK = 6
const val ENSURE_CONSUMER = 7
const val ENSURE_CONSUMER_OK = 8
const val FETCH = 9
const val FETCH_OK = 10
const val ACK = 11
const val ACK_OK = 12
const val NACK = 13
const val NACK_OK = 14
const val RESET = 15
const val RESET_OK = 16
const val RELEASE = 17
const val RELEASE_OK = 18
const val PING = 19
const val PONG = 20
const val OP_ERROR = 21
const val DECLARE_EXCHANGE = 22
const val DECLARE_EXCHANGE_OK = 23
const val DECLARE_QUEUE = 24
const val DECLARE_QUEUE_OK = 25
const val BIND_QUEUE = 26
const val BIND_QUEUE_OK = 27
const val DELETE_QUEUE = 28
const val DELETE_QUEUE_OK = 29
const val DELETE_EXCHANGE = 30
const val DELETE_EXCHANGE_OK = 31
const val PURGE_QUEUE = 32
const val PURGE_QUEUE_OK = 33
const val PUBLISH_EXCHANGE = 34
const val PUBLISH_EXCHANGE_OK = 35

class Writer {
    val buf = ByteArrayOutputStream()
    fun u8(value: Int) { buf.write(value and 0xff) }
    fun u16(value: Int) { u8(value); u8(value shr 8) }
    fun i32(value: Int) { repeat(4) { u8(value shr (8 * it)) } }
    fun i64(value: Long) { repeat(8) { u8((value shr (8 * it)).toInt()) } }
    fun str(value: String) {
        val raw = value.toByteArray(StandardCharsets.UTF_8)
        u16(raw.size)
        buf.write(raw)
    }
    fun blob(value: ByteArray) { i32(value.size); buf.write(value) }
    fun bytes() = buf.toByteArray()
}

class Reader(private val data: ByteArray) {
    private var i = 0
    private fun take(count: Int): ByteArray {
        if (i + count > data.size) error("frame ended early")
        return data.copyOfRange(i, i + count).also { i += count }
    }
    fun u8() = take(1)[0].toInt() and 0xff
    fun u16(): Int { val p = take(2); return (p[0].toInt() and 0xff) or ((p[1].toInt() and 0xff) shl 8) }
    fun i32(): Int {
        val p = take(4)
        var n = 0
        for (shift in 0 until 4) n = n or ((p[shift].toInt() and 0xff) shl (8 * shift))
        return n
    }
    fun i64(): Long {
        val p = take(8)
        var n = 0L
        for (shift in 0 until 8) n = n or ((p[shift].toLong() and 0xff) shl (8 * shift))
        return n
    }
    fun str() = String(take(u16()), StandardCharsets.UTF_8)
    fun blob() = take(i32())
}

data class Receipt(val stream: String, val partition: Int, val offset: Long)
data class Delivery(val partition: Int, val offset: Long, val attempts: Int, val payload: String)

class Client(socket: Socket) {
    private val out = socket.getOutputStream()
    private val input = java.io.DataInputStream(socket.getInputStream())
    private var next = 0
    fun request(op: Int, payload: ByteArray, expected: Int): Reader {
        next += 1
        val length = 6 + payload.size
        val frame = ByteArray(4 + length)
        put32(frame, 0, length)
        put16(frame, 4, op)
        put32(frame, 6, next)
        payload.copyInto(frame, 10)
        out.write(frame)
        out.flush()
        val size = u32(input.readNBytes(4), 0)
        val body = input.readNBytes(size)
        val kind = u16(body, 0)
        val requestId = u32(body, 2)
        val data = body.copyOfRange(6, body.size)
        if (requestId != next) error("response id did not match the request")
        val reader = Reader(data)
        if (kind == OP_ERROR) error("broker error ${reader.u16()}: ${reader.str()}")
        if (kind != expected) error("unexpected frame $kind")
        return reader
    }
    fun hello() {
        val body = Writer().apply {
            str(env("NUVEXA_TOKEN", "")); str("sample-kotlin")
            str(env("NUVEXA_USER", "guest")); str(env("NUVEXA_PASSWORD", "guest")); str(env("NUVEXA_VHOST", "/"))
        }
        println("hello ${request(HELLO, body.bytes(), HELLO_OK).str()}")
    }
    fun ping() { request(PING, ByteArray(0), PONG); println("pong") }
    fun ensureStream(name: String, filter: String, partitions: Int, maxAge: Long, maxBytes: Long, maxMessage: Int) {
        val body = Writer().apply { str(name); u16(1); str(filter); u16(partitions); i64(maxAge); i64(maxBytes); i32(maxMessage) }
        request(ENSURE_STREAM, body.bytes(), ENSURE_STREAM_OK)
        println("stream $name partitions $partitions")
    }
    fun publish(subject: String, payload: String, key: String = "", header: String = "", headerValue: String = ""): List<Receipt> {
        val body = Writer().apply {
            str(subject); str(key)
            if (header.isEmpty()) u16(0) else { u16(1); str(header); blob(headerValue.toByteArray()) }
            blob(payload.toByteArray())
        }
        return receipts(request(PUBLISH, body.bytes(), PUBLISH_OK))
    }
    fun ensureConsumer(stream: String, name: String, filter: String, ephemeral: Boolean, start: Int, offset: Long) {
        val body = Writer().apply {
            str(stream); str(name); str(filter)
            i32(30000); i32(5); i32(1000); u8(if (ephemeral) 1 else 0); u8(start); i64(offset)
        }
        request(ENSURE_CONSUMER, body.bytes(), ENSURE_CONSUMER_OK)
        println("consumer $name on $stream start $start")
    }
    fun fetch(stream: String, name: String, max: Int): List<Delivery> {
        val body = Writer().apply { str(stream); str(name); u16(max); i32(0) }
        val reader = request(FETCH, body.bytes(), FETCH_OK)
        if (reader.u8() == 1) error("offset reset")
        reader.i64()
        return List(reader.u16()) {
            val partition = reader.u16()
            val offset = reader.i64()
            reader.i64()
            val attempts = reader.i32()
            reader.str(); reader.blob()
            repeat(reader.u16()) { reader.str(); reader.blob() }
            Delivery(partition, offset, attempts, String(reader.blob(), StandardCharsets.UTF_8))
        }
    }
    fun ack(stream: String, name: String, message: Delivery) = cursor(ACK, ACK_OK, stream, name, message)
    fun nack(stream: String, name: String, message: Delivery) = cursor(NACK, NACK_OK, stream, name, message)
    fun reset(stream: String, name: String, offset: Long) {
        val body = Writer().apply { str(stream); str(name); u8(1); i64(offset) }
        request(RESET, body.bytes(), RESET_OK)
        println("reset $name offset $offset")
    }
    fun release(stream: String, name: String) {
        val body = Writer().apply { str(stream); str(name) }
        request(RELEASE, body.bytes(), RELEASE_OK)
        println("release $name")
    }
    fun declareExchange(name: String, type: String) {
        val body = Writer().apply { str(name); str(type); u8(1); u8(0) }
        request(DECLARE_EXCHANGE, body.bytes(), DECLARE_EXCHANGE_OK)
        println("exchange $name $type")
    }
    fun declareQueue(name: String, ttl: Long, max: Int, dead: String, deadKey: String) {
        val body = Writer().apply { str(name); u8(1); u8(0); u8(0); i64(ttl); i32(max); str(dead); str(deadKey) }
        request(DECLARE_QUEUE, body.bytes(), DECLARE_QUEUE_OK)
        println("queue $name")
    }
    fun bind(exchange: String, queue: String, key: String, headers: Boolean) {
        val body = Writer().apply {
            str(exchange); str(queue); str(key)
            if (headers) { u16(2); str("format"); str("json"); str("x-match"); str("all") } else u16(0)
        }
        request(BIND_QUEUE, body.bytes(), BIND_QUEUE_OK)
    }
    fun publishExchange(exchange: String, routing: String, payload: String, key: String, header: Boolean): Int {
        val body = Writer().apply {
            str(exchange); str(routing); str(key)
            if (header) { u16(1); str("format"); blob("json".toByteArray()) } else u16(0)
            blob(payload.toByteArray())
        }
        val items = receipts(request(PUBLISH_EXCHANGE, body.bytes(), PUBLISH_EXCHANGE_OK))
        println("routed $exchange -> ${items.size}")
        return items.size
    }
    fun named(op: Int, expected: Int, name: String, label: String) {
        request(op, Writer().apply { str(name) }.bytes(), expected)
        println(label)
    }
    private fun receipts(reader: Reader): List<Receipt> = List(reader.u16()) {
        Receipt(reader.str(), reader.u16(), reader.i64()).also { println("published ${it.stream} partition ${it.partition} offset ${it.offset}") }
    }
    private fun cursor(op: Int, expected: Int, stream: String, name: String, message: Delivery) {
        val body = Writer().apply { str(stream); str(name); u16(message.partition); i64(message.offset) }
        request(op, body.bytes(), expected)
    }
}

fun put16(raw: ByteArray, at: Int, value: Int) {
    raw[at] = (value and 0xff).toByte()
    raw[at + 1] = ((value shr 8) and 0xff).toByte()
}
fun put32(raw: ByteArray, at: Int, value: Int) {
    for (i in 0 until 4) raw[at + i] = ((value shr (8 * i)) and 0xff).toByte()
}
fun u16(raw: ByteArray, at: Int) = (raw[at].toInt() and 0xff) or ((raw[at + 1].toInt() and 0xff) shl 8)
fun u32(raw: ByteArray, at: Int): Int {
    var n = 0
    for (i in 0 until 4) n = n or ((raw[at + i].toInt() and 0xff) shl (8 * i))
    return n
}
fun env(name: String, fallback: String) = System.getenv(name)?.takeIf { it.isNotEmpty() } ?: fallback

fun http(method: String, url: String, body: String?, auth: Boolean) {
    val trust = arrayOf<TrustManager>(object : X509TrustManager {
        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
        override fun checkClientTrusted(chain: Array<X509Certificate>, kind: String) {}
        override fun checkServerTrusted(chain: Array<X509Certificate>, kind: String) {}
    })
    val context = SSLContext.getInstance("TLS")
    context.init(null, trust, SecureRandom())
    val client = HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(5)).build()
    val builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10))
    if (auth && url.contains("/api/")) {
        val token = java.util.Base64.getEncoder().encodeToString("${env("NUVEXA_USER", "guest")}:${env("NUVEXA_PASSWORD", "guest")}".toByteArray())
        builder.header("Authorization", "Basic $token")
    }
    if (body == null) builder.method(method, HttpRequest.BodyPublishers.noBody())
    else builder.method(method, HttpRequest.BodyPublishers.ofString(body)).header("Content-Type", "application/json")
    val response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    println("$method $url ${response.statusCode()} ${response.body().length} bytes")
    if (response.statusCode() >= 400) error(response.body())
}

fun runStream(client: Client) {
    client.ensureStream("catalog-kotlin", "catalog-kotlin.>", 2, 86_400_000, 1_048_576, 65_536)
    val keyed = client.publish("catalog-kotlin.created", "{\"id\":1}", "alpha", "content-type", "application/json")
    val first = client.publish("catalog-kotlin.created", "{\"id\":1}")
    val second = client.publish("catalog-kotlin.created", "{\"id\":1}")
    if (keyed.isEmpty() || first[0].partition == second[0].partition) error("round-robin did not use both partitions")
}

fun runConsume(client: Client) {
    client.ensureStream("mailbox-kotlin", "mailbox-kotlin.>", 1, -1, -1, 0)
    client.ensureConsumer("mailbox-kotlin", "box-kotlin", "mailbox-kotlin.>", false, 0, 0)
    client.publish("mailbox-kotlin.created", "ack me", header = "content-type", headerValue = "text/plain")
    client.publish("mailbox-kotlin.created", "nack me", header = "content-type", headerValue = "text/plain")
    val batch = client.fetch("mailbox-kotlin", "box-kotlin", 32).associateBy { it.payload }
    client.ack("mailbox-kotlin", "box-kotlin", batch.getValue("ack me")); println("ack")
    client.nack("mailbox-kotlin", "box-kotlin", batch.getValue("nack me")); println("nack")
    val again = client.fetch("mailbox-kotlin", "box-kotlin", 32).first { it.payload == "nack me" }
    println("redelivered ${again.attempts}")
    client.ack("mailbox-kotlin", "box-kotlin", again)
    client.reset("mailbox-kotlin", "box-kotlin", 0)
    val reset = client.fetch("mailbox-kotlin", "box-kotlin", 1).first()
    println("after reset offset ${reset.offset}")
    client.ack("mailbox-kotlin", "box-kotlin", reset)
    client.release("mailbox-kotlin", "tail-kotlin")
    client.ensureConsumer("mailbox-kotlin", "tail-kotlin", "", true, 1, 0)
    client.publish("mailbox-kotlin.created", "tail me")
    val tailed = client.fetch("mailbox-kotlin", "tail-kotlin", 32).first { it.payload == "tail me" }
    client.ack("mailbox-kotlin", "tail-kotlin", tailed)
    client.release("mailbox-kotlin", "tail-kotlin")
    client.ensureConsumer("mailbox-kotlin", "from0-kotlin", "", false, 2, 0)
    println("offset consumer from0-kotlin")
}

fun runExchange(client: Client) {
    client.declareExchange("amq.direct", "direct")
    client.declareExchange("direct-kotlin", "direct")
    client.declareExchange("fanout-kotlin", "fanout")
    client.declareExchange("topic-kotlin", "topic")
    client.declareExchange("headers-kotlin", "headers")
    client.declareQueue("dead-kotlin", -1, -1, "", "")
    client.declareQueue("work-kotlin", 60000, 100, "direct-kotlin", "expired")
    client.bind("direct-kotlin", "dead-kotlin", "expired", false)
    client.bind("direct-kotlin", "work-kotlin", "work.created", false)
    client.bind("fanout-kotlin", "work-kotlin", "", false)
    client.bind("topic-kotlin", "work-kotlin", "work.*", false)
    client.bind("headers-kotlin", "work-kotlin", "", true)
    var count = 0
    count += client.publishExchange("direct-kotlin", "work.created", "routed", "order-1", false)
    count += client.publishExchange("fanout-kotlin", "", "routed", "", false)
    count += client.publishExchange("topic-kotlin", "work.created", "routed", "", false)
    count += client.publishExchange("headers-kotlin", "", "routed", "", true)
    count += client.publishExchange("", "work-kotlin", "routed", "", false)
    if (count < 5) error("expected at least 5 receipts")
    client.named(PURGE_QUEUE, PURGE_QUEUE_OK, "work-kotlin", "purge queue work-kotlin")
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "work-kotlin", "delete queue work-kotlin")
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "dead-kotlin", "delete queue dead-kotlin")
    for (name in listOf("direct-kotlin", "fanout-kotlin", "topic-kotlin", "headers-kotlin"))
        client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, "delete exchange $name")
}

fun runAdmin() {
    val host = env("NUVEXA_HOST", "127.0.0.1")
    val health = "http://$host:${env("NUVEXA_HEALTH_PORT", "5762")}"
    http("GET", "$health/health", null, false)
    http("GET", "$health/metrics", null, false)
    val base = "http://$host:${env("NUVEXA_MANAGEMENT_PORT", "5763")}"
    for (path in listOf("/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"))
        http("GET", base + path, null, true)
    http("PUT", "$base/api/vhosts/vh-kotlin", null, true)
    http("PUT", "$base/api/users/user-kotlin", """{"password":"sample-pass","tags":["management"]}""", true)
    http("PUT", "$base/api/permissions", """{"user":"user-kotlin","vhost":"vh-kotlin","configure":".*","write":".*","read":".*"}""", true)
    http("PUT", "$base/api/policies/policy-kotlin", """{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}""", true)
    http("DELETE", "$base/api/policies/policy-kotlin?vhost=/", null, true)
    http("DELETE", "$base/api/permissions?user=user-kotlin&vhost=vh-kotlin", null, true)
    http("DELETE", "$base/api/users/user-kotlin", null, true)
    http("DELETE", "$base/api/vhosts/vh-kotlin", null, true)
    http("GET", "https://$host:${env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764")}/api/whoami", null, true)
}

fun main(args: Array<String>) {
    val command = args.firstOrNull() ?: "ping"
    if (command == "admin") { runAdmin(); return }
    val host = env("NUVEXA_HOST", "127.0.0.1")
    val port = env("NUVEXA_PORT", "5761").toInt()
    Socket(host, port).use { socket ->
        val client = Client(socket)
        client.hello()
        when (command) {
            "ping" -> client.ping()
            "stream" -> runStream(client)
            "consume" -> runConsume(client)
            "exchange" -> runExchange(client)
            else -> error("use ping, stream, consume, exchange, or admin")
        }
    }
}
