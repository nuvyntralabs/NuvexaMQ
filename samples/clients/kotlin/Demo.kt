// Publish one message, then fetch and ack it.
//   kotlinc Demo.kt -include-runtime -d demo.jar && java -jar demo.jar
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
// The same frames are what an Android app would send.

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.net.Socket
import java.nio.charset.StandardCharsets

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
const val OP_ERROR = 21

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

class Client(socket: Socket) {
    private val out = socket.getOutputStream()
    private val input = DataInputStream(socket.getInputStream())
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

fun main() {
    val host = System.getenv("NUVEXA_HOST") ?: "127.0.0.1"
    val port = (System.getenv("NUVEXA_PORT") ?: "5761").toInt()
    val language = "kotlin"
    val text = "hello from $language"
    Socket(host, port).use { socket ->
        val client = Client(socket)
        val hello = Writer().apply { str(""); str("sample-kotlin"); str("guest"); str("guest"); str("/") }
        client.request(HELLO, hello.buf.toByteArray(), HELLO_OK)
        val stream = Writer().apply {
            str("clients"); u16(1); str("clients.>"); u16(1); i64(-1); i64(-1); i32(0)
        }
        client.request(ENSURE_STREAM, stream.buf.toByteArray(), ENSURE_STREAM_OK)
        val publish = Writer().apply {
            str("clients.created"); str(language); u16(0); blob(text.toByteArray(StandardCharsets.UTF_8))
        }
        val published = client.request(PUBLISH, publish.buf.toByteArray(), PUBLISH_OK)
        if (published.u16() < 1) error("publish was not stored")
        println("published ${published.str()} partition ${published.u16()} offset ${published.i64()}")
        val consumer = Writer().apply {
            str("clients"); str("demo-kotlin"); str("")
            i32(30000); i32(5); i32(1000); u8(0); u8(0); i64(0)
        }
        client.request(ENSURE_CONSUMER, consumer.buf.toByteArray(), ENSURE_CONSUMER_OK)
        var found = false
        while (!found) {
            val fetch = Writer().apply { str("clients"); str("demo-kotlin"); u16(32); i32(0) }
            val messages = client.request(FETCH, fetch.buf.toByteArray(), FETCH_OK)
            if (messages.u8() == 1) error("offset reset")
            messages.i64()
            val count = messages.u16()
            if (count == 0) error("published message was not delivered")
            repeat(count) {
                val part = messages.u16()
                val offset = messages.i64()
                messages.i64(); messages.i32(); messages.str(); messages.blob()
                repeat(messages.u16()) { messages.str(); messages.blob() }
                val payload = String(messages.blob(), StandardCharsets.UTF_8)
                println("fetched $offset $payload")
                val ack = Writer().apply { str("clients"); str("demo-kotlin"); u16(part); i64(offset) }
                client.request(ACK, ack.buf.toByteArray(), ACK_OK)
                found = found || payload == text
            }
        }
    }
}
