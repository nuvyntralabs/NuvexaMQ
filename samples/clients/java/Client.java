import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class Client implements AutoCloseable {
    static final int HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4;
    static final int PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8;
    static final int FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, NACK = 13, NACK_OK = 14;
    static final int RESET = 15, RESET_OK = 16, RELEASE = 17, RELEASE_OK = 18, PING = 19, PONG = 20, ERROR = 21;
    static final int DECLARE_EXCHANGE = 22, DECLARE_EXCHANGE_OK = 23, DECLARE_QUEUE = 24, DECLARE_QUEUE_OK = 25;
    static final int BIND_QUEUE = 26, BIND_QUEUE_OK = 27, DELETE_QUEUE = 28, DELETE_QUEUE_OK = 29;
    static final int DELETE_EXCHANGE = 30, DELETE_EXCHANGE_OK = 31, PURGE_QUEUE = 32, PURGE_QUEUE_OK = 33;
    static final int PUBLISH_EXCHANGE = 34, PUBLISH_EXCHANGE_OK = 35;

    final Socket socket;
    final InputStream in;
    final OutputStream out;
    int next;

    Client(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    static Client connect(String language) throws Exception {
        String host = env("NUVEXA_HOST", "127.0.0.1");
        int port = Integer.parseInt(env("NUVEXA_PORT", "5761"));
        Client client = new Client(new Socket(host, port));
        client.hello("sample-" + language);
        return client;
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    public void close() throws IOException { socket.close(); }

    void hello(String name) throws IOException {
        Writer writer = new Writer();
        writer.str(env("NUVEXA_TOKEN", ""));
        writer.str(name);
        writer.str(env("NUVEXA_USER", "guest"));
        writer.str(env("NUVEXA_PASSWORD", "guest"));
        writer.str(env("NUVEXA_VHOST", "/"));
        System.out.println("hello " + new Reader(request(HELLO, writer.bytes(), HELLO_OK)).str());
    }

    void ping() throws IOException {
        request(PING, new byte[0], PONG);
        System.out.println("pong");
    }

    void ensureStream(String name, String[] filters, int partitions, long maxAge, long maxBytes, int maxMessage) throws IOException {
        Writer writer = new Writer();
        writer.str(name);
        writer.u16(filters.length);
        for (String filter : filters) writer.str(filter);
        writer.u16(partitions);
        writer.i64(maxAge);
        writer.i64(maxBytes);
        writer.i32(maxMessage);
        request(ENSURE_STREAM, writer.bytes(), ENSURE_STREAM_OK);
        System.out.println("stream " + name + " partitions " + partitions);
    }

    List<Receipt> publish(String subject, byte[] payload, String key, Header[] headers) throws IOException {
        return receipts(request(PUBLISH, message(subject, key, headers, payload), PUBLISH_OK));
    }

    void ensureConsumer(String stream, String name, String filter, boolean ephemeral, int start, long offset) throws IOException {
        Writer writer = new Writer();
        writer.str(stream);
        writer.str(name);
        writer.str(filter);
        writer.i32(30000);
        writer.i32(5);
        writer.i32(1000);
        writer.u8(ephemeral ? 1 : 0);
        writer.u8(start);
        writer.i64(offset);
        request(ENSURE_CONSUMER, writer.bytes(), ENSURE_CONSUMER_OK);
        System.out.println("consumer " + name + " on " + stream + " start " + start);
    }

    List<Delivery> fetch(String stream, String name, int max) throws IOException {
        Writer writer = new Writer();
        writer.str(stream);
        writer.str(name);
        writer.u16(max);
        writer.i32(0);
        Reader reader = new Reader(request(FETCH, writer.bytes(), FETCH_OK));
        if (reader.u8() == 1) throw new IllegalStateException("offset reset");
        reader.i64();
        int count = reader.u16();
        List<Delivery> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int partition = reader.u16();
            long offset = reader.i64();
            reader.i64();
            int delivery = reader.i32();
            reader.str();
            reader.blob();
            int headers = reader.u16();
            for (int h = 0; h < headers; h++) { reader.str(); reader.blob(); }
            messages.add(new Delivery(partition, offset, delivery, reader.blob()));
        }
        return messages;
    }

    void ack(String stream, String name, Delivery message) throws IOException { cursor(ACK, ACK_OK, stream, name, message); }
    void nack(String stream, String name, Delivery message) throws IOException { cursor(NACK, NACK_OK, stream, name, message); }

    void reset(String stream, String name, long offset) throws IOException {
        Writer writer = new Writer();
        writer.str(stream);
        writer.str(name);
        writer.u8(1);
        writer.i64(offset);
        request(RESET, writer.bytes(), RESET_OK);
        System.out.println("reset " + name + " offset " + offset);
    }

    void release(String stream, String name) throws IOException {
        Writer writer = new Writer();
        writer.str(stream);
        writer.str(name);
        request(RELEASE, writer.bytes(), RELEASE_OK);
        System.out.println("release " + name);
    }

    void declareExchange(String name, String type) throws IOException {
        Writer writer = new Writer();
        writer.str(name);
        writer.str(type);
        writer.u8(1);
        writer.u8(0);
        request(DECLARE_EXCHANGE, writer.bytes(), DECLARE_EXCHANGE_OK);
        System.out.println("exchange " + name + " " + type);
    }

    void declareQueue(String name, long ttl, int max, String dead, String deadKey) throws IOException {
        Writer writer = new Writer();
        writer.str(name);
        writer.u8(1);
        writer.u8(0);
        writer.u8(0);
        writer.i64(ttl);
        writer.i32(max);
        writer.str(dead);
        writer.str(deadKey);
        request(DECLARE_QUEUE, writer.bytes(), DECLARE_QUEUE_OK);
        System.out.println("queue " + name);
    }

    void bind(String exchange, String queue, String routingKey, String[][] arguments) throws IOException {
        Writer writer = new Writer();
        writer.str(exchange);
        writer.str(queue);
        writer.str(routingKey);
        writer.u16(arguments.length);
        for (String[] argument : arguments) { writer.str(argument[0]); writer.str(argument[1]); }
        request(BIND_QUEUE, writer.bytes(), BIND_QUEUE_OK);
    }

    List<Receipt> publishExchange(String exchange, String routingKey, byte[] payload, String key, Header[] headers) throws IOException {
        Writer writer = new Writer();
        writer.str(exchange);
        writer.str(routingKey);
        writer.str(key);
        writer.u16(headers.length);
        for (Header header : headers) { writer.str(header.name); writer.blob(header.value); }
        writer.blob(payload);
        List<Receipt> items = receipts(request(PUBLISH_EXCHANGE, writer.bytes(), PUBLISH_EXCHANGE_OK));
        System.out.println("routed " + exchange + " -> " + items.size());
        return items;
    }

    void purge(String name) throws IOException { named(PURGE_QUEUE, PURGE_QUEUE_OK, name, "purge queue " + name); }
    void deleteQueue(String name) throws IOException { named(DELETE_QUEUE, DELETE_QUEUE_OK, name, "delete queue " + name); }
    void deleteExchange(String name) throws IOException { named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, "delete exchange " + name); }

    byte[] message(String subject, String key, Header[] headers, byte[] payload) {
        Writer writer = new Writer();
        writer.str(subject);
        writer.str(key);
        writer.u16(headers.length);
        for (Header header : headers) { writer.str(header.name); writer.blob(header.value); }
        writer.blob(payload);
        return writer.bytes();
    }

    List<Receipt> receipts(byte[] data) {
        Reader reader = new Reader(data);
        int count = reader.u16();
        List<Receipt> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Receipt item = new Receipt(reader.str(), reader.u16(), reader.i64());
            System.out.println("published " + item.stream + " partition " + item.partition + " offset " + item.offset);
            items.add(item);
        }
        return items;
    }

    void cursor(int op, int expected, String stream, String name, Delivery message) throws IOException {
        Writer writer = new Writer();
        writer.str(stream);
        writer.str(name);
        writer.u16(message.partition);
        writer.i64(message.offset);
        request(op, writer.bytes(), expected);
    }

    void named(int op, int expected, String name, String label) throws IOException {
        Writer writer = new Writer();
        writer.str(name);
        request(op, writer.bytes(), expected);
        System.out.println(label);
    }

    byte[] request(int op, byte[] payload, int expected) throws IOException {
        next++;
        Writer body = new Writer();
        body.u16(op);
        body.i32(next);
        body.buf.writeBytes(payload);
        Writer frame = new Writer();
        frame.i32(body.buf.size());
        frame.buf.writeBytes(body.bytes());
        out.write(frame.bytes());
        out.flush();
        int size = new Reader(readExact(4)).i32();
        byte[] response = readExact(size);
        Reader head = new Reader(response);
        int kind = head.u16();
        int requestId = head.i32();
        byte[] data = new byte[response.length - 6];
        System.arraycopy(response, 6, data, 0, data.length);
        if (requestId != next) throw new IllegalStateException("response id did not match the request");
        if (kind == ERROR) {
            Reader reader = new Reader(data);
            throw new IllegalStateException("broker error " + reader.u16() + ": " + reader.str());
        }
        if (kind != expected) throw new IllegalStateException("unexpected frame " + kind);
        return data;
    }

    byte[] readExact(int count) throws IOException {
        byte[] buffer = new byte[count];
        int offset = 0;
        while (offset < count) {
            int read = in.read(buffer, offset, count - offset);
            if (read < 0) throw new IOException("connection closed");
            offset += read;
        }
        return buffer;
    }

    static void http(String method, String url, String body, boolean auth) throws Exception {
        TrustManager[] trust = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String kind) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String kind) {}
        }};
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust, new java.security.SecureRandom());
        HttpClient http = HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10));
        if (auth && url.contains("/api/")) {
            String token = java.util.Base64.getEncoder().encodeToString((env("NUVEXA_USER", "guest") + ":" + env("NUVEXA_PASSWORD", "guest")).getBytes(StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + token);
        }
        if (body == null) builder.method(method, HttpRequest.BodyPublishers.noBody());
        else builder.method(method, HttpRequest.BodyPublishers.ofString(body)).header("Content-Type", "application/json");
        HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        System.out.println(method + " " + url + " " + response.statusCode() + " " + response.body().length() + " bytes");
        if (response.statusCode() >= 400) throw new IllegalStateException(response.body());
    }

    static final class Writer {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        void u8(int value) { buf.write(value & 0xff); }
        void u16(int value) { buf.write(value & 0xff); buf.write((value >> 8) & 0xff); }
        void i32(int value) { for (int i = 0; i < 4; i++) buf.write((value >> (8 * i)) & 0xff); }
        void i64(long value) { for (int i = 0; i < 8; i++) buf.write((int) ((value >> (8 * i)) & 0xff)); }
        void str(String value) { byte[] raw = value.getBytes(StandardCharsets.UTF_8); u16(raw.length); buf.writeBytes(raw); }
        void blob(byte[] value) { i32(value.length); buf.writeBytes(value); }
        byte[] bytes() { return buf.toByteArray(); }
    }

    static final class Reader {
        final byte[] data;
        int i;
        Reader(byte[] data) { this.data = data; }
        byte[] take(int count) {
            if (i + count > data.length) throw new IllegalStateException("frame ended early");
            byte[] chunk = new byte[count];
            System.arraycopy(data, i, chunk, 0, count);
            i += count;
            return chunk;
        }
        int u8() { return take(1)[0] & 0xff; }
        int u16() { byte[] raw = take(2); return (raw[0] & 0xff) | ((raw[1] & 0xff) << 8); }
        int i32() { byte[] raw = take(4); int value = 0; for (int n = 0; n < 4; n++) value |= (raw[n] & 0xff) << (8 * n); return value; }
        long i64() { byte[] raw = take(8); long value = 0; for (int n = 0; n < 8; n++) value |= (raw[n] & 0xffL) << (8 * n); return value; }
        String str() { return new String(take(u16()), StandardCharsets.UTF_8); }
        byte[] blob() { return take(i32()); }
    }

    static final class Header {
        final String name;
        final byte[] value;
        Header(String name, byte[] value) { this.name = name; this.value = value; }
    }

    static final class Receipt {
        final String stream;
        final int partition;
        final long offset;
        Receipt(String stream, int partition, long offset) { this.stream = stream; this.partition = partition; this.offset = offset; }
    }

    static final class Delivery {
        final int partition;
        final long offset;
        final int attempts;
        final byte[] payload;
        Delivery(int partition, long offset, int attempts, byte[] payload) {
            this.partition = partition;
            this.offset = offset;
            this.attempts = attempts;
            this.payload = payload;
        }
    }
}
