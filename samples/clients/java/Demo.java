// Publish one message, then fetch and ack it.
//   javac Demo.java && java Demo
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Demo {
    static final int HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4;
    static final int PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8;
    static final int FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, ERROR = 21;

    static final class Writer {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        void u8(int value) { buf.write(value & 0xff); }
        void u16(int value) { buf.write(value & 0xff); buf.write((value >> 8) & 0xff); }
        void i32(int value) { for (int i = 0; i < 4; i++) buf.write((value >> (8 * i)) & 0xff); }
        void i64(long value) { for (int i = 0; i < 8; i++) buf.write((int) ((value >> (8 * i)) & 0xff)); }
        void str(String value) { byte[] raw = value.getBytes(StandardCharsets.UTF_8); u16(raw.length); buf.writeBytes(raw); }
        void blob(byte[] value) { i32(value.length); buf.writeBytes(value); }
    }

    static final class Reader {
        private final byte[] data;
        private int i;
        Reader(byte[] data) { this.data = data; }
        private byte[] take(int count) {
            if (i + count > data.length) throw new IllegalStateException("frame ended early");
            byte[] chunk = new byte[count];
            System.arraycopy(data, i, chunk, 0, count);
            i += count;
            return chunk;
        }
        int u8() { return take(1)[0] & 0xff; }
        int u16() { byte[] raw = take(2); return (raw[0] & 0xff) | ((raw[1] & 0xff) << 8); }
        int i32() {
            byte[] raw = take(4);
            int n = 0;
            for (int shift = 0; shift < 4; shift++) n |= (raw[shift] & 0xff) << (8 * shift);
            return n;
        }
        long i64() {
            byte[] raw = take(8);
            long n = 0;
            for (int shift = 0; shift < 8; shift++) n |= (raw[shift] & 0xffL) << (8 * shift);
            return n;
        }
        String str() { return new String(take(u16()), StandardCharsets.UTF_8); }
        byte[] blob() { return take(i32()); }
    }

    static final class Client {
        private final OutputStream out;
        private final DataInputStream in;
        private int nextId;
        Client(Socket socket) throws IOException {
            this.out = socket.getOutputStream();
            this.in = new DataInputStream(socket.getInputStream());
        }
        byte[] request(int op, byte[] payload, int expected) throws IOException {
            nextId++;
            int length = 6 + payload.length;
            byte[] frame = new byte[4 + length];
            put32(frame, 0, length);
            put16(frame, 4, op);
            put32(frame, 6, nextId);
            System.arraycopy(payload, 0, frame, 10, payload.length);
            out.write(frame);
            out.flush();
            byte[] size = in.readNBytes(4);
            byte[] body = in.readNBytes(u32(size, 0));
            int kind = u16(body, 0);
            int requestId = u32(body, 2);
            byte[] data = new byte[body.length - 6];
            System.arraycopy(body, 6, data, 0, data.length);
            if (requestId != nextId) throw new IOException("response id did not match the request");
            if (kind == ERROR) {
                Reader reader = new Reader(data);
                throw new IOException("broker error " + reader.u16() + ": " + reader.str());
            }
            if (kind != expected) throw new IOException("unexpected frame " + kind);
            return data;
        }
        void hello(String user, String password, String vhost, String name) throws IOException {
            Writer writer = new Writer();
            writer.str(""); writer.str(name); writer.str(user); writer.str(password); writer.str(vhost);
            request(HELLO, writer.buf.toByteArray(), HELLO_OK);
        }
        void ensureStream(String name, String[] filters) throws IOException {
            Writer writer = new Writer();
            writer.str(name); writer.u16(filters.length);
            for (String filter : filters) writer.str(filter);
            writer.u16(1); writer.i64(-1); writer.i64(-1); writer.i32(0);
            request(ENSURE_STREAM, writer.buf.toByteArray(), ENSURE_STREAM_OK);
        }
        long[] publish(String subject, byte[] payload, String key) throws IOException {
            Writer writer = new Writer();
            writer.str(subject); writer.str(key); writer.u16(0); writer.blob(payload);
            Reader reader = new Reader(request(PUBLISH, writer.buf.toByteArray(), PUBLISH_OK));
            if (reader.u16() < 1) throw new IOException("publish was not stored");
            String stream = reader.str();
            int partition = reader.u16();
            long offset = reader.i64();
            System.out.println("published " + stream + " partition " + partition + " offset " + offset);
            return new long[] { partition, offset };
        }
        void ensureConsumer(String stream, String name) throws IOException {
            Writer writer = new Writer();
            writer.str(stream); writer.str(name); writer.str("");
            writer.i32(30000); writer.i32(5); writer.i32(1000); writer.u8(0); writer.u8(0); writer.i64(0);
            request(ENSURE_CONSUMER, writer.buf.toByteArray(), ENSURE_CONSUMER_OK);
        }
        void roundTrip(String language) throws IOException {
            byte[] body = ("hello from " + language).getBytes(StandardCharsets.UTF_8);
            hello("guest", "guest", "/", "sample-" + language);
            ensureStream("clients", new String[] { "clients.>" });
            publish("clients.created", body, language);
            String consumer = "demo-" + language;
            ensureConsumer("clients", consumer);
            boolean found = false;
            while (!found) {
                Writer writer = new Writer();
                writer.str("clients"); writer.str(consumer); writer.u16(32); writer.i32(0);
                Reader reader = new Reader(request(FETCH, writer.buf.toByteArray(), FETCH_OK));
                if (reader.u8() == 1) throw new IOException("offset reset at " + reader.i64());
                reader.i64();
                int count = reader.u16();
                if (count == 0) throw new IOException("published message was not delivered");
                for (int i = 0; i < count; i++) {
                    int partition = reader.u16();
                    long offset = reader.i64();
                    reader.i64(); reader.i32(); reader.str(); reader.blob();
                    int headers = reader.u16();
                    for (int h = 0; h < headers; h++) { reader.str(); reader.blob(); }
                    String text = new String(reader.blob(), StandardCharsets.UTF_8);
                    System.out.println("fetched " + offset + " " + text);
                    Writer ack = new Writer();
                    ack.str("clients"); ack.str(consumer); ack.u16(partition); ack.i64(offset);
                    request(ACK, ack.buf.toByteArray(), ACK_OK);
                    found = found || text.equals("hello from " + language);
                }
            }
        }
    }

    static void put16(byte[] raw, int at, int value) {
        raw[at] = (byte) (value & 0xff);
        raw[at + 1] = (byte) ((value >> 8) & 0xff);
    }
    static void put32(byte[] raw, int at, int value) {
        for (int i = 0; i < 4; i++) raw[at + i] = (byte) ((value >> (8 * i)) & 0xff);
    }
    static int u16(byte[] raw, int at) { return (raw[at] & 0xff) | ((raw[at + 1] & 0xff) << 8); }
    static int u32(byte[] raw, int at) {
        int n = 0;
        for (int i = 0; i < 4; i++) n |= (raw[at + i] & 0xff) << (8 * i);
        return n;
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("NUVEXA_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("NUVEXA_PORT", "5761"));
        try (Socket socket = new Socket(host, port)) {
            new Client(socket).roundTrip("java");
        }
    }
}
