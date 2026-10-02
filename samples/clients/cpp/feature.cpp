// Stream, consume, exchange, ping, and management samples.
//   c++ -O2 -std=c++17 -o /tmp/nuvexa-cpp-feature feature.cpp && /tmp/nuvexa-cpp-feature ping

#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>
#include <sys/socket.h>

enum {
    HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4, PUBLISH = 5, PUBLISH_OK = 6,
    ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8, FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12,
    NACK = 13, NACK_OK = 14, RESET = 15, RESET_OK = 16, RELEASE = 17, RELEASE_OK = 18, PING = 19, PONG = 20, OP_ERROR = 21,
    DECLARE_EXCHANGE = 22, DECLARE_EXCHANGE_OK = 23, DECLARE_QUEUE = 24, DECLARE_QUEUE_OK = 25,
    BIND_QUEUE = 26, BIND_QUEUE_OK = 27, DELETE_QUEUE = 28, DELETE_QUEUE_OK = 29,
    DELETE_EXCHANGE = 30, DELETE_EXCHANGE_OK = 31, PURGE_QUEUE = 32, PURGE_QUEUE_OK = 33,
    PUBLISH_EXCHANGE = 34, PUBLISH_EXCHANGE_OK = 35
};

class Writer {
public:
    void u8(unsigned value) { buf.push_back(static_cast<unsigned char>(value)); }
    void u16(unsigned value) { u8(value); u8(value >> 8); }
    void i32(int value) { for (int i = 0; i < 4; i++) u8(static_cast<unsigned>(value >> (8 * i))); }
    void i64(std::int64_t value) { for (int i = 0; i < 8; i++) u8(static_cast<unsigned>(value >> (8 * i))); }
    void str(const std::string &value) { u16(static_cast<unsigned>(value.size())); buf.insert(buf.end(), value.begin(), value.end()); }
    void blob(const std::string &value) { i32(static_cast<int>(value.size())); buf.insert(buf.end(), value.begin(), value.end()); }
    std::vector<unsigned char> buf;
};

class Reader {
public:
    explicit Reader(std::vector<unsigned char> data) : data(std::move(data)) {}
    unsigned u8() { return *take(1); }
    unsigned u16() { auto p = take(2); return p[0] | (p[1] << 8); }
    int i32() { auto p = take(4); return static_cast<int>(p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24)); }
    std::int64_t i64() {
        auto p = take(8);
        std::uint64_t n = 0;
        for (int i = 0; i < 8; i++) n |= static_cast<std::uint64_t>(p[i]) << (8 * i);
        return static_cast<std::int64_t>(n);
    }
    std::string str() { auto n = u16(); auto p = take(n); return std::string(reinterpret_cast<const char *>(p), n); }
    std::string blob() { auto n = i32(); if (n < 0) throw std::runtime_error("negative blob"); auto p = take(static_cast<size_t>(n)); return std::string(reinterpret_cast<const char *>(p), static_cast<size_t>(n)); }
private:
    const unsigned char *take(size_t count) {
        if (i + count > data.size()) throw std::runtime_error("frame ended early");
        auto p = data.data() + i;
        i += count;
        return p;
    }
    std::vector<unsigned char> data;
    size_t i = 0;
};

struct Receipt { std::string stream; unsigned partition; std::int64_t offset; };
struct Delivery { unsigned partition; std::int64_t offset; int attempts; std::string payload; };

static void read_exact(int fd, unsigned char *out, size_t count) {
    size_t got = 0;
    while (got < count) {
        auto n = ::read(fd, out + got, count - got);
        if (n <= 0) throw std::runtime_error("connection closed");
        got += static_cast<size_t>(n);
    }
}

class Client {
public:
    explicit Client(int fd) : fd(fd) {}
    ~Client() { if (fd >= 0) close(fd); }
    Reader request(unsigned op, const Writer &payload, unsigned expected) {
        next++;
        std::vector<unsigned char> frame(10 + payload.buf.size());
        auto length = static_cast<std::uint32_t>(6 + payload.buf.size());
        for (int i = 0; i < 4; i++) frame[static_cast<size_t>(i)] = static_cast<unsigned char>(length >> (8 * i));
        frame[4] = static_cast<unsigned char>(op); frame[5] = static_cast<unsigned char>(op >> 8);
        for (int i = 0; i < 4; i++) frame[static_cast<size_t>(6 + i)] = static_cast<unsigned char>(next >> (8 * i));
        if (!payload.buf.empty()) std::memcpy(frame.data() + 10, payload.buf.data(), payload.buf.size());
        size_t sent = 0;
        while (sent < frame.size()) {
            auto n = ::write(fd, frame.data() + sent, frame.size() - sent);
            if (n <= 0) throw std::runtime_error("write failed");
            sent += static_cast<size_t>(n);
        }
        unsigned char size_raw[4];
        read_exact(fd, size_raw, 4);
        std::uint32_t size = size_raw[0] | (size_raw[1] << 8) | (size_raw[2] << 16) | (static_cast<std::uint32_t>(size_raw[3]) << 24);
        std::vector<unsigned char> body(size);
        read_exact(fd, body.data(), size);
        unsigned kind = body[0] | (body[1] << 8);
        std::uint32_t request_id = body[2] | (body[3] << 8) | (body[4] << 16) | (static_cast<std::uint32_t>(body[5]) << 24);
        if (request_id != next) throw std::runtime_error("response id did not match the request");
        Reader reader({body.begin() + 6, body.end()});
        if (kind == OP_ERROR) throw std::runtime_error("broker error " + std::to_string(reader.u16()) + ": " + reader.str());
        if (kind != expected) throw std::runtime_error("unexpected frame");
        return reader;
    }
    void hello() {
        Writer body;
        body.str(env("NUVEXA_TOKEN", "")); body.str("sample-cpp");
        body.str(env("NUVEXA_USER", "guest")); body.str(env("NUVEXA_PASSWORD", "guest")); body.str(env("NUVEXA_VHOST", "/"));
        auto reader = request(HELLO, body, HELLO_OK);
        std::cout << "hello " << reader.str() << "\n";
    }
    void ping() { request(PING, Writer(), PONG); std::cout << "pong\n"; }
    void ensureStream(const std::string &name, const std::string &filter, unsigned partitions, std::int64_t maxAge, std::int64_t maxBytes, int maxMessage) {
        Writer body;
        body.str(name); body.u16(1); body.str(filter); body.u16(partitions); body.i64(maxAge); body.i64(maxBytes); body.i32(maxMessage);
        request(ENSURE_STREAM, body, ENSURE_STREAM_OK);
        std::cout << "stream " << name << " partitions " << partitions << "\n";
    }
    std::vector<Receipt> publish(const std::string &subject, const std::string &payload, const std::string &key, const std::string &header, const std::string &headerValue) {
        Writer body;
        body.str(subject); body.str(key);
        if (header.empty()) body.u16(0);
        else { body.u16(1); body.str(header); body.blob(headerValue); }
        body.blob(payload);
        return receipts(request(PUBLISH, body, PUBLISH_OK));
    }
    void ensureConsumer(const std::string &stream, const std::string &name, const std::string &filter, bool ephemeral, int start, std::int64_t offset) {
        Writer body;
        body.str(stream); body.str(name); body.str(filter);
        body.i32(30000); body.i32(5); body.i32(1000); body.u8(ephemeral ? 1 : 0); body.u8(static_cast<unsigned>(start)); body.i64(offset);
        request(ENSURE_CONSUMER, body, ENSURE_CONSUMER_OK);
        std::cout << "consumer " << name << " on " << stream << " start " << start << "\n";
    }
    std::vector<Delivery> fetch(const std::string &stream, const std::string &name, int max) {
        Writer body;
        body.str(stream); body.str(name); body.u16(static_cast<unsigned>(max)); body.i32(0);
        auto reader = request(FETCH, body, FETCH_OK);
        if (reader.u8() == 1) throw std::runtime_error("offset reset");
        reader.i64();
        auto count = reader.u16();
        std::vector<Delivery> messages;
        for (unsigned i = 0; i < count; i++) {
            Delivery item;
            item.partition = reader.u16();
            item.offset = reader.i64();
            reader.i64();
            item.attempts = reader.i32();
            reader.str(); reader.blob();
            auto headers = reader.u16();
            for (unsigned h = 0; h < headers; h++) { reader.str(); reader.blob(); }
            item.payload = reader.blob();
            messages.push_back(item);
        }
        return messages;
    }
    void ack(const std::string &stream, const std::string &name, const Delivery &message) { cursor(ACK, ACK_OK, stream, name, message); }
    void nack(const std::string &stream, const std::string &name, const Delivery &message) { cursor(NACK, NACK_OK, stream, name, message); }
    void reset(const std::string &stream, const std::string &name, std::int64_t offset) {
        Writer body; body.str(stream); body.str(name); body.u8(1); body.i64(offset);
        request(RESET, body, RESET_OK);
        std::cout << "reset " << name << " offset " << offset << "\n";
    }
    void release(const std::string &stream, const std::string &name) {
        Writer body; body.str(stream); body.str(name);
        request(RELEASE, body, RELEASE_OK);
        std::cout << "release " << name << "\n";
    }
    void declareExchange(const std::string &name, const std::string &type) {
        Writer body; body.str(name); body.str(type); body.u8(1); body.u8(0);
        request(DECLARE_EXCHANGE, body, DECLARE_EXCHANGE_OK);
        std::cout << "exchange " << name << " " << type << "\n";
    }
    void declareQueue(const std::string &name, std::int64_t ttl, int max, const std::string &dead, const std::string &deadKey) {
        Writer body; body.str(name); body.u8(1); body.u8(0); body.u8(0); body.i64(ttl); body.i32(max); body.str(dead); body.str(deadKey);
        request(DECLARE_QUEUE, body, DECLARE_QUEUE_OK);
        std::cout << "queue " << name << "\n";
    }
    void bind(const std::string &exchange, const std::string &queue, const std::string &key, bool headers) {
        Writer body; body.str(exchange); body.str(queue); body.str(key);
        if (headers) { body.u16(2); body.str("format"); body.str("json"); body.str("x-match"); body.str("all"); }
        else body.u16(0);
        request(BIND_QUEUE, body, BIND_QUEUE_OK);
    }
    int publishExchange(const std::string &exchange, const std::string &routing, const std::string &payload, const std::string &key, bool header) {
        Writer body; body.str(exchange); body.str(routing); body.str(key);
        if (header) { body.u16(1); body.str("format"); body.blob("json"); }
        else body.u16(0);
        body.blob(payload);
        auto items = receipts(request(PUBLISH_EXCHANGE, body, PUBLISH_EXCHANGE_OK));
        std::cout << "routed " << exchange << " -> " << items.size() << "\n";
        return static_cast<int>(items.size());
    }
    void named(unsigned op, unsigned expected, const std::string &name, const std::string &label) {
        Writer body; body.str(name); request(op, body, expected); std::cout << label << "\n";
    }
    int fd;
private:
    std::vector<Receipt> receipts(Reader reader) {
        auto count = reader.u16();
        std::vector<Receipt> items;
        for (unsigned i = 0; i < count; i++) {
            Receipt item{reader.str(), reader.u16(), reader.i64()};
            std::cout << "published " << item.stream << " partition " << item.partition << " offset " << item.offset << "\n";
            items.push_back(item);
        }
        return items;
    }
    void cursor(unsigned op, unsigned expected, const std::string &stream, const std::string &name, const Delivery &message) {
        Writer body; body.str(stream); body.str(name); body.u16(message.partition); body.i64(message.offset);
        request(op, body, expected);
    }
    static std::string env(const char *name, const char *fallback) {
        const char *value = std::getenv(name);
        return value && *value ? value : fallback;
    }
    std::uint32_t next = 0;
};

static int connect_host(const std::string &host, const std::string &port) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) throw std::runtime_error("could not resolve host");
    int fd = -1;
    for (auto it = res; it; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) throw std::runtime_error("could not connect");
    return fd;
}

static std::string env(const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    return value && *value ? value : fallback;
}

static const Delivery *find_payload(const std::vector<Delivery> &messages, const std::string &text) {
    for (const auto &message : messages) if (message.payload == text) return &message;
    return nullptr;
}

static void run_stream(Client &client) {
    client.ensureStream("catalog-cpp", "catalog-cpp.>", 2, 86400000, 1048576, 65536);
    auto keyed = client.publish("catalog-cpp.created", "{\"id\":1}", "alpha", "content-type", "application/json");
    auto first = client.publish("catalog-cpp.created", "{\"id\":1}", "", "", "");
    auto second = client.publish("catalog-cpp.created", "{\"id\":1}", "", "", "");
    if (keyed.empty() || first.empty() || second.empty() || first[0].partition == second[0].partition)
        throw std::runtime_error("round-robin did not use both partitions");
}

static void run_consume(Client &client) {
    client.ensureStream("mailbox-cpp", "mailbox-cpp.>", 1, -1, -1, 0);
    client.ensureConsumer("mailbox-cpp", "box-cpp", "mailbox-cpp.>", false, 0, 0);
    client.publish("mailbox-cpp.created", "ack me", "", "content-type", "text/plain");
    client.publish("mailbox-cpp.created", "nack me", "", "content-type", "text/plain");
    auto messages = client.fetch("mailbox-cpp", "box-cpp", 32);
    auto acked = find_payload(messages, "ack me");
    auto nacked = find_payload(messages, "nack me");
    if (!acked || !nacked) throw std::runtime_error("expected both deliveries");
    client.ack("mailbox-cpp", "box-cpp", *acked);
    std::cout << "ack\n";
    client.nack("mailbox-cpp", "box-cpp", *nacked);
    std::cout << "nack\n";
    messages = client.fetch("mailbox-cpp", "box-cpp", 32);
    auto again = find_payload(messages, "nack me");
    if (!again) throw std::runtime_error("nack was not redelivered");
    std::cout << "redelivered " << again->attempts << "\n";
    client.ack("mailbox-cpp", "box-cpp", *again);
    client.reset("mailbox-cpp", "box-cpp", 0);
    messages = client.fetch("mailbox-cpp", "box-cpp", 1);
    if (messages.empty()) throw std::runtime_error("reset did not return a message");
    std::cout << "after reset offset " << messages[0].offset << "\n";
    client.ack("mailbox-cpp", "box-cpp", messages[0]);
    client.release("mailbox-cpp", "tail-cpp");
    client.ensureConsumer("mailbox-cpp", "tail-cpp", "", true, 1, 0);
    client.publish("mailbox-cpp.created", "tail me", "", "", "");
    messages = client.fetch("mailbox-cpp", "tail-cpp", 32);
    auto tailed = find_payload(messages, "tail me");
    if (!tailed) throw std::runtime_error("tail message was not delivered");
    client.ack("mailbox-cpp", "tail-cpp", *tailed);
    client.release("mailbox-cpp", "tail-cpp");
    client.ensureConsumer("mailbox-cpp", "from0-cpp", "", false, 2, 0);
    std::cout << "offset consumer from0-cpp\n";
}

static void run_exchange(Client &client) {
    client.declareExchange("amq.direct", "direct");
    client.declareExchange("direct-cpp", "direct");
    client.declareExchange("fanout-cpp", "fanout");
    client.declareExchange("topic-cpp", "topic");
    client.declareExchange("headers-cpp", "headers");
    client.declareQueue("dead-cpp", -1, -1, "", "");
    client.declareQueue("work-cpp", 60000, 100, "direct-cpp", "expired");
    client.bind("direct-cpp", "dead-cpp", "expired", false);
    client.bind("direct-cpp", "work-cpp", "work.created", false);
    client.bind("fanout-cpp", "work-cpp", "", false);
    client.bind("topic-cpp", "work-cpp", "work.*", false);
    client.bind("headers-cpp", "work-cpp", "", true);
    int count = 0;
    count += client.publishExchange("direct-cpp", "work.created", "routed", "order-1", false);
    count += client.publishExchange("fanout-cpp", "", "routed", "", false);
    count += client.publishExchange("topic-cpp", "work.created", "routed", "", false);
    count += client.publishExchange("headers-cpp", "", "routed", "", true);
    count += client.publishExchange("", "work-cpp", "routed", "", false);
    if (count < 5) throw std::runtime_error("expected at least 5 receipts");
    client.named(PURGE_QUEUE, PURGE_QUEUE_OK, "work-cpp", "purge queue work-cpp");
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "work-cpp", "delete queue work-cpp");
    client.named(DELETE_QUEUE, DELETE_QUEUE_OK, "dead-cpp", "delete queue dead-cpp");
    client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "direct-cpp", "delete exchange direct-cpp");
    client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "fanout-cpp", "delete exchange fanout-cpp");
    client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "topic-cpp", "delete exchange topic-cpp");
    client.named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "headers-cpp", "delete exchange headers-cpp");
}

static void http(const std::string &method, const std::string &url, const std::string &body, bool auth) {
    std::string command = "curl -sk -o /tmp/nuvexa-cpp-http -w '%{http_code}' -X " + method;
    if (auth) command += " -u '" + env("NUVEXA_USER", "guest") + ":" + env("NUVEXA_PASSWORD", "guest") + "'";
    if (!body.empty()) command += " -H 'Content-Type: application/json' --data '" + body + "'";
    command += " '" + url + "'";
    FILE *pipe = popen(command.c_str(), "r");
    if (!pipe) throw std::runtime_error("curl failed to start");
    char status[16] = {0};
    if (!fgets(status, sizeof status, pipe)) throw std::runtime_error("curl returned no status");
    int code = pclose(pipe);
    FILE *file = fopen("/tmp/nuvexa-cpp-http", "rb");
    long bytes = 0;
    if (file) { fseek(file, 0, SEEK_END); bytes = ftell(file); fclose(file); }
    std::cout << method << " " << url << " " << status << " " << bytes << " bytes\n";
    if (code != 0 || std::atoi(status) >= 400) throw std::runtime_error("management request failed");
}

static void run_admin() {
    auto host = env("NUVEXA_HOST", "127.0.0.1");
    auto health = env("NUVEXA_HEALTH_PORT", "5762");
    auto management = env("NUVEXA_MANAGEMENT_PORT", "5763");
    auto https = env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764");
    http("GET", "http://" + host + ":" + health + "/health", "", false);
    http("GET", "http://" + host + ":" + health + "/metrics", "", false);
    for (const char *path : {"/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"})
        http("GET", "http://" + host + ":" + management + path, "", true);
    auto base = "http://" + host + ":" + management;
    http("PUT", base + "/api/vhosts/vh-cpp", "", true);
    http("PUT", base + "/api/users/user-cpp", "{\"password\":\"sample-pass\",\"tags\":[\"management\"]}", true);
    http("PUT", base + "/api/permissions", "{\"user\":\"user-cpp\",\"vhost\":\"vh-cpp\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}", true);
    http("PUT", base + "/api/policies/policy-cpp", "{\"vhost\":\"/\",\"pattern\":\"sample-.*\",\"priority\":1,\"messageTtlMs\":60000,\"maxLength\":100,\"deadLetterExchange\":\"\",\"deadLetterRoutingKey\":\"\"}", true);
    http("DELETE", base + "/api/policies/policy-cpp?vhost=/", "", true);
    http("DELETE", base + "/api/permissions?user=user-cpp&vhost=vh-cpp", "", true);
    http("DELETE", base + "/api/users/user-cpp", "", true);
    http("DELETE", base + "/api/vhosts/vh-cpp", "", true);
    http("GET", "https://" + host + ":" + https + "/api/whoami", "", true);
}

int main(int argc, char **argv) {
    std::string command = argc > 1 ? argv[1] : "ping";
    try {
        if (command == "admin") { run_admin(); return 0; }
        Client client(connect_host(env("NUVEXA_HOST", "127.0.0.1"), env("NUVEXA_PORT", "5761")));
        client.hello();
        if (command == "ping") client.ping();
        else if (command == "stream") run_stream(client);
        else if (command == "consume") run_consume(client);
        else if (command == "exchange") run_exchange(client);
        else throw std::runtime_error("use ping, stream, consume, exchange, or admin");
    } catch (const std::exception &ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
