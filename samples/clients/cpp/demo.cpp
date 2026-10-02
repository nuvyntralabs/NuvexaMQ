// Publish one message, then fetch and ack it.
//   c++ -O2 -std=c++17 -o demo demo.cpp && ./demo
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

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

enum { HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4, PUBLISH = 5, PUBLISH_OK = 6,
       ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8, FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, OP_ERROR = 21 };

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
    Reader request(unsigned op, const Writer &payload, unsigned expected) {
        next++;
        std::vector<unsigned char> frame(10 + payload.buf.size());
        auto length = static_cast<std::uint32_t>(6 + payload.buf.size());
        for (int i = 0; i < 4; i++) frame[static_cast<size_t>(i)] = static_cast<unsigned char>(length >> (8 * i));
        frame[4] = static_cast<unsigned char>(op); frame[5] = static_cast<unsigned char>(op >> 8);
        for (int i = 0; i < 4; i++) frame[static_cast<size_t>(6 + i)] = static_cast<unsigned char>(next >> (8 * i));
        std::memcpy(frame.data() + 10, payload.buf.data(), payload.buf.size());
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
    int fd;
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

int main() {
    const char *host_env = std::getenv("NUVEXA_HOST");
    const char *port_env = std::getenv("NUVEXA_PORT");
    std::string host = host_env && *host_env ? host_env : "127.0.0.1";
    std::string port = port_env && *port_env ? port_env : "5761";
    const std::string language = "cpp";
    const std::string body = "hello from " + language;
    try {
        Client client(connect_host(host, port));
        Writer hello;
        hello.str(""); hello.str("sample-cpp"); hello.str("guest"); hello.str("guest"); hello.str("/");
        client.request(HELLO, hello, HELLO_OK);
        Writer stream;
        stream.str("clients"); stream.u16(1); stream.str("clients.>"); stream.u16(1); stream.i64(-1); stream.i64(-1); stream.i32(0);
        client.request(ENSURE_STREAM, stream, ENSURE_STREAM_OK);
        Writer publish;
        publish.str("clients.created"); publish.str(language); publish.u16(0); publish.blob(body);
        auto published = client.request(PUBLISH, publish, PUBLISH_OK);
        if (published.u16() < 1) throw std::runtime_error("publish was not stored");
        auto stream_name = published.str();
        auto partition = published.u16();
        auto offset = published.i64();
        std::cout << "published " << stream_name << " partition " << partition << " offset " << offset << "\n";
        Writer consumer;
        consumer.str("clients"); consumer.str("demo-cpp"); consumer.str("");
        consumer.i32(30000); consumer.i32(5); consumer.i32(1000); consumer.u8(0); consumer.u8(0); consumer.i64(0);
        client.request(ENSURE_CONSUMER, consumer, ENSURE_CONSUMER_OK);
        bool found = false;
        while (!found) {
            Writer fetch;
            fetch.str("clients"); fetch.str("demo-cpp"); fetch.u16(32); fetch.i32(0);
            auto messages = client.request(FETCH, fetch, FETCH_OK);
            if (messages.u8() == 1) throw std::runtime_error("offset reset");
            messages.i64();
            auto count = messages.u16();
            if (count == 0) throw std::runtime_error("published message was not delivered");
            for (unsigned i = 0; i < count; i++) {
                auto part = messages.u16();
                auto message_offset = messages.i64();
                messages.i64(); messages.i32(); messages.str(); messages.blob();
                auto headers = messages.u16();
                for (unsigned h = 0; h < headers; h++) { messages.str(); messages.blob(); }
                auto text = messages.blob();
                std::cout << "fetched " << message_offset << " " << text << "\n";
                Writer ack;
                ack.str("clients"); ack.str("demo-cpp"); ack.u16(part); ack.i64(message_offset);
                client.request(ACK, ack, ACK_OK);
                found = found || text == body;
            }
        }
        close(client.fd);
    } catch (const std::exception &ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
