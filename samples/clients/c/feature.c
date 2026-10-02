/* Stream, consume, exchange, ping, and management samples.
 *   cc -O2 -o /tmp/nuvexa-c-feature feature.c && /tmp/nuvexa-c-feature ping
 */
#include <arpa/inet.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

typedef struct { unsigned char *data; size_t len, cap; } Buf;
typedef struct { const unsigned char *data; size_t len, i; } Rd;
typedef struct { char stream[160]; unsigned partition; int64_t offset; } Receipt;
typedef struct { unsigned partition; int64_t offset; int attempts; char payload[128]; } Delivery;

static int fd;
static uint32_t next_id;

static void die(const char *message) { fprintf(stderr, "%s\n", message); exit(1); }
static const char *env(const char *name, const char *fallback) {
    const char *value = getenv(name);
    return value && value[0] ? value : fallback;
}
static void add(Buf *buf, const void *bytes, size_t count) {
    if (buf->len + count > buf->cap) {
        size_t cap = buf->cap ? buf->cap * 2 : 64;
        while (cap < buf->len + count) cap *= 2;
        unsigned char *grown = realloc(buf->data, cap);
        if (!grown) die("out of memory");
        buf->data = grown; buf->cap = cap;
    }
    memcpy(buf->data + buf->len, bytes, count);
    buf->len += count;
}
static void u8(Buf *buf, unsigned value) { unsigned char b = (unsigned char)value; add(buf, &b, 1); }
static void u16(Buf *buf, unsigned value) { unsigned char b[2] = { (unsigned char)value, (unsigned char)(value >> 8) }; add(buf, b, 2); }
static void i32(Buf *buf, int value) { for (int i = 0; i < 4; i++) u8(buf, (unsigned)(value >> (8 * i))); }
static void i64w(Buf *buf, int64_t value) { for (int i = 0; i < 8; i++) u8(buf, (unsigned)(value >> (8 * i))); }
static void str(Buf *buf, const char *value) { size_t n = strlen(value); u16(buf, (unsigned)n); add(buf, value, n); }
static void blob(Buf *buf, const void *value, size_t n) { i32(buf, (int)n); add(buf, value, n); }
static const unsigned char *take(Rd *rd, size_t count) {
    if (rd->i + count > rd->len) die("frame ended early");
    const unsigned char *p = rd->data + rd->i; rd->i += count; return p;
}
static unsigned ru8(Rd *rd) { return *take(rd, 1); }
static unsigned ru16(Rd *rd) { const unsigned char *p = take(rd, 2); return p[0] | (p[1] << 8); }
static int ri32(Rd *rd) { const unsigned char *p = take(rd, 4); return (int)(p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24)); }
static int64_t ri64(Rd *rd) {
    const unsigned char *p = take(rd, 8); uint64_t n = 0;
    for (int i = 0; i < 8; i++) n |= (uint64_t)p[i] << (8 * i);
    return (int64_t)n;
}
static void rstr(Rd *rd, char *out, size_t cap) {
    unsigned n = ru16(rd); const unsigned char *p = take(rd, n);
    if (n >= cap) n = (unsigned)cap - 1;
    memcpy(out, p, n); out[n] = 0;
}
static void rskip_str(Rd *rd) { take(rd, ru16(rd)); }
static void rskip_blob(Rd *rd) { int n = ri32(rd); if (n < 0) die("negative blob"); take(rd, (size_t)n); }
static void read_exact(void *data, size_t count) {
    unsigned char *out = data; size_t got = 0;
    while (got < count) {
        ssize_t n = read(fd, out + got, count - got);
        if (n <= 0) die("connection closed");
        got += (size_t)n;
    }
}
static int connect_host(const char *host, const char *port) {
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) die("could not resolve host");
    int sock = -1;
    for (struct addrinfo *it = res; it; it = it->ai_next) {
        sock = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock < 0) continue;
        if (connect(sock, it->ai_addr, it->ai_addrlen) == 0) break;
        close(sock); sock = -1;
    }
    freeaddrinfo(res);
    if (sock < 0) die("could not connect");
    return sock;
}
static Rd request(unsigned op, Buf *payload, unsigned expected) {
    next_id++;
    unsigned char head[10];
    uint32_t length = (uint32_t)(6 + payload->len);
    head[0] = (unsigned char)length; head[1] = (unsigned char)(length >> 8); head[2] = (unsigned char)(length >> 16); head[3] = (unsigned char)(length >> 24);
    head[4] = (unsigned char)op; head[5] = (unsigned char)(op >> 8);
    head[6] = (unsigned char)next_id; head[7] = (unsigned char)(next_id >> 8); head[8] = (unsigned char)(next_id >> 16); head[9] = (unsigned char)(next_id >> 24);
    if (write(fd, head, 10) != 10) die("write failed");
    size_t sent = 0;
    while (sent < payload->len) {
        ssize_t n = write(fd, payload->data + sent, payload->len - sent);
        if (n <= 0) die("write failed");
        sent += (size_t)n;
    }
    unsigned char size_raw[4];
    read_exact(size_raw, 4);
    uint32_t size = size_raw[0] | (size_raw[1] << 8) | (size_raw[2] << 16) | ((uint32_t)size_raw[3] << 24);
    unsigned char *frame = malloc(size);
    if (!frame) die("out of memory");
    read_exact(frame, size);
    unsigned kind = frame[0] | (frame[1] << 8);
    uint32_t request_id = frame[2] | (frame[3] << 8) | (frame[4] << 16) | ((uint32_t)frame[5] << 24);
    if (request_id != next_id) die("response id did not match the request");
    Rd rd = { frame + 6, size - 6, 0 };
    if (kind == OP_ERROR) { char message[256]; ru16(&rd); rstr(&rd, message, sizeof message); fprintf(stderr, "broker error: %s\n", message); exit(1); }
    if (kind != expected) die("unexpected frame");
    return rd;
}
static void hello(void) {
    Buf body = {0};
    str(&body, env("NUVEXA_TOKEN", "")); str(&body, "sample-c");
    str(&body, env("NUVEXA_USER", "guest")); str(&body, env("NUVEXA_PASSWORD", "guest")); str(&body, env("NUVEXA_VHOST", "/"));
    Rd rd = request(HELLO, &body, HELLO_OK);
    char version[32]; rstr(&rd, version, sizeof version);
    printf("hello %s\n", version);
}
static void ensure_stream(const char *name, const char *filter, unsigned partitions, int64_t max_age, int64_t max_bytes, int max_message) {
    Buf body = {0};
    str(&body, name); u16(&body, 1); str(&body, filter); u16(&body, partitions); i64w(&body, max_age); i64w(&body, max_bytes); i32(&body, max_message);
    request(ENSURE_STREAM, &body, ENSURE_STREAM_OK);
    printf("stream %s partitions %u\n", name, partitions);
}
static int publish(const char *subject, const char *payload, const char *key, const char *header, const char *header_value, Receipt *out, int cap) {
    Buf body = {0};
    str(&body, subject); str(&body, key ? key : "");
    if (header) { u16(&body, 1); str(&body, header); blob(&body, header_value, strlen(header_value)); }
    else u16(&body, 0);
    blob(&body, payload, strlen(payload));
    Rd rd = request(PUBLISH, &body, PUBLISH_OK);
    unsigned count = ru16(&rd);
    int stored = count < (unsigned)cap ? (int)count : cap;
    for (unsigned i = 0; i < count; i++) {
        char stream[160]; unsigned partition = 0; int64_t offset = 0;
        rstr(&rd, stream, sizeof stream); partition = ru16(&rd); offset = ri64(&rd);
        printf("published %s partition %u offset %lld\n", stream, partition, (long long)offset);
        if ((int)i < stored) { snprintf(out[i].stream, sizeof out[i].stream, "%s", stream); out[i].partition = partition; out[i].offset = offset; }
    }
    return (int)count;
}
static void ensure_consumer(const char *stream, const char *name, const char *filter, int ephemeral, int start, int64_t offset) {
    Buf body = {0};
    str(&body, stream); str(&body, name); str(&body, filter ? filter : "");
    i32(&body, 30000); i32(&body, 5); i32(&body, 1000); u8(&body, ephemeral ? 1 : 0); u8(&body, (unsigned)start); i64w(&body, offset);
    request(ENSURE_CONSUMER, &body, ENSURE_CONSUMER_OK);
    printf("consumer %s on %s start %d\n", name, stream, start);
}
static int fetch(const char *stream, const char *name, int max, Delivery *out, int cap) {
    Buf body = {0};
    str(&body, stream); str(&body, name); u16(&body, (unsigned)max); i32(&body, 0);
    Rd rd = request(FETCH, &body, FETCH_OK);
    if (ru8(&rd) == 1) die("offset reset");
    ri64(&rd);
    unsigned count = ru16(&rd);
    int stored = 0;
    for (unsigned i = 0; i < count; i++) {
        unsigned partition = ru16(&rd); int64_t offset = ri64(&rd); ri64(&rd); int attempts = ri32(&rd);
        rskip_str(&rd); rskip_blob(&rd);
        unsigned headers = ru16(&rd);
        for (unsigned h = 0; h < headers; h++) { rskip_str(&rd); rskip_blob(&rd); }
        int length = 0; int n = ri32(&rd); if (n < 0) die("negative blob");
        const unsigned char *payload = take(&rd, (size_t)n);
        if (stored < cap) {
            out[stored].partition = partition; out[stored].offset = offset; out[stored].attempts = attempts;
            length = n < 127 ? n : 127;
            memcpy(out[stored].payload, payload, (size_t)length); out[stored].payload[length] = 0;
            stored++;
        }
        (void)length;
    }
    return stored;
}
static void cursor(unsigned op, unsigned expected, const char *stream, const char *name, Delivery *message) {
    Buf body = {0};
    str(&body, stream); str(&body, name); u16(&body, message->partition); i64w(&body, message->offset);
    request(op, &body, expected);
}
static void reset_consumer(const char *stream, const char *name, int64_t offset) {
    Buf body = {0}; str(&body, stream); str(&body, name); u8(&body, 1); i64w(&body, offset);
    request(RESET, &body, RESET_OK);
    printf("reset %s offset %lld\n", name, (long long)offset);
}
static void release_consumer(const char *stream, const char *name) {
    Buf body = {0}; str(&body, stream); str(&body, name);
    request(RELEASE, &body, RELEASE_OK);
    printf("release %s\n", name);
}
static void declare_exchange(const char *name, const char *type) {
    Buf body = {0}; str(&body, name); str(&body, type); u8(&body, 1); u8(&body, 0);
    request(DECLARE_EXCHANGE, &body, DECLARE_EXCHANGE_OK);
    printf("exchange %s %s\n", name, type);
}
static void declare_queue(const char *name, int64_t ttl, int max, const char *dead, const char *dead_key) {
    Buf body = {0};
    str(&body, name); u8(&body, 1); u8(&body, 0); u8(&body, 0); i64w(&body, ttl); i32(&body, max); str(&body, dead); str(&body, dead_key);
    request(DECLARE_QUEUE, &body, DECLARE_QUEUE_OK);
    printf("queue %s\n", name);
}
static void bind_queue(const char *exchange, const char *queue, const char *key, int header_match) {
    Buf body = {0};
    str(&body, exchange); str(&body, queue); str(&body, key);
    if (header_match) { u16(&body, 2); str(&body, "format"); str(&body, "json"); str(&body, "x-match"); str(&body, "all"); }
    else u16(&body, 0);
    request(BIND_QUEUE, &body, BIND_QUEUE_OK);
}
static int publish_exchange(const char *exchange, const char *routing, const char *payload, const char *key, int with_header) {
    Buf body = {0};
    str(&body, exchange); str(&body, routing); str(&body, key ? key : "");
    if (with_header) { u16(&body, 1); str(&body, "format"); blob(&body, "json", 4); }
    else u16(&body, 0);
    blob(&body, payload, strlen(payload));
    Rd rd = request(PUBLISH_EXCHANGE, &body, PUBLISH_EXCHANGE_OK);
    unsigned count = ru16(&rd);
    for (unsigned i = 0; i < count; i++) {
        char stream[160]; rstr(&rd, stream, sizeof stream); unsigned partition = ru16(&rd); int64_t offset = ri64(&rd);
        printf("published %s partition %u offset %lld\n", stream, partition, (long long)offset);
    }
    printf("routed %s -> %u\n", exchange, count);
    return (int)count;
}
static void named(unsigned op, unsigned expected, const char *name, const char *label) {
    Buf body = {0}; str(&body, name); request(op, &body, expected); printf("%s\n", label);
}
static Delivery *find_payload(Delivery *messages, int count, const char *text) {
    for (int i = 0; i < count; i++) if (strcmp(messages[i].payload, text) == 0) return &messages[i];
    return NULL;
}
static void run_stream(void) {
    ensure_stream("catalog-c", "catalog-c.>", 2, 86400000, 1048576, 65536);
    Receipt keyed[4], first[4], second[4];
    int keyed_count = publish("catalog-c.created", "{\"id\":1}", "alpha", "content-type", "application/json", keyed, 4);
    int first_count = publish("catalog-c.created", "{\"id\":1}", "", NULL, NULL, first, 4);
    int second_count = publish("catalog-c.created", "{\"id\":1}", "", NULL, NULL, second, 4);
    if (keyed_count < 1 || first_count < 1 || second_count < 1 || first[0].partition == second[0].partition) die("round-robin did not use both partitions");
}
static void run_consume(void) {
    ensure_stream("mailbox-c", "mailbox-c.>", 1, -1, -1, 0);
    ensure_consumer("mailbox-c", "box-c", "mailbox-c.>", 0, 0, 0);
    Receipt ignored[2];
    publish("mailbox-c.created", "ack me", "", "content-type", "text/plain", ignored, 2);
    publish("mailbox-c.created", "nack me", "", "content-type", "text/plain", ignored, 2);
    Delivery messages[32];
    int count = fetch("mailbox-c", "box-c", 32, messages, 32);
    Delivery *acked = find_payload(messages, count, "ack me");
    Delivery *nacked = find_payload(messages, count, "nack me");
    if (!acked || !nacked) die("expected both deliveries");
    cursor(ACK, ACK_OK, "mailbox-c", "box-c", acked); printf("ack\n");
    cursor(NACK, NACK_OK, "mailbox-c", "box-c", nacked); printf("nack\n");
    count = fetch("mailbox-c", "box-c", 32, messages, 32);
    Delivery *again = find_payload(messages, count, "nack me");
    if (!again) die("nack was not redelivered");
    printf("redelivered %d\n", again->attempts);
    cursor(ACK, ACK_OK, "mailbox-c", "box-c", again);
    reset_consumer("mailbox-c", "box-c", 0);
    count = fetch("mailbox-c", "box-c", 1, messages, 32);
    if (count < 1) die("reset did not return a message");
    printf("after reset offset %lld\n", (long long)messages[0].offset);
    cursor(ACK, ACK_OK, "mailbox-c", "box-c", &messages[0]);
    release_consumer("mailbox-c", "tail-c");
    ensure_consumer("mailbox-c", "tail-c", "", 1, 1, 0);
    publish("mailbox-c.created", "tail me", "", NULL, NULL, ignored, 2);
    count = fetch("mailbox-c", "tail-c", 32, messages, 32);
    Delivery *tailed = find_payload(messages, count, "tail me");
    if (!tailed) die("tail message was not delivered");
    cursor(ACK, ACK_OK, "mailbox-c", "tail-c", tailed);
    release_consumer("mailbox-c", "tail-c");
    ensure_consumer("mailbox-c", "from0-c", "", 0, 2, 0);
    printf("offset consumer from0-c\n");
}
static void run_exchange(void) {
    declare_exchange("amq.direct", "direct");
    declare_exchange("direct-c", "direct");
    declare_exchange("fanout-c", "fanout");
    declare_exchange("topic-c", "topic");
    declare_exchange("headers-c", "headers");
    declare_queue("dead-c", -1, -1, "", "");
    declare_queue("work-c", 60000, 100, "direct-c", "expired");
    bind_queue("direct-c", "dead-c", "expired", 0);
    bind_queue("direct-c", "work-c", "work.created", 0);
    bind_queue("fanout-c", "work-c", "", 0);
    bind_queue("topic-c", "work-c", "work.*", 0);
    bind_queue("headers-c", "work-c", "", 1);
    int count = 0;
    count += publish_exchange("direct-c", "work.created", "routed", "order-1", 0);
    count += publish_exchange("fanout-c", "", "routed", "", 0);
    count += publish_exchange("topic-c", "work.created", "routed", "", 0);
    count += publish_exchange("headers-c", "", "routed", "", 1);
    count += publish_exchange("", "work-c", "routed", "", 0);
    if (count < 5) die("expected at least 5 receipts");
    named(PURGE_QUEUE, PURGE_QUEUE_OK, "work-c", "purge queue work-c");
    named(DELETE_QUEUE, DELETE_QUEUE_OK, "work-c", "delete queue work-c");
    named(DELETE_QUEUE, DELETE_QUEUE_OK, "dead-c", "delete queue dead-c");
    named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "direct-c", "delete exchange direct-c");
    named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "fanout-c", "delete exchange fanout-c");
    named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "topic-c", "delete exchange topic-c");
    named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, "headers-c", "delete exchange headers-c");
}
static void http(const char *method, const char *url, const char *body, int auth) {
    char command[2048];
    const char *user = env("NUVEXA_USER", "guest");
    const char *password = env("NUVEXA_PASSWORD", "guest");
    if (body) snprintf(command, sizeof command, "curl -sk -o /tmp/nuvexa-c-http -w '%%{http_code}' -X %s %s -H 'Content-Type: application/json' --data '%s' '%s'", method, auth ? "-u" : "", auth ? "" : "", url);
    /* Build the command without injecting the body through the format above when auth varies. */
    if (auth && body)
        snprintf(command, sizeof command, "curl -sk -o /tmp/nuvexa-c-http -w '%%{http_code}' -X %s -u '%s:%s' -H 'Content-Type: application/json' --data '%s' '%s'", method, user, password, body, url);
    else if (auth)
        snprintf(command, sizeof command, "curl -sk -o /tmp/nuvexa-c-http -w '%%{http_code}' -X %s -u '%s:%s' '%s'", method, user, password, url);
    else
        snprintf(command, sizeof command, "curl -sk -o /tmp/nuvexa-c-http -w '%%{http_code}' -X %s '%s'", method, url);
    FILE *pipe = popen(command, "r");
    if (!pipe) die("curl failed to start");
    char status[16] = {0};
    if (!fgets(status, sizeof status, pipe)) die("curl returned no status");
    int code = pclose(pipe);
    FILE *file = fopen("/tmp/nuvexa-c-http", "rb");
    long bytes = 0;
    if (file) { fseek(file, 0, SEEK_END); bytes = ftell(file); fclose(file); }
    printf("%s %s %s %ld bytes\n", method, url, status, bytes);
    if (code != 0 || atoi(status) >= 400) die("management request failed");
}
static void run_admin(void) {
    char url[256];
    const char *host = env("NUVEXA_HOST", "127.0.0.1");
    snprintf(url, sizeof url, "http://%s:%s/health", host, env("NUVEXA_HEALTH_PORT", "5762")); http("GET", url, NULL, 0);
    snprintf(url, sizeof url, "http://%s:%s/metrics", host, env("NUVEXA_HEALTH_PORT", "5762")); http("GET", url, NULL, 0);
    const char *paths[] = { "/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies" };
    for (size_t i = 0; i < sizeof paths / sizeof paths[0]; i++) {
        snprintf(url, sizeof url, "http://%s:%s%s", host, env("NUVEXA_MANAGEMENT_PORT", "5763"), paths[i]);
        http("GET", url, NULL, 1);
    }
    snprintf(url, sizeof url, "http://%s:%s/api/vhosts/vh-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763")); http("PUT", url, NULL, 1);
    snprintf(url, sizeof url, "http://%s:%s/api/users/user-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763"));
    http("PUT", url, "{\"password\":\"sample-pass\",\"tags\":[\"management\"]}", 1);
    snprintf(url, sizeof url, "http://%s:%s/api/permissions", host, env("NUVEXA_MANAGEMENT_PORT", "5763"));
    http("PUT", url, "{\"user\":\"user-c\",\"vhost\":\"vh-c\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}", 1);
    snprintf(url, sizeof url, "http://%s:%s/api/policies/policy-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763"));
    http("PUT", url, "{\"vhost\":\"/\",\"pattern\":\"sample-.*\",\"priority\":1,\"messageTtlMs\":60000,\"maxLength\":100,\"deadLetterExchange\":\"\",\"deadLetterRoutingKey\":\"\"}", 1);
    snprintf(url, sizeof url, "http://%s:%s/api/policies/policy-c?vhost=/", host, env("NUVEXA_MANAGEMENT_PORT", "5763")); http("DELETE", url, NULL, 1);
    snprintf(url, sizeof url, "http://%s:%s/api/permissions?user=user-c&vhost=vh-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763")); http("DELETE", url, NULL, 1);
    snprintf(url, sizeof url, "http://%s:%s/api/users/user-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763")); http("DELETE", url, NULL, 1);
    snprintf(url, sizeof url, "http://%s:%s/api/vhosts/vh-c", host, env("NUVEXA_MANAGEMENT_PORT", "5763")); http("DELETE", url, NULL, 1);
    snprintf(url, sizeof url, "https://%s:%s/api/whoami", host, env("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764")); http("GET", url, NULL, 1);
}
int main(int argc, char **argv) {
    const char *command = argc > 1 ? argv[1] : "ping";
    if (strcmp(command, "admin") == 0) { run_admin(); return 0; }
    fd = connect_host(env("NUVEXA_HOST", "127.0.0.1"), env("NUVEXA_PORT", "5761"));
    hello();
    if (strcmp(command, "ping") == 0) { Buf empty = {0}; request(PING, &empty, PONG); printf("pong\n"); }
    else if (strcmp(command, "stream") == 0) run_stream();
    else if (strcmp(command, "consume") == 0) run_consume();
    else if (strcmp(command, "exchange") == 0) run_exchange();
    else die("use ping, stream, consume, exchange, or admin");
    close(fd);
    return 0;
}
