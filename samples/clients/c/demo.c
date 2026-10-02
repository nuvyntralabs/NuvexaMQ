/* Publish one message, then fetch and ack it.
 *   cc -O2 -o demo demo.c && ./demo
 * NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

enum { HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4, PUBLISH = 5, PUBLISH_OK = 6,
       ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8, FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, OP_ERROR = 21 };

typedef struct { unsigned char *data; size_t len, cap; } Buf;
typedef struct { const unsigned char *data; size_t len, i; } Rd;

static void die(const char *message) { fprintf(stderr, "%s\n", message); exit(1); }

static void add(Buf *buf, const void *bytes, size_t count) {
    if (buf->len + count > buf->cap) {
        size_t cap = buf->cap ? buf->cap * 2 : 64;
        while (cap < buf->len + count) cap *= 2;
        unsigned char *grown = realloc(buf->data, cap);
        if (!grown) die("out of memory");
        buf->data = grown;
        buf->cap = cap;
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
    const unsigned char *p = rd->data + rd->i;
    rd->i += count;
    return p;
}
static unsigned ru8(Rd *rd) { return *take(rd, 1); }
static unsigned ru16(Rd *rd) { const unsigned char *p = take(rd, 2); return p[0] | (p[1] << 8); }
static int ri32(Rd *rd) { const unsigned char *p = take(rd, 4); return (int)(p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24)); }
static int64_t ri64(Rd *rd) {
    const unsigned char *p = take(rd, 8);
    uint64_t n = 0;
    for (int i = 0; i < 8; i++) n |= (uint64_t)p[i] << (8 * i);
    return (int64_t)n;
}
static void rstr(Rd *rd, char *out, size_t cap) {
    unsigned n = ru16(rd);
    const unsigned char *p = take(rd, n);
    if (n >= cap) n = (unsigned)cap - 1;
    memcpy(out, p, n);
    out[n] = 0;
}
static void rskip_str(Rd *rd) { unsigned n = ru16(rd); take(rd, n); }
static void rskip_blob(Rd *rd) { int n = ri32(rd); if (n < 0) die("negative blob"); take(rd, (size_t)n); }
static unsigned char *rblob(Rd *rd, int *length) {
    int n = ri32(rd);
    if (n < 0) die("negative blob");
    unsigned char *copy = malloc((size_t)n + 1);
    if (!copy) die("out of memory");
    memcpy(copy, take(rd, (size_t)n), (size_t)n);
    copy[n] = 0;
    *length = n;
    return copy;
}

static void read_exact(int fd, void *data, size_t count) {
    unsigned char *out = data;
    size_t got = 0;
    while (got < count) {
        ssize_t n = read(fd, out + got, count - got);
        if (n <= 0) die("connection closed");
        got += (size_t)n;
    }
}

static int connect_host(const char *host, const char *port) {
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) die("could not resolve host");
    int fd = -1;
    for (struct addrinfo *it = res; it; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) die("could not connect");
    return fd;
}

static uint32_t next_id;

static Rd request(int fd, unsigned op, Buf *payload, unsigned expected) {
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
    read_exact(fd, size_raw, 4);
    uint32_t size = size_raw[0] | (size_raw[1] << 8) | (size_raw[2] << 16) | ((uint32_t)size_raw[3] << 24);
    unsigned char *frame = malloc(size);
    if (!frame) die("out of memory");
    read_exact(fd, frame, size);
    unsigned kind = frame[0] | (frame[1] << 8);
    uint32_t request_id = frame[2] | (frame[3] << 8) | (frame[4] << 16) | ((uint32_t)frame[5] << 24);
    if (request_id != next_id) die("response id did not match the request");
    Rd rd = { frame + 6, size - 6, 0 };
    if (kind == OP_ERROR) {
        char message[256];
        ru16(&rd);
        rstr(&rd, message, sizeof message);
        fprintf(stderr, "broker error: %s\n", message);
        exit(1);
    }
    if (kind != expected) die("unexpected frame");
    /* The caller reads rd and must free frame. Stash the base in a side channel by leaking until process exit. */
    return rd;
}

int main(void) {
    const char *host = getenv("NUVEXA_HOST");
    const char *port = getenv("NUVEXA_PORT");
    if (!host || !host[0]) host = "127.0.0.1";
    if (!port || !port[0]) port = "5761";
    const char *language = "c";
    char body[64];
    snprintf(body, sizeof body, "hello from %s", language);
    int fd = connect_host(host, port);

    Buf hello = {0};
    str(&hello, ""); str(&hello, "sample-c"); str(&hello, "guest"); str(&hello, "guest"); str(&hello, "/");
    request(fd, HELLO, &hello, HELLO_OK);

    Buf stream = {0};
    str(&stream, "clients"); u16(&stream, 1); str(&stream, "clients.>"); u16(&stream, 1); i64w(&stream, -1); i64w(&stream, -1); i32(&stream, 0);
    request(fd, ENSURE_STREAM, &stream, ENSURE_STREAM_OK);

    Buf publish = {0};
    str(&publish, "clients.created"); str(&publish, language); u16(&publish, 0); blob(&publish, body, strlen(body));
    Rd published = request(fd, PUBLISH, &publish, PUBLISH_OK);
    if (ru16(&published) < 1) die("publish was not stored");
    char stream_name[128];
    rstr(&published, stream_name, sizeof stream_name);
    unsigned partition = ru16(&published);
    int64_t offset = ri64(&published);
    printf("published %s partition %u offset %lld\n", stream_name, partition, (long long)offset);

    Buf consumer = {0};
    str(&consumer, "clients"); str(&consumer, "demo-c"); str(&consumer, "");
    i32(&consumer, 30000); i32(&consumer, 5); i32(&consumer, 1000); u8(&consumer, 0); u8(&consumer, 0); i64w(&consumer, 0);
    request(fd, ENSURE_CONSUMER, &consumer, ENSURE_CONSUMER_OK);

    int found = 0;
    while (!found) {
        Buf fetch = {0};
        str(&fetch, "clients"); str(&fetch, "demo-c"); u16(&fetch, 32); i32(&fetch, 0);
        Rd messages = request(fd, FETCH, &fetch, FETCH_OK);
        if (ru8(&messages) == 1) die("offset reset");
        ri64(&messages);
        unsigned count = ru16(&messages);
        if (count == 0) die("published message was not delivered");
        for (unsigned i = 0; i < count; i++) {
            unsigned part = ru16(&messages);
            int64_t message_offset = ri64(&messages);
            ri64(&messages); ri32(&messages); rskip_str(&messages); rskip_blob(&messages);
            unsigned headers = ru16(&messages);
            for (unsigned h = 0; h < headers; h++) { rskip_str(&messages); rskip_blob(&messages); }
            int length = 0;
            unsigned char *payload = rblob(&messages, &length);
            printf("fetched %lld %s\n", (long long)message_offset, (char *)payload);
            Buf ack = {0};
            str(&ack, "clients"); str(&ack, "demo-c"); u16(&ack, part); i64w(&ack, message_offset);
            request(fd, ACK, &ack, ACK_OK);
            free(ack.data);
            if (strcmp((char *)payload, body) == 0) found = 1;
            free(payload);
        }
        free(fetch.data);
    }
    close(fd);
    return 0;
}
