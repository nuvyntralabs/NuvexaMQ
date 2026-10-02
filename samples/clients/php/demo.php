<?php
// Publish one message, then fetch and ack it.
//   php demo.php
// NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

$host = getenv("NUVEXA_HOST") ?: "127.0.0.1";
$port = intval(getenv("NUVEXA_PORT") ?: "5761");
const HELLO = 1, HELLO_OK = 2, ENSURE_STREAM = 3, ENSURE_STREAM_OK = 4;
const PUBLISH = 5, PUBLISH_OK = 6, ENSURE_CONSUMER = 7, ENSURE_CONSUMER_OK = 8;
const FETCH = 9, FETCH_OK = 10, ACK = 11, ACK_OK = 12, ERROR = 21;

function le(int $value, int $width): string {
    $out = "";
    for ($i = 0; $i < $width; $i++) {
        $out .= chr($value & 0xFF);
        $value >>= 8;
    }
    return $out;
}

class Writer {
    public string $buf = "";
    public function u8(int $value): void { $this->buf .= chr($value & 0xFF); }
    public function u16(int $value): void { $this->buf .= le($value, 2); }
    public function i32(int $value): void { $this->buf .= le($value, 4); }
    public function i64(int $value): void { $this->buf .= le($value, 8); }
    public function str(string $value): void {
        $this->u16(strlen($value));
        $this->buf .= $value;
    }
    public function blob(string $value): void {
        $this->i32(strlen($value));
        $this->buf .= $value;
    }
}

class Reader {
    public function __construct(private string $data, private int $i = 0) {}
    private function take(int $count): string {
        if ($this->i + $count > strlen($this->data)) {
            throw new RuntimeException("frame ended early");
        }
        $chunk = substr($this->data, $this->i, $count);
        $this->i += $count;
        return $chunk;
    }
    public function u8(): int { return ord($this->take(1)); }
    public function u16(): int {
        $raw = $this->take(2);
        return ord($raw[0]) | (ord($raw[1]) << 8);
    }
    public function i32(): int {
        $raw = $this->take(4);
        $n = 0;
        for ($i = 0; $i < 4; $i++) $n |= ord($raw[$i]) << (8 * $i);
        if ($n >= 0x80000000) $n -= 0x100000000;
        return $n;
    }
    public function i64(): int {
        $raw = $this->take(8);
        $n = 0;
        for ($i = 0; $i < 8; $i++) $n |= ord($raw[$i]) << (8 * $i);
        return $n;
    }
    public function str(): string { return $this->take($this->u16()); }
    public function blob(): string { return $this->take($this->i32()); }
}

function read_exact($socket, int $count): string {
    $buf = "";
    while (strlen($buf) < $count) {
        $chunk = socket_read($socket, $count - strlen($buf), PHP_BINARY_READ);
        if ($chunk === false || $chunk === "") throw new RuntimeException("connection closed");
        $buf .= $chunk;
    }
    return $buf;
}

class Client {
    private int $nextId = 0;
    public function __construct(private $socket) {}
    public function request(int $op, string $payload, int $expected): string {
        $this->nextId++;
        $body = le($op, 2) . le($this->nextId, 4) . $payload;
        socket_write($this->socket, le(strlen($body), 4) . $body);
        $length = unpack("V", read_exact($this->socket, 4))[1];
        $frame = read_exact($this->socket, $length);
        $kind = unpack("v", substr($frame, 0, 2))[1];
        $requestId = unpack("V", substr($frame, 2, 4))[1];
        $data = substr($frame, 6);
        if ($requestId !== $this->nextId) throw new RuntimeException("response id did not match the request");
        if ($kind === ERROR) {
            $reader = new Reader($data);
            throw new RuntimeException("broker error " . $reader->u16() . ": " . $reader->str());
        }
        if ($kind !== $expected) throw new RuntimeException("unexpected frame $kind");
        return $data;
    }
    public function hello(string $user, string $password, string $vhost, string $name): void {
        $writer = new Writer();
        $writer->str(""); $writer->str($name); $writer->str($user); $writer->str($password); $writer->str($vhost);
        $this->request(HELLO, $writer->buf, HELLO_OK);
    }
    public function ensureStream(string $name, array $filters): void {
        $writer = new Writer();
        $writer->str($name);
        $writer->u16(count($filters));
        foreach ($filters as $filter) $writer->str($filter);
        $writer->u16(1); $writer->i64(-1); $writer->i64(-1); $writer->i32(0);
        $this->request(ENSURE_STREAM, $writer->buf, ENSURE_STREAM_OK);
    }
    public function publish(string $subject, string $payload, string $key): array {
        $writer = new Writer();
        $writer->str($subject); $writer->str($key); $writer->u16(0); $writer->blob($payload);
        $reader = new Reader($this->request(PUBLISH, $writer->buf, PUBLISH_OK));
        if ($reader->u16() < 1) throw new RuntimeException("publish was not stored");
        return [$reader->str(), $reader->u16(), $reader->i64()];
    }
    public function ensureConsumer(string $stream, string $name): void {
        $writer = new Writer();
        $writer->str($stream); $writer->str($name); $writer->str("");
        $writer->i32(30000); $writer->i32(5); $writer->i32(1000); $writer->u8(0); $writer->u8(0); $writer->i64(0);
        $this->request(ENSURE_CONSUMER, $writer->buf, ENSURE_CONSUMER_OK);
    }
    public function fetch(string $stream, string $name): array {
        $writer = new Writer();
        $writer->str($stream); $writer->str($name); $writer->u16(32); $writer->i32(0);
        $reader = new Reader($this->request(FETCH, $writer->buf, FETCH_OK));
        if ($reader->u8() === 1) throw new RuntimeException("offset reset at " . $reader->i64());
        $reader->i64();
        $messages = [];
        $count = $reader->u16();
        for ($i = 0; $i < $count; $i++) {
            $partition = $reader->u16();
            $offset = $reader->i64();
            $reader->i64(); $reader->i32(); $reader->str(); $reader->blob();
            $headers = $reader->u16();
            for ($h = 0; $h < $headers; $h++) { $reader->str(); $reader->blob(); }
            $messages[] = [$partition, $offset, $reader->blob()];
        }
        return $messages;
    }
    public function ack(string $stream, string $name, int $partition, int $offset): void {
        $writer = new Writer();
        $writer->str($stream); $writer->str($name); $writer->u16($partition); $writer->i64($offset);
        $this->request(ACK, $writer->buf, ACK_OK);
    }
}

$language = "php";
$body = "hello from $language";
$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if (!socket_connect($socket, $host, $port)) throw new RuntimeException(socket_strerror(socket_last_error()));
$client = new Client($socket);
$client->hello("guest", "guest", "/", "sample-$language");
$client->ensureStream("clients", ["clients.>"]);
[$stream, $partition, $offset] = $client->publish("clients.created", $body, $language);
echo "published $stream partition $partition offset $offset\n";
$consumer = "demo-$language";
$client->ensureConsumer("clients", $consumer);
$found = false;
while (!$found) {
    $messages = $client->fetch("clients", $consumer);
    if (!$messages) throw new RuntimeException("published message was not delivered");
    foreach ($messages as [$part, $messageOffset, $payload]) {
        echo "fetched $messageOffset $payload\n";
        $client->ack("clients", $consumer, $part, $messageOffset);
        $found = $found || $payload === $body;
    }
}
socket_close($socket);
