<?php
// Shared length-prefixed client used by the feature samples.

function nuvexa_env(string $name, string $fallback): string {
    $value = getenv($name);
    return $value === false || $value === "" ? $fallback : $value;
}

function nuvexa_le(int $value, int $width): string {
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
    public function u16(int $value): void { $this->buf .= nuvexa_le($value, 2); }
    public function i32(int $value): void { $this->buf .= nuvexa_le($value, 4); }
    public function i64(int $value): void { $this->buf .= nuvexa_le($value, 8); }
    public function str(string $value): void { $this->u16(strlen($value)); $this->buf .= $value; }
    public function blob(string $value): void { $this->i32(strlen($value)); $this->buf .= $value; }
}

class Reader {
    public function __construct(private string $data, private int $i = 0) {}
    private function take(int $count): string {
        if ($this->i + $count > strlen($this->data)) throw new RuntimeException("frame ended early");
        $chunk = substr($this->data, $this->i, $count);
        $this->i += $count;
        return $chunk;
    }
    public function u8(): int { return ord($this->take(1)); }
    public function u16(): int { $raw = $this->take(2); return ord($raw[0]) | (ord($raw[1]) << 8); }
    public function i32(): int { $raw = $this->take(4); $value = 0; for ($i = 0; $i < 4; $i++) $value |= ord($raw[$i]) << (8 * $i); return $value >= 0x80000000 ? $value - 0x100000000 : $value; }
    public function i64(): int { $raw = $this->take(8); $value = 0; for ($i = 0; $i < 8; $i++) $value |= ord($raw[$i]) << (8 * $i); return $value; }
    public function str(): string { return $this->take($this->u16()); }
    public function blob(): string { return $this->take($this->i32()); }
}

class Nuvexa {
    const HELLO = 1; const HELLO_OK = 2; const ENSURE_STREAM = 3; const ENSURE_STREAM_OK = 4;
    const PUBLISH = 5; const PUBLISH_OK = 6; const ENSURE_CONSUMER = 7; const ENSURE_CONSUMER_OK = 8;
    const FETCH = 9; const FETCH_OK = 10; const ACK = 11; const ACK_OK = 12; const NACK = 13; const NACK_OK = 14;
    const RESET = 15; const RESET_OK = 16; const RELEASE = 17; const RELEASE_OK = 18; const PING = 19; const PONG = 20; const ERROR = 21;
    const DECLARE_EXCHANGE = 22; const DECLARE_EXCHANGE_OK = 23; const DECLARE_QUEUE = 24; const DECLARE_QUEUE_OK = 25;
    const BIND_QUEUE = 26; const BIND_QUEUE_OK = 27; const DELETE_QUEUE = 28; const DELETE_QUEUE_OK = 29;
    const DELETE_EXCHANGE = 30; const DELETE_EXCHANGE_OK = 31; const PURGE_QUEUE = 32; const PURGE_QUEUE_OK = 33;
    const PUBLISH_EXCHANGE = 34; const PUBLISH_EXCHANGE_OK = 35;
    private $socket;
    private int $next = 0;
    public function __construct($socket) { $this->socket = $socket; }
    public function hello(string $name): void {
        $w = new Writer();
        $w->str(nuvexa_env("NUVEXA_TOKEN", ""));
        $w->str($name);
        $w->str(nuvexa_env("NUVEXA_USER", "guest"));
        $w->str(nuvexa_env("NUVEXA_PASSWORD", "guest"));
        $w->str(nuvexa_env("NUVEXA_VHOST", "/"));
        $version = (new Reader($this->request(self::HELLO, $w->buf, self::HELLO_OK)))->str();
        echo "hello $version\n";
    }
    public function ping(): void { $this->request(self::PING, "", self::PONG); echo "pong\n"; }
    public function ensureStream(string $name, array $filters, int $partitions = 1, int $maxAge = -1, int $maxBytes = -1, int $maxMessage = 0): void {
        $w = new Writer();
        $w->str($name); $w->u16(count($filters));
        foreach ($filters as $filter) $w->str($filter);
        $w->u16($partitions); $w->i64($maxAge); $w->i64($maxBytes); $w->i32($maxMessage);
        $this->request(self::ENSURE_STREAM, $w->buf, self::ENSURE_STREAM_OK);
        echo "stream $name partitions $partitions\n";
    }
    public function publish(string $subject, string $payload, string $key = "", array $headers = []): array {
        return $this->receipts($this->request(self::PUBLISH, $this->message($subject, $key, $headers, $payload), self::PUBLISH_OK));
    }
    public function ensureConsumer(string $stream, string $name, string $filter = "", bool $ephemeral = false, int $start = 0, int $offset = 0): void {
        $w = new Writer();
        $w->str($stream); $w->str($name); $w->str($filter);
        $w->i32(30000); $w->i32(5); $w->i32(1000);
        $w->u8($ephemeral ? 1 : 0); $w->u8($start); $w->i64($offset);
        $this->request(self::ENSURE_CONSUMER, $w->buf, self::ENSURE_CONSUMER_OK);
        echo "consumer $name on $stream start $start\n";
    }
    public function fetch(string $stream, string $name, int $max = 32): array {
        $w = new Writer();
        $w->str($stream); $w->str($name); $w->u16($max); $w->i32(0);
        $r = new Reader($this->request(self::FETCH, $w->buf, self::FETCH_OK));
        if ($r->u8() === 1) throw new RuntimeException("offset reset");
        $r->i64();
        $messages = [];
        $count = $r->u16();
        for ($i = 0; $i < $count; $i++) {
            $partition = $r->u16();
            $offset = $r->i64();
            $r->i64();
            $delivery = $r->i32();
            $r->str(); $r->blob();
            $headers = $r->u16();
            for ($h = 0; $h < $headers; $h++) { $r->str(); $r->blob(); }
            $messages[] = ["partition" => $partition, "offset" => $offset, "delivery" => $delivery, "payload" => $r->blob()];
        }
        return $messages;
    }
    public function ack(string $stream, string $name, array $message): void { $this->cursor(self::ACK, self::ACK_OK, $stream, $name, $message); }
    public function nack(string $stream, string $name, array $message): void { $this->cursor(self::NACK, self::NACK_OK, $stream, $name, $message); }
    public function reset(string $stream, string $name, int $offset): void {
        $w = new Writer();
        $w->str($stream); $w->str($name); $w->u8(1); $w->i64($offset);
        $this->request(self::RESET, $w->buf, self::RESET_OK);
        echo "reset $name offset $offset\n";
    }
    public function release(string $stream, string $name): void {
        $w = new Writer();
        $w->str($stream); $w->str($name);
        $this->request(self::RELEASE, $w->buf, self::RELEASE_OK);
        echo "release $name\n";
    }
    public function declareExchange(string $name, string $type): void {
        $w = new Writer();
        $w->str($name); $w->str($type); $w->u8(1); $w->u8(0);
        $this->request(self::DECLARE_EXCHANGE, $w->buf, self::DECLARE_EXCHANGE_OK);
        echo "exchange $name $type\n";
    }
    public function declareQueue(string $name, int $ttl = -1, int $max = -1, string $dead = "", string $deadKey = ""): void {
        $w = new Writer();
        $w->str($name); $w->u8(1); $w->u8(0); $w->u8(0); $w->i64($ttl); $w->i32($max); $w->str($dead); $w->str($deadKey);
        $this->request(self::DECLARE_QUEUE, $w->buf, self::DECLARE_QUEUE_OK);
        echo "queue $name\n";
    }
    public function bind(string $exchange, string $queue, string $key, array $arguments = []): void {
        $w = new Writer();
        $w->str($exchange); $w->str($queue); $w->str($key); $w->u16(count($arguments));
        foreach ($arguments as $argument) { $w->str($argument[0]); $w->str($argument[1]); }
        $this->request(self::BIND_QUEUE, $w->buf, self::BIND_QUEUE_OK);
    }
    public function publishExchange(string $exchange, string $routingKey, string $payload, string $key = "", array $headers = []): array {
        $w = new Writer();
        $w->str($exchange); $w->str($routingKey); $w->str($key); $w->u16(count($headers));
        foreach ($headers as $header) { $w->str($header[0]); $w->blob($header[1]); }
        $w->blob($payload);
        $receipts = $this->receipts($this->request(self::PUBLISH_EXCHANGE, $w->buf, self::PUBLISH_EXCHANGE_OK));
        echo "routed $exchange -> " . count($receipts) . "\n";
        return $receipts;
    }
    public function purge(string $name): void { $this->named(self::PURGE_QUEUE, self::PURGE_QUEUE_OK, $name, "purge queue $name"); }
    public function deleteQueue(string $name): void { $this->named(self::DELETE_QUEUE, self::DELETE_QUEUE_OK, $name, "delete queue $name"); }
    public function deleteExchange(string $name): void { $this->named(self::DELETE_EXCHANGE, self::DELETE_EXCHANGE_OK, $name, "delete exchange $name"); }
    public function close(): void { fclose($this->socket); }
    private function message(string $subject, string $key, array $headers, string $payload): string {
        $w = new Writer();
        $w->str($subject); $w->str($key); $w->u16(count($headers));
        foreach ($headers as $header) { $w->str($header[0]); $w->blob($header[1]); }
        $w->blob($payload);
        return $w->buf;
    }
    private function receipts(string $data): array {
        $r = new Reader($data);
        $out = [];
        $count = $r->u16();
        for ($i = 0; $i < $count; $i++) {
            $item = ["stream" => $r->str(), "partition" => $r->u16(), "offset" => $r->i64()];
            echo "published {$item["stream"]} partition {$item["partition"]} offset {$item["offset"]}\n";
            $out[] = $item;
        }
        return $out;
    }
    private function cursor(int $op, int $expected, string $stream, string $name, array $message): void {
        $w = new Writer();
        $w->str($stream); $w->str($name); $w->u16($message["partition"]); $w->i64($message["offset"]);
        $this->request($op, $w->buf, $expected);
    }
    private function named(int $op, int $expected, string $name, string $label): void {
        $w = new Writer();
        $w->str($name);
        $this->request($op, $w->buf, $expected);
        echo "$label\n";
    }
    private function request(int $op, string $payload, int $expected): string {
        $this->next++;
        $body = nuvexa_le($op, 2) . nuvexa_le($this->next, 4) . $payload;
        $frame = nuvexa_le(strlen($body), 4) . $body;
        $written = 0;
        while ($written < strlen($frame)) {
            $n = fwrite($this->socket, substr($frame, $written));
            if ($n === false || $n === 0) throw new RuntimeException("write failed");
            $written += $n;
        }
        $size = $this->readExact(4);
        $length = ord($size[0]) | (ord($size[1]) << 8) | (ord($size[2]) << 16) | (ord($size[3]) << 24);
        $response = $this->readExact($length);
        $kind = ord($response[0]) | (ord($response[1]) << 8);
        $id = ord($response[2]) | (ord($response[3]) << 8) | (ord($response[4]) << 16) | (ord($response[5]) << 24);
        if ($id !== $this->next) throw new RuntimeException("response id did not match the request");
        $data = substr($response, 6);
        if ($kind === self::ERROR) {
            $r = new Reader($data);
            throw new RuntimeException("broker error " . $r->u16() . ": " . $r->str());
        }
        if ($kind !== $expected) throw new RuntimeException("unexpected frame $kind");
        return $data;
    }
    private function readExact(int $count): string {
        $out = "";
        while (strlen($out) < $count) {
            $chunk = fread($this->socket, $count - strlen($out));
            if ($chunk === false || $chunk === "") throw new RuntimeException("connection closed");
            $out .= $chunk;
        }
        return $out;
    }
}

function connect_nuvexa(string $language): Nuvexa {
    $host = nuvexa_env("NUVEXA_HOST", "127.0.0.1");
    $port = intval(nuvexa_env("NUVEXA_PORT", "5761"));
    $socket = stream_socket_client("tcp://$host:$port", $errno, $error, 5);
    if ($socket === false) throw new RuntimeException($error);
    $client = new Nuvexa($socket);
    $client->hello("sample-$language");
    return $client;
}

function nuvexa_http(string $method, string $url, ?string $body = null, bool $auth = true): void {
    $headers = $body === null ? [] : ["Content-Type: application/json"];
    if ($auth && str_contains($url, "/api/")) {
        $user = nuvexa_env("NUVEXA_USER", "guest");
        $password = nuvexa_env("NUVEXA_PASSWORD", "guest");
        $headers[] = "Authorization: Basic " . base64_encode("$user:$password");
    }
    $context = stream_context_create(["http" => [
        "method" => $method,
        "header" => implode("\r\n", $headers),
        "content" => $body ?? "",
        "ignore_errors" => true,
    ], "ssl" => ["verify_peer" => false, "verify_peer_name" => false]]);
    $raw = file_get_contents($url, false, $context);
    $status = 0;
    foreach ($http_response_header ?? [] as $line) {
        if (preg_match("#HTTP/\\S+ (\\d+)#", $line, $match)) $status = intval($match[1]);
    }
    echo "$method $url $status " . strlen($raw === false ? "" : $raw) . " bytes\n";
    if ($status >= 400) throw new RuntimeException($raw === false ? "request failed" : $raw);
}
