require "socket"
require "json"
require "net/http"
require "openssl"
require "uri"

HOST = ENV.fetch("NUVEXA_HOST", "127.0.0.1")
PORT = Integer(ENV.fetch("NUVEXA_PORT", "5761"))
HEALTH_PORT = Integer(ENV.fetch("NUVEXA_HEALTH_PORT", "5762"))
MANAGEMENT_PORT = Integer(ENV.fetch("NUVEXA_MANAGEMENT_PORT", "5763"))
MANAGEMENT_HTTPS_PORT = Integer(ENV.fetch("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764"))
TOKEN = ENV.fetch("NUVEXA_TOKEN", "")
USER = ENV.fetch("NUVEXA_USER", "guest")
PASSWORD = ENV.fetch("NUVEXA_PASSWORD", "guest")
VHOST = ENV.fetch("NUVEXA_VHOST", "/")

class Writer
  def initialize
    @buf = "".b
  end
  def u8(value)
    @buf << [value & 0xff].pack("C")
  end
  def u16(value)
    @buf << [value].pack("S<")
  end
  def i32(value)
    @buf << [value].pack("l<")
  end
  def i64(value)
    @buf << [value].pack("q<")
  end
  def string(value)
    raw = value.b
    u16(raw.bytesize)
    @buf << raw
  end
  def blob(value)
    raw = value.b
    i32(raw.bytesize)
    @buf << raw
  end
  def bytes
    @buf
  end
end

class Reader
  def initialize(data)
    @data = data.b
    @i = 0
  end
  def take(count)
    raise "frame ended early" if @i + count > @data.bytesize
    chunk = @data.byteslice(@i, count)
    @i += count
    chunk
  end
  def u8
    take(1).unpack1("C")
  end
  def u16
    take(2).unpack1("S<")
  end
  def i32
    take(4).unpack1("l<")
  end
  def i64
    take(8).unpack1("q<")
  end
  def string
    take(u16)
  end
  def blob
    take(i32)
  end
end

class Nuvexa
  HELLO = 1
  HELLO_OK = 2
  ENSURE_STREAM = 3
  ENSURE_STREAM_OK = 4
  PUBLISH = 5
  PUBLISH_OK = 6
  ENSURE_CONSUMER = 7
  ENSURE_CONSUMER_OK = 8
  FETCH = 9
  FETCH_OK = 10
  ACK = 11
  ACK_OK = 12
  NACK = 13
  NACK_OK = 14
  RESET = 15
  RESET_OK = 16
  RELEASE = 17
  RELEASE_OK = 18
  PING = 19
  PONG = 20
  ERROR = 21
  DECLARE_EXCHANGE = 22
  DECLARE_EXCHANGE_OK = 23
  DECLARE_QUEUE = 24
  DECLARE_QUEUE_OK = 25
  BIND_QUEUE = 26
  BIND_QUEUE_OK = 27
  DELETE_QUEUE = 28
  DELETE_QUEUE_OK = 29
  DELETE_EXCHANGE = 30
  DELETE_EXCHANGE_OK = 31
  PURGE_QUEUE = 32
  PURGE_QUEUE_OK = 33
  PUBLISH_EXCHANGE = 34
  PUBLISH_EXCHANGE_OK = 35

  def initialize(socket)
    @socket = socket
    @next = 0
  end

  def self.connect(language)
    client = new(TCPSocket.new(HOST, PORT))
    client.hello("sample-#{language}")
    client
  end

  def hello(name)
    writer = Writer.new
    writer.string(TOKEN)
    writer.string(name)
    writer.string(USER)
    writer.string(PASSWORD)
    writer.string(VHOST)
    version = Reader.new(request(HELLO, writer.bytes, HELLO_OK)).string
    puts "hello #{version}"
  end

  def ping
    request(PING, "".b, PONG)
    puts "pong"
  end

  def ensure_stream(name, filters, partitions = 1, max_age = -1, max_bytes = -1, max_message = 0)
    writer = Writer.new
    writer.string(name)
    writer.u16(filters.length)
    filters.each { |filter| writer.string(filter) }
    writer.u16(partitions)
    writer.i64(max_age)
    writer.i64(max_bytes)
    writer.i32(max_message)
    request(ENSURE_STREAM, writer.bytes, ENSURE_STREAM_OK)
    puts "stream #{name} partitions #{partitions}"
  end

  def publish(subject, payload, key = "", headers = [])
    receipts(request(PUBLISH, message(subject, key, headers, payload), PUBLISH_OK))
  end

  def ensure_consumer(stream, name, filter = "", ephemeral = false, start = 0, offset = 0)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.string(filter)
    writer.i32(30000)
    writer.i32(5)
    writer.i32(1000)
    writer.u8(ephemeral ? 1 : 0)
    writer.u8(start)
    writer.i64(offset)
    request(ENSURE_CONSUMER, writer.bytes, ENSURE_CONSUMER_OK)
    puts "consumer #{name} on #{stream} start #{start}"
  end

  def fetch(stream, name, max = 32)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.u16(max)
    writer.i32(0)
    reader = Reader.new(request(FETCH, writer.bytes, FETCH_OK))
    raise "offset reset" if reader.u8 == 1
    reader.i64
    messages = []
    reader.u16.times do
      item = { partition: reader.u16, offset: reader.i64 }
      reader.i64
      item[:delivery] = reader.i32
      reader.string
      reader.blob
      reader.u16.times { reader.string; reader.blob }
      item[:payload] = reader.blob
      messages << item
    end
    messages
  end

  def ack(stream, name, message)
    cursor(ACK, ACK_OK, stream, name, message)
  end

  def nack(stream, name, message)
    cursor(NACK, NACK_OK, stream, name, message)
  end

  def reset(stream, name, offset)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.u8(1)
    writer.i64(offset)
    request(RESET, writer.bytes, RESET_OK)
    puts "reset #{name} offset #{offset}"
  end

  def release(stream, name)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    request(RELEASE, writer.bytes, RELEASE_OK)
    puts "release #{name}"
  end

  def declare_exchange(name, type)
    writer = Writer.new
    writer.string(name)
    writer.string(type)
    writer.u8(1)
    writer.u8(0)
    request(DECLARE_EXCHANGE, writer.bytes, DECLARE_EXCHANGE_OK)
    puts "exchange #{name} #{type}"
  end

  def declare_queue(name, ttl = -1, max = -1, dead = "", dead_key = "")
    writer = Writer.new
    writer.string(name)
    writer.u8(1)
    writer.u8(0)
    writer.u8(0)
    writer.i64(ttl)
    writer.i32(max)
    writer.string(dead)
    writer.string(dead_key)
    request(DECLARE_QUEUE, writer.bytes, DECLARE_QUEUE_OK)
    puts "queue #{name}"
  end

  def bind(exchange, queue, routing_key, arguments = [])
    writer = Writer.new
    writer.string(exchange)
    writer.string(queue)
    writer.string(routing_key)
    writer.u16(arguments.length)
    arguments.each { |name, value| writer.string(name); writer.string(value) }
    request(BIND_QUEUE, writer.bytes, BIND_QUEUE_OK)
  end

  def publish_exchange(exchange, routing_key, payload, key = "", headers = [])
    writer = Writer.new
    writer.string(exchange)
    writer.string(routing_key)
    writer.string(key)
    writer.u16(headers.length)
    headers.each { |name, value| writer.string(name); writer.blob(value) }
    writer.blob(payload)
    items = receipts(request(PUBLISH_EXCHANGE, writer.bytes, PUBLISH_EXCHANGE_OK))
    puts "routed #{exchange} -> #{items.length}"
    items
  end

  def purge(name)
    named(PURGE_QUEUE, PURGE_QUEUE_OK, name, "purge queue #{name}")
  end

  def delete_queue(name)
    named(DELETE_QUEUE, DELETE_QUEUE_OK, name, "delete queue #{name}")
  end

  def delete_exchange(name)
    named(DELETE_EXCHANGE, DELETE_EXCHANGE_OK, name, "delete exchange #{name}")
  end

  def close
    @socket.close
  end

  def message(subject, key, headers, payload)
    writer = Writer.new
    writer.string(subject)
    writer.string(key)
    writer.u16(headers.length)
    headers.each { |name, value| writer.string(name); writer.blob(value) }
    writer.blob(payload)
    writer.bytes
  end

  def receipts(data)
    reader = Reader.new(data)
    items = []
    reader.u16.times do
      item = { stream: reader.string, partition: reader.u16, offset: reader.i64 }
      puts "published #{item[:stream]} partition #{item[:partition]} offset #{item[:offset]}"
      items << item
    end
    items
  end

  def cursor(op, expected, stream, name, message)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.u16(message[:partition])
    writer.i64(message[:offset])
    request(op, writer.bytes, expected)
  end

  def named(op, expected, name, label)
    writer = Writer.new
    writer.string(name)
    request(op, writer.bytes, expected)
    puts label
  end

  def request(op, payload, expected)
    @next += 1
    body = [op].pack("S<") + [@next].pack("L<") + payload
    @socket.write([body.bytesize].pack("L<") + body)
    size = read_exact(4).unpack1("L<")
    frame = read_exact(size)
    kind = frame.byteslice(0, 2).unpack1("S<")
    request_id = frame.byteslice(2, 4).unpack1("L<")
    data = frame.byteslice(6, frame.bytesize - 6)
    raise "response id did not match the request" if request_id != @next
    if kind == ERROR
      reader = Reader.new(data)
      raise "broker error #{reader.u16}: #{reader.string}"
    end
    raise "unexpected frame #{kind}" if kind != expected
    data
  end

  def read_exact(count)
    out = "".b
    while out.bytesize < count
      chunk = @socket.read(count - out.bytesize)
      raise "connection closed" if chunk.nil? || chunk.empty?
      out << chunk
    end
    out
  end
end

def nuvexa_http(method, url, body = nil, auth = true)
  uri = URI(url)
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme == "https"
  http.verify_mode = OpenSSL::SSL::VERIFY_NONE if http.use_ssl?
  klass = { "GET" => Net::HTTP::Get, "PUT" => Net::HTTP::Put, "DELETE" => Net::HTTP::Delete }.fetch(method)
  request = klass.new(uri)
  request.basic_auth(USER, PASSWORD) if auth && url.include?("/api/")
  if body
    request["Content-Type"] = "application/json"
    request.body = body
  end
  response = http.request(request)
  puts "#{method} #{url} #{response.code} #{response.body.to_s.bytesize} bytes"
  raise response.body if response.code.to_i >= 400
end
