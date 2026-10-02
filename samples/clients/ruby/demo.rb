# Publish one message, then fetch and ack it.
#   ruby demo.rb
# NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

require "socket"

HOST = ENV.fetch("NUVEXA_HOST", "127.0.0.1")
PORT = Integer(ENV.fetch("NUVEXA_PORT", "5761"))
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
ERROR = 21

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
    raise EOFError, "frame ended early" if @i + count > @data.bytesize
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

def read_exact(socket, count)
  buf = "".b
  while buf.bytesize < count
    chunk = socket.read(count - buf.bytesize)
    raise EOFError, "connection closed" if chunk.nil? || chunk.empty?
    buf << chunk
  end
  buf
end

class Client
  def initialize(socket)
    @socket = socket
    @next_id = 0
  end

  def request(op, payload, expected)
    @next_id += 1
    body = [op].pack("S<") + [@next_id].pack("L<") + payload
    @socket.write([body.bytesize].pack("L<") + body)
    length = read_exact(@socket, 4).unpack1("L<")
    frame = read_exact(@socket, length)
    kind, request_id = frame.unpack("S<L<")
    data = frame.byteslice(6..-1)
    raise "response id did not match the request" unless request_id == @next_id
    if kind == ERROR
      reader = Reader.new(data)
      raise "broker error #{reader.u16}: #{reader.string}"
    end
    raise "unexpected frame #{kind}" unless kind == expected
    data
  end

  def hello(user, password, vhost, name)
    writer = Writer.new
    writer.string("")
    writer.string(name)
    writer.string(user)
    writer.string(password)
    writer.string(vhost)
    request(HELLO, writer.bytes, HELLO_OK)
  end

  def ensure_stream(name, filters)
    writer = Writer.new
    writer.string(name)
    writer.u16(filters.length)
    filters.each { |filter| writer.string(filter) }
    writer.u16(1)
    writer.i64(-1)
    writer.i64(-1)
    writer.i32(0)
    request(ENSURE_STREAM, writer.bytes, ENSURE_STREAM_OK)
  end

  def publish(subject, payload, key)
    writer = Writer.new
    writer.string(subject)
    writer.string(key)
    writer.u16(0)
    writer.blob(payload)
    reader = Reader.new(request(PUBLISH, writer.bytes, PUBLISH_OK))
    raise "publish was not stored" if reader.u16 < 1
    [reader.string, reader.u16, reader.i64]
  end

  def ensure_consumer(stream, name)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.string("")
    writer.i32(30_000)
    writer.i32(5)
    writer.i32(1000)
    writer.u8(0)
    writer.u8(0)
    writer.i64(0)
    request(ENSURE_CONSUMER, writer.bytes, ENSURE_CONSUMER_OK)
  end

  def fetch(stream, name)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.u16(32)
    writer.i32(0)
    reader = Reader.new(request(FETCH, writer.bytes, FETCH_OK))
    raise "offset reset at #{reader.i64}" if reader.u8 == 1
    reader.i64
    messages = []
    reader.u16.times do
      partition = reader.u16
      offset = reader.i64
      reader.i64
      reader.i32
      reader.string
      reader.blob
      reader.u16.times do
        reader.string
        reader.blob
      end
      messages << [partition, offset, reader.blob]
    end
    messages
  end

  def ack(stream, name, partition, offset)
    writer = Writer.new
    writer.string(stream)
    writer.string(name)
    writer.u16(partition)
    writer.i64(offset)
    request(ACK, writer.bytes, ACK_OK)
  end
end

language = "ruby"
body = "hello from #{language}"
socket = TCPSocket.new(HOST, PORT)
client = Client.new(socket)
client.hello("guest", "guest", "/", "sample-#{language}")
client.ensure_stream("clients", ["clients.>"])
stream, partition, offset = client.publish("clients.created", body, language)
puts "published #{stream} partition #{partition} offset #{offset}"
consumer = "demo-#{language}"
client.ensure_consumer("clients", consumer)
found = false
until found
  messages = client.fetch("clients", consumer)
  abort "published message was not delivered" if messages.empty?
  messages.each do |part, message_offset, payload|
    puts "fetched #{message_offset} #{payload}"
    client.ack("clients", consumer, part, message_offset)
    found = true if payload == body
  end
end
socket.close
