require_relative "client"
language = "ruby"
stream = "mailbox-#{language}"
durable = "box-#{language}"
tail = "tail-#{language}"
client = Nuvexa.connect(language)
client.ensure_stream(stream, ["#{stream}.>"])
client.ensure_consumer(stream, durable, "#{stream}.>")
plain = [["content-type", "text/plain"]]
client.publish("#{stream}.created", "ack me", "", plain)
client.publish("#{stream}.created", "nack me", "", plain)
found = {}
client.fetch(stream, durable).each { |message| found[message[:payload]] = message }
client.ack(stream, durable, found["ack me"])
puts "ack"
client.nack(stream, durable, found["nack me"])
puts "nack"
again = client.fetch(stream, durable).find { |message| message[:payload] == "nack me" }
raise "nack was not redelivered" if again.nil?
puts "redelivered #{again[:delivery]}"
client.ack(stream, durable, again)
client.reset(stream, durable, 0)
reset = client.fetch(stream, durable, 1)
raise "reset did not return a message" if reset.empty?
puts "after reset offset #{reset[0][:offset]}"
client.ack(stream, durable, reset[0])
client.release(stream, tail)
client.ensure_consumer(stream, tail, "", true, 1)
client.publish("#{stream}.created", "tail me")
tailed = client.fetch(stream, tail).find { |message| message[:payload] == "tail me" }
raise "tail message was not delivered" if tailed.nil?
client.ack(stream, tail, tailed)
client.release(stream, tail)
client.ensure_consumer(stream, "from0-#{language}", "", false, 2, 0)
puts "offset consumer from0-#{language}"
client.close
