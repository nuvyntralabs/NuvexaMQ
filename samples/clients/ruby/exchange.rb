require_relative "client"
language = "ruby"
queue = "work-#{language}"
dead = "dead-#{language}"
direct = "direct-#{language}"
fanout = "fanout-#{language}"
topic = "topic-#{language}"
headers = "headers-#{language}"
client = Nuvexa.connect(language)
client.declare_exchange("amq.direct", "direct")
client.declare_exchange(direct, "direct")
client.declare_exchange(fanout, "fanout")
client.declare_exchange(topic, "topic")
client.declare_exchange(headers, "headers")
client.declare_queue(dead)
client.declare_queue(queue, 60000, 100, direct, "expired")
client.bind(direct, dead, "expired")
client.bind(direct, queue, "work.created")
client.bind(fanout, queue, "")
client.bind(topic, queue, "work.*")
client.bind(headers, queue, "", [["format", "json"], ["x-match", "all"]])
count = 0
count += client.publish_exchange(direct, "work.created", "routed", "order-1").length
count += client.publish_exchange(fanout, "", "routed").length
count += client.publish_exchange(topic, "work.created", "routed").length
count += client.publish_exchange(headers, "", "routed", "", [["format", "json"]]).length
count += client.publish_exchange("", queue, "routed").length
raise "expected at least 5 receipts, got #{count}" if count < 5
client.purge(queue)
client.delete_queue(queue)
client.delete_queue(dead)
[direct, fanout, topic, headers].each { |name| client.delete_exchange(name) }
client.close
