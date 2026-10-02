require_relative "client"
language = "ruby"
stream = "catalog-#{language}"
client = Nuvexa.connect(language)
client.ensure_stream(stream, ["#{stream}.>"], 2, 86_400_000, 1_048_576, 65_536)
body = '{"id":1}'
headers = [["content-type", "application/json"]]
keyed = client.publish("#{stream}.created", body, "alpha", headers)
first = client.publish("#{stream}.created", body)
second = client.publish("#{stream}.created", body)
raise "round-robin did not use both partitions" if keyed.empty? || first[0][:partition] == second[0][:partition]
client.close
