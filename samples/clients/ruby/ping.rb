require_relative "client"
client = Nuvexa.connect("ruby")
client.ping
client.close
