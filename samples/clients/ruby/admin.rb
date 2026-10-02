require_relative "client"
language = "ruby"
health = "http://#{HOST}:#{HEALTH_PORT}"
nuvexa_http("GET", "#{health}/health", nil, false)
nuvexa_http("GET", "#{health}/metrics", nil, false)
base = "http://#{HOST}:#{MANAGEMENT_PORT}"
["/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"].each do |path|
  nuvexa_http("GET", base + path)
end
vhost = "vh-#{language}"
account = "user-#{language}"
policy = "policy-#{language}"
nuvexa_http("PUT", "#{base}/api/vhosts/#{vhost}")
nuvexa_http("PUT", "#{base}/api/users/#{account}", JSON.generate(password: "sample-pass", tags: ["management"]))
nuvexa_http("PUT", "#{base}/api/permissions", JSON.generate(user: account, vhost: vhost, configure: ".*", write: ".*", read: ".*"))
nuvexa_http("PUT", "#{base}/api/policies/#{policy}", JSON.generate(vhost: "/", pattern: "sample-.*", priority: 1, messageTtlMs: 60000, maxLength: 100, deadLetterExchange: "", deadLetterRoutingKey: ""))
nuvexa_http("DELETE", "#{base}/api/policies/#{policy}?vhost=/")
nuvexa_http("DELETE", "#{base}/api/permissions?user=#{account}&vhost=#{vhost}")
nuvexa_http("DELETE", "#{base}/api/users/#{account}")
nuvexa_http("DELETE", "#{base}/api/vhosts/#{vhost}")
nuvexa_http("GET", "https://#{HOST}:#{MANAGEMENT_HTTPS_PORT}/api/whoami")
