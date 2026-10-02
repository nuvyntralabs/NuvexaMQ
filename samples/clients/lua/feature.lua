-- Stream, consume, exchange, ping, and management samples.
--   lua feature.lua ping

local socket = require("socket")
local host = os.getenv("NUVEXA_HOST") or "127.0.0.1"
local port = tonumber(os.getenv("NUVEXA_PORT") or "5761")
local health_port = os.getenv("NUVEXA_HEALTH_PORT") or "5762"
local management_port = os.getenv("NUVEXA_MANAGEMENT_PORT") or "5763"
local https_port = os.getenv("NUVEXA_MANAGEMENT_HTTPS_PORT") or "5764"
local user = os.getenv("NUVEXA_USER") or "guest"
local password = os.getenv("NUVEXA_PASSWORD") or "guest"
local vhost = os.getenv("NUVEXA_VHOST") or "/"
local token = os.getenv("NUVEXA_TOKEN") or ""
local command = arg[1] or "ping"

local function u16(n)
  n = n % 65536
  return string.char(n % 256, math.floor(n / 256) % 256)
end
local function i32(n)
  if n < 0 then n = n + 4294967296 end
  local out = {}
  for _ = 1, 4 do
    out[#out + 1] = string.char(n % 256)
    n = math.floor(n / 256)
  end
  return table.concat(out)
end
local function i64(n)
  if n < 0 then return string.rep(string.char(255), 8) end
  local out = {}
  for _ = 1, 8 do
    out[#out + 1] = string.char(n % 256)
    n = math.floor(n / 256)
  end
  return table.concat(out)
end
local function str(value) return u16(#value) .. value end
local function blob(value) return i32(#value) .. value end
local function u8(n) return string.char(n % 256) end

local function http(method, url, body, auth)
  local cmd = string.format("curl -sk -o /tmp/nuvexa-lua-http -w '%%{http_code}' -X %s", method)
  if auth then cmd = cmd .. string.format(" -u '%s:%s'", user, password) end
  if body then cmd = cmd .. string.format(" -H 'Content-Type: application/json' --data '%s'", body) end
  cmd = cmd .. string.format(" '%s'", url)
  local pipe = assert(io.popen(cmd))
  local status = pipe:read("*a")
  pipe:close()
  local file = assert(io.open("/tmp/nuvexa-lua-http", "rb"))
  local raw = file:read("*a") or ""
  file:close()
  print(string.format("%s %s %s %d bytes", method, url, status:gsub("%s+", ""), #raw))
  if tonumber(status) >= 400 then error("management request failed") end
end

if command == "admin" then
  http("GET", "http://" .. host .. ":" .. health_port .. "/health", nil, false)
  http("GET", "http://" .. host .. ":" .. health_port .. "/metrics", nil, false)
  local base = "http://" .. host .. ":" .. management_port
  for _, path in ipairs({"/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies"}) do
    http("GET", base .. path, nil, true)
  end
  http("PUT", base .. "/api/vhosts/vh-lua", nil, true)
  http("PUT", base .. "/api/users/user-lua", '{"password":"sample-pass","tags":["management"]}', true)
  http("PUT", base .. "/api/permissions", '{"user":"user-lua","vhost":"vh-lua","configure":".*","write":".*","read":".*"}', true)
  http("PUT", base .. "/api/policies/policy-lua", '{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}', true)
  http("DELETE", base .. "/api/policies/policy-lua?vhost=/", nil, true)
  http("DELETE", base .. "/api/permissions?user=user-lua&vhost=vh-lua", nil, true)
  http("DELETE", base .. "/api/users/user-lua", nil, true)
  http("DELETE", base .. "/api/vhosts/vh-lua", nil, true)
  http("GET", "https://" .. host .. ":" .. https_port .. "/api/whoami", nil, true)
  os.exit(0)
end

local tcp = assert(socket.tcp())
tcp:settimeout(10)
assert(tcp:connect(host, port))
local next_id = 0

local function read_exact(count)
  local parts, got = {}, 0
  while got < count do
    local chunk, err = tcp:receive(count - got)
    if not chunk then error(err or "connection closed") end
    parts[#parts + 1] = chunk
    got = got + #chunk
  end
  return table.concat(parts)
end

local function request(op, payload, expected)
  next_id = next_id + 1
  local frame = u16(op) .. i32(next_id) .. payload
  assert(tcp:send(i32(#frame) .. frame))
  local size = read_exact(4)
  local b1, b2, b3, b4 = size:byte(1, 4)
  local length = b1 + b2 * 256 + b3 * 65536 + b4 * 16777216
  local response = read_exact(length)
  local kind = response:byte(1) + response:byte(2) * 256
  local request_id = response:byte(3) + response:byte(4) * 256 + response:byte(5) * 65536 + response:byte(6) * 16777216
  if request_id ~= next_id then error("response id did not match the request") end
  local data = response:sub(7)
  local pos = 1
  local function take(n)
    if pos + n - 1 > #data then error("frame ended early") end
    local chunk = data:sub(pos, pos + n - 1)
    pos = pos + n
    return chunk
  end
  local function ru8() return take(1):byte() end
  local function ru16() local a, b = take(2):byte(1, 2); return a + b * 256 end
  local function ri32() local a, b, c, d = take(4):byte(1, 4); return a + b * 256 + c * 65536 + d * 16777216 end
  local function ri64()
    local n = 0
    for i = 0, 7 do n = n + take(1):byte() * (256 ^ i) end
    return n
  end
  local function rstr() return take(ru16()) end
  local function rblob() return take(ri32()) end
  if kind == 21 then error("broker error " .. ru16() .. ": " .. rstr()) end
  if kind ~= expected then error("unexpected frame " .. kind) end
  return { u8 = ru8, u16 = ru16, i32 = ri32, i64 = ri64, str = rstr, blob = rblob }
end

local function receipts(reader)
  local partitions = {}
  for _ = 1, reader.u16() do
    local stream = reader.str()
    local partition = reader.u16()
    local offset = reader.i64()
    print(string.format("published %s partition %d offset %d", stream, partition, offset))
    partitions[#partitions + 1] = partition
  end
  return partitions
end

local hello = request(1, str(token) .. str("sample-lua") .. str(user) .. str(password) .. str(vhost), 2)
print("hello " .. hello.str())

local function publish(subject, payload, key, header, header_value)
  local headers = ""
  if header and header ~= "" then headers = u16(1) .. str(header) .. blob(header_value) else headers = u16(0) end
  return receipts(request(5, str(subject) .. str(key or "") .. headers .. blob(payload), 6))
end

local function ensure_stream(name, filter, partitions, max_age, max_bytes, max_message)
  request(3, str(name) .. u16(1) .. str(filter) .. u16(partitions) .. i64(max_age) .. i64(max_bytes) .. i32(max_message), 4)
  print(string.format("stream %s partitions %d", name, partitions))
end

local function ensure_consumer(stream, name, filter, ephemeral, start, offset)
  request(7, str(stream) .. str(name) .. str(filter or "") .. i32(30000) .. i32(5) .. i32(1000) .. u8(ephemeral) .. u8(start) .. i64(offset), 8)
  print(string.format("consumer %s on %s start %d", name, stream, start))
end

local function fetch(stream, name, max)
  local reader = request(9, str(stream) .. str(name) .. u16(max) .. i32(0), 10)
  if reader.u8() == 1 then error("offset reset") end
  reader.i64()
  local messages = {}
  for _ = 1, reader.u16() do
    local partition = reader.u16()
    local offset = reader.i64()
    reader.i64(); local attempts = reader.i32(); reader.str(); reader.blob()
    for _ = 1, reader.u16() do reader.str(); reader.blob() end
    messages[#messages + 1] = { partition = partition, offset = offset, attempts = attempts, payload = reader.blob() }
  end
  return messages
end

local function find_payload(messages, text)
  for _, message in ipairs(messages) do if message.payload == text then return message end end
  return nil
end

local function cursor(op, expected, stream, name, message)
  request(op, str(stream) .. str(name) .. u16(message.partition) .. i64(message.offset), expected)
end

if command == "ping" then
  request(19, "", 20)
  print("pong")
elseif command == "stream" then
  ensure_stream("catalog-lua", "catalog-lua.>", 2, 86400000, 1048576, 65536)
  local keyed = publish("catalog-lua.created", '{"id":1}', "alpha", "content-type", "application/json")
  local first = publish("catalog-lua.created", '{"id":1}', "", "", "")
  local second = publish("catalog-lua.created", '{"id":1}', "", "", "")
  if #keyed < 1 or first[1] == second[1] then error("round-robin did not use both partitions") end
elseif command == "consume" then
  ensure_stream("mailbox-lua", "mailbox-lua.>", 1, -1, -1, 0)
  ensure_consumer("mailbox-lua", "box-lua", "mailbox-lua.>", 0, 0, 0)
  publish("mailbox-lua.created", "ack me", "", "content-type", "text/plain")
  publish("mailbox-lua.created", "nack me", "", "content-type", "text/plain")
  local batch = fetch("mailbox-lua", "box-lua", 32)
  local acked = find_payload(batch, "ack me")
  local nacked = find_payload(batch, "nack me")
  if not acked or not nacked then error("expected both deliveries") end
  cursor(11, 12, "mailbox-lua", "box-lua", acked); print("ack")
  cursor(13, 14, "mailbox-lua", "box-lua", nacked); print("nack")
  local again = find_payload(fetch("mailbox-lua", "box-lua", 32), "nack me")
  if not again then error("nack was not redelivered") end
  print("redelivered " .. again.attempts)
  cursor(11, 12, "mailbox-lua", "box-lua", again)
  request(15, str("mailbox-lua") .. str("box-lua") .. u8(1) .. i64(0), 16)
  print("reset box-lua offset 0")
  local reset = fetch("mailbox-lua", "box-lua", 1)
  if #reset < 1 then error("reset did not return a message") end
  print("after reset offset " .. reset[1].offset)
  cursor(11, 12, "mailbox-lua", "box-lua", reset[1])
  request(17, str("mailbox-lua") .. str("tail-lua"), 18)
  print("release tail-lua")
  ensure_consumer("mailbox-lua", "tail-lua", "", 1, 1, 0)
  publish("mailbox-lua.created", "tail me", "", "", "")
  local tailed = find_payload(fetch("mailbox-lua", "tail-lua", 32), "tail me")
  if not tailed then error("tail message was not delivered") end
  cursor(11, 12, "mailbox-lua", "tail-lua", tailed)
  request(17, str("mailbox-lua") .. str("tail-lua"), 18)
  print("release tail-lua")
  ensure_consumer("mailbox-lua", "from0-lua", "", 0, 2, 0)
  print("offset consumer from0-lua")
elseif command == "exchange" then
  local function declare_exchange(name, kind)
    request(22, str(name) .. str(kind) .. u8(1) .. u8(0), 23)
    print("exchange " .. name .. " " .. kind)
  end
  local function declare_queue(name, ttl, max, dead, dead_key)
    request(24, str(name) .. u8(1) .. u8(0) .. u8(0) .. i64(ttl) .. i32(max) .. str(dead) .. str(dead_key), 25)
    print("queue " .. name)
  end
  local function bind(exchange, queue, key, headers)
    local args = headers and (u16(2) .. str("format") .. str("json") .. str("x-match") .. str("all")) or u16(0)
    request(26, str(exchange) .. str(queue) .. str(key) .. args, 27)
  end
  local function publish_exchange(exchange, routing, payload, key, header)
    local headers = header and (u16(1) .. str("format") .. blob("json")) or u16(0)
    local count = #receipts(request(34, str(exchange) .. str(routing) .. str(key) .. headers .. blob(payload), 35))
    print("routed " .. exchange .. " -> " .. count)
    return count
  end
  local function named(op, expected, name, label)
    request(op, str(name), expected)
    print(label)
  end
  declare_exchange("amq.direct", "direct")
  declare_exchange("direct-lua", "direct")
  declare_exchange("fanout-lua", "fanout")
  declare_exchange("topic-lua", "topic")
  declare_exchange("headers-lua", "headers")
  declare_queue("dead-lua", -1, -1, "", "")
  declare_queue("work-lua", 60000, 100, "direct-lua", "expired")
  bind("direct-lua", "dead-lua", "expired", false)
  bind("direct-lua", "work-lua", "work.created", false)
  bind("fanout-lua", "work-lua", "", false)
  bind("topic-lua", "work-lua", "work.*", false)
  bind("headers-lua", "work-lua", "", true)
  local count = 0
  count = count + publish_exchange("direct-lua", "work.created", "routed", "order-1", false)
  count = count + publish_exchange("fanout-lua", "", "routed", "", false)
  count = count + publish_exchange("topic-lua", "work.created", "routed", "", false)
  count = count + publish_exchange("headers-lua", "", "routed", "", true)
  count = count + publish_exchange("", "work-lua", "routed", "", false)
  if count < 5 then error("expected at least 5 receipts") end
  named(32, 33, "work-lua", "purge queue work-lua")
  named(28, 29, "work-lua", "delete queue work-lua")
  named(28, 29, "dead-lua", "delete queue dead-lua")
  named(30, 31, "direct-lua", "delete exchange direct-lua")
  named(30, 31, "fanout-lua", "delete exchange fanout-lua")
  named(30, 31, "topic-lua", "delete exchange topic-lua")
  named(30, 31, "headers-lua", "delete exchange headers-lua")
else
  error("use ping, stream, consume, exchange, or admin")
end
tcp:close()
