-- Publish one message, then fetch and ack it.
--   luarocks install luasocket
--   lua demo.lua
-- NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

local socket = require("socket")
local host = os.getenv("NUVEXA_HOST") or "127.0.0.1"
local port = tonumber(os.getenv("NUVEXA_PORT") or "5761")
local language = "lua"
local body = "hello from " .. language
local tcp = assert(socket.tcp())
tcp:settimeout(10)
assert(tcp:connect(host, port))
local next_id = 0

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
local function str(value)
  return u16(#value) .. value
end
local function blob(value)
  return i32(#value) .. value
end

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
  local function ru16()
    local a, b = take(2):byte(1, 2)
    return a + b * 256
  end
  local function ri32()
    local a, b, c, d = take(4):byte(1, 4)
    return a + b * 256 + c * 65536 + d * 16777216
  end
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

request(1, str("") .. str("sample-lua") .. str("guest") .. str("guest") .. str("/"), 2)
request(3, str("clients") .. u16(1) .. str("clients.>") .. u16(1) .. i64(-1) .. i64(-1) .. i32(0), 4)
local published = request(5, str("clients.created") .. str(language) .. u16(0) .. blob(body), 6)
if published.u16() < 1 then error("publish was not stored") end
print(string.format("published %s partition %d offset %d", published.str(), published.u16(), published.i64()))
request(7, str("clients") .. str("demo-lua") .. str("") .. i32(30000) .. i32(5) .. i32(1000) .. string.char(0, 0) .. i64(0), 8)

local found = false
while not found do
  local messages = request(9, str("clients") .. str("demo-lua") .. u16(32) .. i32(0), 10)
  if messages.u8() == 1 then error("offset reset") end
  messages.i64()
  local count = messages.u16()
  if count == 0 then error("published message was not delivered") end
  for _ = 1, count do
    local part = messages.u16()
    local offset = messages.i64()
    messages.i64(); messages.i32(); messages.str(); messages.blob()
    for _ = 1, messages.u16() do messages.str(); messages.blob() end
    local text = messages.blob()
    print(string.format("fetched %d %s", offset, text))
    request(11, str("clients") .. str("demo-lua") .. u16(part) .. i64(offset), 12)
    found = found or text == body
  end
end
tcp:close()
