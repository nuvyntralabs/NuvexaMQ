# Stream, consume, exchange, ping, and management samples.
#   Rscript feature.R ping

args <- commandArgs(trailingOnly = TRUE)
command <- if (length(args) == 0) "ping" else args[[1]]
host <- Sys.getenv("NUVEXA_HOST", "127.0.0.1")
if (!nzchar(host)) host <- "127.0.0.1"
port <- Sys.getenv("NUVEXA_PORT", "5761")
health_port <- Sys.getenv("NUVEXA_HEALTH_PORT", "5762")
management_port <- Sys.getenv("NUVEXA_MANAGEMENT_PORT", "5763")
https_port <- Sys.getenv("NUVEXA_MANAGEMENT_HTTPS_PORT", "5764")
user <- Sys.getenv("NUVEXA_USER", "guest")
password <- Sys.getenv("NUVEXA_PASSWORD", "guest")
vhost <- Sys.getenv("NUVEXA_VHOST", "/")
token <- Sys.getenv("NUVEXA_TOKEN", "")

http <- function(method, url, body = NULL, auth = TRUE) {
  args <- c("-sk", "-o", "/tmp/nuvexa-r-http", "-w", "%{http_code}", "-X", method)
  if (auth) args <- c(args, "-u", paste0(user, ":", password))
  if (!is.null(body)) args <- c(args, "-H", "Content-Type: application/json", "--data", body)
  args <- c(args, url)
  status <- system2("curl", args, stdout = TRUE)
  bytes <- file.info("/tmp/nuvexa-r-http")$size
  cat(sprintf("%s %s %s %.0f bytes\n", method, url, status, bytes))
  if (as.integer(status) >= 400) stop("management request failed")
}

if (command == "admin") {
  http("GET", sprintf("http://%s:%s/health", host, health_port), auth = FALSE)
  http("GET", sprintf("http://%s:%s/metrics", host, health_port), auth = FALSE)
  base <- sprintf("http://%s:%s", host, management_port)
  for (path in c("/api/whoami", "/api/overview", "/api/connections", "/api/channels", "/api/streams", "/api/exchanges", "/api/queues", "/api/bindings", "/api/users", "/api/permissions", "/api/vhosts", "/api/policies")) http("GET", paste0(base, path))
  http("PUT", paste0(base, "/api/vhosts/vh-r"))
  http("PUT", paste0(base, "/api/users/user-r"), '{"password":"sample-pass","tags":["management"]}')
  http("PUT", paste0(base, "/api/permissions"), '{"user":"user-r","vhost":"vh-r","configure":".*","write":".*","read":".*"}')
  http("PUT", paste0(base, "/api/policies/policy-r"), '{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}')
  http("DELETE", paste0(base, "/api/policies/policy-r?vhost=/"))
  http("DELETE", paste0(base, "/api/permissions?user=user-r&vhost=vh-r"))
  http("DELETE", paste0(base, "/api/users/user-r"))
  http("DELETE", paste0(base, "/api/vhosts/vh-r"))
  http("GET", sprintf("https://%s:%s/api/whoami", host, https_port))
  quit(save = "no")
}

con <- socketConnection(host, as.integer(port), open = "a+b", blocking = TRUE)
on.exit(close(con))
next_id <- 0L
w_raw <- function(raw) writeBin(raw, con)
w_u16 <- function(value) writeBin(as.integer(value), con, size = 2, endian = "little")
w_i32 <- function(value) writeBin(as.integer(value), con, size = 4, endian = "little")
w_i64 <- function(value) {
  if (value < 0) w_raw(as.raw(rep(255, 8)))
  else {
    bytes <- raw(8)
    left <- value
    for (i in 1:8) { bytes[i] <- as.raw(left %% 256); left <- left %/% 256 }
    w_raw(bytes)
  }
}
w_string <- function(value) {
  raw <- charToRaw(enc2utf8(value))
  writeBin(as.integer(length(raw)), con, size = 2, endian = "little")
  if (length(raw) > 0) w_raw(raw)
}
w_blob <- function(value) {
  raw <- charToRaw(enc2utf8(value))
  writeBin(as.integer(length(raw)), con, size = 4, endian = "little")
  if (length(raw) > 0) w_raw(raw)
}
read_exact <- function(count) {
  got <- raw()
  while (length(got) < count) {
    chunk <- readBin(con, "raw", count - length(got))
    if (length(chunk) == 0) stop("connection closed")
    got <- c(got, chunk)
  }
  got
}
u16_of <- function(raw, at) as.integer(raw[at]) + as.integer(raw[at + 1]) * 256
i32_of <- function(raw, at) { n <- 0; for (i in 0:3) n <- n + as.integer(raw[at + i]) * (256 ^ i); as.integer(n) }
i64_of <- function(raw, at) { n <- 0; for (i in 0:7) n <- n + as.numeric(raw[at + i]) * (256 ^ i); n }
request <- function(op, payload, expected) {
  next_id <<- next_id + 1L
  writeBin(as.integer(6L + length(payload)), con, size = 4, endian = "little")
  writeBin(as.integer(op), con, size = 2, endian = "little")
  writeBin(as.integer(next_id), con, size = 4, endian = "little")
  if (length(payload) > 0) w_raw(payload)
  body <- read_exact(i32_of(read_exact(4), 1))
  kind <- u16_of(body, 1)
  if (i32_of(body, 3) != next_id) stop("response id did not match the request")
  list(kind = kind, data = if (length(body) > 6) body[7:length(body)] else raw(), i = 1)
}
take <- function(state, count) {
  if (count == 0) return(list(state = state, chunk = raw()))
  if (state$i + count - 1 > length(state$data)) stop("frame ended early")
  list(state = state, chunk = state$data[state$i:(state$i + count - 1)], i = state$i + count)
}
rd_u8 <- function(state) { step <- take(state, 1); list(state = list(kind = state$kind, data = state$data, i = step$i), value = as.integer(step$chunk)) }
rd_u16 <- function(state) { step <- take(state, 2); list(state = list(kind = state$kind, data = state$data, i = step$i), value = u16_of(step$chunk, 1)) }
rd_i32 <- function(state) { step <- take(state, 4); list(state = list(kind = state$kind, data = state$data, i = step$i), value = i32_of(step$chunk, 1)) }
rd_i64 <- function(state) { step <- take(state, 8); list(state = list(kind = state$kind, data = state$data, i = step$i), value = i64_of(step$chunk, 1)) }
rd_str <- function(state) { step <- rd_u16(state); raw <- take(step$state, step$value); list(state = list(kind = state$kind, data = state$data, i = raw$i), value = rawToChar(raw$chunk)) }
rd_blob <- function(state) { step <- rd_i32(state); raw <- take(step$state, step$value); list(state = list(kind = state$kind, data = state$data, i = raw$i), value = rawToChar(raw$chunk)) }
finish <- function(state, expected) {
  if (state$kind == 21) { code <- rd_u16(state); message <- rd_str(code$state); stop(paste("broker error", code$value, message$value)) }
  if (state$kind != expected) stop(paste("unexpected frame", state$kind))
  state
}
payload_of <- function(build) {
  file <- tempfile(); sink_con <- file(file, "wb"); old <- con; con <<- sink_con; build(); close(sink_con); con <<- old
  readBin(file, "raw", file.info(file)$size)
}
receipts <- function(state) {
  count <- rd_u16(state)
  partitions <- numeric()
  cursor <- count$state
  if (count$value > 0) for (i in seq_len(count$value)) {
    stream <- rd_str(cursor); partition <- rd_u16(stream$state); offset <- rd_i64(partition$state)
    cat(sprintf("published %s partition %d offset %.0f\n", stream$value, partition$value, offset$value))
    partitions <- c(partitions, partition$value)
    cursor <- offset$state
  }
  partitions
}
hello <- finish(request(1, payload_of(function() { w_string(token); w_string("sample-r"); w_string(user); w_string(password); w_string(vhost) }), 2), 2)
cat(sprintf("hello %s\n", rd_str(hello)$value))

publish_to <- function(subject, payload, key = "", header = "", header_value = "") {
  receipts(finish(request(5, payload_of(function() {
    w_string(subject); w_string(key)
    if (nzchar(header)) { w_u16(1); w_string(header); w_blob(header_value) } else w_u16(0)
    w_blob(payload)
  }), 6), 6))
}
ensure_stream <- function(name, filter, partitions, max_age, max_bytes, max_message) {
  finish(request(3, payload_of(function() {
    w_string(name); w_u16(1); w_string(filter); w_u16(partitions); w_i64(max_age); w_i64(max_bytes); w_i32(max_message)
  }), 4), 4)
  cat(sprintf("stream %s partitions %d\n", name, partitions))
}
ensure_consumer <- function(stream, name, filter, ephemeral, start, offset) {
  finish(request(7, payload_of(function() {
    w_string(stream); w_string(name); w_string(filter)
    w_i32(30000); w_i32(5); w_i32(1000); writeBin(as.raw(c(ephemeral, start)), con); w_i64(offset)
  }), 8), 8)
  cat(sprintf("consumer %s on %s start %d\n", name, stream, start))
}
fetch_messages <- function(stream, name, max) {
  reader <- finish(request(9, payload_of(function() { w_string(stream); w_string(name); w_u16(max); w_i32(0) }), 10), 10)
  reset <- rd_u8(reader)
  if (reset$value == 1) stop("offset reset")
  first <- rd_i64(reset$state)
  count <- rd_u16(first$state)
  messages <- list()
  state <- count$state
  if (count$value > 0) for (i in seq_len(count$value)) {
    part <- rd_u16(state)
    offset <- rd_i64(part$state)
    state <- rd_i64(offset$state)$state
    attempts <- rd_i32(state)
    state <- rd_str(attempts$state)$state
    state <- rd_blob(state)$state
    headers <- rd_u16(state)
    state <- headers$state
    if (headers$value > 0) for (h in seq_len(headers$value)) { state <- rd_str(state)$state; state <- rd_blob(state)$state }
    text <- rd_blob(state)
    state <- text$state
    messages[[length(messages) + 1]] <- list(partition = part$value, offset = offset$value, attempts = attempts$value, payload = text$value)
  }
  messages
}
find_payload <- function(messages, text) {
  for (message in messages) if (identical(message$payload, text)) return(message)
  NULL
}
cursor <- function(op, expected, stream, name, message) {
  finish(request(op, payload_of(function() { w_string(stream); w_string(name); w_u16(message$partition); w_i64(message$offset) }), expected), expected)
}

if (command == "ping") {
  finish(request(19, raw(), 20), 20)
  cat("pong\n")
} else if (command == "stream") {
  ensure_stream("catalog-r", "catalog-r.>", 2, 86400000, 1048576, 65536)
  keyed <- publish_to("catalog-r.created", '{"id":1}', "alpha", "content-type", "application/json")
  first <- publish_to("catalog-r.created", '{"id":1}')
  second <- publish_to("catalog-r.created", '{"id":1}')
  if (length(keyed) < 1 || first[[1]] == second[[1]]) stop("round-robin did not use both partitions")
} else if (command == "consume") {
  ensure_stream("mailbox-r", "mailbox-r.>", 1, -1, -1, 0)
  ensure_consumer("mailbox-r", "box-r", "mailbox-r.>", 0, 0, 0)
  publish_to("mailbox-r.created", "ack me", "", "content-type", "text/plain")
  publish_to("mailbox-r.created", "nack me", "", "content-type", "text/plain")
  batch <- fetch_messages("mailbox-r", "box-r", 32)
  acked <- find_payload(batch, "ack me")
  nacked <- find_payload(batch, "nack me")
  if (is.null(acked) || is.null(nacked)) stop("expected both deliveries")
  cursor(11, 12, "mailbox-r", "box-r", acked); cat("ack\n")
  cursor(13, 14, "mailbox-r", "box-r", nacked); cat("nack\n")
  again <- find_payload(fetch_messages("mailbox-r", "box-r", 32), "nack me")
  if (is.null(again)) stop("nack was not redelivered")
  cat(sprintf("redelivered %d\n", again$attempts))
  cursor(11, 12, "mailbox-r", "box-r", again)
  finish(request(15, payload_of(function() { w_string("mailbox-r"); w_string("box-r"); writeBin(as.raw(1), con); w_i64(0) }), 16), 16)
  cat("reset box-r offset 0\n")
  reset <- fetch_messages("mailbox-r", "box-r", 1)
  if (length(reset) < 1) stop("reset did not return a message")
  cat(sprintf("after reset offset %.0f\n", reset[[1]]$offset))
  cursor(11, 12, "mailbox-r", "box-r", reset[[1]])
  finish(request(17, payload_of(function() { w_string("mailbox-r"); w_string("tail-r") }), 18), 18)
  cat("release tail-r\n")
  ensure_consumer("mailbox-r", "tail-r", "", 1, 1, 0)
  publish_to("mailbox-r.created", "tail me")
  tailed <- find_payload(fetch_messages("mailbox-r", "tail-r", 32), "tail me")
  if (is.null(tailed)) stop("tail message was not delivered")
  cursor(11, 12, "mailbox-r", "tail-r", tailed)
  finish(request(17, payload_of(function() { w_string("mailbox-r"); w_string("tail-r") }), 18), 18)
  cat("release tail-r\n")
  ensure_consumer("mailbox-r", "from0-r", "", 0, 2, 0)
  cat("offset consumer from0-r\n")
} else if (command == "exchange") {
  declare_exchange <- function(name, kind) {
    finish(request(22, payload_of(function() { w_string(name); w_string(kind); writeBin(as.raw(c(1, 0)), con) }), 23), 23)
    cat(sprintf("exchange %s %s\n", name, kind))
  }
  declare_queue <- function(name, ttl, max, dead, dead_key) {
    finish(request(24, payload_of(function() {
      w_string(name); writeBin(as.raw(c(1, 0, 0)), con); w_i64(ttl); w_i32(max); w_string(dead); w_string(dead_key)
    }), 25), 25)
    cat(sprintf("queue %s\n", name))
  }
  bind <- function(exchange, queue, key, headers) {
    finish(request(26, payload_of(function() {
      w_string(exchange); w_string(queue); w_string(key)
      if (headers) { w_u16(2); w_string("format"); w_string("json"); w_string("x-match"); w_string("all") } else w_u16(0)
    }), 27), 27)
  }
  publish_exchange <- function(exchange, routing, payload, key, header) {
    count <- receipts(finish(request(34, payload_of(function() {
      w_string(exchange); w_string(routing); w_string(key)
      if (header) { w_u16(1); w_string("format"); w_blob("json") } else w_u16(0)
      w_blob(payload)
    }), 35), 35))
    cat(sprintf("routed %s -> %d\n", exchange, length(count)))
    length(count)
  }
  named <- function(op, expected, name, label) {
    finish(request(op, payload_of(function() w_string(name)), expected), expected)
    cat(label, "\n")
  }
  declare_exchange("amq.direct", "direct")
  declare_exchange("direct-r", "direct")
  declare_exchange("fanout-r", "fanout")
  declare_exchange("topic-r", "topic")
  declare_exchange("headers-r", "headers")
  declare_queue("dead-r", -1, -1, "", "")
  declare_queue("work-r", 60000, 100, "direct-r", "expired")
  bind("direct-r", "dead-r", "expired", FALSE)
  bind("direct-r", "work-r", "work.created", FALSE)
  bind("fanout-r", "work-r", "", FALSE)
  bind("topic-r", "work-r", "work.*", FALSE)
  bind("headers-r", "work-r", "", TRUE)
  count <- 0
  count <- count + publish_exchange("direct-r", "work.created", "routed", "order-1", FALSE)
  count <- count + publish_exchange("fanout-r", "", "routed", "", FALSE)
  count <- count + publish_exchange("topic-r", "work.created", "routed", "", FALSE)
  count <- count + publish_exchange("headers-r", "", "routed", "", TRUE)
  count <- count + publish_exchange("", "work-r", "routed", "", FALSE)
  if (count < 5) stop("expected at least 5 receipts")
  named(32, 33, "work-r", "purge queue work-r")
  named(28, 29, "work-r", "delete queue work-r")
  named(28, 29, "dead-r", "delete queue dead-r")
  named(30, 31, "direct-r", "delete exchange direct-r")
  named(30, 31, "fanout-r", "delete exchange fanout-r")
  named(30, 31, "topic-r", "delete exchange topic-r")
  named(30, 31, "headers-r", "delete exchange headers-r")
} else stop("use ping, stream, consume, exchange, or admin")
