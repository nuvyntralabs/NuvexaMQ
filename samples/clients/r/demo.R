# Publish one message, then fetch and ack it.
#   Rscript demo.R
# NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.

host <- Sys.getenv("NUVEXA_HOST", "127.0.0.1")
port <- as.integer(Sys.getenv("NUVEXA_PORT", "5761"))
language <- "r"
body <- paste0("hello from ", language)
con <- socketConnection(host, port, open = "a+b", blocking = TRUE)
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
    for (i in 1:8) {
      bytes[i] <- as.raw(left %% 256)
      left <- left %/% 256
    }
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
i32_of <- function(raw, at) {
  n <- 0
  for (i in 0:3) n <- n + as.integer(raw[at + i]) * (256 ^ i)
  as.integer(n)
}
i64_of <- function(raw, at) {
  n <- 0
  for (i in 0:7) n <- n + as.numeric(raw[at + i]) * (256 ^ i)
  n
}

request <- function(op, payload, expected) {
  next_id <<- next_id + 1L
  length <- 6L + length(payload)
  writeBin(as.integer(length), con, size = 4, endian = "little")
  writeBin(as.integer(op), con, size = 2, endian = "little")
  writeBin(as.integer(next_id), con, size = 4, endian = "little")
  if (length(payload) > 0) w_raw(payload)
  size_raw <- read_exact(4)
  body <- read_exact(i32_of(size_raw, 1))
  kind <- u16_of(body, 1)
  request_id <- i32_of(body, 3)
  if (request_id != next_id) stop("response id did not match the request")
  list(kind = kind, data = if (length(body) > 6) body[7:length(body)] else raw(), i = 1)
}

take <- function(state, count) {
  if (count == 0) return(list(state = state, chunk = raw()))
  if (state$i + count - 1 > length(state$data)) stop("frame ended early")
  chunk <- state$data[state$i:(state$i + count - 1)]
  state$i <- state$i + count
  list(state = state, chunk = chunk)
}
rd_u8 <- function(state) { step <- take(state, 1); list(state = step$state, value = as.integer(step$chunk)) }
rd_u16 <- function(state) { step <- take(state, 2); list(state = step$state, value = u16_of(step$chunk, 1)) }
rd_i32 <- function(state) { step <- take(state, 4); list(state = step$state, value = i32_of(step$chunk, 1)) }
rd_i64 <- function(state) { step <- take(state, 8); list(state = step$state, value = i64_of(step$chunk, 1)) }
rd_str <- function(state) {
  step <- rd_u16(state)
  raw <- take(step$state, step$value)
  list(state = raw$state, value = rawToChar(raw$chunk))
}
rd_blob <- function(state) {
  step <- rd_i32(state)
  raw <- take(step$state, step$value)
  list(state = raw$state, value = rawToChar(raw$chunk))
}

finish <- function(state, expected) {
  if (state$kind == 21) {
    code <- rd_u16(state)
    message <- rd_str(code$state)
    stop(paste("broker error", code$value, message$value))
  }
  if (state$kind != expected) stop(paste("unexpected frame", state$kind))
  state
}

payload_of <- function(build) {
  file <- tempfile()
  on.exit(unlink(file))
  sink_con <- file(file, "wb")
  old <- con
  con <<- sink_con
  build()
  close(sink_con)
  con <<- old
  readBin(file, "raw", file.info(file)$size)
}

hello <- payload_of(function() {
  w_string(""); w_string("sample-r"); w_string("guest"); w_string("guest"); w_string("/")
})
finish(request(1, hello, 2), 2)

stream <- payload_of(function() {
  w_string("clients"); w_u16(1); w_string("clients.>"); w_u16(1); w_i64(-1); w_i64(-1); w_i32(0)
})
finish(request(3, stream, 4), 4)

published_payload <- payload_of(function() {
  w_string("clients.created"); w_string(language); w_u16(0); w_blob(body)
})
published <- finish(request(5, published_payload, 6), 6)
count <- rd_u16(published)
if (count$value < 1) stop("publish was not stored")
stream_name <- rd_str(count$state)
partition <- rd_u16(stream_name$state)
offset <- rd_i64(partition$state)
cat(sprintf("published %s partition %d offset %.0f\n", stream_name$value, partition$value, offset$value))

consumer <- payload_of(function() {
  w_string("clients"); w_string("demo-r"); w_string("")
  w_i32(30000); w_i32(5); w_i32(1000); writeBin(as.raw(c(0, 0)), con); w_i64(0)
})
finish(request(7, consumer, 8), 8)

found <- FALSE
while (!found) {
  fetch <- payload_of(function() { w_string("clients"); w_string("demo-r"); w_u16(32); w_i32(0) })
  messages <- finish(request(9, fetch, 10), 10)
  reset <- rd_u8(messages)
  if (reset$value == 1) stop("offset reset")
  first <- rd_i64(reset$state)
  count <- rd_u16(first$state)
  if (count$value == 0) stop("published message was not delivered")
  state <- count$state
  for (i in seq_len(count$value)) {
    part <- rd_u16(state)
    message_offset <- rd_i64(part$state)
    state <- rd_i64(message_offset$state)$state
    state <- rd_i32(state)$state
    state <- rd_str(state)$state
    state <- rd_blob(state)$state
    headers <- rd_u16(state)
    state <- headers$state
    if (headers$value > 0) for (h in seq_len(headers$value)) {
      state <- rd_str(state)$state
      state <- rd_blob(state)$state
    }
    text <- rd_blob(state)
    state <- text$state
    cat(sprintf("fetched %.0f %s\n", message_offset$value, text$value))
    ack <- payload_of(function() {
      w_string("clients"); w_string("demo-r"); w_u16(part$value); w_i64(message_offset$value)
    })
    finish(request(11, ack, 12), 12)
    found <- found || identical(text$value, body)
  }
}
