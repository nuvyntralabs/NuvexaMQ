#!/bin/bash
# Stream, consume, exchange, ping, and management samples.
#   bash feature.sh ping
# Frames are files because bash variables cannot hold NUL bytes.

set -e
HOST="${NUVEXA_HOST:-127.0.0.1}"
PORT="${NUVEXA_PORT:-5761}"
HEALTH_PORT="${NUVEXA_HEALTH_PORT:-5762}"
MANAGEMENT_PORT="${NUVEXA_MANAGEMENT_PORT:-5763}"
MANAGEMENT_HTTPS_PORT="${NUVEXA_MANAGEMENT_HTTPS_PORT:-5764}"
USER_NAME="${NUVEXA_USER:-guest}"
PASSWORD="${NUVEXA_PASSWORD:-guest}"
VHOST="${NUVEXA_VHOST:-/}"
TOKEN="${NUVEXA_TOKEN:-}"
WORK=$(mktemp -d)
PAYLOAD="$WORK/payload"
BODYFILE="$WORK/body"
trap 'exec 3<&- 2>/dev/null || true; rm -rf "$WORK"' EXIT

w_u8() { printf "\\$(printf '%03o' $(($1 & 255)))" >> "$PAYLOAD"; }
w_u16() { w_u8 "$1"; w_u8 $(($1 >> 8)); }
w_i32() { local i; for i in 0 1 2 3; do w_u8 $(($1 >> (8 * i))); done; }
w_i64() { local i; for i in 0 1 2 3 4 5 6 7; do w_u8 $(($1 >> (8 * i))); done; }
w_chars() {
  local s="$1" i c code
  for ((i = 0; i < ${#s}; i++)); do
    c=${s:i:1}
    printf -v code '%d' "'$c"
    w_u8 "$code"
  done
}
w_string() { w_u16 "${#1}"; w_chars "$1"; }
w_blob() { w_i32 "${#1}"; w_chars "$1"; }
reset_payload() { : > "$PAYLOAD"; }

byte_at() {
  local raw
  raw=$(od -An -tu1 -j "$2" -N 1 "$1")
  raw=${raw//[[:space:]]/}
  if [ -z "$raw" ]; then echo 0; else echo "$raw"; fi
}

POS=0
OUT=0
u8() { OUT=$(byte_at "$BODYFILE" "$POS"); POS=$((POS + 1)); }
u16() { u8; local lo=$OUT; u8; OUT=$((lo + OUT * 256)); }
i32() {
  u8; local b0=$OUT
  u8; local b1=$OUT
  u8; local b2=$OUT
  u8; OUT=$((b0 + b1 * 256 + b2 * 65536 + OUT * 16777216))
}
skip() { POS=$((POS + $1)); }
read_chars() {
  local n="$1" i ch s
  s=""
  for ((i = 0; i < n; i++)); do
    u8
    ch=$OUT
    s+=$(printf "\\$(printf '%03o' "$ch")")
  done
  OUT=$s
}
read_string() { u16; read_chars "$OUT"; }
read_blob() { i32; read_chars "$OUT"; }
skip_string() { u16; skip "$OUT"; }
skip_blob() { i32; skip "$OUT"; }
read_i64_small() { i32; local low=$OUT; skip 4; OUT=$low; }

request() {
  local op="$1" expected="$2"
  NEXT=$((NEXT + 1))
  local plen frame len b0 b1 b2 b3
  plen=$(wc -c < "$PAYLOAD" | tr -d ' ')
  frame="$WORK/frame"
  : > "$frame"
  local saved="$PAYLOAD"
  PAYLOAD="$frame"
  w_i32 $((6 + plen))
  w_u16 "$op"
  w_i32 "$NEXT"
  PAYLOAD="$saved"
  cat "$PAYLOAD" >> "$frame"
  cat "$frame" >&3
  dd bs=1 count=4 <&3 > "$WORK/len" 2>/dev/null
  b0=$(byte_at "$WORK/len" 0); b1=$(byte_at "$WORK/len" 1); b2=$(byte_at "$WORK/len" 2); b3=$(byte_at "$WORK/len" 3)
  len=$((b0 + b1 * 256 + b2 * 65536 + b3 * 16777216))
  dd bs=1 count="$len" <&3 > "$BODYFILE" 2>/dev/null
  POS=0
  local kind request_id
  u16; kind=$OUT
  i32; request_id=$OUT
  if [ "$request_id" != "$NEXT" ]; then echo "response id did not match the request" >&2; exit 1; fi
  if [ "$kind" = 21 ]; then
    u16
    read_string
    echo "broker error: $OUT" >&2
    exit 1
  fi
  if [ "$kind" != "$expected" ]; then echo "unexpected frame $kind" >&2; exit 1; fi
}

hello() {
  reset_payload
  w_string "$TOKEN"
  w_string "sample-bash"
  w_string "$USER_NAME"
  w_string "$PASSWORD"
  w_string "$VHOST"
  request 1 2
  read_string
  echo "hello $OUT"
}

publish_msg() {
  local subject="$1" payload="$2" key="$3" header="$4" value="$5"
  reset_payload
  w_string "$subject"
  w_string "$key"
  if [ -n "$header" ]; then w_u16 1; w_string "$header"; w_blob "$value"; else w_u16 0; fi
  w_blob "$payload"
  request 5 6
  u16; local count=$OUT i=0
  LAST_PARTITION=""
  while [ "$i" -lt "$count" ]; do
    read_string; local stream=$OUT
    u16; local partition=$OUT
    read_i64_small; local offset=$OUT
    echo "published ${stream} partition ${partition} offset ${offset}"
    LAST_PARTITION=$partition
    i=$((i + 1))
  done
  LAST_COUNT=$count
}

fetch_find() {
  local stream="$1" name="$2" max="$3" wanted="$4"
  reset_payload
  w_string "$stream"
  w_string "$name"
  w_u16 "$max"
  w_i32 0
  request 9 10
  u8
  if [ "$OUT" = 1 ]; then echo "offset reset" >&2; exit 1; fi
  skip 8
  u16; local count=$OUT i=0
  FOUND=0
  while [ "$i" -lt "$count" ]; do
    u16; local part=$OUT
    read_i64_small; local offset=$OUT
    skip 8
    i32; local attempts=$OUT
    skip_string
    skip_blob
    u16; local headers=$OUT h=0
    while [ "$h" -lt "$headers" ]; do skip_string; skip_blob; h=$((h + 1)); done
    read_blob; local text=$OUT
    if [ "$text" = "$wanted" ]; then
      FOUND=1
      FOUND_PART=$part
      FOUND_OFFSET=$offset
      FOUND_ATTEMPTS=$attempts
    fi
    i=$((i + 1))
  done
}

cursor() {
  local op="$1" expected="$2" stream="$3" name="$4" part="$5" offset="$6"
  reset_payload
  w_string "$stream"
  w_string "$name"
  w_u16 "$part"
  w_i64 "$offset"
  request "$op" "$expected"
}

http_call() {
  local method="$1" url="$2" body="$3" auth="$4"
  local auth_arg=() data_arg=()
  if [ "$auth" = 1 ]; then auth_arg=(-u "$USER_NAME:$PASSWORD"); fi
  if [ -n "$body" ]; then data_arg=(-H "Content-Type: application/json" --data "$body"); fi
  local code
  code=$(curl -sk -o "$WORK/http" -w '%{http_code}' -X "$method" "${auth_arg[@]}" "${data_arg[@]}" "$url")
  local bytes
  bytes=$(wc -c < "$WORK/http" | tr -d ' ')
  echo "$method $url $code $bytes bytes"
  if [ "$code" -ge 400 ]; then echo "management request failed" >&2; exit 1; fi
}

command="${1:-ping}"
if [ "$command" = "admin" ]; then
  http_call GET "http://${HOST}:${HEALTH_PORT}/health" "" 0
  http_call GET "http://${HOST}:${HEALTH_PORT}/metrics" "" 0
  base="http://${HOST}:${MANAGEMENT_PORT}"
  for path in /api/whoami /api/overview /api/connections /api/channels /api/streams /api/exchanges /api/queues /api/bindings /api/users /api/permissions /api/vhosts /api/policies; do
    http_call GET "${base}${path}" "" 1
  done
  http_call PUT "${base}/api/vhosts/vh-bash" "" 1
  http_call PUT "${base}/api/users/user-bash" '{"password":"sample-pass","tags":["management"]}' 1
  http_call PUT "${base}/api/permissions" '{"user":"user-bash","vhost":"vh-bash","configure":".*","write":".*","read":".*"}' 1
  http_call PUT "${base}/api/policies/policy-bash" '{"vhost":"/","pattern":"sample-.*","priority":1,"messageTtlMs":60000,"maxLength":100,"deadLetterExchange":"","deadLetterRoutingKey":""}' 1
  http_call DELETE "${base}/api/policies/policy-bash?vhost=/" "" 1
  http_call DELETE "${base}/api/permissions?user=user-bash&vhost=vh-bash" "" 1
  http_call DELETE "${base}/api/users/user-bash" "" 1
  http_call DELETE "${base}/api/vhosts/vh-bash" "" 1
  http_call GET "https://${HOST}:${MANAGEMENT_HTTPS_PORT}/api/whoami" "" 1
  exit 0
fi

exec 3<>"/dev/tcp/${HOST}/${PORT}"
NEXT=0
hello

case "$command" in
ping)
  reset_payload
  request 19 20
  echo pong
  ;;
stream)
  reset_payload
  w_string "catalog-bash"; w_u16 1; w_string "catalog-bash.>"; w_u16 2; w_i64 86400000; w_i64 1048576; w_i32 65536
  request 3 4
  echo "stream catalog-bash partitions 2"
  publish_msg "catalog-bash.created" '{"id":1}' "alpha" "content-type" "application/json"
  if [ "$LAST_COUNT" -lt 1 ]; then echo "keyed publish was not stored" >&2; exit 1; fi
  publish_msg "catalog-bash.created" '{"id":1}' "" "" ""
  first=$LAST_PARTITION
  publish_msg "catalog-bash.created" '{"id":1}' "" "" ""
  if [ "$first" = "$LAST_PARTITION" ]; then echo "round-robin did not use both partitions" >&2; exit 1; fi
  ;;
consume)
  reset_payload
  w_string "mailbox-bash"; w_u16 1; w_string "mailbox-bash.>"; w_u16 1; w_i64 -1; w_i64 -1; w_i32 0
  request 3 4
  echo "stream mailbox-bash partitions 1"
  reset_payload
  w_string "mailbox-bash"; w_string "box-bash"; w_string "mailbox-bash.>"
  w_i32 30000; w_i32 5; w_i32 1000; w_u8 0; w_u8 0; w_i64 0
  request 7 8
  echo "consumer box-bash on mailbox-bash start 0"
  publish_msg "mailbox-bash.created" "ack me" "" "content-type" "text/plain"
  publish_msg "mailbox-bash.created" "nack me" "" "content-type" "text/plain"
  fetch_find "mailbox-bash" "box-bash" 32 "ack me"
  if [ "$FOUND" != 1 ]; then echo "expected ack me" >&2; exit 1; fi
  ack_part=$FOUND_PART; ack_offset=$FOUND_OFFSET
  # The same fetch already walked every message. Read it again from the saved body.
  POS=6
  u8; skip 8; u16; count=$OUT; i=0; FOUND=0
  while [ "$i" -lt "$count" ]; do
    u16; part=$OUT
    read_i64_small; offset=$OUT
    skip 8; i32; skip_string; skip_blob
    u16; headers=$OUT; h=0
    while [ "$h" -lt "$headers" ]; do skip_string; skip_blob; h=$((h + 1)); done
    read_blob; text=$OUT
    if [ "$text" = "nack me" ]; then FOUND=1; nack_part=$part; nack_offset=$offset; fi
    i=$((i + 1))
  done
  if [ "$FOUND" != 1 ]; then echo "expected nack me" >&2; exit 1; fi
  cursor 11 12 "mailbox-bash" "box-bash" "$ack_part" "$ack_offset"
  echo ack
  cursor 13 14 "mailbox-bash" "box-bash" "$nack_part" "$nack_offset"
  echo nack
  fetch_find "mailbox-bash" "box-bash" 32 "nack me"
  if [ "$FOUND" != 1 ]; then echo "nack was not redelivered" >&2; exit 1; fi
  echo "redelivered $FOUND_ATTEMPTS"
  cursor 11 12 "mailbox-bash" "box-bash" "$FOUND_PART" "$FOUND_OFFSET"
  reset_payload
  w_string "mailbox-bash"; w_string "box-bash"; w_u8 1; w_i64 0
  request 15 16
  echo "reset box-bash offset 0"
  fetch_find "mailbox-bash" "box-bash" 1 "ack me"
  if [ "$FOUND" != 1 ]; then echo "reset did not return a message" >&2; exit 1; fi
  echo "after reset offset $FOUND_OFFSET"
  cursor 11 12 "mailbox-bash" "box-bash" "$FOUND_PART" "$FOUND_OFFSET"
  reset_payload
  w_string "mailbox-bash"; w_string "tail-bash"
  request 17 18
  echo "release tail-bash"
  reset_payload
  w_string "mailbox-bash"; w_string "tail-bash"; w_string ""
  w_i32 30000; w_i32 5; w_i32 1000; w_u8 1; w_u8 1; w_i64 0
  request 7 8
  echo "consumer tail-bash on mailbox-bash start 1"
  publish_msg "mailbox-bash.created" "tail me" "" "" ""
  fetch_find "mailbox-bash" "tail-bash" 32 "tail me"
  if [ "$FOUND" != 1 ]; then echo "tail message was not delivered" >&2; exit 1; fi
  cursor 11 12 "mailbox-bash" "tail-bash" "$FOUND_PART" "$FOUND_OFFSET"
  reset_payload
  w_string "mailbox-bash"; w_string "tail-bash"
  request 17 18
  echo "release tail-bash"
  reset_payload
  w_string "mailbox-bash"; w_string "from0-bash"; w_string ""
  w_i32 30000; w_i32 5; w_i32 1000; w_u8 0; w_u8 2; w_i64 0
  request 7 8
  echo "offset consumer from0-bash"
  ;;
exchange)
  declare_exchange() {
    reset_payload; w_string "$1"; w_string "$2"; w_u8 1; w_u8 0; request 22 23
    echo "exchange $1 $2"
  }
  declare_queue() {
    reset_payload; w_string "$1"; w_u8 1; w_u8 0; w_u8 0; w_i64 "$2"; w_i32 "$3"; w_string "$4"; w_string "$5"
    request 24 25
    echo "queue $1"
  }
  bind_queue() {
    reset_payload; w_string "$1"; w_string "$2"; w_string "$3"
    if [ "$4" = 1 ]; then w_u16 2; w_string format; w_string json; w_string x-match; w_string all; else w_u16 0; fi
    request 26 27
  }
  publish_exchange() {
    reset_payload; w_string "$1"; w_string "$2"; w_string "$3"
    if [ "$5" = 1 ]; then w_u16 1; w_string format; w_blob json; else w_u16 0; fi
    w_blob "$4"
    request 34 35
    u16; local count=$OUT i=0
    while [ "$i" -lt "$count" ]; do
      read_string; local stream=$OUT
      u16; local partition=$OUT
      read_i64_small; local offset=$OUT
      echo "published ${stream} partition ${partition} offset ${offset}"
      i=$((i + 1))
    done
    echo "routed $1 -> $count"
    ROUTED=$((ROUTED + count))
  }
  named() {
    reset_payload; w_string "$3"; request "$1" "$2"; echo "$4"
  }
  declare_exchange amq.direct direct
  declare_exchange direct-bash direct
  declare_exchange fanout-bash fanout
  declare_exchange topic-bash topic
  declare_exchange headers-bash headers
  declare_queue dead-bash -1 -1 "" ""
  declare_queue work-bash 60000 100 direct-bash expired
  bind_queue direct-bash dead-bash expired 0
  bind_queue direct-bash work-bash work.created 0
  bind_queue fanout-bash work-bash "" 0
  bind_queue topic-bash work-bash "work.*" 0
  bind_queue headers-bash work-bash "" 1
  ROUTED=0
  publish_exchange direct-bash work.created order-1 routed 0
  publish_exchange fanout-bash "" "" routed 0
  publish_exchange topic-bash work.created "" routed 0
  publish_exchange headers-bash "" "" routed 1
  publish_exchange "" work-bash "" routed 0
  if [ "$ROUTED" -lt 5 ]; then echo "expected at least 5 receipts" >&2; exit 1; fi
  named 32 33 work-bash "purge queue work-bash"
  named 28 29 work-bash "delete queue work-bash"
  named 28 29 dead-bash "delete queue dead-bash"
  named 30 31 direct-bash "delete exchange direct-bash"
  named 30 31 fanout-bash "delete exchange fanout-bash"
  named 30 31 topic-bash "delete exchange topic-bash"
  named 30 31 headers-bash "delete exchange headers-bash"
  ;;
*)
  echo "use ping, stream, consume, exchange, or admin" >&2
  exit 1
  ;;
esac
