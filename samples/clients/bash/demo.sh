#!/bin/bash
# Publish one message, then fetch and ack it.
#   bash demo.sh
# NUVEXA_HOST and NUVEXA_PORT default to 127.0.0.1 and 5761.
# Frames are files because bash variables cannot hold NUL bytes.

set -e
HOST="${NUVEXA_HOST:-127.0.0.1}"
PORT="${NUVEXA_PORT:-5761}"
LANGUAGE=bash
BODY="hello from ${LANGUAGE}"
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
u8() {
  OUT=$(byte_at "$BODYFILE" "$POS")
  POS=$((POS + 1))
}
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

exec 3<>"/dev/tcp/${HOST}/${PORT}"
NEXT=0

reset_payload
w_string ""
w_string "sample-bash"
w_string "guest"
w_string "guest"
w_string "/"
request 1 2

reset_payload
w_string "clients"
w_u16 1
w_string "clients.>"
w_u16 1
w_i64 -1
w_i64 -1
w_i32 0
request 3 4

reset_payload
w_string "clients.created"
w_string "$LANGUAGE"
w_u16 0
w_blob "$BODY"
request 5 6
u16; count=$OUT
if [ "$count" -lt 1 ]; then echo "publish was not stored" >&2; exit 1; fi
read_string; stream=$OUT
u16; partition=$OUT
i32; offset=$OUT; skip 4
echo "published ${stream} partition ${partition} offset ${offset}"

reset_payload
w_string "clients"
w_string "demo-bash"
w_string ""
w_i32 30000
w_i32 5
w_i32 1000
w_u8 0
w_u8 0
w_i64 0
request 7 8

found=0
while [ "$found" = 0 ]; do
  reset_payload
  w_string "clients"
  w_string "demo-bash"
  w_u16 32
  w_i32 0
  request 9 10
  u8; reset=$OUT
  if [ "$reset" = 1 ]; then echo "offset reset" >&2; exit 1; fi
  skip 8
  u16; count=$OUT
  if [ "$count" = 0 ]; then echo "published message was not delivered" >&2; exit 1; fi
  i=0
  while [ "$i" -lt "$count" ]; do
    u16; part=$OUT
    i32; message_offset=$OUT; skip 4
    skip 8
    i32
    skip_string
    skip_blob
    u16; headers=$OUT
    h=0
    while [ "$h" -lt "$headers" ]; do skip_string; skip_blob; h=$((h + 1)); done
    read_blob; text=$OUT
    echo "fetched ${message_offset} ${text}"
    saved_pos=$POS
    saved_body=$(mktemp)
    cp "$BODYFILE" "$saved_body"
    reset_payload
    w_string "clients"
    w_string "demo-bash"
    w_u16 "$part"
    w_i64 "$message_offset"
    request 11 12
    cp "$saved_body" "$BODYFILE"
    rm -f "$saved_body"
    POS=$saved_pos
    if [ "$text" = "$BODY" ]; then found=1; fi
    i=$((i + 1))
  done
done
