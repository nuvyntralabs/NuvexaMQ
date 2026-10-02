#!/usr/bin/env python3
"""Durable consume, ack, nack, reset, an ephemeral tail, and a fixed start offset."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from nuvexa import START_OFFSET, START_TAIL, connect

LANGUAGE = "python"
STREAM = f"mailbox-{LANGUAGE}"
DURABLE = f"box-{LANGUAGE}"
TAIL = f"tail-{LANGUAGE}"
OFFSET = f"from0-{LANGUAGE}"


def take(client, consumer, text):
    reset, _first, messages = client.fetch(STREAM, consumer)
    if reset:
        raise SystemExit("offset reset")
    for message in messages:
        payload = message["payload"].decode()
        print(f"fetched {message['offset']} {payload} delivery {message['delivery']} headers {len(message['headers'])}")
        if payload == text:
            return message
    raise SystemExit(f"did not see {text}")


def main():
    client = connect(LANGUAGE)
    client.ensure_stream(STREAM, [f"{STREAM}.>"])
    client.ensure_consumer(STREAM, DURABLE, filter=f"{STREAM}.>", ack_wait_ms=30_000, max_deliver=5, max_ack_pending=1000)
    headers = [("content-type", b"text/plain")]
    client.publish(f"{STREAM}.created", b"ack me", headers=headers)
    client.publish(f"{STREAM}.created", b"nack me", headers=headers)
    _reset, _first, batch = client.fetch(STREAM, DURABLE)
    found = {message["payload"].decode(): message for message in batch}
    acked = found["ack me"]
    nacked = found["nack me"]
    client.ack(STREAM, DURABLE, acked["partition"], acked["offset"])
    print("ack")
    client.nack(STREAM, DURABLE, nacked["partition"], nacked["offset"])
    print("nack")
    again = take(client, DURABLE, "nack me")
    client.ack(STREAM, DURABLE, again["partition"], again["offset"])
    print(f"redelivered {again['delivery']}")

    client.reset(STREAM, DURABLE, absolute=True, offset=0)
    reset, first, messages = client.fetch(STREAM, DURABLE, max_messages=1)
    if reset or not messages:
        raise SystemExit("reset did not return a message")
    print(f"after reset first {first} offset {messages[0]['offset']}")
    client.ack(STREAM, DURABLE, messages[0]["partition"], messages[0]["offset"])

    client.release(STREAM, TAIL)
    client.ensure_consumer(STREAM, TAIL, ephemeral=True, start=START_TAIL)
    client.publish(f"{STREAM}.created", b"tail me")
    tailed = take(client, TAIL, "tail me")
    client.ack(STREAM, TAIL, tailed["partition"], tailed["offset"])
    client.release(STREAM, TAIL)

    client.ensure_consumer(STREAM, OFFSET, start=START_OFFSET, start_offset=0)
    print(f"offset consumer {OFFSET}")
    client.close()


if __name__ == "__main__":
    main()
