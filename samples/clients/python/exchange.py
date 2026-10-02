#!/usr/bin/env python3
"""Exchanges, queues, bindings, routing, purge, and delete."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from nuvexa import connect

LANGUAGE = "python"
QUEUE = f"work-{LANGUAGE}"
DEAD = f"dead-{LANGUAGE}"
DIRECT = f"direct-{LANGUAGE}"
FANOUT = f"fanout-{LANGUAGE}"
TOPIC = f"topic-{LANGUAGE}"
HEADERS = f"headers-{LANGUAGE}"


def main():
    client = connect(LANGUAGE)
    client.declare_exchange("amq.direct", "direct")
    client.declare_exchange(DIRECT, "direct", durable=True, auto_delete=False)
    client.declare_exchange(FANOUT, "fanout")
    client.declare_exchange(TOPIC, "topic")
    client.declare_exchange(HEADERS, "headers")
    client.declare_queue(DEAD, durable=True)
    client.declare_queue(
        QUEUE,
        durable=True,
        exclusive=False,
        auto_delete=False,
        message_ttl_ms=60_000,
        max_length=100,
        dead_letter_exchange=DIRECT,
        dead_letter_routing_key="expired",
    )
    client.bind_queue(DIRECT, DEAD, "expired")
    client.bind_queue(DIRECT, QUEUE, "work.created")
    client.bind_queue(FANOUT, QUEUE, "")
    client.bind_queue(TOPIC, QUEUE, "work.*")
    client.bind_queue(HEADERS, QUEUE, "", [("format", "json"), ("x-match", "all")])
    body = b"routed"
    routed = []
    routed += client.publish_exchange(DIRECT, "work.created", body, key="order-1")
    routed += client.publish_exchange(FANOUT, "", body)
    routed += client.publish_exchange(TOPIC, "work.created", body)
    routed += client.publish_exchange(HEADERS, "", body, headers=[("format", b"json")])
    routed += client.publish_exchange("", QUEUE, body)
    if len(routed) < 5:
        raise SystemExit(f"expected at least 5 receipts, got {len(routed)}")
    client.purge_queue(QUEUE)
    client.delete_queue(QUEUE)
    client.delete_queue(DEAD)
    for name in (DIRECT, FANOUT, TOPIC, HEADERS):
        client.delete_exchange(name)
    client.close()


if __name__ == "__main__":
    main()
