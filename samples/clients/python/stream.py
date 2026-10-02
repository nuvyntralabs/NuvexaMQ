#!/usr/bin/env python3
"""Stream layout, retention limits, partitioned publish, and headers."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from nuvexa import connect

LANGUAGE = "python"
STREAM = f"catalog-{LANGUAGE}"


def main():
    client = connect(LANGUAGE)
    client.ensure_stream(STREAM, [f"{STREAM}.>"], partitions=2, max_age_ms=86_400_000, max_bytes=1_048_576, max_message_bytes=65_536)
    body = b'{"id":1}'
    headers = [("content-type", b"application/json")]
    keyed = client.publish(f"{STREAM}.created", body, key="alpha", headers=headers)
    first = client.publish(f"{STREAM}.created", body)
    second = client.publish(f"{STREAM}.created", body)
    if not keyed or not first or not second:
        raise SystemExit("publish was not stored")
    if first[0][1] == second[0][1]:
        raise SystemExit("round-robin did not use both partitions")
    client.close()


if __name__ == "__main__":
    main()
