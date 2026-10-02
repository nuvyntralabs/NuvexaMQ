#!/usr/bin/env python3
"""Health, metrics, management API, and the HTTPS listener."""

import base64
import json
import os
import ssl
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from nuvexa import HEALTH_PORT, HOST, MANAGEMENT_HTTPS_PORT, MANAGEMENT_PORT, PASSWORD, USER

LANGUAGE = "python"


def request(url, method="GET", body=None, tls=False):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    if "/api/" in url:
        req.add_header("Authorization", f"Basic {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    context = ssl._create_unverified_context() if tls else None
    with urllib.request.urlopen(req, context=context) as response:
        raw = response.read()
        print(f"{method} {url} {response.status} {len(raw)} bytes")
        return raw


def main():
    health = f"http://{HOST}:{HEALTH_PORT}"
    request(f"{health}/health")
    request(f"{health}/metrics")
    base = f"http://{HOST}:{MANAGEMENT_PORT}"
    for path in (
        "/api/whoami",
        "/api/overview",
        "/api/connections",
        "/api/channels",
        "/api/streams",
        "/api/exchanges",
        "/api/queues",
        "/api/bindings",
        "/api/users",
        "/api/permissions",
        "/api/vhosts",
        "/api/policies",
    ):
        request(base + path)
    vhost = f"vh-{LANGUAGE}"
    user = f"user-{LANGUAGE}"
    policy = f"policy-{LANGUAGE}"
    request(f"{base}/api/vhosts/{vhost}", "PUT")
    request(f"{base}/api/users/{user}", "PUT", {"password": "sample-pass", "tags": ["management"]})
    request(f"{base}/api/permissions", "PUT", {"user": user, "vhost": vhost, "configure": ".*", "write": ".*", "read": ".*"})
    request(f"{base}/api/policies/{policy}", "PUT", {
        "vhost": "/",
        "pattern": "sample-.*",
        "priority": 1,
        "messageTtlMs": 60000,
        "maxLength": 100,
        "deadLetterExchange": "",
        "deadLetterRoutingKey": "",
    })
    request(f"{base}/api/policies/{policy}?vhost=/", "DELETE")
    request(f"{base}/api/permissions?user={user}&vhost={vhost}", "DELETE")
    request(f"{base}/api/users/{user}", "DELETE")
    request(f"{base}/api/vhosts/{vhost}", "DELETE")
    request(f"https://{HOST}:{MANAGEMENT_HTTPS_PORT}/api/whoami", tls=True)


if __name__ == "__main__":
    main()
