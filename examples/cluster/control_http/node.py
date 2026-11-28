#!/usr/bin/env python3
"""Guest workload demonstrating `http://rewind` coordination (/meta, /signal, /barrier)."""

import json
import urllib.request

BASE = "http://rewind"


def http_get(path: str) -> str:
    with urllib.request.urlopen(f"{BASE}{path}", timeout=30.0) as resp:
        return resp.read().decode("utf-8")


def http_post(path: str, payload: dict) -> str:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30.0) as resp:
        return resp.read().decode("utf-8")


def main() -> None:
    meta = json.loads(http_get("/meta"))
    name = meta["node"]
    ip = meta["ip"]
    print(f"META node={name} ip={ip} peers={len(meta['nodes'])}", flush=True)

    # 1. One-way signal with structured JSON payload.
    http_post("/signal/ready", {"node": name, "ip": ip})

    # 2. Synchronous barrier: blocks until host releases with a command payload!
    cmd = http_get("/barrier/dispatch").strip()
    print(f"DISPATCHED node={name} cmd={cmd}", flush=True)

    # 3. Signal completion with the command we executed.
    http_post("/signal/done", {"node": name, "cmd": cmd})


if __name__ == "__main__":
    main()
