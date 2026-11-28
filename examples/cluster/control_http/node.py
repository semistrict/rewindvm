#!/usr/bin/env python3
"""Guest workload using the Rewind SDK (`from rewind.guest import meta, signal, barrier`)."""

import asyncio
import sys
from pathlib import Path

BUILD_DIR = Path(__file__).resolve().parent
PKG_DIR = BUILD_DIR / "rewind"
if not PKG_DIR.exists():
    PKG_DIR.mkdir(exist_ok=True)
    for src in BUILD_DIR.glob("rewind__*.py"):
        (PKG_DIR / src.name.removeprefix("rewind__")).write_bytes(src.read_bytes())
sys.path.insert(0, str(BUILD_DIR))

from rewind.guest import barrier_json, meta, signal


async def main() -> None:
    info = await meta()
    name = info["node"]
    ip = info["ip"]
    print(f"META node={name} ip={ip} peers={len(info['nodes'])}", flush=True)

    # 1. One-way signal with structured JSON payload.
    await signal("ready", {"node": name, "ip": ip})

    # 2. Synchronous barrier: blocks until host calls `await c.release("dispatch", {...})`.
    task = await barrier_json("dispatch", {"node": name})
    print(f"DISPATCHED node={name} task={task}", flush=True)

    # 3. Signal completion with the command we executed.
    await signal("done", {"node": name, "cmd": task["cmd"], "ops": task["ops"]})


if __name__ == "__main__":
    asyncio.run(main())
