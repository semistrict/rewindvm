#!/usr/bin/env python3
"""Demonstrate Rewind's deterministic crash-consistent paravirtual disk (`rewind-blk` / `/dev/rwd0`).

Boots a single Linux node with a 16 MiB ext4 disk mounted at `/data`:
  1. Boot 1 writes `/data/durable.txt` + `sync` (issues `REQ_OP_FLUSH`, committing
     to durable blocks), then writes `/data/volatile.txt` WITHOUT `sync` (stays in
     the kernel page cache / disk volatile write cache), and signals `written`.
  2. Host triggers `c.crash("n1")` (sudden power loss: drops volatile disk cache
     while preserving committed blocks) and `c.restart("n1")`.
  3. Boot 2 replays the ext4 journal cleanly, reads back `/data/durable.txt`
     (survived!) and checks `/data/volatile.txt` (lost on power loss!).
  4. Replays the entire multi-boot trace (`c.replay(machine="none")`) and verifies
     bit-for-bit identical execution across both boots.
"""

import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "sdk" / "python"))

from rewind import Cluster

GUEST_SCRIPT = r"""#!/bin/sh
set -eu
if [ ! -f /data/durable.txt ]; then
  echo "BOOT1: writing durable + volatile"
  printf 'HELLO_DURABLE\n' > /data/durable.txt
  sync
  printf 'SHOULD_BE_LOST\n' > /data/volatile.txt
  wget -qO- "http://rewind/signal/written" >/dev/null 2>&1 || true
  sleep 30
else
  durable=$(cat /data/durable.txt 2>/dev/null || echo MISSING)
  volatile=$(cat /data/volatile.txt 2>/dev/null || echo LOST)
  echo "BOOT2: durable=$durable"
  echo "BOOT2: volatile=$volatile"
  wget -qO- "http://rewind/signal/recovered?durable=${durable}&volatile=${volatile}" >/dev/null 2>&1 || true
fi
"""


async def main() -> None:
    async with Cluster("disk-power-loss-demo", max_virtual_ms=30_000, keyframes=False) as c:
        n1 = c.add_node(
            "n1",
            ["/bin/sh", "/build/test_disk.sh"],
            ip="10.0.0.1",
            role="driver",
            files={"/build/test_disk.sh": GUEST_SCRIPT},
            disk_mib=16,
            disk_mount="/data",
        )
        await c.start()

        sig1 = await c.wait_signal("written", node="n1", timeout_ms=10_000)
        assert sig1 is not None, "timed out waiting for boot 1 write"
        before_crash = await n1.disk_stats()

        await c.crash("n1")
        after_crash = await n1.disk_stats()
        await c.restart("n1")

        sig2 = await c.wait_signal("recovered", node="n1", timeout_ms=10_000)
        assert sig2 is not None, "timed out waiting for boot 2 recovery"
        after_recovery = await n1.disk_stats()

        await n1.wait_exit(timeout_ms=5_000)
        run = await c.finish(verdict="passed")
        replay = await c.replay(machine="none")

        assert after_crash["power_losses"] == 1, after_crash
        assert before_crash["flushes"] > 0, before_crash
        assert replay["identical"] is True, replay

        print(
            json.dumps(
                {
                    "cluster_id": run["id"],
                    "wall_ms": run["outcome"]["wall_ms"],
                    "before_crash": before_crash,
                    "after_crash": after_crash,
                    "after_recovery": after_recovery,
                    "replay_identical": replay["identical"],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
