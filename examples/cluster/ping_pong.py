#!/usr/bin/env python3
"""Reactive 3-node distributed TCP cluster driven by the async Rewind Python SDK.

Demonstrates:
- Deterministic guest <-> host synchronization via `http://rewind` (`signal` / `barrier`)
- Live zero-perturbation filesystem inspection (`cat` / `files`)
- Timeline snapshotting and branching (`snapshot` / `restore`)
- Dynamic partitions, heals, link latency updates, and hard node crash/reboot
- Standalone deterministic replay (`rewind cluster replay <id>`) without Python!
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind import Cluster

SH_HELPERS = """\
sig() {
  printf 'POST /signal/%s HTTP/1.1\\r\\nHost: rewind\\r\\nContent-Length: %d\\r\\nConnection: close\\r\\n\\r\\n%s' \
    "$1" "${#2}" "$2" | /rewind/tools/bin/nc rewind 80 >/dev/null
}
bar() {
  printf 'GET /barrier/%s HTTP/1.1\\r\\nHost: rewind\\r\\nConnection: close\\r\\n\\r\\n' \
    "$1" | /rewind/tools/bin/nc rewind 80 >/dev/null
}
"""


async def main() -> None:
    async with Cluster(
        "ping-pong-sdk",
        latency_ms=2,
        jitter_us=200,
        mem_mib=128,
        stop_when="driver",
        max_virtual_ms=20_000,
        echo=os.environ.get("REWIND_ECHO") == "1",
    ) as c:
        n1 = c.add_node(
            "node1",
            SH_HELPERS
            + "echo 'hello-from-node1' > /tmp/reply; "
            "(while true; do /rewind/tools/bin/nc -l -p 7000 -e /bin/sh -c 'cat /tmp/reply'; done) & "
            "until grep -q ':1B58 ' /proc/net/tcp /proc/net/tcp6 2>/dev/null; do sleep 0.01; done; "
            "sig ready node1; wait",
            ip="10.0.0.1",
            role="service",
        )
        n2 = c.add_node(
            "node2",
            SH_HELPERS
            + "echo 'hello-from-node2' > /tmp/reply; "
            "(while true; do /rewind/tools/bin/nc -l -p 7000 -e /bin/sh -c 'cat /tmp/reply'; done) & "
            "until grep -q ':1B58 ' /proc/net/tcp /proc/net/tcp6 2>/dev/null; do sleep 0.01; done; "
            "sig ready node2; wait",
            ip="10.0.0.2",
            role="service",
            clock_skew_ms=25,
            clock_drift_ppm=500,
        )
        cli = c.add_node(
            "client",
            SH_HELPERS
            + """
set -e
probe() {
  /rewind/tools/bin/nc -w 1 "$1" 7000 2>/dev/null || echo "blocked"
}
bar start
sig phase1 "n1=$(probe node1) n2=$(probe node2)"
bar phase2
sig phase2 "n1=$(probe node1) n2=$(probe node2)"
bar phase3
sig phase3 "n1=$(probe node1) n2=$(probe node2)"
sig done ok
""",
            ip="10.0.0.10",
            role="driver",
        )

        await c.start()

        # 1. Wait for both servers and the client's start barrier, then run phase 1.
        await n1.wait_signal("ready")
        await n2.wait_signal("ready")
        await cli.wait_signal("start", barrier=True)
        await cli.release("start")

        p1 = await cli.wait_signal("phase1")
        assert p1 is not None and p1.payload == "n1=hello-from-node1 n2=hello-from-node2", p1

        # 2. Zero-perturbation live guest file inspection.
        assert (await n1.cat("/tmp/reply")).strip() == "hello-from-node1"
        assert any(e["path"] == "reply" for e in await n1.files("/tmp"))

        # 3. Park client at `phase2` barrier and snapshot before injecting faults!
        await cli.wait_signal("phase2", barrier=True)
        snap = await c.snapshot("after-phase1")

        # Branch A: isolate node2, verify node2 is blocked.
        await n2.isolate()
        await cli.release("phase2")
        p2_a = await cli.wait_signal("phase2")
        assert p2_a is not None and p2_a.payload == "n1=hello-from-node1 n2=blocked", p2_a

        # Rewind back to `after-phase1` and explore Branch B!
        await c.restore(snap)

        # Branch B: isolate node1 + add extra latency on client <-> node2.
        await c.partition(["node1"], ["node2", "client"])
        await c.set_link("client", "node2", latency_ms=10)
        await cli.release("phase2")
        p2_b = await cli.wait_signal("phase2")
        assert p2_b is not None and p2_b.payload == "n1=blocked n2=hello-from-node2", p2_b

        # Heal partition and crash + cold-reboot node2 before phase 3.
        await cli.wait_signal("phase3", barrier=True)
        await c.heal()
        await n2.crash()
        await c.advance(ms=150)
        await n2.restart()
        await n2.wait_signal("ready")
        await cli.release("phase3")

        p3 = await cli.wait_signal("phase3")
        assert p3 is not None and p3.payload == "n1=hello-from-node1 n2=hello-from-node2", p3
        await cli.wait_signal("done")
        exit_res = await cli.wait_exit()
        assert exit_res["exited"] and exit_res["exit_code"] == 0, exit_res

        res = await c.finish()
        print(
            f"recorded cluster {res['id']} ({res['verdict']}) "
            f"at {res['outcome']['cluster_ns'] / 1e6:.2f}ms"
        )


if __name__ == "__main__":
    asyncio.run(main())
