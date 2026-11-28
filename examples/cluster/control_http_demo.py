#!/usr/bin/env python3
"""Demonstrates `http://rewind` virtual coordinator signaling, barriers,
partition-immunity, and snapshot branching using the async Python SDK.

Guest workloads talk plain HTTP to `http://rewind` (10.0.0.254):
  - `GET  http://rewind/meta`             -> node identity & peer list
  - `POST http://rewind/signal/<name>`    -> one-way structured signal
  - `GET  http://rewind/barrier/<name>`   -> synchronous host-released barrier

No character devices, no `/dev/rewind-console`, works for non-root processes,
stays reachable even across `c.partition(...)`, and replays bit-for-bit via
`rewind cluster replay`.
"""

import asyncio
import os
import sys
from pathlib import Path

SDK_DIR = Path(__file__).resolve().parents[2] / "sdk" / "python"
sys.path.insert(0, str(SDK_DIR))

from rewind import Cluster

GUEST_DIR = Path(__file__).with_name("control_http")
NODE_SCRIPT = (GUEST_DIR / "node.py").read_text(encoding="utf-8")


def guest_files() -> dict[str, str]:
    files = {"/build/node.py": NODE_SCRIPT}
    for py in sorted((SDK_DIR / "rewind").glob("*.py")):
        files[f"/build/rewind__{py.name}"] = py.read_text(encoding="utf-8")
    return files


async def main() -> None:
    py_erofs = os.environ.get("REWIND_PYTHON_EROFS")
    node_kwargs = {"root": py_erofs} if py_erofs else {"packages": ["nixpkgs#python3"]}

    async with Cluster(
        "control-http-demo",
        latency_ms=1,
        jitter_us=100,
        mem_mib=192,
        stop_when="all",
        max_virtual_ms=30_000,
        slice_us=250,
    ) as c:
        files = guest_files()
        for i in range(1, 4):
            c.add_node(
                f"n{i}",
                ["python3", "-u", "/build/node.py"],
                ip=f"10.0.0.{i}",
                role="driver",
                files=files,
                **node_kwargs,
            )

        await c.start()

        # 1. Wait until all 3 nodes report `ready` via `POST http://rewind/signal/ready`
        #    (simple user-code loop on top of `wait_signal`).
        ready_nodes = {}
        while len(ready_nodes) < 3:
            sig = await c.wait_signal("ready", timeout_ms=20_000)
            assert sig is not None
            ready_nodes[sig.node] = sig.json()
        print(f"[host] all 3 nodes signaled ready: {sorted(ready_nodes)}", flush=True)

        # 2. Partition n1 from n2/n3 BEFORE the barrier — prove http://rewind stays reachable!
        await c.partition(["n1"], ["n2", "n3"])

        waiting = set()
        while len(waiting) < 3:
            sig = await c.wait_signal("dispatch", barrier=True, timeout_ms=20_000)
            assert sig is not None
            waiting.add(sig.node)
        print(
            f"[host] all 3 nodes blocked at /barrier/dispatch (even while n1 is partitioned!)",
            flush=True,
        )

        # 3. Snapshot while all 3 nodes are parked inside `GET http://rewind/barrier/dispatch`!
        snap = await c.snapshot("at-barrier")

        # Branch A: release with "alpha"
        rel_a = await c.release("dispatch", "alpha")
        assert rel_a["released"] == 3, rel_a
        done_a = {}
        while len(done_a) < 3:
            sig = await c.wait_signal("done", timeout_ms=20_000)
            assert sig is not None
            done_a[sig.node] = sig.json()["cmd"]
        assert all(v == "alpha" for v in done_a.values()), done_a
        print(f"[Branch A] all 3 nodes completed: {done_a}", flush=True)

        # 4. Restore back to `at-barrier` and release with "beta" instead!
        await c.restore(snap)
        rel_b = await c.release("dispatch", "beta")
        assert rel_b["released"] == 3, rel_b
        done_b = {}
        while len(done_b) < 3:
            sig = await c.wait_signal("done", timeout_ms=20_000)
            assert sig is not None
            done_b[sig.node] = sig.json()["cmd"]
        assert all(v == "beta" for v in done_b.values()), done_b
        print(f"[Branch B] all 3 nodes completed: {done_b}", flush=True)

        # Wait for all 3 nodes to exit cleanly.
        exit_res = await c.wait_exit(timeout_ms=10_000)
        assert exit_res["exited"], exit_res
        run = await c.finish(verdict="passed")
        print(
            f"recorded control-http cluster {run['id']} ({run['verdict']}) at "
            f"{exit_res['cluster_ns'] / 1e6:.1f}ms VT",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
