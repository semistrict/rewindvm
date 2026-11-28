#!/usr/bin/env python3
"""Multi-node cluster running real Nix packages (`nixpkgs#python3`, `nixpkgs#curl`).

Each node's Nix closure is resolved once, packed into a cached EROFS store
image under `$REWIND_HOME/images/nix-<hash>.erofs`, mounted at `/nix/store`,
and each package's `bin/` directory is automatically prepended to `PATH`.

Requires `nix` on PATH. Run with:
    python3 examples/cluster/nix_http.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind import Cluster


async def main() -> None:
    async with Cluster(
        "nix-http-demo",
        latency_ms=2,
        jitter_us=200,
        mem_mib=256,
        stop_when="driver",
        max_virtual_ms=20_000,
    ) as c:
        srv = c.add_node(
            "api",
            "echo 'hello-from-nix' > /build/greeting.txt && "
            "python3 -m http.server 8000 --directory /build & "
            "until grep -q ':1F40 ' /proc/net/tcp /proc/net/tcp6 2>/dev/null; do sleep 0.01; done && "
            "python3 -c 'import urllib.request; urllib.request.urlopen(\"http://rewind/signal/ready\")' && "
            "wait",
            ip="10.0.0.1",
            role="service",
            packages=["nixpkgs#python3"],
        )
        cli = c.add_node(
            "client",
            """
set -e
curl -sf http://rewind/barrier/start >/dev/null
r1=$(curl -sf --max-time 1 http://api:8000/greeting.txt || echo blocked)
curl -sf -X POST -d "$r1" http://rewind/signal/r1 >/dev/null
curl -sf http://rewind/barrier/r2 >/dev/null
r2=$(curl -sf --max-time 1 http://api:8000/greeting.txt || echo blocked)
curl -sf -X POST -d "$r2" http://rewind/signal/r2 >/dev/null
curl -sf http://rewind/barrier/r3 >/dev/null
r3=$(curl -sf --max-time 1 http://api:8000/greeting.txt || echo blocked)
curl -sf -X POST -d "$r3" http://rewind/signal/r3 >/dev/null
curl -sf -X POST -d "ok" http://rewind/signal/done >/dev/null
""",
            ip="10.0.0.2",
            role="driver",
            packages=["nixpkgs#curl"],
        )

        await c.start()
        await srv.wait_signal("ready")
        await cli.wait_signal("start", barrier=True)
        await cli.release("start")

        r1 = await cli.wait_signal("r1")
        assert r1 is not None and r1.payload == "hello-from-nix", r1
        assert (await srv.cat("/build/greeting.txt")).strip() == "hello-from-nix"

        await cli.wait_signal("r2", barrier=True)
        await c.partition(["api"], ["client"])
        await cli.release("r2")
        r2 = await cli.wait_signal("r2")
        assert r2 is not None and r2.payload == "blocked", r2

        await cli.wait_signal("r3", barrier=True)
        await c.heal()
        await cli.release("r3")
        r3 = await cli.wait_signal("r3")
        assert r3 is not None and r3.payload == "hello-from-nix", r3

        await cli.wait_signal("done")
        res = await cli.wait_exit()
        assert res["exited"] and res["exit_code"] == 0, res
        fin = await c.finish()
        print(f"recorded cluster {fin['id']} ({fin['verdict']})")


if __name__ == "__main__":
    asyncio.run(main())
