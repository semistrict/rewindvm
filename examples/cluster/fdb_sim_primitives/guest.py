#!/usr/bin/env python3
"""Guest workload for `examples/cluster/fdb_sim_primitives.py`.

Exercises fault-domain metadata (`meta()`), two-stage `BUGGIFY` (`buggify()`),
`CODE_PROBE` (`probe()`), clogged UDP burst delivery, vCPU pause/resume, and
dynamic monotonic clock stepping via `http://rewind`.
"""

import asyncio
import socket
import sys
import time
from pathlib import Path

pkg_dir = Path("/tmp/rewind_pkg/rewind")
pkg_dir.mkdir(parents=True, exist_ok=True)
for src in sorted(Path("/build").glob("rewind__*.py")):
    dst = pkg_dir / src.name.removeprefix("rewind__")
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
sys.path.insert(0, "/tmp/rewind_pkg")

from rewind.guest import barrier, buggify, meta, probe, signal


async def run_n1() -> None:
    m = await meta()
    await probe("boot-ok")
    sites = {}
    for i in range(24):
        sites[f"site-{i}"] = await buggify(f"site-{i}", p=1.0)
    await signal("n1-meta", {"meta": m, "sites": sites})

    # Wait for host to disable buggify and send back one activated site to re-check.
    check_site = await barrier("check-buggify-off")
    after_off = await buggify(check_site, p=1.0)
    await signal("buggify-after-off", {"site": check_site, "fire": after_off})

    # Send two UDP datagrams while link n1 -> n2 is clogged.
    await barrier("send-clogged")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(b"msg-1", ("10.0.0.2", 9000))
    sock.sendto(b"msg-2", ("10.0.0.2", 9000))
    await probe("sent-clogged")
    await signal("sent-clogged-ok")

    # Send one UDP datagram while n2's vCPU is paused.
    await barrier("send-while-paused")
    sock.sendto(b"while-paused", ("10.0.0.2", 9000))
    await signal("sent-paused-ok")

    # Observe dynamic forward clock step + drift across barrier.
    t0 = time.monotonic()
    await barrier("clock-step")
    t1 = time.monotonic()
    await signal("clock-result", {"dt_ms": (t1 - t0) * 1000.0})
    await barrier("done")


async def run_n2() -> None:
    m = await meta()
    await probe("boot-ok")
    await signal("n2-meta", {"meta": m})

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 9000))
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    await signal("udp-listening")

    for _ in range(3):
        data = await loop.sock_recv(sock, 1024)
        msg = data.decode("utf-8", errors="replace")
        await probe("udp-recv")
        await signal("udp-recv", {"msg": msg, "mono_ms": time.monotonic() * 1000.0})

    await barrier("done")


async def main() -> None:
    m = await meta()
    if m["node"] == "n1":
        await run_n1()
    else:
        await run_n2()


if __name__ == "__main__":
    asyncio.run(main())
