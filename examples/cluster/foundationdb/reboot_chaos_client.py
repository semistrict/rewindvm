#!/usr/bin/env python3
"""Guest driver for the FoundationDB multi-fault chaos & same-IP reboot scenario.

Injected into the `client` VM (`/build/reboot_chaos_client.py`) by
`examples/cluster/foundationdb_sim.py`.
"""

import asyncio
import subprocess
import sys
from pathlib import Path

pkg_dir = Path("/tmp/rewind_pkg/rewind")
pkg_dir.mkdir(parents=True, exist_ok=True)
for src in sorted(Path("/build").glob("rewind__*.py")):
    dst = pkg_dir / src.name.removeprefix("rewind__")
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
sys.path.insert(0, "/tmp/rewind_pkg")

from rewind.guest import barrier, signal

CLUSTER_FILE = "/tmp/fdb.cluster"
Path(CLUSTER_FILE).write_text(
    "rewind:test1234@10.0.0.1:4500,10.0.0.2:4500,10.0.0.3:4500\n",
    encoding="utf-8",
)


def fdbcli(cmd: str, timeout: int = 8) -> tuple[int, str, str]:
    p = subprocess.run(
        [
            "/usr/local/bin/fdbcli",
            "-C",
            CLUSTER_FILE,
            "--timeout",
            str(timeout),
            "--exec",
            cmd,
        ],
        capture_output=True,
        text=True,
    )
    return p.returncode, p.stdout, p.stderr


async def main() -> None:
    for attempt in range(20):
        rc, out, err = fdbcli("configure new double memory", timeout=5)
        if rc == 0 and ("Database created" in out or "Configuration" in out):
            await signal("configured", {"attempt": attempt, "out": out.strip()})
            break
        await asyncio.sleep(0.5)
    else:
        await signal("configure-failed", {"rc": rc, "out": out, "err": err})
        sys.exit(1)

    rc, out, err = fdbcli("writemode on; set k0 v0; set k1 v1; get k1", timeout=10)
    assert rc == 0 and "`k1' is `v1'" in out, (rc, out, err)
    await signal("initial-kv", {"rc": rc, "out": out.strip()})

    # Wait for host to inject link clog, vCPU pause/resume, clock step, and
    # same-IP TLog crash + restart (multi-boot recovery).
    await barrier("after-fault")

    results = []
    for i in range(5):
        rc, out, err = fdbcli(
            f"writemode on; set post_{i} ok_{i}; get post_{i}", timeout=8
        )
        ok = rc == 0 and f"`post_{i}' is `ok_{i}'" in out
        results.append({"i": i, "rc": rc, "ok": ok})
        if not ok:
            await asyncio.sleep(1.0)

    # Also verify pre-fault key survived recovery intact.
    rc_k0, out_k0, _ = fdbcli("get k0", timeout=8)
    await signal(
        "post-fault-kv",
        {
            "results": results,
            "k0_ok": rc_k0 == 0 and "`k0' is `v0'" in out_k0,
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
