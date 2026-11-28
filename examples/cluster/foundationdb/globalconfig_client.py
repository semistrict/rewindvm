#!/usr/bin/env python3
"""Guest driver for FoundationDB commit f8e1e90a40 (PR #14196):
writing an empty value to `\\xff/globalConfig/v` triggers a heap OOB read and
`Assertion !globalConfigHistory.empty() failed` crash across GRV proxies in
unmodified FoundationDB 8.0.1.

Injected into the `client` VM (`/build/globalconfig_client.py`) by
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

from rewind.guest import signal

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

    rc, out, err = fdbcli("writemode on; set k1 v1; get k1", timeout=8)
    await signal("before-bug", {"rc": rc, "out": out.strip()})

    # Trigger bug f8e1e90a40: write empty value to \xff/globalConfig/v.
    rc, out, err = fdbcli(
        r'writemode on; option on ACCESS_SYSTEM_KEYS; set \xff/globalConfig/v ""',
        timeout=8,
    )
    await signal(
        "wrote-corrupt-globalconfig",
        {"rc": rc, "out": out.strip(), "err": err.strip()},
    )

    # Wait for GRV proxy's globalConfigRefresh() timer to read the corrupted key.
    await asyncio.sleep(3.0)
    rc2, out2, err2 = fdbcli("status json", timeout=8)
    await signal("after-bug-status", {"rc": rc2, "len": len(out2), "err": err2.strip()})


if __name__ == "__main__":
    asyncio.run(main())
