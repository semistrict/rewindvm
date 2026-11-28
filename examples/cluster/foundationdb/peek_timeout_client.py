#!/usr/bin/env python3
"""Guest driver for FoundationDB commit 446079cc6a (PR #13248):
compares `--knob_peek_reply_timeout=0` (pre-fix wait-forever behavior) against
`--knob_peek_reply_timeout=2` (post-fix sender-side peek timeout) while the host
clogs all TLog -> Storage Server links.

Injected into the `client` VM (`/build/peek_timeout_client.py`) by
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

    # Seed initial keys so both Storage Servers (fdb4, fdb5) are actively peeking TLogs.
    rc, out, err = fdbcli("writemode on; set seed_k seed_v; get seed_k", timeout=10)
    assert rc == 0 and "`seed_k' is `seed_v'" in out, (rc, out, err)
    await signal("warmed-up")

    # Wait for host to clog TLog <-> Storage Server links.
    await barrier("clogged")

    # Keep writing new mutations while TLog <-> fdb4 is clogged for ~8s VT.
    # fdb4's ServerPeekCursor will have an in-flight TLogPeekRequest waiting on
    # the clogged TLog; with peek_reply_timeout=2 it fires PeekReplyTimeout every
    # ~2s, whereas with peek_reply_timeout=0 it sits stuck with 0 retries.
    for i in range(6):
        fdbcli(f"writemode on; set clog_{i} val_{i}", timeout=3)
        await asyncio.sleep(1.2)

    await signal("clog-phase-done")
    await barrier("unclogged")

    rc2, out2, _ = fdbcli("writemode on; set post_k post_v; get post_k", timeout=10)
    await signal("done", {"post_ok": rc2 == 0 and "`post_k' is `post_v'" in out2})


if __name__ == "__main__":
    asyncio.run(main())
