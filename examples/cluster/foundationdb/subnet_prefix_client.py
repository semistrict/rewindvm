#!/usr/bin/env python3
"""Guest driver for FoundationDB commit be737227dc:
`AuthAllowedSubnet::fromString()` in FDB 8.0.1 parses `--trusted-subnet-*` CIDR
prefixes with `std::stoi` and never validates `0 <= prefix <= 32` (IPv4) or
rejects trailing junk.

As a result, unmodified FDB 8.0.1 accepts invalid subnets like:
  - `10.0.0.1/-1`    (silently turns mask into 0.0.0.0/0 — matches every IP!)
  - `10.0.0.1/8junk` (trailing garbage silently ignored by std::stoi!)
  - `10.0.0.1/33`    (writes OOB past the 4-byte stack std::array in createBitMask!)

Injected into the `client` VM (`/build/subnet_prefix_client.py`) by
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


def probe_fdbserver_subnet(subnet: str) -> dict:
    """Spawn fdbserver briefly with `--trusted-subnet-1=<subnet>`.
    If FDB rejects the subnet at argument parse time it exits immediately with
    non-zero status; if it accepts the bogus subnet it stays alive until our
    1.5s timeout fires."""
    Path("/tmp/probe-data").mkdir(parents=True, exist_ok=True)
    Path("/tmp/probe-log").mkdir(parents=True, exist_ok=True)
    try:
        p = subprocess.run(
            [
                "/usr/local/bin/fdbserver",
                "--cluster-file",
                CLUSTER_FILE,
                "--listen-address",
                "127.0.0.1:4699",
                "--public-address",
                "127.0.0.1:4699",
                "--datadir",
                "/tmp/probe-data",
                "--logdir",
                "/tmp/probe-log",
                "--memory",
                "128MiB",
                "--cache-memory",
                "32MiB",
                "--knob_disable_posix_kernel_aio=1",
                f"--trusted-subnet-1={subnet}",
            ],
            capture_output=True,
            text=True,
            timeout=1.5,
        )
        return {
            "subnet": subnet,
            "accepted": False,
            "rc": p.returncode,
            "output": (p.stdout + p.stderr).strip()[:200],
        }
    except subprocess.TimeoutExpired:
        return {
            "subnet": subnet,
            "accepted": True,
            "rc": None,
            "output": "started and stayed running (invalid subnet accepted!)",
        }


async def main() -> None:
    # 1. Wait for the 3-node cluster (whose fdb1 was started with `--trusted-subnet-1=10.0.0.1/-1`!)
    #    to come up and accept transactions — proving fdb1 accepted the bogus `-1` prefix!
    for attempt in range(20):
        rc, out, err = fdbcli("configure new double memory", timeout=5)
        if rc == 0 and ("Database created" in out or "Configuration" in out):
            await signal("configured", {"attempt": attempt, "out": out.strip()})
            break
        await asyncio.sleep(0.5)
    else:
        await signal("configure-failed", {"rc": rc, "out": out, "err": err})
        sys.exit(1)

    rc, out, _ = fdbcli("writemode on; set sub_k sub_v; get sub_k", timeout=8)
    cluster_tx_ok = rc == 0 and "`sub_k' is `sub_v'" in out

    # 2. Directly probe additional invalid CIDR prefixes from commit be737227dc's test list.
    probes = [
        probe_fdbserver_subnet("10.0.0.1/-1"),
        probe_fdbserver_subnet("10.0.0.1/33"),
        probe_fdbserver_subnet("10.0.0.1/8junk"),
    ]
    await signal(
        "done",
        {
            "cluster_with_neg1_subnet_ok": cluster_tx_ok,
            "probes": probes,
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
