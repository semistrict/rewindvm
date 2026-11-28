#!/usr/bin/env python3
"""Guest C-API (`libfdb_c.so`) client for FoundationDB commit 1915da14fc (PR #14328):
stale coordinator DNS entries wedge `getClusterProtocolImpl()` in the multi-version
client because `Hostname::resolveWithRetry()` is awaited before watching
`coordinator->onChange()`.

Injected into the `client` VM (`/build/stale_coord_dns_client.py`) by
`examples/cluster/foundationdb_sim.py`.
"""

import asyncio
import ctypes
import struct
import subprocess
import sys
import threading
from pathlib import Path

pkg_dir = Path("/tmp/rewind_pkg/rewind")
pkg_dir.mkdir(parents=True, exist_ok=True)
for src in sorted(Path("/build").glob("rewind__*.py")):
    dst = pkg_dir / src.name.removeprefix("rewind__")
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
sys.path.insert(0, "/tmp/rewind_pkg")

from rewind.guest import signal

LIVE_CLUSTER = "rewind:test1234@10.0.0.1:4500,10.0.0.2:4500,10.0.0.3:4500"
LIVE_FILE = "/tmp/fdb-live.cluster"
Path(LIVE_FILE).write_text(LIVE_CLUSTER + "\n", encoding="utf-8")
Path("/tmp/client-log").mkdir(parents=True, exist_ok=True)


def fdbcli(cmd: str, timeout: int = 8) -> tuple[int, str, str]:
    p = subprocess.run(
        [
            "/usr/local/bin/fdbcli",
            "-C",
            LIVE_FILE,
            "--timeout",
            str(timeout),
            "--exec",
            cmd,
        ],
        capture_output=True,
        text=True,
    )
    return p.returncode, p.stdout, p.stderr


lib = ctypes.CDLL("/usr/lib/libfdb_c.so")
lib.fdb_select_api_version_impl.argtypes = [ctypes.c_int, ctypes.c_int]
lib.fdb_select_api_version_impl.restype = ctypes.c_int
lib.fdb_network_set_option.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
lib.fdb_network_set_option.restype = ctypes.c_int
lib.fdb_setup_network.restype = ctypes.c_int
lib.fdb_run_network.restype = ctypes.c_int
lib.fdb_stop_network.restype = ctypes.c_int
lib.fdb_create_database.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
lib.fdb_create_database.restype = ctypes.c_int
lib.fdb_database_destroy.argtypes = [ctypes.c_void_p]
lib.fdb_database_create_transaction.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_void_p),
]
lib.fdb_database_create_transaction.restype = ctypes.c_int
lib.fdb_transaction_set_option.argtypes = [
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
]
lib.fdb_transaction_set_option.restype = ctypes.c_int
lib.fdb_transaction_set.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
]
lib.fdb_transaction_commit.argtypes = [ctypes.c_void_p]
lib.fdb_transaction_commit.restype = ctypes.c_void_p
lib.fdb_transaction_on_error.argtypes = [ctypes.c_void_p, ctypes.c_int]
lib.fdb_transaction_on_error.restype = ctypes.c_void_p
lib.fdb_transaction_destroy.argtypes = [ctypes.c_void_p]
lib.fdb_future_block_until_ready.argtypes = [ctypes.c_void_p]
lib.fdb_future_block_until_ready.restype = ctypes.c_int
lib.fdb_future_get_error.argtypes = [ctypes.c_void_p]
lib.fdb_future_get_error.restype = ctypes.c_int
lib.fdb_future_destroy.argtypes = [ctypes.c_void_p]


def check(err: int, what: str) -> None:
    if err != 0:
        raise RuntimeError(f"{what} failed with FDB error {err}")


def set_net_opt(code: int, val: bytes = b"") -> None:
    check(lib.fdb_network_set_option(code, val, len(val)), f"set_net_opt({code})")


def try_write_with_timeout(cluster_file: str, key: bytes, timeout_ms: int) -> int:
    """Open a DB using `cluster_file` and attempt one write with `timeout_ms`.
    Returns 0 on success or the FDB error code (1031 = transaction_timed_out)."""
    db = ctypes.c_void_p()
    check(lib.fdb_create_database(cluster_file.encode(), ctypes.byref(db)), "create_db")
    tr = ctypes.c_void_p()
    check(lib.fdb_database_create_transaction(db, ctypes.byref(tr)), "create_tx")
    try:
        tbuf = struct.pack("<q", timeout_ms)
        check(
            lib.fdb_transaction_set_option(tr, 500, tbuf, len(tbuf)),  # TIMEOUT
            "set_tx_timeout",
        )
        while True:
            lib.fdb_transaction_set(tr, key, len(key), b"ok", 2)
            f = lib.fdb_transaction_commit(tr)
            lib.fdb_future_block_until_ready(f)
            err = lib.fdb_future_get_error(f)
            lib.fdb_future_destroy(f)
            if err == 0:
                return 0
            ef = lib.fdb_transaction_on_error(tr, err)
            lib.fdb_future_block_until_ready(ef)
            e2 = lib.fdb_future_get_error(ef)
            lib.fdb_future_destroy(ef)
            if e2 != 0:
                return e2
    finally:
        lib.fdb_transaction_destroy(tr)
        lib.fdb_database_destroy(db)


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

    # Enable the multi-version client wrapper on the local library via
    # FDB_NET_OPTION_DISABLE_CLIENT_BYPASS (72) — the exact path that runs
    # `getClusterProtocolImpl()` before activating the database!
    check(lib.fdb_select_api_version_impl(800, 800), "select_api")
    set_net_opt(30, b"/tmp/client-log")  # TRACE_ENABLE
    set_net_opt(72, b"")  # DISABLE_CLIENT_BYPASS
    check(lib.fdb_setup_network(), "setup_network")
    t = threading.Thread(target=lib.fdb_run_network, daemon=True)
    t.start()

    # 1. Baseline: clean cluster file with only the 3 live coordinators commits cleanly.
    clean_err = await asyncio.to_thread(try_write_with_timeout, LIVE_FILE, b"clean_k", 8_000)

    # 2. Bug 1915da14fc: open 8 DBs concurrently with live coordinators + 9 stale `.invalid`
    #    coordinator hostnames. Whenever the initial coordinator pick hits a `.invalid` entry,
    #    `getClusterProtocolImpl` gets wedged in `resolveWithRetry()` and hits error 1031!
    stale_files = []
    for i in range(8):
        stale = ",".join(f"stale-coord-{i}-{j}.invalid:4500" for j in range(9))
        path = f"/tmp/fdb-stale-{i}.cluster"
        Path(path).write_text(f"{LIVE_CLUSTER},{stale}\n", encoding="utf-8")
        stale_files.append((i, path))

    errs = await asyncio.gather(
        *[
            asyncio.to_thread(try_write_with_timeout, path, f"stale_{i}".encode(), 4_000)
            for i, path in stale_files
        ]
    )

    stuck = [i for i, e in enumerate(errs) if e == 1031]
    succeeded = [i for i, e in enumerate(errs) if e == 0]
    await signal(
        "done",
        {
            "clean_err": clean_err,
            "total_stale_dbs": len(errs),
            "stuck_timeout_1031": len(stuck),
            "succeeded": len(succeeded),
            "errors": errs,
        },
    )
    lib.fdb_stop_network()


if __name__ == "__main__":
    asyncio.run(main())
