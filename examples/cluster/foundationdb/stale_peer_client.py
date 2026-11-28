#!/usr/bin/env python3
"""Guest C-API (`libfdb_c.so`) client for the FDB stale-peer leak reproduction
(FoundationDB commit eb5639a90e / PR #13912 / #14050).

Injected into the `client` VM (`/build/stale_peer_client.py`) by
`examples/cluster/foundationdb_sim.py`.
"""

import asyncio
import ctypes
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

from rewind.guest import barrier, signal

CLUSTER_FILE = "/tmp/fdb.cluster"
Path(CLUSTER_FILE).write_text(
    "rewind:test1234@10.0.0.1:4500,10.0.0.2:4500,10.0.0.3:4500\n",
    encoding="utf-8",
)
Path("/tmp/client-log").mkdir(parents=True, exist_ok=True)


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


# Minimal ctypes wrapper around unmodified libfdb_c.so (API v800).
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
lib.fdb_database_create_transaction.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_void_p),
]
lib.fdb_database_create_transaction.restype = ctypes.c_int
lib.fdb_transaction_set.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
]
lib.fdb_transaction_get.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_int,
]
lib.fdb_transaction_get.restype = ctypes.c_void_p
lib.fdb_transaction_commit.argtypes = [ctypes.c_void_p]
lib.fdb_transaction_commit.restype = ctypes.c_void_p
lib.fdb_transaction_on_error.argtypes = [ctypes.c_void_p, ctypes.c_int]
lib.fdb_transaction_on_error.restype = ctypes.c_void_p
lib.fdb_transaction_destroy.argtypes = [ctypes.c_void_p]
lib.fdb_future_block_until_ready.argtypes = [ctypes.c_void_p]
lib.fdb_future_block_until_ready.restype = ctypes.c_int
lib.fdb_future_get_error.argtypes = [ctypes.c_void_p]
lib.fdb_future_get_error.restype = ctypes.c_int
lib.fdb_future_get_value.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_int),
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.POINTER(ctypes.c_int),
]
lib.fdb_future_get_value.restype = ctypes.c_int
lib.fdb_future_destroy.argtypes = [ctypes.c_void_p]


def check(err: int, what: str) -> None:
    if err != 0:
        raise RuntimeError(f"{what} failed with FDB error {err}")


def set_opt(code: int, val: bytes) -> None:
    check(lib.fdb_network_set_option(code, val, len(val)), f"set_opt({code}, {val!r})")


def do_tx(db: ctypes.c_void_p, op: str, key: bytes, val: bytes = b"") -> bytes | None:
    tr = ctypes.c_void_p()
    check(lib.fdb_database_create_transaction(db, ctypes.byref(tr)), "create_tx")
    try:
        while True:
            if op == "set":
                lib.fdb_transaction_set(tr, key, len(key), val, len(val))
                f = lib.fdb_transaction_commit(tr)
                lib.fdb_future_block_until_ready(f)
                err = lib.fdb_future_get_error(f)
                lib.fdb_future_destroy(f)
                if err == 0:
                    return None
            else:
                f = lib.fdb_transaction_get(tr, key, len(key), 0)
                lib.fdb_future_block_until_ready(f)
                err = lib.fdb_future_get_error(f)
                if err == 0:
                    present = ctypes.c_int()
                    out_ptr = ctypes.c_char_p()
                    out_len = ctypes.c_int()
                    lib.fdb_future_get_value(
                        f,
                        ctypes.byref(present),
                        ctypes.byref(out_ptr),
                        ctypes.byref(out_len),
                    )
                    res = (
                        ctypes.string_at(out_ptr, out_len.value)
                        if present.value
                        else None
                    )
                    lib.fdb_future_destroy(f)
                    return res
                lib.fdb_future_destroy(f)
            ef = lib.fdb_transaction_on_error(tr, err)
            lib.fdb_future_block_until_ready(ef)
            e2 = lib.fdb_future_get_error(ef)
            lib.fdb_future_destroy(ef)
            if e2 != 0:
                raise RuntimeError(f"tx {op} failed fatally: {e2}")
    finally:
        lib.fdb_transaction_destroy(tr)


async def main() -> None:
    mode = sys.argv[1]
    for attempt in range(20):
        rc, out, err = fdbcli("configure new double memory", timeout=5)
        if rc == 0 and ("Database created" in out or "Configuration" in out):
            await signal("configured", {"attempt": attempt, "out": out.strip()})
            break
        await asyncio.sleep(0.5)
    else:
        await signal("configure-failed", {"rc": rc, "out": out, "err": err})
        sys.exit(1)

    check(lib.fdb_select_api_version_impl(800, 800), "select_api")
    set_opt(30, b"/tmp/client-log")  # FDB_NET_OPTION_TRACE_ENABLE
    if mode == "buggy":
        set_opt(40, b"location_cache_peer_evictor_enabled=0")
        set_opt(40, b"shrink_proxy_list_clear_cache_below_threshold=0")
        set_opt(40, b"dbcontext_eager_proxy_update=0")
    else:
        set_opt(40, b"location_cache_peer_evictor_enabled=1")
        set_opt(40, b"location_cache_peer_evictor_failed_threshold=0")
        set_opt(40, b"location_cache_peer_evictor_delay=5.0")
        set_opt(40, b"shrink_proxy_list_clear_cache_below_threshold=1")
        set_opt(40, b"dbcontext_eager_proxy_update=1")
    set_opt(40, b"stale_peer_observability=1")

    check(lib.fdb_setup_network(), "setup_network")
    t = threading.Thread(target=lib.fdb_run_network, daemon=True)
    t.start()

    db = ctypes.c_void_p()
    check(lib.fdb_create_database(CLUSTER_FILE.encode(), ctypes.byref(db)), "create_db")

    # Populate and read keys so DatabaseContext::locationCache caches active Storage Servers.
    for i in range(40):
        k = f"key_{i:03d}".encode()
        await asyncio.to_thread(do_tx, db, "set", k, f"val_{i:03d}".encode())
    for _ in range(3):
        for i in range(40):
            k = f"key_{i:03d}".encode()
            v = await asyncio.to_thread(do_tx, db, "get", k)
            assert v == f"val_{i:03d}".encode(), (k, v)

    await signal("warmed-up")

    # Wait for host to permanently crash the active Storage Server.
    victim_ip = await barrier("ss-killed")

    # Stay alive for 60s virtual time so we can observe whether the idle client
    # keeps spamming reconnects to the dead peer or evicts it cleanly.
    await asyncio.sleep(60.0)

    evictions = 0
    connect_failed_victim = 0
    for xml in sorted(Path("/tmp/client-log").glob("*.xml")):
        txt = xml.read_text(encoding="utf-8", errors="replace")
        evictions += txt.count("LocationCachePeerEvict")
        for line in txt.splitlines():
            if f"{victim_ip}:4500" in line and (
                "ConnectFailed" in line or "ConnectionTimedOut" in line
            ):
                connect_failed_victim += 1

    await signal(
        "done",
        {
            "mode": mode,
            "evictions": evictions,
            "connect_failed_victim": connect_failed_victim,
        },
    )
    lib.fdb_stop_network()


if __name__ == "__main__":
    asyncio.run(main())
