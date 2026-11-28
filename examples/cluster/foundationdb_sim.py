#!/usr/bin/env python3
"""Deterministic FoundationDB 8.0.1 cluster simulation & real historical bug repros.

Boots unmodified official FoundationDB 8.0.1 binaries (`fdbserver`, `fdbcli`,
`libfdb_c.so`) inside multi-VM Rewind clusters across availability zones and
runs three real-world scenarios:

  1. `stale-peer` (default) — FoundationDB commit `eb5639a90e` (PR #13912 / #14050):
     7-VM cluster (`fdb1..fdb6` + `libfdb_c.so` client). Crashes the active
     Storage Server and compares `location_cache_peer_evictor_enabled=0` (buggy)
     vs `1` (fixed) using Rewind's L2 switch trace (`c.net_events()`) and FDB
     client XML traces.

  2. `globalconfig-crash` — FoundationDB commit `f8e1e90a40` (PR #14196):
     4-VM cluster (`fdb1..fdb3` + `client`). Writes an empty value to
     `\\xff/globalConfig/v` and catches the heap OOB read / assertion crash
     across ClusterController/GRV proxies in unmodified FDB 8.0.1.

  3. `stale-coordinator-dns` — FoundationDB commit `1915da14fc` (PR #14328):
     4-VM cluster (`fdb1..fdb3` + `libfdb_c.so` multi-version client). Opens
     databases using a connection string mixing the 3 live coordinators with
     stale `.invalid` coordinator hostnames and catches `getClusterProtocolImpl`
     wedging in `resolveWithRetry()` (error 1031 `transaction_timed_out`) even
     while the cluster is 100% healthy.

  4. `peek-timeout-clog` — FoundationDB commit `446079cc6a` (PR #13248):
     6-VM cluster (`fdb1..fdb5` + `client`). Runs non-streaming peeks
     (`--knob_peek_using_streaming=0`), clogs TLog <-> Storage Server links via
     `c.clog_link()`, and compares `--knob_peek_reply_timeout=0` (pre-fix
     wait-forever) against `--knob_peek_reply_timeout=2` (post-fix sender-side
     peek retries observed directly on Rewind's virtual switch).

  5. `subnet-prefix-bypass` — FoundationDB commit `be737227dc`:
     4-VM cluster (`fdb1..fdb3` + `client`). Starts `fdbserver` with invalid
     `--trusted-subnet-1` CIDR prefixes (`/-1`, `/33`, `/8junk`) and proves
     unmodified FDB 8.0.1 accepts them instead of rejecting with `invalid_option`
     (turning `/-1` into `0.0.0.0/0` and overflowing the 4-byte mask on `/33`).

  6. `reboot-chaos` — Multi-fault recovery & multi-boot trace verification:
     6-VM cluster (`fdb1..fdb5` + `client`). Injects link clogging, vCPU
     pause/resume, forward clock step + drift, and same-IP TLog crash + restart,
     verifying post-fault transactional continuity and multi-boot replay.

Build the EROFS root image once via:
    examples/cluster/foundationdb/build_image.sh

Then run:
    python3 examples/cluster/foundationdb_sim.py --scenario stale-peer
    python3 examples/cluster/foundationdb_sim.py --scenario globalconfig-crash
    python3 examples/cluster/foundationdb_sim.py --scenario stale-coordinator-dns
    python3 examples/cluster/foundationdb_sim.py --scenario peek-timeout-clog
    python3 examples/cluster/foundationdb_sim.py --scenario subnet-prefix-bypass
    python3 examples/cluster/foundationdb_sim.py --scenario reboot-chaos
    python3 examples/cluster/foundationdb_sim.py --scenario all --replay
"""

import argparse
import asyncio
import json
import os
import platform
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_DIR = REPO_ROOT / "sdk" / "python"
GUEST_DIR = Path(__file__).with_name("foundationdb")
sys.path.insert(0, str(SDK_DIR))

from rewind import Cluster


def resolve_fdb_erofs() -> str:
    env_path = os.environ.get("REWIND_FDB_EROFS")
    if env_path:
        return env_path
    arch = platform.machine().lower()
    subdir = "arm64-erofs" if arch in ("arm64", "aarch64") else "x86_64-erofs"
    candidate = REPO_ROOT / "target" / subdir / "fdb.erofs"
    if candidate.is_file():
        return str(candidate)
    raise FileNotFoundError(
        f"FoundationDB EROFS image not found at {candidate}.\n"
        f"Build it with `examples/cluster/foundationdb/build_image.sh` or set "
        f"`REWIND_FDB_EROFS=/path/to/fdb.erofs`."
    )


def guest_files(client_script: str) -> dict[str, str]:
    files = {
        "/build/run_fdb.sh": (GUEST_DIR / "run_fdb.sh").read_text(encoding="utf-8"),
        f"/build/{client_script}": (GUEST_DIR / client_script).read_text(
            encoding="utf-8"
        ),
    }
    for py in sorted((SDK_DIR / "rewind").glob("*.py")):
        files[f"/build/rewind__{py.name}"] = py.read_text(encoding="utf-8")
    return files


async def run_stale_peer_trial(
    fdb_erofs: str, mode: str, *, verify_replay: bool
) -> dict:
    files = guest_files("stale_peer_client.py")

    async with Cluster(
        f"fdb-stale-peer-{mode}",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=180_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        specs = [
            ("fdb1", "10.0.0.1", "az-1", "stateless"),
            ("fdb2", "10.0.0.2", "az-2", "log"),
            ("fdb3", "10.0.0.3", "az-3", "log"),
            ("fdb4", "10.0.0.4", "az-4", "storage"),
            ("fdb5", "10.0.0.5", "az-5", "storage"),
            ("fdb6", "10.0.0.6", "az-6", "storage"),
        ]
        for name, ip, zone, cls in specs:
            c.add_node(
                name,
                ["/bin/sh", "/build/run_fdb.sh", ip, zone, name, cls],
                ip=ip,
                role="service",
                zone=zone,
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/stale_peer_client.py", mode],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        warm = await c.wait_signal("warmed-up", timeout_ms=30_000)
        assert warm is not None
        await c.wait_signal(
            "ss-killed", node="client", barrier=True, timeout_ms=10_000
        )

        # Pick the Storage Server (fdb4/fdb5/fdb6) that the client talked to most.
        pre_events = (await c.net_events(node="client"))["events"]
        ss_counts = []
        for ss_name, ss_ip in [
            ("fdb4", "10.0.0.4"),
            ("fdb5", "10.0.0.5"),
            ("fdb6", "10.0.0.6"),
        ]:
            cnt = sum(
                1
                for e in pre_events
                if e.get("type") == "tx" and f"{ss_ip}:4500" in e.get("summary", "")
            )
            ss_counts.append((cnt, ss_name, ss_ip))
        ss_counts.sort(reverse=True)
        pre_cnt, victim_name, victim_ip = ss_counts[0]
        assert pre_cnt > 0, ss_counts
        print(
            f"[stale-peer:{mode}] warmed up at {warm.ms:.1f}ms VT; crashing active SS "
            f"{victim_name} ({victim_ip}:4500, {pre_cnt} pre-kill TX frames)",
            flush=True,
        )

        kill_res = await c.crash(victim_name)
        kill_ms = int(kill_res["cluster_ns"] / 1_000_000)
        await c.release("ss-killed", victim_ip)

        done = await c.wait_signal("done", timeout_ms=120_000)
        assert done is not None
        await client.wait_exit(timeout_ms=10_000)

        # Count all TX frames and new reconnect attempts (TCP SYN `[S]` / ARP) from
        # client -> dead SS in [kill+25s, kill+60s] (after eviction should finish).
        late_events = (await c.net_events(node="client", from_ms=kill_ms + 25_000))[
            "events"
        ]
        late_to_dead = [
            e
            for e in late_events
            if e.get("type") == "tx" and victim_ip in e.get("summary", "")
        ]
        late_reconnects = [
            e
            for e in late_to_dead
            if "[S]" in e.get("summary", "") or "ARP" in e.get("summary", "")
        ]
        run = await c.finish(verdict="passed")
        info = done.json()

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        print(
            f"[stale-peer:{mode}] late_reconnects={len(late_reconnects)} "
            f"(total_tx={len(late_to_dead)}) "
            f"evictions={info['evictions']} connect_failed={info['connect_failed_victim']} "
            f"wall={run['outcome']['wall_ms']}ms VT={run['outcome']['cluster_ns']/1e6:.1f}ms"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "mode": mode,
            "victim": f"{victim_name} ({victim_ip})",
            "pre_to_victim": pre_cnt,
            "late_reconnects": len(late_reconnects),
            "late_to_dead_victim": len(late_to_dead),
            "evictions": info["evictions"],
            "connect_failed_victim": info["connect_failed_victim"],
            "cluster_id": run["id"],
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def scenario_stale_peer(fdb_erofs: str, *, verify_replay: bool) -> dict:
    buggy = await run_stale_peer_trial(
        fdb_erofs, "buggy", verify_replay=verify_replay
    )
    fixed = await run_stale_peer_trial(
        fdb_erofs, "fixed", verify_replay=verify_replay
    )
    assert buggy["late_reconnects"] > 0 and buggy["evictions"] == 0, buggy
    assert fixed["late_reconnects"] == 0 and fixed["evictions"] > 0, fixed
    assert fixed["connect_failed_victim"] < buggy["connect_failed_victim"], (
        buggy,
        fixed,
    )
    return {"buggy": buggy, "fixed": fixed}


async def scenario_globalconfig_crash(fdb_erofs: str, *, verify_replay: bool) -> dict:
    files = guest_files("globalconfig_client.py")

    async with Cluster(
        "fdb-bug-f8e1e90a40",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=60_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        for i in range(1, 4):
            c.add_node(
                f"fdb{i}",
                [
                    "/bin/sh",
                    "/build/run_fdb.sh",
                    f"10.0.0.{i}",
                    f"az-{i}",
                    f"fdb{i}",
                    "unset",
                ],
                ip=f"10.0.0.{i}",
                role="service",
                zone=f"az-{i}",
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/globalconfig_client.py"],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        await c.wait_signal("before-bug", timeout_ms=20_000)
        await c.wait_signal("wrote-corrupt-globalconfig", timeout_ms=20_000)
        after = await c.wait_signal("after-bug-status", timeout_ms=20_000)
        assert after is not None

        st = await c.status()
        failed_nodes = []
        for n in st["nodes"]:
            if not n["name"].startswith("fdb"):
                continue
            logs = (await c.logs(n["name"]))["lines"]
            assertion_hits = [
                ln for ln in logs if "Assertion !globalConfigHistory.empty() failed" in ln
            ]
            if assertion_hits or (n["exited"] and n["exit_code"] != 0):
                failed_nodes.append(
                    {
                        "node": n["name"],
                        "exited": n["exited"],
                        "exit_code": n["exit_code"],
                        "assertions": len(assertion_hits),
                        "sample": assertion_hits[0] if assertion_hits else (logs[-1] if logs else ""),
                    }
                )

        await client.wait_exit(timeout_ms=10_000)
        run = await c.finish(verdict="passed")

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        assert failed_nodes, "expected unmodified FDB 8.0.1 nodes to hit globalConfigHistory assertion failure on empty globalConfig/v"
        print(
            f"[globalconfig-crash] caught assertion failure on {len(failed_nodes)} fdbserver node(s): "
            f"{[x['node'] for x in failed_nodes]} (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "cluster_id": run["id"],
            "failed_nodes": failed_nodes,
            "after_status": after.json(),
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def scenario_reboot_chaos(fdb_erofs: str, *, verify_replay: bool) -> dict:
    files = guest_files("reboot_chaos_client.py")

    async with Cluster(
        "fdb-reboot-chaos",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=120_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        specs = [
            ("fdb1", "10.0.0.1", "az-1", "stateless"),
            ("fdb2", "10.0.0.2", "az-2", "log"),
            ("fdb3", "10.0.0.3", "az-3", "log"),
            ("fdb4", "10.0.0.4", "az-4", "storage"),
            ("fdb5", "10.0.0.5", "az-5", "storage"),
        ]
        for name, ip, zone, cls in specs:
            c.add_node(
                name,
                [
                    "/bin/sh",
                    "/build/run_fdb.sh",
                    ip,
                    zone,
                    name,
                    cls,
                    "--knob_peek_reply_timeout=2",
                ],
                ip=ip,
                role="service",
                zone=zone,
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/reboot_chaos_client.py"],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        await c.wait_signal("initial-kv", timeout_ms=20_000)
        await c.wait_signal(
            "after-fault", node="client", barrier=True, timeout_ms=10_000
        )

        # 1. Clog TLog fdb2 <-> Storage Server fdb4, pause fdb5 briefly, step fdb4 clock.
        await c.clog_link("fdb2", "fdb4", duration_ms=1_500)
        await c.pause("fdb5")
        await c.step_clock("fdb4", delta_ms=250)
        await c.set_clock_drift("fdb4", 500)
        await c.advance(ms=150)
        await c.resume("fdb5")

        # 2. Same-IP hard crash + fresh reboot of TLog fdb2 (exercises multi-boot trace reset).
        await c.crash("fdb2")
        await c.advance(ms=50)
        await c.restart("fdb2")
        await c.unclog()

        await c.release("after-fault")
        post = await c.wait_signal("post-fault-kv", timeout_ms=60_000)
        assert post is not None
        post_data = post.json()
        assert post_data["k0_ok"] is True, post_data
        assert any(r["ok"] for r in post_data["results"]), post_data

        await client.wait_exit(timeout_ms=10_000)
        run = await c.finish(verdict="passed")

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        print(
            f"[reboot-chaos] post-fault writes={sum(1 for r in post_data['results'] if r['ok'])}/5 "
            f"k0_ok={post_data['k0_ok']} wall={run['outcome']['wall_ms']}ms "
            f"VT={run['outcome']['cluster_ns']/1e6:.1f}ms (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "cluster_id": run["id"],
            "post_fault": post_data,
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def scenario_stale_coordinator_dns(
    fdb_erofs: str, *, verify_replay: bool
) -> dict:
    files = guest_files("stale_coord_dns_client.py")

    async with Cluster(
        "fdb-bug-1915da14fc",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=90_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        for i in range(1, 4):
            c.add_node(
                f"fdb{i}",
                [
                    "/bin/sh",
                    "/build/run_fdb.sh",
                    f"10.0.0.{i}",
                    f"az-{i}",
                    f"fdb{i}",
                    "unset",
                ],
                ip=f"10.0.0.{i}",
                role="service",
                zone=f"az-{i}",
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/stale_coord_dns_client.py"],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        done = await c.wait_signal("done", timeout_ms=60_000)
        assert done is not None
        info = done.json()

        await client.wait_exit(timeout_ms=10_000)
        run = await c.finish(verdict="passed")

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        assert info["clean_err"] == 0, info
        assert info["stuck_timeout_1031"] > 0, info
        print(
            f"[stale-coordinator-dns] clean_err={info['clean_err']} "
            f"stuck_timeout_1031={info['stuck_timeout_1031']}/{info['total_stale_dbs']} "
            f"wall={run['outcome']['wall_ms']}ms (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "cluster_id": run["id"],
            **info,
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def run_peek_timeout_trial(
    fdb_erofs: str, label: str, peek_timeout: int, *, verify_replay: bool
) -> dict:
    files = guest_files("peek_timeout_client.py")

    async with Cluster(
        f"fdb-bug-446079cc6a-{label}",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=120_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        specs = [
            ("fdb1", "10.0.0.1", "az-1", "stateless"),
            ("fdb2", "10.0.0.2", "az-2", "log"),
            ("fdb3", "10.0.0.3", "az-3", "log"),
            ("fdb4", "10.0.0.4", "az-4", "storage"),
            ("fdb5", "10.0.0.5", "az-5", "storage"),
        ]
        for name, ip, zone, cls in specs:
            c.add_node(
                name,
                [
                    "/bin/sh",
                    "/build/run_fdb.sh",
                    ip,
                    zone,
                    name,
                    cls,
                    "--knob_peek_using_streaming=0",
                    f"--knob_peek_reply_timeout={peek_timeout}",
                ],
                ip=ip,
                role="service",
                zone=zone,
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/peek_timeout_client.py"],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        await c.wait_signal("warmed-up", timeout_ms=20_000)
        await c.wait_signal("clogged", node="client", barrier=True, timeout_ms=10_000)

        # Clog both TLogs <-> Storage Server fdb4 for 15s VT so fdb4's ServerPeekCursor
        # sits waiting for a TLogPeekReply that cannot arrive until unclogged.
        clog_res = await c.clog_link("fdb2", "fdb4", duration_ms=15_000)
        await c.clog_link("fdb3", "fdb4", duration_ms=15_000)
        clog_ms = int(clog_res["cluster_ns"] / 1_000_000)
        await c.release("clogged")

        await c.wait_signal("clog-phase-done", timeout_ms=45_000)
        await c.wait_signal("unclogged", node="client", barrier=True, timeout_ms=10_000)
        unclog_res = await c.unclog()
        unclog_ms = int(unclog_res["cluster_ns"] / 1_000_000)
        await c.release("unclogged")

        done = await c.wait_signal("done", timeout_ms=30_000)
        assert done is not None

        # Measure TX bytes sent by Storage Server fdb4 to TLogs (10.0.0.2 / 10.0.0.3)
        # across the clogged + flush window [clog_ms + 1.5s, unclog_ms + 0.2s]:
        # with peek_reply_timeout=2, fdb4 re-sends TLogPeekRequest RPCs while clogged
        # (buffered by TCP and flushed on unclog, adding +172 B per retry); with
        # peek_reply_timeout=0, it sits silent without retrying.
        clog_events = (
            await c.net_events(
                node="fdb4", from_ms=clog_ms + 1_500, to_ms=unclog_ms + 200
            )
        )["events"]
        tlog_tx = [
            e
            for e in clog_events
            if e.get("type") == "tx"
            and (
                "10.0.0.2:4500" in e.get("summary", "")
                or "10.0.0.3:4500" in e.get("summary", "")
            )
        ]
        tlog_tx_bytes = sum(int(e.get("len", 0)) for e in tlog_tx)

        await client.wait_exit(timeout_ms=10_000)
        run = await c.finish(verdict="passed")

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        print(
            f"[peek-timeout-clog:{label}] fdb4_tlog_tx_bytes={tlog_tx_bytes} "
            f"(frames={len(tlog_tx)}) post_ok={done.json()['post_ok']} "
            f"wall={run['outcome']['wall_ms']}ms (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "label": label,
            "peek_reply_timeout_knob": peek_timeout,
            "fdb4_tlog_tx_frames": len(tlog_tx),
            "fdb4_tlog_tx_bytes": tlog_tx_bytes,
            "post_ok": done.json()["post_ok"],
            "cluster_id": run["id"],
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def scenario_peek_timeout_clog(
    fdb_erofs: str, *, verify_replay: bool
) -> dict:
    buggy = await run_peek_timeout_trial(
        fdb_erofs, "buggy-timeout0", 0, verify_replay=verify_replay
    )
    fixed = await run_peek_timeout_trial(
        fdb_erofs, "fixed-timeout2", 2, verify_replay=verify_replay
    )
    assert fixed["fdb4_tlog_tx_bytes"] > buggy["fdb4_tlog_tx_bytes"], (
        buggy,
        fixed,
    )
    assert fixed["post_ok"] is True, fixed
    return {
        "buggy": buggy,
        "fixed": fixed,
        "extra_retry_bytes": fixed["fdb4_tlog_tx_bytes"] - buggy["fdb4_tlog_tx_bytes"],
    }


async def scenario_subnet_prefix_bypass(
    fdb_erofs: str, *, verify_replay: bool
) -> dict:
    files = guest_files("subnet_prefix_client.py")

    async with Cluster(
        "fdb-bug-be737227dc",
        latency_ms=1,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=60_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        for i in range(1, 4):
            extra = ["--trusted-subnet-1=10.0.0.1/-1"] if i == 1 else []
            c.add_node(
                f"fdb{i}",
                [
                    "/bin/sh",
                    "/build/run_fdb.sh",
                    f"10.0.0.{i}",
                    f"az-{i}",
                    f"fdb{i}",
                    "unset",
                    *extra,
                ],
                ip=f"10.0.0.{i}",
                role="service",
                zone=f"az-{i}",
                root=fdb_erofs,
                files=files,
            )
        client = c.add_node(
            "client",
            ["python3", "-u", "/build/subnet_prefix_client.py"],
            ip="10.0.0.10",
            role="driver",
            mem_mib=256,
            root=fdb_erofs,
            files=files,
        )

        await c.start()
        cfg = await c.wait_signal(
            names=["configured", "configure-failed"], timeout_ms=30_000
        )
        assert cfg is not None and cfg.name == "configured", cfg
        done = await c.wait_signal("done", timeout_ms=30_000)
        assert done is not None
        info = done.json()

        await client.wait_exit(timeout_ms=10_000)
        run = await c.finish(verdict="passed")

        replay_ok = None
        if verify_replay:
            rep = await c.replay(machine="none")
            assert rep["identical"] is True, rep
            replay_ok = {
                "identical": True,
                "total_events": rep["total_events"],
                "net_events": rep["net_events"],
            }

        accepted = [p["subnet"] for p in info["probes"] if p["accepted"]]
        assert info["cluster_with_neg1_subnet_ok"] is True, info
        assert len(accepted) == len(info["probes"]), info
        print(
            f"[subnet-prefix-bypass] fdb1 accepted --trusted-subnet-1=10.0.0.1/-1 "
            f"& all {len(accepted)} invalid CIDRs accepted: {accepted} "
            f"wall={run['outcome']['wall_ms']}ms (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "cluster_id": run["id"],
            **info,
            "wall_ms": run["outcome"]["wall_ms"],
            "replay": replay_ok,
        }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run FoundationDB 8.0.1 deterministic simulation examples in Rewind VM."
    )
    parser.add_argument(
        "--scenario",
        choices=[
            "stale-peer",
            "globalconfig-crash",
            "stale-coordinator-dns",
            "peek-timeout-clog",
            "subnet-prefix-bypass",
            "reboot-chaos",
            "all",
        ],
        default="stale-peer",
        help="Scenario to run (default: stale-peer).",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Verify bit-for-bit deterministic replay after each run.",
    )
    args = parser.parse_args()
    fdb_erofs = resolve_fdb_erofs()

    summary: dict[str, object] = {}
    if args.scenario in ("stale-peer", "all"):
        summary["stale-peer"] = await scenario_stale_peer(
            fdb_erofs, verify_replay=args.replay
        )
    if args.scenario in ("globalconfig-crash", "all"):
        summary["globalconfig-crash"] = await scenario_globalconfig_crash(
            fdb_erofs, verify_replay=args.replay
        )
    if args.scenario in ("stale-coordinator-dns", "all"):
        summary["stale-coordinator-dns"] = await scenario_stale_coordinator_dns(
            fdb_erofs, verify_replay=args.replay
        )
    if args.scenario in ("peek-timeout-clog", "all"):
        summary["peek-timeout-clog"] = await scenario_peek_timeout_clog(
            fdb_erofs, verify_replay=args.replay
        )
    if args.scenario in ("subnet-prefix-bypass", "all"):
        summary["subnet-prefix-bypass"] = await scenario_subnet_prefix_bypass(
            fdb_erofs, verify_replay=args.replay
        )
    if args.scenario in ("reboot-chaos", "all"):
        summary["reboot-chaos"] = await scenario_reboot_chaos(
            fdb_erofs, verify_replay=args.replay
        )

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
