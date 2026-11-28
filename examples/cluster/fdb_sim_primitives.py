#!/usr/bin/env python3
"""Demonstrates all six FoundationDB-inspired Rewind simulation primitives
(zero FDB binary dependency — requires only `nixpkgs#python3` or `REWIND_PYTHON_EROFS`):

  1. Network clogging (`clog_link` / `unclog_link`) — queues frames without drops
     and bursts them on unclog.
  2. Node vCPU pause/resume (`pause` / `resume`) — freezes a guest while queueing
     inbound packets until resumed.
  3. Dynamic mid-run clock faults (`step_clock` + `set_clock_drift`).
  4. Quiet / settle phase (`set_nemesis`) + protected nodes (`protect_nodes`).
  5. Fault-domain locality (`zone`, `rack`, `dc`, `nodes_in`, `partition_zone`).
  6. Language-agnostic `http://rewind` `BUGGIFY` (`/buggify/<name>`) & `CODE_PROBE`
     (`/probe/<name>`), persisted in the cluster manifest and verified by
     `replay(machine="every")`.

Run with:
    python3 examples/cluster/fdb_sim_primitives.py
"""

import asyncio
import os
import sys
from pathlib import Path

SDK_DIR = Path(__file__).resolve().parents[2] / "sdk" / "python"
GUEST_DIR = Path(__file__).with_name("fdb_sim_primitives")
sys.path.insert(0, str(SDK_DIR))

from rewind import Cluster, Nemesis


def guest_files() -> dict[str, str]:
    files = {"/build/guest.py": (GUEST_DIR / "guest.py").read_text(encoding="utf-8")}
    for py in sorted((SDK_DIR / "rewind").glob("*.py")):
        files[f"/build/rewind__{py.name}"] = py.read_text(encoding="utf-8")
    return files


async def main() -> None:
    py_erofs = os.environ.get("REWIND_PYTHON_EROFS")
    node_kwargs = {"root": py_erofs} if py_erofs else {"packages": ["nixpkgs#python3"]}

    async with Cluster(
        "fdb-sim-primitives",
        latency_ms=1,
        mem_mib=192,
        stop_when="all",
        max_virtual_ms=60_000,
        slice_us=250,
        seed=777,
        schedule=1,
        buggify=True,
        nemesis=Nemesis(
            tick_ms=200,
            partition_prob_ppm=0,
            heal_prob_ppm=0,
            crash_prob_ppm=0,
            restart_prob_ppm=0,
            max_crashed=0,
            protect_nodes=["n1"],
        ),
        nemesis_enabled=True,
    ) as c:
        files = guest_files()
        n1 = c.add_node(
            "n1",
            ["python3", "-u", "/build/guest.py"],
            ip="10.0.0.1",
            role="driver",
            zone="az-a",
            rack="r1",
            dc="us-east",
            files=files,
            **node_kwargs,
        )
        n2 = c.add_node(
            "n2",
            ["python3", "-u", "/build/guest.py"],
            ip="10.0.0.2",
            role="driver",
            zone="az-b",
            rack="r2",
            dc="us-east",
            files=files,
            **node_kwargs,
        )

        assert await c.nodes_in(zone="az-a") == ["n1"]
        assert await c.nodes_in(zone="az-b") == ["n2"]
        assert await c.nodes_in(dc="us-east") == ["n1", "n2"]

        await c.start()

        # 1. Locality + two-stage BUGGIFY + CODE_PROBE
        s1 = await c.wait_signal("n1-meta", timeout_ms=20_000)
        s2 = await c.wait_signal("n2-meta", timeout_ms=20_000)
        assert s1 is not None and s2 is not None
        d1, d2 = s1.json(), s2.json()
        assert d1["meta"]["zone"] == "az-a" and d1["meta"]["rack"] == "r1"
        assert d1["meta"]["dc"] == "us-east" and d1["meta"]["buggify"] is True
        assert d2["meta"]["zone"] == "az-b"

        fired_sites = [k for k, v in d1["sites"].items() if v]
        quiet_sites = [k for k, v in d1["sites"].items() if not v]
        assert fired_sites and quiet_sites, d1["sites"]
        print(f"[1] locality + two-stage BUGGIFY OK ({len(fired_sites)}/24 active)", flush=True)

        await c.wait_signal("check-buggify-off", node="n1", barrier=True, timeout_ms=10_000)
        await c.set_buggify(False)
        await n1.release("check-buggify-off", fired_sites[0])
        off_sig = await c.wait_signal("buggify-after-off", timeout_ms=10_000)
        assert off_sig is not None and off_sig.json()["fire"] is False, off_sig
        print("[2] set_buggify(False) OK", flush=True)

        assert (await c.probes()).get("boot-ok") == 2

        # 2. Network Clogging (packets queued with 0 drops, burst on unclog)
        await c.wait_signal("udp-listening", node="n2", timeout_ms=10_000)
        await c.wait_signal("send-clogged", node="n1", barrier=True, timeout_ms=10_000)
        await c.clog_link("n1", "n2", duration_ms=5_000, bidirectional=False)
        await n1.release("send-clogged")
        await c.wait_signal("sent-clogged-ok", node="n1", timeout_ms=10_000)

        early = await c.wait_signal(
            "udp-recv", node="n2", timeout_ms=200, detect_deadlock=False, check=False
        )
        assert early is None, f"expected clogged packet to stay queued, got {early}"
        st_clog = await c.status()
        assert st_clog["stats"]["dropped_frames"] == 0, st_clog["stats"]
        assert st_clog["stats"]["clogs"] >= 1, st_clog["stats"]

        await c.unclog_link("n1", "n2", bidirectional=False)
        r1 = await c.wait_signal("udp-recv", node="n2", timeout_ms=5_000)
        r2 = await c.wait_signal("udp-recv", node="n2", timeout_ms=5_000)
        assert r1 is not None and r2 is not None
        assert {r1.json()["msg"], r2.json()["msg"]} == {"msg-1", "msg-2"}
        print("[3] clog_link + unclog_link burst OK (0 drops)", flush=True)

        # 3. Node Pause / Resume
        await c.wait_signal("send-while-paused", node="n1", barrier=True, timeout_ms=10_000)
        await n2.pause()
        st_paused = await c.status()
        n2_st = [x for x in st_paused["nodes"] if x["name"] == "n2"][0]
        assert n2_st["paused"] is True
        step_before = n2_st["step"]

        await n1.release("send-while-paused")
        await c.wait_signal("sent-paused-ok", node="n1", timeout_ms=10_000)
        await c.advance(ms=200)
        n2_still = [x for x in (await c.status())["nodes"] if x["name"] == "n2"][0]
        assert n2_still["step"] == step_before

        await n2.resume()
        r3 = await c.wait_signal("udp-recv", node="n2", timeout_ms=5_000)
        assert r3 is not None and r3.json()["msg"] == "while-paused"
        print("[4] pause + queued RX + resume OK", flush=True)

        # 4. Dynamic Clock Step + Drift
        await c.wait_signal("clock-step", node="n1", barrier=True, timeout_ms=10_000)
        await n1.step_clock(delta_ms=5_000)
        await n1.set_clock_drift(50_000)
        await n1.release("clock-step")
        clk = await c.wait_signal("clock-result", node="n1", timeout_ms=10_000)
        assert clk is not None and clk.json()["dt_ms"] >= 4_900.0
        print(f"[5] step_clock + set_clock_drift OK (+{clk.json()['dt_ms']:.1f}ms)", flush=True)

        # 5. Quiet / Settle Phase + Fault-Domain Partition
        await c.set_nemesis(False)
        assert (await c.status())["nemesis_enabled"] is False
        await c.partition_zone("az-a")
        assert len((await c.status())["active_partitions"]) == 2
        await c.heal()
        assert len((await c.status())["active_partitions"]) == 0
        print("[6] set_nemesis(False) + partition_zone + heal OK", flush=True)

        await c.wait_signal("done", node="n1", barrier=True, timeout_ms=10_000)
        await c.wait_signal("done", node="n2", barrier=True, timeout_ms=10_000)
        rel = await c.release("done")
        assert rel["released"] == 2
        ex = await c.wait_exit(timeout_ms=10_000)
        assert ex["exited"]

        run = await c.finish(verdict="passed")
        manifest = await c.show()
        assert manifest["probes"] == {"boot-ok": 2, "sent-clogged": 1, "udp-recv": 3}
        assert len(manifest["buggify_sites"]) == 24

        rep = await c.replay(machine="every")
        assert rep["identical"] is True, rep
        print(
            f"[7] recorded {run['id']} and verified replay(machine='every') IDENTICAL: "
            f"{rep['total_events']} trace records, {rep['net_events']} net events, "
            f"{rep['keyframes_compared']} keyframe(s)",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
