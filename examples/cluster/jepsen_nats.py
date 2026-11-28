#!/usr/bin/env python3
"""Run real Jepsen (`jepsen 0.3.10` + `jepsen-io/nats` queue workload/checker)
against unmodified NATS 2.12.1 inside a deterministic Rewind VM cluster,
reproducing Kyle Kingsbury's December 8, 2025 Jepsen NATS 2.12.1 analysis.

Cluster layout (4 VMs on Rewind's deterministic L2 virtual switch):
  - `n1`, `n2`, `n3` (10.0.0.1..3): unmodified NATS 2.12.1 JetStream cluster
  - `control`        (10.0.0.10):   OpenJDK 21 + AOT Jepsen uberjar with
                                    `jepsen.rewind` (`RewindNet`, `RewindDB`,
                                    `clock-package`)

Every Jepsen nemesis action (`partition`, `pause`, `kill`, `clock`) flows over
`POST http://rewind/barrier/jepsen-fault` to this host script, which executes
the matching Rewind hypervisor fault (`asymmetric_partition`, `heal`, `pause`,
`resume`, `crash`, `restart`, `step_clock`, `set_clock_drift`) and releases the
barrier.

Build the two EROFS images once via:
    examples/cluster/jepsen_nats/build_images.sh

Then run:
    python3 examples/cluster/jepsen_nats.py --scenario healthy
    python3 examples/cluster/jepsen_nats.py --scenario crash-loss
    python3 examples/cluster/jepsen_nats.py --scenario chaos
    python3 examples/cluster/jepsen_nats.py --scenario all --replay
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
GUEST_DIR = Path(__file__).with_name("jepsen_nats")
sys.path.insert(0, str(SDK_DIR))

from rewind import Cluster


def resolve_images() -> tuple[str, str]:
    arch = platform.machine().lower()
    subdir = "arm64-erofs" if arch in ("arm64", "aarch64") else "x86_64-erofs"
    nats_erofs = os.environ.get(
        "REWIND_NATS_EROFS",
        str(REPO_ROOT / "target" / subdir / "nats-2.12.1.erofs"),
    )
    jepsen_erofs = os.environ.get(
        "REWIND_JEPSEN_NATS_EROFS",
        str(REPO_ROOT / "target" / subdir / "jepsen-nats.erofs"),
    )
    for label, path in [("NATS", nats_erofs), ("Jepsen", jepsen_erofs)]:
        if not Path(path).is_file():
            raise FileNotFoundError(
                f"{label} EROFS image not found at {path}.\n"
                f"Build both images with `examples/cluster/jepsen_nats/build_images.sh`."
            )
    return nats_erofs, jepsen_erofs


async def apply_rewind_fault(
    c: Cluster, cmd: dict, *, crashed: set[str], paused: set[str]
) -> str:
    op = cmd["op"]
    if op == "drop":
        await c.partition([cmd["src"]], [cmd["dst"]], symmetric=False)
    elif op == "asymmetric_partition":
        for a, b in cmd["blocked"]:
            await c.partition([a], [b], symmetric=False)
    elif op == "heal":
        await c.heal()
        await c.unclog()
    elif op == "slow":
        nodes = cmd["nodes"]
        mean_ms = int(cmd.get("mean_ms", 50))
        jitter_ms = int(cmd.get("jitter_ms", 10))
        for a in nodes:
            for b in nodes:
                if a != b:
                    await c.set_link(a, b, latency_ms=mean_ms, jitter_ms=jitter_ms)
    elif op == "flaky":
        nodes = cmd["nodes"]
        drop_ppm = int(cmd.get("drop_ppm", 200_000))
        for a in nodes:
            for b in nodes:
                if a != b:
                    await c.set_link(a, b, drop_ppm=drop_ppm)
    elif op == "fast":
        nodes = cmd["nodes"]
        for a in nodes:
            for b in nodes:
                if a != b:
                    await c.set_link(a, b, latency_ms=1, jitter_ms=0, drop_ppm=0)
    elif op == "crash":
        node = cmd["node"]
        if node not in crashed:
            paused.discard(node)
            await c.crash(node)
            crashed.add(node)
    elif op == "restart":
        node = cmd["node"]
        if node in crashed:
            await c.restart(node)
            crashed.discard(node)
            paused.discard(node)
        else:
            return f"noop:already-running:{node}"
    elif op == "pause":
        node = cmd["node"]
        if node not in crashed and node not in paused:
            await c.pause(node)
            paused.add(node)
        else:
            return f"noop:cannot-pause:{node}"
    elif op == "resume":
        node = cmd["node"]
        if node in paused:
            await c.resume(node)
            paused.discard(node)
        else:
            return f"noop:not-paused:{node}"
    elif op == "step_clock":
        node = cmd["node"]
        if node not in crashed:
            await c.step_clock(node, delta_ms=int(cmd["delta_ms"]))
    elif op == "set_clock_drift":
        node = cmd["node"]
        if node not in crashed:
            await c.set_clock_drift(node, int(cmd["drift_ppm"]))
    else:
        return f"ignored:{op}"
    return f"ok:{op}"


async def run_jepsen_scenario(
    nats_erofs: str,
    jepsen_erofs: str,
    *,
    label: str,
    nemesis: str,
    db_targets: str = "one,majority",
    time_limit: int = 16,
    final_time_limit: int = 10,
    rate: int = 20,
    nemesis_interval: int = 4,
    expect_valid: bool,
    verify_replay: bool,
) -> dict:
    run_nats_sh = (GUEST_DIR / "run_nats.sh").read_text(encoding="utf-8")

    jepsen_cmd = [
        "/opt/java/openjdk/bin/java",
        "-Xms256m",
        "-Xmx512m",
        "-XX:ActiveProcessorCount=1",
        "-Djava.awt.headless=true",
        "-Djava.net.preferIPv4Stack=true",
        "-jar",
        "/opt/jepsen/jepsen-nats.jar",
        "--nodes",
        "n1,n2,n3",
        "--nemesis",
        nemesis,
        "--db-targets",
        db_targets,
        "--time-limit",
        str(time_limit),
        "--final-time-limit",
        str(final_time_limit),
        "--initial-quiet-period",
        "2",
        "--rate",
        str(rate),
        "--concurrency",
        "3",
        "--nemesis-interval",
        str(nemesis_interval),
    ]

    async with Cluster(
        f"jepsen-nats-2.12.1-{label}",
        latency_ms=1,
        mem_mib=256,
        stop_when="driver",
        max_virtual_ms=180_000,
        slice_us=500,
        keyframes=False,
    ) as c:
        for i in range(1, 4):
            c.add_node(
                f"n{i}",
                ["/bin/sh", "/build/run_nats.sh", f"n{i}"],
                ip=f"10.0.0.{i}",
                role="service",
                zone=f"az-{i}",
                mem_mib=256,
                root=nats_erofs,
                files={"/build/run_nats.sh": run_nats_sh},
            )
        control = c.add_node(
            "control",
            jepsen_cmd,
            ip="10.0.0.10",
            role="driver",
            mem_mib=1024,
            env=[
                "HOME=/tmp",
                "TMPDIR=/tmp",
                "PATH=/opt/java/openjdk/bin:/usr/local/bin:/usr/bin:/bin",
            ],
            root=jepsen_erofs,
        )

        await c.start()
        faults_applied: list[dict] = []
        crashed_nodes: set[str] = set()
        paused_nodes: set[str] = set()
        jepsen_summary: dict | None = None

        # Relay loop: wait for either a `jepsen-fault` barrier from `jepsen.rewind`,
        # `jepsen-result`, or `jepsen-error`.
        while True:
            sig = await c.wait_signal(
                names=["jepsen-fault", "jepsen-result", "jepsen-error"],
                node="control",
                timeout_ms=150_000,
            )
            assert sig is not None
            if sig.name == "jepsen-fault":
                cmd = sig.json()
                reply = await apply_rewind_fault(
                    c, cmd, crashed=crashed_nodes, paused=paused_nodes
                )
                if not reply.startswith("noop:"):
                    faults_applied.append({"at_ms": round(sig.ms, 1), **cmd})
                await c.release("jepsen-fault", reply)
            elif sig.name == "jepsen-error":
                raise RuntimeError(f"Jepsen control VM failed: {sig.json()}")
            else:
                jepsen_summary = sig.json()
                break

        await control.wait_exit(timeout_ms=15_000)
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

        assert jepsen_summary is not None
        assert jepsen_summary["acknowledged_count"] > 0, jepsen_summary
        assert jepsen_summary["workload_valid"] is expect_valid, jepsen_summary

        lost_info = jepsen_summary["lost"]
        print(
            f"[jepsen:{label}] valid={jepsen_summary['workload_valid']} "
            f"acked={jepsen_summary['acknowledged_count']} "
            f"read={jepsen_summary['read_count']} "
            f"lost={lost_info.get('lost-count', 0)} "
            f"(prefix={lost_info.get('lost-prefix-count', 0)}, "
            f"middle={lost_info.get('lost-middle-count', 0)}, "
            f"postfix={lost_info.get('lost-postfix-count', 0)}) "
            f"faults={len(faults_applied)} wall={run['outcome']['wall_ms']}ms "
            f"VT={run['outcome']['cluster_ns']/1e6:.1f}ms (cluster {run['id']})"
            + (" replay=IDENTICAL" if replay_ok else ""),
            flush=True,
        )
        return {
            "label": label,
            "cluster_id": run["id"],
            "faults_applied": faults_applied,
            "jepsen": jepsen_summary,
            "wall_ms": run["outcome"]["wall_ms"],
            "virtual_ms": round(run["outcome"]["cluster_ns"] / 1e6, 1),
            "replay": replay_ok,
        }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run real Jepsen against NATS 2.12.1 on Rewind VM."
    )
    parser.add_argument(
        "--scenario",
        choices=["healthy", "crash-loss", "chaos", "all"],
        default="crash-loss",
        help="Scenario to run (default: crash-loss).",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Verify bit-for-bit deterministic replay after each Jepsen run.",
    )
    args = parser.parse_args()
    nats_erofs, jepsen_erofs = resolve_images()

    summary: dict[str, object] = {}
    if args.scenario in ("healthy", "all"):
        summary["healthy"] = await run_jepsen_scenario(
            nats_erofs,
            jepsen_erofs,
            label="healthy",
            nemesis="none",
            time_limit=10,
            final_time_limit=8,
            expect_valid=True,
            verify_replay=args.replay,
        )
    if args.scenario in ("crash-loss", "all"):
        summary["crash-loss"] = await run_jepsen_scenario(
            nats_erofs,
            jepsen_erofs,
            label="crash-loss",
            nemesis="kill",
            db_targets="all",
            time_limit=16,
            final_time_limit=10,
            nemesis_interval=4,
            expect_valid=False,
            verify_replay=args.replay,
        )
    if args.scenario in ("chaos", "all"):
        summary["chaos"] = await run_jepsen_scenario(
            nats_erofs,
            jepsen_erofs,
            label="chaos",
            nemesis="partition,pause,clock,kill",
            db_targets="one,majority,all",
            time_limit=22,
            final_time_limit=10,
            nemesis_interval=3,
            expect_valid=False,
            verify_replay=args.replay,
        )

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
