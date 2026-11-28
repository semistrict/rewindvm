#!/usr/bin/env python3
"""3-VM PostgreSQL + Umami (Next.js + Prisma) + Playwright snapshot-after-login demo.

Demonstrates booting a real 3-tier web stack across 3 separate Rewind VMs:
  - `db`      (10.0.0.1): PostgreSQL 18
  - `app`     (10.0.0.2): Umami 3.5.0 (production Next.js + Prisma 7 standalone server)
  - `browser` (10.0.0.3): Playwright + headless Chromium

Workflow:
  1. Boots Postgres, runs Prisma migrations + boots Next.js, launches Chromium,
     and performs a real UI login (`admin` / `umami`).
  2. Captures ONE whole-cluster snapshot (`c.snapshot("logged-in")`) across all
     3 VMs + the virtual L2 switch while Chromium is already logged in on the
     authenticated dashboard.
  3. Runs 5 completely isolated Playwright E2E tests from that exact same snapshot
     using in-place dirty-page rollback (`c.restore("logged-in")`) + deterministic
     console input dispatch (`browser.send_input("<test>\n")`), including a live
     `app <-> db` network partition + recovery test — with zero DB truncation
     scripts, zero re-login, and zero Chromium restarts!

Supports either Nix (`nixpkgs#postgresql`, `nixpkgs#umami`, etc.) or prebuilt
EROFS images via `REWIND_POSTGRES_EROFS`, `REWIND_UMAMI_EROFS`, and
`REWIND_PLAYWRIGHT_EROFS` (for non-Nix macOS Apple Silicon HVF runs).
"""

import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind import Cluster

RUNNER_SCRIPT = Path(__file__).with_name("umami_playwright_runner.py").read_text(
    encoding="utf-8"
)

DB_START_CMD = """\
set -eu
for d in /nix/store/*-postgresql-*/bin; do
  if [ -x "$d/initdb" ]; then
    PATH="$d:$PATH"
    break
  fi
done
mkdir -p /tmp/pgdata /tmp/pgrun
initdb -D /tmp/pgdata -U postgres --auth=trust >/dev/null
printf "listen_addresses = '0.0.0.0'\nport = 5432\nunix_socket_directories = '/tmp/pgrun'\nfsync = off\nsynchronous_commit = off\n" >> /tmp/pgdata/postgresql.conf
printf "host all all 0.0.0.0/0 trust\n" >> /tmp/pgdata/pg_hba.conf
pg_ctl -D /tmp/pgdata -l /tmp/pg.log -o "-k /tmp/pgrun" start
psql -h /tmp/pgrun -U postgres -c "CREATE USER umami WITH PASSWORD 'umami' SUPERUSER;"
psql -h /tmp/pgrun -U postgres -c "CREATE DATABASE umami OWNER umami;"
echo PG_READY
pg_ctl -D /tmp/pgdata stop >/dev/null
exec postgres -D /tmp/pgdata -k /tmp/pgrun
"""

APP_START_CMD = """\
set -eu
until (nc -z 10.0.0.1 5432 2>/dev/null || bash -c 'exec 3<>/dev/tcp/10.0.0.1/5432' 2>/dev/null); do
  sleep 0.25
done
echo DB_PORT_OPEN
exec umami-server
"""

SPECS = [
    "create_website",
    "track_pageview",
    "update_and_delete_website",
    "db_partition_and_recover",
    "clean_isolation",
]


async def run_one_spec(cluster: Cluster, spec: str) -> dict:
    t0 = time.perf_counter()
    browser = cluster.node("browser")
    await browser.send_input(f"{spec}\n")

    if spec == "db_partition_and_recover":
        await browser.wait_for("READY_FOR_DB_FAULT", from_start=True, timeout_ms=30_000)
        await cluster.partition(["db"], ["app", "browser"])
        await browser.send_input("PARTITIONED\n")
        await browser.wait_for("DB_FAULT_OBSERVED", from_start=True, timeout_ms=30_000)
        await cluster.heal()
        await browser.send_input("HEALED\n")

    await browser.wait_for(f"PASS:{spec}", from_start=True, timeout_ms=60_000)
    exit_res = await browser.wait_exit(timeout_ms=30_000)
    assert exit_res["exit_code"] == 0, f"{spec}: browser exited with {exit_res}"
    run = await cluster.finish(verdict="passed")
    return {
        "spec": spec,
        "run_id": run["id"],
        "wall_s": time.perf_counter() - t0,
        "cluster_ms": exit_res["cluster_ns"] / 1e6,
    }


async def main() -> None:
    pg_erofs = os.environ.get("REWIND_POSTGRES_EROFS")
    umami_erofs = os.environ.get("REWIND_UMAMI_EROFS")
    pw_erofs = os.environ.get("REWIND_PLAYWRIGHT_EROFS")
    display = os.environ.get("REWIND_DISPLAY", "1") not in ("0", "false", "no", "off")
    parallel_specs = "--parallel" in sys.argv or os.environ.get(
        "REWIND_PARALLEL_SPECS", "0"
    ) in ("1", "true", "yes", "on")

    t0 = time.perf_counter()
    async with Cluster(
        "umami-nextjs-playwright-3vm",
        latency_ms=1,
        jitter_us=100,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=300_000,
        slice_us=250,
    ) as c:
        db_kwargs = (
            {"root": pg_erofs} if pg_erofs else {"packages": ["nixpkgs#postgresql"]}
        )
        db = c.add_node(
            "db",
            ["/bin/sh", "-c", DB_START_CMD],
            ip="10.0.0.1",
            role="service",
            uid=70,
            gid=70,
            mem_mib=512,
            **db_kwargs,
        )

        app_kwargs = (
            {"root": umami_erofs} if umami_erofs else {"packages": ["nixpkgs#umami"]}
        )
        app = c.add_node(
            "app",
            ["/bin/sh", "-c", APP_START_CMD],
            ip="10.0.0.2",
            role="service",
            mem_mib=1024,
            env=[
                "DATABASE_URL=postgresql://umami:umami@10.0.0.1:5432/umami",
                "APP_SECRET=rewind-deterministic-secret-0123456789",
                "HOSTNAME=0.0.0.0",
                "PORT=3000",
                "HOME=/tmp",
            ],
            **app_kwargs,
        )

        pw_pkgs = [
            "nixpkgs#python3Packages.playwright",
            "nixpkgs#playwright-driver.browsers-chromium",
        ]
        if display:
            pw_pkgs.extend(
                [
                    "nixpkgs#labwc",
                    "nixpkgs#seatd",
                    "nixpkgs#dejavu_fonts",
                    "nixpkgs#fontconfig",
                ]
            )
        pw_kwargs = {"root": pw_erofs} if pw_erofs else {"packages": pw_pkgs}
        pw_env = [
            "UMAMI_URL=http://10.0.0.2:3000",
            "HOME=/tmp",
            "XDG_CACHE_HOME=/tmp/cache",
            "XDG_CONFIG_HOME=/tmp/config",
        ]
        if pw_erofs:
            pw_env.append("PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers")
        browser = c.add_node(
            "browser",
            ["python3", "-u", "/build/runner.py"],
            ip="10.0.0.3",
            role="driver",
            mem_mib=1536,
            display=display,
            display_width=1280 if display else None,
            display_height=720 if display else None,
            env=pw_env,
            files={"/build/runner.py": RUNNER_SCRIPT},
            **pw_kwargs,
        )

        await c.start()
        try:
            await db.wait_for("PG_READY", from_start=True, timeout_ms=60_000)
            await app.wait_for("Ready in", from_start=True, timeout_ms=90_000)
            await browser.wait_for("READY_LOGGED_IN", from_start=True, timeout_ms=90_000)
            # Let the runner enter its blocking read on /dev/rewind-console.
            await c.advance(ms=100)
            t_boot = time.perf_counter()

            # Snapshot the entire 3-VM stack once while Chromium is already logged in!
            t_snap0 = time.perf_counter()
            snap = await c.snapshot("logged-in")
            t_snap = time.perf_counter() - t_snap0
            print(
                f"[setup] booted Postgres + Umami (Next.js) + Chromium login in "
                f"{t_boot - t0:.2f}s; snapshot={t_snap:.2f}s",
                flush=True,
            )

            if parallel_specs:
                with tempfile.TemporaryDirectory(prefix="rewind-umami-snap-") as tmp:
                    snap_path = Path(tmp) / "logged-in.rwcsnap"
                    t_save0 = time.perf_counter()
                    await c.save_snapshot(snap_path, name=snap)
                    t_save = time.perf_counter() - t_save0
                    # Close the setup cluster before fanning out N parallel clusters so
                    # all host cores and memory go directly to the parallel workers.
                    await c.close()

                    t_fan0 = time.perf_counter()
                    results = await Cluster.fan_out(
                        snap_path,
                        SPECS,
                        run_one_spec,
                        max_workers=int(
                            os.environ.get("REWIND_FANOUT_WORKERS", str(len(SPECS)))
                        ),
                        name_prefix="umami",
                    )
                    t_fan = time.perf_counter() - t_fan0
                    sum_s = sum(r["wall_s"] for _, r in results)
                    for spec, r in results:
                        print(
                            f"[parallel] {spec:26s} PASS  "
                            f"(run={r['wall_s']:5.2f}s, vtime={r['cluster_ms']:.1f}ms, id={r['run_id']})",
                            flush=True,
                        )
                    print(
                        f"fan-out {len(SPECS)} parallel 3-VM clusters ({len(SPECS)*3} VMs) "
                        f"finished in {t_fan:.2f}s wall (sum={sum_s:.2f}s, "
                        f"{sum_s / t_fan:.2f}x speedup, save_snapshot={t_save*1000:.1f}ms)",
                        flush=True,
                    )
                return

            restore_times = []
            for spec in SPECS:
                t_r0 = time.perf_counter()
                await c.restore(snap)
                t_restore = time.perf_counter() - t_r0
                restore_times.append(t_restore)

                t_test0 = time.perf_counter()
                await browser.send_input(f"{spec}\n")

                if spec == "db_partition_and_recover":
                    await browser.wait_for(
                        "READY_FOR_DB_FAULT", from_start=True, timeout_ms=30_000
                    )
                    await c.partition(["db"], ["app", "browser"])
                    await browser.send_input("PARTITIONED\n")
                    await browser.wait_for(
                        "DB_FAULT_OBSERVED", from_start=True, timeout_ms=30_000
                    )
                    await c.heal()
                    await browser.send_input("HEALED\n")

                await browser.wait_for(f"PASS:{spec}", from_start=True, timeout_ms=60_000)
                t_test = time.perf_counter() - t_test0
                print(
                    f"[test] {spec:26s} PASS  (restore={t_restore*1000:6.1f}ms, run={t_test:5.2f}s)",
                    flush=True,
                )

            exit_res = await browser.wait_exit(timeout_ms=30_000)
            assert exit_res["exit_code"] == 0, f"browser exited with {exit_res}"
            run = await c.finish(verdict="passed")
            avg_restore = sum(restore_times) / len(restore_times) * 1000
            vtime_ms = exit_res["cluster_ns"] / 1e6
            print(
                f"recorded cluster {run['id']} ({run['verdict']}) at "
                f"{vtime_ms:.1f}ms virtual time "
                f"(avg restore={avg_restore:.1f}ms across {len(SPECS)} Playwright tests)",
                flush=True,
            )
        except Exception:
            if c._started:
                for name, h in [("db", db), ("app", app), ("browser", browser)]:
                    print(f"\n=== {name} output (tail) ===", file=sys.stderr)
                    for line in (await h.logs(stream="output"))[-30:]:
                        print(line, file=sys.stderr)
            raise


if __name__ == "__main__":
    asyncio.run(main())
