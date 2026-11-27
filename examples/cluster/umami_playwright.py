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

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind_cluster import Cluster

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


def main() -> None:
    pg_erofs = os.environ.get("REWIND_POSTGRES_EROFS")
    umami_erofs = os.environ.get("REWIND_UMAMI_EROFS")
    pw_erofs = os.environ.get("REWIND_PLAYWRIGHT_EROFS")

    t0 = time.perf_counter()
    with Cluster(
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

        pw_kwargs = (
            {"root": pw_erofs}
            if pw_erofs
            else {
                "packages": [
                    "nixpkgs#python3Packages.playwright",
                    "nixpkgs#playwright-driver.browsers-chromium",
                ]
            }
        )
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
            env=pw_env,
            files={"/build/runner.py": RUNNER_SCRIPT},
            **pw_kwargs,
        )

        c.start()
        try:
            db.wait_for("PG_READY", from_start=True, timeout_ms=60_000)
            app.wait_for("Ready in", from_start=True, timeout_ms=90_000)
            browser.wait_for("READY_LOGGED_IN", from_start=True, timeout_ms=90_000)
            # Let the runner enter its blocking read on /dev/rewind-console.
            c.advance(ms=100)
            t_boot = time.perf_counter()

            # Snapshot the entire 3-VM stack once while Chromium is already logged in!
            t_snap0 = time.perf_counter()
            snap = c.snapshot("logged-in")
            t_snap = time.perf_counter() - t_snap0
            print(
                f"[setup] booted Postgres + Umami (Next.js) + Chromium login in "
                f"{t_boot - t0:.2f}s; snapshot={t_snap:.2f}s",
                flush=True,
            )

            restore_times = []
            for spec in SPECS:
                t_r0 = time.perf_counter()
                c.restore(snap)
                t_restore = time.perf_counter() - t_r0
                restore_times.append(t_restore)

                t_test0 = time.perf_counter()
                browser.send_input(f"{spec}\n")

                if spec == "db_partition_and_recover":
                    browser.wait_for(
                        "READY_FOR_DB_FAULT", from_start=True, timeout_ms=30_000
                    )
                    c.partition(["db"], ["app", "browser"])
                    browser.send_input("PARTITIONED\n")
                    browser.wait_for(
                        "DB_FAULT_OBSERVED", from_start=True, timeout_ms=30_000
                    )
                    c.heal()
                    browser.send_input("HEALED\n")

                browser.wait_for(f"PASS:{spec}", from_start=True, timeout_ms=60_000)
                t_test = time.perf_counter() - t_test0
                print(
                    f"[test] {spec:26s} PASS  (restore={t_restore*1000:6.1f}ms, run={t_test:5.2f}s)",
                    flush=True,
                )

            exit_res = browser.wait_exit(timeout_ms=30_000)
            assert exit_res["exit_code"] == 0, f"browser exited with {exit_res}"
            run = c.finish(verdict=True)
            avg_restore = sum(restore_times) / len(restore_times) * 1000
            vtime_ms = exit_res["cluster_ns"] / 1e6
            print(
                f"recorded cluster {run['id']} ({run['verdict']}) at "
                f"{vtime_ms:.1f}ms virtual time "
                f"(avg restore={avg_restore:.1f}ms across {len(SPECS)} Playwright tests)",
                flush=True,
            )
        except Exception:
            for name, h in [("db", db), ("app", app), ("browser", browser)]:
                print(f"\n=== {name} output (tail) ===", file=sys.stderr)
                for line in h.logs(stream="output")[-30:]:
                    print(line, file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
