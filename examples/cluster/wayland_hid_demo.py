#!/usr/bin/env python3
"""Zero-Playwright, zero-CDP native Wayland Chromium demo driven purely by
Rewind's deterministic paravirtual HID devices (`/dev/input/event0` pointer
+ `/dev/input/event1` keyboard).

Boots a full 3-VM web stack on Apple Silicon HVF or Linux KVM:
  - `db`      (10.0.0.1): PostgreSQL 18
  - `app`     (10.0.0.2): Umami 3.5.0 (production Next.js + Prisma server)
  - `desktop` (10.0.0.3): `seatd` + `labwc` (`WLR_BACKENDS=drm,libinput`,
                          `WLR_RENDERER=pixman`) + plain raw Chromium
                          `--ozone-platform=wayland --incognito --kiosk`
                          (NO Playwright, NO CDP, NO remote debugging port!)

Guest scripts live in `examples/cluster/wayland_hid/` and are injected into
`/build/` inside each VM.

Every mouse move, click, and keystroke is recorded as a deterministic cluster
fault so `rewind cluster replay` reproduces the exact pixel-for-pixel execution
and `rewind cluster video --show-input --timestamp` renders a screencast with
cursor, click ripples, and keycast HUD overlaid retroactively.
"""

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind import Cluster

GUEST_DIR = Path(__file__).with_name("wayland_hid")


def read_guest_file(name: str) -> str:
    return (GUEST_DIR / name).read_text(encoding="utf-8")


async def main() -> None:
    pg_erofs = os.environ.get("REWIND_POSTGRES_EROFS")
    umami_erofs = os.environ.get("REWIND_UMAMI_EROFS")
    pw_erofs = os.environ.get("REWIND_PLAYWRIGHT_EROFS")

    t0 = time.perf_counter()
    async with Cluster(
        "umami-wayland-hid-3vm",
        latency_ms=1,
        jitter_us=100,
        mem_mib=512,
        stop_when="driver",
        max_virtual_ms=180_000,
        slice_us=250,
    ) as c:
        db_kwargs = (
            {"root": pg_erofs} if pg_erofs else {"packages": ["nixpkgs#postgresql"]}
        )
        db = c.add_node(
            "db",
            ["/bin/sh", "/build/db_start.sh"],
            ip="10.0.0.1",
            role="service",
            uid=70,
            gid=70,
            mem_mib=512,
            files={"/build/db_start.sh": read_guest_file("db_start.sh")},
            **db_kwargs,
        )

        app_kwargs = (
            {"root": umami_erofs} if umami_erofs else {"packages": ["nixpkgs#umami"]}
        )
        app = c.add_node(
            "app",
            ["/bin/sh", "/build/app_start.sh"],
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
            files={"/build/app_start.sh": read_guest_file("app_start.sh")},
            **app_kwargs,
        )

        desktop_pkgs = [
            "nixpkgs#playwright-driver.browsers-chromium",
            "nixpkgs#labwc",
            "nixpkgs#seatd",
            "nixpkgs#dejavu_fonts",
            "nixpkgs#fontconfig",
            "nixpkgs#python3",
        ]
        desktop_kwargs = {"root": pw_erofs} if pw_erofs else {"packages": desktop_pkgs}
        desktop = c.add_node(
            "desktop",
            ["/bin/sh", "/build/desktop_start.sh"],
            ip="10.0.0.3",
            role="driver",
            mem_mib=1536,
            display=True,
            display_width=1280,
            display_height=720,
            files={
                "/build/desktop_start.sh": read_guest_file("desktop_start.sh"),
                "/build/labwc-rc.xml": read_guest_file("labwc-rc.xml"),
                "/build/wait_http.py": read_guest_file("wait_http.py"),
            },
            **desktop_kwargs,
        )

        await c.start()
        try:
            await db.wait_for("PG_READY", from_start=True, timeout_ms=60_000)
            await app.wait_for("Ready in", from_start=True, timeout_ms=90_000)
            await desktop.wait_for("CHROME_LAUNCHED", from_start=True, timeout_ms=60_000)

            # Give Chromium virtual time to paint the Umami /login page.
            t_launch = await c.advance(ms=2500)
            print(
                f"[desktop] Chromium painted /login at {t_launch['cluster_ns']/1e6:.1f}ms VT",
                flush=True,
            )

            async def glide_mouse(
                x0: int, y0: int, x1: int, y1: int, steps: int = 10, step_ms: int = 20
            ) -> None:
                for step in range(1, steps + 1):
                    frac = step / float(steps)
                    eased = frac * frac * (3.0 - 2.0 * frac)
                    gx = int(round(x0 + (x1 - x0) * eased))
                    gy = int(round(y0 + (y1 - y0) * eased))
                    await desktop.mouse_move(gx, gy)
                    await c.advance(ms=step_ms)

            # 1. Glide to Username input (640, 250), click, and type "admin".
            ux, uy = 640, 250
            px, py = 640, 325
            bx, by = 640, 382
            await glide_mouse(180, 140, ux, uy, steps=12, step_ms=20)
            await c.advance(ms=80)
            await desktop.click(ux, uy, move_ms=20, hold_ms=45)
            await c.advance(ms=120)
            await desktop.type_text("admin", delay_ms=45)
            await c.advance(ms=180)

            # 2. Glide to Password input (640, 325), click, and type "umami".
            await glide_mouse(ux, uy, px, py, steps=8, step_ms=20)
            await c.advance(ms=80)
            await desktop.click(px, py, move_ms=20, hold_ms=45)
            await c.advance(ms=120)
            await desktop.type_text("umami", delay_ms=45)
            await c.advance(ms=200)

            # 3. Glide to the blue Login button (640, 382) and click!
            await glide_mouse(px, py, bx, by, steps=8, step_ms=20)
            await c.advance(ms=80)
            await desktop.click(bx, by, move_ms=20, hold_ms=50)

            # Let Umami authenticate against PostgreSQL and render the dashboard!
            t_after = await c.advance(ms=3500)
            print(
                f"[desktop] HID login sequence finished at {t_after['cluster_ns']/1e6:.1f}ms VT",
                flush=True,
            )

            await desktop.send_input("DONE\n")
            await desktop.wait_for("HID_DEMO_DONE", from_start=True, timeout_ms=15_000)
            exit_res = await desktop.wait_exit(timeout_ms=15_000)
            assert exit_res["exit_code"] == 0, f"desktop exited with {exit_res}"
            run = await c.finish(verdict="passed")
            print(
                f"recorded HID cluster {run['id']} ({run['verdict']}) in "
                f"{time.perf_counter() - t0:.2f}s wall ({exit_res['cluster_ns']/1e6:.1f}ms VT)",
                flush=True,
            )
        except Exception:
            if c._started:
                for name, h in [("db", db), ("app", app), ("desktop", desktop)]:
                    print(f"\n=== {name} output (tail) ===", file=sys.stderr)
                    for line in (await h.logs(stream="output"))[-30:]:
                        print(line, file=sys.stderr)
            raise


if __name__ == "__main__":
    asyncio.run(main())
