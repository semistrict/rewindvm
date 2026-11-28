#!/usr/bin/env python3
"""Zero-Playwright, zero-CDP native Wayland Chromium demo driven purely by
Rewind's deterministic paravirtual HID devices (`/dev/input/event0` pointer
+ `/dev/input/event1` keyboard).

Boots a full 3-VM web stack on Apple Silicon HVF or Linux KVM:
  - `db`      (10.0.0.1): PostgreSQL 18
  - `app`     (10.0.0.2): Umami 3.5.0 (production Next.js + Prisma server)
  - `desktop` (10.0.0.3): `seatd` + `labwc` (`WLR_BACKENDS=drm,libinput`,
                          `WLR_RENDERER=pixman`) + plain raw Chromium
                          `--ozone-platform=wayland --kiosk` (NO Playwright,
                          NO CDP, NO remote debugging port!)

The host drives Chromium purely via hardware input events:
  `desktop.click(x, y)` -> `desktop.type_text("admin")` -> `desktop.key("tab")`
  -> `desktop.type_text("umami")` -> `desktop.key("enter")`

Every mouse move, click, and keystroke is recorded as a deterministic cluster
fault so `rewind cluster replay` reproduces the exact pixel-for-pixel execution
and `rewind cluster video --show-input --timestamp` renders a screencast with
cursor, click ripples, and keycast HUD overlaid retroactively.
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from rewind_cluster import Cluster

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

DESKTOP_START_CMD = """\
set -eu

# 1. Advertise Rewind's paravirtual evdev nodes to libinput (zero udevd daemon!).
mkdir -p /run/udev/data /tmp/xdg /tmp/labwc-cfg /tmp/chrome-profile
chmod 700 /tmp/xdg
printf 'E:ID_INPUT=1\nE:ID_INPUT_MOUSE=1\nE:ID_SEAT=seat0\n' > /run/udev/data/c13:64
printf 'E:ID_INPUT=1\nE:ID_INPUT_KEYBOARD=1\nE:ID_SEAT=seat0\n' > /run/udev/data/c13:65

cat > /tmp/labwc-cfg/rc.xml <<'EOF'
<?xml version="1.0"?>
<labwc_config>
  <windowRules>
    <windowRule identifier="*">
      <action name="Maximize" />
      <action name="ToggleDecorations" />
    </windowRule>
  </windowRules>
</labwc_config>
EOF

# 2. Wait until Umami HTTP server is answering.
python3 - <<'PY'
import time, urllib.request
deadline = time.monotonic() + 120.0
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen("http://10.0.0.2:3000/api/heartbeat", timeout=3.0) as r:
            if 200 <= r.status < 500:
                break
    except Exception:
        pass
    time.sleep(0.25)
else:
    raise SystemExit("timed out waiting for Umami")
PY
echo APP_REACHABLE

# 3. Start seatd + labwc on /dev/dri/card0 (simpledrm) + /dev/input/event{0,1}.
rm -f /run/seatd.sock
seatd -l error >/tmp/seatd.log 2>&1 &
SEATD_PID=$!
i=0
while [ ! -S /run/seatd.sock ] && [ "$i" -lt 200 ]; do
  sleep 0.02
  i=$((i + 1))
done

export XDG_RUNTIME_DIR=/tmp/xdg
export LIBSEAT_BACKEND=seatd
export SEATD_SOCK=/run/seatd.sock
export WLR_BACKENDS=drm,libinput
export WLR_RENDERER=pixman
export WLR_DRM_DEVICES=/dev/dri/card0
export WLR_NO_HARDWARE_CURSORS=1
export HOME=/tmp
export XDG_CACHE_HOME=/tmp/cache
export XDG_CONFIG_HOME=/tmp/config

labwc -C /tmp/labwc-cfg -d >/tmp/labwc.log 2>&1 &
LABWC_PID=$!

WL_SOCK=""
i=0
while [ "$i" -lt 300 ]; do
  for p in /tmp/xdg/wayland-*; do
    case "$p" in
      *.lock|*"*") continue ;;
      *) WL_SOCK=$(basename "$p"); break ;;
    esac
  done
  if [ -n "$WL_SOCK" ]; then
    break
  fi
  sleep 0.05
  i=$((i + 1))
done
if [ -z "$WL_SOCK" ]; then
  cat /tmp/labwc.log >&2 || true
  exit 1
fi
export WAYLAND_DISPLAY="$WL_SOCK"
echo "WAYLAND_READY:$WL_SOCK"

# 4. Find plain Chromium binary (NO Playwright, NO CDP!).
CHROME=""
for cand in \
  /opt/pw-browsers/chromium-*/chrome-linux/chrome \
  /opt/pw-browsers/chromium-*/chrome-linux64/chrome \
  /nix/store/*-playwright-browsers*/chromium-*/chrome-linux*/chrome \
  $(command -v chromium 2>/dev/null || true) \
  $(command -v google-chrome 2>/dev/null || true); do
  if [ -n "$cand" ] && [ -x "$cand" ]; then
    CHROME="$cand"
    break
  fi
done
if [ -z "$CHROME" ]; then
  echo "could not find Chromium binary" >&2
  exit 1
fi
echo "CHROME_BIN:$CHROME"

"$CHROME" \
  --ozone-platform=wayland \
  --no-sandbox \
  --disable-setuid-sandbox \
  --disable-dev-shm-usage \
  --disable-gpu \
  --no-first-run \
  --no-default-browser-check \
  --disable-infobars \
  --incognito \
  --kiosk \
  --window-size=1280,720 \
  --user-data-dir=/tmp/chrome-profile \
  "http://10.0.0.2:3000/login" >/tmp/chrome.log 2>&1 &
CHROME_PID=$!
echo CHROME_LAUNCHED

# Block on /dev/rewind-console until the host finishes sending HID input.
read -r CMD < /dev/rewind-console || true
echo "DESKTOP_CMD:$CMD"

# Check labwc saw both Rewind paravirtual HID devices.
grep -E "Adding Rewind Paravirtual|libinput" /tmp/labwc.log || true

kill "$CHROME_PID" "$LABWC_PID" "$SEATD_PID" 2>/dev/null || true
echo HID_DEMO_DONE
"""


def main() -> None:
    pg_erofs = os.environ.get("REWIND_POSTGRES_EROFS")
    umami_erofs = os.environ.get("REWIND_UMAMI_EROFS")
    pw_erofs = os.environ.get("REWIND_PLAYWRIGHT_EROFS")

    t0 = time.perf_counter()
    with Cluster(
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
            ["/bin/sh", "-c", DESKTOP_START_CMD],
            ip="10.0.0.3",
            role="driver",
            mem_mib=1536,
            display=True,
            display_width=1280,
            display_height=720,
            **desktop_kwargs,
        )

        c.start()
        try:
            db.wait_for("PG_READY", from_start=True, timeout_ms=60_000)
            app.wait_for("Ready in", from_start=True, timeout_ms=90_000)
            desktop.wait_for("CHROME_LAUNCHED", from_start=True, timeout_ms=60_000)

            # Give Chromium virtual time to paint the Umami /login page.
            t_launch = c.advance(ms=2500)
            print(
                f"[desktop] Chromium painted /login at {t_launch['cluster_ns']/1e6:.1f}ms VT",
                flush=True,
            )

            def glide_mouse(x0: int, y0: int, x1: int, y1: int, steps: int = 10, step_ms: int = 20) -> None:
                for step in range(1, steps + 1):
                    frac = step / float(steps)
                    eased = frac * frac * (3.0 - 2.0 * frac)
                    gx = int(round(x0 + (x1 - x0) * eased))
                    gy = int(round(y0 + (y1 - y0) * eased))
                    desktop.mouse_move(gx, gy)
                    c.advance(ms=step_ms)

            # 1. Glide to Username input (640, 250), click, and type "admin".
            ux, uy = 640, 250
            px, py = 640, 325
            bx, by = 640, 382
            glide_mouse(180, 140, ux, uy, steps=12, step_ms=20)
            c.advance(ms=80)
            desktop.click(ux, uy, move_ms=20, hold_ms=45)
            c.advance(ms=120)
            desktop.type_text("admin", delay_ms=45)
            c.advance(ms=180)

            # 2. Glide to Password input (640, 325), click, and type "umami".
            glide_mouse(ux, uy, px, py, steps=8, step_ms=20)
            c.advance(ms=80)
            desktop.click(px, py, move_ms=20, hold_ms=45)
            c.advance(ms=120)
            desktop.type_text("umami", delay_ms=45)
            c.advance(ms=200)

            # 3. Glide to the blue Login button (640, 382) and click!
            glide_mouse(px, py, bx, by, steps=8, step_ms=20)
            c.advance(ms=80)
            desktop.click(bx, by, move_ms=20, hold_ms=50)

            # Let Umami authenticate against PostgreSQL and render the dashboard!
            t_after = c.advance(ms=3500)
            print(
                f"[desktop] HID login sequence finished at {t_after['cluster_ns']/1e6:.1f}ms VT",
                flush=True,
            )

            desktop.send_input("DONE\n")
            desktop.wait_for("HID_DEMO_DONE", from_start=True, timeout_ms=15_000)
            exit_res = desktop.wait_exit(timeout_ms=15_000)
            assert exit_res["exit_code"] == 0, f"desktop exited with {exit_res}"
            run = c.finish(verdict="passed")
            print(
                f"recorded HID cluster {run['id']} ({run['verdict']}) in "
                f"{time.perf_counter() - t0:.2f}s wall ({exit_res['cluster_ns']/1e6:.1f}ms VT)",
                flush=True,
            )
        except Exception:
            if c._started:
                for name, h in [("db", db), ("app", app), ("desktop", desktop)]:
                    print(f"\n=== {name} output (tail) ===", file=sys.stderr)
                    for line in h.logs(stream="output")[-30:]:
                        print(line, file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
