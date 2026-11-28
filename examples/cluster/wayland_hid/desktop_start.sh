#!/bin/sh
set -eu

# 1. Advertise Rewind's paravirtual evdev nodes to libinput (zero udevd daemon!).
mkdir -p /run/udev/data /tmp/xdg /tmp/labwc-cfg /tmp/chrome-profile
chmod 700 /tmp/xdg
cp /build/labwc-rc.xml /tmp/labwc-cfg/rc.xml
printf 'E:ID_INPUT=1\nE:ID_INPUT_MOUSE=1\nE:ID_SEAT=seat0\n' > /run/udev/data/c13:64
printf 'E:ID_INPUT=1\nE:ID_INPUT_KEYBOARD=1\nE:ID_SEAT=seat0\n' > /run/udev/data/c13:65

# 2. Wait until Umami HTTP server is answering.
python3 /build/wait_http.py "http://10.0.0.2:3000/api/heartbeat" 120
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

grep -E "Adding Rewind Paravirtual|libinput" /tmp/labwc.log || true

kill "$CHROME_PID" "$LABWC_PID" "$SEATD_PID" 2>/dev/null || true
echo HID_DEMO_DONE
