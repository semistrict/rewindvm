#!/usr/bin/env python3
"""In-guest Playwright test runner for the 3-VM PostgreSQL + Umami (Next.js) example.

Injected into the `browser` VM (`/build/runner.py`) by `umami_playwright.py`.

Lifecycle:
  1. Waits for Umami (`http://10.0.0.2:3000`) to come up.
  2. Launches headless Chromium, logs in as `admin` / `umami`, and waits for
     the authenticated dashboard.
  3. Prints `READY_LOGGED_IN` and blocks reading a test name from
     `/dev/rewind-console` (so the host can `c.snapshot("logged-in")` once and
     branch into many isolated Playwright specs via `c.restore` + `send_input`).
"""

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright

APP_URL = os.environ.get("UMAMI_URL", "http://10.0.0.2:3000")
CONSOLE_DEV = "/dev/rewind-console"


def start_wayland_compositor() -> tuple[list[subprocess.Popen], dict[str, str]]:
    """Starts seatd + labwc (wlroots/pixman) on /dev/dri/card0 when available.

    Returns `(background_procs, extra_env)` so Chromium can run headed as a
    native Wayland client (`--ozone-platform=wayland`), painting deterministically
    into the guest's `simpledrm` framebuffer for `rewind cluster video`.
    """
    if not os.path.exists("/dev/dri/card0"):
        return [], {}
    seatd_bin = shutil.which("seatd")
    labwc_bin = shutil.which("labwc")
    if not seatd_bin or not labwc_bin:
        return [], {}

    os.makedirs("/run", exist_ok=True)
    xdg_dir = "/tmp/xdg"
    os.makedirs(xdg_dir, mode=0o700, exist_ok=True)
    os.chmod(xdg_dir, 0o700)

    # Minimal labwc config: borderless maximized windows so Chromium fills 1280x720.
    cfg_dir = "/tmp/labwc-cfg"
    os.makedirs(cfg_dir, exist_ok=True)
    with open(f"{cfg_dir}/rc.xml", "w", encoding="utf-8") as f:
        f.write(
            '<?xml version="1.0"?>\n'
            "<labwc_config>\n"
            "  <windowRules>\n"
            '    <windowRule identifier="*">\n'
            '      <action name="Maximize" />\n'
            '      <action name="ToggleDecorations" />\n'
            "    </windowRule>\n"
            "  </windowRules>\n"
            "</labwc_config>\n"
        )

    procs: list[subprocess.Popen] = []
    seatd_sock = "/run/seatd.sock"
    if os.path.exists(seatd_sock):
        os.unlink(seatd_sock)
    seatd_log = open("/tmp/seatd.log", "wb")
    procs.append(
        subprocess.Popen(
            [seatd_bin, "-l", "error"],
            stdout=seatd_log,
            stderr=seatd_log,
        )
    )
    deadline = time.monotonic() + 10.0
    while not os.path.exists(seatd_sock) and time.monotonic() < deadline:
        time.sleep(0.02)
    if not os.path.exists(seatd_sock):
        raise RuntimeError("seatd did not create /run/seatd.sock")

    wl_env = os.environ.copy()
    wl_env.update(
        {
            "XDG_RUNTIME_DIR": xdg_dir,
            "LIBSEAT_BACKEND": "seatd",
            "SEATD_SOCK": seatd_sock,
            "WLR_BACKENDS": "drm",
            "WLR_RENDERER": "pixman",
            "WLR_LIBINPUT_NO_DEVICES": "1",
            "WLR_DRM_DEVICES": "/dev/dri/card0",
            "WLR_NO_HARDWARE_CURSORS": "1",
        }
    )
    labwc_log = open("/tmp/labwc.log", "wb")
    procs.append(
        subprocess.Popen(
            [labwc_bin, "-C", cfg_dir],
            env=wl_env,
            stdout=labwc_log,
            stderr=labwc_log,
        )
    )

    wl_sock = None
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        for entry in sorted(os.listdir(xdg_dir)):
            if entry.startswith("wayland-") and not entry.endswith(".lock"):
                wl_sock = entry
                break
        if wl_sock:
            break
        if procs[-1].poll() is not None:
            labwc_log.flush()
            with open("/tmp/labwc.log", "r", errors="replace") as lf:
                raise RuntimeError(f"labwc exited early: {lf.read()}")
        time.sleep(0.05)
    if not wl_sock:
        raise RuntimeError("timed out waiting for labwc Wayland socket")

    print(f"WAYLAND_READY:{wl_sock}", flush=True)
    return procs, {
        "XDG_RUNTIME_DIR": xdg_dir,
        "WAYLAND_DISPLAY": wl_sock,
    }


def wait_for_http(url: str, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_err = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3.0) as resp:
                if 200 <= resp.status < 500:
                    return
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {url}: {last_err}")


def rewind_signal(name: str, payload: str = "") -> None:
    req = urllib.request.Request(
        f"http://rewind/signal/{name}",
        data=payload.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    with urllib.request.urlopen(req, timeout=30.0):
        pass


def rewind_barrier(name: str) -> str:
    with urllib.request.urlopen(f"http://rewind/barrier/{name}", timeout=120.0) as resp:
        return resp.read().decode("utf-8", errors="replace")


def api_request(page, method: str, path: str, token: str, payload=None):
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    fn = getattr(page.request, method.lower())
    kwargs = {"headers": headers, "timeout": 15_000}
    if payload is not None:
        kwargs["data"] = json.dumps(payload)
    return fn(f"{APP_URL}{path}", **kwargs)


def list_websites(page, token: str) -> list[dict]:
    resp = api_request(page, "get", "/api/websites", token)
    assert resp.ok, f"GET /api/websites failed: {resp.status} {resp.text()}"
    body = resp.json()
    return body["data"] if isinstance(body, dict) and "data" in body else body


def create_website_via_ui(page, name: str, domain: str) -> None:
    page.goto(f"{APP_URL}/websites", wait_until="networkidle")
    add_btn = page.get_by_role("button", name="Add website").first
    add_btn.wait_for(state="visible", timeout=20_000)
    add_btn.click()

    # Fill Name and Domain inputs inside the modal dialog.
    dialog = page.get_by_role("dialog").first
    dialog.wait_for(state="visible", timeout=10_000)
    name_input = dialog.locator('input[name="name"]').first
    domain_input = dialog.locator('input[name="domain"]').first
    name_input.fill(name)
    domain_input.fill(domain)
    dialog.get_by_role("button", name="Save").first.click()
    dialog.wait_for(state="hidden", timeout=15_000)


def test_create_website(page, token: str) -> None:
    create_website_via_ui(page, "Rewind Demo", "rewind.local")
    # Reload from scratch to verify persistence in PostgreSQL (db VM).
    page.reload(wait_until="networkidle")
    page.get_by_text("Rewind Demo").first.wait_for(state="visible", timeout=15_000)
    sites = list_websites(page, token)
    names = [s["name"] for s in sites]
    assert "Rewind Demo" in names, f"expected Rewind Demo in Postgres websites: {names}"


def test_track_pageview(page, context, token: str) -> None:
    create_website_via_ui(page, "Tracked App", "tracked.local")
    sites = list_websites(page, token)
    site = next((s for s in sites if s["name"] == "Tracked App"), None)
    assert site is not None, f"Tracked App not found in {sites}"
    site_id = site["id"]

    # Fire 2 real Umami pageview collection requests from a visitor browser context.
    for path in ["/", "/pricing"]:
        res = context.request.post(
            f"{APP_URL}/api/send",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 Chrome/158.0.0.0 Safari/537.36",
            },
            data=json.dumps(
                {
                    "type": "event",
                    "payload": {
                        "website": site_id,
                        "hostname": "tracked.local",
                        "language": "en-US",
                        "referrer": "",
                        "screen": "1280x720",
                        "title": f"Page {path}",
                        "url": path,
                    },
                }
            ),
            timeout=15_000,
        )
        assert res.ok, f"POST /api/send failed: {res.status} {res.text()}"

    # Verify Umami persisted the pageviews in Postgres and serves them in stats API + UI.
    now_ms = int(time.time() * 1000)
    stats_resp = api_request(
        page,
        "get",
        f"/api/websites/{site_id}/stats?startAt={now_ms - 3600_000}&endAt={now_ms + 3600_000}",
        token,
    )
    assert stats_resp.ok, f"stats API failed: {stats_resp.status} {stats_resp.text()}"
    stats = stats_resp.json()
    pageviews = stats.get("pageviews", 0)
    if isinstance(pageviews, dict):
        pageviews = pageviews.get("value", 0)
    assert pageviews >= 2, f"expected >=2 pageviews in Umami stats, got {stats}"

    # Open the website analytics page in Chromium and confirm it renders cleanly.
    page.goto(f"{APP_URL}/websites/{site_id}", wait_until="networkidle")
    page.get_by_text("Tracked App").first.wait_for(state="visible", timeout=15_000)


def test_update_and_delete_website(page, token: str) -> None:
    create_website_via_ui(page, "Temp Site", "temp.local")
    sites = list_websites(page, token)
    site = next((s for s in sites if s["name"] == "Temp Site"), None)
    assert site is not None
    site_id = site["id"]

    # Update name via Umami API and verify UI reflects Postgres update after reload.
    upd = api_request(
        page,
        "post",
        f"/api/websites/{site_id}",
        token,
        {"name": "Renamed Site", "domain": "renamed.local"},
    )
    assert upd.ok, f"update failed: {upd.status} {upd.text()}"
    page.goto(f"{APP_URL}/websites", wait_until="networkidle")
    page.get_by_text("Renamed Site").first.wait_for(state="visible", timeout=15_000)

    # Delete website and confirm clean removal.
    dl = api_request(page, "delete", f"/api/websites/{site_id}", token)
    assert dl.ok, f"delete failed: {dl.status} {dl.text()}"
    remaining = [s["name"] for s in list_websites(page, token)]
    assert "Renamed Site" not in remaining, f"site still present after delete: {remaining}"


def test_db_partition_and_recover(page, token: str) -> None:
    # Park at `db_partition` barrier so the host partitions app <-> db cleanly.
    rewind_barrier("db_partition")

    # With app partitioned from PostgreSQL, writes MUST fail (never silently succeed).
    failed = False
    try:
        resp = page.request.post(
            f"{APP_URL}/api/websites",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data=json.dumps({"name": "Should Fail", "domain": "fail.local"}),
            timeout=6_000,
        )
        if not resp.ok:
            failed = True
    except Exception:
        failed = True
    assert failed, "write succeeded even though app <-> db was partitioned!"

    # Park at `db_heal` barrier until the host heals app <-> db.
    rewind_barrier("db_heal")

    # Retry until Postgres connection pool reconnects after heal.
    last_err = None
    for _ in range(10):
        try:
            create_website_via_ui(page, "Post-Heal Site", "healed.local")
            last_err = None
            break
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    if last_err is not None:
        raise RuntimeError(f"failed to recover after db heal: {last_err}")

    page.reload(wait_until="networkidle")
    page.get_by_text("Post-Heal Site").first.wait_for(state="visible", timeout=15_000)


def test_clean_isolation(page, token: str) -> None:
    # Verify ZERO state leaked from any prior Playwright branch!
    sites = list_websites(page, token)
    assert len(sites) == 0, f"STATE LEAKED ACROSS RESTORE: expected 0 websites, got {sites}"
    page.goto(f"{APP_URL}/websites", wait_until="networkidle")
    assert page.get_by_text("Rewind Demo").count() == 0
    assert page.get_by_text("Tracked App").count() == 0
    assert page.get_by_text("Post-Heal Site").count() == 0


def main() -> None:
    print("RUNNER_WAITING_FOR_APP", flush=True)
    wait_for_http(f"{APP_URL}/api/heartbeat", timeout_s=120.0)
    print("APP_REACHABLE", flush=True)

    wl_procs, wl_env = start_wayland_compositor()
    if wl_env:
        os.environ.update(wl_env)

    chromium_args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
    ]
    if wl_env:
        chromium_args.extend(
            [
                "--ozone-platform=wayland",
                "--window-size=1280,720",
                "--start-maximized",
            ]
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chromium",
            headless=not bool(wl_env),
            args=chromium_args,
        )
        context = browser.new_context(viewport={"width": 1280, "height": 720})
        page = context.new_page()

        # Perform real UI login once before the cluster snapshot.
        page.goto(f"{APP_URL}/login", wait_until="networkidle", timeout=30_000)
        page.locator('input[name="username"]').fill("admin")
        page.locator('input[name="password"]').fill("umami")
        with page.expect_response(
            lambda r: "/api/auth/login" in r.url and r.request.method == "POST",
            timeout=20_000,
        ) as login_resp_info:
            page.get_by_role("button", name="Login").first.click()

        login_resp = login_resp_info.value
        assert login_resp.ok, f"UI login failed: {login_resp.status} {login_resp.text()}"
        token = login_resp.json()["token"]

        # Wait until authenticated app finishes loading, then complete Umami 3's
        # one-time "Change password" onboarding modal (if shown) BEFORE snapshotting
        # so every branched Playwright test starts cleanly on /websites!
        page.wait_for_url(lambda u: "/login" not in u, timeout=20_000)
        page.wait_for_load_state("networkidle", timeout=20_000)
        page.goto(f"{APP_URL}/websites", wait_until="networkidle", timeout=20_000)

        change_pw_heading = page.get_by_text("Change password").first
        try:
            change_pw_heading.wait_for(state="visible", timeout=3_000)
            dialog = page.get_by_role("dialog").first
            inputs = dialog.locator('input[type="password"]')
            inputs.nth(0).fill("umami")
            inputs.nth(1).fill("rewind-secure-pass")
            inputs.nth(2).fill("rewind-secure-pass")
            dialog.get_by_role("button", name="Save").first.click()
            dialog.wait_for(state="hidden", timeout=15_000)
        except Exception:
            pass

        page.get_by_role("button", name="Add website").first.wait_for(
            state="visible", timeout=15_000
        )
        # Re-read the active bearer token from localStorage in case password
        # change rotated it (Umami stores a JSON-encoded string in `umami.auth`).
        stored_auth = page.evaluate("window.localStorage.getItem('umami.auth')")
        if stored_auth:
            try:
                parsed = json.loads(stored_auth)
                if isinstance(parsed, str) and parsed:
                    token = parsed
            except Exception:
                token = stored_auth
        # Park on `http://rewind/barrier/dispatch` so the host snapshots all 3 VMs
        # right here (PostgreSQL migrated, Next.js warm, Chromium already logged in)
        # and dispatches each isolated test spec via `browser.release("dispatch", spec)`.
        spec = rewind_barrier("dispatch")
        print(f"RUNNING:{spec}", flush=True)
        if spec == "create_website":
            test_create_website(page, token)
        elif spec == "track_pageview":
            test_track_pageview(page, context, token)
        elif spec == "update_and_delete_website":
            test_update_and_delete_website(page, token)
        elif spec == "db_partition_and_recover":
            test_db_partition_and_recover(page, token)
        elif spec == "clean_isolation":
            test_clean_isolation(page, token)
        else:
            raise ValueError(f"unknown Playwright test spec: {spec!r}")
        rewind_signal("pass", spec)

        browser.close()
    for proc in reversed(wl_procs):
        proc.terminate()


if __name__ == "__main__":
    main()
