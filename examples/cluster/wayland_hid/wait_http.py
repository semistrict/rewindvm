#!/usr/bin/env python3
"""Polls an HTTP endpoint until it returns any non-5xx response."""

import sys
import time
import urllib.request


def main() -> None:
    url = sys.argv[1]
    timeout_s = float(sys.argv[2]) if len(sys.argv) > 2 else 120.0
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3.0) as resp:
                if 200 <= resp.status < 500:
                    return
        except Exception:
            pass
        time.sleep(0.25)
    raise SystemExit(f"timed out waiting for {url}")


if __name__ == "__main__":
    main()
