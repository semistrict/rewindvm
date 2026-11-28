"""Async guest-side client for the ``http://rewind`` virtual coordinator endpoint.

Intended to be imported from *inside* a guest VM running in a Rewind cluster::

    import asyncio
    from rewind.guest import barrier, meta, signal

    async def main() -> None:
        info = await meta()
        await signal("ready", {"node": info["node"], "ip": info["ip"]})
        task = await barrier("dispatch")
        await signal("done", {"cmd": task["cmd"]})

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote, urlsplit

from .types import ClusterError

DEFAULT_CONTROL_URL = "http://rewind"


def _encode_payload(payload: Any) -> Tuple[bytes, str]:
    if payload is None:
        return b"", "text/plain; charset=utf-8"
    if isinstance(payload, bytes):
        return payload, "application/octet-stream"
    if isinstance(payload, str):
        return payload.encode("utf-8"), "text/plain; charset=utf-8"
    return json.dumps(payload).encode("utf-8"), "application/json"


def _decode_response_body(content_type: str, body: bytes) -> Any:
    ctype = content_type.split(";", 1)[0].strip().lower()
    if ctype == "application/x-rewind-empty" and not body:
        return None
    if ctype == "application/json":
        return json.loads(body.decode("utf-8"))
    if ctype == "application/octet-stream":
        return body
    return body.decode("utf-8", errors="replace")


async def _http_request(
    base_url: str,
    method: str,
    path: str,
    *,
    body: bytes = b"",
    content_type: str = "text/plain; charset=utf-8",
    timeout: Optional[float] = 30.0,
) -> Tuple[int, Dict[str, str], bytes]:
    parsed = urlsplit(base_url)
    host = parsed.hostname or "rewind"
    port = parsed.port or 80
    prefix = parsed.path.rstrip("/")
    target = f"{prefix}{path}"

    async def _exchange() -> Tuple[int, Dict[str, str], bytes]:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            headers = [
                f"{method} {target} HTTP/1.1",
                f"Host: {host}",
                "Connection: close",
            ]
            if body:
                headers.append(f"Content-Type: {content_type}")
                headers.append(f"Content-Length: {len(body)}")
            raw_req = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + body
            writer.write(raw_req)
            await writer.drain()

            raw_resp = await reader.read()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        sep = raw_resp.find(b"\r\n\r\n")
        if sep < 0:
            raise ClusterError(
                f"malformed HTTP response from {base_url}{path}: {raw_resp!r}"
            )
        head = raw_resp[:sep].decode("iso-8859-1")
        resp_body = raw_resp[sep + 4 :]
        lines = head.split("\r\n")
        parts = lines[0].split(" ", 2)
        if len(parts) < 2:
            raise ClusterError(f"invalid HTTP status line: {lines[0]!r}")
        status = int(parts[1])
        resp_headers: Dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                resp_headers[k.strip().lower()] = v.strip()
        return status, resp_headers, resp_body

    if timeout is None:
        return await _exchange()
    return await asyncio.wait_for(_exchange(), timeout=timeout)


class GuestClient:
    """Async guest-side client talking HTTP/1.1 to ``http://rewind``."""

    def __init__(self, base_url: str = DEFAULT_CONTROL_URL) -> None:
        self.base_url = base_url.rstrip("/")

    async def meta(self, *, timeout: float = 30.0) -> Dict[str, Any]:
        """Fetches this node's identity and cluster peer list from ``GET /meta``."""
        status, _, body = await _http_request(
            self.base_url, "GET", "/meta", timeout=timeout
        )
        if status != 200:
            raise ClusterError(
                f"GET /meta failed ({status}): {body.decode('utf-8', errors='replace')}"
            )
        return json.loads(body.decode("utf-8"))

    async def signal(
        self,
        name: str,
        payload: Any = None,
        *,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        """Emits a one-way signal via ``POST /signal/<name>``."""
        data, ctype = _encode_payload(payload)
        encoded = quote(name, safe="")
        status, _, body = await _http_request(
            self.base_url,
            "POST",
            f"/signal/{encoded}",
            body=data,
            content_type=ctype,
            timeout=timeout,
        )
        if status != 200:
            raise ClusterError(
                f"POST /signal/{name} failed ({status}): "
                f"{body.decode('utf-8', errors='replace')}"
            )
        return json.loads(body.decode("utf-8"))

    async def barrier(
        self,
        name: str,
        payload: Any = None,
        *,
        timeout: Optional[float] = None,
    ) -> Any:
        """Blocks on ``/barrier/<name>`` until the host calls ``release(name, body)``.

        Decodes the host's response automatically based on ``Content-Type``
        (``application/json`` -> parsed JSON, ``application/octet-stream`` ->
        ``bytes``, empty release -> ``None``, otherwise UTF-8 ``str``).
        """
        data, ctype = _encode_payload(payload)
        method = "POST" if data else "GET"
        encoded = quote(name, safe="")
        status, headers, body = await _http_request(
            self.base_url,
            method,
            f"/barrier/{encoded}",
            body=data,
            content_type=ctype,
            timeout=timeout,
        )
        if not (200 <= status < 300):
            raise ClusterError(
                f"{method} /barrier/{name} failed ({status}): "
                f"{body.decode('utf-8', errors='replace')}"
            )
        return _decode_response_body(headers.get("content-type", ""), body)


async def meta(
    *, base_url: str = DEFAULT_CONTROL_URL, timeout: float = 30.0
) -> Dict[str, Any]:
    """Fetches this guest node's identity and peer list from ``http://rewind/meta``."""
    return await GuestClient(base_url).meta(timeout=timeout)


async def signal(
    name: str,
    payload: Any = None,
    *,
    base_url: str = DEFAULT_CONTROL_URL,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """Emits a one-way signal from inside a guest via ``http://rewind/signal/<name>``."""
    return await GuestClient(base_url).signal(name, payload, timeout=timeout)


async def barrier(
    name: str,
    payload: Any = None,
    *,
    base_url: str = DEFAULT_CONTROL_URL,
    timeout: Optional[float] = None,
) -> Any:
    """Blocks inside a guest on ``http://rewind/barrier/<name>`` until host release."""
    return await GuestClient(base_url).barrier(name, payload, timeout=timeout)
