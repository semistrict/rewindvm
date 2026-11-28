"""Async NDJSON-RPC 2.0 transport to ``rewind cluster serve``."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from .types import ClusterError

# Generous asyncio StreamReader line limit so large base64 `cat`/`logs` RPC
# replies never trip asyncio's default 64 KiB LimitOverrunError.
_STREAM_LIMIT = 64 * 1024 * 1024


class AsyncRpcClient:
    """Manages an async connection to ``rewind cluster serve`` via stdio or Unix socket."""

    def __init__(
        self,
        *,
        rewind_bin: str,
        socket_path: Optional[str] = None,
        clock: Optional[str] = None,
        cpu: Optional[str] = None,
    ) -> None:
        self._rewind_bin = rewind_bin
        self._socket_path = socket_path
        self._clock = clock
        self._cpu = cpu
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock: Optional[asyncio.Lock] = None
        self._req_id = 0

    @property
    def connected(self) -> bool:
        return self._writer is not None

    async def connect(self) -> None:
        if self._writer is not None:
            return
        if self._socket_path:
            self._reader, self._writer = await asyncio.open_unix_connection(
                self._socket_path, limit=_STREAM_LIMIT
            )
            return
        cmd = [self._rewind_bin, "cluster", "serve"]
        if self._clock:
            cmd.extend(["--clock", self._clock])
        if self._cpu:
            cmd.extend(["--cpu", self._cpu])
        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,
            limit=_STREAM_LIMIT,
        )
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._writer = self._proc.stdin
        self._reader = self._proc.stdout

    async def call(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            await self.connect()
            assert self._writer is not None and self._reader is not None
            self._req_id += 1
            payload = {
                "jsonrpc": "2.0",
                "id": self._req_id,
                "method": method,
                "params": params or {},
            }
            line = (json.dumps(payload) + "\n").encode("utf-8")
            self._writer.write(line)
            await self._writer.drain()
            raw = await self._reader.readline()
            if not raw:
                raise ClusterError(
                    f"rewind cluster serve closed connection during {method}"
                )
            resp = json.loads(raw.decode("utf-8"))
            if "error" in resp and resp["error"] is not None:
                raise ClusterError(resp["error"].get("message", str(resp["error"])))
            return resp.get("result")

    async def close(self) -> None:
        try:
            if self._writer is not None:
                try:
                    await self.call("shutdown")
                except Exception:
                    pass
        finally:
            if self._writer is not None:
                try:
                    self._writer.close()
                    if self._proc is None:
                        await self._writer.wait_closed()
                except Exception:
                    pass
                self._writer = None
            if self._proc is not None:
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    self._proc.kill()
                    await self._proc.wait()
                self._proc = None
            self._reader = None
