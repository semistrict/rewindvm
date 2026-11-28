"""Async interactive controller for a deterministic multi-node Rewind VM cluster."""

from __future__ import annotations

import asyncio
import base64
import json
import os
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from . import hid
from .node import NodeHandle
from .rpc import AsyncRpcClient
from .types import (
    ClusterError,
    Nemesis,
    SignalEvent,
    WaitAnyResult,
    WaitResult,
)

T = TypeVar("T")
R = TypeVar("R")


class Cluster:
    """Async interactive controller for a deterministic multi-node Rewind VM cluster."""

    def __init__(
        self,
        name: str = "cluster",
        *,
        subnet: str = "10.0.0.0/24",
        latency_ms: Optional[int] = 1,
        latency_us: Optional[int] = None,
        jitter_ms: Optional[int] = None,
        jitter_us: Optional[int] = None,
        drop_ppm: int = 0,
        duplicate_ppm: int = 0,
        mem_mib: int = 128,
        cores: int = 1,
        stop_when: str = "driver",
        max_virtual_ms: int = 60_000,
        slice_us: Optional[int] = None,
        packages: Optional[Sequence[str]] = None,
        seed: Optional[int] = None,
        schedule: Optional[int] = None,
        epoch: Optional[int] = None,
        faults: Optional[Sequence[Dict[str, Any]]] = None,
        fault_window_from_ms: Optional[int] = None,
        fault_window_until_ms: Optional[int] = None,
        nemesis: Optional[Union[Nemesis, Dict[str, Any]]] = None,
        nemesis_enabled: Optional[bool] = None,
        keyframes: Optional[bool] = None,
        echo: bool = False,
        wall_timeout_ms: Optional[int] = None,
        clock: Optional[str] = None,
        cpu: Optional[str] = None,
        rewind_bin: Optional[str] = None,
        socket_path: Optional[str] = None,
    ) -> None:
        self.name = name
        self._network: Dict[str, Any] = {
            "subnet": subnet,
            "drop_ppm": drop_ppm,
            "duplicate_ppm": duplicate_ppm,
        }
        if latency_us is not None:
            self._network["latency_us"] = latency_us
        elif latency_ms is not None:
            self._network["latency_ms"] = latency_ms
        if jitter_us is not None:
            self._network["jitter_us"] = jitter_us
        elif jitter_ms is not None:
            self._network["jitter_ms"] = jitter_ms

        self._mem_mib = mem_mib
        self._cores = cores
        self._stop_when = stop_when
        self._max_virtual_ms = max_virtual_ms
        self._slice_us = slice_us
        self._packages = list(packages) if packages is not None else []
        self._seed = seed
        self._schedule = schedule
        self._epoch = epoch
        self._faults = list(faults) if faults is not None else []
        self._fault_window_from_ms = fault_window_from_ms
        self._fault_window_until_ms = fault_window_until_ms
        self._nemesis = nemesis.to_dict() if isinstance(nemesis, Nemesis) else nemesis
        self._nemesis_enabled = nemesis_enabled
        self._keyframes = keyframes
        self._echo = echo
        self._wall_timeout_ms = wall_timeout_ms
        self._rpc = AsyncRpcClient(
            rewind_bin=rewind_bin or os.environ.get("REWIND_BIN") or "rewind",
            socket_path=socket_path or os.environ.get("REWIND_CLUSTER_SOCK"),
            clock=clock or os.environ.get("REWIND_CLOCK"),
            cpu=cpu,
        )

        self._nodes: List[Dict[str, Any]] = []
        self._links: List[Dict[str, Any]] = []
        self._started = False
        self._finished = False
        self.result: Optional[Dict[str, Any]] = None

    def add_node(
        self,
        name: str,
        command: Union[str, Sequence[str]],
        *,
        ip: Optional[str] = None,
        role: str = "service",
        root: Optional[str] = None,
        packages: Optional[Sequence[str]] = None,
        env: Optional[Union[Dict[str, str], Sequence[str]]] = None,
        cwd: Optional[str] = None,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        mem_mib: Optional[int] = None,
        cores: Optional[int] = None,
        clock_skew_ms: int = 0,
        clock_drift_ppm: int = 0,
        display: bool = False,
        display_width: Optional[int] = None,
        display_height: Optional[int] = None,
        files: Optional[Dict[str, str]] = None,
    ) -> NodeHandle:
        """Registers a node before calling :meth:`start`."""
        if self._started:
            raise ClusterError("cannot add nodes after cluster.start()")
        cmd = ["/bin/sh", "-c", command] if isinstance(command, str) else list(command)
        env_list: List[str] = []
        if isinstance(env, dict):
            env_list = [f"{k}={v}" for k, v in env.items()]
        elif env is not None:
            env_list = list(env)
        file_list = (
            [{"path": p, "contents": c} for p, c in files.items()] if files else []
        )
        node: Dict[str, Any] = {
            "name": name,
            "role": role,
            "command": cmd,
            "env": env_list,
            "clock_skew_ms": clock_skew_ms,
            "clock_drift_ppm": clock_drift_ppm,
            "files": file_list,
        }
        if ip is not None:
            node["ip"] = ip
        if root is not None:
            node["root"] = root
        if packages is not None:
            node["packages"] = list(packages)
        if cwd is not None:
            node["cwd"] = cwd
        if uid is not None:
            node["uid"] = uid
        if gid is not None:
            node["gid"] = gid
        if mem_mib is not None:
            node["mem_mib"] = mem_mib
        if cores is not None:
            node["cores"] = cores
        if display:
            node["display"] = True
        if display_width is not None:
            node["display_width"] = display_width
        if display_height is not None:
            node["display_height"] = display_height
        self._nodes.append(node)
        return NodeHandle(self, name)

    def add_link(
        self,
        src: str,
        dst: str,
        *,
        latency_ms: Optional[int] = None,
        latency_us: Optional[int] = None,
        jitter_ms: Optional[int] = None,
        jitter_us: Optional[int] = None,
        drop_ppm: Optional[int] = None,
        bidirectional: bool = True,
    ) -> None:
        """Adds an initial link override before :meth:`start`."""
        pairs = [(src, dst), (dst, src)] if bidirectional else [(src, dst)]
        for a, b in pairs:
            entry: Dict[str, Any] = {"from": a, "to": b}
            if latency_ms is not None:
                entry["latency_ms"] = latency_ms
            if latency_us is not None:
                entry["latency_us"] = latency_us
            if jitter_ms is not None:
                entry["jitter_ms"] = jitter_ms
            if jitter_us is not None:
                entry["jitter_us"] = jitter_us
            if drop_ppm is not None:
                entry["drop_ppm"] = drop_ppm
            self._links.append(entry)

    def node(self, name: str) -> NodeHandle:
        """Returns a :class:`NodeHandle` for ``name`` (useful after :meth:`from_snapshot`)."""
        return NodeHandle(self, name)

    async def start(self) -> Dict[str, Any]:
        """Boots all configured VMs and begins deterministic recording."""
        if not self._nodes:
            raise ClusterError("add at least one node before calling start()")
        params: Dict[str, Any] = {
            "name": self.name,
            "mem_mib": self._mem_mib,
            "cores": self._cores,
            "stop_when": self._stop_when,
            "max_virtual_ms": self._max_virtual_ms,
            "echo": self._echo,
            "network": self._network,
            "links": self._links,
            "packages": self._packages,
            "nodes": self._nodes,
        }
        if self._slice_us is not None:
            params["slice_us"] = self._slice_us
        if self._seed is not None:
            params["seed"] = self._seed
        if self._schedule is not None:
            params["schedule"] = self._schedule
        if self._epoch is not None:
            params["epoch"] = self._epoch
        if self._faults:
            params["faults"] = self._faults
        if self._fault_window_from_ms is not None:
            params["fault_window_from_ms"] = self._fault_window_from_ms
        if self._fault_window_until_ms is not None:
            params["fault_window_until_ms"] = self._fault_window_until_ms
        if self._nemesis is not None:
            params["nemesis"] = self._nemesis
        if self._nemesis_enabled is not None:
            params["nemesis_enabled"] = self._nemesis_enabled
        if self._keyframes is not None:
            params["keyframes"] = self._keyframes
        if self._wall_timeout_ms is not None:
            params["wall_timeout_ms"] = self._wall_timeout_ms

        res = await self._rpc.call("cluster.start", params)
        self._started = True
        return res

    async def status(self) -> Dict[str, Any]:
        return await self._rpc.call("cluster.status")

    async def advance(
        self,
        *,
        ms: Optional[int] = None,
        us: Optional[int] = None,
        ns: Optional[int] = None,
        until_ms: Optional[int] = None,
        wall_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if ms is not None:
            params["ms"] = ms
        if us is not None:
            params["us"] = us
        if ns is not None:
            params["ns"] = ns
        if until_ms is not None:
            params["until_ms"] = until_ms
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        return await self._rpc.call("cluster.advance", params)

    async def wait_for(
        self,
        node: str,
        contains: str,
        *,
        stream: str = "output",
        timeout_ms: Optional[int] = 10_000,
        from_start: bool = False,
        wall_timeout_ms: Optional[int] = None,
        check: bool = True,
    ) -> WaitResult:
        params: Dict[str, Any] = {
            "node": node,
            "contains": contains,
            "stream": stream,
            "from_start": from_start,
        }
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        res = await self._rpc.call("cluster.wait_for", params)
        out = WaitResult(
            matched=bool(res["matched"]),
            line=res.get("line"),
            cluster_ns=int(res["cluster_ns"]),
            cluster_step=int(res["cluster_step"]),
            node_step=int(res["node_step"]),
        )
        if check and not out.matched:
            raise AssertionError(
                f"timed out waiting for {node!r} ({stream}) to emit {contains!r} "
                f"at {out.cluster_ms:.1f}ms"
            )
        return out

    async def wait_any(
        self,
        nodes: Sequence[str],
        contains: str,
        *,
        stream: str = "output",
        timeout_ms: Optional[int] = 10_000,
        from_start: bool = False,
        wall_timeout_ms: Optional[int] = None,
        check: bool = True,
    ) -> WaitAnyResult:
        """Waits until any node in ``nodes`` (or any node if empty) emits ``contains``."""
        params: Dict[str, Any] = {
            "nodes": list(nodes),
            "contains": contains,
            "stream": stream,
            "from_start": from_start,
        }
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        res = await self._rpc.call("cluster.wait_any", params)
        out = WaitAnyResult(
            matched=bool(res["matched"]),
            node=res.get("node"),
            line=res.get("line"),
            cluster_ns=int(res["cluster_ns"]),
            cluster_step=int(res["cluster_step"]),
            node_step=int(res["node_step"]),
        )
        if check and not out.matched:
            raise AssertionError(
                f"timed out waiting for any of {list(nodes)!r} ({stream}) to emit "
                f"{contains!r} at {out.cluster_ms:.1f}ms"
            )
        return out

    async def wait_signal(
        self,
        name: Optional[str] = None,
        *,
        names: Optional[Sequence[str]] = None,
        node: Optional[str] = None,
        nodes: Optional[Sequence[str]] = None,
        barrier: Optional[bool] = None,
        timeout_ms: Optional[int] = 10_000,
        from_start: bool = False,
        wall_timeout_ms: Optional[int] = None,
        check: bool = True,
    ) -> Optional[SignalEvent]:
        """Waits for a guest to hit ``http://rewind/signal/<name>`` or ``/barrier/<name>``."""
        params: Dict[str, Any] = {"from_start": from_start}
        if name is not None:
            params["name"] = name
        if names is not None:
            params["names"] = list(names)
        if node is not None:
            params["node"] = node
        if nodes is not None:
            params["nodes"] = list(nodes)
        if barrier is not None:
            params["barrier"] = barrier
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        res = await self._rpc.call("cluster.wait_signal", params)
        if res.get("matched") and res.get("signal") is not None:
            return SignalEvent.from_dict(res["signal"])
        if check:
            wanted = list(names) if names is not None else ([name] if name else ["*"])
            at_ms = int(res["cluster_ns"]) / 1_000_000.0
            raise AssertionError(
                f"timed out waiting for signal {wanted!r} at {at_ms:.1f}ms"
            )
        return None

    async def release(
        self,
        name: str,
        body: Any = None,
        *,
        node: Optional[str] = None,
        status: int = 200,
    ) -> Dict[str, Any]:
        """Releases guests blocked on ``http://rewind/barrier/<name>`` with ``body``.

        The HTTP ``Content-Type`` header matches the Python type so the guest's
        ``await barrier(name)`` decodes it symmetrically:
        - ``None``  -> empty body (``await barrier(...)`` returns ``None``)
        - ``str``   -> ``text/plain; charset=utf-8`` (returns ``str``)
        - ``bytes`` -> ``application/octet-stream`` (returns ``bytes``)
        - other     -> ``application/json`` (returns parsed JSON object)
        """
        params: Dict[str, Any] = {"name": name, "status": status}
        if node is not None:
            params["node"] = node
        if body is None:
            params["body"] = ""
            params["content_type"] = "application/x-rewind-empty"
        elif isinstance(body, bytes):
            params["body_base64"] = base64.b64encode(body).decode("ascii")
            params["content_type"] = "application/octet-stream"
        elif isinstance(body, str):
            params["body"] = body
            params["content_type"] = "text/plain; charset=utf-8"
        else:
            params["body"] = json.dumps(body)
            params["content_type"] = "application/json"
        return await self._rpc.call("cluster.release", params)

    async def signals(self, *, since: int = 0) -> List[SignalEvent]:
        """Returns all recorded ``http://rewind`` signals with ``seq > since``."""
        res = await self._rpc.call("cluster.signals", {"since": since})
        return [SignalEvent.from_dict(d) for d in res.get("signals", [])]

    async def wait_exit(
        self,
        node: Optional[str] = None,
        *,
        timeout_ms: Optional[int] = 30_000,
        wall_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if node is not None:
            params["node"] = node
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        return await self._rpc.call("cluster.wait_exit", params)

    async def partition(
        self,
        a: Iterable[str],
        b: Optional[Iterable[str]] = None,
        *,
        symmetric: bool = True,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"a": list(a), "symmetric": symmetric}
        if b is not None:
            params["b"] = list(b)
        return await self._rpc.call("cluster.partition", params)

    async def heal(
        self,
        a: Optional[Iterable[str]] = None,
        b: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if a is not None:
            params["a"] = list(a)
        if b is not None:
            params["b"] = list(b)
        return await self._rpc.call("cluster.heal", params)

    async def set_link(
        self,
        src: str,
        dst: str,
        *,
        latency_ms: Optional[int] = None,
        latency_us: Optional[int] = None,
        jitter_ms: Optional[int] = None,
        jitter_us: Optional[int] = None,
        drop_ppm: Optional[int] = None,
        duplicate_ppm: Optional[int] = None,
        bidirectional: bool = True,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "from": src,
            "to": dst,
            "bidirectional": bidirectional,
        }
        if latency_ms is not None:
            params["latency_ms"] = latency_ms
        if latency_us is not None:
            params["latency_us"] = latency_us
        if jitter_ms is not None:
            params["jitter_ms"] = jitter_ms
        if jitter_us is not None:
            params["jitter_us"] = jitter_us
        if drop_ppm is not None:
            params["drop_ppm"] = drop_ppm
        if duplicate_ppm is not None:
            params["duplicate_ppm"] = duplicate_ppm
        return await self._rpc.call("cluster.set_link", params)

    async def crash(self, node: str) -> Dict[str, Any]:
        return await self._rpc.call("cluster.crash", {"node": node})

    async def restart(self, node: str) -> Dict[str, Any]:
        return await self._rpc.call("cluster.restart", {"node": node})

    async def send_input(self, node: str, data: Union[str, bytes]) -> Dict[str, Any]:
        """Injects deterministic console input into ``node``'s ``/dev/rewind-console``."""
        if isinstance(data, bytes):
            params: Dict[str, Any] = {
                "node": node,
                "data_base64": base64.b64encode(data).decode("ascii"),
            }
        else:
            params = {"node": node, "data": data}
        return await self._rpc.call("cluster.send_input", params)

    async def send_hid(
        self, node: str, events: Sequence[Tuple[int, int, int]]
    ) -> Dict[str, Any]:
        """Injects deterministic Linux evdev ``(type, code, value)`` events."""
        payload = [
            {"type": int(t), "code": int(c), "value": int(v)} for t, c, v in events
        ]
        return await self._rpc.call(
            "cluster.send_hid", {"node": node, "events": payload}
        )

    async def mouse_move(
        self, node: str, x: int, y: int, *, width: int = 1280, height: int = 720
    ) -> Dict[str, Any]:
        return await self.send_hid(
            node, hid.abs_move_events(x, y, width=width, height=height)
        )

    async def click(
        self,
        node: str,
        x: int,
        y: int,
        *,
        button: str = "left",
        width: int = 1280,
        height: int = 720,
        move_ms: int = 20,
        hold_ms: int = 30,
    ) -> Dict[str, Any]:
        btn = hid.button_code(button)
        await self.mouse_move(node, x, y, width=width, height=height)
        if move_ms > 0:
            await self.advance(ms=move_ms)
        await self.send_hid(node, [(hid.EV_KEY, btn, 1), (hid.EV_SYN, hid.SYN_REPORT, 0)])
        if hold_ms > 0:
            await self.advance(ms=hold_ms)
        return await self.send_hid(
            node, [(hid.EV_KEY, btn, 0), (hid.EV_SYN, hid.SYN_REPORT, 0)]
        )

    async def key(
        self, node: str, key_name: str, *, hold_ms: int = 20
    ) -> Dict[str, Any]:
        code = hid.key_code(key_name)
        await self.send_hid(
            node, [(hid.EV_KEY, code, 1), (hid.EV_SYN, hid.SYN_REPORT, 0)]
        )
        if hold_ms > 0:
            await self.advance(ms=hold_ms)
        return await self.send_hid(
            node, [(hid.EV_KEY, code, 0), (hid.EV_SYN, hid.SYN_REPORT, 0)]
        )

    async def type_text(self, node: str, text: str, *, delay_ms: int = 15) -> None:
        for ch in text:
            press, release = hid.char_press_release(ch)
            await self.send_hid(node, press)
            if delay_ms > 0:
                await self.advance(ms=delay_ms)
            await self.send_hid(node, release)
            if delay_ms > 0:
                await self.advance(ms=delay_ms)

    async def logs(
        self,
        node: str,
        *,
        stream: str = "output",
        since: int = 0,
        step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"node": node, "stream": stream, "since": since}
        if step is not None:
            params["step"] = step
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.logs", params)

    async def net_events(
        self,
        *,
        since: int = 0,
        node: Optional[str] = None,
        faults: bool = False,
        drops: bool = False,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"since": since}
        if node is not None:
            params["node"] = node
        if faults:
            params["faults"] = True
        if drops:
            params["drops"] = True
        if from_ms is not None:
            params["from_ms"] = from_ms
        if to_ms is not None:
            params["to_ms"] = to_ms
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.net_events", params)

    async def ps(
        self,
        node: str,
        *,
        step: Optional[int] = None,
        all: bool = False,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Returns processes alive on ``node`` at ``step`` (or the current/final step)."""
        params: Dict[str, Any] = {"node": node, "all": all}
        if step is not None:
            params["step"] = step
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.ps", params)

    async def events(
        self,
        node: str,
        *,
        from_step: Optional[int] = None,
        to_step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Returns trace events recorded for ``node`` in ``[from_step, to_step]``."""
        params: Dict[str, Any] = {"node": node}
        if from_step is not None:
            params["from"] = from_step
        if to_step is not None:
            params["to"] = to_step
        if cluster is not None:
            params["cluster"] = cluster
        res = await self._rpc.call("cluster.events", params)
        return list(res.get("events", []))

    async def cat_bytes(
        self,
        node: str,
        path: str,
        *,
        pid: Optional[int] = None,
        step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> bytes:
        params: Dict[str, Any] = {"node": node, "path": path}
        if pid is not None:
            params["pid"] = pid
        if step is not None:
            params["step"] = step
        if cluster is not None:
            params["cluster"] = cluster
        res = await self._rpc.call("cluster.cat", params)
        if not res.get("found"):
            raise FileNotFoundError(res.get("error") or f"{node}:{path} not found")
        return base64.b64decode(res["content_base64"])

    async def cat(
        self,
        node: str,
        path: str,
        *,
        pid: Optional[int] = None,
        step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> str:
        return (
            await self.cat_bytes(node, path, pid=pid, step=step, cluster=cluster)
        ).decode("utf-8", errors="replace")

    async def cat_to_file(
        self,
        node: str,
        path: str,
        out_path: Union[str, os.PathLike[str]],
        *,
        pid: Optional[int] = None,
        step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Writes guest file ``path`` directly to ``out_path`` on disk (no base64 over JSON)."""
        params: Dict[str, Any] = {
            "node": node,
            "path": path,
            "out_path": os.fspath(out_path),
        }
        if pid is not None:
            params["pid"] = pid
        if step is not None:
            params["step"] = step
        if cluster is not None:
            params["cluster"] = cluster
        res = await self._rpc.call("cluster.cat", params)
        if not res.get("found"):
            raise FileNotFoundError(res.get("error") or f"{node}:{path} not found")
        return res

    async def files(
        self,
        node: str,
        path: str,
        *,
        pid: Optional[int] = None,
        step: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"node": node, "path": path}
        if pid is not None:
            params["pid"] = pid
        if step is not None:
            params["step"] = step
        if cluster is not None:
            params["cluster"] = cluster
        res = await self._rpc.call("cluster.files", params)
        if not res.get("found"):
            raise FileNotFoundError(res.get("error") or f"{node}:{path} not found")
        return res["entries"]

    async def screenshot(
        self,
        node: str,
        path: Union[str, os.PathLike[str]],
        *,
        timestamp: bool = False,
        show_input: bool = False,
    ) -> Dict[str, Any]:
        """Captures ``node``'s live framebuffer directly to ``path`` on disk.

        Supports zero-dependency ``.bmp``, ``.ppm``, ``.raw``/``.bgr0`` as well
        as ``.png``/``.jpg``/``.webp`` (encoded via ``ffmpeg``). Never pipes
        bulk image bytes over JSON-RPC.
        """
        return await self._rpc.call(
            "cluster.screenshot",
            {
                "node": node,
                "path": os.fspath(path),
                "timestamp": timestamp,
                "show_input": show_input,
            },
        )

    async def export_video(
        self,
        node: str,
        path: Union[str, os.PathLike[str]],
        *,
        fps: int = 30,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
        timestamp: bool = False,
        show_input: bool = False,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Exports a retroactive screencast (``.mp4``, ``.webm``, ``.gif``, ``.png``,
        or ``.raw``) of ``node`` directly to ``path`` on disk.

        Call after :meth:`finish` (or pass ``cluster`` to export any recorded run).
        """
        params: Dict[str, Any] = {
            "node": node,
            "path": os.fspath(path),
            "fps": fps,
            "timestamp": timestamp,
            "show_input": show_input,
        }
        if from_ms is not None:
            params["from_ms"] = from_ms
        if to_ms is not None:
            params["to_ms"] = to_ms
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.export_video", params)

    async def export_pcap(
        self,
        path: Union[str, os.PathLike[str]],
        *,
        node: Optional[str] = None,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reconstructs a full Ethernet ``.pcap`` capture directly to ``path`` on disk.

        Call after :meth:`finish` (or pass ``cluster`` to export any recorded run).
        """
        params: Dict[str, Any] = {"path": os.fspath(path)}
        if node is not None:
            params["node"] = node
        if from_ms is not None:
            params["from_ms"] = from_ms
        if to_ms is not None:
            params["to_ms"] = to_ms
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.export_pcap", params)

    async def run_spec(
        self,
        path: Union[str, os.PathLike[str]],
        *,
        seed: Optional[int] = None,
        schedule: Optional[int] = None,
        epoch: Optional[int] = None,
        mem_mib: Optional[int] = None,
        cores: Optional[int] = None,
        max_virtual_ms: Optional[int] = None,
        nemesis_enabled: Optional[bool] = None,
        keyframes: Optional[bool] = None,
        echo: bool = False,
        wall_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Boots and records a cluster from a declarative ``.toml`` or ``.json`` spec file."""
        params: Dict[str, Any] = {"path": os.fspath(path), "echo": echo}
        if seed is not None:
            params["seed"] = seed
        if schedule is not None:
            params["schedule"] = schedule
        if epoch is not None:
            params["epoch"] = epoch
        if mem_mib is not None:
            params["mem_mib"] = mem_mib
        if cores is not None:
            params["cores"] = cores
        if max_virtual_ms is not None:
            params["max_virtual_ms"] = max_virtual_ms
        if nemesis_enabled is not None:
            params["nemesis_enabled"] = nemesis_enabled
        if keyframes is not None:
            params["keyframes"] = keyframes
        if wall_timeout_ms is not None:
            params["wall_timeout_ms"] = wall_timeout_ms
        res = await self._rpc.call("cluster.run_spec", params)
        self._finished = True
        self.result = res
        return res

    async def ls(self) -> List[Dict[str, Any]]:
        """Lists recorded cluster run manifests in ``REWIND_HOME``, newest first."""
        res = await self._rpc.call("cluster.ls")
        return list(res.get("clusters", []))

    async def show(self, cluster: Optional[str] = None) -> Dict[str, Any]:
        """Returns the full manifest of a recorded cluster run (defaults to last finished)."""
        params: Dict[str, Any] = {}
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.show", params)

    async def remove(self, *clusters: str) -> List[str]:
        """Removes one or more recorded cluster runs by id, prefix, name, or ``@N``."""
        res = await self._rpc.call("cluster.remove", {"clusters": list(clusters)})
        return list(res.get("removed", []))

    async def replay(
        self,
        cluster: Optional[str] = None,
        *,
        machine: str = "every",
    ) -> Dict[str, Any]:
        """Replays a recorded cluster run standalone and checks bit-for-bit determinism."""
        params: Dict[str, Any] = {"machine": machine}
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.replay", params)

    async def fork(
        self,
        at_ms: int,
        *,
        schedule: int = 1,
        seed: Optional[int] = None,
        keyframes: bool = True,
        echo: bool = False,
        cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Forks a recorded cluster run from ``at_ms`` under another schedule or seed."""
        params: Dict[str, Any] = {
            "at_ms": at_ms,
            "schedule": schedule,
            "keyframes": keyframes,
            "echo": echo,
        }
        if seed is not None:
            params["seed"] = seed
        if cluster is not None:
            params["cluster"] = cluster
        return await self._rpc.call("cluster.fork", params)

    async def snapshot(self, name: Optional[str] = None) -> str:
        params = {"name": name} if name is not None else {}
        res = await self._rpc.call("cluster.snapshot", params)
        return str(res["snapshot"])

    async def save_snapshot(
        self, path: Union[str, os.PathLike[str]], name: Optional[str] = None
    ) -> str:
        """Persists a snapshot to ``path`` so parallel workers can boot from it."""
        params: Dict[str, Any] = {"path": os.fspath(path)}
        if name is not None:
            params["name"] = name
        res = await self._rpc.call("cluster.save_snapshot", params)
        return str(res["path"])

    @classmethod
    async def from_snapshot(
        cls,
        path: Union[str, os.PathLike[str]],
        *,
        name: Optional[str] = None,
        keyframes: Optional[bool] = None,
        echo: bool = False,
        clock: Optional[str] = None,
        cpu: Optional[str] = None,
        rewind_bin: Optional[str] = None,
        socket_path: Optional[str] = None,
    ) -> "Cluster":
        """Starts an independent cluster session restored directly from ``path``.

        Safe to call concurrently from many tasks or processes sharing the
        same Rewind page store for parallel fan-out from one warm snapshot.
        """
        c = cls(
            name=name or "snapshot-branch",
            keyframes=keyframes,
            echo=echo,
            clock=clock,
            cpu=cpu,
            rewind_bin=rewind_bin,
            socket_path=socket_path,
        )
        await c._rpc.connect()
        params: Dict[str, Any] = {"path": os.fspath(path), "echo": echo}
        if name is not None:
            params["name"] = name
        if keyframes is not None:
            params["keyframes"] = keyframes
        await c._rpc.call("cluster.start_from_snapshot", params)
        c._started = True
        return c

    @classmethod
    async def fan_out(
        cls,
        snapshot_path: Union[str, os.PathLike[str]],
        items: Sequence[T],
        worker_fn: Callable[["Cluster", T], Awaitable[R]],
        *,
        max_workers: Optional[int] = None,
        name_prefix: str = "branch",
        keyframes: Optional[bool] = None,
        echo: bool = False,
        clock: Optional[str] = None,
        cpu: Optional[str] = None,
        rewind_bin: Optional[str] = None,
    ) -> List[Tuple[T, R]]:
        """Runs ``await worker_fn(cluster, item)`` concurrently across ``items``,
        each starting from its own independent cluster restored from ``snapshot_path``.
        """
        workers = max_workers or min(len(items), os.cpu_count() or 1) or 1
        sem = asyncio.Semaphore(workers)

        async def _run_one(item: T) -> Tuple[T, R]:
            async with sem:
                c = await cls.from_snapshot(
                    snapshot_path,
                    name=f"{name_prefix}-{item}",
                    keyframes=keyframes,
                    echo=echo,
                    clock=clock,
                    cpu=cpu,
                    rewind_bin=rewind_bin,
                )
                async with c:
                    return item, await worker_fn(c, item)

        return list(await asyncio.gather(*(_run_one(item) for item in items)))

    async def restore(self, snapshot: str) -> Dict[str, Any]:
        return await self._rpc.call("cluster.restore", {"snapshot": snapshot})

    async def finish(
        self,
        *,
        verdict: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._finished:
            assert self.result is not None
            return self.result
        params: Dict[str, Any] = {}
        if verdict is not None:
            params["verdict"] = verdict
        if reason is not None:
            params["reason"] = reason
        res = await self._rpc.call("cluster.finish", params)
        self._finished = True
        self.result = res
        return res

    async def close(self) -> None:
        self._started = False
        await self._rpc.close()

    async def __aenter__(self) -> "Cluster":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._started and not self._finished:
                if exc is not None:
                    await self.finish(
                        verdict="failed",
                        reason=f"{exc_type.__name__}: {exc}",
                    )
                else:
                    await self.finish()
        finally:
            await self.close()
