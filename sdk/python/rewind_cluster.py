"""Async Python SDK for Rewind VM multi-node deterministic cluster simulation.

Speaks NDJSON-RPC 2.0 to ``rewind cluster serve`` over stdin/stdout (default)
or a Unix domain socket using ``asyncio``. Zero third-party dependencies —
stdlib only.

Example::

    import asyncio
    from rewind_cluster import Cluster

    async def main() -> None:
        async with Cluster("raft-test", latency_ms=2, jitter_us=200) as c:
            n1 = c.add_node("node1", ["/bin/sh", "-c", "..."], ip="10.0.0.1")
            n2 = c.add_node("node2", ["/bin/sh", "-c", "..."], ip="10.0.0.2")
            cli = c.add_node("client", ["/bin/sh", "-c", "..."], role="driver")
            await c.start()

            await n1.wait_for("ready")
            snap = await c.snapshot("after-boot")
            await c.partition(["node1"], ["node2", "client"])
            await c.advance(ms=500)
            assert (await n1.cat("/tmp/state")).strip() == "ok"
            await c.heal()
            await cli.wait_exit()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import dataclass
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

T = TypeVar("T")
R = TypeVar("R")

# Generous asyncio StreamReader line limit so large base64 `cat`/`logs` RPC
# replies never trip asyncio's default 64 KiB LimitOverrunError.
_STREAM_LIMIT = 64 * 1024 * 1024

__all__ = [
    "Cluster",
    "ClusterError",
    "NodeHandle",
    "Nemesis",
    "SignalEvent",
    "WaitAnyResult",
    "WaitResult",
]

# Linux input-event-codes.h constants for paravirtual HID (/dev/input/event*).
EV_SYN = 0
EV_KEY = 1
EV_REL = 2
EV_ABS = 3
SYN_REPORT = 0
ABS_X = 0
ABS_Y = 1
REL_HWHEEL = 6
REL_WHEEL = 8
BTN_LEFT = 0x110
BTN_RIGHT = 0x111
BTN_MIDDLE = 0x112
KEY_LEFTSHIFT = 42

_KEY_NAMES: Dict[str, int] = {
    "esc": 1, "escape": 1,
    "1": 2, "2": 3, "3": 4, "4": 5, "5": 6, "6": 7, "7": 8, "8": 9, "9": 10, "0": 11,
    "minus": 12, "-": 12, "equal": 13, "=": 13, "backspace": 14, "tab": 15,
    "q": 16, "w": 17, "e": 18, "r": 19, "t": 20, "y": 21, "u": 22, "i": 23, "o": 24, "p": 25,
    "[": 26, "]": 27, "enter": 28, "return": 28, "ctrl": 29, "leftctrl": 29,
    "a": 30, "s": 31, "d": 32, "f": 33, "g": 34, "h": 35, "j": 36, "k": 37, "l": 38,
    ";": 39, "'": 40, "`": 41, "shift": 42, "leftshift": 42, "\\": 43,
    "z": 44, "x": 45, "c": 46, "v": 47, "b": 48, "n": 49, "m": 50,
    ",": 51, ".": 52, "/": 53, "rightshift": 54, "alt": 56, "leftalt": 56,
    "space": 57, " ": 57,
    "home": 102, "up": 103, "pageup": 104, "left": 105, "right": 106,
    "end": 107, "down": 108, "pagedown": 109, "delete": 111,
}

_SHIFTED_CHARS: Dict[str, int] = {
    "!": 2, "@": 3, "#": 4, "$": 5, "%": 6, "^": 7, "&": 8, "*": 9, "(": 10, ")": 11,
    "_": 12, "+": 13, "{": 26, "}": 27, ":": 39, '"': 40, "~": 41, "|": 43,
    "<": 51, ">": 52, "?": 53,
}

_BUTTON_CODES: Dict[str, int] = {
    "left": BTN_LEFT,
    "right": BTN_RIGHT,
    "middle": BTN_MIDDLE,
}


class ClusterError(RuntimeError):
    """Raised when the Rewind cluster RPC server returns an error."""


@dataclass
class Nemesis:
    """Schedule-driven fault injection profile (active when schedule > 0 or nemesis=True)."""

    tick_ms: int = 100
    partition_prob_ppm: int = 150_000
    heal_prob_ppm: int = 250_000
    crash_prob_ppm: int = 100_000
    restart_prob_ppm: int = 300_000
    max_crashed: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tick_ms": self.tick_ms,
            "partition_prob_ppm": self.partition_prob_ppm,
            "heal_prob_ppm": self.heal_prob_ppm,
            "crash_prob_ppm": self.crash_prob_ppm,
            "restart_prob_ppm": self.restart_prob_ppm,
            "max_crashed": self.max_crashed,
        }


@dataclass
class WaitResult:
    matched: bool
    line: Optional[str]
    cluster_ns: int
    cluster_step: int
    node_step: int

    @property
    def cluster_ms(self) -> float:
        return self.cluster_ns / 1_000_000.0


@dataclass
class WaitAnyResult:
    matched: bool
    node: Optional[str]
    line: Optional[str]
    cluster_ns: int
    cluster_step: int
    node_step: int

    @property
    def cluster_ms(self) -> float:
        return self.cluster_ns / 1_000_000.0


@dataclass
class SignalEvent:
    """One signal or barrier arrival sent by a guest to ``http://rewind``."""

    seq: int
    ns: int
    node: str
    name: str
    payload: str
    barrier: bool
    conn_id: Optional[int] = None

    @property
    def ms(self) -> float:
        return self.ns / 1_000_000.0

    def json(self) -> Any:
        return json.loads(self.payload)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SignalEvent":
        return cls(
            seq=int(d["seq"]),
            ns=int(d["ns"]),
            node=str(d["node"]),
            name=str(d["name"]),
            payload=str(d.get("payload", "")),
            barrier=bool(d.get("barrier", False)),
            conn_id=int(d["conn_id"]) if d.get("conn_id") is not None else None,
        )


class NodeHandle:
    """Async convenience handle for one node in a :class:`Cluster`."""

    def __init__(self, cluster: "Cluster", name: str) -> None:
        self._cluster = cluster
        self.name = name

    async def wait_for(
        self,
        contains: str,
        *,
        stream: str = "output",
        timeout_ms: Optional[int] = 10_000,
        from_start: bool = False,
        wall_timeout_ms: Optional[int] = None,
        check: bool = True,
    ) -> WaitResult:
        return await self._cluster.wait_for(
            self.name,
            contains,
            stream=stream,
            timeout_ms=timeout_ms,
            from_start=from_start,
            wall_timeout_ms=wall_timeout_ms,
            check=check,
        )

    async def wait_exit(
        self,
        *,
        timeout_ms: Optional[int] = 30_000,
        wall_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self._cluster.wait_exit(
            self.name, timeout_ms=timeout_ms, wall_timeout_ms=wall_timeout_ms
        )

    async def wait_signal(
        self,
        name: Optional[str] = None,
        *,
        names: Optional[Sequence[str]] = None,
        barrier: Optional[bool] = None,
        timeout_ms: Optional[int] = 10_000,
        from_start: bool = False,
        wall_timeout_ms: Optional[int] = None,
        check: bool = True,
    ) -> Optional[SignalEvent]:
        """Waits for this node to hit ``http://rewind/signal/<name>`` or ``/barrier/<name>``."""
        return await self._cluster.wait_signal(
            name,
            names=names,
            node=self.name,
            barrier=barrier,
            timeout_ms=timeout_ms,
            from_start=from_start,
            wall_timeout_ms=wall_timeout_ms,
            check=check,
        )

    async def release(
        self,
        name: str,
        body: Union[str, bytes] = "",
        *,
        status: int = 200,
    ) -> Dict[str, Any]:
        """Releases this node from ``http://rewind/barrier/<name>`` with ``body``."""
        return await self._cluster.release(
            name, body, node=self.name, status=status
        )

    async def crash(self) -> Dict[str, Any]:
        return await self._cluster.crash(self.name)

    async def restart(self) -> Dict[str, Any]:
        return await self._cluster.restart(self.name)

    async def send_input(self, data: Union[str, bytes]) -> Dict[str, Any]:
        return await self._cluster.send_input(self.name, data)

    async def send_hid(
        self, events: Sequence[Tuple[int, int, int]]
    ) -> Dict[str, Any]:
        """Injects raw Linux evdev ``(type, code, value)`` HID events."""
        return await self._cluster.send_hid(self.name, events)

    async def mouse_move(
        self, x: int, y: int, *, width: int = 1280, height: int = 720
    ) -> Dict[str, Any]:
        """Moves the absolute pointer to pixel ``(x, y)`` on a ``width x height`` display."""
        return await self._cluster.mouse_move(
            self.name, x, y, width=width, height=height
        )

    async def click(
        self,
        x: int,
        y: int,
        *,
        button: str = "left",
        width: int = 1280,
        height: int = 720,
        move_ms: int = 20,
        hold_ms: int = 30,
    ) -> Dict[str, Any]:
        """Moves the pointer to ``(x, y)`` and clicks ``button`` deterministically."""
        return await self._cluster.click(
            self.name,
            x,
            y,
            button=button,
            width=width,
            height=height,
            move_ms=move_ms,
            hold_ms=hold_ms,
        )

    async def key(self, key_name: str, *, hold_ms: int = 20) -> Dict[str, Any]:
        """Presses and releases one named key (e.g. ``'enter'``, ``'tab'``, ``'a'``)."""
        return await self._cluster.key(self.name, key_name, hold_ms=hold_ms)

    async def type_text(self, text: str, *, delay_ms: int = 15) -> None:
        """Types an ASCII string deterministically via ``/dev/input/event1``."""
        await self._cluster.type_text(self.name, text, delay_ms=delay_ms)

    async def isolate(self, *, symmetric: bool = True) -> Dict[str, Any]:
        return await self._cluster.partition([self.name], None, symmetric=symmetric)

    async def heal(self) -> Dict[str, Any]:
        return await self._cluster.heal([self.name])

    async def cat(self, path: str, *, pid: Optional[int] = None) -> str:
        return await self._cluster.cat(self.name, path, pid=pid)

    async def cat_bytes(self, path: str, *, pid: Optional[int] = None) -> bytes:
        return await self._cluster.cat_bytes(self.name, path, pid=pid)

    async def files(
        self, path: str, *, pid: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return await self._cluster.files(self.name, path, pid=pid)

    async def logs(self, *, stream: str = "output", since: int = 0) -> List[str]:
        return (await self._cluster.logs(self.name, stream=stream, since=since))[
            "lines"
        ]


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
        nemesis: Optional[Union[Nemesis, Dict[str, Any]]] = None,
        nemesis_enabled: Optional[bool] = None,
        keyframes: Optional[bool] = None,
        echo: bool = False,
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
        self._nemesis = nemesis.to_dict() if isinstance(nemesis, Nemesis) else nemesis
        self._nemesis_enabled = nemesis_enabled
        self._keyframes = keyframes
        self._echo = echo
        self._clock = clock or os.environ.get("REWIND_CLOCK")
        self._cpu = cpu
        self._rewind_bin = rewind_bin or os.environ.get("REWIND_BIN") or "rewind"
        self._socket_path = socket_path or os.environ.get("REWIND_CLUSTER_SOCK")

        self._nodes: List[Dict[str, Any]] = []
        self._links: List[Dict[str, Any]] = []
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._call_lock: Optional[asyncio.Lock] = None
        self._req_id = 0
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

    async def _connect(self) -> None:
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

    async def _call(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        if self._call_lock is None:
            self._call_lock = asyncio.Lock()
        async with self._call_lock:
            await self._connect()
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
        if self._nemesis is not None:
            params["nemesis"] = self._nemesis
        if self._nemesis_enabled is not None:
            params["nemesis_enabled"] = self._nemesis_enabled
        if self._keyframes is not None:
            params["keyframes"] = self._keyframes

        res = await self._call("cluster.start", params)
        self._started = True
        return res

    async def status(self) -> Dict[str, Any]:
        return await self._call("cluster.status")

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
        return await self._call("cluster.advance", params)

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
        res = await self._call("cluster.wait_for", params)
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
        res = await self._call("cluster.wait_any", params)
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
        res = await self._call("cluster.wait_signal", params)
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
        body: Union[str, bytes] = "",
        *,
        node: Optional[str] = None,
        status: int = 200,
    ) -> Dict[str, Any]:
        """Releases guests blocked on ``http://rewind/barrier/<name>`` with HTTP ``body``."""
        params: Dict[str, Any] = {"name": name, "status": status}
        if node is not None:
            params["node"] = node
        if isinstance(body, bytes):
            params["body_base64"] = base64.b64encode(body).decode("ascii")
        else:
            params["body"] = body
        return await self._call("cluster.release", params)

    async def signals(self, *, since: int = 0) -> List[SignalEvent]:
        """Returns all recorded ``http://rewind`` signals with ``seq > since``."""
        res = await self._call("cluster.signals", {"since": since})
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
        return await self._call("cluster.wait_exit", params)

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
        return await self._call("cluster.partition", params)

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
        return await self._call("cluster.heal", params)

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
        return await self._call("cluster.set_link", params)

    async def crash(self, node: str) -> Dict[str, Any]:
        return await self._call("cluster.crash", {"node": node})

    async def restart(self, node: str) -> Dict[str, Any]:
        return await self._call("cluster.restart", {"node": node})

    async def send_input(self, node: str, data: Union[str, bytes]) -> Dict[str, Any]:
        """Injects deterministic console input into ``node``'s ``/dev/rewind-console``."""
        if isinstance(data, bytes):
            params: Dict[str, Any] = {
                "node": node,
                "data_base64": base64.b64encode(data).decode("ascii"),
            }
        else:
            params = {"node": node, "data": data}
        return await self._call("cluster.send_input", params)

    async def send_hid(
        self, node: str, events: Sequence[Tuple[int, int, int]]
    ) -> Dict[str, Any]:
        """Injects deterministic Linux evdev ``(type, code, value)`` events."""
        payload = [
            {"type": int(t), "code": int(c), "value": int(v)} for t, c, v in events
        ]
        return await self._call("cluster.send_hid", {"node": node, "events": payload})

    async def mouse_move(
        self, node: str, x: int, y: int, *, width: int = 1280, height: int = 720
    ) -> Dict[str, Any]:
        ax = max(0, min(32767, int(round(x * 32767 / max(1, width - 1)))))
        ay = max(0, min(32767, int(round(y * 32767 / max(1, height - 1)))))
        return await self.send_hid(
            node,
            [
                (EV_ABS, ABS_X, ax),
                (EV_ABS, ABS_Y, ay),
                (EV_SYN, SYN_REPORT, 0),
            ],
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
        btn = _BUTTON_CODES.get(button.lower())
        if btn is None:
            raise ValueError(
                f"unknown mouse button {button!r} (expected left, right, middle)"
            )
        await self.mouse_move(node, x, y, width=width, height=height)
        if move_ms > 0:
            await self.advance(ms=move_ms)
        await self.send_hid(node, [(EV_KEY, btn, 1), (EV_SYN, SYN_REPORT, 0)])
        if hold_ms > 0:
            await self.advance(ms=hold_ms)
        return await self.send_hid(node, [(EV_KEY, btn, 0), (EV_SYN, SYN_REPORT, 0)])

    async def key(
        self, node: str, key_name: str, *, hold_ms: int = 20
    ) -> Dict[str, Any]:
        code = _KEY_NAMES.get(key_name.lower())
        if code is None:
            raise ValueError(f"unknown key name {key_name!r}")
        await self.send_hid(node, [(EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)])
        if hold_ms > 0:
            await self.advance(ms=hold_ms)
        return await self.send_hid(node, [(EV_KEY, code, 0), (EV_SYN, SYN_REPORT, 0)])

    async def type_text(self, node: str, text: str, *, delay_ms: int = 15) -> None:
        for ch in text:
            if ch == "\n":
                await self.key(node, "enter", hold_ms=delay_ms)
            elif ch == "\t":
                await self.key(node, "tab", hold_ms=delay_ms)
            elif "A" <= ch <= "Z":
                code = _KEY_NAMES[ch.lower()]
                await self.send_hid(
                    node,
                    [
                        (EV_KEY, KEY_LEFTSHIFT, 1),
                        (EV_KEY, code, 1),
                        (EV_SYN, SYN_REPORT, 0),
                    ],
                )
                if delay_ms > 0:
                    await self.advance(ms=delay_ms)
                await self.send_hid(
                    node,
                    [
                        (EV_KEY, code, 0),
                        (EV_KEY, KEY_LEFTSHIFT, 0),
                        (EV_SYN, SYN_REPORT, 0),
                    ],
                )
            elif ch in _SHIFTED_CHARS:
                code = _SHIFTED_CHARS[ch]
                await self.send_hid(
                    node,
                    [
                        (EV_KEY, KEY_LEFTSHIFT, 1),
                        (EV_KEY, code, 1),
                        (EV_SYN, SYN_REPORT, 0),
                    ],
                )
                if delay_ms > 0:
                    await self.advance(ms=delay_ms)
                await self.send_hid(
                    node,
                    [
                        (EV_KEY, code, 0),
                        (EV_KEY, KEY_LEFTSHIFT, 0),
                        (EV_SYN, SYN_REPORT, 0),
                    ],
                )
            elif ch in _KEY_NAMES:
                await self.key(node, ch, hold_ms=delay_ms)
            else:
                raise ValueError(f"unsupported ASCII character {ch!r} in type_text")
            if delay_ms > 0:
                await self.advance(ms=delay_ms)

    async def logs(
        self, node: str, *, stream: str = "output", since: int = 0
    ) -> Dict[str, Any]:
        return await self._call(
            "cluster.logs", {"node": node, "stream": stream, "since": since}
        )

    async def net_events(self, *, since: int = 0) -> Dict[str, Any]:
        return await self._call("cluster.net_events", {"since": since})

    async def cat_bytes(
        self, node: str, path: str, *, pid: Optional[int] = None
    ) -> bytes:
        params: Dict[str, Any] = {"node": node, "path": path}
        if pid is not None:
            params["pid"] = pid
        res = await self._call("cluster.cat", params)
        if not res.get("found"):
            raise FileNotFoundError(res.get("error") or f"{node}:{path} not found")
        return base64.b64decode(res["content_base64"])

    async def cat(self, node: str, path: str, *, pid: Optional[int] = None) -> str:
        return (await self.cat_bytes(node, path, pid=pid)).decode(
            "utf-8", errors="replace"
        )

    async def files(
        self, node: str, path: str, *, pid: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"node": node, "path": path}
        if pid is not None:
            params["pid"] = pid
        res = await self._call("cluster.files", params)
        if not res.get("found"):
            raise FileNotFoundError(res.get("error") or f"{node}:{path} not found")
        return res["entries"]

    async def snapshot(self, name: Optional[str] = None) -> str:
        params = {"name": name} if name is not None else {}
        res = await self._call("cluster.snapshot", params)
        return str(res["snapshot"])

    async def save_snapshot(
        self, path: Union[str, os.PathLike[str]], name: Optional[str] = None
    ) -> str:
        """Persists a snapshot to ``path`` so parallel workers can boot from it."""
        params: Dict[str, Any] = {"path": os.fspath(path)}
        if name is not None:
            params["name"] = name
        res = await self._call("cluster.save_snapshot", params)
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
        await c._connect()
        params: Dict[str, Any] = {"path": os.fspath(path), "echo": echo}
        if name is not None:
            params["name"] = name
        if keyframes is not None:
            params["keyframes"] = keyframes
        await c._call("cluster.start_from_snapshot", params)
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
        return await self._call("cluster.restore", {"snapshot": snapshot})

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
        res = await self._call("cluster.finish", params)
        self._finished = True
        self.result = res
        return res

    async def close(self) -> None:
        self._started = False
        try:
            if self._writer is not None:
                try:
                    await self._call("shutdown")
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
