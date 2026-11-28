"""Async per-node handle for a Rewind :class:`Cluster`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

from .types import SignalEvent, WaitResult

if TYPE_CHECKING:
    from .cluster import Cluster


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
        body: Any = None,
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
