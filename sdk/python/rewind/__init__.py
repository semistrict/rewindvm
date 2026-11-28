"""Async Python SDK for Rewind VM multi-node deterministic cluster simulation.

Speaks NDJSON-RPC 2.0 to ``rewind cluster serve`` over stdin/stdout (default)
or a Unix domain socket using ``asyncio``. Zero third-party dependencies —
stdlib only.

Example::

    import asyncio
    from rewind import Cluster

    async def main() -> None:
        async with Cluster("raft-test", latency_ms=2, jitter_us=200) as c:
            n1 = c.add_node("node1", ["/bin/sh", "-c", "..."], ip="10.0.0.1")
            await c.start()
            sig = await c.wait_signal("ready")
            await c.release("dispatch", "go")
            await n1.wait_exit()

    asyncio.run(main())
"""

from . import guest
from .cluster import Cluster
from .hid import (
    ABS_X,
    ABS_Y,
    BTN_LEFT,
    BTN_MIDDLE,
    BTN_RIGHT,
    EV_ABS,
    EV_KEY,
    EV_REL,
    EV_SYN,
    KEY_LEFTSHIFT,
    REL_HWHEEL,
    REL_WHEEL,
    SYN_REPORT,
)
from .node import NodeHandle
from .types import (
    ClusterError,
    Nemesis,
    SignalEvent,
)

__all__ = [
    "Cluster",
    "ClusterError",
    "Nemesis",
    "NodeHandle",
    "SignalEvent",
    "guest",
    "EV_SYN",
    "EV_KEY",
    "EV_REL",
    "EV_ABS",
    "SYN_REPORT",
    "ABS_X",
    "ABS_Y",
    "REL_HWHEEL",
    "REL_WHEEL",
    "BTN_LEFT",
    "BTN_RIGHT",
    "BTN_MIDDLE",
    "KEY_LEFTSHIFT",
]
