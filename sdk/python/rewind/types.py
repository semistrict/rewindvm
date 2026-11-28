"""Data types and errors for the async Rewind cluster SDK."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


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
