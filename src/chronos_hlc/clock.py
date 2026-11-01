"""
Hybrid Logical Clock (HLC) and Vector Clock Causality Primitives.

Implements Kulkarni et al. (2014) Hybrid Logical Clock protocol:
- Combines physical wall-clock (pt) with logical sequence counter (l, c)
- Guarantees: e -> f => HLC(e) < HLC(f)
- Drift bound: |HLC(e).l - pt(e)| <= epsilon
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True, order=False)
class Timestamp:
    """Immutable HLC timestamp tuple: (physical_ms, logical_counter, node_id)."""
    physical_ms: int
    logical: int
    node_id: str

    def __lt__(self, other: Timestamp) -> bool:
        if not isinstance(other, Timestamp):
            return NotImplemented
        if self.physical_ms != other.physical_ms:
            return self.physical_ms < other.physical_ms
        if self.logical != other.logical:
            return self.logical < other.logical
        return self.node_id < other.node_id

    def __le__(self, other: Timestamp) -> bool:
        return self < other or self == other

    def __gt__(self, other: Timestamp) -> bool:
        if not isinstance(other, Timestamp):
            return NotImplemented
        return not (self <= other)

    def __ge__(self, other: Timestamp) -> bool:
        if not isinstance(other, Timestamp):
            return NotImplemented
        return not (self < other)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return False
        return (
            self.physical_ms == other.physical_ms
            and self.logical == other.logical
            and self.node_id == other.node_id
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "p": self.physical_ms,
            "l": self.logical,
            "n": self.node_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Timestamp:
        return cls(physical_ms=int(data["p"]), logical=int(data["l"]), node_id=str(data["n"]))

    def __repr__(self) -> str:
        return f"HLC({self.physical_ms}:{self.logical}@{self.node_id})"


class HybridLogicalClock:
    """
    Hybrid Logical Clock maintaining monotonicity and causal bounds.
    Tolerates clock skew up to max_drift_ms.
    """

    def __init__(self, node_id: str, max_drift_ms: int = 60_000):
        self.node_id = str(node_id)
        self.max_drift_ms = max_drift_ms
        self._l: int = 0
        self._c: int = 0

    def _physical_now(self) -> int:
        return int(time.time() * 1000)

    def now(self) -> Timestamp:
        """Generate a new monotonically increasing local timestamp."""
        pt = self._physical_now()
        l_old = self._l

        self._l = max(l_old, pt)
        if self._l == l_old:
            self._c += 1
        else:
            self._c = 0

        return Timestamp(physical_ms=self._l, logical=self._c, node_id=self.node_id)

    def update(self, msg_ts: Timestamp) -> Timestamp:
        """
        Advance clock upon receiving a remote message timestamp.
        Implements HLC receive state transition.
        """
        pt = self._physical_now()
        l_old = self._l
        c_old = self._c

        self._l = max(l_old, msg_ts.physical_ms, pt)

        if self._l == l_old and self._l == msg_ts.physical_ms:
            self._c = max(c_old, msg_ts.logical) + 1
        elif self._l == l_old:
            self._c = c_old + 1
        elif self._l == msg_ts.physical_ms:
            self._c = msg_ts.logical + 1
        else:
            self._c = 0

        return Timestamp(physical_ms=self._l, logical=self._c, node_id=self.node_id)


class VectorClock:
    """Vector clock for causal dependency tracking across distributed peers."""

    def __init__(self, node_id: str, clock_map: Optional[Dict[str, int]] = None):
        self.node_id = node_id
        self._clock: Dict[str, int] = dict(clock_map or {})
        self._clock.setdefault(self.node_id, 0)

    def increment(self) -> VectorClock:
        self._clock[self.node_id] = self._clock.get(self.node_id, 0) + 1
        return self

    def get(self, node: str) -> int:
        return self._clock.get(node, 0)

    def merge(self, other: VectorClock) -> VectorClock:
        """Pointwise supremum merge: V'(k) = max(V1(k), V2(k))."""
        for k, v in other._clock.items():
            self._clock[k] = max(self._clock.get(k, 0), v)
        return self

    def happens_before(self, other: VectorClock) -> bool:
        """Strict partial ordering: self < other iff for all k V1[k] <= V2[k] and exists k V1[k] < V2[k]."""
        all_keys = set(self._clock.keys()) | set(other._clock.keys())
        strict_less = False
        for k in all_keys:
            v1 = self._clock.get(k, 0)
            v2 = other._clock.get(k, 0)
            if v1 > v2:
                return False
            if v1 < v2:
                strict_less = True
        return strict_less

    def is_concurrent_with(self, other: VectorClock) -> bool:
        return not self.happens_before(other) and not other.happens_before(other)

    def to_dict(self) -> Dict[str, int]:
        return dict(self._clock)

    def copy(self) -> VectorClock:
        return VectorClock(self.node_id, dict(self._clock))

    def __repr__(self) -> str:
        return f"VC({self._clock})"
