"""
Conflict-Free Replicated Data Types (Delta-CRDTs).

Implements Strong Eventual Consistency (SEC) across:
1. PNCounter: Positive-Negative Distributed Counter
2. LWWRegister: Last-Write-Wins Register with HLC tie-breaking
3. ORSet: Observed-Remove Set with tombstone pruning
4. MVRegister: Multi-Value Register with Vector Clock causality
"""

from __future__ import annotations

import copy
import uuid
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, TypeVar
from .clock import HybridLogicalClock, Timestamp, VectorClock

T = TypeVar("T")


class PNCounter:
    """Positive-Negative Counter CRDT supporting concurrent increments and decrements."""

    def __init__(self, node_id: str):
        self.node_id = str(node_id)
        self.pos: Dict[str, int] = {self.node_id: 0}
        self.neg: Dict[str, int] = {self.node_id: 0}

    def inc(self, delta: int = 1) -> PNCounter:
        if delta < 0:
            return self.dec(-delta)
        self.pos[self.node_id] = self.pos.get(self.node_id, 0) + delta
        return self

    def dec(self, delta: int = 1) -> PNCounter:
        if delta < 0:
            return self.inc(-delta)
        self.neg[self.node_id] = self.neg.get(self.node_id, 0) + delta
        return self

    @property
    def value(self) -> int:
        return sum(self.pos.values()) - sum(self.neg.values())

    def merge(self, other: PNCounter) -> PNCounter:
        """Commutative, associative, and idempotent join: P'(k) = max(P1[k], P2[k])."""
        all_nodes = set(self.pos.keys()) | set(other.pos.keys())
        for n in all_nodes:
            self.pos[n] = max(self.pos.get(n, 0), other.pos.get(n, 0))
            self.neg[n] = max(self.neg.get(n, 0), other.neg.get(n, 0))
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {"pos": dict(self.pos), "neg": dict(self.neg)}

    @classmethod
    def from_dict(cls, node_id: str, data: Dict[str, Any]) -> PNCounter:
        inst = cls(node_id)
        inst.pos = {k: int(v) for k, v in data.get("pos", {}).items()}
        inst.neg = {k: int(v) for k, v in data.get("neg", {}).items()}
        return inst


class LWWRegister(Generic[T]):
    """Last-Write-Wins Register resolved by Hybrid Logical Clock (HLC)."""

    def __init__(self, node_id: str, clock: Optional[HybridLogicalClock] = None):
        self.node_id = node_id
        self.clock = clock or HybridLogicalClock(node_id)
        self.value: Optional[T] = None
        self.timestamp: Optional[Timestamp] = None

    def set(self, val: T) -> Timestamp:
        ts = self.clock.now()
        self.value = val
        self.timestamp = ts
        return ts

    def merge(self, other_val: Optional[T], other_ts: Optional[Timestamp]) -> bool:
        """Merge remote state. Returns True if local value was updated."""
        if other_ts is None:
            return False
        if self.timestamp is None or other_ts > self.timestamp:
            self.value = other_val
            self.timestamp = other_ts
            self.clock.update(other_ts)
            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "val": self.value,
            "ts": self.timestamp.to_dict() if self.timestamp else None,
        }

    @classmethod
    def from_dict(cls, node_id: str, data: Dict[str, Any]) -> LWWRegister:
        inst = cls(node_id)
        inst.value = data.get("val")
        ts_data = data.get("ts")
        if ts_data:
            inst.timestamp = Timestamp.from_dict(ts_data)
        return inst


class ORSet(Generic[T]):
    """
    Observed-Remove Set (ORSet) supporting adds and removes of duplicate elements.
    Each element addition creates a globally unique tag token.
    An element is present iff its added tags are not in the tombstone set.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        # elem -> set of unique tag strings
        self.entries: Dict[T, Set[str]] = {}
        # set of observed tombstones
        self.tombstones: Set[str] = set()

    def add(self, elem: T) -> str:
        tag = f"{self.node_id}:{uuid.uuid4().hex[:12]}"
        self.entries.setdefault(elem, set()).add(tag)
        return tag

    def remove(self, elem: T) -> Set[str]:
        """Tombstone all currently observed tags for elem."""
        tags = self.entries.get(elem, set())
        removed = set(tags)
        self.tombstones.update(tags)
        self.entries.pop(elem, None)
        return removed

    def contains(self, elem: T) -> bool:
        tags = self.entries.get(elem, set())
        active = tags - self.tombstones
        return len(active) > 0

    def elements(self) -> Set[T]:
        res = set()
        for elem, tags in self.entries.items():
            if len(tags - self.tombstones) > 0:
                res.add(elem)
        return res

    def merge(self, other: ORSet[T]) -> ORSet[T]:
        """Join of ORSets is set union of entries and set union of tombstones."""
        self.tombstones.update(other.tombstones)
        for elem, tags in other.entries.items():
            self.entries.setdefault(elem, set()).update(tags)

        # Garbage collect completely tombstoned entries
        for elem in list(self.entries.keys()):
            self.entries[elem] = self.entries[elem] - self.tombstones
            if not self.entries[elem]:
                self.entries.pop(elem, None)

        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entries": {str(k): list(v) for k, v in self.entries.items()},
            "tombstones": list(self.tombstones),
        }


class MVRegister(Generic[T]):
    """
    Multi-Value Register (MVRegister).
    Maintains multiple concurrent values when causal ordering is ambiguous.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        # List of tuples: (value, VectorClock)
        self.values: List[Tuple[T, VectorClock]] = []
        self.clock = VectorClock(node_id)

    def write(self, val: T) -> VectorClock:
        self.clock.increment()
        self.values = [(val, self.clock.copy())]
        return self.clock.copy()

    def read(self) -> List[T]:
        return [v for v, _ in self.values]

    def merge(self, other: MVRegister[T]) -> MVRegister[T]:
        merged_values: List[Tuple[T, VectorClock]] = []
        all_candidates = self.values + other.values

        for val_a, clock_a in all_candidates:
            # Check if clock_a is superseded by any other clock
            superseded = False
            for val_b, clock_b in all_candidates:
                if clock_a.happens_before(clock_b):
                    superseded = True
                    break
            if not superseded:
                # Add if not duplicate
                if not any(v == val_a and c.to_dict() == clock_a.to_dict() for v, c in merged_values):
                    merged_values.append((val_a, clock_a))

        self.values = merged_values
        self.clock.merge(other.clock)
        return self
