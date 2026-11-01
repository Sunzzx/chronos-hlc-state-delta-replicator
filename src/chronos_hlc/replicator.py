"""
P2P Anti-Entropy Delta State Gossip Engine & Replicator Mesh.

Coordinates peer nodes, replicates state deltas, tolerates network partitions,
and ensures convergence via Strong Eventual Consistency (SEC).
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .clock import HybridLogicalClock, Timestamp, VectorClock
from .crdt import LWWRegister, ORSet, PNCounter
from .wal import WALRecord, WriteAheadLog

logger = logging.getLogger("chronos.replicator")


@dataclass
class StateDelta:
    origin_node: str
    target_crdt: str  # "counter", "register", "set"
    key: str
    delta_payload: Dict[str, Any]
    timestamp: Timestamp
    seq: int


class PeerNode:
    """Individual distributed node with local HLC, CRDT storage, and WAL persistence."""

    def __init__(self, node_id: str, data_dir: Optional[Path] = None):
        self.node_id = str(node_id)
        self.clock = HybridLogicalClock(self.node_id)
        self.vector_clock = VectorClock(self.node_id)
        self.data_dir = data_dir

        self.wal: Optional[WriteAheadLog] = None
        if self.data_dir:
            self.wal = WriteAheadLog(self.data_dir / f"{self.node_id}.wal")

        # Local CRDT Stores
        self.counters: Dict[str, PNCounter] = {}
        self.registers: Dict[str, LWWRegister[Any]] = {}
        self.sets: Dict[str, ORSet[Any]] = {}

        # Delta log and per-peer acknowledgment cursors
        self.outbox_deltas: List[StateDelta] = []
        self.peer_ack_seq: Dict[str, int] = {}
        self._seq = 0

    # -------------------------------------------------------------------------
    # Counter Operations
    # -------------------------------------------------------------------------
    def counter_inc(self, key: str, amount: int = 1) -> int:
        c = self.counters.setdefault(key, PNCounter(self.node_id))
        c.inc(amount)
        ts = self.clock.now()
        self.vector_clock.increment()
        self._record_delta("counter", key, c.to_dict(), ts)
        return c.value

    def counter_dec(self, key: str, amount: int = 1) -> int:
        c = self.counters.setdefault(key, PNCounter(self.node_id))
        c.dec(amount)
        ts = self.clock.now()
        self.vector_clock.increment()
        self._record_delta("counter", key, c.to_dict(), ts)
        return c.value

    def counter_get(self, key: str) -> int:
        c = self.counters.get(key)
        return c.value if c else 0

    # -------------------------------------------------------------------------
    # Register Operations
    # -------------------------------------------------------------------------
    def register_set(self, key: str, value: Any) -> Timestamp:
        reg = self.registers.setdefault(key, LWWRegister(self.node_id, self.clock))
        ts = reg.set(value)
        self.vector_clock.increment()
        self._record_delta("register", key, reg.to_dict(), ts)
        return ts

    def register_get(self, key: str) -> Optional[Any]:
        reg = self.registers.get(key)
        return reg.value if reg else None

    # -------------------------------------------------------------------------
    # Set Operations
    # -------------------------------------------------------------------------
    def set_add(self, key: str, element: Any) -> str:
        s = self.sets.setdefault(key, ORSet(self.node_id))
        tag = s.add(element)
        ts = self.clock.now()
        self.vector_clock.increment()
        self._record_delta("set", key, s.to_dict(), ts)
        return tag

    def set_remove(self, key: str, element: Any) -> Set[str]:
        s = self.sets.setdefault(key, ORSet(self.node_id))
        removed = s.remove(element)
        ts = self.clock.now()
        self.vector_clock.increment()
        self._record_delta("set", key, s.to_dict(), ts)
        return removed

    def set_elements(self, key: str) -> Set[Any]:
        s = self.sets.get(key)
        return s.elements() if s else set()

    # -------------------------------------------------------------------------
    # Internal Delta & Replication
    # -------------------------------------------------------------------------
    def _record_delta(self, target_crdt: str, key: str, payload: Dict[str, Any], ts: Timestamp) -> None:
        self._seq += 1
        delta = StateDelta(
            origin_node=self.node_id,
            target_crdt=target_crdt,
            key=key,
            delta_payload=payload,
            timestamp=ts,
            seq=self._seq,
        )
        self.outbox_deltas.append(delta)
        if self.wal:
            self.wal.append(
                record_type=f"{target_crdt}_delta",
                key=key,
                payload=payload,
                ts=ts.to_dict(),
            )

    def receive_delta(self, delta: StateDelta) -> bool:
        """Merge a received delta into local CRDT storage. Returns True if state changed."""
        self.clock.update(delta.timestamp)

        if delta.target_crdt == "counter":
            c = self.counters.setdefault(delta.key, PNCounter(self.node_id))
            remote = PNCounter.from_dict(delta.origin_node, delta.delta_payload)
            c.merge(remote)
            return True

        elif delta.target_crdt == "register":
            reg = self.registers.setdefault(delta.key, LWWRegister(self.node_id, self.clock))
            val = delta.delta_payload.get("val")
            ts = delta.timestamp
            return reg.merge(val, ts)

        elif delta.target_crdt == "set":
            s = self.sets.setdefault(delta.key, ORSet(self.node_id))
            remote_entries = delta.delta_payload.get("entries", {})
            remote_tombstones = set(delta.delta_payload.get("tombstones", []))
            s.tombstones.update(remote_tombstones)
            for elem, tags in remote_entries.items():
                s.entries.setdefault(elem, set()).update(tags)
            return True

        return False


class StateReplicatorMesh:
    """Simulates a distributed mesh of peer nodes with network partition simulation."""

    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, PeerNode] = {nid: PeerNode(nid) for nid in node_ids}
        # Disallowed peer communication pairs: set of frozenset({node_a, node_b})
        self.partitions: Set[frozenset] = set()

    def partition(self, node_a: str, node_b: str) -> None:
        self.partitions.add(frozenset([node_a, node_b]))

    def heal_partition(self, node_a: str, node_b: str) -> None:
        self.partitions.discard(frozenset([node_a, node_b]))

    def heal_all(self) -> None:
        self.partitions.clear()

    def sync_peers(self) -> int:
        """
        Gossip round: each node propagates unacknowledged deltas to reachable peers.
        Tracks per-peer acknowledgment cursors so network partitions do not lose deltas.
        Returns total number of successfully delivered deltas.
        """
        delivered = 0
        for src_id, src_node in self.nodes.items():
            for dst_id, dst_node in self.nodes.items():
                if src_id == dst_id:
                    continue
                if frozenset([src_id, dst_id]) in self.partitions:
                    continue  # Network partition: drop packet
                last_ack = src_node.peer_ack_seq.get(dst_id, 0)
                pending = [d for d in src_node.outbox_deltas if d.seq > last_ack]
                for delta in pending:
                    dst_node.receive_delta(delta)
                    src_node.peer_ack_seq[dst_id] = delta.seq
                    delivered += 1
        return delivered

    def verify_convergence(self, keys: List[str]) -> bool:
        """Verifies that all nodes hold identical state for the specified keys."""
        if not self.nodes:
            return True

        node_list = list(self.nodes.values())
        base = node_list[0]

        for other in node_list[1:]:
            for k in keys:
                # Compare counters
                if base.counter_get(k) != other.counter_get(k):
                    return False
                # Compare registers
                if base.register_get(k) != other.register_get(k):
                    return False
                # Compare sets
                if base.set_elements(k) != other.set_elements(k):
                    return False
        return True
