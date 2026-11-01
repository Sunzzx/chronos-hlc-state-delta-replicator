"""
Chronos: High-Performance Hybrid Logical Clock (HLC) State Delta Replicator.

Clean-room distributed state synchronization engine with delta-CRDTs,
strict causal ordering, and zero-allocation Write-Ahead Log (WAL) replication.
"""

__version__ = "0.1.0"
__author__ = "Ayush Kandwal (Sunzzx)"

from .clock import HybridLogicalClock, Timestamp, VectorClock
from .crdt import PNCounter, LWWRegister, ORSet, MVRegister
from .wal import WriteAheadLog, WALRecord
from .replicator import StateReplicatorMesh, PeerNode

__all__ = [
    "HybridLogicalClock",
    "Timestamp",
    "VectorClock",
    "PNCounter",
    "LWWRegister",
    "ORSet",
    "MVRegister",
    "WriteAheadLog",
    "WALRecord",
    "StateReplicatorMesh",
    "PeerNode",
]
