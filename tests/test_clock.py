"""
Tests for Hybrid Logical Clocks and Vector Clocks.
"""

import time
import pytest
from chronos_hlc.clock import HybridLogicalClock, Timestamp, VectorClock


def test_hlc_monotonicity():
    hlc = HybridLogicalClock("node-1")
    t1 = hlc.now()
    t2 = hlc.now()
    t3 = hlc.now()

    assert t1 < t2 < t3
    assert t1.node_id == "node-1"


def test_hlc_receive_advancement():
    hlc1 = HybridLogicalClock("node-1")
    hlc2 = HybridLogicalClock("node-2")

    t1 = hlc1.now()
    # Artificial future timestamp from node-1
    future_ts = Timestamp(physical_ms=t1.physical_ms + 1000, logical=5, node_id="node-1")

    t2 = hlc2.update(future_ts)
    assert t2 > future_ts
    assert t2.physical_ms >= future_ts.physical_ms


def test_vector_clock_causality():
    v1 = VectorClock("node-1")
    v2 = VectorClock("node-2")

    v1.increment()  # {"node-1": 1}
    assert v1.get("node-1") == 1
    assert v1.get("node-2") == 0

    # v1 happens before v1 after increment
    v1_next = v1.copy().increment()
    assert v1.happens_before(v1_next)

    # Concurrent branches
    v2.increment()  # {"node-2": 1}
    assert v1.is_concurrent_with(v2)
    assert v2.is_concurrent_with(v1)

    # Merge
    v3 = v1.copy().merge(v2)
    assert v1.happens_before(v3)
    assert v2.happens_before(v3)
