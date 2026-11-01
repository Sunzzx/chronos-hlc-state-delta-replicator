"""
Tests for Delta-CRDT Primitives (PNCounter, LWWRegister, ORSet, MVRegister).
"""

import time
import pytest
from chronos_hlc.crdt import PNCounter, LWWRegister, ORSet, MVRegister
from chronos_hlc.clock import HybridLogicalClock, Timestamp


def test_pncounter_properties():
    c1 = PNCounter("n1")
    c2 = PNCounter("n2")

    c1.inc(10)
    c1.dec(3)  # value = 7

    c2.inc(5)
    c2.dec(1)  # value = 4

    # Commutative merge
    merged1 = PNCounter("n1").merge(c1).merge(c2)
    merged2 = PNCounter("n2").merge(c2).merge(c1)

    assert merged1.value == 11
    assert merged2.value == 11

    # Idempotency
    merged1.merge(c1)
    assert merged1.value == 11


def test_lww_register_resolution():
    clock = HybridLogicalClock("n1")
    reg1 = LWWRegister("n1", clock)
    reg2 = LWWRegister("n2", clock)

    ts1 = reg1.set("Initial State")
    time.sleep(0.002)
    ts2 = reg2.set("Updated State")

    assert ts2 > ts1
    updated = reg1.merge(reg2.value, reg2.timestamp)
    assert updated is True
    assert reg1.value == "Updated State"

    # Stale write rejected
    stale_ts = Timestamp(physical_ms=ts1.physical_ms - 500, logical=0, node_id="n3")
    rejected = reg1.merge("Stale Value", stale_ts)
    assert rejected is False
    assert reg1.value == "Updated State"


def test_orset_add_remove():
    s1 = ORSet("n1")
    s2 = ORSet("n2")

    s1.add("item_a")
    s1.add("item_b")
    s2.add("item_c")

    s1.merge(s2)
    assert s1.elements() == {"item_a", "item_b", "item_c"}

    # Remove item_b on s1
    s1.remove("item_b")
    assert not s1.contains("item_b")
    assert s1.elements() == {"item_a", "item_c"}

    # Concurrent add on s2 of same item_b survives removal from s1
    s2.add("item_b")  # Generates fresh tag
    s1.merge(s2)
    assert s1.contains("item_b")


def test_mvregister_concurrent():
    r1 = MVRegister("n1")
    r2 = MVRegister("n2")

    # Concurrent writes
    r1.write("Option A")
    r2.write("Option B")

    r1.merge(r2)
    values = r1.read()
    assert len(values) == 2
    assert "Option A" in values
    assert "Option B" in values

    # Causal overwrite resolves conflict
    r1.write("Reconciled Option C")
    assert r1.read() == ["Reconciled Option C"]
