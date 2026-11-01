"""
Tests for Replicator Mesh, Network Partitions, and SEC Convergence.
"""

import time
import pytest
from chronos_hlc.replicator import StateReplicatorMesh


def test_mesh_partition_and_convergence():
    mesh = StateReplicatorMesh(["Node-A", "Node-B", "Node-C"])
    a = mesh.nodes["Node-A"]
    b = mesh.nodes["Node-B"]
    c = mesh.nodes["Node-C"]

    # Initial mutations
    a.counter_inc("counter", 10)
    b.register_set("leader", "Node-B")
    c.set_add("active_tasks", "task_init")
    mesh.sync_peers()

    assert mesh.verify_convergence(["counter", "leader", "active_tasks"])

    # Introduce network partition: Node-A isolated from Node-B and Node-C
    mesh.partition("Node-A", "Node-B")
    mesh.partition("Node-A", "Node-C")

    # Concurrent mutations in isolated partitions
    a.counter_inc("counter", 5)
    b.counter_inc("counter", 7)
    a.set_add("active_tasks", "task_alpha")
    c.set_add("active_tasks", "task_gamma")

    mesh.sync_peers()

    # During partition, nodes have diverged
    assert a.counter_get("counter") == 15
    assert b.counter_get("counter") == 17
    assert not mesh.verify_convergence(["counter", "active_tasks"])

    # Heal partition
    mesh.heal_all()
    delivered = mesh.sync_peers()
    assert delivered > 0

    # Strong Eventual Consistency: All nodes converged
    assert mesh.verify_convergence(["counter", "active_tasks"])
    assert a.counter_get("counter") == 22  # 10 + 5 + 7
    assert b.counter_get("counter") == 22
    assert c.counter_get("counter") == 22
    assert a.set_elements("active_tasks") == {"task_init", "task_alpha", "task_gamma"}
