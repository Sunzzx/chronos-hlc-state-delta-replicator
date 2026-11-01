"""
Visual Simulation & Benchmark Demo for Chronos Replicator Mesh.
"""

from __future__ import annotations

import time
from .replicator import StateReplicatorMesh


def run_partition_demo():
    print("======================================================================")
    print("🌐 CHRONOS DISTRIBUTED REPLICATOR: SPLIT-BRAIN & CONVERGENCE DEMO")
    print("======================================================================")

    mesh = StateReplicatorMesh(["Node-Alpha", "Node-Beta", "Node-Gamma"])
    alpha = mesh.nodes["Node-Alpha"]
    beta = mesh.nodes["Node-Beta"]
    gamma = mesh.nodes["Node-Gamma"]

    print("\n[Phase 1] Initial Synchronous Mutations:")
    alpha.counter_inc("cluster_ops", 10)
    beta.set_add("active_services", "auth-gateway")
    gamma.register_set("cluster_leader", "Node-Gamma")

    delivered = mesh.sync_peers()
    print(f"  • Propagated {delivered} deltas across 3-node full mesh.")
    print(f"  • Alpha counter: {alpha.counter_get('cluster_ops')}")
    print(f"  • Beta services: {beta.set_elements('active_services')}")
    print(f"  • Gamma leader:  {gamma.register_get('cluster_leader')}")
    print(f"  • Convergence Check: {'PASSED ✅' if mesh.verify_convergence(['cluster_ops', 'active_services', 'cluster_leader']) else 'FAILED ❌'}")

    print("\n[Phase 2] Simulating Network Partition (Split-Brain):")
    print("  ⚡ Severing link: Node-Alpha <---> Node-Beta and Node-Gamma")
    mesh.partition("Node-Alpha", "Node-Beta")
    mesh.partition("Node-Alpha", "Node-Gamma")

    print("\n[Phase 3] Concurrent Mutations on Isolated Partitions:")
    # Partition 1 (Alpha in isolation)
    alpha.counter_inc("cluster_ops", 50)
    alpha.set_add("active_services", "metrics-collector")
    alpha.register_set("cluster_leader", "Node-Alpha")  # Alpha claims leadership

    # Partition 2 (Beta & Gamma interconnected)
    beta.counter_inc("cluster_ops", 25)
    gamma.set_add("active_services", "payment-service")
    time.sleep(0.002)  # Advance wall clock slightly
    gamma.register_set("cluster_leader", "Node-Beta")   # Beta elected leader

    # Sync within partitions
    mesh.sync_peers()
    print("  • Alpha state (Isolated Partition 1):")
    print(f"    - counter:  {alpha.counter_get('cluster_ops')} (Expected 60)")
    print(f"    - services: {alpha.set_elements('active_services')}")
    print(f"    - leader:   {alpha.register_get('cluster_leader')}")

    print("  • Beta state (Partition 2):")
    print(f"    - counter:  {beta.counter_get('cluster_ops')} (Expected 35)")
    print(f"    - services: {beta.set_elements('active_services')}")
    print(f"    - leader:   {beta.register_get('cluster_leader')}")

    print("\n[Phase 4] Healing Partition & Reconciling State via HLC + CRDTs:")
    print("  ⚡ Restoring all network routes...")
    mesh.heal_all()
    converged_deltas = mesh.sync_peers()
    print(f"  • Reconciled {converged_deltas} partition-crossing deltas.")

    converged = mesh.verify_convergence(["cluster_ops", "active_services", "cluster_leader"])
    print(f"\n[Result] Convergence Verified: {'PASSED ✅' if converged else 'FAILED ❌'}")
    print(f"  • Final Global Counter (all nodes):  {alpha.counter_get('cluster_ops')} (10 + 50 + 25 = 85)")
    print(f"  • Final Global Set (all nodes):      {alpha.set_elements('active_services')}")
    print(f"  • Final Global Register (HLC winner): {alpha.register_get('cluster_leader')}")
    print("======================================================================\n")


def run_benchmark(num_ops: int = 50_000):
    print(f"[*] Running Chronos In-Memory Benchmark ({num_ops} operations)...")
    mesh = StateReplicatorMesh(["Node-A", "Node-B"])
    node_a = mesh.nodes["Node-A"]
    node_b = mesh.nodes["Node-B"]

    start = time.perf_counter()
    for i in range(num_ops):
        node_a.counter_inc("bench_metric", 1)
        if i % 10_000 == 0:
            node_b.register_set("status", f"step_{i}")

    mesh.sync_peers()
    elapsed = time.perf_counter() - start
    ops_per_sec = num_ops / elapsed
    print(f"[✔] Benchmark Finished: {num_ops} ops in {elapsed:.4f}s ({ops_per_sec:,.0f} ops/sec)")
    print(f"  • Final Counter Value: {node_b.counter_get('bench_metric')}")
    print(f"  • Final Register State: {node_b.register_get('status')}")
