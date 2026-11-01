"""
Command-Line Interface (CLI) for Chronos HLC State Delta Replicator.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from .replicator import PeerNode, StateReplicatorMesh


def main():
    parser = argparse.ArgumentParser(
        prog="chronos",
        description="Chronos: Hybrid Logical Clock State Delta Replicator"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # demo command
    sub.add_parser("demo", help="Run interactive partition & convergence simulation")

    # benchmark command
    sub.add_parser("benchmark", help="Run high-throughput CRDT mutation and merge benchmark")

    args = parser.parse_args()

    if args.command == "demo":
        from .demo import run_partition_demo
        run_partition_demo()
    elif args.command == "benchmark":
        from .demo import run_benchmark
        run_benchmark()


if __name__ == "__main__":
    main()
