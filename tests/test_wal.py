"""
Tests for Write-Ahead Log (WAL), CRC32 validation, and crash recovery.
"""

from pathlib import Path
import pytest
from chronos_hlc.wal import WriteAheadLog, WALRecord


def test_wal_append_and_replay(tmp_path):
    wal_file = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_file)

    wal.append("set", "user:1", {"name": "Alice"})
    wal.append("set", "user:2", {"name": "Bob"})

    records = list(wal.replay())
    assert len(records) == 2
    assert records[0].seq == 1
    assert records[0].key == "user:1"
    assert records[0].payload == {"name": "Alice"}
    assert records[1].seq == 2
    assert records[1].key == "user:2"


def test_wal_corruption_recovery(tmp_path):
    wal_file = tmp_path / "test_corrupt.wal"
    wal = WriteAheadLog(wal_file)

    wal.append("set", "key1", {"v": 1})
    wal.append("set", "key2", {"v": 2})

    # Corrupt last bytes of the file
    with open(wal_file, "ab") as f:
        f.write(b"CORRUPTED_TRAILING_GARBAGE_BYTES")

    # Reopen WAL: recovery should truncate corrupted tail cleanly
    recovered_wal = WriteAheadLog(wal_file)
    records = list(recovered_wal.replay())
    assert len(records) == 2
    assert records[1].key == "key2"

    # Subsequent append continues monotonic sequence
    rec3 = recovered_wal.append("set", "key3", {"v": 3})
    assert rec3.seq == 3
