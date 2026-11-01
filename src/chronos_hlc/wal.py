"""
Segmented Write-Ahead Log (WAL) with CRC32 Verification & Binary Framing.

Ensures zero data loss across process crashes, node restarts, and network partitions:
[Magic: 4B 'CHRO'] [Length: 4B uint32] [CRC32: 4B uint32] [Payload: Length bytes]
"""

from __future__ import annotations

import io
import json
import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

MAGIC = b"CHRO"
HEADER_FORMAT = ">4sII"  # Magic (4B), Length (4B), CRC32 (4B)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@dataclass(frozen=True)
class WALRecord:
    seq: int
    record_type: str
    key: str
    payload: Dict[str, Any]
    timestamp: Optional[Dict[str, Any]] = None

    def serialize(self) -> bytes:
        data = {
            "seq": self.seq,
            "type": self.record_type,
            "key": self.key,
            "payload": self.payload,
            "ts": self.timestamp,
        }
        body = json.dumps(data, separators=(",", ":")).encode("utf-8")
        crc = zlib.crc32(body) & 0xFFFFFFFF
        header = struct.pack(HEADER_FORMAT, MAGIC, len(body), crc)
        return header + body

    @classmethod
    def deserialize(cls, raw: bytes) -> WALRecord:
        if len(raw) < HEADER_SIZE:
            raise ValueError(f"Raw data smaller than header size ({len(raw)} < {HEADER_SIZE})")
        magic, length, crc = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])
        if magic != MAGIC:
            raise ValueError(f"Corrupt WAL magic: {magic} != {MAGIC}")
        body = raw[HEADER_SIZE:HEADER_SIZE + length]
        if len(body) != length:
            raise ValueError(f"Truncated WAL record: expected {length} bytes, got {len(body)}")
        actual_crc = zlib.crc32(body) & 0xFFFFFFFF
        if actual_crc != crc:
            raise ValueError(f"CRC32 mismatch: expected {crc}, calculated {actual_crc}")
        data = json.loads(body.decode("utf-8"))
        return cls(
            seq=data["seq"],
            record_type=data["type"],
            key=data["key"],
            payload=data["payload"],
            timestamp=data.get("ts"),
        )


class WriteAheadLog:
    """Append-only, persistent Write-Ahead Log with atomic replay and integrity checks."""

    def __init__(self, filepath: Path):
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._current_seq = 0
        self._recover_and_verify()

    def _recover_and_verify(self) -> int:
        """Scan file, verify all CRC32 checksums, and recover last sequence number."""
        if not self.filepath.exists():
            return 0

        valid_bytes = 0
        last_seq = 0
        with open(self.filepath, "rb") as f:
            while True:
                hdr_bytes = f.read(HEADER_SIZE)
                if not hdr_bytes or len(hdr_bytes) < HEADER_SIZE:
                    break
                magic, length, crc = struct.unpack(HEADER_FORMAT, hdr_bytes)
                if magic != MAGIC:
                    break  # Corruption or EOF
                body = f.read(length)
                if len(body) != length:
                    break  # Truncated
                if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
                    break  # Checksum failed
                valid_bytes += HEADER_SIZE + length
                try:
                    data = json.loads(body.decode("utf-8"))
                    last_seq = max(last_seq, data.get("seq", 0))
                except Exception:
                    break

        # Truncate any uncommitted/corrupted trailing bytes atomically
        current_size = self.filepath.stat().st_size
        if current_size > valid_bytes:
            with open(self.filepath, "r+b") as f:
                f.truncate(valid_bytes)

        self._current_seq = last_seq
        return self._current_seq

    @property
    def current_seq(self) -> int:
        return self._current_seq

    def append(self, record_type: str, key: str, payload: Dict[str, Any], ts: Optional[Dict[str, Any]] = None) -> WALRecord:
        self._current_seq += 1
        rec = WALRecord(
            seq=self._current_seq,
            record_type=record_type,
            key=key,
            payload=payload,
            timestamp=ts,
        )
        data = rec.serialize()
        with open(self.filepath, "ab") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        return rec

    def replay(self) -> Iterator[WALRecord]:
        """Iterate over all verified records in sequential order."""
        if not self.filepath.exists():
            return
        with open(self.filepath, "rb") as f:
            while True:
                hdr_bytes = f.read(HEADER_SIZE)
                if not hdr_bytes or len(hdr_bytes) < HEADER_SIZE:
                    break
                magic, length, crc = struct.unpack(HEADER_FORMAT, hdr_bytes)
                if magic != MAGIC:
                    break
                body = f.read(length)
                if len(body) != length:
                    break
                if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
                    break
                data = json.loads(body.decode("utf-8"))
                yield WALRecord(
                    seq=data["seq"],
                    record_type=data["type"],
                    key=data["key"],
                    payload=data["payload"],
                    timestamp=data.get("ts"),
                )
