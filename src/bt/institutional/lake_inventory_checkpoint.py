"""Private local inventory checkpoints, not execution-integrity attestations."""

import json
import os
import sqlite3
import stat
from pathlib import Path


INVENTORY_SEMANTICS_VERSION = "lake-inventory-semantics-v1"


def fingerprint(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


class InventoryCheckpoint:
    def __init__(
        self,
        path: Path,
        *,
        root: Path,
        semantics_version: str = INVENTORY_SEMANTICS_VERSION,
    ):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077):
                raise ValueError("inventory checkpoint must be private and operator-owned")
        finally:
            os.close(descriptor)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS objects (binding TEXT, path TEXT, fingerprint TEXT, "
            "item TEXT, PRIMARY KEY(binding, path))"
        )
        if not semantics_version:
            raise ValueError("inventory semantics version is required")
        self.binding = json.dumps(
            [str(root.resolve(strict=True)), semantics_version, "inventory-checkpoint-v2"]
        )
        self.reused = 0
        self.written = 0

    def get(self, relative: Path, info):
        row = self.connection.execute(
            "SELECT fingerprint, item FROM objects WHERE binding=? AND path=?",
            (self.binding, relative.as_posix()),
        ).fetchone()
        if row is None or json.loads(row[0]) != fingerprint(info):
            return None
        item = json.loads(row[1])
        if (item.get("partition_id") != relative.as_posix()
                or item.get("execution_eligible") is not False):
            raise ValueError("invalid inventory checkpoint record")
        self.reused += 1
        return item

    def put(self, relative: Path, info, item):
        self.connection.execute(
            "INSERT OR REPLACE INTO objects VALUES (?, ?, ?, ?)",
            (self.binding, relative.as_posix(), json.dumps(fingerprint(info)),
             json.dumps(item, sort_keys=True, allow_nan=False)),
        )
        self.connection.commit()
        self.written += 1

    def close(self):
        self.connection.close()
