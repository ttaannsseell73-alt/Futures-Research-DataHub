from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import tomllib
from datetime import UTC, datetime
from pathlib import Path

INTERVALS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
KINDS = ("ohlcv", "mark_price", "index_price", "funding")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def utcnow():
    return datetime.now(UTC).isoformat()


def millis(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timestamp requires explicit UTC offset, e.g. 2026-01-01T00:00:00Z")
    return int(dt.timestamp() * 1000)


def safe_name(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", value):
        raise ValueError("Unsafe identifier")
    return value


def safe_symbol(value):
    """Allow exchange symbols including Unicode while blocking path/control injection."""
    if not isinstance(value, str) or not value or len(value) > 100:
        raise ValueError("Unsafe symbol")
    forbidden = '/\\<>:"|?*\x00'
    if value in {".", ".."} or any(ch in value for ch in forbidden):
        raise ValueError("Unsafe symbol")
    if value.endswith((".", " ")) or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise ValueError("Unsafe symbol")
    return value


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(canonical(value))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        Path(temp).unlink(missing_ok=True)


def data_root(explicit=None, config=None):
    configured = {}
    if config:
        configured = tomllib.loads(Path(config).read_text(encoding="utf-8"))
    value = explicit or os.getenv("DATAHUB_DATA_ROOT") or configured.get("data_root")
    if not value:
        raise ValueError("Set --root, DATAHUB_DATA_ROOT, or --config; no implicit data directory")
    root = Path(value).expanduser().resolve()
    for parent in [root, *root.parents]:
        if (parent / ".git").exists():
            raise ValueError("Data root must be outside every Git working tree")
    root.mkdir(parents=True, exist_ok=True)
    return root
