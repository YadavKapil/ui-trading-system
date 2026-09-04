"""Canonical normalization helpers for stable replay artifacts."""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def canonical_value(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return canonical_value(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(key): canonical_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, set):
        return sorted(canonical_value(item) for item in value)
    if isinstance(value, dt.datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Path):
        return value.as_posix()
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(canonical_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def state_hash(value: Any) -> str:
    return f"sha256:{sha256_bytes(canonical_json(value).encode('utf-8'))}"


class StableIds:
    """Assign stable aliases to volatile external identifiers."""

    def __init__(self) -> None:
        self._aliases: dict[tuple[str, str], str] = {}
        self._counts: dict[str, int] = {}

    def alias(self, category: str, raw: object) -> str:
        key = (category, str(raw))
        if key not in self._aliases:
            self._counts[category] = self._counts.get(category, 0) + 1
            self._aliases[key] = f"{category.upper()}-{self._counts[category]}"
        return self._aliases[key]

