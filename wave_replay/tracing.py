"""Canonical JSONL event tracing with secret-safe payloads."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Iterable

from .normalize import canonical_json, canonical_value, state_hash


class SecretLeakError(AssertionError):
    """Raised if a canary secret reaches a replay artifact."""


class TraceRecorder:
    SCHEMA_VERSION = "1.0.0"

    def __init__(self, *, secrets: Iterable[str] = ()) -> None:
        self._events: list[dict[str, Any]] = []
        self._secrets = tuple(secret for secret in secrets if secret)

    @property
    def events(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._events)

    def add(
        self,
        kind: str,
        *,
        at: dt.datetime,
        actor: str = "coordinator",
        operation: str | None = None,
        caused_by: str | None = None,
        details: dict[str, Any] | None = None,
        input_data: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        state_before: str | None = None,
        state_after: str | None = None,
    ) -> dict[str, Any]:
        event = {
            "schema_version": self.SCHEMA_VERSION,
            "seq": len(self._events) + 1,
            "at": at,
            "actor": actor,
            "kind": kind,
            "operation": operation,
            "caused_by": caused_by,
            "input": input_data or {},
            "result": result or {},
            "state_before": state_before,
            "state_after": state_after,
            "details": details or {},
        }
        self._assert_safe(event)
        normalized = canonical_value(event)
        self._events.append(normalized)
        return normalized

    def digest(self) -> str:
        return state_hash(self._events)

    def write_jsonl(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        text = "".join(canonical_json(event) + "\n" for event in self._events)
        self._assert_safe(text)
        path.write_text(text, encoding="utf-8")
        return path

    def _assert_safe(self, value: object) -> None:
        serialized = value if isinstance(value, str) else json.dumps(canonical_value(value), sort_keys=True)
        for secret in self._secrets:
            if secret in serialized:
                raise SecretLeakError("Canary secret reached a replay artifact")
