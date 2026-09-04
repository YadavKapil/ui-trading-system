"""Deterministic broker fault scripts."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScriptedFailure:
    operation: str
    error_type: str
    message: str
    apply_side_effect: bool = False


class FaultScript:
    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self._events: dict[str, deque[ScriptedFailure]] = defaultdict(deque)
        for event in events or []:
            if event.get("kind", "failure") != "failure":
                continue
            self.activate(event)

    def activate(self, event: dict[str, Any]) -> None:
        failure = ScriptedFailure(
                operation=str(event["operation"]),
                error_type=str(event.get("error_type", "RuntimeError")),
                message=str(event.get("message", "scripted failure")),
                apply_side_effect=bool(event.get("apply_side_effect", False)),
            )
        self._events[failure.operation].append(failure)

    def pop(self, operation: str) -> ScriptedFailure | None:
        queue = self._events.get(operation)
        return queue.popleft() if queue else None

    def remaining(self) -> dict[str, int]:
        return {operation: len(queue) for operation, queue in self._events.items() if queue}


class BrokerTransportError(ConnectionError):
    """Scripted transport failure."""


class BrokerBusinessError(RuntimeError):
    """Scripted broker rejection."""


def raise_failure(failure: ScriptedFailure) -> None:
    error_cls = BrokerTransportError if failure.error_type == "transport" else BrokerBusinessError
    raise error_cls(failure.message)
