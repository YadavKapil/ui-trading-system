"""Stable discrete-event scheduler used by all deterministic replay work."""

from __future__ import annotations

import datetime as dt
import heapq
import itertools
from dataclasses import dataclass, field
from typing import Callable

from .tracing import TraceRecorder


class SchedulerError(RuntimeError):
    """Raised for invalid, deadlocked, or runaway schedules."""


@dataclass(order=True)
class ScheduledEvent:
    at: dt.datetime
    priority: int
    sequence: int
    event_id: str = field(compare=False)
    callback: Callable[[], None] = field(compare=False, repr=False)
    cancelled: bool = field(default=False, compare=False)


class DeterministicScheduler:
    def __init__(self, *, start: dt.datetime, trace: TraceRecorder, max_events: int = 100_000) -> None:
        if start.tzinfo is None:
            raise ValueError("Scheduler start must be timezone-aware")
        self.now = start
        self.trace = trace
        self.max_events = max_events
        self._sequence = itertools.count(1)
        self._queue: list[ScheduledEvent] = []
        self._events_by_id: dict[str, ScheduledEvent] = {}
        self.current_event_id: str | None = None
        self.events_executed = 0

    def schedule_at(
        self,
        at: dt.datetime,
        callback: Callable[[], None],
        *,
        priority: int = 100,
        event_id: str | None = None,
    ) -> str:
        if at.tzinfo is None:
            raise ValueError("Scheduled timestamps must be timezone-aware")
        if at < self.now:
            raise SchedulerError(f"Cannot schedule in the past: {at.isoformat()} < {self.now.isoformat()}")
        sequence = next(self._sequence)
        event_id = event_id or f"event-{sequence}"
        if event_id in self._events_by_id:
            raise SchedulerError(f"Duplicate event id: {event_id}")
        event = ScheduledEvent(at, priority, sequence, event_id, callback)
        self._events_by_id[event_id] = event
        heapq.heappush(self._queue, event)
        self.trace.add(
            "EVENT_SCHEDULED",
            at=self.now,
            operation=event_id,
            caused_by=self.current_event_id,
            details={"scheduled_at": at.isoformat(), "priority": priority},
        )
        return event_id

    def schedule_after(
        self,
        seconds: float,
        callback: Callable[[], None],
        *,
        priority: int = 100,
        event_id: str | None = None,
    ) -> str:
        if seconds < 0:
            raise ValueError("Delay cannot be negative")
        return self.schedule_at(
            self.now + dt.timedelta(seconds=seconds),
            callback,
            priority=priority,
            event_id=event_id,
        )

    def cancel(self, event_id: str) -> bool:
        event = self._events_by_id.get(event_id)
        if event is None or event.cancelled:
            return False
        event.cancelled = True
        self.trace.add(
            "EVENT_CANCELLED",
            at=self.now,
            operation=event_id,
            caused_by=self.current_event_id,
        )
        return True

    def run(
        self,
        *,
        until: dt.datetime | None = None,
        predicate: Callable[[], bool] | None = None,
    ) -> int:
        started = self.events_executed
        while self._queue:
            if predicate is not None and predicate():
                break
            event = heapq.heappop(self._queue)
            if event.cancelled:
                continue
            if until is not None and event.at > until:
                heapq.heappush(self._queue, event)
                self.now = until
                break
            if self.events_executed >= self.max_events:
                raise SchedulerError(f"Schedule exceeded max_events={self.max_events}")
            self.now = event.at
            previous = self.current_event_id
            self.current_event_id = event.event_id
            self.trace.add(
                "EVENT_STARTED",
                at=self.now,
                operation=event.event_id,
                caused_by=previous,
                details={"priority": event.priority},
            )
            try:
                event.callback()
            except BaseException as exc:
                self.trace.add(
                    "EVENT_FAILED",
                    at=self.now,
                    operation=event.event_id,
                    details={"error_type": type(exc).__name__, "error": str(exc)},
                )
                raise
            finally:
                self.current_event_id = previous
            self.events_executed += 1
        return self.events_executed - started

    def has_pending(self) -> bool:
        return any(not event.cancelled for event in self._queue)

    def cancel_pending(self, *, reason: str) -> int:
        count = 0
        for event in self._queue:
            if not event.cancelled:
                event.cancelled = True
                count += 1
        self.trace.add(
            "PENDING_EVENTS_CANCELLED",
            at=self.now,
            operation=reason,
            details={"count": count},
        )
        return count

    def pending_snapshot(self) -> list[dict[str, object]]:
        return [
            {
                "event_id": event.event_id,
                "at": event.at.isoformat(),
                "priority": event.priority,
                "sequence": event.sequence,
            }
            for event in sorted(self._queue)
            if not event.cancelled
        ]
