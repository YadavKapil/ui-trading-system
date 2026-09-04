"""Scheduler-owned KiteTicker callback facade."""

from __future__ import annotations

from typing import Any, Callable

from .scheduler import DeterministicScheduler
from .tasks import SimTaskRuntime
from .tracing import TraceRecorder


class FakeTicker:
    MODE_FULL = "full"

    def __init__(self, scheduler: DeterministicScheduler, tasks: SimTaskRuntime, trace: TraceRecorder) -> None:
        self.scheduler = scheduler
        self.tasks = tasks
        self.trace = trace
        self.on_ticks: Callable[..., Any] | None = None
        self.on_connect: Callable[..., Any] | None = None
        self.on_order_update: Callable[..., Any] | None = None
        self.on_noreconnect: Callable[..., Any] | None = None
        self.subscriptions: list[int] = []
        self.connected = False

    def connect(self, threaded: bool = True) -> None:
        self.connected = True
        if self.on_connect is not None:
            self.deliver("connect", {}, delay=0)

    def subscribe(self, tokens: list[int]) -> None:
        self.subscriptions = list(tokens)

    def set_mode(self, mode: str, tokens: list[int]) -> None:
        self.subscriptions = list(tokens)

    def deliver(self, kind: str, payload: Any, *, delay: float = 0.0, priority: int = 30) -> str:
        callback = {
            "connect": self.on_connect,
            "ticks": self.on_ticks,
            "order": self.on_order_update,
            "noreconnect": self.on_noreconnect,
        }[kind]
        if callback is None:
            return ""

        def register() -> None:
            args = (self, payload) if kind != "noreconnect" else (self,)
            self.tasks.spawn(callback, name=f"ticker-{kind}", args=args, priority=priority)

        return self.scheduler.schedule_after(
            delay,
            register,
            priority=priority,
            event_id=f"ticker-{kind}-{len(self.trace.events) + 1}",
        )

    def order_dispatch(self, payload: dict[str, Any], delay: float, priority: int = 30) -> None:
        self.deliver("order", payload, delay=delay, priority=priority)
