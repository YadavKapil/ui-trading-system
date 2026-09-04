"""Simulated clock wired to the deterministic task runtime."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from .scheduler import DeterministicScheduler

if TYPE_CHECKING:
    from .tasks import SimTaskRuntime


class SimClock:
    def __init__(self, scheduler: DeterministicScheduler) -> None:
        self.scheduler = scheduler
        self.task_runtime: SimTaskRuntime | None = None

    def bind_task_runtime(self, runtime: "SimTaskRuntime") -> None:
        self.task_runtime = runtime

    def now_ist(self) -> dt.datetime:
        return self.scheduler.now

    def epoch(self) -> float:
        return self.scheduler.now.timestamp()

    def monotonic(self) -> float:
        return self.scheduler.now.timestamp()

    def sleep(self, seconds: float, *, reason: str = "legacy_sleep") -> None:
        if self.task_runtime is None:
            if seconds:
                self.scheduler.now += dt.timedelta(seconds=seconds)
            return
        self.task_runtime.sleep(seconds, reason=reason)
