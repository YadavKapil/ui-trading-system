"""Stack-preserving cooperative replacements for Wave thread and timer work."""

from __future__ import annotations

import contextlib
import dataclasses
import _thread
import concurrent.futures
import threading
from collections.abc import Callable, Iterator
from typing import Any

from greenlet import getcurrent, greenlet

from .scheduler import DeterministicScheduler, SchedulerError
from .tracing import TraceRecorder


class UncontrolledConcurrencyError(RuntimeError):
    """Raised when Wave attempts to escape the cooperative task runtime."""


@dataclasses.dataclass
class SimTask:
    task_id: str
    name: str
    target: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    daemon: bool
    parent_task_id: str | None
    state: str = "new"
    result: Any = None
    exception: BaseException | None = None
    greenlet: greenlet | None = None
    wake_event_id: str | None = None
    join_waiters: set[str] = dataclasses.field(default_factory=set)


class SimThread:
    def __init__(
        self,
        runtime: "SimTaskRuntime",
        group: object = None,
        target: Callable[..., Any] | None = None,
        name: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        if group is not None:
            raise ValueError("group must be None")
        self._runtime = runtime
        self._target = target or (lambda: None)
        self._args = tuple(args)
        self._kwargs = dict(kwargs or {})
        self.name = name or runtime.next_name("Thread")
        self.daemon = bool(daemon)
        self._task_id: str | None = None

    @property
    def ident(self) -> str | None:
        return self._task_id

    def start(self) -> None:
        if self._task_id is not None:
            raise RuntimeError("threads can only be started once")
        self._task_id = self._runtime.spawn(
            self._target,
            name=self.name,
            args=self._args,
            kwargs=self._kwargs,
            daemon=self.daemon,
        )

    def join(self, timeout: float | None = None) -> None:
        if self._task_id is None:
            raise RuntimeError("cannot join thread before it is started")
        self._runtime.join(self._task_id, timeout)

    def is_alive(self) -> bool:
        return self._task_id is not None and self._runtime.is_alive(self._task_id)


class SimTimer(SimThread):
    def __init__(
        self,
        runtime: "SimTaskRuntime",
        interval: float,
        function: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            runtime,
            target=function,
            name=runtime.next_name("Timer"),
            args=tuple(args or ()),
            kwargs=kwargs,
            daemon=True,
        )
        self.interval = float(interval)
        self.finished = False

    def start(self) -> None:
        if self._task_id is not None:
            raise RuntimeError("threads can only be started once")
        self._task_id = self._runtime.spawn(
            self._target,
            name=self.name,
            args=self._args,
            kwargs=self._kwargs,
            daemon=True,
            delay=self.interval,
        )

    def cancel(self) -> None:
        if self._task_id is not None:
            self.finished = self._runtime.cancel(self._task_id)


class SimTaskRuntime:
    ALIVE_STATES = {"scheduled", "running", "sleeping", "blocked"}

    def __init__(self, scheduler: DeterministicScheduler, trace: TraceRecorder) -> None:
        self.scheduler = scheduler
        self.trace = trace
        self._root = getcurrent()
        self._tasks: dict[str, SimTask] = {}
        self._task_counter = 0
        self._name_counter = 0
        self.current_task_id: str | None = None
        self._original_thread = threading.Thread
        self._original_timer = threading.Timer
        self._original_start_new_thread = _thread.start_new_thread
        self._original_threading_start_new_thread = threading._start_new_thread
        self._original_start_new = getattr(_thread, "start_new", None)
        self._original_executor_submit = concurrent.futures.ThreadPoolExecutor.submit
        self._patched = False

    @property
    def tasks(self) -> dict[str, SimTask]:
        return dict(self._tasks)

    def next_name(self, prefix: str) -> str:
        self._name_counter += 1
        return f"{prefix}-{self._name_counter}"

    def spawn(
        self,
        target: Callable[..., Any],
        *,
        name: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        daemon: bool = False,
        delay: float = 0.0,
        priority: int = 40,
    ) -> str:
        self._task_counter += 1
        task_id = f"task-{self._task_counter}"
        task = SimTask(
            task_id=task_id,
            name=name,
            target=target,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            daemon=daemon,
            parent_task_id=self.current_task_id,
            state="scheduled",
        )
        self._tasks[task_id] = task
        task.wake_event_id = self.scheduler.schedule_after(
            delay,
            lambda: self._resume(task_id),
            priority=priority,
            event_id=f"resume-{task_id}-start",
        )
        self.trace.add(
            "TASK_REGISTERED",
            at=self.scheduler.now,
            actor=task_id,
            caused_by=self.scheduler.current_event_id,
            details={"name": name, "daemon": daemon, "delay": delay, "parent_task_id": self.current_task_id},
        )
        return task_id

    def sleep(self, seconds: float, *, reason: str = "sleep") -> None:
        if self.current_task_id is None:
            if seconds < 0:
                raise ValueError("sleep length must be non-negative")
            self.scheduler.now += __import__("datetime").timedelta(seconds=seconds)
            return
        task = self._tasks[self.current_task_id]
        task.state = "sleeping"
        task.wake_event_id = self.scheduler.schedule_after(
            max(0.0, float(seconds)),
            lambda task_id=task.task_id: self._resume(task_id),
            priority=40,
            event_id=f"resume-{task.task_id}-{self.scheduler.events_executed + 1}",
        )
        self.trace.add(
            "TASK_BLOCKED",
            at=self.scheduler.now,
            actor=task.task_id,
            operation=reason,
            details={"seconds": float(seconds), "wake_event_id": task.wake_event_id},
        )
        getcurrent().parent.switch()

    def join(self, task_id: str, timeout: float | None = None) -> None:
        target = self._tasks[task_id]
        if not self.is_alive(task_id):
            return
        if self.current_task_id is None:
            deadline = (
                None
                if timeout is None
                else self.scheduler.now + __import__("datetime").timedelta(seconds=timeout)
            )
            self.scheduler.run(until=deadline, predicate=lambda: not self.is_alive(task_id))
            if timeout is None and self.is_alive(task_id):
                raise SchedulerError(f"Task {task_id} deadlocked during join")
            return
        waiter = self._tasks[self.current_task_id]
        target.join_waiters.add(waiter.task_id)
        waiter.state = "blocked"
        self.trace.add(
            "TASK_JOINED",
            at=self.scheduler.now,
            actor=waiter.task_id,
            operation=task_id,
            details={"timeout": timeout},
        )
        if timeout is not None:
            self.scheduler.schedule_after(
                timeout,
                lambda: self._wake_joiner(waiter.task_id, task_id, timed_out=True),
                priority=45,
                event_id=f"join-timeout-{waiter.task_id}-{task_id}",
            )
        getcurrent().parent.switch()

    def is_alive(self, task_id: str) -> bool:
        return self._tasks[task_id].state in self.ALIVE_STATES

    def cancel(self, task_id: str) -> bool:
        task = self._tasks[task_id]
        if task.state != "scheduled" or task.wake_event_id is None:
            return False
        if not self.scheduler.cancel(task.wake_event_id):
            return False
        task.state = "cancelled"
        self.trace.add("TASK_CANCELLED", at=self.scheduler.now, actor=task_id)
        return True

    def run(self, *, until=None, allow_incomplete: bool = False) -> int:
        count = self.scheduler.run(until=until)
        live = [task.task_id for task in self._tasks.values() if self.is_alive(task.task_id)]
        if live and not allow_incomplete:
            raise SchedulerError(f"Scheduler drained with live tasks: {live}")
        return count

    def cancel_all(self, *, reason: str) -> None:
        self.scheduler.cancel_pending(reason=reason)
        for task in self._tasks.values():
            if self.is_alive(task.task_id):
                if task.greenlet is not None and not task.greenlet.dead:
                    task.greenlet.throw(SystemExit(0))
                task.state = "cancelled"
                self.trace.add(
                    "TASK_CANCELLED",
                    at=self.scheduler.now,
                    actor=task.task_id,
                    operation=reason,
                )

    def _resume(self, task_id: str) -> None:
        task = self._tasks[task_id]
        if task.state in {"completed", "cancelled", "failed"}:
            return
        if task.greenlet is None:
            task.greenlet = greenlet(lambda: self._task_main(task_id), parent=self._root)
        previous = self.current_task_id
        self.current_task_id = task_id
        task.state = "running"
        self.trace.add("TASK_STARTED", at=self.scheduler.now, actor=task_id, details={"name": task.name})
        try:
            task.greenlet.switch()
        finally:
            self.current_task_id = previous
        if not task.greenlet.dead and task.state == "running":
            raise SchedulerError(f"Task {task_id} yielded without a blocked state")

    def _task_main(self, task_id: str) -> None:
        task = self._tasks[task_id]
        try:
            task.result = task.target(*task.args, **task.kwargs)
        except SystemExit as exc:
            if exc.code in (None, 0):
                task.result = exc.code
                task.state = "completed"
                self.trace.add(
                    "TASK_COMPLETED",
                    at=self.scheduler.now,
                    actor=task_id,
                    operation="system_exit_0",
                )
            else:
                task.exception = exc
                task.state = "failed"
                self.trace.add(
                    "TASK_FAILED",
                    at=self.scheduler.now,
                    actor=task_id,
                    details={"error_type": type(exc).__name__, "error": str(exc)},
                )
        except BaseException as exc:
            task.exception = exc
            task.state = "failed"
            self.trace.add(
                "TASK_FAILED",
                at=self.scheduler.now,
                actor=task_id,
                details={"error_type": type(exc).__name__, "error": str(exc)},
            )
        else:
            task.state = "completed"
            self.trace.add("TASK_COMPLETED", at=self.scheduler.now, actor=task_id)
        for waiter_id in sorted(task.join_waiters):
            self.scheduler.schedule_after(
                0,
                lambda waiter_id=waiter_id: self._wake_joiner(waiter_id, task_id, timed_out=False),
                priority=45,
            )

    def _wake_joiner(self, waiter_id: str, target_id: str, *, timed_out: bool) -> None:
        waiter = self._tasks[waiter_id]
        if waiter.state != "blocked":
            return
        self._tasks[target_id].join_waiters.discard(waiter_id)
        waiter.state = "scheduled"
        self.trace.add(
            "TASK_WOKE",
            at=self.scheduler.now,
            actor=waiter_id,
            operation="join_timeout" if timed_out else "join_complete",
            details={"target": target_id},
        )
        self.scheduler.schedule_after(0, lambda: self._resume(waiter_id), priority=40)

    @contextlib.contextmanager
    def patched_stdlib(self) -> Iterator[None]:
        if self._patched:
            raise RuntimeError("Task runtime is already patched")
        runtime = self

        def thread_factory(*args: Any, **kwargs: Any) -> SimThread:
            return SimThread(runtime, *args, **kwargs)

        def timer_factory(interval: float, function: Callable[..., Any], args=None, kwargs=None) -> SimTimer:
            return SimTimer(runtime, interval, function, args=args, kwargs=kwargs)

        threading.Thread = thread_factory  # type: ignore[assignment]
        threading.Timer = timer_factory  # type: ignore[assignment]
        _thread.start_new_thread = lambda *args, **kwargs: (_ for _ in ()).throw(
            UncontrolledConcurrencyError("direct _thread.start_new_thread is denied during replay")
        )
        threading._start_new_thread = _thread.start_new_thread
        if hasattr(_thread, "start_new"):
            _thread.start_new = _thread.start_new_thread
        concurrent.futures.ThreadPoolExecutor.submit = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            UncontrolledConcurrencyError("ThreadPoolExecutor is denied during replay")
        )
        self._patched = True
        try:
            yield
        finally:
            _thread.start_new_thread = self._original_start_new_thread
            threading._start_new_thread = self._original_threading_start_new_thread
            if self._original_start_new is not None:
                _thread.start_new = self._original_start_new
            concurrent.futures.ThreadPoolExecutor.submit = self._original_executor_submit
            threading.Timer = self._original_timer
            threading.Thread = self._original_thread
            self._patched = False
