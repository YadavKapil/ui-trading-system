import datetime as dt
import _thread
import concurrent.futures
import threading

import pytest

from wave_replay.clock import SimClock
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.scheduler import SchedulerError
from wave_replay.schemas import IST
from wave_replay.tasks import SimTaskRuntime, UncontrolledConcurrencyError
from wave_replay.tracing import TraceRecorder


def _runtime():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 1, 1, 9, 15, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    return trace, scheduler, tasks, clock


def test_threads_sleep_and_join_are_cooperative_and_deterministic():
    def execute():
        trace, scheduler, tasks, clock = _runtime()
        order = []
        host_threads = {thread.ident for thread in threading.enumerate()}

        def child():
            order.append("child-start")
            clock.sleep(5)
            order.append("child-end")

        def parent():
            thread = threading.Thread(target=child, name="delayed-refresh", daemon=True)
            thread.start()
            order.append(("alive", thread.is_alive()))
            thread.join()
            order.append("joined")

        with tasks.patched_stdlib():
            tasks.spawn(parent, name="parent")
            scheduler.run()
        assert {thread.ident for thread in threading.enumerate()} == host_threads
        assert order == [("alive", True), "child-start", "child-end", "joined"]
        return trace.digest()

    assert execute() == execute()


def test_timer_cancel_and_timer_fire_use_simulated_time():
    trace, scheduler, tasks, _clock = _runtime()
    fired = []
    with tasks.patched_stdlib():
        cancelled = threading.Timer(1, lambda: fired.append("cancelled"))
        live = threading.Timer(2, lambda: fired.append("live"))
        cancelled.start()
        live.start()
        cancelled.cancel()
        scheduler.run()
    assert fired == ["live"]
    assert any(event["kind"] == "TASK_CANCELLED" for event in trace.events)


def test_nested_and_simultaneous_callback_poll_order_is_scheduler_controlled():
    trace, scheduler, tasks, clock = _runtime()
    observed = []

    def callback():
        observed.append("callback-start")
        tasks.spawn(lambda: observed.append("nested"), name="nested", priority=30)
        clock.sleep(0)
        observed.append("callback-end")

    scheduler.schedule_after(1, lambda: tasks.spawn(callback, name="callback", priority=30), priority=30)
    scheduler.schedule_after(1, lambda: tasks.spawn(lambda: observed.append("poll"), name="poll", priority=60), priority=60)
    scheduler.run()
    assert observed == ["callback-start", "nested", "callback-end", "poll"]
    assert all(event["schema_version"] == "1.0.0" for event in trace.events)


def test_root_join_respects_simulated_timeout():
    _trace, scheduler, tasks, clock = _runtime()
    task_id = tasks.spawn(lambda: clock.sleep(10), name="slow")
    tasks.join(task_id, timeout=1)
    assert tasks.is_alive(task_id)
    assert scheduler.now.second == 1
    tasks.run()
    assert not tasks.is_alive(task_id)


def test_scheduler_reports_deadlocked_cooperative_task():
    _trace, _scheduler, tasks, _clock = _runtime()
    box = {}

    def self_join():
        tasks.join(box["task_id"])

    box["task_id"] = tasks.spawn(self_join, name="self-join")
    with pytest.raises(SchedulerError, match="live tasks"):
        tasks.run()


def test_low_level_thread_aliases_and_executor_are_fail_closed():
    _trace, _scheduler, tasks, _clock = _runtime()
    with tasks.patched_stdlib():
        with pytest.raises(UncontrolledConcurrencyError):
            _thread.start_new_thread(lambda: None, ())
        if hasattr(_thread, "start_new"):
            with pytest.raises(UncontrolledConcurrencyError):
                _thread.start_new(lambda: None, ())
        with pytest.raises(UncontrolledConcurrencyError):
            concurrent.futures.ThreadPoolExecutor().submit(lambda: None)


def test_zero_system_exit_is_clean_completion_but_nonzero_fails():
    trace, _scheduler, tasks, _clock = _runtime()
    clean = tasks.spawn(lambda: (_ for _ in ()).throw(SystemExit(0)), name="clean-exit")
    failed = tasks.spawn(lambda: (_ for _ in ()).throw(SystemExit(2)), name="failed-exit")
    tasks.run()
    assert tasks.tasks[clean].state == "completed"
    assert tasks.tasks[failed].state == "failed"
    assert any(
        event["kind"] == "TASK_COMPLETED" and event["operation"] == "system_exit_0"
        for event in trace.events
    )
