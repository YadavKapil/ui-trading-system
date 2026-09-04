import datetime as dt
import _socket
import _io
import multiprocessing
import os
import socket
import sqlite3
import subprocess
import threading
import time
import logging
import sys
from pathlib import Path

import pytest

from wave_replay.bootstrap import ReplayBootstrap
from wave_replay.broker_engine import BrokerEngine
from wave_replay.clock import SimClock
from wave_replay.fake_kite import FakeKite
from wave_replay.fake_ticker import FakeTicker
from wave_replay.filesystem_guard import FilesystemGuard
from wave_replay.network_guard import UnexpectedNetworkAccess
from wave_replay.process_guard import UnexpectedProcessAccess
from wave_replay.schemas import IST
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.tasks import SimTaskRuntime
from wave_replay.tracing import TraceRecorder


def _components():
    trace = TraceRecorder(
        secrets=("REPLAY_CANARY_API_KEY_DO_NOT_LOG", "REPLAY_CANARY_API_SECRET_DO_NOT_LOG")
    )
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 15, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    engine = BrokerEngine(scheduler=scheduler, trace=trace)
    kite = FakeKite(engine)
    ticker = FakeTicker(scheduler, tasks, trace)
    return trace, scheduler, tasks, clock, kite, ticker


def test_gate_zero_imports_staged_wave_with_fakes_and_no_real_threads():
    trace, scheduler, tasks, clock, kite, ticker = _components()
    before = {thread.ident for thread in threading.enumerate()}
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
        strict_thread_inventory=True,
    ) as bootstrap:
        module = bootstrap.load_worker("gate-zero", [])
        assert str(module.__file__).startswith(str(bootstrap.paths.root))
        assert module.kite is kite
        assert module.kws._ticker is ticker
        assert module.kws.listener_id == "gate-zero"
        assert module._memcache_client is None
        assert not (bootstrap.paths.root / "instruments.db").exists()
        assert all(str(path).startswith(str(bootstrap.paths.root)) for path in bootstrap.paths.root.rglob("*"))
    assert {thread.ident for thread in threading.enumerate()} == before


def test_network_is_fail_closed_and_trace_does_not_leak_arguments():
    trace, scheduler, tasks, clock, kite, ticker = _components()
    secret_host = "REPLAY_CANARY_API_SECRET_DO_NOT_LOG.example"
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ):
        with pytest.raises(UnexpectedNetworkAccess):
            socket.getaddrinfo(secret_host, 443)
    network_event = next(event for event in trace.events if event["kind"] == "UNEXPECTED_NETWORK_ACCESS")
    assert secret_host not in str(network_event)
    assert "dns" in str(network_event)


def test_worker_reimport_gets_fresh_mutable_globals():
    trace, scheduler, tasks, clock, kite, ticker = _components()
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        first = bootstrap.load_worker("one", [])
        second = bootstrap.load_worker("two", [])
        first.orders["x"] = {}
        assert second.orders == {}
        assert bootstrap.loader.mutable_identity_snapshot(first) != bootstrap.loader.mutable_identity_snapshot(second)


def test_replay_does_not_replace_host_time_functions():
    original_time = time.time
    original_monotonic = time.monotonic
    trace, scheduler, tasks, clock, kite, ticker = _components()
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker("time-shim", [])
        assert module.time.time is not original_time
        assert time.time is original_time
    assert time.time is original_time
    assert time.monotonic is original_monotonic


def test_worker_local_date_and_datetime_follow_sim_clock_without_global_leak():
    real_date_class = dt.date
    trace, scheduler, tasks, clock, kite, ticker = _components()
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker("datetime-shim", [])
        assert module.datetime.date.today().isoformat() == "2025-07-07"
        assert module.datetime.datetime.now().isoformat().startswith("2025-07-07T09:15:00")
        assert module.datetime.datetime.utcnow().isoformat().startswith("2025-07-07T03:45:00")
        assert module.datetime.date is not dt.date
        assert dt.date is real_date_class
    assert dt.date is real_date_class


def test_filesystem_guard_denies_real_repository_reads_and_ledgers_sandbox_access():
    real_config = Path("configfile.ini.example").resolve()
    trace, scheduler, tasks, clock, kite, ticker = _components()
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        bootstrap.load_worker("filesystem", [])
        with pytest.raises(PermissionError, match="outside the sandbox"):
            real_config.read_text(encoding="utf-8")
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.listdir(real_config.parent)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.remove(real_config)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.chmod(real_config, 0o644)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.utime(real_config, None)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.stat(real_config)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.lstat(real_config)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.readlink(real_config)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            _io.open(real_config, "r")
        with pytest.raises(PermissionError, match="outside the sandbox"):
            _io.FileIO(real_config, "r")
        with pytest.raises(PermissionError, match="outside the sandbox"):
            os.link(real_config, bootstrap.paths.output / "hard-link")
        platform = __import__(os.name)
        with pytest.raises(PermissionError, match="outside the sandbox"):
            platform.open(real_config, os.O_RDONLY)
        with pytest.raises(PermissionError, match="descriptor-relative"):
            os.open("escape.txt", os.O_CREAT | os.O_WRONLY, dir_fd=123)
        allowed = bootstrap.paths.output / "allowed.txt"
        allowed.write_text("ok", encoding="utf-8")
        assert allowed.read_text(encoding="utf-8") == "ok"
        assert any(item["path"] == "output\\allowed.txt" or item["path"] == "output/allowed.txt" for item in bootstrap.filesystem_guard.ledger)


def test_raw_socket_process_and_sqlite_uri_escapes_are_denied():
    real_db = (Path(".").resolve() / "instruments.db").as_posix()
    trace, scheduler, tasks, clock, kite, ticker = _components()
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ):
        with pytest.raises(UnexpectedNetworkAccess):
            _socket.socket()
        with pytest.raises(UnexpectedNetworkAccess):
            _socket.SocketType()
        with pytest.raises(UnexpectedNetworkAccess):
            _socket.getaddrinfo("denied.invalid", 443)
        with pytest.raises(UnexpectedNetworkAccess):
            socket.gethostbyname("denied.invalid")
        with pytest.raises(UnexpectedProcessAccess):
            subprocess.run(["python", "-V"])
        process = multiprocessing.get_context("spawn").Process(target=lambda: None)
        with pytest.raises(UnexpectedProcessAccess):
            process.start()
        if hasattr(os, "startfile"):
            with pytest.raises(UnexpectedProcessAccess):
                os.startfile("denied")
        with pytest.raises(PermissionError, match="escaped sandbox"):
            sqlite3.connect(f"file:{real_db}?mode=rwc", uri=True)
        import _sqlite3

        with pytest.raises(PermissionError, match="escaped sandbox"):
            _sqlite3.connect(f"file:{real_db}?mode=rwc", uri=True)
        connection = sqlite3.connect(":memory:")
        with pytest.raises(PermissionError, match="cannot be replaced"):
            connection.set_authorizer(None)
        with pytest.raises(sqlite3.DatabaseError):
            connection.execute(f"ATTACH DATABASE '{real_db}' AS escaped")
        connection.close()
        if sys.platform == "win32":
            import _winapi

            with pytest.raises(UnexpectedProcessAccess):
                _winapi.CreateProcess(None, None, None, None, 0, 0, None, None, None)
        platform = __import__(os.name)
        with pytest.raises(UnexpectedProcessAccess):
            platform.system("echo denied")
    kinds = {event["kind"] for event in trace.events}
    assert "UNEXPECTED_NETWORK_ACCESS" in kinds
    assert "UNEXPECTED_PROCESS_ACCESS" in kinds


def test_failed_bootstrap_entry_restores_process_state(monkeypatch):
    trace, scheduler, tasks, clock, kite, ticker = _components()
    cwd = Path.cwd()
    environment = dict(os.environ)
    original_run = subprocess.run
    original_add = trace.add

    def fail_ready(kind, **kwargs):
        if kind == "BOOTSTRAP_READY":
            raise RuntimeError("forced bootstrap failure")
        return original_add(kind, **kwargs)

    monkeypatch.setattr(trace, "add", fail_ready)
    with pytest.raises(RuntimeError, match="forced bootstrap failure"):
        with ReplayBootstrap(
            repository_root=".",
            scheduler=scheduler,
            tasks=tasks,
            clock=clock,
            fake_kite=kite,
            fake_ticker=ticker,
            trace=trace,
        ):
            pass
    assert Path.cwd() == cwd
    assert dict(os.environ) == environment
    assert subprocess.run is original_run


def test_worker_import_restores_root_logging_and_failed_module_is_removed():
    trace, scheduler, tasks, clock, kite, ticker = _components()
    root = logging.getLogger()
    before = (list(root.handlers), list(root.filters), root.level, root.disabled, root.propagate)
    with ReplayBootstrap(
        repository_root=".",
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        bootstrap.load_worker("logging", [])
        assert not any(handler in root.handlers for handler in before[0])
        assert len(root.handlers) == 1
        assert str(bootstrap.paths.logs) in root.handlers[0].baseFilename
        (bootstrap.paths.source / "common_lib.py").write_text("raise RuntimeError('broken import')", encoding="utf-8")
        with pytest.raises(RuntimeError, match="broken import"):
            bootstrap.load_worker("broken", [])
        assert "_wave_replay_workers.broken.common_lib" not in sys.modules
    assert (list(root.handlers), list(root.filters), root.level, root.disabled, root.propagate) == before


def test_preopened_host_file_handler_is_detached_for_full_replay_context(tmp_path):
    external = tmp_path / "host.log"
    handler = logging.FileHandler(external, encoding="utf-8")
    root = logging.getLogger()
    root.addHandler(handler)
    root.warning("before")
    handler.flush()
    before = external.read_text(encoding="utf-8")
    trace, scheduler, tasks, clock, kite, ticker = _components()
    try:
        with ReplayBootstrap(
            repository_root=".",
            scheduler=scheduler,
            tasks=tasks,
            clock=clock,
            fake_kite=kite,
            fake_ticker=ticker,
            trace=trace,
        ) as bootstrap:
            bootstrap.load_worker("logging-isolation", [])
            root.error("must stay inside replay sandbox")
            assert handler not in root.handlers
        handler.flush()
        assert external.read_text(encoding="utf-8") == before
        assert handler in root.handlers
    finally:
        root.removeHandler(handler)
        handler.close()


def test_filesystem_guard_resolves_symlink_or_junction_escape(tmp_path):
    sandbox = tmp_path / "sandbox"
    sandbox.mkdir()
    external = tmp_path / "external.txt"
    external.write_text("secret", encoding="utf-8")
    link = sandbox / "link.txt"
    try:
        link.symlink_to(external)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    guard = FilesystemGuard(sandbox_root=sandbox)
    with guard.installed():
        with pytest.raises(PermissionError, match="outside the sandbox"):
            link.read_text(encoding="utf-8")
