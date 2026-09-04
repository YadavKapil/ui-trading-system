"""Hermetic pre-import bootstrap for legacy Wave execution."""

from __future__ import annotations

import contextlib
import datetime as host_datetime
import io
import logging
import os
import shutil
import sqlite3
import _sqlite3
import tempfile
import threading
import time as host_time
import types
from urllib.parse import unquote
from pathlib import Path
from typing import Iterator

from .clock import SimClock
from .fake_kite import FakeKite
from .fake_ticker import FakeTicker
from .filesystem_guard import FilesystemGuard
from .import_fakes import FakeModuleInstaller
from .network_guard import NetworkGuard
from .normalize import sha256_file
from .persistence import SandboxPaths
from .process_guard import ProcessGuard
from .scheduler import DeterministicScheduler
from .tasks import SimTaskRuntime
from .tracing import SecretLeakError, TraceRecorder
from .worker_loader import WorkerModuleLoader


class GuardedSQLiteConnection:
    """Proxy that keeps the replay isolation authorizer installed."""

    def __init__(self, connection, authorizer) -> None:
        object.__setattr__(self, "_connection", connection)
        object.__setattr__(self, "_authorizer", authorizer)
        connection.set_authorizer(authorizer)

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __setattr__(self, name, value):
        if name in {"_connection", "_authorizer"}:
            object.__setattr__(self, name, value)
        else:
            setattr(self._connection, name, value)

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        return self._connection.__exit__(exc_type, exc, traceback)

    def set_authorizer(self, callback):
        if callback is not self._authorizer:
            raise PermissionError("Replay SQLite isolation authorizer cannot be replaced")
        return self._connection.set_authorizer(callback)


class ReplayBootstrap:
    """Create all isolation before importing any Wave-reachable source."""

    def __init__(
        self,
        *,
        repository_root: str | Path,
        scheduler: DeterministicScheduler,
        tasks: SimTaskRuntime,
        clock: SimClock,
        fake_kite: FakeKite,
        fake_ticker: FakeTicker,
        trace: TraceRecorder,
        live_trading: bool = True,
        memcache_mode: str = "disabled",
        strict_thread_inventory: bool = False,
    ) -> None:
        self.repository_root = Path(repository_root).resolve()
        self.scheduler = scheduler
        self.tasks = tasks
        self.clock = clock
        self.fake_kite = fake_kite
        self.fake_ticker = fake_ticker
        self.trace = trace
        self.live_trading = live_trading
        self.memcache_mode = memcache_mode
        self.strict_thread_inventory = strict_thread_inventory
        self.canary_api_key = "REPLAY_CANARY_API_KEY_DO_NOT_LOG"
        self.canary_api_secret = "REPLAY_CANARY_API_SECRET_DO_NOT_LOG"
        self.paths: SandboxPaths | None = None
        self.loader: WorkerModuleLoader | None = None
        self._temp: tempfile.TemporaryDirectory[str] | None = None
        self._stack: contextlib.ExitStack | None = None
        self._old_cwd: Path | None = None
        self._old_env: dict[str, str] | None = None
        self._initial_thread_ids: set[int | None] = set()
        self.filesystem_guard: FilesystemGuard | None = None
        self._logging_state: dict | None = None
        self._sandbox_handler: logging.Handler | None = None
        self.source_hashes: dict[str, str] = {}

    def __enter__(self) -> "ReplayBootstrap":
        try:
            self._temp = tempfile.TemporaryDirectory(prefix="wave-replay-")
            self.paths = SandboxPaths.create(Path(self._temp.name).resolve())
            self._stage_source()
            self._write_config()
            self.paths.initialize_databases()
            self._old_cwd = Path.cwd()
            self._old_env = dict(os.environ)
            os.chdir(self.paths.root)
            os.environ.clear()
            os.environ.update({
                "HOME": str(self.paths.home),
                "USERPROFILE": str(self.paths.home),
                "TMP": str(self.paths.temp),
                "TEMP": str(self.paths.temp),
                "WAVE_REPLAY": "1",
                "PYTHONHASHSEED": "0",
            })
            self._install_sandbox_logging()
            self._initial_thread_ids = {thread.ident for thread in threading.enumerate()}
            self._stack = contextlib.ExitStack()
            self._stack.enter_context(NetworkGuard(trace=self.trace, now=self.clock.now_ist).installed())
            self._stack.enter_context(ProcessGuard(trace=self.trace, now=self.clock.now_ist).installed())
            self._stack.enter_context(self.tasks.patched_stdlib())
            self._stack.enter_context(self._guard_sqlite())
            self.filesystem_guard = FilesystemGuard(sandbox_root=self.paths.root)
            self._stack.enter_context(self.filesystem_guard.installed())
            installer = FakeModuleInstaller(
                fake_kite=self.fake_kite,
                fake_ticker=self.fake_ticker,
                trace=self.trace,
                memcache_mode=self.memcache_mode,
            )
            self._stack.enter_context(installer.installed())
            self.loader = WorkerModuleLoader(self.paths.source)
            self.trace.add(
                "BOOTSTRAP_READY",
                at=self.scheduler.now,
                details={"sandbox": "<sandbox>", "live_trading": self.live_trading, "memcache_mode": self.memcache_mode},
            )
            return self
        except BaseException:
            self._rollback_entry()
            raise

    def load_worker(self, worker_id: str, instruments: list[dict]) -> object:
        if self.loader is None:
            raise RuntimeError("ReplayBootstrap is not active")
        captured_out = io.StringIO()
        captured_err = io.StringIO()
        root_logger = logging.getLogger()
        logging_state = {
            "handlers": list(root_logger.handlers),
            "filters": list(root_logger.filters),
            "level": root_logger.level,
            "disabled": root_logger.disabled,
            "propagate": root_logger.propagate,
        }
        try:
            with contextlib.redirect_stdout(captured_out), contextlib.redirect_stderr(captured_err):
                module = self.loader.load_common_lib(worker_id, instruments)
        finally:
            added_handlers = [
                handler for handler in root_logger.handlers if handler not in logging_state["handlers"]
            ]
            root_logger.handlers[:] = logging_state["handlers"]
            root_logger.filters[:] = logging_state["filters"]
            root_logger.setLevel(logging_state["level"])
            root_logger.disabled = logging_state["disabled"]
            root_logger.propagate = logging_state["propagate"]
            for handler in added_handlers:
                handler.close()
        captured = captured_out.getvalue() + captured_err.getvalue()
        self.trace.add(
            "IMPORT_OUTPUT_CAPTURED",
            at=self.scheduler.now,
            actor=worker_id,
            details={
                "characters": len(captured),
                "redacted_canary": self.canary_api_key in captured or self.canary_api_secret in captured,
            },
        )
        module.kite = self.fake_kite
        ticker_facade = self.fake_ticker.facade(worker_id)
        module.kws = ticker_facade
        module.KiteTicker = lambda *_args, **_kwargs: ticker_facade
        module.get_ist_now = self.clock.now_ist
        time_shim = types.ModuleType(f"{worker_id}.time")
        for name in dir(host_time):
            setattr(time_shim, name, getattr(host_time, name))
        time_shim.time = self.clock.epoch
        time_shim.monotonic = self.clock.monotonic
        time_shim.sleep = lambda seconds: self.clock.sleep(seconds, reason="legacy_time.sleep")
        module.time = time_shim
        datetime_shim = types.ModuleType(f"{worker_id}.datetime")
        for name in dir(host_datetime):
            setattr(datetime_shim, name, getattr(host_datetime, name))
        clock = self.clock

        class SimDate(host_datetime.date):
            @classmethod
            def today(cls):
                current = clock.now_ist().date()
                return cls(current.year, current.month, current.day)

        class SimDateTime(host_datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                current = clock.now_ist()
                if tz is not None:
                    current = current.astimezone(tz)
                return cls.fromtimestamp(current.timestamp(), tz=current.tzinfo)

            @classmethod
            def utcnow(cls):
                current = clock.now_ist().astimezone(host_datetime.timezone.utc).replace(tzinfo=None)
                return cls(
                    current.year,
                    current.month,
                    current.day,
                    current.hour,
                    current.minute,
                    current.second,
                    current.microsecond,
                )

        datetime_shim.date = SimDate
        datetime_shim.datetime = SimDateTime
        module.datetime = datetime_shim
        return module

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            if self.loader is not None:
                self.loader.unload_all()
            if self._stack is not None:
                self._stack.close()
            self._restore_logging()
            final_thread_ids = {thread.ident for thread in threading.enumerate()}
            self.trace.add(
                "HOST_THREAD_INVENTORY",
                at=self.scheduler.now,
                details={
                    "unchanged": final_thread_ids == self._initial_thread_ids,
                },
            )
            if self.strict_thread_inventory and final_thread_ids != self._initial_thread_ids:
                raise AssertionError("Host thread inventory changed during strict replay")
            self._assert_no_secret_leak()
            self._assert_sandbox_files()
        finally:
            self._restore_logging()
            if self._old_env is not None:
                os.environ.clear()
                os.environ.update(self._old_env)
            if self._old_cwd is not None:
                os.chdir(self._old_cwd)
            if self._temp is not None:
                self._temp.cleanup()

    def _rollback_entry(self) -> None:
        if self.loader is not None:
            self.loader.unload_all()
        if self._stack is not None:
            self._stack.close()
        self._restore_logging()
        if self._old_env is not None:
            os.environ.clear()
            os.environ.update(self._old_env)
        if self._old_cwd is not None:
            os.chdir(self._old_cwd)
        if self._temp is not None:
            self._temp.cleanup()

    def _install_sandbox_logging(self) -> None:
        assert self.paths is not None
        root = logging.getLogger()
        self._logging_state = {
            "handlers": list(root.handlers),
            "filters": list(root.filters),
            "level": root.level,
            "disabled": root.disabled,
            "propagate": root.propagate,
        }
        root.handlers[:] = []
        root.filters[:] = []
        handler = logging.FileHandler(self.paths.logs / "replay.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
        handler.setLevel(logging.WARNING)
        root.addHandler(handler)
        root.setLevel(logging.WARNING)
        root.disabled = False
        self._sandbox_handler = handler

    def _restore_logging(self) -> None:
        if self._logging_state is None:
            return
        root = logging.getLogger()
        if self._sandbox_handler is not None:
            root.removeHandler(self._sandbox_handler)
            self._sandbox_handler.close()
        root.handlers[:] = self._logging_state["handlers"]
        root.filters[:] = self._logging_state["filters"]
        root.setLevel(self._logging_state["level"])
        root.disabled = self._logging_state["disabled"]
        root.propagate = self._logging_state["propagate"]
        self._sandbox_handler = None
        self._logging_state = None

    def _stage_source(self) -> None:
        assert self.paths is not None
        for filename in ("common_lib.py", "ticker_single_scraper_new.py", "configfile.ini.example"):
            source = self.repository_root / filename
            if not source.exists():
                raise FileNotFoundError(source)
            shutil.copy2(source, self.paths.source / filename)
            if filename.endswith(".py"):
                self.source_hashes[filename] = sha256_file(source)

    def _write_config(self) -> None:
        assert self.paths is not None
        text = f"""[kite_login_details]
api_key = {self.canary_api_key}
api_secret = {self.canary_api_secret}

[option_details]
current_volatility = 13
interest_rate = 10
min_bank_nifty_delta = -3000
max_bank_nifty_delta = 1000
min_nifty_delta = -2000
max_nifty_delta = 2000
delta_calculation_days = 20

[others]
cool_off_time = 0
order_gtt_regular = regular
gtt_trigger_buffer_pct = 0.05

[safety]
live_trading = {'true' if self.live_trading else 'false'}

[greeks]
library = mibian
shadow_compare = false
shadow_tolerance = 0.005
"""
        (self.paths.source / "configfile.ini").write_text(text, encoding="utf-8")
        (self.paths.source / "wave_extractor_config.json").write_text("{}", encoding="utf-8")
        (self.paths.source / "delta_limits.json").write_text("{}", encoding="utf-8")

    def _assert_no_secret_leak(self) -> None:
        assert self.paths is not None
        secrets = (self.canary_api_key, self.canary_api_secret)
        for root in (self.paths.status, self.paths.logs, self.paths.output):
            for path in root.rglob("*"):
                if path.is_file():
                    text = path.read_text(encoding="utf-8", errors="replace")
                    if any(secret in text for secret in secrets):
                        raise SecretLeakError(f"Canary secret leaked to {path.name}")
        serialized = str(self.trace.events)
        if any(secret in serialized for secret in secrets):
            raise SecretLeakError("Canary secret leaked to trace")

    def _assert_sandbox_files(self) -> None:
        assert self.paths is not None
        for path in self.paths.root.rglob("*"):
            self.paths.assert_inside(path)

    @contextlib.contextmanager
    def _guard_sqlite(self) -> Iterator[None]:
        assert self.paths is not None
        original = sqlite3.connect
        original_dbapi = sqlite3.dbapi2.connect
        original_raw = _sqlite3.connect
        original_connection = sqlite3.Connection
        original_dbapi_connection = sqlite3.dbapi2.Connection
        original_raw_connection = _sqlite3.Connection

        def guarded(database, *args, **kwargs):
            raw = os.fspath(database)
            if kwargs.get("uri") and str(raw).startswith("file:"):
                uri_path = unquote(str(raw)[5:].split("?", 1)[0])
                if uri_path in {"", ":memory:"}:
                    path = Path(":memory:")
                else:
                    path = self.paths.assert_inside(uri_path)
            elif str(raw) != ":memory:":
                path = self.paths.assert_inside(raw)
            else:
                path = Path(":memory:")
            self.trace.add(
                "SQLITE_OPEN",
                at=self.scheduler.now,
                details={
                    "path": (
                        ":memory:"
                        if str(path) == ":memory:"
                        else str(path.relative_to(self.paths.root))
                    )
                },
            )
            connection = original(database, *args, **kwargs)

            def authorizer(action, _arg1, _arg2, _database, _trigger):
                if action in {sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH}:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            return GuardedSQLiteConnection(connection, authorizer)

        sqlite3.connect = guarded  # type: ignore[assignment]
        sqlite3.dbapi2.connect = guarded  # type: ignore[assignment]
        _sqlite3.connect = guarded  # type: ignore[assignment]
        sqlite3.Connection = guarded  # type: ignore[assignment]
        sqlite3.dbapi2.Connection = guarded  # type: ignore[assignment]
        _sqlite3.Connection = guarded  # type: ignore[assignment]
        try:
            yield
        finally:
            sqlite3.connect = original  # type: ignore[assignment]
            sqlite3.dbapi2.connect = original_dbapi  # type: ignore[assignment]
            _sqlite3.connect = original_raw  # type: ignore[assignment]
            sqlite3.Connection = original_connection  # type: ignore[assignment]
            sqlite3.dbapi2.Connection = original_dbapi_connection  # type: ignore[assignment]
            _sqlite3.Connection = original_raw_connection  # type: ignore[assignment]
