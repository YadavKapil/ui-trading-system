"""Fresh worker-scoped loading of the staged legacy Wave module."""

from __future__ import annotations

import importlib.util
import atexit
import signal
import sys
import types
from pathlib import Path
from typing import Any

from .import_fakes import make_instrument_cache


class WorkerModuleLoader:
    def __init__(self, source_root: Path) -> None:
        self.source_root = source_root
        self._loaded: dict[str, types.ModuleType] = {}

    def load_common_lib(self, worker_id: str, instruments: list[dict[str, Any]]) -> types.ModuleType:
        module_name = f"_wave_replay_workers.{worker_id}.common_lib"
        instrument_cache = make_instrument_cache(instruments)
        previous_cache = sys.modules.get("instrument_cache")
        previous_common = sys.modules.get("common_lib")
        sys.modules["instrument_cache"] = instrument_cache
        try:
            spec = importlib.util.spec_from_file_location(module_name, self.source_root / "common_lib.py")
            if spec is None or spec.loader is None:
                raise ImportError("Cannot construct staged common_lib import")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            sys.modules["common_lib"] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                sys.modules.pop(module_name, None)
                raise
            self._loaded[module_name] = module
            return module
        finally:
            if previous_cache is None:
                sys.modules.pop("instrument_cache", None)
            else:
                sys.modules["instrument_cache"] = previous_cache
            if previous_common is None:
                sys.modules.pop("common_lib", None)
            else:
                sys.modules["common_lib"] = previous_common

    def execute_ticker(
        self,
        worker_id: str,
        common_module: types.ModuleType,
        *,
        argv: list[str],
        max_poll_cycles: int,
    ) -> dict[str, Any]:
        module_name = f"_wave_replay_workers.{worker_id}.ticker_single_scraper_new"
        previous_common = sys.modules.get("common_lib")
        previous_argv = list(sys.argv)
        original_signal = signal.signal
        original_atexit = atexit.register
        cleanup_handlers: list[Any] = []
        signal_handlers: dict[int, Any] = {}
        poll_sleeps = {"count": 0}
        exit_code = {"value": None}
        original_sleep = common_module.time.sleep
        globals_restored = {"value": False}

        class ReplayLoopComplete(BaseException):
            pass

        def controlled_sleep(seconds):
            restore_process_globals()
            if float(seconds) == 180:
                poll_sleeps["count"] += 1
                if poll_sleeps["count"] > max_poll_cycles:
                    raise ReplayLoopComplete()
            return original_sleep(seconds)

        def restore_process_globals():
            if globals_restored["value"]:
                return
            signal.signal = original_signal
            atexit.register = original_atexit
            sys.argv = previous_argv
            if previous_common is None:
                sys.modules.pop("common_lib", None)
            else:
                sys.modules["common_lib"] = previous_common
            globals_restored["value"] = True

        sys.modules["common_lib"] = common_module
        sys.argv = list(argv)
        signal.signal = lambda number, handler: signal_handlers.__setitem__(number, handler)
        atexit.register = lambda handler, *args, **kwargs: cleanup_handlers.append(
            lambda: handler(*args, **kwargs)
        )
        common_module.time.sleep = controlled_sleep
        try:
            spec = importlib.util.spec_from_file_location(
                module_name,
                self.source_root / "ticker_single_scraper_new.py",
            )
            if spec is None or spec.loader is None:
                raise ImportError("Cannot construct staged ticker import")
            ticker_module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = ticker_module
            try:
                spec.loader.exec_module(ticker_module)
            except ReplayLoopComplete:
                pass
            except SystemExit as exc:
                if exc.code not in (None, 0):
                    raise
                exit_code["value"] = exc.code or 0
            self._loaded[module_name] = ticker_module
            return {
                "poll_sleeps": poll_sleeps["count"] - 1,
                "signal_handlers": sorted(signal_handlers),
                "cleanup_handlers": len(cleanup_handlers),
                "exit_code": exit_code["value"],
            }
        finally:
            for cleanup in reversed(cleanup_handlers):
                cleanup()
            common_module.time.sleep = original_sleep
            restore_process_globals()

    def unload_all(self) -> None:
        disconnected_listeners: set[str] = set()
        for name, module in tuple(self._loaded.items()):
            ticker = getattr(module, "kws", None)
            listener_id = getattr(ticker, "listener_id", None)
            if (
                ticker is not None
                and hasattr(ticker, "remove")
                and listener_id not in disconnected_listeners
            ):
                ticker.remove()
                disconnected_listeners.add(listener_id)
            sys.modules.pop(name, None)
        self._loaded.clear()

    def mutable_identity_snapshot(self, module: types.ModuleType) -> dict[str, int]:
        names = (
            "orders",
            "pending_gtt_fallbacks",
            "_spot_price_history",
            "_same_side_fill_times",
            "_cooldown_active_until",
            "_order_history_cache",
        )
        return {name: id(getattr(module, name)) for name in names if hasattr(module, name)}
