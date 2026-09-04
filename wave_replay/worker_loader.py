"""Fresh worker-scoped loading of the staged legacy Wave module."""

from __future__ import annotations

import importlib.util
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

    def unload_all(self) -> None:
        for name in tuple(self._loaded):
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
