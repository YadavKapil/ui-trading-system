"""Pre-import module substitutes used by the hermetic Wave worker loader."""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from mibian import BS

from .fake_kite import FakeKite
from .fake_ticker import FakeTicker
from .tracing import TraceRecorder


class FakeMemcache:
    def __init__(self, mode: str = "disabled", values: dict[bytes, bytes] | None = None) -> None:
        self.mode = mode
        self.values = dict(values or {})

    def get(self, key: bytes):
        if self.mode == "error":
            raise RuntimeError("scripted memcache error")
        return self.values.get(key) if self.mode == "hit" else None

    def set(self, key: bytes, value: bytes, expire: int = 0):
        if self.mode == "error":
            raise RuntimeError("scripted memcache error")
        if self.mode != "disabled":
            self.values[key] = value
        return True


def make_instrument_cache(instruments: list[dict[str, Any]]) -> types.ModuleType:
    module = types.ModuleType("instrument_cache")
    by_symbol = {item["tradingsymbol"]: dict(item) for item in instruments}
    by_token = {int(item["instrument_token"]): dict(item) for item in instruments}
    gtt_tags: dict[str, str] = {}
    module.DB_PATH = ":memory:"
    module.SYNC_MARKER_PATH = ":memory:"
    module.is_market_holiday = lambda _day: False
    module.get_instrument = lambda symbol: dict(by_symbol[symbol]) if symbol in by_symbol else None
    module.get_all_fut_opt_instruments = lambda: {token: dict(value) for token, value in by_token.items()}
    module.get_gtt_algo_source = lambda value: gtt_tags.get(str(value))
    module.save_gtt_algo_tag = lambda key, value: gtt_tags.__setitem__(str(key), str(value))
    module.needs_sync = lambda: False
    module.init_db = lambda: None
    return module


class FakeModuleInstaller:
    def __init__(
        self,
        *,
        fake_kite: FakeKite,
        fake_ticker: FakeTicker,
        trace: TraceRecorder,
        memcache_mode: str = "disabled",
    ) -> None:
        self.fake_kite = fake_kite
        self.fake_ticker = fake_ticker
        self.trace = trace
        self.memcache = FakeMemcache(memcache_mode)
        self._previous: dict[str, types.ModuleType | None] = {}

    def _module_map(self) -> dict[str, types.ModuleType]:
        exceptions = types.ModuleType("kiteconnect.exceptions")

        class KiteException(Exception):
            pass

        class NetworkException(KiteException):
            pass

        class DataException(KiteException):
            pass

        exceptions.KiteException = KiteException
        exceptions.NetworkException = NetworkException
        exceptions.DataException = DataException

        kiteconnect = types.ModuleType("kiteconnect")
        kiteconnect.KiteConnect = lambda *args, **kwargs: self.fake_kite
        kiteconnect.KiteTicker = lambda *args, **kwargs: self.fake_ticker
        kiteconnect.exceptions = exceptions

        monitor = types.ModuleType("kite_api_monitor")
        monitor.MonitoredKite = lambda client: client
        monitor.invalidate_positions_cache = lambda: None

        base = types.ModuleType("pymemcache.client.base")
        base.Client = lambda *args, **kwargs: self.memcache
        client = types.ModuleType("pymemcache.client")
        client.base = base
        pymemcache = types.ModuleType("pymemcache")
        pymemcache.client = client

        greeks = types.ModuleType("greeks_lib")
        greeks.BS = BS
        greeks.reread_greeks_config = lambda: None
        greeks.get_active_backend = lambda: "mibian"

        notifications_service = types.ModuleType("notifications.service")
        notifications_service.dispatch = lambda event, **kwargs: self.trace.add(
            "NOTIFICATION",
            at=self.fake_kite.engine.scheduler.now,
            operation=str(event),
            details={"title": str(kwargs.get("title", ""))},
        )
        notifications = types.ModuleType("notifications")
        notifications.service = notifications_service

        class Reactor:
            def __init__(self):
                self.running = False

            def callFromThread(self, callback, *args, **kwargs):
                return callback(*args, **kwargs)

            def startRunning(self):
                self.running = True

            def stop(self):
                self.running = False

        twisted_internet = types.ModuleType("twisted.internet")
        twisted_internet.reactor = Reactor()
        self.fake_ticker.bind_reactor(twisted_internet.reactor)
        twisted_internet.ssl = types.SimpleNamespace(ClientContextFactory=lambda: object())
        twisted = types.ModuleType("twisted")
        twisted.internet = twisted_internet
        autobahn_ws = types.ModuleType("autobahn.twisted.websocket")
        autobahn_ws.connectWS = lambda *args, **kwargs: None
        autobahn_twisted = types.ModuleType("autobahn.twisted")
        autobahn_twisted.websocket = autobahn_ws
        autobahn = types.ModuleType("autobahn")
        autobahn.twisted = autobahn_twisted

        return {
            "kiteconnect": kiteconnect,
            "kiteconnect.exceptions": exceptions,
            "kite_api_monitor": monitor,
            "pymemcache": pymemcache,
            "pymemcache.client": client,
            "pymemcache.client.base": base,
            "greeks_lib": greeks,
            "notifications": notifications,
            "notifications.service": notifications_service,
            "twisted": twisted,
            "twisted.internet": twisted_internet,
            "autobahn": autobahn,
            "autobahn.twisted": autobahn_twisted,
            "autobahn.twisted.websocket": autobahn_ws,
        }

    @contextmanager
    def installed(self) -> Iterator[None]:
        modules = self._module_map()
        for name, module in modules.items():
            self._previous[name] = sys.modules.get(name)
            sys.modules[name] = module
        try:
            yield
        finally:
            for name in reversed(tuple(modules)):
                previous = self._previous[name]
                if previous is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = previous
