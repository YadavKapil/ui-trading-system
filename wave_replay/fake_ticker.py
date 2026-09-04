"""Scheduler-owned, connection-scoped KiteTicker callback facade."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Callable

from .scheduler import DeterministicScheduler
from .tasks import SimTaskRuntime
from .tracing import TraceRecorder


@dataclass
class TickerListener:
    listener_id: str
    connected: bool = False
    generation: int = 0
    callbacks: dict[str, Callable[..., Any] | None] = field(
        default_factory=lambda: {
            "ticks": None,
            "connect": None,
            "order": None,
            "noreconnect": None,
        }
    )
    subscriptions: list[int] = field(default_factory=list)


class FakeTicker:
    MODE_FULL = "full"

    def __init__(self, scheduler: DeterministicScheduler, tasks: SimTaskRuntime, trace: TraceRecorder) -> None:
        self.scheduler = scheduler
        self.tasks = tasks
        self.trace = trace
        self._listeners: dict[str, TickerListener] = {}
        self._reactor = None
        self.register_listener("default")

    def bind_reactor(self, reactor: Any) -> None:
        self._reactor = reactor

    def register_listener(
        self,
        listener_id: str,
        *,
        on_ticks=None,
        on_connect=None,
        on_order_update=None,
        on_noreconnect=None,
    ) -> TickerListener:
        if listener_id in self._listeners:
            listener = self._listeners[listener_id]
        else:
            listener = TickerListener(listener_id)
            self._listeners[listener_id] = listener
        listener.callbacks.update({
            "ticks": on_ticks if on_ticks is not None else listener.callbacks["ticks"],
            "connect": on_connect if on_connect is not None else listener.callbacks["connect"],
            "order": on_order_update if on_order_update is not None else listener.callbacks["order"],
            "noreconnect": on_noreconnect if on_noreconnect is not None else listener.callbacks["noreconnect"],
        })
        return listener

    @property
    def on_ticks(self):
        return self._listeners["default"].callbacks["ticks"]

    @on_ticks.setter
    def on_ticks(self, value):
        self._listeners["default"].callbacks["ticks"] = value

    @property
    def on_connect(self):
        return self._listeners["default"].callbacks["connect"]

    @on_connect.setter
    def on_connect(self, value):
        self._listeners["default"].callbacks["connect"] = value

    @property
    def on_order_update(self):
        return self._listeners["default"].callbacks["order"]

    @on_order_update.setter
    def on_order_update(self, value):
        self._listeners["default"].callbacks["order"] = value

    @property
    def on_noreconnect(self):
        return self._listeners["default"].callbacks["noreconnect"]

    @on_noreconnect.setter
    def on_noreconnect(self, value):
        self._listeners["default"].callbacks["noreconnect"] = value

    @property
    def connected(self) -> bool:
        return self._listeners["default"].connected

    @property
    def subscriptions(self) -> list[int]:
        return list(self._listeners["default"].subscriptions)

    def connect(self, threaded: bool = True, *, listener_id: str = "default") -> None:
        listener = self._listeners[listener_id]
        if self._reactor is not None and not self._reactor.running:
            self._reactor.startRunning()
        listener.generation += 1
        listener.connected = True
        self.deliver("connect", {}, delay=0, listener_id=listener_id)

    def disconnect(self, *, listener_id: str = "default", reason: str = "disconnect") -> None:
        listener = self._listeners[listener_id]
        listener.connected = False
        listener.generation += 1
        self.trace.add(
            "TICKER_DISCONNECTED",
            at=self.scheduler.now,
            actor=listener_id,
            operation=reason,
            details={"generation": listener.generation},
        )

    def crash(self, *, listener_id: str = "default") -> None:
        self.disconnect(listener_id=listener_id, reason="crash")

    def remove_listener(self, listener_id: str) -> None:
        if listener_id in self._listeners:
            self.disconnect(listener_id=listener_id, reason="unload")
            self._listeners.pop(listener_id, None)

    def subscribe(self, tokens: list[int], *, listener_id: str = "default") -> None:
        self._listeners[listener_id].subscriptions = list(tokens)

    def set_mode(self, mode: str, tokens: list[int], *, listener_id: str = "default") -> None:
        self._listeners[listener_id].subscriptions = list(tokens)

    def deliver(
        self,
        kind: str,
        payload: Any,
        *,
        delay: float = 0.0,
        priority: int = 30,
        listener_id: str | None = None,
    ) -> str:
        targets = (
            [listener_id]
            if listener_id is not None
            else sorted(self._listeners)
        )
        scheduled_ids = []
        for target_id in targets:
            listener = self._listeners[target_id]
            generation = listener.generation
            listener_payload = copy.deepcopy(payload)

            def register(
                target_id=target_id,
                generation=generation,
                kind=kind,
                payload=listener_payload,
            ) -> None:
                current = self._listeners.get(target_id)
                if (
                    current is None
                    or not current.connected
                    or current.generation != generation
                ):
                    self.trace.add(
                        "TICKER_CALLBACK_SUPPRESSED",
                        at=self.scheduler.now,
                        actor=target_id,
                        operation=kind,
                        details={"scheduled_generation": generation},
                    )
                    return
                callback = current.callbacks[kind]
                if callback is None:
                    return

                def invoke_callback() -> None:
                    latest = self._listeners.get(target_id)
                    if (
                        latest is None
                        or not latest.connected
                        or latest.generation != generation
                    ):
                        self.trace.add(
                            "TICKER_CALLBACK_SUPPRESSED",
                            at=self.scheduler.now,
                            actor=target_id,
                            operation=f"{kind}:task",
                            details={"scheduled_generation": generation},
                        )
                        return
                    args = (
                        (self, copy.deepcopy(payload))
                        if kind != "noreconnect"
                        else (self,)
                    )
                    callback(*args)

                self.tasks.spawn(
                    invoke_callback,
                    name=f"ticker-{target_id}-{kind}",
                    priority=priority,
                )

            scheduled_ids.append(
                self.scheduler.schedule_after(
                    delay,
                    register,
                    priority=priority,
                    event_id=(
                        f"ticker-{target_id}-{kind}-{len(self.trace.events) + len(scheduled_ids) + 1}"
                    ),
                )
            )
        return ",".join(scheduled_ids)

    def order_dispatch(self, payload: dict[str, Any], delay: float, priority: int = 30) -> None:
        self.deliver("order", payload, delay=delay, priority=priority)

    def facade(self, listener_id: str) -> "WorkerTickerFacade":
        self.register_listener(listener_id)
        return WorkerTickerFacade(self, listener_id)


class WorkerTickerFacade:
    """KiteTicker-shaped view whose callbacks belong to one worker listener."""

    MODE_FULL = "full"

    def __init__(self, ticker: FakeTicker, listener_id: str) -> None:
        self._ticker = ticker
        self.listener_id = listener_id
        self.socket_url = "wss://fake.invalid"
        self._user_agent = lambda: "wave-replay"
        self.factory = object()
        self.connect_timeout = 0

    def _callback(self, kind):
        return self._ticker._listeners[self.listener_id].callbacks[kind]

    def _set_callback(self, kind, value):
        self._ticker._listeners[self.listener_id].callbacks[kind] = value

    on_ticks = property(lambda self: self._callback("ticks"), lambda self, value: self._set_callback("ticks", value))
    on_connect = property(lambda self: self._callback("connect"), lambda self, value: self._set_callback("connect", value))
    on_order_update = property(lambda self: self._callback("order"), lambda self, value: self._set_callback("order", value))
    on_noreconnect = property(lambda self: self._callback("noreconnect"), lambda self, value: self._set_callback("noreconnect", value))

    def connect(self, threaded: bool = True) -> None:
        self._ticker.connect(threaded, listener_id=self.listener_id)

    def subscribe(self, tokens: list[int]) -> None:
        self._ticker.subscribe(tokens, listener_id=self.listener_id)

    def set_mode(self, mode: str, tokens: list[int]) -> None:
        self._ticker.set_mode(mode, tokens, listener_id=self.listener_id)

    def disconnect(self) -> None:
        self._ticker.disconnect(listener_id=self.listener_id)

    def close(self) -> None:
        self.disconnect()

    def remove(self) -> None:
        self._ticker.remove_listener(self.listener_id)

    def _create_connection(self, *args, **kwargs) -> None:
        self.connect()
