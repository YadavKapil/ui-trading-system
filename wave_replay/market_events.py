"""Schedule completed one-minute bars and expose no-lookahead closes."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from .schemas import MarketBar
from .scheduler import DeterministicScheduler
from .tracing import TraceRecorder


class MarketEventFeed:
    def __init__(self, scheduler: DeterministicScheduler, trace: TraceRecorder) -> None:
        self.scheduler = scheduler
        self.trace = trace
        self.latest: dict[str, MarketBar] = {}
        self._subscribers: list[Callable[[MarketBar], None]] = []

    def subscribe(self, callback: Callable[[MarketBar], None]) -> None:
        self._subscribers.append(callback)

    def schedule(self, bars: list[MarketBar]) -> None:
        for index, bar in enumerate(sorted(bars, key=lambda item: (item.available_at, item.instrument_id))):
            self.scheduler.schedule_at(
                bar.available_at,
                lambda bar=bar: self._publish(bar),
                priority=8 if bar.security_type in {"index", "equity"} else 10,
                event_id=f"bar-{index + 1}-{bar.instrument_id}",
            )

    def close(self, instrument_id: str) -> Decimal:
        return self.latest[instrument_id].close

    def _publish(self, bar: MarketBar) -> None:
        self.latest[bar.instrument_id] = bar
        self.trace.add(
            "BAR_AVAILABLE",
            at=self.scheduler.now,
            actor=bar.instrument_id,
            details={
                "wave_trading_symbol": bar.wave_trading_symbol,
                "lean_symbol": bar.lean_symbol,
                "bar_start": bar.bar_start.isoformat(),
                "bar_end": bar.bar_end.isoformat(),
                "close": str(bar.close),
                "source_archive_sha256": bar.source_archive_sha256,
            },
        )
        for subscriber in tuple(self._subscribers):
            subscriber(bar)
