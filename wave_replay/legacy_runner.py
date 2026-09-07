"""Run the actual legacy Wave decision/callback path against deterministic truth."""

from __future__ import annotations

import contextlib
import copy
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from .bootstrap import ReplayBootstrap
from .broker_engine import BrokerEngine, FeeSchedule
from .clock import SimClock
from .datasets import ReplayDataset, resolve_missing_quote
from .fake_kite import FakeKite
from .fake_ticker import FakeTicker
from .faults import FaultScript
from .market_events import MarketEventFeed
from .modes import LEGACY_BASELINE_COMMIT
from .normalize import state_hash
from .schemas import Contract, ReplayScenario, parse_timestamp
from .scheduler import DeterministicScheduler
from .seams import configure_legacy_module, instrument_row
from .tasks import SimTaskRuntime
from .tracing import TraceRecorder


class LegacyWaveRunner:
    """Cooperative oracle for one approved historical Wave scenario."""

    def __init__(
        self,
        *,
        repository_root: str | Path,
        dataset: ReplayDataset,
        scenario: ReplayScenario,
        persistent_broker_state: dict[str, Any] | None = None,
        continuous_context: dict[str, Any] | None = None,
    ) -> None:
        self.repository_root = Path(repository_root).resolve()
        self.dataset = dataset
        self.scenario = scenario
        self.persistent_broker_state = copy.deepcopy(persistent_broker_state)
        self.continuous_context = dict(continuous_context or {})
        self.final_broker_state: dict[str, Any] | None = None
        self.equity_points: list[dict[str, Any]] = []
        self.end_at = (
            parse_timestamp(scenario.replay["end_at"])
            if scenario.replay.get("end_at")
            else None
        )
        if scenario.dataset_id != dataset.manifest.dataset_id:
            raise ValueError("Scenario dataset_id does not match loaded dataset")
        if scenario.manifest_sha256 != dataset.manifest_sha256:
            raise ValueError("Scenario manifest hash does not match loaded dataset")
        self._validate_frozen_selection()

    def run(self) -> tuple[dict[str, Any], TraceRecorder]:
        contract = self.dataset.contract_for_instrument(self.scenario.instrument_id)
        from .normalize import sha256_file

        source_hashes = {
            filename: sha256_file(self.repository_root / filename)
            for filename in ("common_lib.py", "ticker_single_scraper_new.py")
        }
        day_bars = self._scenario_bars(contract)
        start = min(bar.bar_start for bar in day_bars)
        trace = TraceRecorder(
            secrets=("REPLAY_CANARY_API_KEY_DO_NOT_LOG", "REPLAY_CANARY_API_SECRET_DO_NOT_LOG")
        )
        scheduler = DeterministicScheduler(start=start, trace=trace, max_events=200_000)
        clock = SimClock(scheduler)
        tasks = SimTaskRuntime(scheduler, trace)
        clock.bind_task_runtime(tasks)
        fee_schedule = FeeSchedule(self.dataset.fee_schedule)
        engine = BrokerEngine(
            scheduler=scheduler,
            trace=trace,
            starting_cash=self.scenario.initial_cash,
            visibility=self.scenario.visibility,
            faults=FaultScript([
                event
                for event in self.scenario.broker_script
                if event.get("kind", "failure") == "failure"
                and not event.get("at")
                and event.get("after_seconds") is None
            ]),
            fee_schedule=fee_schedule,
            capital_tracking=bool(
                self.scenario.replay.get("capital_model")
            ),
            full_precision_fees=bool(
                self.scenario.replay.get("capital_model")
            ),
        )
        if self.persistent_broker_state is not None:
            engine.restore_persistent_state(self.persistent_broker_state)
        if self.continuous_context:
            engine.open_exchange_session()
        initial_day_cancellations = engine.day_cancellation_count
        initial_post_close_rejections = engine.post_close_rejection_count
        initial_roll_rejections = engine.roll_rejection_count
        fake_kite = FakeKite(engine, instruments=[instrument_row(contract, self.scenario)])
        ticker = FakeTicker(scheduler, tasks, trace)
        engine.bind_callback_dispatch(ticker.order_dispatch)
        feed = MarketEventFeed(scheduler, trace)
        started = {"value": False}
        module_box: dict[str, Any] = {}
        bars_at: dict[dt.datetime, dict[str, Any]] = {}
        incomplete_at: set[dt.datetime] = set()
        missing_decisions: list[dict[str, Any]] = []
        unknown_data = {"value": False}
        ticker_result_box: dict[str, Any] = {}
        roll_result: dict[str, Any] = {}

        with ReplayBootstrap(
            repository_root=self.repository_root,
            scheduler=scheduler,
            tasks=tasks,
            clock=clock,
            fake_kite=fake_kite,
            fake_ticker=ticker,
            trace=trace,
        ) as bootstrap:
            module = bootstrap.load_worker("we-1", [instrument_row(contract, self.scenario)])
            module_box["module"] = module
            configure_legacy_module(module, self.scenario, contract)
            if self.continuous_context:
                self._rehydrate_gtt_fallbacks(module, engine, contract, trace, scheduler.now)
            session = self._session_row()
            session_open = self._session_timestamp(session["session_open_ist"])
            session_close = self._session_timestamp(session["session_close_ist"])
            if self.continuous_context:
                def authoritative_market_open() -> bool:
                    now = module.get_ist_now()
                    is_open = (
                        now.date() == self.scenario.selected_date
                        and session_open.time() <= now.time() <= session_close.time()
                    )
                    if now == session_close:
                        trace.add(
                            "AUTHORITATIVE_CLOSE_BOUNDARY_CHECK",
                            at=now,
                            actor="we-1",
                            result={"is_open": is_open, "inclusive_close": True},
                        )
                    return is_open

                module.is_market_open = authoritative_market_open
                trace.add(
                    "AUTHORITATIVE_SESSION_CALENDAR_BOUND",
                    at=scheduler.now,
                    actor="we-1",
                    details={
                        "session_open": session_open.isoformat(),
                        "session_close": session_close.isoformat(),
                        "special_session": bool(session.get("special_session", False)),
                    },
                )
            staged_root = Path(module.__file__).parent
            (staged_root / "delta_limits.json").write_text(
                json.dumps(self.scenario.wave_settings["delta_limits"], indent=2, sort_keys=True),
                encoding="utf-8",
            )
            (staged_root / "wave_extractor_config.json").write_text(
                json.dumps({"NIFTY": self.scenario.wave_settings}, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            self._install_decision_tracing(module, trace)

            def on_bar(bar) -> None:
                key = "underlying" if bar.security_type in {"index", "equity"} else "option"
                if key == "option" and bar.instrument_id != contract.instrument_id:
                    return
                pair = bars_at.setdefault(bar.bar_end, {})
                pair[key] = bar
                if set(pair) != {"underlying", "option"}:
                    return
                underlying_bar, option_bar = pair["underlying"], pair["option"]
                if underlying_bar.missing_bar or option_bar.missing_bar:
                    incomplete_at.add(bar.bar_end)
                    engine.latest_quotes.pop(self._quote_key(underlying_bar), None)
                    engine.latest_quotes.pop(self._quote_key(option_bar), None)
                    engine.record_capital_snapshot()
                    missing_id = (
                        underlying_bar.instrument_id if underlying_bar.missing_bar else option_bar.instrument_id
                    )
                    decision = resolve_missing_quote(
                        self.scenario.missing_quote_policy,
                        instrument_id=missing_id,
                        timestamp=bar.bar_end.isoformat(),
                    )
                    if self.continuous_context:
                        decision = {
                            **decision,
                            "timestamp": bar.bar_end.isoformat(),
                            "instrument_id": missing_id,
                        }
                    missing_decisions.append(decision)
                    if decision["action"] == "mark_unknown":
                        unknown_data["value"] = True
                    trace.add(
                        "MISSING_QUOTE_POLICY",
                        at=scheduler.now,
                        operation=decision["action"],
                        details=decision,
                    )
                    return
                engine.set_quote(
                    self._quote_key(underlying_bar),
                    last_price=underlying_bar.close,
                    instrument_token=0,
                )
                engine.set_quote(
                    self._quote_key(option_bar),
                    last_price=option_bar.close,
                    instrument_token=contract.instrument_token,
                )
                engine.record_capital_snapshot()
                if self.continuous_context and not started["value"]:
                    engine.mark_to_market(contract.wave_trading_symbol, option_bar.close)
                if started["value"]:
                    engine.evaluate_close_bar(
                        option_bar,
                        fill_price_mode=self.scenario.replay["fill_price_mode"],
                    )
                self._record_equity_point(engine, option_bar, unknown=False)

            feed.subscribe(on_bar)
            trace.add(
                "TRACE_HEADER",
                at=scheduler.now,
                actor="we-1",
                details={
                    "trace_schema_version": TraceRecorder.SCHEMA_VERSION,
                    "wave_source": {
                        "baseline_commit": LEGACY_BASELINE_COMMIT,
                        "source_hashes": source_hashes,
                        "reachability_hash": state_hash({
                            "common_lib.py": source_hashes["common_lib.py"],
                            "ticker_single_scraper_new.py": source_hashes["ticker_single_scraper_new.py"],
                            "scope": "wave-stage-1-10",
                        }),
                        "config_hashes": self.scenario.wave_settings_hashes,
                    },
                    "dataset": {
                        "schema": self.dataset.manifest.schema,
                        "schema_version": self.dataset.manifest.schema_version,
                        "dataset_id": self.dataset.manifest.dataset_id,
                        "manifest_sha256": self.dataset.manifest_sha256,
                        "producer": self.dataset.manifest.producer,
                        "source": self.dataset.manifest.source,
                        "identity": self.dataset.manifest.identity,
                    },
                    "scenario": {
                        "scenario_id": self.scenario.scenario_id,
                        "role": self.scenario.role,
                        "seed": self.scenario.seed,
                        "config_hashes": self.scenario.wave_settings_hashes,
                    },
                    "contract": {
                        "instrument_id": contract.instrument_id,
                        "source_instrument_id": contract.source_instrument_id,
                        "wave_trading_symbol": contract.wave_trading_symbol,
                        "lean_symbol": contract.lean_symbol,
                        "replay_instrument_token": contract.instrument_token,
                        "expiry": contract.expiry.isoformat(),
                        "strike": str(contract.strike),
                        "lot_size": contract.lot_size,
                        "source_contract_multiplier": contract.source_contract_multiplier,
                        "resolved_contract_multiplier": contract.resolved_contract_multiplier,
                        "contract_metadata_sha256": contract.metadata_sha256,
                    },
                    "initial_state": {
                        "cash": str(engine.cash),
                        "positions": (
                            []
                            if self.persistent_broker_state is None
                            else copy.deepcopy(engine.positions_by_symbol)
                        ),
                        "regular_orders": [],
                        "gtts": (
                            []
                            if self.persistent_broker_state is None
                            else engine.get_gtts()
                        ),
                    },
                    "replay": self.scenario.replay,
                },
                state_after=state_hash(self._legacy_state(module)),
            )
            feed.schedule(day_bars)

            def exchange_close() -> None:
                engine.close_exchange_session()

            if self.continuous_context:
                scheduler.schedule_at(
                    session_close,
                    exchange_close,
                    priority=90,
                    event_id="exchange-day-close",
                )
            if self.continuous_context.get("roll_before_next_session"):
                roll_at = session_close - dt.timedelta(minutes=1)
                roll_bar = self._latest_synchronized_option_bar(contract, roll_at)

                def roll_position() -> None:
                    engine.cancel_open_day_orders(reason="simulator_contract_roll")
                    for trigger in list(engine.gtts_by_id.values()):
                        if trigger.status == "active" and trigger.symbol == contract.wave_trading_symbol:
                            engine.delete_gtt(trigger.trigger_id)
                    before_realized = engine.realized_pnl
                    before_fees = engine.fees_paid
                    order_id = engine.liquidate_position(
                        contract.wave_trading_symbol,
                        price=roll_bar.close,
                        reason="next_session_dte_below_5",
                    )
                    engine.block_symbol_until_session_end(contract.wave_trading_symbol)
                    roll_result.update({
                        "at": scheduler.now.isoformat(),
                        "order_id": order_id,
                        "price": str(roll_bar.close),
                        "quote_at": roll_bar.bar_end.isoformat(),
                        "realized_pnl": str(engine.realized_pnl - before_realized),
                        "fees": str(engine.fees_paid - before_fees),
                    })
                    self._record_equity_snapshot(engine, scheduler.now, unknown=False)

                scheduler.schedule_at(
                    roll_at,
                    roll_position,
                    priority=18,
                    event_id="simulator-contract-roll",
                )

            def startup() -> None:
                started["value"] = True
                trace.add(
                    "COOL_OFF_BYPASSED",
                    at=scheduler.now,
                    actor="we-1",
                    details={"cool_off_mode": "disabled", "production_seconds": 10},
                )
                if engine.active_gtt_count(contract.wave_trading_symbol):
                    original_place_duo = module.place_duo_order

                    def guarded_place_duo(*args, **kwargs):
                        if engine.active_gtt_count(contract.wave_trading_symbol):
                            trace.add(
                                "CONTINUOUS_DUO_SKIPPED_PENDING_GTT",
                                at=scheduler.now,
                                actor=contract.wave_trading_symbol,
                                details={"active_gtts": engine.active_gtt_count(contract.wave_trading_symbol)},
                            )
                            return {}
                        return original_place_duo(*args, **kwargs)

                    module.place_duo_order = guarded_place_duo
                assert bootstrap.loader is not None
                ticker_result = bootstrap.loader.execute_ticker(
                    "we-1",
                    module,
                    argv=[
                        "ticker_single_scraper_new.py",
                        str(self.scenario.buy_gap_points),
                        str(self.scenario.sell_gap_points),
                        self.scenario.symbol,
                        f"{self.scenario.lot_size}:{self.scenario.lot_size}",
                        "access:replay-token",
                        "NRML",
                    ],
                    max_poll_cycles=200,
                )
                if self.continuous_context:
                    ticker_result["completed_at"] = scheduler.now.isoformat()
                ticker_result_box.update(ticker_result)
                trace.add(
                    "TICKER_ENTRYPOINT_COMPLETED",
                    at=scheduler.now,
                    actor="we-1",
                    details=ticker_result,
                )

            scheduler.schedule_at(
                self.scenario.option_close_at_selection_at,
                lambda: tasks.spawn(startup, name="wave-startup"),
                priority=20,
                event_id="wave-startup",
            )
            consumed_script = self._schedule_broker_script(
                scheduler, tasks, engine, ticker, module
            )
            assert bootstrap.paths is not None
            with (bootstrap.paths.logs / "legacy.log").open("w", encoding="utf-8") as legacy_log:
                with contextlib.redirect_stdout(legacy_log), contextlib.redirect_stderr(legacy_log):
                    if self.end_at is None:
                        tasks.run()
                    else:
                        tasks.run(until=self.end_at, allow_incomplete=True)
                        tasks.cancel_all(reason="replay_end_at")
            failures = [task for task in tasks.tasks.values() if task.exception is not None]
            if failures:
                raise RuntimeError(
                    f"Replay task {failures[0].task_id} failed: {failures[0].exception}"
                ) from failures[0].exception
            if engine.faults.remaining():
                raise RuntimeError(f"Unconsumed broker failures: {engine.faults.remaining()}")
            expected_actions = {
                index
                for index, event in enumerate(self.scenario.broker_script)
                if event.get("kind", "failure") != "failure"
                or event.get("at") is not None
                or event.get("after_seconds") is not None
            }
            if consumed_script != expected_actions:
                raise RuntimeError(
                    f"Unconsumed broker actions: {sorted(expected_actions - consumed_script)}"
                )
            if self.continuous_context:
                self._record_equity_snapshot(
                    engine,
                    scheduler.now,
                    unknown=unknown_data["value"],
                    reason="session_final_post_events",
                )
            report = self._report(
                engine,
                trace,
                contract,
                module,
                unknown_data=unknown_data["value"],
                source_hashes=source_hashes,
            )
            report["data_quality"]["missing_quote_decisions"] = missing_decisions
            if self.continuous_context:
                report["session_end"].update({
                    "snapshot_type": "continuous_day_boundary_after_day_cancel_and_worker_exit",
                    "day_order_cancellation_modeled": True,
                    "day_orders_cancelled": engine.day_cancellation_count - initial_day_cancellations,
                    "post_close_rejections": (
                        engine.post_close_rejection_count - initial_post_close_rejections
                    ),
                    "roll_rejections": engine.roll_rejection_count - initial_roll_rejections,
                    "worker_lifecycle": ticker_result_box,
                    "roll": roll_result or None,
                    "authoritative_session_open": session_open.isoformat(),
                    "authoritative_session_close": session_close.isoformat(),
                    "first_post_close_poll_at": ticker_result_box.get("completed_at"),
                    "subsequent_shutdown_poll_modeled": True,
                })
                report["equity_curve"] = list(self.equity_points)
                if self.scenario.replay.get("capital_model"):
                    report["capital_curve"] = list(engine.capital_snapshots)
                self.final_broker_state = engine.export_persistent_state()
            report_path = bootstrap.paths.output / "report.json"
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        report["trace_digest"] = trace.digest()
        return report, trace

    def _session_row(self) -> dict[str, Any]:
        return next(
            row
            for row in self.dataset.sessions
            if str(row["session_date"]) == self.scenario.selected_date.isoformat()
        )

    @staticmethod
    def _session_timestamp(value: Any) -> dt.datetime:
        return value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value))

    @staticmethod
    def _quote_key_for_contract(contract: Contract) -> str:
        symbol = str(contract.wave_trading_symbol)
        return symbol if ":" in symbol else f"{contract.exchange}:{symbol}"

    def _record_equity_point(self, engine: BrokerEngine, option_bar, *, unknown: bool) -> None:
        self._record_equity_snapshot(engine, option_bar.bar_end, unknown=unknown)

    def _record_equity_snapshot(
        self,
        engine: BrokerEngine,
        at: dt.datetime,
        *,
        unknown: bool,
        reason: str = "completed_option_minute",
    ) -> None:
        equity = None if unknown else engine.current_equity()
        unrealized = sum(
            Decimal(str(position.get("m2m", 0)))
            for position in engine.positions_by_symbol.values()
        )
        self.equity_points.append({
            "at": at.isoformat(),
            "cash": str(engine.cash),
            "equity": None if equity is None else str(equity),
            "realized_pnl": str(engine.realized_pnl),
            "unrealized_pnl": None if unknown else str(unrealized),
            "fees": str(engine.fees_paid),
            "reason": reason,
        })

    def _latest_synchronized_option_bar(
        self,
        contract: Contract,
        at: dt.datetime,
    ):
        underlying_ends = {
            bar.bar_end
            for bar in self.dataset.underlying_bars
            if bar.session_date == self.scenario.selected_date
            and not bar.missing_bar
            and bar.bar_end <= at
        }
        return max(
            (
                bar
                for bar in self.dataset.option_bars
                if bar.instrument_id == contract.instrument_id
                and bar.session_date == self.scenario.selected_date
                and not bar.missing_bar
                and bar.bar_end in underlying_ends
            ),
            key=lambda bar: bar.bar_end,
        )

    @staticmethod
    def _rehydrate_gtt_fallbacks(
        module,
        engine: BrokerEngine,
        contract: Contract,
        trace: TraceRecorder,
        at: dt.datetime,
    ) -> None:
        active = [
            trigger
            for trigger in engine.gtts_by_id.values()
            if trigger.status == "active"
            and trigger.symbol == contract.wave_trading_symbol
        ]
        tags = [trigger.tag for trigger in active if trigger.tag]
        for trigger in active:
            if not trigger.tag:
                raise RuntimeError(f"Active GTT {trigger.trigger_id} has no reconciliation tag")
            template = trigger.orders[0]
            siblings = [tag for tag in tags if tag != trigger.tag]
            module.pending_gtt_fallbacks[trigger.tag] = {
                "trigger_id": trigger.trigger_id,
                "symbol": trigger.symbol,
                "exchange": trigger.exchange,
                "product": template.get("product", "NRML"),
                "quantity": int(template["quantity"]),
                "transaction_type": str(template["transaction_type"]).upper(),
                "price": float(template.get("price", trigger.trigger_values[0])),
                "associated_regular_order_id": -1,
                "associated_gtt_tag": siblings[0] if len(siblings) == 1 else None,
            }
        if active:
            trace.add(
                "CONTINUOUS_GTT_REHYDRATED",
                at=at,
                actor=contract.wave_trading_symbol,
                details={
                    "trigger_ids": [trigger.trigger_id for trigger in active],
                    "tags": tags,
                },
            )

    def _validate_frozen_selection(self) -> None:
        contract = self.dataset.contract_for_instrument(self.scenario.instrument_id)
        expected = {
            "symbol": (self.scenario.symbol, contract.wave_trading_symbol),
            "source_instrument_id": (self.scenario.source_instrument_id, contract.source_instrument_id),
            "lean_symbol": (self.scenario.lean_symbol, contract.lean_symbol),
            "replay_instrument_token": (self.scenario.instrument_token, contract.instrument_token),
            "expiry": (self.scenario.expiry, contract.expiry),
            "strike": (self.scenario.strike, contract.strike),
            "lot_size": (self.scenario.lot_size, contract.lot_size),
            "calendar_dte": (
                self.scenario.calendar_dte,
                (contract.expiry - self.scenario.selected_date).days,
            ),
            "contract_metadata_sha256": (
                self.scenario.contract_metadata_sha256,
                contract.metadata_sha256,
            ),
            "fee_schedule_sha256": (
                self.scenario.fee_schedule_sha256,
                self.dataset.manifest.fee_schedule_sha256,
            ),
        }
        mismatches = [name for name, (actual, wanted) in expected.items() if actual != wanted]
        if mismatches:
            raise ValueError(f"Scenario frozen metadata mismatch: {', '.join(mismatches)}")
        underlying_candidates = [
            bar
            for bar in self.dataset.underlying_bars
            if bar.underlying == "NIFTY"
            and bar.bar_end.date() == self.scenario.selected_date
            and not bar.missing_bar
        ]
        first_underlying = (
            next(
                (
                    bar
                    for bar in underlying_candidates
                    if bar.bar_end == self.scenario.first_underlying_close_at
                ),
                None,
            )
            if self.continuous_context
            else min(underlying_candidates, key=lambda bar: bar.bar_end)
        )
        option = next(
            (
                bar
                for bar in self.dataset.option_bars
                if bar.instrument_id == contract.instrument_id
                and bar.bar_end == self.scenario.option_close_at_selection_at
                and not bar.missing_bar
            ),
            None,
        )
        if (
            first_underlying is None
            or first_underlying.bar_end != self.scenario.first_underlying_close_at
            or first_underlying.close != self.scenario.first_underlying_close
            or option is None
            or option.close != self.scenario.option_close_at_selection
        ):
            raise ValueError("Scenario frozen selection closes do not match the dataset")

    def _scenario_bars(self, contract: Contract) -> list[Any]:
        bars = self.dataset.underlying_bars_with_missing(self.scenario.selected_date)
        option_bars = self.dataset.option_bars_with_missing(contract, self.scenario.selected_date)
        replay_start = self.continuous_context.get("session_replay_start_at")
        if replay_start:
            start_at = parse_timestamp(replay_start)
            bars = [bar for bar in bars if bar.bar_end >= start_at]
            option_bars = [bar for bar in option_bars if bar.bar_end >= start_at]
        if self.end_at:
            bars = [bar for bar in bars if bar.bar_end <= self.end_at]
            option_bars = [bar for bar in option_bars if bar.bar_end <= self.end_at]
        if not bars or not any(not bar.missing_bar for bar in option_bars):
            raise ValueError("Scenario requires synchronized non-missing underlying and option bars")
        return [*bars, *option_bars]

    def _schedule_polls(self, scheduler, tasks, module, bars, incomplete_at) -> None:
        first = self.scenario.option_close_at_selection_at + dt.timedelta(seconds=180)
        last = max(bar.bar_end for bar in bars)
        poll_at = first
        poll_number = 0
        while poll_at <= last:
            poll_number += 1

            def poll() -> None:
                if scheduler.now in incomplete_at:
                    return
                if not module.is_market_open():
                    return
                module.check_changes_in_restrictions(self.scenario.symbol)
                module.write_status_to_file()
                module.check_orders()
                if not module.check_is_any_order_active():
                    module.check_gtt_orders()
                    if not module.is_any_gtt_pending():
                        module.place_duo_order(self.scenario.symbol, "NRML", True)

            scheduler.schedule_at(
                poll_at,
                lambda number=poll_number: tasks.spawn(poll, name=f"wave-poll-{number}", priority=60),
                priority=60,
                event_id=f"poll-{poll_number}",
            )
            poll_at += dt.timedelta(seconds=180)

    def _schedule_broker_script(self, scheduler, tasks, engine, ticker, module) -> set[int]:
        consumed: set[int] = set()
        for index, event in enumerate(self.scenario.broker_script):
            kind = event.get("kind", "failure")
            is_timed_failure = kind == "failure" and (
                event.get("at") is not None or event.get("after_seconds") is not None
            )
            if kind == "failure" and not is_timed_failure:
                continue
            at = (
                parse_timestamp(event["at"])
                if event.get("at")
                else self.scenario.first_underlying_close_at
                + dt.timedelta(seconds=float(event.get("after_seconds", 0)))
            )
            if self.end_at is not None and at > self.end_at:
                raise ValueError(f"broker_script event {index} is after replay end_at")

            def execute(event=event, kind=kind, index=index):
                if kind == "failure":
                    engine.faults.activate(event)
                elif kind == "gtt_trigger":
                    trigger_id = self._resolve_trigger_id(engine, event.get("trigger_id", "first_active"))
                    engine.trigger_gtt(
                        trigger_id,
                        immediate_complete=bool(event.get("immediate_complete", False)),
                    )
                elif kind == "gtt_create":
                    side = str(event.get("side", "BUY")).upper()
                    price = Decimal(str(event.get("price", self.scenario.option_close_at_selection)))
                    engine.place_gtt(
                        trigger_type="single",
                        tradingsymbol=self.scenario.symbol,
                        exchange="NFO",
                        trigger_values=[float(price)],
                        last_price=float(price),
                        orders=[{
                            "transaction_type": side,
                            "quantity": self.scenario.lot_size,
                            "product": "NRML",
                            "order_type": "LIMIT",
                            "price": float(price),
                            "tag": str(event.get("tag", f"GTT_{side[0]}_CONTINUOUS")),
                        }],
                    )
                elif kind == "partial_fill":
                    order_id = self._resolve_order_id(engine, event.get("order_id", "first_open"), event.get("side"))
                    engine.partial_fill(
                        order_id,
                        quantity=int(event["quantity"]),
                        price=Decimal(str(event["price"])),
                    )
                elif kind == "reject":
                    order_id = self._resolve_order_id(engine, event.get("order_id", "first_open"), event.get("side"))
                    engine.reject(order_id, reason=str(event.get("reason", "scripted reject")))
                elif kind == "callback":
                    ticker.deliver("order", dict(event["payload"]), delay=0)
                elif kind == "poll":
                    tasks.spawn(module.check_orders, name="scripted-poll", priority=60)
                elif kind == "cancel":
                    order_id = self._resolve_order_id(engine, event.get("order_id", "first_open"), event.get("side"))
                    engine.cancel_order(variety="regular", order_id=order_id)
                elif kind == "modify":
                    order_id = self._resolve_order_id(engine, event.get("order_id", "first_open"), event.get("side"))
                    request = {"variety": "regular", "order_id": order_id}
                    if event.get("price") is not None:
                        request["price"] = event["price"]
                    if event.get("quantity") is not None:
                        request["quantity"] = event["quantity"]
                    engine.modify_order(**request)
                else:
                    raise ValueError(f"Unsupported broker_script kind: {kind}")
                consumed.add(index)

            scheduler.schedule_at(
                at,
                execute,
                priority=int(event.get("priority", 25)),
                event_id=f"broker-script-{index + 1}-{kind}",
            )
        return consumed

    @staticmethod
    def _resolve_trigger_id(engine, value) -> int:
        if value == "first_active":
            active = [
                trigger.trigger_id
                for trigger in engine.gtts_by_id.values()
                if trigger.status == "active"
            ]
            if not active:
                raise ValueError("No active GTT for scripted event")
            return min(active)
        return int(value)

    @staticmethod
    def _resolve_order_id(engine, value, side=None) -> str:
        if value == "first_open":
            candidates = [
                order
                for order in engine.orders_by_id.values()
                if order.status in {"OPEN", "PARTIAL"}
                and (side is None or order.side == str(side).upper())
            ]
            if not candidates:
                raise ValueError("No matching open order for scripted event")
            return min(candidates, key=lambda order: int(order.order_id)).order_id
        return str(value)

    @staticmethod
    def _quote_key(bar) -> str:
        return (
            bar.wave_trading_symbol
            if ":" in str(bar.wave_trading_symbol)
            else f"{bar.exchange}:{bar.wave_trading_symbol}"
        )

    def _report(
        self,
        engine: BrokerEngine,
        trace: TraceRecorder,
        contract: Contract,
        module,
        *,
        unknown_data: bool,
        source_hashes: dict[str, str],
    ) -> dict[str, Any]:
        latest_option = max(
            (
                bar
                for bar in self.dataset.option_bars
                if bar.instrument_id == contract.instrument_id
                and bar.bar_end.date() == self.scenario.selected_date
                and not bar.missing_bar
                and (self.end_at is None or bar.bar_end <= self.end_at)
            ),
            key=lambda bar: bar.bar_end,
        )
        position = engine.positions_by_symbol.get(contract.wave_trading_symbol, {"quantity": 0})
        market_value = None if unknown_data else Decimal(int(position.get("quantity", 0))) * latest_option.close
        equity = None if market_value is None else engine.cash + market_value
        transition_events = [
            event for event in trace.events if event["kind"] == "BROKER_ORDER_TRANSITION"
        ]
        fill_events = [event for event in transition_events if event["operation"] == "COMPLETE"]
        cancel_events = [event for event in transition_events if event["operation"] == "CANCELLED"]
        decision_events = [event for event in trace.events if event["kind"] == "WAVE_DECISION"]
        return {
            "scenario_id": self.scenario.scenario_id,
            "scenario_role": self.scenario.role,
            "decision_path": "legacy_decision_path",
            "wave_source": {
                "baseline_commit": LEGACY_BASELINE_COMMIT,
                "source_hashes": source_hashes,
                "reachability_hash": state_hash({
                    "common_lib.py": source_hashes["common_lib.py"],
                    "ticker_single_scraper_new.py": source_hashes["ticker_single_scraper_new.py"],
                    "scope": "wave-stage-1-10",
                }),
                "config_hashes": self.scenario.wave_settings_hashes,
            },
            "equivalence_status": "ineligible_unknown_data" if unknown_data else "modeled_not_exact",
            "dataset": {
                "schema": self.dataset.manifest.schema,
                "schema_version": self.dataset.manifest.schema_version,
                "dataset_id": self.dataset.manifest.dataset_id,
                "manifest_sha256": self.dataset.manifest_sha256,
                "implementation_commit": self.dataset.manifest.producer["implementation_commit"],
                "source_commits": {
                    name: block["repository_commit"]
                    for name, block in self.dataset.manifest.source.items()
                    if isinstance(block, dict) and block.get("repository_commit")
                },
            },
            "contract": {
                "instrument_id": contract.instrument_id,
                "source_instrument_id": contract.source_instrument_id,
                "wave_trading_symbol": contract.wave_trading_symbol,
                "lean_symbol": contract.lean_symbol,
                "replay_instrument_token": contract.instrument_token,
                "instrument_token_kind": "synthetic_replay",
                "expiry": contract.expiry.isoformat(),
                "strike": str(contract.strike),
                "lot_size": contract.lot_size,
                "source_contract_multiplier": contract.source_contract_multiplier,
                "resolved_contract_multiplier": contract.resolved_contract_multiplier,
            },
            "settings": {
                "cool_off_mode": "disabled",
                "same_side_fill_cooldown": "enabled",
                "trigger_mode": "close_only",
                "activation_policy": "subsequent_bar",
                "fill_price_mode": "limit",
                "order_mode": "regular",
                "buy_gap_pct": 0.10,
                "sell_gap_pct": 0.10,
                "buy_gap_points": str(self.scenario.buy_gap_points),
                "sell_gap_points": str(self.scenario.sell_gap_points),
                "wave_settings_hashes": self.scenario.wave_settings_hashes,
            },
            "account": {
                "starting_cash": str(self.scenario.initial_cash),
                "ending_cash": str(engine.cash),
                "market_value": None if market_value is None else str(market_value),
                "ending_equity": None if equity is None else str(equity),
                "pnl": None if equity is None else str(equity - self.scenario.initial_cash),
                "positions": engine.positions_by_symbol,
                "open_orders": [
                    order.api_dict()
                    for order in engine.orders_by_id.values()
                    if order.status not in {"COMPLETE", "CANCELLED", "REJECTED"}
                ],
                "open_gtts": [value for value in engine.get_gtts() if value["status"] == "active"],
            },
            "activity": {
                "regular_orders_placed": sum(
                    event["kind"] == "BROKER_CALL" and event["operation"] == "place_order"
                    for event in trace.events
                ),
                "fills": len(fill_events),
                "cancels": len(cancel_events),
                "wave_decisions": len(decision_events),
                "trades": [
                    {
                        "order_id": event["details"]["order_id"],
                        "side": event["details"]["transaction_type"],
                        "quantity": event["details"]["filled_quantity"],
                        "price": event["details"]["average_price"],
                        "at": event["at"],
                    }
                    for event in fill_events
                ],
                "fill_events": list(engine.fill_events),
                "final_restrictions": None if unknown_data else {
                    "buy": module.global_restrict_buy,
                    "sell": module.global_restrict_sell,
                },
                "last_multiplier_info": None if unknown_data else module._last_multiplier_info,
            },
            "fees": {
                "broker_fee_model": "upstox_approximation",
                "zerodha_fee_fidelity": False,
                "fees_paid": str(engine.fees_paid),
                **(
                    {
                        "components": {
                            name: str(value)
                            for name, value in engine.fee_components_paid.items()
                        },
                        "events": list(engine.fee_events),
                    }
                    if self.scenario.replay.get("capital_model")
                    else {}
                ),
            },
            "risk_outputs": {
                "status": "unknown_missing_quote" if unknown_data else "computed",
                "restrictions": None if unknown_data else {
                    "buy": module.global_restrict_buy,
                    "sell": module.global_restrict_sell,
                },
            },
            "session_end": {
                "snapshot_type": (
                    "boundary_after_market_close_shutdown_poll"
                    if self.end_at is None
                    else "boundary_before_day_order_cancellation"
                ),
                "day_order_cancellation_modeled": False,
                "subsequent_shutdown_poll_modeled": self.end_at is None,
                "open_orders_and_positions_are_final_settlement": False,
            },
            "data_quality": {
                **self.dataset.validation_report.to_mapping(),
                "selected_contract": {
                    "instrument_id": contract.instrument_id,
                    "observed_bar_count": contract.observed_bar_count,
                    "missing_bar_count": contract.missing_bar_count,
                },
            },
            "trace_digest": None,
            "legacy_orders_state": module.orders,
        }

    def _install_decision_tracing(self, module, trace: TraceRecorder) -> None:
        original_place = module.place_duo_order
        original_restrictions = module.check_changes_in_restrictions
        original_reload = module.reread_option_defaults

        def place_duo_order(*args, **kwargs):
            before_state = self._legacy_state(module)
            result = original_place(*args, **kwargs)
            after_state = self._legacy_state(module)
            trace.add(
                "WAVE_DECISION",
                at=module.get_ist_now(),
                actor="we-1",
                operation="place_duo_order",
                input_data={
                    "symbol": module.symbol,
                    "position": module.current_positions,
                    "initial_position": module.initial_positions,
                    "base_buy_gap": module.buy_gap,
                    "base_sell_gap": module.sell_gap,
                    "wave_settings_hashes": self.scenario.wave_settings_hashes,
                },
                result={
                    "restrictions": {
                        "buy": module.global_restrict_buy,
                        "sell": module.global_restrict_sell,
                    },
                    "multiplier_info": dict(module._last_multiplier_info),
                    "tracked_order_ids": sorted(str(key) for key in module.orders),
                },
                details={
                    "fill_cooldown": self.scenario.wave_settings["fill_cooldown"],
                    "delta_multiplier": self.scenario.wave_settings["delta_multiplier"],
                    "velocity_guard": self.scenario.wave_settings["velocity_guard"],
                    "multiplier_scale": self.scenario.wave_settings["multiplier_scale"],
                },
                state_before=state_hash(before_state),
                state_after=state_hash(after_state),
            )
            return result

        def check_changes_in_restrictions(*args, **kwargs):
            before_state = self._legacy_state(module)
            result = original_restrictions(*args, **kwargs)
            trace.add(
                "WAVE_DECISION",
                at=module.get_ist_now(),
                actor="we-1",
                operation="check_changes_in_restrictions",
                input_data={"symbol": module.symbol, "tracked_orders": sorted(str(key) for key in before_state["orders"])},
                result={
                    "restrictions": {
                        "buy": module.global_restrict_buy,
                        "sell": module.global_restrict_sell,
                    },
                    "tracked_order_ids": sorted(str(key) for key in module.orders),
                },
                state_before=state_hash(before_state),
                state_after=state_hash(self._legacy_state(module)),
            )
            return result

        module.place_duo_order = place_duo_order
        module.check_changes_in_restrictions = check_changes_in_restrictions

        def reread_option_defaults(*args, **kwargs):
            result = original_reload(*args, **kwargs)
            trace.add(
                "WAVE_CONFIG_RELOADED",
                at=module.get_ist_now(),
                actor="we-1",
                details={
                    "config_hashes": self.scenario.wave_settings_hashes,
                    "effective_delta_limits_hash": state_hash(module.delta_limits_config),
                    "expected_delta_limits_hash": state_hash(
                        self.scenario.wave_settings["delta_limits"]
                    ),
                },
            )
            return result

        module.reread_option_defaults = reread_option_defaults


    @staticmethod
    def _legacy_state(module) -> dict[str, Any]:
        return copy.deepcopy({
            "orders": module.orders,
            "initial_positions": module.initial_positions,
            "current_positions": module.current_positions,
            "restrict_buy": module.global_restrict_buy,
            "restrict_sell": module.global_restrict_sell,
            "last_multiplier_info": module._last_multiplier_info,
            "same_side_fill_times": module._same_side_fill_times,
            "cooldown_active_until": module._cooldown_active_until,
            "pending_gtt_fallbacks": module.pending_gtt_fallbacks,
        })
