"""Small executable characterization scenarios for broker and orchestration edges."""

from __future__ import annotations

import datetime as dt
import random
from decimal import Decimal
from pathlib import Path
from typing import Any

from .bootstrap import ReplayBootstrap
from .broker_engine import BrokerEngine, FeeSchedule
from .clock import SimClock
from .fake_kite import FakeKite
from .fake_ticker import FakeTicker
from .faults import BrokerTransportError, FaultScript
from .schemas import IST, MarketBar, VisibilityOffsets
from .scheduler import DeterministicScheduler
from .tasks import SimTaskRuntime
from .tracing import TraceRecorder

EXECUTABLE_CASES = (
    "neutral_cycle",
    "long_short_accumulation",
    "long_buy_restriction",
    "hard_delta_restriction",
    "step_delta_velocity_fill_cooldown",
    "velocity_down",
    "sibling_cancellation",
    "repricing",
    "manual_reprice",
    "auto_reprice",
    "regular_reject",
    "partial_fill",
    "partial_then_reject",
    "cancel_failure",
    "callback_poll_race",
    "rest_lag",
    "read_faults",
    "gtt_fallback_trigger_delete_race",
    "gtt_complete_before_open",
    "market_close",
    "restart_recovery",
    "two_workers_shared_broker",
)


def _fee_schedule() -> FeeSchedule:
    return FeeSchedule({
        "schema": "wave-replay-fee-schedule-v1",
        "usage_label": "upstox_approximation",
        "broker_fidelity": "not_zerodha",
        "resolved_schedule": {
            "broker": {"option_brokerage": "20"},
            "statutory": {
                "option_stt_rate": "0.0015",
                "option_exchange_rate": "0.00053",
                "sebi_rate": "0.000001",
                "option_stamp_rate": "0.00003",
                "gst_rate": "0.18",
            },
        },
    })


def _harness(*, faults=None, visibility=None):
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 16, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    engine = BrokerEngine(
        scheduler=scheduler,
        trace=trace,
        faults=faults,
        visibility=visibility,
        fee_schedule=_fee_schedule(),
    )
    ticker = FakeTicker(scheduler, tasks, trace)
    engine.bind_callback_dispatch(ticker.order_dispatch)
    return trace, scheduler, tasks, engine, ticker, FakeKite(engine)


def _place(kite: FakeKite, side: str, price: int, quantity: int = 25, tag: str = "") -> str:
    return kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type=side,
        quantity=quantity,
        product="NRML",
        order_type="LIMIT",
        price=price,
        tag=tag or side,
    )


def _bar(scheduler, close: int) -> MarketBar:
    end = scheduler.now + dt.timedelta(minutes=1)
    return MarketBar(
        instrument_id="NIFTY|2025-07-17|C|25400",
        source_instrument_id="NSE_FO|EDGE",
        replay_instrument_token=123,
        wave_trading_symbol="NIFTYCE",
        lean_symbol="NIFTY 250717C25400000",
        underlying="NIFTY",
        security_type="indexoption",
        market="india",
        exchange="NFO",
        session_date=end.date(),
        bar_start=end - dt.timedelta(minutes=1),
        bar_end=end,
        available_at=end,
        resolution_seconds=60,
        open=Decimal(close),
        high=Decimal(close + 1),
        low=Decimal(close - 1),
        close=Decimal(close),
        volume=100,
        open_interest=1000,
        source_archive="edge.zip",
        source_archive_sha256="archive",
        source_member="edge.csv",
        source_member_sha256="member",
        source_row=1,
        source_universe_sha256="universe",
    )


def execute_case(case_id: str) -> dict[str, Any]:
    if case_id not in EXECUTABLE_CASES:
        raise KeyError(case_id)
    if case_id in {"hard_delta_restriction", "market_close"}:
        return _execute_legacy_probe(case_id)
    if case_id == "step_delta_velocity_fill_cooldown":
        return _execute_legacy_risk_pipeline()
    if case_id == "auto_reprice":
        return _execute_legacy_auto_reprice()
    if case_id == "manual_reprice":
        return _execute_legacy_manual_reprice()
    if case_id in {"restart_recovery", "two_workers_shared_broker"}:
        return _execute_worker_lifecycle(case_id)
    if case_id in {"long_buy_restriction", "velocity_down"}:
        return _execute_legacy_direction(case_id)
    if case_id in {"sibling_cancellation", "gtt_complete_before_open"}:
        return _execute_legacy_callback_case(case_id)
    if case_id == "cancel_failure":
        faults = FaultScript([{
            "kind": "failure",
            "operation": "cancel_order",
            "error_type": "transport",
            "message": "timeout",
        }])
    else:
        faults = FaultScript()
    visibility = (
        VisibilityOffsets(callback_seconds=0, order_history_seconds=2, orders_seconds=3, positions_seconds=4)
        if case_id in {"callback_poll_race", "rest_lag"}
        else VisibilityOffsets()
    )
    trace, scheduler, tasks, engine, ticker, kite = _harness(faults=faults, visibility=visibility)
    summary: dict[str, Any] = {}

    if case_id == "neutral_cycle":
        buy, sell = _place(kite, "BUY", 90, tag="B"), _place(kite, "SELL", 110, tag="S")
        engine._apply_fill(engine.orders_by_id[buy], quantity=25, price=Decimal("90"), complete=True)
        kite.cancel_order(variety="regular", order_id=sell)
        summary = {"buy": engine.orders_by_id[buy].status, "sell": engine.orders_by_id[sell].status}
    elif case_id == "long_short_accumulation":
        buy, sell = _place(kite, "BUY", 90, 50), _place(kite, "SELL", 110, 75)
        engine._apply_fill(engine.orders_by_id[buy], quantity=50, price=Decimal("90"), complete=True)
        engine._apply_fill(engine.orders_by_id[sell], quantity=75, price=Decimal("110"), complete=True)
        summary = {"position": engine.positions_by_symbol["NIFTYCE"]["quantity"], "realized": str(engine.realized_pnl)}
    elif case_id == "repricing":
        order = _place(kite, "SELL", 110)
        kite.modify_order("regular", order, price=115)
        summary = {
            "price": str(engine.orders_by_id[order].price),
            "status": engine.orders_by_id[order].status,
            "auto_reprice": False,
        }
    elif case_id == "regular_reject":
        order = _place(kite, "BUY", 90)
        engine.reject(order, reason="margin")
        summary = {"status": engine.orders_by_id[order].status}
    elif case_id == "partial_fill":
        order = _place(kite, "BUY", 90, 75)
        engine.partial_fill(order, quantity=25, price=Decimal("90"))
        summary = {"status": engine.orders_by_id[order].status, "filled": engine.orders_by_id[order].filled_quantity}
    elif case_id == "partial_then_reject":
        order = _place(kite, "BUY", 90, 75)
        engine.partial_fill(order, quantity=25, price=Decimal("90"))
        engine.reject(order, reason="remaining quantity rejected")
        summary = {
            "status": engine.orders_by_id[order].status,
            "filled": engine.orders_by_id[order].filled_quantity,
            "position": engine.positions_by_symbol["NIFTYCE"]["quantity"],
        }
    elif case_id == "cancel_failure":
        order = _place(kite, "BUY", 90)
        try:
            kite.cancel_order(variety="regular", order_id=order)
        except BrokerTransportError:
            pass
        summary = {"status": engine.orders_by_id[order].status, "failure_consumed": not engine.faults.remaining()}
    elif case_id in {"callback_poll_race", "rest_lag"}:
        callbacks = []
        polls = []
        ticker.on_order_update = lambda _ws, data: callbacks.append(
            {
                "callback": data["status"],
                "orders": [item["status"] for item in kite.orders()],
                "history": [item["status"] for item in kite.order_history(data["order_id"])],
            }
        )
        ticker.connect()
        order = _place(kite, "BUY", 90)
        if case_id == "callback_poll_race":
            scheduler.schedule_after(
                0,
                lambda: polls.append([item["status"] for item in kite.orders()]),
                priority=60,
            )
        scheduler.run()
        summary = {
            "callbacks": callbacks,
            "polls": polls,
            "visible": kite.orders()[-1]["status"],
            "order_id": order,
        }
    elif case_id == "read_faults":
        engine.faults = FaultScript([
            {
                "kind": "failure",
                "operation": "positions",
                "error_type": "stale",
                "stale_result": {"net": [{"tradingsymbol": "STALE"}], "day": []},
            },
            {
                "kind": "failure",
                "operation": "quote",
                "error_type": "transport",
                "message": "quote timeout",
            },
        ])
        stale = kite.positions()
        try:
            kite.quote("NSE:NIFTY 50")
        except BrokerTransportError:
            quote_failed = True
        else:
            quote_failed = False
        summary = {
            "stale_position_symbol": stale["net"][0]["tradingsymbol"],
            "quote_transport_failed": quote_failed,
            "all_faults_consumed": not engine.faults.remaining(),
        }
    elif case_id == "gtt_fallback_trigger_delete_race":
        trigger = kite.place_gtt(
            trigger_type="single",
            tradingsymbol="NIFTYCE",
            exchange="NFO",
            trigger_values=[90],
            last_price=100,
            orders=[{
                "tradingsymbol": "NIFTYCE",
                "transaction_type": "BUY",
                "quantity": 25,
                "product": "NRML",
                "order_type": "LIMIT",
                "price": 90,
                "tag": "GTT_B",
            }],
        )["trigger_id"]
        child = engine.trigger_gtt(trigger)
        engine.delete_gtt(trigger)
        summary = {
            "trigger_after_delete_race": engine.gtts_by_id[trigger].status,
            "child_tag": engine.orders_by_id[child].tag,
            "child_survived": child in engine.orders_by_id,
        }

    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor=case_id,
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": case_id, "summary": summary, "trace_digest": trace.digest()}


def _execute_worker_lifecycle(case_id: str) -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        first = bootstrap.load_worker("worker-a", [])
        if case_id == "restart_recovery":
            callbacks = []
            first.kws.on_order_update = lambda _ws, data: callbacks.append(("crashed", data["status"]))
            first.kws.connect()
            scheduler.run()
            first.orders["process-local"] = {"status": "OPEN"}
            broker_order = _place(kite, "SELL", 110)
            scheduler.run()
            bootstrap.loader.unload_all()
            restarted = bootstrap.load_worker("worker-restarted", [])
            restarted.kws.on_order_update = lambda _ws, data: callbacks.append(("restarted", data["status"]))
            restarted.kws.connect()
            scheduler.run()
            callbacks.clear()
            _place(kite, "BUY", 90)
            scheduler.run()
            summary = {
                "process_state_reset": restarted.orders == {},
                "broker_order_survived": broker_order in engine.orders_by_id,
                "worker_modules_distinct": first is not restarted,
                "post_restart_callback_workers": sorted({worker for worker, _status in callbacks}),
            }
        else:
            second = bootstrap.load_worker("worker-b", [])
            received = []
            first.kws.on_order_update = lambda _ws, data: received.append(("a", data["status"]))
            second.kws.on_order_update = lambda _ws, data: received.append(("b", data["status"]))
            first.kws.connect()
            second.kws.connect()
            first.orders["local"] = {}
            broker_order = _place(kite, "BUY", 90)
            scheduler.run()
            summary = {
                "worker_globals_isolated": second.orders == {},
                "worker_modules_distinct": first is not second,
                "listeners": sorted({worker for worker, _status in received}),
                "shared_broker_order": broker_order in engine.orders_by_id,
            }
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor=case_id,
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": case_id, "summary": summary, "trace_digest": trace.digest()}


def _execute_legacy_probe(case_id: str) -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    if case_id == "market_close":
        scheduler.now = dt.datetime(2025, 7, 7, 15, 31, tzinfo=IST)
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker(f"scenario-{case_id}", [])
        if case_id == "hard_delta_restriction":
            positive = module.reset_restrictions()["nifty"]
            negative = module.reset_restrictions()["nifty"]
            module._apply_delta_restriction(positive, 4000, -3500, 3500)
            module._apply_delta_restriction(negative, -4000, -3500, 3500)
            summary = {
                "positive": {
                    "buy": int(positive["ce"]["buy"] == "no"),
                    "sell": int(positive["ce"]["sell"] == "no"),
                },
                "negative": {
                    "buy": int(negative["ce"]["buy"] == "no"),
                    "sell": int(negative["ce"]["sell"] == "no"),
                },
            }
        else:
            replacements = {"count": 0}
            module.symbol = "NIFTYTESTCE"
            module.orders = {
                "1": {
                    "price": 100,
                    "quantity": 25,
                    "transaction_type": "SELL",
                    "symbol": module.symbol,
                    "associated_order": -1,
                    "system_cancelled": False,
                }
            }
            module.typeOfProduct = "NRML"
            module.exceptNFBNF = True
            module.buy_gap = module.sell_gap = 10.0
            module.buy_gap_percentage = module.sell_gap_percentage = 0.10
            module.initial_sell_gap = 10.0
            module.scraper_last_price = 100.0
            module.tag = "Scraper"
            module.cancel_all_pending_gtts_for_symbol = lambda _symbol: None
            module.printCurrentStatus = lambda: None
            module.place_duo_order = lambda *args, **kwargs: replacements.__setitem__(
                "count", replacements["count"] + 1
            )
            try:
                module.order_complete("1", True, "")
            except SystemExit as exc:
                exit_code = exc.code
            else:
                exit_code = None
            summary = {
                "market_open_1531": module.is_market_open(),
                "replacement_orders": replacements["count"],
                "system_exit": exit_code,
                "boundary_only": True,
            }
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor=case_id,
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": case_id, "summary": summary, "trace_digest": trace.digest()}


def _execute_legacy_risk_pipeline() -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    settings = {
        "multiplier_scale": {"-3": [1, 2.5], "0": [1, 1]},
        "delta_multiplier": {"soft_limit": 0, "hard_limit": 10, "max_multiplier": 3.0},
        "velocity_guard": {"window_minutes": 15, "threshold_pct": 0.001, "gap_multiplier": 1.5},
        "fill_cooldown": {"enabled": True, "window_minutes": 10, "trigger_count": 2, "cooldown_duration_minutes": 15},
    }
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker("scenario-risk-pipeline", [])
        module.symbol = "NIFTYTESTCE"
        module.symbol_type = "ce"
        module.exchange = "NFO"
        module.quantity = "25"
        module.buy_quantity = 25
        module.sell_quantity = 25
        module.buy_gap = module.sell_gap = 10.0
        module.buy_gap_percentage = module.sell_gap_percentage = 0.10
        module.initial_sell_gap = 10.0
        module.initial_positions = {"position": 0}
        module.current_positions = {"position": -75}
        module.scraper_last_price = 100.0
        module.multiplier_scale = settings["multiplier_scale"]
        module._we_config_cache = {"NIFTY": settings}
        module.cool_off_time = 0
        module.typeOfProduct = "NRML"
        module.tag = "Scraper"
        module.randint = random.Random(104729).randint
        module.live_trading_enabled = True
        module.orders = {}
        module.get_position_for_symbol = lambda _symbol: -75
        module.set_restrictions = lambda: module.reset_restrictions()
        module.get_nifty_current_greeks = lambda restrict_days=True: {"delta": 4000, "expiry_delta": {}}
        engine.set_quote("NFO:NIFTYTESTCE", last_price=Decimal("100"), instrument_token=123)
        engine.set_quote("NSE:NIFTY 50", last_price=Decimal("20020"), instrument_token=0)
        module._spot_price_history = [
            (scheduler.now - dt.timedelta(minutes=10), 20000.0),
            (scheduler.now, 20020.0),
        ]
        module.record_same_side_fill("SELL", module.symbol, settings)
        module.record_same_side_fill("SELL", module.symbol, settings)
        delta = module.get_delta_multiplier("ce", module.symbol)
        velocity = module.get_velocity_multiplier("ce", module.symbol, settings)
        cooldown = module.is_side_in_cooldown("SELL", module.symbol)
        module.place_duo_order(module.symbol, "NRML", True)
        multiplier = dict(module._last_multiplier_info)
        summary = {
            "step": multiplier["step"],
            "step_sell_multiplier": multiplier["scale_sell"],
            "delta_multiplier": max(delta),
            "velocity_multiplier": max(velocity),
            "fill_cooldown_activated": cooldown,
            "sell_suppressed": multiplier["sell_cooldown"],
        }
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor="step_delta_velocity_fill_cooldown",
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {
        "case_id": "step_delta_velocity_fill_cooldown",
        "summary": summary,
        "trace_digest": trace.digest(),
    }


def _execute_legacy_auto_reprice() -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker("scenario-auto-reprice", [])
        order_id = _place(kite, "BUY", 90)
        module.symbol = "NIFTYTESTCE"
        module.symbol_type = "ce"
        module.buy_gap = module.sell_gap = 10.0
        module.orders = {
            order_id: {
                "price": 90.0,
                "quantity": 25,
                "transaction_type": "BUY",
                "symbol": module.symbol,
                "associated_order": -1,
                "multiplier_info": {
                    "anchor_buy_price": 100.0,
                    "base_buy_gap": 10.0,
                    "scale_buy": 1.0,
                    "final_buy_gap": 10.0,
                },
            }
        }
        module.load_wave_extractor_config = lambda _symbol: {}
        module.get_delta_multiplier = lambda _symbol_type, _symbol: (2.0, 1.0)
        module.get_velocity_multiplier = lambda _symbol_type, _symbol, _config: (1.0, 1.0)
        repriced = module._reprice_orders_on_multiplier_change()
        summary = {
            "repriced_count": repriced,
            "legacy_price": module.orders[order_id]["price"],
            "broker_price": str(engine.orders_by_id[order_id].price),
            "auto_reprice_pending_callback": order_id in module._pending_auto_reprice_ids,
        }
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor="auto_reprice",
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": "auto_reprice", "summary": summary, "trace_digest": trace.digest()}


def _execute_legacy_manual_reprice() -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker("scenario-manual-reprice", [])
        module.already_updating_order = 0
        module.orders = {
            "1": {
                "price": 90.0,
                "quantity": 25,
                "transaction_type": "BUY",
                "symbol": "NIFTYTESTCE",
                "associated_order": -1,
            }
        }
        module.on_order_update(None, {
            "order_id": "1",
            "status": "OPEN",
            "price": 95.0,
            "quantity": 25,
        })
        summary = {
            "manually_overridden": module.orders["1"]["manually_overridden"],
            "legacy_price": module.orders["1"]["price"],
        }
    trace.add("CHARACTERIZATION_RESULT", at=scheduler.now, actor="manual_reprice", details=summary)
    return {"case_id": "manual_reprice", "summary": summary, "trace_digest": trace.digest()}


def _execute_legacy_direction(case_id: str) -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker(f"scenario-{case_id}", [])
        if case_id == "velocity_down":
            config = {
                "velocity_guard": {
                    "window_minutes": 15,
                    "threshold_pct": 0.1,
                    "gap_multiplier": 1.5,
                }
            }
            module._spot_price_history = [
                (scheduler.now - dt.timedelta(minutes=10), 20000.0),
                (scheduler.now, 19900.0),
            ]
            buy, sell = module.get_velocity_multiplier("ce", "NIFTYTESTCE", config)
            summary = {"buy_multiplier": buy, "sell_multiplier": sell}
        else:
            module.symbol = "NIFTYTESTCE"
            module.symbol_type = "ce"
            module.exchange = "NFO"
            module.quantity = "25"
            module.buy_quantity = module.sell_quantity = 25
            module.buy_gap = module.sell_gap = 10.0
            module.buy_gap_percentage = module.sell_gap_percentage = 0.10
            module.initial_sell_gap = 10.0
            module.initial_positions = {"position": 0}
            module.current_positions = {"position": 25}
            module.scraper_last_price = 100.0
            module.multiplier_scale = {"1": [1.3, 1]}
            module._we_config_cache = {"NIFTY": {"cool_off_time": 0, "multiplier_scale": module.multiplier_scale}}
            module.live_trading_enabled = True
            module.randint = random.Random(104729).randint
            module.order_gtt_regular = "regular"
            module.cool_off_time = 0
            module.get_position_for_symbol = lambda _symbol: 25
            module.set_restrictions = lambda: module.reset_restrictions()
            module.get_delta_multiplier = lambda *_args: (1.0, 1.0)
            module.get_velocity_multiplier = lambda *_args: (1.0, 1.0)
            engine.set_quote("NFO:NIFTYTESTCE", last_price=Decimal("100"), instrument_token=123)
            engine.set_quote("NSE:NIFTY 50", last_price=Decimal("20000"), instrument_token=0)
            module.place_duo_order(module.symbol, "NRML", True)
            sides = sorted(order.side for order in engine.orders_by_id.values())
            summary = {
                "restrict_buy": module.global_restrict_buy,
                "restrict_sell": module.global_restrict_sell,
                "broker_order_sides": sides,
            }
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor=case_id,
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": case_id, "summary": summary, "trace_digest": trace.digest()}


def _execute_legacy_callback_case(case_id: str) -> dict[str, Any]:
    trace, scheduler, tasks, engine, ticker, kite = _harness()
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    with ReplayBootstrap(
        repository_root=Path.cwd(),
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
    ) as bootstrap:
        module = bootstrap.load_worker(f"scenario-{case_id}", [])
        replacement = {"count": 0}
        module.symbol = "NIFTYTESTCE"
        module.typeOfProduct = "NRML"
        module.exceptNFBNF = True
        module.buy_gap = module.sell_gap = 10.0
        module.buy_gap_percentage = module.sell_gap_percentage = 0.10
        module.initial_sell_gap = 10.0
        module.scraper_last_price = 100.0
        module.tag = "Scraper"
        module.already_updating_order = 0
        module.record_order_complete = lambda *_args: True
        module.cancel_all_pending_gtts_for_symbol = lambda _symbol: None
        module.place_duo_order = lambda *_args, **_kwargs: replacement.__setitem__(
            "count", replacement["count"] + 1
        )
        module.printCurrentStatus = lambda: None
        sibling = _place(kite, "SELL", 110)
        module.orders = {
            sibling: {
                "price": 110.0,
                "quantity": 25,
                "transaction_type": "SELL",
                "symbol": module.symbol,
                "associated_order": "2",
            }
        }
        if case_id == "sibling_cancellation":
            module.orders["2"] = {
                "price": 90.0,
                "quantity": 25,
                "transaction_type": "BUY",
                "symbol": module.symbol,
                "associated_order": sibling,
            }
            module.on_order_update(None, {
                "order_id": "2",
                "status": "COMPLETE",
                "average_price": 90.0,
                "price": 90.0,
                "quantity": 25,
            })
            summary = {
                "sibling_status": engine.orders_by_id[sibling].status,
                "replacement_count": replacement["count"],
                "completed_removed": "2" not in module.orders,
            }
        else:
            module.pending_gtt_fallbacks = {
                "GTT_B": {
                    "trigger_id": 1,
                    "symbol": module.symbol,
                    "exchange": "NFO",
                    "product": "NRML",
                    "quantity": 25,
                    "transaction_type": "BUY",
                    "price": 90.0,
                    "associated_regular_order_id": sibling,
                    "associated_gtt_tag": None,
                }
            }
            payload = {
                "order_id": "2",
                "status": "COMPLETE",
                "tag": "GTT_B",
                "transaction_type": "BUY",
                "tradingsymbol": module.symbol,
                "average_price": 90.0,
                "price": 90.0,
                "quantity": 25,
            }
            module.on_order_update(None, payload)
            summary = {
                "pending_gtt_removed": "GTT_B" not in module.pending_gtt_fallbacks,
                "sibling_status": engine.orders_by_id[sibling].status,
                "replacement_count": replacement["count"],
                "complete_before_open_processed": "2" not in module.orders,
            }
        tasks.cancel_all(reason="scenario_complete")
    trace.add(
        "CHARACTERIZATION_RESULT",
        at=scheduler.now,
        actor=case_id,
        details=summary,
        state_after=engine.snapshot_hash(),
    )
    return {"case_id": case_id, "summary": summary, "trace_digest": trace.digest()}
