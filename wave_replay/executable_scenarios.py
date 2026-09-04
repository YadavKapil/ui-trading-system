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
    "hard_delta_restriction",
    "step_delta_velocity_fill_cooldown",
    "sibling_cancellation",
    "repricing",
    "regular_reject",
    "partial_fill",
    "cancel_failure",
    "callback_poll_race",
    "rest_lag",
    "gtt_fallback_trigger_delete_race",
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

    if case_id in {"neutral_cycle", "sibling_cancellation"}:
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
        summary = {"price": str(engine.orders_by_id[order].price), "status": engine.orders_by_id[order].status}
    elif case_id == "regular_reject":
        order = _place(kite, "BUY", 90)
        engine.reject(order, reason="margin")
        summary = {"status": engine.orders_by_id[order].status}
    elif case_id == "partial_fill":
        order = _place(kite, "BUY", 90, 75)
        engine.partial_fill(order, quantity=25, price=Decimal("90"))
        summary = {"status": engine.orders_by_id[order].status, "filled": engine.orders_by_id[order].filled_quantity}
    elif case_id == "cancel_failure":
        order = _place(kite, "BUY", 90)
        try:
            kite.cancel_order(variety="regular", order_id=order)
        except BrokerTransportError:
            pass
        summary = {"status": engine.orders_by_id[order].status, "failure_consumed": not engine.faults.remaining()}
    elif case_id in {"callback_poll_race", "rest_lag"}:
        callbacks = []
        ticker.on_order_update = lambda _ws, data: callbacks.append(
            {
                "callback": data["status"],
                "orders": [item["status"] for item in kite.orders()],
                "history": [item["status"] for item in kite.order_history(data["order_id"])],
            }
        )
        ticker.connect()
        order = _place(kite, "BUY", 90)
        scheduler.run()
        summary = {"callbacks": callbacks, "visible": kite.orders()[-1]["status"], "order_id": order}
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
    elif case_id == "restart_recovery":
        order = _place(kite, "SELL", 110)
        ticker.register_listener("restarted", on_order_update=lambda _ws, _data: None)
        ticker.connect(listener_id="restarted")
        summary = {"broker_order_survived": order in engine.orders_by_id, "new_listener_connected": True}
    elif case_id == "two_workers_shared_broker":
        received = []
        for worker in ("a", "b"):
            ticker.register_listener(
                worker,
                on_order_update=lambda _ws, data, worker=worker: received.append((worker, data["status"])),
            )
            ticker.connect(listener_id=worker)
        _place(kite, "BUY", 90)
        scheduler.run()
        summary = {"listeners": sorted({worker for worker, _status in received}), "shared_orders": len(engine.orders_by_id)}

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
