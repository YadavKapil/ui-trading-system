import datetime as dt
from decimal import Decimal

import pytest

from wave_replay.broker_engine import BrokerEngine, FeeSchedule
from wave_replay.clock import SimClock
from wave_replay.fake_kite import FakeKite
from wave_replay.fake_ticker import FakeTicker
from wave_replay.faults import BrokerTransportError, FaultScript
from wave_replay.fill_models import close_only_fill, validate_sibling_bracket
from wave_replay.schemas import IST, MarketBar, VisibilityOffsets
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.tasks import SimTaskRuntime
from wave_replay.tracing import TraceRecorder


def _broker():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 16, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    fees = FeeSchedule({
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
    engine = BrokerEngine(
        scheduler=scheduler,
        trace=trace,
        visibility=VisibilityOffsets(),
        fee_schedule=fees,
    )
    return trace, scheduler, tasks, engine, FakeKite(engine)


def test_close_only_requires_subsequent_bar_and_fills_at_limit():
    created = dt.datetime(2025, 7, 7, 9, 16, tzinfo=IST)
    same = close_only_fill(
        side="BUY",
        limit_price=Decimal("90"),
        close=Decimal("80"),
        created_at_bar_end=created,
        current_bar_end=created,
    )
    later = close_only_fill(
        side="BUY",
        limit_price=Decimal("90"),
        close=Decimal("80"),
        created_at_bar_end=created,
        current_bar_end=created + dt.timedelta(minutes=1),
    )
    assert same.touched is False
    assert later.fill_price == Decimal("90")


def test_regular_partial_modify_complete_and_cancel_lifecycle():
    trace, scheduler, _tasks, engine, kite = _broker()
    engine.set_quote("NFO:NIFTYCE", last_price=Decimal("100"), instrument_token=123)
    buy = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=75,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    engine.partial_fill(buy, quantity=25, price=Decimal("90"))
    kite.modify_order("regular", buy, price=91)
    scheduler.run()
    scheduler.now += dt.timedelta(minutes=1)
    bar = MarketBar(
        instrument_id="id",
        source_instrument_id="source",
        replay_instrument_token=123,
        wave_trading_symbol="NIFTYCE",
        lean_symbol="lean",
        underlying="NIFTY",
        security_type="index_option",
        market="india",
        exchange="NFO",
        session_date=scheduler.now.date(),
        bar_start=scheduler.now - dt.timedelta(minutes=1),
        bar_end=scheduler.now,
        available_at=scheduler.now,
        resolution_seconds=60,
        open=Decimal("90"),
        high=Decimal("92"),
        low=Decimal("89"),
        close=Decimal("90"),
        volume=100,
        open_interest=1000,
        source_archive_sha256="hash",
        source_archive="archive",
        source_member="member",
        source_member_sha256="member-hash",
        source_row=1,
        source_universe_sha256="universe-hash",
    )
    assert engine.evaluate_close_bar(bar) == [buy]
    scheduler.run()
    assert engine.orders_by_id[buy].status == "COMPLETE"
    assert engine.positions_by_symbol["NIFTYCE"]["quantity"] == 75
    assert engine.fees_paid > 0
    sell = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="SELL",
        quantity=75,
        product="NRML",
        order_type="LIMIT",
        price=110,
        tag="S",
    )
    kite.cancel_order(variety="regular", order_id=sell)
    assert engine.orders_by_id[sell].status == "CANCELLED"
    assert {entry["status"] for entry in engine.order_history(buy)} >= {"OPEN", "PARTIAL", "COMPLETE"}
    assert trace.events


def test_scripted_gtt_preserves_tag_and_creates_distinct_child():
    _trace, _scheduler, _tasks, engine, kite = _broker()
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
            "tag": "GTT_B_1",
        }],
    )
    child = engine.trigger_gtt(trigger["trigger_id"])
    assert engine.orders_by_id[child].tag == "GTT_B_1"
    assert engine.gtts_by_id[trigger["trigger_id"]].status == "triggered"


def test_invalid_bracket_is_rejected():
    try:
        validate_sibling_bracket(Decimal("100"), Decimal("90"))
    except ValueError as exc:
        assert str(exc) == "INVALID_OVERLAPPING_BRACKET"
    else:
        raise AssertionError("overlapping bracket should fail")


def test_visibility_offsets_delay_callbacks_history_orders_and_positions():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 16, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    engine = BrokerEngine(
        scheduler=scheduler,
        trace=trace,
        visibility=VisibilityOffsets(
            callback_seconds=2,
            order_history_seconds=3,
            orders_seconds=4,
            positions_seconds=5,
        ),
    )
    kite = FakeKite(engine)
    ticker = FakeTicker(scheduler, tasks, trace)
    updates = []
    ticker.on_order_update = lambda _ws, data: updates.append(data["status"])
    ticker.connect()
    engine.bind_callback_dispatch(ticker.order_dispatch)
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    assert kite.order_history(order_id) == []
    assert kite.orders() == []
    scheduler.run(until=scheduler.now + dt.timedelta(seconds=2))
    assert updates == ["ACK", "OPEN"]
    scheduler.schedule_after(2, lambda: None)
    scheduler.run()
    assert kite.order_history(order_id)[-1]["status"] == "OPEN"
    assert kite.orders()[-1]["status"] == "OPEN"


def test_transport_failure_and_reject_are_distinct():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 16, tzinfo=IST),
        trace=trace,
    )
    engine = BrokerEngine(
        scheduler=scheduler,
        trace=trace,
        faults=FaultScript([{"operation": "place_order", "error_type": "transport", "message": "timeout"}]),
    )
    kite = FakeKite(engine)
    with pytest.raises(BrokerTransportError, match="timeout"):
        kite.place_order(
            variety="regular",
            exchange="NFO",
            tradingsymbol="NIFTYCE",
            transaction_type="BUY",
            quantity=25,
            product="NRML",
            order_type="LIMIT",
            price=90,
            tag="B",
        )
    engine.faults = FaultScript()
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    engine.reject(order_id, reason="margin")
    assert engine.orders_by_id[order_id].status == "REJECTED"


def test_default_callback_precedes_rest_order_and_position_visibility():
    trace, scheduler, tasks, engine, kite = _broker()
    ticker = FakeTicker(scheduler, tasks, trace)
    observed = {}

    def callback(_ws, data):
        if data["status"] == "COMPLETE":
            observed["history_status"] = kite.order_history(data["order_id"])[-1]["status"]
            observed["orders_status"] = kite.orders()[-1]["status"]
            observed["positions"] = kite.positions()["net"]

    ticker.on_order_update = callback
    ticker.connect()
    engine.bind_callback_dispatch(ticker.order_dispatch)
    engine.set_quote("NFO:NIFTYCE", last_price=Decimal("100"), instrument_token=123)
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    scheduler.run()
    scheduler.now += dt.timedelta(minutes=1)
    bar = MarketBar(
        instrument_id="id",
        source_instrument_id="source",
        replay_instrument_token=123,
        wave_trading_symbol="NIFTYCE",
        lean_symbol="lean",
        underlying="NIFTY",
        security_type="indexoption",
        market="india",
        exchange="NFO",
        session_date=scheduler.now.date(),
        bar_start=scheduler.now - dt.timedelta(minutes=1),
        bar_end=scheduler.now,
        available_at=scheduler.now,
        resolution_seconds=60,
        open=Decimal("90"),
        high=Decimal("91"),
        low=Decimal("89"),
        close=Decimal("90"),
        volume=100,
        open_interest=1000,
        source_archive_sha256="hash",
        source_archive="archive",
        source_member="member",
        source_member_sha256="member-hash",
        source_row=1,
        source_universe_sha256="universe-hash",
    )
    engine.evaluate_close_bar(bar)
    scheduler.run()
    assert engine.orders_by_id[order_id].status == "COMPLETE"
    assert observed == {"history_status": "OPEN", "orders_status": "OPEN", "positions": []}
    assert kite.positions()["net"][0]["quantity"] == 25


def test_callback_mutation_cannot_change_rest_history_and_terminal_orders_stay_terminal():
    trace, scheduler, tasks, engine, kite = _broker()
    ticker = FakeTicker(scheduler, tasks, trace)

    def callback(_ws, data):
        data["status"] = "MUTATED"
        data["price"] = -1

    ticker.on_order_update = callback
    ticker.connect()
    engine.bind_callback_dispatch(ticker.order_dispatch)
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    scheduler.run()
    assert kite.orders()[-1]["status"] == "OPEN"
    assert kite.orders()[-1]["price"] == 90
    kite.cancel_order(variety="regular", order_id=order_id)
    scheduler.run()
    with pytest.raises(RuntimeError, match="terminal"):
        engine.reject(order_id)
    with pytest.raises(RuntimeError, match="state CANCELLED"):
        engine.partial_fill(order_id, quantity=1, price=Decimal("90"))


def test_partial_fills_charge_flat_brokerage_once_per_order():
    def total_fee(split):
        _trace, _scheduler, _tasks, engine, kite = _broker()
        order_id = kite.place_order(
            variety="regular",
            exchange="NFO",
            tradingsymbol="NIFTYCE",
            transaction_type="BUY",
            quantity=75,
            product="NRML",
            order_type="LIMIT",
            price=100,
            tag="B",
        )
        if split:
            engine.partial_fill(order_id, quantity=25, price=Decimal("100"))
            engine._apply_fill(
                engine.orders_by_id[order_id],
                quantity=50,
                price=Decimal("100"),
                complete=True,
            )
        else:
            engine._apply_fill(
                engine.orders_by_id[order_id],
                quantity=75,
                price=Decimal("100"),
                complete=True,
            )
        return engine.fees_paid

    assert abs(total_fee(True) - total_fee(False)) < Decimal("0.05")


def test_positions_mark_to_market_on_each_completed_option_bar():
    _trace, scheduler, _tasks, engine, kite = _broker()
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    engine._apply_fill(
        engine.orders_by_id[order_id],
        quantity=25,
        price=Decimal("90"),
        complete=True,
    )
    scheduler.run()
    scheduler.now += dt.timedelta(minutes=1)
    bar = MarketBar(
        instrument_id="id",
        source_instrument_id="source",
        replay_instrument_token=123,
        wave_trading_symbol="NIFTYCE",
        lean_symbol="lean",
        underlying="NIFTY",
        security_type="indexoption",
        market="india",
        exchange="NFO",
        session_date=scheduler.now.date(),
        bar_start=scheduler.now - dt.timedelta(minutes=1),
        bar_end=scheduler.now,
        available_at=scheduler.now,
        resolution_seconds=60,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=100,
        open_interest=1000,
        source_archive_sha256="hash",
        source_archive="archive",
        source_member="member",
        source_member_sha256="member-hash",
        source_row=2,
        source_universe_sha256="universe-hash",
    )
    engine.evaluate_close_bar(bar)
    scheduler.run()
    position = kite.positions()["net"][0]
    assert position["last_price"] == 100.0
    assert position["m2m"] == 250.0


def test_limit_fill_keeps_completed_close_as_current_market_mark():
    _trace, scheduler, _tasks, engine, kite = _broker()
    order_id = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    scheduler.run()
    scheduler.now += dt.timedelta(minutes=1)
    template = MarketBar(
        instrument_id="id",
        source_instrument_id="source",
        replay_instrument_token=123,
        wave_trading_symbol="NIFTYCE",
        lean_symbol="lean",
        underlying="NIFTY",
        security_type="indexoption",
        market="india",
        exchange="NFO",
        session_date=scheduler.now.date(),
        bar_start=scheduler.now - dt.timedelta(minutes=1),
        bar_end=scheduler.now,
        available_at=scheduler.now,
        resolution_seconds=60,
        open=Decimal("85"),
        high=Decimal("86"),
        low=Decimal("79"),
        close=Decimal("80"),
        volume=100,
        open_interest=1000,
        source_archive_sha256="hash",
        source_archive="archive",
        source_member="member",
        source_member_sha256="member-hash",
        source_row=3,
        source_universe_sha256="universe-hash",
    )
    engine.evaluate_close_bar(template)
    scheduler.run()
    order = engine.orders_by_id[order_id]
    position = kite.positions()["net"][0]
    assert order.average_price == Decimal("90")
    assert position["last_price"] == 80.0
    assert position["m2m"] == -250.0
