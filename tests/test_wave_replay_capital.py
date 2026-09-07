from decimal import Decimal, ROUND_HALF_UP

from wave_replay.broker_engine import FeeSchedule
from wave_replay.broker_engine import BrokerEngine
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.schemas import IST
from wave_replay.tracing import TraceRecorder
import datetime as dt
from wave_replay.capital import (
    DISCLAIMER,
    MODEL_ID,
    calculate_capital_peaks,
    calculate_capital_snapshot,
    calculate_profit_on_peak_capital,
)
from wave_replay.continuous import _close_at_opening_switch
from wave_replay.datasets import ReplayDataset
from wave_replay.synthetic import create_synthetic_dataset


def _order(symbol, side, quantity, price, status="OPEN"):
    return {
        "symbol": symbol,
        "side": side,
        "remaining": quantity,
        "price": Decimal(str(price)),
        "status": status,
    }


def _position(buys, sells):
    return {
        "buy_quantity": buys,
        "sell_quantity": sells,
        "quantity": buys - sells,
    }


def test_exact_contract_netting_and_no_cross_contract_offsets():
    snapshot = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={
            "A": _position(65, 130),
            "B": _position(65, 0),
        },
        orders=[],
        option_marks={"A": Decimal("50"), "B": Decimal("40")},
        underlying_spot=Decimal("26000"),
    )
    assert snapshot["model_id"] == MODEL_ID
    assert snapshot["disclaimer"] == DISCLAIMER
    assert snapshot["capital_used_excluding_margin"] == "2600"
    assert snapshot["projected_short_qty"] == 65
    assert snapshot["estimated_naked_margin"] == "169000.00"
    assert snapshot["capital_used_including_margin"] == "171600.00"
    assert snapshot["capital_recommended"] == "188760.0000"


def test_pending_sell_is_naked_and_pending_buy_only_reserves_premium():
    snapshot = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={"A": _position(0, 0)},
        orders=[
            _order("A", "SELL", 65, 60),
            _order("A", "BUY", 65, 45),
        ],
        option_marks={"A": Decimal("50")},
        underlying_spot=Decimal("26000"),
    )
    row = snapshot["decomposition"][0]
    assert row["pending_buy_qty"] == 65
    assert row["pending_sell_qty"] == 65
    assert row["conservative_projected_short_qty"] == 65
    assert snapshot["capital_used_excluding_margin"] == "2925"
    assert snapshot["estimated_naked_margin"] == "169000.00"


def test_both_filled_same_contract_nets_and_long_only_uses_mark():
    flat = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={"A": _position(65, 65)},
        orders=[],
        option_marks={"A": Decimal("50")},
        underlying_spot=Decimal("26000"),
    )
    assert flat["capital_used_excluding_margin"] == "0"
    assert flat["estimated_naked_margin"] == "0.00"

    long = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={"A": _position(130, 65)},
        orders=[],
        option_marks={"A": Decimal("55")},
        underlying_spot=None,
    )
    assert long["capital_used_excluding_margin"] == "3575"
    assert long["estimated_naked_margin"] == "0.00"
    assert long["metric_available"] is True


def test_missing_mark_or_spot_fails_closed():
    missing_mark = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={"A": _position(65, 0)},
        orders=[],
        option_marks={},
        underlying_spot=Decimal("26000"),
    )
    assert missing_mark["capital_used_excluding_margin"] is None
    assert missing_mark["capital_used_including_margin"] is None
    assert missing_mark["missing_option_marks"] == ["A"]

    missing_spot = calculate_capital_snapshot(
        at="2026-01-05T10:00:00+05:30",
        positions={"A": _position(0, 65)},
        orders=[],
        option_marks={"A": Decimal("50")},
        underlying_spot=None,
    )
    assert missing_spot["estimated_naked_margin"] is None
    assert missing_spot["capital_used_including_margin"] is None
    assert missing_spot["missing_underlying_spot"] is True


def test_peak_tracking_and_simple_linear_annualization():
    points = [
        {
            "at": "2026-01-01T10:00:00+05:30",
            "underlying_spot": "26000",
            "gross_filled_net_qty": 65,
            "net_filled_qty": -65,
            "projected_short_qty": 65,
            "capital_used_excluding_margin": "1000",
            "capital_used_including_margin": "170000",
            "capital_recommended": "187000",
            "decomposition": [{"symbol": "A"}],
        },
        {
            "at": "2026-01-02T10:00:00+05:30",
            "underlying_spot": "26100",
            "gross_filled_net_qty": 130,
            "net_filled_qty": -130,
            "projected_short_qty": 130,
            "capital_used_excluding_margin": "2000",
            "capital_used_including_margin": "341300",
            "capital_recommended": "375430",
            "decomposition": [{"symbol": "A"}, {"symbol": "B"}],
        },
    ]
    peaks = calculate_capital_peaks(points)
    assert peaks["capital_used_including_margin"]["at"].startswith("2026-01-02")
    assert len(peaks["capital_recommended"]["decomposition"]) == 2
    profit = calculate_profit_on_peak_capital(
        monthly_profit=Decimal("3413"),
        peaks=peaks,
    )
    assert profit["monthly_profit_pct_including_margin"] == "1.00"
    assert profit["annualized_profit_pct_including_margin"] == "12.00"
    assert profit["classification"] == "in_sample_linear_annualized"

    zero = calculate_profit_on_peak_capital(
        monthly_profit=Decimal("10"),
        peaks={
            "capital_used_excluding_margin": {"value": "0"},
            "capital_used_including_margin": {"value": "0"},
        },
    )
    assert zero["monthly_profit_pct_excluding_margin"] is None
    assert zero["annualized_profit_pct_including_margin"] is None

    incomplete = calculate_capital_peaks([
        points[0],
        {
            **points[1],
            "capital_used_including_margin": None,
            "capital_recommended": None,
        },
    ])
    assert incomplete["capital_used_including_margin"] is None
    assert incomplete["coverage"]["capital_used_including_margin"][
        "missing_observation_count"
    ] == 1


def _fee_schedule():
    return FeeSchedule({
        "schema": "wave-replay-fee-schedule-v1",
        "usage_label": "upstox_approximation",
        "broker_fidelity": "not_zerodha",
        "resolved_schedule": {
            "broker": {"option_brokerage": "20"},
            "statutory": {
                "option_exchange_rate": "0.00053",
                "sebi_rate": "0.000001",
                "option_stt_rate": "0.0015",
                "option_stamp_rate": "0.00003",
                "gst_rate": "0.18",
            },
        },
    })


def test_upstox_fee_component_and_full_precision_parity():
    schedule = _fee_schedule()
    buy = schedule.calculate_breakdown(
        side="BUY",
        quantity=75,
        price=Decimal("100"),
    )
    sell = schedule.calculate_breakdown(
        side="SELL",
        quantity=75,
        price=Decimal("120"),
    )
    aggregate = {
        name: buy[name] + sell[name]
        for name in buy
    }
    rounded_components = sum(
        (
            aggregate[name].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            for name in (
                "brokerage",
                "exchange",
                "stt",
                "sebi",
                "gst",
                "stamp_duty",
                "dp",
                "ipft",
            )
        ),
        Decimal("0"),
    )
    assert aggregate["brokerage"] == Decimal("40")
    assert aggregate["stt"] == Decimal("13.5000")
    assert aggregate["exchange"] == Decimal("8.74500")
    assert rounded_components == Decimal("71.28")
    assert aggregate["total"] == Decimal("71.26357000")


def test_actual_bear_call_fee_fixture_matches_indian_wheel():
    schedule = _fee_schedule()
    entry = (
        schedule.calculate_breakdown(
            side="SELL", quantity=65, price=Decimal("55.10")
        )["total"]
        + schedule.calculate_breakdown(
            side="BUY", quantity=65, price=Decimal("42.85")
        )["total"]
    )
    exit_fees = (
        schedule.calculate_breakdown(
            side="BUY", quantity=65, price=Decimal("55.25")
        )["total"]
        + schedule.calculate_breakdown(
            side="SELL", quantity=65, price=Decimal("42.50")
        )["total"]
    )
    assert entry == Decimal("56.6450857150")
    assert exit_fees == Decimal("55.4326201750")


def test_full_precision_fee_mode_survives_continuous_state_restore():
    trace = TraceRecorder()
    engine = BrokerEngine(
        scheduler=DeterministicScheduler(
            start=dt.datetime(2026, 1, 5, 9, 15, tzinfo=IST),
            trace=trace,
        ),
        trace=trace,
        fee_schedule=_fee_schedule(),
        full_precision_fees=True,
    )
    engine.restore_persistent_state({
        "cash": Decimal("1000000"),
        "fees_paid": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "positions": {},
        "gtts": {},
        "order_counter": 0,
        "trigger_counter": 0,
    })
    assert engine.full_precision_fees is True


def test_experiment_brokerage_is_charged_per_nonzero_partial_fill():
    trace = TraceRecorder()
    engine = BrokerEngine(
        scheduler=DeterministicScheduler(
            start=dt.datetime(2026, 1, 5, 9, 15, tzinfo=IST),
            trace=trace,
        ),
        trace=trace,
        fee_schedule=_fee_schedule(),
        full_precision_fees=True,
    )
    order_id = engine.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYTESTCE",
        transaction_type="BUY",
        quantity=65,
        product="NRML",
        order_type="LIMIT",
        price=50,
    )
    engine.partial_fill(order_id, quantity=25, price=Decimal("50"))
    engine._apply_fill(
        engine.orders_by_id[order_id],
        quantity=40,
        price=Decimal("50"),
        complete=True,
    )
    assert engine.fee_components_paid["brokerage"] == Decimal("40")
    assert len(engine.fee_events) == 2


def test_opening_switch_records_pre_liquidation_capital_and_full_precision_fee(tmp_path):
    dataset = ReplayDataset.load(
        create_synthetic_dataset(
            tmp_path / "dataset",
            corpus=[{
                "role": "range",
                "date": dt.date(2026, 1, 5),
                "strike": Decimal("24000"),
                "first_close": Decimal("23995"),
                "expiry": dt.date(2026, 1, 13),
                "lot_size": 65,
                "missing": 0,
                "option_base": "50",
            }],
            integrity_profile_override="wave-replay-special-sessions-v3",
        ),
        allow_special_sessions_v3=True,
    )
    contract = dataset.contracts[0]
    option_bar = min(dataset.option_bars, key=lambda bar: bar.bar_end)
    state = {
        "cash": Decimal("1000000"),
        "fees_paid": Decimal("0"),
        "fee_components_paid": {
            name: Decimal("0")
            for name in (
                "brokerage",
                "exchange",
                "stt",
                "sebi",
                "gst",
                "stamp_duty",
                "dp",
                "ipft",
            )
        },
        "realized_pnl": Decimal("0"),
        "positions": {
            contract.wave_trading_symbol: {
                "tradingsymbol": contract.wave_trading_symbol,
                "instrument_token": contract.instrument_token,
                "quantity": -65,
                "buy_quantity": 0,
                "sell_quantity": 65,
                "buy_price": 0,
                "sell_price": 50,
                "last_price": 50,
                "average_price": 50,
                "m2m": 0,
                "realised": 0,
                "product": "NRML",
            }
        },
        "gtts": {},
        "order_counter": 0,
        "trigger_counter": 0,
    }
    _state, _trace, switch = _close_at_opening_switch(
        dataset,
        state,
        contract,
        option_bar,
        reason="test",
        full_precision_fees=True,
    )
    opening = switch["capital_snapshots"][0]
    assert opening["projected_short_qty"] == 65
    assert opening["estimated_naked_margin"] == "155967.500"
    assert switch["fee_events"]
