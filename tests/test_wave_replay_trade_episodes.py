import copy
import hashlib
import json
from decimal import Decimal

import pytest

from wave_replay.trade_dashboard import (
    _downsample_preserving,
    _last_point_by_timestamp,
    build_trade_dashboard_payload,
    generate_trade_episode_dashboard,
)
from wave_replay.normalize import canonical_json, state_hash
from wave_replay.trade_episodes import build_trade_episode_model


SYMBOL = "NIFTYTESTCE"
CONTRACT = {
    "wave_trading_symbol": SYMBOL,
    "instrument_id": "NIFTY|2026-01-08|C|25000",
    "lean_symbol": "NIFTY 260108C25000000",
    "strike": "25000",
    "expiry": "2026-01-08",
    "lot_size": 10,
}


def _event(seq, kind, at, details, *, operation=None):
    return {
        "seq": seq,
        "kind": kind,
        "at": at,
        "continuous_session_date": at[:10],
        "operation": operation,
        "details": details,
        "input": {},
        "result": {},
    }


def _order(seq, order_id, side, at, *, pair_id=None, status="OPEN"):
    return _event(
        seq,
        "BROKER_ORDER_TRANSITION",
        at,
        {
            "order_id": str(order_id),
            "tradingsymbol": SYMBOL,
            "transaction_type": side,
            "quantity": 100,
            "filled_quantity": 0,
            "pending_quantity": 100,
            "order_type": "LIMIT",
            "price": 10,
            "status": status,
            "tag": f"{side[0]}_{order_id}",
            "bracket_id": pair_id,
        },
        operation=status,
    )


def _fill(seq, order_id, side, quantity, price, at, *, complete=True):
    return _event(
        seq,
        "BROKER_FILL",
        at,
        {
            "order_id": str(order_id),
            "side": side,
            "quantity": quantity,
            "price": str(price),
            "complete": complete,
            "cumulative_quantity": quantity,
        },
    )


def _bar(seq, at, close, *, symbol=SYMBOL):
    return _event(
        seq,
        "BAR_AVAILABLE",
        at,
        {
            "close": str(close),
            "lean_symbol": "NIFTY" if symbol == "NIFTY" else "NIFTY OPTION",
            "wave_trading_symbol": "NSE:NIFTY 50" if symbol == "NIFTY" else symbol,
        },
    )


def _fee(order_id, side, quantity, price, at, total):
    return {
        "order_id": str(order_id),
        "side": side,
        "quantity": quantity,
        "price": str(price),
        "at": at,
        "components": {
            "brokerage": str(total),
            "exchange": "0",
            "stt": "0",
            "sebi": "0",
            "gst": "0",
            "stamp_duty": "0",
            "dp": "0",
            "ipft": "0",
            "total": str(total),
        },
    }


def _report(*, fees, exits=None, close_event=None, boundary="2026-01-02T15:30:00+05:30"):
    lifecycle = {
        "contract": copy.deepcopy(CONTRACT),
        "first_session": "2026-01-01",
        "last_session": boundary[:10],
        "session_decisions": [],
        "close_event": close_event,
    }
    return {
        "contract_lifecycles": [lifecycle],
        "sessions": [],
        "switches": [],
        "rolls": [],
        "missing_data": [],
        "fee_audit": {"events": fees},
        "forced_close_sensitivity": {
            "exits": exits or [],
            "pnl": "0",
            "ending_equity": "1000000",
            "realized_pnl": "0",
            "unrealized_pnl": "0",
            "total_fees": "0",
        },
        "equity_curve": [
            {
                "at": boundary,
                "cash": "1000000",
                "equity": "1000000",
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "fees": "0",
            }
        ],
        "capital": {
            "model_id": "crude_naked_option_margin_v1",
            "disclaimer": "not Zerodha/NSE SPAN; no cross-contract offsets",
            "curve": [],
            "peaks": {
                "capital_used_excluding_margin": None,
                "capital_used_including_margin": None,
                "capital_recommended": None,
            },
            "profit_on_peak_capital": {
                "classification": "in_sample_linear_annualized",
                "annualization": "simple monthly percentage multiplied by 12",
                "monthly_profit": "0",
                "monthly_profit_pct_excluding_margin": None,
                "monthly_profit_pct_including_margin": None,
                "annualized_profit_pct_excluding_margin": None,
                "annualized_profit_pct_including_margin": None,
            },
        },
    }


def test_flat_to_flat_segments_multiple_episodes_and_reconciles_cash_flow():
    events = [
        _order(1, 1, "BUY", "2026-01-01T09:16:00+05:30"),
        _bar(2, "2026-01-01T09:17:00+05:30", 10),
        _fill(3, 1, "BUY", 10, 10, "2026-01-01T09:17:00+05:30"),
        _order(4, 2, "SELL", "2026-01-01T09:18:00+05:30"),
        _bar(5, "2026-01-01T09:19:00+05:30", 12),
        _fill(6, 2, "SELL", 10, 12, "2026-01-01T09:19:00+05:30"),
        _order(7, 3, "SELL", "2026-01-01T10:00:00+05:30"),
        _fill(8, 3, "SELL", 5, 20, "2026-01-01T10:01:00+05:30"),
        _order(9, 4, "BUY", "2026-01-01T10:02:00+05:30"),
        _fill(10, 4, "BUY", 5, 18, "2026-01-01T10:03:00+05:30"),
    ]
    fees = [
        _fee(1, "BUY", 10, 10, events[2]["at"], 1),
        _fee(2, "SELL", 10, 12, events[5]["at"], 1),
        _fee(3, "SELL", 5, 20, events[7]["at"], 1),
        _fee(4, "BUY", 5, 18, events[9]["at"], 1),
    ]
    model = build_trade_episode_model(_report(fees=fees), events)
    assert [episode["ordinal"] for episode in model["episodes"]] == [1, 2]
    assert [episode["opening_direction"] for episode in model["episodes"]] == [
        "LONG",
        "SHORT",
    ]
    assert [episode["display_net_pnl"] for episode in model["episodes"]] == [
        "18",
        "8",
    ]
    assert len({episode["episode_id"] for episode in model["episodes"]}) == 2


def test_sign_cross_splits_fill_and_allocates_fee_pro_rata():
    events = [
        _order(1, 1, "BUY", "2026-01-01T09:16:00+05:30"),
        _fill(2, 1, "BUY", 10, 10, "2026-01-01T09:17:00+05:30"),
        _order(3, 2, "SELL", "2026-01-01T09:18:00+05:30"),
        _fill(4, 2, "SELL", 15, 12, "2026-01-01T09:19:00+05:30"),
        _order(5, 3, "BUY", "2026-01-01T09:20:00+05:30"),
        _fill(6, 3, "BUY", 5, 11, "2026-01-01T09:21:00+05:30"),
    ]
    fees = [
        _fee(1, "BUY", 10, 10, events[1]["at"], 1),
        _fee(2, "SELL", 15, 12, events[3]["at"], 3),
        _fee(3, "BUY", 5, 11, events[5]["at"], 1),
    ]
    model = build_trade_episode_model(_report(fees=fees), events)
    first, second = model["episodes"]
    assert first["fills"][-1]["derived_split"] is True
    assert first["fills"][-1]["quantity"] == 10
    assert first["fills"][-1]["fee"] == "2.000000000000000000000000000"
    assert second["fills"][0]["quantity"] == 5
    assert second["fills"][0]["fee"] == "1.000000000000000000000000000"
    assert first["display_net_pnl"] == "17.00000000000000000000000000"
    assert second["display_net_pnl"] == "3.000000000000000000000000000"
    assert {ref["trace_seq"] for ref in second["source_event_refs"]} == {4, 6}


def test_partial_fills_overnight_and_lifecycle_fallback_are_preserved():
    close_event = {
        "at": "2026-01-02T09:16:00+05:30",
        "old_contract": copy.deepcopy(CONTRACT),
        "close_price": "12",
        "close_side": "SELL",
        "quantity": 10,
        "fees": "2",
        "close_order_id": "99",
        "reason": "held_premium_outside_35_65",
    }
    events = [
        _order(1, 1, "BUY", "2026-01-01T15:20:00+05:30"),
        _fill(2, 1, "BUY", 4, 10, "2026-01-01T15:21:00+05:30", complete=False),
        _fill(3, 1, "BUY", 6, 10, "2026-01-01T15:22:00+05:30"),
        _event(4, "EXCHANGE_SESSION_CLOSE", "2026-01-01T15:30:00+05:30", {}),
    ]
    fees = [
        _fee(1, "BUY", 4, 10, events[1]["at"], "0.4"),
        _fee(1, "BUY", 6, 10, events[2]["at"], "0.6"),
    ]
    model = build_trade_episode_model(
        _report(fees=fees, close_event=close_event),
        events,
    )
    episode = model["episodes"][0]
    assert episode["status"] == "closed_lifecycle_fallback"
    assert episode["source_fill_count"] == 2
    assert episode["fill_piece_count"] == 3
    assert episode["overnight_count"] == 1
    assert episode["display_net_pnl"] == "17.0"
    assert episode["fills"][-1]["source"] == "derived_lifecycle_fallback"
    assert episode["report_only_order_ids"] == ["99"]


def test_final_open_episode_keeps_boundary_state_and_virtual_close_separate():
    events = [
        _order(1, 1, "SELL", "2026-01-02T14:00:00+05:30"),
        _fill(2, 1, "SELL", 10, 20, "2026-01-02T14:01:00+05:30"),
        _bar(3, "2026-01-02T15:29:00+05:30", 25000, symbol="NIFTY"),
        _bar(4, "2026-01-02T15:29:00+05:30", 16),
    ]
    exit_event = {
        "at": "2026-01-02T15:30:00+05:30",
        "symbol": SYMBOL,
        "side": "BUY",
        "quantity": 10,
        "price": "15",
        "exit_fee": "2",
        "fee_components": {"brokerage": "2", "total": "2"},
    }
    model = build_trade_episode_model(
        _report(
            fees=[_fee(1, "SELL", 10, 20, events[1]["at"], 1)],
            exits=[exit_event],
        ),
        events,
    )
    episode = model["episodes"][0]
    assert episode["status"] == "boundary_open"
    assert episode["ending_quantity"] == -10
    assert episode["boundary_net_pnl"] == "39"
    assert episode["virtual_forced_close"]["observed"] is False
    assert episode["virtual_forced_close"]["net_pnl"] == "47"
    assert episode["display_net_pnl"] == "47"
    payload = build_trade_dashboard_payload(
        _report(
            fees=[_fee(1, "SELL", 10, 20, events[1]["at"], 1)],
            exits=[exit_event],
        ),
        model,
    )
    assert payload["episodes"][0]["inventory_curve"][-1]["value"] == -10


def test_pair_linkage_backfills_peer_and_derives_sibling_cancel():
    at = "2026-01-01T09:17:00+05:30"
    events = [
        _order(1, 1, "SELL", "2026-01-01T09:16:00+05:30"),
        _order(2, 2, "BUY", "2026-01-01T09:16:00+05:30", pair_id="pair-1-2"),
        _fill(3, 2, "BUY", 10, 10, at),
        _order(4, 2, "BUY", at, pair_id="pair-1-2", status="COMPLETE"),
        _order(5, 1, "SELL", at, status="CANCELLED"),
    ]
    model = build_trade_episode_model(
        _report(fees=[_fee(2, "BUY", 10, 10, at, 1)]),
        events,
    )
    orders = {order["order_id"]: order for order in model["orders"]}
    assert orders["1"]["pair_id"] == "pair-1-2"
    assert orders["1"]["pair_linkage"] == "derived_from_peer_transition"
    assert orders["1"]["cancel_cause"] == "sibling_filled"
    assert orders["1"]["cancel_cause_source"] == "derived"
    assert model["pairs"][0]["linkage"] == "observed_and_backfilled"


def test_downsampling_retains_required_markers_and_extrema():
    points = [
        {
            "at": f"2026-01-01T09:{minute:02d}:00+05:30",
            "close": str(minute),
        }
        for minute in range(60)
    ]
    points[20]["close"] = "999"
    points[17]["at"] = "2026-01-01T09:17:00.000000+05:30"
    required = {
        "2026-01-01T09:17:00+05:30",
        points[41]["at"],
    }
    sampled = _downsample_preserving(
        points,
        20,
        required,
        value_key="close",
    )
    assert {
        __import__("datetime").datetime.fromisoformat(value).timestamp()
        for value in required
    } <= {
        __import__("datetime").datetime.fromisoformat(point["at"]).timestamp()
        for point in sampled
    }
    assert max(Decimal(point["close"]) for point in sampled) == Decimal("999")
    assert len(sampled) <= 20


def test_equivalent_iso_timestamps_collapse_to_one_chart_point():
    points = [
        {"at": "2026-01-02T15:30:00+05:30", "quantity": -10},
        {"at": "2026-01-02T15:30:00.000000+05:30", "quantity": 0},
    ]
    assert _last_point_by_timestamp(points) == [points[-1]]


def test_missing_zero_bar_is_not_used_for_mfe_or_mae():
    events = [
        _order(1, 1, "BUY", "2026-01-01T09:16:00+05:30"),
        _fill(2, 1, "BUY", 10, 10, "2026-01-01T09:17:00+05:30"),
        _bar(3, "2026-01-01T09:18:00+05:30", 0, symbol="NIFTY"),
        _bar(4, "2026-01-01T09:18:00+05:30", 100),
        _bar(5, "2026-01-01T09:19:00+05:30", 25000, symbol="NIFTY"),
        _bar(6, "2026-01-01T09:19:00+05:30", 12),
        _order(7, 2, "SELL", "2026-01-01T09:19:00+05:30"),
        _fill(8, 2, "SELL", 10, 12, "2026-01-01T09:20:00+05:30"),
    ]
    model = build_trade_episode_model(
        _report(
            fees=[
                _fee(1, "BUY", 10, 10, events[1]["at"], 1),
                _fee(2, "SELL", 10, 12, events[-1]["at"], 1),
            ]
        ),
        events,
    )
    assert Decimal(model["episodes"][0]["close_mark_mae"]) >= Decimal("-1")


def test_missing_fee_audit_fails_closed():
    events = [
        _order(1, 1, "BUY", "2026-01-01T09:16:00+05:30"),
        _fill(2, 1, "BUY", 10, 10, "2026-01-01T09:17:00+05:30"),
    ]
    with pytest.raises(ValueError, match="Missing fee audit event"):
        build_trade_episode_model(_report(fees=[]), events)


def test_trade_dashboard_is_offline_deterministic_and_contains_controls(tmp_path):
    at = "2026-01-01T09:17:00+05:30"
    events = [
        _bar(1, "2026-01-01T09:16:00+05:30", 25000, symbol="NIFTY"),
        _bar(2, at, 10),
        _order(3, 1, "BUY", "2026-01-01T09:16:00+05:30"),
        _fill(4, 1, "BUY", 10, 10, at),
        _order(5, 2, "SELL", "2026-01-01T09:18:00+05:30"),
        _fill(6, 2, "SELL", 10, 12, "2026-01-01T09:19:00+05:30"),
    ]
    report = _report(
        fees=[
            _fee(1, "BUY", 10, 10, at, 1),
            _fee(2, "SELL", 10, 12, events[-1]["at"], 1),
        ]
    )
    report.update(
        {
            "collection": {
                "collection_id": "test-collection",
                "collection_sha256": "collection-sha",
            },
            "trace_digest": "trace-digest",
            "report_hash": "report-hash",
            "account": {
                "ending_equity": "1000018",
                "pnl": "18",
                "realized_pnl": "20",
                "unrealized_pnl": "0",
                "fees": "2",
                "positions": {},
            },
            "activity": {
                "orders": 2,
                "fills": 2,
                "cancels": 0,
                "fill_events": [],
            },
            "drawdown": {
                "max_drawdown": "0",
                "max_drawdown_pct": "0",
            },
            "inventory": {"max_total_absolute_quantity": 10},
            "no_trade_days": [],
            "missing_data_summary": {"skipped_minutes": 0},
            "final_boundary_mtm": {
                "ending_equity": "1000018",
                "realized_pnl": "20",
                "unrealized_pnl": "0",
                "fees": "2",
            },
        }
    )
    report_path = tmp_path / "report.json"
    trace_path = tmp_path / "trace.jsonl"
    asset_path = tmp_path / "charts.js"
    first = tmp_path / "first.html"
    second = tmp_path / "second.html"
    trace_text = "".join(json.dumps(event) + "\n" for event in events)
    trace_path.write_text(trace_text, encoding="utf-8")
    report["trace_digest"] = hashlib.sha256(
        "".join(canonical_json(event) + "\n" for event in events).encode("utf-8")
    ).hexdigest()
    report_for_hash = copy.deepcopy(report)
    report_for_hash.pop("report_hash", None)
    report["report_hash"] = state_hash(report_for_hash)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    asset_path.write_text("window.LightweightCharts={};", encoding="utf-8")
    kwargs = {
        "report_path": report_path,
        "trace_path": trace_path,
        "chart_asset_path": asset_path,
        "simulator_commit": "test-commit",
        "title": "Trade test",
    }
    one = generate_trade_episode_dashboard(output_path=first, **kwargs)
    two = generate_trade_episode_dashboard(output_path=second, **kwargs)
    rendered = first.read_text(encoding="utf-8")
    assert one["sha256"] == two["sha256"]
    assert hashlib.sha256(first.read_bytes()).hexdigest() == one["sha256"]
    assert first.read_bytes() == second.read_bytes()
    assert "IN-SAMPLE OPTIMIZATION RESULT" in rendered
    assert "history.replaceState" in rendered
    assert "const life=showLifecycle?" in rendered
    assert "derived lifecycle fallback" in rendered
    assert 'id="next"' in rendered
    assert 'id="outcome"' in rendered
    assert "https://" not in rendered
    payload = build_trade_dashboard_payload(
        report,
        build_trade_episode_model(report, events),
    )
    assert payload["counts"]["episodes"] == 1


def test_dashboard_rejects_report_trace_digest_mismatch(tmp_path):
    report = _report(fees=[])
    report["trace_digest"] = "wrong"
    report_for_hash = copy.deepcopy(report)
    report_for_hash.pop("report_hash", None)
    report["report_hash"] = state_hash(report_for_hash)
    report_path = tmp_path / "report.json"
    trace_path = tmp_path / "trace.jsonl"
    asset_path = tmp_path / "charts.js"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    trace_path.write_text("", encoding="utf-8")
    asset_path.write_text("window.LightweightCharts={};", encoding="utf-8")
    with pytest.raises(ValueError, match="Trace digest does not match report"):
        generate_trade_episode_dashboard(
            report_path=report_path,
            trace_path=trace_path,
            output_path=tmp_path / "dashboard.html",
            chart_asset_path=asset_path,
            simulator_commit="test",
        )


def test_dashboard_rejects_report_hash_mismatch(tmp_path):
    report = _report(fees=[])
    report["trace_digest"] = hashlib.sha256(b"").hexdigest()
    report["report_hash"] = "sha256:wrong"
    report_path = tmp_path / "report.json"
    trace_path = tmp_path / "trace.jsonl"
    asset_path = tmp_path / "charts.js"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    trace_path.write_text("", encoding="utf-8")
    asset_path.write_text("window.LightweightCharts={};", encoding="utf-8")
    with pytest.raises(ValueError, match="Report hash does not match content"):
        generate_trade_episode_dashboard(
            report_path=report_path,
            trace_path=trace_path,
            output_path=tmp_path / "dashboard.html",
            chart_asset_path=asset_path,
            simulator_commit="test",
        )
