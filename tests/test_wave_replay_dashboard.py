import json
from pathlib import Path

from wave_replay.dashboard import (
    _build_payload,
    _capital_section,
    _collect_trace_data,
    _downsample,
    generate_continuous_dashboard,
    generate_provisional_trace_dashboard,
)


def _report():
    return {
        "schema": "wave-replay-continuous-report-v1",
        "trace_digest": "trace-digest",
        "report_hash": "report-hash",
        "requested_range": {
            "start": "2024-09-19",
            "end": "2024-09-20",
            "actual_sessions": ["2024-09-19", "2024-09-20"],
        },
        "collection": {
            "collection_id": "wave-collection-v1-test",
            "collection_sha256": "collection-hash",
        },
        "account": {
            "ending_equity": "990000",
            "pnl": "-10000",
            "realized_pnl": "-9000",
            "unrealized_pnl": "-1000",
            "fees": "500",
            "positions": {"NIFTYTESTCE": {"quantity": -25}},
        },
        "activity": {
            "orders": 3,
            "fills": 1,
            "cancels": 2,
            "fill_events": [{
                "at": "2024-09-19T09:20:00+05:30",
                "order_id": "1",
                "side": "SELL",
                "quantity": 25,
                "price": "50",
                "symbol": "NIFTYTESTCE",
            }],
        },
        "drawdown": {"max_drawdown": "-10000", "max_drawdown_pct": "-0.01"},
        "switches": [{
            "at": "2024-09-20T09:16:00+05:30",
            "old_contract": {"wave_trading_symbol": "NIFTYTESTCE"},
        }],
        "rolls": [],
        "no_trade_days": [],
        "missing_data": [{
            "timestamp": "2024-09-19T10:00:00+05:30",
            "session_date": "2024-09-19",
        }],
        "missing_data_summary": {"skipped_minutes": 1},
        "inventory": {"max_total_absolute_quantity": 25},
        "sessions": [{
            "session_date": "2024-09-19",
            "contract": {"wave_trading_symbol": "NIFTYTESTCE"},
            "contract_decision": {"action": "select", "at": "2024-09-19T09:16:00+05:30"},
        }],
        "equity_curve": [
            {
                "at": "2024-09-19T09:16:00+05:30",
                "equity": "1000000",
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "fees": "0",
            },
            {
                "at": "2024-09-20T15:30:00+05:30",
                "equity": "990000",
                "realized_pnl": "-9000",
                "unrealized_pnl": "-1000",
                "fees": "500",
            },
        ],
        "final_boundary_mtm": {
            "ending_equity": "990000",
            "realized_pnl": "-9000",
            "unrealized_pnl": "-1000",
            "fees": "500",
        },
        "forced_close_sensitivity": {
            "ending_equity": "989950",
            "realized_pnl": "-10000",
            "unrealized_pnl": "0",
            "total_fees": "550",
        },
        "summaries": {
            "annual": [{
                "period": "2024",
                "start_equity": "1000000",
                "end_equity": "990000",
                "pnl": "-10000",
                "fees": "500",
            }],
            "monthly": [{
                "period": "2024-09",
                "start_equity": "1000000",
                "end_equity": "990000",
                "pnl": "-10000",
                "fees": "500",
            }],
        },
    }


def test_dashboard_generator_is_self_contained_and_preserves_event_markers(tmp_path):
    report = tmp_path / "report.json"
    trace = tmp_path / "trace.jsonl"
    asset = tmp_path / "charts.js"
    output = tmp_path / "dashboard.html"
    report.write_text(json.dumps(_report()), encoding="utf-8")
    events = [
        {
            "kind": "BAR_AVAILABLE",
            "at": "2024-09-19T09:16:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "close": "25000",
                "lean_symbol": "NIFTY",
                "wave_trading_symbol": "NSE:NIFTY 50",
            },
        },
        {
            "kind": "BAR_AVAILABLE",
            "at": "2024-09-19T09:16:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "close": "50",
                "lean_symbol": "NIFTY OPTION",
                "wave_trading_symbol": "NIFTYTESTCE",
            },
        },
        {
            "kind": "EXCHANGE_SESSION_CLOSE",
            "at": "2024-09-19T15:30:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {},
        },
    ]
    trace.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    asset.write_text("window.LightweightCharts={};", encoding="utf-8")

    result = generate_continuous_dashboard(
        report_path=report,
        trace_path=trace,
        output_path=output,
        chart_asset_path=asset,
        simulator_commit="4d45550",
        title="Test Wave Report",
    )

    rendered = output.read_text(encoding="utf-8")
    assert "window.LightweightCharts={};" in rendered
    assert "https://cdn" not in rendered
    assert "wave-collection-v1-test" in rendered
    assert "Boundary MTM" in rendered
    assert "Forced close" in rendered
    assert "timeZone:'Asia/Kolkata'" in rendered
    assert result["underlying_points"] == 1
    assert result["option_points"] == 4
    assert result["fill_markers"] == 1
    assert result["selection_markers"] == 1
    assert result["lifecycle_markers"] == 1
    assert result["session_boundaries"] == 1
    assert result["missing_markers"] == 1
    payload = _build_payload(
        _report(),
        _collect_trace_data(
            trace,
            max_points=20,
            required_times=set(),
        ),
        max_points=20,
    )
    switch_time = int(
        __import__("datetime").datetime.fromisoformat(
            "2024-09-20T09:16:00+05:30"
        ).timestamp()
    )
    assert {"time": switch_time} in payload["option"]


def test_final_dashboard_label_is_not_provisional(tmp_path):
    report = tmp_path / "report.json"
    trace = tmp_path / "trace.jsonl"
    asset = tmp_path / "charts.js"
    output = tmp_path / "dashboard.html"
    report.write_text(json.dumps(_report()), encoding="utf-8")
    trace.write_text("", encoding="utf-8")
    asset.write_text("window.LightweightCharts={};", encoding="utf-8")
    generate_continuous_dashboard(
        report_path=report,
        trace_path=trace,
        output_path=output,
        chart_asset_path=asset,
        simulator_commit="4d45550",
        title="Wave Full Result",
    )
    rendered = output.read_text(encoding="utf-8")
    assert "DETERMINISTIC FINAL · 1 SESSIONS" in rendered
    assert "final deterministic full-period result" in rendered


def test_downsample_retains_first_last_and_extrema():
    points = [{"time": index, "value": float(index % 11)} for index in range(100)]
    points[20] = {"time": 20, "value": 99.0}
    sampled = _downsample(points, 20)
    assert sampled[0] == points[0]
    assert sampled[-1] == points[-1]
    assert max(item["value"] for item in sampled) == 99
    assert len(sampled) <= 20
    assert len({item["time"] for item in sampled}) == len(sampled)
    duplicates = _downsample(
        [{"time": 1, "value": 1.0}, {"time": 1, "value": 2.0}],
        20,
    )
    assert duplicates == [{"time": 1, "value": 2.0}]


def test_streaming_sampler_keeps_required_marker_timestamp(tmp_path):
    trace = tmp_path / "trace.jsonl"
    events = [
        {
            "kind": "BAR_AVAILABLE",
            "at": f"2024-09-19T09:{minute:02d}:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "close": str(25000 + minute),
                "lean_symbol": "NIFTY",
                "wave_trading_symbol": "NSE:NIFTY 50",
            },
        }
        for minute in range(10, 60)
    ]
    trace.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    required = int(__import__("datetime").datetime.fromisoformat(events[17]["at"]).timestamp())
    result = _collect_trace_data(
        trace,
        max_points=12,
        required_times={required},
    )
    assert required in {point["time"] for point in result["underlying"]}


def test_downsample_rejects_impossible_point_budget():
    import pytest

    with pytest.raises(ValueError, match="at least 4"):
        _downsample([{"time": 1, "value": 1.0}], 3)
    trace = Path(__file__).parent / "fixtures" / "wave_replay" / "goldens" / "nifty_range_startup.jsonl"
    with pytest.raises(ValueError, match="fewer than four slots"):
        _collect_trace_data(trace, max_points=4, required_times={1})


def test_provisional_trace_dashboard_does_not_require_final_report(tmp_path):
    trace = tmp_path / "trace.jsonl"
    manifest = tmp_path / "collection_manifest.json"
    asset = tmp_path / "charts.js"
    output = tmp_path / "provisional.html"
    events = [
        {
            "kind": "BAR_AVAILABLE",
            "at": "2024-09-19T09:16:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "close": "25000",
                "lean_symbol": "NIFTY",
                "wave_trading_symbol": "NSE:NIFTY 50",
            },
        },
        {
            "kind": "TRACE_HEADER",
            "at": "2024-09-19T09:16:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {"contract": {"wave_trading_symbol": "NIFTYTESTCE"}},
        },
        {
            "kind": "BROKER_FILL",
            "at": "2024-09-19T09:17:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "order_id": "1",
                "side": "BUY",
                "quantity": 25,
                "price": "50",
            },
        },
        {
            "kind": "BROKER_ORDER_TRANSITION",
            "at": "2024-09-19T09:17:00+05:30",
            "continuous_session_date": "2024-09-19",
            "details": {
                "order_id": "1",
                "tradingsymbol": "NIFTYTESTCE",
            },
        },
    ]
    trace.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    manifest.write_text(
        json.dumps({
            "collection_id": "wave-collection-v1-full",
            "collection_sha256": "full-hash",
            "start_date": "2024-09-19",
            "end_date": "2026-08-04",
        }),
        encoding="utf-8",
    )
    asset.write_text("window.LightweightCharts={};", encoding="utf-8")
    result = generate_provisional_trace_dashboard(
        trace_path=trace,
        collection_manifest_path=manifest,
        output_path=output,
        chart_asset_path=asset,
        simulator_commit="835339a",
        title="Pass 1",
        max_points=20,
    )
    rendered = output.read_text(encoding="utf-8")
    assert "PASS 2 VERIFICATION PENDING" in rendered
    assert "wave-collection-v1-full" in rendered
    assert "Equity, drawdown" in rendered
    assert result["sessions"] == 1
    assert result["fill_markers"] == 1


def test_dashboard_capital_section_discloses_model_and_annualization():
    report = {
        "capital": {
            "model_id": "crude_naked_option_margin_v1",
            "disclaimer": "not Zerodha/NSE SPAN; no cross-contract offsets",
            "peaks": {
                "capital_used_excluding_margin": {
                    "value": "10000",
                    "at": "2026-01-05T10:00:00+05:30",
                    "projected_short_qty": 0,
                },
                "capital_used_including_margin": {
                    "value": "180000",
                    "at": "2026-01-05T10:00:00+05:30",
                    "projected_short_qty": 65,
                },
                "capital_recommended": {
                    "value": "198000",
                    "at": "2026-01-05T10:00:00+05:30",
                    "projected_short_qty": 65,
                },
            },
            "profit_on_peak_capital": {
                "monthly_profit": "1800",
                "monthly_profit_pct_excluding_margin": "18",
                "monthly_profit_pct_including_margin": "1",
                "annualized_profit_pct_excluding_margin": "216",
                "annualized_profit_pct_including_margin": "12",
            },
        }
    }
    rendered = _capital_section(report)
    assert "crude_naked_option_margin_v1" in rendered
    assert "not Zerodha/NSE SPAN" in rendered
    assert "in-sample_linear_annualized" in rendered
    assert "12.0000%" in rendered
