import datetime as dt
import hashlib
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

import wave_replay.order_chart as order_chart
from wave_replay.cli import build_parser
from wave_replay.normalize import sha256_file
from wave_replay.order_chart import (
    _epoch,
    build_order_chart_payload,
    build_order_step_segment,
    generate_order_chart,
    load_pinned_episode_ohlc,
)


IST = ZoneInfo("Asia/Kolkata")
UTC = dt.timezone.utc
SYMBOL = "NIFTYTESTCE"
INSTRUMENT = "NIFTY|2026-01-08|C|25000"


def _episode(*, order_ids=None):
    return {
        "episode_id": "episode-default",
        "ordinal": 1,
        "symbol": SYMBOL,
        "contract": {
            "wave_trading_symbol": SYMBOL,
            "instrument_id": INSTRUMENT,
            "expiry": "2026-01-08",
            "strike": "25000",
        },
        "status": "closed_flat",
        "start_at": "2026-01-05T09:17:00+05:30",
        "end_at": "2026-01-05T09:20:00+05:30",
        "display_net_pnl": "18",
        "order_ids": order_ids or [],
        "fills": [
            {
                "at": "2026-01-05T09:17:00+05:30",
                "order_id": "1",
                "side": "BUY",
                "quantity": 10,
                "price": "10",
                "source": "observed",
            }
        ],
        "virtual_forced_close": None,
    }


def _order(order_id, side, *, pair_id=None, start_minute=16, end_minute=20):
    return {
        "order_id": str(order_id),
        "symbol": SYMBOL,
        "side": side,
        "tag": f"{side[0]}_{order_id}",
        "pair_id": pair_id,
        "pair_linkage": "observed" if pair_id else "unpaired",
        "terminal_status": "CANCELLED",
        "terminal_at": f"2026-01-05T09:{end_minute:02d}:00+05:30",
        "cancel_cause": "unknown",
        "cancel_cause_source": "unavailable",
        "transitions": [
            {
                "at": f"2026-01-05T09:{start_minute:02d}:00+05:30",
                "status": "OPEN",
                "price": "10",
                "rejection_reason": None,
            },
            {
                "at": "2026-01-05T09:18:00+05:30",
                "status": "OPEN",
                "price": "11",
                "rejection_reason": None,
            },
            {
                "at": f"2026-01-05T09:{end_minute:02d}:00+05:30",
                "status": "CANCELLED",
                "price": "11",
                "rejection_reason": None,
            },
        ],
        "limit_points": [
            {
                "at": f"2026-01-05T09:{start_minute:02d}:00+05:30",
                "price": "10",
                "status": "OPEN",
            },
            {
                "at": "2026-01-05T09:18:00+05:30",
                "price": "11",
                "status": "OPEN",
            },
            {
                "at": f"2026-01-05T09:{end_minute:02d}:00+05:30",
                "price": "11",
                "status": "CANCELLED",
            },
        ],
    }


def _model(*, orders=None, candles_close=True):
    episode = _episode(order_ids=[order["order_id"] for order in (orders or [])])
    return {
        "episodes": [episode],
        "orders": orders or [],
        "market": {
            "option_by_symbol": {
                SYMBOL: (
                    [
                        {
                            "at": "2026-01-05T09:17:00+05:30",
                            "close": "10",
                        },
                        {
                            "at": "2026-01-05T09:18:00+05:30",
                            "close": "11",
                        },
                    ]
                    if candles_close
                    else []
                )
            }
        },
    }


def _report():
    return {
        "collection": {
            "collection_id": "collection-id",
            "collection_sha256": "collection-sha",
        },
        "requested_range": {
            "start": "2026-01-05",
            "end": "2026-01-05",
        },
        "trace_digest": "trace-digest",
        "report_hash": "report-hash",
    }


def test_pinned_parquet_selects_exact_contract_ohlc_and_ist(monkeypatch, tmp_path):
    partition = tmp_path / "2026-01-05"
    partition.mkdir()
    option_path = partition / "option_bars.parquet"
    rows = [
        {
            "instrument_id": INSTRUMENT,
            "bar_end_utc": dt.datetime(2026, 1, 5, 3, 47, tzinfo=UTC),
            "bar_end_ist": dt.datetime(2026, 1, 5, 9, 17, tzinfo=IST),
            "resolution_seconds": 60,
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "missing_bar": False,
            "source_archive_sha256": "archive",
            "source_member_sha256": "member",
            "source_row": 7,
        },
        {
            "instrument_id": "OTHER",
            "bar_end_utc": dt.datetime(2026, 1, 5, 3, 47, tzinfo=UTC),
            "bar_end_ist": dt.datetime(2026, 1, 5, 9, 17, tzinfo=IST),
            "resolution_seconds": 60,
            "open": 99.0,
            "high": 99.0,
            "low": 99.0,
            "close": 99.0,
            "missing_bar": False,
            "source_archive_sha256": "archive",
            "source_member_sha256": "member",
            "source_row": 8,
        },
    ]
    pq.write_table(pa.Table.from_pylist(rows), option_path)
    record = {
        "size_bytes": option_path.stat().st_size,
        "sha256": sha256_file(option_path),
    }
    fake_collection = SimpleNamespace(
        partitions=[
            SimpleNamespace(
                session_date=dt.date(2026, 1, 5),
                path=partition,
                dataset_id="dataset-id",
                manifest_sha256="manifest-sha",
            )
        ],
        to_manifest=lambda: {
            "collection_id": "collection-id",
            "collection_sha256": "collection-sha",
        },
    )
    monkeypatch.setattr(
        order_chart.ReplayDatasetCollection,
        "from_inputs",
        lambda _inputs: fake_collection,
    )
    monkeypatch.setattr(
        order_chart.DatasetManifest,
        "load",
        lambda _path: SimpleNamespace(files={"option_bars.parquet": record}),
    )
    result = load_pinned_episode_ohlc(
        _report(),
        _model(),
        tmp_path / "collection_manifest.json",
    )
    candles = result["candles"]["episode-default"]
    assert candles == [
        {
            "time": int(rows[0]["bar_end_utc"].timestamp()),
            "at": "2026-01-05T09:17:00+05:30",
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "source_archive_sha256": "archive",
            "source_member_sha256": "member",
            "source_row": 7,
        }
    ]
    assert result["pins"]["episode-default"][0]["option_bars_sha256"] == record["sha256"]


def test_order_segment_steps_reprice_and_end_without_stale_extension():
    segment = build_order_step_segment(_order(1, "BUY"))
    assert [point["value"] for point in segment["points"]] == [10.0, 11.0, 11.0]
    assert segment["starts_at"] == "2026-01-05T09:16:00+05:30"
    assert segment["ends_at"] == "2026-01-05T09:20:00+05:30"
    assert segment["terminal_markers"] == [
        {
            "time": _epoch("2026-01-05T09:20:00+05:30"),
            "at": "2026-01-05T09:20:00+05:30",
            "order_id": "1",
            "side": "BUY",
            "status": "CANCELLED",
            "reason": "unknown",
            "reason_source": "unavailable",
        }
    ]
    assert max(point["time"] for point in segment["points"]) == _epoch(
        segment["ends_at"]
    )


def test_overlapping_orders_render_as_individual_labelled_lines():
    orders = [
        _order(1, "BUY", pair_id="pair-1-2", end_minute=25),
        _order(2, "SELL", pair_id="pair-1-2", start_minute=17),
    ]
    payload = build_order_chart_payload(
        _report(),
        _model(orders=orders),
        {
            "collection": {
                "collection_id": "collection-id",
                "collection_sha256": "collection-sha",
            },
            "candles": {"episode-default": []},
            "pins": {},
        },
        default_episode_id="missing",
    )
    episode = payload["episodes"][0]
    assert payload["default_episode_id"] == "episode-default"
    assert episode["series_kind"] == "line"
    assert {line["order_id"] for line in episode["order_lines"]} == {"1", "2"}
    assert episode["fills"][0]["time"] == _epoch(
        "2026-01-05T09:17:00+05:30"
    )
    assert episode["display_end_at"] == "2026-01-05T09:25:00+05:30"


def test_generator_is_offline_deterministic_and_labels_contract(monkeypatch, tmp_path):
    orders = [_order(1, "BUY")]
    report = _report()
    model = _model(orders=orders)
    ohlc = {
        "collection": {
            "collection_id": "collection-id",
            "collection_sha256": "collection-sha",
        },
        "candles": {
            "episode-default": [
                {
                    "time": _epoch("2026-01-05T09:17:00+05:30"),
                    "at": "2026-01-05T09:17:00+05:30",
                    "open": 10,
                    "high": 12,
                    "low": 9,
                    "close": 11,
                }
            ]
        },
        "pins": {"episode-default": []},
    }
    monkeypatch.setattr(
        order_chart,
        "load_trade_episode_model",
        lambda _report, _trace: (report, model),
    )
    monkeypatch.setattr(
        order_chart,
        "load_pinned_episode_ohlc",
        lambda _report, _model, _collection: ohlc,
    )
    asset = tmp_path / "charts.js"
    asset.write_text("window.LightweightCharts={};", encoding="utf-8")
    first = tmp_path / "first.html"
    second = tmp_path / "second.html"
    kwargs = {
        "report_path": tmp_path / "report.json",
        "trace_path": tmp_path / "trace.jsonl",
        "collection_manifest_path": tmp_path / "collection.json",
        "chart_asset_path": asset,
        "simulator_commit": "commit-sha",
        "title": "Order test",
    }
    one = generate_order_chart(output_path=first, **kwargs)
    two = generate_order_chart(output_path=second, **kwargs)
    rendered = first.read_text(encoding="utf-8")
    assert first.read_bytes() == second.read_bytes()
    assert one["sha256"] == two["sha256"] == hashlib.sha256(
        first.read_bytes()
    ).hexdigest()
    assert "NIFTYTESTCE" in rendered
    assert "Pinned 1-minute OHLC candles" in rendered
    assert "IST_OFFSET_SECONDS=19800" in rendered
    assert "getUTC" in rendered
    assert "limits.map(esc)" in rendered
    assert "https://" not in rendered
    assert one["default_candles"] == 1
    assert one["default_order_lines"] == 1
    assert one["default_fills"] == 1


def test_render_order_chart_cli_is_registered():
    args = build_parser().parse_args(
        [
            "render-order-chart",
            "--report",
            "report.json",
            "--trace",
            "trace.jsonl",
            "--collection-manifest",
            "collection.json",
            "--output",
            "orders.html",
            "--simulator-commit",
            "commit",
        ]
    )
    assert args.command == "render-order-chart"
    assert args.default_episode == "episode-2b11ea0b7d-001"
