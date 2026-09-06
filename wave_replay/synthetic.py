"""Compact deterministic ``wave-replay-dataset-v1`` fixture generation."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .modes import DATASET_SCHEMA
from .normalize import canonical_json, sha256_bytes, sha256_file
from .schemas import (
    BAR_ARROW_SCHEMA,
    CONTRACT_ARROW_SCHEMA,
    SESSION_ARROW_SCHEMA,
    IST,
    derive_replay_instrument_token,
)


def _bar(
    *,
    instrument_id: str,
    source_instrument_id: str,
    wave_symbol: str,
    lean_symbol: str,
    underlying: str,
    security_type: str,
    exchange: str,
    start: dt.datetime,
    values: tuple[Decimal, Decimal, Decimal, Decimal],
    volume: int,
    row: int,
    open_interest: int | None = None,
) -> dict[str, Any]:
    end = start + dt.timedelta(minutes=1)
    open_, high, low, close = values
    return {
        "instrument_id": instrument_id,
        "source_instrument_id": source_instrument_id,
        "replay_instrument_token": derive_replay_instrument_token(instrument_id),
        "wave_trading_symbol": wave_symbol,
        "lean_symbol": lean_symbol,
        "underlying": underlying,
        "security_type": security_type,
        "market": "india",
        "exchange": exchange,
        "session_date": start.date(),
        "bar_start_utc": start.astimezone(dt.timezone.utc),
        "bar_end_utc": end.astimezone(dt.timezone.utc),
        "available_at_utc": end.astimezone(dt.timezone.utc),
        "bar_start_ist": start,
        "bar_end_ist": end,
        "available_at_ist": end,
        "resolution_seconds": 60,
        "open": open_.quantize(Decimal("0.0001")),
        "high": high.quantize(Decimal("0.0001")),
        "low": low.quantize(Decimal("0.0001")),
        "close": close.quantize(Decimal("0.0001")),
        "volume": volume,
        "open_interest": open_interest,
        "source_timestamp_kind": "bar_start",
        "missing_bar": False,
        "missing_reason": None,
        "source_archive": f"synthetic/{instrument_id}.zip",
        "source_archive_sha256": sha256_bytes(f"archive:{instrument_id}".encode()),
        "source_member": f"{instrument_id}.csv",
        "source_member_sha256": sha256_bytes(f"member:{instrument_id}".encode()),
        "source_row": row,
        "source_universe_sha256": sha256_bytes(f"universe:{start.date()}".encode()),
    }


def create_synthetic_dataset(
    root: str | Path,
    *,
    corpus: list[dict[str, Any]] | None = None,
    calendar_definition_override: dict[str, Any] | None = None,
    integrity_profile_override: str | None = None,
) -> Path:
    """Create four tiny NIFTY days exercising the approved selector roles."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    dataset_id = "synthetic-nifty-wave-v1"
    corpus = corpus or [
        {
            "role": "trend",
            "date": dt.date(2024, 11, 22),
            "strike": Decimal("23400"),
            "first_close": Decimal("23399.65"),
            "expiry": dt.date(2024, 11, 28),
            "lot_size": 25,
            "missing": 0,
        },
        {
            "role": "missing_data",
            "date": dt.date(2025, 4, 7),
            "strike": Decimal("22050"),
            "first_close": Decimal("22026.60"),
            "expiry": dt.date(2025, 4, 17),
            "lot_size": 75,
            "missing": 12,
        },
        {
            "role": "range",
            "date": dt.date(2025, 7, 7),
            "strike": Decimal("25400"),
            "first_close": Decimal("25424.65"),
            "expiry": dt.date(2025, 7, 17),
            "lot_size": 75,
            "missing": 0,
        },
        {
            "role": "expiry_high_gamma",
            "date": dt.date(2026, 4, 2),
            "strike": Decimal("22200"),
            "first_close": Decimal("22224.20"),
            "expiry": dt.date(2026, 4, 7),
            "lot_size": 65,
            "missing": 0,
        },
        {
            "role": "gap_open",
            "date": dt.date(2026, 4, 8),
            "strike": Decimal("23850"),
            "first_close": Decimal("23844.70"),
            "expiry": dt.date(2026, 4, 13),
            "lot_size": 65,
            "missing": 0,
        },
    ]
    underlying_rows: list[dict[str, Any]] = []
    option_rows: list[dict[str, Any]] = []
    contracts: list[dict[str, Any]] = []
    sessions: list[dict[str, Any]] = []
    total_underlying_missing = 0
    total_option_missing = 0
    partial_underlying_dates: list[str] = []
    missing_underlying_dates: list[str] = []

    for day_index, item in enumerate(corpus):
        trading_date = item["date"]
        strike = item["strike"]
        first_close = item["first_close"]
        expiry = item["expiry"]
        lot_size = item["lot_size"]
        missing_count = item["missing"]
        missing_underlying_rows = set(item.get("missing_underlying_rows", []))
        missing_option_rows = set(item.get("missing_option_rows", [])) | set(range(missing_count))
        total_underlying_missing += len(missing_underlying_rows)
        total_option_missing += len(missing_option_rows)
        session_start = dt.datetime.combine(
            trading_date,
            item.get("session_open", dt.time(9, 15)),
            tzinfo=IST,
        )
        session_close = dt.datetime.combine(
            trading_date,
            item.get("session_close", dt.time(15, 30)),
            tzinfo=IST,
        )
        bar_count = int((session_close - session_start).total_seconds() // 60)
        if len(missing_underlying_rows) == bar_count:
            missing_underlying_dates.append(trading_date.isoformat())
        elif missing_underlying_rows:
            partial_underlying_dates.append(trading_date.isoformat())
        sessions.append({
            "session_date": trading_date,
            "market": "india",
            "exchange": "NSE",
            "timezone": "Asia/Kolkata",
            "session_open_utc": session_start.astimezone(dt.timezone.utc),
            "session_close_utc": session_close.astimezone(dt.timezone.utc),
            "session_open_ist": session_start,
            "session_close_ist": session_close,
            "resolution_seconds": 60,
            "observed_bar_count": bar_count - len(missing_underlying_rows),
            "expected_bar_count": bar_count,
            "missing_underlying_bar_count": len(missing_underlying_rows),
            "special_session": bool(item.get("special_session", False)),
            "source_archive": "synthetic/nifty-index.zip",
            "source_archive_sha256": sha256_bytes(b"archive:nifty-index"),
        })
        previous = first_close
        for row_index in range(bar_count):
            if row_index == 0:
                close = first_close
            elif item["role"] == "trend":
                close = strike + Decimal(row_index) * Decimal("0.8")
            elif item["role"] == "range":
                close = strike + Decimal((row_index % 12) - 6) * Decimal("2")
            elif item["role"] == "gap_open":
                close = strike + Decimal(row_index % 5)
            elif item["role"] == "expiry_high_gamma":
                close = strike + Decimal((row_index % 8) - 4) * Decimal("3")
            else:
                close = strike + Decimal((row_index % 6) - 3)
            high = max(previous, close) + Decimal("1")
            low = min(previous, close) - Decimal("1")
            if row_index not in missing_underlying_rows:
                underlying_rows.append(_bar(
                    instrument_id="nifty-index",
                    source_instrument_id="NSE_INDEX|Nifty 50",
                    wave_symbol="NSE:NIFTY 50",
                    lean_symbol="NIFTY",
                    underlying="NIFTY",
                    security_type="index",
                    exchange="NSE",
                    start=session_start + dt.timedelta(minutes=row_index),
                    values=(previous, high, low, close),
                    volume=0,
                    row=day_index * 10 + row_index,
                ))
            previous = close

        instrument_id = f"NIFTY|{expiry.isoformat()}|C|{int(strike)}"
        source_id = f"NSE_FO|{day_index + 1000}"
        monthly = trading_date == dt.date(2024, 11, 22)
        if monthly:
            symbol = f"NIFTY{expiry:%y}{expiry:%b}{int(strike)}CE".upper()
            expiry_classification = "monthly"
        else:
            month_code = str(expiry.month) if expiry.month <= 9 else {10: "O", 11: "N", 12: "D"}[expiry.month]
            symbol = f"NIFTY{expiry:%y}{month_code}{expiry:%d}{int(strike)}CE"
            expiry_classification = "weekly"
        lean_symbol = f"NIFTY {expiry:%y%m%d}C{int(strike * 1000):08d}"
        metadata_sha = sha256_bytes(f"{instrument_id}:metadata".encode())
        token = derive_replay_instrument_token(instrument_id)
        available_option_rows = [
            index for index in range(bar_count) if index not in missing_option_rows
        ]
        contracts.append({
            "instrument_id": instrument_id,
            "source_instrument_id": source_id,
            "wave_trading_symbol": symbol,
            "lean_symbol": lean_symbol,
            "underlying": "NIFTY",
            "replay_instrument_token": token,
            "security_type": "index_option",
            "market": "india",
            "exchange": "NFO",
            "expiry": expiry,
            "strike": strike.quantize(Decimal("0.0001")),
            "right": "call",
            "option_style": "european",
            "expiry_classification": expiry_classification,
            "classification_method": "latest_expiry_in_complete_source_month",
            "source_interval": "1minute",
            "first_session_date": trading_date,
            "last_session_date": trading_date,
            "first_available_at_utc": (
                session_start + dt.timedelta(minutes=min(available_option_rows) + 1)
            ).astimezone(dt.timezone.utc),
            "last_available_at_utc": (
                session_start + dt.timedelta(minutes=max(available_option_rows) + 1)
            ).astimezone(dt.timezone.utc),
            "source_contract_multiplier": lot_size,
            "resolved_contract_multiplier": 65,
            "lot_size": lot_size,
            "multiplier_resolution": "source_preferred",
            "observed_bar_count": bar_count - len(missing_option_rows),
            "missing_bar_count": len(missing_option_rows),
            "sessions_observed": 1,
            "source_contract_metadata_sha256": metadata_sha,
        })
        option_base = Decimal(str(item.get("option_base", "100")))
        previous_option = option_base
        for row_index in range(bar_count):
            close = option_base + Decimal((row_index % 10) - 5) * Decimal("3")
            if row_index in missing_option_rows:
                previous_option = close
                continue
            row = _bar(
                instrument_id=instrument_id,
                source_instrument_id=source_id,
                wave_symbol=symbol,
                lean_symbol=lean_symbol,
                underlying="NIFTY",
                security_type="index_option",
                exchange="NFO",
                start=session_start + dt.timedelta(minutes=row_index),
                values=(previous_option, max(previous_option, close) + 1, min(previous_option, close) - 1, close),
                volume=1000,
                open_interest=5000,
                row=day_index * 10 + row_index,
            )
            option_rows.append(row)
            previous_option = close

    table_values = {
        "underlying_bars.parquet": (underlying_rows, BAR_ARROW_SCHEMA),
        "option_bars.parquet": (option_rows, BAR_ARROW_SCHEMA),
        "contracts.parquet": (contracts, CONTRACT_ARROW_SCHEMA),
        "sessions.parquet": (sessions, SESSION_ARROW_SCHEMA),
    }
    for filename, (rows, schema) in table_values.items():
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), root / filename)

    fee_schedule = {
        "schema": "wave-replay-fee-schedule-v1",
        "usage_label": "upstox_approximation",
        "broker_fee_model": "upstox_approximation",
        "broker": "upstox",
        "broker_fidelity": "not_zerodha",
        "approximation_only": True,
        "resolved_schedule_id": "latest-2026-04",
        "date_effective_for_historical_sessions": False,
        "historical_approximation_reason": "Approved uniform latest schedule for replay sensitivity.",
        "decision_effect": "economic_reporting_only",
        "configuration": {"broker": "upstox", "equity_product": "delivery", "schedule": "latest-2026-04"},
        "resolved_schedule": {
            "broker": {
                "identifier": "upstox-standard-2026",
                "option_brokerage": "20",
            },
            "statutory": {
                "identifier": "india-latest-2026-04",
                "option_exchange_rate": "0.00053",
                "sebi_rate": "0.000001",
                "option_stt_rate": "0.0015",
                "option_stamp_rate": "0.00003",
                "gst_rate": "0.18",
            },
            "rounding": "ROUND_HALF_UP for aggregate fixture components",
        },
    }
    fee_path = root / "fee_schedule.json"
    fee_path.write_text(json.dumps(fee_schedule, indent=2, sort_keys=True), encoding="utf-8")

    corpus_dates = {item["date"] for item in corpus}
    synthetic_holidays = []
    cursor = corpus[0]["date"]
    while cursor <= corpus[-1]["date"]:
        if cursor.weekday() < 5 and cursor not in corpus_dates:
            synthetic_holidays.append(cursor.isoformat())
        cursor += dt.timedelta(days=1)
    calendar_definition = calendar_definition_override or {
        "timezone": "Asia/Kolkata",
        "exchange_time_zone": "Asia/Kolkata",
        "market_segments": {
            day: ([{"start": "09:15:00", "end": "15:30:00"}] if index < 5 else [])
            for index, day in enumerate(
                ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
            )
        },
        "early_closes": {},
        "late_opens": {},
        "holidays": synthetic_holidays,
    }
    calendar_definition = json.loads(json.dumps(calendar_definition))
    if integrity_profile_override == "wave-replay-special-sessions-v3":
        calendar_definition.setdefault("special_sessions", {})
    calendar_definition_hash = sha256_bytes(
        (canonical_json(calendar_definition) + "\n").encode()
    )
    integrity_profile = (
        integrity_profile_override
        or "wave-replay-authoritative-sessions-v2"
    )
    expected_dates = [item["date"].isoformat() for item in corpus]
    underlying_coverage = {
        "profile": "lean-market-hours-requested-range-v1",
        "status": "authoritative",
        "authority": "lean_market_hours_database",
        "source_path": "synthetic/market-hours.json",
        "source_sha256": sha256_bytes(b"synthetic-market-hours"),
        "entry_key": "Index-india-[*]",
        "definition": calendar_definition,
        "definition_sha256": calendar_definition_hash,
        "expected_session_dates": expected_dates,
        "missing_underlying_session_dates": missing_underlying_dates,
        "partial_underlying_session_dates": partial_underlying_dates,
        "observed_special_session_dates": [],
        "archive_only_special_session_dates": [],
        "coverage_complete": not partial_underlying_dates and not missing_underlying_dates,
        "equivalence_eligible": not partial_underlying_dates and not missing_underlying_dates,
        "missing_session_policy": "explicit_session_row_without_fabricated_prices",
        "calendar_scope": "weekly_market_hours_holidays_early_closes_late_opens",
        "reason": None,
    }
    validation = {
        "valid": True,
        "schema": DATASET_SCHEMA,
        "schema_version": "1.1.0",
        "resolution_seconds": 60,
        "counts": {
            "underlying_bars": len(underlying_rows),
            "option_bars": len(option_rows),
            "contracts": len(contracts),
            "sessions": len(sessions),
            "missing_underlying_bars": total_underlying_missing,
            "missing_option_bars": total_option_missing,
            "source_resolved_multiplier_mismatches": 3,
            "option_session_outages": 0,
            "contract_session_outages": 0,
            "missing_underlying_sessions": len(missing_underlying_dates),
            "partial_underlying_sessions": len(partial_underlying_dates),
        },
        "option_session_outage_dates": [],
        "underlying_session_outage_dates": missing_underlying_dates,
        "underlying_session_coverage": underlying_coverage,
        "contract_session_outages": [],
        "policies": {"option_forward_fill": "forbidden"},
        "warnings": [],
    }
    validation_path = root / "validation_report.json"
    validation_path.write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")

    files = []
    for filename in [*table_values, "fee_schedule.json", "validation_report.json"]:
        files.append({
            "path": filename,
            "size_bytes": (root / filename).stat().st_size,
            "sha256": sha256_file(root / filename),
        })
    table_manifest = {
        "underlying_bars": {"path": "underlying_bars.parquet", "row_count": len(underlying_rows), "columns": BAR_ARROW_SCHEMA.names},
        "option_bars": {"path": "option_bars.parquet", "row_count": len(option_rows), "columns": BAR_ARROW_SCHEMA.names},
        "contracts": {"path": "contracts.parquet", "row_count": len(contracts), "columns": CONTRACT_ARROW_SCHEMA.names},
        "sessions": {"path": "sessions.parquet", "row_count": len(sessions), "columns": SESSION_ARROW_SCHEMA.names},
    }
    instrument_ids = ["nifty-index", *(row["instrument_id"] for row in contracts)]
    source_files = [
        {
            "path": f"synthetic/{instrument_id}.zip",
            "sha256": sha256_bytes(f"archive:{instrument_id}".encode()),
            "members": [{
                "path": f"{instrument_id}.csv",
                "sha256": sha256_bytes(f"member:{instrument_id}".encode()),
            }],
        }
        for instrument_id in instrument_ids
    ]
    identity = {
        "schema": DATASET_SCHEMA,
        "schema_version": "1.1.0",
        "underlying": "NIFTY",
        "market": "india",
        "start_date": corpus[0]["date"].isoformat(),
        "end_date": corpus[-1]["date"].isoformat(),
        "resolution_seconds": 60,
        "implementation_commit": "7dab0e1",
        "integrity_profile": integrity_profile,
        "session_calendar_profile": "lean-market-hours-requested-range-v1",
        "session_calendar_definition_sha256": calendar_definition_hash,
        "config_source_commit": "d667bcb",
        "lean_data_source_commit": "d667bcb",
        "raw_cache_source_commit": "d667bcb",
        "source_files": source_files,
    }
    dataset_identity = {**identity, "output_files": files}
    dataset_id = f"wave-v1-{sha256_bytes((canonical_json(dataset_identity) + chr(10)).encode())[:20]}"
    manifest = {
        "schema": DATASET_SCHEMA,
        "schema_version": "1.1.0",
        "dataset_id": dataset_id,
        "producer": {
            "repository": "synthetic-fixture",
            "commit": "7dab0e1",
            "implementation_commit": "7dab0e1",
            "command": "wave_replay create-synthetic",
            "pyarrow_version": pa.__version__,
        },
        "source": {
            "config": {
                "kind": "data_config",
                "present": True,
                "repository_commit": "d667bcb",
                "provenance": "synthetic",
            },
            "lean_data": {
                "kind": "lean_data",
                "present": True,
                "repository_commit": "d667bcb",
                "provenance": "synthetic",
            },
            "raw_cache": {
                "kind": "raw_cache",
                "present": True,
                "repository_commit": "d667bcb",
                "provenance": "synthetic",
            },
        },
        "identity": identity,
        "request": {
            "data_config": "synthetic",
            "data_config_sha256": sha256_bytes(b"synthetic-config"),
            "data_root": "synthetic",
            "raw_root": None,
            "underlying": "NIFTY",
            "start_date": corpus[0]["date"].isoformat(),
            "end_date": corpus[-1]["date"].isoformat(),
        },
        "data_contract": {
            "integrity_profile": integrity_profile,
            "session_calendar_profile": "lean-market-hours-requested-range-v1",
            "resolution_seconds": 60,
            "source_timestamp_kind": "bar_start",
            "available_at": "bar_end",
            "option_forward_fill": False,
            "replay_token_origin": "sha256(instrument_id)[:15]",
            "source_broker_token_available": False,
            "replay_lot_size_policy": "source_contract_multiplier_else_resolved",
            "fee_model": "upstox_approximation",
            "broker_fee_model": "upstox_approximation",
            "resolved_fee_schedule_id": "latest-2026-04",
            "fee_date_effective_for_historical_sessions": False,
            "historical_fee_approximation_reason": "Approved uniform latest schedule for replay sensitivity.",
            "option_session_outage_dates": [],
            "contract_session_outages": {
                "count": 0,
                "rows": [],
                "eligibility": "first_valid_member_declaration_or_observed_session_through_expiry_or_export_end",
                "policy": "count_missing_bars_without_fabricating_prices",
            },
            "underlying_session_coverage": underlying_coverage,
        },
        "tables": table_manifest,
        "source_files": source_files,
        "files": files,
        "declared_files": sorted([entry["path"] for entry in files] + ["manifest.json"]),
        "test_metadata": {
            "underlying": "NIFTY",
            "resolution_seconds": 60,
            "start_date": corpus[0]["date"].isoformat(),
            "end_date": corpus[-1]["date"].isoformat(),
            "underlying_bar_count": len(underlying_rows),
            "option_bar_count": len(option_rows),
            "contract_count": len(contracts),
            "approved_corpus": {
                "range": "2025-07-07",
                "trend": "2024-11-22",
                "gap_open": "2026-04-08",
                "expiry_high_gamma": "2026-04-02",
            },
            "missing_data_case": {
                "date": "2025-04-07",
                "missing_timestamps": 12,
                "first_available_start": "09:27:00",
                "available_bars": 363,
            },
        },
    }
    manifest_hash = sha256_bytes((canonical_json(manifest) + "\n").encode("utf-8"))
    manifest["manifest_sha256"] = manifest_hash
    (root / "manifest.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    return root
