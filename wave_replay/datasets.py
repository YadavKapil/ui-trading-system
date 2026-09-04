"""Load and validate immutable ``wave-replay-dataset-v1`` exports."""

from __future__ import annotations

import json
import datetime as dt
import dataclasses
from decimal import Decimal
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .dataset_validate import ValidationReport
from .normalize import sha256_file
from .schemas import (
    BAR_ARROW_SCHEMA,
    CONTRACT_ARROW_SCHEMA,
    SESSION_ARROW_SCHEMA,
    Contract,
    DatasetManifest,
    MarketBar,
    SchemaError,
    derive_replay_instrument_token,
)

PARQUET_PARTITIONS = {
    "underlying_bars": "underlying_bars.parquet",
    "option_bars": "option_bars.parquet",
    "contracts": "contracts.parquet",
    "sessions": "sessions.parquet",
}
ARROW_SCHEMAS = {
    "underlying_bars": BAR_ARROW_SCHEMA,
    "option_bars": BAR_ARROW_SCHEMA,
    "contracts": CONTRACT_ARROW_SCHEMA,
    "sessions": SESSION_ARROW_SCHEMA,
}
REQUIRED_FILES = {*PARQUET_PARTITIONS.values(), "fee_schedule.json"}
DECLARED_FILES = REQUIRED_FILES | {"validation_report.json", "manifest.json"}


class ReplayDataset:
    def __init__(
        self,
        *,
        root: Path,
        manifest: DatasetManifest,
        manifest_sha256: str,
        underlying_bars: list[MarketBar],
        option_bars: list[MarketBar],
        contracts: list[Contract],
        sessions: list[dict[str, Any]],
        fee_schedule: dict[str, Any],
        report: ValidationReport,
    ) -> None:
        self.root = root
        self.manifest = manifest
        self.manifest_sha256 = manifest_sha256
        self.underlying_bars = underlying_bars
        self.option_bars = option_bars
        self.contracts = contracts
        self.sessions = sessions
        self.fee_schedule = fee_schedule
        self.validation_report = report
        self.contracts_by_id = {contract.instrument_id: contract for contract in contracts}
        self.contracts_by_wave_symbol: dict[str, list[Contract]] = defaultdict(list)
        for contract in contracts:
            if contract.wave_trading_symbol is not None:
                self.contracts_by_wave_symbol[contract.wave_trading_symbol].append(contract)

    @classmethod
    def load(cls, root: str | Path, *, validate: bool = True) -> "ReplayDataset":
        root = Path(root).resolve()
        manifest_path = root / "manifest.json"
        manifest = DatasetManifest.load(root)
        report = ValidationReport(dataset_id=manifest.dataset_id)
        _validate_inventory(root, manifest, report)

        tables: dict[str, list[dict[str, Any]]] = {}
        for key, filename in PARQUET_PARTITIONS.items():
            path = root / filename
            if path.exists():
                table = pq.read_table(path)
                if table.schema != ARROW_SCHEMAS[key]:
                    report.error(f"Arrow schema mismatch for {filename}")
                tables[key] = table.to_pylist()
            else:
                tables[key] = []
        _validate_raw_tables(tables, manifest, report)

        underlying_bars = [MarketBar.from_mapping(row) for row in tables["underlying_bars"]]
        option_bars = [MarketBar.from_mapping(row) for row in tables["option_bars"]]
        contracts = [Contract.from_mapping(row) for row in tables["contracts"]]
        sessions = tables["sessions"]
        fee_schedule = json.loads((root / "fee_schedule.json").read_text(encoding="utf-8"))

        _validate_rows(underlying_bars, option_bars, contracts, report)
        report.counts.update({
            "underlying_bars": len(underlying_bars),
            "option_bars": len(option_bars),
            "contracts": len(contracts),
            "sessions": len(sessions),
        })
        report.missingness.update({
            "underlying_missing_bars": sum(int(row.get("missing_underlying_bar_count", 0)) for row in sessions),
            "option_missing_bars": sum(contract.missing_bar_count for contract in contracts),
        })
        report.provenance = {
            "manifest_sha256": manifest.manifest_sha256,
            "manifest_file_sha256": sha256_file(manifest_path),
            "schema": manifest.schema,
            "schema_version": manifest.schema_version,
            "producer": manifest.producer,
            "source": manifest.source,
            "identity": manifest.identity,
            "request": manifest.request,
        }
        if validate and not report.valid:
            raise SchemaError("; ".join(report.errors))
        return cls(
            root=root,
            manifest=manifest,
            manifest_sha256=manifest.manifest_sha256,
            underlying_bars=underlying_bars,
            option_bars=option_bars,
            contracts=contracts,
            sessions=sessions,
            fee_schedule=fee_schedule,
            report=report,
        )

    def contract_for_instrument(self, instrument_id: str) -> Contract:
        try:
            return self.contracts_by_id[instrument_id]
        except KeyError as exc:
            raise SchemaError(f"Unknown instrument_id {instrument_id!r}") from exc

    def contract_for_wave_symbol(self, symbol: str) -> Contract:
        candidates = self.contracts_by_wave_symbol.get(symbol, [])
        if len(candidates) != 1:
            raise SchemaError(f"Wave symbol {symbol!r} maps to {len(candidates)} contracts")
        return candidates[0]

    def option_bars_with_missing(self, contract: Contract, trading_date: dt.date) -> list[MarketBar]:
        observed = sorted(
            (
                bar
                for bar in self.option_bars
                if bar.instrument_id == contract.instrument_id and bar.session_date == trading_date
            ),
            key=lambda bar: bar.bar_end,
        )
        if not observed:
            return []
        session = next(
            (
                row
                for row in self.sessions
                if str(row["session_date"]) == trading_date.isoformat()
            ),
            None,
        )
        if session is None:
            raise SchemaError(f"No session metadata for {trading_date}")
        session_open = session["session_open_ist"]
        if not isinstance(session_open, dt.datetime):
            session_open = dt.datetime.fromisoformat(str(session_open))
        expected = {
            session_open + dt.timedelta(minutes=index + 1)
            for index in range(int(session["expected_bar_count"]))
        }
        by_end = {bar.bar_end: bar for bar in observed}
        template = observed[0]
        materialized = list(observed)
        for end in sorted(expected - set(by_end)):
            materialized.append(dataclasses.replace(
                template,
                bar_start=end - dt.timedelta(minutes=1),
                bar_end=end,
                available_at=end,
                open=Decimal("0"),
                high=Decimal("0"),
                low=Decimal("0"),
                close=Decimal("0"),
                volume=0,
                open_interest=None,
                source_row=-1,
                missing_bar=True,
                missing_reason="source_absent",
            ))
        return sorted(materialized, key=lambda bar: bar.bar_end)

    def underlying_bars_with_missing(self, trading_date: dt.date) -> list[MarketBar]:
        observed = sorted(
            (
                bar
                for bar in self.underlying_bars
                if bar.underlying == "NIFTY" and bar.session_date == trading_date
            ),
            key=lambda bar: bar.bar_end,
        )
        if not observed:
            return []
        session = next(
            row for row in self.sessions if str(row["session_date"]) == trading_date.isoformat()
        )
        session_open = session["session_open_ist"]
        if not isinstance(session_open, dt.datetime):
            session_open = dt.datetime.fromisoformat(str(session_open))
        expected = {
            session_open + dt.timedelta(minutes=index + 1)
            for index in range(int(session["expected_bar_count"]))
        }
        by_end = {bar.bar_end: bar for bar in observed}
        template = observed[0]
        materialized = list(observed)
        for end in sorted(expected - set(by_end)):
            materialized.append(dataclasses.replace(
                template,
                bar_start=end - dt.timedelta(minutes=1),
                bar_end=end,
                available_at=end,
                open=Decimal("0"),
                high=Decimal("0"),
                low=Decimal("0"),
                close=Decimal("0"),
                volume=0,
                open_interest=None,
                source_row=-1,
                missing_bar=True,
                missing_reason="source_absent",
            ))
        return sorted(materialized, key=lambda bar: bar.bar_end)


def resolve_missing_quote(policy: str, *, instrument_id: str, timestamp: str) -> dict[str, Any]:
    message = f"Missing completed quote for {instrument_id} at {timestamp}"
    if policy == "fail":
        raise SchemaError(message)
    if policy == "skip_decision":
        return {"action": "skip_decision", "complete": False, "reason": message}
    if policy == "mark_unknown":
        return {"action": "mark_unknown", "complete": False, "reason": message}
    raise SchemaError(f"Unknown missing_quote_policy {policy!r}")


def _validate_inventory(root: Path, manifest: DatasetManifest, report: ValidationReport) -> None:
    declared = set(manifest.declared_files)
    if declared != DECLARED_FILES:
        report.error(f"declared_files must equal {sorted(DECLARED_FILES)}, got {sorted(declared)}")
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file() and not path.name.endswith(".consumer-validation.json")
    }
    undeclared = actual - declared
    missing = declared - actual
    if undeclared:
        report.error(f"Undeclared dataset files: {sorted(undeclared)}")
    if missing:
        report.error(f"Missing declared dataset files: {sorted(missing)}")
    if set(manifest.files) != declared - {"manifest.json"}:
        report.error("Manifest files inventory must contain exactly the six non-manifest outputs")
    required_contract = {
        "resolution_seconds": 60,
        "source_timestamp_kind": "bar_start",
        "available_at": "bar_end",
        "option_forward_fill": False,
        "replay_token_origin": "sha256(instrument_id)[:15]",
        "source_broker_token_available": False,
        "fee_model": "upstox_approximation",
    }
    for key, expected in required_contract.items():
        if manifest.data_contract.get(key) != expected:
            report.error(f"Manifest data_contract {key} must be {expected!r}")
    implementation_commit = manifest.producer.get("implementation_commit")
    data_source_commit = manifest.source.get("data_source_commit")
    if (
        not implementation_commit
        or manifest.producer.get("commit") != implementation_commit
        or manifest.identity.get("implementation_commit") != implementation_commit
    ):
        report.error("Producer implementation_commit provenance is inconsistent")
    if (
        not data_source_commit
        or manifest.source.get("repository_commit") != data_source_commit
        or manifest.identity.get("data_source_commit") != data_source_commit
    ):
        report.error("Source data_source_commit provenance is inconsistent")
    projected_source_files = [
        {
            "path": item["path"],
            "sha256": item["sha256"],
            "members": [
                {"path": member["path"], "sha256": member["sha256"]}
                for member in item.get("members", [])
            ],
        }
        for item in manifest.source_files
    ]
    if manifest.identity.get("source_files") != projected_source_files:
        report.error("Manifest source_files provenance does not match identity")
    expected_dataset_id = "wave-v1-" + __import__("hashlib").sha256(
        (
            __import__("json").dumps(
                {**manifest.identity, "output_files": list(manifest.files.values())},
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    ).hexdigest()[:20]
    if manifest.dataset_id != expected_dataset_id:
        report.error("dataset_id is not content-derived from identity and output files")
    for filename in sorted((declared & actual) - {"manifest.json"}):
        entry = manifest.files.get(filename)
        if not entry:
            report.error(f"No manifest file record for {filename}")
            continue
        actual_hash = sha256_file(root / filename)
        if actual_hash != entry.get("sha256"):
            report.error(f"Hash mismatch for {filename}")
        if (root / filename).stat().st_size != int(entry.get("size_bytes", -1)):
            report.error(f"Size mismatch for {filename}")
    fee_hash = sha256_file(root / "fee_schedule.json") if (root / "fee_schedule.json").exists() else ""
    if fee_hash != manifest.fee_schedule_sha256:
        report.error("fee_schedule_sha256 mismatch")
    if (root / "validation_report.json").exists():
        producer_validation = json.loads((root / "validation_report.json").read_text(encoding="utf-8"))
        if producer_validation.get("valid") is not True:
            report.error("Producer validation_report.json is not valid")


def _validate_rows(
    underlying_bars: list[MarketBar],
    option_bars: list[MarketBar],
    contracts: list[Contract],
    report: ValidationReport,
) -> None:
    if not underlying_bars:
        report.error("Dataset has no underlying bars")
    if not option_bars:
        report.error("Dataset has no option bars")
    if not contracts:
        report.error("Dataset has no contracts")
    keys: set[tuple[str, object]] = set()
    cadence: dict[tuple[str, object], list[MarketBar]] = defaultdict(list)
    for bar in [*underlying_bars, *option_bars]:
        key = (bar.instrument_id, bar.bar_end)
        if key in keys:
            report.error(f"Duplicate bar for {bar.instrument_id} at {bar.bar_end.isoformat()}")
        keys.add(key)
        if bar.resolution_seconds != 60:
            report.error(f"Non-one-minute bar for {bar.instrument_id}")
        if (bar.bar_end - bar.bar_start).total_seconds() != 60:
            report.error(f"Bar duration is not 60 seconds for {bar.instrument_id}")
        if not (bar.bar_start < bar.bar_end <= bar.available_at):
            report.error(f"Invalid time ordering for {bar.instrument_id} at {bar.bar_end.isoformat()}")
        if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
            report.error(f"Invalid OHLC for {bar.instrument_id} at {bar.bar_end.isoformat()}")
        prices = (bar.open, bar.high, bar.low, bar.close)
        if not bar.missing_bar and (
            any(not price.is_finite() or price <= 0 for price in prices)
            or bar.volume < 0
            or (bar.open_interest is not None and bar.open_interest < 0)
        ):
            report.error(f"Non-tradable bar values for {bar.instrument_id} at {bar.bar_end.isoformat()}")
        cadence[(bar.instrument_id, bar.bar_end.date())].append(bar)

    for (instrument_id, day), bars in cadence.items():
        ordered = sorted(bars, key=lambda item: item.bar_end)
        for left, right in zip(ordered, ordered[1:]):
            delta_seconds = (right.bar_end - left.bar_end).total_seconds()
            if delta_seconds <= 0 or delta_seconds % 60:
                report.error(f"Invalid one-minute cadence for {instrument_id} on {day}")

    token_to_id: dict[int, str] = {}
    identities: set[str] = set()
    symbols: set[str] = set()
    for contract in contracts:
        if contract.instrument_id in identities:
            report.error(f"Duplicate contract instrument_id {contract.instrument_id}")
        identities.add(contract.instrument_id)
        if contract.wave_trading_symbol is None:
            report.warnings.append(f"Contract has no safe Wave symbol: {contract.instrument_id}")
        elif contract.wave_trading_symbol in symbols:
            report.error(f"Duplicate wave_trading_symbol {contract.wave_trading_symbol}")
        else:
            symbols.add(contract.wave_trading_symbol)
        expected_token = derive_replay_instrument_token(contract.instrument_id)
        if contract.instrument_token != expected_token:
            report.error(f"Unexpected replay token override for {contract.instrument_id}")
        prior = token_to_id.get(expected_token)
        if prior is not None and prior != contract.instrument_id:
            report.error(f"Replay token collision: {prior} and {contract.instrument_id}")
        token_to_id[expected_token] = contract.instrument_id
        if contract.lot_size <= 0 or contract.resolved_contract_multiplier <= 0:
            report.error(f"Invalid lot size/multiplier for {contract.instrument_id}")
        if contract.right != "call":
            report.warnings.append(f"Non-call contract present but excluded from initial corpus: {contract.instrument_id}")

    contracts_by_id = {contract.instrument_id: contract for contract in contracts}
    option_counts: dict[str, int] = defaultdict(int)
    for bar in option_bars:
        option_counts[bar.instrument_id] += 1
        contract = contracts_by_id.get(bar.instrument_id)
        if contract is None:
            report.error(f"Option bar has no contract metadata: {bar.instrument_id}")
        elif bar.wave_trading_symbol != contract.wave_trading_symbol:
            report.error(f"Option bar symbol mismatch for {bar.instrument_id}")
    for contract in contracts:
        if option_counts[contract.instrument_id] != contract.observed_bar_count:
            report.error(f"Contract observed_bar_count mismatch for {contract.instrument_id}")


def _validate_raw_tables(
    tables: dict[str, list[dict[str, Any]]],
    manifest: DatasetManifest,
    report: ValidationReport,
) -> None:
    common_bar = {
        "instrument_id",
        "source_instrument_id",
        "replay_instrument_token",
        "wave_trading_symbol",
        "lean_symbol",
        "underlying",
        "security_type",
        "market",
        "exchange",
        "session_date",
        "bar_start_utc",
        "bar_end_utc",
        "available_at_utc",
        "bar_start_ist",
        "bar_end_ist",
        "available_at_ist",
        "resolution_seconds",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "source_timestamp_kind",
        "missing_bar",
        "missing_reason",
        "source_archive",
        "source_archive_sha256",
        "source_member",
        "source_member_sha256",
        "source_row",
        "source_universe_sha256",
    }
    contract_required = {
        "instrument_id",
        "source_instrument_id",
        "wave_trading_symbol",
        "lean_symbol",
        "underlying",
        "replay_instrument_token",
        "security_type",
        "market",
        "exchange",
        "expiry",
        "strike",
        "right",
        "option_style",
        "expiry_classification",
        "classification_method",
        "source_interval",
        "first_session_date",
        "last_session_date",
        "first_available_at_utc",
        "last_available_at_utc",
        "source_contract_multiplier",
        "resolved_contract_multiplier",
        "lot_size",
        "multiplier_resolution",
        "observed_bar_count",
        "missing_bar_count",
        "sessions_observed",
        "source_contract_metadata_sha256",
    }
    session_required = {
        "session_date",
        "market",
        "exchange",
        "timezone",
        "session_open_utc",
        "session_close_utc",
        "session_open_ist",
        "session_close_ist",
        "resolution_seconds",
        "observed_bar_count",
        "expected_bar_count",
        "missing_underlying_bar_count",
        "special_session",
        "source_archive",
        "source_archive_sha256",
    }
    requirements = {
        "underlying_bars": common_bar,
        "option_bars": common_bar,
        "contracts": contract_required,
        "sessions": session_required,
    }
    for key, required in requirements.items():
        rows = tables[key]
        if rows:
            missing = required - set(rows[0])
            if missing:
                report.error(f"{key} missing required columns: {sorted(missing)}")
        filename = PARQUET_PARTITIONS[key]
        declared_count = manifest.tables.get(key, {}).get("row_count")
        if declared_count is not None and int(declared_count) != len(rows):
            report.error(f"Manifest row_count mismatch for {filename}")
        declared_columns = manifest.tables.get(key, {}).get("columns")
        if declared_columns is not None and list(declared_columns) != ARROW_SCHEMAS[key].names:
            report.error(f"Manifest table columns mismatch for {filename}")

    sessions = {str(row["session_date"]): row for row in tables["sessions"] if session_required <= set(row)}
    underlying_counts: dict[str, int] = defaultdict(int)
    for row in tables["underlying_bars"]:
        underlying_counts[str(row.get("session_date"))] += 1
    for session_date, session in sessions.items():
        session_open_value = session["session_open_ist"]
        session_close_value = session["session_close_ist"]
        session_open = session_open_value if isinstance(session_open_value, dt.datetime) else dt.datetime.fromisoformat(str(session_open_value))
        session_close = session_close_value if isinstance(session_close_value, dt.datetime) else dt.datetime.fromisoformat(str(session_close_value))
        duration_slots = int((session_close - session_open).total_seconds() / 60)
        if duration_slots != int(session["expected_bar_count"]):
            report.error(f"Session expected_bar_count does not match duration for {session_date}")
        if underlying_counts[session_date] != int(session["observed_bar_count"]):
            report.error(f"Session observed_bar_count mismatch for {session_date}")
        if (
            int(session["observed_bar_count"])
            + int(session["missing_underlying_bar_count"])
            != int(session["expected_bar_count"])
        ):
            report.error(f"Session underlying missingness does not reconcile for {session_date}")
    for table_name in ("underlying_bars", "option_bars"):
        for row in tables[table_name]:
            if not common_bar <= set(row):
                continue
            try:
                start_utc = row["bar_start_utc"] if isinstance(row["bar_start_utc"], dt.datetime) else dt.datetime.fromisoformat(str(row["bar_start_utc"]))
                end_utc = row["bar_end_utc"] if isinstance(row["bar_end_utc"], dt.datetime) else dt.datetime.fromisoformat(str(row["bar_end_utc"]))
                available_utc = row["available_at_utc"] if isinstance(row["available_at_utc"], dt.datetime) else dt.datetime.fromisoformat(str(row["available_at_utc"]))
                start_ist = row["bar_start_ist"] if isinstance(row["bar_start_ist"], dt.datetime) else dt.datetime.fromisoformat(str(row["bar_start_ist"]))
                end_ist = row["bar_end_ist"] if isinstance(row["bar_end_ist"], dt.datetime) else dt.datetime.fromisoformat(str(row["bar_end_ist"]))
                available_ist = row["available_at_ist"] if isinstance(row["available_at_ist"], dt.datetime) else dt.datetime.fromisoformat(str(row["available_at_ist"]))
            except ValueError:
                report.error(f"Invalid timestamp in {table_name}")
                continue
            if (
                start_utc.astimezone(dt.timezone.utc) != start_ist.astimezone(dt.timezone.utc)
                or end_utc.astimezone(dt.timezone.utc) != end_ist.astimezone(dt.timezone.utc)
                or available_utc.astimezone(dt.timezone.utc) != available_ist.astimezone(dt.timezone.utc)
            ):
                report.error(f"UTC/IST timestamp mismatch in {table_name}")
            if row["source_timestamp_kind"] != manifest.data_contract.get("source_timestamp_kind"):
                report.error(f"Timestamp provenance mismatch in {table_name}")
            if str(row["session_date"]) != end_ist.date().isoformat():
                report.error(f"Bar session_date mismatch in {table_name}")
            session = sessions.get(end_ist.date().isoformat())
            if session:
                session_open = session["session_open_ist"] if isinstance(session["session_open_ist"], dt.datetime) else dt.datetime.fromisoformat(str(session["session_open_ist"]))
                session_close = session["session_close_ist"] if isinstance(session["session_close_ist"], dt.datetime) else dt.datetime.fromisoformat(str(session["session_close_ist"]))
                if start_ist < session_open or end_ist > session_close:
                    report.error(f"Bar outside session in {table_name}: {row['instrument_id']}")
            else:
                report.error(f"Bar has no declared session in {table_name}: {row['instrument_id']}")

    contract_rows = {str(row["instrument_id"]): row for row in tables["contracts"] if "instrument_id" in row}
    for row in tables["contracts"]:
        if row.get("source_interval") != "1minute":
            report.error(f"Contract source_interval must be 1minute for {row.get('instrument_id')}")
        first_session = str(row["first_session_date"])
        last_session = str(row["last_session_date"])
        contract_sessions = [
            session
            for day, session in sessions.items()
            if first_session <= day <= last_session
        ]
        if len(contract_sessions) != int(row["sessions_observed"]):
            report.error(f"Contract sessions_observed mismatch for {row.get('instrument_id')}")
        expected_slots = sum(int(session["expected_bar_count"]) for session in contract_sessions)
        if (
            int(row["observed_bar_count"]) + int(row["missing_bar_count"])
            != expected_slots
        ):
            report.error(f"Contract observed/missing counts do not match sessions for {row.get('instrument_id')}")
    for row in tables["option_bars"]:
        contract = contract_rows.get(str(row.get("instrument_id")))
        if contract is None:
            continue
        for field in (
            "source_instrument_id",
            "replay_instrument_token",
            "wave_trading_symbol",
            "lean_symbol",
            "underlying",
            "security_type",
            "market",
            "exchange",
        ):
            if str(row.get(field)) != str(contract.get(field)):
                report.error(f"Option/contract {field} mismatch for {row.get('instrument_id')}")
        archive = next(
            (item for item in manifest.source_files if item.get("path") == row.get("source_archive")),
            None,
        )
        if archive is None or archive.get("sha256") != row.get("source_archive_sha256"):
            report.error(f"Bar source archive provenance mismatch for {row.get('instrument_id')}")
        else:
            members = {item["path"]: item["sha256"] for item in archive.get("members", [])}
            if members.get(row.get("source_member")) != row.get("source_member_sha256"):
                report.error(f"Bar source member provenance mismatch for {row.get('instrument_id')}")
    for row in tables["underlying_bars"]:
        archive = next(
            (item for item in manifest.source_files if item.get("path") == row.get("source_archive")),
            None,
        )
        if archive is None or archive.get("sha256") != row.get("source_archive_sha256"):
            report.error(f"Underlying source archive provenance mismatch for {row.get('instrument_id')}")
        else:
            members = {item["path"]: item["sha256"] for item in archive.get("members", [])}
            if members.get(row.get("source_member")) != row.get("source_member_sha256"):
                report.error(f"Underlying source member provenance mismatch for {row.get('instrument_id')}")
