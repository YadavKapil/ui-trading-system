"""Typed input contracts for Wave replay datasets and scenarios."""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import yaml
import pyarrow as pa

from .modes import APPROVED_DEFAULTS, DATASET_SCHEMA

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


class SchemaError(ValueError):
    """Raised when a portable dataset or scenario violates the frozen contract."""


def derive_replay_instrument_token(instrument_id: str) -> int:
    """Derive a stable positive simulator token; this is never a Zerodha token."""
    return int(hashlib.sha256(instrument_id.encode("utf-8")).hexdigest()[:15], 16) or 1


PRICE_TYPE = pa.decimal128(18, 4)
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")
IST_TIMESTAMP = pa.timestamp("us", tz="Asia/Kolkata")
BAR_ARROW_SCHEMA = pa.schema([
    ("instrument_id", pa.string()),
    ("source_instrument_id", pa.string()),
    ("replay_instrument_token", pa.int64()),
    ("wave_trading_symbol", pa.string()),
    ("lean_symbol", pa.string()),
    ("underlying", pa.string()),
    ("security_type", pa.string()),
    ("market", pa.string()),
    ("exchange", pa.string()),
    ("session_date", pa.date32()),
    ("bar_start_utc", UTC_TIMESTAMP),
    ("bar_end_utc", UTC_TIMESTAMP),
    ("available_at_utc", UTC_TIMESTAMP),
    ("bar_start_ist", IST_TIMESTAMP),
    ("bar_end_ist", IST_TIMESTAMP),
    ("available_at_ist", IST_TIMESTAMP),
    ("resolution_seconds", pa.int32()),
    ("open", PRICE_TYPE),
    ("high", PRICE_TYPE),
    ("low", PRICE_TYPE),
    ("close", PRICE_TYPE),
    ("volume", pa.int64()),
    ("open_interest", pa.int64()),
    ("source_timestamp_kind", pa.string()),
    ("missing_bar", pa.bool_()),
    ("missing_reason", pa.string()),
    ("source_archive", pa.string()),
    ("source_archive_sha256", pa.string()),
    ("source_member", pa.string()),
    ("source_member_sha256", pa.string()),
    ("source_row", pa.int64()),
    ("source_universe_sha256", pa.string()),
])
CONTRACT_ARROW_SCHEMA = pa.schema([
    ("instrument_id", pa.string()),
    ("source_instrument_id", pa.string()),
    ("replay_instrument_token", pa.int64()),
    ("wave_trading_symbol", pa.string()),
    ("lean_symbol", pa.string()),
    ("underlying", pa.string()),
    ("security_type", pa.string()),
    ("market", pa.string()),
    ("exchange", pa.string()),
    ("expiry", pa.date32()),
    ("strike", PRICE_TYPE),
    ("right", pa.string()),
    ("option_style", pa.string()),
    ("expiry_classification", pa.string()),
    ("classification_method", pa.string()),
    ("source_interval", pa.string()),
    ("first_session_date", pa.date32()),
    ("last_session_date", pa.date32()),
    ("first_available_at_utc", UTC_TIMESTAMP),
    ("last_available_at_utc", UTC_TIMESTAMP),
    ("source_contract_multiplier", pa.int32()),
    ("resolved_contract_multiplier", pa.int32()),
    ("lot_size", pa.int32()),
    ("multiplier_resolution", pa.string()),
    ("observed_bar_count", pa.int64()),
    ("missing_bar_count", pa.int64()),
    ("sessions_observed", pa.int32()),
    ("source_contract_metadata_sha256", pa.string()),
])
SESSION_ARROW_SCHEMA = pa.schema([
    ("session_date", pa.date32()),
    ("market", pa.string()),
    ("exchange", pa.string()),
    ("timezone", pa.string()),
    ("session_open_utc", UTC_TIMESTAMP),
    ("session_close_utc", UTC_TIMESTAMP),
    ("session_open_ist", IST_TIMESTAMP),
    ("session_close_ist", IST_TIMESTAMP),
    ("resolution_seconds", pa.int32()),
    ("observed_bar_count", pa.int32()),
    ("expected_bar_count", pa.int32()),
    ("missing_underlying_bar_count", pa.int32()),
    ("special_session", pa.bool_()),
    ("source_archive", pa.string()),
    ("source_archive_sha256", pa.string()),
])


def parse_timestamp(value: str | dt.datetime) -> dt.datetime:
    parsed = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise SchemaError(f"Timestamp must include a timezone: {value!r}")
    return parsed


@dataclass(frozen=True)
class VisibilityOffsets:
    callback_seconds: float = 0.0
    order_history_seconds: float = 0.0
    orders_seconds: float = 0.0
    positions_seconds: float = 0.0
    callback_priority: int = 30
    order_history_priority: int = 40
    orders_priority: int = 41
    positions_priority: int = 42

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "VisibilityOffsets":
        value = value or {}
        resolved = {}
        for item in dataclasses.fields(cls):
            raw = value.get(item.name, item.default)
            resolved[item.name] = int(raw) if "priority" in item.name else float(raw)
        return cls(**resolved)


@dataclass(frozen=True)
class MarketBar:
    instrument_id: str
    source_instrument_id: str | None
    replay_instrument_token: int
    wave_trading_symbol: str | None
    lean_symbol: str
    underlying: str
    security_type: str
    market: str
    exchange: str
    session_date: dt.date
    bar_start: dt.datetime
    bar_end: dt.datetime
    available_at: dt.datetime
    resolution_seconds: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    open_interest: int | None
    source_archive_sha256: str
    source_archive: str
    source_member: str
    source_member_sha256: str
    source_row: int
    source_universe_sha256: str | None
    missing_bar: bool = False
    missing_reason: str | None = None

    @property
    def symbol(self) -> str:
        if self.wave_trading_symbol is None:
            raise SchemaError(f"No Wave symbol for {self.instrument_id}")
        return self.wave_trading_symbol

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "MarketBar":
        def decimal(name: str) -> Decimal:
            return Decimal(str(row[name]))

        return cls(
            instrument_id=str(row["instrument_id"]),
            source_instrument_id=None if row["source_instrument_id"] is None else str(row["source_instrument_id"]),
            replay_instrument_token=int(row["replay_instrument_token"]),
            wave_trading_symbol=None if row["wave_trading_symbol"] is None else str(row["wave_trading_symbol"]),
            lean_symbol=str(row["lean_symbol"]),
            underlying=str(row["underlying"]),
            security_type=str(row["security_type"]),
            market=str(row["market"]),
            exchange=str(row["exchange"]),
            session_date=row["session_date"] if isinstance(row["session_date"], dt.date) else dt.date.fromisoformat(str(row["session_date"])),
            bar_start=parse_timestamp(row["bar_start_ist"]),
            bar_end=parse_timestamp(row["bar_end_ist"]),
            available_at=parse_timestamp(row["available_at_ist"]),
            resolution_seconds=int(row["resolution_seconds"]),
            open=decimal("open"),
            high=decimal("high"),
            low=decimal("low"),
            close=decimal("close"),
            volume=int(row["volume"]),
            open_interest=None if row.get("open_interest") is None else int(row["open_interest"]),
            source_archive_sha256=str(row["source_archive_sha256"]),
            source_archive=str(row["source_archive"]),
            source_member=str(row["source_member"]),
            source_member_sha256=str(row["source_member_sha256"]),
            source_row=int(row["source_row"]),
            source_universe_sha256=None if row["source_universe_sha256"] is None else str(row["source_universe_sha256"]),
            missing_bar=bool(row.get("missing_bar", False)),
            missing_reason=row.get("missing_reason"),
        )


@dataclass(frozen=True)
class Contract:
    instrument_id: str
    source_instrument_id: str | None
    wave_trading_symbol: str | None
    lean_symbol: str
    underlying: str
    instrument_token: int
    security_type: str
    market: str
    exchange: str
    expiry: dt.date
    strike: Decimal
    right: str
    option_style: str
    expiry_classification: str
    classification_method: str
    source_interval: str
    first_session_date: dt.date
    last_session_date: dt.date
    lot_size: int
    source_contract_multiplier: int | None
    resolved_contract_multiplier: int
    multiplier_resolution: str
    observed_bar_count: int
    missing_bar_count: int
    sessions_observed: int
    first_bar: dt.datetime
    last_bar: dt.datetime
    source_contract_metadata_sha256: str

    @property
    def symbol(self) -> str:
        if self.wave_trading_symbol is None:
            raise SchemaError(f"No Wave symbol for {self.instrument_id}")
        return self.wave_trading_symbol

    @property
    def segment(self) -> str:
        return "NFO-OPT"

    @property
    def tick_size(self) -> Decimal:
        return Decimal("0.05")

    @property
    def contract_multiplier(self) -> int:
        return self.resolved_contract_multiplier

    @property
    def metadata_sha256(self) -> str:
        return self.source_contract_metadata_sha256

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "Contract":
        return cls(
            instrument_id=str(row["instrument_id"]),
            source_instrument_id=None if row["source_instrument_id"] is None else str(row["source_instrument_id"]),
            wave_trading_symbol=None if row["wave_trading_symbol"] is None else str(row["wave_trading_symbol"]),
            lean_symbol=str(row["lean_symbol"]),
            underlying=str(row["underlying"]),
            instrument_token=int(row["replay_instrument_token"]),
            security_type=str(row["security_type"]),
            market=str(row["market"]),
            exchange=str(row["exchange"]),
            expiry=dt.date.fromisoformat(str(row["expiry"])[:10]),
            strike=Decimal(str(row["strike"])),
            right={"c": "call", "p": "put"}.get(str(row["right"]).lower(), str(row["right"]).lower()),
            option_style=str(row["option_style"]).lower(),
            expiry_classification=str(row["expiry_classification"]),
            classification_method=str(row["classification_method"]),
            source_interval=str(row["source_interval"]),
            first_session_date=row["first_session_date"] if isinstance(row["first_session_date"], dt.date) else dt.date.fromisoformat(str(row["first_session_date"])),
            last_session_date=row["last_session_date"] if isinstance(row["last_session_date"], dt.date) else dt.date.fromisoformat(str(row["last_session_date"])),
            lot_size=int(row["lot_size"]),
            source_contract_multiplier=None if row["source_contract_multiplier"] is None else int(row["source_contract_multiplier"]),
            resolved_contract_multiplier=int(row["resolved_contract_multiplier"]),
            multiplier_resolution=str(row["multiplier_resolution"]),
            observed_bar_count=int(row["observed_bar_count"]),
            missing_bar_count=int(row["missing_bar_count"]),
            sessions_observed=int(row["sessions_observed"]),
            first_bar=parse_timestamp(row["first_available_at_utc"]),
            last_bar=parse_timestamp(row["last_available_at_utc"]),
            source_contract_metadata_sha256=str(row["source_contract_metadata_sha256"]),
        )


@dataclass
class ReplayScenario:
    scenario_id: str
    selected_date: dt.date
    symbol: str
    instrument_id: str
    source_instrument_id: str | None
    lean_symbol: str
    instrument_token: int
    expiry: dt.date
    calendar_dte: int
    strike: Decimal
    lot_size: int
    option_close_at_selection: Decimal
    option_close_at_selection_at: dt.datetime
    first_underlying_close: Decimal
    first_underlying_close_at: dt.datetime
    dataset_id: str
    manifest_sha256: str
    contract_metadata_sha256: str
    fee_schedule_sha256: str
    wave_settings: dict[str, Any]
    wave_settings_hashes: dict[str, str]
    missing_quote_policy: str = "fail"
    role: str = "manual"
    seed: int = 104729
    visibility: VisibilityOffsets = field(default_factory=VisibilityOffsets)
    broker_script: list[dict[str, Any]] = field(default_factory=list)
    replay: dict[str, Any] = field(default_factory=dict)
    initial_cash: Decimal = Decimal("1000000")
    initial_positions: list[dict[str, Any]] = field(default_factory=list)
    initial_regular_orders: list[dict[str, Any]] = field(default_factory=list)
    initial_gtts: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        defaults = dataclasses.asdict(APPROVED_DEFAULTS)
        defaults.update(self.replay)
        self.replay = defaults
        self._validate()

    @property
    def buy_gap_points(self) -> Decimal:
        return (self.option_close_at_selection * Decimal("0.10")).quantize(Decimal("0.1"))

    @property
    def sell_gap_points(self) -> Decimal:
        return (self.option_close_at_selection * Decimal("0.10")).quantize(Decimal("0.1"))

    def _validate(self) -> None:
        if not self.symbol.startswith("NIFTY") or not self.symbol.endswith("CE"):
            raise SchemaError("Initial historical scenarios require an exact NIFTY CE symbol")
        if not 5 <= self.calendar_dte <= 20:
            raise SchemaError("Selected contract must have 5-20 calendar DTE")
        if self.lot_size <= 0 or self.option_close_at_selection <= 0:
            raise SchemaError("Lot size and selected option close must be positive")
        if self.initial_cash != Decimal("1000000"):
            raise SchemaError("Approved historical baseline requires INR 1,000,000 starting cash")
        if self.initial_positions or self.initial_regular_orders or self.initial_gtts:
            raise SchemaError("Approved historical baseline must start flat with no orders or GTTs")
        required = {
            "market_resolution": "1minute",
            "cool_off_mode": "disabled",
            "trigger_mode": "close_only",
            "activation_policy": "subsequent_bar",
            "fill_price_mode": "limit",
            "order_mode": "regular",
            "broker_fee_model": "upstox_approximation",
            "equivalence_status": "modeled_not_exact",
        }
        for name, expected in required.items():
            if self.replay.get(name) != expected:
                raise SchemaError(f"{name} must be {expected!r}, got {self.replay.get(name)!r}")
        fill_cooldown = self.wave_settings.get("fill_cooldown", {})
        if fill_cooldown.get("enabled", True) is not True:
            raise SchemaError("Same-side fill cooldown must remain enabled")
        from .normalize import state_hash

        if self.wave_settings_hashes.get("resolved_wave_settings") != state_hash(self.wave_settings):
            raise SchemaError("Resolved Wave settings hash does not match scenario settings")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplayScenario":
        selection = value["selection"]
        dataset = value["market_dataset"]
        worker = value["workers"][0]
        initial = value.get("initial", {})
        config = value.get("config", {})
        cash = initial.get("cash", {}).get("amount", APPROVED_DEFAULTS.starting_cash_inr)
        lot_size = int(worker["lot_size"])
        option_close = Decimal(str(worker["option_close_at_selection"]))
        expected_gap = (option_close * Decimal("0.10")).quantize(Decimal("0.1"))
        worker_expectations = {
            "instance_id": (worker.get("instance_id"), "we-1"),
            "symbol": (worker.get("symbol"), selection["symbol"]),
            "buy_gap_pct": (Decimal(str(worker.get("buy_gap_pct"))), Decimal("0.10")),
            "sell_gap_pct": (Decimal(str(worker.get("sell_gap_pct"))), Decimal("0.10")),
            "buy_gap_points": (Decimal(str(worker.get("buy_gap_points"))), expected_gap),
            "sell_gap_points": (Decimal(str(worker.get("sell_gap_points"))), expected_gap),
            "lots": (int(worker.get("lots", 0)), 1),
            "buy_quantity": (int(worker.get("buy_quantity", 0)), lot_size),
            "sell_quantity": (int(worker.get("sell_quantity", 0)), lot_size),
            "order_mode": (worker.get("order_mode"), "regular"),
        }
        mismatches = [name for name, pair in worker_expectations.items() if pair[0] != pair[1]]
        if mismatches:
            raise SchemaError(f"Scenario worker settings mismatch: {', '.join(mismatches)}")
        return cls(
            scenario_id=str(value["scenario_id"]),
            role=str(value.get("scenario_role", "manual")),
            selected_date=dt.date.fromisoformat(selection["selected_date"]),
            symbol=str(selection["symbol"]),
            instrument_id=str(selection["instrument_id"]),
            source_instrument_id=(
                None
                if selection.get("source_instrument_id") is None
                else str(selection["source_instrument_id"])
            ),
            lean_symbol=str(selection["lean_symbol"]),
            instrument_token=int(
                selection.get("replay_instrument_token")
                or derive_replay_instrument_token(str(selection["instrument_id"]))
            ),
            expiry=dt.date.fromisoformat(selection["expiry"]),
            calendar_dte=int(selection["calendar_dte"]),
            strike=Decimal(str(selection["strike"])),
            lot_size=lot_size,
            option_close_at_selection=option_close,
            option_close_at_selection_at=parse_timestamp(
                selection.get("option_close_at_selection_at")
                or selection["first_completed_underlying_close_at"]
            ),
            first_underlying_close=Decimal(str(selection["first_completed_underlying_close"])),
            first_underlying_close_at=parse_timestamp(selection["first_completed_underlying_close_at"]),
            dataset_id=str(dataset["dataset_id"]),
            manifest_sha256=str(dataset["manifest_sha256"]),
            contract_metadata_sha256=str(selection["contract_metadata_sha256"]),
            fee_schedule_sha256=str(value["replay"]["fee_schedule_sha256"]),
            wave_settings=dict(config["resolved_wave_settings"]),
            wave_settings_hashes=dict(config.get("hashes", {})),
            missing_quote_policy=str(dataset.get("missing_quote_policy", "fail")),
            seed=int(value.get("seed", 104729)),
            visibility=VisibilityOffsets.from_mapping(value.get("visibility")),
            broker_script=list(value.get("broker_script") or []),
            replay=dict(value.get("replay", {})),
            initial_cash=Decimal(str(cash)),
            initial_positions=list(initial.get("positions", [])),
            initial_regular_orders=list(initial.get("regular_orders", [])),
            initial_gtts=list(initial.get("gtts", [])),
        )

    @classmethod
    def load(cls, path: str | Path) -> "ReplayScenario":
        path = Path(path)
        with path.open("r", encoding="utf-8") as handle:
            value = yaml.safe_load(handle) if path.suffix.lower() in {".yaml", ".yml"} else json.load(handle)
        return cls.from_mapping(value)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "scenario_id": self.scenario_id,
            "scenario_role": self.role,
            "seed": self.seed,
            "selection": {
                "selected_date": self.selected_date.isoformat(),
                "first_completed_underlying_close_at": self.first_underlying_close_at.isoformat(),
                "first_completed_underlying_close": str(self.first_underlying_close),
                "option_close_at_selection_at": self.option_close_at_selection_at.isoformat(),
                "right": "call",
                "expiry": self.expiry.isoformat(),
                "calendar_dte": self.calendar_dte,
                "strike": str(self.strike),
                "symbol": self.symbol,
                "instrument_id": self.instrument_id,
                "source_instrument_id": self.source_instrument_id,
                "lean_symbol": self.lean_symbol,
                "replay_instrument_token": self.instrument_token,
                "instrument_token_kind": "synthetic_replay",
                "contract_metadata_sha256": self.contract_metadata_sha256,
            },
            "market_dataset": {
                "schema": DATASET_SCHEMA,
                "dataset_id": self.dataset_id,
                "manifest_sha256": self.manifest_sha256,
                "date": self.selected_date.isoformat(),
                "missing_quote_policy": self.missing_quote_policy,
            },
            "initial": {
                "cash": {"currency": "INR", "amount": str(self.initial_cash)},
                "positions": self.initial_positions,
                "regular_orders": self.initial_regular_orders,
                "gtts": self.initial_gtts,
            },
            "config": {
                "resolved_wave_settings": self.wave_settings,
                "hashes": self.wave_settings_hashes,
            },
            "workers": [{
                "instance_id": "we-1",
                "symbol": self.symbol,
                "option_close_at_selection": str(self.option_close_at_selection),
                "buy_gap_pct": 0.10,
                "sell_gap_pct": 0.10,
                "buy_gap_points": str(self.buy_gap_points),
                "sell_gap_points": str(self.sell_gap_points),
                "lots": 1,
                "lot_size": self.lot_size,
                "buy_quantity": self.lot_size,
                "sell_quantity": self.lot_size,
                "order_mode": "regular",
            }],
            "visibility": dataclasses.asdict(self.visibility),
            "replay": dict(self.replay, fee_schedule_sha256=self.fee_schedule_sha256),
            "broker_script": self.broker_script or None,
        }


@dataclass(frozen=True)
class DatasetManifest:
    schema: str
    schema_version: str
    dataset_id: str
    files: dict[str, dict[str, Any]]
    declared_files: tuple[str, ...]
    producer: dict[str, Any]
    source: dict[str, Any]
    identity: dict[str, Any]
    request: dict[str, Any]
    data_contract: dict[str, Any]
    tables: dict[str, Any]
    source_files: tuple[dict[str, Any], ...]
    manifest_sha256: str
    coverage: dict[str, Any]
    fee_schedule_sha256: str

    @classmethod
    def load(cls, root: str | Path) -> "DatasetManifest":
        path = Path(root) / "manifest.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SchemaError(f"Cannot read dataset manifest {path}: {exc}") from exc
        if value.get("schema") != DATASET_SCHEMA:
            raise SchemaError(f"Expected {DATASET_SCHEMA}, got {value.get('schema')!r}")
        schema_version = str(value.get("schema_version", ""))
        if not schema_version or schema_version.split(".", 1)[0] != "1":
            raise SchemaError(f"Unsupported semantic schema_version: {schema_version!r}")
        without_hash = dict(value)
        claimed_hash = str(without_hash.pop("manifest_sha256", ""))
        computed_hash = hashlib.sha256(
            (json.dumps(without_hash, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        ).hexdigest()
        if not claimed_hash or claimed_hash != computed_hash:
            raise SchemaError("manifest_sha256 does not match canonical manifest content")
        files = {str(entry["path"]): dict(entry) for entry in value.get("files", [])}
        fee_record = files.get("fee_schedule.json")
        if fee_record is None:
            raise SchemaError("Manifest is missing fee_schedule.json inventory")
        return cls(
            schema=value["schema"],
            schema_version=schema_version,
            dataset_id=str(value["dataset_id"]),
            files=files,
            declared_files=tuple(value["declared_files"]),
            producer=dict(value["producer"]),
            source=dict(value["source"]),
            identity=dict(value["identity"]),
            request=dict(value["request"]),
            data_contract=dict(value["data_contract"]),
            tables=dict(value["tables"]),
            source_files=tuple(value["source_files"]),
            manifest_sha256=claimed_hash,
            coverage=dict(value.get("test_metadata", {})),
            fee_schedule_sha256=str(fee_record["sha256"]),
        )
