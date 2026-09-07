"""Continuous multi-session replay over immutable daily Wave datasets."""

from __future__ import annotations

import copy
import datetime as dt
import glob
import json
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq

from .broker_engine import BrokerEngine, FeeSchedule
from .capital import (
    DISCLAIMER as CAPITAL_DISCLAIMER,
    MODEL_ID as CAPITAL_MODEL_ID,
    calculate_capital_peaks,
    calculate_capital_snapshot,
    calculate_profit_on_peak_capital,
)
from .datasets import ReplayDataset, session_segments_for_date
from .scheduler import DeterministicScheduler
from .tracing import TraceRecorder
from .legacy_runner import LegacyWaveRunner
from .normalize import canonical_json, sha256_bytes, state_hash
from .schemas import Contract, DatasetManifest, ReplayScenario, SchemaError

COLLECTION_SCHEMA = "wave-replay-dataset-collection-v1"


def _deep_update(target: dict[str, Any], updates: dict[str, Any]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


@dataclass(frozen=True)
class DatasetPartition:
    session_date: dt.date
    path: Path
    dataset_id: str
    manifest_sha256: str


class ReplayDatasetCollection:
    """Validated date index that loads at most one full dataset partition at a time."""

    def __init__(
        self,
        partitions: Iterable[DatasetPartition],
        compatibility: dict[str, Any],
        *,
        source_manifest: dict[str, Any] | None = None,
        partial_underlying_dates: Iterable[dt.date] = (),
        missing_underlying_dates: Iterable[dt.date] = (),
    ) -> None:
        values = sorted(partitions, key=lambda item: item.session_date)
        dates = [item.session_date for item in values]
        if not values:
            raise SchemaError("Continuous replay collection is empty")
        if dates != sorted(set(dates)):
            raise SchemaError("Continuous replay collection dates must be unique and ordered")
        self.partitions = tuple(values)
        self.compatibility = compatibility
        self._by_date = {item.session_date: item for item in values}
        self._source_manifest = copy.deepcopy(source_manifest)
        self.partial_underlying_dates = tuple(sorted(partial_underlying_dates))
        self.missing_underlying_dates = tuple(sorted(missing_underlying_dates))

    @classmethod
    def from_inputs(cls, inputs: Iterable[str | Path]) -> "ReplayDatasetCollection":
        inputs = [str(value) for value in inputs]
        if len(inputs) == 1:
            candidate = Path(inputs[0])
            manifest_path = (
                candidate / "collection_manifest.json"
                if candidate.is_dir()
                else candidate
            )
            if manifest_path.name == "collection_manifest.json" and manifest_path.exists():
                return cls._from_manifest(manifest_path)
        paths: list[Path] = []
        for value in inputs:
            matches = [Path(item) for item in glob.glob(value)]
            if not matches and Path(value).exists():
                matches = [Path(value)]
            paths.extend(matches)
        return cls._from_dataset_paths(paths)

    @classmethod
    def _from_manifest(cls, path: Path) -> "ReplayDatasetCollection":
        value = json.loads(path.read_text(encoding="utf-8"))
        if value.get("schema") != COLLECTION_SCHEMA or int(value.get("schema_version", 0)) != 1:
            raise SchemaError("Unsupported Wave replay collection manifest")
        if value.get("underlying") != "NIFTY":
            raise SchemaError("Continuous collection underlying must be NIFTY")
        definition = value.get("session_calendar_definition")
        if (
            not isinstance(definition, dict)
            or definition != value.get("compatibility", {}).get(
                "session_calendar_definition"
            )
            or value.get("special_sessions") != definition.get("special_sessions")
        ):
            raise SchemaError("Collection session calendar definition is not cross-bound")
        _validate_collection_hashes(value)
        root = path.parent.resolve()
        entries = value.get("partitions", [])
        expected_dates = list(value.get("expected_session_dates", []))
        actual_dates = [str(entry["session_date"]) for entry in entries]
        if actual_dates != sorted(actual_dates) or actual_dates != expected_dates:
            raise SchemaError("Collection partition order must exactly match expected_session_dates")
        start_date = dt.date.fromisoformat(str(value["start_date"]))
        end_date = dt.date.fromisoformat(str(value["end_date"]))
        derived_dates = [
            day.isoformat()
            for day in _expected_collection_dates(
                definition,
                start_date,
                end_date,
            )
        ]
        if expected_dates != derived_dates:
            raise SchemaError("Collection expected sessions are not calendar-derived")
        paths = []
        pins = {}
        for entry in entries:
            partition_path = (root / str(entry["path"])).resolve()
            if root not in partition_path.parents:
                raise SchemaError("Collection partition path escapes collection root")
            paths.append(partition_path)
            pins[dt.date.fromisoformat(str(entry["session_date"]))] = {
                "dataset_id": str(entry["dataset_id"]),
                "manifest_sha256": str(entry["manifest_sha256"]),
            }
        collection = cls._from_dataset_paths(paths)
        session_calendar = value.get("session_calendar")
        if not isinstance(session_calendar, dict):
            raise SchemaError("Collection session_calendar block is required")
        declared_partial = tuple(
            dt.date.fromisoformat(day)
            for day in session_calendar.get("partial_underlying_session_dates", [])
        )
        declared_missing = tuple(
            dt.date.fromisoformat(day)
            for day in session_calendar.get("missing_underlying_session_dates", [])
        )
        if (
            collection.partial_underlying_dates != declared_partial
            or collection.missing_underlying_dates != declared_missing
        ):
            raise SchemaError("Collection underlying missingness inventory mismatch")
        if set(pins) != {item.session_date for item in collection.partitions}:
            raise SchemaError("Collection pins do not exactly match partition sessions")
        for partition in collection.partitions:
            pin = pins.get(partition.session_date)
            if pin is None:
                raise SchemaError(f"Unpinned collection session {partition.session_date}")
            if (
                partition.dataset_id != pin["dataset_id"]
                or partition.manifest_sha256 != pin["manifest_sha256"]
            ):
                raise SchemaError(f"Collection pin mismatch for {partition.session_date}")
        if value.get("compatibility") != collection.compatibility:
            raise SchemaError("Collection compatibility block does not match partitions")
        collection._source_manifest = copy.deepcopy(value)
        return collection

    @classmethod
    def _from_dataset_paths(cls, paths: Iterable[Path]) -> "ReplayDatasetCollection":
        paths = sorted({path.resolve() for path in paths})
        partitions: list[DatasetPartition] = []
        compatibility: dict[str, Any] | None = None
        partial_underlying_dates: list[dt.date] = []
        missing_underlying_dates: list[dt.date] = []
        for path in paths:
            manifest = DatasetManifest.load(path)
            current = _compatibility(manifest)
            if compatibility is None:
                compatibility = current
            elif current != compatibility:
                raise SchemaError(f"Incompatible continuous dataset partition: {path}")
            coverage = manifest.data_contract.get("underlying_session_coverage", {})
            partial_underlying_dates.extend(
                dt.date.fromisoformat(day)
                for day in coverage.get("partial_underlying_session_dates", [])
            )
            missing_underlying_dates.extend(
                dt.date.fromisoformat(day)
                for day in coverage.get("missing_underlying_session_dates", [])
            )
            sessions = pq.read_table(path / "sessions.parquet", columns=["session_date"]).to_pylist()
            if len(sessions) != 1:
                raise SchemaError(
                    "Continuous collections require one immutable date partition per dataset root"
                )
            if (
                manifest.identity.get("integrity_profile")
                != "wave-replay-special-sessions-v3"
            ):
                raise SchemaError(
                    "Continuous collection partitions require "
                    "integrity_profile=wave-replay-special-sessions-v3"
                )
            for row in sessions:
                date = row["session_date"]
                partitions.append(
                    DatasetPartition(
                        session_date=date,
                        path=path,
                        dataset_id=manifest.dataset_id,
                        manifest_sha256=manifest.manifest_sha256,
                    )
                )
        return cls(
            partitions,
            compatibility or {},
            partial_underlying_dates=partial_underlying_dates,
            missing_underlying_dates=missing_underlying_dates,
        )

    def dates(self, start: dt.date | None = None, end: dt.date | None = None) -> list[dt.date]:
        return [
            item.session_date
            for item in self.partitions
            if (start is None or item.session_date >= start)
            and (end is None or item.session_date <= end)
        ]

    def validate_range(self, start: dt.date, end: dt.date) -> None:
        expected = _expected_collection_dates(
            self.compatibility["session_calendar_definition"],
            start,
            end,
        )
        actual = self.dates(start, end)
        if actual != expected:
            raise SchemaError(
                f"Continuous collection missing/reordered sessions: "
                f"expected={[day.isoformat() for day in expected]}, "
                f"actual={[day.isoformat() for day in actual]}"
            )

    def load_session(self, session_date: dt.date) -> ReplayDataset:
        partition = self._by_date[session_date]
        dataset = ReplayDataset.load(
            partition.path,
            allow_special_sessions_v3=True,
            allow_partial_underlying=True,
        )
        if (
            dataset.manifest.dataset_id != partition.dataset_id
            or dataset.manifest_sha256 != partition.manifest_sha256
        ):
            raise SchemaError(f"Dataset partition changed after collection validation: {partition.path}")
        return dataset

    def to_manifest(self) -> dict[str, Any]:
        if self._source_manifest is not None:
            return copy.deepcopy(self._source_manifest)
        return seal_collection_manifest({
            "schema": COLLECTION_SCHEMA,
            "schema_version": 1,
            "underlying": "NIFTY",
            "session_calendar_definition": copy.deepcopy(
                self.compatibility["session_calendar_definition"]
            ),
            "special_sessions": copy.deepcopy(
                self.compatibility["session_calendar_definition"].get(
                    "special_sessions",
                    {},
                )
            ),
            "start_date": self.partitions[0].session_date.isoformat(),
            "end_date": self.partitions[-1].session_date.isoformat(),
            "expected_session_dates": [item.session_date.isoformat() for item in self.partitions],
            "session_calendar": {
                "partial_underlying_session_dates": [
                    day.isoformat() for day in self.partial_underlying_dates
                ],
                "missing_underlying_session_dates": [
                    day.isoformat() for day in self.missing_underlying_dates
                ],
            },
            "compatibility": self.compatibility,
            "partitions": [
                {
                    "session_date": item.session_date.isoformat(),
                    "path": item.path.name,
                    "dataset_id": item.dataset_id,
                    "manifest_sha256": item.manifest_sha256,
                }
                for item in self.partitions
            ],
        })


@dataclass
class ContinuousReplayConfig:
    start: dt.date
    end: dt.date
    wave_settings: dict[str, Any]
    wave_settings_hashes: dict[str, str]
    missing_quote_policy: str = "skip_decision"
    initial_cash: Decimal = Decimal("1000000")
    session_broker_scripts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    experiment_id: str | None = None
    buy_gap_pct: Decimal = Decimal("0.10")
    sell_gap_pct: Decimal = Decimal("0.10")
    selection_min_premium: Decimal = Decimal("35")
    selection_max_premium: Decimal = Decimal("65")
    selection_target_premium: Decimal = Decimal("50")
    keep_min_premium: Decimal = Decimal("35")
    keep_max_premium: Decimal = Decimal("65")
    minimum_calendar_dte: int = 2
    pre_expiry_exit_dte: int = 1
    sample_classification: str = "in_sample"

    def __post_init__(self) -> None:
        self.wave_settings_hashes = dict(self.wave_settings_hashes)
        self.wave_settings_hashes["resolved_wave_settings"] = state_hash(self.wave_settings)
        if self.missing_quote_policy != "skip_decision":
            raise SchemaError("Continuous replay missing_quote_policy must be skip_decision")
        if not Decimal("0.01") <= self.buy_gap_pct <= Decimal("0.50"):
            raise SchemaError("Experiment buy_gap_pct must be between 0.01 and 0.50")
        if not Decimal("0.01") <= self.sell_gap_pct <= Decimal("0.50"):
            raise SchemaError("Experiment sell_gap_pct must be between 0.01 and 0.50")
        if not (
            Decimal("0") < self.selection_min_premium
            <= self.selection_target_premium
            <= self.selection_max_premium
        ):
            raise SchemaError("Experiment selection premium band/target is invalid")
        if not (
            Decimal("0") < self.keep_min_premium <= self.keep_max_premium
        ):
            raise SchemaError("Experiment keep premium band is invalid")
        if self.minimum_calendar_dte < 2 or self.pre_expiry_exit_dte < 1:
            raise SchemaError("Experiment DTE values are below safety minimums")
        if self.sample_classification not in {"in_sample", "out_of_sample"}:
            raise SchemaError("Unsupported experiment sample classification")

    @classmethod
    def from_mapping(
        cls,
        value: dict[str, Any],
        *,
        wave_settings: dict[str, Any],
        wave_settings_hashes: dict[str, str],
        default_start: str,
        default_end: str,
    ) -> "ContinuousReplayConfig":
        schema = value.get("schema") if value else None
        experiment = schema == "wave-replay-continuous-experiment-v1"
        if value and (
            schema not in {
                "wave-replay-continuous-config-v1",
                "wave-replay-continuous-experiment-v1",
            }
            or int(value.get("schema_version", 0)) != 1
        ):
            raise SchemaError("Unsupported continuous replay config")
        experiment_id = value.get("experiment_id")
        if experiment and (
            not isinstance(experiment_id, str)
            or not experiment_id.strip()
        ):
            raise SchemaError("Experiment schema requires a non-empty experiment_id")
        strategy = dict(value.get("strategy", {}))
        approved = {
            "trigger_mode": "close_only",
            "activation_policy": "subsequent_bar",
            "fill_price_mode": "limit",
            "cool_off_mode": "disabled",
            "same_side_fill_cooldown": "enabled",
            "broker_fee_model": "upstox_approximation",
            "starting_cash_inr": 1_000_000,
            "selection_moneyness": "strictly_otm",
            "minimum_premium": 35,
            "maximum_premium": 65,
            "target_premium": 50,
            "minimum_calendar_dte": 2,
            "carry_into_one_dte_session": False,
        }
        contradictions = [
            key
            for key, expected in approved.items()
            if key in strategy and strategy[key] != expected
        ]
        if contradictions and not experiment:
            raise SchemaError(
                f"Continuous strategy settings contradict approved profile: {contradictions}"
            )
        experimental_keys = {
            "buy_gap_pct",
            "sell_gap_pct",
            "selection_min_premium",
            "selection_max_premium",
            "selection_target_premium",
            "keep_min_premium",
            "keep_max_premium",
            "pre_expiry_exit_dte",
        }
        if not experiment and experimental_keys & set(strategy):
            raise SchemaError(
                "Experiment strategy overrides require "
                "schema=wave-replay-continuous-experiment-v1"
            )
        resolved_wave_settings = copy.deepcopy(wave_settings)
        if experiment:
            _deep_update(
                resolved_wave_settings,
                dict(value.get("wave_settings_overrides", {})),
            )
        return cls(
            start=dt.date.fromisoformat(str(value.get("start", default_start))),
            end=dt.date.fromisoformat(str(value.get("end", default_end))),
            wave_settings=resolved_wave_settings,
            wave_settings_hashes=wave_settings_hashes,
            missing_quote_policy=str(value.get("missing_quote_policy", "skip_decision")),
            session_broker_scripts=dict(value.get("session_broker_scripts", {})),
            experiment_id=(
                str(value.get("experiment_id"))
                if experiment and value.get("experiment_id")
                else None
            ),
            buy_gap_pct=Decimal(str(strategy.get("buy_gap_pct", "0.10"))) if experiment else Decimal("0.10"),
            sell_gap_pct=Decimal(str(strategy.get("sell_gap_pct", "0.10"))) if experiment else Decimal("0.10"),
            selection_min_premium=Decimal(
                str(strategy.get("selection_min_premium", "35") if experiment else "35")
            ),
            selection_max_premium=Decimal(
                str(strategy.get("selection_max_premium", "65") if experiment else "65")
            ),
            selection_target_premium=Decimal(
                str(strategy.get("selection_target_premium", "50") if experiment else "50")
            ),
            keep_min_premium=Decimal(
                str(strategy.get("keep_min_premium", "35") if experiment else "35")
            ),
            keep_max_premium=Decimal(
                str(strategy.get("keep_max_premium", "65") if experiment else "65")
            ),
            minimum_calendar_dte=int(
                strategy.get("minimum_calendar_dte", 2) if experiment else 2
            ),
            pre_expiry_exit_dte=int(
                strategy.get("pre_expiry_exit_dte", 1) if experiment else 1
            ),
            sample_classification=str(
                value.get("sample_classification", "in_sample")
                if experiment
                else "in_sample"
            ),
        )

    def experiment_mapping(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "buy_gap_pct": str(self.buy_gap_pct),
            "sell_gap_pct": str(self.sell_gap_pct),
            "selection_min_premium": str(self.selection_min_premium),
            "selection_max_premium": str(self.selection_max_premium),
            "selection_target_premium": str(self.selection_target_premium),
            "keep_min_premium": str(self.keep_min_premium),
            "keep_max_premium": str(self.keep_max_premium),
            "minimum_calendar_dte": self.minimum_calendar_dte,
            "pre_expiry_exit_dte": self.pre_expiry_exit_dte,
            "sample_classification": self.sample_classification,
            "wave_settings": self.wave_settings,
            "wave_settings_hashes": self.wave_settings_hashes,
        }


class ContinuousTrace:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def append_session(self, session_date: dt.date, events: list[dict[str, Any]]) -> None:
        for event in events:
            value = copy.deepcopy(event)
            value["seq"] = len(self.events) + 1
            value["continuous_session_date"] = session_date.isoformat()
            self.events.append(value)

    def digest(self) -> str:
        return sha256_bytes(
            "".join(canonical_json(event) + "\n" for event in self.events).encode("utf-8")
        )

    def write_jsonl(self, path: str | Path) -> None:
        Path(path).write_text(
            "".join(canonical_json(event) + "\n" for event in self.events),
            encoding="utf-8",
        )


class ContinuousWaveRunner:
    def __init__(
        self,
        *,
        repository_root: str | Path,
        collection: ReplayDatasetCollection,
        config: ContinuousReplayConfig,
    ) -> None:
        self.repository_root = Path(repository_root).resolve()
        self.collection = collection
        self.config = config

    def run(self) -> tuple[dict[str, Any], ContinuousTrace]:
        self.collection.validate_range(self.config.start, self.config.end)
        dates = self.collection.dates(self.config.start, self.config.end)
        if not dates:
            raise SchemaError("No collection sessions inside requested continuous range")
        starting_cash = self.config.initial_cash
        persistent_state = _empty_persistent_state(starting_cash)
        held_identity = None
        trace = ContinuousTrace()
        equity_curve: list[dict[str, Any]] = []
        capital_curve: list[dict[str, Any]] = []
        sessions: list[dict[str, Any]] = []
        rolls: list[dict[str, Any]] = []
        overnight: list[dict[str, Any]] = []
        pending_roll: dict[str, Any] | None = None
        switch_events: list[dict[str, Any]] = []
        no_trade_days: list[dict[str, Any]] = []

        for index, session_date in enumerate(dates):
            dataset = self.collection.load_session(session_date)
            opening_state = copy.deepcopy(persistent_state)
            decision: dict[str, Any]
            if held_identity is None:
                contract, underlying_bar, option_bar, candidates = _select_otm_target(
                    dataset,
                    session_date,
                    config=self.config,
                )
                decision = {
                    "action": "select" if contract is not None else "no_trade",
                    "reason": (
                        "flat_select_nearest_expiry_otm_premium"
                        if contract is not None
                        else _selection_failure_reason(candidates)
                    ),
                    "at": None if underlying_bar is None else underlying_bar.bar_end.isoformat(),
                    "held_premium": None,
                    "qualifying_candidates": candidates,
                }
                if pending_roll is not None:
                    pending_roll["new_contract"] = (
                        None if contract is None else _contract_identity(contract)
                    )
                    pending_roll = None
            else:
                held_contract = _map_carried_contract(dataset, session_date, held_identity)
                try:
                    held_underlying, held_option = _synchronized_opening_pair(
                        dataset,
                        held_contract,
                        session_date,
                    )
                except SchemaError:
                    next_date = dates[index + 1] if index + 1 < len(dates) else None
                    if (
                        next_date is not None
                        and (held_contract.expiry - next_date).days
                        <= self.config.pre_expiry_exit_dte
                    ):
                        raise SchemaError(
                            f"No synchronized quote to close {held_contract.instrument_id} "
                            f"before the {next_date} pre-expiry session"
                        )
                    unavailable = _unavailable_held_session_summary(
                        dataset,
                        session_date,
                        persistent_state,
                        held_contract,
                    )
                    sessions.append(unavailable)
                    equity_curve.extend(unavailable["equity_curve"])
                    if self.config.experiment_id is not None:
                        capital_curve.append(
                            calculate_capital_snapshot(
                                at=unavailable["equity_curve"][0]["at"],
                                positions=persistent_state["positions"],
                                orders=[],
                                option_marks={},
                                underlying_spot=None,
                            )
                        )
                    no_trade_days.append({
                        "session_date": session_date.isoformat(),
                        "reason": "held_contract_quote_unavailable",
                        "qualifying_candidates": [],
                    })
                    unavailable_trace = TraceRecorder()
                    unavailable_trace.add(
                        "CONTINUOUS_HELD_QUOTE_UNAVAILABLE",
                        at=dt.datetime.fromisoformat(unavailable["equity_curve"][0]["at"]),
                        actor=held_contract.wave_trading_symbol,
                        details={"policy": "skip_decision"},
                    )
                    trace.append_session(session_date, unavailable_trace.events)
                    if next_date is not None and _nonzero_positions(persistent_state["positions"]):
                        overnight.append({
                            "session_date": session_date.isoformat(),
                            "next_session_date": next_date.isoformat(),
                            "positions": _nonzero_positions(persistent_state["positions"]),
                        })
                    del dataset
                    continue
                held_dte = (held_contract.expiry - session_date).days
                held_in_range = (
                    self.config.keep_min_premium
                    <= held_option.close
                    <= self.config.keep_max_premium
                )
                if held_dte >= self.config.minimum_calendar_dte and held_in_range:
                    contract = held_contract
                    underlying_bar = held_underlying
                    option_bar = held_option
                    decision = {
                        "action": "keep",
                        "reason": "held_contract_expiry_and_premium_eligible",
                        "at": (
                            option_bar.bar_end.isoformat()
                            if contract is not None
                            else held_option.bar_end.isoformat()
                        ),
                        "held_check_at": held_option.bar_end.isoformat(),
                        "held_premium": str(held_option.close),
                        "qualifying_candidates": [],
                    }
                else:
                    if persistent_state is None:
                        raise RuntimeError("Held contract has no persistent broker state")
                    persistent_state, switch_trace, switch = _close_at_opening_switch(
                        dataset,
                        persistent_state,
                        held_contract,
                        held_option,
                        reason=(
                            (
                                "held_expiry_below_minimum_dte"
                                if self.config.experiment_id is not None
                                else "held_expiry_below_2_dte"
                            )
                            if held_dte < self.config.minimum_calendar_dte
                            else (
                                "held_premium_outside_"
                                f"{self.config.keep_min_premium}_"
                                f"{self.config.keep_max_premium}"
                            )
                        ),
                        full_precision_fees=(
                            self.config.experiment_id is not None
                        ),
                    )
                    capital_curve.extend(
                        switch.get("capital_snapshots", [])
                    )
                    trace.append_session(session_date, switch_trace.events)
                    switch_events.append(switch)
                    contract, underlying_bar, option_bar, candidates = _select_otm_target(
                        dataset,
                        session_date,
                        at=held_option.bar_end,
                        config=self.config,
                    )
                    decision = {
                        "action": "switch" if contract is not None else "switch_to_flat",
                        "reason": (
                            switch["reason"]
                            if contract is not None
                            else f"{switch['reason']}:{_selection_failure_reason(candidates)}"
                        ),
                        "at": (
                            option_bar.bar_end.isoformat()
                            if contract is not None
                            else held_option.bar_end.isoformat()
                        ),
                        "held_check_at": held_option.bar_end.isoformat(),
                        "held_premium": str(held_option.close),
                        "qualifying_candidates": candidates,
                        "switch_close": switch,
                    }
            next_date = dates[index + 1] if index + 1 < len(dates) else None
            if contract is None:
                no_trade = _flat_session_summary(
                    dataset,
                    session_date,
                    persistent_state,
                    decision,
                )
                if decision.get("switch_close"):
                    _merge_switch_activity(no_trade, decision["switch_close"])
                    _apply_pre_start_gap_to_flat_session(
                        no_trade,
                        dataset,
                        opening_state,
                        decision["switch_close"],
                    )
                sessions.append(no_trade)
                no_trade_trace = TraceRecorder()
                no_trade_trace.add(
                    "CONTINUOUS_NO_TRADE_SESSION",
                    at=dt.datetime.fromisoformat(no_trade["equity_curve"][0]["at"]),
                    details={
                        "reason": decision["reason"],
                        "qualifying_candidates": decision["qualifying_candidates"],
                    },
                )
                trace.append_session(session_date, no_trade_trace.events)
                no_trade_days.append({
                    "session_date": session_date.isoformat(),
                    "reason": decision["reason"],
                    "qualifying_candidates": decision["qualifying_candidates"],
                })
                equity_curve.extend(no_trade["equity_curve"])
                if self.config.experiment_id is not None:
                    pre_gap = next(
                        (
                            point
                            for point in no_trade["equity_curve"]
                            if point.get("reason")
                            == "waiting_for_first_synchronized_contract_quote"
                        ),
                        None,
                    )
                    if (
                        pre_gap is not None
                        and _nonzero_positions(opening_state["positions"])
                    ):
                        capital_curve.append(
                            calculate_capital_snapshot(
                                at=pre_gap["at"],
                                positions=opening_state["positions"],
                                orders=[],
                                option_marks={},
                                underlying_spot=None,
                            )
                        )
                    capital_curve.append(
                        calculate_capital_snapshot(
                            at=no_trade["equity_curve"][-1]["at"],
                            positions=persistent_state["positions"],
                            orders=[],
                            option_marks={},
                            underlying_spot=None,
                        )
                    )
                held_identity = None
                del dataset
                continue
            roll_before_next = (
                next_date is not None
                and (contract.expiry - next_date).days
                <= self.config.pre_expiry_exit_dte
            )
            scenario = _continuous_scenario(
                dataset,
                contract,
                session_date,
                underlying_bar,
                option_bar,
                self.config,
            )
            runner = LegacyWaveRunner(
                repository_root=self.repository_root,
                dataset=dataset,
                scenario=scenario,
                persistent_broker_state=persistent_state,
                continuous_context={
                    "roll_before_next_session": roll_before_next,
                    "next_session_date": None if next_date is None else next_date.isoformat(),
                    "session_replay_start_at": option_bar.bar_end.isoformat(),
                },
            )
            report, session_trace = runner.run()
            if runner.final_broker_state is None:
                raise RuntimeError("Continuous session did not produce persistent broker state")
            persistent_state = runner.final_broker_state
            trace.append_session(session_date, session_trace.events)
            gap_end = (
                dt.datetime.fromisoformat(decision["switch_close"]["at"])
                if decision.get("switch_close")
                else option_bar.bar_end
            )
            pre_points, pre_missing, _gap_duration = _pre_start_gap(
                dataset,
                session_date,
                gap_end,
                opening_state,
                contract.instrument_id,
            )
            if (
                self.config.experiment_id is not None
                and pre_points
                and _nonzero_positions(opening_state["positions"])
            ):
                capital_curve.append(
                    calculate_capital_snapshot(
                        at=pre_points[0]["at"],
                        positions=opening_state["positions"],
                        orders=[],
                        option_marks={},
                        underlying_spot=None,
                    )
                )
            time_without_contract = 0
            if decision.get("switch_close"):
                switch = decision["switch_close"]
                switch_at = dt.datetime.fromisoformat(switch["at"])
                pre_points.append({
                    "at": switch["at"],
                    "cash": switch["ending_cash"],
                    "equity": switch["ending_equity"],
                    "realized_pnl": switch["ending_realized_pnl"],
                    "unrealized_pnl": switch["ending_unrealized_pnl"],
                    "fees": switch["cumulative_fees"],
                    "reason": "contract_switch_before_new_selection",
                })
                time_without_contract = int(
                    (option_bar.bar_end - switch_at).total_seconds()
                )
            report["equity_curve"] = [*pre_points, *report["equity_curve"]]
            report["data_quality"]["missing_quote_decisions"] = [
                *pre_missing,
                *report["data_quality"]["missing_quote_decisions"],
            ]
            equity_curve.extend(report["equity_curve"])
            capital_curve.extend(report.get("capital_curve", []))
            session_summary = _session_summary(report, contract, session_date)
            session_summary["contract_decision"] = decision
            session_summary["time_without_contract_seconds"] = time_without_contract
            if decision.get("switch_close"):
                _merge_switch_activity(session_summary, decision["switch_close"])
            sessions.append(session_summary)
            nonzero_positions = _nonzero_positions(persistent_state["positions"])
            if next_date is not None and nonzero_positions:
                overnight.append({
                    "session_date": session_date.isoformat(),
                    "next_session_date": next_date.isoformat(),
                    "positions": nonzero_positions,
                })
            roll = report["session_end"].get("roll")
            if roll is not None:
                roll.update({
                    "session_date": session_date.isoformat(),
                    "next_session_date": next_date.isoformat() if next_date else None,
                    "old_contract": _contract_identity(contract),
                    "new_contract": None,
                })
                rolls.append(roll)
                pending_roll = roll
                held_identity = None
            else:
                held_identity = _contract_identity(contract)
            runner.dataset = None
            del runner
            del dataset

        assert persistent_state is not None
        ending_positions = _nonzero_positions(persistent_state["positions"])
        ending_equity = _last_known_equity(equity_curve, persistent_state)
        ending_valuation_known = ending_equity is not None
        final_dataset = self.collection.load_session(dates[-1])
        forced_close = _forced_close_sensitivity(
            final_dataset,
            dates[-1],
            persistent_state,
            starting_cash,
            full_precision_fees=self.config.experiment_id is not None,
        )
        del final_dataset
        capital_curve.sort(key=lambda point: point["at"])
        capital_peaks = (
            calculate_capital_peaks(capital_curve)
            if self.config.experiment_id is not None
            else {}
        )
        capital_profit = (
            (
                calculate_profit_on_peak_capital(
                    monthly_profit=Decimal(str(forced_close["pnl"])),
                    peaks=capital_peaks,
                    sample_classification=self.config.sample_classification,
                )
                if _is_complete_calendar_month(self.config.start, self.config.end)
                else _unavailable_non_monthly_profit(
                    Decimal(str(forced_close["pnl"])),
                    sample_classification=self.config.sample_classification,
                )
            )
            if self.config.experiment_id is not None
            and forced_close.get("pnl") is not None
            else {}
        )
        missing_events = [
            {
                "session_date": item["session_date"],
                **decision,
                "duration_seconds": int(decision.get("duration_seconds", 60)),
            }
            for item in sessions
            for decision in item["missing_quote_decisions"]
        ]
        report = {
            "schema": "wave-replay-continuous-report-v1",
            "mode": "legacy_decision_path_continuous",
            "equivalence_status": "modeled_not_exact",
            "requested_range": {
                "start": self.config.start.isoformat(),
                "end": self.config.end.isoformat(),
                "actual_sessions": [day.isoformat() for day in dates],
            },
            "collection": self.collection.to_manifest(),
            "settings": {
                "trigger_mode": "close_only",
                "activation_policy": "subsequent_bar",
                "fill_price_mode": "limit",
                "buy_gap_pct": float(self.config.buy_gap_pct),
                "sell_gap_pct": float(self.config.sell_gap_pct),
                "cool_off_mode": "disabled",
                "same_side_fill_cooldown": "enabled",
                "broker_fee_model": "upstox_approximation",
                "starting_cash": str(starting_cash),
                "roll_policy": {
                    "owner": "simulator",
                    "not_original_wave": True,
                    "minimum_session_dte": self.config.minimum_calendar_dte,
                    "carry_into_one_dte_session": False,
                    "liquidation_mark": "15:29_available_completed_minute_close",
                    "next_session_source": "authoritative_collection_calendar",
                    "overlap_allowed": False,
                },
                "selection_policy": {
                    "right": "call",
                    "moneyness": "strictly_otm",
                    "minimum_premium": str(self.config.selection_min_premium),
                    "maximum_premium": str(self.config.selection_max_premium),
                    "target_premium": str(self.config.selection_target_premium),
                    "expiry": (
                        "nearest_listed_with_at_least_"
                        f"{self.config.minimum_calendar_dte}_calendar_dte"
                    ),
                    "tie_break": [
                        "absolute_premium_distance",
                        "lower_strike",
                        "instrument_id",
                    ],
                },
            },
            "account": {
                "starting_cash": str(starting_cash),
                "ending_cash": str(persistent_state["cash"]),
                "ending_equity": None if ending_equity is None else str(ending_equity),
                "pnl": None if ending_equity is None else str(ending_equity - starting_cash),
                "realized_pnl": str(persistent_state["realized_pnl"]),
                "unrealized_pnl": (
                    str(_unrealized(persistent_state["positions"]))
                    if ending_valuation_known
                    else None
                ),
                "fees": str(persistent_state["fees_paid"]),
                "positions": ending_positions,
            },
            "activity": {
                "orders": sum(item["orders"] for item in sessions),
                "fills": sum(item["fills"] for item in sessions),
                "cancels": sum(item["cancels"] for item in sessions),
                "day_cancellations": sum(item["day_cancellations"] for item in sessions),
                "post_close_rejections": sum(item["post_close_rejections"] for item in sessions),
                "roll_rejections": sum(item["roll_rejections"] for item in sessions),
                "trades": [
                    {**trade, "session_date": item["session_date"]}
                    for item in sessions
                    for trade in item["trades"]
                ],
                "fill_events": [
                    {**fill, "session_date": item["session_date"]}
                    for item in sessions
                    for fill in item["fill_events"]
                ],
            },
            "inventory": _inventory_summary(sessions, switch_events),
            "equity_curve": equity_curve,
            "drawdown": _drawdown(equity_curve),
            "sessions": sessions,
            "overnight_holdings": overnight,
            "rolls": rolls,
            "switches": switch_events,
            "no_trade_days": no_trade_days,
            "contract_lifecycles": _contract_lifecycles(
                sessions,
                switch_events,
                rolls,
            ),
            "gtt_overnight_state": [
                {
                    "session_date": item["session_date"],
                    "active_gtts": item["active_gtts"],
                }
                for item in sessions
            ],
            "missing_data": [
                *missing_events,
            ],
            "missing_data_summary": {
                "policy": self.config.missing_quote_policy,
                "skipped_minutes": sum(
                    item["duration_seconds"] // 60 for item in missing_events
                ),
                "skipped_duration_seconds": sum(
                    item["duration_seconds"] for item in missing_events
                ),
                "stale_quote_decisions": 0,
                "partial_underlying_session_dates": [
                    day.isoformat() for day in self.collection.partial_underlying_dates
                ],
                "fully_missing_underlying_session_dates": [
                    day.isoformat() for day in self.collection.missing_underlying_dates
                ],
            },
            "contract_statistics": {
                "selections": sum(
                    item.get("contract_decision", {}).get("action") == "select"
                    for item in sessions
                ),
                "keeps": sum(
                    item.get("contract_decision", {}).get("action") == "keep"
                    for item in sessions
                ),
                "switches": len(switch_events),
                "expiry_rolls": len(rolls),
                "no_trade_days": len(no_trade_days),
            },
            "final_boundary_mtm": {
                "ending_cash": str(persistent_state["cash"]),
                "ending_equity": None if ending_equity is None else str(ending_equity),
                "realized_pnl": str(persistent_state["realized_pnl"]),
                "unrealized_pnl": (
                    str(_unrealized(persistent_state["positions"]))
                    if ending_valuation_known
                    else None
                ),
                "fees": str(persistent_state["fees_paid"]),
                "positions": ending_positions,
                "positions_not_mutated": True,
                "valuation_status": (
                    "synchronized" if ending_valuation_known else "unknown_missing_final_quote"
                ),
            },
            "forced_close_sensitivity": forced_close,
            "summaries": _period_summaries(
                equity_curve,
                starting_cash,
                final_valuation_known=ending_valuation_known,
            ),
            "final_state": {
                "positions_remain_open": bool(ending_positions),
                "reason": (
                    "input_ended_before_a_future_roll_or_explicit_close_policy"
                    if ending_positions
                    else "flat_after_roll_or_trading"
                ),
                "day_orders_persisted_overnight": False,
                "active_gtts": sum(
                    trigger.status == "active"
                    for trigger in persistent_state["gtts"].values()
                ),
            },
        }
        if self.config.experiment_id is not None:
            report["experiment"] = self.config.experiment_mapping()
            report["capital"] = {
                "model_id": CAPITAL_MODEL_ID,
                "disclaimer": CAPITAL_DISCLAIMER,
                "curve": capital_curve,
                "peaks": capital_peaks,
                "profit_on_peak_capital": capital_profit,
            }
            report["fee_audit"] = {
                "model_id": "upstox_latest_2026_04_full_precision",
                "broker_fidelity": "not_zerodha",
                "components": {
                    name: str(value)
                    for name, value in persistent_state[
                        "fee_components_paid"
                    ].items()
                },
                "full_precision_total": str(persistent_state["fees_paid"]),
                "rounded_component_total": str(
                    sum(
                        (
                            Decimal(str(value)).quantize(
                                Decimal("0.01"),
                                rounding=ROUND_HALF_UP,
                            )
                            for value in persistent_state[
                                "fee_components_paid"
                            ].values()
                        ),
                        Decimal("0"),
                    )
                ),
                "events": [
                    {**event, "session_date": session["session_date"]}
                    for session in sessions
                    for event in session.get("fee_events", [])
                ],
                "forced_close": {
                    "components": forced_close.get(
                        "exit_fee_components",
                        {},
                    ),
                    "exit_fees": forced_close.get("exit_fees"),
                    "total_fees": forced_close.get("total_fees"),
                    "reconciles": (
                        Decimal(str(persistent_state["fees_paid"]))
                        + Decimal(str(forced_close.get("exit_fees", 0)))
                        == Decimal(str(forced_close.get("total_fees", 0)))
                    ),
                },
            }
            report["settings"]["roll_policy"]["pre_expiry_exit_dte"] = (
                self.config.pre_expiry_exit_dte
            )
        report["trace_digest"] = trace.digest()
        report["report_hash"] = state_hash(report)
        return report, trace


def _compatibility(manifest: DatasetManifest) -> dict[str, Any]:
    source_commits = {
        name: block.get("repository_commit")
        for name, block in manifest.source.items()
        if isinstance(block, dict)
    }
    return {
        "dataset_schema": manifest.schema,
        "schema_version": manifest.schema_version,
        "integrity_profile": manifest.identity.get("integrity_profile"),
        "producer_implementation_commit": manifest.producer.get("implementation_commit"),
        "source_repository_commits": source_commits,
        "fee_schedule_sha256": manifest.fee_schedule_sha256,
        "session_calendar_profile": manifest.identity.get("session_calendar_profile"),
        "session_calendar_definition_sha256": manifest.identity.get(
            "session_calendar_definition_sha256"
        ),
        "session_calendar_definition": copy.deepcopy(
            manifest.data_contract.get("underlying_session_coverage", {}).get(
                "definition",
                {},
            )
        ),
    }


def _expected_collection_dates(
    definition: dict[str, Any],
    start: dt.date,
    end: dt.date,
) -> list[dt.date]:
    dates = []
    cursor = start
    while cursor <= end:
        if session_segments_for_date(definition, cursor):
            dates.append(cursor)
        cursor += dt.timedelta(days=1)
    return dates


def _continuous_scenario(
    dataset: ReplayDataset,
    contract: Contract,
    session_date: dt.date,
    underlying_bar,
    option_bar,
    config: ContinuousReplayConfig,
) -> ReplayScenario:
    replay = {
        "min_calendar_dte": 2,
        "selection_policy": "nearest_expiry_strict_otm_premium_35_65_target_50",
    }
    if config.experiment_id is not None:
        replay.update({
            "min_calendar_dte": config.minimum_calendar_dte,
            "buy_gap_pct": str(config.buy_gap_pct),
            "sell_gap_pct": str(config.sell_gap_pct),
            "capital_model": CAPITAL_MODEL_ID,
        })
    return ReplayScenario(
        scenario_id=f"continuous-{session_date.isoformat()}-{contract.instrument_token}",
        selected_date=session_date,
        symbol=contract.symbol,
        instrument_id=contract.instrument_id,
        source_instrument_id=contract.source_instrument_id,
        lean_symbol=contract.lean_symbol,
        instrument_token=contract.instrument_token,
        expiry=contract.expiry,
        calendar_dte=(contract.expiry - session_date).days,
        strike=contract.strike,
        lot_size=contract.lot_size,
        option_close_at_selection=option_bar.close,
        option_close_at_selection_at=option_bar.bar_end,
        first_underlying_close=underlying_bar.close,
        first_underlying_close_at=underlying_bar.bar_end,
        dataset_id=dataset.manifest.dataset_id,
        manifest_sha256=dataset.manifest_sha256,
        contract_metadata_sha256=contract.metadata_sha256,
        fee_schedule_sha256=dataset.manifest.fee_schedule_sha256,
        wave_settings=copy.deepcopy(config.wave_settings),
        wave_settings_hashes=dict(config.wave_settings_hashes),
        missing_quote_policy=config.missing_quote_policy,
        role="continuous",
        broker_script=copy.deepcopy(
            config.session_broker_scripts.get(session_date.isoformat(), [])
        ),
        replay=replay,
        initial_cash=config.initial_cash,
    )


def _map_carried_contract(
    dataset: ReplayDataset,
    session_date: dt.date,
    identity: dict[str, Any],
) -> Contract:
    candidates = [
        contract
        for contract in dataset.contracts
        if contract.underlying == "NIFTY"
        and contract.right == "call"
        and contract.expiry.isoformat() == identity["expiry"]
        and str(contract.strike) == identity["strike"]
        and contract.lot_size == identity["lot_size"]
        and contract.source_contract_multiplier == identity["source_contract_multiplier"]
        and contract.resolved_contract_multiplier == identity["resolved_contract_multiplier"]
        and contract.metadata_sha256 == identity["contract_metadata_sha256"]
        and (
            (
                identity["source_instrument_id"] is not None
                and contract.source_instrument_id == identity["source_instrument_id"]
            )
            or contract.lean_symbol == identity["lean_symbol"]
            or contract.wave_trading_symbol == identity["wave_trading_symbol"]
        )
    ]
    if len(candidates) != 1:
        raise SchemaError(
            f"Carried contract maps to {len(candidates)} contracts on {session_date}"
        )
    return candidates[0]


def _synchronized_opening_pair(
    dataset: ReplayDataset,
    contract: Contract,
    session_date: dt.date,
):
    underlying_by_end = {
        bar.bar_end: bar
        for bar in dataset.underlying_bars
        if bar.session_date == session_date and not bar.missing_bar
    }
    option_by_end = {
        bar.bar_end: bar
        for bar in dataset.option_bars
        if bar.instrument_id == contract.instrument_id
        and bar.session_date == session_date
        and not bar.missing_bar
    }
    common = sorted(set(underlying_by_end) & set(option_by_end))
    if not common:
        raise SchemaError(
            f"No synchronized underlying/option minute for {contract.instrument_id} on {session_date}"
        )
    first = common[0]
    return underlying_by_end[first], option_by_end[first]


def _select_otm_target(
    dataset: ReplayDataset,
    session_date: dt.date,
    *,
    at: dt.datetime | None = None,
    config: ContinuousReplayConfig | None = None,
) -> tuple[Contract | None, Any | None, Any | None, list[dict[str, Any]]]:
    config = config or ContinuousReplayConfig(
        start=session_date,
        end=session_date,
        wave_settings={},
        wave_settings_hashes={},
    )
    underlying_by_end = {
        bar.bar_end: bar
        for bar in dataset.underlying_bars
        if bar.session_date == session_date
        and bar.underlying == "NIFTY"
        and not bar.missing_bar
    }
    option_by_end: dict[dt.datetime, dict[str, Any]] = {}
    for bar in dataset.option_bars:
        if bar.session_date == session_date and not bar.missing_bar and bar.close > 0:
            option_by_end.setdefault(bar.bar_end, {})[bar.instrument_id] = bar
    eligible = [
        contract
        for contract in dataset.contracts
        if contract.underlying == "NIFTY"
        and contract.right == "call"
        and contract.wave_trading_symbol is not None
        and (contract.expiry - session_date).days >= config.minimum_calendar_dte
    ]
    if not eligible:
        return None, None, None, []
    nearest_expiry = min(contract.expiry for contract in eligible)
    expiry_candidates = [
        contract for contract in eligible if contract.expiry == nearest_expiry
    ]
    timestamps = sorted(set(underlying_by_end) & set(option_by_end))
    if at is not None:
        timestamps = [timestamp for timestamp in timestamps if timestamp >= at]
    last_incomplete_candidates: list[dict[str, Any]] = []
    for timestamp in timestamps:
        if timestamp not in underlying_by_end:
            continue
        otm_contracts = [
            contract
            for contract in expiry_candidates
            if contract.strike > underlying_by_end[timestamp].close
        ]
        if not otm_contracts:
            continue
        underlying = underlying_by_end[timestamp]
        candidates = []
        qualified = []
        for contract in sorted(
            expiry_candidates,
            key=lambda item: (item.strike, item.instrument_id),
        ):
            bar = option_by_end.get(timestamp, {}).get(contract.instrument_id)
            premium = None if bar is None else bar.close
            strictly_otm = contract.strike > underlying.close
            premium_in_range = (
                premium is not None
                and config.selection_min_premium
                <= premium
                <= config.selection_max_premium
            )
            candidate = {
                "instrument_id": contract.instrument_id,
                "symbol": contract.wave_trading_symbol,
                "expiry": contract.expiry.isoformat(),
                "strike": str(contract.strike),
                "premium": None if premium is None else str(premium),
                "strictly_otm": strictly_otm,
                "premium_in_range": premium_in_range,
                "at": timestamp.isoformat(),
            }
            candidates.append(candidate)
            if strictly_otm and premium_in_range:
                qualified.append((contract, bar))
        missing_otm_quotes = any(
            candidate["strictly_otm"] and candidate["premium"] is None
            for candidate in candidates
        )
        if missing_otm_quotes:
            last_incomplete_candidates = candidates
            continue
        if not qualified:
            return None, underlying, None, candidates
        selected, option = min(
            qualified,
            key=lambda pair: (
                abs(pair[1].close - config.selection_target_premium),
                pair[0].strike,
                pair[0].instrument_id,
            ),
        )
        return selected, underlying, option, candidates
    return None, None, None, last_incomplete_candidates


def _empty_persistent_state(starting_cash: Decimal) -> dict[str, Any]:
    return {
        "cash": starting_cash,
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
        "positions": {},
        "gtts": {},
        "order_counter": 0,
        "trigger_counter": 0,
        "day_cancellation_count": 0,
        "post_close_rejection_count": 0,
        "roll_rejection_count": 0,
    }


def _close_at_opening_switch(
    dataset: ReplayDataset,
    persistent_state: dict[str, Any],
    contract: Contract,
    option_bar,
    *,
    reason: str,
    full_precision_fees: bool = False,
) -> tuple[dict[str, Any], TraceRecorder, dict[str, Any]]:
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(start=option_bar.bar_end, trace=trace)
    engine = BrokerEngine(
        scheduler=scheduler,
        trace=trace,
        fee_schedule=FeeSchedule(dataset.fee_schedule),
        full_precision_fees=full_precision_fees,
        capital_tracking=full_precision_fees,
    )
    engine.restore_persistent_state(persistent_state)
    engine.open_exchange_session()
    underlying_bar = next(
        bar
        for bar in dataset.underlying_bars
        if bar.session_date == option_bar.session_date
        and bar.bar_end == option_bar.bar_end
        and not bar.missing_bar
    )
    engine.set_quote(
        "NSE:NIFTY 50",
        last_price=underlying_bar.close,
        instrument_token=underlying_bar.replay_instrument_token,
    )
    engine.set_quote(
        f"{contract.exchange}:{contract.wave_trading_symbol}",
        last_price=option_bar.close,
        instrument_token=contract.instrument_token,
    )
    engine.record_capital_snapshot()
    for trigger in list(engine.gtts_by_id.values()):
        if trigger.status == "active" and trigger.symbol == contract.wave_trading_symbol:
            engine.delete_gtt(trigger.trigger_id)
    before_realized = engine.realized_pnl
    before_fees = engine.fees_paid
    opening_quantity = int(
        engine.positions_by_symbol.get(
            contract.wave_trading_symbol,
            {},
        ).get("quantity", 0)
    )
    order_id = engine.liquidate_position(
        contract.wave_trading_symbol,
        price=option_bar.close,
        reason=reason,
    )
    engine.block_symbol_until_session_end(contract.wave_trading_symbol)
    switch = {
        "at": option_bar.bar_end.isoformat(),
        "reason": reason,
        "old_contract": _contract_identity(contract),
        "close_order_id": order_id,
        "close_side": "SELL" if opening_quantity > 0 else "BUY",
        "quantity": abs(opening_quantity),
        "close_price": str(option_bar.close),
        "realized_pnl": str(engine.realized_pnl - before_realized),
        "fees": str(engine.fees_paid - before_fees),
        "orders": 1 if order_id is not None else 0,
        "fills": 1 if order_id is not None else 0,
        "trades": (
            [{
                "order_id": order_id,
                "symbol": contract.wave_trading_symbol,
                "side": "SELL" if opening_quantity > 0 else "BUY",
                "quantity": abs(opening_quantity),
                "price": str(option_bar.close),
                "at": option_bar.bar_end.isoformat(),
            }]
            if order_id is not None
            else []
        ),
        "fill_events": [
            {**fill, "symbol": contract.wave_trading_symbol}
            for fill in engine.fill_events
        ],
        "fee_events": list(engine.fee_events),
        "ending_cash": str(engine.cash),
        "ending_equity": str(engine.current_equity()),
        "ending_realized_pnl": str(engine.realized_pnl),
        "ending_unrealized_pnl": str(_unrealized(engine.positions_by_symbol)),
        "cumulative_fees": str(engine.fees_paid),
        "capital_snapshots": list(engine.capital_snapshots),
    }
    trace.add(
        "CONTINUOUS_CONTRACT_SWITCH_CLOSE",
        at=scheduler.now,
        actor=contract.wave_trading_symbol,
        details=switch,
    )
    return engine.export_persistent_state(), trace, switch


def _flat_session_summary(
    dataset: ReplayDataset,
    session_date: dt.date,
    persistent_state: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    session = next(
        row for row in dataset.sessions if str(row["session_date"]) == session_date.isoformat()
    )
    session_open = session["session_open_ist"]
    session_close = session["session_close_ist"]
    if not isinstance(session_open, dt.datetime):
        session_open = dt.datetime.fromisoformat(str(session_open))
    if not isinstance(session_close, dt.datetime):
        session_close = dt.datetime.fromisoformat(str(session_close))
    equity = Decimal(str(persistent_state["cash"])) + sum(
        Decimal(str(position.get("last_price", 0))) * int(position.get("quantity", 0))
        for position in persistent_state["positions"].values()
    )
    points = [
        {
            "at": timestamp.isoformat(),
            "cash": str(persistent_state["cash"]),
            "equity": str(equity),
            "realized_pnl": str(persistent_state["realized_pnl"]),
            "unrealized_pnl": str(_unrealized(persistent_state["positions"])),
            "fees": str(persistent_state["fees_paid"]),
            "reason": reason,
        }
        for timestamp, reason in (
            (session_open, "no_contract_session_open"),
            (session_close, "no_contract_session_close"),
        )
    ]
    return {
        "session_date": session_date.isoformat(),
        "contract": None,
        "contract_decision": decision,
        "orders": 0,
        "fills": 0,
        "cancels": 0,
        "day_cancellations": 0,
        "post_close_rejections": 0,
        "roll_rejections": 0,
        "worker_lifecycle": {"not_started": True, "reason": decision["reason"]},
        "roll": None,
        "ending_cash": str(persistent_state["cash"]),
        "positions": copy.deepcopy(persistent_state["positions"]),
        "fees": str(persistent_state["fees_paid"]),
        "restrictions": None,
        "last_multiplier_info": None,
        "trades": [],
        "fill_events": [],
        "fee_events": [],
        "ending_open_orders": [],
        "missing_quote_decisions": [],
        "active_gtts": sum(
            trigger.status == "active"
            for trigger in persistent_state["gtts"].values()
        ),
        "time_without_contract_seconds": int((session_close - session_open).total_seconds()),
        "equity_curve": points,
    }


def _pre_start_gap(
    dataset: ReplayDataset,
    session_date: dt.date,
    replay_start: dt.datetime,
    opening_state: dict[str, Any],
    instrument_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    session = next(
        row for row in dataset.sessions if str(row["session_date"]) == session_date.isoformat()
    )
    session_open = session["session_open_ist"]
    if not isinstance(session_open, dt.datetime):
        session_open = dt.datetime.fromisoformat(str(session_open))
    first_completed = session_open + dt.timedelta(minutes=1)
    if replay_start <= first_completed:
        return [], [], 0
    duration = int((replay_start - first_completed).total_seconds())
    requires_mark = bool(_nonzero_positions(opening_state["positions"]))
    equity = None if requires_mark else Decimal(str(opening_state["cash"]))
    point = {
        "at": first_completed.isoformat(),
        "cash": str(opening_state["cash"]),
        "equity": None if equity is None else str(equity),
        "realized_pnl": str(opening_state["realized_pnl"]),
        "unrealized_pnl": None if requires_mark else "0",
        "fees": str(opening_state["fees_paid"]),
        "reason": "waiting_for_first_synchronized_contract_quote",
    }
    decision = {
        "action": "skip_decision",
        "complete": False,
        "reason": (
            f"No synchronized selection/held quote for {instrument_id} "
            f"from {first_completed.isoformat()} until {replay_start.isoformat()}"
        ),
        "timestamp": first_completed.isoformat(),
        "instrument_id": instrument_id,
        "duration_seconds": duration,
    }
    return [point], [decision], duration


def _is_complete_calendar_month(start: dt.date, end: dt.date) -> bool:
    if start.day != 1 or (start.year, start.month) != (end.year, end.month):
        return False
    next_month = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return end == next_month - dt.timedelta(days=1)


def _unavailable_non_monthly_profit(
    period_profit: Decimal,
    *,
    sample_classification: str,
) -> dict[str, Any]:
    return {
        "classification": "unavailable_non_monthly_period",
        "sample_classification": sample_classification,
        "annualization": (
            "monthly and annualized percentages require one complete calendar month"
        ),
        "period_profit": str(period_profit),
        "monthly_profit": None,
        "monthly_profit_pct_excluding_margin": None,
        "monthly_profit_pct_including_margin": None,
        "annualized_profit_pct_excluding_margin": None,
        "annualized_profit_pct_including_margin": None,
    }


def _apply_pre_start_gap_to_flat_session(
    session: dict[str, Any],
    dataset: ReplayDataset,
    opening_state: dict[str, Any],
    switch: dict[str, Any],
) -> None:
    switch_at = dt.datetime.fromisoformat(switch["at"])
    pre_points, pre_missing, duration = _pre_start_gap(
        dataset,
        dt.date.fromisoformat(session["session_date"]),
        switch_at,
        opening_state,
        switch["old_contract"]["instrument_id"],
    )
    switch_point = {
        "at": switch["at"],
        "cash": switch["ending_cash"],
        "equity": switch["ending_equity"],
        "realized_pnl": switch["ending_realized_pnl"],
        "unrealized_pnl": switch["ending_unrealized_pnl"],
        "fees": switch["cumulative_fees"],
        "reason": "contract_switch_to_flat",
    }
    session["equity_curve"] = [
        *pre_points,
        switch_point,
        session["equity_curve"][-1],
    ]
    session["missing_quote_decisions"] = pre_missing
    session_close = dt.datetime.fromisoformat(session["equity_curve"][-1]["at"])
    session["time_without_contract_seconds"] = int(
        (session_close - switch_at).total_seconds()
    )


def _unavailable_held_session_summary(
    dataset: ReplayDataset,
    session_date: dt.date,
    persistent_state: dict[str, Any],
    contract: Contract,
) -> dict[str, Any]:
    session = next(
        row for row in dataset.sessions if str(row["session_date"]) == session_date.isoformat()
    )
    session_open = session["session_open_ist"]
    session_close = session["session_close_ist"]
    if not isinstance(session_open, dt.datetime):
        session_open = dt.datetime.fromisoformat(str(session_open))
    if not isinstance(session_close, dt.datetime):
        session_close = dt.datetime.fromisoformat(str(session_close))
    duration = int((session_close - session_open).total_seconds())
    requires_mark = bool(_nonzero_positions(persistent_state["positions"]))
    known_equity = None if requires_mark else Decimal(str(persistent_state["cash"]))
    decision = {
        "action": "skip_decision",
        "complete": False,
        "reason": (
            f"No synchronized held-contract quote for {contract.instrument_id} "
            f"on {session_date}"
        ),
        "timestamp": session_open.isoformat(),
        "instrument_id": contract.instrument_id,
        "duration_seconds": duration,
    }
    points = [
        {
            "at": timestamp.isoformat(),
            "cash": str(persistent_state["cash"]),
            "equity": None if known_equity is None else str(known_equity),
            "realized_pnl": str(persistent_state["realized_pnl"]),
            "unrealized_pnl": None if requires_mark else "0",
            "fees": str(persistent_state["fees_paid"]),
            "reason": reason,
        }
        for timestamp, reason in (
            (session_open, "held_quote_unavailable_session_open"),
            (session_close, "held_quote_unavailable_session_close"),
        )
    ]
    return {
        "session_date": session_date.isoformat(),
        "contract": _contract_identity(contract),
        "contract_decision": {
            "action": "skip",
            "reason": "held_contract_quote_unavailable",
            "at": session_open.isoformat(),
            "held_premium": None,
            "qualifying_candidates": [],
        },
        "orders": 0,
        "fills": 0,
        "cancels": 0,
        "day_cancellations": 0,
        "post_close_rejections": 0,
        "roll_rejections": 0,
        "worker_lifecycle": {"not_started": True, "reason": "held_contract_quote_unavailable"},
        "roll": None,
        "ending_cash": str(persistent_state["cash"]),
        "positions": copy.deepcopy(persistent_state["positions"]),
        "fees": str(persistent_state["fees_paid"]),
        "restrictions": None,
        "last_multiplier_info": None,
        "trades": [],
        "fill_events": [],
        "fee_events": [],
        "ending_open_orders": [],
        "missing_quote_decisions": [decision],
        "active_gtts": sum(
            trigger.status == "active"
            for trigger in persistent_state["gtts"].values()
        ),
        "time_without_contract_seconds": duration,
        "equity_curve": points,
    }


def _merge_switch_activity(
    session: dict[str, Any],
    switch: dict[str, Any],
) -> None:
    session["orders"] += int(switch["orders"])
    session["fills"] += int(switch["fills"])
    session["trades"] = [*switch["trades"], *session["trades"]]
    session["fill_events"] = [*switch["fill_events"], *session["fill_events"]]
    session["fee_events"] = [*switch["fee_events"], *session["fee_events"]]


def _selection_failure_reason(candidates: list[dict[str, Any]]) -> str:
    if any(
        candidate["strictly_otm"] and candidate["premium"] is None
        for candidate in candidates
    ):
        return "incomplete_nearest_expiry_otm_quote_universe"
    return "no_qualifying_otm_premium_in_nearest_expiry"


def _contract_identity(contract: Contract) -> dict[str, Any]:
    return {
        "instrument_id": contract.instrument_id,
        "source_instrument_id": contract.source_instrument_id,
        "wave_trading_symbol": contract.wave_trading_symbol,
        "lean_symbol": contract.lean_symbol,
        "expiry": contract.expiry.isoformat(),
        "strike": str(contract.strike),
        "lot_size": contract.lot_size,
        "source_contract_multiplier": contract.source_contract_multiplier,
        "resolved_contract_multiplier": contract.resolved_contract_multiplier,
        "contract_metadata_sha256": contract.metadata_sha256,
    }


def _nonzero_positions(positions: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        symbol: copy.deepcopy(position)
        for symbol, position in positions.items()
        if int(position.get("quantity", 0)) != 0
    }


def _unrealized(positions: dict[str, dict[str, Any]]) -> Decimal:
    return sum(
        (Decimal(str(position.get("m2m", 0))) for position in positions.values()),
        Decimal("0"),
    )


def _last_known_equity(
    equity_curve: list[dict[str, Any]],
    _persistent_state: dict[str, Any],
) -> Decimal | None:
    if not equity_curve or equity_curve[-1]["equity"] is None:
        return None
    return Decimal(str(equity_curve[-1]["equity"]))


def _session_summary(
    report: dict[str, Any],
    contract: Contract,
    session_date: dt.date,
) -> dict[str, Any]:
    end = report["session_end"]
    return {
        "session_date": session_date.isoformat(),
        "contract": _contract_identity(contract),
        "orders": report["activity"]["regular_orders_placed"],
        "fills": report["activity"]["fills"],
        "cancels": report["activity"]["cancels"],
        "day_cancellations": end["day_orders_cancelled"],
        "post_close_rejections": end["post_close_rejections"],
        "roll_rejections": end["roll_rejections"],
        "worker_lifecycle": end["worker_lifecycle"],
        "roll": end["roll"],
        "ending_cash": report["account"]["ending_cash"],
        "positions": report["account"]["positions"],
        "fees": report["fees"]["fees_paid"],
        "restrictions": report["risk_outputs"]["restrictions"],
        "last_multiplier_info": report["activity"]["last_multiplier_info"],
        "trades": report["activity"]["trades"],
        "fill_events": [
            {**fill, "symbol": contract.wave_trading_symbol}
            for fill in report["activity"]["fill_events"]
        ],
        "fee_events": list(report["fees"].get("events", [])),
        "ending_open_orders": report["account"]["open_orders"],
        "missing_quote_decisions": report["data_quality"]["missing_quote_decisions"],
        "active_gtts": len(report["account"]["open_gtts"]),
        "equity_curve": report["equity_curve"],
        "time_without_contract_seconds": 0,
    }


def _contract_lifecycles(
    sessions: list[dict[str, Any]],
    switches: list[dict[str, Any]],
    rolls: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    lifecycles: dict[str, dict[str, Any]] = {}
    for session in sessions:
        contract = session.get("contract")
        if contract is None:
            continue
        lifecycle = lifecycles.setdefault(
            contract["instrument_id"],
            {
                "contract": contract,
                "first_session": session["session_date"],
                "last_session": session["session_date"],
                "session_decisions": [],
                "close_event": None,
            },
        )
        lifecycle["last_session"] = session["session_date"]
        lifecycle["session_decisions"].append(session["contract_decision"])
    for close_event in [*switches, *rolls]:
        old_contract = close_event["old_contract"]
        lifecycle = lifecycles.setdefault(
            old_contract["instrument_id"],
            {
                "contract": old_contract,
                "first_session": close_event["at"][:10],
                "last_session": close_event["at"][:10],
                "session_decisions": [],
                "close_event": None,
            },
        )
        lifecycle["close_event"] = close_event
    return [lifecycles[key] for key in sorted(lifecycles)]


def _inventory_summary(
    sessions: list[dict[str, Any]],
    _switches: list[dict[str, Any]],
) -> dict[str, Any]:
    events = [
        {
            "at": fill["at"],
            "symbol": fill["symbol"],
            "side": fill["side"],
            "quantity": int(fill["quantity"]),
            "kind": "fill",
        }
        for session in sessions
        for fill in session["fill_events"]
    ]
    inventory: dict[str, int] = {}
    max_abs_by_symbol: dict[str, int] = {}
    max_total = 0
    max_total_at = None
    for event in sorted(
        events,
        key=lambda item: (
            item["at"],
            1,
            item["symbol"],
        ),
    ):
        signed = event["quantity"] if event["side"] == "BUY" else -event["quantity"]
        inventory[event["symbol"]] = inventory.get(event["symbol"], 0) + signed
        max_abs_by_symbol[event["symbol"]] = max(
            max_abs_by_symbol.get(event["symbol"], 0),
            abs(inventory[event["symbol"]]),
        )
        total = sum(abs(quantity) for quantity in inventory.values())
        if total > max_total:
            max_total = total
            max_total_at = event["at"]
    return {
        "max_absolute_quantity_by_symbol": max_abs_by_symbol,
        "max_total_absolute_quantity": max_total,
        "max_total_at": max_total_at,
    }


def _drawdown(equity_curve: list[dict[str, Any]]) -> dict[str, Any]:
    known = [
        (point["at"], Decimal(str(point["equity"])))
        for point in equity_curve
        if point["equity"] is not None
    ]
    if not known:
        return {"max_drawdown": None, "max_drawdown_pct": None, "peak_at": None, "trough_at": None}
    peak_at, peak = known[0]
    max_drawdown = Decimal("0")
    max_pct = Decimal("0")
    max_peak_at = peak_at
    trough_at = peak_at
    global_peak = max(known, key=lambda item: item[1])
    global_trough = min(known, key=lambda item: item[1])
    for at, equity in known:
        if equity > peak:
            peak_at, peak = at, equity
        drawdown = equity - peak
        pct = drawdown / peak if peak else Decimal("0")
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            max_pct = pct
            max_peak_at = peak_at
            trough_at = at
    return {
        "max_drawdown": str(max_drawdown),
        "max_drawdown_pct": str(max_pct),
        "drawdown_peak_at": max_peak_at,
        "drawdown_trough_at": trough_at,
        "peak_equity": str(global_peak[1]),
        "peak_at": global_peak[0],
        "trough_equity": str(global_trough[1]),
        "trough_at": global_trough[0],
    }


def _period_summaries(
    equity_curve: list[dict[str, Any]],
    initial_equity: Decimal,
    *,
    final_valuation_known: bool,
) -> dict[str, list[dict[str, Any]]]:
    del final_valuation_known
    result = {}
    for name, width in (("annual", 4), ("monthly", 7)):
        grouped: dict[str, list[dict[str, Any]]] = {}
        for point in equity_curve:
            grouped.setdefault(point["at"][:width], []).append(point)
        summaries = []
        previous_equity = initial_equity
        previous_fees = Decimal("0")
        for period, points in sorted(grouped.items()):
            final_point = points[-1]
            ending_equity = (
                None
                if final_point["equity"] is None
                else Decimal(str(final_point["equity"]))
            )
            ending_fees = Decimal(str(final_point["fees"]))
            summary = {
                "period": period,
                "start_equity": str(previous_equity),
                "end_equity": (
                    None if ending_equity is None else str(ending_equity)
                ),
                "pnl": (
                    None
                    if ending_equity is None
                    else str(ending_equity - previous_equity)
                ),
                "fees": str(ending_fees - previous_fees),
                "ending_realized_pnl": final_point["realized_pnl"],
                "ending_unrealized_pnl": final_point["unrealized_pnl"],
                "valuation_status": (
                    "synchronized"
                    if ending_equity is not None
                    else "unknown_missing_final_quote"
                ),
            }
            summaries.append(summary)
            if ending_equity is not None:
                previous_equity = ending_equity
            previous_fees = ending_fees
        result[name] = summaries
    return result


def seal_collection_manifest(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    result.pop("collection_id", None)
    result.pop("collection_sha256", None)
    result["collection_id"] = "wave-collection-v1-" + sha256_bytes(
        (canonical_json(result) + "\n").encode("utf-8")
    )[:20]
    result["collection_sha256"] = sha256_bytes(
        (canonical_json(result) + "\n").encode("utf-8")
    )
    return result


def _validate_collection_hashes(value: dict[str, Any]) -> None:
    claimed_hash = str(value.get("collection_sha256", ""))
    without_hash = dict(value)
    without_hash.pop("collection_sha256", None)
    computed_hash = sha256_bytes(
        (canonical_json(without_hash) + "\n").encode("utf-8")
    )
    without_identity = dict(without_hash)
    claimed_id = str(without_identity.pop("collection_id", ""))
    computed_id = "wave-collection-v1-" + sha256_bytes(
        (canonical_json(without_identity) + "\n").encode("utf-8")
    )[:20]
    if claimed_hash != computed_hash or claimed_id != computed_id:
        raise SchemaError("Collection identity/hash does not match canonical content")


def _forced_close_sensitivity(
    dataset: ReplayDataset,
    session_date: dt.date,
    persistent_state: dict[str, Any],
    starting_cash: Decimal,
    *,
    full_precision_fees: bool = False,
) -> dict[str, Any]:
    from .broker_engine import FeeSchedule

    cash = Decimal(str(persistent_state["cash"]))
    realized = Decimal(str(persistent_state["realized_pnl"]))
    existing_fees = Decimal(str(persistent_state["fees_paid"]))
    exit_fees = Decimal("0")
    exit_fee_components = {
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
    }
    exits = []
    unavailable_symbols = []
    fee_schedule = FeeSchedule(dataset.fee_schedule)
    underlying_ends = {
        bar.bar_end
        for bar in dataset.underlying_bars
        if bar.session_date == session_date and not bar.missing_bar
    }
    for symbol, position in sorted(persistent_state["positions"].items()):
        quantity = int(position.get("quantity", 0))
        if quantity == 0:
            continue
        contract = dataset.contract_for_wave_symbol(symbol)
        bars = [
            item
            for item in dataset.option_bars
            if item.instrument_id == contract.instrument_id
            and item.session_date == session_date
            and not item.missing_bar
            and item.bar_end in underlying_ends
        ]
        if not bars:
            unavailable_symbols.append(symbol)
            continue
        bar = max(bars, key=lambda item: item.bar_end)
        side = "SELL" if quantity > 0 else "BUY"
        absolute = abs(quantity)
        fee_breakdown = fee_schedule.calculate_breakdown(
            side=side,
            quantity=absolute,
            price=bar.close,
        )
        fee = (
            fee_breakdown["total"]
            if full_precision_fees
            else fee_breakdown["total"].quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
        )
        cash += (
            bar.close * absolute
            if side == "SELL"
            else -bar.close * absolute
        ) - fee
        average = Decimal(str(position.get("average_price", 0)))
        realized_delta = (
            (bar.close - average) * absolute
            if quantity > 0
            else (average - bar.close) * absolute
        )
        realized += realized_delta
        exit_fees += fee
        for name in exit_fee_components:
            exit_fee_components[name] += fee_breakdown[name]
        exits.append({
            "symbol": symbol,
            "side": side,
            "quantity": absolute,
            "price": str(bar.close),
            "at": bar.bar_end.isoformat(),
            "exit_fee": str(fee),
            "fee_components": {
                name: str(value)
                for name, value in fee_breakdown.items()
            },
            "realized_pnl": str(realized_delta),
        })
    complete = not unavailable_symbols
    return {
        "assumption": "separate_non_mutating_exit_at_last_synchronized_completed_minute",
        "available": complete,
        "unavailable_symbols": unavailable_symbols,
        "exits": exits,
        "ending_cash": str(cash) if complete else None,
        "ending_equity": str(cash) if complete else None,
        "realized_pnl": str(realized) if complete else None,
        "unrealized_pnl": "0" if complete else None,
        "existing_fees": str(existing_fees),
        "exit_fees": str(exit_fees),
        "exit_fee_components": {
            name: str(value)
            for name, value in exit_fee_components.items()
        },
        "total_fees": str(existing_fees + exit_fees),
        "pnl": str(cash - starting_cash) if complete else None,
    }
