"""Deterministic NIFTY representative-day and ATM CE selection."""

from __future__ import annotations

import datetime as dt
import json
from collections import defaultdict
from decimal import Decimal
from typing import Any

from .datasets import ReplayDataset
from .modes import APPROVED_DEFAULTS
from .normalize import state_hash
from .schemas import Contract, ReplayScenario, SchemaError


DEFAULT_WAVE_SETTINGS: dict[str, Any] = {
    "multiplier_scale": {
        "0": [1, 1],
        "1": [1.3, 1],
        "2": [1.7, 1],
        "3": [2.5, 1],
        "4": [3, 1],
        "5": [10, 1],
        "6": [10, 1],
        "7": [10, 1],
        "8": [15, 1],
        "9": [15, 1],
        "10": [15, 1],
        "-1": [1, 1.3],
        "-2": [1, 1.7],
        "-3": [1, 2.5],
        "-4": [1, 3],
        "-5": [1, 10],
        "-6": [1, 10],
        "-7": [1, 10],
        "-8": [1, 15],
        "-9": [1, 15],
        "-10": [1, 15],
    },
    "delta_multiplier": {"soft_limit": 500, "hard_limit": 2000, "max_multiplier": 3.0},
    "velocity_guard": {"window_minutes": 15, "threshold_pct": 0.8, "gap_multiplier": 1.5},
    "fill_cooldown": {
        "enabled": True,
        "window_minutes": 10,
        "trigger_count": 2,
        "cooldown_duration_minutes": 15,
    },
    "delta_limits": {"NIFTY": {"default": {"min": -3500, "max": 3500}}},
}
APPROVED_CORPUS = {
    "range": {
        "date": dt.date(2025, 7, 7),
        "strike": Decimal("25400"),
        "expiry": dt.date(2025, 7, 17),
        "lot_size": 75,
    },
    "trend": {
        "date": dt.date(2024, 11, 22),
        "strike": Decimal("23400"),
        "expiry": dt.date(2024, 11, 28),
        "lot_size": 25,
    },
    "gap_open": {
        "date": dt.date(2026, 4, 8),
        "strike": Decimal("23850"),
        "expiry": dt.date(2026, 4, 13),
        "lot_size": 65,
    },
    "expiry_high_gamma": {
        "date": dt.date(2026, 4, 2),
        "strike": Decimal("22200"),
        "expiry": dt.date(2026, 4, 7),
        "lot_size": 65,
    },
    "missing_data": {
        "date": dt.date(2025, 4, 7),
        "strike": Decimal("22050"),
        "expiry": dt.date(2025, 4, 17),
        "lot_size": 75,
    },
}


def select_contract_for_day(
    dataset: ReplayDataset,
    trading_date: dt.date,
    *,
    allow_missing_at_open: bool = False,
) -> tuple[Contract, Any, Any]:
    underlying = sorted(
        (
            bar
            for bar in dataset.underlying_bars
            if bar.underlying == "NIFTY" and bar.bar_end.date() == trading_date and not bar.missing_bar
        ),
        key=lambda bar: bar.bar_end,
    )
    if not underlying:
        raise SchemaError(f"No NIFTY one-minute underlying bars for {trading_date}")
    first = underlying[0]
    option_bars_at_selection = {
        bar.instrument_id: bar
        for bar in dataset.option_bars
        if bar.bar_end == first.bar_end and not bar.missing_bar and bar.close > 0
    }
    candidates: list[Contract] = []
    for contract in dataset.contracts:
        dte = (contract.expiry - trading_date).days
        if (
            contract.underlying == "NIFTY"
            and contract.right == "call"
            and contract.wave_trading_symbol is not None
            and APPROVED_DEFAULTS.min_calendar_dte <= dte <= APPROVED_DEFAULTS.max_calendar_dte
            and contract.first_bar is not None
            and contract.last_bar is not None
            and contract.first_bar <= first.bar_end <= contract.last_bar
        ):
            candidates.append(contract)
    if allow_missing_at_open:
        candidates = [
            contract
            for contract in dataset.contracts
            if contract.underlying == "NIFTY"
            and contract.right == "call"
            and contract.wave_trading_symbol is not None
            and APPROVED_DEFAULTS.min_calendar_dte
            <= (contract.expiry - trading_date).days
            <= APPROVED_DEFAULTS.max_calendar_dte
        ]
    else:
        candidates = [
            contract
            for contract in candidates
            if contract.instrument_id in option_bars_at_selection
        ]
    if not candidates:
        raise SchemaError(f"No eligible NIFTY CE contract with 5-20 DTE for {trading_date}")
    nearest_expiry = min(contract.expiry for contract in candidates)
    expiry_candidates = [contract for contract in candidates if contract.expiry == nearest_expiry]
    selected = min(
        expiry_candidates,
        key=lambda contract: (abs(contract.strike - first.close), contract.strike, contract.instrument_id),
    )
    selected_bar = option_bars_at_selection.get(selected.instrument_id)
    if selected_bar is None and allow_missing_at_open:
        selected_bar = min(
            (
                bar
                for bar in dataset.option_bars
                if bar.instrument_id == selected.instrument_id
                and bar.session_date == trading_date
                and bar.close > 0
            ),
            key=lambda bar: bar.bar_end,
        )
    if selected_bar is None:
        raise SchemaError(f"No completed option close for {selected.instrument_id}")
    return selected, first, selected_bar


def score_representative_days(dataset: ReplayDataset) -> dict[dt.date, dict[str, Decimal]]:
    grouped: dict[dt.date, list[Any]] = defaultdict(list)
    for bar in dataset.underlying_bars:
        if bar.underlying == "NIFTY" and not bar.missing_bar:
            grouped[bar.bar_end.date()].append(bar)
    scores: dict[dt.date, dict[str, Decimal]] = {}
    sorted_dates = sorted(grouped)
    prior_close: Decimal | None = None
    for trading_date in sorted_dates:
        bars = sorted(grouped[trading_date], key=lambda bar: bar.bar_end)
        try:
            contract, _, _ = select_contract_for_day(dataset, trading_date)
        except SchemaError:
            prior_close = bars[-1].close
            continue
        first, last = bars[0].close, bars[-1].close
        high = max(bar.high for bar in bars)
        low = min(bar.low for bar in bars)
        intraday_range = max(Decimal("0.01"), high - low)
        absolute_return = abs(last - first)
        efficiency = absolute_return / intraday_range
        gap = abs(first - prior_close) if prior_close is not None else Decimal("0")
        dte = Decimal((contract.expiry - trading_date).days)
        scores[trading_date] = {
            "range": Decimal("1") - min(Decimal("1"), efficiency),
            "trend": efficiency * intraday_range,
            "gap_open": gap,
            "expiry_high_gamma": Decimal("21") - dte,
        }
        prior_close = last
    return scores


def select_representative_days(dataset: ReplayDataset) -> dict[str, dt.date]:
    dates = {bar.session_date for bar in dataset.underlying_bars if bar.underlying == "NIFTY"}
    available = {
        role: values["date"]
        for role, values in APPROVED_CORPUS.items()
        if values["date"] in dates
    }
    baseline_roles = {"range", "trend", "gap_open", "expiry_high_gamma"}
    if len(available) == 1:
        pass
    elif baseline_roles <= set(available):
        available = {role: available[role] for role in APPROVED_CORPUS if role in baseline_roles}
    else:
        raise SchemaError("Dataset must contain one approved day or all four baseline days")
    selected: dict[str, dt.date] = {}
    for role, day in available.items():
        frozen = APPROVED_CORPUS[role]
        contract, _, _ = select_contract_for_day(
            dataset,
            day,
            allow_missing_at_open=role == "missing_data",
        )
        mismatches = [
            name
            for name, actual in (
                ("strike", contract.strike),
                ("expiry", contract.expiry),
                ("lot_size", contract.lot_size),
            )
            if actual != frozen[name]
        ]
        if mismatches:
            raise SchemaError(f"Approved {role} contract mismatch: {', '.join(mismatches)}")
        selected[role] = day
    return selected


def build_scenarios(
    dataset: ReplayDataset,
    *,
    wave_settings: dict[str, Any] | None = None,
    wave_settings_hashes: dict[str, str] | None = None,
) -> list[ReplayScenario]:
    settings = wave_settings or DEFAULT_WAVE_SETTINGS
    settings_hash = state_hash(settings)
    hashes = dict(wave_settings_hashes or {})
    hashes["resolved_wave_settings"] = settings_hash
    scenarios: list[ReplayScenario] = []
    for role, day in select_representative_days(dataset).items():
        contract, underlying_bar, option_bar = select_contract_for_day(
            dataset,
            day,
            allow_missing_at_open=role == "missing_data",
        )
        scenarios.append(
            ReplayScenario(
                scenario_id=f"nifty-{role.replace('_', '-')}-atm-ce",
                role=role,
                selected_date=day,
                symbol=contract.wave_trading_symbol,
                instrument_id=contract.instrument_id,
                source_instrument_id=contract.source_instrument_id,
                lean_symbol=contract.lean_symbol,
                instrument_token=contract.instrument_token,
                expiry=contract.expiry,
                calendar_dte=(contract.expiry - day).days,
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
                wave_settings=settings,
                wave_settings_hashes=hashes,
            )
        )
    return scenarios


def load_saved_wave_settings(
    wave_config_path,
    *,
    delta_limits_path=None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Resolve current saved NIFTY settings without reading credential-bearing config."""
    from pathlib import Path

    settings = json.loads(json.dumps(DEFAULT_WAVE_SETTINGS))
    hashes: dict[str, str] = {}
    wave_path = Path(wave_config_path)
    if wave_path.exists():
        value = json.loads(wave_path.read_text(encoding="utf-8"))
        section = value.get("NIFTY", {})
        for key in ("multiplier_scale", "delta_multiplier", "velocity_guard", "fill_cooldown"):
            if key in section:
                if isinstance(settings.get(key), dict) and isinstance(section[key], dict):
                    settings[key].update(section[key])
                else:
                    settings[key] = section[key]
        from .normalize import sha256_file

        hashes["wave_extractor_config"] = sha256_file(wave_path)
    else:
        hashes["wave_extractor_config"] = state_hash({"source": "code_defaults"})
    settings.setdefault("fill_cooldown", {})["enabled"] = True

    if delta_limits_path is not None and Path(delta_limits_path).exists():
        delta_path = Path(delta_limits_path)
        settings["delta_limits"] = json.loads(delta_path.read_text(encoding="utf-8"))
        from .normalize import sha256_file

        hashes["delta_limits"] = sha256_file(delta_path)
    else:
        settings["delta_limits"] = {"NIFTY": {"default": {"min": -3500, "max": 3500}}}
        hashes["delta_limits"] = state_hash(settings["delta_limits"])
    return settings, hashes
