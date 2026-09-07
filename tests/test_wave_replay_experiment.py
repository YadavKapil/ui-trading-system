import datetime as dt
from decimal import Decimal

import pytest

from wave_replay.continuous import ContinuousReplayConfig
from wave_replay.normalize import state_hash
from wave_replay.schemas import ReplayScenario, SchemaError
from wave_replay.selector import DEFAULT_WAVE_SETTINGS


def _hashes(settings):
    return {"resolved_wave_settings": state_hash(settings)}


def test_experiment_config_overrides_are_explicit_and_default_safe():
    defaults = ContinuousReplayConfig.from_mapping(
        {},
        wave_settings=DEFAULT_WAVE_SETTINGS,
        wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
        default_start="2026-01-01",
        default_end="2026-01-31",
    )
    assert defaults.buy_gap_pct == Decimal("0.10")
    assert defaults.selection_min_premium == Decimal("35")
    assert defaults.minimum_calendar_dte == 2

    experiment = ContinuousReplayConfig.from_mapping(
        {
            "schema": "wave-replay-continuous-experiment-v1",
            "schema_version": 1,
            "experiment_id": "gap-20-cooldown",
            "strategy": {
                "buy_gap_pct": "0.20",
                "sell_gap_pct": "0.20",
                "selection_min_premium": "40",
                "selection_max_premium": "60",
                "selection_target_premium": "50",
                "keep_min_premium": "40",
                "keep_max_premium": "60",
                "minimum_calendar_dte": 3,
            },
            "wave_settings_overrides": {
                "fill_cooldown": {
                    "trigger_count": 1,
                    "cooldown_duration_minutes": 30,
                }
            },
        },
        wave_settings=DEFAULT_WAVE_SETTINGS,
        wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
        default_start="2026-01-01",
        default_end="2026-01-31",
    )
    assert experiment.buy_gap_pct == Decimal("0.20")
    assert experiment.keep_min_premium == Decimal("40")
    assert experiment.minimum_calendar_dte == 3
    assert experiment.wave_settings["fill_cooldown"]["trigger_count"] == 1
    assert DEFAULT_WAVE_SETTINGS["fill_cooldown"]["trigger_count"] == 2


def test_experiment_config_rejects_unsafe_ranges():
    with pytest.raises(SchemaError, match="gap_pct"):
        ContinuousReplayConfig(
            start=dt.date(2026, 1, 1),
            end=dt.date(2026, 1, 31),
            wave_settings=DEFAULT_WAVE_SETTINGS,
            wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
            buy_gap_pct=Decimal("0"),
        )
    with pytest.raises(SchemaError, match="premium band"):
        ContinuousReplayConfig(
            start=dt.date(2026, 1, 1),
            end=dt.date(2026, 1, 31),
            wave_settings=DEFAULT_WAVE_SETTINGS,
            wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
            selection_min_premium=Decimal("60"),
            selection_target_premium=Decimal("50"),
        )


def test_continuous_scenario_gap_points_use_experiment_percentages():
    scenario = ReplayScenario(
        scenario_id="experiment",
        selected_date=dt.date(2026, 1, 5),
        symbol="NIFTY2611326000CE",
        instrument_id="NIFTY|2026-01-13|C|26000",
        source_instrument_id="source",
        lean_symbol="lean",
        instrument_token=123,
        expiry=dt.date(2026, 1, 13),
        calendar_dte=8,
        strike=Decimal("26000"),
        lot_size=65,
        option_close_at_selection=Decimal("50"),
        option_close_at_selection_at=dt.datetime.fromisoformat(
            "2026-01-05T09:16:00+05:30"
        ),
        first_underlying_close=Decimal("25950"),
        first_underlying_close_at=dt.datetime.fromisoformat(
            "2026-01-05T09:16:00+05:30"
        ),
        dataset_id="dataset",
        manifest_sha256="manifest",
        contract_metadata_sha256="contract",
        fee_schedule_sha256="fees",
        wave_settings=DEFAULT_WAVE_SETTINGS,
        wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
        role="continuous",
        replay={"buy_gap_pct": "0.15", "sell_gap_pct": "0.25"},
    )
    assert scenario.buy_gap_points == Decimal("7.5")
    assert scenario.sell_gap_points == Decimal("12.5")
