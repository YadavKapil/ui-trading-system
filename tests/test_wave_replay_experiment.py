import datetime as dt
from decimal import Decimal

import pytest

from wave_replay.continuous import ContinuousReplayConfig
from wave_replay.continuous import ContinuousWaveRunner, ReplayDatasetCollection
from wave_replay.capital import MODEL_ID
from wave_replay.normalize import state_hash
from wave_replay.schemas import ReplayScenario, SchemaError
from wave_replay.selector import DEFAULT_WAVE_SETTINGS
from wave_replay.synthetic import create_synthetic_dataset


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
    with pytest.raises(SchemaError, match="Experiment strategy overrides require"):
        ContinuousReplayConfig.from_mapping(
            {
                "schema": "wave-replay-continuous-config-v1",
                "schema_version": 1,
                "strategy": {"buy_gap_pct": "0.25"},
            },
            wave_settings=DEFAULT_WAVE_SETTINGS,
            wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
            default_start="2026-01-01",
            default_end="2026-01-31",
        )
    for invalid_id in (None, 0, False, ""):
        with pytest.raises(SchemaError, match="non-empty experiment_id"):
            ContinuousReplayConfig.from_mapping(
                {
                    "schema": "wave-replay-continuous-experiment-v1",
                    "schema_version": 1,
                    "experiment_id": invalid_id,
                    "strategy": {"buy_gap_pct": "0.20"},
                },
                wave_settings=DEFAULT_WAVE_SETTINGS,
                wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
                default_start="2026-01-01",
                default_end="2026-01-31",
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


def test_experiment_report_includes_capital_peaks_and_fee_audit(tmp_path):
    day = dt.date(2026, 1, 5)
    dataset = create_synthetic_dataset(
        tmp_path / "dataset",
        corpus=[{
            "role": "range",
            "date": day,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995"),
            "expiry": dt.date(2026, 1, 13),
            "lot_size": 65,
            "missing": 0,
            "option_base": "50",
        }],
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    config = ContinuousReplayConfig.from_mapping(
        {
            "schema": "wave-replay-continuous-experiment-v1",
            "schema_version": 1,
            "experiment_id": "capital-report",
            "strategy": {
                "buy_gap_pct": "0.20",
                "sell_gap_pct": "0.20",
                "minimum_calendar_dte": 3,
            },
        },
        wave_settings=DEFAULT_WAVE_SETTINGS,
        wave_settings_hashes=_hashes(DEFAULT_WAVE_SETTINGS),
        default_start=day.isoformat(),
        default_end=day.isoformat(),
    )
    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=ReplayDatasetCollection.from_inputs([dataset]),
        config=config,
    ).run()
    assert report["capital"]["model_id"] == MODEL_ID
    assert report["capital"]["curve"]
    assert report["capital"]["peaks"]["capital_used_including_margin"]
    assert report["capital"]["profit_on_peak_capital"][
        "classification"
    ] == "unavailable_non_monthly_period"
    assert report["capital"]["profit_on_peak_capital"][
        "monthly_profit_pct_including_margin"
    ] is None
    assert report["settings"]["selection_policy"]["expiry"] == (
        "nearest_listed_with_at_least_3_calendar_dte"
    )
    assert isinstance(report["fee_audit"]["events"], list)
    assert report["fee_audit"]["forced_close"]["reconciles"] is True
    assert Decimal(report["fee_audit"]["full_precision_total"]) == Decimal(
        report["account"]["fees"]
    )
