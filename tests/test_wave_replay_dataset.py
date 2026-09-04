import datetime as dt
import dataclasses
import json

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from wave_replay.datasets import ReplayDataset, resolve_missing_quote
from wave_replay.normalize import sha256_file
from wave_replay.schemas import SchemaError, derive_replay_instrument_token
from wave_replay.schemas import BAR_ARROW_SCHEMA, CONTRACT_ARROW_SCHEMA, SESSION_ARROW_SCHEMA
from wave_replay.selector import build_scenarios, load_saved_wave_settings, select_contract_for_day
from wave_replay.selector import (
    APPROVED_CORPUS,
    DEFAULT_WAVE_SETTINGS,
    _first_distinct_assignment,
    score_representative_days,
    select_representative_days,
)
from wave_replay.synthetic import create_synthetic_dataset
from wave_replay.seams import instrument_row


def _refresh_manifest_file(root, filename):
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["files"] if item["path"] == filename)
    entry["sha256"] = sha256_file(root / filename)
    entry["size_bytes"] = (root / filename).stat().st_size
    manifest_without_hash = dict(manifest)
    manifest_without_hash.pop("manifest_sha256")
    from wave_replay.normalize import canonical_json, sha256_bytes

    manifest["manifest_sha256"] = sha256_bytes((canonical_json(manifest_without_hash) + "\n").encode())
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")


@pytest.fixture()
def replay_dataset(tmp_path):
    return ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))


def test_synthetic_v1_validates_exact_corpus_and_lot_sizes(replay_dataset):
    assert replay_dataset.manifest.schema == "wave-replay-dataset-v1"
    assert replay_dataset.manifest.schema_version == "1.1.0"
    assert len(replay_dataset.underlying_bars) == 1875
    assert len(replay_dataset.option_bars) == 1863
    assert replay_dataset.validation_report.missingness["option_missing_bars"] == 12
    assert pq.read_table(replay_dataset.root / "underlying_bars.parquet").schema == BAR_ARROW_SCHEMA
    assert pq.read_table(replay_dataset.root / "option_bars.parquet").schema == BAR_ARROW_SCHEMA
    assert pq.read_table(replay_dataset.root / "contracts.parquet").schema == CONTRACT_ARROW_SCHEMA
    assert pq.read_table(replay_dataset.root / "sessions.parquet").schema == SESSION_ARROW_SCHEMA
    assert set(replay_dataset.manifest.declared_files) == {
        "contracts.parquet",
        "fee_schedule.json",
        "manifest.json",
        "option_bars.parquet",
        "sessions.parquet",
        "underlying_bars.parquet",
        "validation_report.json",
    }
    assert replay_dataset.manifest.data_contract["replay_token_origin"] == "sha256(instrument_id)[:15]"
    assert replay_dataset.manifest.producer["implementation_commit"] == "7dab0e1"
    assert replay_dataset.manifest.source["config"]["repository_commit"] == "d667bcb"
    assert replay_dataset.validation_report.missingness["option_session_outages"] == 0

    scenarios = {scenario.role: scenario for scenario in build_scenarios(replay_dataset)}
    assert {
        role: (scenario.selected_date.isoformat(), scenario.symbol, scenario.lot_size)
        for role, scenario in scenarios.items()
    } == {
        "range": ("2025-07-07", "NIFTY2571725400CE", 75),
        "trend": ("2024-11-22", "NIFTY24NOV23400CE", 25),
        "gap_open": ("2026-04-08", "NIFTY2641323850CE", 65),
        "expiry_high_gamma": ("2026-04-02", "NIFTY2640722200CE", 65),
    }
    for scenario in scenarios.values():
        assert scenario.replay["cool_off_mode"] == "disabled"
        assert scenario.replay["trigger_mode"] == "close_only"
        assert scenario.replay["broker_fee_model"] == "upstox_approximation"
        assert float(scenario.buy_gap_points) == pytest.approx(float(scenario.option_close_at_selection) * 0.1)
        contract = replay_dataset.contract_for_instrument(scenario.instrument_id)
        assert contract.lot_size == contract.source_contract_multiplier
        assert contract.resolved_contract_multiplier == 65
        assert contract.source_interval == "1minute"


def test_missing_data_day_fails_contract_selection(replay_dataset):
    with pytest.raises(SchemaError, match="No eligible NIFTY CE contract"):
        select_contract_for_day(replay_dataset, dt.date(2025, 4, 7))


@pytest.mark.parametrize(
    ("policy", "action"),
    [("skip_decision", "skip_decision"), ("mark_unknown", "mark_unknown")],
)
def test_missing_quote_nonfatal_policies(policy, action):
    result = resolve_missing_quote(policy, instrument_id="contract", timestamp="2025-04-07T09:16:00+05:30")
    assert result["action"] == action
    assert result["complete"] is False


def test_missing_quote_fail_policy():
    with pytest.raises(SchemaError, match="Missing completed quote"):
        resolve_missing_quote("fail", instrument_id="contract", timestamp="time")


def test_manifest_hash_and_undeclared_partition_are_rejected(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset")
    (root / "undeclared.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(SchemaError, match="Undeclared dataset files"):
        ReplayDataset.load(root)


def test_replay_tokens_are_synthetic_stable_and_collision_checked(replay_dataset):
    tokens = [contract.instrument_token for contract in replay_dataset.contracts]
    assert len(tokens) == len(set(tokens))
    assert all(token > 0 for token in tokens)
    assert all(
        contract.instrument_token == derive_replay_instrument_token(contract.instrument_id)
        for contract in replay_dataset.contracts
    )
    scenario = build_scenarios(replay_dataset)[0].to_mapping()
    assert scenario["selection"]["instrument_token_kind"] == "synthetic_replay"
    assert scenario["selection"]["replay_instrument_token"] > 0


def test_scenario_round_trip_freezes_settings(replay_dataset, tmp_path):
    scenario = build_scenarios(replay_dataset)[0]
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario.to_mapping()), encoding="utf-8")
    loaded = type(scenario).load(path)
    assert loaded.symbol == scenario.symbol
    assert loaded.instrument_token == scenario.instrument_token
    assert loaded.wave_settings == scenario.wave_settings


def test_scenario_loader_rejects_tampered_worker_execution_fields(replay_dataset, tmp_path):
    scenario = build_scenarios(replay_dataset)[0].to_mapping()
    scenario["workers"][0]["buy_quantity"] = 65 if scenario["workers"][0]["lot_size"] != 65 else 25
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(scenario), encoding="utf-8")
    with pytest.raises(SchemaError, match="buy_quantity"):
        type(build_scenarios(replay_dataset)[0]).load(path)


def test_saved_wave_settings_override_defaults_but_keep_fill_cooldown_enabled(tmp_path):
    config = tmp_path / "wave.json"
    limits = tmp_path / "delta.json"
    config.write_text(json.dumps({
        "NIFTY": {
            "delta_multiplier": {"soft_limit": 700},
            "velocity_guard": {"threshold_pct": 1.1},
            "fill_cooldown": {"trigger_count": 3},
        }
    }), encoding="utf-8")
    limits.write_text(json.dumps({"NIFTY": {"default": {"min": -1000, "max": 1000}}}), encoding="utf-8")
    settings, hashes = load_saved_wave_settings(config, delta_limits_path=limits)
    assert settings["delta_multiplier"]["soft_limit"] == 700
    assert settings["velocity_guard"]["threshold_pct"] == 1.1
    assert settings["fill_cooldown"]["enabled"] is True
    assert settings["fill_cooldown"]["trigger_count"] == 3
    assert {"wave_extractor_config", "delta_limits"} <= hashes.keys()


def test_production_fallbacks_include_full_step_scale_and_delta_limits(tmp_path):
    settings, _hashes = load_saved_wave_settings(
        tmp_path / "missing-wave.json",
        delta_limits_path=tmp_path / "missing-delta.json",
    )
    assert settings["multiplier_scale"]["-3"] == [1, 2.5]
    assert settings["multiplier_scale"]["10"] == [15, 1]
    assert settings["delta_limits"]["NIFTY"]["default"] == {"min": -3500, "max": 3500}


def test_single_day_selector_still_enforces_frozen_contract_metadata(replay_dataset):
    range_contract = next(
        contract for contract in replay_dataset.contracts if contract.wave_trading_symbol == "NIFTY2571725400CE"
    )
    replay_dataset.underlying_bars = [
        bar for bar in replay_dataset.underlying_bars if bar.session_date == dt.date(2025, 7, 7)
    ]
    tampered = dataclasses.replace(range_contract, lot_size=999)
    replay_dataset.contracts = [
        tampered if contract.instrument_id == tampered.instrument_id else contract
        for contract in replay_dataset.contracts
    ]
    replay_dataset.contracts_by_id[tampered.instrument_id] = tampered
    with pytest.raises(SchemaError, match="lot_size"):
        select_representative_days(replay_dataset)


def test_legacy_instrument_uses_production_business_day_dte(replay_dataset):
    scenario = next(item for item in build_scenarios(replay_dataset) if item.role == "expiry_high_gamma")
    contract = replay_dataset.contract_for_instrument(scenario.instrument_id)
    assert scenario.calendar_dte == 5
    assert instrument_row(contract, scenario)["days_to_expiry"] == 4


def test_general_selector_scores_alternate_dates_instead_of_requiring_approved_corpus(replay_dataset):
    offset = dt.timedelta(days=730)
    replay_dataset.underlying_bars = [
        dataclasses.replace(
            bar,
            session_date=bar.session_date + offset,
            bar_start=bar.bar_start + offset,
            bar_end=bar.bar_end + offset,
            available_at=bar.available_at + offset,
        )
        for bar in replay_dataset.underlying_bars
    ]
    replay_dataset.option_bars = [
        dataclasses.replace(
            bar,
            session_date=bar.session_date + offset,
            bar_start=bar.bar_start + offset,
            bar_end=bar.bar_end + offset,
            available_at=bar.available_at + offset,
        )
        for bar in replay_dataset.option_bars
    ]
    replay_dataset.contracts = [
        dataclasses.replace(
            contract,
            expiry=contract.expiry + offset,
            first_session_date=contract.first_session_date + offset,
            last_session_date=contract.last_session_date + offset,
            first_bar=contract.first_bar + offset,
            last_bar=contract.last_bar + offset,
        )
        for contract in replay_dataset.contracts
    ]
    for session in replay_dataset.sessions:
        session["session_date"] += offset
        for field in ("session_open_utc", "session_close_utc", "session_open_ist", "session_close_ist"):
            session[field] += offset
    selected = select_representative_days(replay_dataset)
    assert set(selected) == {"range", "trend", "gap_open", "expiry_high_gamma"}
    assert not set(selected.values()) & {value["date"] for value in APPROVED_CORPUS.values()}


def test_general_selector_does_not_collapse_mixed_corpus_to_single_approved_date(replay_dataset):
    keep = APPROVED_CORPUS["trend"]["date"]
    offset = dt.timedelta(days=730)
    moved = {
        bar.session_date: bar.session_date if bar.session_date == keep else bar.session_date + offset
        for bar in replay_dataset.underlying_bars
    }
    replay_dataset.underlying_bars = [
        dataclasses.replace(
            bar,
            session_date=moved[bar.session_date],
            bar_start=bar.bar_start if bar.session_date == keep else bar.bar_start + offset,
            bar_end=bar.bar_end if bar.session_date == keep else bar.bar_end + offset,
            available_at=bar.available_at if bar.session_date == keep else bar.available_at + offset,
        )
        for bar in replay_dataset.underlying_bars
    ]
    replay_dataset.option_bars = [
        dataclasses.replace(
            bar,
            session_date=moved[bar.session_date],
            bar_start=bar.bar_start if bar.session_date == keep else bar.bar_start + offset,
            bar_end=bar.bar_end if bar.session_date == keep else bar.bar_end + offset,
            available_at=bar.available_at if bar.session_date == keep else bar.available_at + offset,
        )
        for bar in replay_dataset.option_bars
    ]
    replay_dataset.contracts = [
        dataclasses.replace(
            contract,
            expiry=contract.expiry if contract.first_session_date == keep else contract.expiry + offset,
            first_session_date=moved[contract.first_session_date],
            last_session_date=moved[contract.last_session_date],
            first_bar=contract.first_bar if contract.first_session_date == keep else contract.first_bar + offset,
            last_bar=contract.last_bar if contract.last_session_date == keep else contract.last_bar + offset,
        )
        for contract in replay_dataset.contracts
    ]
    for session in replay_dataset.sessions:
        original = session["session_date"]
        session["session_date"] = moved[original]
        if original != keep:
            for field in ("session_open_utc", "session_close_utc", "session_open_ist", "session_close_ist"):
                session[field] += offset

    selected = select_representative_days(replay_dataset)
    assert len(selected) == 4
    assert len(set(selected.values())) == 4


def test_general_selector_excludes_partial_underlying_sessions(replay_dataset):
    partial = APPROVED_CORPUS["range"]["date"]
    session = next(row for row in replay_dataset.sessions if row["session_date"] == partial)
    session["observed_bar_count"] -= 1
    session["missing_underlying_bar_count"] = 1

    with pytest.raises(SchemaError, match="four distinct eligible"):
        select_representative_days(replay_dataset)


def test_gap_score_requires_reliable_immediately_preceding_session(replay_dataset):
    dates = sorted(row["session_date"] for row in replay_dataset.sessions)
    prior, candidate = dates[1], dates[2]
    session = next(row for row in replay_dataset.sessions if row["session_date"] == prior)
    session["observed_bar_count"] -= 1
    session["missing_underlying_bar_count"] = 1

    assert score_representative_days(replay_dataset)[candidate]["gap_open"] is None


def test_gap_score_is_independent_of_underlying_row_order(replay_dataset):
    before = score_representative_days(replay_dataset)
    replay_dataset.underlying_bars.reverse()
    after = score_representative_days(replay_dataset)
    assert after == before


def test_representative_assignment_backtracks_when_best_range_is_only_gap_candidate():
    roles = ("range", "trend", "gap_open", "expiry_high_gamma")
    days = [dt.date(2027, 1, day) for day in range(4, 8)]
    ranked = {
        "range": days,
        "trend": days,
        "gap_open": [days[0]],
        "expiry_high_gamma": days,
    }
    selected = _first_distinct_assignment(roles, ranked)
    assert selected is not None
    assert selected["gap_open"] == days[0]
    assert len(set(selected.values())) == 4


def test_bar_duration_is_validated_even_when_manifest_hash_is_updated(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset")
    path = root / "option_bars.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["bar_start_ist"] = rows[0]["bar_end_ist"]
    pq.write_table(pa.Table.from_pylist(rows), path)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    next(item for item in manifest["files"] if item["path"] == "option_bars.parquet")["sha256"] = sha256_file(path)
    manifest_without_hash = dict(manifest)
    manifest_without_hash.pop("manifest_sha256")
    from wave_replay.normalize import canonical_json, sha256_bytes

    manifest["manifest_sha256"] = sha256_bytes((canonical_json(manifest_without_hash) + "\n").encode())
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    with pytest.raises(SchemaError, match="Bar duration|Invalid time ordering"):
        ReplayDataset.load(root)


def test_nonmissing_zero_price_and_negative_volume_are_rejected(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset")
    path = root / "option_bars.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["close"] = rows[0]["open"] = rows[0]["high"] = rows[0]["low"] = 0
    rows[0]["volume"] = -1
    pq.write_table(pa.Table.from_pylist(rows, schema=BAR_ARROW_SCHEMA), path)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["files"] if item["path"] == "option_bars.parquet")
    entry["sha256"] = sha256_file(path)
    entry["size_bytes"] = path.stat().st_size
    manifest_without_hash = dict(manifest)
    manifest_without_hash.pop("manifest_sha256")
    from wave_replay.normalize import canonical_json, sha256_bytes

    manifest["manifest_sha256"] = sha256_bytes((canonical_json(manifest_without_hash) + "\n").encode())
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    with pytest.raises(SchemaError, match="Non-tradable bar values"):
        ReplayDataset.load(root)


def test_bar_source_provenance_must_match_identity_and_source_inventory(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset")
    path = root / "option_bars.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["source_archive_sha256"] = "0" * 64
    pq.write_table(pa.Table.from_pylist(rows, schema=BAR_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "option_bars.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("source archive provenance mismatch" in error for error in report.errors)


def test_bar_session_date_and_expected_slots_are_reconciled(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset")
    option_path = root / "option_bars.parquet"
    option_rows = pq.read_table(option_path).to_pylist()
    option_rows[0]["session_date"] = option_rows[0]["session_date"] + dt.timedelta(days=1)
    pq.write_table(pa.Table.from_pylist(option_rows, schema=BAR_ARROW_SCHEMA), option_path)
    _refresh_manifest_file(root, "option_bars.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("session_date mismatch" in error for error in report.errors)

    root = create_synthetic_dataset(tmp_path / "dataset-count")
    session_path = root / "sessions.parquet"
    sessions = pq.read_table(session_path).to_pylist()
    sessions[0]["expected_bar_count"] = 376
    pq.write_table(pa.Table.from_pylist(sessions, schema=SESSION_ARROW_SCHEMA), session_path)
    _refresh_manifest_file(root, "sessions.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("expected_bar_count does not match duration" in error for error in report.errors)


def test_manifest_scope_grid_and_source_preferred_lot_are_cross_bound(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset-grid")
    path = root / "option_bars.parquet"
    rows = pq.read_table(path).to_pylist()
    for field in (
        "bar_start_utc",
        "bar_end_utc",
        "available_at_utc",
        "bar_start_ist",
        "bar_end_ist",
        "available_at_ist",
    ):
        rows[0][field] += dt.timedelta(seconds=30)
    pq.write_table(pa.Table.from_pylist(rows, schema=BAR_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "option_bars.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("session grid" in error for error in report.errors)

    root = create_synthetic_dataset(tmp_path / "dataset-lot")
    path = root / "contracts.parquet"
    contracts = pq.read_table(path).to_pylist()
    contracts[0]["lot_size"] = contracts[0]["resolved_contract_multiplier"]
    assert contracts[0]["lot_size"] != contracts[0]["source_contract_multiplier"]
    pq.write_table(pa.Table.from_pylist(contracts, schema=CONTRACT_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "contracts.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("Source-preferred lot_size policy" in error for error in report.errors)

    root = create_synthetic_dataset(tmp_path / "dataset-scope")
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["request"]["underlying"] = "BANKNIFTY"
    manifest_without_hash = dict(manifest)
    manifest_without_hash.pop("manifest_sha256")
    from wave_replay.normalize import canonical_json, sha256_bytes

    manifest["manifest_sha256"] = sha256_bytes((canonical_json(manifest_without_hash) + "\n").encode())
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("underlying" in error for error in report.errors)


def test_partition_session_and_contract_lifecycle_semantics_are_bound(tmp_path):
    root = create_synthetic_dataset(tmp_path / "dataset-partition")
    path = root / "underlying_bars.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["security_type"] = "indexoption"
    pq.write_table(pa.Table.from_pylist(rows, schema=BAR_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "underlying_bars.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("Underlying partition" in error for error in report.errors)

    root = create_synthetic_dataset(tmp_path / "dataset-session-duplicate")
    path = root / "sessions.parquet"
    rows = pq.read_table(path).to_pylist()
    rows.append(dict(rows[0]))
    pq.write_table(pa.Table.from_pylist(rows, schema=SESSION_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "sessions.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("Duplicate session_date" in error for error in report.errors)

    root = create_synthetic_dataset(tmp_path / "dataset-lifecycle")
    path = root / "contracts.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["first_available_at_utc"] += dt.timedelta(minutes=1)
    pq.write_table(pa.Table.from_pylist(rows, schema=CONTRACT_ARROW_SCHEMA), path)
    _refresh_manifest_file(root, "contracts.parquet")
    report = ReplayDataset.load(root, validate=False).validation_report
    assert any("availability bounds" in error for error in report.errors)
