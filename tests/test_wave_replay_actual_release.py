import json
from pathlib import Path

import pytest

from wave_replay.datasets import ReplayDataset
from wave_replay.legacy_runner import LegacyWaveRunner
from wave_replay.schemas import ReplayScenario, SchemaError

EXPECTATIONS_PATH = Path("tests/fixtures/wave_replay/actual_release_expectations.json")
SCENARIO_ROOT = Path("tests/fixtures/wave_replay/scenarios/actual")
DATA_ROOT = Path("data/wave-replay")


def _expectations():
    return json.loads(EXPECTATIONS_PATH.read_text(encoding="utf-8"))


def test_release_expectations_reference_frozen_scenarios():
    document = _expectations()
    assert document["producer_commit"].startswith("f5cd9f7")
    assert document["schema_version"] == "1.1.0"
    assert document["integrity_profile"] == "wave-replay-authoritative-sessions-v2"
    assert document["source_commit"].startswith("d667bcb")
    for expected in document["cases"].values():
        scenario_path = SCENARIO_ROOT / expected["scenario"]
        assert scenario_path.exists()
        scenario = ReplayScenario.load(scenario_path)
        assert scenario.dataset_id == expected["dataset_id"]
        assert scenario.manifest_sha256 == expected["manifest_sha256"]
    assert (SCENARIO_ROOT / document["missing_fail"]["scenario"]).exists()


@pytest.mark.parametrize("case_name", sorted(_expectations()["cases"]))
def test_actual_release_replay_matches_compact_expectation_when_dataset_present(case_name):
    expected = _expectations()["cases"][case_name]
    dataset_root = DATA_ROOT / expected["dataset_dir"]
    if not dataset_root.exists():
        pytest.skip(f"ignored release dataset not present: {dataset_root}")
    dataset = ReplayDataset.load(dataset_root)
    scenario = ReplayScenario.load(SCENARIO_ROOT / expected["scenario"])
    first, first_trace = LegacyWaveRunner(
        repository_root=".",
        dataset=dataset,
        scenario=scenario,
    ).run()
    second, second_trace = LegacyWaveRunner(
        repository_root=".",
        dataset=dataset,
        scenario=scenario,
    ).run()
    assert first_trace.digest() == second_trace.digest() == expected["trace_digest"]
    position = next(iter(first["account"]["positions"].values()))
    assert first["activity"]["regular_orders_placed"] == expected["orders"]
    assert first["activity"]["fills"] == expected["fills"]
    assert first["activity"]["cancels"] == expected["cancels"]
    assert position["quantity"] == expected["final_position"]
    assert first["account"]["ending_cash"] == expected["ending_cash"]
    assert first["fees"]["fees_paid"] == expected["fees"]
    assert first["account"]["pnl"] == expected["pnl"]
    assert first["equivalence_status"] == expected["equivalence_status"]
    assert first["data_quality"]["selected_contract"]["missing_bar_count"] == expected["selected_missing_bars"]
    assert first["data_quality"]["missingness"]["option_session_outages"] == expected["option_session_outages"]
    assert first["session_end"]["snapshot_type"] == expected["session_boundary"]


def test_actual_release_missing_fail_policy_when_dataset_present():
    expected = _expectations()["missing_fail"]
    dataset_root = DATA_ROOT / expected["dataset_dir"]
    if not dataset_root.exists():
        pytest.skip(f"ignored release dataset not present: {dataset_root}")
    dataset = ReplayDataset.load(dataset_root)
    scenario = ReplayScenario.load(SCENARIO_ROOT / expected["scenario"])
    with pytest.raises(SchemaError, match=expected["error"]):
        LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()
