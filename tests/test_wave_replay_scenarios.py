import json
from pathlib import Path

import pytest

from wave_replay.executable_scenarios import EXECUTABLE_CASES, execute_case
from wave_replay.scenario_matrix import CHARACTERIZATION_MATRIX, DECLARED_TO_EXECUTABLE


@pytest.mark.parametrize("case_id", EXECUTABLE_CASES)
def test_executable_characterization_scenario_matches_canonical_digest(case_id):
    expected = json.loads(
        Path("tests/fixtures/wave_replay/goldens/characterization_digests.json").read_text(
            encoding="utf-8"
        )
    )
    first = execute_case(case_id)
    second = execute_case(case_id)
    assert first == second
    assert first["trace_digest"] == expected[case_id]["trace_digest"]
    assert first["summary"] == expected[case_id]["summary"]


def test_every_declared_stage_case_maps_to_an_executed_golden():
    expected = json.loads(
        Path("tests/fixtures/wave_replay/goldens/characterization_digests.json").read_text(
            encoding="utf-8"
        )
    )
    declared = {case.case_id for case in CHARACTERIZATION_MATRIX}
    assert set(DECLARED_TO_EXECUTABLE) == declared
    for executable_id in DECLARED_TO_EXECUTABLE.values():
        assert executable_id in EXECUTABLE_CASES
        assert executable_id in expected
        assert execute_case(executable_id)["trace_digest"] == expected[executable_id]["trace_digest"]
