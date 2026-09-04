import json
from pathlib import Path

import pytest

from wave_replay.executable_scenarios import EXECUTABLE_CASES, execute_case


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
