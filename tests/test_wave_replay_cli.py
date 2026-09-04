import json
import subprocess
import sys
from pathlib import Path

from wave_replay.cli import main
from wave_replay.compare import approve_golden, compare_jsonl
from wave_replay.scenario_matrix import CHARACTERIZATION_MATRIX


def test_cli_creates_validates_and_selects_frozen_corpus(tmp_path, capsys):
    dataset = tmp_path / "dataset"
    scenarios = tmp_path / "scenarios"
    assert main(["create-synthetic", "--output", str(dataset)]) == 0
    assert main(["validate-dataset", "--dataset", str(dataset)]) == 0
    assert main(["select-corpus", "--dataset", str(dataset), "--output", str(scenarios)]) == 0
    values = [json.loads(path.read_text(encoding="utf-8")) for path in scenarios.glob("*.json")]
    assert len(values) == 4
    assert {value["selection"]["selected_date"] for value in values} == {
        "2024-11-22",
        "2025-07-07",
        "2026-04-02",
        "2026-04-08",
    }
    assert {value["workers"][0]["lot_size"] for value in values} == {25, 65, 75}
    capsys.readouterr()


def test_compare_and_explicit_golden_approval(tmp_path):
    actual = tmp_path / "actual.jsonl"
    golden = tmp_path / "golden.jsonl"
    actual.write_text('{"seq":1}\n', encoding="utf-8")
    try:
        approve_golden(actual, golden, explicit=False)
    except PermissionError:
        pass
    else:
        raise AssertionError("golden approval must be explicit")
    approve_golden(actual, golden, explicit=True)
    assert compare_jsonl(golden, actual).equal
    actual.write_text('{"seq":2}\n', encoding="utf-8")
    difference = compare_jsonl(golden, actual)
    assert not difference.equal
    assert difference.line == 1


def test_characterization_matrix_covers_required_categories():
    categories = {case.category for case in CHARACTERIZATION_MATRIX}
    assert {
        "position",
        "restriction",
        "velocity",
        "fill_cooldown",
        "cancellation",
        "repricing",
        "gtt",
        "order_lifecycle",
        "concurrency",
        "visibility",
        "restart",
        "market_hours",
        "multi_instance",
    } <= categories


def test_cli_replay_matches_approved_startup_golden_in_fresh_process(tmp_path):
    dataset = tmp_path / "dataset"
    scenarios = tmp_path / "scenarios"
    output = tmp_path / "output"
    main(["create-synthetic", "--output", str(dataset)])
    main(["select-corpus", "--dataset", str(dataset), "--output", str(scenarios)])
    scenario_path = scenarios / "nifty-range-atm-ce.json"
    scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
    scenario["replay"]["end_at"] = "2025-07-07T09:17:00+05:30"
    scenario_path.write_text(json.dumps(scenario, indent=2, sort_keys=True), encoding="utf-8")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "wave_replay",
            "replay",
            "--dataset",
            str(dataset),
            "--scenario",
            str(scenario_path),
            "--repository-root",
            ".",
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    golden = Path("tests/fixtures/wave_replay/goldens/nifty_range_startup.jsonl")
    difference = compare_jsonl(golden, output / "trace.jsonl")
    assert difference.equal, difference
    events = [
        json.loads(line)
        for line in (output / "trace.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert max(event["at"] for event in events) <= "2025-07-07T09:17:00.000000+05:30"
    report = json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert report["account"]["pnl"] == "0.0000"
