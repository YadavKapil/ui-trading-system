import json
import dataclasses
import datetime as dt
import types

import pytest

from wave_replay.datasets import ReplayDataset
from wave_replay.legacy_runner import LegacyWaveRunner
from wave_replay.selector import build_scenarios
from wave_replay.synthetic import create_synthetic_dataset
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.tasks import SimTaskRuntime
from wave_replay.tracing import TraceRecorder


def test_legacy_decision_path_replay_is_deterministic_and_reports_defaults(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")

    report_a, trace_a = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()
    report_b, trace_b = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()

    assert trace_a.digest() == trace_b.digest()
    assert report_a["trace_digest"] == trace_a.digest()
    assert report_a["decision_path"] == "legacy_decision_path"
    assert report_a["equivalence_status"] == "modeled_not_exact"
    assert report_a["contract"]["wave_trading_symbol"] == "NIFTY2571725400CE"
    assert report_a["contract"]["lot_size"] == 75
    assert report_a["settings"]["cool_off_mode"] == "disabled"
    assert report_a["settings"]["same_side_fill_cooldown"] == "enabled"
    assert report_a["settings"]["trigger_mode"] == "close_only"
    assert report_a["settings"]["order_mode"] == "regular"
    assert report_a["fees"]["broker_fee_model"] == "upstox_approximation"
    assert report_a["fees"]["zerodha_fee_fidelity"] is False
    assert report_a["account"]["open_gtts"] == []
    assert any(event["kind"] == "COOL_OFF_BYPASSED" for event in trace_a.events)
    header = next(event for event in trace_a.events if event["kind"] == "TRACE_HEADER")
    assert header["details"]["contract"]["lot_size"] == 75
    decisions = [event for event in trace_a.events if event["kind"] == "WAVE_DECISION"]
    assert decisions
    assert {"fill_cooldown", "delta_multiplier", "velocity_guard", "multiplier_scale"} <= decisions[0]["details"].keys()
    assert decisions[0]["result"]["multiplier_info"]
    assert all({"schema_version", "input", "result", "state_before", "state_after"} <= event.keys() for event in trace_a.events)


def test_all_frozen_corpus_lot_sizes_flow_into_scenarios(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenarios = {item.role: item for item in build_scenarios(dataset)}
    assert scenarios["trend"].lot_size == 25
    assert scenarios["range"].lot_size == 75
    assert scenarios["gap_open"].lot_size == 65
    assert scenarios["expiry_high_gamma"].lot_size == 65
    assert all(item.initial_cash == 1_000_000 for item in scenarios.values())
    assert all(not item.initial_gtts for item in scenarios.values())


def test_runner_rejects_scenario_contract_metadata_drift(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    tampered = dataclasses.replace(scenario, lot_size=25)
    with pytest.raises(ValueError, match="lot_size"):
        LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=tampered)


def test_scripted_regular_failures_can_drive_gtt_child_order(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    failures = [
        {"kind": "failure", "operation": "place_order", "error_type": "transport", "message": "timeout"}
        for _ in range(5)
    ]
    script = failures + [{
        "kind": "gtt_trigger",
        "after_seconds": 120,
        "trigger_id": "first_active",
        "immediate_complete": True,
    }]
    scripted = dataclasses.replace(scenario, broker_script=script)
    report, trace = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scripted).run()
    operations = [
        event["operation"]
        for event in trace.events
        if event["kind"] == "BROKER_CALL"
    ]
    assert "place_gtt" in operations
    assert report["decision_path"] == "legacy_decision_path"


@pytest.mark.parametrize("policy", ["skip_decision", "mark_unknown"])
def test_intraday_missing_bar_policy_prevents_stale_quote_decisions(tmp_path, policy):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    for index, bar in enumerate(dataset.option_bars):
        if (
            bar.instrument_id == scenario.instrument_id
            and bar.bar_end.date() == scenario.selected_date
            and bar.bar_end.time().hour == 9
            and bar.bar_end.time().minute == 19
        ):
            dataset.option_bars[index] = dataclasses.replace(
                bar,
                missing_bar=True,
                missing_reason="source_absent",
            )
            break
    scenario = dataclasses.replace(scenario, missing_quote_policy=policy)
    report, trace = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()
    decisions = report["data_quality"]["missing_quote_decisions"]
    assert decisions and decisions[0]["action"] == policy
    assert any(event["kind"] == "MISSING_QUOTE_POLICY" for event in trace.events)
    if policy == "mark_unknown":
        assert report["equivalence_status"] == "ineligible_unknown_data"
        assert report["account"]["pnl"] is None
        assert report["risk_outputs"]["restrictions"] is None


def test_intraday_missing_bar_fail_policy_stops_replay(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    target = next(
        index
        for index, bar in enumerate(dataset.option_bars)
        if bar.instrument_id == scenario.instrument_id
        and bar.bar_end.date() == scenario.selected_date
        and bar.bar_end.time().minute == 19
    )
    dataset.option_bars[target] = dataclasses.replace(
        dataset.option_bars[target],
        missing_bar=True,
        missing_reason="source_absent",
    )
    with pytest.raises(Exception, match="Missing completed quote"):
        LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()


def test_missing_underlying_slot_uses_same_policy_without_stale_spot(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    dataset.underlying_bars = [
        bar
        for bar in dataset.underlying_bars
        if not (
            bar.session_date == scenario.selected_date
            and bar.bar_end.time().hour == 9
            and bar.bar_end.time().minute == 19
        )
    ]
    scenario = dataclasses.replace(scenario, missing_quote_policy="skip_decision")
    report, _trace = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scenario).run()
    decisions = report["data_quality"]["missing_quote_decisions"]
    assert decisions and decisions[0]["action"] == "skip_decision"
    assert "nifty-index" in decisions[0]["reason"]


def test_unconsumed_broker_script_failure_is_rejected(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    scripted = dataclasses.replace(
        scenario,
        broker_script=[{
            "kind": "failure",
            "operation": "delete_gtt",
            "error_type": "transport",
            "message": "unused",
        }],
    )
    with pytest.raises(RuntimeError, match="Unconsumed broker failures"):
        LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scripted).run()


def test_poll_schedule_is_anchored_to_delayed_worker_start(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    scenario = dataclasses.replace(
        scenario,
        option_close_at_selection_at=scenario.first_underlying_close_at + dt.timedelta(minutes=12),
    )
    runner = object.__new__(LegacyWaveRunner)
    runner.scenario = scenario
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(start=scenario.first_underlying_close_at, trace=trace)
    tasks = SimTaskRuntime(scheduler, trace)
    dummy_module = types.SimpleNamespace()
    bar = types.SimpleNamespace(bar_end=scenario.option_close_at_selection_at + dt.timedelta(minutes=10))
    runner._schedule_polls(scheduler, tasks, dummy_module, [bar], set())
    first_poll = next(event for event in scheduler.pending_snapshot() if event["event_id"] == "poll-1")
    assert first_poll["at"] == (
        scenario.option_close_at_selection_at + dt.timedelta(seconds=180)
    ).isoformat()


def test_timed_failure_activates_at_declared_scheduler_time(tmp_path):
    dataset = ReplayDataset.load(create_synthetic_dataset(tmp_path / "dataset"))
    scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
    scripted = dataclasses.replace(
        scenario,
        broker_script=[{
            "kind": "failure",
            "operation": "place_order",
            "error_type": "transport",
            "message": "timed",
            "after_seconds": 60,
        }],
    )
    report, trace = LegacyWaveRunner(repository_root=".", dataset=dataset, scenario=scripted).run()
    activation = next(
        event for event in trace.events if event["operation"] == "broker-script-1-failure"
        and event["kind"] == "EVENT_STARTED"
    )
    assert activation["at"].endswith("09:17:00.000000+05:30")
    assert report["decision_path"] == "legacy_decision_path"
