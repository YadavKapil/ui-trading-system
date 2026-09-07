"""Command-line interface for validation, selection, replay, and goldens."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

from .bootstrap import ReplayBootstrap
from .broker_engine import BrokerEngine
from .clock import SimClock
from .compare import approve_golden, compare_jsonl
from .coverage_manifest import assert_wave_coverage_complete, build_wave_coverage
from .continuous import (
    ContinuousReplayConfig,
    ContinuousWaveRunner,
    ReplayDatasetCollection,
)
from .datasets import ReplayDataset
from .dashboard import (
    generate_continuous_dashboard,
    generate_provisional_trace_dashboard,
)
from .fake_kite import FakeKite
from .fake_ticker import FakeTicker
from .legacy_runner import LegacyWaveRunner
from .network_guard import UnexpectedNetworkAccess
from .normalize import state_hash
from .schemas import IST, ReplayScenario
from .scheduler import DeterministicScheduler
from .selector import build_scenarios, load_saved_wave_settings
from .synthetic import create_synthetic_dataset
from .tasks import SimTaskRuntime
from .tracing import TraceRecorder


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wave-replay")
    subparsers = parser.add_subparsers(dest="command", required=True)

    synthetic = subparsers.add_parser("create-synthetic", help="create compact schema-v1 fixtures")
    synthetic.add_argument("--output", required=True)

    validate = subparsers.add_parser("validate-dataset")
    validate.add_argument("--dataset", required=True)
    validate.add_argument("--report")

    select = subparsers.add_parser("select-corpus")
    select.add_argument("--dataset", required=True)
    select.add_argument("--output", required=True)
    select.add_argument("--wave-config", default="wave_extractor_config.json")
    select.add_argument("--delta-limits", default="delta_limits.json")

    replay = subparsers.add_parser("replay")
    replay.add_argument("--dataset", required=True)
    replay.add_argument("--scenario", required=True)
    replay.add_argument("--repository-root", default=".")
    replay.add_argument("--output", required=True)
    replay.add_argument("--repeat", type=int, default=1)

    continuous = subparsers.add_parser("replay-continuous")
    continuous.add_argument("--datasets", action="append", required=True)
    continuous.add_argument("--start", required=True)
    continuous.add_argument("--end", required=True)
    continuous.add_argument("--output", required=True)
    continuous.add_argument("--repeat", type=int, default=1)
    continuous.add_argument("--repository-root", default=".")
    continuous.add_argument("--wave-config", default="wave_extractor_config.json")
    continuous.add_argument("--delta-limits", default="delta_limits.json")
    continuous.add_argument("--config")
    continuous.add_argument("--compact", action="store_true")

    dashboard = subparsers.add_parser("render-continuous-dashboard")
    dashboard.add_argument("--report")
    dashboard.add_argument("--trace", required=True)
    dashboard.add_argument("--output", required=True)
    dashboard.add_argument(
        "--chart-asset",
        default="static/vendor/lightweight-charts/4.1.0/lightweight-charts.standalone.production.js",
    )
    dashboard.add_argument(
        "--title",
        default="Wave Continuous Replay — Provisional Result",
    )
    dashboard.add_argument("--simulator-commit", required=True)
    dashboard.add_argument("--max-points", type=int, default=8_000)
    dashboard.add_argument("--provisional-trace-only", action="store_true")
    dashboard.add_argument("--collection-manifest")

    compare = subparsers.add_parser("compare")
    compare.add_argument("--expected", required=True)
    compare.add_argument("--actual", required=True)

    approve = subparsers.add_parser("approve-golden")
    approve.add_argument("--actual", required=True)
    approve.add_argument("--golden", required=True)
    approve.add_argument("--yes", action="store_true")

    coverage = subparsers.add_parser("coverage-manifest")
    coverage.add_argument("--source", default="common_lib.py")
    coverage.add_argument("--output", required=True)
    coverage.add_argument("--repository-root", default=".")
    coverage.add_argument(
        "--digests",
        default="tests/fixtures/wave_replay/goldens/characterization_digests.json",
    )

    require_actual = subparsers.add_parser("require-actual")
    require_actual.add_argument(
        "--expectations",
        default="tests/fixtures/wave_replay/actual_release_expectations.json",
    )
    require_actual.add_argument("--data-root", default="data/wave-replay")

    boundary = subparsers.add_parser("boundary-probe", help=argparse.SUPPRESS)
    boundary.add_argument("--repository-root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create-synthetic":
        path = create_synthetic_dataset(args.output)
        print(json.dumps({"dataset": str(path.resolve())}, sort_keys=True))
        return 0
    if args.command == "validate-dataset":
        dataset = ReplayDataset.load(args.dataset)
        report = dataset.validation_report
        if args.report:
            report.write(args.report)
        print(json.dumps(report.to_mapping(), indent=2, sort_keys=True))
        return 0 if report.valid else 2
    if args.command == "select-corpus":
        dataset = ReplayDataset.load(args.dataset)
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        settings, hashes = load_saved_wave_settings(
            args.wave_config,
            delta_limits_path=args.delta_limits,
        )
        scenarios = build_scenarios(dataset, wave_settings=settings, wave_settings_hashes=hashes)
        for scenario in scenarios:
            (output / f"{scenario.scenario_id}.json").write_text(
                json.dumps(scenario.to_mapping(), indent=2, sort_keys=True),
                encoding="utf-8",
            )
        print(json.dumps({"scenarios": [scenario.scenario_id for scenario in scenarios]}, sort_keys=True))
        return 0
    if args.command == "replay":
        return _run_replay(args)
    if args.command == "replay-continuous":
        return _run_continuous_replay(args)
    if args.command == "render-continuous-dashboard":
        if args.provisional_trace_only:
            if not args.collection_manifest:
                raise ValueError(
                    "--collection-manifest is required for provisional trace-only mode"
                )
            result = generate_provisional_trace_dashboard(
                trace_path=args.trace,
                collection_manifest_path=args.collection_manifest,
                output_path=args.output,
                chart_asset_path=args.chart_asset,
                simulator_commit=args.simulator_commit,
                title=args.title,
                max_points=args.max_points,
            )
        else:
            if not args.report:
                raise ValueError("--report is required unless --provisional-trace-only is set")
            result = generate_continuous_dashboard(
                report_path=args.report,
                trace_path=args.trace,
                output_path=args.output,
                chart_asset_path=args.chart_asset,
                simulator_commit=args.simulator_commit,
                title=args.title,
                max_points=args.max_points,
            )
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.command == "compare":
        difference = compare_jsonl(args.expected, args.actual)
        print(json.dumps(difference.__dict__, indent=2, sort_keys=True))
        return 0 if difference.equal else 1
    if args.command == "approve-golden":
        approve_golden(args.actual, args.golden, explicit=args.yes)
        print(json.dumps({"golden": str(Path(args.golden).resolve())}, sort_keys=True))
        return 0
    if args.command == "coverage-manifest":
        manifest = build_wave_coverage(args.repository_root, args.digests)
        assert_wave_coverage_complete(manifest)
        Path(args.output).write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return 0
    if args.command == "require-actual":
        document = json.loads(Path(args.expectations).read_text(encoding="utf-8"))
        missing = []
        invalid = []
        checked_paths = set()
        for value in document["cases"].values():
            path = Path(args.data_root) / value["dataset_dir"]
            if path in checked_paths:
                continue
            checked_paths.add(path)
            if not path.exists():
                missing.append(str(path))
                continue
            try:
                dataset = ReplayDataset.load(path)
            except Exception as exc:
                invalid.append(f"{path}: {exc}")
                continue
            if dataset.manifest.dataset_id != value["dataset_id"]:
                invalid.append(f"{path}: dataset_id mismatch")
            if dataset.manifest.manifest_sha256 != value["manifest_sha256"]:
                invalid.append(f"{path}: manifest_sha256 mismatch")
        if missing or invalid:
            raise RuntimeError(f"Required actual artifacts unavailable: missing={missing}, invalid={invalid}")
        print(json.dumps({"required_actual": "ready", "datasets": len(checked_paths)}))
        return 0
    if args.command == "boundary-probe":
        print(json.dumps(_boundary_probe(Path(args.repository_root).resolve()), sort_keys=True))
        return 0
    raise AssertionError(args.command)


def _run_replay(args) -> int:
    dataset = ReplayDataset.load(args.dataset)
    scenario = ReplayScenario.load(args.scenario)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    digests: list[str] = []
    final_report = None
    for index in range(args.repeat):
        runner = LegacyWaveRunner(
            repository_root=args.repository_root,
            dataset=dataset,
            scenario=scenario,
        )
        report, trace = runner.run()
        trace_path = output / (f"trace-{index + 1}.jsonl" if args.repeat > 1 else "trace.jsonl")
        trace.write_jsonl(trace_path)
        digests.append(trace.digest())
        final_report = report
    if len(set(digests)) != 1:
        raise RuntimeError(f"Non-deterministic traces: {digests}")
    assert final_report is not None
    final_report["determinism"] = {"repetitions": args.repeat, "trace_digest": digests[0]}
    (output / "report.json").write_text(
        json.dumps(final_report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({"report": str((output / "report.json").resolve()), "trace_digest": digests[0]}, sort_keys=True))
    return 0


def _run_continuous_replay(args) -> int:
    collection = ReplayDatasetCollection.from_inputs(args.datasets)
    settings, hashes = load_saved_wave_settings(
        args.wave_config,
        delta_limits_path=args.delta_limits,
    )
    overrides = (
        json.loads(Path(args.config).read_text(encoding="utf-8"))
        if args.config
        else {}
    )
    config = ContinuousReplayConfig.from_mapping(
        overrides,
        wave_settings=settings,
        wave_settings_hashes=hashes,
        default_start=args.start,
        default_end=args.end,
    )
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    digests = []
    reports = []
    for index in range(args.repeat):
        report, trace = ContinuousWaveRunner(
            repository_root=args.repository_root,
            collection=collection,
            config=config,
        ).run()
        if not args.compact:
            trace.write_jsonl(
                output
                / (f"continuous-trace-{index + 1}.jsonl" if args.repeat > 1 else "continuous-trace.jsonl")
            )
        digests.append(trace.digest())
        reports.append(report)
    if len(set(digests)) != 1 or len({report["report_hash"] for report in reports}) != 1:
        raise RuntimeError(
            f"Non-deterministic continuous replay: traces={digests}, "
            f"reports={[report['report_hash'] for report in reports]}"
        )
    final_report = reports[-1]
    final_report["determinism"] = {
        "repetitions": args.repeat,
        "trace_digest": digests[0],
        "report_hash": final_report["report_hash"],
    }
    if args.compact:
        report_path = output / "experiment-manifest.json"
        report_path.write_text(
            json.dumps(
                _compact_experiment_manifest(
                    final_report,
                    config,
                    collection,
                    args,
                ),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    else:
        report_path = output / "continuous-report.json"
        report_path.write_text(
            json.dumps(final_report, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    print(
        json.dumps(
            {"report": str(report_path.resolve()), "trace_digest": digests[0]},
            sort_keys=True,
        )
    )
    return 0


def _compact_experiment_manifest(
    report: dict,
    config: ContinuousReplayConfig,
    collection: ReplayDatasetCollection,
    args,
) -> dict:
    forced = report["forced_close_sensitivity"]
    return {
        "schema": "wave-replay-january-experiment-result-v1",
        "experiment_id": config.experiment_id,
        "parameter_hash": state_hash(config.experiment_mapping()),
        "resolved_parameters": config.experiment_mapping(),
        "code_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
        ).strip(),
        "collection_id": collection.to_manifest()["collection_id"],
        "collection_sha256": collection.to_manifest()["collection_sha256"],
        "requested_range": report["requested_range"],
        "boundary": report["final_boundary_mtm"],
        "forced_close": forced,
        "net_pnl": forced["pnl"],
        "fees": forced["total_fees"],
        "max_drawdown": report["drawdown"],
        "fills": report["activity"]["fills"],
        "orders": report["activity"]["orders"],
        "cancels": report["activity"]["cancels"],
        "inventory": report["inventory"],
        "contract_statistics": report["contract_statistics"],
        "trace_digest": report["trace_digest"],
        "report_hash": report["report_hash"],
        "trace_written": not args.compact,
        "acceptance": {
            "forced_close_profitable": (
                forced["pnl"] is not None and float(forced["pnl"]) > 0
            ),
            "minimum_fills": report["activity"]["fills"] >= 10,
            "forced_close_flat": (
                forced.get("available") is True
                and not forced.get("unavailable_symbols")
                and forced.get("unrealized_pnl") == "0"
            ),
        },
    }


def _boundary_probe(repository_root: Path) -> dict:
    start = dt.datetime(2025, 1, 6, 9, 15, tzinfo=IST)
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(start=start, trace=trace)
    tasks = SimTaskRuntime(scheduler, trace)
    clock = SimClock(scheduler)
    clock.bind_task_runtime(tasks)
    engine = BrokerEngine(scheduler=scheduler, trace=trace)
    kite = FakeKite(engine)
    ticker = FakeTicker(scheduler, tasks, trace)
    checks = {}
    with ReplayBootstrap(
        repository_root=repository_root,
        scheduler=scheduler,
        tasks=tasks,
        clock=clock,
        fake_kite=kite,
        fake_ticker=ticker,
        trace=trace,
        strict_thread_inventory=True,
    ) as bootstrap:
        first = bootstrap.load_worker("boundary-one", [])
        second = bootstrap.load_worker("boundary-two", [])
        imported_from_sandbox = str(first.__file__).startswith(str(bootstrap.paths.root))
        first.orders["isolation"] = {}
        checks["globals_isolated"] = second.orders == {}
        checks["broker_is_fake"] = first.kite is kite and second.kite is kite
        checks["database_files"] = sorted(path.name for path in bootstrap.paths.databases.glob("*.db"))
        first.symbol = "BOUNDARYCE"
        status_path = bootstrap.paths.status / f"status_BOUNDARYCE_{os.getpid()}.json"
        status_path.write_text("{}", encoding="utf-8")
        first.delete_own_status_file()
        checks["atexit_cleanup_semantics"] = not status_path.exists()
        ticker_source = (bootstrap.paths.source / "ticker_single_scraper_new.py").read_text(encoding="utf-8")
        checks["signal_handler_present"] = "signal.signal(signal.SIGTERM" in ticker_source
        checks["atexit_handler_present"] = "atexit.register(delete_own_status_file)" in ticker_source
        try:
            socket.getaddrinfo("denied.invalid", 443)
        except UnexpectedNetworkAccess:
            checks["network_denied"] = True
        else:
            checks["network_denied"] = False
        bootstrap.loader.unload_all()
        restarted = bootstrap.load_worker("boundary-restart", [])
        checks["restart_globals_clean"] = restarted.orders == {}
        checks["filesystem_ledger_entries"] = len(bootstrap.filesystem_guard.ledger)
    return {
        "lane": "non_golden_subprocess_boundary",
        "imported_from_sandbox": imported_from_sandbox,
        "checks": checks,
        "trace_digest": trace.digest(),
    }


if __name__ == "__main__":
    sys.exit(main())
