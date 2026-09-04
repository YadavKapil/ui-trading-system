"""Rooted Wave-only reachability and executable coverage manifest."""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import io
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from .datasets import ReplayDataset
from .executable_scenarios import EXECUTABLE_CASES, execute_case
from .legacy_runner import LegacyWaveRunner
from .normalize import state_hash
from .scenario_matrix import DECLARED_TO_EXECUTABLE
from .selector import build_scenarios
from .synthetic import create_synthetic_dataset

ROOTS = {
    "ticker_module",
    "initilise",
    "set_execution_variables",
    "get_instrument_details",
    "place_duo_order",
    "on_order_update",
    "check_orders",
    "check_changes_in_restrictions",
    "set_restrictions",
    "check_gtt_orders",
    "register_gtt_triggered_order",
    "order_complete",
}
REQUIRED_EXTERNALS = {"quote", "positions", "orders", "order_history", "place_order", "cancel_order", "modify_order", "place_gtt", "delete_gtt"}
CALLBACK_ROOTS = {
    "on_connect",
    "on_ticks",
    "on_order_update",
    "_on_noreconnect_handler",
    "_handle_sigterm",
    "delete_own_status_file",
}
CALLBACK_REGISTRARS = {"register", "signal", "Thread", "Timer"}


def build_wave_coverage(repository_root: str | Path, digest_path: str | Path) -> dict[str, Any]:
    root = Path(repository_root)
    common_tree = ast.parse((root / "common_lib.py").read_text(encoding="utf-8"))
    ticker_tree = ast.parse((root / "ticker_single_scraper_new.py").read_text(encoding="utf-8"))
    common_functions = _function_index(common_tree)
    ticker_functions = _function_index(ticker_tree)
    duplicate_names = set(common_functions) & set(ticker_functions)
    if duplicate_names:
        raise ValueError(f"Ambiguous Wave function names: {sorted(duplicate_names)}")
    functions = common_functions | ticker_functions
    function_files = {
        **{name: "common_lib.py" for name in common_functions},
        **{name: "ticker_single_scraper_new.py" for name in ticker_functions},
    }
    ticker_module_nodes = [
        node
        for node in ticker_tree.body
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    ticker_calls = sorted(
        {
            _called_name(call)
            for node in ticker_module_nodes
            for call in ast.walk(node)
            if isinstance(call, ast.Call)
        }
    )
    ticker_seeds = {name for name in ticker_calls if name in functions}
    callback_targets = {name for name in CALLBACK_ROOTS if name in functions}
    for node in ticker_module_nodes:
        for call in (child for child in ast.walk(node) if isinstance(child, ast.Call)):
            ticker_seeds.update(_callback_references(call, functions))
    reachable = {name for name in ROOTS if name in functions} | callback_targets | ticker_seeds
    queue = list(reachable)
    external = set()
    thread_targets = set()
    persistence = set()
    while queue:
        name = queue.pop()
        node = functions[name]
        for assignment in (
            child
            for child in ast.walk(node)
            if isinstance(child, (ast.Assign, ast.AnnAssign))
        ):
            value = assignment.value
            targets = assignment.targets if isinstance(assignment, ast.Assign) else [assignment.target]
            if (
                isinstance(value, ast.Name)
                and value.id in functions
                and any(
                    isinstance(target, ast.Attribute) and target.attr.startswith("on_")
                    for target in targets
                )
            ):
                callback_targets.add(value.id)
                if value.id not in reachable:
                    reachable.add(value.id)
                    queue.append(value.id)
        for call in (child for child in ast.walk(node) if isinstance(child, ast.Call)):
            called = _called_name(call)
            if called in functions and called not in reachable:
                reachable.add(called)
                queue.append(called)
            if called in REQUIRED_EXTERNALS:
                external.add(called)
            if called == "Thread":
                for keyword in call.keywords:
                    if keyword.arg == "target" and isinstance(keyword.value, ast.Name):
                        thread_targets.add(keyword.value.id)
                        if keyword.value.id in functions and keyword.value.id not in reachable:
                            reachable.add(keyword.value.id)
                            queue.append(keyword.value.id)
            for callback in _callback_references(call, functions):
                if callback not in reachable:
                    reachable.add(callback)
                    queue.append(callback)
            if called in {"open", "json.dump", "json.load", "save_executed_order", "write_status_to_file"}:
                persistence.add(called)
    digests = json.loads(Path(digest_path).read_text(encoding="utf-8"))
    mapping = {
        case_id: {
            "executable_id": executable_id,
            "trace_digest": digests[executable_id]["trace_digest"],
        }
        for case_id, executable_id in DECLARED_TO_EXECUTABLE.items()
    }
    uncovered_roots = sorted(ROOTS - (reachable | {"ticker_module"}))
    uncovered_externals = sorted(REQUIRED_EXTERNALS - external)
    missing_executables = sorted(set(DECLARED_TO_EXECUTABLE.values()) - set(EXECUTABLE_CASES))
    missing_goldens = sorted(set(EXECUTABLE_CASES) - set(digests))
    runtime_lines, startup_digest = _runtime_line_evidence(root)
    evidence = [case_id for case_id in EXECUTABLE_CASES if case_id in digests]
    branch_outcomes = {
        name: _branch_outcomes(name, functions[name])
        for name in sorted(reachable)
    }
    branch_outcomes["ticker_module"] = _branch_outcomes(
        "ticker_module",
        ast.Module(body=ticker_module_nodes, type_ignores=[]),
    )
    branch_ids = {
        name: [outcome_id for outcome_id, _header, _lines, _digest in outcomes]
        for name, outcomes in branch_outcomes.items()
    }
    function_coverage = {
        name: _scenarios_for_range(
            runtime_lines,
            function_files[name],
            node.lineno + 1,
            node.end_lineno,
        )
        for name, node in functions.items()
        if name in reachable
    }
    function_coverage["ticker_module"] = ["nifty_range_startup"]
    branch_coverage = {
        outcome_id: _scenarios_for_branch_outcome(
            runtime_lines,
            "ticker_single_scraper_new.py" if name == "ticker_module" else function_files[name],
            header,
            outcome_lines,
        )
        for name, outcomes in branch_outcomes.items()
        for outcome_id, header, outcome_lines, _digest in outcomes
    }
    branch_source_hashes = {
        outcome_id: digest
        for outcomes in branch_outcomes.values()
        for outcome_id, _header, _lines, digest in outcomes
    }
    function_source_hashes = {
        name: state_hash(ast.dump(functions[name], include_attributes=False))
        for name in function_coverage
        if name != "ticker_module"
    }
    function_source_hashes["ticker_module"] = state_hash(
        ast.dump(ast.Module(body=ticker_module_nodes, type_ignores=[]), include_attributes=False)
    )
    waiver_document = json.loads(
        (root / "wave_replay" / "coverage_waivers.json").read_text(encoding="utf-8")
    )
    function_waivers = _waiver_hashes(waiver_document["function_waivers"])
    branch_waivers = _waiver_hashes(waiver_document["branch_waivers"])
    manifest = {
        "scope": "wave-reachable-stage-1-10",
        "roots": sorted(ROOTS),
        "reachable_functions": sorted(reachable),
        "branch_ids": branch_ids,
        "function_coverage": function_coverage,
        "branch_coverage": branch_coverage,
        "function_waivers": function_waivers,
        "branch_waivers": branch_waivers,
        "source_hashes": {
            "functions": function_source_hashes,
            "branch_outcomes": branch_source_hashes,
        },
        "executable_goldens": {
            case_id: digests[case_id]["trace_digest"]
            for case_id in evidence
        } | {"nifty_range_startup": startup_digest},
        "ticker_calls": ticker_calls,
        "callback_roots": sorted(callback_targets),
        "thread_targets": sorted(thread_targets),
        "persistence_obligations": sorted(persistence),
        "external_obligations": sorted(external),
        "scenario_mapping": mapping,
        "waivers": {
            "ohlc_touch_branched": "Deferred by approved close_only scope",
        },
        "uncovered_required": {
            "roots": uncovered_roots,
            "external_obligations": uncovered_externals,
            "executable_mappings": missing_executables,
            "golden_digests": missing_goldens,
            "functions": sorted(
                name
                for name in function_coverage
                if not function_coverage[name] and name not in function_waivers
            ),
            "branches": sorted(
                branch_id
                for branch_id in branch_coverage
                if not branch_coverage[branch_id] and branch_id not in branch_waivers
            ),
            "stale_waivers": sorted(
                (set(function_waivers) - set(function_coverage))
                | (set(branch_waivers) - set(branch_coverage))
            ),
            "waiver_source_mismatches": sorted(
                [
                    name
                    for name, digest in function_waivers.items()
                    if function_source_hashes.get(name) != digest
                ]
                + [
                    branch_id
                    for branch_id, digest in branch_waivers.items()
                    if branch_source_hashes.get(branch_id) != digest
                ]
            ),
            "thread_targets": sorted(thread_targets - reachable),
        },
    }
    manifest["manifest_hash"] = state_hash(manifest)
    return manifest


def assert_wave_coverage_complete(manifest: dict[str, Any]) -> None:
    uncovered = manifest["uncovered_required"]
    failures = {key: value for key, value in uncovered.items() if value}
    if failures:
        raise AssertionError(f"Uncovered Wave requirements: {failures}")


def function_inventory(path: str | Path) -> list[dict[str, object]]:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    return [
        {"name": node.name, "line": node.lineno, "end_line": node.end_lineno}
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    ]


def _function_index(tree: ast.AST) -> dict[str, ast.FunctionDef]:
    nodes = sorted(
        (
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        ),
        key=lambda node: node.lineno,
    )
    return {node.name: node for node in nodes}


def _waiver_hashes(value: dict[str, str] | list[str]) -> dict[str, str]:
    if isinstance(value, dict):
        return value
    return {identifier: "" for identifier in value}


def _called_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _callback_references(call: ast.Call, functions: dict[str, ast.FunctionDef]) -> set[str]:
    if _called_name(call) not in CALLBACK_REGISTRARS:
        return set()
    references = {
        value.id
        for value in call.args
        if isinstance(value, ast.Name) and value.id in functions
    }
    references.update(
        keyword.value.id
        for keyword in call.keywords
        if isinstance(keyword.value, ast.Name) and keyword.value.id in functions
    )
    return references


def _branch_outcomes(
    owner: str,
    tree: ast.AST,
) -> list[tuple[str, int, set[int], str]]:
    outcomes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            outcomes.extend(_body_outcomes(owner, node, "if", node.body, node.orelse))
        elif isinstance(node, (ast.For, ast.While)):
            outcomes.extend(_body_outcomes(owner, node, node.__class__.__name__.lower(), node.body, node.orelse))
        elif isinstance(node, ast.Try):
            outcomes.append(_outcome(owner, node, "try:body", node.body))
            for index, handler in enumerate(node.handlers, 1):
                outcomes.append(_outcome(owner, node, f"try:except:{index}", handler.body))
            if node.orelse:
                outcomes.append(_outcome(owner, node, "try:else", node.orelse))
            if node.finalbody:
                outcomes.append(_outcome(owner, node, "try:finally", node.finalbody))
        elif isinstance(node, ast.Match):
            for index, case in enumerate(node.cases, 1):
                outcomes.append(_outcome(owner, node, f"match:case:{index}", case.body))
    return outcomes


def _body_outcomes(
    owner: str,
    node: ast.If | ast.For | ast.While,
    kind: str,
    body: list[ast.stmt],
    alternate: list[ast.stmt],
) -> list[tuple[str, int, set[int], str]]:
    return [
        _outcome(owner, node, f"{kind}:body", body),
        _outcome(owner, node, f"{kind}:alternate", alternate),
    ]


def _outcome(
    owner: str,
    node: ast.AST,
    outcome: str,
    statements: list[ast.stmt],
) -> tuple[str, int, set[int], str]:
    identifier = f"{owner}:{outcome}:{node.lineno}"
    digest = state_hash(
        {
            "outcome": outcome,
            "source": ast.dump(node, include_attributes=False),
        }
    )
    return identifier, node.lineno, _statement_lines(statements), digest


def _statement_lines(statements: list[ast.stmt]) -> set[int]:
    return {
        child.lineno
        for statement in statements
        for child in ast.walk(statement)
        if hasattr(child, "lineno")
    }


def _scenarios_for_branch_outcome(
    runtime_lines: dict[str, dict[str, set[int]]],
    filename: str,
    header: int,
    outcome_lines: set[int],
) -> list[str]:
    covered = []
    for case_id, by_file in runtime_lines.items():
        executed = by_file.get(filename, set())
        hit = bool(outcome_lines & executed)
        if hit:
            covered.append(case_id)
    return sorted(covered)


def _runtime_line_evidence(root: Path) -> tuple[dict[str, dict[str, set[int]]], str]:
    evidence: dict[str, dict[str, set[int]]] = {}

    def execute(case_id: str, operation) -> Any:
        lines = {"common_lib.py": set(), "ticker_single_scraper_new.py": set()}

        def tracer(frame, event, _arg):
            if event == "line":
                filename = Path(frame.f_code.co_filename).name
                if filename in lines:
                    lines[filename].add(frame.f_lineno)
            return tracer

        previous = sys.gettrace()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            try:
                sys.settrace(tracer)
                result = operation()
            finally:
                sys.settrace(previous)
        evidence[case_id] = lines
        return result

    for case_id in EXECUTABLE_CASES:
        execute(case_id, lambda case_id=case_id: execute_case(case_id))

    with tempfile.TemporaryDirectory(prefix="wave-coverage-") as temporary:
        dataset = ReplayDataset.load(create_synthetic_dataset(Path(temporary) / "dataset"))
        scenario = next(item for item in build_scenarios(dataset) if item.role == "range")
        scenario = dataclasses.replace(
            scenario,
            replay={**scenario.replay, "end_at": f"{scenario.selected_date.isoformat()}T09:17:00+05:30"},
        )
        _report, startup_trace = execute(
            "nifty_range_startup",
            lambda: LegacyWaveRunner(repository_root=root, dataset=dataset, scenario=scenario).run(),
        )
    return evidence, startup_trace.digest()


def _scenarios_for_range(
    runtime_lines: dict[str, dict[str, set[int]]],
    filename: str,
    first_line: int,
    last_line: int | None,
) -> list[str]:
    end = first_line if last_line is None else last_line
    return sorted(
        case_id
        for case_id, by_file in runtime_lines.items()
        if any(first_line <= line <= end for line in by_file.get(filename, set()))
    )
