"""Canonical trace comparison and deliberate golden approval."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TraceDifference:
    equal: bool
    line: int | None = None
    expected: str | None = None
    actual: str | None = None


def compare_jsonl(expected: str | Path, actual: str | Path) -> TraceDifference:
    expected_lines = Path(expected).read_text(encoding="utf-8").splitlines()
    actual_lines = Path(actual).read_text(encoding="utf-8").splitlines()
    for index in range(max(len(expected_lines), len(actual_lines))):
        left = expected_lines[index] if index < len(expected_lines) else None
        right = actual_lines[index] if index < len(actual_lines) else None
        if left != right:
            return TraceDifference(False, index + 1, left, right)
    return TraceDifference(True)


def approve_golden(actual: str | Path, golden: str | Path, *, explicit: bool) -> Path:
    if not explicit:
        raise PermissionError("Golden approval requires explicit=True")
    golden = Path(golden)
    golden.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(actual, golden)
    return golden
