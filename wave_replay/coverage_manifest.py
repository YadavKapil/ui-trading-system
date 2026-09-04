"""Static lower-bound reachability inventory for the legacy Wave path."""

from __future__ import annotations

import ast
from pathlib import Path


def function_inventory(path: str | Path) -> list[dict[str, object]]:
    path = Path(path)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        {
            "name": node.name,
            "line": node.lineno,
            "end_line": node.end_lineno,
            "branches": sum(isinstance(child, (ast.If, ast.Try, ast.For, ast.While, ast.Match)) for child in ast.walk(node)),
        }
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]

