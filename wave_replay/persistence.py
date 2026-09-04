"""Scenario-owned filesystem and SQLite state."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SandboxPaths:
    root: Path
    source: Path
    status: Path
    logs: Path
    output: Path
    databases: Path
    temp: Path
    home: Path

    @classmethod
    def create(cls, root: Path) -> "SandboxPaths":
        paths = cls(
            root=root,
            source=root / "source",
            status=root / "status",
            logs=root / "logs",
            output=root / "output",
            databases=root / "databases",
            temp=root / "tmp",
            home=root / "home",
        )
        for path in (
            paths.source,
            paths.status,
            paths.logs,
            paths.output,
            paths.databases,
            paths.temp,
            paths.home,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return paths

    def assert_inside(self, path: str | Path) -> Path:
        resolved = Path(path).resolve()
        try:
            resolved.relative_to(self.root.resolve())
        except ValueError as exc:
            raise PermissionError(f"Replay path escaped sandbox: {resolved}") from exc
        return resolved

    def initialize_databases(self) -> dict[str, Path]:
        result = {}
        for name in ("instruments.db", "api_monitor.db", "trade_state.db"):
            path = self.databases / name
            connection = sqlite3.connect(path)
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("CREATE TABLE IF NOT EXISTS replay_preflight (id INTEGER PRIMARY KEY)")
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                connection.close()
            result[name] = path
        return result
