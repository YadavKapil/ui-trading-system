"""Non-golden fresh-process boundary probe."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def run_boundary_probe(repository_root: str | Path) -> dict:
    command = [
        sys.executable,
        "-m",
        "wave_replay.cli",
        "boundary-probe",
        "--repository-root",
        str(Path(repository_root).resolve()),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
    return json.loads(completed.stdout)
