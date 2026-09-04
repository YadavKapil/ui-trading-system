"""Canonical branch deduplication for optional OHLC sensitivity runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .normalize import state_hash


@dataclass
class ReplayBranch:
    branch_id: str
    state: dict[str, Any]
    labels: tuple[str, ...]

    @property
    def digest(self) -> str:
        return state_hash(self.state)


class BranchSet:
    def __init__(self, *, budget: int = 256) -> None:
        self.budget = budget
        self.branches: list[ReplayBranch] = []
        self.pruned = 0

    def add(self, branch: ReplayBranch) -> ReplayBranch:
        for existing in self.branches:
            if existing.digest == branch.digest:
                existing.labels = tuple(sorted(set(existing.labels + branch.labels)))
                return existing
        self.branches.append(branch)
        self.branches.sort(key=lambda item: (item.digest, item.branch_id))
        if len(self.branches) > self.budget:
            self.branches.pop()
            self.pruned += 1
        return branch

