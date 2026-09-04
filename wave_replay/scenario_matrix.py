"""Required scripted characterization matrix beyond the flat historical corpus."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CharacterizationCase:
    case_id: str
    category: str
    purpose: str


CHARACTERIZATION_MATRIX = (
    CharacterizationCase("neutral-duo", "position", "Flat account places linked regular BUY/SELL orders"),
    CharacterizationCase("long-buy-restriction", "position", "Positive option position suppresses additional BUY"),
    CharacterizationCase("short-step-scale", "position", "Short position selects the negative step multiplier"),
    CharacterizationCase("hard-delta-buy", "restriction", "Positive expiry delta hard-restricts the risk-adding side"),
    CharacterizationCase("hard-delta-sell", "restriction", "Negative expiry delta hard-restricts the risk-adding side"),
    CharacterizationCase("velocity-up", "velocity", "Upward spot velocity widens the configured CE side"),
    CharacterizationCase("velocity-down", "velocity", "Downward spot velocity widens the configured CE side"),
    CharacterizationCase("same-side-cooldown", "fill_cooldown", "Rapid same-side fills enable the existing cooldown"),
    CharacterizationCase("sibling-cancel-race", "cancellation", "Fill and sibling cancellation race"),
    CharacterizationCase("manual-reprice", "repricing", "Manual override is not auto-repriced"),
    CharacterizationCase("auto-reprice", "repricing", "Multiplier changes modify live regular orders"),
    CharacterizationCase("gtt-complete-before-open", "gtt", "GTT child COMPLETE arrives before OPEN"),
    CharacterizationCase("partial-then-reject", "order_lifecycle", "Partial fill followed by business reject"),
    CharacterizationCase("callback-poll-same-time", "concurrency", "Callback and polling visibility share a timestamp"),
    CharacterizationCase("rest-lag", "visibility", "Callback precedes order and position REST visibility"),
    CharacterizationCase("restart-with-open-orders", "restart", "Fresh worker sees broker orders from a crashed worker"),
    CharacterizationCase("market-close-exit", "market_hours", "No replacement/GTT is placed after close"),
    CharacterizationCase("two-workers-shared-broker", "multi_instance", "Distinct worker globals share account broker truth"),
)

DECLARED_TO_EXECUTABLE = {
    "neutral-duo": "neutral_cycle",
    "long-buy-restriction": "long_buy_restriction",
    "short-step-scale": "step_delta_velocity_fill_cooldown",
    "hard-delta-buy": "hard_delta_restriction",
    "hard-delta-sell": "hard_delta_restriction",
    "velocity-up": "step_delta_velocity_fill_cooldown",
    "velocity-down": "velocity_down",
    "same-side-cooldown": "step_delta_velocity_fill_cooldown",
    "sibling-cancel-race": "sibling_cancellation",
    "manual-reprice": "manual_reprice",
    "auto-reprice": "auto_reprice",
    "gtt-complete-before-open": "gtt_complete_before_open",
    "partial-then-reject": "partial_then_reject",
    "callback-poll-same-time": "callback_poll_race",
    "rest-lag": "rest_lag",
    "restart-with-open-orders": "restart_recovery",
    "market-close-exit": "market_close",
    "two-workers-shared-broker": "two_workers_shared_broker",
}


def matrix_by_category() -> dict[str, tuple[CharacterizationCase, ...]]:
    categories: dict[str, list[CharacterizationCase]] = {}
    for case in CHARACTERIZATION_MATRIX:
        categories.setdefault(case.category, []).append(case)
    return {category: tuple(cases) for category, cases in categories.items()}
