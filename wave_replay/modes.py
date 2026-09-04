"""Replay mode constants and approved initial defaults."""

from __future__ import annotations

from dataclasses import dataclass


DATASET_SCHEMA = "wave-replay-dataset-v1"
INITIAL_PROFILE = "nifty-atm-ce-close-only-v1"


@dataclass(frozen=True)
class ApprovedDefaults:
    market_resolution: str = "1minute"
    underlying: str = "NIFTY"
    option_right: str = "call"
    min_calendar_dte: int = 5
    max_calendar_dte: int = 20
    buy_gap_pct: float = 0.10
    sell_gap_pct: float = 0.10
    lots: int = 1
    starting_cash_inr: int = 1_000_000
    cool_off_mode: str = "disabled"
    trigger_mode: str = "close_only"
    activation_policy: str = "subsequent_bar"
    fill_price_mode: str = "limit"
    order_mode: str = "regular"
    broker_fee_model: str = "upstox_approximation"
    equivalence_status: str = "modeled_not_exact"


APPROVED_DEFAULTS = ApprovedDefaults()
