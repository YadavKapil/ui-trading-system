"""Approved close-only and optional OHLC-touch fill decisions."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FillDecision:
    touched: bool
    fill_price: Decimal | None
    reason: str


def close_only_fill(
    *,
    side: str,
    limit_price: Decimal,
    close: Decimal,
    created_at_bar_end,
    current_bar_end,
    fill_price_mode: str = "limit",
) -> FillDecision:
    if current_bar_end <= created_at_bar_end:
        return FillDecision(False, None, "activation_requires_subsequent_bar")
    normalized = side.upper()
    touched = close <= limit_price if normalized == "BUY" else close >= limit_price
    if not touched:
        return FillDecision(False, None, "close_did_not_cross")
    if fill_price_mode == "limit":
        price = limit_price
    elif fill_price_mode == "close_price":
        price = min(close, limit_price) if normalized == "BUY" else max(close, limit_price)
    else:
        raise ValueError(f"Unsupported fill_price_mode: {fill_price_mode}")
    if normalized == "BUY" and price > limit_price:
        raise AssertionError("BUY limit fill cannot be worse than its limit")
    if normalized == "SELL" and price < limit_price:
        raise AssertionError("SELL limit fill cannot be worse than its limit")
    return FillDecision(True, price, f"close_only_{fill_price_mode}")


def ohlc_touch(*, side: str, limit_price: Decimal, high: Decimal, low: Decimal) -> bool:
    return low <= limit_price if side.upper() == "BUY" else high >= limit_price


def validate_sibling_bracket(buy_limit: Decimal, sell_limit: Decimal) -> None:
    if buy_limit >= sell_limit:
        raise ValueError("INVALID_OVERLAPPING_BRACKET")

