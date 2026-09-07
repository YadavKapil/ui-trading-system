"""Explicit crude capital reporting for naked option replay experiments."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

MODEL_ID = "crude_naked_option_margin_v1"
DISCLAIMER = "not Zerodha/NSE SPAN; no cross-contract offsets"
NAKED_MARGIN_RATE = Decimal("0.10")
RECOMMENDED_BUFFER = Decimal("1.10")


def calculate_capital_snapshot(
    *,
    at: str,
    positions: dict[str, dict[str, Any]],
    orders: Iterable[Any],
    option_marks: dict[str, Decimal],
    underlying_spot: Decimal | None,
) -> dict[str, Any]:
    pending: dict[str, dict[str, Any]] = {}
    for order in orders:
        status = str(_value(order, "status", "")).upper()
        if status not in {"ACK", "OPEN", "PARTIAL"}:
            continue
        symbol = str(_value(order, "symbol", _value(order, "tradingsymbol", "")))
        side = str(_value(order, "side", _value(order, "transaction_type", ""))).upper()
        remaining = int(_value(order, "remaining", _value(order, "pending_quantity", 0)))
        price = Decimal(str(_value(order, "price", 0)))
        item = pending.setdefault(
            symbol,
            {
                "pending_buy_qty": 0,
                "pending_sell_qty": 0,
                "pending_buy_premium_reserve": Decimal("0"),
            },
        )
        if side == "BUY":
            item["pending_buy_qty"] += remaining
            item["pending_buy_premium_reserve"] += price * remaining
        elif side == "SELL":
            item["pending_sell_qty"] += remaining

    symbols = sorted(set(positions) | set(pending))
    decomposition = []
    missing_marks = []
    excluding = Decimal("0")
    projected_short_total = 0
    for symbol in symbols:
        position = positions.get(symbol, {})
        filled_buy = int(position.get("buy_quantity", 0))
        filled_sell = int(position.get("sell_quantity", 0))
        net = filled_buy - filled_sell
        net_long = max(net, 0)
        pending_item = pending.get(
            symbol,
            {
                "pending_buy_qty": 0,
                "pending_sell_qty": 0,
                "pending_buy_premium_reserve": Decimal("0"),
            },
        )
        projected_short = max(
            filled_sell
            - filled_buy
            + int(pending_item["pending_sell_qty"]),
            0,
        )
        mark = option_marks.get(symbol)
        if net_long and mark is None:
            missing_marks.append(symbol)
        filled_long_premium = None if net_long and mark is None else Decimal(net_long) * (mark or Decimal("0"))
        pending_buy_reserve = Decimal(pending_item["pending_buy_premium_reserve"])
        if filled_long_premium is not None:
            excluding += filled_long_premium + pending_buy_reserve
        projected_short_total += projected_short
        decomposition.append({
            "symbol": symbol,
            "filled_buy_qty": filled_buy,
            "filled_sell_qty": filled_sell,
            "filled_net_qty": net,
            "filled_net_long_qty": net_long,
            "pending_buy_qty": int(pending_item["pending_buy_qty"]),
            "pending_sell_qty": int(pending_item["pending_sell_qty"]),
            "conservative_projected_short_qty": projected_short,
            "current_option_mark": None if mark is None else str(mark),
            "filled_net_long_premium_capital": (
                None if filled_long_premium is None else str(filled_long_premium)
            ),
            "pending_buy_premium_reserve": str(pending_buy_reserve),
        })

    excluding_available = not missing_marks
    margin_available = projected_short_total == 0 or underlying_spot is not None
    naked_margin = (
        None
        if not margin_available
        else NAKED_MARGIN_RATE
        * (underlying_spot or Decimal("0"))
        * projected_short_total
    )
    including = (
        excluding + naked_margin
        if excluding_available and naked_margin is not None
        else None
    )
    recommended = (
        including * RECOMMENDED_BUFFER if including is not None else None
    )
    return {
        "at": at,
        "model_id": MODEL_ID,
        "disclaimer": DISCLAIMER,
        "broker_margin_exact": False,
        "bounded_risk": False,
        "formula": {
            "capital_used_excluding_margin": (
                "sum(filled_net_long_qty * current_option_mark) + "
                "sum(pending_buy_qty * buy_limit)"
            ),
            "estimated_naked_margin": (
                "0.10 * current_underlying_spot * "
                "sum(conservative_projected_short_qty)"
            ),
            "capital_used_including_margin": (
                "capital_used_excluding_margin + estimated_naked_margin"
            ),
            "capital_recommended": "1.10 * capital_used_including_margin",
        },
        "underlying_spot": (
            None if underlying_spot is None else str(underlying_spot)
        ),
        "gross_filled_net_qty": sum(
            abs(int(item["filled_net_qty"])) for item in decomposition
        ),
        "net_filled_qty": sum(
            int(item["filled_net_qty"]) for item in decomposition
        ),
        "projected_short_qty": projected_short_total,
        "capital_used_excluding_margin": (
            str(excluding) if excluding_available else None
        ),
        "estimated_naked_margin": (
            None if naked_margin is None else str(naked_margin)
        ),
        "capital_used_including_margin": (
            None if including is None else str(including)
        ),
        "capital_recommended": (
            None if recommended is None else str(recommended)
        ),
        "metric_available": including is not None,
        "missing_option_marks": missing_marks,
        "missing_underlying_spot": not margin_available,
        "decomposition": decomposition,
    }


def calculate_capital_peaks(points: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = (
        "capital_used_excluding_margin",
        "capital_used_including_margin",
        "capital_recommended",
    )
    peaks: dict[str, Any] = {}
    coverage: dict[str, Any] = {}
    for metric in metrics:
        missing_at = [
            point["at"] for point in points if point.get(metric) is None
        ]
        coverage[metric] = {
            "complete": not missing_at,
            "missing_observation_count": len(missing_at),
            "first_missing_at": missing_at[0] if missing_at else None,
        }
        if missing_at:
            peaks[metric] = None
            continue
        available = [point for point in points if point.get(metric) is not None]
        if not available:
            peaks[metric] = None
            continue
        peak = max(available, key=lambda point: Decimal(str(point[metric])))
        peaks[metric] = {
            "value": peak[metric],
            "at": peak["at"],
            "underlying_spot": peak["underlying_spot"],
            "gross_filled_net_qty": peak["gross_filled_net_qty"],
            "net_filled_qty": peak["net_filled_qty"],
            "projected_short_qty": peak["projected_short_qty"],
            "decomposition": peak["decomposition"],
        }
    peaks["coverage"] = coverage
    return peaks


def calculate_profit_on_peak_capital(
    *,
    monthly_profit: Decimal,
    peaks: dict[str, Any],
    sample_classification: str = "in_sample",
) -> dict[str, Any]:
    if sample_classification not in {"in_sample", "out_of_sample"}:
        raise ValueError("Unsupported sample classification")

    def percentage(metric: str) -> str | None:
        peak = peaks.get(metric)
        if peak is None:
            return None
        denominator = Decimal(str(peak["value"]))
        if denominator == 0:
            return None
        return str(monthly_profit / denominator * Decimal("100"))

    excluding = percentage("capital_used_excluding_margin")
    including = percentage("capital_used_including_margin")
    return {
        "classification": f"{sample_classification}_linear_annualized",
        "annualization": (
            "simple monthly percentage multiplied by 12; not CAGR or compounding"
        ),
        "monthly_profit": str(monthly_profit),
        "monthly_profit_pct_excluding_margin": excluding,
        "monthly_profit_pct_including_margin": including,
        "annualized_profit_pct_excluding_margin": (
            None
            if excluding is None
            else str(Decimal(excluding) * Decimal("12"))
        ),
        "annualized_profit_pct_including_margin": (
            None
            if including is None
            else str(Decimal(including) * Decimal("12"))
        ),
    }


def _value(obj: Any, name: str, default: Any) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)
