"""Approximate NIFTY 50 index reconstruction from CAS constituent prices.

NIFTY 50 is free-float market-cap weighted, so its return over any interval
equals the weight-averaged return of its constituents:

    NIFTY(t) ~= NIFTY(anchor) * (1 + sum(weight_i * (price_i(t) - ref_i) / ref_i))

`price_i(t)` and `ref_i` are read from whatever the live CAS payload calls
its auction/indicative price and its pre-auction reference price. The exact
field names are not published (CAS is a brand-new NSE feature as of
2026-08-03), so this module detects them heuristically from whatever keys
are actually present in a row, rather than hardcoding names that might not
match on the first live tick.
"""

import logging
from typing import Any, Optional

from cas_tracker.nifty50_weights import NIFTY50_WEIGHTS_PCT

logger = logging.getLogger(__name__)

_SYMBOL_KEY_HINTS = ["symbol"]
_PRICE_KEY_HINTS = ["iep", "indicativeprice", "finalprice", "auctionprice"]
# `refrencePrice` (sic, NSE's own typo) looked like a promising per-stock
# reference — it's already populated the moment the CAS window opens,
# before continuous trading even ends — but confirmed 2026-08-05 to NOT be
# a point price at the CAS cutoff: pairing it with NIFTY's own point value
# at that cutoff (24570.2) overstated the session's final value by ~39
# points (24663 vs the actual ~24624). It's most likely a VWAP over some
# window before the cutoff (e.g. 15:00-15:15), not a snapshot — a
# different quantity than what `stock_reference_prices` below supplies, so
# mixing the two silently double-counts or cancels part of the move.
# `prevClose` is yesterday's close (reads 0 for the first few minutes of
# the window until NSE populates it) — the wrong reference day entirely
# for an anchor pinned to today's CAS cutoff.
# Both are kept only as a last-resort fallback for when the precise
# per-stock reference isn't available yet (see estimate_nifty_from_cas).
_REF_KEY_HINTS = ["refrenceprice", "referenceprice", "refprice", "previousclose", "prevclose"]


def _normalize(key: str) -> str:
    """Lowercase a dict key and strip separators for loose matching."""
    return key.lower().replace("_", "").replace(" ", "")


def _find_key(row: dict[str, Any], hints: list[str]) -> Optional[str]:
    """Find the first key in `row` whose normalized form matches a hint.

    Args:
        row: A single CAS data row.
        hints: Candidate normalized substrings, checked in priority order.

    Returns:
        The original (non-normalized) key name, or None if no hint matched.
    """
    normalized = {_normalize(k): k for k in row.keys()}
    for hint in hints:
        for norm_key, original_key in normalized.items():
            if hint in norm_key:
                return original_key
    return None


def detect_price_fields(sample_row: dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Heuristically detect the symbol, reference-price, and live-price keys.

    Args:
        sample_row: One row from the CAS API's `data` array.

    Returns:
        A (symbol_key, ref_price_key, price_key) tuple; any element is None
        if no matching field could be found.
    """
    return (
        _find_key(sample_row, _SYMBOL_KEY_HINTS),
        _find_key(sample_row, _REF_KEY_HINTS),
        _find_key(sample_row, _PRICE_KEY_HINTS),
    )


def extract_cas_digest(rows: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Reduce a raw CAS payload to only the numbers the charts actually need.

    A single raw snapshot is ~150 KB of NSE JSON, and /api/candles re-parses
    every snapshot for the date on each rebuild (~23 MB for one session).
    Almost all of that is fields nothing reads. This keeps four numbers per
    NIFTY 50 constituent, which is ~100x smaller.

    Deliberately does NOT fold in the anchors: they're captured
    asynchronously and can arrive after a snapshot is written, so a stored
    estimate would silently go stale. Everything here is anchor-independent,
    so applying anchors later still produces the correct result.

    Args:
        rows: The `data` array from the CAS API response.

    Returns:
        A dict with the detected field names and a 'rows' list of
        [symbol, price, ref_price, traded_qty] entries (ref_price may be
        None), or None if the payload is empty or its fields are
        undetectable.
    """
    if not rows:
        return None

    symbol_key, fallback_ref_price_key, price_key = detect_price_fields(rows[0])
    if not symbol_key or not price_key:
        # Field names are still reported so callers can log what was seen;
        # estimate_nifty_from_digest() applies the usability guard.
        return {
            "symbol_key": symbol_key,
            "ref_price_key": fallback_ref_price_key,
            "price_key": price_key,
            "rows": [],
        }

    digest_rows: list[list[Any]] = []
    for row in rows:
        symbol = row.get(symbol_key)
        if symbol not in NIFTY50_WEIGHTS_PCT:
            continue
        try:
            current_price = float(row.get(price_key))
        except (TypeError, ValueError):
            # Unusable price — the row could never contribute, with or
            # without anchors, so drop it rather than store a hole.
            continue

        ref_price: Optional[float] = None
        if fallback_ref_price_key is not None:
            try:
                ref_price = float(row.get(fallback_ref_price_key))
            except (TypeError, ValueError):
                ref_price = None

        traded_qty = 0.0
        try:
            traded_qty = float(row.get("totTradedQty", 0))
        except (TypeError, ValueError):
            traded_qty = 0.0

        digest_rows.append([symbol, current_price, ref_price, traded_qty])

    return {
        "symbol_key": symbol_key,
        "ref_price_key": fallback_ref_price_key,
        "price_key": price_key,
        "rows": digest_rows,
    }


def estimate_nifty_from_digest(
    digest: Optional[dict[str, Any]],
    nifty_anchor: Optional[float],
    stock_reference_prices: Optional[dict[str, float]] = None,
) -> dict[str, Any]:
    """Reconstruct NIFTY from a digest produced by extract_cas_digest().

    This is the shared core of the reconstruction —
    estimate_nifty_from_cas() extracts a digest and delegates here — so the
    stored-digest path and the raw-payload path can never drift apart.

    Args:
        digest: Output of extract_cas_digest(), or None.
        nifty_anchor: NIFTY 50's own value at the reference moment. If None,
            no absolute estimate is produced.
        stock_reference_prices: Per-symbol price at that same moment. Falls
            back to each row's own reference price where absent.

    Returns:
        The same shape estimate_nifty_from_cas() returns.
    """
    total_constituents = len(NIFTY50_WEIGHTS_PCT)
    result: dict[str, Any] = {
        "nifty_estimate": None,
        "weighted_return_pct": None,
        "matched_count": 0,
        "total_constituents": total_constituents,
        "coverage_pct": 0.0,
        "symbol_key": None,
        "ref_price_key": None,
        "price_key": None,
    }

    if not digest:
        return result

    symbol_key = digest.get("symbol_key")
    fallback_ref_price_key = digest.get("ref_price_key")
    price_key = digest.get("price_key")

    result["symbol_key"] = symbol_key
    result["ref_price_key"] = (
        "stock_reference_prices" if stock_reference_prices else fallback_ref_price_key
    )
    result["price_key"] = price_key

    # No symbol/price fields, or no reference of any kind, means there is
    # nothing meaningful to compute. Returning early leaves
    # weighted_return_pct as None rather than a misleading 0.0, which
    # callers rely on to skip the tick entirely.
    if not symbol_key or not price_key or (not stock_reference_prices and not fallback_ref_price_key):
        return result

    weighted_return = 0.0

    for symbol, current_price, fallback_ref_price, _traded_qty in digest.get("rows", []):
        weight_pct = NIFTY50_WEIGHTS_PCT.get(symbol)
        if weight_pct is None:
            continue

        ref_price = (stock_reference_prices or {}).get(symbol)
        ref_price = float(ref_price) if ref_price is not None else fallback_ref_price

        if not ref_price or current_price == 0:
            # current_price (IEP) is 0 before the auction establishes an
            # equilibrium price for that stock (order book not yet primed) —
            # this is "no price yet", not a real price of zero. Treating it
            # as a real price makes the stock's return look like -100%.
            continue

        stock_return = (current_price - ref_price) / ref_price
        weighted_return += (weight_pct / 100.0) * stock_return
        result["matched_count"] += 1

    result["coverage_pct"] = round(100.0 * result["matched_count"] / total_constituents, 1)
    result["weighted_return_pct"] = round(weighted_return * 100.0, 4)

    if nifty_anchor is not None:
        # Computed even when matched_count is 0 (before any constituent has
        # an IEP yet): weighted_return is 0.0 in that case, so this
        # correctly evaluates to the anchor itself — i.e. "no move yet" in
        # absolute terms — rather than leaving nifty_estimate as None, which
        # previously made callers fall back to the raw percentage figure
        # (~0.0) as if it were an absolute NIFTY level, badly skewing charts
        # that mix absolute and percentage scales.
        result["nifty_estimate"] = round(nifty_anchor * (1 + weighted_return), 2)

    return result


def estimate_nifty_from_cas(
    rows: list[dict[str, Any]],
    nifty_anchor: Optional[float],
    stock_reference_prices: Optional[dict[str, float]] = None,
) -> dict[str, Any]:
    """Reconstruct an approximate NIFTY 50 value from live CAS auction rows.

    Args:
        rows: The `data` array from the CAS API response.
        nifty_anchor: NIFTY 50's own value at the same reference moment as
            `stock_reference_prices` (e.g. its historical 1-min candle
            close at the CAS cutoff). If None, no absolute estimate can be
            produced.
        stock_reference_prices: Per-symbol price at that exact same
            reference moment (see blueprint.py's
            _get_nifty50_stock_anchors()), so each stock's return is
            measured against the identical instant as `nifty_anchor` —
            required for the reconstruction to be internally consistent.
            Falls back to whatever reference field the CAS payload itself
            supplies (see _REF_KEY_HINTS) for any symbol missing from this
            dict, or entirely if the dict is empty/not yet populated —
            an approximation, since that field is not confirmed to be a
            point price at the same moment.

    Returns:
        A dict with 'nifty_estimate' (float or None), 'weighted_return_pct',
        'matched_count', 'total_constituents', 'coverage_pct', and the
        detected field names used, for transparency/debugging.
    """
    digest = extract_cas_digest(rows)

    if digest is None and rows:
        symbol_key, fallback_ref_price_key, price_key = detect_price_fields(rows[0])
        logger.warning(
            "Could not detect CAS row fields (symbol=%s, ref_price=%s, price=%s) from keys %s",
            symbol_key, fallback_ref_price_key, price_key, list(rows[0].keys()),
        )

    return estimate_nifty_from_digest(digest, nifty_anchor, stock_reference_prices)


def estimate_nifty_from_live_quotes(quotes: dict[str, Any], nifty_prev_close: Optional[float]) -> dict[str, Any]:
    """Reconstruct NIFTY from constituents' regular live Kite quotes.

    Unlike estimate_nifty_from_cas(), this works any time the market is
    open (not just during the CAS window) since it reads each stock's
    ordinary last_price and previous close rather than CAS-specific
    fields. It exists to validate that the weights/formula themselves are
    correct, independent of the CAS API's unconfirmed schema.

    Args:
        quotes: Raw response from kite.quote([...]), keyed by "NSE:<symbol>".
        nifty_prev_close: NIFTY 50's own previous close, to compound the
            weighted return onto. If None, only the relative return and
            per-stock breakdown are computed.

    Returns:
        Dict with 'nifty_estimate', 'weighted_return_pct', 'matched_count',
        'total_constituents', 'coverage_pct', 'breakdown', 'positive_contributors',
        'negative_contributors', 'total_positive_pts', 'total_negative_pts',
        'net_contribution_pts', 'positive_count', and 'negative_count'.
    """
    total_constituents = len(NIFTY50_WEIGHTS_PCT)
    weighted_return = 0.0
    breakdown: list[dict[str, Any]] = []

    for symbol, weight_pct in NIFTY50_WEIGHTS_PCT.items():
        quote = quotes.get(f"NSE:{symbol}")
        if not quote or not isinstance(quote, dict):
            continue

        try:
            ohlc = quote.get("ohlc", {})
            if not isinstance(ohlc, dict):
                continue
            prev_close = float(ohlc["close"])
            ltp = float(quote["last_price"])
            high = float(ohlc.get("high", ltp))
            low = float(ohlc.get("low", ltp))
            open_price = float(ohlc.get("open", prev_close))
        except (KeyError, TypeError, ValueError):
            continue


        if prev_close == 0:
            continue

        change_pts = ltp - prev_close
        stock_return_pct = 100.0 * change_pts / prev_close
        contribution_pct = (weight_pct / 100.0) * stock_return_pct
        weighted_return += contribution_pct / 100.0

        contrib_pts = round(nifty_prev_close * (contribution_pct / 100.0), 2) if nifty_prev_close else round((weight_pct / 100.0) * change_pts, 2)

        breakdown.append(
            {
                "symbol": symbol,
                "weight": weight_pct,
                "weight_pct": weight_pct,
                "prev_close": prev_close,
                "ltp": ltp,
                "open": open_price,
                "high": high,
                "low": low,
                "change_pts": round(change_pts, 2),
                "return_pct": round(stock_return_pct, 2),
                "contribution_pct": round(contribution_pct, 4),
                "contribution_pts": contrib_pts,
            }
        )


    breakdown.sort(key=lambda row: abs(row["contribution_pct"]), reverse=True)
    matched_count = len(breakdown)

    positive_contributors = sorted(
        [item for item in breakdown if item["contribution_pts"] >= 0],
        key=lambda row: row["contribution_pts"],
        reverse=True,
    )
    negative_contributors = sorted(
        [item for item in breakdown if item["contribution_pts"] < 0],
        key=lambda row: row["contribution_pts"],
    )

    total_pos_pts = round(sum(item["contribution_pts"] for item in positive_contributors), 2)
    total_neg_pts = round(sum(item["contribution_pts"] for item in negative_contributors), 2)
    net_contrib_pts = round(total_pos_pts + total_neg_pts, 2)

    result: dict[str, Any] = {
        "nifty_estimate": None,
        "weighted_return_pct": round(weighted_return * 100.0, 4) if matched_count else None,
        "matched_count": matched_count,
        "total_constituents": total_constituents,
        "coverage_pct": round(100.0 * matched_count / total_constituents, 1),
        "breakdown": breakdown,
        "positive_contributors": positive_contributors,
        "negative_contributors": negative_contributors,
        "total_positive_pts": total_pos_pts,
        "total_negative_pts": total_neg_pts,
        "net_contribution_pts": net_contrib_pts,
        "positive_count": len(positive_contributors),
        "negative_count": len(negative_contributors),
    }

    if nifty_prev_close is not None and matched_count > 0:
        result["nifty_estimate"] = round(nifty_prev_close * (1 + weighted_return), 2)

    return result

