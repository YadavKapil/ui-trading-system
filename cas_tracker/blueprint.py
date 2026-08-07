"""NIFTY Closing Auction Session (CAS) tracker — Flask blueprint.

Registered in flask_app.py as:
    from cas_tracker.blueprint import cas_tracker_bp, init_cas_tracker
    app.register_blueprint(cas_tracker_bp)
    init_cas_tracker()

The scraper (cas_tracker/scraper.py) remains a standalone background process
that polls the NSE CAS API and writes to cas_tracker.db. This blueprint only
serves the live dashboard, the JSON APIs it polls, and a watchdog that
restarts the scraper if it stalls during the CAS window; it does not run the
scraper itself.
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from flask import Blueprint, g, jsonify, render_template, request
from kiteconnect.exceptions import TokenException

from rate_limiter import limiter
import request_metrics

from cas_tracker.database import (
    backfill_tick_digests,
    count_snapshots_missing_digest,
    get_cas_anchors,
    get_chart_cache,
    get_tick_digests_for_date,
    get_contributor_dates,
    get_contributor_history_for_date,
    get_db,
    get_depth_distinct_dates,
    get_depth_snapshot_near,
    get_depth_timestamps_for_date,
    get_distinct_dates,
    get_latest_snapshot,
    get_snapshot_near,
    get_snapshot_timestamps_for_date,
    get_snapshots_for_date,
    init_db,
    purge_old_contributor_snapshots,
    save_cas_anchors,
    save_chart_cache,
    save_contributor_snapshot,
    save_depth_snapshot,
)

from cas_tracker.index_calculator import (
    estimate_nifty_from_cas,
    estimate_nifty_from_digest,
    estimate_nifty_from_live_quotes,
)
from cas_tracker.nifty50_weights import NIFTY50_WEIGHTS_PCT

logger = logging.getLogger(__name__)

# When true, this process never imports common_lib — i.e. it never creates a
# Kite session of its own. Set by public_api.py (the process split off from
# flask_app.py to isolate public traffic from the trading engine) before this
# module is imported. flask_app.py never sets this, so its behavior — full
# Kite-backed anchor capture and live LTP comparison — is unchanged.
#
# The tradeoff this accepts: a public request during the CAS window gets
# nifty_live=None instead of a live comparison figure (see
# _get_live_nifty_ltp), and a public request for a past date whose anchors
# were never captured gets a relative-only (%) chart instead of an absolute
# NIFTY-level one (see _build_candles_response) — until the main process (or
# an admin viewing the private dashboard) populates that date's anchors, at
# which point every process reads the same persisted cas_anchors row. Neither
# is a hard failure; both are the existing graceful-degradation paths this
# module already has for "anchors not available yet".
PUBLIC_ONLY_MODE = os.environ.get("CAS_TRACKER_PUBLIC_ONLY", "").strip() == "1"

_IST_OFFSET = timedelta(hours=5, minutes=30)
# Kept tight to 15:14-15:40 IST: the NSE CAS API returns no data outside
# this range, so polling wider than this only adds pointless load.
CAS_WINDOW_START = "15:14"
CAS_WINDOW_END = "15:40"
_STALE_THRESHOLD_SECONDS = 60
_RESTART_COOLDOWN_SECONDS = 120

# A single self-hosted instance has no separate public-marketing-site
# deployment, so there is nothing to detect here — see _is_public_request().
_PUBLIC_SITE_HOSTS: frozenset[str] = frozenset()

# How long a public visitor may be served a stale chart. Matches the poll
# interval the public page uses, so a visitor never waits more than one extra
# tick for new data.
_PUBLIC_CANDLES_TTL_SECONDS = 10.0

# /api/dates changes at most once a day (a new date appears after a session),
# so the public site can hold it far longer than the chart itself.
_PUBLIC_DATES_TTL_SECONDS = 300.0

# Per-date timestamp lists (orderbook/depth) only grow once a session ends —
# same reasoning as _PUBLIC_DATES_TTL_SECONDS.
_PUBLIC_TIMESTAMPS_TTL_SECONDS = 300.0

# Time-series reads (contributors history, replay, stock history). Shorter
# than the "dates" TTLs because today's date is still accumulating rows
# during the CAS window.
_PUBLIC_HISTORY_TTL_SECONDS = 60.0

# Single symbol/time point lookups (orderbook_at, depth_at) — same
# accumulating-today reasoning as _PUBLIC_HISTORY_TTL_SECONDS.
_PUBLIC_POINT_TTL_SECONDS = 60.0

# Rate limits for the public GET surface under /cas_tracker/api/* — every
# route here is exempted from login by enforce_auth() in flask_app.py, so
# these are the only throttle standing between it and abuse/scraping.
# Deliberately generous: this is defense-in-depth on top of the _PublicCache
# TTLs above (which already absorb the DB/compute cost), and the private
# trading dashboard polls some of these same routes from the office/home IP,
# so a limit that's too tight would break live trading monitoring, not just
# public traffic — worse than the problem this is meant to solve.
# "High freq" covers routes polled continuously while a page is open
# (/api/latest every ~1s per dashboard.html); "standard" covers routes hit
# once per page load or user interaction (date pickers, history lookups).
_PUBLIC_API_HIGH_FREQ_LIMIT = "300 per minute"
_PUBLIC_API_STANDARD_LIMIT = "120 per minute"

cas_tracker_bp = Blueprint(
    "cas_tracker",
    __name__,
    url_prefix="/cas_tracker",
    template_folder="templates",
)

_monitor_started = False
_monitor_lock = threading.Lock()
_last_auto_restart: datetime | None = None


@cas_tracker_bp.before_request
def _stamp_request_start() -> None:
    """Record the start time so _record_request_metrics can compute latency."""
    g._metrics_start = time.monotonic()


@cas_tracker_bp.after_request
def _record_request_metrics(response):
    """Feed every request into request_metrics, tagged with this process's label.

    Lets /cas_tracker/api/_metrics on the main process (flask_app.py) and on
    the split-off public-only process (public_api.py) be compared side by
    side — same instrumentation, different PROCESS_LABEL env var.
    """
    start = getattr(g, "_metrics_start", None)
    if start is not None and request.endpoint:
        latency_ms = (time.monotonic() - start) * 1000.0
        request_metrics.record(request.endpoint, response.status_code, latency_ms)
    return response

# Shared response caches for public-site traffic, keyed by date. Each entry is
# (monotonic_timestamp, payload). The locks are what make these useful: hundreds
# of visitors poll concurrently, so without single-flight every one of them
# would rebuild the same payload the moment an entry expires.
_public_candles_cache: dict[str, tuple[float, dict]] = {}
_public_candles_lock = threading.Lock()
_public_dates_cache: tuple[float, dict] | None = None
_public_dates_lock = threading.Lock()


class _PublicCache:
    """A shared, TTL-bound, single-flight response cache for one public GET endpoint.

    Generalizes the pattern _get_public_candles() proved out for
    /api/candles during the CAS Public Traffic Incident fix: the lock — not
    the TTL — is what matters under load. It collapses a burst of concurrent
    callers whose entry just expired into a single rebuild instead of one DB
    hit per caller. Each endpoint owns its own instance so unrelated
    endpoints never block on each other's lock.
    """

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = ttl_seconds
        self._store: dict[Any, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_build(self, key: Any, builder: Callable[[], Any]) -> Any:
        """Return the cached payload for *key*, rebuilding via *builder* if stale.

        Args:
            key: Cache key for this request — a date string, a parameter
                tuple, or None for a parameterless endpoint.
            builder: Zero-arg callable that computes the fresh payload; only
                invoked on a cache miss/expiry, and only once per stampede.

        Returns:
            The cached or freshly built payload.
        """
        cached = self._store.get(key)
        if cached is not None and time.monotonic() - cached[0] < self._ttl_seconds:
            return cached[1]

        with self._lock:
            # Re-check under the lock: whoever held it before us has just
            # refreshed this key, and we should reuse their work rather than
            # immediately repeating it.
            cached = self._store.get(key)
            if cached is not None and time.monotonic() - cached[0] < self._ttl_seconds:
                return cached[1]

            payload = builder()
            now = time.monotonic()
            # Drop entries nobody is requesting any more so browsing through
            # many dates/symbols can't grow this store without bound.
            for stale_key in [
                k
                for k, (cached_at, _) in self._store.items()
                if now - cached_at > self._ttl_seconds and k != key
            ]:
                del self._store[stale_key]
            self._store[key] = (now, payload)
            return payload


_contributors_dates_cache = _PublicCache(ttl_seconds=_PUBLIC_DATES_TTL_SECONDS)
_contributors_history_cache = _PublicCache(ttl_seconds=_PUBLIC_HISTORY_TTL_SECONDS)
_orderbook_timestamps_cache = _PublicCache(ttl_seconds=_PUBLIC_TIMESTAMPS_TTL_SECONDS)
_orderbook_at_cache = _PublicCache(ttl_seconds=_PUBLIC_POINT_TTL_SECONDS)
_depth_dates_cache = _PublicCache(ttl_seconds=_PUBLIC_DATES_TTL_SECONDS)
_depth_timestamps_cache = _PublicCache(ttl_seconds=_PUBLIC_TIMESTAMPS_TTL_SECONDS)
_depth_at_cache = _PublicCache(ttl_seconds=_PUBLIC_POINT_TTL_SECONDS)
_replay_cache = _PublicCache(ttl_seconds=_PUBLIC_HISTORY_TTL_SECONDS)
_stock_history_cache = _PublicCache(ttl_seconds=_PUBLIC_HISTORY_TTL_SECONDS)
_latest_cache = _PublicCache(ttl_seconds=_PUBLIC_CANDLES_TTL_SECONDS)

# NIFTY reconstruction anchors: NIFTY 50's own value at the CAS reference
# cutoff (~15:14-15:15 IST), plus each of its 49 constituents' own value at
# that identical moment — needed together so every stock's return is
# measured against the same instant as the NIFTY anchor. Both are sourced
# from historical 1-minute candles (kite.historical_data(), one Kite call
# per symbol) rather than any live quote field: `ohlc.close` is documented
# as the *previous* trading day's close, `last_price` sampled after hours
# reflects the 15:30 regular-close value (not the 15:14-15:15 CAS cutoff),
# and the CAS payload's own `refrencePrice` field looked promising but
# confirmed 2026-08-05 to be some other quantity (most likely a pre-cutoff
# VWAP, not a point price) — pairing any of these with the point-price
# anchor overstated the session's move by 39-90+ points. A historical
# minute candle is pinned to the exact right moment no matter when it's
# fetched, and — unlike a live quote — is retroactively available for any
# past date, so the exact same mechanism serves both today's live
# reconstruction and later "show me a past date's chart" requests.
#
# Keyed by date string ("YYYY-MM-DD") so past dates stay available after
# the calendar day rolls over — a single "today-only" cache (the original
# design) went stale and silently lost yesterday's anchors once "today"
# advanced. Cache is in-memory only (resets on restart, same as every
# other cache in this file); a stale/never-populated date just means that
# date's chart falls back to displaying % instead of absolute NIFTY levels.
_cas_anchors_cache: dict[str, tuple[float | None, dict[str, float]]] = {}
_cas_anchors_capture_lock = threading.Lock()
_STOCK_ANCHOR_FETCH_DELAY_SECONDS = 0.35  # keeps historical_data() calls under Kite's ~3 req/sec limit

# Failure cooldown: the dashboard polls /api/latest every second
# (dashboard.html's fetchLive()). Without this, a broken/expired Kite
# token turns every poll into a fresh retry + full traceback log, all day,
# until someone happens to re-login — 3600+ retries/hour instead of 2.
_KITE_RETRY_COOLDOWN_SECONDS = 30
_last_kite_failure_ts: float = 0.0


def _kite_recently_failed() -> bool:
    """Whether a Kite call failed within the last _KITE_RETRY_COOLDOWN_SECONDS."""
    return (time.monotonic() - _last_kite_failure_ts) < _KITE_RETRY_COOLDOWN_SECONDS


def _record_kite_failure() -> None:
    global _last_kite_failure_ts
    _last_kite_failure_ts = time.monotonic()

# Live replication check: a background thread (not per-request) polls Kite
# at a fixed, bounded rate and caches the result here, so the Kite call rate
# stays constant regardless of how many dashboard tabs are open. The kite
# client is shared with the live trading engine, so this must not scale
# with request volume.
LIVE_CHECK_POLL_INTERVAL_SECONDS = 4
_live_check_started = False
_live_check_lock = threading.Lock()
_live_check_cache: dict | None = None


def _ist_now() -> datetime:
    """Return the current wall-clock time in IST, derived from UTC.

    Returns:
        Naive datetime representing the current IST time.
    """
    return (datetime.now(timezone.utc) + _IST_OFFSET).replace(tzinfo=None)


def _is_public_request() -> bool:
    """Always False in the open-source build.

    The maintainer's own deployment fronts this app with two hostnames (a
    public marketing site and a private dashboard) and caches responses
    differently for each. A single self-hosted instance has no such split,
    so every request takes the always-fresh, uncached path.
    """
    return False


def _is_cas_window() -> bool:
    """Check whether the current IST time falls within the buffered CAS window.

    Returns:
        True on a weekday between CAS_WINDOW_START and CAS_WINDOW_END IST.
    """
    now = _ist_now()
    if now.weekday() > 4:
        return False
    start = datetime.strptime(CAS_WINDOW_START, "%H:%M").time()
    end = datetime.strptime(CAS_WINDOW_END, "%H:%M").time()
    return start <= now.time() <= end


def _restart_scraper_internal() -> None:
    """Kill and relaunch the standalone scraper process."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scraper_path = os.path.join(base_dir, "scraper.py")
    log_file = os.path.join(base_dir, "scraper.log")

    os.system("pkill -f 'cas_tracker/scraper.py'")
    os.system(f"cd {base_dir} && nohup python3 {scraper_path} >> {log_file} 2>&1 &")
    logger.info("Restarted CAS tracker scraper process")


def _monitor_scraper() -> None:
    """Background thread: restart the scraper if it stalls during the CAS window."""
    global _last_auto_restart

    while True:
        try:
            time.sleep(30)

            if not _is_cas_window():
                continue

            _ensure_today_cas_anchors_captured()

            conn = get_db()
            latest = get_latest_snapshot(conn)
            conn.close()

            if latest is None:
                continue

            last_dt = datetime.strptime(latest["timestamp"], "%Y-%m-%d %H:%M:%S")
            stale_seconds = (_ist_now() - last_dt).total_seconds()

            if stale_seconds <= _STALE_THRESHOLD_SECONDS:
                continue

            if _last_auto_restart and (datetime.now() - _last_auto_restart).total_seconds() < _RESTART_COOLDOWN_SECONDS:
                continue

            logger.warning("CAS scraper stale (%.0fs since last snapshot); restarting", stale_seconds)
            _restart_scraper_internal()
            _last_auto_restart = datetime.now()
        except Exception:
            logger.exception("CAS scraper watchdog iteration failed")


def _get_live_nifty_ltp() -> float | None:
    """Fetch NIFTY 50's live LTP for the estimate-vs-actual comparison.

    Imported lazily (not at module load) since common_lib initializes the
    shared Kite session as a side effect of import; by the time a request
    handler runs, flask_app.py has already imported it, so this is safe
    here even though cas_tracker/scraper.py deliberately avoids common_lib.

    Returns:
        The last traded price for "NSE:NIFTY 50", or None if the quote
        could not be fetched (e.g. Kite session expired), a recent attempt
        already failed (see _KITE_RETRY_COOLDOWN_SECONDS), or this process
        is running in PUBLIC_ONLY_MODE (never creates its own Kite session).
    """
    if PUBLIC_ONLY_MODE or _kite_recently_failed():
        return None

    try:
        import common_lib
        quote = common_lib.get_nifty_current_quote()
        return quote.get("last_price")
    except Exception:
        logger.exception("Failed to fetch live NIFTY quote for CAS comparison")
        _record_kite_failure()
        return None


def _fetch_cas_cutoff_candle(token: int, date_str: str) -> float | None:
    """Historical 1-min candle close at the CAS reference cutoff for one instrument/date.

    Args:
        token: Kite instrument_token for the index or equity.
        date_str: Date string "YYYY-MM-DD" to fetch the candle for.

    Returns:
        The close of the CAS_WINDOW_START (15:14) minute candle, or None if
        that candle doesn't exist (e.g. a holiday, or the date is too
        recent for the window to have opened yet).
    """
    import common_lib

    year, month, day = (int(p) for p in date_str.split("-"))
    from_dt = datetime(year, month, day, 15, 10, 0)
    to_dt = datetime(year, month, day, 15, 20, 0)
    candles = common_lib.kite.historical_data(token, from_dt, to_dt, "minute")
    candle = next((c for c in candles if c["date"].strftime("%H:%M") == CAS_WINDOW_START), None)
    return candle["close"] if candle else None


def _capture_cas_anchors_for_date(date_str: str) -> tuple[float | None, dict[str, float]]:
    """Fetch NIFTY's and all 49 constituents' value at the CAS cutoff for one date.

    ~50 sequential kite.historical_data() calls, throttled to stay under
    Kite's historical-data rate limit (~17-20 seconds total) — expensive
    enough that callers must invoke this at most once per date (see
    _get_cas_anchors / _ensure_today_cas_anchors_captured) and persist the
    result via database.save_cas_anchors rather than ever repeating it.

    Args:
        date_str: Date string "YYYY-MM-DD".

    Returns:
        (nifty_anchor, stock_anchors) — nifty_anchor is None and
        stock_anchors is missing a symbol if that specific historical
        candle wasn't available.
    """
    import common_lib
    import instrument_cache

    nifty_token = common_lib.get_nifty_current_quote()["instrument_token"]
    nifty_anchor = _fetch_cas_cutoff_candle(nifty_token, date_str)

    stock_anchors: dict[str, float] = {}
    for symbol in NIFTY50_WEIGHTS_PCT:
        try:
            token = instrument_cache.get_nse_equity_token(symbol)
            if token is None:
                logger.warning("No NSE equity instrument token cached for %s; skipping CAS anchor", symbol)
                continue
            price = _fetch_cas_cutoff_candle(token, date_str)
            if price is not None:
                stock_anchors[symbol] = price
        except Exception:
            logger.exception("Failed to fetch CAS-cutoff candle for %s on %s", symbol, date_str)
        time.sleep(_STOCK_ANCHOR_FETCH_DELAY_SECONDS)

    return nifty_anchor, stock_anchors


def _do_capture_and_persist(date_str: str) -> tuple[float | None, dict[str, float]]:
    """Run _capture_cas_anchors_for_date and save the result to memory + disk.

    Args:
        date_str: Date string "YYYY-MM-DD".

    Returns:
        (nifty_anchor, stock_anchors) — (None, {}) if the capture itself
        raised (nothing is cached in that case, so a later call can retry,
        subject to _KITE_RETRY_COOLDOWN_SECONDS).
    """
    try:
        result = _capture_cas_anchors_for_date(date_str)
    except Exception:
        logger.exception("Failed to capture CAS anchors for %s", date_str)
        _record_kite_failure()
        return (None, {})

    _cas_anchors_cache[date_str] = result
    conn = get_db()
    save_cas_anchors(conn, date_str, result[0], result[1])
    conn.close()
    logger.info(
        "Captured & persisted CAS anchors for %s: nifty_anchor=%s, %d/%d stocks",
        date_str, result[0], len(result[1]), len(NIFTY50_WEIGHTS_PCT),
    )
    return result


def _get_cas_anchors(date_str: str, allow_blocking_fetch: bool) -> tuple[float | None, dict[str, float]]:
    """Get (nifty_anchor, stock_anchors) for a date: memory -> disk -> optionally fetch.

    Args:
        date_str: Date string "YYYY-MM-DD".
        allow_blocking_fetch: Whether this call may perform the ~50-call,
            ~20-second synchronous Kite fetch if the date isn't cached
            anywhere yet. Must be False for today's live-polled endpoints
            (today's capture happens asynchronously — see
            _ensure_today_cas_anchors_captured, run from the background
            watchdog thread) and is safe to set True only for a
            deliberate, infrequent "show me this date's chart" request
            for a date other than today.

    Returns:
        (nifty_anchor, stock_anchors) — (None, {}) if never captured and
        either blocking wasn't allowed or the fetch failed.
    """
    if date_str in _cas_anchors_cache:
        return _cas_anchors_cache[date_str]

    conn = get_db()
    persisted = get_cas_anchors(conn, date_str)
    conn.close()
    if persisted is not None:
        _cas_anchors_cache[date_str] = persisted
        return persisted

    if not allow_blocking_fetch:
        return (None, {})

    with _cas_anchors_capture_lock:
        if date_str in _cas_anchors_cache:  # populated by a concurrent request while we waited
            return _cas_anchors_cache[date_str]
        if _kite_recently_failed():
            return (None, {})
        return _do_capture_and_persist(date_str)


def _ensure_today_cas_anchors_captured() -> None:
    """Background-thread trigger: capture and persist today's CAS anchors, once.

    Called from _monitor_scraper()'s loop (already gated to
    _is_cas_window()). Never invoked from a request handler — see
    _get_cas_anchors's allow_blocking_fetch docstring for why today must
    stay non-blocking there.
    """
    today = _ist_now().strftime("%Y-%m-%d")
    if today in _cas_anchors_cache:
        return

    conn = get_db()
    persisted = get_cas_anchors(conn, today)
    conn.close()
    if persisted is not None:
        _cas_anchors_cache[today] = persisted
        return

    window_start = datetime.strptime(CAS_WINDOW_START, "%H:%M").time()
    if _ist_now().time() < window_start:
        return
    if _kite_recently_failed():
        return

    _do_capture_and_persist(today)


def _build_index_estimate_fields(rows: list, in_window: bool) -> dict:
    """Compute the NIFTY reconstruction + live comparison fields for an API response.

    Args:
        rows: The CAS `data` array (live or replayed).
        in_window: Whether to fetch a live LTP for the estimate-vs-actual
            comparison (False for replay, where only the relative
            weighted-return is meaningful — there's no "live" for history).

    Returns:
        Dict of fields to merge into the /api/latest or /api/replay response.
    """
    live_ltp = _get_live_nifty_ltp() if in_window else None
    today = _ist_now().strftime("%Y-%m-%d")
    anchor, stock_anchors = _get_cas_anchors(today, allow_blocking_fetch=False)

    estimate = estimate_nifty_from_cas(rows, anchor, stock_anchors)

    diff = None
    diff_pct = None
    if estimate["nifty_estimate"] is not None and live_ltp is not None:
        diff = round(estimate["nifty_estimate"] - live_ltp, 2)
        diff_pct = round(100.0 * diff / live_ltp, 4) if live_ltp else None

    return {
        "nifty_estimate": estimate["nifty_estimate"],
        "nifty_live": live_ltp,
        "nifty_anchor": anchor,
        "nifty_diff": diff,
        "nifty_diff_pct": diff_pct,
        "weighted_return_pct": estimate["weighted_return_pct"],
        "matched_constituents": estimate["matched_count"],
        "total_constituents": estimate["total_constituents"],
        "coverage_pct": estimate["coverage_pct"],
    }


def _is_market_hours() -> bool:
    """Check whether NSE's regular cash market session is open (IST).

    Extended slightly past the official 15:30 close to 15:40 so this also
    covers the CAS print. Separate from _is_cas_window(): this feature
    validates the weighting/formula logic using ordinary live prices any
    time the market trades, not just during the CAS auction itself.

    Returns:
        True on a weekday between 09:15 and 15:40 IST.
    """
    now = _ist_now()
    if now.weekday() > 4:
        return False
    start = datetime.strptime("09:15", "%H:%M").time()
    end = datetime.strptime("15:40", "%H:%M").time()
    return start <= now.time() <= end


def _persist_cas_depth_snapshot(quotes: dict) -> None:
    """Persist each stock's bid/ask depth from an already-fetched quotes batch.

    Piggybacks on the quotes _poll_live_replication() already fetches
    during the CAS window — adds zero extra Kite calls. Recorded to
    empirically settle whether Kite's 5-level market depth field is
    actually populated with real order-book data during the closing
    auction, which isn't confirmed either way in Kite's docs as of CAS's
    2026-08-03 launch (Zerodha's own CAS explainer said market-depth
    support for CAS was still being built at launch).

    Args:
        quotes: Raw response from kite.quote([...]), keyed by "NSE:<symbol>".
    """
    depth_by_symbol: dict[str, dict] = {}
    for symbol in NIFTY50_WEIGHTS_PCT:
        quote = quotes.get(f"NSE:{symbol}")
        depth = quote.get("depth") if quote else None
        if depth:
            depth_by_symbol[symbol] = depth

    if not depth_by_symbol:
        return

    conn = get_db()
    save_depth_snapshot(conn, _ist_now().strftime("%Y-%m-%d %H:%M:%S"), depth_by_symbol)
    conn.close()


def _poll_live_replication() -> None:
    """Background thread: periodically reconstruct NIFTY from live constituent quotes.

    Runs inside the Flask process (not as a standalone script) so it can use
    the already-authenticated common_lib.kite singleton directly, and caches
    its result in _live_check_cache so /api/live_check never triggers a Kite
    call itself. Also persists each constituent's bid/ask depth during the
    CAS window specifically (see _persist_cas_depth_snapshot) — same quotes,
    no extra Kite calls.
    """
    global _live_check_cache

    instruments = [f"NSE:{symbol}" for symbol in NIFTY50_WEIGHTS_PCT] + ["NSE:NIFTY 50"]
    last_auth_warning_at = 0.0
    AUTH_WARNING_INTERVAL_SECONDS = 300  # avoid a full traceback every tick while the token is stale/missing

    while True:
        try:
            is_open = _is_market_hours()
            if not is_open and _live_check_cache is not None:
                time.sleep(30)
                continue

            import common_lib

            # No Kite session yet (e.g. fresh local dev start before login) —
            # skip the call instead of hammering the API every
            # LIVE_CHECK_POLL_INTERVAL_SECONDS. Resumes automatically once
            # common_lib.kite has a token.
            if not getattr(common_lib.kite, "access_token", None):
                logger.debug("Live replication poll: no Kite session yet — waiting for login")
                time.sleep(15)
                continue

            try:
                quotes = common_lib.kite.quote(instruments)
            except TokenException:
                # A token IS set but Kite rejects it — expired/stale session,
                # expected once a day until re-login. Back off and log a single
                # concise warning every AUTH_WARNING_INTERVAL_SECONDS instead of
                # a full traceback every LIVE_CHECK_POLL_INTERVAL_SECONDS.
                now = time.monotonic()
                if now - last_auth_warning_at > AUTH_WARNING_INTERVAL_SECONDS:
                    logger.warning(
                        "Live replication poll: Kite session token is invalid/expired — "
                        "waiting for re-login (further repeats suppressed for %ss)",
                        AUTH_WARNING_INTERVAL_SECONDS,
                    )
                    last_auth_warning_at = now
                time.sleep(15)
                continue

            if _is_cas_window():
                _persist_cas_depth_snapshot(quotes)

            nifty_quote = quotes.get("NSE:NIFTY 50")
            nifty_prev_close = None
            nifty_live = None
            if nifty_quote:
                nifty_prev_close = nifty_quote.get("ohlc", {}).get("close")
                nifty_live = nifty_quote.get("last_price")

            estimate = estimate_nifty_from_live_quotes(quotes, nifty_prev_close)

            diff = None
            diff_pct = None
            if estimate["nifty_estimate"] is not None and nifty_live is not None:
                diff = round(estimate["nifty_estimate"] - nifty_live, 2)
                diff_pct = round(100.0 * diff / nifty_live, 4) if nifty_live else None

            pos_contribs = estimate.get("positive_contributors", [])
            neg_contribs = estimate.get("negative_contributors", [])
            top5_pos_pts = round(sum(item["contribution_pts"] for item in pos_contribs[:5]), 2)
            top5_neg_pts = round(sum(item["contribution_pts"] for item in neg_contribs[:5]), 2)
            top5_net_pts = round(top5_pos_pts + top5_neg_pts, 2)

            now_ist = _ist_now()
            ts_str = now_ist.strftime("%Y-%m-%d %H:%M:%S")
            date_str = now_ist.strftime("%Y-%m-%d")

            _live_check_cache = {
                "timestamp": ts_str,
                "nifty_estimate": estimate["nifty_estimate"],
                "nifty_live": nifty_live,
                "nifty_prev_close": nifty_prev_close,
                "nifty_diff": diff,
                "nifty_diff_pct": diff_pct,
                "weighted_return_pct": estimate["weighted_return_pct"],
                "matched_constituents": estimate["matched_count"],
                "total_constituents": estimate["total_constituents"],
                "coverage_pct": estimate["coverage_pct"],
                "breakdown": estimate["breakdown"],
                "positive_contributors": pos_contribs,
                "negative_contributors": neg_contribs,
                "total_positive_pts": estimate.get("total_positive_pts", 0.0),
                "total_negative_pts": estimate.get("total_negative_pts", 0.0),
                "net_contribution_pts": estimate.get("net_contribution_pts", 0.0),
                "top5_pos_pts": top5_pos_pts,
                "top5_neg_pts": top5_neg_pts,
                "top5_net_pts": top5_net_pts,
                "positive_count": estimate.get("positive_count", 0),
                "negative_count": estimate.get("negative_count", 0),
            }

            try:
                conn = get_db()
                save_contributor_snapshot(
                    conn=conn,
                    timestamp=ts_str,
                    date=date_str,
                    nifty_live=nifty_live,
                    nifty_estimate=estimate["nifty_estimate"],
                    top5_pos_pts=top5_pos_pts,
                    top5_neg_pts=top5_neg_pts,
                    top5_net_pts=top5_net_pts,
                    net_contribution_pts=estimate.get("net_contribution_pts", 0.0),
                )
                purge_old_contributor_snapshots(conn, days_to_keep=7)
                conn.close()
            except Exception:
                logger.exception("Failed to save contributor snapshot to DB")
        except Exception:
            logger.exception("Live replication check poll failed; retrying next tick")

        time.sleep(LIVE_CHECK_POLL_INTERVAL_SECONDS)


def init_cas_tracker() -> None:
    """Ensure the CAS tracker schema exists and start its background threads.

    init_db() is idempotent (CREATE TABLE IF NOT EXISTS) and cheap, so it's
    safe to call here on every Flask startup — this is what adds new tables
    (e.g. cas_anchors) to a database file created by an older schema
    version, without depending on the standalone scraper.py process (which
    only runs init_db() once, at its own startup) having been restarted.
    """
    init_db()

    global _monitor_started, _live_check_started
    with _monitor_lock:
        if not _monitor_started:
            threading.Thread(target=_monitor_scraper, daemon=True).start()
            _monitor_started = True
    with _live_check_lock:
        if not _live_check_started:
            threading.Thread(target=_poll_live_replication, daemon=True).start()
            _live_check_started = True


def _filter_to_active_nifty50(rows: list) -> list:
    """Restrict CAS rows to NIFTY 50 constituents that currently have an active price.

    Args:
        rows: The full CAS `data` array (covers all ~208 F&O CAS-eligible
            stocks, a superset of the 49/50 weighted constituents).

    Returns:
        Rows whose 'symbol' is a known NIFTY 50 constituent AND whose IEP is
        non-zero — a stock reads IEP=0 before the auction has established
        any equilibrium price for it yet (order book still collecting), so
        those rows are noise for a display table, not "the active ones".
    """
    return [
        row
        for row in rows
        if row.get("symbol") in NIFTY50_WEIGHTS_PCT and row.get("IEP", 0) not in (0, None)
    ]


@cas_tracker_bp.route("/")
def index():
    return render_template(
        "cas_tracker/dashboard.html",
        nifty50_symbols=sorted(NIFTY50_WEIGHTS_PCT.keys()),
    )


def _build_latest_response() -> dict:
    """Compute /api/latest's payload fresh from the DB."""
    conn = get_db()
    latest = get_latest_snapshot(conn)
    conn.close()

    in_window = _is_cas_window()

    if latest is None:
        return {
            "in_window": in_window,
            "timestamp": None,
            "data": [],
            "symbols": [],
            "message": "No snapshot recorded yet.",
        }

    payload = latest["data"]
    rows = _filter_to_active_nifty50(payload.get("data", []))
    response = {
        "in_window": in_window,
        "timestamp": latest["timestamp"],
        "data": rows,
        "symbols": payload.get("symbols", []),
        "totalValue": payload.get("totalValue"),
        "totalQuantity": payload.get("totalQuantity"),
    }
    response.update(_build_index_estimate_fields(rows, in_window))
    return response


@cas_tracker_bp.route("/api/latest")
@limiter.limit(_PUBLIC_API_HIGH_FREQ_LIMIT)
def api_latest():
    """Most recent CAS snapshot, reconstructed into a NIFTY estimate.

    Mirrors /api/candles: public-site traffic is served from a shared
    short-lived cache (same TTL as candles, since this is the "live tip" of
    the same series); the private dashboard always gets a freshly computed
    response.
    """
    if not _is_public_request():
        return jsonify(_build_latest_response())

    payload = _latest_cache.get_or_build(None, _build_latest_response)
    response = jsonify(payload)
    response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_CANDLES_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/dates")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_dates():
    """List session dates that have recorded CAS data.

    Every public visitor calls this once on page load, but a new date only
    appears after a session closes — so public traffic is served from a
    long-lived shared cache. The private dashboard still reads through.
    """
    global _public_dates_cache

    if not _is_public_request():
        conn = get_db()
        dates = get_distinct_dates(conn)
        conn.close()
        return jsonify({"dates": dates})

    cached = _public_dates_cache
    if cached is not None and time.monotonic() - cached[0] < _PUBLIC_DATES_TTL_SECONDS:
        payload = cached[1]
    else:
        with _public_dates_lock:
            cached = _public_dates_cache
            if cached is not None and time.monotonic() - cached[0] < _PUBLIC_DATES_TTL_SECONDS:
                payload = cached[1]
            else:
                conn = get_db()
                payload = {"dates": get_distinct_dates(conn)}
                conn.close()
                _public_dates_cache = (time.monotonic(), payload)

    response = jsonify(payload)
    response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_DATES_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/replay")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_replay():
    date = request.args.get("date")
    at_time = request.args.get("at", "15:20:00")
    if not date:
        return jsonify({"error": "Missing required query param 'date' (YYYY-MM-DD)"}), 400

    def _build() -> dict | None:
        conn = get_db()
        snapshot = get_snapshot_near(conn, date, at_time)
        conn.close()
        if snapshot is None:
            return None

        payload = snapshot["data"]
        rows = _filter_to_active_nifty50(payload.get("data", []))
        response = {
            "timestamp": snapshot["timestamp"],
            "data": rows,
            "symbols": payload.get("symbols", []),
            "totalValue": payload.get("totalValue"),
            "totalQuantity": payload.get("totalQuantity"),
        }
        # Replay has no live anchor to compound onto; only the relative
        # weighted-return figure is meaningful for a historical snapshot.
        response.update(_build_index_estimate_fields(rows, in_window=False))
        return response

    is_public = _is_public_request()
    response = _replay_cache.get_or_build((date, at_time), _build) if is_public else _build()

    if response is None:
        return jsonify({"error": f"No recorded data for {date}"}), 404

    resp = jsonify(response)
    if is_public:
        resp.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_HISTORY_TTL_SECONDS)}"
    return resp


def _compute_minute_volumes(digest_ticks: list) -> dict[str, float]:
    """Approximate a per-minute traded-value volume across NIFTY 50 constituents.

    NSE's CAS payload exposes `totTradedQty` per stock as a running
    indicative-match count that climbs during price discovery (order
    collection) and freezes once the auction locks a final price — it is
    NOT a per-tick delta, so "volume for this minute" has to be computed as
    (last totTradedQty seen this minute) - (last totTradedQty seen last
    minute), per symbol, floored at 0. Each symbol's delta is converted to
    an approximate traded value using its reference price (shares of
    different-priced stocks aren't directly comparable), then summed
    across all matched constituents for a single NIFTY-level volume bar.

    Args:
        digest_ticks: Chronological list of {'timestamp', 'digest'} from
            get_tick_digests_for_date(). The digest already carries only
            NIFTY 50 constituents, with traded quantity and reference price
            pre-parsed.

    Returns:
        Dict of minute string ("YYYY-MM-DD HH:MM") -> approximate ₹ volume.
    """
    minute_symbol_last: dict[str, dict[str, tuple[float, float]]] = {}

    for tick in digest_ticks:
        digest = tick.get("digest")
        if not digest:
            continue
        rows = digest.get("rows", [])
        if not rows:
            continue
        minute = tick["timestamp"][:16]
        symbol_last = minute_symbol_last.setdefault(minute, {})
        for symbol, _price, ref_price, traded_qty in rows:
            # A symbol with no reference price contributes no traded value;
            # 0.0 keeps it in the running total's symbol state without
            # inventing a price for it.
            symbol_last[symbol] = (traded_qty, ref_price or 0.0)

    minute_volume: dict[str, float] = {}
    prev_qty_by_symbol: dict[str, float] = {}
    for minute in sorted(minute_symbol_last.keys()):
        total_value = 0.0
        for symbol, (qty, ref_price) in minute_symbol_last[minute].items():
            prev_qty = prev_qty_by_symbol.get(symbol, 0.0)
            delta_qty = max(0.0, qty - prev_qty)
            total_value += delta_qty * ref_price
            prev_qty_by_symbol[symbol] = qty
        minute_volume[minute] = total_value

    return minute_volume


def _build_candles_response(date: str, today_str: str) -> dict:
    """Rebuild the candle/tick payload for one date.

    Reads the compact per-tick digests written alongside each snapshot rather
    than the raw payloads they came from — ~1.5 KB per tick instead of
    ~150 KB, which is what made this affordable to call at all (a session's
    raw JSON is ~23 MB and used to be re-parsed on every request).

    Digests are derived data: anchors are deliberately not folded into them,
    because they arrive asynchronously and would otherwise freeze a stale
    estimate into storage. The anchor is applied here, at read time.

    Args:
        date: Session date to build, as YYYY-MM-DD.
        today_str: Today's date in IST, used to decide whether the on-disk
            chart cache may be read/written (a finalized past date never
            changes; today is still accumulating snapshots).

    Uses NIFTY 50's value at the CAS reference cutoff plus each constituent's
    own value at that same cutoff (_get_cas_anchors()) to convert each tick's
    weighted return into an absolute NIFTY-like value; if that's ever
    unavailable, 'estimate' is None but 'weighted_return_pct' is still
    returned so a relative-move chart is possible.

    For any date other than today, anchors not yet on disk are fetched on
    demand (blocking for ~20s the first time a given past date is viewed) and
    persisted, so every later view of that date — including after a restart —
    is instant. Today's anchors are never fetched here; they're populated
    asynchronously by the background watchdog thread (see
    _ensure_today_cas_anchors_captured), so this never blocks on them.

    Rebuilding is expensive even for a finalized date (e.g. ~7s for
    2026-08-05's ~1683 rows, recorded at the old 1s poll interval) and
    produces the same output every time, since that date's data never
    changes. So past dates are cached on disk in cas_chart_cache. Today can't
    use that cache (still accumulating snapshots) — it is instead throttled
    per-caller by api_candles().

    Args:
        date: Session date to build, as YYYY-MM-DD.
        today_str: Today's date in IST, used to decide whether the on-disk
            chart cache may be read/written.

    Returns:
        Dict with 'date', 'is_absolute', 'candles' and 'ticks'.
    """
    if date != today_str:
        conn = get_db()
        cached = get_chart_cache(conn, date)
        conn.close()
        if cached is not None:
            return cached

    conn = get_db()
    try:
        # Snapshots recorded before digests existed (or by an older scraper)
        # are converted on first use, so this stays correct on historical
        # data without a separate migration step having to run first.
        missing = count_snapshots_missing_digest(conn, date)
        if missing:
            logger.info(
                "Backfilling %d missing CAS tick digests for %s.", missing, date
            )
            backfill_tick_digests(conn, date)
        digest_ticks = get_tick_digests_for_date(conn, date)
    finally:
        conn.close()

    # PUBLIC_ONLY_MODE never blocks on a Kite fetch — see the flag's
    # docstring. A never-before-viewed past date just renders relative-only
    # until the main process (or an admin view) captures and persists its
    # anchors, at which point this process picks up the same cached row.
    allow_blocking_fetch = (date != today_str) and not PUBLIC_ONLY_MODE
    anchor, stock_anchors = _get_cas_anchors(date, allow_blocking_fetch=allow_blocking_fetch)
    minute_volume = _compute_minute_volumes(digest_ticks)

    ticks = []
    for digest_tick in digest_ticks:
        digest = digest_tick.get("digest")
        # A digest exists precisely when the raw payload had rows, so this
        # mirrors the old `if not rows: continue` exactly. Do not also skip
        # on an empty rows list — a payload whose constituents are all
        # unusable still produced a tick before, and still must.
        if not digest:
            continue
        estimate = estimate_nifty_from_digest(digest, anchor, stock_anchors)
        ticks.append(
            {
                "timestamp": digest_tick["timestamp"],
                "minute": digest_tick["timestamp"][:16],
                "estimate": estimate["nifty_estimate"],
                "weighted_return_pct": estimate["weighted_return_pct"],
            }
        )

    candles: dict[str, dict] = {}
    for tick in ticks:
        value = tick["estimate"] if tick["estimate"] is not None else tick["weighted_return_pct"]
        if value is None:
            continue
        minute = tick["minute"]
        if minute not in candles:
            candles[minute] = {"minute": minute, "open": value, "high": value, "low": value, "close": value}
        else:
            c = candles[minute]
            c["high"] = max(c["high"], value)
            c["low"] = min(c["low"], value)
            c["close"] = value

    for minute, c in candles.items():
        c["volume"] = round(minute_volume.get(minute, 0.0), 2)

    tick_series = [
        {"timestamp": tick["timestamp"], "value": tick["estimate"] if tick["estimate"] is not None else tick["weighted_return_pct"]}
        for tick in ticks
        if (tick["estimate"] if tick["estimate"] is not None else tick["weighted_return_pct"]) is not None
    ]

    response = {
        "date": date,
        "is_absolute": anchor is not None,
        "candles": sorted(candles.values(), key=lambda c: c["minute"]),
        "ticks": tick_series,
    }

    if date != today_str and digest_ticks:
        conn = get_db()
        save_chart_cache(conn, date, response)
        conn.close()

    return response


def _get_public_candles(date: str, today_str: str) -> dict:
    """Serve the public site's candles from a shared, short-lived cache.

    Hundreds of visitors poll this concurrently and today's payload cannot use
    the on-disk chart cache, so without this every request would rebuild the
    whole session from raw rows. The lock — not the TTL — is what does the
    heavy lifting: it collapses a burst of simultaneous callers into a single
    rebuild, so an expired entry can't trigger a stampede.

    Args:
        date: Session date being viewed, as YYYY-MM-DD.
        today_str: Today's date in IST.

    Returns:
        The candle/tick payload, possibly up to _PUBLIC_CANDLES_TTL_SECONDS old.
    """
    cached = _public_candles_cache.get(date)
    if cached is not None and time.monotonic() - cached[0] < _PUBLIC_CANDLES_TTL_SECONDS:
        return cached[1]

    with _public_candles_lock:
        # Re-check under the lock: whoever held it before us has just
        # refreshed this date, and we should reuse their work rather than
        # immediately repeating it.
        cached = _public_candles_cache.get(date)
        if cached is not None and time.monotonic() - cached[0] < _PUBLIC_CANDLES_TTL_SECONDS:
            return cached[1]

        response = _build_candles_response(date, today_str)
        now = time.monotonic()
        # Drop entries for dates nobody is viewing any more so browsing back
        # through history can't grow this dict without bound.
        for stale_date in [
            key
            for key, (cached_at, _) in _public_candles_cache.items()
            if now - cached_at > _PUBLIC_CANDLES_TTL_SECONDS and key != date
        ]:
            del _public_candles_cache[stale_date]
        _public_candles_cache[date] = (now, response)
        return response


@cas_tracker_bp.route("/api/candles")
@limiter.limit(_PUBLIC_API_HIGH_FREQ_LIMIT)
def api_candles():
    """1-min OHLC(V) candles of the reconstructed NIFTY estimate.

    Public-site traffic (see _is_public_request()) is served from a shared
    cache and told to hold the result client-side for the same window; the
    private dashboard always gets a freshly computed response.
    """
    date = request.args.get("date") or _ist_now().strftime("%Y-%m-%d")
    today_str = _ist_now().strftime("%Y-%m-%d")

    if not _is_public_request():
        return jsonify(_build_candles_response(date, today_str))

    response = jsonify(_get_public_candles(date, today_str))
    response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_CANDLES_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/stock_history")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_stock_history():
    """Every recorded data point for one NIFTY 50 stock on a given date.

    Drill-down view: reuses the same raw snapshots as /api/candles, but
    returns one row per tick for a single symbol instead of an aggregated
    index estimate, with the snapshot timestamp merged in as the first field.
    """
    symbol = request.args.get("symbol")
    date = request.args.get("date") or _ist_now().strftime("%Y-%m-%d")

    if not symbol:
        return jsonify({"error": "Missing required query param 'symbol'"}), 400
    if symbol not in NIFTY50_WEIGHTS_PCT:
        return jsonify({"error": f"'{symbol}' is not a tracked NIFTY 50 constituent"}), 400

    def _build() -> dict:
        conn = get_db()
        snapshots = get_snapshots_for_date(conn, date)
        conn.close()

        history = []
        for snap in snapshots:
            rows = snap["data"].get("data", [])
            for row in rows:
                if row.get("symbol") == symbol:
                    history.append({"timestamp": snap["timestamp"], **row})
                    break

        return {"date": date, "symbol": symbol, "rows": history}

    is_public = _is_public_request()
    payload = _stock_history_cache.get_or_build((symbol, date), _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_HISTORY_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/live_check")
@limiter.limit(_PUBLIC_API_HIGH_FREQ_LIMIT)
def api_live_check():
    if _live_check_cache is None:
        return jsonify(
            {
                "ready": False,
                "in_market_hours": _is_market_hours(),
                "message": "No live replication data yet — the background poller may still be starting, or the market is closed.",
            }
        )
    response = dict(_live_check_cache)
    response["ready"] = True
    response["in_market_hours"] = _is_market_hours()
    return jsonify(response)


@cas_tracker_bp.route("/api/_metrics")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_metrics():
    """Per-route request counts/latency for this process, see request_metrics.py.

    Hit this on both the main process (port 5010) and the split-off
    public-only process (port 6010) to compare load side by side — same
    instrumentation on both, distinguished by the 'process' field.
    """
    return jsonify(request_metrics.snapshot())


@cas_tracker_bp.route("/contributors")
def contributors_page():
    return render_template("nifty_contributors.html")


@cas_tracker_bp.route("/api/contributors")
@limiter.limit(_PUBLIC_API_HIGH_FREQ_LIMIT)
def api_contributors():
    if _live_check_cache is None:
        return jsonify(
            {
                "ready": False,
                "in_market_hours": _is_market_hours(),
                "message": "No live replication data yet — the background poller may still be starting, or the market is closed.",
            }
        )
    response = dict(_live_check_cache)
    response["ready"] = True
    response["in_market_hours"] = _is_market_hours()
    return jsonify(response)


@cas_tracker_bp.route("/api/contributors/dates")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_contributors_dates():
    """Return available historical dates for contributor snapshot records."""

    def _build() -> dict:
        conn = get_db()
        try:
            return {"dates": get_contributor_dates(conn)}
        finally:
            conn.close()

    is_public = _is_public_request()
    payload = _contributors_dates_cache.get_or_build(None, _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_DATES_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/contributors/history")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_contributors_history():
    """Return historical time series of contributor metrics for a given date."""
    date = request.args.get("date") or _ist_now().strftime("%Y-%m-%d")

    def _build() -> dict:
        conn = get_db()
        try:
            return {"date": date, "history": get_contributor_history_for_date(conn, date)}
        finally:
            conn.close()

    is_public = _is_public_request()
    payload = _contributors_history_cache.get_or_build(date, _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_HISTORY_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/order_tracker")
def order_tracker_page():
    return render_template(
        "cas_tracker/order_tracker.html",
        nifty50_symbols=sorted(NIFTY50_WEIGHTS_PCT.keys()),
    )


@cas_tracker_bp.route("/api/orderbook_timestamps")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_orderbook_timestamps():
    """Return every recorded CAS-payload timestamp for a date, ascending."""
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "Missing required query param 'date' (YYYY-MM-DD)"}), 400

    def _build() -> dict:
        conn = get_db()
        try:
            return {"date": date, "timestamps": get_snapshot_timestamps_for_date(conn, date)}
        finally:
            conn.close()

    is_public = _is_public_request()
    payload = _orderbook_timestamps_cache.get_or_build(date, _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_TIMESTAMPS_TTL_SECONDS)}"
    return response


def _merge_order_book(
    nse_levels: list[dict] | None, kite_depth: dict | None
) -> list[dict]:
    """Merge NSE's CAS price ladder with Kite's market depth into one ladder.

    Neither source alone is complete, and they are not nested: measured
    2026-08-06 across liquid constituents, NSE's `orderBook` carries 4
    levels per side while Kite's `depth` carries 5, with Kite's extra one
    sitting nearest the equilibrium price (the most interesting level) —
    but Kite's depth is sometimes absent entirely for a symbol where NSE's
    is present. Taking the union gives the widest ladder either source can
    support, and lets the order-count column (which only Kite provides) be
    filled wherever it is known.

    Unpriced rows are dropped here. Kite represents at-auction orders as a
    synthetic `price: 0` depth level, which is not a real price point and
    would sort to the bottom of the ladder as if it were; those quantities
    are surfaced separately from NSE's authoritative atoBuyQuantity /
    atoSellQuantity fields instead.

    Args:
        nse_levels: The CAS payload's `orderBook` array — entries of
            {price, buyQuantity, sellQuantity}, one side normally zero.
        kite_depth: Kite's `depth` dict — {"buy": [...], "sell": [...]},
            each level {price, quantity, orders}. May be None.

    Returns:
        Levels sorted by price descending, each
        {price, buy_qty, buy_orders, sell_qty, sell_orders}, with order
        counts None where unknown.
    """
    merged: dict[float, dict] = {}

    def slot(price: float) -> dict:
        return merged.setdefault(
            price,
            {
                "price": price,
                "buy_qty": 0,
                "buy_orders": None,
                "sell_qty": 0,
                "sell_orders": None,
            },
        )

    for level in nse_levels or []:
        price = level.get("price")
        if not price:
            continue
        entry = slot(price)
        if level.get("buyQuantity"):
            entry["buy_qty"] = level["buyQuantity"]
        if level.get("sellQuantity"):
            entry["sell_qty"] = level["sellQuantity"]

    if kite_depth:
        for side, qty_key, orders_key in (
            ("buy", "buy_qty", "buy_orders"),
            ("sell", "sell_qty", "sell_orders"),
        ):
            for level in kite_depth.get(side) or []:
                price = level.get("price")
                quantity = level.get("quantity")
                if not price or not quantity:
                    continue
                entry = slot(price)
                if not entry[qty_key]:
                    entry[qty_key] = quantity
                entry[orders_key] = level.get("orders")

    return sorted(merged.values(), key=lambda e: e["price"], reverse=True)


@cas_tracker_bp.route("/api/orderbook_at")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_orderbook_at():
    """Return one stock's full CAS order book at (or just before) a date/time.

    Sourced from NSE's own `orderBook` array in the CAS payload rather than
    Kite's `depth` field. NSE publishes up to 8 price levels (vs Kite's 5)
    and, crucially, also gives the numbers needed to read those levels
    correctly: the ±3% price band, the indicative equilibrium price, the
    unpriced at-auction quantities, and the *whole* book's buy/sell totals.
    Without those, the visible levels are badly misleading — they sum to a
    small fraction of the real book (see the `book_total_*` vs per-level
    quantities for any liquid stock mid-auction).

    Kite's 5-level depth for the same instant is returned alongside as
    `kite_depth` for cross-checking, but is not the primary source.
    """
    date = request.args.get("date")
    at_time = request.args.get("time")
    symbol = request.args.get("symbol")
    if not date or not at_time or not symbol:
        return jsonify(
            {"error": "Required query params: 'date' (YYYY-MM-DD), 'time' (HH:MM:SS), 'symbol'"}
        ), 400

    def _build() -> tuple[dict | None, str | None]:
        """Returns (payload, error_message) — exactly one is not None."""
        conn = get_db()
        snapshot = get_snapshot_near(conn, date, at_time)
        depth_snapshot = get_depth_snapshot_near(conn, date, at_time)
        conn.close()

        if snapshot is None:
            return None, f"No CAS data recorded for {date}"

        row = next(
            (r for r in snapshot["data"].get("data", []) if r.get("symbol") == symbol),
            None,
        )
        if row is None:
            return None, f"No CAS row for {symbol} in the snapshot nearest {at_time}"

        kite_depth = None
        if depth_snapshot is not None:
            kite_depth = depth_snapshot["depth"].get(symbol)

        payload = {
            "date": date,
            "requested_time": at_time,
            "timestamp": snapshot["timestamp"],
            "symbol": symbol,
            # Union of NSE's ladder and Kite's depth — see _merge_order_book.
            "ladder": _merge_order_book(row.get("orderBook"), kite_depth),
            # NSE's raw ladder, kept for reference/debugging.
            "order_book": row.get("orderBook") or [],
            # Price discovery state
            "iep": row.get("IEP"),
            "reference_price": row.get("refrencePrice"),
            "prev_close": row.get("prevClose"),
            "upper_band": row.get("upperBand"),
            "lower_band": row.get("lowerBand"),
            # Quantities. matched_qty is the volume that would clear at the
            # IEP right now — NOT the size of the book, which is
            # book_total_buy / book_total_sell.
            "matched_qty": row.get("totTradedQty"),
            "final_qty": row.get("finalQuantity"),
            "final_price": row.get("finalPrice"),
            "ato_buy_qty": row.get("atoBuyQuantity"),
            "ato_sell_qty": row.get("atoSellQuantity"),
            "book_total_buy": row.get("totalBuyQuantity"),
            "book_total_sell": row.get("totalSellQuantity"),
            "last_update_time": row.get("lastUpdateTime"),
            "kite_depth": kite_depth,
        }
        return payload, None

    is_public = _is_public_request()
    cache_key = (date, at_time, symbol)
    payload, error = (
        _orderbook_at_cache.get_or_build(cache_key, _build) if is_public else _build()
    )

    if payload is None:
        return jsonify({"error": error}), 404

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_POINT_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/depth_dates")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_depth_dates():
    """Return dates for which bid/ask market-depth snapshots were recorded."""

    def _build() -> dict:
        conn = get_db()
        try:
            return {"dates": get_depth_distinct_dates(conn)}
        finally:
            conn.close()

    is_public = _is_public_request()
    payload = _depth_dates_cache.get_or_build(None, _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_DATES_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/depth_timestamps")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_depth_timestamps():
    """Return every recorded depth-snapshot timestamp for a date, ascending.

    Powers order_tracker.html's time selector — bound to exactly the
    seconds real data exists for (one entry per ~4-5s poll during the CAS
    window), so scrubbing through it can never land on an empty second.
    """
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "Missing required query param 'date' (YYYY-MM-DD)"}), 400

    def _build() -> dict:
        conn = get_db()
        try:
            return {"date": date, "timestamps": get_depth_timestamps_for_date(conn, date)}
        finally:
            conn.close()

    is_public = _is_public_request()
    payload = _depth_timestamps_cache.get_or_build(date, _build) if is_public else _build()

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_TIMESTAMPS_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/api/depth_at")
@limiter.limit(_PUBLIC_API_STANDARD_LIMIT)
def api_depth_at():
    """Return one stock's bid/ask depth at (or just before) a given date/time.

    Each stored depth row covers all 49 NIFTY50 constituents together
    (see save_depth_snapshot); this extracts just the requested symbol
    server-side so the response stays small regardless of how many
    constituents were captured that poll.
    """
    date = request.args.get("date")
    at_time = request.args.get("time")
    symbol = request.args.get("symbol")
    if not date or not at_time or not symbol:
        return jsonify(
            {"error": "Required query params: 'date' (YYYY-MM-DD), 'time' (HH:MM:SS), 'symbol'"}
        ), 400

    def _build() -> tuple[dict | None, str | None]:
        """Returns (payload, error_message) — exactly one is not None."""
        conn = get_db()
        snapshot = get_depth_snapshot_near(conn, date, at_time)
        conn.close()

        if snapshot is None:
            return None, f"No depth data recorded for {date}"

        depth = snapshot["depth"].get(symbol)
        if depth is None:
            return None, f"No depth data for {symbol} in the snapshot nearest {at_time}"

        payload = {
            "date": date,
            "requested_time": at_time,
            "timestamp": snapshot["timestamp"],
            "symbol": symbol,
            "buy": depth.get("buy", []),
            "sell": depth.get("sell", []),
        }
        return payload, None

    is_public = _is_public_request()
    cache_key = (date, at_time, symbol)
    payload, error = (
        _depth_at_cache.get_or_build(cache_key, _build) if is_public else _build()
    )

    if payload is None:
        return jsonify({"error": error}), 404

    response = jsonify(payload)
    if is_public:
        response.headers["Cache-Control"] = f"public, max-age={int(_PUBLIC_POINT_TTL_SECONDS)}"
    return response


@cas_tracker_bp.route("/restart", methods=["POST"])
def restart_scraper_endpoint():
    threading.Thread(target=_restart_scraper_internal).start()
    return "Restarting CAS tracker scraper... It should resume in a few seconds.", 200


