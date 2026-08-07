"""APScheduler-based notification scheduler.

Registers five cron jobs inside the Flask process:
  - _job_kite_login_check      → weekdays at 9:20 AM IST
  - _job_early_exit_required   → weekdays at 9:08 AM IST
  - _job_daily_pnl_summary     → weekdays at 3:35 PM IST
  - _job_scalping_auto_start   → weekdays at 10:00 AM IST (optional, paper mode)
  - _job_scalping_auto_stop    → weekdays at 3:45 PM IST  (optional)
  - _job_nifty_itm_loss_check  → every 15 min, market hours weekdays

Call ``init_scheduler()`` once from flask_app.py after all blueprints are
registered. The BackgroundScheduler runs as a daemon thread and stops when the
Flask process exits.
"""

import configparser
import importlib
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_BASE_DIR, "configfile.ini")
_DB_PATH = os.path.join(_BASE_DIR, "instruments.db")

_scheduler = None  # BackgroundScheduler instance, set by init_scheduler()
_scalping_auto_start_fn = None  # Callback injected by flask_app.py
_scalping_auto_stop_fn = None   # Callback injected by flask_app.py


# ---------------------------------------------------------------------------
# Scheduler initialisation
# ---------------------------------------------------------------------------


def init_scheduler(  # type: ignore[type-arg]
    flask_app,
    scalping_auto_start_fn=None,
    scalping_auto_stop_fn=None,
) -> None:
    """Create and start the BackgroundScheduler with cron jobs.

    Safe to call only once. Uses Asia/Kolkata timezone for all cron triggers.
    The scheduler runs as a daemon thread so it stops automatically when the
    Flask process exits.

    Args:
        flask_app: The Flask application instance (reserved for future use
            with app_context if needed).
        scalping_auto_start_fn: Optional zero-arg callable invoked at 10:00 AM
            IST on weekdays to start the scalping strategy in paper mode.
        scalping_auto_stop_fn: Optional zero-arg callable invoked at 3:45 PM
            IST on weekdays to stop the scalping strategy.
    """
    global _scheduler, _scalping_auto_start_fn, _scalping_auto_stop_fn
    _scalping_auto_start_fn = scalping_auto_start_fn
    _scalping_auto_stop_fn = scalping_auto_stop_fn

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
    except ImportError:
        logger.error(
            "APScheduler not installed — notification scheduler disabled. "
            "Run: pip install apscheduler"
        )
        return

    _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    _scheduler.add_job(
        func=_job_kite_login_check,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=20,
            timezone="Asia/Kolkata",
        ),
        id="kite_login_check",
        replace_existing=True,
        misfire_grace_time=600,  # fire up to 10 min late if process was down
    )

    _scheduler.add_job(
        func=_job_early_exit_required,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=8,
            timezone="Asia/Kolkata",
        ),
        id="early_exit_required",
        replace_existing=True,
        misfire_grace_time=600,
    )

    _scheduler.add_job(
        func=_job_daily_pnl_summary,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=35,
            timezone="Asia/Kolkata",
        ),
        id="daily_pnl_summary",
        replace_existing=True,
        misfire_grace_time=600,
    )

    if scalping_auto_start_fn is not None:
        _scheduler.add_job(
            func=_job_scalping_auto_start,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=10,
                minute=0,
                timezone="Asia/Kolkata",
            ),
            id="scalping_auto_start",
            replace_existing=True,
            misfire_grace_time=600,
        )

    if scalping_auto_stop_fn is not None:
        _scheduler.add_job(
            func=_job_scalping_auto_stop,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=15,
                minute=45,
                timezone="Asia/Kolkata",
            ),
            id="scalping_auto_stop",
            replace_existing=True,
            misfire_grace_time=600,
        )

    _scheduler.add_job(
        func=_job_reconcile_journal,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=40,
            timezone="Asia/Kolkata",
        ),
        id="reconcile_journal",
        replace_existing=True,
        misfire_grace_time=600,
    )

    _scheduler.add_job(
        func=_job_fetch_candles_eod,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=50,
            timezone="Asia/Kolkata",
        ),
        id="fetch_candles_eod",
        replace_existing=True,
        misfire_grace_time=600,
    )

    _scheduler.add_job(
        func=_job_purge_old_notifications,
        trigger=CronTrigger(
            hour=4,
            minute=0,
            timezone="Asia/Kolkata",
        ),
        id="daily_notification_purge",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    _scheduler.add_job(
        func=_job_nifty_margin_check,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=13,
            minute=0,
            timezone="Asia/Kolkata",
        ),
        id="nifty_margin_check",
        replace_existing=True,
        misfire_grace_time=600,
    )

    _scheduler.add_job(
        func=_job_nifty_delta_check,
        trigger=IntervalTrigger(minutes=15, timezone="Asia/Kolkata"),
        id="nifty_delta_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_nifty_itm_loss_check,
        trigger=IntervalTrigger(minutes=15, timezone="Asia/Kolkata"),
        id="nifty_itm_loss_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_gtt_monitor_check,
        trigger=IntervalTrigger(minutes=10, timezone="Asia/Kolkata"),
        id="gtt_monitor_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_duplicate_order_check,
        trigger=IntervalTrigger(minutes=5, timezone="Asia/Kolkata"),
        id="duplicate_order_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_copytrade_margin_check,
        trigger=IntervalTrigger(minutes=15, timezone="Asia/Kolkata"),
        id="copytrade_margin_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_position_guard_check,
        trigger=IntervalTrigger(minutes=5, timezone="Asia/Kolkata"),
        id="position_guard_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        func=_job_delta_live_snapshot,
        trigger=IntervalTrigger(minutes=1, timezone="Asia/Kolkata"),
        id="delta_live_snapshot",
        replace_existing=True,
        misfire_grace_time=60,
    )

    _scheduler.add_job(
        func=_job_nifty_zone_check,
        trigger=IntervalTrigger(minutes=5, timezone="Asia/Kolkata"),
        id="nifty_zone_check",
        replace_existing=True,
        misfire_grace_time=300,
    )

    job_count = 14 + (1 if scalping_auto_start_fn else 0) + (1 if scalping_auto_stop_fn else 0)
    _scheduler.start()
    logger.info(
        "Notification scheduler started — %d jobs registered "
        "(kite_login_check @ 9:20 AM, early_exit_required @ 9:08 AM, "
        "daily_pnl_summary @ 3:35 PM, reconcile_journal @ 3:40 PM, "
        "fetch_candles_eod @ 3:50 PM, nifty_margin_check @ 1:00 PM, "
        "nifty_delta_check @ every 15 min, gtt_monitor_check @ every 10 min, "
        "duplicate_order_check @ every 5 min, copytrade_margin_check @ every 15 min, "
        "position_guard_check @ every 5 min, delta_live_snapshot @ every 1 min, "
        "nifty_zone_check @ every 5 min, "
        "notification_purge @ 4:00 AM%s IST)",
        job_count,
        ", scalping_auto_start @ 10:00 AM, scalping_auto_stop @ 3:45 PM"
        if scalping_auto_start_fn and scalping_auto_stop_fn
        else "",
    )


def shutdown_scheduler() -> None:
    """Gracefully stop the scheduler (call from Flask teardown if needed)."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Notification scheduler stopped")


# ---------------------------------------------------------------------------
# Job: Kite login check (9:20 AM IST weekdays)
# ---------------------------------------------------------------------------


def _job_kite_login_check() -> None:
    """Check that the main Kite account has a valid session at 9:20 AM IST.

    Reads the stored access token from instruments.db and calls kite.profile()
    to verify the session is active. Dispatches a KITE_LOGIN_CHECK notification
    if the token is missing or the API call fails.

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running KITE_LOGIN_CHECK job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            _notify_login_missing("No session token found on server")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("KITE_LOGIN_CHECK: api_key not found in configfile.ini")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        kite.profile()  # raises KiteException if token is invalid or expired
        logger.info("KITE_LOGIN_CHECK: session valid at 9:20 AM IST")

    except Exception as exc:
        logger.warning("KITE_LOGIN_CHECK: session check failed — %s", exc)
        _notify_login_missing(str(exc))


def _notify_login_missing(reason: str) -> None:
    """Dispatch the KITE_LOGIN_CHECK notification.

    Args:
        reason: Human-readable explanation of why the check failed.
    """
    try:
        from notifications.service import dispatch
        dispatch(
            notification_type="KITE_LOGIN_CHECK",
            title="Kite Login Required",
            body=f"Main account session is not active at 9:20 AM. Reason: {reason}",
            metadata={"reason": reason},
        )
    except Exception as exc:
        logger.error("Failed to dispatch KITE_LOGIN_CHECK notification: %s", exc)


# ---------------------------------------------------------------------------
# Job: Early exit required (9:08 AM IST weekdays)
# ---------------------------------------------------------------------------


def _job_early_exit_required() -> None:
    """Check for expiry day with open short positions at 9:08 AM IST.

    1. Queries instruments.db to find NIFTY/SENSEX options expiring today.
    2. If expiry is today, fetches live positions from the Kite API.
    3. If any short positions exist, dispatches EARLY_EXIT_REQUIRED.

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running EARLY_EXIT_REQUIRED job")
    try:
        today_str = date.today().isoformat()

        if not _has_expiry_today(today_str):
            logger.info("EARLY_EXIT_REQUIRED: no NIFTY/SENSEX expiry today (%s)", today_str)
            return

        logger.info("EARLY_EXIT_REQUIRED: expiry detected for %s — checking positions", today_str)

        import instrument_cache
        from kiteconnect import KiteConnect

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("EARLY_EXIT_REQUIRED: no access token — cannot check positions")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("EARLY_EXIT_REQUIRED: api_key not found in configfile.ini")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        positions_data = kite.positions()
        net_positions: list[dict] = positions_data.get("net", [])

        short_positions = [p for p in net_positions if int(p.get("quantity", 0)) < 0]

        if not short_positions:
            logger.info(
                "EARLY_EXIT_REQUIRED: expiry today but no open short positions — no alert needed"
            )
            return

        symbols = ", ".join(p["tradingsymbol"] for p in short_positions[:5])
        extra = f" (+{len(short_positions) - 5} more)" if len(short_positions) > 5 else ""

        try:
            from notifications.service import dispatch
            dispatch(
                notification_type="EARLY_EXIT_REQUIRED",
                title="Early Exit Setup Needed",
                body=(
                    f"Today is expiry day ({today_str}) and you have "
                    f"{len(short_positions)} open short position(s): {symbols}{extra}. "
                    "Set up early exit GTT orders now."
                ),
                metadata={
                    "expiry_date": today_str,
                    "short_count": len(short_positions),
                    "symbols": [p["tradingsymbol"] for p in short_positions],
                },
            )
        except Exception as exc:
            logger.error("Failed to dispatch EARLY_EXIT_REQUIRED notification: %s", exc)

    except Exception as exc:
        logger.error("EARLY_EXIT_REQUIRED job failed unexpectedly: %s", exc)


def _has_expiry_today(today_str: str) -> bool:
    """Return True if any NIFTY/SENSEX options expire today.

    Args:
        today_str: ISO date string (YYYY-MM-DD) for today.

    Returns:
        True if at least one NFO-OPT or BFO-OPT instrument expires today.
    """
    try:
        with sqlite3.connect(_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*) FROM instruments
                WHERE expiry = ?
                  AND instrument_type IN ('CE', 'PE')
                  AND segment IN ('NFO-OPT', 'BFO-OPT')
                """,
                (today_str,),
            )
            count: int = cursor.fetchone()[0]
        return count > 0
    except sqlite3.Error as exc:
        logger.error("_has_expiry_today DB query failed: %s", exc)
        return False


def _read_api_key() -> str:
    """Read the Kite API key from configfile.ini.

    Returns:
        API key string, or empty string if not found.
    """
    cfg = configparser.ConfigParser()
    cfg.read(_CONFIG_PATH)
    return cfg.get("kite_login_details", "api_key", fallback="").strip()


# ---------------------------------------------------------------------------
# Job: Daily P&L summary (3:35 PM IST weekdays)
# ---------------------------------------------------------------------------


def _job_daily_pnl_summary() -> None:
    """Send a daily P&L summary to Telegram at 3:35 PM IST (after market close).

    Fetches live positions and today's orders from the Kite API to compile
    realized P&L, unrealized P&L, and executed order count. Dispatches a
    DAILY_PNL_SUMMARY notification.

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running DAILY_PNL_SUMMARY job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("DAILY_PNL_SUMMARY: no access token — skipping")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("DAILY_PNL_SUMMARY: api_key not found in configfile.ini")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        positions_data = kite.positions()
        net_positions: list[dict] = positions_data.get("net", [])
        day_positions: list[dict] = positions_data.get("day", [])

        realized_pnl: float = sum(float(p.get("realised", 0)) for p in net_positions)
        unrealized_pnl: float = sum(float(p.get("unrealised", 0)) for p in net_positions)
        day_pnl: float = sum(float(p.get("pnl", 0)) for p in day_positions)

        orders: list[dict] = kite.orders()
        executed_count: int = sum(1 for o in orders if o.get("status") == "COMPLETE")
        total_count: int = len(orders)

        today_str = date.today().strftime("%d %b %Y")
        body_lines = [
            f"Date: {today_str}",
            f"Realized P&L:   ₹{realized_pnl:+.2f}",
            f"Unrealized P&L: ₹{unrealized_pnl:+.2f}",
            f"Day P&L:        ₹{day_pnl:+.2f}",
            f"Orders:         {executed_count} executed / {total_count} total",
        ]

        try:
            from notifications.service import dispatch
            dispatch(
                notification_type="DAILY_PNL_SUMMARY",
                title="Daily P&L Summary",
                body="\n".join(body_lines),
                metadata={
                    "realized_pnl": realized_pnl,
                    "unrealized_pnl": unrealized_pnl,
                    "day_pnl": day_pnl,
                    "executed_orders": executed_count,
                    "total_orders": total_count,
                },
            )
        except Exception as exc:
            logger.error("Failed to dispatch DAILY_PNL_SUMMARY notification: %s", exc)

    except Exception as exc:
        logger.error("DAILY_PNL_SUMMARY job failed unexpectedly: %s", exc)


# ---------------------------------------------------------------------------
# Job: Scalping strategy auto-start (10:00 AM IST weekdays)
# ---------------------------------------------------------------------------


def _job_scalping_auto_start() -> None:
    """Auto-start the scalping strategy in paper trading mode at 10:00 AM IST.

    Delegates to the callback registered via ``init_scheduler()``.
    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running SCALPING_AUTO_START job")
    if _scalping_auto_start_fn is None:
        logger.warning("SCALPING_AUTO_START: no callback registered — skipping")
        return
    try:
        _scalping_auto_start_fn()
    except Exception as exc:
        logger.error("SCALPING_AUTO_START job failed: %s", exc)


# ---------------------------------------------------------------------------
# Job: Scalping strategy auto-stop (3:45 PM IST weekdays)
# ---------------------------------------------------------------------------


def _job_scalping_auto_stop() -> None:
    """Auto-stop the scalping strategy at 3:45 PM IST (after force-exit window).

    Delegates to the callback registered via ``init_scheduler()``.
    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running SCALPING_AUTO_STOP job")
    if _scalping_auto_stop_fn is None:
        logger.warning("SCALPING_AUTO_STOP: no callback registered — skipping")
        return
    try:
        _scalping_auto_stop_fn()
    except Exception as exc:
        logger.error("SCALPING_AUTO_STOP job failed: %s", exc)


# ---------------------------------------------------------------------------
# Job: EOD trade journal reconciliation (3:40 PM IST weekdays)
# ---------------------------------------------------------------------------


def _job_reconcile_journal() -> None:
    """Reconcile today's trade journal against Zerodha's trade API at 3:40 PM IST.

    Fetches kite.trades() for today's session and inserts any fills that are
    missing from the local executed_orders JSON (e.g., trades placed manually
    on Zerodha outside the trading system). Missing trades are tagged as
    algo_source='Manual'.

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running RECONCILE_JOURNAL job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect
        from trade_journal import reconcile_with_zerodha

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("RECONCILE_JOURNAL: no access token — skipping")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("RECONCILE_JOURNAL: api_key not found in configfile.ini")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        result = reconcile_with_zerodha(kite, date.today())
        logger.info(
            "RECONCILE_JOURNAL: added=%d skipped=%d partial_skipped=%d errors=%d",
            result["added"], result["skipped"],
            result.get("partial_skipped", 0), len(result["errors"]),
        )
        if result["errors"]:
            for err in result["errors"]:
                logger.warning("RECONCILE_JOURNAL error: %s", err)

        # After reconcile, rebuild cache so unpaired query reflects new data
        from trade_journal_db import force_rebuild
        force_rebuild()

        # Validate trade journal positions against Zerodha actuals
        from trade_journal import validate_positions_vs_zerodha
        validation = validate_positions_vs_zerodha(kite, date.today())
        logger.info(
            "POSITION_VALIDATION: matched=%d discrepancies=%d phantoms=%d",
            len(validation.get("matched", [])),
            len(validation.get("discrepancies", [])),
            len(validation.get("phantoms", [])),
        )
        if validation.get("discrepancies"):
            for d in validation["discrepancies"]:
                logger.warning(
                    "POSITION_VALIDATION discrepancy: %s journal=%s zerodha=%s avg_price=%.2f",
                    d["symbol"], d["journal_qty"], d["zerodha_qty"], d["zerodha_avg_price"],
                )
        if validation.get("phantoms"):
            for p in validation["phantoms"]:
                logger.warning(
                    "POSITION_VALIDATION phantom: %s journal_qty=%s",
                    p["symbol"], p["journal_qty"],
                )

    except Exception as exc:
        logger.error("RECONCILE_JOURNAL job failed unexpectedly: %s", exc)


# ---------------------------------------------------------------------------
# Job: EOD candle fetch for backtesting DB (3:50 PM IST weekdays)
# ---------------------------------------------------------------------------


def _job_fetch_candles_eod() -> None:
    """Fetch and store today's 1-min OHLCV+OI candles at 3:50 PM IST.

    Populates bt_index_candles_1m and bt_option_candles_1m (and
    bt_instrument_master) using the Zerodha KiteConnect historical data
    API. Covers NIFTY and SENSEX, ATM ± 10 strikes, all active expiries.

    Requires a valid Kite access token stored in instruments.db (set
    automatically when the user logs in via the web dashboard).

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running FETCH_CANDLES_EOD job")
    try:
        # Optional integration: the backtesting package is not part of the
        # open-source distribution — resolved dynamically so its absence
        # simply skips the job.
        try:
            _fetcher_module = importlib.import_module("backtesting.zerodha_candle_fetcher")
        except ImportError:
            logger.info("FETCH_CANDLES_EOD: backtesting package not installed — skipping")
            return
        ZerodhaCandleFetcher = _fetcher_module.ZerodhaCandleFetcher

        result = ZerodhaCandleFetcher().run_daily_fetch(
            underlyings=["NIFTY", "SENSEX", "BANKNIFTY"],
            n_strikes_each_side=10,
        )
        logger.info("FETCH_CANDLES_EOD: complete — %s", result)
    except Exception as exc:
        logger.error("FETCH_CANDLES_EOD job failed unexpectedly: %s", exc, exc_info=True)


def _job_purge_old_notifications() -> None:
    """Delete completed and ignored notifications older than 30 days.

    Runs daily at 4:00 AM IST (any day of week). Active notifications are
    never deleted. Never raises — exceptions are caught and logged.
    """
    logger.info("Running DAILY_NOTIFICATION_PURGE job")
    try:
        from notifications import database
        deleted_count = database.purge_old_non_active_notifications(days=30)
        logger.info("DAILY_NOTIFICATION_PURGE: removed %d old notifications", deleted_count)
    except Exception as exc:
        logger.error("DAILY_NOTIFICATION_PURGE job failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Helper: trading-day counter
# ---------------------------------------------------------------------------


def _count_trading_days(from_date: date, to_date: date) -> int:
    """Count weekday (Mon–Fri) trading days from from_date (exclusive) to to_date (inclusive).

    Args:
        from_date: Start date (not counted).
        to_date: End date (counted if it is a weekday).

    Returns:
        Number of weekdays strictly between from_date and to_date (inclusive of to_date).
    """
    count = 0
    d = from_date + timedelta(days=1)
    while d <= to_date:
        if d.weekday() < 5:  # Mon=0 … Fri=4
            count += 1
        d += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# Job: NIFTY next-expiry margin check (1:00 PM IST weekdays)
# ---------------------------------------------------------------------------


def _job_nifty_margin_check() -> None:
    """Check if NIFTY next-expiry margin deployed is sufficient.

    Fires at 1:00 PM IST on D-5, D-4, and D-3 trading days before the next
    NIFTY weekly expiry (Tuesday). Dispatches a NIFTY_MARGIN_ALERT warning if
    the internal margin estimate for next-expiry positions falls below the
    day-specific threshold:
      D-5 (Tue prior week) → < ₹90 L (9,000,000)
      D-4 (Wed)            → < ₹1 Cr (10,000,000)
      D-3 (Thu)            → < ₹1.2 Cr (12,000,000)

    Never raises — all exceptions are caught and logged.
    """
    logger.info("Running NIFTY_MARGIN_CHECK job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect
        from positions_lib import (
            get_all_nifty_instruments,
            get_next_expiry_date,
            get_nifty_positions_summary,
        )

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("NIFTY_MARGIN_CHECK: no access token — skipping")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("NIFTY_MARGIN_CHECK: api_key missing in configfile.ini — skipping")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        # Determine next NIFTY expiry and how many trading days away it is
        all_nifty_instruments = get_all_nifty_instruments(kite)
        next_expiry_str: Optional[str] = get_next_expiry_date(all_nifty_instruments)
        if not next_expiry_str:
            logger.warning("NIFTY_MARGIN_CHECK: could not determine next NIFTY expiry — skipping")
            return

        today = date.today()
        next_expiry = date.fromisoformat(next_expiry_str)
        trading_days: int = _count_trading_days(today, next_expiry)

        threshold_map: dict[int, int] = {5: 9_000_000, 4: 10_000_000, 3: 12_000_000}
        if trading_days not in threshold_map:
            logger.info(
                "NIFTY_MARGIN_CHECK: D-%d is not a check day (only D-3/4/5) — skipping",
                trading_days,
            )
            return
        min_margin: int = threshold_map[trading_days]

        # Fetch margin + CE/PE quantities + delta for next expiry only
        summary = get_nifty_positions_summary(kite, restrict_to_next_expiry=True, prefetched_instruments=all_nifty_instruments)
        margin_utilised: float = float(summary.get("margin_required", 0))
        delta: float = float(summary.get("total_delta", 0))
        calls_sold: int = abs(int(summary.get("ce_sold", 0)))
        puts_sold: int = abs(int(summary.get("pe_sold", 0)))

        if margin_utilised >= min_margin:
            logger.info(
                "NIFTY_MARGIN_CHECK: margin Rs.%.0f >= Rs.%.0f (D-%d) — OK",
                margin_utilised,
                min_margin,
                trading_days,
            )
            return

        logger.warning(
            "NIFTY_MARGIN_CHECK: margin Rs.%.0f < Rs.%.0f (D-%d) — dispatching alert",
            margin_utilised,
            min_margin,
            trading_days,
        )

        body_lines = [
            f"Days to Expiry:    {trading_days} trading days ({next_expiry_str})",
            f"Margin Utilised:   Rs.{margin_utilised:,.0f}",
            f"Min Required:      Rs.{min_margin:,.0f}",
            f"Puts Sold:         {puts_sold}",
            f"Calls Sold:        {calls_sold}",
            f"Next Expiry Delta: {delta:.2f}",
        ]

        try:
            from notifications.service import dispatch
            dispatch(
                notification_type="NIFTY_MARGIN_ALERT",
                title=f"Insufficient Funds Deployed for Next Expiry (D-{trading_days})",
                body="\n".join(body_lines),
                metadata={
                    "days_to_expiry": trading_days,
                    "next_expiry": next_expiry_str,
                    "margin_utilised": margin_utilised,
                    "min_margin": min_margin,
                    "puts_sold": puts_sold,
                    "calls_sold": calls_sold,
                    "delta": delta,
                },
            )
        except Exception as exc:
            logger.error("Failed to dispatch NIFTY_MARGIN_ALERT notification: %s", exc)

    except Exception as exc:
        logger.error("NIFTY_MARGIN_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: NIFTY next-expiry delta out-of-bounds check (every 15 min, market hours)
# ---------------------------------------------------------------------------


def _job_nifty_delta_check() -> None:
    """Scan NIFTY next-expiry delta every 15 min during market hours on D, D-1, D-2.

    Calendar mapping (NIFTY expiry = Tuesday):
        D-2 (Friday)  → trading_days=2, threshold ±3,000
        D-1 (Monday)  → trading_days=1, threshold ±4,500
        D   (Tuesday) → trading_days=0, threshold ±5,500

    A per-D-level 1-hour cooldown is stored in the alert_cooldowns SQLite table so
    repeated alerts within the same hour are suppressed across process restarts.
    Available margin is fetched live from kite.margins() (non-fatal on failure).
    Never raises — all exceptions are caught and logged.
    """
    # Gate: market hours 9:15 – 15:30 IST only
    now_ist = datetime.now(_IST)
    market_open  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open <= now_ist <= market_close):
        logger.debug("NIFTY_DELTA_CHECK: outside market hours — skipping")
        return

    # Gate: weekdays only (IntervalTrigger fires on weekends too)
    if now_ist.weekday() >= 5:
        logger.debug("NIFTY_DELTA_CHECK: weekend — skipping")
        return

    logger.info("Running NIFTY_DELTA_CHECK job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect
        from positions_lib import (
            get_all_nifty_instruments,
            get_next_expiry_date,
            get_nifty_positions_summary,
        )

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("NIFTY_DELTA_CHECK: no access token — skipping")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("NIFTY_DELTA_CHECK: api_key missing in configfile.ini — skipping")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        all_nifty_instruments = get_all_nifty_instruments(kite)
        next_expiry_str: Optional[str] = get_next_expiry_date(all_nifty_instruments)
        if not next_expiry_str:
            logger.warning("NIFTY_DELTA_CHECK: could not determine next NIFTY expiry — skipping")
            return

        today = date.today()
        next_expiry = date.fromisoformat(next_expiry_str)
        trading_days: int = _count_trading_days(today, next_expiry)

        threshold_map: dict[int, int] = {2: 3000, 1: 4500, 0: 5500}
        if trading_days not in threshold_map:
            logger.info(
                "NIFTY_DELTA_CHECK: D-%d is not a check day (only D-0/1/2) — skipping",
                trading_days,
            )
            return
        threshold: int = threshold_map[trading_days]

        summary = get_nifty_positions_summary(kite, restrict_to_next_expiry=True, prefetched_instruments=all_nifty_instruments)
        delta: float = float(summary.get("total_delta", 0))
        margin_utilised: float = float(summary.get("margin_required", 0))
        calls_sold: int = abs(int(summary.get("ce_sold", 0)))
        puts_sold: int = abs(int(summary.get("pe_sold", 0)))

        if abs(delta) <= threshold:
            logger.info(
                "NIFTY_DELTA_CHECK: delta=%.1f within ±%d (D-%d) — OK",
                delta, threshold, trading_days,
            )
            return

        # Delta out of bounds — enforce 1-hour cooldown per D-level
        from notifications import database as notif_db
        cooldown_key = f"NIFTY_DELTA_ALERT_D{trading_days}"
        if not notif_db.check_and_update_cooldown(cooldown_key, cooldown_seconds=3600):
            logger.info(
                "NIFTY_DELTA_CHECK: delta=%.1f out of bounds but cooldown active — suppressed",
                delta,
            )
            return

        # Fetch available margin from Kite equity segment (non-fatal on failure)
        margin_available: float = 0.0
        try:
            margins_resp = kite.margins()
            equity = margins_resp.get("equity", {})
            margin_available = float(equity.get("available", {}).get("live_balance", 0))
        except Exception as exc:
            logger.warning("NIFTY_DELTA_CHECK: kite.margins() failed: %s", exc)

        day_label = "Expiry Day" if trading_days == 0 else f"D-{trading_days}"
        title = (
            f"Expiry Day Delta Out of Bound (> {threshold})"
            if trading_days == 0
            else f"D-{trading_days} Delta Out of Bound (> {threshold})"
        )
        body_lines = [
            f"Day:                 {day_label} ({next_expiry_str})",
            f"Delta (Next Expiry): {delta:.1f}  [Threshold: ±{threshold}]",
            f"Margin Utilised:     Rs.{margin_utilised:,.0f}",
            f"Margin Available:    Rs.{margin_available:,.0f}",
            f"Puts Sold:           {puts_sold}",
            f"Calls Sold:          {calls_sold}",
        ]

        logger.warning(
            "NIFTY_DELTA_CHECK: delta=%.1f exceeds ±%d on %s — dispatching alert",
            delta, threshold, day_label,
        )

        try:
            from notifications.service import dispatch
            dispatch(
                notification_type="NIFTY_DELTA_ALERT",
                title=title,
                body="\n".join(body_lines),
                metadata={
                    "days_to_expiry": trading_days,
                    "next_expiry": next_expiry_str,
                    "delta": delta,
                    "threshold": threshold,
                    "margin_utilised": margin_utilised,
                    "margin_available": margin_available,
                    "puts_sold": puts_sold,
                    "calls_sold": calls_sold,
                },
            )
        except Exception as exc:
            logger.error("Failed to dispatch NIFTY_DELTA_ALERT: %s", exc)

    except Exception as exc:
        logger.error("NIFTY_DELTA_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: NIFTY next-expiry ITM-loss-exceeds-offset check (every 15 min, market hours)
# ---------------------------------------------------------------------------

# Net P&L at/below this level (Rs.) is treated as a critical breach: no cooldown,
# so the alert re-fires on every 15-min check until the position is fixed.
_ITM_LOSS_CRITICAL_THRESHOLD = -100_000.0
_ITM_LOSS_MILD_COOLDOWN_SECONDS = 3600


def _job_nifty_itm_loss_check() -> None:
    """Scan NIFTY next-expiry Net P&L (PE SOLD/CE SOLD scenarios) every 15 min.

    For each scenario, the ITM (offsetting) side's loss is compared against the
    OTM (sold) side's time value. If the ITM loss exceeds the time value
    collected, Net P&L goes negative:
      - Net P&L < 0: notify at most once/hour (per scenario) via a durable
        SQLite cooldown, so repeated alerts within the hour are suppressed
        across process restarts.
      - Net P&L <= -Rs.1,00,000: notify on every 15-min check (no cooldown).

    Never raises — all exceptions are caught and logged.
    """
    now_ist = datetime.now(_IST)
    market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open <= now_ist <= market_close):
        logger.debug("NIFTY_ITM_LOSS_CHECK: outside market hours — skipping")
        return

    if now_ist.weekday() >= 5:
        logger.debug("NIFTY_ITM_LOSS_CHECK: weekend — skipping")
        return

    logger.info("Running NIFTY_ITM_LOSS_CHECK job")
    try:
        import instrument_cache
        from kiteconnect import KiteConnect
        from positions_lib import (
            get_all_nifty_instruments,
            get_next_expiry_date,
            get_nifty_positions_summary,
        )

        access_token: Optional[str] = instrument_cache.get_kite_token()
        if not access_token:
            logger.warning("NIFTY_ITM_LOSS_CHECK: no access token — skipping")
            return

        api_key = _read_api_key()
        if not api_key:
            logger.error("NIFTY_ITM_LOSS_CHECK: api_key missing in configfile.ini — skipping")
            return

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        all_nifty_instruments = get_all_nifty_instruments(kite)
        next_expiry_str: Optional[str] = get_next_expiry_date(all_nifty_instruments)
        if not next_expiry_str:
            logger.warning("NIFTY_ITM_LOSS_CHECK: could not determine next NIFTY expiry — skipping")
            return

        summary = get_nifty_positions_summary(
            kite, restrict_to_next_expiry=True, prefetched_instruments=all_nifty_instruments
        )

        scenarios = [
            (
                "PE_SOLD",
                "PE SOLD — CE OFFSET",
                float(summary.get("pe_net", 0.0)),
                float(summary.get("pe_time_value", 0.0)),
                float(summary.get("ce_itm_loss", 0.0)),
            ),
            (
                "CE_SOLD",
                "CE SOLD — PE OFFSET",
                float(summary.get("ce_net", 0.0)),
                float(summary.get("ce_time_value", 0.0)),
                float(summary.get("pe_itm_loss", 0.0)),
            ),
        ]

        from notifications import database as notif_db
        from notifications.service import dispatch

        for scenario_key, scenario_label, net_pnl, time_value, itm_loss in scenarios:
            if net_pnl >= 0:
                logger.debug(
                    "NIFTY_ITM_LOSS_CHECK: %s Net P&L=%.2f is non-negative — OK",
                    scenario_label, net_pnl,
                )
                continue

            is_critical = net_pnl <= _ITM_LOSS_CRITICAL_THRESHOLD
            cooldown_seconds = 0 if is_critical else _ITM_LOSS_MILD_COOLDOWN_SECONDS
            cooldown_key = f"NIFTY_ITM_LOSS_ALERT_{scenario_key}"

            if not notif_db.check_and_update_cooldown(cooldown_key, cooldown_seconds=cooldown_seconds):
                logger.info(
                    "NIFTY_ITM_LOSS_CHECK: %s Net P&L=%.2f negative but cooldown active — suppressed",
                    scenario_label, net_pnl,
                )
                continue

            title = (
                f"{scenario_label} Net P&L Negative"
                + (" (Critical)" if is_critical else "")
            )
            body_lines = [
                f"Scenario:       {scenario_label}",
                f"Next Expiry:    {next_expiry_str}",
                f"Time Value:     Rs.{time_value:,.0f}",
                f"ITM Loss:       Rs.{itm_loss:,.0f}",
                f"Net P&L:        Rs.{net_pnl:,.0f}",
            ]

            logger.warning(
                "NIFTY_ITM_LOSS_CHECK: %s Net P&L=%.2f (critical=%s) — dispatching alert",
                scenario_label, net_pnl, is_critical,
            )

            try:
                dispatch(
                    notification_type="NIFTY_ITM_LOSS_ALERT",
                    title=title,
                    body="\n".join(body_lines),
                    metadata={
                        "scenario": scenario_key,
                        "next_expiry": next_expiry_str,
                        "net_pnl": net_pnl,
                        "time_value": time_value,
                        "itm_loss": itm_loss,
                        "is_critical": is_critical,
                    },
                )
            except Exception as exc:
                logger.error("Failed to dispatch NIFTY_ITM_LOSS_ALERT for %s: %s", scenario_label, exc)

    except Exception as exc:
        logger.error("NIFTY_ITM_LOSS_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: Duplicate order check (every 5 min, market hours weekdays)
# ---------------------------------------------------------------------------


def _job_duplicate_order_check() -> None:
    """Delegate to duplicate_order_monitor.check_and_notify_duplicates().

    The market-hours guard and all error handling live in the monitor module
    so the logic stays testable independent of APScheduler.
    Never raises.
    """
    try:
        from duplicate_order_monitor import check_and_notify_duplicates

        check_and_notify_duplicates()
    except Exception as exc:
        logger.error("DUPLICATE_ORDER_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: Position Guard long-exposure check (every 5 min, market hours weekdays)
# ---------------------------------------------------------------------------


def _job_position_guard_check() -> None:
    """Delegate to position_guard.detector.check_and_notify_position_guard().

    The market-hours guard and all error handling live in the detector module
    so the logic stays testable independent of APScheduler.
    Never raises.
    """
    try:
        from position_guard.detector import check_and_notify_position_guard

        check_and_notify_position_guard()
    except Exception as exc:
        logger.error("POSITION_GUARD_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: NIFTY confluence zone enter/exit check (every 5 min, market hours weekdays)
# ---------------------------------------------------------------------------


def _job_nifty_zone_check() -> None:
    """Delegate to swing_levels.zone_alert_monitor.check_nifty_zone_transitions().

    The market-hours guard and all error handling live in the monitor module
    so the logic stays testable independent of APScheduler.
    Never raises.
    """
    try:
        from swing_levels.zone_alert_monitor import check_nifty_zone_transitions

        check_nifty_zone_transitions()
    except Exception as exc:
        logger.error("NIFTY_ZONE_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: GTT vs Position mismatch check (every 10 min, market hours weekdays)
# ---------------------------------------------------------------------------


def _job_gtt_monitor_check() -> None:
    """Delegate to gtt_monitor.run_gtt_monitor_check().

    The market-hours guard and all error handling live in gtt_monitor so
    the logic stays testable independent of APScheduler.
    Never raises.
    """
    try:
        from gtt_monitor import run_gtt_monitor_check

        run_gtt_monitor_check()
    except Exception as exc:
        logger.error("GTT_MONITOR_CHECK job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: Delta Limits live-delta snapshot (every 1 min, market hours only)
# ---------------------------------------------------------------------------


def _job_delta_live_snapshot() -> None:
    """Delegate to delta_live_tracker.compute_live_delta_snapshot().

    The market-hours guard and all error handling live in delta_live_tracker
    so the logic stays testable independent of APScheduler.
    Never raises.
    """
    try:
        from delta_live_tracker import compute_live_delta_snapshot

        compute_live_delta_snapshot()
    except Exception as exc:
        logger.error("DELTA_LIVE_SNAPSHOT job failed unexpectedly: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Job: CopyTrade low available margin check (every 15 min, market hours weekdays)
# ---------------------------------------------------------------------------

_COPY_MARGIN_LOW_THRESHOLD_PCT: float = 10.0
_COPY_MARGIN_ALERT_COOLDOWN_SECONDS: int = 3600


def _read_server_base_url() -> str:
    """Read the dashboard's public base URL for links inside notifications.

    Configured via ``[notifications] server_base_url`` in configfile.ini;
    defaults to the local listen address when unset.

    Returns:
        Base URL string without a trailing slash.
    """
    parser = configparser.ConfigParser()
    parser.read(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "configfile.ini",
        )
    )
    return parser.get(
        "notifications", "server_base_url", fallback="http://127.0.0.1:5010"
    ).rstrip("/")


_SERVER_BASE_URL: str = _read_server_base_url()


def _job_copytrade_margin_check() -> None:
    """Alert when any copytrade account's available margin drops below 10% of total.

    Total = Used + Available from the Kite equity margin segment. Covers the
    main account and every active real copy account. Runs every 15 minutes
    during market hours (9:15-15:30 IST, weekdays). Never raises.
    """
    now_ist = datetime.now(_IST)
    market_open  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open <= now_ist <= market_close):
        logger.debug("COPY_MARGIN_CHECK: outside market hours — skipping")
        return
    if now_ist.weekday() >= 5:
        logger.debug("COPY_MARGIN_CHECK: weekend — skipping")
        return

    logger.info("Running COPY_MARGIN_CHECK job")
    try:
        check_copytrade_margins()
    except Exception as exc:
        logger.error("COPY_MARGIN_CHECK job failed unexpectedly: %s", exc, exc_info=True)


def check_copytrade_margins() -> None:
    """Fetch per-account margins and dispatch COPY_MARGIN_LOW alerts.

    For each account (main + active real copy accounts) with
    available / (available + used) below _COPY_MARGIN_LOW_THRESHOLD_PCT,
    dispatches a COPY_MARGIN_LOW notification (Telegram + web push + in-app)
    linking to that account's free-margin page. A per-account cooldown in the
    alert_cooldowns table suppresses repeats within
    _COPY_MARGIN_ALERT_COOLDOWN_SECONDS.

    Raises:
        Exception: Propagates unexpected errors to the caller (the scheduler
            job wrapper catches and logs them).
    """
    # Optional integration: copytrade is not part of the open-source
    # distribution — resolved dynamically so its absence skips the check.
    try:
        _copytrade_blueprint = importlib.import_module("copytrade.blueprint")
    except ImportError:
        logger.info("COPY_MARGIN_CHECK: copytrade package not installed — skipping")
        return
    get_all_account_margin_details = _copytrade_blueprint.get_all_account_margin_details
    from notifications import database as notif_db
    from notifications.service import dispatch

    margin_details = get_all_account_margin_details()
    for account_key, margin in margin_details.items():
        if margin is None:
            logger.warning(
                "COPY_MARGIN_CHECK: no margin data for account %s (token expired or "
                "fetch failed) — skipping",
                account_key,
            )
            continue

        available = float(margin["available"])
        used = float(margin["used"])
        total = available + used
        if total <= 0:
            logger.debug(
                "COPY_MARGIN_CHECK: account %s has zero total margin — skipping",
                account_key,
            )
            continue

        available_pct = available / total * 100.0
        if available_pct >= _COPY_MARGIN_LOW_THRESHOLD_PCT:
            logger.info(
                "COPY_MARGIN_CHECK: account %s at %.1f%% available — OK",
                account_key,
                available_pct,
            )
            continue

        cooldown_key = f"COPY_MARGIN_LOW_{account_key}"
        if not notif_db.check_and_update_cooldown(
            cooldown_key, cooldown_seconds=_COPY_MARGIN_ALERT_COOLDOWN_SECONDS
        ):
            logger.info(
                "COPY_MARGIN_CHECK: account %s low (%.1f%%) but cooldown active — suppressed",
                account_key,
                available_pct,
            )
            continue

        account_label = str(margin.get("label") or account_key)
        free_margin_path = f"/copytrade/accounts/{account_key}/free-margin"
        body_lines = [
            f"Account:           {account_label}",
            f"Available Margin:  Rs.{available:,.0f} ({available_pct:.1f}% of total)",
            f"Used Margin:       Rs.{used:,.0f}",
            f"Total Margin:      Rs.{total:,.0f}",
            "",
            f"👉 {_SERVER_BASE_URL}{free_margin_path}",
        ]

        logger.warning(
            "COPY_MARGIN_CHECK: account %s available margin %.1f%% < %.0f%% — dispatching alert",
            account_key,
            available_pct,
            _COPY_MARGIN_LOW_THRESHOLD_PCT,
        )
        dispatch(
            notification_type="COPY_MARGIN_LOW",
            title=f"Low Margin: {account_label} at {available_pct:.1f}%",
            body="\n".join(body_lines),
            metadata={
                "account": account_key,
                "available": available,
                "used": used,
                "total": total,
                "available_pct": round(available_pct, 2),
                "action_url": free_margin_path,
                "action_label": "Free Margin",
            },
        )
