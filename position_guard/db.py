"""SQLite persistence for Position Guard's per-symbol ignore list.

Tracks symbols the user has reviewed and dismissed from the Position Guard
dashboard. An ignore is scoped to the exact set of orders/GTTs/position that
contributed to the symbol's long exposure at the time it was ignored (via a
fingerprint) — if that set changes at all, the ignore is treated as stale and
the symbol reappears automatically.

DB file: position_guard.db (created on first use).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "position_guard.db")

_DDL = """
CREATE TABLE IF NOT EXISTS ignored_symbols (
    tradingsymbol TEXT NOT NULL,
    exchange      TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    ignored_at    TEXT NOT NULL,
    PRIMARY KEY (tradingsymbol, exchange)
);
"""


def _get_conn() -> sqlite3.Connection:
    """Return a WAL-mode SQLite connection with dict-like row access."""
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    """Create the ignored_symbols table if it does not exist."""
    conn = _get_conn()
    try:
        conn.executescript(_DDL)
        conn.commit()
    except sqlite3.Error:
        logger.exception("position_guard_db.init_db failed")
    finally:
        conn.close()


def ignore_symbol(tradingsymbol: str, exchange: str, fingerprint: str) -> None:
    """Mark a symbol as ignored for its current fingerprint.

    Args:
        tradingsymbol: The Kite trading symbol to ignore.
        exchange: The exchange the symbol trades on (e.g. "NFO").
        fingerprint: The fingerprint of the exposure sources being dismissed,
            from ``position_guard._fingerprint``.
    """
    from common_lib import get_ist_now

    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO ignored_symbols (tradingsymbol, exchange, fingerprint, ignored_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tradingsymbol, exchange)
            DO UPDATE SET fingerprint = excluded.fingerprint, ignored_at = excluded.ignored_at
            """,
            (tradingsymbol, exchange, fingerprint, get_ist_now().isoformat()),
        )
        conn.commit()
        logger.info("position_guard_db: ignored %s (%s)", tradingsymbol, exchange)
    except sqlite3.Error:
        logger.exception("position_guard_db.ignore_symbol failed for %s", tradingsymbol)
    finally:
        conn.close()


def unignore_symbol(tradingsymbol: str, exchange: str) -> None:
    """Remove an ignore row, making the symbol reappear in the main table.

    Args:
        tradingsymbol: The Kite trading symbol to un-ignore.
        exchange: The exchange the symbol trades on.
    """
    conn = _get_conn()
    try:
        conn.execute(
            "DELETE FROM ignored_symbols WHERE tradingsymbol = ? AND exchange = ?",
            (tradingsymbol, exchange),
        )
        conn.commit()
        logger.info("position_guard_db: un-ignored %s (%s)", tradingsymbol, exchange)
    except sqlite3.Error:
        logger.exception("position_guard_db.unignore_symbol failed for %s", tradingsymbol)
    finally:
        conn.close()


def is_ignored(tradingsymbol: str, exchange: str, fingerprint: str) -> bool:
    """Check whether a symbol is currently ignored for the given fingerprint.

    If an ignore row exists but its stored fingerprint no longer matches the
    live one (the underlying orders/GTTs/position changed), the stale row is
    deleted and this returns False.

    Args:
        tradingsymbol: The Kite trading symbol to check.
        exchange: The exchange the symbol trades on.
        fingerprint: The current live fingerprint for this symbol.

    Returns:
        True if the symbol is ignored and its fingerprint still matches.
    """
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT fingerprint FROM ignored_symbols WHERE tradingsymbol = ? AND exchange = ?",
            (tradingsymbol, exchange),
        ).fetchone()
        if row is None:
            return False
        if row["fingerprint"] != fingerprint:
            conn.execute(
                "DELETE FROM ignored_symbols WHERE tradingsymbol = ? AND exchange = ?",
                (tradingsymbol, exchange),
            )
            conn.commit()
            logger.info(
                "position_guard_db: stale ignore for %s (%s) auto-expired", tradingsymbol, exchange
            )
            return False
        return True
    except sqlite3.Error:
        logger.exception("position_guard_db.is_ignored failed for %s", tradingsymbol)
        return False
    finally:
        conn.close()


def cleanup_ignored_symbols_not_in_instruments() -> int:
    """Remove ignore rows for symbols no longer present in the instrument cache.

    Called after each instrument sync so ignores for expired contracts don't
    linger indefinitely once the underlying symbol drops out of the
    instruments table.

    Returns:
        Number of ignore rows removed.
    """
    from instrument_cache import get_instrument

    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT tradingsymbol, exchange FROM ignored_symbols"
        ).fetchall()
        removed = 0
        for row in rows:
            if get_instrument(row["tradingsymbol"], row["exchange"]) is None:
                conn.execute(
                    "DELETE FROM ignored_symbols WHERE tradingsymbol = ? AND exchange = ?",
                    (row["tradingsymbol"], row["exchange"]),
                )
                removed += 1
        if removed:
            conn.commit()
            logger.info(
                "position_guard_db: pruned %d stale ignore(s) after instrument sync", removed
            )
        return removed
    except sqlite3.Error:
        logger.exception("position_guard_db.cleanup_ignored_symbols_not_in_instruments failed")
        return 0
    finally:
        conn.close()


def list_ignored() -> list[dict[str, Any]]:
    """Return all currently ignored symbols.

    Returns:
        List of dicts with keys: tradingsymbol, exchange, fingerprint, ignored_at.
    """
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT tradingsymbol, exchange, fingerprint, ignored_at FROM ignored_symbols "
            "ORDER BY ignored_at DESC"
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.Error:
        logger.exception("position_guard_db.list_ignored failed")
        return []
    finally:
        conn.close()
