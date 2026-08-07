"""Shared SQLite concurrency helpers for modules with a long-lived writer.

Extracted from the pattern proven in ``cas_tracker/database.py`` after the
CAS Public Traffic Incident: under SQLite's default rollback journal, a
long-lived writer connection (a scraper's poll loop) and per-request reader
connections (dashboard/API traffic) block each other outright, surfacing to
callers as "database is locked". WAL lets readers and a writer proceed
concurrently instead.

Usage for a module with its own ``database.py``::

    from db_utils import connect_with_busy_timeout, enable_wal_once

    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "foo.db")

    def get_db() -> sqlite3.Connection:
        return connect_with_busy_timeout(DB_PATH)

    def init_db() -> None:
        enable_wal_once(DB_PATH)
        ...  # CREATE TABLE IF NOT EXISTS ...
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)

_SQLITE_BUSY_TIMEOUT_SECONDS = 5.0


def connect_with_busy_timeout(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with row-by-name access and a bounded lock wait.

    Deliberately does no schema or journal-mode work: this is meant to be
    called on every request. Switching journal mode needs a brief exclusive
    lock, so doing it here would make every request contend with a writer's
    locks. WAL is a persistent property of the database file — call
    ``enable_wal_once()`` once at process startup and every later connection
    simply inherits it.

    Args:
        db_path: Filesystem path to the SQLite database file.

    Returns:
        A connection with ``row_factory`` set to ``sqlite3.Row`` and
        ``busy_timeout`` set.

    Raises:
        sqlite3.Error: If the database cannot be opened.
    """
    conn = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES,
        timeout=_SQLITE_BUSY_TIMEOUT_SECONDS,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {int(_SQLITE_BUSY_TIMEOUT_SECONDS * 1000)}")
    return conn


def enable_wal_once(db_path: str) -> bool:
    """Put a SQLite database into WAL journal mode, once, at process startup.

    Switching mode needs a brief exclusive lock, so this can legitimately
    lose a race with a concurrent writer mid-write. That is not fatal — the
    database still works under the old journal — so this reports failure
    rather than raising, and is safe to call again on the next start.

    Args:
        db_path: Filesystem path to the SQLite database file.

    Returns:
        True if the database is in WAL mode when this returns.
    """
    try:
        conn = connect_with_busy_timeout(db_path)
    except sqlite3.Error as exc:
        logger.error("Could not open %s to enable WAL: %s", db_path, exc)
        return False

    try:
        current_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if current_mode.lower() == "wal":
            return True

        new_mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if new_mode.lower() == "wal":
            logger.info("%s switched to WAL journal mode.", db_path)
            return True

        logger.warning(
            "Could not switch %s to WAL (still %s); readers and writers will "
            "continue to block each other.",
            db_path,
            new_mode,
        )
        return False
    except sqlite3.DatabaseError as exc:
        logger.warning("Could not enable WAL on %s: %s", db_path, exc)
        return False
    finally:
        conn.close()
