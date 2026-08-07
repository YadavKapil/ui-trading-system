"""SQLite storage for the NIFTY Closing Auction Session (CAS) tracker.

Four tables:
    snapshots           — one row per successful poll, full raw API
                           response. Never purged: this is the durable
                           dataset used for backtesting/replay on days when
                           the live CAS API is (correctly) returning no
                           data outside the window.
    latest_snapshot     — single upserted row (id=1) for fast "current
                           state" reads by the dashboard's polling loop.
    cas_anchors         — one row per date: NIFTY's own value at the CAS
                           reference cutoff plus each constituent's own
                           value at that same moment (see blueprint.py's
                           _capture_cas_anchors_for_date()). These come
                           from ~50 historical-data Kite calls, so they're
                           computed once per date and persisted here rather
                           than in-memory only — otherwise every process
                           restart would silently lose a day's anchors and
                           force a full 50-call re-fetch the next time that
                           date's chart is viewed.
    cas_depth_snapshots — one row per poll during the CAS window: each
                           NIFTY50 constituent's 5-level bid/ask market
                           depth from Kite's quote() response (see
                           blueprint.py's _persist_cas_depth_snapshot()).
                           Recorded to empirically settle whether Kite's
                           depth field is actually populated with real
                           order-book data during the closing auction —
                           unconfirmed in Kite's docs as of CAS's
                           2026-08-03 launch.
    cas_chart_cache     — one row per *finalized* (non-today) date: the
                           full /api/candles JSON response, computed once
                           and reused forever after. Rebuilding it means
                           re-reading every raw snapshot for that date
                           (~7s for a day recorded at the old 1s poll
                           interval, e.g. 2026-08-05's ~1683 rows) — this
                           table is what makes every view after the first
                           instant. Never written for "today" (still
                           accumulating new snapshots throughout the CAS
                           window, so there's nothing stable to cache yet).

See init_db() for the authoritative, always-up-to-date table list.
"""

import json
import logging
import os
import sqlite3
from typing import Any, Optional

from cas_tracker.index_calculator import extract_cas_digest

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cas_tracker.db")

# How long a statement waits for a competing lock before raising
# "database is locked". Reads are short, so a few seconds is ample headroom
# even while the scraper is writing.
_SQLITE_BUSY_TIMEOUT_SECONDS = 5.0

# Every table _create_schema() is responsible for. Used to decide whether a
# locked-out schema check is survivable (all tables already there) or fatal
# (fresh database that genuinely needs creating).
_EXPECTED_TABLES = frozenset(
    {
        "snapshots",
        "latest_snapshot",
        "cas_anchors",
        "cas_depth_snapshots",
        "contributor_snapshots",
        "cas_chart_cache",
        "cas_tick_digests",
    }
)


def get_db() -> sqlite3.Connection:
    """Open a connection to the CAS tracker database.

    Deliberately does no schema or journal-mode work: this runs on every API
    request, and switching journal mode needs an exclusive lock, so doing it
    here would make every request contend with the scraper's writes. WAL is a
    persistent property of the database file — enable_wal() sets it once at
    startup and every later connection simply inherits it.

    Returns:
        A sqlite3 connection with row_factory set to sqlite3.Row.

    Raises:
        sqlite3.Error: If the database cannot be opened.
    """
    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES,
        timeout=_SQLITE_BUSY_TIMEOUT_SECONDS,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {int(_SQLITE_BUSY_TIMEOUT_SECONDS * 1000)}")
    return conn


def enable_wal() -> bool:
    """Put the database into WAL journal mode, once, at startup.

    Under the default rollback journal the scraper's writes and the
    dashboard's reads block each other outright — public traffic made readers
    fail with "database is locked". WAL lets them proceed concurrently.

    Switching mode needs a brief exclusive lock, so this can legitimately lose
    a race with the scraper mid-write. That is not fatal — the database still
    works under the old journal — so this reports failure rather than raising
    and is safe to call again on the next start.

    Returns:
        True if the database is in WAL mode when this returns.
    """
    try:
        conn = get_db()
    except sqlite3.Error as exc:
        logger.error("Could not open %s to enable WAL: %s", DB_PATH, exc)
        return False

    try:
        current_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if current_mode.lower() == "wal":
            return True

        new_mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if new_mode.lower() == "wal":
            logger.info("cas_tracker.db switched to WAL journal mode.")
            return True

        logger.warning(
            "Could not switch %s to WAL (still %s); readers and writers will "
            "continue to block each other.",
            DB_PATH,
            new_mode,
        )
        return False
    except sqlite3.DatabaseError as exc:
        logger.warning("Could not enable WAL on %s: %s", DB_PATH, exc)
        return False
    finally:
        conn.close()


def init_db() -> None:
    """Create the CAS tracker tables if they do not already exist.

    Raises:
        sqlite3.OperationalError: If the schema cannot be created and the
            expected tables are not already present.
    """
    enable_wal()

    conn = get_db()
    try:
        _create_schema(conn)
    except sqlite3.OperationalError as exc:
        # The standalone scraper writes to this database continuously, so a
        # startup that lands mid-write can lose the lock. When the schema is
        # already in place there is nothing to create and no reason to take
        # the whole Flask app down with us — but on a genuinely fresh
        # database this is fatal and must surface.
        if "locked" not in str(exc).lower() or not _schema_exists(conn):
            raise
        logger.warning(
            "cas_tracker schema check skipped — database was locked, but all "
            "expected tables already exist: %s",
            exc,
        )
    finally:
        conn.close()


def _schema_exists(conn: sqlite3.Connection) -> bool:
    """Check that every table init_db() creates is already present.

    Args:
        conn: An open connection from get_db().

    Returns:
        True if no table is missing.
    """
    try:
        present = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    except sqlite3.DatabaseError as exc:
        logger.error("Could not read cas_tracker schema: %s", exc)
        return False
    return _EXPECTED_TABLES.issubset(present)


def _create_schema(conn: sqlite3.Connection) -> None:
    """Create every CAS tracker table and index if absent.

    Args:
        conn: An open connection from get_db().
    """
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            raw_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots(timestamp)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS latest_snapshot (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            raw_json TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cas_anchors (
            date TEXT PRIMARY KEY,
            nifty_anchor REAL,
            stock_anchors_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cas_depth_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            depth_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cas_depth_snapshots_timestamp ON cas_depth_snapshots(timestamp)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS contributor_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            date TEXT NOT NULL,
            nifty_live REAL,
            nifty_estimate REAL,
            top5_pos_pts REAL,
            top5_neg_pts REAL,
            top5_net_pts REAL,
            net_contribution_pts REAL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_contrib_snap_date ON contributor_snapshots(date)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_contrib_snap_timestamp ON contributor_snapshots(timestamp)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cas_chart_cache (
            date TEXT PRIMARY KEY,
            response_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cas_tick_digests (
            snapshot_id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            date TEXT NOT NULL,
            digest_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cas_tick_digests_date ON cas_tick_digests(date)"
    )

    conn.commit()


def save_snapshot(conn: sqlite3.Connection, timestamp: str, data: dict[str, Any]) -> None:
    """Persist one poll's raw response to history and update the latest-state row.

    Args:
        conn: An open connection from get_db().
        timestamp: IST timestamp string ("%Y-%m-%d %H:%M:%S") for this poll.
        data: The parsed JSON response from the CAS API.
    """
    raw_json = json.dumps(data)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO snapshots (timestamp, raw_json) VALUES (?, ?)",
        (timestamp, raw_json),
    )
    snapshot_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO latest_snapshot (id, raw_json, timestamp)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET raw_json = excluded.raw_json, timestamp = excluded.timestamp
        """,
        (raw_json, timestamp),
    )
    # Written in the same transaction as the snapshot it derives from, so the
    # two can never drift apart or leave a snapshot without its digest.
    _insert_tick_digest(cursor, snapshot_id, timestamp, data)
    conn.commit()


def _insert_tick_digest(
    cursor: sqlite3.Cursor,
    snapshot_id: int,
    timestamp: str,
    data: dict[str, Any],
) -> None:
    """Store the compact chart digest derived from one raw snapshot.

    A row is written even when the payload was empty — stored as JSON null —
    so that every snapshot is accounted for. Skipping those would leave them
    permanently "missing", re-triggering the backfill on every read, and the
    null is also what tells the read path to drop the tick entirely (matching
    the original's `if not rows: continue`).

    Args:
        cursor: Cursor inside the caller's transaction — this does not commit.
        snapshot_id: Row id of the snapshot this digest derives from.
        timestamp: The snapshot's IST timestamp string.
        data: The parsed CAS API response.
    """
    digest = extract_cas_digest(data.get("data", []))

    cursor.execute(
        """
        INSERT INTO cas_tick_digests (snapshot_id, timestamp, date, digest_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(snapshot_id) DO UPDATE SET
            timestamp = excluded.timestamp,
            date = excluded.date,
            digest_json = excluded.digest_json
        """,
        (snapshot_id, timestamp, timestamp[:10], json.dumps(digest, separators=(",", ":"))),
    )


def get_tick_digests_for_date(conn: sqlite3.Connection, date: str) -> list[dict[str, Any]]:
    """Fetch the compact per-tick digests for a date, in chronological order.

    This is the fast path behind /api/candles: it reads ~1.5 KB per tick
    instead of the ~150 KB raw snapshot it was derived from.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        List of {'timestamp': str, 'digest': parsed dict} in ascending time order.
    """
    rows = conn.execute(
        """
        SELECT timestamp, digest_json FROM cas_tick_digests
        WHERE date = ?
        ORDER BY timestamp ASC, snapshot_id ASC
        """,
        (date,),
    ).fetchall()
    return [
        {"timestamp": row["timestamp"], "digest": json.loads(row["digest_json"])}
        for row in rows
    ]


def count_snapshots_missing_digest(conn: sqlite3.Connection, date: Optional[str] = None) -> int:
    """Count snapshots that have no digest row yet.

    Args:
        conn: An open connection from get_db().
        date: Restrict to one date, or None for every date.

    Returns:
        Number of snapshots still awaiting a digest.
    """
    sql = """
        SELECT COUNT(*) FROM snapshots s
        LEFT JOIN cas_tick_digests d ON d.snapshot_id = s.id
        WHERE d.snapshot_id IS NULL
    """
    params: tuple = ()
    if date is not None:
        sql += " AND substr(s.timestamp, 1, 10) = ?"
        params = (date,)
    return conn.execute(sql, params).fetchone()[0]


def backfill_tick_digests(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
    batch_size: int = 200,
) -> int:
    """Derive and store digests for snapshots recorded before digests existed.

    Streams in batches and commits per batch: a full session's raw JSON is
    hundreds of megabytes, so materialising it all at once would blow up
    memory on a 4 GB VM.

    Args:
        conn: An open connection from get_db().
        date: Restrict to one date, or None for every date.
        batch_size: Snapshots to decode per batch.

    Returns:
        Number of digests written.
    """
    written = 0
    while True:
        sql = """
            SELECT s.id, s.timestamp, s.raw_json FROM snapshots s
            LEFT JOIN cas_tick_digests d ON d.snapshot_id = s.id
            WHERE d.snapshot_id IS NULL
        """
        params: tuple = ()
        if date is not None:
            sql += " AND substr(s.timestamp, 1, 10) = ?"
            params = (date,)
        sql += " ORDER BY s.id LIMIT ?"
        params = params + (batch_size,)

        rows = conn.execute(sql, params).fetchall()
        if not rows:
            break

        cursor = conn.cursor()
        for row in rows:
            try:
                data = json.loads(row["raw_json"])
            except (ValueError, TypeError) as exc:
                logger.error("Snapshot %s has unparseable raw_json: %s", row["id"], exc)
                data = {}
            _insert_tick_digest(cursor, row["id"], row["timestamp"], data)
        conn.commit()
        written += len(rows)

    if written:
        logger.info("backfill_tick_digests: wrote %d digests.", written)
    return written


def get_latest_snapshot(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    """Fetch the most recent snapshot for live display.

    Args:
        conn: An open connection from get_db().

    Returns:
        A dict with 'raw_json' (parsed) and 'timestamp', or None if no data yet.
    """
    row = conn.execute(
        "SELECT raw_json, timestamp FROM latest_snapshot WHERE id = 1"
    ).fetchone()
    if row is None:
        return None
    return {"data": json.loads(row["raw_json"]), "timestamp": row["timestamp"]}


def get_snapshots_for_date(conn: sqlite3.Connection, date: str) -> list[dict[str, Any]]:
    """Fetch every recorded snapshot for a date, in chronological order.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        List of {'timestamp': str, 'data': parsed dict} in ascending time order.
    """
    rows = conn.execute(
        """
        SELECT timestamp, raw_json FROM snapshots
        WHERE substr(timestamp, 1, 10) = ?
        ORDER BY timestamp ASC
        """,
        (date,),
    ).fetchall()
    return [{"timestamp": row["timestamp"], "data": json.loads(row["raw_json"])} for row in rows]


def get_distinct_dates(conn: sqlite3.Connection) -> list[str]:
    """List distinct dates (YYYY-MM-DD) for which snapshots were recorded.

    Args:
        conn: An open connection from get_db().

    Returns:
        Dates in descending order, most recent first.
    """
    rows = conn.execute(
        "SELECT DISTINCT substr(timestamp, 1, 10) AS day FROM snapshots ORDER BY day DESC"
    ).fetchall()
    return [row["day"] for row in rows]


def get_snapshot_near(conn: sqlite3.Connection, date: str, at_time: str) -> Optional[dict[str, Any]]:
    """Find the recorded snapshot closest to (and not after) a given date/time.

    Falls back to the earliest snapshot on that date if none exist before the
    requested time, so a replay request near session-open still returns data.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".
        at_time: Time string "HH:MM:SS".

    Returns:
        A dict with 'raw_json' (parsed) and 'timestamp', or None if that date has no data.
    """
    target = f"{date} {at_time}"
    row = conn.execute(
        """
        SELECT raw_json, timestamp FROM snapshots
        WHERE substr(timestamp, 1, 10) = ? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
        """,
        (date, target),
    ).fetchone()
    if row is None:
        row = conn.execute(
            """
            SELECT raw_json, timestamp FROM snapshots
            WHERE substr(timestamp, 1, 10) = ?
            ORDER BY timestamp ASC LIMIT 1
            """,
            (date,),
        ).fetchone()
    if row is None:
        return None
    return {"data": json.loads(row["raw_json"]), "timestamp": row["timestamp"]}


def save_cas_anchors(
    conn: sqlite3.Connection,
    date: str,
    nifty_anchor: Optional[float],
    stock_anchors: dict[str, float],
) -> None:
    """Persist a date's CAS reference-cutoff anchors so they never need re-fetching.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD" the anchors were captured for.
        nifty_anchor: NIFTY 50's own value at the CAS cutoff, or None if
            that specific historical candle couldn't be fetched.
        stock_anchors: {symbol: price_at_cas_cutoff}, possibly partial.
    """
    conn.execute(
        """
        INSERT INTO cas_anchors (date, nifty_anchor, stock_anchors_json)
        VALUES (?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            nifty_anchor = excluded.nifty_anchor,
            stock_anchors_json = excluded.stock_anchors_json
        """,
        (date, nifty_anchor, json.dumps(stock_anchors)),
    )
    conn.commit()


def get_cas_anchors(
    conn: sqlite3.Connection, date: str
) -> Optional[tuple[Optional[float], dict[str, float]]]:
    """Fetch a previously-persisted date's CAS reference-cutoff anchors.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        (nifty_anchor, stock_anchors) if this date was ever captured
        (nifty_anchor may itself still be None if only stocks succeeded),
        or None if this date has never been captured at all.
    """
    row = conn.execute(
        "SELECT nifty_anchor, stock_anchors_json FROM cas_anchors WHERE date = ?",
        (date,),
    ).fetchone()
    if row is None:
        return None
    return row["nifty_anchor"], json.loads(row["stock_anchors_json"])


def save_depth_snapshot(
    conn: sqlite3.Connection, timestamp: str, depth_by_symbol: dict[str, dict[str, Any]]
) -> None:
    """Persist one poll's per-symbol bid/ask market depth.

    Args:
        conn: An open connection from get_db().
        timestamp: IST timestamp string ("%Y-%m-%d %H:%M:%S").
        depth_by_symbol: {symbol: {"buy": [...], "sell": [...]}}, each side
            up to 5 levels of {"price", "quantity", "orders"} straight from
            Kite's quote() response — one entry per NIFTY50 constituent
            that had a non-empty 'depth' field in that poll.
    """
    conn.execute(
        "INSERT INTO cas_depth_snapshots (timestamp, depth_json) VALUES (?, ?)",
        (timestamp, json.dumps(depth_by_symbol)),
    )
    conn.commit()


def get_depth_snapshots_for_date(conn: sqlite3.Connection, date: str) -> list[dict[str, Any]]:
    """Fetch every recorded depth snapshot for a date, in chronological order.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        List of {'timestamp': str, 'depth': {symbol: {"buy": [...], "sell": [...]}}}
        in ascending time order.
    """
    rows = conn.execute(
        """
        SELECT timestamp, depth_json FROM cas_depth_snapshots
        WHERE substr(timestamp, 1, 10) = ?
        ORDER BY timestamp ASC
        """,
        (date,),
    ).fetchall()
    return [{"timestamp": row["timestamp"], "depth": json.loads(row["depth_json"])} for row in rows]


def get_snapshot_timestamps_for_date(conn: sqlite3.Connection, date: str) -> list[str]:
    """List every recorded CAS-payload timestamp for a date, ascending.

    Lightweight (timestamps only, no payload) — powers the order-book
    page's time selector, bound to exactly the seconds real data exists
    for. Finer-grained than get_depth_timestamps_for_date(): the CAS
    payload is polled by scraper.py every CAS_POLL_INTERVAL_SECONDS
    (1s on 2026-08-05, 5s since), whereas depth piggybacks on the slower
    live-replication poll.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        Full "YYYY-MM-DD HH:MM:SS" timestamp strings, ascending.
    """
    rows = conn.execute(
        """
        SELECT timestamp FROM snapshots
        WHERE substr(timestamp, 1, 10) = ?
        ORDER BY timestamp ASC
        """,
        (date,),
    ).fetchall()
    return [row["timestamp"] for row in rows]


def get_depth_distinct_dates(conn: sqlite3.Connection) -> list[str]:
    """List distinct dates (YYYY-MM-DD) for which depth snapshots were recorded.

    Args:
        conn: An open connection from get_db().

    Returns:
        Dates in descending order, most recent first.
    """
    rows = conn.execute(
        "SELECT DISTINCT substr(timestamp, 1, 10) AS day FROM cas_depth_snapshots ORDER BY day DESC"
    ).fetchall()
    return [row["day"] for row in rows]


def get_depth_timestamps_for_date(conn: sqlite3.Connection, date: str) -> list[str]:
    """List every recorded depth-snapshot timestamp for a date, ascending.

    Lightweight (timestamps only, no depth payload) — meant for populating a
    time selector bound to exactly the seconds real data exists for, per
    order_tracker.html's "select a time for which you have data" UI.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        Full "YYYY-MM-DD HH:MM:SS" timestamp strings, ascending.
    """
    rows = conn.execute(
        """
        SELECT timestamp FROM cas_depth_snapshots
        WHERE substr(timestamp, 1, 10) = ?
        ORDER BY timestamp ASC
        """,
        (date,),
    ).fetchall()
    return [row["timestamp"] for row in rows]


def get_depth_snapshot_near(conn: sqlite3.Connection, date: str, at_time: str) -> Optional[dict[str, Any]]:
    """Find the recorded depth snapshot closest to (and not after) a given date/time.

    Mirrors get_snapshot_near()'s semantics for the main snapshots table.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".
        at_time: Time string "HH:MM:SS".

    Returns:
        {'timestamp': str, 'depth': {symbol: {"buy": [...], "sell": [...]}}}
        for every symbol captured in that poll, or None if that date has no
        depth data at all.
    """
    target = f"{date} {at_time}"
    row = conn.execute(
        """
        SELECT timestamp, depth_json FROM cas_depth_snapshots
        WHERE substr(timestamp, 1, 10) = ? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
        """,
        (date, target),
    ).fetchone()
    if row is None:
        row = conn.execute(
            """
            SELECT timestamp, depth_json FROM cas_depth_snapshots
            WHERE substr(timestamp, 1, 10) = ?
            ORDER BY timestamp ASC LIMIT 1
            """,
            (date,),
        ).fetchone()
    if row is None:
        return None
    return {"timestamp": row["timestamp"], "depth": json.loads(row["depth_json"])}


def save_chart_cache(conn: sqlite3.Connection, date: str, response: dict[str, Any]) -> None:
    """Persist a finalized date's full /api/candles response.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD". Callers must never pass today's
            date — that data is still accumulating, so caching it would
            freeze an incomplete chart.
        response: The full JSON-serializable response dict (date,
            is_absolute, candles, ticks) as returned by /api/candles.
    """
    conn.execute(
        """
        INSERT INTO cas_chart_cache (date, response_json)
        VALUES (?, ?)
        ON CONFLICT(date) DO UPDATE SET response_json = excluded.response_json
        """,
        (date, json.dumps(response)),
    )
    conn.commit()


def get_chart_cache(conn: sqlite3.Connection, date: str) -> Optional[dict[str, Any]]:
    """Fetch a previously-cached /api/candles response for a finalized date.

    Args:
        conn: An open connection from get_db().
        date: Date string "YYYY-MM-DD".

    Returns:
        The cached response dict, or None if this date was never cached
        (e.g. never viewed before, or it's today).
    """
    row = conn.execute(
        "SELECT response_json FROM cas_chart_cache WHERE date = ?", (date,)
    ).fetchone()
    return json.loads(row["response_json"]) if row else None


def save_contributor_snapshot(
    conn: sqlite3.Connection,
    timestamp: str,
    date: str,
    nifty_live: Optional[float],
    nifty_estimate: Optional[float],
    top5_pos_pts: float,
    top5_neg_pts: float,
    top5_net_pts: float,
    net_contribution_pts: float,
) -> None:
    """Save a contributor metrics snapshot to the database."""
    conn.execute(
        """
        INSERT INTO contributor_snapshots (
            timestamp, date, nifty_live, nifty_estimate, top5_pos_pts, top5_neg_pts, top5_net_pts, net_contribution_pts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            date,
            nifty_live,
            nifty_estimate,
            top5_pos_pts,
            top5_neg_pts,
            top5_net_pts,
            net_contribution_pts,
        ),
    )
    conn.commit()


def purge_old_contributor_snapshots(conn: sqlite3.Connection, days_to_keep: int = 7) -> None:
    """Retain only the last N days of contributor snapshot records in the database."""
    conn.execute(
        """
        DELETE FROM contributor_snapshots
        WHERE date < (
            SELECT DISTINCT date FROM contributor_snapshots
            ORDER BY date DESC
            LIMIT 1 OFFSET (? - 1)
        )
        """,
        (days_to_keep,),
    )
    conn.commit()


def get_contributor_dates(conn: sqlite3.Connection) -> list[str]:
    """Fetch distinct dates available for contributor history, descending."""
    rows = conn.execute(
        "SELECT DISTINCT date FROM contributor_snapshots ORDER BY date DESC"
    ).fetchall()
    return [row["date"] for row in rows]


def get_contributor_history_for_date(conn: sqlite3.Connection, date: str) -> list[dict[str, Any]]:
    """Fetch all contributor snapshot records for a given date in chronological order."""
    rows = conn.execute(
        """
        SELECT timestamp, date, nifty_live, nifty_estimate, top5_pos_pts, top5_neg_pts, top5_net_pts, net_contribution_pts
        FROM contributor_snapshots
        WHERE date = ?
        ORDER BY timestamp ASC
        """,
        (date,),
    ).fetchall()
    return [
        {
            "timestamp": row["timestamp"],
            "time": row["timestamp"].split(" ")[1] if " " in row["timestamp"] else row["timestamp"],
            "date": row["date"],
            "nifty_live": row["nifty_live"],
            "nifty_estimate": row["nifty_estimate"],
            "top5_pos_pts": row["top5_pos_pts"],
            "top5_neg_pts": row["top5_neg_pts"],
            "top5_net_pts": row["top5_net_pts"],
            "net_contribution_pts": row["net_contribution_pts"],
        }
        for row in rows
    ]


if __name__ == "__main__":
    init_db()
    print(f"CAS tracker database initialized at {DB_PATH}")

