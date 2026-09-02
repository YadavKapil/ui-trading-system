"""
Zerodha KiteConnect API call monitor.

Provides a transparent proxy (MonitoredKite) that wraps any KiteConnect
instance and records every API call — method name, caller location, response
time, and errors — into a shared module-level store backed by SQLite for
persistence across restarts (last 3 days retained).

Usage:
    from kite_api_monitor import MonitoredKite, get_monitor_stats

    kite = MonitoredKite(KiteConnect(api_key=...))
    kite.set_access_token(token)
    positions = kite.positions()        # tracked automatically

    stats = get_monitor_stats()         # retrieve aggregated stats for the dashboard
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CallRecord:
    """A single recorded API call."""

    timestamp_ist: str       # "YYYY-MM-DD HH:MM:SS" full datetime for multi-day history
    timestamp_epoch: float   # Unix epoch seconds for range queries and DB storage
    method: str
    caller_file: str
    caller_function: str
    caller_line: int
    args_repr: str
    elapsed_ms: float
    error: str | None  # None means success; repr(exc) otherwise
    is_429: bool = False


@dataclass
class MethodStats:
    """Aggregated stats per KiteConnect method name."""

    total_calls: int = 0
    error_count: int = 0
    count_429: int = 0
    total_elapsed_ms: float = 0.0

    @property
    def avg_elapsed_ms(self) -> float:
        """Average response time in milliseconds."""
        return self.total_elapsed_ms / self.total_calls if self.total_calls else 0.0


@dataclass
class SiteStats:
    """Aggregated stats per unique call-site (file:function:line)."""

    total_calls: int = 0
    methods: set[str] = field(default_factory=set)
    last_seen_ist: str = ""


# ---------------------------------------------------------------------------
# Module-level singleton store — shared across all MonitoredKite instances
# ---------------------------------------------------------------------------

_HISTORY_MAXLEN = 10_000
_HISTORY_DAYS = 3          # retain records for this many days in SQLite

_lock = threading.Lock()
_call_history: deque[CallRecord] = deque(maxlen=_HISTORY_MAXLEN)
_method_stats: dict[str, MethodStats] = defaultdict(MethodStats)
_site_stats: dict[str, SiteStats] = defaultdict(SiteStats)

# Ring buffer of epoch timestamps for calls/min and calls/hour computation
_call_timestamps: deque[float] = deque(maxlen=_HISTORY_MAXLEN)

# Process start time (used to label the session start in the dashboard).
_session_start_time: float = time.time()

# Methods that do not make outbound HTTP requests and should not clutter stats.
_NON_HTTP_METHODS: frozenset[str] = frozenset({
    "set_access_token",
    "login_url",
})

_AUTH_METHODS: frozenset[str] = frozenset({
    "generate_session",
    "renew_access_token",
    "invalidate_access_token",
})

_REDACTED = "[REDACTED]"
_AUTH_ARGS_OMITTED = "<authentication arguments omitted>"
_AUTH_ERROR_OMITTED = "<authentication error details omitted>"
_SENSITIVE_NAMES: frozenset[str] = frozenset({
    "api_secret",
    "request_token",
    "access_token",
    "refresh_token",
    "authorization",
    "proxy_authorization",
    "x_authorization",
    "x_api_key",
    "x_auth_token",
    "api_key",
    "token",
    "password",
    "passwd",
    "passphrase",
    "secret",
    "checksum",
    "authentication",
    "www_authenticate",
    "cookie",
    "set_cookie",
})
_SENSITIVE_COMPACT_NAMES: frozenset[str] = frozenset(
    item.replace("_", "") for item in _SENSITIVE_NAMES
)
_SENSITIVE_COMPONENTS: frozenset[str] = frozenset({
    "token",
    "password",
    "passwd",
    "passphrase",
    "secret",
    "checksum",
    "authorization",
    "authentication",
})
_SENSITIVE_TEXT_NAMES = (
    r"api[_-]?secret|request[_-]?token|access[_-]?token|refresh[_-]?token|"
    r"proxy[_-]?authorization|x[_-]?authorization|x[_-]?api[_-]?key|"
    r"x[_-]?auth[_-]?token|api[_-]?key|authorization|token|password|passwd|"
    r"passphrase|secret|checksum|authentication|www[_-]?authenticate|"
    r"set[_-]?cookie|cookie"
)
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    rf"""(?ix)
    (?<![a-z0-9])
    (?P<prefix>
        (?P<quote>["']?)
        (?:{_SENSITIVE_TEXT_NAMES})
        (?P=quote)
        (?![a-z0-9])
        \s*[:=]\s*
    )
    (?P<value>
        "(?:\\.|[^"])*"
        |
        '(?:\\.|[^'])*'
        |
        [^,\r\n&}}\]]+
    )
    """
)
_AUTH_SCHEME_RE = re.compile(
    r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+"
)


def _normalise_name(name: Any) -> tuple[str, set[str]]:
    """Return a comparable sensitive-field name and its word components."""
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name)).lower()
    components = {part for part in re.split(r"[^a-z0-9]+", text) if part}
    return "_".join(re.findall(r"[a-z0-9]+", text)), components


def _is_sensitive_name(name: Any) -> bool:
    """Return whether a mapping key or header name identifies a credential."""
    normalised, components = _normalise_name(name)
    compact = normalised.replace("_", "")
    return (
        compact in _SENSITIVE_COMPACT_NAMES
        or bool(components & _SENSITIVE_COMPONENTS)
        or compact.endswith(("apikey", "authtoken"))
    )


def _redact_url(value: str) -> str:
    """Redact credentials and sensitive query parameters in a URL-like string."""
    try:
        parsed = urlsplit(value)
    except (TypeError, ValueError):
        return value

    if not parsed.scheme or not parsed.netloc:
        return value

    netloc = parsed.netloc
    if "@" in netloc:
        host = netloc.rsplit("@", 1)[1]
        netloc = f"{_REDACTED}@{host}"

    query = urlencode(
        [
            (key, _REDACTED if _is_sensitive_name(key) else _sanitize_text(item))
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        ],
        doseq=True,
    )
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


def _sanitize_text(value: str, secret_values: tuple[str, ...] = ()) -> str:
    """Redact credentials from free-form text without including them in errors."""
    sanitized = value
    for secret_value in secret_values:
        if len(secret_value) >= 4:
            sanitized = sanitized.replace(secret_value, _REDACTED)

    stripped = sanitized.strip()
    if stripped.startswith(("{", "[")):
        try:
            decoded = json.loads(sanitized)
        except (json.JSONDecodeError, TypeError):
            pass
        else:
            return json.dumps(
                sanitize_monitor_data(decoded, secret_values=secret_values),
                ensure_ascii=True,
                separators=(",", ":"),
            )

    sanitized = _redact_url(sanitized)
    sanitized = _SENSITIVE_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group('prefix')}{_REDACTED!r}",
        sanitized,
    )
    return _AUTH_SCHEME_RE.sub(lambda match: f"{match.group(1)} {_REDACTED}", sanitized)


def sanitize_monitor_data(
    value: Any,
    *,
    secret_values: tuple[str, ...] = (),
    _seen: set[int] | None = None,
    _depth: int = 0,
) -> Any:
    """Recursively redact credentials and return JSON-serializable monitor data.

    This is the sole sanitizer for captured calls, SQLite persistence, legacy
    row migration, and dashboard responses. It intentionally never logs the
    value being sanitized, including when an object's representation fails.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if _depth > 20:
        return "<maximum depth exceeded>"
    if isinstance(value, str):
        return _sanitize_text(value, secret_values)
    if isinstance(value, bytes):
        return _sanitize_text(value.decode("utf-8", errors="replace"), secret_values)
    if isinstance(value, BaseException):
        return _sanitize_text(
            f"{type(value).__name__}: {value}",
            secret_values,
        )

    if _seen is None:
        _seen = set()
    value_id = id(value)
    if value_id in _seen:
        return "<circular reference>"

    if isinstance(value, Mapping):
        _seen.add(value_id)
        try:
            is_auth_record = str(value.get("method", "")) in _AUTH_METHODS
            return {
                str(key): (
                    (
                        _AUTH_ARGS_OMITTED
                        if str(key) in {"args", "args_repr"}
                        else _AUTH_ERROR_OMITTED
                    )
                    if is_auth_record
                    and (
                        str(key) in {"args", "args_repr"}
                        or (str(key) == "error" and item)
                    )
                    else (
                        _REDACTED
                        if _is_sensitive_name(key)
                        else sanitize_monitor_data(
                            item,
                            secret_values=secret_values,
                            _seen=_seen,
                            _depth=_depth + 1,
                        )
                    )
                )
                for key, item in value.items()
            }
        finally:
            _seen.remove(value_id)

    if isinstance(value, (list, tuple, set, frozenset)):
        _seen.add(value_id)
        try:
            return [
                sanitize_monitor_data(
                    item,
                    secret_values=secret_values,
                    _seen=_seen,
                    _depth=_depth + 1,
                )
                for item in value
            ]
        finally:
            _seen.remove(value_id)

    try:
        representation = repr(value)
    except Exception:  # noqa: BLE001
        return f"<unserializable {type(value).__name__}>"
    return _sanitize_text(representation, secret_values)


def _collect_secret_values(method: str, args: tuple, kwargs: dict) -> tuple[str, ...]:
    """Collect known credential values so exception text can be scrubbed."""
    secrets: list[str] = []
    seen: set[int] = set()

    def collect(value: Any, sensitive: bool = False) -> None:
        if isinstance(value, (str, bytes)):
            text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
            stripped = text.strip()
            if stripped.startswith(("{", "[")):
                try:
                    decoded = json.loads(text)
                except (json.JSONDecodeError, TypeError):
                    pass
                else:
                    collect(decoded, sensitive)
            if text:
                if sensitive:
                    secrets.append(text)
            return

        value_id = id(value)
        if value_id in seen:
            return
        if isinstance(value, Mapping):
            seen.add(value_id)
            try:
                for key, item in value.items():
                    collect(item, sensitive or _is_sensitive_name(key))
            finally:
                seen.remove(value_id)
        elif isinstance(value, (list, tuple, set, frozenset)):
            seen.add(value_id)
            try:
                for item in value:
                    collect(item, sensitive)
            finally:
                seen.remove(value_id)

    if method in _AUTH_METHODS:
        collect(args, sensitive=True)
        collect(kwargs, sensitive=True)
    else:
        collect(kwargs)
        collect(args)
    return tuple(dict.fromkeys(secrets))


def _sanitize_error(
    method: str,
    error: str | None,
    secret_values: tuple[str, ...] = (),
) -> str | None:
    """Return safe error text, suppressing all authentication error details."""
    if not error:
        return None
    if method in _AUTH_METHODS:
        return _AUTH_ERROR_OMITTED
    return _sanitize_text(error, secret_values)


def _sanitize_args_repr(method: str, args_repr: str) -> str:
    """Return safe stored argument text, omitting all authentication arguments."""
    if method in _AUTH_METHODS:
        return _AUTH_ARGS_OMITTED
    return _sanitize_text(args_repr)

# ---------------------------------------------------------------------------
# TTL result cache — deduplicates repeated calls within a short window
# ---------------------------------------------------------------------------

_CACHED_METHODS: frozenset[str] = frozenset({"positions", "quote"})
_CACHE_TTL_SECONDS: float = 15.0


@dataclass
class _CacheEntry:
    """A single cached API result with an expiry timestamp."""

    result: Any
    expires_at: float  # time.time() + _CACHE_TTL_SECONDS


_result_cache: dict[str, _CacheEntry] = {}
_cache_lock = threading.Lock()  # separate lock to avoid contention with _lock


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

_DB_PATH: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api_monitor.db")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS api_calls (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_epoch  REAL    NOT NULL,
    timestamp_ist    TEXT    NOT NULL,
    date_ist         TEXT    NOT NULL,
    method           TEXT    NOT NULL,
    caller_file      TEXT    NOT NULL,
    caller_function  TEXT    NOT NULL,
    caller_line      INTEGER NOT NULL,
    args_repr        TEXT    NOT NULL,
    elapsed_ms       REAL    NOT NULL,
    error            TEXT,
    is_429           INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_api_calls_epoch ON api_calls(timestamp_epoch);
CREATE INDEX IF NOT EXISTS idx_api_calls_date  ON api_calls(date_ist);
"""

_INSERT_SQL = """
INSERT INTO api_calls
    (timestamp_epoch, timestamp_ist, date_ist, method,
     caller_file, caller_function, caller_line,
     args_repr, elapsed_ms, error, is_429)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _db_connect() -> sqlite3.Connection:
    """Open a SQLite connection with sensible defaults.

    Returns:
        An open ``sqlite3.Connection`` with WAL mode enabled.
    """
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_db() -> None:
    """Create the api_calls table and indexes if they don't exist."""
    try:
        conn = _db_connect()
        conn.executescript(_CREATE_TABLE_SQL)
        conn.commit()
        conn.close()
        logger.info("KiteAPIMonitor: DB initialised at %s", _DB_PATH)
    except Exception as exc:
        logger.error("KiteAPIMonitor: DB init failed — %s", exc)


_WAL_CHECKPOINT_RETRIES = 5
_WAL_CHECKPOINT_RETRY_DELAY = 0.05


def _checkpoint_wal(
    conn: sqlite3.Connection,
    *,
    retries: int = _WAL_CHECKPOINT_RETRIES,
    retry_delay: float = _WAL_CHECKPOINT_RETRY_DELAY,
) -> None:
    """Truncate the WAL, retrying busy checkpoints and validating completion."""
    for attempt in range(retries):
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result is None or len(result) < 3:
            raise RuntimeError("SQLite returned an invalid WAL checkpoint result")
        if int(result[0]) == 0:
            return
        if attempt + 1 < retries:
            time.sleep(retry_delay)
    raise RuntimeError(f"SQLite WAL checkpoint remained busy after {retries} attempts")


def _sanitize_existing_records() -> None:
    """Rewrite legacy monitor rows in place, preserving non-sensitive history.

    Secure deletion plus a compact/checkpoint removes superseded credential
    bytes from SQLite pages and WAL files after the logical rows are updated.
    """
    conn: sqlite3.Connection | None = None
    try:
        conn = _db_connect()
        conn.execute("PRAGMA secure_delete=ON")
        rows = conn.execute(
            "SELECT id, method, args_repr, error FROM api_calls"
        ).fetchall()
        updates: list[tuple[str, str | None, int]] = []
        for row in rows:
            args_repr = _sanitize_args_repr(row["method"], row["args_repr"])
            error = _sanitize_error(row["method"], row["error"])
            if args_repr != row["args_repr"] or error != row["error"]:
                updates.append((args_repr, error, row["id"]))
        if updates:
            conn.executemany(
                "UPDATE api_calls SET args_repr = ?, error = ? WHERE id = ?",
                updates,
            )
            conn.commit()
            logger.info(
                "KiteAPIMonitor: sanitized %d legacy DB records in place",
                len(updates),
            )
        else:
            conn.commit()

        # Always complete physical cleanup. A previous run may have committed
        # logical updates but failed before truncating the WAL or compacting.
        _checkpoint_wal(conn)
        conn.execute("VACUUM")
        _checkpoint_wal(conn)
    except Exception:
        logger.critical(
            "KiteAPIMonitor: credential migration cleanup failed; refusing startup"
        )
        raise RuntimeError(
            "Kite API monitor credential migration cleanup failed; refusing startup"
        ) from None
    finally:
        if conn is not None:
            conn.close()


def _db_purge_old_records() -> None:
    """Delete records older than _HISTORY_DAYS days from SQLite.

    Called periodically from the write thread.
    """
    cutoff = time.time() - _HISTORY_DAYS * 86_400
    try:
        conn = _db_connect()
        conn.execute("DELETE FROM api_calls WHERE timestamp_epoch < ?", (cutoff,))
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("KiteAPIMonitor: DB purge failed — %s", exc)


def _db_insert_batch(records: list[CallRecord]) -> None:
    """Batch-insert a list of CallRecord objects into SQLite.

    Args:
        records: Records to persist.
    """
    rows = []
    for record in records:
        args_repr = _sanitize_args_repr(record.method, record.args_repr)
        error = _sanitize_error(record.method, record.error)
        rows.append(
            (
                record.timestamp_epoch,
                record.timestamp_ist,
                record.timestamp_ist[:10],   # "YYYY-MM-DD" date part
                record.method,
                record.caller_file,
                record.caller_function,
                record.caller_line,
                args_repr,
                record.elapsed_ms,
                error,
                int(record.is_429),
            )
        )
    try:
        conn = _db_connect()
        conn.executemany(_INSERT_SQL, rows)
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("KiteAPIMonitor: DB batch insert failed — %s", exc)


# ---------------------------------------------------------------------------
# Background write thread
# ---------------------------------------------------------------------------

_write_queue: queue.Queue[CallRecord] = queue.Queue()
_BATCH_FLUSH_INTERVAL = 2.0   # seconds between forced flushes
_BATCH_MAX_SIZE = 100          # flush immediately when queue reaches this size
_PURGE_EVERY_N_BATCHES = 300  # purge old records every ~10 minutes (300 × 2 s)


def _db_writer_loop() -> None:
    """Drain the write queue and persist records to SQLite in batches.

    Runs as a daemon thread so it exits when the main process does.
    """
    batch_count = 0
    while True:
        batch: list[CallRecord] = []
        try:
            # Block up to _BATCH_FLUSH_INTERVAL seconds waiting for first item
            first = _write_queue.get(timeout=_BATCH_FLUSH_INTERVAL)
            batch.append(first)
            # Non-blocking drain of remaining queued items
            while len(batch) < _BATCH_MAX_SIZE:
                try:
                    batch.append(_write_queue.get_nowait())
                except queue.Empty:
                    break
        except queue.Empty:
            pass  # nothing queued in the last flush interval — skip

        if batch:
            _db_insert_batch(batch)
            batch_count += 1
            if batch_count % _PURGE_EVERY_N_BATCHES == 0:
                _db_purge_old_records()


def _start_writer_thread() -> None:
    """Start the background SQLite writer daemon thread (idempotent)."""
    t = threading.Thread(target=_db_writer_loop, name="api-monitor-db-writer", daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Startup: load existing records from SQLite into memory
# ---------------------------------------------------------------------------

def _load_from_db() -> None:
    """Populate in-memory stats from the last _HISTORY_DAYS days of SQLite data.

    Called once at module import time so stats survive Flask restarts.
    """
    cutoff = time.time() - _HISTORY_DAYS * 86_400
    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT * FROM api_calls WHERE timestamp_epoch >= ? ORDER BY timestamp_epoch ASC",
            (cutoff,),
        ).fetchall()
        conn.close()
    except Exception as exc:
        logger.error("KiteAPIMonitor: failed to load history from DB — %s", exc)
        return

    loaded = 0
    with _lock:
        for row in rows:
            record = CallRecord(
                timestamp_ist=row["timestamp_ist"],
                timestamp_epoch=row["timestamp_epoch"],
                method=row["method"],
                caller_file=row["caller_file"],
                caller_function=row["caller_function"],
                caller_line=row["caller_line"],
                args_repr=_sanitize_args_repr(row["method"], row["args_repr"]),
                elapsed_ms=row["elapsed_ms"],
                error=_sanitize_error(row["method"], row["error"]),
                is_429=bool(row["is_429"]),
            )
            _call_history.append(record)
            _call_timestamps.append(row["timestamp_epoch"])

            ms = _method_stats[record.method]
            ms.total_calls += 1
            ms.total_elapsed_ms += record.elapsed_ms
            if record.error:
                ms.error_count += 1
            if record.is_429:
                ms.count_429 += 1

            site_key = f"{record.caller_file}:{record.caller_function}:{record.caller_line}"
            ss = _site_stats[site_key]
            ss.total_calls += 1
            ss.methods.add(record.method)
            ss.last_seen_ist = record.timestamp_ist

            loaded += 1

    logger.info("KiteAPIMonitor: loaded %d records from DB (last %d days)", loaded, _HISTORY_DAYS)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_caller_info() -> tuple[str, str, int]:
    """Walk the call stack and return (file, function, line) of the first
    frame outside this module.

    Returns:
        A 3-tuple of (short_file_path, function_name, line_number).
    """
    this_file = os.path.abspath(__file__)
    for frame_info in inspect.stack():
        abs_path = os.path.abspath(frame_info.filename)
        if abs_path == this_file:
            continue
        # Return a short path relative to the project root, if possible.
        try:
            rel = os.path.relpath(abs_path)
        except ValueError:
            rel = abs_path
        return rel, frame_info.function, frame_info.lineno
    return "<unknown>", "<unknown>", 0


def _is_429(error_repr: str | None) -> bool:
    """Return True when the error looks like a Zerodha rate-limit response."""
    if not error_repr:
        return False
    lower = error_repr.lower()
    return "too many requests" in lower or "429" in lower or "rate limit" in lower


def _make_cache_key(method: str, args: tuple, kwargs: dict, account_id: str = "") -> str:
    """Build a stable string cache key from method name, call arguments, and account identity.

    Args:
        method: KiteConnect method name.
        args: Positional arguments.
        kwargs: Keyword arguments.
        account_id: Unique identifier for the Kite account (e.g. api_key or "main").
            Including this prevents cross-account cache collisions for zero-argument
            methods like ``positions()`` that would otherwise hash identically for
            every account.

    Returns:
        A string suitable for use as a dict key.
    """
    return f"{account_id}|{method}|{args!r}|{sorted(kwargs.items())!r}"


def _safe_args_repr(
    method: str,
    args: tuple,
    kwargs: dict,
    secret_values: tuple[str, ...] = (),
) -> str:
    """Produce a short, safe string representation of positional / keyword args.

    For quote / ltp calls the symbol list is the most useful thing to show.
    We cap the total length to avoid giant strings from bulk calls.

    Args:
        method: Name of the KiteConnect method being called.
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        A human-readable string, ≤ 200 characters.
    """
    if method in _AUTH_METHODS:
        return _AUTH_ARGS_OMITTED

    try:
        safe_args = sanitize_monitor_data(args, secret_values=secret_values)
        safe_kwargs = sanitize_monitor_data(kwargs, secret_values=secret_values)
        parts: list[str] = [repr(a) for a in safe_args] + [
            f"{key}={value!r}" for key, value in safe_kwargs.items()
        ]
        combined = ", ".join(parts)
        if len(combined) > 200:
            combined = combined[:197] + "..."
        return _sanitize_text(combined, secret_values)
    except Exception:  # noqa: BLE001
        return "<args repr failed>"


def _record_call(
    method: str,
    caller: tuple[str, str, int],
    elapsed_ms: float,
    error: str | None,
    args: tuple,
    kwargs: dict,
) -> None:
    """Write a call record to the shared in-memory store and queue it for DB persistence.

    Args:
        method: KiteConnect method name.
        caller: (file, function, line) tuple.
        elapsed_ms: Wall-clock time in milliseconds.
        error: repr(exc) if the call raised, else None.
        args: Positional arguments to the method.
        kwargs: Keyword arguments to the method.
    """
    caller_file, caller_function, caller_line = caller
    secret_values = _collect_secret_values(method, args, kwargs)
    is_429 = _is_429(error)
    sanitized_error = _sanitize_error(method, error, secret_values)
    now = time.time()
    now_ist_dt = datetime.fromtimestamp(now, tz=_IST)
    now_ist = now_ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    record = CallRecord(
        timestamp_ist=now_ist,
        timestamp_epoch=now,
        method=method,
        caller_file=caller_file,
        caller_function=caller_function,
        caller_line=caller_line,
        args_repr=_safe_args_repr(method, args, kwargs, secret_values),
        elapsed_ms=round(elapsed_ms, 1),
        error=sanitized_error,
        is_429=is_429,
    )

    site_key = f"{caller_file}:{caller_function}:{caller_line}"

    with _lock:
        _call_history.append(record)
        _call_timestamps.append(now)

        ms = _method_stats[method]
        ms.total_calls += 1
        ms.total_elapsed_ms += elapsed_ms
        if sanitized_error:
            ms.error_count += 1
        if is_429:
            ms.count_429 += 1

        ss = _site_stats[site_key]
        ss.total_calls += 1
        ss.methods.add(method)
        ss.last_seen_ist = now_ist

    # Non-blocking enqueue for background SQLite write
    try:
        _write_queue.put_nowait(record)
    except queue.Full:
        logger.warning("KiteAPIMonitor: write queue full, dropping DB record for %s", method)


# ---------------------------------------------------------------------------
# Public stats API
# ---------------------------------------------------------------------------

def get_monitor_stats() -> dict[str, Any]:
    """Return a snapshot of all monitored API call statistics (in-memory).

    The in-memory store is seeded from the last 3 days of SQLite data on
    startup, so totals survive Flask restarts.

    Returns:
        A dict with keys:
          - ``recent_calls``: last 100 calls (newest first), with full datetime
          - ``method_stats``: per-method aggregated counters
          - ``top_sites``: top 25 call sites by total_calls
          - ``summary``: total calls, 429 count, calls/min, calls/hour
    """
    now = time.time()

    with _lock:
        # Rolling window counts
        calls_last_60s = sum(1 for ts in _call_timestamps if now - ts <= 60)
        calls_last_3600s = sum(1 for ts in _call_timestamps if now - ts <= 3600)

        total_calls = sum(s.total_calls for s in _method_stats.values())
        total_429 = sum(s.count_429 for s in _method_stats.values())

        # Method stats snapshot
        method_rows = [
            {
                "method": m,
                "total_calls": s.total_calls,
                "error_count": s.error_count,
                "count_429": s.count_429,
                "avg_ms": round(s.avg_elapsed_ms, 1),
            }
            for m, s in sorted(
                _method_stats.items(), key=lambda kv: kv[1].total_calls, reverse=True
            )
        ]

        # Top call sites
        top_sites = sorted(
            [
                {
                    "site": site_key,
                    "total_calls": ss.total_calls,
                    "methods": sorted(ss.methods),
                    "last_seen": ss.last_seen_ist,
                }
                for site_key, ss in _site_stats.items()
            ],
            key=lambda r: r["total_calls"],
            reverse=True,
        )[:25]

        # Recent 100 calls (newest first)
        recent = list(_call_history)[-100:][::-1]
        recent_rows = [
            {
                "time": r.timestamp_ist,
                "method": r.method,
                "file": r.caller_file,
                "function": r.caller_function,
                "line": r.caller_line,
                "args": r.args_repr,
                "ms": r.elapsed_ms,
                "ok": r.error is None,
                "error": r.error or "",
                "is_429": r.is_429,
            }
            for r in recent
        ]

    return sanitize_monitor_data({
        "recent_calls": recent_rows,
        "method_stats": method_rows,
        "top_sites": top_sites,
        "summary": {
            "total_calls": total_calls,
            "total_429": total_429,
            "calls_per_min": calls_last_60s,
            "calls_per_hour": calls_last_3600s,
            "session_start": datetime.fromtimestamp(_session_start_time, tz=_IST).strftime(
                "%Y-%m-%d %H:%M:%S IST"
            ),
        },
    })


def get_history_for_date(date_ist: str) -> dict[str, Any]:
    """Query SQLite for all API calls on a specific IST date.

    Args:
        date_ist: Date string in ``"YYYY-MM-DD"`` format (IST).

    Returns:
        A dict with keys:
          - ``recent_calls``: all calls for that date, newest first (up to 2000)
          - ``method_stats``: per-method totals for that date
          - ``top_sites``: top 25 call sites for that date
          - ``summary``: totals for that date
    """
    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT * FROM api_calls WHERE date_ist = ? ORDER BY timestamp_epoch DESC LIMIT 2000",
            (date_ist,),
        ).fetchall()
        conn.close()
    except Exception as exc:
        logger.error("KiteAPIMonitor: get_history_for_date failed — %s", exc)
        return {"recent_calls": [], "method_stats": [], "top_sites": [], "summary": {}}

    recent_calls = [
        {
            "time": row["timestamp_ist"],
            "method": row["method"],
            "file": row["caller_file"],
            "function": row["caller_function"],
            "line": row["caller_line"],
            "args": _sanitize_args_repr(row["method"], row["args_repr"]),
            "ms": row["elapsed_ms"],
            "ok": row["error"] is None,
            "error": _sanitize_error(row["method"], row["error"]) or "",
            "is_429": bool(row["is_429"]),
        }
        for row in rows
    ]

    # Compute per-method and per-site aggregates from the fetched rows
    method_agg: dict[str, dict] = {}
    site_agg: dict[str, dict] = {}

    for row in rows:
        m = row["method"]
        if m not in method_agg:
            method_agg[m] = {"total_calls": 0, "error_count": 0, "count_429": 0, "total_ms": 0.0}
        method_agg[m]["total_calls"] += 1
        method_agg[m]["total_ms"] += row["elapsed_ms"]
        if row["error"]:
            method_agg[m]["error_count"] += 1
        if row["is_429"]:
            method_agg[m]["count_429"] += 1

        site_key = f"{row['caller_file']}:{row['caller_function']}:{row['caller_line']}"
        if site_key not in site_agg:
            site_agg[site_key] = {"total_calls": 0, "methods": set(), "last_seen": ""}
        site_agg[site_key]["total_calls"] += 1
        site_agg[site_key]["methods"].add(row["method"])
        if not site_agg[site_key]["last_seen"]:
            site_agg[site_key]["last_seen"] = row["timestamp_ist"]

    method_rows = sorted(
        [
            {
                "method": m,
                "total_calls": v["total_calls"],
                "error_count": v["error_count"],
                "count_429": v["count_429"],
                "avg_ms": round(v["total_ms"] / v["total_calls"], 1) if v["total_calls"] else 0.0,
            }
            for m, v in method_agg.items()
        ],
        key=lambda r: r["total_calls"],
        reverse=True,
    )

    top_sites = sorted(
        [
            {
                "site": site_key,
                "total_calls": v["total_calls"],
                "methods": sorted(v["methods"]),
                "last_seen": v["last_seen"],
            }
            for site_key, v in site_agg.items()
        ],
        key=lambda r: r["total_calls"],
        reverse=True,
    )[:25]

    total_calls = sum(v["total_calls"] for v in method_agg.values())
    total_429 = sum(v["count_429"] for v in method_agg.values())
    total_errors = sum(v["error_count"] for v in method_agg.values())

    return sanitize_monitor_data({
        "recent_calls": recent_calls,
        "method_stats": method_rows,
        "top_sites": top_sites,
        "summary": {
            "total_calls": total_calls,
            "total_429": total_429,
            "total_errors": total_errors,
            "date": date_ist,
        },
    })


def get_available_dates() -> list[str]:
    """Return the distinct IST dates present in the DB, newest first.

    Returns:
        List of ``"YYYY-MM-DD"`` strings.
    """
    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT DISTINCT date_ist FROM api_calls ORDER BY date_ist DESC LIMIT ?",
            (_HISTORY_DAYS,),
        ).fetchall()
        conn.close()
        return [row["date_ist"] for row in rows]
    except Exception as exc:
        logger.error("KiteAPIMonitor: get_available_dates failed — %s", exc)
        return []


def reset_stats() -> None:
    """Clear all in-memory stats and truncate the SQLite table.

    Note:
        This is a destructive operation; all history is lost.
    """
    global _session_start_time
    with _lock:
        _call_history.clear()
        _call_timestamps.clear()
        _method_stats.clear()
        _site_stats.clear()
        _session_start_time = time.time()
    with _cache_lock:
        _result_cache.clear()
    try:
        conn = _db_connect()
        conn.execute("DELETE FROM api_calls")
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("KiteAPIMonitor: DB reset failed — %s", exc)
    logger.info("KiteAPIMonitor: stats reset (memory + DB).")


def invalidate_positions_cache() -> None:
    """Remove all cached entries for the 'positions' method.

    Leaves quote and other caches intact. Call this before a forced refresh
    so the next kite.positions() call goes directly to Zerodha.
    """
    with _cache_lock:
        stale_keys = [k for k in _result_cache if "|positions|" in k or k.startswith("positions|")]
        for key in stale_keys:
            del _result_cache[key]
    logger.info("KiteAPIMonitor: positions cache invalidated (%d entries removed).", len(stale_keys))


# ---------------------------------------------------------------------------
# MonitoredKite proxy
# ---------------------------------------------------------------------------

class MonitoredKite:
    """Transparent proxy around a KiteConnect instance that records every call.

    All attribute accesses are forwarded to the underlying ``_kite`` object.
    Callable attributes are wrapped so that every invocation writes a
    ``CallRecord`` to the shared module-level store and queues it for
    SQLite persistence.

    Constants (non-callable class attributes such as ``EXCHANGE_NSE``) are
    returned directly without wrapping.

    Args:
        kite: The real KiteConnect instance to wrap.

    Example:
        kite = MonitoredKite(KiteConnect(api_key="xxx"))
        kite.set_access_token("yyy")
        data = kite.positions()  # recorded automatically
    """

    def __init__(self, kite: Any, account_id: str = "") -> None:
        # Use object.__setattr__ to avoid triggering our own __setattr__.
        object.__setattr__(self, "_kite", kite)
        object.__setattr__(self, "_account_id", account_id)

    # Forward attribute setting to the underlying kite object (e.g. on_order_update).
    def __setattr__(self, name: str, value: Any) -> None:
        if name in ("_kite", "_account_id"):
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_kite"), name, value)

    def __getattr__(self, name: str) -> Any:
        """Intercept attribute access and wrap callables for monitoring.

        Args:
            name: Attribute name being accessed.

        Returns:
            The attribute value, wrapped in a monitored call if callable.
        """
        kite = object.__getattribute__(self, "_kite")
        attr = getattr(kite, name)

        if not callable(attr) or name in _NON_HTTP_METHODS:
            return attr

        return self._make_monitored_call(name, attr)

    def _make_monitored_call(self, method_name: str, func: Callable) -> Callable:
        """Return a wrapper function that records timing and caller info.

        Args:
            method_name: Name of the KiteConnect method.
            func: The bound method to wrap.

        Returns:
            A callable that invokes ``func`` and records the call.
        """
        account_id = object.__getattribute__(self, "_account_id")

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            caller = _get_caller_info()

            # Cache check — short-circuit for methods in _CACHED_METHODS
            if method_name in _CACHED_METHODS:
                cache_key = _make_cache_key(method_name, args, kwargs, account_id)
                with _cache_lock:
                    entry = _result_cache.get(cache_key)
                    if entry is not None and time.time() < entry.expires_at:
                        # Record the hit with 0 ms so the dashboard shows cache activity
                        try:
                            _record_call(method_name, caller, 0.0, None, args, kwargs)
                        except Exception as record_exc:  # noqa: BLE001
                            logger.warning("MonitoredKite: failed to record cache hit: %s", record_exc)
                        return entry.result

            # Real API call
            start = time.monotonic()
            error: str | None = None
            try:
                result = func(*args, **kwargs)
                if method_name in _CACHED_METHODS:
                    # quote() data stales fast — use a shorter TTL than positions()
                    ttl = 3.0 if method_name == "quote" else _CACHE_TTL_SECONDS
                    with _cache_lock:
                        _result_cache[cache_key] = _CacheEntry(
                            result=result,
                            expires_at=time.time() + ttl,
                        )
                return result
            except Exception as exc:
                error = repr(exc)
                raise
            finally:
                elapsed_ms = (time.monotonic() - start) * 1000
                try:
                    _record_call(method_name, caller, elapsed_ms, error, args, kwargs)
                except Exception as record_exc:  # noqa: BLE001
                    logger.warning("MonitoredKite: failed to record call: %s", record_exc)

        return wrapper

    def __repr__(self) -> str:
        return f"MonitoredKite({object.__getattribute__(self, '_kite')!r})"


# ---------------------------------------------------------------------------
# Module initialisation — runs once at import time
# ---------------------------------------------------------------------------

_init_db()
_sanitize_existing_records()
_load_from_db()
_start_writer_thread()
