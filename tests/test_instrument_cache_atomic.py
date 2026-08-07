"""Tests for instrument_cache atomic swap behaviour.

Covers:
- Successful sync → live table populated, backup table updated
- Failed sync (API error) → live table untouched, backup preserved
- needs_sync() returns True when live table is empty
- restore_from_backup() copies backup rows into the empty live table
- clear_db() wipes live table but preserves backup
"""

import datetime
import os
import sqlite3
import sys
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path + dependency mocks — must come before any vibhu-package imports
# ---------------------------------------------------------------------------

# Add vibhu/ directory to path so instrument_cache is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock kiteconnect before anything that imports it
sys.modules.setdefault("kiteconnect", MagicMock())

# Minimal common_lib stub
_mock_common_lib = MagicMock()
import pytz  # noqa: E402 — available in the project venv

IST = pytz.timezone("Asia/Kolkata")
_mock_common_lib.IST = IST
_mock_common_lib.get_ist_now.return_value = datetime.datetime.now(tz=IST)
sys.modules.setdefault("common_lib", _mock_common_lib)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_fake_instruments(count: int = 10):
    """Return a list of fake instrument dicts for use as kite.instruments() return value."""
    return [
        {
            "instrument_token": 1000 + i,
            "tradingsymbol": f"FAKE{i:04d}CE",
            "name": "FAKEUNDERLYING",
            "expiry": "2026-06-26",
            "strike": 20000.0 + i * 50,
            "instrument_type": "CE",
            "lot_size": 50,
            "segment": "NFO-OPT",
            "exchange": "NFO",
        }
        for i in range(count)
    ]


@pytest.fixture()
def tmp_db(tmp_path: os.PathLike) -> Generator[str, None, None]:
    """Provide a temporary DB path and patch instrument_cache constants."""
    db_path = str(tmp_path / "instruments.db")
    marker_path = str(tmp_path / ".last_instrument_sync")

    with (
        patch("instrument_cache.DB_PATH", db_path),
        patch("instrument_cache.SYNC_MARKER_PATH", marker_path),
    ):
        import instrument_cache  # noqa: PLC0415

        instrument_cache.init_db()
        yield db_path


def _get_table_count(db_path: str, table: str) -> int:
    """Return the row count of *table* inside *db_path*."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        return cur.fetchone()[0]
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def _table_exists(db_path: str, table: str) -> bool:
    """Return True if *table* exists in the SQLite DB at *db_path*."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cur.fetchone()[0] > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# sync_instruments — happy path
# ---------------------------------------------------------------------------


class TestSyncInstrumentsSuccess:
    """sync_instruments() should atomically populate the live table."""

    def test_instruments_populated_after_sync(self, tmp_db: str) -> None:
        """Arrange: empty DB + 10 fake instruments from Kite.
        Act: call sync_instruments.
        Assert: live instruments table has 10 rows.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(10)

        result = instrument_cache.sync_instruments(kite)

        assert result is True
        assert _get_table_count(tmp_db, "instruments") == 10

    def test_backup_table_created_after_sync(self, tmp_db: str) -> None:
        """Arrange: two successive syncs.
        Act: second sync.
        Assert: instruments_backup holds rows from the first sync.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(5)
        instrument_cache.sync_instruments(kite)  # first sync → backup will be saved

        kite.instruments.return_value = _make_fake_instruments(8)
        instrument_cache.sync_instruments(kite)  # second sync

        assert _get_table_count(tmp_db, "instruments") == 8
        assert _get_table_count(tmp_db, "instruments_backup") == 5

    def test_sync_marker_written_on_success(self, tmp_db: str, tmp_path) -> None:
        """Arrange: no sync marker.
        Act: successful sync.
        Assert: .last_instrument_sync file is written with an ISO timestamp.
        """
        import instrument_cache  # noqa: PLC0415

        marker_path = str(tmp_path / ".last_instrument_sync")
        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(3)

        with patch("instrument_cache.SYNC_MARKER_PATH", marker_path):
            instrument_cache.sync_instruments(kite)

        assert os.path.exists(marker_path)
        content = open(marker_path).read().strip()
        import datetime  # noqa: PLC0415

        datetime.datetime.fromisoformat(content)  # should not raise

    def test_sync_history_row_written_success(self, tmp_db: str) -> None:
        """Arrange: empty DB.
        Act: successful sync.
        Assert: sync_history contains a SUCCESS row.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(2)
        instrument_cache.sync_instruments(kite)

        conn = sqlite3.connect(tmp_db)
        cur = conn.execute("SELECT status FROM sync_history ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "SUCCESS"


# ---------------------------------------------------------------------------
# sync_instruments — failure path
# ---------------------------------------------------------------------------


class TestSyncInstrumentsFailure:
    """When Kite API fails, the live table must remain untouched."""

    def test_live_table_preserved_on_api_failure(self, tmp_db: str) -> None:
        """Arrange: pre-populate live table with 5 rows.
        Act: sync with API raising an exception.
        Assert: live table still has 5 rows.
        """
        import instrument_cache  # noqa: PLC0415

        # Pre-populate via a successful sync
        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(5)
        instrument_cache.sync_instruments(kite)

        # Now simulate API failure
        kite.instruments.side_effect = RuntimeError("network error")
        result = instrument_cache.sync_instruments(kite)

        assert result is False
        assert _get_table_count(tmp_db, "instruments") == 5

    def test_staging_table_cleaned_up_on_failure(self, tmp_db: str) -> None:
        """Arrange: sync where API fails partway.
        Act: failed sync.
        Assert: instruments_new does not exist in the DB.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.side_effect = OSError("connection refused")
        instrument_cache.sync_instruments(kite)

        assert not _table_exists(tmp_db, "instruments_new")

    def test_sync_history_row_written_failure(self, tmp_db: str) -> None:
        """Arrange: empty DB.
        Act: failed sync (API error).
        Assert: sync_history contains a FAILED row with an error message.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.side_effect = ValueError("bad credentials")
        instrument_cache.sync_instruments(kite)

        conn = sqlite3.connect(tmp_db)
        cur = conn.execute(
            "SELECT status, error_message FROM sync_history ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "FAILED"
        assert "bad credentials" in row[1]

    def test_sync_marker_not_written_on_failure(self, tmp_db: str, tmp_path) -> None:
        """Arrange: no marker file.
        Act: failed sync.
        Assert: marker file is NOT created.
        """
        import instrument_cache  # noqa: PLC0415

        marker_path = str(tmp_path / ".last_instrument_sync")
        kite = MagicMock()
        kite.instruments.side_effect = ConnectionError("timeout")

        with patch("instrument_cache.SYNC_MARKER_PATH", marker_path):
            instrument_cache.sync_instruments(kite)

        assert not os.path.exists(marker_path)


# ---------------------------------------------------------------------------
# needs_sync
# ---------------------------------------------------------------------------


class TestNeedsSync:
    """needs_sync() edge-case coverage."""

    def test_returns_true_when_db_missing(self, tmp_path) -> None:
        """Arrange: non-existent DB path.
        Act: needs_sync().
        Assert: True.
        """
        import instrument_cache  # noqa: PLC0415

        with (
            patch("instrument_cache.DB_PATH", str(tmp_path / "missing.db")),
            patch("instrument_cache.SYNC_MARKER_PATH", str(tmp_path / ".marker")),
        ):
            assert instrument_cache.needs_sync() is True

    def test_returns_true_when_instruments_table_empty(self, tmp_db: str) -> None:
        """Arrange: DB exists, marker exists, but instruments table is empty.
        Act: needs_sync().
        Assert: True (data-loss guard).
        """
        import instrument_cache  # noqa: PLC0415

        # Write a marker as if the sync just ran (so normal path says "no sync needed")
        now = datetime.datetime.now(tz=IST)
        marker_path = tmp_db.replace("instruments.db", ".last_instrument_sync")
        with patch("instrument_cache.SYNC_MARKER_PATH", marker_path):
            with open(marker_path, "w") as fh:
                fh.write(now.isoformat())

            # Ensure the table is empty (init_db creates it, but no rows)
            assert _get_table_count(tmp_db, "instruments") == 0
            assert instrument_cache.needs_sync() is True

    def test_returns_false_when_synced_today_after_9am(self, tmp_db: str, tmp_path) -> None:
        """Arrange: marker written after today's 9 AM, table populated.
        Act: needs_sync().
        Assert: False.
        """
        import instrument_cache  # noqa: PLC0415

        # Populate the table so the empty-table guard doesn't fire
        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(2)

        marker_path = str(tmp_path / ".last_instrument_sync")
        # Use a fixed time of 10:00 AM IST so the marker written by sync_instruments
        # appears to be post-9-AM and hence no re-sync is needed.
        fixed_now = datetime.datetime.now(tz=IST).replace(
            hour=10, minute=0, second=0, microsecond=0
        )
        with (
            patch("instrument_cache.SYNC_MARKER_PATH", marker_path),
            patch("instrument_cache.get_ist_now", return_value=fixed_now),
        ):
            instrument_cache.sync_instruments(kite)
            # needs_sync() also calls get_ist_now; it will see 10 AM
            # and the marker was just written at 10 AM → no sync needed
            assert instrument_cache.needs_sync() is False


# ---------------------------------------------------------------------------
# restore_from_backup
# ---------------------------------------------------------------------------


class TestRestoreFromBackup:
    """restore_from_backup() should copy backup rows into an empty live table."""

    def test_restores_rows_into_empty_live_table(self, tmp_db: str) -> None:
        """Arrange: successful sync (populates backup after second sync), then clear live.
        Act: restore_from_backup().
        Assert: live table has the expected row count.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(7)
        instrument_cache.sync_instruments(kite)  # populates live

        # Second sync so that previous data goes into backup
        kite.instruments.return_value = _make_fake_instruments(9)
        instrument_cache.sync_instruments(kite)

        # Clear live table to simulate data loss
        instrument_cache.clear_db()
        assert _get_table_count(tmp_db, "instruments") == 0

        restored = instrument_cache.restore_from_backup()

        assert restored is True
        # Backup had 7 rows (from first sync)
        assert _get_table_count(tmp_db, "instruments") == 7

    def test_no_restore_when_live_table_has_data(self, tmp_db: str) -> None:
        """Arrange: live table already populated.
        Act: restore_from_backup().
        Assert: returns False, live table unchanged.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(4)
        instrument_cache.sync_instruments(kite)
        instrument_cache.sync_instruments(kite)  # ensure backup exists too

        result = instrument_cache.restore_from_backup()

        assert result is False
        assert _get_table_count(tmp_db, "instruments") == 4

    def test_returns_false_when_backup_also_empty(self, tmp_db: str) -> None:
        """Arrange: both live and backup tables are empty.
        Act: restore_from_backup().
        Assert: returns False.
        """
        import instrument_cache  # noqa: PLC0415

        result = instrument_cache.restore_from_backup()
        assert result is False


# ---------------------------------------------------------------------------
# init_db — must not race a concurrent sync's staging table
# ---------------------------------------------------------------------------


class TestInitDbSyncRaceGuard:
    """init_db()'s orphan-cleanup DROP must not corrupt an in-progress sync.

    Regression test for a bug where init_db() unconditionally dropped
    instruments_new, so any unrelated call to init_db() firing while
    sync_instruments() was mid-fetch would delete the sync's staging table
    out from under it (observed in prod as "no such table: instruments_new").
    """

    def test_init_db_skips_drop_while_sync_lock_held(self, tmp_db: str) -> None:
        """Arrange: simulate an in-progress sync by holding _sync_lock and
        creating instruments_new with data in it.
        Act: call init_db() (as an unrelated concurrent caller would).
        Assert: instruments_new is left untouched.
        """
        import instrument_cache  # noqa: PLC0415

        conn = sqlite3.connect(tmp_db)
        conn.execute(f"CREATE TABLE instruments_new ({instrument_cache._INSTRUMENTS_DDL})")  # noqa: SLF001
        conn.execute(
            "INSERT INTO instruments_new (instrument_token, tradingsymbol) VALUES (1, 'INPROGRESS')"
        )
        conn.commit()
        conn.close()

        assert instrument_cache._sync_lock.acquire(blocking=False)  # noqa: SLF001
        try:
            instrument_cache.init_db()
        finally:
            instrument_cache._sync_lock.release()  # noqa: SLF001

        assert _table_exists(tmp_db, "instruments_new")
        assert _get_table_count(tmp_db, "instruments_new") == 1

    def test_init_db_drops_orphan_when_no_sync_running(self, tmp_db: str) -> None:
        """Arrange: a leftover instruments_new table from a crashed sync,
        with no sync currently in progress (_sync_lock free).
        Act: call init_db().
        Assert: the orphaned staging table is cleaned up as before.
        """
        import instrument_cache  # noqa: PLC0415

        conn = sqlite3.connect(tmp_db)
        conn.execute(f"CREATE TABLE instruments_new ({instrument_cache._INSTRUMENTS_DDL})")  # noqa: SLF001
        conn.commit()
        conn.close()

        instrument_cache.init_db()

        assert not _table_exists(tmp_db, "instruments_new")


# ---------------------------------------------------------------------------
# clear_db
# ---------------------------------------------------------------------------


class TestClearDb:
    """clear_db() should wipe the live table but not the backup."""

    def test_clears_live_table(self, tmp_db: str) -> None:
        """Arrange: populated instruments table.
        Act: clear_db().
        Assert: instruments table is empty.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(6)
        instrument_cache.sync_instruments(kite)
        instrument_cache.sync_instruments(kite)  # ensure backup too

        instrument_cache.clear_db()

        assert _get_table_count(tmp_db, "instruments") == 0

    def test_does_not_clear_backup(self, tmp_db: str) -> None:
        """Arrange: two syncs (so backup has data).
        Act: clear_db().
        Assert: instruments_backup is preserved.
        """
        import instrument_cache  # noqa: PLC0415

        kite = MagicMock()
        kite.instruments.return_value = _make_fake_instruments(5)
        instrument_cache.sync_instruments(kite)

        kite.instruments.return_value = _make_fake_instruments(8)
        instrument_cache.sync_instruments(kite)

        backup_before = _get_table_count(tmp_db, "instruments_backup")
        instrument_cache.clear_db()

        assert _get_table_count(tmp_db, "instruments_backup") == backup_before
