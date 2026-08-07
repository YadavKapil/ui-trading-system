"""Tests for the precomputed CAS tick digests behind /api/candles.

A raw CAS snapshot is ~150 KB of NSE JSON and the chart endpoint used to
re-parse every snapshot for the date on each rebuild (~23 MB for one
session). Each snapshot now carries a compact digest holding only the
numbers the chart needs.

The property that matters most is equivalence: the digest path must produce
exactly what the raw path produced, including on the awkward inputs (empty
payloads, unparseable prices, anchors arriving late).

Uses a real on-disk SQLite database (no mocking of the storage layer).
"""

import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cas_tracker import database  # noqa: E402
from cas_tracker.index_calculator import (  # noqa: E402
    estimate_nifty_from_cas,
    estimate_nifty_from_digest,
    extract_cas_digest,
)
from cas_tracker.nifty50_weights import NIFTY50_WEIGHTS_PCT  # noqa: E402


CONSTITUENTS = list(NIFTY50_WEIGHTS_PCT.keys())


def _row(symbol: str, iep: float, ref: float, qty: float = 1000.0) -> dict:
    """Build one CAS payload row in the shape NSE returns."""
    return {
        "symbol": symbol,
        "iep": iep,
        "refrencePrice": ref,
        "totTradedQty": qty,
        # Padding that exists in the real payload and nothing reads — the
        # whole point of the digest is to not carry this around.
        "noise": "x" * 200,
    }


def _payload(count: int = 10, iep: float = 110.0, ref: float = 100.0) -> list[dict]:
    """Build a CAS `data` array covering `count` real constituents."""
    return [_row(sym, iep, ref) for sym in CONSTITUENTS[:count]]


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A real, empty CAS database."""
    monkeypatch.setattr(database, "DB_PATH", str(tmp_path / "digests.db"))
    database.init_db()
    return database


# ---------------------------------------------------------------- equivalence


@pytest.mark.parametrize(
    "nifty_anchor,stock_anchors",
    [
        (None, None),
        (24570.2, None),
        (24570.2, {sym: 95.0 for sym in CONSTITUENTS[:10]}),
        (24570.2, {sym: 95.0 for sym in CONSTITUENTS[:3]}),  # partial coverage
    ],
    ids=["no-anchors", "nifty-only", "full-stock-anchors", "partial-stock-anchors"],
)
def test_digest_path_matches_raw_path(nifty_anchor, stock_anchors):
    """The two paths must agree under every anchor configuration."""
    rows = _payload()
    expected = estimate_nifty_from_cas(rows, nifty_anchor, stock_anchors)
    actual = estimate_nifty_from_digest(extract_cas_digest(rows), nifty_anchor, stock_anchors)
    assert actual == expected


def test_digest_survives_json_round_trip():
    """Digests are stored as JSON, so a revived one must behave identically."""
    rows = _payload()
    digest = extract_cas_digest(rows)
    revived = json.loads(json.dumps(digest))

    assert estimate_nifty_from_digest(revived, 24570.2, None) == estimate_nifty_from_digest(
        digest, 24570.2, None
    )


def test_zero_price_rows_are_excluded_from_the_average():
    """IEP is 0 before a stock's auction primes — that is 'no price yet'.

    Treating it as a real price would read as a -100% return on that name.
    """
    rows = _payload(count=5) + [_row(CONSTITUENTS[5], 0.0, 100.0)]
    result = estimate_nifty_from_digest(extract_cas_digest(rows), None, None)

    assert result["matched_count"] == 5
    assert result == estimate_nifty_from_cas(rows, None, None)


def test_unparseable_price_matches_raw_path():
    """A junk price drops the row on both paths, not just one."""
    rows = _payload(count=4) + [_row(CONSTITUENTS[4], "n/a", 100.0)]

    assert estimate_nifty_from_digest(
        extract_cas_digest(rows), 24570.2, None
    ) == estimate_nifty_from_cas(rows, 24570.2, None)


def test_non_constituents_are_dropped():
    """Only NIFTY 50 names carry weight; anything else is noise."""
    rows = _payload(count=3) + [_row("NOTANINDEXNAME", 110.0, 100.0)]
    digest = extract_cas_digest(rows)

    assert [r[0] for r in digest["rows"]] == CONSTITUENTS[:3]
    assert estimate_nifty_from_digest(digest, None, None) == estimate_nifty_from_cas(
        rows, None, None
    )


def test_empty_payload_yields_no_digest():
    """None is what tells the read path to drop the tick entirely."""
    assert extract_cas_digest([]) is None


def test_late_arriving_anchors_change_the_result():
    """Anchors must NOT be baked into the digest.

    They are captured asynchronously and can land after a snapshot is
    written; a stored estimate would silently freeze the pre-anchor value.
    """
    digest = extract_cas_digest(_payload())

    without = estimate_nifty_from_digest(digest, 24570.2, None)
    with_anchors = estimate_nifty_from_digest(
        digest, 24570.2, {sym: 95.0 for sym in CONSTITUENTS[:10]}
    )

    assert without["nifty_estimate"] != with_anchors["nifty_estimate"]


def test_digest_is_substantially_smaller():
    """The whole point: stop carrying ~150 KB of unread fields per tick."""
    rows = _payload(count=50)
    raw_size = len(json.dumps({"data": rows}))
    digest_size = len(json.dumps(extract_cas_digest(rows), separators=(",", ":")))

    assert digest_size * 5 < raw_size, f"digest {digest_size} vs raw {raw_size}"


# ------------------------------------------------------------------- storage


def test_save_snapshot_writes_digest_atomically(db):
    """Snapshot and digest are one transaction, so they cannot drift."""
    conn = db.get_db()
    db.save_snapshot(conn, "2026-08-07 15:20:00", {"data": _payload()})

    assert db.count_snapshots_missing_digest(conn) == 0
    digests = db.get_tick_digests_for_date(conn, "2026-08-07")
    assert len(digests) == 1
    assert digests[0]["timestamp"] == "2026-08-07 15:20:00"
    assert len(digests[0]["digest"]["rows"]) == 10
    conn.close()


def test_empty_payload_is_recorded_as_null_not_skipped(db):
    """Empty payloads still get a row, else backfill re-runs forever."""
    conn = db.get_db()
    db.save_snapshot(conn, "2026-08-07 15:14:00", {"data": []})

    assert db.count_snapshots_missing_digest(conn) == 0
    digests = db.get_tick_digests_for_date(conn, "2026-08-07")
    assert digests[0]["digest"] is None
    conn.close()


def test_backfill_covers_snapshots_written_before_digests_existed(db):
    """Historical rows must convert without a separate migration step."""
    conn = db.get_db()
    for minute in range(5):
        conn.execute(
            "INSERT INTO snapshots (timestamp, raw_json) VALUES (?, ?)",
            (f"2026-08-05 15:2{minute}:00", json.dumps({"data": _payload()})),
        )
    conn.commit()
    assert db.count_snapshots_missing_digest(conn) == 5

    written = db.backfill_tick_digests(conn)

    assert written == 5
    assert db.count_snapshots_missing_digest(conn) == 0
    conn.close()


def test_backfill_terminates_on_all_empty_payloads(db):
    """A session opens with empty payloads before the auction primes.

    An earlier version treated "nothing stored" as "stop", which abandoned
    the rest of the date; the rows must instead all be accounted for.
    """
    conn = db.get_db()
    for i in range(5):
        conn.execute(
            "INSERT INTO snapshots (timestamp, raw_json) VALUES (?, ?)",
            (f"2026-08-05 15:1{i}:00", json.dumps({"data": []})),
        )
    # A real payload *after* the empty ones — this is what used to be missed.
    conn.execute(
        "INSERT INTO snapshots (timestamp, raw_json) VALUES (?, ?)",
        ("2026-08-05 15:20:00", json.dumps({"data": _payload()})),
    )
    conn.commit()

    db.backfill_tick_digests(conn, batch_size=2)

    assert db.count_snapshots_missing_digest(conn) == 0
    digests = db.get_tick_digests_for_date(conn, "2026-08-05")
    assert [d["digest"] is None for d in digests] == [True] * 5 + [False]
    conn.close()


def test_backfill_is_idempotent(db):
    """Re-running must not duplicate or corrupt rows."""
    conn = db.get_db()
    db.save_snapshot(conn, "2026-08-07 15:20:00", {"data": _payload()})

    assert db.backfill_tick_digests(conn) == 0
    assert conn.execute("SELECT COUNT(*) FROM cas_tick_digests").fetchone()[0] == 1
    conn.close()


def test_digests_are_ordered_chronologically(db):
    """Volume deltas depend on tick order, so this cannot be left to chance."""
    conn = db.get_db()
    for minute in (30, 10, 20):
        db.save_snapshot(conn, f"2026-08-07 15:{minute}:00", {"data": _payload()})

    stamps = [d["timestamp"] for d in db.get_tick_digests_for_date(conn, "2026-08-07")]

    assert stamps == sorted(stamps)
    conn.close()
