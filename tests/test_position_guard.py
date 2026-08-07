"""Unit tests for position_guard (detector + ignore-list persistence).

Tests pure logic (no live Kite API, no Flask). Covers:
  - scan_long_exposure grouping across position/regular-orders/GTTs
  - fingerprint stability and change-detection
  - position_guard.db ignore/unignore/auto-expire-on-change behavior (real SQLite,
    per project convention — no DB mocking)
"""

import os
import sys

# Ensure the vibhu package root is on the path so position_guard imports cleanly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from position_guard import db as position_guard_db
from position_guard.detector import scan_long_exposure


# ---------------------------------------------------------------------------
# Fake Kite client
# ---------------------------------------------------------------------------


class _FakeKite:
    """Minimal stand-in for MonitoredKite/KiteConnect returning canned data."""

    def __init__(self, positions=None, orders=None, gtts=None):
        self._positions = positions or {"net": [], "day": []}
        self._orders = orders or []
        self._gtts = gtts or []

    def positions(self):
        return self._positions

    def orders(self):
        return self._orders

    def get_gtts(self):
        return self._gtts


def _order(symbol, order_id="1", qty=50, price=100.0, status="OPEN",
           txn="BUY", exchange="NFO", tag="Unknown", instrument_token="123"):
    return {
        "order_id": order_id,
        "tradingsymbol": symbol,
        "quantity": qty,
        "price": price,
        "filled_quantity": 0,
        "status": status,
        "transaction_type": txn,
        "exchange": exchange,
        "product": "NRML",
        "variety": "regular",
        "tag": tag,
        "order_timestamp": "2026-07-08 10:00:00",
        "instrument_token": instrument_token,
    }


def _position(symbol, qty=50, exchange="NFO", instrument_token="123"):
    return {
        "tradingsymbol": symbol,
        "quantity": qty,
        "exchange": exchange,
        "instrument_token": instrument_token,
    }


def _gtt(trigger_id=1, symbol="NIFTY25JUN25000CE", qty=50, price=100.0,
         trigger_value=95.0, status="active", txn="BUY"):
    return {
        "id": trigger_id,
        "status": status,
        "condition": {
            "tradingsymbol": symbol,
            "exchange": "NFO",
            "trigger_values": [trigger_value],
        },
        "orders": [
            {
                "tradingsymbol": symbol,
                "transaction_type": txn,
                "quantity": qty,
                "price": price,
                "product": "NRML",
                "order_type": "LIMIT",
            }
        ],
    }


# ---------------------------------------------------------------------------
# scan_long_exposure
# ---------------------------------------------------------------------------


def test_scan_finds_symbol_with_single_pending_order():
    kite = _FakeKite(orders=[_order("NIFTY25JUN25000CE")])
    rows = scan_long_exposure(kite)
    assert len(rows) == 1
    assert rows[0]["tradingsymbol"] == "NIFTY25JUN25000CE"
    assert rows[0]["source_count"] == 1
    assert rows[0]["total_long_qty"] == 50


def test_scan_merges_position_order_and_gtt_into_one_row():
    symbol = "NIFTY25JUN25000CE"
    kite = _FakeKite(
        positions={"net": [_position(symbol, qty=50)], "day": []},
        orders=[_order(symbol, order_id="1", qty=50)],
        gtts=[_gtt(trigger_id=1, symbol=symbol, qty=50)],
    )
    rows = scan_long_exposure(kite)
    assert len(rows) == 1
    row = rows[0]
    assert row["source_count"] == 3  # position + regular order + gtt
    assert row["total_long_qty"] == 150
    assert len(row["regular_orders"]) == 1
    assert len(row["gtts"]) == 1


def test_short_position_fully_offset_by_buys_is_excluded():
    symbol = "ASIANPAINT25JUL2900CE"
    kite = _FakeKite(
        positions={"net": [_position(symbol, qty=-200)], "day": []},
        gtts=[
            _gtt(trigger_id=1, symbol=symbol, qty=100),
            _gtt(trigger_id=2, symbol=symbol, qty=100),
        ],
    )
    rows = scan_long_exposure(kite)
    assert rows == []


def test_short_position_partially_offset_still_flags_if_net_positive():
    symbol = "ASIANPAINT25JUL2900CE"
    kite = _FakeKite(
        positions={"net": [_position(symbol, qty=-150)], "day": []},
        gtts=[_gtt(trigger_id=1, symbol=symbol, qty=200)],
    )
    rows = scan_long_exposure(kite)
    assert len(rows) == 1
    assert rows[0]["total_long_qty"] == 50
    assert rows[0]["position_qty"] == 0  # not itself long, so not counted as a display source


def test_short_position_still_short_after_buys_is_excluded():
    symbol = "ASIANPAINT25JUL2900CE"
    kite = _FakeKite(
        positions={"net": [_position(symbol, qty=-300)], "day": []},
        gtts=[_gtt(trigger_id=1, symbol=symbol, qty=200)],
    )
    rows = scan_long_exposure(kite)
    assert rows == []


def test_equity_stock_gtt_is_excluded_entirely():
    """Reproduces the original bug report: an equity GTT (not an option) must never appear."""
    symbol = "ASIANPAINT"
    kite = _FakeKite(gtts=[
        _gtt(trigger_id=1, symbol=symbol, qty=100),
        _gtt(trigger_id=2, symbol=symbol, qty=100),
    ])
    rows = scan_long_exposure(kite)
    assert rows == []


def test_stock_futures_are_excluded():
    kite = _FakeKite(orders=[_order("RELIANCE25JUNFUT")])
    rows = scan_long_exposure(kite)
    assert rows == []


def test_scan_ignores_sell_orders_and_closed_orders():
    symbol = "NIFTY25JUN25000CE"
    kite = _FakeKite(orders=[
        _order(symbol, order_id="1", txn="SELL"),
        _order(symbol, order_id="2", status="COMPLETE"),
    ])
    rows = scan_long_exposure(kite)
    assert rows == []


def test_scan_ignores_non_fno_exchange():
    kite = _FakeKite(orders=[_order("RELIANCE", exchange="NSE")])
    rows = scan_long_exposure(kite)
    assert rows == []


def test_scan_ignores_gtt_with_only_sell_children():
    symbol = "NIFTY25JUN25000CE"
    kite = _FakeKite(gtts=[_gtt(symbol=symbol, txn="SELL")])
    rows = scan_long_exposure(kite)
    assert rows == []


def test_scan_returns_empty_on_api_error():
    class _BrokenKite:
        def positions(self):
            raise RuntimeError("boom")

    rows = scan_long_exposure(_BrokenKite())
    assert rows == []


def test_fingerprint_changes_when_order_set_changes():
    symbol = "NIFTY25JUN25000CE"
    kite_one_order = _FakeKite(orders=[_order(symbol, order_id="1")])
    kite_two_orders = _FakeKite(orders=[_order(symbol, order_id="1"), _order(symbol, order_id="2")])

    fp_one = scan_long_exposure(kite_one_order)[0]["fingerprint"]
    fp_two = scan_long_exposure(kite_two_orders)[0]["fingerprint"]
    assert fp_one != fp_two


def test_fingerprint_stable_for_same_inputs():
    symbol = "NIFTY25JUN25000CE"
    kite = _FakeKite(orders=[_order(symbol, order_id="1")])
    fp_a = scan_long_exposure(kite)[0]["fingerprint"]
    fp_b = scan_long_exposure(kite)[0]["fingerprint"]
    assert fp_a == fp_b


def test_indices_sort_before_stocks():
    kite = _FakeKite(orders=[
        _order("RELIANCE25JUL2900CE", order_id="1"),
        _order("BANKNIFTY25JUN45000CE", order_id="2"),
        _order("SENSEX25JUN80000CE", order_id="3"),
        _order("NIFTY25JUN25000CE", order_id="4"),
        _order("TCS25JUL4000PE", order_id="5"),
    ])
    rows = scan_long_exposure(kite)
    symbols_in_order = [row["tradingsymbol"] for row in rows]
    assert symbols_in_order == [
        "NIFTY25JUN25000CE",
        "SENSEX25JUN80000CE",
        "BANKNIFTY25JUN45000CE",
        "RELIANCE25JUL2900CE",
        "TCS25JUL4000PE",
    ]


# ---------------------------------------------------------------------------
# position_guard.db (real SQLite, temp file per project convention)
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """Point position_guard.db at a throwaway SQLite file for this test."""
    db_file = tmp_path / "position_guard_test.db"
    monkeypatch.setattr(position_guard_db, "_DB_PATH", str(db_file))
    position_guard_db.init_db()
    return db_file


def test_ignore_and_is_ignored(temp_db):
    position_guard_db.ignore_symbol("NIFTY25JUN25000CE", "NFO", "fp1")
    assert position_guard_db.is_ignored("NIFTY25JUN25000CE", "NFO", "fp1") is True


def test_is_ignored_false_when_never_ignored(temp_db):
    assert position_guard_db.is_ignored("NIFTY25JUN25000CE", "NFO", "fp1") is False


def test_ignore_auto_expires_when_fingerprint_changes(temp_db):
    position_guard_db.ignore_symbol("NIFTY25JUN25000CE", "NFO", "fp1")
    assert position_guard_db.is_ignored("NIFTY25JUN25000CE", "NFO", "fp2") is False
    # Stale row should have been deleted, so re-checking the old fingerprint also fails now.
    assert position_guard_db.is_ignored("NIFTY25JUN25000CE", "NFO", "fp1") is False


def test_unignore_removes_row(temp_db):
    position_guard_db.ignore_symbol("NIFTY25JUN25000CE", "NFO", "fp1")
    position_guard_db.unignore_symbol("NIFTY25JUN25000CE", "NFO")
    assert position_guard_db.is_ignored("NIFTY25JUN25000CE", "NFO", "fp1") is False


def test_list_ignored_returns_all_rows(temp_db):
    position_guard_db.ignore_symbol("NIFTY25JUN25000CE", "NFO", "fp1")
    position_guard_db.ignore_symbol("BANKNIFTY25JUN45000PE", "NFO", "fp2")
    rows = position_guard_db.list_ignored()
    symbols = {row["tradingsymbol"] for row in rows}
    assert symbols == {"NIFTY25JUN25000CE", "BANKNIFTY25JUN45000PE"}


def test_cleanup_prunes_ignores_for_symbols_missing_from_instruments(temp_db, monkeypatch):
    """Ignore rows for expired contracts (no longer in instruments) get pruned."""
    import instrument_cache

    position_guard_db.ignore_symbol("NIFTY25JUN25000CE", "NFO", "fp1")  # still live
    position_guard_db.ignore_symbol("NIFTY24APR25000CE", "NFO", "fp2")  # expired

    def _fake_get_instrument(symbol, exchange=None):
        return {"tradingsymbol": symbol} if symbol == "NIFTY25JUN25000CE" else None

    monkeypatch.setattr(instrument_cache, "get_instrument", _fake_get_instrument)

    removed = position_guard_db.cleanup_ignored_symbols_not_in_instruments()

    assert removed == 1
    symbols = {row["tradingsymbol"] for row in position_guard_db.list_ignored()}
    assert symbols == {"NIFTY25JUN25000CE"}
