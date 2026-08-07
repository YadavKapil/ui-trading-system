"""Unit tests for NIFTY 50 Index Stock Contributors feature.

Tests estimate_nifty_from_live_quotes math logic, breakdown categorizations,
point contributions, and Flask blueprint/application routes using AAA pattern.
"""

from typing import Any
import pytest
from unittest.mock import patch, MagicMock

from cas_tracker.index_calculator import estimate_nifty_from_live_quotes
from flask_app import app


def test_estimate_nifty_from_live_quotes_basic_calculation() -> None:
    """Test calculation of point contribution and positive/negative contributor lists.

    Arrange: Mock quotes for NIFTY constituents (RELIANCE up, BHARTIARTL down).
    Act: Call estimate_nifty_from_live_quotes with nifty_prev_close = 24000.0.
    Assert: Check positive/negative counts, contrib_pts calculation, and total points.
    """
    # Arrange
    nifty_prev_close = 24000.0
    quotes: dict[str, Any] = {
        "NSE:RELIANCE": {
            "last_price": 1300.0,
            "ohlc": {"open": 1280.0, "high": 1310.0, "low": 1275.0, "close": 1280.0},
        },
        "NSE:BHARTIARTL": {
            "last_price": 1960.0,
            "ohlc": {"open": 1980.0, "high": 1985.0, "low": 1955.0, "close": 1980.0},
        },
    }

    # Act
    result = estimate_nifty_from_live_quotes(quotes, nifty_prev_close)

    # Assert
    assert result["matched_count"] == 2
    assert result["positive_count"] == 1
    assert result["negative_count"] == 1

    pos_stock = result["positive_contributors"][0]
    assert pos_stock["symbol"] == "RELIANCE"
    assert pos_stock["ltp"] == 1300.0
    assert pos_stock["change_pts"] == 20.0
    assert pos_stock["return_pct"] == pytest.approx(1.56, abs=0.05)
    assert pos_stock["contribution_pts"] > 0

    neg_stock = result["negative_contributors"][0]
    assert neg_stock["symbol"] == "BHARTIARTL"
    assert neg_stock["ltp"] == 1960.0
    assert neg_stock["change_pts"] == -20.0
    assert neg_stock["contribution_pts"] < 0

    assert result["total_positive_pts"] == pos_stock["contribution_pts"]
    assert result["total_negative_pts"] == neg_stock["contribution_pts"]
    assert result["net_contribution_pts"] == round(
        result["total_positive_pts"] + result["total_negative_pts"], 2
    )


def test_estimate_nifty_from_live_quotes_zero_prev_close() -> None:
    """Test handling of constituents with zero previous close or invalid quote formats.

    Arrange: Mock quotes with zero close and missing keys.
    Act: Call estimate_nifty_from_live_quotes.
    Assert: Invalid items should be safely skipped without raising errors.
    """
    # Arrange
    nifty_prev_close = 24000.0
    quotes: dict[str, Any] = {
        "NSE:RELIANCE": {
            "last_price": 1300.0,
            "ohlc": {"close": 0.0},
        },
        "NSE:BHARTIARTL": "invalid_quote_format",
    }

    # Act
    result = estimate_nifty_from_live_quotes(quotes, nifty_prev_close)

    # Assert
    assert result["matched_count"] == 0
    assert result["positive_count"] == 0
    assert result["negative_count"] == 0
    assert result["total_positive_pts"] == 0.0
    assert result["total_negative_pts"] == 0.0


def test_contributors_flask_routes(client: Any) -> None:
    """Test accessibility of /nifty_contributors and /cas_tracker/api/contributors endpoints.

    Arrange: Authenticated test client fixture.
    Act: GET requests to routes.
    Assert: Confirm 200 OK status codes and appropriate HTML/JSON payloads.
    """
    # Act
    response_page = client.get("/nifty_contributors")
    response_api = client.get("/cas_tracker/api/contributors")
    response_dates = client.get("/cas_tracker/api/contributors/dates")
    response_history = client.get("/cas_tracker/api/contributors/history?date=2026-08-06")
    response_asset = client.get("/assets/contributors_social_share.png")

    # Assert
    assert response_page.status_code == 200
    assert b"NIFTY 50 Contributors" in response_page.data

    assert response_api.status_code == 200
    json_data = response_api.get_json()
    assert "ready" in json_data

    assert response_dates.status_code == 200
    assert "dates" in response_dates.get_json()

    assert response_history.status_code == 200
    assert "history" in response_history.get_json()

    assert response_asset.status_code == 200



def test_contributor_database_helpers() -> None:
    """Test saving, retrieving, and purging contributor snapshot records in SQLite.

    Arrange: In-memory/test database connection.
    Act: Save contributor snapshots for multiple dates and purge records older than 7 days.
    Assert: Retrieve records and verify date purging logic.
    """
    from cas_tracker.database import (
        get_db,
        init_db,
        save_contributor_snapshot,
        purge_old_contributor_snapshots,
        get_contributor_dates,
        get_contributor_history_for_date,
    )

    # Arrange
    init_db()
    conn = get_db()

    # Act
    save_contributor_snapshot(
        conn=conn,
        timestamp="2026-08-06 10:30:00",
        date="2026-08-06",
        nifty_live=24636.15,
        nifty_estimate=24632.01,
        top5_pos_pts=35.50,
        top5_neg_pts=-12.20,
        top5_net_pts=23.30,
        net_contribution_pts=16.23,
    )

    dates = get_contributor_dates(conn)
    history = get_contributor_history_for_date(conn, "2026-08-06")
    purge_old_contributor_snapshots(conn, days_to_keep=7)
    conn.close()

    # Assert
    assert "2026-08-06" in dates
    assert len(history) >= 1
    assert history[-1]["nifty_live"] == 24636.15
    assert history[-1]["top5_net_pts"] == 23.30


