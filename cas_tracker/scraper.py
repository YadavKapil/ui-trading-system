"""Standalone poller for NSE's Closing Auction Session (CAS) indicative data.

Run as its own process (like sensibull/scraper.py), independent of the Flask
app, so redeploying/restarting flask_app.py never interrupts a live poll:

    cd cas_tracker && python3 scraper.py

CAS runs 15:15-15:35 IST (order collection 15:20-15:30, randomized match
15:30-15:35). This script sleeps outside a buffered window (15:14-15:40) and
polls at CAS_POLL_INTERVAL_SECONDS inside it. Outside 15:14-15:40 the NSE
API never returns data, so the window is kept tight to avoid pointless
calls. The endpoint (confirmed live
via manual curl during development) requires no cookies/session state -
a browser-like User-Agent + Referer is sufficient, so no Playwright/headless
browser is needed here (unlike the Sensibull scraper).

The exact field names inside a populated `data[]` row are not published
anywhere (CAS is a brand-new NSE feature). Rather than guess a schema and
risk silently dropping fields, every poll's raw JSON is stored as-is; the
dashboard renders whatever keys are actually present.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# This runs as a standalone process from inside cas_tracker/, but the package
# must be importable as `cas_tracker.*` — the same way Flask imports it — or
# `database` and `cas_tracker.database` become two separate module objects
# with separate state, and the storage layer can't share code with the rest
# of the package.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cas_tracker.database import get_db, init_db, save_snapshot  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_IST_OFFSET = timedelta(hours=5, minutes=30)

CAS_API_URL = "https://www.nseindia.com/api/NextApi/apiClient/casApi?functionName=getCASData"
CAS_REFERER = "https://www.nseindia.com/market-data/closing-auction-session"
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# Buffered around the official 15:15-15:35 window to catch the transition
# in/out, but kept tight since the NSE API returns no data at all outside
# 15:14-15:40 IST — polling wider than this just adds pointless load.
CAS_WINDOW_START = "15:14"
CAS_WINDOW_END = "15:40"
# NSE only recalculates IEP/matched-quantity roughly once every 45-60s
# during CAS (confirmed 2026-08-05 by comparing consecutive 1s polls —
# RELIANCE's IEP and totTradedQty were byte-identical for ~55s at a time).
# 5s polling still catches every real update with margin to spare, at a
# fifth of the API calls that 1s polling made.
CAS_POLL_INTERVAL_SECONDS = 5.0
IDLE_SLEEP_SECONDS = 20
REQUEST_TIMEOUT_SECONDS = 10

_session = requests.Session()
_session.headers.update(
    {
        "User-Agent": _BROWSER_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": CAS_REFERER,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
)


def _ist_now() -> datetime:
    """Return the current time in IST, computed from UTC (server clock may not be IST).

    Returns:
        Naive datetime representing the current IST wall-clock time.
    """
    return (datetime.now(timezone.utc) + _IST_OFFSET).replace(tzinfo=None)


def is_cas_window(now: datetime | None = None) -> bool:
    """Check whether `now` (IST) falls within the buffered CAS polling window.

    Args:
        now: IST datetime to check; defaults to the current IST time.

    Returns:
        True on a weekday between CAS_WINDOW_START and CAS_WINDOW_END IST.
    """
    now = now or _ist_now()
    if now.weekday() > 4:
        return False
    start = datetime.strptime(CAS_WINDOW_START, "%H:%M").time()
    end = datetime.strptime(CAS_WINDOW_END, "%H:%M").time()
    return start <= now.time() <= end


def fetch_cas_data() -> dict | None:
    """Fetch one poll of CAS data from NSE.

    Returns:
        The parsed JSON response, or None if the request failed or the
        response was not valid JSON.
    """
    try:
        response = _session.get(CAS_API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException:
        logger.exception("CAS API request failed")
        return None

    if response.status_code == 429:
        logger.warning("CAS API rate-limited (429); backing off")
        time.sleep(5)
        return None

    if response.status_code != 200:
        logger.warning("CAS API returned HTTP %s: %s", response.status_code, response.text[:200])
        return None

    try:
        return response.json()
    except ValueError:
        logger.exception("CAS API response was not valid JSON")
        return None


def run_poll_loop() -> None:
    """Poll the CAS API continuously, recording data only during the CAS window."""
    init_db()
    conn = get_db()
    logger.info(
        "CAS tracker scraper started (window %s-%s IST, poll interval %ss)",
        CAS_WINDOW_START, CAS_WINDOW_END, CAS_POLL_INTERVAL_SECONDS,
    )

    was_in_window = False
    while True:
        try:
            in_window = is_cas_window()
            if not in_window:
                if was_in_window:
                    logger.info("CAS window closed; resuming idle sleep")
                was_in_window = False
                time.sleep(IDLE_SLEEP_SECONDS)
                continue

            if not was_in_window:
                logger.info("CAS window opened; starting %ss polling", CAS_POLL_INTERVAL_SECONDS)
            was_in_window = True

            data = fetch_cas_data()
            if data is not None:
                timestamp = _ist_now().strftime("%Y-%m-%d %H:%M:%S")
                save_snapshot(conn, timestamp, data)
                row_count = len(data.get("data", []))
                logger.info("Recorded snapshot at %s (%d active rows)", timestamp, row_count)

            time.sleep(CAS_POLL_INTERVAL_SECONDS)
        except Exception:
            logger.exception("Unexpected error in CAS poll loop; continuing")
            time.sleep(5)


if __name__ == "__main__":
    run_poll_loop()
