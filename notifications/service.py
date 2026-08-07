"""Central notification dispatch service.

Every notification flows through ``dispatch()``:
  1. Persisted to SQLite (durable, survives process restart).
  2. Fanned out to each configured channel in order.
  3. ``sent_at`` updated on the DB row.

Adding a new notification type requires only a new entry in ROUTING_TABLE and
SEVERITY_TABLE — no other code changes.
"""

import logging
from typing import Any, Optional

from notifications import database
from notifications import telegram
from notifications import webpush

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-type routing configuration
# ---------------------------------------------------------------------------

ROUTING_TABLE: dict[str, list[str]] = {
    "KITE_LOGIN_CHECK":         ["telegram", "web_push", "in_app"],
    "COPY_ACCOUNT_ERROR":       ["telegram", "in_app"],
    "COPY_ORDER_TRIGGERED":     ["telegram", "in_app"],
    "EARLY_EXIT_REQUIRED":      ["telegram", "web_push", "in_app"],
    "ORDER_EXECUTED":           ["telegram", "in_app"],
    "ORDER_CANCELLED":          ["telegram", "in_app"],
    "IV_SPIKE_DETECTED":        ["telegram", "in_app"],
    "DAILY_PNL_SUMMARY":        ["telegram", "in_app"],
    "NIFTY_MARGIN_ALERT":       ["telegram", "in_app"],
    "NIFTY_DELTA_ALERT":        ["telegram", "in_app"],
    "NIFTY_ITM_LOSS_ALERT":     ["telegram", "web_push", "in_app"],
    # Intraday stock paper-trading strategy
    "INTRADAY_TRADE_TRIGGERED": ["telegram", "web_push", "in_app"],
    "INTRADAY_TP_HIT":          ["telegram", "web_push", "in_app"],
    "INTRADAY_SL_HIT":          ["telegram", "web_push", "in_app"],
    "INTRADAY_EOD_SUMMARY":     ["telegram", "in_app"],
    # GTT vs position mismatch monitor
    "GTT_POSITION_EXCESS":      ["telegram", "web_push", "in_app"],
    "GTT_ORPHANED":             ["telegram", "web_push", "in_app"],
    # Morning auto-sync
    "MORNING_SYNC_FIRED":       ["telegram", "in_app"],
    # CopyTrade WebSocket health
    "COPYTRADE_WS_DOWN":        ["telegram", "web_push", "in_app"],
    "COPYTRADE_WS_RECOVERED":   ["telegram", "in_app"],
    # Duplicate orders from multiple algos on the same symbol within 2%
    "DUPLICATE_ORDER_ALERT":    ["telegram", "web_push", "in_app"],
    # EOD GTT cleanup sweep (3:31 PM safety net)
    "EOD_GTT_CLEANUP":          ["telegram", "in_app"],
    # CopyTrade account available margin below 10% of total (used + available)
    "COPY_MARGIN_LOW":          ["telegram", "web_push", "in_app"],
    # Position Guard: symbol carrying unreviewed long ("BUY") exposure across
    # existing position + regular orders + GTTs
    "POSITION_GUARD_ALERT":     ["telegram", "web_push", "in_app"],
    # Intraday Algo: live scan-triggered options alerts requiring Take/Paper/Ignore
    "INTRADAY_ALGO_SCAN_TRIGGERED": ["telegram", "web_push", "in_app"],
    "INTRADAY_ALGO_TRADE_TAKEN":    ["telegram", "in_app"],
    "INTRADAY_ALGO_STOP_HIT":       ["telegram", "in_app"],
    "INTRADAY_ALGO_TARGET_HIT":     ["telegram", "in_app"],
    # NIFTY price enters/exits a swing_levels combined confluence zone
    "NIFTY_ZONE_ALERT":             ["telegram", "web_push", "in_app"],
}

SEVERITY_TABLE: dict[str, str] = {
    "KITE_LOGIN_CHECK":         "error",
    "COPY_ACCOUNT_ERROR":       "error",
    "COPY_ORDER_TRIGGERED":     "info",
    "EARLY_EXIT_REQUIRED":      "warning",
    "ORDER_EXECUTED":           "info",
    "ORDER_CANCELLED":          "warning",
    "IV_SPIKE_DETECTED":        "warning",
    "DAILY_PNL_SUMMARY":        "info",
    "NIFTY_MARGIN_ALERT":       "warning",
    "NIFTY_DELTA_ALERT":        "warning",
    "NIFTY_ITM_LOSS_ALERT":     "warning",
    # Intraday stock paper-trading strategy
    "INTRADAY_TRADE_TRIGGERED": "info",
    "INTRADAY_TP_HIT":          "info",
    "INTRADAY_SL_HIT":          "warning",
    "INTRADAY_EOD_SUMMARY":     "info",
    # GTT vs position mismatch monitor
    "GTT_POSITION_EXCESS":      "warning",
    "GTT_ORPHANED":             "warning",
    # Morning auto-sync
    "MORNING_SYNC_FIRED":       "info",
    # CopyTrade WebSocket health
    "COPYTRADE_WS_DOWN":        "error",
    "COPYTRADE_WS_RECOVERED":   "info",
    # Duplicate orders from multiple algos on the same symbol within 2%
    "DUPLICATE_ORDER_ALERT":    "warning",
    # EOD GTT cleanup sweep (3:31 PM safety net)
    "EOD_GTT_CLEANUP":          "info",
    # CopyTrade account available margin below 10% of total (used + available)
    "COPY_MARGIN_LOW":          "warning",
    # Position Guard: symbol carrying unreviewed long ("BUY") exposure across
    # existing position + regular orders + GTTs
    "POSITION_GUARD_ALERT":     "warning",
    # Intraday Algo: live scan-triggered options alerts requiring Take/Paper/Ignore
    "INTRADAY_ALGO_SCAN_TRIGGERED": "info",
    "INTRADAY_ALGO_TRADE_TAKEN":    "info",
    "INTRADAY_ALGO_STOP_HIT":       "warning",
    "INTRADAY_ALGO_TARGET_HIT":     "info",
    # NIFTY price enters/exits a swing_levels combined confluence zone
    "NIFTY_ZONE_ALERT":             "warning",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def dispatch(
    notification_type: str,
    title: str,
    body: str,
    metadata: Optional[dict[str, Any]] = None,
) -> int:
    """Persist and fan-out a notification to all configured channels.

    A channel failure is logged but never re-raised — the remaining channels
    are always attempted. The notification is always saved to the DB first so
    it can be surfaced as an in-app toast on the user's next visit even if all
    other channels fail.

    Args:
        notification_type: Key into ROUTING_TABLE / SEVERITY_TABLE
            (e.g. 'KITE_LOGIN_CHECK').
        title: Short heading shown in toast / push notification.
        body: Full notification message.
        metadata: Optional extra context stored as JSON in the DB row.

    Returns:
        The SQLite row id of the newly created notification.
    """
    severity = SEVERITY_TABLE.get(notification_type, "info")
    channels = ROUTING_TABLE.get(notification_type, ["in_app"])

    notification_id = database.save_notification(
        notification_type=notification_type,
        title=title,
        body=body,
        severity=severity,
        channels=channels,
        metadata=metadata,
    )

    logger.info(
        "Notification dispatched: type=%s id=%d channels=%s",
        notification_type,
        notification_id,
        channels,
    )

    for channel in channels:
        if channel == "in_app":
            continue  # passive — frontend polls /notifications/unread
        _send_to_channel(channel, notification_type, title, body, severity, notification_id)

    database.mark_sent(notification_id)
    return notification_id


def _send_to_channel(
    channel: str,
    notification_type: str,
    title: str,
    body: str,
    severity: str,
    notification_id: int,
) -> None:
    """Attempt delivery to a single channel, swallowing all exceptions.

    Args:
        channel: Channel name — 'telegram' or 'web_push'.
        notification_type: Notification type key for type-specific formatting.
        title: Notification title.
        body: Notification body.
        severity: Severity level for formatting.
        notification_id: DB row id for correlation.
    """
    try:
        if channel == "telegram":
            message_text = telegram.format_notification(notification_type, title, body, severity)
            telegram.send_message(message_text)
        elif channel == "web_push":
            webpush.push_to_all(title, body, notification_id)
        else:
            logger.warning("Unknown notification channel '%s' — skipping", channel)
    except Exception as exc:
        logger.error(
            "Notification channel '%s' raised unexpectedly for id=%d: %s",
            channel,
            notification_id,
            exc,
        )
