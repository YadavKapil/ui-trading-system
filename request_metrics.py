"""Lightweight per-route HTTP request metrics, in the same spirit as
kite_api_monitor.py's stats tracking but for Flask request/response cycles
instead of Kite API calls.

Built for comparing the split-off public-only process (public_api.py, port
6010) against the main trading-engine process (flask_app.py, port 5010):
both import cas_tracker.blueprint, and that module tags every request with
this module's PROCESS_LABEL before recording it, so /cas_tracker/api/_metrics
on either process reports only that process's own load — pointing a browser
at both endpoints side by side is the "new system vs old system" comparison.

Thread-safe, in-memory, resets on restart — matches every other lightweight
stats tracker already in this codebase (MonitoredKite's call stats, the
various _*_cache TTL dicts).
"""

import os
import threading
import time
from collections import deque
from typing import Any

# Identifies which process recorded a given metrics snapshot. Set via the
# PROCESS_LABEL env var so a deployed public_api.py and flask_app.py report
# distinguishably even though both run identical cas_tracker.blueprint code.
PROCESS_LABEL = os.environ.get("PROCESS_LABEL", "main")

# Per-route latency samples kept for percentile calculation, bounded so a
# high-traffic route can't grow this without bound.
_MAX_SAMPLES_PER_ROUTE = 1000

_lock = threading.Lock()
_route_stats: dict[str, dict[str, Any]] = {}
_started_at = time.monotonic()


def record(route: str, status_code: int, latency_ms: float) -> None:
    """Record one completed request.

    Args:
        route: Flask's matched endpoint/rule (stable across query params),
            e.g. "cas_tracker.api_latest".
        status_code: HTTP response status code.
        latency_ms: Wall-clock time spent handling the request, in
            milliseconds.
    """
    with _lock:
        stats = _route_stats.setdefault(
            route,
            {
                "count": 0,
                "error_count": 0,
                "total_latency_ms": 0.0,
                "samples": deque(maxlen=_MAX_SAMPLES_PER_ROUTE),
            },
        )
        stats["count"] += 1
        if status_code >= 400:
            stats["error_count"] += 1
        stats["total_latency_ms"] += latency_ms
        stats["samples"].append(latency_ms)


def _percentile(sorted_samples: list[float], pct: float) -> float:
    """Nearest-rank percentile of an already-sorted sample list."""
    if not sorted_samples:
        return 0.0
    index = min(len(sorted_samples) - 1, int(round(pct / 100.0 * (len(sorted_samples) - 1))))
    return round(sorted_samples[index], 2)


def snapshot() -> dict[str, Any]:
    """Return the current metrics as a JSON-serializable snapshot.

    Returns:
        Dict with 'process', 'uptime_seconds', 'routes' (per-route count,
        error_count, avg/p50/p95 latency_ms), and 'total_requests'.
    """
    with _lock:
        routes = {}
        total_requests = 0
        for route, stats in _route_stats.items():
            count = stats["count"]
            total_requests += count
            sorted_samples = sorted(stats["samples"])
            routes[route] = {
                "count": count,
                "error_count": stats["error_count"],
                "avg_latency_ms": round(stats["total_latency_ms"] / count, 2) if count else 0.0,
                "p50_latency_ms": _percentile(sorted_samples, 50),
                "p95_latency_ms": _percentile(sorted_samples, 95),
            }

    return {
        "process": PROCESS_LABEL,
        "uptime_seconds": round(time.monotonic() - _started_at, 1),
        "total_requests": total_requests,
        "routes": routes,
    }
