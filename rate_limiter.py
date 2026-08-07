"""Shared flask-limiter instance for flask_app.py and public-facing blueprints.

Extracted so a blueprint module (e.g. cas_tracker/blueprint.py) can decorate
its own routes with ``@limiter.limit(...)`` without importing flask_app.py —
flask_app.py imports blueprint modules at startup, so the reverse import
would be circular, and blueprint modules must stay importable standalone for
tests (see tests/test_cas_public_cache.py, which registers cas_tracker_bp on
a bare Flask app with no flask_app import at all).

``limiter`` is created without an app bound, which is flask-limiter's
standard deferred-init extension pattern: ``@limiter.limit(...)`` decorators
can run at import time in any module regardless of import order, because
they only need the ``Limiter`` object to exist, not a specific Flask app.
flask_app.py calls ``limiter.init_app(app)`` once, after creating ``app``, to
bind this shared instance to the real application.
"""

import logging
import os
from typing import Any

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address

    _FLASK_LIMITER_AVAILABLE = True
except ImportError as _import_error:
    Limiter = None  # type: ignore[assignment,misc]
    get_remote_address = None  # type: ignore[assignment]
    _FLASK_LIMITER_AVAILABLE = False
    logging.warning(
        "flask-limiter not installed (%s) — rate limiting is DISABLED. "
        "Run: pip install flask-limiter",
        _import_error,
    )


class _NoopLimiter:
    """Stand-in when flask-limiter is unavailable: decorators become no-ops."""

    def limit(self, *args: Any, **kwargs: Any):  # noqa: ANN201 - decorator factory
        def decorator(func):  # noqa: ANN001, ANN202
            return func

        return decorator

    def exempt(self, obj: Any) -> Any:
        return obj

    def init_app(self, app: Any) -> None:
        return None


# No default limits — dashboards and public pollers alike hit these routes
# frequently by design. Rate limiting is applied only via explicit
# @limiter.limit(...) decorators on specific routes.
#
# Storage backend is configurable via RATE_LIMITER_STORAGE_URI so a Redis
# deployment is a one-line env var flip later, with zero code change, once
# the public tier (public_api.py) genuinely needs multiple worker processes
# sharing one limit — until then in-memory is correct and simpler to run.
# In-memory state is process-local: fine within one process, resets on
# restart, and NOT shared across multiple processes/workers (each would
# enforce its own independent limit) — that's exactly the gap a redis://
# URI closes.
_STORAGE_URI = os.environ.get("RATE_LIMITER_STORAGE_URI", "memory://")

if _STORAGE_URI.startswith("redis://") or _STORAGE_URI.startswith("rediss://"):
    try:
        import redis  # noqa: F401  (import-only availability probe)
    except ImportError as _redis_import_error:
        logging.warning(
            "RATE_LIMITER_STORAGE_URI=%s but the 'redis' package is not "
            "installed (%s) — falling back to memory:// (per-process only). "
            "Run: pip install redis",
            _STORAGE_URI,
            _redis_import_error,
        )
        _STORAGE_URI = "memory://"

limiter: Any = (
    Limiter(get_remote_address, default_limits=[], storage_uri=_STORAGE_URI)
    if _FLASK_LIMITER_AVAILABLE
    else _NoopLimiter()
)
