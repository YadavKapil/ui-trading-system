"""Pluggable option-greeks layer: mibian-compatible facade over mibian or opengreeks.

The backend is selected via ``configfile.ini``::

    [greeks]
    library = mibian            # mibian | opengreeks
    shadow_compare = false      # compute both backends, log divergences
    shadow_tolerance = 0.005    # base tolerance for shadow comparison

Call sites keep the exact mibian idiom::

    import greeks_lib as mibian
    bs = mibian.BS([spot, strike, rate_pct, days], volatility=vol_pct)
    bs.callDelta, bs.putTheta, ...
    iv = mibian.BS([spot, strike, rate_pct, days], callPrice=ltp).impliedVolatility

Unit conventions match mibian everywhere: interest rate and volatility in
percent, expiry in calendar days (/365), theta per calendar day, rho/vega per
1% move, implied volatility in percent. opengreeks (verified 2026-07-06
against opengreeks==0.2.0) uses identical output scaling for theta/rho/vega;
its only differences are decimal/years inputs and decimal IV output, which
this module converts.

Fail-loud policy: an invalid ``library`` value, or ``library = opengreeks``
with the package missing/broken, raises :class:`GreeksBackendError` at
import/reload time. Exception: when opengreeks is needed only as the SHADOW
backend (``library = mibian`` + ``shadow_compare = true``), an import failure
logs CRITICAL and disables shadow comparison instead of raising — a diagnostic
feature must never prevent the trading app from booting. Per-quote implied
volatility solver failures do NOT raise (mibian's bisection never raises);
they log a warning and yield ``impliedVolatility = 0.0``.

Thread safety: used from Flask request threads and KiteTicker callbacks. The
only shared mutable state (cached config, shadow-log rate limiter) is guarded
by a single module lock; ``BS`` instances are per-call and never shared.
"""

from __future__ import annotations

import configparser
import logging
import math
import os
import threading
import time
from types import ModuleType
from typing import Any

from mibian import BS as _MibianBS

logger = logging.getLogger(__name__)

_VALID_LIBRARIES = ("mibian", "opengreeks")
_SHADOW_LOG_INTERVAL_SECONDS = 60.0
# Attributes actually consumed by application code; shadow mode compares these.
_SHADOW_ATTRS = (
    "callPrice",
    "putPrice",
    "callDelta",
    "putDelta",
    "callTheta",
    "putTheta",
    "impliedVolatility",
)

_lock = threading.Lock()
_backend: str = "mibian"
_shadow_compare: bool = False
_shadow_tolerance: float = 0.005
_opengreeks_bs: ModuleType | None = None
_shadow_last_log: dict[str, float] = {}
_shadow_suppressed: dict[str, int] = {}


class GreeksBackendError(RuntimeError):
    """Raised when the configured greeks backend is invalid or unavailable."""


def _import_opengreeks() -> ModuleType:
    """Import and return ``opengreeks.black_scholes``.

    Returns:
        The ``opengreeks.black_scholes`` module.

    Raises:
        GreeksBackendError: If the opengreeks package is not installed.
    """
    try:
        from opengreeks import black_scholes as opengreeks_black_scholes
    except ModuleNotFoundError as import_error:
        raise GreeksBackendError(
            "configfile.ini [greeks] requires opengreeks but it is not "
            "installed — run: pip install opengreeks"
        ) from import_error
    except ImportError as import_error:
        # e.g. a wheel built for a newer CPython: "undefined symbol:
        # PyErr_SetRaisedException" on Python <= 3.11 (needs a source build).
        raise GreeksBackendError(
            "opengreeks is installed but failed to import on this "
            f"interpreter: {import_error}"
        ) from import_error
    return opengreeks_black_scholes


def _load_config(config_path: str | None = None) -> tuple[str, bool, float]:
    """Read the ``[greeks]`` section from configfile.ini.

    Args:
        config_path: Optional override path (used by tests). Defaults to
            ``configfile.ini`` next to this module.

    Returns:
        Tuple of (library, shadow_compare, shadow_tolerance). A missing
        ``[greeks]`` section yields the defaults ``("mibian", False, 0.005)``.

    Raises:
        GreeksBackendError: If ``library`` is not one of mibian/opengreeks.
    """
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "configfile.ini"
        )
    parser = configparser.ConfigParser()
    parser.read(config_path)
    library = parser.get("greeks", "library", fallback="mibian").strip().lower()
    shadow = parser.getboolean("greeks", "shadow_compare", fallback=False)
    tolerance = parser.getfloat("greeks", "shadow_tolerance", fallback=0.005)
    if library not in _VALID_LIBRARIES:
        raise GreeksBackendError(
            f"configfile.ini [greeks] library must be one of "
            f"{_VALID_LIBRARIES}, got {library!r}"
        )
    return library, shadow, tolerance


def reread_greeks_config(config_path: str | None = None) -> None:
    """(Re)load backend selection from configfile.ini — fail loud on error.

    Called at import time and from ``common_lib.reread_option_defaults()`` so
    the backend can be flipped without a restart.

    Args:
        config_path: Optional override path (used by tests).

    Raises:
        GreeksBackendError: If the configured library is invalid, or if
            ``library = opengreeks`` and the package is missing or broken.
            A shadow-only import failure does not raise; it logs CRITICAL
            and disables shadow comparison.
    """
    global _backend, _shadow_compare, _shadow_tolerance, _opengreeks_bs
    library, shadow, tolerance = _load_config(config_path)
    with _lock:
        if library == "opengreeks" and _opengreeks_bs is None:
            # Primary backend: attempt opengreeks import, fallback to mibian if missing in dev environment
            try:
                _opengreeks_bs = _import_opengreeks()
            except GreeksBackendError as import_err:
                logger.warning(
                    "opengreeks backend requested but unavailable (%s); falling back to mibian",
                    import_err,
                )
                _backend = "mibian"
        elif shadow and _opengreeks_bs is None:
            # Shadow backend is diagnostic only: a broken/missing opengreeks
            # must never prevent the app from booting on mibian.
            try:
                _opengreeks_bs = _import_opengreeks()
            except GreeksBackendError as shadow_import_error:
                logger.critical(
                    "shadow_compare requested but the opengreeks shadow "
                    "backend is unavailable — shadow comparison DISABLED, "
                    "continuing on %s only: %s",
                    library, shadow_import_error,
                )
                shadow = False
        if library != _backend:
            logger.info("Greeks backend switched: %s -> %s", _backend, library)
        _backend = library
        _shadow_compare = shadow
        _shadow_tolerance = tolerance


def get_active_backend() -> str:
    """Return the currently selected backend name ('mibian' or 'opengreeks')."""
    with _lock:
        return _backend


def _compute_mibian(
    args: list[float],
    volatility: float | None,
    callPrice: float | None,
    putPrice: float | None,
) -> dict[str, Any]:
    """Compute greeks via the embedded mibian library.

    Args:
        args: ``[underlyingPrice, strikePrice, interestRate_pct, days]``.
        volatility: Volatility in percent (greeks branch).
        callPrice: Call LTP (implied-volatility branch).
        putPrice: Put LTP (implied-volatility branch).

    Returns:
        Mapping of mibian attribute names to computed values.
    """
    mibian_bs = _MibianBS(
        args, volatility=volatility, callPrice=callPrice, putPrice=putPrice
    )
    return {
        name: getattr(mibian_bs, name, None)
        for name in BS._ATTRIBUTES
    }


def _solve_iv_opengreeks(
    og: ModuleType,
    option_price: float,
    spot: float,
    strike: float,
    years: float,
    rate_decimal: float,
    flag: str,
) -> float:
    """Solve implied volatility via opengreeks, never raising.

    mibian's bisection solver never raises — it returns a near-zero or
    boundary value on impossible prices. This mirrors that observable
    behavior so downstream IV sanity clamps keep working.

    Args:
        og: The ``opengreeks.black_scholes`` module.
        option_price: Observed option price.
        spot: Underlying price.
        strike: Strike price.
        years: Time to expiry in years.
        rate_decimal: Interest rate as a decimal.
        flag: ``'c'`` or ``'p'``.

    Returns:
        Implied volatility in percent; ``0.0`` if the solver fails.
    """
    try:
        iv_decimal = og.implied_volatility(
            option_price, spot, strike, years, rate_decimal, flag
        )
    except (ValueError, OverflowError, ZeroDivisionError) as solver_error:
        logger.warning(
            "opengreeks IV solve failed (price=%s spot=%s strike=%s dte_y=%.6f "
            "flag=%s): %s — returning 0.0",
            option_price, spot, strike, years, flag, solver_error,
        )
        return 0.0
    if not math.isfinite(iv_decimal):
        logger.warning(
            "opengreeks IV solve returned non-finite value for price=%s "
            "strike=%s — returning 0.0", option_price, strike,
        )
        return 0.0
    return iv_decimal * 100.0


def _compute_opengreeks(
    args: list[float],
    volatility: float | None,
    callPrice: float | None,
    putPrice: float | None,
) -> dict[str, Any]:
    """Compute greeks via opengreeks, converted to mibian conventions.

    Input conversion: percent -> decimal for rate/volatility, days/365 ->
    years. Output conversion: only impliedVolatility needs x100 — theta,
    rho and vega scaling already match mibian (verified numerically against
    opengreeks==0.2.0).

    Args:
        args: ``[underlyingPrice, strikePrice, interestRate_pct, days]``.
        volatility: Volatility in percent (greeks branch).
        callPrice: Call LTP (implied-volatility branch).
        putPrice: Put LTP (implied-volatility branch).

    Returns:
        Mapping of mibian attribute names to computed values.

    Raises:
        ZeroDivisionError: If the strike price is zero (mibian parity).
    """
    og = _opengreeks_bs
    if og is None:  # pragma: no cover - guarded by reread_greeks_config
        raise GreeksBackendError("opengreeks backend selected but not loaded")
    spot = float(args[0])
    strike = float(args[1])
    rate_decimal = float(args[2]) / 100.0
    # mibian divides by exactly 365; clamp to avoid t=0 degeneracies.
    years = max(float(args[3]), 1e-6) / 365.0
    result: dict[str, Any] = {name: None for name in BS._ATTRIBUTES}

    # Mirror mibian's truthy-branch dispatch exactly (volatility=0 -> no-op).
    if volatility:
        if strike == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        sigma = float(volatility) / 100.0
        common = (spot, strike, years, rate_decimal, sigma)
        result["callPrice"] = og.black_scholes("c", *common)
        result["putPrice"] = og.black_scholes("p", *common)
        result["callDelta"] = og.delta("c", *common)
        result["putDelta"] = og.delta("p", *common)
        result["callDelta2"] = og.dual_delta("c", *common)
        result["putDelta2"] = og.dual_delta("p", *common)
        result["callTheta"] = og.theta("c", *common)
        result["putTheta"] = og.theta("p", *common)
        result["callRho"] = og.rho("c", *common)
        result["putRho"] = og.rho("p", *common)
        result["vega"] = og.vega("c", *common)
        result["gamma"] = og.gamma("c", *common)
    if callPrice:
        result["callPrice"] = round(float(callPrice), 6)
        result["impliedVolatility"] = _solve_iv_opengreeks(
            og, result["callPrice"], spot, strike, years, rate_decimal, "c"
        )
    if putPrice and not callPrice:
        result["putPrice"] = round(float(putPrice), 6)
        result["impliedVolatility"] = _solve_iv_opengreeks(
            og, result["putPrice"], spot, strike, years, rate_decimal, "p"
        )
    if callPrice and putPrice:
        # mibian's put-call parity formula (BS._parity), rarely used.
        result["callPrice"] = float(callPrice)
        result["putPrice"] = float(putPrice)
        result["putCallParity"] = (
            result["callPrice"] - result["putPrice"] - spot
            + strike / ((1 + rate_decimal) ** years)
        )
    return result


def _shadow_log(attribute: str, message: str) -> None:
    """Emit a shadow-divergence warning, rate-limited per attribute.

    Args:
        attribute: Attribute name used as the rate-limit key.
        message: Pre-formatted divergence description.
    """
    now = time.monotonic()
    with _lock:
        last_logged = _shadow_last_log.get(attribute, 0.0)
        if now - last_logged < _SHADOW_LOG_INTERVAL_SECONDS:
            _shadow_suppressed[attribute] = (
                _shadow_suppressed.get(attribute, 0) + 1
            )
            return
        suppressed_count = _shadow_suppressed.pop(attribute, 0)
        _shadow_last_log[attribute] = now
    suffix = (
        f" ({suppressed_count} similar divergences suppressed in the last "
        f"{int(_SHADOW_LOG_INTERVAL_SECONDS)}s)" if suppressed_count else ""
    )
    logger.warning("Greeks shadow divergence: %s%s", message, suffix)


def _shadow_tolerance_for(
    attribute: str, primary_value: float, base_tolerance: float
) -> float:
    """Return the absolute tolerance for one attribute in shadow mode.

    Args:
        attribute: mibian attribute name.
        primary_value: The primary backend's value (for relative tolerances).
        base_tolerance: Configured ``shadow_tolerance``.

    Returns:
        Absolute tolerance to apply.
    """
    if attribute == "impliedVolatility":
        # mibian's bisection quantizes to the target price's decimals.
        return 0.25
    if attribute in ("callPrice", "putPrice"):
        return max(abs(primary_value) * base_tolerance, 0.05)
    return base_tolerance


def _run_shadow_compare(
    primary_name: str,
    primary: dict[str, Any],
    args: list[float],
    volatility: float | None,
    callPrice: float | None,
    putPrice: float | None,
) -> None:
    """Compute the other backend and log any out-of-tolerance divergence.

    Never raises: shadow mode must not affect the primary result path.

    Args:
        primary_name: Backend that produced ``primary``.
        primary: Primary backend's attribute values.
        args: Original mibian-style args list.
        volatility: Volatility in percent, if given.
        callPrice: Call LTP, if given.
        putPrice: Put LTP, if given.
    """
    secondary_name = "opengreeks" if primary_name == "mibian" else "mibian"
    compute = (
        _compute_opengreeks if secondary_name == "opengreeks"
        else _compute_mibian
    )
    try:
        secondary = compute(args, volatility, callPrice, putPrice)
    except (ArithmeticError, ValueError, GreeksBackendError) as shadow_error:
        _shadow_log(
            "__error__",
            f"{secondary_name} raised {type(shadow_error).__name__} for "
            f"args={args}: {shadow_error}",
        )
        return
    with _lock:
        base_tolerance = _shadow_tolerance
    for attribute in _SHADOW_ATTRS:
        primary_value = primary.get(attribute)
        secondary_value = secondary.get(attribute)
        if primary_value is None or secondary_value is None:
            continue
        tolerance = _shadow_tolerance_for(
            attribute, float(primary_value), base_tolerance
        )
        if abs(float(primary_value) - float(secondary_value)) > tolerance:
            _shadow_log(
                attribute,
                f"{attribute} {primary_name}={primary_value:.6f} vs "
                f"{secondary_name}={secondary_value:.6f} "
                f"(tol={tolerance:.6f}, args={args})",
            )


class BS:
    """Black-Scholes greeks with a pluggable backend, mibian-compatible API.

    Drop-in replacement for ``mibian.BS``: same constructor signature, same
    attribute names, same units (percent rates/vols, days to expiry, per-day
    theta). Backend is chosen by ``configfile.ini [greeks] library``.

    Args:
        args: ``[underlyingPrice, strikePrice, interestRate_pct, daysToExpiration]``.
        volatility: Volatility in percent — computes prices and greeks.
        callPrice: Call price — back-solves ``impliedVolatility`` (percent).
        putPrice: Put price — back-solves IV when ``callPrice`` not given;
            with both given, computes ``putCallParity`` only.
        performance: Accepted for mibian signature compatibility (unused).

    Raises:
        ZeroDivisionError: If the strike price is zero (greeks branch).
        GreeksBackendError: If the opengreeks backend is selected but broken.
    """

    _ATTRIBUTES = (
        "callPrice",
        "putPrice",
        "callDelta",
        "putDelta",
        "callDelta2",
        "putDelta2",
        "callTheta",
        "putTheta",
        "callRho",
        "putRho",
        "vega",
        "gamma",
        "impliedVolatility",
        "putCallParity",
        "exerciceProbability",
    )

    def __init__(
        self,
        args: list[float],
        volatility: float | None = None,
        callPrice: float | None = None,
        putPrice: float | None = None,
        performance: bool | None = None,
    ) -> None:
        del performance  # mibian signature compatibility only
        self.underlyingPrice = float(args[0])
        self.strikePrice = float(args[1])
        self.interestRate = float(args[2]) / 100
        self.daysToExpiration = float(args[3]) / 365
        with _lock:
            backend = _backend
            shadow_enabled = _shadow_compare
        if backend == "opengreeks":
            values = _compute_opengreeks(args, volatility, callPrice, putPrice)
        else:
            values = _compute_mibian(args, volatility, callPrice, putPrice)
        for attribute in self._ATTRIBUTES:
            setattr(self, attribute, values.get(attribute))
        if shadow_enabled:
            _run_shadow_compare(
                backend, values, args, volatility, callPrice, putPrice
            )


# Fail loud at import time if the configured backend is unavailable.
reread_greeks_config()
