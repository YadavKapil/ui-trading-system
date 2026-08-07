"""Static snapshot of NIFTY 50 free-float market-cap weights.

Source: smart-investing.in's NIFTY weightage table, snapshot taken 2026-08-04
(a third-party derived page, not NSE/NSE-Indices themselves). NSE-Indices'
own live weight feed (blob.niftyindices.com) is decommissioned as of
2026-08-05 (DNS resolves to a dead Azure CDN endpoint) and the community
scrapers that depend on it (nifiio, NiftyIndicesScrapper) are stale.

Known gap: NESTLEIND is a current NIFTY 50 constituent per NSE-Indices'
official ind_nifty50list.csv, but did not appear in the smart-investing.in
snapshot used here, and its true current weight is unknown. It is omitted
below rather than guessed. The 49 remaining weights already summed to
~100.01% in the source snapshot, so the reconstruction is a close
approximation, not an exact replica of the official index formula.

Since NIFTY 50 is free-float-cap-weighted, these weights drift daily (not
just at the semi-annual rebalance), so treat this file as a snapshot to be
refreshed periodically, not a permanent constant.
"""

WEIGHTS_SNAPSHOT_DATE = "2026-08-04"

NIFTY50_WEIGHTS_PCT: dict[str, float] = {
    "RELIANCE": 8.91,
    "BHARTIARTL": 6.22,
    "HDFCBANK": 5.80,
    "ICICIBANK": 5.28,
    "SBIN": 4.86,
    "TCS": 4.51,
    "BAJFINANCE": 3.65,
    "LT": 2.81,
    "HINDUNILVR": 2.49,
    "INFY": 2.41,
    "SUNPHARMA": 2.39,
    "MARUTI": 2.26,
    "TITAN": 2.23,
    "M&M": 2.16,
    "ADANIENT": 2.08,
    "KOTAKBANK": 1.99,
    "AXISBANK": 1.99,
    "ADANIPORTS": 1.98,
    "HCLTECH": 1.87,
    "ITC": 1.82,
    "ULTRACEMCO": 1.79,
    "BAJAJFINSV": 1.69,
    "NTPC": 1.69,
    "BAJAJ-AUTO": 1.62,
    "JSWSTEEL": 1.60,
    "ONGC": 1.55,
    "ETERNAL": 1.53,
    "BEL": 1.46,
    "ASIANPAINT": 1.34,
    "POWERGRID": 1.34,
    "COALINDIA": 1.30,
    "SHRIRAMFIN": 1.29,
    "TATASTEEL": 1.21,
    "HINDALCO": 1.16,
    "EICHERMOT": 1.11,
    "GRASIM": 1.08,
    "INDIGO": 1.05,
    "SBILIFE": 0.95,
    "WIPRO": 0.94,
    "JIOFIN": 0.89,
    "TRENT": 0.83,
    "TECHM": 0.82,
    "APOLLOHOSP": 0.65,
    "TMPV": 0.65,
    "CIPLA": 0.60,
    "HDFCLIFE": 0.59,
    "TATACONSUM": 0.55,
    "MAXHEALTH": 0.53,
    "DRREDDY": 0.49,
}
