# Wave Extractor

Dashboard: **`/wave-extractor`**

A continuous options gap-trading strategy. One **scraper process**
(`ticker_single_scraper_new.py`) runs per trading symbol:

1. At startup it places a pair of limit orders — BUY at
   `price − buy_gap`, SELL at `price + sell_gap`.
2. When one leg fills, the sibling is cancelled and a fresh pair is placed.
3. The cycle repeats all session; each fill "extracts" one wave of the
   price oscillation.
4. Gaps are scaled by a **multiplier stack** (step → delta → velocity)
   that widens or narrows the bracket based on position risk and momentum.

## Lifecycle

```
POST /api/start  {symbol, buy_gap, sell_gap, buy_quantity, sell_quantity}
  → spawns ticker_single_scraper_new.py (one PID per symbol)
  → logs to ../logs/<SYMBOL>.log
  → status to status/status_<SYMBOL>.json (drives the dashboard)
```

- A **duplicate-instance guard** rejects a second start for the same
  symbol (409) unless `force` is passed — two scrapers on one symbol would
  double the legs.
- Every 180 s the scraper polls order state (`check_orders()`) — the
  WebSocket `on_order_update` callback is fast-path only; polling is the
  authoritative fallback (COMPLETE can arrive before OPEN, especially for
  GTT-fired orders).
- `/api/stop`, `/api/stop_group`, `/api/stop_all` stop instances by PID,
  optionally cancelling their open orders.

## GTT fallback

If a duo order fails (circuit limit / margin), a GTT is placed as
fallback. When it fires, the new order id is linked back to the strategy's
order state via tag echo.

## Historical replay

The `wave_replay` package provides a hermetic, deterministic consumer for
portable `wave-replay-dataset-v1` exports. Initial historical runs accept
synchronized one-minute NIFTY underlying and option data only and use the
approved modeled profile: one frozen ATM CE contract, one date-effective lot,
10% BUY/SELL gaps, flat INR 1,000,000 starting cash, regular orders,
placement cool-off disabled, same-side fill cooldown enabled, completed-close
triggers, subsequent-bar activation, limit fills, and an explicitly labeled
Upstox fee approximation.

OHLC-touch branching remains deliberately deferred; the current historical
simulator supports `close_only` only.

```powershell
python -m wave_replay create-synthetic --output data\wave-replay\synthetic-v1
python -m wave_replay validate-dataset --dataset data\wave-replay\synthetic-v1
python -m wave_replay select-corpus --dataset data\wave-replay\synthetic-v1 --output wave_replay_output\scenarios
python -m wave_replay replay --dataset data\wave-replay\synthetic-v1 --scenario wave_replay_output\scenarios\nifty-range-atm-ce.json --output wave_replay_output\range --repeat 2
```

The bootstrap stages Wave source and fake credentials in a temporary sandbox,
denies outbound network access, substitutes fake broker/ticker modules before
import, and runs callbacks/threads through the cooperative scheduler. Generated
datasets and traces are gitignored.
