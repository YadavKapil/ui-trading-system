# Wave Trade Dashboard Instrumentation Plan

## Executive summary

The trade-episode dashboard MVP intentionally reconstructs episodes, order pairs, sibling cancellations, and risk state from existing replay reports and traces. It does not change production Wave code or replay behavior. This document defines the deferred trace-schema work needed to replace reconstruction with directly observed, versioned events.

The next fidelity phase should add stable episode/pair/sibling identifiers, explicit order-action reasons, complete per-decision Wave state, inventory before/after, OHLC bars, and a first-class virtual forced-close event. Rollout must preserve existing traces and goldens until versioned consumers and compatibility tests are ready.

## Current MVP boundary

Implemented by the MVP:

- Exact-contract, sequence-ordered fill normalization.
- Flat-to-flat inventory episodes with sign-cross splitting.
- Pro-rata fee allocation for split fills.
- Lifecycle fallback and a clearly virtual final forced-close endpoint.
- Derived pair linkage where a peer order eventually exposes `bracket_id`.
- Derived sibling-fill and DAY-expiry cancellation causes where evidence is sufficient.
- Completed-minute close-mark MFE/MAE.

Not implemented by the MVP:

- New production or replay trace events.
- Fabricated historical anchors, multipliers, cooldowns, restrictions, or placement reasons.
- Intrabar MFE/MAE.
- Exact broker margin.

## Proposed schema

Introduce schema version `wave-replay-event-v2` without changing v1 event meaning. Every new field is additive and nullable while dual-read compatibility is active.

### Stable identifiers

Every order-related event should include:

- `episode_id`: stable exact-contract inventory-cycle identifier assigned when inventory first leaves zero.
- `pair_id`: stable ID for the logical Wave BUY/SELL pair.
- `order_id`: broker/simulator order ID.
- `sibling_order_id`: reciprocal regular-order ID when it exists.
- `contract_id`: canonical exact expiry/right/strike instrument ID.
- `worker_id`: stable process/worker instance ID.
- `decision_id`: ID shared by calculation, placement, reprice, and resulting order events.

IDs must be deterministic in replay and opaque in production. A replay ID may be derived from session, worker, monotonic decision sequence, and exact contract; it must not depend on wall-clock scheduling or process IDs.

### Order-action reasons

Add required enums to order transition and broker-call events:

- `placement_reason`: `initial_pair`, `fill_replacement`, `restart_recovery`, `gtt_fallback`, `switch_liquidation`, `roll_liquidation`, `forced_close_sensitivity`, or `other`.
- `cancel_reason`: `sibling_filled`, `reprice_replace`, `day_expiry`, `contract_switch`, `pre_expiry_roll`, `shutdown`, `manual`, `broker_reject_cleanup`, `recovery_reconciliation`, or `unknown`.
- `reprice_reason`: `anchor_changed`, `inventory_step_changed`, `delta_multiplier_changed`, `velocity_guard_changed`, `cooldown_changed`, `restriction_changed`, `quote_changed`, or `recovery_reconciliation`.
- `reason_source`: `observed`, `derived`, or `unavailable`.

Do not infer an observed reason from timing alone. Unknown remains a valid explicit value.

### Per-decision Wave state

Emit one `WAVE_ORDER_DECISION_V2` event before each place, modify, cancel, or skip result:

```json
{
  "decision_id": "decision-...",
  "episode_id": "episode-...",
  "pair_id": "pair-...",
  "contract_id": "NIFTY|2026-01-06|C|26300",
  "observed_at": "2026-01-01T09:19:03+05:30",
  "quote": {
    "option_close": "49.65",
    "underlying_close": "26183.50",
    "quote_at": "2026-01-01T09:19:00+05:30",
    "fresh": true
  },
  "anchors": {
    "buy": "40.65",
    "sell": "61.00"
  },
  "limits": {
    "buy": "40.65",
    "sell": "61.00",
    "tick_size": "0.05",
    "rounding": "legacy"
  },
  "gaps": {
    "base_buy": "10.20",
    "base_sell": "10.20",
    "final_buy": "10.70",
    "final_sell": "10.20"
  },
  "inventory": {
    "before": 65,
    "after_if_buy_fills": 130,
    "after_if_sell_fills": 0,
    "step": 1
  },
  "multipliers": {
    "scale_buy": "1.30",
    "scale_sell": "1.00",
    "delta_buy": "1.00",
    "delta_sell": "1.00",
    "velocity_buy": "1.00",
    "velocity_sell": "1.00",
    "effective_buy": "1.30",
    "effective_sell": "1.00"
  },
  "cooldown": {
    "buy_active": false,
    "sell_active": false,
    "buy_until": null,
    "sell_until": null
  },
  "restrictions": {
    "buy_allowed": true,
    "sell_allowed": false,
    "reasons": ["hard_delta_limit"]
  },
  "result": {
    "action": "place_pair",
    "skip_reason": null
  }
}
```

The payload must record values actually used by legacy code, after legacy rounding and branch decisions. It must not recompute them in the trace layer.

### Inventory state

Every fill and lifecycle liquidation event should include:

- `inventory_before`.
- `inventory_after`.
- `gross_absolute_inventory_after`.
- `risk_effect`: `adding`, `reducing`, `flat`, or `sign_flip`.
- `episode_id_before`.
- `episode_id_after`.
- `fill_piece_index` and `fill_piece_count` when a fill crosses zero.

The broker engine should remain the authority for inventory changes. Dashboard code should validate, not redefine, these fields.

### OHLC for true excursion metrics

Current `BAR_AVAILABLE` events contain close only. True intrabar MFE/MAE requires:

- `open`, `high`, `low`, and `close`.
- Bar interval and timezone.
- Missing/stale flags.
- Source archive identity.
- The exact availability timestamp used by the replay scheduler.

Until OHLC is emitted and cross-bound to source hashes, metrics must remain labeled `completed-minute close-mark MFE/MAE`.

### Virtual forced-close sensitivity

Emit `VIRTUAL_FORCED_CLOSE_SENSITIVITY` after the historical boundary:

- `virtual: true`.
- Exact contract, side, quantity, price, and mark timestamp.
- Fee components and fee model ID.
- Inventory before and hypothetical inventory after.
- Gross and net P&L contribution.
- Explicit statement that broker state and historical trace state were not mutated.

This event must never be consumed as a real fill or order.

## Versioning and compatibility

1. Add event schema version 2 while continuing to read version 1.
2. Keep v1 canonical serialization and golden digests unchanged by default.
3. Gate v2 emission with an explicit replay configuration flag during migration.
4. Add a normalizer adapter that maps v2 observed fields to the existing dashboard model and falls back to current deterministic reconstruction for v1.
5. Record `trace_schema_version` and enabled instrumentation features in `TRACE_HEADER`.
6. Reject unknown major versions. Ignore unknown additive fields within a supported major version.
7. Keep production logging opt-in until payload sensitivity and volume are reviewed.

## Golden and replay migration

1. Freeze current v1 goldens and January v3 audit artifacts.
2. Add focused v2 fixture traces for pair placement, one-side fill, sibling cancel, reprice, partial fill, sign flip, DAY expiry, reject, restart recovery, switch, roll, and virtual close.
3. Run v1 and v2 emitters against the same broker scripts.
4. Prove economic outputs, order states, fills, positions, and capital metrics are identical.
5. Approve separate v2 goldens rather than replacing v1 files in place.
6. Regenerate the January audited trace only after parity gates pass.

## Acceptance tests

- Every regular paired order has one stable `pair_id`; all related events carry it.
- Every known sibling event carries reciprocal `sibling_order_id`.
- Every cancellation and modification carries an allowed reason enum.
- Every fill inventory transition reconciles exactly with broker state.
- Episode IDs remain stable across repeat-2 deterministic replay.
- Decision events reproduce the exact placed/repriced limits without dashboard recomputation.
- Cooldown, velocity, delta, scale, and restriction values match legacy branch outcomes.
- Missing quote/spot data produces explicit unavailable fields, not stale fallback.
- OHLC MFE/MAE fixtures distinguish high/low excursion from close-only excursion.
- Virtual forced close reconciles to sensitivity P&L and does not mutate historical state.
- v1 input remains renderable with derived/unavailable labels.
- v2 input renders observed labels and produces no reconstruction disagreement.

## Phased rollout

1. **Schema and fixtures:** Define enums, IDs, examples, and compatibility reader.
2. **Replay-only emission:** Add v2 events behind an explicit flag; retain v1 defaults.
3. **Parity:** Run focused and continuous repeat-2 comparisons; approve separate v2 goldens.
4. **Dashboard adoption:** Prefer observed v2 fields and display reconstruction mismatches as errors.
5. **Production evaluation:** Review volume, privacy, and operational cost before any live enablement.
6. **Audit regeneration:** Produce a new versioned January artifact while retaining the current MVP artifact.

The MVP remains valid and intentionally conservative while this plan is deferred.
