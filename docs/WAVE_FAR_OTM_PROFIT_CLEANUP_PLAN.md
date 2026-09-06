# Optional Wave Far-OTM Profit Cleanup

## Summary

Add an optional, Wave-only cleanup that buys back deeply profitable short options after they become far out of the money. This behavior is **not present in original Wave**, defaults to disabled, and must have identical live and replay semantics before it can be enabled.

## Safety boundary

- Act only on positions demonstrably opened and owned by Wave.
- Never infer ownership from symbol, instrument type, account position, or proximity to a Wave order alone.
- Never modify manual positions or positions owned by another strategy.
- Persist ownership through durable Wave order/position records and/or immutable strategy tags so restart reconciliation can prove provenance.
- Fail closed when ownership, entry premium, current premium, delta, or complete short quantity cannot be established reliably.

## Configuration

Use a Wave-specific disabled-by-default block:

```json
{
  "far_otm_profit_cleanup": {
    "enabled": false,
    "max_abs_delta": 0.10,
    "minimum_profit_pct": 0.50,
    "maximum_current_premium": 20.0
  }
}
```

Thresholds are inclusive. Configuration changes must be validated, persisted with other Wave settings, and included in live/replay audit metadata.

## Evaluation and action

Evaluate once within Wave's existing 180-second reconciliation scan, before restoring, placing, or repricing Wave orders.

A position is eligible only when all conditions hold:

1. Ownership is durably proven as Wave.
2. The net position is short.
3. `abs(option_delta) <= 0.10`.
4. Premium profit is at least 50% relative to the Wave-recorded entry premium.
5. Current option premium is at most INR 20.
6. The complete short quantity and all related pending Wave orders are known.

For an eligible position:

1. Mark the position as cleanup-in-progress for the current scan.
2. Cancel all related pending Wave regular orders and GTTs.
3. Reconcile cancellation results; abort closed if exposure is ambiguous.
4. Buy back the complete short quantity.
5. Persist the closure and ownership transition.
6. Suppress restore, replacement, and repricing for that position for the remainder of the scan.

## Ownership and restart design

Persist a Wave position record keyed by stable instrument identity, not symbol alone. It should contain the originating Wave instance/strategy tag, opening order and fill IDs, signed owned quantity, weighted entry premium, timestamps, and closure state. Reconciliation must rebuild the owned quantity only from attributable fills and detect conflicts with account-level positions.

On restart, Wave may evaluate cleanup only after durable records, broker orders/fills, and the current account position reconcile exactly. Missing records, tag collisions, mixed ownership, quantity disagreement, or incomplete history must disable cleanup for that position and emit an audit event.

## Live/replay parity and reporting

Implement one shared decision contract for live Wave and the replay simulator. Replay must preserve the same scan ordering, ownership checks, thresholds, cancellation outcomes, broker callbacks, fees, and no-recreation guard. Corrected behavior remains behind the explicit feature flag and must not alter original-Wave goldens when disabled.

Every evaluation should record:

- ownership evidence and strategy tags;
- entry premium, current premium, profit percentage, delta, and quantity;
- threshold results and eligibility decision;
- skipped/fail-closed reason;
- cancelled regular orders and GTTs;
- closure order, fill price, fees, and realized P&L;
- scan identifier and no-recreation state.

## Implementation considerations

- Use executed-fill economics for entry premium; do not use submitted limit prices.
- Define mixed Wave/manual quantities as ineligible unless ownership can be separated exactly.
- Use synchronized, non-stale option price and delta inputs.
- Make cancellation and buyback idempotent across callback, polling, timeout, and restart races.
- Prevent partial cancellation or buyback from appearing successful.
- Preserve existing market-hours, expiry, delta-restriction, cooldown, and shutdown safeguards.
- Add trace schema fields additively and version any durable ownership record.

## Acceptance criteria

- Disabled configuration produces byte-identical original Wave and replay behavior.
- Manual and foreign-strategy positions are never selected, including same-symbol positions.
- Exact boundaries pass: `abs(delta) == 0.10`, profit `== 50%`, and premium `== INR 20`.
- Values outside any threshold do not close.
- Missing/stale price, delta, entry premium, ownership, or quantity fails closed.
- Restart reconstructs proven Wave ownership and rejects incomplete or conflicting records.
- All related Wave regular orders and GTTs are cancelled before buyback.
- The full attributable short quantity is bought back with fees and realized P&L reconciled.
- Callback/poll/cancellation races are deterministic and idempotent.
- The same scan cannot restore, reprice, or recreate the cleaned position.
- Live and replay fixtures produce equivalent decisions and canonical traces.

No implementation is included in this task.
