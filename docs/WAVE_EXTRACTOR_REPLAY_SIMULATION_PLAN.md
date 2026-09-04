# Wave Extractor Historical Replay and Simulation Plan

> **Executive summary:** Deliver this as two coordinated repository changes: the `indian-wheel` exporter branch/PR first, then the `ui-trading-system` simulator/consumer branch/PR against its immutable Parquet/JSON artifact. The approved initial historical corpus is NIFTY only: automatically identify verified one-minute range, trend, gap-open, and expiry/high-gamma days, then freeze one exact ATM CE contract per day. Choose the nearest expiry with 5-20 calendar DTE and nearest strike from the first completed underlying close after open; fail rather than relax those constraints. Each run starts flat with INR 1,000,000, no orders/GTTs, one date-effective lot, symmetric 10% premium gaps, regular orders, current saved step/delta/velocity settings, and same-side fill cooldown enabled. The initial market model remains explicit: `cool_off_mode: disabled`, `trigger_mode: close_only`, subsequent-bar activation, limit-price fills, and `broker_fee_model: upstox_approximation`. These are implementation defaults for the first characterization corpus, not universal strategy recommendations or claims of exact Zerodha/cool-off fidelity. Preserve the hermetic pre-import bootstrap and cooperative deterministic oracle; keep GTT and failure behavior in scripted characterization scenarios and defer clean-core/LEAN comparison until legacy traces are established.

**Document status:** Implementation plan only. No production behavior is changed by this document.

## 1. Objective and fidelity contract

The immediate objective is not to create an idealized trading strategy or a generic backtester. It is to make the original Wave implementation observable and repeatable enough that its behavior can be characterized before any refactor.

The harness must answer, for a fixed initial state and event stream:

1. Which broker calls did the legacy implementation make, in what order, and with which values?
2. Which callbacks and polling results did it consume?
3. How did its process globals, tracked orders, positions, restrictions, multipliers, GTTs, status files, and persisted order history change?
4. Which outcomes depend on callback order, REST visibility lag, retries, wall-clock boundaries, or process isolation?
5. Does a later implementation make the same decisions for the same causes?

The fidelity contract has three layers:

| Layer | Required comparison |
|---|---|
| Decision fidelity | Exact order side, quantity, product, price after tick-size conversion, GTT/regular choice, cancel/modify action, restriction, and timing decision |
| State fidelity | Exact normalized tracked-order graph, broker ledger, position ledger, cooldown/velocity history, multiplier snapshot, status snapshot, and recovery state |
| Economic fidelity | Fill price, filled quantity, fees, slippage, realized/unrealized P&L, exposure, and delta; exact when source order events exist, bounded when inferred from market data |

Decision and state fidelity are gating requirements for legacy equivalence. A plausible P&L match is not a substitute for matching actions and state transitions.

## 2. Three distinct modes and result sets

The harness must never blend legacy quirks, proposed fixes, and LEAN-specific behavior into one result.

| Mode | Purpose | Default behavior | Acceptance artifact |
|---|---|---|---|
| `legacy_equivalent` | Reproduce the current repository implementation | All compatibility flags off; current quirks retained | Approved golden trace and normalized final state |
| `corrected` | Evaluate specific fixes without changing the baseline | One or more named corrections explicitly enabled | Golden delta explaining each intentional difference |
| `lean_comparison` | Evaluate a later LEAN brokerage/algorithm adapter | Uses the same input events and comparison schema, with documented engine translations | Differential report against both legacy and corrected references |

Rules:

- `legacy_equivalent` is the first deliverable and remains immutable after approval. If the production baseline changes, create a new baseline version rather than overwriting the old one.
- The first actual one-minute market-day runs use the legacy decision path with required replay metadata `cool_off_mode=disabled`, `trigger_mode=close_only`, `fill_price_mode=limit`, and `equivalence_status=modeled_not_exact`. They are not labeled fully legacy-equivalent for timing or intraminute execution. Exact production cool-off fidelity requires tick/second captures and remains a later characterization mode.
- The NIFTY/ATM-CE/account/gap/order/fee values in section 10.3 are approved **initial implementation defaults**, not optimized parameters, profitability claims, or recommendations for other underlyings or production trading.
- Every correction is a separately named flag. Do not create a broad `fixed=true` switch.
- A correction must identify the scenario, legacy trace, corrected trace, risk rationale, and expected economic effect.
- LEAN integration cannot redefine the legacy oracle. LEAN-only concepts must be mapped or reported as non-equivalent.
- Reports must always show mode, baseline commit SHA, fixture version, fill model, ambiguity policy, config hashes, and random seed.

Candidate correction flags, to be confirmed only after characterization, include:

- `queue_concurrent_order_callbacks`
- `retain_tracking_until_cancel_confirmed`
- `handle_rejected_terminal_state`
- `track_partial_fills`
- `normalize_order_id_and_sibling_types`
- `wire_start_gtt_flag`
- `enforce_cross_instance_limits`
- `reload_runtime_wave_config`
- `guard_initial_placement_by_market_hours`
- `make_status_writes_atomic`
- `use_one_delta_engine_for_trade_and_status`

## 3. Current Wave boundary to preserve

### 3.1 Entry and process lifecycle

The production path is:

```text
templates/wave_extractor.html
  -> POST /api/start in flask_app.py
  -> input validation and duplicate-process guard
  -> subprocess: ticker_single_scraper_new.py
  -> initilise(...) in common_lib.py
  -> initial quote, percentage-gap setup, and place_duo_order(...)
  -> WebSocket callbacks plus the 180-second polling loop
  -> status_<SYMBOL>_<PID>.json and executed-order persistence
```

The harness must characterize both the strategy worker and the small control-plane surface that changes strategy behavior:

- start payload validation and `buy_quantity:sell_quantity` argument encoding;
- duplicate start, forced duplicate start, and restart timing;
- per-symbol/per-PID status ownership;
- stop-one, stop-group, and stop-all sequencing when order cancellation is requested;
- dashboard configuration snapshots used by a worker;
- process exit at market close and status cleanup.

The browser presentation itself is not part of the matching engine. Its payload construction and runtime configuration fields are in scope because they affect worker inputs.

### 3.2 Legacy startup sequence

The legacy runner must preserve this order:

1. Parse gaps, symbol, quantity string, token, and optional product type from `sys.argv`.
2. Call `set_execution_variables(...)`.
3. Select `NFO` or `BFO` from the symbol prefix.
4. Call `initilise(...)`, authenticate, load lot sizes, start the ticker, parse asymmetric quantities, obtain initial positions, and derive symbol type.
5. Resolve instrument details and set delta-calculation days from that instrument's DTE.
6. Fetch the initial option quote.
7. Convert absolute gaps to percentages through `set_scraper_last_price(...)`.
8. Set product type and call `place_duo_order(...)`.
9. Enter the 180-second loop: reload option defaults, test market hours, refresh position/restrictions/status, poll tracked orders, reconcile GTT-triggered orders, and place a new duo when no regular or pending GTT order remains.

Callbacks can run while a cool-off sleep, broker call, poll, status write, or another callback is in progress. The replay scheduler must model those interleavings rather than reducing the worker to a synchronous bar-by-bar strategy.

### 3.3 Duo placement decision pipeline

For each placement attempt, preserve and trace the current decision order:

1. Acquire the `already_executing_order` global guard.
2. Compute hard restrictions from the current symbol's expiry delta.
3. Restrict additional option BUY orders when the symbol position is already positive.
4. Compute the position step as `(current_position - initial_position) / int(quantity)`.
5. Select the configured step multiplier or the out-of-range fallback (`[1, 100]` for negative steps, `[100, 1]` for non-negative steps).
6. Apply the continuous portfolio-delta multiplier.
7. Apply same-side fill cooldown restrictions.
8. Fetch uncached underlying spot and update the process-local velocity history.
9. Apply the velocity multiplier.
10. Fetch the option quote, combine it with `scraper_last_price`, sleep for the configured cool-off interval, fetch again, and select the conservative buy/sell anchors.
11. Apply the NIFTY low-premium BUY exception at a final buy price of 25 or below.
12. Attempt SELL first, then BUY.
13. For each failed regular placement, attempt a same-side GTT fallback only while the market is open.
14. Link regular siblings, regular-to-GTT siblings, or paired GTT metadata.
15. Save price anchors and multiplier metadata for later repricing.

Every intermediate value must appear in the trace. A final price alone is insufficient to diagnose a mismatch.

### 3.4 Fill, callback, polling, and replacement flow

The harness must preserve both fast and authoritative paths:

- WebSocket `on_order_update` for tracked regular orders;
- WebSocket discovery of GTT-spawned regular orders by echoed tag;
- `check_orders()` polling of tracked order history;
- `check_gtt_orders()` polling of all orders when a GTT callback was missed;
- reconnect-triggered cache invalidation and polling;
- delayed post-fill status refresh;
- sibling cancellation, stale GTT cleanup, and immediate duo replacement;
- system-cancelled sibling suppression;
- restriction-driven cancellation, missing-leg recreation, and multiplier repricing;
- manual price override detection versus auto-reprice callback echoes.

Polling and callbacks must be allowed to report the same terminal transition. Idempotency behavior, repeated counters, and stale-cache behavior are part of the baseline.

## 4. Scope boundaries

### 4.1 In scope

Include every function and branch transitively reachable from the Wave lifecycle, even if the function lives in a shared module:

- `ticker_single_scraper_new.py` module startup, signal cleanup, initial placement, and main loop;
- Wave start/stop/status/config control-plane behavior in `flask_app.py`;
- authentication setup only to the point needed to construct the worker's broker client;
- market-hours and holiday checks;
- instrument lookup, symbol type, DTE, lot-size, and exchange selection;
- quote, position, order-history, order-list, and GTT access used by Wave;
- gap percentage updates and price rounding/flooring;
- hard delta restrictions, soft delta multiplier, step multiplier, velocity guard, fill cooldown, positive-position restriction, and low-premium exception;
- duo placement, GTT fallback, callbacks, polling, cancellation, modification, retry, reconnect, and recovery;
- executed-order and status persistence as observable side effects;
- the latest-spot bridge file consumed by `record_order_complete()` to persist `underlying_at_execution`, while treating its external producer as an input source;
- one worker, duplicate workers, multiple symbols, and portfolio-wide broker state;
- notification calls only as a recorded side-effect sink, not as a simulation of delivery providers.

Create a machine-readable reachability manifest during implementation. Seed it from a static call graph rooted at the ticker module body, callback assignments, loop functions, and Flask lifecycle endpoints. Add dynamic edges for imports, callbacks, decorators, thread targets, and file-backed helpers. Store each reachable function with source file, line range, branch IDs, scenarios, and coverage status.

### 4.2 Out of scope

Do not simulate unrelated `common_lib.py` capabilities merely because they share the file:

- Survivor, expiry-trade, early-exit, covered-call, stock-selection, trade-journal, duplicate-order-monitor, or unrelated dashboard workflows. The one exception is replaying writes to Survivor's latest-spot bridge file because Wave reads that file when recording execution metadata; the Survivor strategy itself remains out of scope;
- generic historical-data downloading;
- unrelated notification transports;
- full Zerodha API behavior beyond methods and fields exercised by Wave;
- exchange matching-engine microstructure beyond the selected fill model;
- strategy optimization, parameter search, or claims of profitability;
- clean-core design changes before the legacy oracle is approved.

If a shared helper is reached only through a Wave call, include the helper. If it is not reachable, leave it out and mark it excluded in the manifest.

## 5. Characterize known implementation issues; do not silently fix them

The first test suite must lock down observed behavior for the following items. Some may later be classified as intended compatibility behavior, but none should be "cleaned up" while building the oracle.

| Current behavior or concern | Required characterization |
|---|---|
| The dashboard's `is_gtt` start option appends `NRML GTT`, while the ticker reads product type but does not consume the extra GTT argument | Compare the UI flag with the process-wide `configfile.ini` `order_gtt_regular` value and record which actually controls placement |
| `cross_instance` limits are editable and persisted by the dashboard but are not used by Wave order logic | Prove current non-enforcement with two workers before adding an opt-in correction |
| The initial `place_duo_order()` occurs before the ticker loop's market-hours guard | Replay pre-open, close-boundary, and after-close starts, including regular rejection and GTT suppression |
| Callback serialization uses an integer global and returns immediately when already updating | Deliver simultaneous and nested callbacks; record which updates are dropped and later recovered by polling |
| `cancel_order()` deletes local tracking before broker cancellation is confirmed, retries broad exceptions five times without delay, and then returns success-shaped `None` | Inject timeout, reject, already-filled, and all-retries-failed outcomes; observe ghost/live broker orders and local state |
| Sibling sentinels use both integer `-1` and string `"-1"` | Exercise every regular/GTT pairing and order-ID type to capture cancellation and active-order consequences |
| `check_orders()` handles `COMPLETE` and `CANCELLED` but not `REJECTED`; terminal histories are cached indefinitely | Replay rejection through callback, polling-only, and reconnect paths |
| Partial-fill fields are not explicitly represented in the tracked order model | Deliver repeated `OPEN`/partial updates, partial-then-cancel, partial-then-complete, and position changes visible before terminal handling |
| `record_order_complete()` increments side counters before executed-order persistence succeeds | Fail persistence, retry through polling, and determine whether counts can be incremented repeatedly |
| Completion removes the order before notification metadata is read | Capture the notification side effect and its current empty/default metadata |
| Config is cached per process; saving dashboard config updates the Flask process, not necessarily existing worker caches | Change config during a replay and record which settings change immediately, at 180-second reload, or only after restart |
| Step calculation uses `quantity`, which becomes the SELL quantity when a `buy:sell` value is supplied | Test asymmetric quantities and fractional/negative position differences |
| Positive order prices are converted with `int(price * 20) / 20`, which floors rather than rounds to the nearest tick | Golden-test exact boundary values, floating-point edges, and the 0.05 non-positive clamp |
| Market close is inclusive at 15:30 and the loop checks only after a 180-second sleep | Characterize delayed shutdown, fills/callbacks at 15:30, and actions before the next poll |
| Wave tick callbacks log/fan out ticks, while velocity uses uncached REST spot quotes during placement/repricing | Keep tick and quote streams distinct; test divergent values and missing spot quotes |
| Initial instrument-token lookup uses an unqualified symbol quote and logs failures without aborting | Replay missing token, cache fallback, and status-file token recovery |
| Position reads use TTL caches and may return stale data after failures | Script REST visibility lag, stale fallback, invalidation, and broker errors |
| DTE calculation uses host date/business-day counting in some paths and does not consistently share the IST clock/holiday calendar | Replay midnight/time-zone, holiday, expiry-day, and stale instrument-cache cases |
| SENSEX trade restrictions/multipliers and status display can use different Greeks paths | Capture both values in the same trace and do not normalize the difference away |
| Status JSON is written directly rather than atomically; readers skip malformed files | Interrupt writes and read concurrently; record dashboard omission and recovery |
| `/api/status` may overlay or inject live broker orders not yet present in status, except for duplicate-symbol instances | Test attribution, duplicate suppression, stale broker results, and mismatched key types |
| Dry-run placement returns `-1`, which drives failure/GTT fallback logic rather than a complete simulated lifecycle | Treat current dry-run as its own characterized input, not as the new replay engine |
| The duo retry path recursively retries selected Kite exceptions once after sleeping | Inject first-failure/second-success and repeated failure; capture semaphore and duplicate-action behavior |
| Existing tests include weak or placeholder assertions around selected retry/delta paths | Retain them, but make the new scenario/trace assertions authoritative |

The resulting issue catalog should label each item `legacy_quirk`, `confirmed_defect`, `data_limitation`, or `unknown_pending_capture`. Only `corrected` mode may alter a confirmed defect.

## 6. Proposed architecture

### 6.1 Component view

```text
indian-wheel Upstox raw cache + LEAN archives
  -> export-wave-replay (indian-wheel-owned)
  -> immutable Parquet dataset + JSON manifest
                              |
                  +-----------+-----------+
                  |                       |
                  v                       v
        Historical market bars    Scripted broker/fault events
                  \                       /
                   \                     /
                    v                   v
                 Hermetic pre-import bootstrap
                              |
                              v
                    Deterministic scheduler
                    /         |          \
                   v          v           v
             Fake clock  SimTaskRuntime  Fault script
                   \          |           /
                    \     Fake ticker     /
                     \        |          /
                      Legacy Wave runner
                (fresh worker module namespace)
                              |
                              v
                   Narrow Fake Kite adapter
                    /         |          \
              order book  position ledger  GTT registry
                    \         |          /
                              v
                    Canonical trace recorder
                              |
              +---------------+----------------+
              v                                v
        Golden comparator              Differential runner
                                          /          \
                                  clean core        LEAN adapter
```

The `indian-wheel` exporter owns source-format interpretation and provenance. The Wave repository owns validation, bar-to-event conversion, hermetic bootstrap, cooperative task execution, scheduling, broker simulation, and strategy execution. The bootstrap establishes filesystem, import, network, database, time, and concurrency isolation **before** Wave code is imported. The scheduler owns simulated time and causality. `SimTaskRuntime` ensures no deterministic callback, timer, delayed refresh, or ticker handler runs on an OS thread. The fake broker owns broker truth. Each legacy worker owns a distinct module graph and process-local state container. The trace recorder observes all of these without deciding strategy behavior.

### 6.2 Proposed file layout

Use a dedicated package rather than adding simulation conditionals throughout `common_lib.py`. The implementation spans two repositories with a stable schema boundary:

```text
# sibling indian-wheel repository (separate PR)
src/backtesting_lean/wave_export/
  __init__.py
  cli.py                    # export-wave-replay subcommand wiring for existing backtesting_lean.cli
  reader.py                 # LEAN zip/universe/config readers
  timestamps.py             # start/end/availability conversion
  schema.py                 # wave-replay-dataset-v1 writer schema
  validate.py               # source/archive/partition validation
  manifest.py               # immutable provenance and quality manifest

data/exports/wave-replay/   # generated and gitignored
  <dataset_id>/
    manifest.json
    underlying_bars.parquet
    option_bars.parquet
    contracts.parquet
    sessions.parquet
    fee_schedule.json

# ui-trading-system repository (separate, dependent PR)
wave_replay/
  __init__.py
  cli.py                    # replay, record, compare, approve-golden commands
  datasets.py               # portable export loader and schema-version gate
  dataset_validate.py       # hash, partition, timestamp, and coverage checks
  market_events.py          # bars -> scheduler availability events
  bootstrap.py              # pre-import sandbox, import fakes, network denial, reset
  sandbox.py                # isolated source/config/runtime/SQLite filesystem layout
  modes.py                  # legacy/corrected/lean mode and compatibility flags
  schemas.py                # typed scenario, event, order, state, and trace records
  scheduler.py              # stable discrete-event queue and task wakeups
  tasks.py                  # SimTaskRuntime, Thread/Timer/join/liveness interception
  clock.py                  # SimClock and legacy time patching
  legacy_runner.py          # cooperative oracle and non-golden process validation lane
  worker_loader.py          # fresh worker-scoped module graphs and reset/unload
  seams.py                  # minimal broker/clock/filesystem callback injection
  import_fakes.py           # fake kite/monitor/memcache/notification/ticker modules
  network_guard.py          # fail-closed socket/DNS/HTTP interception
  fake_kite.py              # narrow KiteConnect-compatible facade
  fake_ticker.py            # connect/reconnect and callback delivery
  broker_engine.py          # order/GTT/position state machines
  fill_models.py            # recorded, tick, quote, and one-minute OHLC models
  branches.py               # same-bar state forks, hashes, merges, and budgets
  faults.py                 # deterministic response/callback/failure scripts
  persistence.py            # sandboxed status/executed-order/config stores
  tracing.py                # canonical JSONL trace and state hashes
  normalize.py              # stable ID/PID/path/time normalization
  compare.py                # golden and differential comparison
  coverage_manifest.py      # reachable-function and branch catalog
  lean_adapter.py           # added only in the LEAN phase

data/wave-replay/           # copied/downloaded exports; generated and gitignored
  <dataset_id>/             # same immutable layout as exporter output

tests/
  characterization/wave/
    test_startup.py
    test_duo_matrix.py
    test_callbacks_and_polling.py
    test_restrictions_and_multipliers.py
    test_repricing.py
    test_gtt_lifecycle.py
    test_market_hours_and_expiry.py
    test_restart_recovery.py
    test_multi_instance.py
    test_persistence_and_status.py
  replay/wave/
    test_bootstrap_preflight.py
    test_task_runtime.py
    test_worker_namespaces.py
    test_scheduler.py
    test_fake_broker.py
    test_fill_models.py
    test_trace_determinism.py
    test_differential.py
  fixtures/wave_replay/
    scenarios/
    broker/
    instruments/
    configs/
    goldens/
    manifests/
```

If repository conventions favor `simulation/` over `wave_replay/`, preserve the internal separation. Do not put the fake broker into `common_lib.py`. Full historical exports remain gitignored in both repositories; they are addressed by `dataset_id` and manifest hash, transferred as an artifact or copied explicitly, and never located by traversing to `..\indian-wheel`.

### 6.3 Pre-import bootstrap

`wave_replay.bootstrap.ReplayBootstrap` is mandatory for every deterministic run. It executes before importing `common_lib.py`, `ticker_single_scraper_new.py`, `flask_app.py`, or any module that imports them:

1. Create a unique temporary sandbox and make it the working directory.
2. Materialize a scenario-owned import root containing the exact hashed Wave-reachable source snapshot, so module-relative `__file__` paths resolve inside the sandbox rather than the production checkout.
3. Write scenario-owned `configfile.ini` with fake canary credentials and an explicit `live_trading` value. `live_trading=true` is allowed only because all broker/network imports are already fake; dry-run scenarios set it false explicitly.
4. Set a minimal allowlisted environment and remove real token/key/home/runtime variables.
5. Install fail-closed socket, DNS, HTTP, WebSocket, Twisted, and broker transport guards.
6. Pre-register deterministic/fake modules in `sys.modules`: `kiteconnect.KiteConnect`/`KiteTicker` and exception types; `kite_api_monitor.MonitoredKite` and cache hooks; `pymemcache.client.base.Client`; `notifications.service.dispatch`; and Wave-reachable Twisted/Autobahn/requests transport entry points. Fake modules preserve the narrow interfaces and constants consumed by Wave but cannot open a transport.
7. Patch `threading.Thread`, `threading.Timer`, direct `_thread` starts, time, randomness, and ticker delivery to scheduler-owned implementations.
8. Configure scenario-owned SQLite paths/fixtures for instruments, API-monitor state, GTT tags, and trade/executed-order state. Wrap `sqlite3.connect` before import to reject non-sandbox paths and record sandbox-relative database creation, schema migration, WAL/SHM/journal creation, and call site.
9. Create isolated status, log, executed-order, Wave config, delta-limit, spot-bridge, and temporary paths.
10. Clear prior worker modules/import caches, then import Wave code through the worker loader.
11. Inject `FakeKite`, `FakeTicker`, `SimClock`, `SimTaskRuntime`, persistence sinks, and trace hooks before executing startup.

Any attempt to import or construct a real broker/network client fails preflight. Any socket, DNS, HTTP, or WebSocket operation not registered with the fake transport emits a sanitized `UNEXPECTED_NETWORK_ACCESS` event containing only transport category and sandbox-relative call site, never hostnames, URLs, headers, tokens, payloads, or arguments, then fails the run.

Memcache is forced to a scenario fixture (`disabled`, `miss`, `hit`, or scripted error) before import. The run must never probe `localhost:11211` or infer behavior from machine state.

At teardown, close SQLite connections, checkpoint/delete sandbox WAL/SHM artifacts, unload every worker-scoped module, restore host imports/environment/cwd, scan all trace/log/error artifacts for canary secrets, and remove the sandbox. A failed cleanup is a test failure. No replay reads or writes the real repository's runtime files, user home, configured databases, credentials, status, or logs.

### 6.4 Cooperative execution seams

The deterministic oracle uses the current decision code through bootstrap-installed substitution, not moved/copied strategy logic:

- module-global `kite` resolves to `FakeKite`, and ticker construction resolves to `FakeTicker`;
- all status/config/executed-order paths resolve to the scenario sandbox;
- all time sources and sleeps resolve to `SimClock`;
- all Wave-reachable thread/timer creation and ticker callbacks resolve to `SimTaskRuntime`;
- random tag generation is seeded;
- instrument/cache/database state comes from scenario fixtures;
- logs and notification dispatches become sanitized trace events;
- the ticker startup/loop runs as cooperative tasks until a declared event/time/end condition.

After characterization tests are green, introduce a small formal `BrokerPort`, `ClockPort`, or `TaskPort` only if needed. First prove each port produces the same cooperative trace as direct bootstrap substitution.

## 7. Interfaces and schemas

### 7.1 Clock interface

```python
class ClockPort(Protocol):
    def now_ist(self) -> datetime: ...
    def epoch(self) -> float: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float, *, reason: str = "") -> None: ...
```

The legacy bridge must route all of the following to one timeline:

- `get_ist_now()`;
- `time.time()`;
- `time.monotonic()`;
- `time.sleep()`;
- direct date/DTE calculations;
- tag timestamps;
- retry backoff and jitter;
- cache TTLs;
- cool-off sleeps;
- 180-second poll sleeps;
- delayed five-second status-refresh threads;
- stop-flow half-second waits.

`SimClock.sleep()` must suspend only the calling simulated task. Other callbacks and broker events at earlier timestamps continue to run.

### 7.2 Cooperative task interface

```python
class TaskRuntimePort(Protocol):
    def spawn(self, target, *, name: str, args=(), kwargs=None, daemon=False): ...
    def schedule_timer(self, delay: float, target, *, args=(), kwargs=None): ...
    def join(self, task_id: str, timeout: float | None = None) -> None: ...
    def is_alive(self, task_id: str) -> bool: ...
    def cancel(self, task_id: str) -> bool: ...
```

`SimTaskRuntime` patches `threading.Thread` and `threading.Timer` before Wave import:

- The initial substrate is one host OS thread running stackful `greenlet` tasks. This preserves arbitrary legacy Python call stacks across patched `time.sleep()`, broker waits, joins, and callback yield points without rewriting Wave functions as generators. Only the scheduler calls `greenlet.switch()`.
- `Thread.start()` never calls the OS. It validates one-start-only semantics, assigns a stable synthetic `ident`, and registers the target as a scheduler task at the current simulated timestamp.
- `Timer.start()` registers a scheduler event at `now + interval`; `cancel()` deterministically suppresses an event that has not begun.
- `join()` called by a cooperative task blocks that task and lets the scheduler run other eligible events until the target completes or simulated timeout expires. A harness-side `join()` drives the scheduler under the same rule.
- `is_alive()` is true for `scheduled`, `running`, `sleeping`, or `blocked` tasks and false for `completed`, `cancelled`, or `failed`.
- `daemon`, name, parent task, start/join/cancel timestamps, wake reason, and terminal exception are trace fields; daemon status never changes deterministic shutdown.
- direct `_thread.start_new_thread`, unregistered executors, and any other Wave-reachable concurrency primitive fail closed unless explicitly adapted.
- cooperative locks used by Wave are scheduler-aware and surface deterministic deadlock rather than blocking the host interpreter.

`FakeTicker` never invokes callbacks on its own stack or a socket thread. It schedules callback tasks with explicit timestamp, priority, parent event, and insertion sequence. Nested callbacks are delivered only at declared scheduler yield points, which allows re-entrant/drop behavior around `already_updating_order` to be reproduced deliberately. The five-second delayed status-refresh `Thread` in `on_order_update`, callback handlers, poll-loop work, timers, and any future Wave-reachable thread target all use the same runtime.

### 7.3 Narrow broker interface

Implement only the Kite surface reached by Wave:

```python
class WaveBrokerPort(Protocol):
    def quote(self, instruments): ...
    def positions(self): ...
    def orders(self): ...
    def order_history(self, order_id): ...
    def place_order(self, **request): ...
    def modify_order(self, **request): ...
    def cancel_order(self, **request): ...
    def place_gtt(self, **request): ...
    def delete_gtt(self, trigger_id): ...
    def instruments(self, exchange=None): ...
```

The `FakeKite` compatibility facade also exposes exactly the constants read by current code and token/session methods used at startup. Any unexpected method or missing field must fail the scenario with a clear "outside Wave contract" error. Do not return permissive `MagicMock` values.

### 7.4 Scenario schema

Use YAML for reviewable scenarios and JSONL/Parquet for large event streams. A scenario includes:

```yaml
schema_version: 1
scenario_id: nifty_range_atm_ce
mode: historical_modeled
baseline_commit: "<sha>"
seed: 104729
timezone: Asia/Kolkata
scenario_role: range
selection:
  selected_date: "2025-07-07"
  first_completed_underlying_close_at: "2025-07-07T09:16:00+05:30"
  first_completed_underlying_close: 25424.65
  right: call
  min_calendar_dte: 5
  max_calendar_dte: 20
  expiry: "2025-07-17"
  calendar_dte: 10
  strike: 25400
  symbol: "NIFTY2571725400CE"
  instrument_id: "NIFTY|2025-07-17|C|25400"
  source_instrument_id: "<frozen Upstox/source contract id>"
  lean_symbol: "<frozen LEAN symbol>"
  replay_instrument_token: "<frozen collision-checked positive token>"
  instrument_token_kind: synthetic_replay
  contract_metadata_sha256: "<sha256>"
start_at: "2025-07-07T09:15:00+05:30"
end_condition:
  session_close: true
config:
  configfile: "configs/default.ini"
  configfile_sha256: "<sha256>"
  wave_extractor: "configs/nifty-default.json"
  wave_extractor_sha256: "<sha256>"
  delta_limits: "configs/delta-default.json"
  delta_limits_sha256: "<sha256>"
  resolved_wave_settings:
    source: current_ui_saved
    multiplier_scale: "<frozen map>"
    delta_multiplier: "<frozen map>"
    velocity_guard: "<frozen map>"
    fill_cooldown:
      enabled: true
      window_minutes: "<resolved>"
      trigger_count: "<resolved>"
      cooldown_duration_minutes: "<resolved>"
instruments: "instruments/2025-07-07.json"
market_dataset:
  dataset_id: "nifty-bcs-1m-2025-02-v1"
  path: "data/wave-replay/nifty-bcs-1m-2025-02-v1"
  manifest_sha256: "<sha256>"
  date: "2025-07-07"
  missing_quote_policy: fail
initial:
  cash:
    currency: INR
    amount: 1000000
  positions: []
  regular_orders: []
  gtts: []
  persisted_status: []
  executed_orders: []
workers:
  - instance_id: we-1
    symbol: "<same frozen exact NIFTY CE symbol>"
    option_close_at_selection: "<resolved positive close>"
    buy_gap_pct: 0.10
    sell_gap_pct: 0.10
    buy_gap_points: "<round(option close * 0.10, 1)>"
    sell_gap_points: "<round(option close * 0.10, 1)>"
    lots: 1
    lot_size: "<date-effective exported contract lot size>"
    buy_quantity: "<lot_size>"
    sell_quantity: "<lot_size>"
    order_mode: regular
replay:
  market_resolution: 1minute
  cool_off_mode: disabled
  trigger_mode: close_only
  fill_price_mode: limit
  broker_fee_model: upstox_approximation
  fee_schedule_sha256: "<sha256>"
  activation_policy: subsequent_bar
  equivalence_status: modeled_not_exact
sensitivity:
  trigger_mode: ohlc_touch_branched
  branch_budget: 256
broker_script: null
expected:
  golden: "goldens/nifty_range_atm_ce.jsonl"
```

Reject scenarios with implicit current date, unfrozen contract selection, unpinned resolved Wave settings, missing exchange, unversioned instrument or market data, a manifest hash mismatch, an undeclared dataset partition, or a non-deterministic seed.

### 7.5 Portable historical dataset contract

The cross-repository contract is `wave-replay-dataset-v1`. Parquet stores typed, columnar bars and contract metadata; `manifest.json` is the authoritative inventory and provenance record. The consumer must reject unknown major schema versions and may accept backward-compatible minor additions.

`underlying_bars.parquet` and `option_bars.parquet` share:

| Field | Type | Meaning |
|---|---|---|
| `instrument_id` | string | Stable cross-repository contract/bar identity |
| `source_instrument_id` | nullable string | Original Upstox/source instrument identity |
| `replay_instrument_token` | int64 | Exporter-derived simulator token using `sha256(instrument_id)[:15]`; never a Zerodha token |
| `wave_trading_symbol` | nullable string | Exact synthesized symbol exposed to FakeKite and legacy Wave when calendar classification is safe |
| `lean_symbol` | string | LEAN archive/universe symbol preserved for provenance |
| `underlying` | string | `NIFTY`, `RELIANCE`, and so on |
| `security_type` | enum | `index`, `equity`, `index_option`, or `equity_option` |
| `market` | string | `india` |
| `exchange` | string | Resolved exchange/segment |
| `session_date` | date | Exchange-local session date |
| `bar_start_utc`, `bar_end_utc`, `available_at_utc` | timestamp with UTC timezone | Canonical ordering timestamps |
| `bar_start_ist`, `bar_end_ist`, `available_at_ist` | timestamp with `Asia/Kolkata` offset | Reviewable local timestamps |
| `resolution_seconds` | int | Must equal `60` for historical Wave replay; validated rather than inferred from the LEAN `/minute/` folder name |
| `open`, `high`, `low`, `close` | decimal | Source OHLC after documented LEAN scaling reversal |
| `volume` | int64 | Source volume, including legitimate zero |
| `open_interest` | int64 nullable | Source OI when present; null when unavailable, never fabricated |
| `source_timestamp_kind` | enum | `bar_start` or `bar_end` as stored by the source archive |
| `missing_bar` | bool | Explicit gap row generated by calendar/contract coverage validation |
| `missing_reason` | string nullable | `source_absent`, `invalid_dropped`, `contract_unavailable`, or other declared reason |
| `source_archive`, `source_archive_sha256` | string | LEAN archive path and hash |
| `source_member`, `source_member_sha256`, `source_row` | string/hash/int64 | Stable archive-member and row locator |
| `source_universe_sha256` | nullable string | Universe/symbol metadata provenance |

Both bar files use the producer's exact shared Arrow schema. Expiry, strike, right, style, classification, lifecycle, and multipliers live in `contracts.parquet`, including the additive `source_interval` (required to equal `1minute`), `source_contract_multiplier`, `resolved_contract_multiplier`, source-preferred `lot_size`, observed/missing counts, and source contract metadata hash.

Upstox/LEAN has no authentic Zerodha instrument token. The exporter derives `replay_instrument_token` from the canonical `instrument_id`; the consumer verifies collisions and freezes the value in scenario/trace metadata with `instrument_token_kind=synthetic_replay`. Neither side invents or claims a real Zerodha token.

`contracts.parquet` contains the complete declared contract lifecycle: first/last available bar, expiry, strike, right, style, market, lot-size regime, source contract key, strike-window inclusion, weekly/monthly classification, and exclusion reason. `sessions.parquet` contains the `Asia/Kolkata` session calendar, holidays, regular open/close, and any declared special session. Wave uses the regular F&O session unless a scenario explicitly targets a source special session.

#### Timestamp and availability semantics

The exporter must normalize two source conventions without lookahead:

- LEAN-delivered events are timestamped at completed-bar **end**.
- Candle CSVs keyed by bar **start** become available only at `bar_start + resolution`.

For either input, the export records both `bar_start` and `bar_end`, and sets `available_at = bar_end` plus any declared source latency. A Wave `quote()` or market observation at simulated time `T` can see only bars with `available_at <= T`. The strategy may use that bar's close after availability, but an order created from that observation cannot retroactively fill against the same completed bar's high or low.

One-minute data is the minimum and only accepted bar resolution for historical Wave replay. A completed bar becomes observable only at `bar_end`.

The initial backtest sets `cool_off_mode: disabled`. At decision time `t`:

1. All option and underlying `quote()` reads resolve to the most recent synchronized completed close `C_t`.
2. The production cool-off sleep is bypassed, and both legacy before/after-cool-off quote reads see `C_t`; emit `COOL_OFF_BYPASSED` with the configured production duration.
3. Strategy decisions and new order prices use completed closes only.
4. An order created from `C_t` is ineligible for `C_t` and becomes active only for subsequent completed bars, starting with `C_(t+1)`.

This is an intentional first-mode simplification, not a correction to production and not exact legacy equivalence for timing. Every trace and report must state `cool_off_mode=disabled` and `equivalence_status=modeled_not_exact`.

Production behavior remains documented for later fidelity: `place_duo_order()` observes a quote, waits the configured duration (commonly 10 seconds), observes again, and chooses conservative anchors. One-minute bars cannot provide a truthful second observation 10 seconds later. Exact reproduction of that sequence is out of scope for the initial backtest and requires captured tick/second prices and callback timing.

#### Missing bars and synchronized portfolio snapshots

Portfolio-delta replay requires the underlying spot and a synchronized available bar for every held option at the calculation time. The scenario declares one policy:

- `fail` (default for legacy decision/differential claims): stop with the exact missing instruments/timestamps;
- `skip_decision`: do not invoke the decision point and mark the replay interval incomplete;
- `mark_unknown`: continue orchestration but set delta/restriction/economic output to unknown and make the run ineligible for equivalence claims.

Never forward-fill an option trade bar. A stale option close can materially change Greeks and restrictions and must remain missing. Do not carry a stale underlying into historical Wave decisions either; use the same declared missing-quote policy. Every delta snapshot records the bar IDs and source hashes used for each leg.

### 7.6 Input event schema

Every event has:

- `event_id`;
- `at` simulated IST timestamp;
- `type`;
- `source`;
- `instance_id` or `broker` scope;
- `payload`;
- optional `caused_by`;
- optional scheduler `priority`;
- fixture provenance.

Required input event types:

- `OPTION_TICK`, `SPOT_TICK`, `QUOTE_SNAPSHOT`;
- `UNDERLYING_BAR_AVAILABLE`, `OPTION_BAR_AVAILABLE`, `OPEN_INTEREST_AVAILABLE`, `MISSING_BAR`;
- `SESSION_OPEN`, `SESSION_CLOSE`, `CONTRACT_LIST_AVAILABLE`;
- `ORDER_ACK`, `ORDER_CALLBACK`, `ORDER_HISTORY_VISIBLE`, `ORDERS_VISIBLE`;
- `GTT_TRIGGER`, `GTT_DELETE_RESULT`;
- `BROKER_ERROR`, `NETWORK_TIMEOUT`, `RATE_LIMIT`, `REST_VISIBILITY_LAG`;
- `TICKER_CONNECT`, `TICKER_DISCONNECT`, `TICKER_NORECONNECT`;
- `POLL_WAKE`, `CONFIG_WRITE`, `STATUS_READ`, `SPOT_BRIDGE_WRITE`;
- `PROCESS_CRASH`, `PROCESS_SIGTERM`, `PROCESS_RESTART`;
- `HOLIDAY_CALENDAR_CHANGE` only for targeted cache-staleness scenarios.

Historical market events come only from the exported dataset and its declared partitions. Broker/fault events come from the scenario script. Their causal streams share the scheduler but retain separate provenance.

### 7.7 Canonical output trace schema

Write one JSON object per line:

```json
{
  "schema_version": 1,
  "seq": 42,
  "at": "2026-07-06T09:17:58.000000+05:30",
  "actor": "we-1",
  "kind": "broker_call",
  "operation": "cancel_order",
  "caused_by": "callback-17",
  "input": {"order_id": "O2", "variety": "regular"},
  "result": {"status": "accepted"},
  "state_before": "sha256:...",
  "state_after": "sha256:...",
  "details": {}
}
```

Trace kinds must cover:

- external event delivery;
- clock sleep/wake;
- broker request, response, and state transition;
- callback enqueue, start, drop, finish, and exception;
- polling request and visible snapshot;
- strategy decision with all multiplier/restriction inputs;
- regular/GTT order tracking graph mutation;
- position and fill ledger mutation;
- retry and cache hit/miss;
- persistence read/write/failure;
- process start/exit/restart;
- normalized status and executed-order snapshots;
- notification side effects;
- invariant violation.

Canonicalization must replace volatile PIDs, raw order IDs, trigger IDs, temporary paths, and wall-clock log prefixes with stable aliases while preserving their identity relationships. Preserve prices, quantities, statuses, event order, and simulated timestamps exactly.

## 8. Deterministic replay scheduler

Use a single stable priority queue ordered by:

1. simulated timestamp;
2. explicit scenario priority;
3. source sequence from the input dataset;
4. deterministic insertion sequence.

Define default same-timestamp priorities, but allow scenarios to override them:

1. exchange fill/trigger state mutation;
2. broker acknowledgement or error;
3. WebSocket callback delivery;
4. sleeping worker wakeup;
5. REST visibility update;
6. poll invocation;
7. status reader.

This ordering is only the default. Callback permutation scenarios must explicitly enumerate alternate orderings such as:

- `OPEN -> COMPLETE`;
- `COMPLETE` without `OPEN`;
- `COMPLETE -> delayed OPEN`;
- sibling `COMPLETE` events at the same timestamp;
- `CANCELLED` before cancel REST returns;
- fill while cancellation is in flight;
- modify `OPEN` echo before or after another manual modification;
- callback during cool-off or retry sleep;
- callback dropped by `already_updating_order`, then recovered or not recovered by polling.

Scheduler requirements:

- no real sleeping;
- no real OS thread, timer thread, ticker/network callback thread, or delayed-refresh thread in deterministic characterization mode;
- deterministic worker and callback task IDs;
- explicit branch IDs and parent ambiguity event for every state fork;
- independent queues and state snapshots after `LOW_FIRST`, `HIGH_FIRST`, and latency-stress forks;
- canonical state hashing, equivalent-state merging, deterministic branch-budget enforcement, and pruning diagnostics;
- maximum-event and maximum-simulated-time guards;
- deadlock/livelock detection;
- a traceable reason for every wakeup;
- configurable REST and WebSocket visibility delays;
- reproducible random jitter;
- ability to pause immediately before and after any broker call;
- exact replay from a failure trace with the same seed.

The scheduler is the only component allowed to advance a cooperative task. Every `TASK_REGISTERED`, `TASK_STARTED`, `TASK_BLOCKED`, `TASK_WOKE`, `TASK_JOINED`, `TASK_CANCELLED`, `TASK_FAILED`, and callback-delivery event includes its task/parent IDs and causal scheduler sequence.

Required concurrency fixtures prove:

- an order callback runs while another task is in simulated retry/poll/cool-off-disabled sleep without a host thread;
- a nested callback at a declared yield point exercises the current update guard deterministically;
- simultaneous callback and poll events follow each explicit priority permutation;
- the post-fill delayed status-refresh `Thread` becomes a scheduled task, sleeps five simulated seconds, and never starts an OS thread;
- `threading.Timer` registration, cancellation, firing, join timeout, and liveness use simulated time;
- strict `_thread`/executor guards detect any unadapted spawn;
- repeated runs produce byte-identical task and strategy traces.

Run every deterministic fixture repeatedly in CI and require byte-identical canonical traces. Golden traces are generated only by this cooperative scheduler lane.

## 9. Fake broker semantics

### 9.1 Source of truth

The fake broker, not the strategy `orders` dict, is authoritative for:

- live and terminal regular orders;
- order history;
- fills and average price;
- positions;
- GTT registry and trigger state;
- REST visibility;
- callback emission.

This separation is necessary to expose divergence between broker truth and local tracking.

### 9.2 Regular order lifecycle

Model at least:

```text
NEW_REQUEST
  -> REJECTED
  -> ACKNOWLEDGED -> OPEN
  -> OPEN -> PARTIALLY_FILLED -> OPEN
  -> OPEN/PARTIALLY_FILLED -> COMPLETE
  -> OPEN/PARTIALLY_FILLED -> CANCEL_PENDING -> CANCELLED
  -> CANCEL_PENDING -> COMPLETE
  -> MODIFY_PENDING -> OPEN / REJECTED_MODIFY
```

Keep broker acknowledgement, REST visibility, order-history visibility, position visibility, and WebSocket delivery as separate scheduled events. Zerodha-like order dictionaries must contain only realistic types and fields; do not add harness-only fields to objects consumed by legacy code.

### 9.3 Position ledger

Apply every fill, including partial fills, to a broker position ledger. Produce the `net` and `day` views read by current code, including:

- signed quantity;
- buy/sell quantities and average prices;
- last price;
- realized and mark-to-market P&L;
- instrument token, symbol, exchange, and product.

Support account-wide positions so two workers see the same portfolio while retaining separate process-local caches.

### 9.4 GTT lifecycle

Model a GTT as a persistent trigger plus its child-order template:

```text
CREATE_REQUEST -> ACTIVE
ACTIVE -> DELETE_PENDING -> DELETED
ACTIVE -> TRIGGERED -> child regular order with new order_id and same tag
ACTIVE -> REJECTED/EXPIRED/DISABLED
```

The adapter must:

- validate trigger price versus the `last_price` supplied to `place_gtt`;
- retain the strategy tag exactly;
- create a distinct regular order ID when triggered;
- permit `OPEN`, `TRIGGER PENDING`, or immediate `COMPLETE` child visibility;
- independently delay callback and `orders()` visibility;
- preserve a triggered child when trigger deletion races with firing;
- inject create/delete failures and missing tags;
- support regular+GTT and GTT+GTT sibling combinations;
- model restart with GTTs still active after process-local metadata is lost.

Do not assume GTTs are equivalent to resting exchange orders. They are server-side triggers that create regular child orders.

### 9.5 Cancellation and modification

Cancellation scenarios must cover:

- accepted cancellation with immediate callback;
- accepted cancellation with delayed callback;
- callback before REST response;
- already complete;
- already cancelled;
- network timeout where the cancel did happen;
- network timeout where it did not happen;
- five consecutive failures;
- sibling fills between local deletion and broker cancellation;
- GTT deletion racing with trigger.

Modification scenarios must cover:

- successful multiplier repricing;
- broker reject/circuit-limit failure;
- callback echo matching expected price;
- callback echo rounded differently by the broker;
- manual modification before, during, and after auto-reprice;
- fill during modify;
- partial fill followed by quantity/price modification.

### 9.6 Polling reconciliation

Allow independent scripts for:

- `order_history(order_id)` result at each call;
- `orders()` result at each call;
- `positions()` result at each call;
- empty histories;
- stale non-terminal histories;
- terminal history before callback;
- callback before terminal history;
- REST errors followed by stale cached positions;
- reconnect clearing the order-history cache.

The fake must expose call counts and visibility versions in the trace so cache behavior is testable.

## 10. Historical market data and fill models

### 10.1 First-class source: `indian-wheel` Upstox/LEAN data

The default historical market-path source is the existing sibling `indian-wheel` repository (`YadavKapil/Backtesting`), exported through the contract in section 7.5. Direct file reads from a sibling checkout are prohibited.

The source boundary includes:

- `config/data/*.json` data settings: underlying and instrument key, market, date range, interval, `option_history_days`, underlying type, option inclusion/style, strike window, contract multiplier, expiry selection, `raw_cache_name`, `lean_data_name`, and fixed-multiplier policy;
- raw Upstox caches under `data/raw/<raw_cache_name>/`, including candles, contracts, expiries, and per-response manifests;
- the standard `data/lean` root and isolated roots selected through `lean_data_name`, especially `data/lean-nifty-bear-call-spread-1minute`;
- LEAN index/equity underlying trade bars;
- LEAN index-option/equity-option trade bars and separate open-interest archives where present;
- option universe CSVs and symbol metadata;
- `symbol-properties-database.csv` contract multipliers/tick sizes;
- `market-hours-database.json`, market `india`, and option style;
- quality reports and all archive/config hashes needed for provenance.

The source stores multiple bar periods under LEAN's `/minute/` hierarchy, so the folder name alone is not evidence of resolution. Historical Wave export and replay must select and validate synchronized **one-minute underlying and one-minute option trade bars only**. Any archive whose config or observed row spacing is not 60 seconds is inventoried as unavailable to this harness and excluded from export.

### 10.2 Verified dataset inventory and selection

The implementation phase must regenerate an inventory from manifests before choosing dates. The current generated-data checkout establishes this bounded, non-promissory baseline:

| Source/config | Verified content | Intended Wave use |
|---|---|---|
| `config/data/nifty-bear-call-spread-1minute.json` with `data/lean-nifty-bear-call-spread-1minute` | Isolated one-minute NIFTY underlying and index-option conversion; current checkout has hundreds of daily archives, with option history observed from roughly September 2024 into August 2026 | **Accepted actual-data source** for historical Wave replay, subject to synchronized contract/day validation |
| Standard NIFTY configs under `data/lean` | Generated NIFTY and European index-option archives exist at a coarser resolution | Inventory/provenance only; **excluded from all Wave historical replay and tests** |
| `reliance.json`, `hdfcbank.json`, `icicibank.json`, `infy.json`, `sbin.json` | Generated American equity-option trade/OI archives exist, predominantly at a coarser resolution | **Excluded** unless a separate verified one-minute underlying-and-option export is produced later |
| `nifty.json` and related NIFTY configs | Multiple lot-size regimes have been observed (`25`, `65`, `75`) and configurations may force a current lot size | Contract-lifecycle and multiplier-validation scenarios |
| NIFTYBEES equity bars | A potential proxy source exists, but the currently inventoried standard bars are not accepted by the one-minute contract | No current Wave use; future volume logic would require a separately verified synchronized one-minute NIFTYBEES export |

Before an end-to-end run, the exporter records exact archive counts, members, contract counts, date ranges, resolutions, and missingness in the dataset manifest. The plan does not assume that every strike, expiry, or requested market day is available.

Use synchronized one-minute NIFTY underlying and option data for every historical Wave scenario: range, trend, gap, expiry, high-gamma, configuration, session boundary, and risk behavior. If one-minute coverage is unavailable, the historical scenario is unsupported; do not substitute or down-label coarser data.

NIFTY index bars legitimately carry zero volume. Wave currently does not consume volume, so preserve zero and do not fabricate activity. If a later feature needs a volume signal, it must explicitly select synchronized NIFTYBEES as a proxy and treat that as corrected/new behavior, not legacy equivalence.

### 10.3 Approved initial corpus and scenario defaults

These defaults define the first implementation corpus only. They are intentionally narrow, reviewable, and reproducible; they are not universal Wave configuration advice.

The exporter inventory has frozen these initial fixtures:

| Role | Trading date | Frozen ATM CE | Expiry | Lot size | Option coverage |
|---|---|---|---|---:|---|
| Range | 2025-07-07 | NIFTY 25400 CE | 2025-07-17 | 75 | 375 complete minute bars |
| Trend | 2024-11-22 | NIFTY 23400 CE | 2024-11-28 | 25 | 375 complete minute bars |
| Gap open | 2026-04-08 | NIFTY 23850 CE | 2026-04-13 | 65 | 375 complete minute bars |
| Expiry/high-gamma stress | 2026-04-02 | NIFTY 22200 CE | 2026-04-07 | 65 | 375 complete minute bars |
| Missing-data policy | 2025-04-07 | NIFTY 22050 CE | 2025-04-17 | 75 | 363 bars; missing 09:15-09:26, first bar starts 09:27 |

The first four are the actual-data baseline. The fifth verifies `fail`, `skip_decision`, and `mark_unknown`. Scenario generation tests must assert these dates, strikes, expiries, and date-effective lot sizes; quantity must never be forced to a config-wide lot size.

#### Representative-day auto-selection

The exporter inventories verified dates with complete one-minute NIFTY underlying coverage and at least one eligible one-minute CE contract. A versioned selector scores that candidate set and proposes one distinct day for each role:

- `range`: low close-to-close directional efficiency relative to intraday range, with repeated movement around the session midpoint;
- `trend`: high absolute session return and directional efficiency;
- `gap_open`: largest absolute difference between the first completed close after open and the prior session's last completed close;
- `expiry_high_gamma`: smallest eligible DTE and, as a tie-breaker, highest computed ATM gamma using the pinned Wave Greeks backend and synchronized first closes.

Scoring uses the complete day only to choose test fixtures, never to make an in-replay trading decision. The selector records its version, formulas, inputs, candidate/exclusion reasons, scores, tie-breaks, and source hashes. Deterministic ties choose the earliest date. A reviewer approves the proposed days, after which `nifty-representative-days-v1.json` freezes the exact dates and scenario roles. If fewer than four distinct eligible days exist, fail corpus construction rather than reuse or weaken a role silently.

#### Deterministic contract selection

For each selected date:

1. Use the first completed one-minute NIFTY underlying close available after the 09:15 IST session open (normally the bar available at 09:16) as the ATM reference.
2. From exported contract metadata, retain NIFTY calls only.
3. Compute calendar DTE as `expiry_date - trading_date`; retain only `5 <= DTE <= 20`.
4. Choose the nearest eligible expiry, meaning the minimum expiry date/minimum DTE in that window. If none exists, fail the scenario; do not select below 5 or above 20 DTE.
5. Within that expiry, retain contracts with valid date-effective lot size and required one-minute trade coverage.
6. Select the strike minimizing absolute distance from the ATM reference. A distance tie chooses the lower strike, then the lexicographically smaller source instrument ID.
7. Resolve the first synchronized positive option close available at the selection timestamp. If it is missing or non-positive, fail rather than switch contract.
8. Freeze the date, first underlying close/time, exact symbol, source instrument ID, expiry, calendar DTE, strike, right, option close/time, lot size, and all source hashes into the scenario manifest.

Selection runs once while constructing the corpus. Replay never reselects a contract, rolls expiry, or substitutes a symbol.

#### Account, Wave, and order defaults

Each initial historical scenario has exactly one Wave instance trading the frozen ATM CE:

- flat starting portfolio;
- no open regular orders;
- no active GTTs;
- INR 1,000,000 starting cash;
- one lot, with quantity equal to the date-effective `lot_size` exported for that contract;
- BUY gap = 10% of the selected option close;
- SELL gap = 10% of the selected option close;
- resolved absolute gaps rounded to one decimal before entering the existing legacy percentage-gap path;
- regular order mode for the actual-data baseline;
- `cool_off_mode: disabled` for the 10-second placement cool-off only;
- same-side fill cooldown remains enabled using the current saved Wave configuration or current code defaults when absent;
- current UI/saved step multiplier, delta multiplier/restrictions, and velocity settings, resolved once and frozen with their source files and hashes;
- one-minute `close_only`, subsequent-bar activation, and limit-price fills;
- `broker_fee_model: upstox_approximation`.

The scenario manifest persists both raw configuration references and fully resolved values so later UI/config changes cannot alter a historical result. The report states that the Upstox fee schedule is an approximation for economic comparison and does **not** claim Zerodha brokerage/statutory/rounding fidelity.

GTT behavior remains mandatory in scripted characterization and fault scenarios, but the initial actual-data baseline neither starts with a GTT nor requires regular-order failure to produce one naturally.

### 10.4 Historical availability and contract lifecycle

The exporter and selector must model source limitations:

- Upstox's expired-instrument endpoint provides expired contracts, not an unconstrained historical option chain.
- `option_history_days` limits the lookback requested for each expiry: the current NIFTY bear-call configurations use a short window (for example 14 days), while equity configurations commonly use a longer window (for example 59 days).
- `option_strike_window_points` bounds downloaded strikes around the configured reference; absence outside that window is not a zero price.
- `option_expiry_selection` can include all expiries or monthly-only contracts; weekly contracts cannot be inferred when a monthly-only export was used.
- Separate `raw_cache_name` and `lean_data_name` values isolate data roots; the exporter must not merge them implicitly.
- Fixed multiplier conversion can hide historical lot-size changes. Export both source and resolved multipliers and reject mixed-lot replay unless the scenario states how quantities are translated.
- Illiquid contracts can have missing bars, zero OHLC, zero volume, or zero OI. Distinguish a valid zero from an absent/invalid row and never turn either into a synthetic tradable quote.
- At expiry and roll boundaries, stop producing events after the contract's verified last bar. A replacement expiry is a new contract chosen by an explicit scenario rule, not an automatic symbol continuation.
- Universe files contain zero placeholder Greeks rather than historical IV/Greeks. Wave delta must be recomputed from synchronized prices using the pinned Wave Greeks backend; it must not consume those placeholders as observations.

Dataset selection fails early if the requested Wave symbol cannot be mapped to an exported contract or if the required underlying/held-option coverage is incomplete under the chosen missing-quote policy.

### 10.5 Dual market and broker input model

Actual Upstox/LEAN data supplies market path:

- session open/close;
- available underlying and option OHLCV bars;
- option OI when present;
- contract/universe lifecycle;
- latest observable option/spot prices used by `quote()`;
- bar-range evidence used by the selected fill model.

It cannot supply Zerodha strategy-specific broker behavior. Keep a separate scripted stream for:

- placement acknowledgements and rejects;
- partial fills and average-price evolution;
- cancellation/modify races and failures;
- callback ordering, duplication, omission, and delay;
- REST order/position visibility lag;
- GTT creation, trigger, child order, and delete behavior;
- disconnect/reconnect, restart, token, rate-limit, timeout, and API failures.

A historical scenario can use default deterministic broker semantics for ordinary placements and add only exceptional scripted events. Every trace marks whether a fill/status was `bar_inferred`, `recorded_broker`, or `scripted_fault`. Historical bars must never be treated as evidence that a specific callback or REST sequence occurred.

### 10.6 Recorded-event and tick/quote fill models

When production broker events are available, replay actual acknowledgement, status sequence, filled quantity, average price, rejection reason, callback timestamp, and REST visibility timestamp. This remains the highest-fidelity broker model.

If a future dataset supplies quotes:

- BUY is marketable when ask is at or below the limit; SELL when bid is at or above the limit;
- last-trade crossing is weaker evidence and is labeled `last_trade_inferred`;
- depth/volume governs partial fills only when actually present;
- GTT trigger crossing and child-limit fill remain distinct;
- request price, exchange tick conversion, execution price, and mark price remain separate.

The current Upstox trade bars do not include bid/ask or queue position. Do not infer them silently.

### 10.7 Initial trigger mode: `close_only`

`close_only` is the required default for the first end-to-end Upstox/LEAN Wave backtest. It is a modeling simplification, not observed intraminute execution.

Semantics:

1. Strategy decisions, risk calculations, latest prices, and newly calculated order prices use synchronized completed one-minute closes only.
2. A regular or GTT-child order created from close `C_t` becomes eligible only on subsequent completed bars; it cannot trigger or fill from `C_t`.
3. For every later eligible close `C_n`:
   - BUY fills only when `C_n <= buy_limit`;
   - SELL fills only when `C_n >= sell_limit`;
   - high and low are ignored for trigger and fill decisions.
4. If the close crosses only BUY, fill BUY at the configured fill price and deliver the normal Wave sibling-cancellation flow before evaluating future closes.
5. If the close crosses only SELL, fill SELL and follow the corresponding cancellation flow.
6. If neither predicate is true, neither order fills.

For a valid positive bracket, enforce `buy_limit < sell_limit`; one close then cannot cross both sides. If limits overlap or are inverted (`buy_limit >= sell_limit`), emit `INVALID_OVERLAPPING_BRACKET`, fill neither side, and fail that replay branch by default. A targeted scripted characterization may declare `malformed_bracket_policy: buy_first` or `sell_first`, but the policy must be explicit and cannot enter the normal historical corpus.

`fill_price_mode: limit` is the default. The optional `fill_price_mode: close_price` sensitivity fills BUY at `min(C_n, buy_limit)` and SELL at `max(C_n, sell_limit)`, preserving legal price improvement and reporting results separately. The simulator must never switch to close-price fills silently.

Because `close_only` ignores intraminute high/low touches, it will generally miss touches and undercount fills relative to a live/tick replay. Reports must label `trigger_mode=close_only`, count close-cross fills, and state this expected downward fill bias. Compare it later against `ohlc_touch_branched` on the same dataset and configuration.

### 10.8 Deferred sensitivity mode: `ohlc_touch_branched`

**Implementation status:** explicitly deferred beyond approved stages 1-10. The initial shipped simulator implements `close_only` only. The design below remains the later sensitivity contract and must not be inferred as implemented.

Convert each one-minute bar into an availability event at `bar_end`. Evaluate its full range only for orders that became eligible from a prior completed close and were active as the bar interval began. An order created from `C_t` never uses bar `t`; bar `t+1` is its first eligible full-range bar. Never apply already elapsed high/low to an order activated mid-bar by a scripted broker event.

For an active linked BUY/SELL pair on a later eligible bar:

| Bar evidence | Required result |
|---|---|
| Neither limit touched | No fill |
| Only BUY touched (`low <= buy_limit`) | BUY fills; normal Wave sibling-cancel/replacement flow follows |
| Only SELL touched (`high >= sell_limit`) | SELL fills; normal Wave sibling-cancel/replacement flow follows |
| Both limits touched | Ambiguous; OHLC does not reveal whether low or high occurred first |

For every both-touched bar, do not force a hidden answer. Fork the full replay state into at least:

- `LOW_FIRST`: BUY-side touch/fill is delivered first, then Wave cancellation and subsequent state evolve from that event;
- `HIGH_FIRST`: SELL-side touch/fill is delivered first, then Wave cancellation and subsequent state evolve independently;
- optional `CANCELLATION_LATENCY_STRESS`: both fills can occur before sibling cancellation becomes effective, using an explicit latency/fault schedule.

Each branch owns an independent copy-on-write scheduler queue, fake-broker ledger, Wave worker state, caches, cooldown/velocity history, tracked sibling graph, persistence state, and trace. Later bars are evaluated separately because the first fill changes anchors, positions, delta, restrictions, cooldown, and future orders.

Additional analyses are permitted but are never observed truth:

- deterministic `O-L-H-C` and `O-H-L-C` paths;
- seeded Brownian-bridge paths constrained to the observed OHLC, with seed/model parameters recorded;
- `exclude` for a report limited to unambiguous fills.

`LOW_FIRST` and `HIGH_FIRST` across every ambiguous bar are scenario paths, not automatically proven global best/worst bounds. The result must not call them exact bounds unless monotonicity has been demonstrated for that scenario.

#### Branch control and path explosion

After every event, hash the canonical broker and Wave state plus pending causally relevant scheduler events. Merge branches only when those hashes and future event inputs are equivalent; retain all originating path labels and weights/diagnostics.

Set a configurable branch budget. If expansion would exceed it:

1. merge equivalent states first;
2. preserve explicitly adverse and best-known representative states under a declared ranking;
3. sample or prune remaining states deterministically;
4. label all downstream ranges `bounded/approximate`, record pruned count and criteria, and stop making exact-envelope claims.

Reports must include ambiguous bar count, branch creations, merged/pruned states, peak/live branch count, branch budget, each terminal path policy, fills, P&L, exposure/delta extrema, and dispersion.

This mode is not the initial default. It is a later sensitivity analysis that estimates the effect of intraminute touches omitted by `close_only`.

### 10.9 Fill prices, execution uncertainty, and fees

Trade bars have no bid/ask. The default `limit` policy fills a touched limit order at its limit price, then applies fees. This conservative no-price-improvement policy is valid for BUY and SELL and never executes a limit at a worse price than its limit.

If a bar opens through a resting limit, support only:

- `limit`: fill at the limit, conservatively discarding price improvement;
- `open_price_improvement`: BUY may fill at `min(open, limit)` and SELL at `max(open, limit)`, reported separately;
- `recorded_fill`: use a captured broker fill, while validating the fill against order semantics.

Slippage must never make a limit fill worse than its limit. Model execution uncertainty through explicit touch-to-fill probability, deterministic missed-fill rules, or scripted broker rejection/latency—not an illegal execution price. Any seeded fill-probability model records its seed and produces sensitivity results, not legacy truth.

Do not use bar volume as queue depth without an explicit participation model. NIFTY index zero volume is never used for option fills.

Use `indian-wheel`'s portable Upstox fee-engine inputs for the initial corpus. The exporter should emit `fee_schedule.json` containing the resolved Upstox brokerage and date-effective Indian statutory rates, product/security mappings, DP-charge policy, currency rounding rules, schedule identifier, source path, and hash. This avoids importing sibling Python code at runtime while preserving its fee assumptions. If the fee policy becomes a separately versioned package later, the manifest pins that package version and resolved schedule.

Set `broker_fee_model=upstox_approximation` in the scenario, trace header, and every economic report. Calculate brokerage, STT, exchange charges, SEBI charges, GST, stamp duty, and any applicable DP charge after inferred fills. Never label this result as Zerodha fee fidelity. In `legacy_equivalent`, fees and slippage affect economic reports but not decisions unless current Wave code consumes them. LEAN comparisons must use the same exported fee schedule and fill model or report the difference explicitly.

## 11. State, process, and persistence isolation

### 11.1 Authoritative legacy execution

The authoritative decision/state/interleaving oracle is the deterministic cooperative runner. The initial supported multi-worker mechanism is **fresh worker-scoped module namespaces in one controlled interpreter**, not shared `common_lib` globals:

1. `WorkerModuleLoader` loads a fresh `common_lib` module object and ticker module object under a synthetic worker package name.
2. During import and task dispatch, a worker-scoped import router resolves legacy absolute imports such as `common_lib` to that worker's module object.
3. Each function's globals remain bound to its own module dictionary after loading.
4. Only one cooperative task executes Python code at a time; the scheduler switches workers explicitly.
5. The loader asserts that mutable module objects, locks, caches, order dictionaries, and callback guards are not identity-shared between workers.

Each `WorkerContext` contains the process-local state production currently obtains through globals:

- `orders` and sibling graph;
- initial/current position snapshots;
- order counters;
- restriction flags;
- position and order-history caches;
- Wave config cache;
- spot velocity history;
- same-side fill history and cooldown;
- repricing markers;
- completed-order IDs;
- random tags and process status.

The coordinator owns `FakeKite`, market data, the account ledger, simulated time, and event ordering. Worker calls enter shared broker truth only through deterministic adapter requests. No worker directly reads another worker's module namespace.

If worker-scoped import routing proves unable to isolate a Wave-reachable extension module, the fallback is one step-locked interpreter subprocess per worker: the coordinator sends exactly one task step and receives its result before advancing another event. That fallback still installs bootstrap and `SimTaskRuntime`, permits no autonomous thread/callback execution, and must produce the same canonical trace. It is not the separate process-validation lane described below.

### 11.2 Shared versus local state

Share through the coordinator:

- broker account and position ledger;
- market data;
- instrument master and holidays;
- persistent GTT registry;
- dashboard configuration files;
- executed-order store when production workers share it;
- simulated exchange clock.

Keep local to each worker:

- all `common_lib.py` globals;
- caches and cooldown histories;
- callback guard values;
- worker status file;
- process lifecycle and failure state.

### 11.3 Filesystem sandbox

Each scenario gets a temporary root with explicit subdirectories for:

- config snapshots;
- status;
- logs;
- executed orders;
- GTT tag persistence;
- instrument database;
- spot-price bridge files.

The sandbox also owns:

- the staged source/import root;
- fake `HOME`, temp, cache, and application-state directories;
- explicit fake/canary credentials and environment;
- instrument, API-monitor, GTT-tag, trade, and executed-order SQLite databases;
- every SQLite journal, WAL, SHM, lock, and temporary file;
- a creation ledger of every opened/written path.

All paths must resolve beneath the sandbox root after canonicalization. The bootstrap fails before Wave import if any real `configfile.ini`, API key/token source, user home, repository runtime state, localhost service, or production database is reachable. Canary credential values are deliberately unique, and teardown asserts they occur nowhere in trace JSON, logs, exceptions, snapshots, or test output.

The network guard denies sockets, DNS, HTTP, WebSockets, and broker transports before argument formatting/logging. It traces only sanitized transport type and call site as `UNEXPECTED_NETWORK_ACCESS`. No replay may create a real Kite, ticker, monitored API, notification transport, or memcache client.

### 11.4 Process-boundary validation lane

Run a separate, non-golden subprocess lane to verify:

- the sandbox/bootstrap is established before imports in a fresh interpreter;
- separate processes do not share `common_lib` globals;
- SIGTERM, `atexit`, status cleanup, crash, restart, and duplicate-PID flows cross real process boundaries;
- imports and databases still remain inside the sandbox;
- fake broker objects and network denial remain installed.

This lane may validate production process/thread construction in explicitly non-deterministic smoke cases, but real-thread order is not compared byte-for-byte and never creates or updates golden traces. Decision/state/interleaving goldens come only from the cooperative oracle. Any behavior observed only in the process lane is reported separately as a boundary discrepancy.

### 11.5 Restart and recovery

Test two meanings of restart:

1. **Current legacy restart:** process globals disappear; broker orders/GTTs and persisted files remain; a new worker initializes mostly from broker positions and current config.
2. **Proposed corrected recovery:** an explicit recovery component reconstructs tracked order/GTT relationships before placing anything.

Legacy scenarios must cover:

- crash with two live regular orders;
- crash with one regular and one GTT;
- crash with two GTTs;
- restart after one orphan order filled;
- stale status file and dead PID;
- access-token/session failure;
- process restart at a different price;
- duplicate forced restart while the prior PID is still exiting;
- status deletion before/after `atexit`;
- active GTT with no process-local tag metadata.

The baseline should reveal duplicate-order or orphan-risk behavior rather than concealing it with recovery logic.

## 12. Multi-instance behavior

Run multiple isolated workers against one fake account and scheduler.

Required topologies:

- two different symbols, same underlying and expiry;
- CE and PE workers sharing an expiry;
- same symbol with two forced duplicate workers;
- NIFTY and BANKNIFTY;
- NFO and BFO;
- asymmetric buy/sell quantities;
- one worker with stale positions while another fills;
- one worker changing portfolio delta enough to reprice/restrict another;
- simultaneous callbacks to different worker processes;
- dashboard aggregation with live, dead, duplicate, and malformed status files.

Trace both:

- account-wide truth: positions, delta inputs, regular orders, and GTTs;
- per-worker perception: cached positions, config cache, tracked orders, cooldown, velocity, restrictions, and status.

The `cross_instance` configuration currently appears as a control-plane setting without matching runtime enforcement. Legacy mode must prove that behavior. Corrected mode may add an account-level coordinator, but its decisions must be explicit trace events and individually flag-gated.

## 13. Scenario matrix

Every row below expands into focused scenarios for CE and PE where direction matters, and for NIFTY/BANKNIFTY/SENSEX where exchange or delta behavior differs.

| Area | Minimum scenarios |
|---|---|
| Startup | valid regular start; asymmetric quantity; invalid symbol/gap/quantity; token failure; instrument missing; unqualified quote failure; duplicate 409; forced duplicate; pre-open; exact open; exact close; post-close; holiday; weekend |
| Bootstrap preflight | Wave import only after sandbox activation; fake Kite/MonitoredKite/ticker modules constructed; real config/home/repo paths denied; network/DNS/HTTP attempt emits sanitized `UNEXPECTED_NETWORK_ACCESS`; memcache disabled/hit/miss/error fixtures; SQLite/WAL/SHM under sandbox; canary secrets absent from every artifact; unload/reimport starts clean |
| Cooperative tasks | callback during simulated sleep; nested callback at yield; simultaneous callback/poll priority permutations; delayed five-second status refresh; timer fire/cancel/join timeout/liveness; direct `_thread` denial; no real OS thread; repeated task traces byte-identical |
| Initial state | flat; long option; short option; non-zero initial position; unrelated portfolio positions; current position changes during init; missing/stale instrument data |
| Exported actual days | auto-score then freeze distinct one-minute NIFTY range, trend, gap-open, and expiry/high-gamma days; exact selected dates/contracts, selector version/scores, dataset ID, and manifest hashes in every scenario |
| Contract selector | first completed NIFTY close after open; CE only; nearest expiry inside inclusive 5-20 calendar DTE; nearest valid strike with deterministic tie-break; positive synchronized option close; fail on no eligible contract; frozen exact symbol and metadata |
| Initial account | INR 1,000,000 cash; flat positions; no regular orders; no GTTs; one Wave worker; one date-effective exported lot |
| Initial settings | symmetric 10% option-premium gaps; current UI/saved step, delta, restrictions, and velocity settings frozen with hashes; same-side fill cooldown enabled from current config/default; regular orders; `broker_fee_model=upstox_approximation` |
| Initial replay model | `market_resolution=1minute`; `cool_off_mode=disabled` only for placement sleep; both legacy quote reads see `C_t`; `COOL_OFF_BYPASSED` trace; `trigger_mode=close_only`; `fill_price_mode=limit`; `equivalence_status=modeled_not_exact` |
| Bar timestamps | LEAN end-keyed row; candle start-keyed row; exact completion boundary; decision from `C_t`; no same-bar activation/fill; first eligible `C_(t+1)`; UTC/IST equivalence |
| Close-only triggers | neither close predicate; BUY-only close crossing; SELL-only close crossing; high/low-only touch ignored; overlapping/inverted bracket rejected; explicit malformed-order override; close-price sensitivity; undercount versus OHLC touch |
| OHLC sensitivity | neither touched; only BUY; only SELL; both siblings touched; `LOW_FIRST`; `HIGH_FIRST`; `CANCELLATION_LATENCY_STRESS`; GTT trigger/child; fill/cancel and fill/modify in one bar; O-L-H-C/O-H-L-C; seeded Brownian-bridge sensitivity; exclude; equivalent-state merge; branch-budget pruning |
| Contract lifecycle | weekly and monthly; `option_history_days` boundary; strike-window edge; missing contract; zero/invalid price; source/fixed lot-size mismatch; expiry final bar; roll to explicit new symbol |
| Data completeness | missing underlying; one held option missing; OI absent; declared missing interval; unexpected partition; archive hash mismatch; fail/skip/unknown policies; no option forward fill |
| Gap logic | first percentage initialization; lower/higher fill repricing; SELL minimum gap; zero/negative computed price clamp; tick-boundary floor; quote below/above last fill; movement during cool-off |
| Step multiplier | steps -10 through +10; configured and missing keys; beyond range; fractional step truncation; asymmetric quantity denominator; restart resetting initial position |
| Delta multiplier | below/at/above soft limit; cap; positive/negative delta; CE/PE direction; Greeks failure fallback; stale positions; quote-cache hit/miss; multiple expiries |
| Hard restrictions | below/at/above min/max; per-expiry versus total delta; missing expiry config/default; no expiry lookup; positive-position BUY restriction; NIFTY premium <=25 exception |
| Velocity | insufficient history; threshold equality; upward/downward crossing; CE/PE side mapping; old samples trimmed; REST spot failure; stale tick versus fresh quote; process-local histories |
| Same-side fill cooldown | remains enabled while placement cool-off is disabled; current/default values frozen; below threshold; exact trigger; repeated same-side fills; opposite-side fill; expiry/lift; callback lost then polling completion; restart clears cooldown; duplicate workers do not share cooldown |
| Regular placement | both succeed; SELL fails/BUY succeeds; SELL succeeds/BUY fails; both fail; retry succeeds on attempts 2-5; all retries fail; broker returns numeric/string IDs |
| GTT fallback | scripted characterization only for initial corpus: SELL-only GTT; BUY-only GTT; two GTTs; GTT create failure; market closes between legs; trigger `OPEN`; immediate `COMPLETE`; missed callback then poll; missing/wrong tag; delete failure; trigger/delete race |
| Partial fills | one partial; many partials; partial then complete; partial then cancel; partial then reject; both siblings partially fill; position visible before callback; average-price evolution |
| Callback order | OPEN/COMPLETE; COMPLETE-only; COMPLETE-before-OPEN; duplicate COMPLETE; sibling simultaneous fills; CANCELLED after local delete; callback while guard held; callback during cool-off; auto-reprice echo versus manual override |
| Polling | empty history; stale OPEN; COMPLETE; CANCELLED; REJECTED; history timeout; terminal cache; reconnect cache flush; GTT child visible only through `orders()` |
| Cancellation | success; callback-first; response-first; timeout-applied; timeout-not-applied; already-filled; all five attempts fail; sibling link detached; process stop then cancel |
| Repricing | delta-only change; velocity-only change; both; no price change; manual override; modify reject; fill race; missing multiplier anchor; GTT child not repriced |
| Persistence | executed-order success/failure/retry; duplicate terminal observation; malformed existing file; status write failure; partial JSON read; delayed status refresh; live-order overlay/injection |
| Market lifecycle | 09:14:59 to 09:15:00; 15:29:59 to 15:30:00; callback at 15:30; first poll after close; holiday cache; expiry day; DTE across IST midnight |
| Fees/execution | limit-price default; open-price improvement as a separate policy; touched-but-missed fill; seeded fill probability; reject any worse-than-limit execution; no bid/ask; resolved Upstox fee schedule; statutory date boundary; lot-size regime; P&L before/after costs |
| Restart/recovery | crash at each order/GTT state; stale status; active broker orders; active GTT; fill during downtime; restart duplicate; config changed while down |
| Multi-instance | shared delta reaction; stale per-process cache; forced same-symbol duplicate; status duplicate badge; attribution ambiguity; cross-instance setting non-enforcement |
| Worker isolation | two fresh worker module graphs have distinct globals/caches/locks; coordinator-shared FakeKite ledger; deterministic cross-worker event order; unload one worker without mutating another; subprocess boundary lane confirms process/global separation |
| Faults | network/data exception; timeout; rate limit; malformed response; missing required field; quote outage; positions outage with/without stale cache; disk full/permission error |
| Dry-run | regular calls blocked; GTT calls blocked; resulting local state and repeated main-loop attempts |

Add a scenario only when it has a distinct decision branch, race, state invariant, or data-fidelity assumption. Use parameterization for pure value boundaries.

## 14. Branch enumeration and coverage strategy

### 14.1 Reachability inventory

Generate the inventory from the exact baseline commit:

1. Parse `ticker_single_scraper_new.py`, the selected Flask endpoints, and callback registrations.
2. Traverse calls into `common_lib.py`.
3. Add dynamic edges for decorators, nested thread targets, callback assignment, dynamic imports, and persistence helpers.
4. Record external calls as adapter obligations.
5. Mark branches that require process exit, failure injection, callback interleaving, or file corruption.
6. Review the manifest manually against the production flow.

The initial static inventory should be treated as a lower bound; runtime tracing may discover additional dynamic edges.

### 14.2 Coverage requirements

For the accepted baseline:

- 100% statement and branch coverage for the Wave-reachable manifest, excluding only documented defensive branches proven impossible under the Kite schema;
- every excluded branch has a reason, reviewer, and fixture gap note;
- every broker method outcome has at least success, terminal business failure, and transport failure coverage;
- every state transition in the regular and GTT models is covered;
- every correction flag has at least one scenario where it changes the trace and one where it does not;
- callback permutation coverage uses pairwise generation first, followed by exhaustive orderings for safety-critical races;
- condition-boundary tests cover equality around market times, delta limits, velocity threshold, cooldown count/time, price 0/0.05/25, and multiplier caps.

Coverage numbers alone are not sufficient. Maintain a requirement-to-scenario-to-trace matrix.

### 14.3 Invariants

Check invariants after every scheduler event, but report rather than alter legacy state:

- broker filled quantity never exceeds requested quantity;
- broker position changes equal fills;
- terminal broker states are monotonic;
- each tracked regular sibling relation is either reciprocal or explicitly in a transitional GTT state;
- no unknown broker call escapes the fake;
- no live network or wall-clock sleep occurs;
- all trace timestamps are monotonic by scheduler sequence;
- every strategy action has a causal input;
- every accepted broker action eventually has a scripted or modeled result;
- final economic ledger reconciles cash, fees, fills, and positions.

Legacy invariant violations become trace events and issue-catalog entries. They must not abort golden generation unless continuing would be undefined.

## 15. Golden traces

### 15.1 Creation

Create goldens only after:

1. the scenario has pinned data/config;
2. the trace is deterministic across repeated runs;
3. the observed action sequence has been compared with logs or a direct execution capture where available;
4. volatile fields are normalized without hiding behavioral values;
5. a reviewer approves the behavior as a valid legacy observation.

Store:

- canonical JSONL trace;
- normalized final state JSON;
- short Markdown explanation for complex scenarios;
- source fixture hashes;
- baseline commit SHA and environment metadata.

### 15.2 Comparison levels

Classify trace fields:

- **Exact:** event kind/order, side, quantity, normalized IDs, status, restriction, multiplier inputs/outputs, price after legacy rounding, simulated time.
- **Tolerance:** Greeks and P&L floating-point values only, with named absolute/relative tolerances.
- **Set-equivalent:** only where the external API explicitly has no order guarantee.
- **Ignored:** log prose, real PID, absolute temporary path, and non-causal formatting noise.

Do not ignore callback order, broker-call order, timestamps that control caches/cooldowns, or status omission.

### 15.3 Approval and updates

Golden updates must be a deliberate command that produces:

- old/new trace diff;
- affected scenarios;
- baseline SHA change;
- classification as expected production change, correction flag, fixture correction, or harness defect.

CI must never auto-accept goldens.

### 15.4 Actual-data replay results

Keep actual Upstox/LEAN market-day results adjacent to, but distinct from, broker-characterization goldens:

- the result names the approved initial-default profile and frozen representative-day/ATM-CE selection manifest;
- the trace header includes exact contract metadata, date-effective lot size, initial INR 1,000,000 cash, flat/no-order/no-GTT state, one-lot quantity, 10% gap inputs/resolved points, regular-order mode, and hashes of every resolved saved/default Wave setting;
- the initial result uses `cool_off_mode=disabled`, `trigger_mode=close_only`, and `fill_price_mode=limit`; it is golden only for that declared model and is never labeled exact production timing/execution;
- same-side fill cooldown state/config remains visible independently of disabled placement cool-off;
- economic output declares `broker_fee_model=upstox_approximation` and explicitly disclaims Zerodha fee fidelity;
- the report states that close-only triggering ignores intraminute touches and normally undercounts fills;
- a separate `close_price` fill sensitivity may be shown, never substituted into the default result;
- later `ohlc_touch_branched` sensitivity forks or terminates at each ambiguous bar according to the selected policy;
- post-ambiguity fills, positions, delta, fees, and P&L are conditional outcomes, not a single broker-truth golden;
- the result index records dataset/manifest hash, scenario/config hashes, source bar IDs, missing-quote decisions, ambiguity counts, branch weights (if any), and outcome ranges;
- the close-only baseline, unambiguous OHLC-touch trades, and ambiguous branched outcomes/P&L/exposure dispersion are reported in separate blocks;
- no single headline P&L is emitted without naming `cool_off_mode`, trigger mode, fill-price mode, ambiguity policy, and whether branch pruning made it approximate;
- scripted broker events layered onto an actual day remain golden only for the script semantics, not evidence that the real historical broker emitted those events.

This separation prevents actual market-path evidence from being overstated while still making it a required regression corpus.

## 16. Differential testing

### 16.1 Comparator contract

Feed the same scenario, fake broker semantics, clock, initial state, and input events to two runners. Compare in this order:

1. decision events;
2. broker requests;
3. local and broker state transitions;
4. terminal positions and open orders/GTTs;
5. economic results.

Stop at the first causal divergence and show:

- last matching event;
- first differing decision;
- relevant state slice;
- multiplier/restriction inputs;
- callback/poll visibility;
- source function or adapter component;
- downstream economic delta.

### 16.2 Clean-core migration

After legacy goldens pass:

1. Define a clean state object and ports without changing legacy mode.
2. Reimplement one decision slice at a time, starting with pure gap/multiplier calculations.
3. Run every scenario against legacy and clean runners.
4. Preserve exact parity by default.
5. For an intentional fix, enable one correction flag and approve its golden delta.
6. Move orchestration only after regular/GTT state-machine parity is proven.
7. Keep the legacy runner in CI until production has run the clean core through a defined observation period.

Never validate a rewrite only with end-of-day P&L.

### 16.3 LEAN integration

Add LEAN only after the fake-broker and clean-core gates pass:

- consume the same `wave-replay-dataset-v1` manifest or prove a hash-equivalent reconstruction from the declared LEAN archives; do not select a different historical universe silently;
- map Wave symbols/instrument metadata into LEAN securities;
- implement or configure an Indian options brokerage model for lot size, tick size, market hours, holidays, fees, and supported order types;
- model Zerodha GTT as a custom coordinator because it is not a normal resting exchange order;
- preserve `bar_start`, `bar_end`, and availability semantics so LEAN consolidation cannot introduce lookahead;
- accept only synchronized one-minute underlying and option bars for historical comparison; reproduce the initial `cool_off_mode=disabled` plus `close_only` baseline first, then run the separate `ohlc_touch_branched` sensitivity with the same missing-quote policies;
- map LEAN order events into the canonical trace schema;
- keep Wave's 180-second poll and REST-lag scenarios in the external harness even if LEAN normally emits synchronous engine events;
- compare decisions before comparing fills, because LEAN fill-model differences can be legitimate and must be isolated;
- report unsupported Zerodha semantics rather than approximating them silently.

LEAN results are `lean_comparison`, not proof of legacy parity, until the canonical decision trace passes.

## 17. Datasets and capture plan

### 17.1 Export boundary and commands

Add an exporter to `indian-wheel`; do not teach `ui-trading-system` to understand Upstox raw JSON, LEAN zip member naming, price scaling, universe CSVs, or `lean_data_name`.

Proposed producer command:

```powershell
# Run in indian-wheel (exporter implementation: commit 73dcb4c).
python -m backtesting_lean.cli export-wave-replay `
  --data-config config\data\nifty-bear-call-spread-1minute.json `
  --start 2025-01-27 `
  --end 2025-02-07 `
  --underlying NIFTY `
  --output data\exports\wave-replay\nifty-bcs-1m-20250127-20250207-v1 `
  --format parquet
```

The exporter resolves:

1. `config/data/<name>.json`;
2. the selected `data/lean` or `data/<lean_data_name>` root;
3. underlying, option trade, OI, universe, symbol-properties, and market-hours inputs;
4. source raw/quality manifests where available;
5. timestamp convention and actual resolution;
6. resolved fee schedule;
7. complete archive/member hashes.

It writes to a new directory and fails if that `dataset_id` already exists with different content. Generated exports are gitignored. Distribution can use a checksummed archive, CI artifact, or explicit local copy; no consumer code may substitute an absolute sibling path.

Proposed consumer commands:

```powershell
# Run in ui-trading-system after copying/downloading the exported directory.
python -m wave_replay.cli validate-dataset `
  --dataset data\wave-replay\nifty-bcs-1m-20250127-20250207-v1

python -m wave_replay.cli replay `
  --scenario tests\fixtures\wave_replay\scenarios\nifty_expiry_range.yaml `
  --dataset data\wave-replay\nifty-bcs-1m-20250127-20250207-v1 `
  --mode legacy_equivalent `
  --ambiguity-policy branch_all
```

The replay command records the dataset ID and manifest hash into every output trace. A scenario cannot silently fall back to synthetic bars when the declared dataset is absent.

### 17.2 Immutable manifest

The accepted producer contract at `indian-wheel` commits `73dcb4c`, `eb29b32`, and provenance fix `ef3a590` defines:

- `schema`, semantic `schema_version`, content-derived immutable `dataset_id`, and canonical `manifest_sha256`;
- `producer.commit` and `producer.implementation_commit` for exporter implementation provenance;
- `source.repository_commit` and `source.data_source_commit` for the data-config/source checkout;
- `identity.implementation_commit` and `identity.data_source_commit`, both included in content-derived dataset-ID recomputation;
- `request` data config/hash, resolved data/raw roots, underlying, and date range;
- `data_contract` resolution, start/end availability, no-forward-fill rule, Wave symbol classification, replay-token origin, source/resolved multiplier policy, source-preferred lot-size policy, and `upstox_approximation` fee label;
- `tables` paths, exact Arrow columns, and row counts;
- `source_files` archive/member hashes;
- `files`, a hash/size inventory of the six non-manifest outputs;
- `declared_files`, the exact seven-file directory inventory including `manifest.json`.

The exact outputs are `underlying_bars.parquet`, `option_bars.parquet`, `contracts.parquet`, `sessions.parquet`, `fee_schedule.json`, `validation_report.json`, and `manifest.json`. The manifest itself is canonicalized and hashed after omitting its own hash field. Dataset versioning is content-immutable: changing a source/config/archive, converter logic, timestamp normalization, fee inputs, or output row creates a new dataset ID and manifest hash.

### 17.3 Export validation gates

The producer and consumer independently validate:

- all declared files exist and match hashes;
- no undeclared Parquet file or partition is loaded;
- unique contract identity is stable across bars;
- OHLC is finite, non-negative where valid, and `low <= open/close <= high`;
- `bar_start < bar_end <= available_at`;
- UTC and IST timestamps represent the same instant;
- row spacing matches declared resolution or is listed as a missing interval;
- bars are within the declared session/date and contract lifecycle;
- no option bar appears after expiry/verified last availability;
- start-keyed candle rows and end-keyed LEAN events normalize to identical availability semantics;
- no duplicate symbol/timestamp rows survive without an explicit source-duplicate policy;
- contract multiplier/style/right/strike/expiry agree across trade, OI, universe, and symbol-property sources;
- all held options selected by an end-to-end scenario have synchronized underlying coverage or a declared missing-quote result;
- NIFTY zero volume remains zero;
- option OI remains null when no source OI archive exists;
- actual min/max timestamps, counts, missingness, and source hashes match the manifest.

Validation produces a machine-readable report consumed by the replay result. A run with validation errors is invalid, not partially successful.

### 17.4 Market-day corpus selection

After inventorying exact coverage, run the versioned selector in section 10.3 to propose and then freeze exactly four distinct NIFTY dates:

- a range-bound NIFTY day with repeated level touches;
- a directional/trend day that stresses one-sided fills, velocity, and delta widening;
- a gap-open day;
- the eligible nearest-expiry/high-gamma day.

For each day, select one ATM CE with inclusive 5-20 calendar DTE using the first completed NIFTY close after open, then freeze the exact symbol and all contract metadata in the scenario manifest. Each selection records why it was chosen, selector scores/tie-breaks, candidate exclusions, first underlying and option closes, expiry/DTE/strike/right, date-effective lot size, required strike window, and actual missingness. Both underlying and selected option bars must be verified at one-minute resolution. Do not substitute another right, expiry outside the DTE window, another underlying, or any coarser data; an uncovered role remains unsupported and fails corpus construction.

The initial actual-data corpus has four single-instance scenarios, each starting from the approved account/settings profile in section 10.3. Missing/illiquid-data behavior remains covered by focused validation and scripted fixtures; it does not replace one of the four representative baseline days.

### 17.5 Broker captures and scripted fixtures

Market exports are paired with:

- small hand-authored broker/fault JSONL scripts for complete branch and race coverage;
- sanitized production broker captures where available, including callbacks, histories, positions, and REST lag;
- Wave config, delta limits, starting positions/orders/GTTs, status/executed-order state, and latest-spot bridge inputs;
- deterministic seeds and expected trace references.

Redact credentials, account identifiers, and unrelated trades. Preserve order relationships with stable pseudonyms, retain raw timestamps/known skew, and hash all files. Never merge captures from different sessions without an explicit composition manifest.

### 17.6 Cross-repository ownership and sequencing

Implement as exactly two coordinated repo-specific branches/PRs:

1. **`indian-wheel` exporter PR:** define `wave-replay-dataset-v1`, read actual Upstox/LEAN/config/manifest inputs, normalize timestamps, export Parquet/JSON/fee schedule, validate provenance, and publish one checksummed sample dataset artifact.
2. **`ui-trading-system` simulator/consumer PR:** after the exporter artifact is available, pin supported schema versions, load and validate portable exports, auto-select/freeze the initial NIFTY corpus, convert bars to scheduler events, run the legacy Wave worker, and report ambiguity/missingness.

Sequence:

1. Review and freeze schema v1 with representative rows and manifest before either implementation depends on it.
2. Implement and merge, or otherwise make available for review, the `indian-wheel` exporter branch/PR first.
3. Produce and validate the sample actual-data export.
4. Only then implement the `ui-trading-system` simulator/consumer branch/PR against the artifact, not the producer checkout.
5. Add compatibility tests using the schema example plus an end-to-end actual export.
6. Treat incompatible schema changes as a new major version; keep older consumers and datasets reproducible.

The producer owns source-format changes. The consumer owns replay semantics. Neither repository reaches into the other's internal Python modules at runtime.

## 18. Phased implementation and estimates

Estimates assume one engineer familiar with Python, Kite Connect, and the repository. Real broker capture availability can change the calendar.

| Phase | Deliverables | Owner | Estimate | Exit gate |
|---|---|---|---:|---|
| 0. Freeze and inventory | Wave baseline SHA/reachability/issue catalog plus exact `indian-wheel` config, root, archive, contract, resolution, date-range, missingness, multiplier, fee, and timestamp inventory | Both repos | 3-5 days | Inventory reviewed; unavailable history is explicit; no rewrite started |
| 1. Exporter branch/PR | Freeze `wave-replay-dataset-v1`; implement the `indian-wheel` export command, Parquet tables, provenance, date-effective lot sizes, Upstox fee schedule, selector inputs, and hash/partition validation | `indian-wheel` | 7-10 days | Exporter PR is available first; one immutable one-minute NIFTY export passes producer validation |
| 2. Simulator/consumer foundation | Start the dependent `ui-trading-system` PR; implement dataset consumer, pre-import sandbox, fake modules, fail-closed network, deterministic memcache, isolated SQLite/files, `SimTaskRuntime`, fresh worker namespaces, and Gate 0 | `ui-trading-system` | 10-14 days | Artifact compatibility and Gate 0 pass; no real thread/network/production path; no golden may yet be approved |
| 3. Legacy characterization shell | Cooperative existing-code runner, FakeKite/FakeTicker, SimClock, startup/duo/callback/task permutations, subprocess boundary lane, first candidate traces | `ui-trading-system` | 6-9 days | Gate 0 remains green; task/interleaving traces are byte-identical; current code remains oracle; first goldens may be reviewed |
| 4. Initial NIFTY corpus selection | Auto-score verified dates for range/trend/gap-open/expiry-high-gamma roles; select one 5-20 DTE ATM CE from first completed close; freeze four scenario manifests and resolved current settings | `ui-trading-system` | 3-5 days | Four distinct dates/contracts pass review; no relaxed DTE/right/resolution fallback; all settings and hashes frozen |
| 5. Initial actual-market Wave days | Run one regular-order Wave instance per frozen NIFTY ATM CE with flat INR 1,000,000 account, one date-effective lot, symmetric 10% gaps, enabled same-side fill cooldown, disabled placement cool-off, close-only triggers, limit fills, and Upstox fee approximation | `ui-trading-system` | 5-8 days | Four baseline runs declare all approved defaults, data quality, fill undercount caveat, fee approximation, and `modeled_not_exact` status |
| 6. OHLC-touch sensitivity | Re-run the same actual days with `ohlc_touch_branched`, LOW_FIRST/HIGH_FIRST, optional cancellation-latency stress, state merging, and branch budgets | `ui-trading-system` | 5-8 days | Close-only versus OHLC comparison and conditional outcome dispersion are reported separately |
| 7. Fake regular broker and scripted fault matrix | Orders, positions, partial fills, rejects, cancel/modify races, callback permutations, visibility lag, API failures layered over actual or minimal market paths | `ui-trading-system` | 7-10 days | Regular-order transition and reconciliation matrix passes |
| 8. Scripted GTT and lifecycle fidelity | GTT state machine, child tags, reconnect, market close, restart/crash, and persistence failures outside the initial natural historical baseline | `ui-trading-system` | 7-10 days | GTT, recovery, and close-boundary characterization goldens approved |
| 9. Risk and multi-instance fidelity | Step/delta/velocity/fill-cooldown/repricing, synchronized portfolio bars, cooperative worker namespaces, shared broker truth, and subprocess boundary checks | `ui-trading-system` | 7-10 days | Current-setting boundaries, missing-quote policies, worker isolation, and multi-worker races covered |
| 10. Golden corpus and coverage closure | Actual-day result set, sanitized broker captures, scripted scenarios, reachable-branch closure, CI artifact workflow | Both repos | 5-8 days | Cooperative legacy oracle and modeled actual-data replay corpus approved |
| 11. Corrected mode | Individual correction flags and reviewed golden deltas | `ui-trading-system` | 5-10 days | Each fix isolated, reversible, and differentially tested |
| 12. Clean core | Incremental extraction/reimplementation behind ports | `ui-trading-system` | 10-20 days | Full scenario parity or approved flag-specific deltas |
| 13. LEAN adapter | Reuse exported data/schema, map brokerage/GTT behavior, canonical traces, close-only baseline, and OHLC sensitivity | Both repos | 10-15 days | LEAN comparison report passes decision and data-provenance gates |

Expected effort:

- Exporter, historical consumer, legacy characterization harness, and approved oracle: roughly 8-12 engineer-weeks across the two repositories.
- Corrected mode plus clean-core migration: an additional 3-6 weeks.
- LEAN integration: an additional 2-3 weeks after the portable data boundary exists.

The implementation order is strict: exporter branch/PR first, validated artifact second, simulator/consumer branch/PR third. Within the consumer PR, candidate characterization traces may be inspected after Gate 0, but **no golden approval begins until hermetic bootstrap and cooperative-task determinism pass**. Do not begin the clean rewrite before both golden scenarios and the four approved actual-day runs pass.

## 19. Verification gates

### Gate 0: Pre-import hermetic and cooperative preflight

This gate runs before any golden-producing characterization:

- bootstrap is active before the first Wave-reachable import;
- source/config/runtime paths resolve only beneath the scenario sandbox;
- scenario-owned fake credentials and explicit `live_trading` config are used; real config, home, tokens, API keys, databases, status, and logs are unreachable;
- `kiteconnect`, `MonitoredKite`, ticker, memcache, notification, and transport objects imported or constructed by Wave are approved fakes;
- socket, DNS, HTTP, WebSocket, and unregistered transport probes fail closed and emit a sanitized `UNEXPECTED_NETWORK_ACCESS` without arguments or secrets;
- memcache behavior is scenario-pinned and no localhost probe occurs;
- instrument/API-monitor/trade SQLite files and any WAL/SHM/journal files are created only under the sandbox;
- `threading.Thread`, `threading.Timer`, ticker callbacks, delayed refresh, and all Wave-reachable task starts resolve to `SimTaskRuntime`; direct `_thread`/executor creation fails;
- the host real-thread inventory is unchanged across import and execution;
- a filesystem creation ledger shows no production path touched;
- canary secrets are absent from traces, logs, snapshots, exceptions, and test output;
- teardown unloads worker modules and a second import starts with fresh globals.

Any failure blocks Phase 2 characterization and all golden approval.

### Gate A: No accidental rewrite

- Characterization tests execute the current decision functions.
- Production order logic is not copied into the fake.
- The fake determines fills and API responses, not whether Wave should place an order.

### Gate B: Hermetic execution

- Network access is blocked.
- Real credentials and live state are unreachable.
- No real sleeps or host-time decisions occur.
- No deterministic callback, timer, ticker handler, or delayed refresh executes on an OS thread.
- Temporary state is isolated and removed after tests.

### Gate C: Portable data integrity

- The replay loads only a `wave-replay-dataset-v1` export, never a sibling checkout.
- Producer and consumer independently verify config, archive, output, and manifest hashes.
- Exact date range, contracts, resolution, counts, lot sizes, missingness, timestamp conversions, and declared partitions reconcile.
- Every historical replay partition has synchronized one-minute underlying and option bars; all non-60-second partitions are rejected.
- Start/end/availability-time tests prove no bar is visible before completion.
- Initial-mode tests prove both legacy quote reads use completed close `C_t`, cool-off is bypassed and traced, an order from `C_t` cannot fill on bar `t`, and its first eligible close is `C_(t+1)`.
- Corpus selection tests deterministically score four distinct NIFTY roles, use the first completed close after open, enforce CE and inclusive 5-20 calendar DTE, choose the nearest expiry/strike with documented tie-breaks, and fail rather than relax a constraint.
- Every approved scenario freezes the exact symbol/contract/lot-size metadata, selector result, current resolved Wave settings, and source hashes.
- NIFTY zero volume, absent OI, absent contracts, and missing option bars remain explicit rather than fabricated or forward-filled.

### Gate D: Determinism

- Repeated runs have byte-identical canonical traces and final-state hashes.
- Seed and same-timestamp ordering are visible in reports.
- A failing run can be reproduced from its bundle alone.
- callback-during-sleep, nested callback, simultaneous callback/poll, delayed refresh, timer, join, and liveness fixtures are byte-identical;
- golden traces are generated only by the cooperative runtime; subprocess lane results are separately labeled and non-golden.

### Gate E: Broker fidelity

- Regular and GTT transition matrices pass.
- Partial fills, failures, races, callbacks, REST lag, and polling reconciliation are covered.
- Position and cash ledgers reconcile after every fill.

### Gate F: Legacy branch completeness

- The reviewed reachability manifest is fully covered or explicitly waived.
- Scenario matrix and branch IDs are cross-linked.
- Known quirks have approved characterization traces.

### Gate G: Golden and historical credibility

- Four single-instance Wave replays run end to end on the frozen one-minute NIFTY range, trend, gap-open, and expiry/high-gamma scenarios.
- Each replay uses one ATM CE with 5-20 calendar DTE, flat INR 1,000,000 account, no starting orders/GTTs, one date-effective lot, symmetric 10% gaps, regular orders, current frozen step/delta/velocity settings, enabled same-side fill cooldown, `cool_off_mode=disabled`, `trigger_mode=close_only`, `fill_price_mode=limit`, and `equivalence_status=modeled_not_exact`.
- The first report quantifies close-cross fills, missing contracts/bars, unknown quotes, and provenance, and states that ignored high/low touches can undercount fills.
- Economic reports state `broker_fee_model=upstox_approximation` and do not imply Zerodha fee fidelity.
- Later sensitivity reports quantify unambiguous OHLC touches, ambiguous/touched/excluded bars, LOW_FIRST/HIGH_FIRST/latency-stress branches, state merges/pruning, branch count/budget, outcome range, and P&L/exposure dispersion.
- Reports never present a single headline P&L without cool-off, trigger, fill-price, ambiguity, and pruning policies.
- A failure/reconnect session is backed by a sanitized real capture where available or an explicit scripted broker stream.
- Hand-authored fixtures and captured sessions agree on shared semantics.
- Goldens cannot be updated automatically.

### Gate H: Differential readiness

- Comparator finds seeded mutations in side, price, quantity, timing, restriction, callback handling, and GTT linkage.
- Reports identify the first causal divergence.
- Correction flags produce only their approved deltas.

### Gate I: LEAN readiness

- Clean core passes the non-ambiguous tick/recorded-event corpus.
- Clean core also passes the exported actual-market decision corpus up to each declared ambiguity boundary.
- Brokerage, exported fee schedule, market-hours, timestamp availability, and GTT translations are documented.
- Bar-data ambiguity is reported as an envelope, not hidden.

## 20. Risks and tradeoffs

| Risk | Mitigation |
|---|---|
| The fake broker encodes assumptions that differ from Zerodha | Prefer recorded events; version semantics; keep response/callback scripts; validate against captured sessions |
| Import-time side effects occur before ordinary monkeypatching | Establish `ReplayBootstrap`, fake `sys.modules`, network denial, source/config sandbox, time, and task seams before importing any Wave-reachable module |
| A Wave callback/thread escapes cooperative scheduling | Patch Thread/Timer/ticker delivery, deny direct `_thread`/executors, compare host thread inventory, and block Gate 0 on any real spawn |
| Fresh worker namespaces accidentally share `common_lib` globals | Assert object identity isolation, dispatch through a worker-scoped import router, and cross-check with the separate subprocess boundary lane |
| Cooperative execution hides process-boundary defects | Keep deterministic goldens in the cooperative oracle and separately test fresh-process imports, signals, `atexit`, crash, and restart without treating OS scheduling as golden |
| Sandbox config or exception output leaks credentials | Use canary-only fake credentials, sanitize network traces before arguments are formatted, and scan every artifact/output at teardown |
| SQLite side files escape configured database paths | Resolve DB paths before import, ledger every open/write, and require database/WAL/SHM/journal cleanup under the sandbox |
| Exact callback timing is unavailable historically | Model visibility windows and permutations; label inferred results; do not claim exact equivalence |
| OHLC creates false certainty | Fork LOW_FIRST and HIGH_FIRST Wave states, optionally stress cancellation latency, and keep deterministic/Brownian paths secondary rather than observed truth |
| Same-bar branching grows exponentially | Hash and merge equivalent states, enforce a deterministic budget, retain declared adverse/best-known representatives, and label pruned results bounded/approximate |
| One-minute bars cannot reproduce the 10-second cool-off | Disable cool-off in the initial mode, trace that divergence, and require captured tick/second data for exact production cool-off equivalence |
| Close-only triggering misses intraminute touches | Label the expected fill undercount and compare against a separately reported OHLC-touch branched sensitivity |
| Direct sibling-checkout coupling makes tests non-portable | Export an immutable Parquet/JSON artifact with a stable schema; reject absolute/sibling fallback paths |
| LEAN start/end timestamp confusion introduces lookahead | Export start, end, and availability explicitly; gate quote visibility and same-bar eligibility on availability |
| Upstox expired-contract, strike-window, and history-day limits omit needed contracts | Inventory first; fail/skip/mark unknown explicitly; never synthesize or forward-fill an option |
| One-minute NIFTY history is narrower than underlying history | Choose dates from verified overlapping coverage and publish exact actual ranges/counts in the manifest |
| Historical lot-size regimes conflict with fixed conversion config | Export source and resolved multipliers; pin scenario quantity translation and reject undeclared mixed regimes |
| Trade bars have no bid/ask or queue depth | Version slippage assumptions, report bar-inferred fills, and avoid pretending volume is executable depth |
| NIFTY index volume is zero | Preserve zero; Wave does not use volume; require an explicit NIFTYBEES proxy only for future volume-dependent behavior |
| Cross-repository schema changes break old replays | Immutable major versions, producer/consumer compatibility tests, and pinned manifest/schema hashes |
| Golden traces bless a bug | Label quirks, require review, and maintain a separate corrected result set |
| Goldens become brittle due to noise | Canonicalize only non-behavioral fields; preserve causal order and values |
| Greeks differ across libraries or platforms | Pin backend/version/config and use narrow numeric tolerances; compare decision thresholds explicitly |
| Real capture contains sensitive account data | Redact and pseudonymize locally; include only Wave-relevant records |
| Shared executed-order/status files create nondeterministic writes | Schedule file operations, use isolated roots, and trace write ordering |
| Multi-instance runs explode the interleaving space | Use targeted exhaustive safety races, pairwise generation elsewhere, then captured schedules |
| LEAN cannot represent Zerodha GTT/polling semantics directly | Keep a custom brokerage/coordinator layer and report unsupported mappings |
| Fidelity work delays a clean architecture | Time-box phases with gates; extraction begins immediately after the oracle is credible, not before |

The primary tradeoff is speed versus confidence. A quick clean rewrite may look simpler, but without a legacy oracle it cannot distinguish improvement from accidental divergence. The plan deliberately spends effort first on observability, event ordering, and broker truth.

## 21. Explicit definition of done

The historical replay/simulation effort is complete only when all of the following are true:

1. A reviewed manifest lists every Wave-reachable function, branch, external call, callback, thread target, and persistence side effect from the frozen baseline.
2. Gate 0 proves the temporary source/config/runtime sandbox, environment, fake import modules, network denial, deterministic task/time seams, and SQLite paths are established before any Wave-reachable import.
3. Scenario-owned `configfile.ini` uses fake canary credentials and explicit live-trading mode; real credentials, home, repository runtime state, databases, status, and logs are unreachable.
4. All socket/DNS/HTTP/WebSocket/broker transports fail closed; `UNEXPECTED_NETWORK_ACCESS` is sanitized; memcache is scenario-pinned; all files including SQLite WAL/SHM/journals remain beneath the sandbox; no canary secret appears in any artifact.
5. `SimTaskRuntime` intercepts Thread, Timer, ticker callbacks, delayed refresh, joins, liveness, and direct/unregistered task starts before import; no deterministic Wave task executes on an OS thread.
6. Callback-during-sleep, nested callback, simultaneous callback/poll, delayed refresh, timer fire/cancel, join timeout, and liveness scenarios have byte-identical task/strategy traces.
7. Every simulated worker has a fresh module graph and distinct process-local globals/caches/locks; coordinator-owned `FakeKite` is the only shared broker truth.
8. Golden decision/state/interleaving traces come only from the cooperative oracle. A separate non-golden subprocess lane validates fresh imports, process/global boundaries, signals, `atexit`, crash, shutdown, and restart.
9. Teardown unloads/reimports workers cleanly, restores host state, removes the sandbox, and leaves no production or temporary runtime artifact behind.
10. The unmodified legacy decision path runs hermetically against narrow fake Kite/ticker adapters, simulated clock, and cooperative tasks.
11. The work is delivered as two coordinated repo-specific branches/PRs: the `indian-wheel` exporter first, followed by the `ui-trading-system` simulator/consumer against its artifact.
12. `indian-wheel` has a reviewed exporter for `wave-replay-dataset-v1` that reads actual configured Upstox/LEAN roots, including isolated `lean_data_name` roots, without exposing source-format logic to the consumer.
13. At least one immutable, gitignored one-minute NIFTY option/underlying export includes Parquet data, JSON manifest, session calendar, contract metadata, date-effective lot sizes, Upstox fee schedule, source/config/archive hashes, exact date/resolution/count inventory, and validation report.
14. `ui-trading-system` consumes that export by path/artifact and manifest hash without importing sibling modules or traversing to a sibling checkout.
15. Timestamp tests prove LEAN end-time and candle start-time sources share a no-lookahead `available_at` convention, and orders created from `C_t` cannot fill on bar `t`.
16. A versioned selector automatically proposes four distinct verified one-minute NIFTY dates for range, trend, gap-open, and expiry/high-gamma roles; its formulas, scores, candidate exclusions, ties, version, and source hashes are persisted.
17. Each selected date uses the first completed NIFTY close after open, CE right, nearest expiry inside inclusive 5-20 calendar DTE, and nearest valid strike with deterministic tie-breaks. Missing eligibility fails rather than relaxing DTE, right, resolution, or underlying.
18. Review-approved scenario manifests freeze the exact selected date, role, underlying close/time, symbol, instrument ID, expiry, DTE, strike, right, option close/time, date-effective lot size, selector/config/data hashes, and fully resolved Wave settings.
19. Four initial actual-data runs each use one Wave instance and one frozen ATM CE contract.
20. Each initial account starts with INR 1,000,000 cash, a flat portfolio, no open regular orders, no active GTTs, and quantity equal to one date-effective lot.
21. Each initial run uses BUY and SELL gaps equal to 10% of the selected option premium, current UI/saved step/delta/restriction/velocity settings, and same-side fill cooldown enabled from current config/defaults; all inputs and resolved values are hashed in the scenario.
22. The initial actual-data baseline uses regular orders. GTT behavior remains covered by scripted characterization/fault scenarios and is not required to arise naturally in those four days.
23. Each initial run declares `cool_off_mode=disabled`, `trigger_mode=close_only`, `activation_policy=subsequent_bar`, `fill_price_mode=limit`, and `equivalence_status=modeled_not_exact`.
24. Each economic report declares `broker_fee_model=upstox_approximation`, uses the exported Upstox schedule, and explicitly does not claim Zerodha fee fidelity.
25. Portfolio-delta decisions identify synchronized source bars for every held option and spot; missing option bars are never forward-filled.
26. NIFTY zero volume remains source-zero, and no synthetic volume or undeclared NIFTYBEES proxy enters legacy behavior.
27. The harness replays deterministic option/spot availability events, callbacks, REST snapshots, errors, process events, and file events without live network, wall-clock sleeping, or uncontrolled concurrency.
28. Regular orders, GTTs, child orders, partial fills, rejects, timeouts, retries, cancellation/modify races, and callback/polling reconciliation are modeled through a broker/fault stream separate from historical bars.
29. Initial close-only timing is explicit: both legacy quote reads see synchronized completed `C_t`, placement cool-off is bypassed and traced, new orders become eligible only from `C_(t+1)`, and high/low never drives this mode; same-side fill cooldown remains enabled.
30. `close_only` fills BUY only on a later close at/below its limit and SELL only on a later close at/above its limit, rejects overlapping/inverted brackets by default, fills at the limit, and reports optional close-price sensitivity separately.
31. A later `ohlc_touch_branched` run forks independent `LOW_FIRST` and `HIGH_FIRST` Wave/broker states, with optional `CANCELLATION_LATENCY_STRESS`; deterministic O-L-H-C/O-H-L-C and seeded Brownian-bridge runs remain secondary sensitivity analyses.
32. Branch state hashing/merging, budget, deterministic pruning, and approximate/bounded labeling are implemented, and all-low-first/all-high-first paths are not called exact global bounds without proof.
33. No limit executes worse than its limit, and execution uncertainty is represented by missed/probabilistic fills or scripted broker behavior.
34. Market hours, holidays, expiry/DTE, contract roll/lifecycle, tick-size behavior, disabled placement cool-off, 180-second polling, delayed refresh, and market-close shutdown are deterministic.
35. Step multipliers, percentage gaps, portfolio-delta multipliers, per-expiry hard restrictions, velocity, same-side fill cooldown, repricing, low-premium exception, and asymmetric quantities have boundary scenarios.
36. Crash, restart, stale status, orphan regular order, orphan GTT, missed callback, and duplicate-instance scenarios have approved outcomes.
37. Status files, executed-order persistence, dashboard overlay/injection, notification side effects, exported fee inputs, execution assumptions, positions, and P&L are represented in canonical traces or explicitly scoped as analytics.
38. Every deterministic scenario produces a byte-identical canonical trace across repeated runs.
39. All Wave-reachable branches are covered or have a reviewed, specific waiver, and known implementation issues are characterized as legacy behavior before any correction is enabled.
40. An approved legacy golden corpus includes modeled actual market-path days, scripted failure/reconnect/GTT, and multi-instance behavior, using sanitized real broker captures where available.
41. Actual-data results present the close-only baseline separately from OHLC-touch sensitivity and ambiguous branched outcomes, including ambiguity counts, branch count, merge/prune statistics, outcome ranges, and P&L/exposure dispersion; no single headline P&L appears without cool-off, trigger, fill-price, fee, and ambiguity policies.
42. Differential tests detect seeded strategy, data-availability, and orchestration divergences and report the first causal mismatch.
43. `legacy_equivalent`, individually flagged `corrected`, and `lean_comparison` outputs are separate and clearly labeled; no correction becomes default without reviewed trace deltas.
44. No clean core or LEAN implementation is considered equivalent based only on aggregate fills or P&L.

The first implementation milestone is therefore a characterization test around the existing code, not a rewrite. The first cross-repository milestone is a portable, validated actual-data export, not a runtime link between checkouts. The first durable result is the combined legacy golden characterization corpus and quantified actual-market replay corpus; the clean core and LEAN adapter follow from them.
