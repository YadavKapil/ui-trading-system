import datetime as dt
import dataclasses
import json
from decimal import Decimal

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from wave_replay.broker_engine import BrokerEngine, FeeSchedule
from wave_replay.cli import main
from wave_replay.continuous import (
    COLLECTION_SCHEMA,
    ContinuousReplayConfig,
    ContinuousWaveRunner,
    DatasetPartition,
    ReplayDatasetCollection,
    _select_otm_target,
    _period_summaries,
    _unavailable_held_session_summary,
    seal_collection_manifest,
)
from wave_replay.fake_ticker import FakeTicker
from wave_replay.datasets import ReplayDataset, session_segments_for_date
from wave_replay.normalize import canonical_json, sha256_bytes, sha256_file
from wave_replay.schemas import SESSION_ARROW_SCHEMA
from wave_replay.schemas import IST, SchemaError, derive_replay_instrument_token
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.selector import load_saved_wave_settings
from wave_replay.synthetic import create_synthetic_dataset
from wave_replay.tasks import SimTaskRuntime
from wave_replay.tracing import TraceRecorder


def _fee_schedule():
    return FeeSchedule({
        "schema": "wave-replay-fee-schedule-v1",
        "usage_label": "upstox_approximation",
        "broker_fidelity": "not_zerodha",
        "resolved_schedule": {
            "broker": {"option_brokerage": "20"},
            "statutory": {
                "option_stt_rate": "0.0015",
                "option_exchange_rate": "0.00053",
                "sebi_rate": "0.000001",
                "option_stamp_rate": "0.00003",
                "gst_rate": "0.18",
            },
        },
    })


def test_exchange_close_cancels_day_orders_and_rejects_callback_replacement():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 1, 6, 15, 30, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    ticker = FakeTicker(scheduler, tasks, trace)
    engine = BrokerEngine(scheduler=scheduler, trace=trace, fee_schedule=_fee_schedule())
    engine.bind_callback_dispatch(ticker.order_dispatch)
    listener = ticker.facade("worker")
    replacements = []

    def on_order(_ws, payload):
        if payload["status"] == "CANCELLED":
            replacements.append(engine.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol="NIFTYCE",
                transaction_type="BUY",
                quantity=25,
                product="NRML",
                order_type="LIMIT",
                price=90,
            ))

    listener.on_order_update = on_order
    listener.connect()
    engine.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="SELL",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=110,
    )
    engine.close_exchange_session()
    with pytest.raises(RuntimeError, match="exchange_session_closed"):
        engine.place_gtt(
            trigger_type="single",
            tradingsymbol="NIFTYCE",
            exchange="NFO",
            trigger_values=[70],
            last_price=70,
            orders=[{
                "transaction_type": "BUY",
                "quantity": 25,
                "product": "NRML",
                "order_type": "LIMIT",
                "price": 70,
            }],
        )
    scheduler.run()

    assert engine.day_cancellation_count == 1
    assert engine.post_close_rejection_count == 2
    assert engine.orders_by_id[replacements[0]].status == "REJECTED"
    assert not any(order.status in {"OPEN", "ACK", "PARTIAL"} for order in engine.orders_by_id.values())


def _continuous_collection(tmp_path):
    calendar = {
        "timezone": "Asia/Kolkata",
        "exchange_time_zone": "Asia/Kolkata",
        "market_segments": {
            day: ([{"start": "09:15:00", "end": "15:30:00"}] if index < 5 else [])
            for index, day in enumerate(
                ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
            )
        },
        "early_closes": {},
        "late_opens": {},
        "holidays": ["2025-01-08"],
    }
    cases = [
        (dt.date(2025, 1, 6), dt.date(2025, 1, 10), Decimal("24000")),
        (dt.date(2025, 1, 7), dt.date(2025, 1, 10), Decimal("24000")),
        (dt.date(2025, 1, 9), dt.date(2025, 1, 20), Decimal("24200")),
    ]
    paths = []
    for index, (session_date, expiry, strike) in enumerate(cases):
        path = tmp_path / f"day-{session_date}"
        create_synthetic_dataset(
            path,
            corpus=[{
                "role": "range",
                "date": session_date,
                "strike": strike,
                "first_close": strike - Decimal("4.65"),
                "expiry": expiry,
                "lot_size": 75,
                "missing": 0,
                "option_base": "50",
                "missing_underlying_rows": [30] if index == 0 else [],
                "missing_option_rows": [45] if index == 1 else [],
            }],
            calendar_definition_override=calendar,
            integrity_profile_override="wave-replay-special-sessions-v3",
        )
        paths.append(path)
    collection = ReplayDatasetCollection.from_inputs(paths)
    manifest = collection.to_manifest()
    manifest["partitions"] = [
        {**entry, "path": str(paths[index].relative_to(tmp_path))}
        for index, entry in enumerate(manifest["partitions"])
    ]
    manifest = seal_collection_manifest(manifest)
    (tmp_path / "collection_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return ReplayDatasetCollection.from_inputs([tmp_path]), manifest


def _continuous_config(*, scripts=None):
    settings, hashes = load_saved_wave_settings(
        "wave_extractor_config.json",
        delta_limits_path="delta_limits.json",
    )
    return ContinuousReplayConfig(
        start=dt.date(2025, 1, 6),
        end=dt.date(2025, 1, 9),
        wave_settings=settings,
        wave_settings_hashes=hashes,
        session_broker_scripts=scripts or {},
    )


def _refresh_dataset_file(root, filename):
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["files"] if item["path"] == filename)
    entry["sha256"] = sha256_file(root / filename)
    entry["size_bytes"] = (root / filename).stat().st_size
    without_hash = dict(manifest)
    without_hash.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = sha256_bytes(
        (canonical_json(without_hash) + "\n").encode("utf-8")
    )
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")


def _selection_dataset(tmp_path):
    session_date = dt.date(2025, 1, 6)
    path = tmp_path / "selection"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995.35"),
            "expiry": dt.date(2025, 1, 10),
            "lot_size": 75,
            "missing": 0,
            "option_base": "50",
        }],
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    return ReplayDataset.load(path, allow_special_sessions_v3=True), session_date


def _add_selection_contract(dataset, *, strike, premium, expiry, suffix):
    base = dataset.contracts[0]
    source = min(dataset.option_bars, key=lambda bar: bar.bar_end)
    instrument_id = f"NIFTY|{expiry.isoformat()}|C|{strike}|{suffix}"
    symbol = f"NIFTYTEST{strike}{suffix}CE"
    contract = dataclasses.replace(
        base,
        instrument_id=instrument_id,
        source_instrument_id=f"SOURCE-{suffix}",
        wave_trading_symbol=symbol,
        lean_symbol=f"NIFTY {expiry:%y%m%d}C{strike:08d}{suffix}",
        instrument_token=derive_replay_instrument_token(instrument_id),
        expiry=expiry,
        strike=Decimal(str(strike)),
        source_contract_metadata_sha256=(suffix * 64)[:64],
    )
    bar = dataclasses.replace(
        source,
        instrument_id=instrument_id,
        source_instrument_id=contract.source_instrument_id,
        replay_instrument_token=contract.instrument_token,
        wave_trading_symbol=symbol,
        lean_symbol=contract.lean_symbol,
        open=Decimal(str(premium)),
        high=Decimal(str(premium)),
        low=Decimal(str(premium)),
        close=Decimal(str(premium)),
    )
    dataset.contracts.append(contract)
    dataset.option_bars.append(bar)
    return contract


def _add_full_session_contract(dataset, *, strike, premium, expiry, suffix):
    base = dataset.contracts[0]
    instrument_id = f"NIFTY|{expiry.isoformat()}|C|{strike}|{suffix}"
    symbol = f"NIFTYTEST{strike}{suffix}CE"
    contract = dataclasses.replace(
        base,
        instrument_id=instrument_id,
        source_instrument_id=f"SOURCE-{suffix}",
        wave_trading_symbol=symbol,
        lean_symbol=f"NIFTY {expiry:%y%m%d}C{strike:08d}{suffix}",
        instrument_token=derive_replay_instrument_token(instrument_id),
        strike=Decimal(str(strike)),
        source_contract_metadata_sha256=(suffix * 64)[:64],
    )
    source_bars = [
        bar for bar in dataset.option_bars if bar.instrument_id == base.instrument_id
    ]
    bars = [
        dataclasses.replace(
            bar,
            instrument_id=instrument_id,
            source_instrument_id=contract.source_instrument_id,
            replay_instrument_token=contract.instrument_token,
            wave_trading_symbol=symbol,
            lean_symbol=contract.lean_symbol,
            open=Decimal(str(premium)),
            high=Decimal(str(premium)),
            low=Decimal(str(premium)),
            close=Decimal(str(premium)),
        )
        for bar in source_bars
    ]
    dataset.contracts.append(contract)
    dataset.option_bars.extend(bars)
    dataset.contracts_by_id[contract.instrument_id] = contract
    dataset.contracts_by_wave_symbol[contract.wave_trading_symbol].append(contract)
    return contract


class _MutatingCollection:
    def __init__(self, source, mutator):
        self.source = source
        self.mutator = mutator

    def validate_range(self, start, end):
        return self.source.validate_range(start, end)

    def dates(self, start=None, end=None):
        return self.source.dates(start, end)

    def to_manifest(self):
        return self.source.to_manifest()

    @property
    def partial_underlying_dates(self):
        return self.source.partial_underlying_dates

    @property
    def missing_underlying_dates(self):
        return self.source.missing_underlying_dates

    def load_session(self, session_date):
        dataset = self.source.load_session(session_date)
        self.mutator(dataset, session_date)
        return dataset


def test_otm_selection_accepts_35_65_tie_excludes_itm_and_ignores_later_expiry(tmp_path):
    dataset, session_date = _selection_dataset(tmp_path)
    nearest_expiry = dataset.contracts[0].expiry
    _add_selection_contract(
        dataset,
        strike=24050,
        premium=65,
        expiry=nearest_expiry,
        suffix="B",
    )
    _add_selection_contract(
        dataset,
        strike=23900,
        premium=50,
        expiry=nearest_expiry,
        suffix="C",
    )
    later = _add_selection_contract(
        dataset,
        strike=24010,
        premium=50,
        expiry=dt.date(2025, 1, 17),
        suffix="D",
    )
    selected, underlying, option, candidates = _select_otm_target(dataset, session_date)
    assert selected == dataset.contracts[0]
    assert option.close == Decimal("35")
    assert underlying.close == Decimal("23995.3500")
    assert {item["premium"] for item in candidates if item["strictly_otm"]} == {
        "35.0000",
        "65",
    }
    assert any(not item["strictly_otm"] for item in candidates)
    assert later.instrument_id not in {item["instrument_id"] for item in candidates}


def test_otm_selection_chooses_closest_to_50_then_reports_no_qualifier(tmp_path):
    dataset, session_date = _selection_dataset(tmp_path)
    selected_contract = _add_selection_contract(
        dataset,
        strike=24025,
        premium=49,
        expiry=dataset.contracts[0].expiry,
        suffix="E",
    )
    selected, _underlying, option, _candidates = _select_otm_target(
        dataset,
        session_date,
    )
    assert selected == selected_contract
    assert option.close == Decimal("49")

    for index, bar in enumerate(dataset.option_bars):
        if bar.bar_end.time() == dt.time(9, 16):
            dataset.option_bars[index] = dataclasses.replace(
                bar,
                open=Decimal("70"),
                high=Decimal("70"),
                low=Decimal("70"),
                close=Decimal("70"),
            )
    selected, _underlying, option, candidates = _select_otm_target(
        dataset,
        session_date,
    )
    assert selected is None
    assert option is None
    assert candidates and not any(item["premium_in_range"] for item in candidates)


def test_otm_selection_waits_for_complete_nearest_expiry_quote_universe(tmp_path):
    dataset, session_date = _selection_dataset(tmp_path)
    delayed = _add_selection_contract(
        dataset,
        strike=24025,
        premium=50,
        expiry=dataset.contracts[0].expiry,
        suffix="LATE",
    )
    delayed_bar = dataset.option_bars[-1]
    dataset.option_bars[-1] = dataclasses.replace(
        delayed_bar,
        bar_start=delayed_bar.bar_start + dt.timedelta(minutes=1),
        bar_end=delayed_bar.bar_end + dt.timedelta(minutes=1),
        available_at=delayed_bar.available_at + dt.timedelta(minutes=1),
    )
    selected, underlying, option, _candidates = _select_otm_target(
        dataset,
        session_date,
    )
    assert selected == delayed
    assert underlying.bar_end == option.bar_end
    assert option.bar_end.time() == dt.time(9, 17)


def test_out_of_range_held_contract_switches_without_overlap(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def mutate(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        old = dataset.contracts[0]
        first_end = min(
            bar.bar_end
            for bar in dataset.option_bars
            if bar.instrument_id == old.instrument_id
        )
        dataset.option_bars = [
            (
                dataclasses.replace(
                    bar,
                    open=Decimal("70"),
                    high=Decimal("70"),
                    low=Decimal("70"),
                    close=Decimal("70"),
                )
                if bar.instrument_id == old.instrument_id and bar.bar_end == first_end
                else bar
            )
            for bar in dataset.option_bars
        ]
        _add_full_session_contract(
            dataset,
            strike=24050,
            premium=50,
            expiry=old.expiry,
            suffix="SWITCH",
        )

    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, mutate),
        config=dataclasses.replace(
            _continuous_config(),
            experiment_id="band-40-60",
            keep_min_premium=Decimal("40"),
            keep_max_premium=Decimal("60"),
        ),
    ).run()
    decision = report["sessions"][1]["contract_decision"]
    assert decision["action"] == "switch"
    assert decision["reason"] == "held_premium_outside_40_60"
    assert decision["held_premium"] == "70"
    assert report["switches"][0]["close_price"] == "70"
    assert report["sessions"][1]["contract"]["strike"] == "24050"
    old_symbol = report["switches"][0]["old_contract"]["wave_trading_symbol"]
    assert report["sessions"][1]["positions"][old_symbol]["quantity"] == 0


def test_delayed_out_of_range_switch_does_not_backdate_post_switch_state(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def mutate(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        old = dataset.contracts[0]
        ends = sorted(
            bar.bar_end
            for bar in dataset.option_bars
            if bar.instrument_id == old.instrument_id
        )
        dataset.option_bars = [
            (
                dataclasses.replace(
                    bar,
                    open=Decimal("70"),
                    high=Decimal("70"),
                    low=Decimal("70"),
                    close=Decimal("70"),
                )
                if bar.instrument_id == old.instrument_id and bar.bar_end == ends[1]
                else bar
            )
            for bar in dataset.option_bars
            if not (bar.instrument_id == old.instrument_id and bar.bar_end == ends[0])
        ]
        _add_full_session_contract(
            dataset,
            strike=24050,
            premium=50,
            expiry=old.expiry,
            suffix="DELAYED",
        )

    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, mutate),
        config=dataclasses.replace(
            _continuous_config(),
            end=dt.date(2025, 1, 7),
            experiment_id="delayed-carried-quote",
        ),
    ).run()
    session = report["sessions"][1]
    assert session["contract_decision"]["action"] == "switch"
    assert session["contract_decision"]["held_check_at"] == "2025-01-07T09:17:00+05:30"
    assert session["equity_curve"][0]["at"] == "2025-01-07T09:16:00+05:30"
    assert session["equity_curve"][0]["equity"] is None
    assert all(
        point["equity"] is None
        for point in session["equity_curve"]
        if point["at"] < "2025-01-07T09:17:00"
    )
    capital = report["capital"]
    assert any(
        point["at"] == "2025-01-07T09:16:00+05:30"
        and point["capital_used_including_margin"] is None
        and point["missing_underlying_spot"] is True
        for point in capital["curve"]
    )
    assert capital["peaks"]["coverage"]["capital_used_including_margin"][
        "complete"
    ] is False
    assert capital["peaks"]["capital_used_including_margin"] is None
    assert capital["profit_on_peak_capital"]["classification"] == (
        "unavailable_non_monthly_period"
    )


def test_no_qualifying_contract_produces_flat_no_wave_day(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def mutate(dataset, session_date):
        if session_date != dt.date(2025, 1, 6):
            return
        first_end = min(bar.bar_end for bar in dataset.option_bars)
        dataset.option_bars = [
            (
                dataclasses.replace(
                    bar,
                    open=Decimal("70"),
                    high=Decimal("70"),
                    low=Decimal("70"),
                    close=Decimal("70"),
                )
                if bar.bar_end == first_end
                else bar
            )
            for bar in dataset.option_bars
        ]

    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, mutate),
        config=_continuous_config(),
    ).run()
    assert report["no_trade_days"][0]["session_date"] == "2025-01-06"
    assert report["sessions"][0]["contract"] is None
    assert report["sessions"][0]["worker_lifecycle"]["not_started"] is True
    assert report["sessions"][0]["time_without_contract_seconds"] == 22500
    assert any(
        event["kind"] == "CONTINUOUS_NO_TRADE_SESSION"
        and event["continuous_session_date"] == "2025-01-06"
        for event in trace.events
    )


def test_out_of_range_switch_to_flat_counts_close_activity(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def mutate(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        first_end = min(bar.bar_end for bar in dataset.option_bars)
        dataset.option_bars = [
            (
                dataclasses.replace(
                    bar,
                    open=Decimal("70"),
                    high=Decimal("70"),
                    low=Decimal("70"),
                    close=Decimal("70"),
                )
                if bar.bar_end == first_end
                else bar
            )
            for bar in dataset.option_bars
        ]

    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, mutate),
        config=dataclasses.replace(_continuous_config(), end=dt.date(2025, 1, 7)),
    ).run()
    session = report["sessions"][1]
    assert session["contract_decision"]["action"] == "switch_to_flat"
    assert session["orders"] == 1
    assert session["fills"] == 1
    assert session["trades"]
    assert report["inventory"]["max_total_absolute_quantity"] >= 75


def test_held_contract_keeps_at_65_and_waits_for_later_synchronized_quote(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def keep_at_65(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        first_end = min(bar.bar_end for bar in dataset.option_bars)
        dataset.option_bars = [
            (
                dataclasses.replace(
                    bar,
                    open=Decimal("65"),
                    high=Decimal("65"),
                    low=Decimal("65"),
                    close=Decimal("65"),
                )
                if bar.bar_end == first_end
                else bar
            )
            for bar in dataset.option_bars
        ]

    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, keep_at_65),
        config=_continuous_config(),
    ).run()
    assert report["sessions"][1]["contract_decision"]["action"] == "keep"
    assert report["sessions"][1]["contract_decision"]["held_premium"] == "65"

    def delay_held_quote(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        first_end = min(bar.bar_end for bar in dataset.option_bars)
        dataset.option_bars = [
            bar for bar in dataset.option_bars if bar.bar_end != first_end
        ]

    report, _trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, delay_held_quote),
        config=_continuous_config(),
    ).run()
    decision = report["sessions"][1]["contract_decision"]
    assert decision["action"] == "keep"
    assert decision["at"] == "2025-01-07T09:17:00+05:30"
    assert any(
        item["session_date"] == "2025-01-07"
        and item["timestamp"].startswith("2025-01-07T09:16:00")
        for item in report["missing_data"]
    )


def test_full_session_held_quote_outage_skips_worker_without_stale_mark(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)

    def remove_held_quotes(dataset, session_date):
        if session_date != dt.date(2025, 1, 7):
            return
        old = dataset.contracts[0]
        _add_full_session_contract(
            dataset,
            strike=24050,
            premium=50,
            expiry=old.expiry,
            suffix="OTHER",
        )
        dataset.option_bars = [
            bar for bar in dataset.option_bars if bar.instrument_id != old.instrument_id
        ]

    config = dataclasses.replace(_continuous_config(), end=dt.date(2025, 1, 7))
    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=_MutatingCollection(collection, remove_held_quotes),
        config=config,
    ).run()
    assert report["sessions"][1]["contract_decision"]["action"] == "skip"
    assert report["sessions"][1]["worker_lifecycle"]["not_started"] is True
    assert report["sessions"][1]["equity_curve"][-1]["equity"] is None
    assert report["missing_data_summary"]["skipped_minutes"] >= 375
    assert report["forced_close_sensitivity"]["available"] is False
    assert report["account"]["ending_equity"] is None
    assert report["account"]["pnl"] is None
    assert report["final_boundary_mtm"]["valuation_status"] == "unknown_missing_final_quote"
    assert report["summaries"]["annual"][-1]["end_equity"] is None
    assert any(
        event["kind"] == "CONTINUOUS_HELD_QUOTE_UNAVAILABLE"
        for event in trace.events
    )


def test_continuous_replay_carries_overnight_rolls_before_holiday_and_restarts_workers(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)
    first_report, first_trace = ContinuousWaveRunner(
        repository_root=".",
        collection=collection,
        config=_continuous_config(),
    ).run()
    second_report, second_trace = ContinuousWaveRunner(
        repository_root=".",
        collection=collection,
        config=_continuous_config(),
    ).run()

    assert first_trace.digest() == second_trace.digest()
    assert first_report["report_hash"] == second_report["report_hash"]
    assert first_report["requested_range"]["actual_sessions"] == [
        "2025-01-06",
        "2025-01-07",
        "2025-01-09",
    ]
    assert first_report["overnight_holdings"][0]["next_session_date"] == "2025-01-07"
    assert first_report["rolls"][0]["session_date"] == "2025-01-07"
    assert first_report["rolls"][0]["next_session_date"] == "2025-01-09"
    assert first_report["rolls"][0]["old_contract"]["expiry"] == "2025-01-10"
    assert first_report["rolls"][0]["new_contract"]["expiry"] == "2025-01-20"
    assert first_report["sessions"][0]["contract"] == first_report["sessions"][1]["contract"]
    assert first_report["sessions"][1]["contract_decision"]["action"] == "keep"
    assert first_report["sessions"][1]["contract_decision"]["held_premium"] == "35.0000"
    assert (
        first_report["sessions"][1]["contract"]["instrument_id"]
        != first_report["sessions"][2]["contract"]["instrument_id"]
    )
    old_symbol = first_report["sessions"][1]["contract"]["wave_trading_symbol"]
    assert first_report["sessions"][1]["positions"][old_symbol]["quantity"] == 0
    assert all(
        position["quantity"] == 0
        for symbol, position in first_report["sessions"][2]["positions"].items()
        if symbol == old_symbol
    )
    assert all(session["day_cancellations"] >= 1 for session in first_report["sessions"])
    assert all(session["worker_lifecycle"]["exit_code"] == 0 for session in first_report["sessions"])
    assert all(session["worker_lifecycle"]["cleanup_handlers"] == 1 for session in first_report["sessions"])
    assert all(
        session["worker_lifecycle"]["completed_at"]
        > session["session_date"] + "T15:30:00"
        for session in first_report["sessions"]
    )
    assert all(not session["ending_open_orders"] for session in first_report["sessions"])
    assert first_report["activity"]["post_close_rejections"] >= 1
    assert first_report["activity"]["roll_rejections"] >= 1
    assert first_report["final_state"]["day_orders_persisted_overnight"] is False
    assert len(first_report["equity_curve"]) >= 3 * 375 - 2
    assert first_report["missing_data_summary"]["policy"] == "skip_decision"
    assert first_report["missing_data_summary"]["skipped_minutes"] == 2
    assert first_report["missing_data_summary"]["skipped_duration_seconds"] == 120
    assert first_report["missing_data_summary"]["stale_quote_decisions"] == 0
    assert first_report["missing_data_summary"]["partial_underlying_session_dates"] == [
        "2025-01-06"
    ]
    assert first_report["final_boundary_mtm"]["positions_not_mutated"] is True
    assert (
        first_report["drawdown"]["drawdown_peak_at"]
        <= first_report["drawdown"]["drawdown_trough_at"]
    )
    if first_report["final_boundary_mtm"]["positions"]:
        assert first_report["forced_close_sensitivity"]["exits"]
        assert Decimal(first_report["forced_close_sensitivity"]["exit_fees"]) > 0
    assert (
        sum(
            (Decimal(item["pnl"]) for item in first_report["summaries"]["annual"]),
            Decimal("0"),
        )
        == Decimal(first_report["account"]["pnl"])
    )


def test_continuous_replay_preserves_and_reconciles_gtt_without_duplicate_duo(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)
    fallback = [{
        "kind": "gtt_create",
        "at": "2025-01-06T15:29:00+05:30",
        "side": "BUY",
        "price": "70",
        "tag": "GTT_B_SCRIPTED_FALLBACK",
    }]
    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=collection,
        config=_continuous_config(scripts={
            "2025-01-06": fallback,
            "2025-01-07": [{
                "kind": "gtt_trigger",
                "at": "2025-01-07T09:17:00+05:30",
                "trigger_id": "first_active",
                "immediate_complete": True,
            }],
            "2025-01-09": [
                {
                    "kind": "gtt_create",
                    "at": "2025-01-09T15:29:00+05:30",
                    "side": "BUY",
                    "price": "97",
                    "tag": "GTT_B_FINAL_BAR",
                },
                {
                    "kind": "gtt_trigger",
                    "at": "2025-01-09T15:30:00+05:30",
                    "trigger_id": "first_active",
                    "immediate_complete": True,
                },
            ],
        }),
    ).run()
    assert report["sessions"][0]["active_gtts"] >= 1
    assert any(
        event["kind"] == "CONTINUOUS_GTT_REHYDRATED"
        and event["continuous_session_date"] == "2025-01-07"
        for event in trace.events
    )
    assert any(
        event["kind"] == "CONTINUOUS_DUO_SKIPPED_PENDING_GTT"
        and event["continuous_session_date"] == "2025-01-07"
        for event in trace.events
    )
    assert not any(
        event["kind"] == "BROKER_CALL"
        and event.get("operation") == "place_order"
        and event["continuous_session_date"] == "2025-01-07"
        and event["at"] < "2025-01-07T09:17:00"
        for event in trace.events
    )
    assert report["equity_curve"][-1]["reason"] == "session_final_post_events"
    assert report["equity_curve"][-1]["fees"] == report["account"]["fees"]
    assert report["equity_curve"][-1]["equity"] == report["account"]["ending_equity"]


def test_collection_manifest_rejects_missing_day_and_tampered_pin(tmp_path):
    _collection, manifest = _continuous_collection(tmp_path)
    manifest_path = tmp_path / "collection_manifest.json"
    broken = dict(manifest)
    broken["expected_session_dates"] = [*manifest["expected_session_dates"], "2025-01-10"]
    broken = seal_collection_manifest(broken)
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(SchemaError, match="exactly match"):
        ReplayDatasetCollection.from_inputs([tmp_path])

    broken = json.loads(json.dumps(manifest))
    broken["partitions"] = [
        item for item in broken["partitions"] if item["session_date"] != "2025-01-07"
    ]
    broken["expected_session_dates"] = [
        day for day in broken["expected_session_dates"] if day != "2025-01-07"
    ]
    broken = seal_collection_manifest(broken)
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(SchemaError, match="calendar-derived"):
        ReplayDatasetCollection.from_inputs([tmp_path])

    broken = json.loads(json.dumps(manifest))
    broken["partitions"][0]["manifest_sha256"] = "0" * 64
    broken = seal_collection_manifest(broken)
    manifest_path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(SchemaError, match="pin mismatch"):
        ReplayDatasetCollection.from_inputs([tmp_path])


def test_collection_manifest_schema_is_explicit(tmp_path):
    collection, manifest = _continuous_collection(tmp_path)
    assert manifest["schema"] == COLLECTION_SCHEMA
    assert collection.compatibility["schema_version"] == "1.1.0"
    assert collection.compatibility["integrity_profile"] == "wave-replay-special-sessions-v3"
    assert collection.to_manifest() == manifest
    assert "partial_underlying_session_dates" not in manifest
    assert "missing_underlying_session_dates" not in manifest
    assert manifest["session_calendar"] == {
        "partial_underlying_session_dates": ["2025-01-06"],
        "missing_underlying_session_dates": [],
    }


def test_collection_session_calendar_missingness_tampering_fails(tmp_path):
    _collection, manifest = _continuous_collection(tmp_path)
    manifest["session_calendar"]["partial_underlying_session_dates"] = []
    manifest = seal_collection_manifest(manifest)
    (tmp_path / "collection_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    with pytest.raises(SchemaError, match="missingness inventory mismatch"):
        ReplayDatasetCollection.from_inputs([tmp_path])


def test_generated_collection_identity_is_portable_across_parent_directories(tmp_path):
    compatibility = {
        "session_calendar_definition": {
            "market_segments": {},
            "holidays": [],
            "special_sessions": {},
        }
    }
    partition = {
        "session_date": dt.date(2025, 1, 6),
        "dataset_id": "wave-v1-portable",
        "manifest_sha256": "a" * 64,
    }
    first = ReplayDatasetCollection(
        [DatasetPartition(path=tmp_path / "first" / "day-1", **partition)],
        compatibility,
    )
    second = ReplayDatasetCollection(
        [DatasetPartition(path=tmp_path / "second" / "day-1", **partition)],
        compatibility,
    )
    assert first.to_manifest()["collection_id"] == second.to_manifest()["collection_id"]
    assert first.to_manifest()["collection_sha256"] == second.to_manifest()["collection_sha256"]


def test_collection_rejects_monolithic_and_incompatible_daily_partitions(tmp_path):
    monolithic = create_synthetic_dataset(tmp_path / "monolithic")
    with pytest.raises(SchemaError, match="one immutable date partition"):
        ReplayDatasetCollection.from_inputs([monolithic])

    calendar = {
        "timezone": "Asia/Kolkata",
        "exchange_time_zone": "Asia/Kolkata",
        "market_segments": {
            day: ([{"start": "09:15:00", "end": "15:30:00"}] if index < 5 else [])
            for index, day in enumerate(
                ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
            )
        },
        "early_closes": {},
        "late_opens": {},
        "holidays": [],
    }
    paths = []
    for index, day in enumerate((dt.date(2025, 1, 6), dt.date(2025, 1, 7))):
        altered_calendar = dict(calendar)
        altered_calendar["holidays"] = [] if index == 0 else ["2025-01-08"]
        path = tmp_path / f"incompatible-{index}"
        create_synthetic_dataset(
            path,
            corpus=[{
                "role": "range",
                "date": day,
                "strike": Decimal("24000"),
                "first_close": Decimal("24004.65"),
                "expiry": dt.date(2025, 1, 13),
                "lot_size": 75,
                "missing": 0,
            }],
            calendar_definition_override=altered_calendar,
            integrity_profile_override="wave-replay-special-sessions-v3",
        )
        paths.append(path)
    with pytest.raises(SchemaError, match="Incompatible continuous dataset partition"):
        ReplayDatasetCollection.from_inputs(paths)


def test_continuous_partial_underlying_mode_is_explicit_and_exact(tmp_path):
    session_date = dt.date(2025, 1, 6)
    path = tmp_path / "partial-underlying"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995.35"),
            "expiry": dt.date(2025, 1, 10),
            "lot_size": 75,
            "missing": 0,
            "missing_underlying_rows": [27],
            "option_base": "50",
        }],
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    with pytest.raises(SchemaError, match="not equivalence eligible"):
        ReplayDataset.load(path, allow_special_sessions_v3=True)

    collection = ReplayDatasetCollection.from_inputs([path])
    dataset = collection.load_session(session_date)
    materialized = dataset.underlying_bars_with_missing(session_date)
    missing = [bar for bar in materialized if bar.missing_bar]
    assert len(missing) == 1
    assert missing[0].bar_end.time() == dt.time(9, 43)

    rows = pq.read_table(path / "sessions.parquet").to_pylist()
    rows[0]["missing_underlying_bar_count"] = 0
    pq.write_table(
        pa.Table.from_pylist(rows, schema=SESSION_ARROW_SCHEMA),
        path / "sessions.parquet",
    )
    _refresh_dataset_file(path, "sessions.parquet")
    with pytest.raises(SchemaError, match="missingness|declarations"):
        collection.load_session(session_date)


def test_continuous_rejects_fully_missing_underlying_partition(tmp_path):
    session_date = dt.date(2025, 1, 6)
    path = tmp_path / "missing-underlying-session"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995.35"),
            "expiry": dt.date(2025, 1, 10),
            "lot_size": 75,
            "missing": 0,
            "missing_underlying_rows": list(range(375)),
            "option_base": "50",
        }],
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    collection = ReplayDatasetCollection.from_inputs([path])
    with pytest.raises(SchemaError, match="fully missing|no underlying bars"):
        collection.load_session(session_date)


def test_special_sunday_session_uses_authoritative_hours_for_staged_worker(tmp_path):
    session_date = dt.date(2026, 2, 1)
    calendar = {
        "timezone": "Asia/Kolkata",
        "exchange_time_zone": "Asia/Kolkata",
        "market_segments": {
            day: ([{"start": "09:15:00", "end": "15:30:00"}] if index < 5 else [])
            for index, day in enumerate(
                ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
            )
        },
        "early_closes": {},
        "late_opens": {},
        "holidays": [],
        "special_sessions": {
            "2026-02-01": [{"start": "09:15:00", "end": "15:30:00"}],
        },
    }
    path = tmp_path / "special-session"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("25000"),
            "first_close": Decimal("24995.35"),
            "expiry": dt.date(2026, 2, 9),
            "lot_size": 65,
            "missing": 0,
            "special_session": True,
            "option_base": "50",
        }],
        calendar_definition_override=calendar,
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    settings, hashes = load_saved_wave_settings(
        "wave_extractor_config.json",
        delta_limits_path="delta_limits.json",
    )
    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=ReplayDatasetCollection.from_inputs([path]),
        config=ContinuousReplayConfig(
            start=session_date,
            end=session_date,
            wave_settings=settings,
            wave_settings_hashes=hashes,
        ),
    ).run()
    assert report["sessions"][0]["worker_lifecycle"]["exit_code"] == 0
    assert any(
        event["kind"] == "AUTHORITATIVE_SESSION_CALENDAR_BOUND"
        and event["details"]["special_session"] is True
        for event in trace.events
    )


def test_opening_missing_quotes_wait_for_first_synchronized_pair(tmp_path):
    session_date = dt.date(2025, 1, 6)
    path = tmp_path / "opening-gap"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995.35"),
            "expiry": dt.date(2025, 1, 13),
            "lot_size": 75,
            "missing": 0,
            "missing_option_rows": [0],
            "missing_underlying_rows": [1],
            "option_base": "50",
        }],
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    settings, hashes = load_saved_wave_settings(
        "wave_extractor_config.json",
        delta_limits_path="delta_limits.json",
    )
    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=ReplayDatasetCollection.from_inputs([path]),
        config=ContinuousReplayConfig(
            start=session_date,
            end=session_date,
            wave_settings=settings,
            wave_settings_hashes=hashes,
        ),
    ).run()
    selected_dataset = ReplayDataset.load(
        path,
        allow_special_sessions_v3=True,
        allow_partial_underlying=True,
    )
    _contract, underlying, option, _candidates = _select_otm_target(
        selected_dataset,
        session_date,
    )
    assert underlying.bar_end == option.bar_end == dt.datetime(
        2025, 1, 6, 9, 18, tzinfo=IST
    )
    assert report["missing_data_summary"]["skipped_minutes"] == 2
    first_order = next(
        event
        for event in trace.events
        if event["kind"] == "BROKER_CALL" and event.get("operation") == "place_order"
    )
    assert first_order["at"] >= "2025-01-06T09:18:00"


def test_exact_close_remains_inclusive_but_broker_rejects_post_close_callback_work(tmp_path):
    session_date = dt.date(2025, 1, 6)
    calendar = {
        "timezone": "Asia/Kolkata",
        "exchange_time_zone": "Asia/Kolkata",
        "market_segments": {
            "monday": [{"start": "09:15:00", "end": "15:30:00"}],
            "tuesday": [{"start": "09:15:00", "end": "15:30:00"}],
            "wednesday": [{"start": "09:15:00", "end": "15:30:00"}],
            "thursday": [{"start": "09:15:00", "end": "15:30:00"}],
            "friday": [{"start": "09:15:00", "end": "15:30:00"}],
            "saturday": [],
            "sunday": [],
        },
        "early_closes": {"2025-01-06": "09:19:00"},
        "late_opens": {},
        "holidays": [],
    }
    path = tmp_path / "aligned-close"
    create_synthetic_dataset(
        path,
        corpus=[{
            "role": "range",
            "date": session_date,
            "strike": Decimal("24000"),
            "first_close": Decimal("23995.35"),
            "expiry": dt.date(2025, 1, 13),
            "lot_size": 75,
            "missing": 0,
            "session_close": dt.time(9, 19),
            "option_base": "50",
        }],
        calendar_definition_override=calendar,
        integrity_profile_override="wave-replay-special-sessions-v3",
    )
    settings, hashes = load_saved_wave_settings(
        "wave_extractor_config.json",
        delta_limits_path="delta_limits.json",
    )
    report, trace = ContinuousWaveRunner(
        repository_root=".",
        collection=ReplayDatasetCollection.from_inputs([path]),
        config=ContinuousReplayConfig(
            start=session_date,
            end=session_date,
            wave_settings=settings,
            wave_settings_hashes=hashes,
        ),
    ).run()
    close_check = next(
        event for event in trace.events if event["kind"] == "AUTHORITATIVE_CLOSE_BOUNDARY_CHECK"
    )
    exchange_close = next(
        event for event in trace.events if event["kind"] == "EXCHANGE_SESSION_CLOSE"
    )
    assert close_check["result"] == {"is_open": True, "inclusive_close": True}
    assert exchange_close["seq"] < close_check["seq"]
    assert report["activity"]["post_close_rejections"] >= 1


def test_continuous_cli_repeats_byte_identically(tmp_path):
    _collection, _manifest = _continuous_collection(tmp_path)
    output = tmp_path / "output"
    assert main([
        "replay-continuous",
        "--datasets",
        str(tmp_path),
        "--start",
        "2025-01-06",
        "--end",
        "2025-01-09",
        "--output",
        str(output),
        "--repeat",
        "2",
    ]) == 0
    report = json.loads((output / "continuous-report.json").read_text(encoding="utf-8"))
    assert report["determinism"]["repetitions"] == 2
    assert (
        (output / "continuous-trace-1.jsonl").read_bytes()
        == (output / "continuous-trace-2.jsonl").read_bytes()
    )


def test_continuous_config_rejects_strategy_or_missing_policy_drift():
    settings, hashes = load_saved_wave_settings(
        "wave_extractor_config.json",
        delta_limits_path="delta_limits.json",
    )
    with pytest.raises(SchemaError, match="contradict approved profile"):
        ContinuousReplayConfig.from_mapping(
            {
                "schema": "wave-replay-continuous-config-v1",
                "schema_version": 1,
                "strategy": {"trigger_mode": "ohlc_touch"},
            },
            wave_settings=settings,
            wave_settings_hashes=hashes,
            default_start="2025-01-06",
            default_end="2025-01-09",
        )
    with pytest.raises(SchemaError, match="must be skip_decision"):
        ContinuousReplayConfig.from_mapping(
            {
                "schema": "wave-replay-continuous-config-v1",
                "schema_version": 1,
                "missing_quote_policy": "mark_unknown",
            },
            wave_settings=settings,
            wave_settings_hashes=hashes,
            default_start="2025-01-06",
            default_end="2025-01-09",
        )


def test_calendar_resolves_late_open_and_early_close_before_session_validation():
    definition = {
        "market_segments": {
            "monday": [{"start": "09:15:00", "end": "15:30:00"}],
        },
        "holidays": [],
        "special_sessions": {},
        "late_opens": {"2025-01-06": "10:00:00"},
        "early_closes": {"2025-01-06": {"time": "13:00:00"}},
    }
    assert session_segments_for_date(definition, dt.date(2025, 1, 6)) == [
        {"start": "10:00:00", "end": "13:00:00"}
    ]


def test_unknown_final_period_does_not_overwrite_prior_completed_period():
    curve = [
        {
            "at": "2025-01-31T15:30:00+05:30",
            "equity": "1000100",
            "fees": "10",
            "realized_pnl": "100",
            "unrealized_pnl": "0",
        },
        {
            "at": "2025-02-03T09:16:00+05:30",
            "equity": None,
            "fees": "10",
            "realized_pnl": "100",
            "unrealized_pnl": None,
        },
    ]
    summaries = _period_summaries(
        curve,
        Decimal("1000000"),
        final_valuation_known=False,
    )
    assert summaries["monthly"][0]["period"] == "2025-01"
    assert summaries["monthly"][0]["end_equity"] == "1000100"
    assert summaries["monthly"][1]["period"] == "2025-02"
    assert summaries["monthly"][1]["end_equity"] is None


def test_flat_account_remains_valued_when_held_identity_quote_is_unavailable(tmp_path):
    collection, _manifest = _continuous_collection(tmp_path)
    dataset = collection.load_session(dt.date(2025, 1, 6))
    contract = dataset.contracts[0]
    summary = _unavailable_held_session_summary(
        dataset,
        dt.date(2025, 1, 6),
        {
            "cash": Decimal("1000000"),
            "fees_paid": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "positions": {},
            "gtts": {},
        },
        contract,
    )
    assert all(point["equity"] == "1000000" for point in summary["equity_curve"])
    assert all(point["unrealized_pnl"] == "0" for point in summary["equity_curve"])
