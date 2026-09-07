"""Deterministic Wave inventory-episode normalization for replay dashboards."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .normalize import canonical_json, state_hash


EPISODE_SCHEMA = "wave-replay-trade-episodes-v1"
PAIR_PATTERN = re.compile(r"^pair-(\d+)-(\d+)$")
ACTIVE_ORDER_STATUSES = {"ACK", "OPEN", "PARTIAL"}
TERMINAL_ORDER_STATUSES = {"COMPLETE", "CANCELLED", "REJECTED"}


def build_trade_episode_model(
    report: dict[str, Any],
    trace_events: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Normalize replay events into auditable Wave inventory episodes."""
    contracts = _contract_index(report)
    parsed = _parse_trace(
        trace_events,
        missing_bars=_missing_bar_keys(report, contracts),
    )
    fees = _fee_event_index(report)
    lifecycle = _lifecycle_index(report)
    fills, unmatched_fees = _enrich_fills(
        parsed["fills"],
        parsed["orders"],
        fees,
    )
    episodes = _segment_episodes(fills, contracts)
    fallback_order_ids = _apply_lifecycle_fallbacks(episodes, lifecycle)
    _validate_unmatched_fees(
        unmatched_fees,
        fallback_order_ids=fallback_order_ids,
        lifecycle=lifecycle,
    )
    _finalize_episodes(
        episodes,
        report=report,
        orders=parsed["orders"],
        bars_by_symbol=parsed["bars_by_symbol"],
        boundaries=parsed["boundaries"],
        lifecycle=lifecycle,
    )
    orders, pairs = _finalize_orders(
        parsed["orders"],
        fills=fills,
        boundaries=parsed["boundaries"],
        lifecycle=lifecycle,
    )
    return {
        "schema": EPISODE_SCHEMA,
        "episode_definition": {
            "unit": "exact-contract signed inventory cycle",
            "start": "inventory transitions from zero to nonzero",
            "end": "inventory returns to zero",
            "fallback": "contract lifecycle close boundary when a close fill is absent",
            "final_open": (
                "boundary-open inventory remains historical; forced close is virtual"
            ),
            "pnl": "signed cash flows less exact allocated fill fees; no FIFO pairing",
            "mfe_mae": "synchronized completed-minute close marks; not intrabar OHLC",
        },
        "episodes": episodes,
        "orders": orders,
        "pairs": pairs,
        "market": {
            "underlying": parsed["underlying"],
            "option_by_symbol": {
                symbol: values
                for symbol, values in sorted(parsed["bars_by_symbol"].items())
            },
        },
        "markers": {
            "selections": _selection_markers(report),
            "lifecycle": _lifecycle_markers(report),
            "missing": _missing_markers(report),
            "session_boundaries": parsed["boundaries"],
        },
        "counts": {
            "episodes": len(episodes),
            "closed_flat": sum(
                episode["status"] == "closed_flat" for episode in episodes
            ),
            "closed_lifecycle_fallback": sum(
                episode["status"] == "closed_lifecycle_fallback"
                for episode in episodes
            ),
            "boundary_open": sum(
                episode["status"] == "boundary_open" for episode in episodes
            ),
            "orders": len(orders),
            "pairs": len(pairs),
            "fills": len(fills),
        },
    }


def load_trade_episode_model(
    report_path: str | Path,
    trace_path: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    report_for_hash = copy.deepcopy(report)
    expected_report_hash = report_for_hash.pop("report_hash", None)
    report_for_hash.pop("determinism", None)
    actual_report_hash = state_hash(report_for_hash)
    if actual_report_hash != expected_report_hash:
        raise ValueError(
            "Report hash does not match content: "
            f"actual={actual_report_hash} expected={expected_report_hash}"
        )
    trace_path = Path(trace_path)
    digest = _continuous_trace_digest(trace_path)
    if digest != report.get("trace_digest"):
        raise ValueError(
            "Trace digest does not match report: "
            f"actual={digest} expected={report.get('trace_digest')}"
        )
    with trace_path.open(encoding="utf-8") as handle:
        model = build_trade_episode_model(
            report,
            (json.loads(line) for line in handle if line.strip()),
        )
    return report, model


def _parse_trace(
    events: Iterable[dict[str, Any]],
    *,
    missing_bars: set[tuple[dt.datetime, str]],
) -> dict[str, Any]:
    orders: dict[str, dict[str, Any]] = {}
    fills: list[dict[str, Any]] = []
    underlying: list[dict[str, Any]] = []
    bars_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    boundaries: list[dict[str, Any]] = []
    for event in events:
        kind = event.get("kind")
        details = event.get("details", {})
        seq = int(event.get("seq", 0))
        at = str(event.get("at"))
        session = event.get("continuous_session_date")
        if kind == "BROKER_CALL" and event.get("operation") == "place_order":
            payload = details.get("order", {})
            order = _ensure_order(orders, payload)
            order["transitions"].append(
                _order_transition(event, payload, status="PLACE_CALL")
            )
        elif kind == "BROKER_ORDER_TRANSITION":
            order = _ensure_order(orders, details)
            order["transitions"].append(_order_transition(event, details))
        elif kind == "BROKER_FILL":
            fills.append(
                {
                    "trace_seq": seq,
                    "at": at,
                    "session_date": session,
                    "order_id": str(details["order_id"]),
                    "side": str(details["side"]),
                    "quantity": int(details["quantity"]),
                    "price": str(details["price"]),
                    "complete": bool(details.get("complete", False)),
                    "cumulative_quantity": int(
                        details.get("cumulative_quantity", details["quantity"])
                    ),
                }
            )
        elif kind == "BAR_AVAILABLE":
            symbol = str(
                details.get("wave_trading_symbol")
                or details.get("lean_symbol", "")
            )
            close = Decimal(str(details["close"]))
            if close <= 0 or (_time(at), symbol) in missing_bars:
                continue
            point = {
                "trace_seq": seq,
                "at": at,
                "session_date": session,
                "close": str(details["close"]),
                "symbol": symbol,
                "source": "observed_close",
            }
            if details.get("lean_symbol") == "NIFTY":
                underlying.append(point)
            else:
                bars_by_symbol[point["symbol"]].append(point)
        elif kind == "EXCHANGE_SESSION_CLOSE":
            boundaries.append(
                {
                    "trace_seq": seq,
                    "at": at,
                    "session_date": session,
                    "source": "observed",
                }
            )
    for order in orders.values():
        order["transitions"].sort(key=_sequence_key)
    fills.sort(key=_sequence_key)
    underlying.sort(key=_sequence_key)
    for values in bars_by_symbol.values():
        values.sort(key=_sequence_key)
    synchronized_underlying_times = {
        _time(point["at"]) for point in underlying
    }
    bars_by_symbol = {
        symbol: [
            point
            for point in values
            if _time(point["at"]) in synchronized_underlying_times
        ]
        for symbol, values in bars_by_symbol.items()
    }
    boundaries.sort(key=_sequence_key)
    _propagate_pair_ids(orders)
    return {
        "orders": orders,
        "fills": fills,
        "underlying": underlying,
        "bars_by_symbol": bars_by_symbol,
        "boundaries": boundaries,
    }


def _ensure_order(
    orders: dict[str, dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, Any]:
    order_id = str(payload["order_id"])
    order = orders.setdefault(
        order_id,
        {
            "order_id": order_id,
            "symbol": "",
            "side": "",
            "quantity": 0,
            "order_type": "",
            "validity": "DAY",
            "tag": "",
            "pair_id": None,
            "pair_linkage": "unpaired",
            "rejection_reason": None,
            "transitions": [],
        },
    )
    updates = {
        "symbol": payload.get("tradingsymbol"),
        "side": payload.get("transaction_type"),
        "quantity": payload.get("quantity"),
        "order_type": payload.get("order_type"),
        "validity": payload.get("validity"),
        "tag": payload.get("tag"),
        "pair_id": payload.get("bracket_id"),
        "rejection_reason": payload.get("rejection_reason"),
    }
    for key, value in updates.items():
        if value not in (None, ""):
            order[key] = value
    if payload.get("bracket_id"):
        order["pair_linkage"] = "observed"
    return order


def _order_transition(
    event: dict[str, Any],
    payload: dict[str, Any],
    *,
    status: str | None = None,
) -> dict[str, Any]:
    return {
        "trace_seq": int(event.get("seq", 0)),
        "at": str(event["at"]),
        "status": str(status or payload.get("status") or event.get("operation") or ""),
        "price": (
            None
            if payload.get("price") is None
            else str(payload.get("price"))
        ),
        "filled_quantity": int(payload.get("filled_quantity", 0)),
        "pending_quantity": int(payload.get("pending_quantity", 0)),
        "pair_id": payload.get("bracket_id"),
        "rejection_reason": payload.get("rejection_reason"),
        "source": "observed",
    }


def _propagate_pair_ids(orders: dict[str, dict[str, Any]]) -> None:
    pair_members: dict[str, set[str]] = defaultdict(set)
    for order in orders.values():
        if order["pair_id"]:
            pair_members[str(order["pair_id"])].add(order["order_id"])
            match = PAIR_PATTERN.match(str(order["pair_id"]))
            if match:
                pair_members[str(order["pair_id"])].update(match.groups())
    for pair_id, members in pair_members.items():
        for order_id in members:
            if order_id not in orders:
                continue
            order = orders[order_id]
            if order["pair_id"] is None:
                order["pair_id"] = pair_id
                order["pair_linkage"] = "derived_from_peer_transition"


def _fee_event_index(
    report: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, event in enumerate(report.get("fee_audit", {}).get("events", [])):
        result[str(event["order_id"])].append(
            {
                **copy.deepcopy(event),
                "fee_event_index": index,
            }
        )
    return result


def _enrich_fills(
    fills: list[dict[str, Any]],
    orders: dict[str, dict[str, Any]],
    fee_events: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    result = []
    fee_offsets: dict[str, int] = defaultdict(int)
    for fill in fills:
        order = orders.get(fill["order_id"])
        if order is None or not order["symbol"]:
            raise ValueError(f"Missing order metadata for fill {fill['order_id']}")
        offset = fee_offsets[fill["order_id"]]
        candidates = fee_events.get(fill["order_id"], [])
        fee = candidates[offset] if offset < len(candidates) else None
        if fee is None:
            raise ValueError(
                "Missing fee audit event for "
                f"order={fill['order_id']} trace_seq={fill['trace_seq']}"
            )
        fee_offsets[fill["order_id"]] += 1
        components = {
            key: Decimal(str(value))
            for key, value in (fee or {}).get("components", {}).items()
            if key != "total"
        }
        total = Decimal(str((fee or {}).get("components", {}).get("total", "0")))
        result.append(
            {
                **fill,
                "symbol": order["symbol"],
                "pair_id": order["pair_id"],
                "pair_linkage": order["pair_linkage"],
                "tag": order["tag"],
                "fee": total,
                "fee_components": components,
                "fee_event_index": (
                    None if fee is None else fee["fee_event_index"]
                ),
                "source": "observed",
            }
        )
    unmatched = {
        order_id: values[fee_offsets[order_id] :]
        for order_id, values in fee_events.items()
        if len(values) != fee_offsets[order_id]
    }
    return result, unmatched


def _contract_index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for lifecycle in report.get("contract_lifecycles", []):
        contract = lifecycle["contract"]
        result[contract["wave_trading_symbol"]] = copy.deepcopy(contract)
    for session in report.get("sessions", []):
        contract = session.get("contract")
        if contract:
            result[contract["wave_trading_symbol"]] = copy.deepcopy(contract)
    return result


def _missing_bar_keys(
    report: dict[str, Any],
    contracts: dict[str, dict[str, Any]],
) -> set[tuple[dt.datetime, str]]:
    symbols_by_instrument = {
        contract.get("instrument_id"): symbol
        for symbol, contract in contracts.items()
    }
    result = set()
    for item in report.get("missing_data", []):
        timestamp = item.get("timestamp")
        instrument_id = item.get("instrument_id")
        if not timestamp:
            continue
        symbol = symbols_by_instrument.get(instrument_id)
        if symbol is None and instrument_id and "NIFTY" in str(instrument_id):
            symbol = "NSE:NIFTY 50"
        if symbol:
            result.add((_time(str(timestamp)), symbol))
    return result


def _lifecycle_index(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    switch_keys = {
        (item["old_contract"]["wave_trading_symbol"], item["at"])
        for item in report.get("switches", [])
    }
    roll_keys = {
        (item["old_contract"]["wave_trading_symbol"], item["at"])
        for item in report.get("rolls", [])
    }
    for lifecycle in report.get("contract_lifecycles", []):
        event = lifecycle.get("close_event")
        if not event:
            continue
        symbol = lifecycle["contract"]["wave_trading_symbol"]
        key = (symbol, event["at"])
        kind = "switch" if key in switch_keys else "roll" if key in roll_keys else "lifecycle"
        result[symbol].append(
            {
                "kind": kind,
                "at": event["at"],
                "reason": event.get("reason") or kind,
                "price": str(event.get("close_price", event.get("price", "0"))),
                "quantity": int(event.get("quantity", 0)),
                "fees": str(event.get("fees", "0")),
                "order_id": (
                    None
                    if event.get("close_order_id", event.get("order_id")) is None
                    else str(event.get("close_order_id", event.get("order_id")))
                ),
                "source": "observed_report_lifecycle",
            }
        )
    for values in result.values():
        values.sort(key=lambda item: item["at"])
    return result


def _segment_episodes(
    fills: list[dict[str, Any]],
    contracts: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    inventory: dict[str, int] = defaultdict(int)
    active: dict[str, dict[str, Any]] = {}
    ordinals: dict[str, int] = defaultdict(int)
    episodes: list[dict[str, Any]] = []
    for fill in fills:
        symbol = fill["symbol"]
        before = inventory[symbol]
        signed = fill["quantity"] if fill["side"] == "BUY" else -fill["quantity"]
        quantities = _split_at_zero(before, signed)
        allocated = _allocate_fill_fee(fill, quantities)
        for piece in allocated:
            if inventory[symbol] == 0:
                ordinals[symbol] += 1
                active[symbol] = _new_episode(
                    symbol=symbol,
                    ordinal=ordinals[symbol],
                    contract=contracts.get(symbol, {"wave_trading_symbol": symbol}),
                    piece=piece,
                )
            episode = active[symbol]
            episode["_pieces"].append(piece)
            inventory[symbol] += piece["signed_quantity"]
            episode["_max_abs_inventory"] = max(
                episode["_max_abs_inventory"],
                abs(inventory[symbol]),
            )
            if inventory[symbol] == 0:
                episode["status"] = "closed_flat"
                episode["end_at"] = piece["at"]
                episode["_end_seq"] = piece["trace_seq"]
                episode["exit_reason"] = "inventory_returned_flat"
                episodes.append(episode)
                del active[symbol]
    episodes.extend(active.values())
    episodes.sort(key=lambda item: (item["start_at"], item["episode_id"]))
    return episodes


def _split_at_zero(before: int, signed: int) -> list[int]:
    if before == 0 or before * signed >= 0 or abs(signed) <= abs(before):
        return [signed]
    return [-before, signed + before]


def _allocate_fill_fee(
    fill: dict[str, Any],
    signed_quantities: list[int],
) -> list[dict[str, Any]]:
    total_quantity = sum(abs(value) for value in signed_quantities)
    remaining_fee = fill["fee"]
    remaining_components = dict(fill["fee_components"])
    result = []
    for index, signed in enumerate(signed_quantities):
        last = index == len(signed_quantities) - 1
        if last:
            fee = remaining_fee
            components = remaining_components
        else:
            ratio = Decimal(abs(signed)) / Decimal(total_quantity)
            fee = fill["fee"] * ratio
            components = {
                name: value * ratio
                for name, value in fill["fee_components"].items()
            }
            remaining_fee -= fee
            for name, value in components.items():
                remaining_components[name] -= value
        result.append(
            {
                **{
                    key: value
                    for key, value in fill.items()
                    if key not in {"fee", "fee_components"}
                },
                "quantity": abs(signed),
                "signed_quantity": signed,
                "side": "BUY" if signed > 0 else "SELL",
                "fee": fee,
                "fee_components": components,
                "split_index": index,
                "split_count": len(signed_quantities),
                "derived_split": len(signed_quantities) > 1,
            }
        )
    return result


def _new_episode(
    *,
    symbol: str,
    ordinal: int,
    contract: dict[str, Any],
    piece: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(
        str(contract.get("instrument_id", symbol)).encode("utf-8")
    ).hexdigest()[:10]
    return {
        "episode_id": f"episode-{digest}-{ordinal:03d}",
        "ordinal": ordinal,
        "symbol": symbol,
        "contract": copy.deepcopy(contract),
        "opening_direction": "LONG" if piece["signed_quantity"] > 0 else "SHORT",
        "start_at": piece["at"],
        "end_at": None,
        "status": "boundary_open",
        "exit_reason": "boundary_open",
        "_pieces": [],
        "_max_abs_inventory": 0,
        "_fallback": None,
        "_start_seq": piece["trace_seq"],
        "_end_seq": None,
    }


def _apply_lifecycle_fallbacks(
    episodes: list[dict[str, Any]],
    lifecycle: dict[str, list[dict[str, Any]]],
) -> set[str]:
    fallback_order_ids = set()
    for episode in episodes:
        if episode["status"] != "boundary_open":
            continue
        candidates = [
            event
            for event in lifecycle.get(episode["symbol"], [])
            if _time(event["at"]) >= _time(episode["start_at"])
        ]
        if not candidates:
            continue
        event = candidates[0]
        inventory = sum(piece["signed_quantity"] for piece in episode["_pieces"])
        if inventory == 0:
            continue
        quantity = abs(inventory)
        declared_quantity = int(event.get("quantity", 0))
        if declared_quantity and declared_quantity != quantity:
            continue
        fee = Decimal(str(event.get("fees", "0")))
        piece = {
            "trace_seq": None,
            "at": event["at"],
            "session_date": event["at"][:10],
            "order_id": event.get("order_id"),
            "side": "SELL" if inventory > 0 else "BUY",
            "quantity": quantity,
            "signed_quantity": -inventory,
            "price": event["price"],
            "complete": True,
            "cumulative_quantity": quantity,
            "symbol": episode["symbol"],
            "pair_id": None,
            "pair_linkage": "unpaired",
            "tag": f"DERIVED_{event['kind'].upper()}_FALLBACK",
            "fee": fee,
            "fee_components": {"total_unallocated": fee},
            "fee_event_index": None,
            "source": "derived_lifecycle_fallback",
            "split_index": 0,
            "split_count": 1,
            "derived_split": False,
        }
        episode["_pieces"].append(piece)
        episode["_fallback"] = event
        episode["status"] = "closed_lifecycle_fallback"
        episode["end_at"] = event["at"]
        episode["_end_seq"] = None
        episode["exit_reason"] = f"{event['kind']}:{event['reason']}"
        if event.get("order_id"):
            fallback_order_ids.add(str(event["order_id"]))
    return fallback_order_ids


def _validate_unmatched_fees(
    unmatched: dict[str, list[dict[str, Any]]],
    *,
    fallback_order_ids: set[str],
    lifecycle: dict[str, list[dict[str, Any]]],
) -> None:
    lifecycle_fees = {
        str(event["order_id"]): Decimal(str(event["fees"]))
        for values in lifecycle.values()
        for event in values
        if event.get("order_id")
    }
    unexpected = {
        order_id: len(events)
        for order_id, events in unmatched.items()
        if order_id not in fallback_order_ids
    }
    if unexpected:
        raise ValueError(f"Unmatched fee audit events: {unexpected}")
    for order_id in fallback_order_ids:
        events = unmatched.get(order_id, [])
        if not events:
            continue
        if len(events) != 1:
            raise ValueError(
                f"Lifecycle fallback order {order_id} has {len(events)} fee events"
            )
        fee_total = Decimal(str(events[0]["components"]["total"]))
        if fee_total != lifecycle_fees[order_id]:
            raise ValueError(
                f"Lifecycle fallback fee mismatch for order {order_id}: "
                f"audit={fee_total} lifecycle={lifecycle_fees[order_id]}"
            )


def _finalize_episodes(
    episodes: list[dict[str, Any]],
    *,
    report: dict[str, Any],
    orders: dict[str, dict[str, Any]],
    bars_by_symbol: dict[str, list[dict[str, Any]]],
    boundaries: list[dict[str, Any]],
    lifecycle: dict[str, list[dict[str, Any]]],
) -> None:
    boundary_at = _boundary_at(report)
    forced = {
        item["symbol"]: item
        for item in report.get("forced_close_sensitivity", {}).get("exits", [])
    }
    capital_curve = report.get("capital", {}).get("curve", [])
    lifecycle_by_key = {
        (symbol, _time(event["at"])): event
        for symbol, values in lifecycle.items()
        for event in values
    }
    for episode in episodes:
        pieces = episode["_pieces"]
        effective_end = episode["end_at"] or boundary_at
        historical_fee = sum((piece["fee"] for piece in pieces), Decimal("0"))
        cash_flow = sum(
            (
                -Decimal(piece["signed_quantity"]) * Decimal(str(piece["price"]))
                for piece in pieces
            ),
            Decimal("0"),
        )
        ending_quantity = sum(piece["signed_quantity"] for piece in pieces)
        bars = [
            point
            for point in bars_by_symbol.get(episode["symbol"], [])
            if _time(episode["start_at"])
            <= _time(point["at"])
            <= _time(effective_end)
        ]
        boundary_mark = Decimal(bars[-1]["close"]) if bars else None
        boundary_gross = (
            None
            if boundary_mark is None
            else cash_flow + Decimal(ending_quantity) * boundary_mark
        )
        boundary_net = (
            None if boundary_gross is None else boundary_gross - historical_fee
        )
        virtual = None
        if episode["status"] == "boundary_open" and episode["symbol"] in forced:
            exit_event = forced[episode["symbol"]]
            exit_fee = Decimal(str(exit_event["exit_fee"]))
            virtual_cash = Decimal(ending_quantity) * Decimal(
                str(exit_event["price"])
            )
            virtual_gross = cash_flow + virtual_cash
            virtual = {
                "kind": "virtual_forced_close",
                "source": "report_forced_close_sensitivity",
                "observed": False,
                "at": exit_event["at"],
                "side": exit_event["side"],
                "quantity": int(exit_event["quantity"]),
                "price": str(exit_event["price"]),
                "fee": str(exit_fee),
                "fee_components": copy.deepcopy(exit_event["fee_components"]),
                "gross_pnl": str(virtual_gross),
                "net_pnl": str(virtual_gross - historical_fee - exit_fee),
            }
        pnl_curve = _episode_pnl_curve(
            pieces,
            bars,
            virtual=virtual,
        )
        inventory_curve = _inventory_curve(pieces)
        if (
            inventory_curve
            and inventory_curve[-1]["at"] != effective_end
        ):
            inventory_curve.append(
                {
                    "at": effective_end,
                    "trace_seq": None,
                    "quantity": ending_quantity,
                    "fill_side": None,
                    "fill_quantity": 0,
                    "risk_effect": "state_continuation",
                    "source": "derived",
                }
            )
        known_net = [
            Decimal(point["net_pnl"])
            for point in pnl_curve
            if point.get("net_pnl") is not None
            and point.get("source") != "virtual"
        ]
        all_fill_order_ids = {
            str(piece["order_id"]) for piece in pieces if piece.get("order_id")
        }
        fill_order_ids = {
            order_id
            for order_id in all_fill_order_ids
            if order_id in orders
        }
        report_only_order_ids = sorted(
            all_fill_order_ids - fill_order_ids,
            key=_order_sort_key,
        )
        fill_pair_ids = {
            str(piece["pair_id"])
            for piece in pieces
            if piece.get("pair_id")
        }
        related_order_ids = {
            order_id
            for order_id, order in orders.items()
            if order["symbol"] == episode["symbol"]
            and (
                order["pair_id"] in fill_pair_ids
                or _order_overlaps_episode(
                    order,
                    start_seq=episode["_start_seq"],
                    end_seq=episode["_end_seq"],
                )
            )
        }
        order_ids = sorted(
            fill_order_ids | related_order_ids,
            key=_order_sort_key,
        )
        pair_ids = sorted(
            {
                str(orders[order_id]["pair_id"])
                for order_id in order_ids
                if orders[order_id].get("pair_id")
            }
        )
        close_event = lifecycle_by_key.get(
            (episode["symbol"], _time(effective_end))
        )
        if close_event and episode["status"] == "closed_flat":
            episode["exit_reason"] = f"{close_event['kind']}:{close_event['reason']}"
        episode.update(
            {
                "end_at": effective_end,
                "duration_seconds": int(
                    (
                        dt.datetime.fromisoformat(effective_end)
                        - dt.datetime.fromisoformat(episode["start_at"])
                    ).total_seconds()
                ),
                "ending_quantity": ending_quantity,
                "source_fill_count": len(
                    {piece["trace_seq"] for piece in pieces if piece["trace_seq"]}
                ),
                "fill_piece_count": len(pieces),
                "order_count": len(order_ids) + len(report_only_order_ids),
                "pair_count": len(pair_ids),
                "order_ids": order_ids,
                "report_only_order_ids": report_only_order_ids,
                "pair_ids": pair_ids,
                "max_abs_inventory": episode["_max_abs_inventory"],
                "overnight_count": sum(
                    _time(episode["start_at"]) < _time(item["at"])
                    and item["at"][:10] < effective_end[:10]
                    for item in boundaries
                ),
                "historical_gross_pnl": (
                    str(cash_flow) if ending_quantity == 0 else None
                ),
                "historical_fees": str(historical_fee),
                "historical_net_pnl": (
                    str(cash_flow - historical_fee)
                    if ending_quantity == 0
                    else None
                ),
                "boundary_mark": (
                    None if boundary_mark is None else str(boundary_mark)
                ),
                "boundary_gross_pnl": (
                    None if boundary_gross is None else str(boundary_gross)
                ),
                "boundary_net_pnl": (
                    None if boundary_net is None else str(boundary_net)
                ),
                "virtual_forced_close": virtual,
                "display_net_pnl": (
                    virtual["net_pnl"]
                    if virtual
                    else str(cash_flow - historical_fee)
                    if ending_quantity == 0
                    else None
                ),
                "close_mark_mfe": (
                    None if not known_net else str(max(known_net))
                ),
                "close_mark_mae": (
                    None if not known_net else str(min(known_net))
                ),
                "pnl_curve": pnl_curve,
                "inventory_curve": inventory_curve,
                "fills": [_serialize_piece(piece) for piece in pieces],
                "peak_capital": _episode_peak_capital(
                    capital_curve,
                    episode["start_at"],
                    effective_end,
                ),
                "source_event_refs": [
                    {
                        "trace_seq": piece["trace_seq"],
                        "order_id": piece.get("order_id"),
                        "fee_event_index": piece.get("fee_event_index"),
                        "source": piece["source"],
                        "split_index": piece["split_index"],
                    }
                    for piece in pieces
                ],
            }
        )
        episode.pop("_pieces")
        episode.pop("_max_abs_inventory")
        episode.pop("_fallback")
        episode.pop("_start_seq")
        episode.pop("_end_seq")


def _episode_pnl_curve(
    pieces: list[dict[str, Any]],
    bars: list[dict[str, Any]],
    *,
    virtual: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    events = [
        {
            "kind": "bar",
            "at": bar["at"],
            "trace_seq": bar["trace_seq"],
            "mark": Decimal(str(bar["close"])),
            "source": "observed_close",
        }
        for bar in bars
    ]
    events.extend(
        {
            "kind": "fill",
            "at": piece["at"],
            "trace_seq": piece["trace_seq"] or 10**18,
            "piece": piece,
            "source": piece["source"],
        }
        for piece in pieces
    )
    events.sort(
        key=lambda item: (
            dt.datetime.fromisoformat(item["at"]),
            item["trace_seq"],
            0 if item["kind"] == "bar" else 1,
        )
    )
    quantity = 0
    average = Decimal("0")
    realized = Decimal("0")
    fees = Decimal("0")
    mark: Decimal | None = None
    points = []
    for event in events:
        if event["kind"] == "bar":
            mark = event["mark"]
        else:
            piece = event["piece"]
            signed = int(piece["signed_quantity"])
            price = Decimal(str(piece["price"]))
            if quantity == 0 or quantity * signed > 0:
                average = (
                    average * abs(quantity) + price * abs(signed)
                ) / abs(quantity + signed)
            else:
                closing = min(abs(quantity), abs(signed))
                realized += (
                    (price - average) * closing
                    if quantity > 0
                    else (average - price) * closing
                )
                if quantity + signed == 0:
                    average = Decimal("0")
            quantity += signed
            fees += piece["fee"]
            if mark is None:
                mark = price
        unrealized = (
            None if mark is None else (mark - average) * quantity
        )
        gross = None if unrealized is None else realized + unrealized
        points.append(
            {
                "at": event["at"],
                "trace_seq": event["trace_seq"],
                "event": event["kind"],
                "source": event["source"],
                "quantity": quantity,
                "mark": None if mark is None else str(mark),
                "realized_pnl": str(realized),
                "unrealized_pnl": (
                    None if unrealized is None else str(unrealized)
                ),
                "gross_pnl": None if gross is None else str(gross),
                "cumulative_fees": str(fees),
                "net_pnl": None if gross is None else str(gross - fees),
            }
        )
    if virtual:
        points.append(
            {
                "at": virtual["at"],
                "trace_seq": None,
                "event": "virtual_forced_close",
                "source": "virtual",
                "quantity": 0,
                "mark": virtual["price"],
                "realized_pnl": virtual["gross_pnl"],
                "unrealized_pnl": "0",
                "gross_pnl": virtual["gross_pnl"],
                "cumulative_fees": str(
                    fees + Decimal(str(virtual["fee"]))
                ),
                "net_pnl": virtual["net_pnl"],
            }
        )
    return _consolidate_curve(points)


def _consolidate_curve(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {
        (point["at"], point["event"], point.get("trace_seq")): point
        for point in points
    }
    return sorted(
        by_key.values(),
        key=lambda item: (
            dt.datetime.fromisoformat(item["at"]),
            item.get("trace_seq") or 10**18,
            item["event"],
        ),
    )


def _inventory_curve(pieces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    quantity = 0
    result = []
    for piece in pieces:
        before = quantity
        quantity += piece["signed_quantity"]
        result.append(
            {
                "at": piece["at"],
                "trace_seq": piece["trace_seq"],
                "quantity": quantity,
                "fill_side": piece["side"],
                "fill_quantity": piece["quantity"],
                "risk_effect": (
                    "risk_adding"
                    if abs(quantity) > abs(before)
                    else "risk_reducing"
                ),
                "source": piece["source"],
            }
        )
    return result


def _episode_peak_capital(
    capital_curve: list[dict[str, Any]],
    start: str,
    end: str,
) -> dict[str, Any]:
    points = [
        point
        for point in capital_curve
        if _time(start) <= _time(point["at"]) <= _time(end)
    ]
    result = {}
    for metric in (
        "capital_used_excluding_margin",
        "capital_used_including_margin",
        "capital_recommended",
    ):
        available = [point for point in points if point.get(metric) is not None]
        if not available:
            result[metric] = None
            continue
        peak = max(available, key=lambda point: Decimal(str(point[metric])))
        result[metric] = {"value": peak[metric], "at": peak["at"]}
    return result


def _finalize_orders(
    orders: dict[str, dict[str, Any]],
    *,
    fills: list[dict[str, Any]],
    boundaries: list[dict[str, Any]],
    lifecycle: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fills_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        fills_by_order[fill["order_id"]].append(fill)
    boundary_times = {item["at"] for item in boundaries}
    lifecycle_times = {
        (symbol, item["at"])
        for symbol, values in lifecycle.items()
        for item in values
    }
    pair_members: dict[str, list[str]] = defaultdict(list)
    result = []
    for order_id in sorted(orders, key=_order_sort_key):
        order = copy.deepcopy(orders[order_id])
        transitions = order["transitions"]
        terminal = next(
            (
                item
                for item in reversed(transitions)
                if item["status"] in TERMINAL_ORDER_STATUSES
            ),
            None,
        )
        cancel_cause = None
        cancel_cause_source = None
        if terminal and terminal["status"] == "CANCELLED":
            sibling_filled = False
            if order["pair_id"]:
                sibling_filled = any(
                    fill["pair_id"] == order["pair_id"]
                    and fill["order_id"] != order_id
                    and fill["at"] == terminal["at"]
                    and fill["complete"]
                    for fill in fills
                )
            if sibling_filled:
                cancel_cause = "sibling_filled"
                cancel_cause_source = "derived"
            elif terminal["at"] in boundary_times and order["validity"] == "DAY":
                cancel_cause = "day_expiry"
                cancel_cause_source = "derived"
            elif (order["symbol"], terminal["at"]) in lifecycle_times:
                cancel_cause = "contract_lifecycle_close"
                cancel_cause_source = "derived"
            else:
                cancel_cause = "unknown"
                cancel_cause_source = "unavailable"
        order["terminal_status"] = None if terminal is None else terminal["status"]
        order["terminal_at"] = None if terminal is None else terminal["at"]
        order["cancel_cause"] = cancel_cause
        order["cancel_cause_source"] = cancel_cause_source
        order["fills"] = [
            _serialize_fill_reference(fill)
            for fill in fills_by_order.get(order_id, [])
        ]
        order["limit_points"] = _limit_points(transitions)
        if order["pair_id"]:
            pair_members[str(order["pair_id"])].append(order_id)
        result.append(order)
    pairs = []
    for pair_id in sorted(pair_members):
        members = sorted(set(pair_members[pair_id]), key=_order_sort_key)
        member_orders = [orders[order_id] for order_id in members]
        pairs.append(
            {
                "pair_id": pair_id,
                "order_ids": members,
                "symbol": next(
                    (order["symbol"] for order in member_orders if order["symbol"]),
                    "",
                ),
                "linkage": (
                    "observed_and_backfilled"
                    if any(
                        order["pair_linkage"] == "derived_from_peer_transition"
                        for order in member_orders
                    )
                    else "observed"
                ),
                "source": "observed_bracket_id_with_deterministic_backfill",
            }
        )
    return result, pairs


def _limit_points(transitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: dict[str, dict[str, Any]] = {}
    last_price = None
    for transition in transitions:
        if transition["price"] is not None:
            last_price = transition["price"]
        if last_price is None:
            continue
        if (
            transition["status"] in ACTIVE_ORDER_STATUSES
            or transition["status"] in TERMINAL_ORDER_STATUSES
        ):
            points[transition["at"]] = {
                "at": transition["at"],
                "price": last_price,
                "status": transition["status"],
                "source": "observed",
            }
    return [points[key] for key in sorted(points)]


def _selection_markers(report: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for session in report.get("sessions", []):
        decision = session.get("contract_decision") or {}
        contract = session.get("contract")
        if not contract or not decision.get("at"):
            continue
        result.append(
            {
                "at": decision["at"],
                "action": decision.get("action"),
                "reason": decision.get("reason"),
                "symbol": contract["wave_trading_symbol"],
                "strike": contract["strike"],
                "expiry": contract["expiry"],
                "source": "observed_report",
            }
        )
    return result


def _lifecycle_markers(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "at": event["at"],
            "kind": kind,
            "reason": event.get("reason") or kind,
            "symbol": event["old_contract"]["wave_trading_symbol"],
            "source": "observed_report",
        }
        for kind, values in (
            ("switch", report.get("switches", [])),
            ("roll", report.get("rolls", [])),
        )
        for event in values
    ]


def _missing_markers(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "at": item["timestamp"],
            "reason": item.get("reason"),
            "instrument_id": item.get("instrument_id"),
            "source": "observed_report",
        }
        for item in report.get("missing_data", [])
        if item.get("timestamp")
    ]


def _boundary_at(report: dict[str, Any]) -> str:
    points = report.get("equity_curve", [])
    known = [point["at"] for point in points if point.get("at")]
    forced = report.get("forced_close_sensitivity", {}).get("exits", [])
    known.extend(item["at"] for item in forced if item.get("at"))
    if not known:
        raise ValueError("Replay report has no boundary timestamp")
    return max(known)


def _serialize_piece(piece: dict[str, Any]) -> dict[str, Any]:
    return {
        **{
            key: value
            for key, value in piece.items()
            if key not in {"fee", "fee_components"}
        },
        "fee": str(piece["fee"]),
        "fee_components": {
            name: str(value)
            for name, value in sorted(piece["fee_components"].items())
        },
    }


def _serialize_fill_reference(fill: dict[str, Any]) -> dict[str, Any]:
    return {
        "trace_seq": fill["trace_seq"],
        "at": fill["at"],
        "side": fill["side"],
        "quantity": fill["quantity"],
        "price": fill["price"],
        "complete": fill["complete"],
        "source": "observed",
    }


def _sequence_key(item: dict[str, Any]) -> tuple[int, str]:
    return int(item.get("trace_seq") or 0), str(item.get("at", ""))


def _order_overlaps_episode(
    order: dict[str, Any],
    *,
    start_seq: int | None,
    end_seq: int | None,
) -> bool:
    sequences = [
        int(item["trace_seq"])
        for item in order["transitions"]
        if item.get("trace_seq") is not None
    ]
    if not sequences or start_seq is None:
        return False
    effective_end = 10**18 if end_seq is None else int(end_seq)
    return min(sequences) <= effective_end and max(sequences) >= int(start_seq)


def _time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def _continuous_trace_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            digest.update((canonical_json(event) + "\n").encode("utf-8"))
    return digest.hexdigest()


def _order_sort_key(value: str) -> tuple[int, str]:
    return (0, f"{int(value):020d}") if str(value).isdigit() else (1, str(value))
