"""Deterministic broker truth separated from legacy Wave module globals."""

from __future__ import annotations

import dataclasses
import datetime as dt
import copy
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable

from .faults import FaultScript, raise_failure
from .fill_models import close_only_fill, validate_sibling_bracket
from .normalize import state_hash
from .schemas import MarketBar, VisibilityOffsets
from .scheduler import DeterministicScheduler
from .tracing import TraceRecorder

TERMINAL = {"COMPLETE", "CANCELLED", "REJECTED"}


@dataclasses.dataclass
class BrokerOrder:
    order_id: str
    symbol: str
    instrument_token: int
    exchange: str
    side: str
    quantity: int
    product: str
    order_type: str
    price: Decimal
    tag: str
    created_at: dt.datetime
    status: str = "OPEN"
    filled_quantity: int = 0
    average_price: Decimal = Decimal("0")
    variety: str = "regular"
    bracket_id: str | None = None
    history: list[dict[str, Any]] = dataclasses.field(default_factory=list)

    @property
    def remaining(self) -> int:
        return self.quantity - self.filled_quantity

    def api_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "tradingsymbol": self.symbol,
            "instrument_token": self.instrument_token,
            "exchange": self.exchange,
            "transaction_type": self.side,
            "quantity": self.quantity,
            "filled_quantity": self.filled_quantity,
            "pending_quantity": self.remaining,
            "product": self.product,
            "order_type": self.order_type,
            "price": float(self.price),
            "average_price": float(self.average_price),
            "status": self.status,
            "tag": self.tag,
            "variety": self.variety,
            "bracket_id": self.bracket_id,
        }


@dataclasses.dataclass
class GttTrigger:
    trigger_id: int
    symbol: str
    exchange: str
    trigger_values: tuple[Decimal, ...]
    orders: list[dict[str, Any]]
    tag: str
    status: str = "active"


class FeeSchedule:
    """Small portable approximation consuming exported Upstox fee inputs."""

    def __init__(self, value: dict[str, Any]) -> None:
        self.value = value
        if (
            value.get("schema") != "wave-replay-fee-schedule-v1"
            or value.get("usage_label") != "upstox_approximation"
            or value.get("broker_fidelity") != "not_zerodha"
        ):
            raise ValueError("fee schedule must declare the Upstox approximation contract")

    def calculate(
        self,
        *,
        side: str,
        quantity: int,
        price: Decimal,
        include_brokerage: bool = True,
    ) -> Decimal:
        turnover = Decimal(quantity) * price
        resolved = self.value["resolved_schedule"]
        broker = resolved["broker"]
        statutory = resolved["statutory"]
        brokerage_flat = (
            Decimal(str(broker["option_brokerage"]))
            if turnover and include_brokerage
            else Decimal("0")
        )
        stt_rate = Decimal(str(statutory["option_stt_rate"])) if side == "SELL" else Decimal("0")
        exchange_rate = Decimal(str(statutory["option_exchange_rate"]))
        sebi_rate = Decimal(str(statutory["sebi_rate"]))
        stamp_rate = Decimal(str(statutory["option_stamp_rate"])) if side == "BUY" else Decimal("0")
        gst_rate = Decimal(str(statutory["gst_rate"]))
        exchange = turnover * exchange_rate
        sebi = turnover * sebi_rate
        subtotal = brokerage_flat + exchange + sebi
        total = subtotal + subtotal * gst_rate + turnover * stt_rate + turnover * stamp_rate
        return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class BrokerEngine:
    def __init__(
        self,
        *,
        scheduler: DeterministicScheduler,
        trace: TraceRecorder,
        starting_cash: Decimal = Decimal("1000000"),
        visibility: VisibilityOffsets | None = None,
        faults: FaultScript | None = None,
        fee_schedule: FeeSchedule | None = None,
    ) -> None:
        self.scheduler = scheduler
        self.trace = trace
        self.cash = starting_cash
        self.visibility = visibility or VisibilityOffsets()
        self.faults = faults or FaultScript()
        self.fee_schedule = fee_schedule
        self.orders_by_id: dict[str, BrokerOrder] = {}
        self.gtts_by_id: dict[int, GttTrigger] = {}
        self.positions_by_symbol: dict[str, dict[str, Any]] = {}
        self.latest_quotes: dict[str, dict[str, Any]] = {}
        self._order_counter = 0
        self._trigger_counter = 0
        self._callback_dispatch: Callable[[dict[str, Any], float, int], None] | None = None
        self._visible_positions: dict[str, dict[str, Any]] = {}
        self.instrument_tokens: dict[str, int] = {}
        self.fees_paid = Decimal("0")
        self.realized_pnl = Decimal("0")
        self._brokerage_charged_orders: set[str] = set()
        self.fill_events: list[dict[str, Any]] = []

    def bind_callback_dispatch(self, callback: Callable[[dict[str, Any], float, int], None]) -> None:
        self._callback_dispatch = callback

    def set_quote(self, key: str, *, last_price: Decimal, instrument_token: int) -> None:
        self.latest_quotes[key] = {"last_price": float(last_price), "instrument_token": instrument_token}
        symbol = key.split(":", 1)[-1]
        self.instrument_tokens[symbol] = instrument_token

    def quote(self, instruments: str | list[str]) -> dict[str, dict[str, Any]]:
        keys = [instruments] if isinstance(instruments, str) else list(instruments)
        missing = [key for key in keys if key not in self.latest_quotes]
        if missing:
            raise KeyError(f"No completed quote available for: {missing}")
        return {key: dict(self.latest_quotes[key]) for key in keys}

    def place_order(self, **request: Any) -> str:
        failure = self.faults.pop("place_order")
        if failure and not failure.apply_side_effect:
            raise_failure(failure)
        self._order_counter += 1
        order_id = str(self._order_counter)
        symbol = str(request["tradingsymbol"])
        order = BrokerOrder(
            order_id=order_id,
            symbol=symbol,
            instrument_token=int(
                request.get("instrument_token")
                or self.instrument_tokens.get(symbol, 0)
            ),
            exchange=str(request["exchange"]),
            side=str(request["transaction_type"]).upper(),
            quantity=int(request["quantity"]),
            product=str(request["product"]),
            order_type=str(request["order_type"]),
            price=Decimal(str(request.get("price", 0))),
            tag=str(request.get("tag", "")),
            created_at=self.scheduler.now,
            variety=str(request.get("variety", "regular")),
            bracket_id=request.get("bracket_id"),
        )
        self.orders_by_id[order_id] = order
        if order.bracket_id is None:
            sibling = next(
                (
                    candidate
                    for candidate in reversed(list(self.orders_by_id.values()))
                    if candidate.order_id != order.order_id
                    and candidate.symbol == order.symbol
                    and candidate.side != order.side
                    and candidate.bracket_id is None
                    and candidate.status in {"OPEN", "ACK"}
                ),
                None,
            )
            if sibling is not None:
                pair_id = f"pair-{sibling.order_id}-{order.order_id}"
                sibling.bracket_id = pair_id
                order.bracket_id = pair_id
        self._transition(order, "ACK", callback_delay=self.visibility.callback_seconds)
        self._transition(order, "OPEN", callback_delay=self.visibility.callback_seconds)
        self.trace.add(
            "BROKER_CALL",
            at=self.scheduler.now,
            operation="place_order",
            details={"order": order.api_dict(), "state_hash": self.snapshot_hash()},
        )
        if failure:
            raise_failure(failure)
        return order_id

    def cancel_order(self, **request: Any) -> str:
        failure = self.faults.pop("cancel_order")
        order = self.orders_by_id[str(request["order_id"])]
        if failure and not failure.apply_side_effect:
            raise_failure(failure)
        if order.status not in TERMINAL:
            self._transition(order, "CANCELLED", callback_delay=self.visibility.callback_seconds)
        self.trace.add(
            "BROKER_CALL",
            at=self.scheduler.now,
            operation="cancel_order",
            details={"order_id": order.order_id, "status": order.status},
        )
        if failure:
            raise_failure(failure)
        return order.order_id

    def modify_order(self, *args: Any, **request: Any) -> str:
        failure = self.faults.pop("modify_order")
        if args:
            request.setdefault("variety", args[0])
            request.setdefault("order_id", args[1])
        order = self.orders_by_id[str(request["order_id"])]
        if failure and not failure.apply_side_effect:
            raise_failure(failure)
        if order.status in TERMINAL:
            raise RuntimeError(f"Cannot modify terminal order {order.order_id}")
        if "price" in request:
            order.price = Decimal(str(request["price"]))
        if "quantity" in request:
            quantity = int(request["quantity"])
            if quantity < order.filled_quantity:
                raise ValueError("quantity below filled quantity")
            order.quantity = quantity
        self._transition(order, "OPEN", callback_delay=self.visibility.callback_seconds)
        self.trace.add(
            "BROKER_CALL",
            at=self.scheduler.now,
            operation="modify_order",
            details={"order": order.api_dict()},
        )
        if failure:
            raise_failure(failure)
        return order.order_id

    def reject(self, order_id: str, *, reason: str = "scripted reject") -> None:
        order = self.orders_by_id[str(order_id)]
        if order.status in TERMINAL:
            raise RuntimeError(f"Cannot reject terminal order {order.order_id}")
        self._transition(order, "REJECTED", callback_delay=self.visibility.callback_seconds, reason=reason)

    def partial_fill(self, order_id: str, *, quantity: int, price: Decimal) -> None:
        order = self.orders_by_id[str(order_id)]
        if order.status not in {"OPEN", "PARTIAL"}:
            raise RuntimeError(f"Cannot partially fill order in state {order.status}")
        if quantity <= 0 or quantity >= order.remaining:
            raise ValueError("Partial fill quantity must be positive and below remaining quantity")
        self._apply_fill(order, quantity=quantity, price=price, complete=False)

    def evaluate_close_bar(self, bar: MarketBar, *, fill_price_mode: str = "limit") -> list[str]:
        self.set_quote(
            f"{bar.exchange}:{bar.symbol}",
            last_price=bar.close,
            instrument_token=self._instrument_token_for(bar.symbol),
        )
        self._mark_to_market(bar.symbol, bar.close)
        active = [
            order
            for order in self.orders_by_id.values()
            if order.symbol == bar.symbol and order.status in {"OPEN", "PARTIAL"}
        ]
        brackets: dict[str, list[BrokerOrder]] = defaultdict(list)
        for order in active:
            bracket_key = self._bracket_key(order)
            if bracket_key is not None:
                brackets[bracket_key].append(order)
        for orders in brackets.values():
            buys = [order.price for order in orders if order.side == "BUY"]
            sells = [order.price for order in orders if order.side == "SELL"]
            if buys and sells:
                validate_sibling_bracket(max(buys), min(sells))
        filled: list[str] = []
        for order in sorted(self.orders_by_id.values(), key=lambda item: int(item.order_id)):
            if order.symbol != bar.symbol or order.status not in {"OPEN", "PARTIAL"}:
                continue
            decision = close_only_fill(
                side=order.side,
                limit_price=order.price,
                close=bar.close,
                created_at_bar_end=order.created_at,
                current_bar_end=bar.bar_end,
                fill_price_mode=fill_price_mode,
            )
            self.trace.add(
                "FILL_DECISION",
                at=self.scheduler.now,
                operation=order.order_id,
                details={
                    "trigger_mode": "close_only",
                    "bar_end": bar.bar_end.isoformat(),
                    "close": str(bar.close),
                    "limit": str(order.price),
                    "touched": decision.touched,
                    "reason": decision.reason,
                },
            )
            if decision.touched and decision.fill_price is not None:
                self._apply_fill(order, quantity=order.remaining, price=decision.fill_price, complete=True)
                filled.append(order.order_id)
        return filled

    def place_gtt(self, **request: Any) -> dict[str, int]:
        failure = self.faults.pop("place_gtt")
        if failure and not failure.apply_side_effect:
            raise_failure(failure)
        self._trigger_counter += 1
        orders = [dict(value) for value in request["orders"]]
        trigger = GttTrigger(
            trigger_id=self._trigger_counter,
            symbol=str(request["tradingsymbol"]),
            exchange=str(request["exchange"]),
            trigger_values=tuple(Decimal(str(value)) for value in request["trigger_values"]),
            orders=orders,
            tag=str(orders[0].get("tag", "")),
        )
        self.gtts_by_id[trigger.trigger_id] = trigger
        self.trace.add(
            "BROKER_CALL",
            at=self.scheduler.now,
            operation="place_gtt",
            details={"trigger_id": trigger.trigger_id, "symbol": trigger.symbol, "tag": trigger.tag},
        )
        if failure:
            raise_failure(failure)
        return {"trigger_id": trigger.trigger_id}

    def delete_gtt(self, trigger_id: int) -> int:
        failure = self.faults.pop("delete_gtt")
        if failure and not failure.apply_side_effect:
            raise_failure(failure)
        trigger = self.gtts_by_id[int(trigger_id)]
        trigger.status = "deleted"
        self.trace.add(
            "BROKER_CALL",
            at=self.scheduler.now,
            operation="delete_gtt",
            details={"trigger_id": trigger.trigger_id},
        )
        if failure:
            raise_failure(failure)
        return trigger.trigger_id

    def trigger_gtt(self, trigger_id: int, *, immediate_complete: bool = False) -> str:
        trigger = self.gtts_by_id[int(trigger_id)]
        if trigger.status != "active":
            raise RuntimeError(f"GTT {trigger_id} is not active")
        trigger.status = "triggered"
        template = trigger.orders[0]
        order_id = self.place_order(
            variety="regular",
            exchange=trigger.exchange,
            tradingsymbol=trigger.symbol,
            transaction_type=template["transaction_type"],
            quantity=template["quantity"],
            product=template["product"],
            order_type=template.get("order_type", "LIMIT"),
            price=template.get("price", 0),
            tag=template.get("tag", trigger.tag),
        )
        if immediate_complete:
            order = self.orders_by_id[order_id]
            self._apply_fill(order, quantity=order.remaining, price=order.price, complete=True)
        return order_id

    def orders(self) -> list[dict[str, Any]]:
        result = []
        for order_id in sorted(self.orders_by_id, key=int):
            order = self.orders_by_id[order_id]
            visible = [entry for entry in order.history if entry["orders_visible"]]
            if visible:
                result.append(dict(visible[-1]["data"]))
        return result

    def order_history(self, order_id: str) -> list[dict[str, Any]]:
        return [
            dict(entry["data"])
            for entry in self.orders_by_id[str(order_id)].history
            if entry["history_visible"]
        ]

    def positions(self) -> dict[str, list[dict[str, Any]]]:
        positions = [dict(value) for _, value in sorted(self._visible_positions.items())]
        return {"net": positions, "day": positions}

    def get_gtts(self) -> list[dict[str, Any]]:
        return [
            {
                "id": trigger.trigger_id,
                "status": trigger.status,
                "condition": {"tradingsymbol": trigger.symbol, "trigger_values": [float(v) for v in trigger.trigger_values]},
                "orders": [dict(order) for order in trigger.orders],
            }
            for trigger in sorted(self.gtts_by_id.values(), key=lambda item: item.trigger_id)
        ]

    def snapshot(self) -> dict[str, Any]:
        return {
            "cash": str(self.cash),
            "fees_paid": str(self.fees_paid),
            "orders": {key: value.api_dict() for key, value in sorted(self.orders_by_id.items())},
            "positions": self.positions_by_symbol,
            "gtts": {key: dataclasses.asdict(value) for key, value in sorted(self.gtts_by_id.items())},
        }

    def snapshot_hash(self) -> str:
        return state_hash(self.snapshot())

    def _transition(
        self,
        order: BrokerOrder,
        status: str,
        *,
        callback_delay: float,
        reason: str | None = None,
    ) -> None:
        before = self.snapshot_hash()
        order.status = status
        data = order.api_dict()
        if reason:
            data["status_message"] = reason
        entry = {
            "history_visible": False,
            "orders_visible": False,
            "data": copy.deepcopy(data),
        }
        order.history.append(entry)
        self.scheduler.schedule_after(
            self.visibility.order_history_seconds,
            lambda entry=entry, order_id=order.order_id: self._publish_order_view(
                order_id, entry, "history_visible"
            ),
            priority=self.visibility.order_history_priority,
        )
        self.scheduler.schedule_after(
            self.visibility.orders_seconds,
            lambda entry=entry, order_id=order.order_id: self._publish_order_view(
                order_id, entry, "orders_visible"
            ),
            priority=self.visibility.orders_priority,
        )
        self.trace.add(
            "BROKER_ORDER_TRANSITION",
            at=self.scheduler.now,
            actor=f"order-{order.order_id}",
            operation=status,
            details=data,
            input_data={"order_id": order.order_id, "target_status": status},
            result={"status": status},
            state_before=before,
            state_after=self.snapshot_hash(),
        )
        if self._callback_dispatch is not None:
            self._callback_dispatch(
                copy.deepcopy(data),
                callback_delay,
                self.visibility.callback_priority,
            )

    def _apply_fill(self, order: BrokerOrder, *, quantity: int, price: Decimal, complete: bool) -> None:
        if order.status not in {"OPEN", "PARTIAL"}:
            raise RuntimeError(f"Cannot fill order in state {order.status}")
        if quantity <= 0 or quantity > order.remaining:
            raise ValueError("Fill quantity must be positive and within remaining quantity")
        prior_value = order.average_price * order.filled_quantity
        order.filled_quantity += quantity
        order.average_price = (prior_value + price * quantity) / order.filled_quantity
        include_brokerage = order.order_id not in self._brokerage_charged_orders
        fee = (
            self.fee_schedule.calculate(
                side=order.side,
                quantity=quantity,
                price=price,
                include_brokerage=include_brokerage,
            )
            if self.fee_schedule
            else Decimal("0")
        )
        if include_brokerage and self.fee_schedule:
            self._brokerage_charged_orders.add(order.order_id)
        self.fees_paid += fee
        signed = quantity if order.side == "BUY" else -quantity
        self.cash += (-price * quantity if order.side == "BUY" else price * quantity) - fee
        position = self.positions_by_symbol.setdefault(
            order.symbol,
            {
                "tradingsymbol": order.symbol,
                "instrument_token": order.instrument_token,
                "quantity": 0,
                "buy_quantity": 0,
                "sell_quantity": 0,
                "buy_price": 0.0,
                "sell_price": 0.0,
                "last_price": float(price),
                "average_price": 0.0,
                "m2m": 0.0,
                "realised": 0.0,
                "product": order.product,
            },
        )
        old_net = int(position["quantity"])
        old_average = Decimal(str(position.get("average_price", 0)))
        new_net = old_net + signed
        realized_delta = Decimal("0")
        if old_net == 0 or (old_net > 0) == (signed > 0):
            new_average = (
                old_average * abs(old_net) + price * abs(signed)
            ) / abs(new_net)
        else:
            closing = min(abs(old_net), abs(signed))
            realized_delta = (
                (price - old_average) * closing
                if old_net > 0
                else (old_average - price) * closing
            )
            if new_net == 0:
                new_average = Decimal("0")
            elif (new_net > 0) == (old_net > 0):
                new_average = old_average
            else:
                new_average = price
        self.realized_pnl += realized_delta
        position["quantity"] = new_net
        position["average_price"] = float(new_average)
        position["last_price"] = float(price)
        position["realised"] = float(Decimal(str(position.get("realised", 0))) + realized_delta)
        position["m2m"] = float((price - new_average) * new_net)
        fill_event = {
            "order_id": order.order_id,
            "at": self.scheduler.now.isoformat(),
            "side": order.side,
            "quantity": quantity,
            "price": str(price),
            "cumulative_quantity": order.filled_quantity,
            "complete": complete,
        }
        self.fill_events.append(fill_event)
        self.trace.add(
            "BROKER_FILL",
            at=self.scheduler.now,
            actor=f"order-{order.order_id}",
            details=dict(fill_event),
        )
        quantity_key = "buy_quantity" if order.side == "BUY" else "sell_quantity"
        price_key = "buy_price" if order.side == "BUY" else "sell_price"
        old_qty = int(position[quantity_key])
        position[quantity_key] = old_qty + quantity
        position[price_key] = float(
            ((Decimal(str(position[price_key])) * old_qty) + price * quantity) / (old_qty + quantity)
        )
        position_snapshot = {key: dict(value) for key, value in self.positions_by_symbol.items()}
        self.scheduler.schedule_after(
            self.visibility.positions_seconds,
            lambda snapshot=position_snapshot: self._publish_positions(snapshot),
            priority=self.visibility.positions_priority,
        )
        self._transition(
            order,
            "COMPLETE" if complete else "PARTIAL",
            callback_delay=self.visibility.callback_seconds,
        )
        latest_mark = self._latest_mark_for_symbol(order.symbol)
        if latest_mark is not None:
            self._mark_to_market(order.symbol, latest_mark)

    def _mark_to_market(self, symbol: str, price: Decimal) -> None:
        position = self.positions_by_symbol.get(symbol)
        if position is None:
            return
        average = Decimal(str(position.get("average_price", 0)))
        quantity = int(position.get("quantity", 0))
        position["last_price"] = float(price)
        position["m2m"] = float((price - average) * quantity)
        snapshot = {key: dict(value) for key, value in self.positions_by_symbol.items()}
        self.scheduler.schedule_after(
            self.visibility.positions_seconds,
            lambda snapshot=snapshot: self._publish_positions(snapshot),
            priority=self.visibility.positions_priority,
        )

    def _latest_mark_for_symbol(self, symbol: str) -> Decimal | None:
        for key, quote in self.latest_quotes.items():
            if key == symbol or key.endswith(f":{symbol}"):
                return Decimal(str(quote["last_price"]))
        return None

    def _publish_positions(self, snapshot: dict[str, dict[str, Any]]) -> None:
        self._visible_positions = {key: dict(value) for key, value in snapshot.items()}
        self.trace.add(
            "POSITIONS_VISIBLE",
            at=self.scheduler.now,
            details={"symbols": sorted(snapshot)},
        )

    def _publish_order_view(self, order_id: str, entry: dict[str, Any], view: str) -> None:
        entry[view] = True
        self.trace.add(
            "ORDER_VIEW_VISIBLE",
            at=self.scheduler.now,
            actor=f"order-{order_id}",
            operation=view,
            details={"status": entry["data"]["status"]},
        )

    def _instrument_token_for(self, symbol: str) -> int:
        if symbol in self.instrument_tokens:
            return self.instrument_tokens[symbol]
        for order in self.orders_by_id.values():
            if order.symbol == symbol and order.instrument_token:
                return order.instrument_token
        return 0

    @staticmethod
    def _bracket_key(order: BrokerOrder) -> str | None:
        if order.bracket_id:
            return f"explicit:{order.bracket_id}"
        return None
