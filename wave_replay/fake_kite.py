"""Narrow KiteConnect-compatible facade over :mod:`broker_engine`."""

from __future__ import annotations

from typing import Any

from .broker_engine import BrokerEngine


class FakeKite:
    VARIETY_REGULAR = "regular"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_SL = "SL"
    PRODUCT_NRML = "NRML"
    GTT_TYPE_SINGLE = "single"
    MARKET_PROTECTION_AUTO = -1

    def __init__(self, engine: BrokerEngine, *, instruments: list[dict[str, Any]] | None = None) -> None:
        self.engine = engine
        self._instruments = list(instruments or [])
        self.access_token = ""

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def generate_session(self, request_token: str, api_secret: str) -> dict[str, str]:
        return {"access_token": f"fake:{request_token}"}

    def quote(self, instruments):
        return self.engine.quote(instruments)

    def positions(self):
        return self.engine.positions()

    def orders(self):
        return self.engine.orders()

    def order_history(self, order_id):
        return self.engine.order_history(str(order_id))

    def place_order(self, **request):
        return self.engine.place_order(**request)

    def cancel_order(self, *args, **request):
        if args:
            request.setdefault("variety", args[0])
            request.setdefault("order_id", args[1])
        return self.engine.cancel_order(**request)

    def modify_order(self, *args, **request):
        return self.engine.modify_order(*args, **request)

    def place_gtt(self, **request):
        return self.engine.place_gtt(**request)

    def delete_gtt(self, trigger_id):
        return self.engine.delete_gtt(int(trigger_id))

    def get_gtts(self):
        return self.engine.get_gtts()

    def instruments(self, exchange=None):
        if exchange is None:
            return list(self._instruments)
        return [item for item in self._instruments if item.get("exchange") == exchange]

