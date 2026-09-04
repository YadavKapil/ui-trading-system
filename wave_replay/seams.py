"""Legacy Wave state configuration kept separate from broker truth."""

from __future__ import annotations

import random
from typing import Any

import numpy as np

from .schemas import Contract, ReplayScenario


def instrument_row(contract: Contract, scenario: ReplayScenario) -> dict[str, Any]:
    business_dte = int(np.busday_count(scenario.selected_date, contract.expiry) + 1)
    return {
        "instrument_token": contract.instrument_token,
        "tradingsymbol": contract.symbol,
        "name": "NIFTY",
        "expiry": contract.expiry.isoformat(),
        "strike": float(contract.strike),
        "instrument_type": "CE",
        "lot_size": contract.lot_size,
        "segment": contract.segment,
        "exchange": contract.exchange,
        "tick_size": float(contract.tick_size),
        "days_to_expiry": business_dte,
    }


def configure_legacy_module(module: object, scenario: ReplayScenario, contract: Contract) -> None:
    row = instrument_row(contract, scenario)
    module.symbol = scenario.symbol
    module.quantity = str(scenario.lot_size)
    module.buy_quantity = scenario.lot_size
    module.sell_quantity = scenario.lot_size
    module.buy_gap = float(scenario.buy_gap_points)
    module.sell_gap = float(scenario.sell_gap_points)
    module.initial_positions = {"position": 0}
    module.current_positions = {"position": 0}
    module.symbol_type = "ce"
    module.exchange = "NFO"
    module.tag = "Scraper"
    module.typeOfProduct = "NRML"
    module.exceptNFBNF = False
    module.instrument_token = str(contract.instrument_token)
    module.segment = contract.segment
    module.start_time = scenario.first_underlying_close_at.isoformat()
    module.scraper_last_price = -1
    module.old_quote_price = -1
    module.duo_old_buy_price = -1
    module.duo_old_sell_price = -1
    module.orders = {}
    module.order_ids_completed = {}
    module.order_numbers_storage = {}
    module.pending_gtt_fallbacks = {}
    module._pending_auto_reprice_ids = set()
    module._spot_price_history = []
    module._same_side_fill_times = {"BUY": [], "SELL": []}
    module._cooldown_active_until = {"BUY": None, "SELL": None}
    module._last_multiplier_info = {}
    module.cached_positions = None
    module._order_history_cache = {}
    module.already_executing_order = 0
    module.already_updating_order = 0
    module.live_trading_enabled = True
    module.order_gtt_regular = "regular"
    module.randint = random.Random(scenario.seed).randint
    module.cool_off_time = 0
    module.multiplier_scale = scenario.wave_settings["multiplier_scale"]
    module._we_config_cache = {"NIFTY": dict(scenario.wave_settings, cool_off_time=0)}
    module.delta_limits_config = scenario.wave_settings.get(
        "delta_limits",
        {"NIFTY": {"default": {"min": -3500, "max": 3500}}},
    )
    module.all_instruments = {contract.instrument_token: row}
    module.token_symbol_map = {contract.symbol: contract.instrument_token}
    module.set_scraper_last_price(float(scenario.option_close_at_selection))
