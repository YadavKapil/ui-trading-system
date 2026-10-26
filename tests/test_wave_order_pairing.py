import threading
from unittest.mock import MagicMock, patch

import pytest

import common_lib


@pytest.fixture(autouse=True)
def reset_pairing_state():
    common_lib._order_pairing_depth = 0
    common_lib._order_update_drain_active = False
    common_lib._deferred_order_updates = []
    yield
    common_lib._order_pairing_depth = 0
    common_lib._order_update_drain_active = False
    common_lib._deferred_order_updates = []


def _set_order_state():
    common_lib.orders = {}
    common_lib.already_executing_order = 0
    common_lib.already_updating_order = 0
    common_lib.symbol = "NIFTY2611326600CE"
    common_lib.symbol_type = "ce"
    common_lib.exchange = "NFO"
    common_lib.typeOfProduct = "NRML"
    common_lib.tag = "Scraper"
    common_lib.exceptNFBNF = True
    common_lib.buy_quantity = 65
    common_lib.sell_quantity = 65
    common_lib.buy_gap = 1.0
    common_lib.sell_gap = 1.0
    common_lib.duo_old_buy_price = 50.1
    common_lib.duo_old_sell_price = 54.4
    common_lib.global_restrict_buy = 0
    common_lib.global_restrict_sell = 0

    kite = MagicMock()
    kite.VARIETY_REGULAR = "regular"
    kite.TRANSACTION_TYPE_SELL = "SELL"
    kite.TRANSACTION_TYPE_BUY = "BUY"
    kite.quote.return_value = {"NFO:NIFTY2611326600CE": {"last_price": 52.0}}
    common_lib.kite = kite


def _restrictions():
    unrestricted = {
        "ce": {"buy": "yes", "sell": "yes"},
        "pe": {"buy": "yes", "sell": "yes"},
        "futures": {"buy": "yes", "sell": "yes"},
    }
    return {
        "nifty": unrestricted,
        "bank_nifty": unrestricted,
        "sensex": unrestricted,
    }


def test_restored_orders_are_reciprocally_paired():
    _set_order_state()
    order_ids = iter(("37", "38"))

    with (
        patch.object(common_lib, "set_restrictions", return_value=_restrictions()),
        patch.object(common_lib, "get_position_for_symbol", return_value=0),
        patch.object(common_lib, "place_order", side_effect=lambda *args: next(order_ids)),
        patch.object(common_lib, "invalidate_positions_cache"),
        patch.object(common_lib, "_reprice_orders_on_multiplier_change", return_value=False),
        patch.object(common_lib, "write_status_to_file"),
    ):
        common_lib.check_changes_in_restrictions("NIFTY2611326600CE")

    assert common_lib.orders["37"]["associated_order"] == "38"
    assert common_lib.orders["38"]["associated_order"] == "37"


def _run_normal_pair_callback(callback_on_placement, *, threaded=False):
    _set_order_state()
    common_lib.quantity = 65
    common_lib.cool_off_time = 0
    common_lib.scraper_last_price = 52.0
    common_lib.old_quote_price = 52.0
    common_lib.buy_gap_percentage = 0.02
    common_lib.sell_gap_percentage = 0.02
    common_lib.initial_positions = {"position": 0}
    common_lib.multiplier_scale = {"0": [1, 1]}
    common_lib.pending_gtt_fallbacks = {}

    placed = []

    def place_order(*args):
        order_id = "37" if not placed else "38"
        placed.append(order_id)
        if len(placed) == callback_on_placement:
            update = {
                "order_id": "37",
                "status": "COMPLETE",
                "transaction_type": "SELL",
                "tradingsymbol": "NIFTY2611326600CE",
                "price": 54.4,
                "average_price": 54.4,
                "quantity": 65,
            }
            if threaded:
                callback = threading.Thread(
                    target=common_lib.on_order_update,
                    args=(None, update),
                )
                callback.start()
                callback.join(timeout=1)
                assert not callback.is_alive()
            else:
                common_lib.on_order_update(None, update)
        return order_id

    original_place_duo_order = common_lib.place_duo_order
    cancel_order = MagicMock()

    with (
        patch.object(common_lib, "place_duo_order") as replacement_place_duo,
        patch.object(common_lib, "set_restrictions", return_value=_restrictions()),
        patch.object(common_lib, "get_position_for_symbol", return_value=0),
        patch.object(common_lib, "get_delta_multiplier", return_value=(1.0, 1.0)),
        patch.object(common_lib, "place_order", side_effect=place_order),
        patch.object(common_lib, "cancel_order", cancel_order),
        patch.object(common_lib, "record_order_complete"),
        patch.object(common_lib, "set_scraper_last_price"),
        patch.object(common_lib, "delete_order_from_list"),
        patch.object(common_lib, "cancel_all_pending_gtts_for_symbol"),
        patch.object(common_lib, "invalidate_positions_cache"),
        patch.object(common_lib.time, "sleep"),
    ):
        original_place_duo_order("NIFTY2611326600CE", "NRML", True)

    return cancel_order, replacement_place_duo


def test_update_before_sell_registration_waits_until_pair_is_linked():
    cancel_order, replacement_place_duo = _run_normal_pair_callback(1)

    cancel_order.assert_called_once_with("regular", "38")
    replacement_place_duo.assert_called_once()
    assert common_lib._deferred_order_updates == []
    assert common_lib._order_update_drain_active is False


def test_update_after_sell_registration_waits_until_pair_is_linked():
    cancel_order, replacement_place_duo = _run_normal_pair_callback(
        2,
        threaded=True,
    )

    cancel_order.assert_called_once_with("regular", "38")
    replacement_place_duo.assert_called_once()
    assert common_lib._deferred_order_updates == []
    assert common_lib._order_update_drain_active is False


@pytest.mark.parametrize("sentinel", [-1, "-1", None])
def test_order_complete_ignores_missing_order_sentinel(sentinel):
    _set_order_state()
    common_lib.orders = {
        "37": {
            "price": 54.4,
            "quantity": 65,
            "transaction_type": "SELL",
            "symbol": "NIFTY2611326600CE",
            "associated_order": sentinel,
            "system_cancelled": True,
        }
    }

    with (
        patch.object(common_lib, "set_scraper_last_price"),
        patch.object(common_lib, "delete_order_from_list"),
        patch.object(common_lib, "cancel_order") as cancel_order,
    ):
        common_lib.order_complete("37", False, is_complete="complete")

    cancel_order.assert_not_called()
