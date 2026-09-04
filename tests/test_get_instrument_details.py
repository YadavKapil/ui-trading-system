import datetime

import common_lib


def test_get_instrument_details_accepts_a_native_expiry_date(monkeypatch):
    expiry = datetime.date.today() + datetime.timedelta(days=7)
    monkeypatch.setattr(common_lib, "token_symbol_map", {})
    monkeypatch.setattr(
        common_lib.instrument_cache,
        "get_instrument",
        lambda symbol: {
            "tradingsymbol": symbol,
            "expiry": expiry,
        },
    )

    details = common_lib.get_instrument_details("NIFTY2690823900CE")

    assert details["expiry"] == expiry
    assert details["days_to_expiry"] > 0


def test_get_instrument_details_accepts_an_iso_expiry_string(monkeypatch):
    expiry = datetime.date.today() + datetime.timedelta(days=7)
    monkeypatch.setattr(common_lib, "token_symbol_map", {})
    monkeypatch.setattr(
        common_lib.instrument_cache,
        "get_instrument",
        lambda symbol: {
            "tradingsymbol": symbol,
            "expiry": expiry.isoformat(),
        },
    )

    details = common_lib.get_instrument_details("NIFTY2690823900CE")

    assert details["days_to_expiry"] > 0
