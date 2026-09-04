from wave_replay.coverage_manifest import assert_wave_coverage_complete, build_wave_coverage


def test_rooted_wave_coverage_maps_required_scope_to_executable_digests():
    manifest = build_wave_coverage(
        ".",
        "tests/fixtures/wave_replay/goldens/characterization_digests.json",
    )
    assert_wave_coverage_complete(manifest)
    assert "initilise" in manifest["reachable_functions"]
    assert "set_execution_variables" in manifest["reachable_functions"]
    assert "get_instrument_details" in manifest["reachable_functions"]
    assert "place_duo_order" in manifest["reachable_functions"]
    assert "on_order_update" in manifest["callback_roots"]
    assert manifest["scenario_mapping"]
    assert all(
        scenarios or name in manifest["function_waivers"]
        for name, scenarios in manifest["function_coverage"].items()
    )
    assert all(
        scenarios or branch_id in manifest["branch_waivers"]
        for branch_id, scenarios in manifest["branch_coverage"].items()
    )
    assert all(
        manifest["source_hashes"]["branch_outcomes"][branch_id] == digest
        for branch_id, digest in manifest["branch_waivers"].items()
    )
    assert "_refresh_status_after_fill" in manifest["reachable_functions"]
    assert "_wrapped_on_connect" in manifest["callback_roots"]
    assert "set_exchange" in manifest["reachable_functions"]
    assert "_handle_sigterm" in manifest["function_waivers"]
    assert manifest["waivers"]["ohlc_touch_branched"]
