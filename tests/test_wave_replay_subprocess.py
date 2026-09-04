from wave_replay.subprocess_lane import run_boundary_probe


def test_non_golden_subprocess_boundary_probe_imports_from_sandbox():
    result = run_boundary_probe(".")
    assert result["lane"] == "non_golden_subprocess_boundary"
    assert result["imported_from_sandbox"] is True
    assert result["checks"]["globals_isolated"] is True
    assert result["checks"]["broker_is_fake"] is True
    assert result["checks"]["network_denied"] is True
    assert result["checks"]["restart_globals_clean"] is True
    assert result["checks"]["signal_handler_present"] is True
    assert result["checks"]["atexit_handler_present"] is True
    assert result["checks"]["atexit_cleanup_semantics"] is True
    assert result["checks"]["database_files"] == ["api_monitor.db", "instruments.db", "trade_state.db"]
    assert result["trace_digest"].startswith("sha256:")
