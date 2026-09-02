import json
import sqlite3

import pytest

import flask_app
import kite_api_monitor as monitor


class _SessionClient:
    def generate_session(self, *args, **kwargs):
        return {"access_token": "not-recorded"}


class _FailingSessionClient:
    def generate_session(self, request_token, api_secret=None):
        raise RuntimeError(
            f"request_token={request_token} api_secret={api_secret} "
            f"Authorization: Bearer {request_token}"
        )


class _JsonFailingSessionClient:
    def generate_session(self, serialized_payload):
        api_secret = json.loads(serialized_payload)["api_secret"]
        raise RuntimeError(f"credential rejected: {api_secret}")


class _PayloadClient:
    def submit(self, payload, headers, url):
        raise RuntimeError(f"request failed: {payload!r}, headers={headers!r}, url={url}")


class _BrokenRepr:
    def __init__(self, sentinel):
        self.sentinel = sentinel

    def __repr__(self):
        raise RuntimeError(self.sentinel)


def _monitor_json() -> str:
    return json.dumps(monitor.get_monitor_stats(), sort_keys=True)


def _database_text(db_path) -> str:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT method, args_repr, error FROM api_calls ORDER BY id"
        ).fetchall()
    return json.dumps(rows)


def _database_files_bytes(db_path) -> bytes:
    contents = db_path.read_bytes()
    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(f"{db_path.name}{suffix}")
        if sidecar.exists():
            contents += sidecar.read_bytes()
    return contents


def test_recursive_sanitizer_redacts_nested_payloads_headers_urls_and_responses():
    sentinels = {
        "secret": "SENTINEL_SECRET_7c92",
        "request": "SENTINEL_REQUEST_d811",
        "access": "SENTINEL_ACCESS_a451",
        "password": "SENTINEL_PASSWORD_b633",
        "checksum": "SENTINEL_CHECKSUM_882e",
        "api_key": "SENTINEL_API_KEY_f302",
    }
    payload = {
        "payload": {
            "api_secret": sentinels["secret"],
            "nested": [{"requestToken": sentinels["request"]}],
        },
        "headers": {
            "Authorization": f"Bearer {sentinels['access']}",
            "X-API-Key": sentinels["api_key"],
        },
        "url": (
            "https://kite.example/session?"
            f"access_token={sentinels['access']}&symbol=NIFTY"
        ),
        "response_body": json.dumps(
            {
                "data": {
                    "password": sentinels["password"],
                    "checksum": sentinels["checksum"],
                }
            }
        ),
        "safe": {"symbol": "NIFTY", "quantity": 50},
    }

    serialized = json.dumps(monitor.sanitize_monitor_data(payload), sort_keys=True)

    assert all(sentinel not in serialized for sentinel in sentinels.values())
    assert "NIFTY" in serialized
    assert '"quantity": 50' in serialized
    assert monitor._REDACTED in serialized


def test_recursive_sanitizer_suppresses_bare_auth_record_details():
    sentinel = "SENTINEL_AUTH_RECORD_BARE_5ae1"
    record = {
        "method": "generate_session",
        "args": f"opaque positional value {sentinel}",
        "error": f"credential rejected: {sentinel}",
    }

    sanitized = monitor.sanitize_monitor_data(record)
    serialized = json.dumps(sanitized)

    assert sentinel not in serialized
    assert sanitized["args"] == monitor._AUTH_ARGS_OMITTED
    assert sanitized["error"] == monitor._AUTH_ERROR_OMITTED


def test_sanitizer_failure_does_not_log_the_original_value(caplog):
    sentinel = "SENTINEL_REPR_FAILURE_7d3a"

    sanitized = monitor.sanitize_monitor_data(_BrokenRepr(sentinel))

    assert sentinel not in sanitized
    assert sentinel not in caplog.text
    assert sanitized == "<unserializable _BrokenRepr>"


def test_generate_session_positional_and_keyword_args_never_reach_db_or_stats(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()

    positional_request = "SENTINEL_POSITIONAL_REQUEST_5a4c"
    positional_secret = "SENTINEL_POSITIONAL_SECRET_c831"
    keyword_request = "SENTINEL_KEYWORD_REQUEST_6b22"
    keyword_secret = "SENTINEL_KEYWORD_SECRET_e119"
    client = monitor.MonitoredKite(_SessionClient())

    client.generate_session(positional_request, positional_secret)
    positional_record = monitor._call_history[-1]
    monitor._db_insert_batch([positional_record])

    client.generate_session(
        request_token=keyword_request,
        api_secret=keyword_secret,
    )
    keyword_record = monitor._call_history[-1]
    monitor._db_insert_batch([keyword_record])

    database_text = _database_text(db_path)
    database_bytes = _database_files_bytes(db_path)
    output = _monitor_json()
    for sentinel in (
        positional_request,
        positional_secret,
        keyword_request,
        keyword_secret,
    ):
        assert sentinel not in database_text
        assert sentinel.encode() not in database_bytes
        assert sentinel not in output
    assert positional_record.args_repr == monitor._AUTH_ARGS_OMITTED
    assert keyword_record.args_repr == monitor._AUTH_ARGS_OMITTED


def test_generate_session_exception_scrubs_known_positional_secrets(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()
    request_token = "SENTINEL_ERROR_REQUEST_d532"
    api_secret = "SENTINEL_ERROR_SECRET_c901"
    client = monitor.MonitoredKite(_FailingSessionClient())

    try:
        client.generate_session(request_token, api_secret)
    except RuntimeError:
        pass
    else:
        raise AssertionError("generate_session should have failed")

    record = monitor._call_history[-1]
    monitor._db_insert_batch([record])
    persisted = _database_text(db_path)
    output = _monitor_json()
    for sentinel in (request_token, api_secret):
        assert sentinel not in persisted
        assert sentinel not in output
    assert record.error == monitor._AUTH_ERROR_OMITTED


def test_generate_session_json_arg_and_bare_exception_secret_are_suppressed(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()
    api_secret = "SENTINEL_JSON_AUTH_SECRET_392c"
    serialized_payload = json.dumps(
        {"api_secret": api_secret, "safe_field": "preserved only outside auth"}
    )
    assert api_secret in monitor._collect_secret_values(
        "generate_session", (serialized_payload,), {}
    )
    client = monitor.MonitoredKite(_JsonFailingSessionClient())

    with pytest.raises(RuntimeError, match="credential rejected"):
        client.generate_session(serialized_payload)

    record = monitor._call_history[-1]
    monitor._db_insert_batch([record])
    assert record.args_repr == monitor._AUTH_ARGS_OMITTED
    assert record.error == monitor._AUTH_ERROR_OMITTED
    assert api_secret not in _database_text(db_path)
    assert api_secret.encode() not in _database_files_bytes(db_path)
    assert api_secret not in _monitor_json()


def test_nested_request_data_and_error_text_are_sanitized_before_persistence(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()
    payload_secret = "SENTINEL_PAYLOAD_SECRET_2c71"
    header_token = "SENTINEL_HEADER_TOKEN_a59e"
    query_token = "SENTINEL_QUERY_TOKEN_097b"
    client = monitor.MonitoredKite(_PayloadClient())
    payload = {"order": {"api_secret": payload_secret}, "symbol": "NIFTY"}
    headers = {"X-Auth-Token": header_token, "Accept": "application/json"}
    url = f"https://kite.example/orders?access_token={query_token}&exchange=NFO"

    try:
        client.submit(payload, headers, url)
    except RuntimeError:
        pass
    else:
        raise AssertionError("submit should have failed")

    record = monitor._call_history[-1]
    monitor._db_insert_batch([record])
    persisted = _database_text(db_path)
    output = _monitor_json()
    for sentinel in (payload_secret, header_token, query_token):
        assert sentinel not in persisted
        assert sentinel not in output
    assert "NIFTY" in persisted
    assert "application/json" in persisted


def test_legacy_rows_are_sanitized_in_place_and_history_is_safe(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()
    request_token = "SENTINEL_LEGACY_REQUEST_1e45"
    api_secret = "SENTINEL_LEGACY_SECRET_981f"
    bare_error_secret = "SENTINEL_LEGACY_BARE_ERROR_f82d"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            monitor._INSERT_SQL,
            (
                1.0,
                "2026-09-02 09:00:00",
                "2026-09-02",
                "generate_session",
                "flask_app.py",
                "establish_session",
                1,
                f"{request_token!r}, api_secret={api_secret!r}",
                10.0,
                f"credential rejected: {bare_error_secret}",
                0,
            ),
        )
        conn.execute(
            monitor._INSERT_SQL,
            (
                2.0,
                "2026-09-02 09:01:00",
                "2026-09-02",
                "orders",
                "orders.py",
                "refresh",
                2,
                "'NIFTY', exchange='NFO'",
                4.0,
                None,
                0,
            ),
        )

    monitor._sanitize_existing_records()

    database_text = _database_text(db_path)
    database_bytes = _database_files_bytes(db_path)
    history = json.dumps(monitor.get_history_for_date("2026-09-02"))
    for sentinel in (request_token, api_secret, bare_error_secret):
        assert sentinel not in database_text
        assert sentinel.encode() not in database_bytes
        assert sentinel not in history
    assert monitor._AUTH_ARGS_OMITTED in history
    assert monitor._AUTH_ERROR_OMITTED in history
    assert "'NIFTY', exchange='NFO'" in database_text


class _CheckpointResult:
    def __init__(self, result):
        self.result = result

    def fetchone(self):
        return self.result


class _CheckpointConnection:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def execute(self, statement):
        assert statement == "PRAGMA wal_checkpoint(TRUNCATE)"
        self.calls += 1
        return _CheckpointResult(self.results.pop(0))


def test_wal_checkpoint_retries_busy_result_then_succeeds():
    conn = _CheckpointConnection([(1, 2, 1), (1, 1, 0), (0, 0, 0)])

    monitor._checkpoint_wal(conn, retries=3, retry_delay=0)

    assert conn.calls == 3


def test_wal_checkpoint_permanent_busy_fails_clearly():
    conn = _CheckpointConnection([(1, 2, 0), (1, 2, 0)])

    with pytest.raises(RuntimeError, match="remained busy after 2 attempts"):
        monitor._checkpoint_wal(conn, retries=2, retry_delay=0)


def test_migration_runs_physical_cleanup_without_logical_updates(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()
    checkpoints = []
    original_checkpoint = monitor._checkpoint_wal

    def tracked_checkpoint(conn):
        checkpoints.append(True)
        original_checkpoint(conn, retry_delay=0)

    monkeypatch.setattr(monitor, "_checkpoint_wal", tracked_checkpoint)

    monitor._sanitize_existing_records()

    assert len(checkpoints) == 2


def test_migration_cleanup_failure_refuses_startup(tmp_path, monkeypatch):
    db_path = tmp_path / "api_monitor.db"
    monkeypatch.setattr(monitor, "_DB_PATH", str(db_path))
    monitor._init_db()

    def fail_checkpoint(conn):
        raise RuntimeError("busy")

    monkeypatch.setattr(monitor, "_checkpoint_wal", fail_checkpoint)

    with pytest.raises(RuntimeError, match="refusing startup"):
        monitor._sanitize_existing_records()


def test_api_monitor_endpoint_applies_final_response_sanitization(
    client, monkeypatch
):
    sentinel = "SENTINEL_ENDPOINT_ACCESS_12af"
    monkeypatch.setattr(
        flask_app,
        "get_monitor_stats",
        lambda: {
            "recent_calls": [
                {
                    "args": {"access_token": sentinel},
                    "error": f"access_token={sentinel}",
                }
            ]
        },
    )

    monkeypatch.setattr(flask_app, "_setup_config_complete", lambda: True)
    response = client.get("/api/api-monitor-data")

    assert response.status_code == 200
    assert sentinel.encode() not in response.data
    assert monitor._REDACTED.encode() in response.data
