import datetime as dt

from wave_replay.broker_engine import BrokerEngine
from wave_replay.fake_kite import FakeKite
from wave_replay.fake_ticker import FakeTicker
from wave_replay.schemas import IST
from wave_replay.scheduler import DeterministicScheduler
from wave_replay.tasks import SimTaskRuntime
from wave_replay.tracing import TraceRecorder


def _ticker():
    trace = TraceRecorder()
    scheduler = DeterministicScheduler(
        start=dt.datetime(2025, 7, 7, 9, 15, tzinfo=IST),
        trace=trace,
    )
    tasks = SimTaskRuntime(scheduler, trace)
    ticker = FakeTicker(scheduler, tasks, trace)
    return trace, scheduler, tasks, ticker


def test_disconnect_suppresses_queued_callbacks_and_reconnect_uses_new_generation():
    trace, scheduler, _tasks, ticker = _ticker()
    received = []
    ticker.register_listener(
        "worker-a",
        on_order_update=lambda _ws, data: received.append(("a", data["status"])),
    )
    ticker.register_listener(
        "worker-b",
        on_order_update=lambda _ws, data: received.append(("b", data["status"])),
    )
    ticker.connect(listener_id="worker-a")
    ticker.connect(listener_id="worker-b")
    ticker.deliver("order", {"status": "OPEN"}, delay=5)
    scheduler.schedule_after(2, lambda: ticker.disconnect(listener_id="worker-a"), priority=20)
    scheduler.run()
    assert received == [("b", "OPEN")]
    assert any(event["kind"] == "TICKER_CALLBACK_SUPPRESSED" for event in trace.events)

    ticker.connect(listener_id="worker-a")
    ticker.deliver("order", {"status": "COMPLETE"}, listener_id="worker-a")
    scheduler.run()
    assert received[-1] == ("a", "COMPLETE")


def test_broker_dispatch_fans_out_only_to_connected_worker_listeners():
    trace, scheduler, tasks, ticker = _ticker()
    received = []
    for listener_id in ("worker-a", "worker-b"):
        ticker.register_listener(
            listener_id,
            on_order_update=lambda _ws, data, listener_id=listener_id: received.append(
                (listener_id, data["status"])
            ),
        )
        ticker.connect(listener_id=listener_id)
    ticker.crash(listener_id="worker-b")
    engine = BrokerEngine(scheduler=scheduler, trace=trace)
    engine.bind_callback_dispatch(ticker.order_dispatch)
    kite = FakeKite(engine)
    kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol="NIFTYCE",
        transaction_type="BUY",
        quantity=25,
        product="NRML",
        order_type="LIMIT",
        price=90,
        tag="B",
    )
    scheduler.run()
    assert received == [("worker-a", "ACK"), ("worker-a", "OPEN")]


def test_disconnect_between_registration_and_task_execution_suppresses_callback():
    trace, scheduler, _tasks, ticker = _ticker()
    received = []
    ticker.on_order_update = lambda _ws, data: received.append(data["status"])
    ticker.connect()
    scheduler.run()
    ticker.deliver("order", {"status": "OPEN"}, delay=0, priority=30)
    scheduler.schedule_after(0, lambda: ticker.disconnect(), priority=30)
    scheduler.run()
    assert received == []
    assert any(
        event["kind"] == "TICKER_CALLBACK_SUPPRESSED"
        and event["operation"] == "order:task"
        for event in trace.events
    )


def test_each_listener_receives_an_independent_payload_copy():
    _trace, scheduler, _tasks, ticker = _ticker()
    received = []

    def mutate(_ws, data):
        data["status"] = "MUTATED"

    ticker.register_listener("a", on_order_update=mutate)
    ticker.register_listener(
        "b",
        on_order_update=lambda _ws, data: received.append(data["status"]),
    )
    ticker.connect(listener_id="a")
    ticker.connect(listener_id="b")
    ticker.deliver("order", {"status": "OPEN"})
    scheduler.run()
    assert received == ["OPEN"]
