"""Comprehensive test suite for tiny_memo. Stdlib-only; pytest-friendly."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from tiny_memo import (
    BatchCollector,
    Bus,
    BusError,
    DeadLetter,
    Event,
    Subscription,
    TopicMismatchError,
    Transaction,
)


# ---- Basics --------------------------------------------------------------


def test_event_is_frozen_and_has_id():
    e = Event(topic="x", payload=1)
    assert e.topic == "x"
    assert e.payload == 1
    assert isinstance(e.id, str) and len(e.id) == 32
    with pytest.raises(Exception):
        e.topic = "y"  # type: ignore[misc]


def test_publish_returns_event():
    b = Bus()
    ev = b.publish("foo", 1)
    assert isinstance(ev, Event)
    assert ev.topic == "foo"
    assert ev.payload == 1


def test_invalid_topic_raises():
    b = Bus()
    with pytest.raises(BusError):
        b.publish("", 1)
    with pytest.raises(BusError):
        b.publish(123, 1)  # type: ignore[arg-type]


def test_invalid_pattern_raises():
    b = Bus()
    with pytest.raises(BusError):
        b.subscribe("", lambda e: None)
    with pytest.raises(BusError):
        b.subscribe(123, lambda e: None)  # type: ignore[arg-type]


# ---- Pattern matching ----------------------------------------------------


def test_glob_star_matches_single_segment():
    b = Bus()
    got = []
    b.subscribe("user.*", lambda e: got.append(e.topic))
    b.publish("user.created")
    b.publish("user.deleted")
    b.publish("user.profile.updated")  # not matched (two dots)
    b.publish("order.created")  # not matched
    assert got == ["user.created", "user.deleted"]


def test_glob_star_prefix():
    b = Bus()
    got = []
    b.subscribe("*.created", lambda e: got.append(e.topic))
    b.publish("user.created")
    b.publish("order.created")
    b.publish("user.deleted")  # not matched
    assert got == ["user.created", "order.created"]


def test_glob_question_mark():
    b = Bus()
    got = []
    b.subscribe("user.?", lambda e: got.append(e.topic))
    b.publish("user.x")
    b.publish("user.yy")  # not matched
    assert got == ["user.x"]


def test_subscribe_all_matches_everything():
    b = Bus()
    got = []
    b.subscribe_all(lambda e: got.append(e.topic))
    for t in ["a", "user.created", "x.y.z", "deep.nested.path"]:
        b.publish(t)
    assert got == ["a", "user.created", "x.y.z", "deep.nested.path"]


def test_multiple_subscribers_same_pattern():
    b = Bus()
    got_a, got_b = [], []
    b.subscribe("user.*", lambda e: got_a.append(e.payload))
    b.subscribe("user.*", lambda e: got_b.append(e.payload))
    b.publish("user.created", "hi")
    assert got_a == ["hi"] and got_b == ["hi"]


# ---- Filters & priority -------------------------------------------------


def test_filter_predicate():
    b = Bus()
    got = []
    b.subscribe("ticket.*", lambda e: got.append(e.topic), filter=lambda e: e.payload > 10)
    b.publish("ticket.open", 5)
    b.publish("ticket.open", 20)
    b.publish("ticket.open", 100)
    assert got == ["ticket.open", "ticket.open"]


def test_priority_higher_first():
    b = Bus()
    order = []
    b.subscribe("x", lambda e: order.append("low"), priority=0)
    b.subscribe("x", lambda e: order.append("high"), priority=10)
    b.subscribe("x", lambda e: order.append("mid"), priority=5)
    b.publish("x")
    assert order == ["high", "mid", "low"]


def test_negative_priority():
    b = Bus()
    order = []
    b.subscribe("x", lambda e: order.append("after"), priority=-5)
    b.subscribe("x", lambda e: order.append("before"), priority=5)
    b.publish("x")
    assert order == ["before", "after"]


# ---- Once ---------------------------------------------------------------


def test_once_auto_unsubscribes():
    b = Bus()
    got = []
    sub = b.subscribe_once("ping", lambda e: got.append(e.topic))
    b.publish("ping")
    b.publish("ping")
    b.publish("ping")
    assert got == ["ping"]
    assert not sub.active
    assert b.subscribers() == 0


def test_once_unsubscribe_manual():
    b = Bus()
    sub = b.subscribe_once("ping", lambda e: None)
    assert sub.unsubscribe() is True
    assert sub.unsubscribe() is False


# ---- Unsubscribe ---------------------------------------------------------


def test_unsubscribe():
    b = Bus()
    got = []
    sub = b.subscribe("a", lambda e: got.append(e.topic))
    assert sub.unsubscribe() is True
    assert b.subscribers() == 0
    b.publish("a")
    assert got == []


def test_unsubscribe_all():
    b = Bus()
    b.subscribe("a", lambda e: None)
    b.subscribe("b", lambda e: None)
    assert b.subscribers() == 2
    n = b.unsubscribe_all()
    assert n == 2
    assert b.subscribers() == 0


def test_callback_must_be_callable():
    b = Bus()
    with pytest.raises(BusError):
        b.subscribe("x", "not_callable")  # type: ignore[arg-type]


def test_has_subscriber():
    b = Bus()
    assert b.has_subscriber("foo") is False
    b.subscribe("foo", lambda e: None)
    assert b.has_subscriber("foo") is True


# ---- History -------------------------------------------------------------


def test_history_disabled_by_default():
    b = Bus()
    b.publish("x", 1)
    assert b.history("x") == []


def test_history_records_events():
    b = Bus(history_size=5)
    for i in range(3):
        b.publish("x", i)
    hist = b.history("x")
    assert [e.payload for e in hist] == [0, 1, 2]
    assert all(isinstance(e, Event) for e in hist)


def test_history_bounded():
    b = Bus(history_size=3)
    for i in range(10):
        b.publish("x", i)
    assert [e.payload for e in b.history("x")] == [7, 8, 9]


def test_clear_history_specific():
    b = Bus(history_size=5)
    b.publish("x", 1)
    b.publish("y", 2)
    b.clear_history("x")
    assert b.history("x") == []
    assert [e.payload for e in b.history("y")] == [2]


def test_clear_history_all():
    b = Bus(history_size=5)
    b.publish("x", 1)
    b.publish("y", 2)
    b.clear_history()
    assert b.history("x") == []
    assert b.history("y") == []


# ---- Dead letter queue ---------------------------------------------------


def test_dlq_captures_exceptions():
    b = Bus()

    def boom(e):
        raise RuntimeError("nope")

    b.subscribe("x", boom)
    b.publish("x", 1)
    letters = b.dead_letters
    assert len(letters) == 1
    assert isinstance(letters[0], DeadLetter)
    assert isinstance(letters[0].error, RuntimeError)
    assert letters[0].subscriber.pattern == "x"


def test_dlq_bounded():
    b = Bus(dead_letter_size=2)
    b.subscribe("x", lambda e: (_ for _ in ()).throw(ValueError("a")))
    for i in range(5):
        b.publish("x", i)
    assert len(b.dead_letters) == 2


def test_dlq_clear():
    b = Bus()
    b.subscribe("x", lambda e: (_ for _ in ()).throw(ValueError("a")))
    b.publish("x")
    assert b.dead_letters
    b.clear_dead_letters()
    assert b.dead_letters == []


def test_dlq_replay():
    b = Bus()
    counter = {"n": 0}

    def handler(e):
        counter["n"] += 1
        if counter["n"] == 1:
            raise RuntimeError("first")

    b.subscribe("x", handler)
    b.publish("x", 1)
    assert counter["n"] == 1
    assert len(b.dead_letters) == 1
    n = b.replay_dead_letters()
    assert n == 1
    assert counter["n"] == 2


# ---- Transactions --------------------------------------------------------


def test_transaction_commit_emits_in_order():
    b = Bus()
    got = []
    b.subscribe("*", lambda e: got.append(e.topic))
    with b.transaction() as tx:
        b.publish("a", 1)
        b.publish("b", 2)
        b.publish("c", 3)
    assert got == ["a", "b", "c"]


def test_transaction_rollback_discards():
    b = Bus()
    got = []
    b.subscribe("*", lambda e: got.append(e.topic))
    try:
        with b.transaction():
            b.publish("a", 1)
            b.publish("b", 2)
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    assert got == []


def test_transaction_open_event_not_dispatched_until_commit():
    b = Bus()
    got = []
    b.subscribe("*", lambda e: got.append(e.topic))
    with b.transaction():
        b.publish("a", 1)
        assert got == []  # not yet
    assert got == ["a"]


def test_double_transaction_raises():
    b = Bus()
    with pytest.raises(BusError):
        with b.transaction(), b.transaction():
            pass


def test_async_transaction_commit():
    b = Bus()
    got = []

    async def ahandler(e):
        got.append(e.topic)

    def shandler(e):
        got.append("sync:" + e.topic)

    b.subscribe("*", ahandler)
    b.subscribe("*", shandler)
    # Use sync publish inside async transaction; sync subscriber fires
    # immediately and async fires in background.
    asyncio.run(b.apublish("pre"))
    async def run():
        async with b.transaction():
            b.publish("x", 1)
            b.publish("y", 2)
    asyncio.run(run())
    # Sync subscriber saw both events during commit.
    assert "sync:x" in got and "sync:y" in got


# ---- Async --------------------------------------------------------------


def test_apublish_async_subscriber():
    b = Bus()
    got = []

    async def handler(e):
        await asyncio.sleep(0.01)
        got.append(e.payload)

    b.subscribe("a", handler)
    asyncio.run(b.apublish("a", "hello"))
    assert got == ["hello"]


def test_apublish_mixed_sync_async():
    b = Bus()
    got = []

    async def ahandler(e):
        got.append(("async", e.topic))

    def shandler(e):
        got.append(("sync", e.topic))

    b.subscribe("a", shandler)
    b.subscribe("a", ahandler, priority=1)
    asyncio.run(b.apublish("a"))
    # Higher-priority async runs first, then sync
    assert got == [("async", "a"), ("sync", "a")]


def test_apublish_in_async_subscriber_failure_goes_to_dlq():
    b = Bus()

    async def bad(e):
        raise ValueError("bad")

    b.subscribe("x", bad)
    asyncio.run(b.apublish("x"))
    assert len(b.dead_letters) == 1
    assert isinstance(b.dead_letters[0].error, ValueError)


# ---- Thread-safety ------------------------------------------------------


def test_threaded_publish_no_deadlocks():
    b = Bus(history_size=1000)
    got = []
    lock = threading.Lock()

    def handler(e):
        with lock:
            got.append(e.payload)

    b.subscribe_all(handler)

    def worker(prefix: str):
        for i in range(50):
            b.publish(f"t.{prefix}", f"{prefix}-{i}")

    threads = [threading.Thread(target=worker, args=(f"p{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(got) == 200


def test_concurrent_subscribe_and_publish():
    b = Bus()
    stop = threading.Event()

    def subscriber_loop():
        while not stop.is_set():
            sub = b.subscribe("x", lambda e: None)
            sub.unsubscribe()

    def publisher_loop():
        for i in range(500):
            b.publish("x", i)

    t_sub = threading.Thread(target=subscriber_loop)
    t_pub = threading.Thread(target=publisher_loop)
    t_sub.start()
    t_pub.start()
    t_pub.join()
    stop.set()
    t_sub.join()
    # No assertions on counts (race-y) — pass if no exception/deadlock.


# ---- Batching ------------------------------------------------------------


def test_batch_flushes_on_size():
    b = Bus()
    batches = []

    def cb(events):
        batches.append([e.payload for e in events])

    bc = BatchCollector(b, "m.*", cb, max_size=3, max_wait=10.0)
    bc.open()
    b.publish("m.x", 1)
    b.publish("m.x", 2)
    assert batches == []
    b.publish("m.x", 3)
    # Allow callback to run synchronously.
    assert batches == [[1, 2, 3]]
    bc.close()


def test_batch_flushes_on_close():
    b = Bus()
    batches = []

    def cb(events):
        batches.append([e.payload for e in events])

    bc = BatchCollector(b, "m.*", cb, max_size=10, max_wait=10.0)
    bc.open()
    b.publish("m.x", 1)
    b.publish("m.x", 2)
    bc.close()
    assert batches == [[1, 2]]


def test_batch_context_manager():
    b = Bus()
    batches = []

    def cb(events):
        batches.append(len(events))

    with BatchCollector(b, "m.*", cb, max_size=2, max_wait=10.0) as bc:
        b.publish("m.x", 1)
        b.publish("m.x", 2)
        assert batches == [2]


def test_batch_validates_args():
    b = Bus()
    with pytest.raises(BusError):
        BatchCollector(b, "x", lambda e: None, max_size=0, max_wait=1)
    with pytest.raises(BusError):
        BatchCollector(b, "x", lambda e: None, max_size=1, max_wait=0)


def test_batch_failure_goes_to_dlq():
    b = Bus()

    def bad(events):
        raise RuntimeError("batch fail")

    bc = BatchCollector(b, "m.*", bad, max_size=2, max_wait=10.0)
    bc.open()
    b.publish("m.x", 1)
    b.publish("m.x", 2)
    # Force any pending callbacks to settle.
    bc.close()
    assert len(b.dead_letters) >= 1


# ---- Edge cases ---------------------------------------------------------


def test_publish_with_no_subscribers():
    b = Bus(history_size=5)
    ev = b.publish("nothing", 42)
    assert ev.payload == 42
    assert b.history("nothing")[0].payload == 42


def test_publish_many():
    b = Bus()
    got = []
    b.subscribe("*", lambda e: got.append(e.topic))
    b.publish_many([("a", 1), ("b", 2), ("c", 3)])
    assert got == ["a", "b", "c"]


def test_subscription_dataclass():
    b = Bus()
    sub = b.subscribe("foo", lambda e: None, priority=3, name="myhandler")
    assert sub.pattern == "foo"
    assert sub.priority == 3
    assert sub.name == "myhandler"
    assert sub.active is True


def test_event_source_field():
    b = Bus()
    got = []

    def handler(e):
        got.append(e.source)

    b.subscribe("x", handler)
    b.publish("x", source="producer-7")
    assert got == ["producer-7"]


def test_subscription_handle_isolated_from_bus():
    """Removing the subscription by another path invalidates the handle."""
    b = Bus()
    sub = b.subscribe("x", lambda e: None)
    b.unsubscribe_all()
    assert sub.active is False


def test_filter_rejects_event():
    b = Bus()
    got = []
    b.subscribe("user.*", lambda e: got.append(e.topic), filter=lambda e: False)
    b.publish("user.created")
    assert got == []


def test_wildcard_star_does_not_match_nothing():
    b = Bus()
    got = []
    b.subscribe("*", lambda e: got.append(e.topic))
    with pytest.raises(BusError):
        b.publish("")
    assert got == []
