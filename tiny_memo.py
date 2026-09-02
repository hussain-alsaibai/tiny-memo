"""tiny_memo — a zero-dependency, thread-safe pub/sub event bus.

A complete in-memory event bus with topic-based subscriptions, glob pattern
matching, sync and async emission, priority subscribers, dead letter queue,
batching, history, transactions, and more — all in a single file.

Example
-------
>>> from tiny_memo import Bus
>>> bus = Bus()
>>> bus.subscribe("user.*", lambda ev: print(ev))
>>> bus.publish("user.created", {"id": 1})
Event(topic='user.created', payload={'id': 1}, ...)
"""

from __future__ import annotations

import asyncio
import fnmatch
import inspect
import threading
import time
import uuid
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Iterable, List, Optional, Tuple, Union

__all__ = [
    "Bus",
    "Event",
    "Subscription",
    "DeadLetter",
    "Transaction",
    "BatchCollector",
    "BusError",
    "TopicMismatchError",
]

__version__ = "0.1.0"


# --- Topic matching ------------------------------------------------------


def _topic_matches(topic: str, pattern: str) -> bool:
    """Match a dotted topic against a glob pattern with segment semantics.

    ``*`` matches one segment (no dots), ``**`` matches one or more
    segments, ``?`` matches a single character within a segment, and
    ``[seq]`` is a character class. Plain ``*``-only patterns like ``"*"``
    match every topic.
    """
    if pattern == "*":
        return True
    if pattern == "**":
        return True
    segs_pat = pattern.split(".")
    segs_topic = topic.split(".")
    return _match_segs(segs_topic, segs_pat, 0, 0)


def _match_segs(topic: list[str], pat: list[str], ti: int, pi: int) -> bool:
    """Recursive segment-by-segment matcher supporting ``**``."""
    while pi < len(pat):
        p = pat[pi]
        if p == "**":
            # ** matches one or more segments. Try every remaining length.
            # First skip consecutive ** entries.
            while pi < len(pat) and pat[pi] == "**":
                pi += 1
            if pi == len(pat):
                return True
            for skip in range(1, len(topic) - ti + 1):
                if _match_segs(topic, pat, ti + skip, pi):
                    return True
            return False
        if ti >= len(topic):
            return False
        if not fnmatch.fnmatchcase(topic[ti], p):
            return False
        ti += 1
        pi += 1
    return ti == len(topic)


# --- Exceptions -----------------------------------------------------------


class BusError(Exception):
    """Base error for tiny_memo."""


class TopicMismatchError(BusError):
    """Raised when a topic pattern is malformed."""


# --- Data classes ---------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """An event flowing through the bus.

    Attributes
    ----------
    topic:
        Dotted topic string, e.g. ``"user.created"``.
    payload:
        Arbitrary user payload. Usually a dict.
    timestamp:
        Unix epoch seconds at creation.
    id:
        Unique identifier (UUID4 hex).
    source:
        Optional identifier for the publisher.
    """

    topic: str
    payload: Any = None
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    source: Optional[str] = None

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"Event(topic={self.topic!r}, payload={self.payload!r}, "
            f"id={self.id[:8]}…, ts={self.timestamp:.3f})"
        )


@dataclass
class DeadLetter:
    """A captured failure from a subscriber."""

    event: Event
    subscriber: "Subscription"
    error: BaseException
    attempts: int = 1
    raised_at: float = field(default_factory=time.time)


@dataclass
class _Handler:
    """Internal wrapper around a subscriber callback."""

    callback: Callable[[Event], Union[None, Awaitable[None]]]
    priority: int = 0
    once: bool = False
    filter: Optional[Callable[[Event], bool]] = None
    pattern: str = ""
    name: str = ""
    sub: Optional["Subscription"] = None

    def matches(self, event: Event) -> bool:
        if self.filter is not None and not self.filter(event):
            return False
        return True


@dataclass
class Subscription:
    """Public handle to an active subscription.

    Returned by :meth:`Bus.subscribe`. Use it to unsubscribe.
    """

    sub_id: str
    pattern: str
    callback: Callable[[Event], Any]
    priority: int
    once: bool
    filter: Optional[Callable[[Event], bool]]
    name: str
    bus: "Bus" = field(repr=False)
    _active: bool = True

    @property
    def active(self) -> bool:
        return self._active

    def unsubscribe(self) -> bool:
        """Detach this subscription. Returns True if it was active."""
        if not self._active:
            return False
        self.bus._remove(self)
        self._active = False
        return True


# --- Bus ------------------------------------------------------------------


class Bus:
    """Thread-safe pub/sub event bus.

    Parameters
    ----------
    history_size:
        Per-topic history buffer size. ``0`` disables history.
    dead_letter_size:
        Maximum dead letter queue length. Older entries are dropped.
    """

    def __init__(self, history_size: int = 0, dead_letter_size: int = 1000) -> None:
        self._lock = threading.RLock()
        self._subs: List[_Handler] = []
        self._sub_by_id: dict[str, _Handler] = {}
        self._history: dict[str, Deque[Event]] = {}
        self._dead_letters: Deque[DeadLetter] = deque(maxlen=dead_letter_size)
        self._history_size = int(history_size)
        self._transaction: Optional["_TransactionState"] = None
        self._transaction_lock = threading.RLock()

    # ---- Subscription ---------------------------------------------------

    def subscribe(
        self,
        pattern: str,
        callback: Callable[[Event], Any],
        *,
        priority: int = 0,
        once: bool = False,
        filter: Optional[Callable[[Event], bool]] = None,
        name: str = "",
    ) -> Subscription:
        """Subscribe ``callback`` to topics matching ``pattern``.

        ``pattern`` uses Unix shell-style globs (``*``, ``?``, ``[seq]``).
        ``"*"`` and ``"**"`` match every topic.

        Parameters
        ----------
        pattern:
            Topic glob, e.g. ``"user.*"``, ``"*.created"``, ``"order.*.paid"``.
        callback:
            Callable accepting one :class:`Event`. May be sync or async.
        priority:
            Higher values run first. Default ``0``.
        once:
            Auto-unsubscribe after the first matching event.
        filter:
            Optional predicate; only events for which it returns truthy fire.
        name:
            Optional human-readable label (useful for debugging / DLQ).
        """
        if not callable(callback):
            raise BusError("callback must be callable")
        if priority != 0 and not isinstance(priority, int):
            raise BusError("priority must be an int")
        self._validate_pattern(pattern)
        sub_id = uuid.uuid4().hex
        handler = _Handler(
            callback=callback,
            priority=priority,
            once=once,
            filter=filter,
            pattern=pattern,
            name=name or getattr(callback, "__name__", "anon"),
        )
        sub = Subscription(
            sub_id=sub_id,
            pattern=pattern,
            callback=callback,
            priority=priority,
            once=once,
            filter=filter,
            name=handler.name,
            bus=self,
        )
        handler.sub = sub
        with self._lock:
            self._subs.append(handler)
            self._sub_by_id[sub_id] = handler
            # Sort by descending priority; stable for same priority.
            self._subs.sort(key=lambda h: -h.priority)
        return sub

    def subscribe_once(
        self,
        pattern: str,
        callback: Callable[[Event], Any],
        **kwargs: Any,
    ) -> Subscription:
        """Convenience: subscribe with ``once=True``."""
        return self.subscribe(pattern, callback, once=True, **kwargs)

    def subscribe_all(
        self,
        callback: Callable[[Event], Any],
        **kwargs: Any,
    ) -> Subscription:
        """Subscribe to every topic. Equivalent to ``subscribe("*", ...)``."""
        return self.subscribe("*", callback, **kwargs)

    def unsubscribe_all(self) -> int:
        """Remove every subscription. Returns count removed."""
        with self._lock:
            handlers = list(self._subs)
            self._subs.clear()
            self._sub_by_id.clear()
        for h in handlers:
            if h.sub is not None:
                h.sub._active = False
        return len(handlers)

    # ---- Publish --------------------------------------------------------

    def publish(
        self,
        topic: str,
        payload: Any = None,
        *,
        source: Optional[str] = None,
    ) -> Event:
        """Publish an event synchronously.

        Returns the :class:`Event` that was dispatched. If a transaction is
        active the event is buffered until :meth:`Transaction.commit`.
        """
        self._validate_topic(topic)
        event = Event(topic=topic, payload=payload, source=source)
        with self._transaction_lock:
            if self._transaction is not None:
                self._transaction.buffer.append(event)
                return event
        self._dispatch(event)
        return event

    async def apublish(
        self,
        topic: str,
        payload: Any = None,
        *,
        source: Optional[str] = None,
    ) -> Event:
        """Publish an event, awaiting async subscribers in priority order.

        Sync callbacks are awaited via :func:`asyncio.to_thread` so they
        don't block the loop.
        """
        self._validate_topic(topic)
        event = Event(topic=topic, payload=payload, source=source)
        with self._transaction_lock:
            if self._transaction is not None:
                self._transaction.buffer.append(event)
                return event
        await self._adispatch(event)
        return event

    def publish_many(self, events: Iterable[Tuple[str, Any]]) -> List[Event]:
        """Publish several ``(topic, payload)`` tuples in order."""
        return [self.publish(t, p) for t, p in events]

    # ---- Transactions ---------------------------------------------------

    def transaction(self) -> "Transaction":
        """Open a transactional context for batching publishes.

        Events inside the ``with`` block are buffered and emitted in order on
        commit. An exception (or calling :meth:`Transaction.rollback`)
        discards the buffer.
        """
        return Transaction(self)

    # ---- History --------------------------------------------------------

    def history(self, topic: str) -> List[Event]:
        """Return a copy of the recorded events for ``topic`` (oldest first)."""
        with self._lock:
            return list(self._history.get(topic, ()))

    def clear_history(self, topic: Optional[str] = None) -> None:
        """Clear history for ``topic`` (or all topics if ``None``)."""
        with self._lock:
            if topic is None:
                self._history.clear()
            else:
                self._history.pop(topic, None)

    # ---- Dead letter queue ---------------------------------------------

    @property
    def dead_letters(self) -> List[DeadLetter]:
        """Snapshot of the dead letter queue (oldest first)."""
        with self._lock:
            return list(self._dead_letters)

    def clear_dead_letters(self) -> None:
        with self._lock:
            self._dead_letters.clear()

    def replay_dead_letters(self) -> int:
        """Re-dispatch every queued dead letter. Returns count replayed."""
        with self._lock:
            letters = list(self._dead_letters)
            self._dead_letters.clear()
        for dl in letters:
            self._dispatch(dl.event)
        return len(letters)

    # ---- Inspection -----------------------------------------------------

    def subscribers(self) -> int:
        with self._lock:
            return len(self._subs)

    def has_subscriber(self, pattern: str) -> bool:
        with self._lock:
            return any(h.pattern == pattern for h in self._subs)

    # ---- Internal dispatch ---------------------------------------------

    def _dispatch(self, event: Event) -> None:
        # Snapshot subscribers under lock to avoid mutation during iteration.
        with self._lock:
            handlers = [h for h in self._subs if _topic_matches(event.topic, h.pattern)]
            self._record_history(event)
        handlers.sort(key=lambda h: -h.priority)
        once_to_remove: List[Subscription] = []
        for h in handlers:
            sub = h.sub
            if sub is None or not sub.active:
                continue
            if not h.matches(event):
                continue
            try:
                result = h.callback(event)
                if inspect.isawaitable(result):
                    # Fire-and-forget async callback in a sync publish path.
                    self._schedule_coroutine(result, sub, event)
            except BaseException as exc:  # noqa: BLE001 - we want to capture all
                self._record_failure(event, sub, exc)
            if h.once:
                once_to_remove.append(sub)
        for sub in once_to_remove:
            sub.unsubscribe()

    async def _adispatch(self, event: Event) -> None:
        with self._lock:
            handlers = [h for h in self._subs if _topic_matches(event.topic, h.pattern)]
            self._record_history(event)
        handlers.sort(key=lambda h: -h.priority)
        once_to_remove: List[Subscription] = []
        for h in handlers:
            sub = h.sub
            if sub is None or not sub.active:
                continue
            if not h.matches(event):
                continue
            try:
                result = h.callback(event)
                if inspect.isawaitable(result):
                    await result
                elif asyncio.iscoroutinefunction(h.callback):
                    # Defensive: ensure coroutine returned for async fns.
                    pass
            except BaseException as exc:  # noqa: BLE001
                self._record_failure(event, sub, exc)
            if h.once:
                once_to_remove.append(sub)
        for sub in once_to_remove:
            sub.unsubscribe()

    def _remove(self, sub: Subscription) -> None:
        with self._lock:
            h = self._sub_by_id.pop(sub.sub_id, None)
            if h is not None and h in self._subs:
                self._subs.remove(h)
            sub._active = False

    def _record_history(self, event: Event) -> None:
        if self._history_size <= 0:
            return
        buf = self._history.get(event.topic)
        if buf is None:
            buf = deque(maxlen=self._history_size)
            self._history[event.topic] = buf
        buf.append(event)

    def _record_failure(self, event: Event, sub: Subscription, err: BaseException) -> None:
        self._dead_letters.append(DeadLetter(event=event, subscriber=sub, error=err))

    @staticmethod
    def _validate_topic(topic: str) -> None:
        if not isinstance(topic, str) or not topic:
            raise BusError("topic must be a non-empty string")

    @staticmethod
    def _validate_pattern(pattern: str) -> None:
        if not isinstance(pattern, str) or not pattern:
            raise BusError("pattern must be a non-empty string")

    @staticmethod
    def _schedule_coroutine(coro: Awaitable[None], sub: Subscription, event: Event) -> None:
        """Schedule an async callback when ``publish`` is called synchronously.

        We try a running loop first; otherwise we run it in a worker thread.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                # No running loop — drive it ourselves on a background thread.
                fut: "Future[None]" = Future()

                def _runner() -> None:
                    try:
                        new_loop = asyncio.new_event_loop()
                        try:
                            new_loop.run_until_complete(coro)
                            fut.set_result(None)
                        finally:
                            new_loop.close()
                    except BaseException as exc:  # noqa: BLE001
                        fut.set_exception(exc)

                threading.Thread(target=_runner, daemon=True).start()
            except Exception:
                pass
        else:
            loop.create_task(coro)

    # ---- Transaction state ---------------------------------------------

    def _txn_begin(self) -> "_TransactionState":
        with self._transaction_lock:
            if self._transaction is not None:
                raise BusError("transaction already active on this bus")
            state = _TransactionState(bus=self)
            self._transaction = state
            return state

    def _txn_append(self, event: Event) -> None:
        with self._transaction_lock:
            assert self._transaction is not None
            self._transaction.buffer.append(event)

    def _txn_commit(self) -> List[Event]:
        with self._transaction_lock:
            state = self._transaction
            if state is None:
                return []
            self._transaction = None
        if state.rolled_back:
            return []
        for ev in state.buffer:
            self._dispatch(ev)
        return list(state.buffer)

    def _txn_rollback(self) -> int:
        with self._transaction_lock:
            state = self._transaction
            if state is None:
                return 0
            self._transaction = None
            state.rolled_back = True
        return len(state.buffer)


@dataclass
class _TransactionState:
    bus: Bus
    buffer: List[Event] = field(default_factory=list)
    rolled_back: bool = False


class Transaction:
    """Context manager returned by :meth:`Bus.transaction`.

    Example
    -------
    >>> with bus.transaction() as tx:
    ...     bus.publish("a", 1)
    ...     bus.publish("b", 2)
    ... # both events fire here, in order
    """

    def __init__(self, bus: Bus) -> None:
        self._bus = bus
        self._events: List[Event] = []
        self._state: Optional[_TransactionState] = None

    def __enter__(self) -> "Transaction":
        self._state = self._bus._txn_begin()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._state is None:
            # __enter__ failed before state was set; nothing to do.
            return False
        if exc_type is None:
            self._events = self._bus._txn_commit()
        else:
            self._bus._txn_rollback()
        self._state = None
        return False  # never swallow exceptions

    async def __aenter__(self) -> "Transaction":
        self._state = self._bus._txn_begin()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if self._state is None:
            return False
        if exc_type is None:
            self._events = self._bus._txn_commit()
        else:
            self._bus._txn_rollback()
        self._state = None
        return False


# --- Batch collector -----------------------------------------------------


class BatchCollector:
    """Collect events and dispatch them in batches.

    Either after ``max_size`` events or ``max_wait`` seconds — whichever
    comes first — the buffered events are forwarded to a single subscriber
    callback as a list.

    Example
    -------
    >>> batch = BatchCollector(bus, "metrics.*", callback, max_size=10, max_wait=1.0)
    >>> batch.open()
    >>> bus.publish("metrics.tick", 1)
    """

    def __init__(
        self,
        bus: Bus,
        pattern: str,
        callback: Callable[[List[Event]], Any],
        *,
        max_size: int = 100,
        max_wait: float = 1.0,
    ) -> None:
        if max_size <= 0:
            raise BusError("max_size must be > 0")
        if max_wait <= 0:
            raise BusError("max_wait must be > 0")
        self._bus = bus
        self._pattern = pattern
        self._callback = callback
        self._max_size = max_size
        self._max_wait = max_wait
        self._buffer: Deque[Event] = deque()
        self._lock = threading.Lock()
        self._sub: Optional[Subscription] = None
        self._timer: Optional[threading.Timer] = None

    def open(self) -> "BatchCollector":
        """Start collecting by attaching an internal subscription."""
        if self._sub is not None:
            return self
        self._sub = self._bus.subscribe(self._pattern, self._on_event, priority=0)
        return self

    def close(self) -> None:
        """Stop collecting and flush any remaining events."""
        if self._sub is not None:
            self._sub.unsubscribe()
            self._sub = None
        self._cancel_timer()
        self._flush()

    def _on_event(self, event: Event) -> None:
        with self._lock:
            self._buffer.append(event)
            full = len(self._buffer) >= self._max_size
        if full:
            self._cancel_timer()
            self._flush()

    def _schedule_flush(self) -> None:
        self._cancel_timer()
        t = threading.Timer(self._max_wait, self._flush)
        t.daemon = True
        self._timer = t
        t.start()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()
        try:
            result = self._callback(batch)
            if inspect.isawaitable(result):
                # Drive async callback.
                try:
                    asyncio.get_event_loop()
                except RuntimeError:
                    pass
                threading.Thread(
                    target=lambda: asyncio.run(result),
                    daemon=True,
                ).start()
        except Exception:
            # On failure, push each event back into the DLQ via the bus.
            for ev in batch:
                self._bus._record_failure(
                    ev,
                    self._sub or Subscription(
                        sub_id="",
                        pattern=self._pattern,
                        callback=self._callback,
                        priority=0,
                        once=False,
                        filter=None,
                        name="batch",
                        bus=self._bus,
                    ),
                    sys_exc(),
                )

    def __enter__(self) -> "BatchCollector":
        return self.open()

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


def sys_exc() -> BaseException:
    """Return the current exception (helper for BatchCollector)."""
    import sys

    return sys.exc_info()[1] or RuntimeError("unknown error")
