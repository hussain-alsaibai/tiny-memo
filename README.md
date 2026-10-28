# tiny-memo

[![PyPI](https://img.shields.io/badge/PyPI-tiny--memo-blue)](https://github.com/hussain-alsaibai/tiny-memo#install)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](#)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-49%20passing-brightgreen)](#)

> A zero-dependency, thread-safe Python pub/sub event bus with topic globs, async
> first-class support, dead letter queue, batching, history, and atomic transactions —
> in a single file.

---

## Visual architecture

```
                                 tiny_memo.Bus
                                ┌──────────────────────────────────┐
                                │  topics   ── glob matcher ──┐    │
                                │  subs[]   ── priority queue │    │
   publisher ── publish() ──▶   │  history  ── per-topic ring │    │
                                │  DLQ      ── failed handlers│   │
                                │  txn      ── atomic commit  │    │
                                └──┬──────────┬────────────┬──┘    │
                                   ▼          ▼            ▼       │
                              subscriber  subscriber   subscriber  │
                              (sync/async, (priority N, (filter,     │
                              optional   optional      once)        │
                              priority) once)                      │
                                                                    
              ★ topic patterns:   "user.*"   "*.created"   "**"
```

---

## Why this exists

Python already has event buses, but each trades off something you probably want:

| Library       | Dependencies | Topic globs | Async | Priority | DLQ | History | Batching | Transactions |
|---------------|--------------|-------------|-------|----------|-----|---------|----------|--------------|
| **tiny-memo** | **0**        | **yes**     | **yes** | **yes** | **yes** | **yes** | **yes** | **yes** |
| pypubsub      | 0            | weak        | no    | yes     | no  | no      | no       | no           |
| blinker       | 0            | weak        | no    | no      | no  | no      | no       | no           |
| pyee          | twisted/aiostream | no    | yes   | no      | no  | no      | no       | no           |
| aio_pika      | heavy        | yes         | yes   | no      | yes | no      | no       | no           |
| asyncio.Queue | 0            | no          | yes   | no      | no  | no      | no       | no           |

**tiny-memo** is for the case where you want a real topic-routed pub/sub that fits in one
file, ships zero transitive deps, supports modern `async/await`, and doesn't make you
choose between features.

---

## Installation

```bash
pip install tiny-memo
```

Or just copy `tiny_memo.py` into your project — it's one file with no dependencies.

Requires **Python 3.8+**.

---

## Quick start

```python
from tiny_memo import Bus

bus = Bus()

@bus.subscribe("user.*", priority=10)
def notify(e):
    print(f"notify: {e.payload}")

@bus.subscribe("*.created")
def log(e):
    print(f"created: {e.topic}")

bus.publish("user.created", {"id": 1, "name": "Hussain"})
bus.publish("order.created", {"id": 42})
bus.publish("user.deleted", {"id": 1})  # matches notify only
```

Output:

```
notify: {'id': 1, 'name': 'Hussain'}
created: user.created
notify: ... (matched by *.created first because priority=10 wins on user.created)
```

---

## Core concepts

### Topics

Topics are dotted strings like `"user.created"`, `"order.item.paid"`, or `"metric.cpu"`.
A topic is just a string — the convention is dotted hierarchy.

### Publishers

Call `bus.publish(topic, payload)` (sync) or `await bus.apublish(topic, payload)`
(async). Both return the `Event` that was created.

### Subscribers

A callback registered for a topic pattern. You can subscribe in three ways:

```python
bus.subscribe("user.*", handler)            # function form
sub = bus.subscribe("user.*", handler)      # returns a Subscription handle
bus.subscribe_all(handler)                   # receive every event
```

`Subscription.unsubscribe()` removes the subscription. `Subscription.active` tells you
if it's still attached.

### Wildcards

Patterns use Unix-shell-style globs **with dot-aware segment semantics**:

| Pattern           | Matches                                              |
|-------------------|------------------------------------------------------|
| `"*"`             | every topic                                          |
| `"user.*"`        | `user.created`, `user.deleted` — but **not** `user.profile.updated` |
| `"*.created"`     | `user.created`, `order.created`                      |
| `"order.**"`      | `order`, `order.created`, `order.item.paid`          |
| `"user.?"`        | `user.x`, `user.a` — one character                   |
| `"user.[abc]"`    | `user.a`, `user.b`, `user.c` — character class       |

---

## API Reference

### `Bus(history_size=0, dead_letter_size=1000)`

| Method | Description |
|---|---|
| `subscribe(pattern, callback, *, priority=0, once=False, filter=None, name="")` | Register a subscriber. |
| `subscribe_once(pattern, callback, **kw)` | Subscribe with `once=True`. |
| `subscribe_all(callback, **kw)` | Subscribe to every topic. |
| `unsubscribe_all()` | Remove all subscribers. Returns count. |
| `publish(topic, payload=None, *, source=None)` | Emit a sync event. Returns the `Event`. |
| `apublish(topic, payload=None, *, source=None)` | Async emit. Awaits async subscribers. |
| `publish_many([(topic, payload), ...])` | Publish in order. |
| `transaction()` | Context manager. Buffers events until commit. |
| `history(topic)` | List of past events for a topic. |
| `clear_history(topic=None)` | Clear history for one or all topics. |
| `dead_letters` | Snapshot of failed deliveries. |
| `clear_dead_letters()` | Drop the DLQ. |
| `replay_dead_letters()` | Re-emit everything in the DLQ. Returns count. |
| `subscribers()` | Current subscriber count. |
| `has_subscriber(pattern)` | Quick lookup. |

### `Event`

Frozen dataclass: `topic`, `payload`, `timestamp`, `id`, `source`.

### `DeadLetter`

`event`, `subscriber`, `error`, `attempts`, `raised_at`.

### `Transaction`

Context manager. Use sync (`with`) or async (`async with`). On exit-without-exception
the buffered events are dispatched in order; on exception they're discarded.

### `BatchCollector(bus, pattern, callback, *, max_size, max_wait)`

Collect events matching a pattern and dispatch them in batches to `callback(events)`
when either `max_size` events accumulate or `max_wait` seconds elapse.

---

## Pattern matching examples

```python
from tiny_memo import Bus

bus = Bus()

# Only direct children of "user"
bus.subscribe("user.*", handler)

# Anything under "audit" at any depth
bus.subscribe("audit.**", handler)

# Any single-character topic
bus.subscribe("?", handler)

# Specific path
bus.subscribe("order.item.paid", handler)
```

---

## Async examples

```python
import asyncio
from tiny_memo import Bus

bus = Bus()

async def fetch_user(e):
    user = await db.get(e.payload["id"])  # pretend this is async
    await bus.apublish("user.fetched", user)

bus.subscribe("user.fetch_requested", fetch_user)
asyncio.run(bus.apublish("user.fetch_requested", {"id": 7}))
```

Sync subscribers on an async publish are scheduled on a worker thread so they don't
block the event loop. Async subscribers are awaited in priority order.

---

## Dead letter queue usage

```python
from tiny_memo import Bus

bus = Bus(dead_letter_size=100)

def flaky_handler(e):
    if e.payload.get("force_fail"):
        raise RuntimeError("nope")

bus.subscribe("risk.*", flaky_handler)
bus.publish("risk.check", {"force_fail": True})

print(f"DLQ size: {len(bus.dead_letters)}")
for dl in bus.dead_letters:
    print(f"failed: {dl.event.topic} via {dl.subscriber.name}: {dl.error}")
# Replay later when the downstream is healthy:
bus.replay_dead_letters()
```

---

## Batching and history

```python
from tiny_memo import Bus, BatchCollector

bus = Bus(history_size=200)  # remember the last 200 events per topic

def flush(batch):
    metrics_client.send_multi(batch)

batch = BatchCollector(bus, "metric.*", flush, max_size=100, max_wait=0.5)
batch.open()  # start collecting
bus.publish("metric.cpu", 0.42)
batch.close()  # final flush

# Late subscribers can see what they missed:
for past in bus.history("metric.cpu"):
    print(past.payload)
```

---

## Use cases

- **Plugin systems** — Plugins subscribe to lifecycle topics (`app.start`, `plugin.loaded`,
  `shutdown`). History lets late-joining plugins replay what they missed.
- **Microservice wiring** — Replace an in-process message broker for service-to-service
  chatter where Redis/Kafka is overkill.
- **UI events** — Decouple buttons, widgets, analytics, and toasts via topic routing.
- **Metrics and audit** — Aggregate high-volume events with `BatchCollector`; send
  failures to the DLQ for later replay.
- **Game/event loops** — Decouple input, physics, audio, and rendering systems.
- **Testing** — Inject fake subscribers to assert what your code emits.

---

## Comparison table

| Feature                  | tiny-memo | pypubsub     | blinker      | pyee         |
|--------------------------|-----------|--------------|--------------|--------------|
| Zero external deps       | ✅         | ✅            | ✅            | ❌ (twisted)  |
| Topic glob matching      | ✅ segment | ✅ limited    | ⚠️ limited   | ❌            |
| Async subscribers        | ✅         | ❌            | ❌            | ✅            |
| Priority dispatch        | ✅         | ✅            | ❌            | ❌            |
| Filter predicates        | ✅         | ✅            | ⚠️ via sub    | ❌            |
| Once-only subscribe      | ✅         | ⚠️ manual    | ⚠️ manual    | ❌            |
| Dead letter queue        | ✅         | ❌            | ❌            | ❌            |
| Per-topic history        | ✅         | ❌            | ❌            | ❌            |
| Batching                 | ✅         | ❌            | ❌            | ❌            |
| Atomic transactions      | ✅         | ❌            | ❌            | ❌            |
| Thread-safe              | ✅         | ✅            | ✅            | ⚠️ partial    |
| Single file              | ✅         | ❌            | ✅            | ❌            |

---

## License

MIT — see [LICENSE](LICENSE).
