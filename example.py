"""Realistic usage examples for tiny_memo.

Run any of the sections below directly:
    python3 example.py

Each example is self-contained and prints something useful.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter

from tiny_memo import BatchCollector, Bus, Event


# ---------------------------------------------------------------------------
# 1. Event-driven UI
# ---------------------------------------------------------------------------


def event_driven_ui() -> None:
    """A tiny UI event flow: button presses update widgets and analytics."""
    print("=== 1. Event-driven UI ===")
    bus = Bus(history_size=20)

    def on_button_click(e: Event) -> None:
        button_id = e.payload["id"]
        bus.publish("widget.update", {"id": button_id, "label": f"#{button_id} pressed"})
        bus.publish("analytics.event", {"name": "click", "props": {"id": button_id}})

    def update_widget(e: Event) -> None:
        print(f"  widget: {e.payload}")

    def track(e: Event) -> None:
        print(f"  analytics: {e.payload}")

    bus.subscribe("button.click", on_button_click)
    bus.subscribe("widget.update", update_widget, priority=10)  # UI before analytics
    bus.subscribe("analytics.event", track, priority=5)

    bus.publish("button.click", {"id": "submit"})
    bus.publish("button.click", {"id": "cancel"})
    print()


# ---------------------------------------------------------------------------
# 2. Plugin system
# ---------------------------------------------------------------------------


class PluginHost:
    """Hosts plugins that subscribe to lifecycle events on the shared bus."""

    def __init__(self) -> None:
        self.bus = Bus()
        self._plugins: list[tuple[str, "Plugin"]] = []

    def register(self, plugin: "Plugin") -> None:
        self._plugins.append((plugin.name, plugin))
        plugin.install(self.bus)
        self.bus.publish("plugin.loaded", {"name": plugin.name})

    def start(self) -> None:
        self.bus.publish("app.start")
        # Late subscribers can replay what they missed via history.
        new_logger = PrintLogger()
        new_logger.install(self.bus)
        self.bus.publish("app.ready")

    def shutdown(self) -> None:
        self.bus.publish("app.shutdown")


class Plugin:
    """Base class for plugins."""

    name: str = "plugin"

    def install(self, bus: Bus) -> None:
        raise NotImplementedError


class MetricsPlugin(Plugin):
    name = "metrics"

    def install(self, bus: Bus) -> None:
        self._counts: Counter[str] = Counter()
        bus.subscribe("app.*", lambda e: self._counts.__setitem__(e.topic, self._counts[e.topic] + 1))
        bus.subscribe("plugin.loaded", lambda e: print(f"  metrics: plugin {e.payload['name']} seen"))

    def snapshot(self) -> dict[str, int]:
        return dict(self._counts)


class PrintLogger(Plugin):
    """A plugin that joins late and replays history."""

    name = "print-logger"

    def install(self, bus: Bus) -> None:
        bus.subscribe("app.*", lambda e: print(f"  log[{e.topic}]"))
        for topic in ("app.start",):
            for past in bus.history(topic):
                print(f"  log[replay:{past.topic}] -> {past.payload}")


def plugin_system() -> None:
    print("=== 2. Plugin system ===")
    host = PluginHost()
    metrics = MetricsPlugin()
    host.register(metrics)
    host.start()
    host.shutdown()
    print(f"  final metrics: {metrics.snapshot()}")
    print()


# ---------------------------------------------------------------------------
# 3. Metrics aggregation with batching
# ---------------------------------------------------------------------------


def metrics_aggregation() -> None:
    """Flush a batch of metrics every 0.2s or every 50 events."""
    print("=== 3. Metrics aggregation ===")
    bus = Bus()

    def flush(batch: list[Event]) -> None:
        counter: Counter[str] = Counter(e.payload["name"] for e in batch)
        print(f"  flushed {len(batch)} metrics: {dict(counter)}")

    bc = BatchCollector(bus, "metric.*", flush, max_size=50, max_wait=0.2)
    bc.open()
    for i in range(120):
        name = "cpu" if i % 2 == 0 else "mem"
        bus.publish(f"metric.{name}", {"name": name, "value": float(i)})
        if i % 25 == 0:
            time.sleep(0.05)
    bc.close()
    print()


# ---------------------------------------------------------------------------
# 4. Audit log with DLQ
# ---------------------------------------------------------------------------


def audit_log() -> None:
    """Send audit events through a pipeline where one bad handler doesn't
    break the rest; the bad handler's failures end up in the DLQ."""
    print("=== 4. Audit log (with dead letter queue) ===")
    bus = Bus(dead_letter_size=100)

    def stdout_logger(e: Event) -> None:
        print(f"  audit[{e.topic}] actor={e.payload['actor']} action={e.payload['action']}")

    def siem_forwarder(e: Event) -> None:
        # Pretend this sometimes fails (e.g. network).
        if e.payload.get("action") == "deny":
            raise RuntimeError("SIEM unreachable")
        print(f"  siem[{e.topic}] forwarded")

    def alert_on_dlq() -> None:
        print(f"  DLQ size after run: {len(bus.dead_letters)}")
        for dl in bus.dead_letters:
            print(f"    DLQ: {dl.event.topic} via {dl.subscriber.name}: {dl.error}")

    bus.subscribe("audit.*", stdout_logger, priority=10)
    bus.subscribe("audit.*", siem_forwarder, priority=5)
    for action in ("login", "read", "deny", "write", "deny", "logout"):
        bus.publish("audit.event", {"actor": "alice", "action": action})
    alert_on_dlq()
    # Replay the dead letters — this time they may succeed.
    bus.clear_dead_letters()
    print()


# ---------------------------------------------------------------------------
# 5. Async fan-out
# ---------------------------------------------------------------------------


async def async_fan_out() -> None:
    print("=== 5. Async fan-out ===")
    bus = Bus(history_size=10)

    async def fetch_user(e: Event) -> None:
        await asyncio.sleep(0.01)
        print(f"  fetch[{e.payload['id']}] -> user record")

    async def send_email(e: Event) -> None:
        await asyncio.sleep(0.02)
        print(f"  email[{e.payload['id']}] -> sent")

    bus.subscribe("user.created", fetch_user)
    bus.subscribe("user.created", send_email)
    for uid in (1, 2, 3):
        await bus.apublish("user.created", {"id": uid})
    print()


# ---------------------------------------------------------------------------
# 6. Threaded workers
# ---------------------------------------------------------------------------


def threaded_workers() -> None:
    """Publish from multiple threads and watch a single handler collect all."""
    print("=== 6. Threaded workers ===")
    bus = Bus()
    counter = {"n": 0}
    lock = threading.Lock()

    def handler(e: Event) -> None:
        with lock:
            counter["n"] += 1

    bus.subscribe_all(handler)

    def worker(prefix: str) -> None:
        for i in range(50):
            bus.publish(f"job.{prefix}", {"i": i})

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  total events handled: {counter['n']} (expected 200)")
    print()


def main() -> None:
    event_driven_ui()
    plugin_system()
    metrics_aggregation()
    audit_log()
    asyncio.run(async_fan_out())
    threaded_workers()


if __name__ == "__main__":
    main()
