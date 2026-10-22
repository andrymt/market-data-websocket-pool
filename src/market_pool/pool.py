"""Bounded fan-in, redundant readers, finite deduplication and reconnect supervision."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .adapters import Adapter, FeedError
from .model import Trade

LOG = logging.getLogger(__name__)
Sink = Callable[[dict], Awaitable[None]]


class Deduper:
    """Arrival-window deduplication, not a durable exactly-once guarantee."""

    def __init__(self, capacity: int = 50_000, ttl: float = 300) -> None:
        if capacity < 1 or not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("Deduplication capacity and TTL must be positive")
        self.capacity, self.ttl = capacity, ttl
        self.items: OrderedDict[tuple, tuple[float, tuple]] = OrderedDict()

    def check(self, trade: Trade, now: float) -> str:
        while self.items and now - next(iter(self.items.values()))[0] >= self.ttl:
            self.items.popitem(last=False)
        signature = trade.price, trade.quantity, trade.taker_side, trade.event_time_ns
        old = self.items.get(trade.key)
        if old:
            if old[1] == signature:
                return "duplicate"
            # Preserve insertion time/order so TTL eviction remains valid.
            self.items[trade.key] = old[0], signature
            return "correction"
        self.items[trade.key] = now, signature
        if len(self.items) > self.capacity:
            self.items.popitem(last=False)
        return "new"


@dataclass(frozen=True)
class Settings:
    copies: int = 1
    queue_size: int = 4096
    read_timeout: float = 60
    ack_timeout: float = 15
    backoff_base: float = 1
    backoff_cap: float = 30
    stable_after: float = 60
    dedup_capacity: int = 50_000
    dedup_ttl: float = 300

    def __post_init__(self):
        if self.copies not in (1, 2):
            raise ValueError("Use one or two connections per exchange")
        if self.queue_size < 1 or self.dedup_capacity < 1:
            raise ValueError("Queue and dedup capacities must be positive")
        for value in (
            self.read_timeout,
            self.ack_timeout,
            self.backoff_base,
            self.backoff_cap,
            self.stable_after,
            self.dedup_ttl,
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Timeouts and backoff values must be finite and positive")
        if self.backoff_cap < self.backoff_base:
            raise ValueError("Backoff cap must be at least the base")


def backoff(attempt: int, settings: Settings, random_value: float) -> float:
    ceiling = min(settings.backoff_cap, settings.backoff_base * 2 ** min(attempt, 20))
    # Equal jitter prevents immediate tight reconnect loops, including random_value=0.
    return ceiling * (0.5 + 0.5 * random_value)


class Pool:
    def __init__(
        self,
        adapters: list[Adapter],
        symbols: tuple[str, ...],
        sink: Sink,
        settings: Settings | None = None,
        connector=connect,
    ) -> None:
        self.settings = settings or Settings()
        self.adapters, self.symbols, self.sink = adapters, symbols, sink
        if not adapters or not symbols:
            raise ValueError("At least one exchange and symbol are required")
        self.connector = connector
        self.queue: asyncio.Queue[Trade | dict] = asyncio.Queue(self.settings.queue_size)
        self.deduper = Deduper(self.settings.dedup_capacity, self.settings.dedup_ttl)
        self.counters: Counter = Counter()
        self.connections: dict[str, dict] = {}
        self.last_event: dict[tuple[str, str], int] = {}
        self.last_trade: dict[tuple[str, str], float] = {}

    async def reader(self, adapter: Adapter, copy: int) -> None:
        label = f"{adapter.exchange}:{copy}"
        state = self.connections[label] = {"state": "starting", "last_message": None}
        attempt = 0
        while True:
            opened = time.monotonic()
            state["state"] = "connecting"
            try:
                async with self.connector(
                    adapter.endpoint,
                    open_timeout=10,
                    close_timeout=3,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_048_576,
                    max_queue=16,
                ) as ws:
                    opened = time.monotonic()
                    await ws.send(json.dumps(adapter.subscribe(self.symbols)))
                    state["state"] = "subscribing"
                    pending = set(self.symbols)
                    deadline = opened + self.settings.ack_timeout
                    while True:
                        timeout = self.settings.read_timeout
                        if pending:
                            timeout = min(timeout, max(0.001, deadline - time.monotonic()))
                        # Keep recv in this task: wait_for's child-task cancellation race
                        # on Python 3.11 can swallow a concurrent shutdown cancellation.
                        async with asyncio.timeout(timeout):
                            raw = await ws.recv()
                        received = time.time_ns()
                        state["last_message"] = time.monotonic()
                        self.counters["messages_received"] += 1
                        parsed = adapter.parse(raw, self.symbols, received)
                        pending -= parsed.acknowledged
                        if not pending:
                            state["state"] = "subscribed"
                        elif time.monotonic() >= deadline:
                            raise FeedError(f"Subscription not acknowledged: {sorted(pending)}")
                        for trade in parsed.trades:
                            self.counters["trades_received"] += 1
                            if self.queue.full():
                                self.counters["backpressure_events"] += 1
                            await self.queue.put(trade)
            except asyncio.CancelledError:
                state["state"] = "stopped"
                raise
            except (OSError, TimeoutError, WebSocketException, FeedError) as exc:
                self.counters["reconnects"] += 1
                state["state"] = "backoff"
                LOG.warning("%s disconnected: %s", label, exc)
                # A second reader may cover the outage, but does not prove completeness.
                await self.queue.put(
                    {
                        "type": "continuity_risk",
                        "exchange": adapter.exchange,
                        "connection": label,
                        "observed_time_ns": time.time_ns(),
                        "reason": str(exc)[:500],
                    }
                )
                if time.monotonic() - opened >= self.settings.stable_after:
                    attempt = 0
                await asyncio.sleep(backoff(attempt, self.settings, random.random()))
                attempt += 1

    async def consume(self) -> None:
        while True:
            item = await self.queue.get()
            try:
                if isinstance(item, dict):
                    await self.sink(item)
                    continue
                result = self.deduper.check(item, time.monotonic())
                self.counters[result] += 1
                if result == "duplicate":
                    continue
                stream = item.exchange, item.symbol
                highwater = self.last_event.get(stream, 0)
                late = item.event_time_ns < highwater
                self.last_event[stream] = max(highwater, item.event_time_ns)
                self.last_trade[stream] = time.monotonic()
                event = item.as_dict()
                event["out_of_order"] = late
                if result == "correction":
                    event["type"] = "trade_correction"
                await self.sink(event)
                self.counters["emitted"] += 1
                if late:
                    self.counters["out_of_order"] += 1
            finally:
                self.queue.task_done()

    def status(self) -> dict:
        now = time.monotonic()
        return {
            "type": "status",
            "observed_time_ns": time.time_ns(),
            "queue_depth": self.queue.qsize(),
            "counters": dict(self.counters),
            "connections": {
                key: {
                    "state": value["state"],
                    "message_age_seconds": None
                    if value["last_message"] is None
                    else round(now - value["last_message"], 3),
                }
                for key, value in self.connections.items()
            },
            "trade_age_seconds": {
                f"{a.exchange}:{s}": None
                if (a.exchange, s) not in self.last_trade
                else round(now - self.last_trade[a.exchange, s], 3)
                for a in self.adapters
                for s in self.symbols
            },
        }

    async def run(self, stop: asyncio.Event) -> None:
        consumer = asyncio.create_task(self.consume(), name="sink")
        readers = [
            asyncio.create_task(self.reader(a, i), name=f"{a.exchange}:{i}")
            for a in self.adapters
            for i in range(self.settings.copies)
        ]
        stopping = asyncio.create_task(stop.wait(), name="stop")
        try:
            done, _ = await asyncio.wait(
                [consumer, stopping, *readers], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is not stopping:
                    task.result()  # Surface sink failures and programming errors immediately.
                    raise RuntimeError(f"Worker unexpectedly stopped: {task.get_name()}")
        finally:
            for task in [stopping, *readers]:
                task.cancel()
            await asyncio.gather(stopping, *readers, return_exceptions=True)
            for state in self.connections.values():
                state["state"] = "stopped"
            try:
                if not consumer.done():
                    # Keep the consumer running while draining already accepted messages.
                    await asyncio.wait_for(self.queue.join(), timeout=10)
                elif not consumer.cancelled():
                    consumer.result()
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
