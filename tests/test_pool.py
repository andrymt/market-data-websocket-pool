import asyncio
import json
import unittest
from dataclasses import replace

from market_pool.adapters import ADAPTERS
from market_pool.model import Trade
from market_pool.pool import Deduper, Pool, Settings, backoff

TRADE = Trade("coinbase", "BTC-USD", "42", "90000", "0.01", "buy", 100, 200)
ACK = json.dumps(
    {
        "type": "subscriptions",
        "channels": [
            {"name": "matches", "product_ids": ["BTC-USD"]},
        ],
    }
)
MATCH = json.dumps(
    {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 42,
        "price": "90000",
        "size": "0.01",
        "side": "buy",
        "time": "2025-10-09T12:30:00Z",
    }
)


class DeduperTests(unittest.TestCase):
    def test_duplicates_ignore_receive_timestamp(self):
        d = Deduper()
        self.assertEqual(d.check(TRADE, 0), "new")
        self.assertEqual(d.check(replace(TRADE, received_time_ns=300), 1), "duplicate")

    def test_same_id_other_exchange_or_symbol_is_distinct(self):
        d = Deduper()
        d.check(TRADE, 0)
        self.assertEqual(d.check(replace(TRADE, exchange="kraken"), 0), "new")
        self.assertEqual(d.check(replace(TRADE, symbol="ETH-USD"), 0), "new")

    def test_correction_is_not_silently_discarded(self):
        d = Deduper()
        d.check(TRADE, 0)
        changed = replace(TRADE, price="91000")
        self.assertEqual(d.check(changed, 1), "correction")
        self.assertEqual(d.check(changed, 2), "duplicate")

    def test_capacity_and_ttl_eviction(self):
        d = Deduper(capacity=1, ttl=10)
        d.check(TRADE, 0)
        d.check(replace(TRADE, trade_id="43"), 1)
        self.assertEqual(len(d.items), 1)
        self.assertEqual(d.check(TRADE, 2), "new")
        self.assertEqual(d.check(TRADE, 12), "new")

    def test_backoff_has_jitter_and_cap(self):
        settings = Settings()
        self.assertEqual(backoff(0, settings, 0), 0.5)
        self.assertEqual(backoff(0, settings, 1), 1)
        self.assertEqual(backoff(1_000_000, settings, 1), 30)

    def test_invalid_settings(self):
        for value in (float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Settings(read_timeout=value)
        for args in ({"copies": 3}, {"queue_size": 0}, {"read_timeout": 0}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                Settings(**args)


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def send(self, message):
        self.sent.append(json.loads(message))

    async def recv(self):
        if self.messages:
            value = self.messages.pop(0)
            if isinstance(value, Exception):
                raise value
            return value
        await asyncio.Future()


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.events = []

    async def sink(self, event):
        self.events.append(event)

    def pool(self, **kwargs):
        return Pool([ADAPTERS["coinbase"]], ("BTC-USD",), self.sink, **kwargs)

    async def test_ordering_duplicate_and_correction(self):
        pool = self.pool()
        for trade in (
            TRADE,
            TRADE,
            replace(TRADE, trade_id="43", event_time_ns=90),
            replace(TRADE, price="91000"),
        ):
            await pool.queue.put(trade)
        consumer = asyncio.create_task(pool.consume())
        await asyncio.wait_for(pool.queue.join(), 1)
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        self.assertEqual(len(self.events), 3)
        self.assertTrue(self.events[1]["out_of_order"])
        self.assertEqual(self.events[2]["type"], "trade_correction")
        self.assertEqual(pool.counters["duplicate"], 1)

    async def test_reconnect_and_resubscribe(self):
        sockets = []

        def connect(*args, **kwargs):
            ws = FakeSocket([ACK, OSError("network lost")] if not sockets else [ACK, MATCH])
            sockets.append(ws)
            return ws

        settings = Settings(backoff_base=0.001, backoff_cap=0.002)
        pool = self.pool(settings=settings, connector=connect)
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(stop))
        async with asyncio.timeout(2):
            while pool.counters["emitted"] < 1:
                await asyncio.sleep(0.001)
        stop.set()
        await asyncio.wait_for(task, 1)
        self.assertGreaterEqual(len(sockets), 2)
        self.assertTrue(all(ws.sent[0]["type"] == "subscribe" for ws in sockets))
        self.assertTrue(all(ws.closed for ws in sockets))
        self.assertEqual(self.events[0]["type"], "continuity_risk")

    async def test_two_readers_deduplicate(self):
        pool = self.pool(
            settings=Settings(copies=2), connector=lambda *a, **kw: FakeSocket([ACK, MATCH])
        )
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(stop))
        async with asyncio.timeout(2):
            while pool.counters["duplicate"] < 1:
                await asyncio.sleep(0.001)
        stop.set()
        await task
        self.assertEqual(pool.counters["emitted"], 1)

    async def test_unacknowledged_connection_restarts(self):
        pool = self.pool(
            settings=Settings(ack_timeout=0.01, backoff_base=0.001, backoff_cap=0.002),
            connector=lambda *a, **kw: FakeSocket([]),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(stop))
        async with asyncio.timeout(2):
            while pool.counters["reconnects"] < 1:
                await asyncio.sleep(0.001)
        stop.set()
        await task
        self.assertEqual(pool.counters["emitted"], 0)

    async def test_silent_feed_restarts(self):
        pool = self.pool(
            settings=Settings(read_timeout=0.01, backoff_base=0.001, backoff_cap=0.002),
            connector=lambda *a, **kw: FakeSocket([ACK]),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(pool.run(stop))
        async with asyncio.timeout(2):
            while pool.counters["reconnects"] < 1:
                await asyncio.sleep(0.001)
        stop.set()
        await task
        self.assertEqual(self.events[0]["type"], "continuity_risk")

    async def test_bounded_queue_applies_backpressure(self):
        pool = self.pool(settings=Settings(queue_size=1))
        await pool.queue.put(TRADE)
        blocked = asyncio.create_task(pool.queue.put(TRADE))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())
        pool.queue.get_nowait()
        pool.queue.task_done()
        await blocked
        self.assertEqual(pool.queue.qsize(), 1)

    async def test_sink_failure_propagates_and_closes_readers(self):
        ws = FakeSocket([ACK, MATCH])

        async def broken_sink(event):
            raise OSError("disk full")

        pool = Pool(
            [ADAPTERS["coinbase"]], ("BTC-USD",), broken_sink, connector=lambda *a, **kw: ws
        )
        with self.assertRaisesRegex(OSError, "disk full"):
            await asyncio.wait_for(pool.run(asyncio.Event()), 1)
        self.assertTrue(ws.closed)
