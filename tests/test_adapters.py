import json
import unittest
from decimal import Decimal

from market_pool.adapters import ADAPTERS, FeedError
from market_pool.model import positive_decimal, rfc3339_ns, symbol

NOW = 1_760_000_001_000_000_000
PAIRS = ("BTC-USD", "ETH-USD")


def parse(exchange, data):
    return ADAPTERS[exchange].parse(json.dumps(data), PAIRS, NOW)


def coinbase(**changes):
    return {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 42,
        "price": "90000.00000001",
        "size": "0.01000000",
        "side": "buy",
        "time": "2025-10-09T12:30:00.123456789Z",
        **changes,
    }


class AdapterTests(unittest.TestCase):
    def test_coinbase_maker_side_is_inverted(self):
        trade = parse("coinbase", coinbase()).trades[0]
        self.assertEqual(trade.taker_side, "sell")
        self.assertEqual(trade.price, "90000.00000001")
        self.assertEqual(trade.quantity, "0.01000000")
        self.assertEqual(trade.event_time_ns % 1_000_000_000, 123456789)
        self.assertEqual(trade.received_time_ns, NOW)

    def test_coinbase_snapshot_and_heartbeat_not_new_trades(self):
        for typ in ("last_match", "heartbeat", "status"):
            self.assertFalse(parse("coinbase", coinbase(type=typ)).trades)

    def test_coinbase_ack_only_requested_match_products(self):
        result = parse(
            "coinbase",
            {
                "type": "subscriptions",
                "channels": [
                    {"name": "heartbeat", "product_ids": ["ETH-USD"]},
                    {"name": "matches", "product_ids": ["BTC-USD", "SOL-USD"]},
                ],
            },
        )
        self.assertEqual(result.acknowledged, {"BTC-USD"})

    def test_kraken_batch_decimal_and_taker_side(self):
        raw = (
            '{"channel":"trade","type":"update","data":['
            '{"symbol":"BTC/USD","trade_id":7,"price":90000.123456789123,'
            '"qty":0.000000000123,"side":"buy",'
            '"timestamp":"2025-10-09T12:30:00.123456Z"}]}'
        )
        trade = ADAPTERS["kraken"].parse(raw, PAIRS, NOW).trades[0]
        self.assertEqual(trade.price, "90000.123456789123")
        self.assertEqual(trade.quantity, "0.000000000123")
        self.assertEqual(trade.taker_side, "buy")
        self.assertEqual(trade.symbol, "BTC-USD")

    def test_kraken_snapshot_ignored_and_ack_per_symbol(self):
        self.assertFalse(parse("kraken", {"channel": "trade", "type": "snapshot"}).trades)
        ack = parse(
            "kraken",
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "trade", "symbol": "BTC/USD"},
            },
        )
        self.assertEqual(ack.acknowledged, {"BTC-USD"})

    def test_gemini_precision_timestamp_and_side(self):
        data = {
            "E": 1759873803503023900,
            "s": "btcusd",
            "t": 2840140956529623,
            "p": "120649.97000",
            "q": "0.0046190900",
            "m": True,
        }
        trade = parse("gemini", data).trades[0]
        self.assertEqual(trade.event_time_ns, data["E"])
        self.assertEqual(trade.trade_id, "2840140956529623")
        self.assertEqual(trade.taker_side, "sell")
        data["m"] = False
        self.assertEqual(parse("gemini", data).trades[0].taker_side, "buy")

    def test_gemini_current_subscribe_format(self):
        self.assertEqual(
            ADAPTERS["gemini"].subscribe(PAIRS),
            {
                "id": "subscribe-1",
                "method": "subscribe",
                "params": ["btcusd@trade", "ethusd@trade"],
            },
        )
        ack = parse("gemini", {"id": "subscribe-1", "status": 200, "result": None})
        self.assertEqual(ack.acknowledged, set(PAIRS))
        self.assertFalse(parse("gemini", {"id": "ping", "status": 200}).acknowledged)

    def test_errors_do_not_look_like_success(self):
        cases = [
            ("coinbase", {"type": "error", "message": "bad symbol"}),
            ("kraken", {"success": False, "error": "bad symbol"}),
            ("gemini", {"id": "subscribe-1", "status": 400}),
        ]
        for exchange, message in cases:
            with self.subTest(exchange=exchange), self.assertRaises(FeedError):
                parse(exchange, message)

    def test_malformed_and_invalid_values_rejected(self):
        for raw in ("{bad", "[]", '{"type":"match"}'):
            with self.subTest(raw=raw), self.assertRaises(FeedError):
                ADAPTERS["coinbase"].parse(raw, PAIRS, NOW)
        for price in ("NaN", "Infinity", "-1", "0"):
            with self.subTest(price=price), self.assertRaises(FeedError):
                parse("coinbase", coinbase(price=price))

    def test_unrequested_symbol_ignored(self):
        self.assertFalse(parse("coinbase", coinbase(product_id="SOL-USD")).trades)

    def test_timezones_and_decimal_validation(self):
        self.assertEqual(
            rfc3339_ns("2025-10-09T12:30:00.123456789Z"),
            rfc3339_ns("2025-10-09T14:30:00.123456789+02:00"),
        )
        self.assertEqual(positive_decimal(Decimal("1.0000000001")), "1.0000000001")
        with self.assertRaises(ValueError):
            positive_decimal(0.1)
        with self.assertRaises(ValueError):
            symbol("BTCUSD")
        self.assertEqual(symbol("eth-usd"), "ETH-USD")
