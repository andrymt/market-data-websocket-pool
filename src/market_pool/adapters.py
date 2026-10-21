"""Small, testable protocol boundaries. No credentials or trading endpoints."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal

from .model import Exchange, Trade, positive_decimal, rfc3339_ns


class FeedError(ValueError):
    """Explicit exchange rejection or invalid market-data payload."""


@dataclass
class Parsed:
    trades: list[Trade] = field(default_factory=list)
    acknowledged: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class Adapter:
    exchange: Exchange
    endpoint: str

    def wire_symbol(self, pair: str) -> str:
        if self.exchange == "kraken":
            return pair.replace("-", "/")
        if self.exchange == "gemini":
            return pair.replace("-", "").lower()
        return pair

    def subscribe(self, symbols: tuple[str, ...]) -> dict:
        pairs = [self.wire_symbol(s) for s in symbols]
        if self.exchange == "coinbase":
            return {"type": "subscribe", "product_ids": pairs, "channels": ["matches", "heartbeat"]}
        if self.exchange == "kraken":
            return {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": pairs,
                    "snapshot": False,
                },
                "req_id": 1,
            }
        return {"id": "subscribe-1", "method": "subscribe", "params": [f"{s}@trade" for s in pairs]}

    def parse(self, raw: str | bytes, symbols: tuple[str, ...], received_ns: int) -> Parsed:
        try:
            data = json.loads(raw, parse_float=Decimal)
            if not isinstance(data, dict):
                raise ValueError("Expected a JSON object")
            if data.get("error") or data.get("type") == "error" or data.get("success") is False:
                raise FeedError(f"{self.exchange} rejected message: {data}")
            if self.exchange == "coinbase":
                return self._coinbase(data, symbols, received_ns)
            if self.exchange == "kraken":
                return self._kraken(data, symbols, received_ns)
            return self._gemini(data, symbols, received_ns)
        except FeedError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise FeedError(f"Invalid {self.exchange} payload: {exc}") from exc

    def _trade(self, pair, tid, price, qty, side, event_ns, received_ns) -> Trade:
        return Trade(
            self.exchange,
            pair,
            str(tid),
            positive_decimal(price),
            positive_decimal(qty),
            side,
            event_ns,
            received_ns,
        )

    def _coinbase(self, data, symbols, now) -> Parsed:
        if data.get("type") == "subscriptions":
            acknowledged = set()
            for channel in data.get("channels", []):
                if channel["name"] == "matches":
                    acknowledged.update(set(channel["product_ids"]) & set(symbols))
            return Parsed(acknowledged=acknowledged)
        # last_match is a snapshot; do not pass it off as a newly executed trade.
        if data.get("type") != "match":
            return Parsed()
        pair = data["product_id"]
        if pair not in symbols:
            return Parsed()
        # Coinbase side denotes the MAKER; the normalized schema is always TAKER side.
        maker = data["side"]
        if maker not in ("buy", "sell"):
            raise ValueError("Unknown maker side")
        return Parsed(
            [
                self._trade(
                    pair,
                    data["trade_id"],
                    data["price"],
                    data["size"],
                    "sell" if maker == "buy" else "buy",
                    rfc3339_ns(data["time"]),
                    now,
                )
            ]
        )

    def _kraken(self, data, symbols, now) -> Parsed:
        reverse = {self.wire_symbol(s): s for s in symbols}
        if data.get("method") == "subscribe" and data.get("success") is True:
            result = data["result"]
            pair = reverse.get(result.get("symbol"))
            return Parsed(
                acknowledged={pair} if pair and result.get("channel") == "trade" else set()
            )
        if data.get("channel") != "trade" or data.get("type") != "update":
            return Parsed()
        trades = []
        for item in data["data"]:
            pair = reverse.get(item["symbol"])
            if pair:
                trades.append(
                    self._trade(
                        pair,
                        item["trade_id"],
                        item["price"],
                        item["qty"],
                        item["side"],
                        rfc3339_ns(item["timestamp"]),
                        now,
                    )
                )
        return Parsed(trades)

    def _gemini(self, data, symbols, now) -> Parsed:
        if "status" in data:
            if data["status"] != 200:
                raise FeedError(f"Gemini returned status {data['status']}")
            return Parsed(acknowledged=set(symbols) if data.get("id") == "subscribe-1" else set())
        if not {"s", "t", "p", "q", "m", "E"}.issubset(data):
            return Parsed()
        reverse = {self.wire_symbol(s): s for s in symbols}
        pair = reverse.get(data["s"].lower())
        if not pair:
            return Parsed()
        if not isinstance(data["m"], bool) or not isinstance(data["E"], int):
            raise ValueError("Invalid maker flag or nanosecond event timestamp")
        return Parsed(
            [
                self._trade(
                    pair,
                    data["t"],
                    data["p"],
                    data["q"],
                    "sell" if data["m"] else "buy",
                    data["E"],
                    now,
                )
            ]
        )


ADAPTERS = {
    "coinbase": Adapter("coinbase", "wss://ws-feed.exchange.coinbase.com"),
    "kraken": Adapter("kraken", "wss://ws.kraken.com/v2"),
    "gemini": Adapter("gemini", "wss://ws.gemini.com"),
}
