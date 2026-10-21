"""Normalized trades retain source decimal precision and nanosecond timestamps."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

Exchange = Literal["coinbase", "kraken", "gemini"]
_SYMBOL = re.compile(r"^[A-Z0-9]{2,12}-[A-Z0-9]{2,12}$")
_TIME = re.compile(r"^(.{19})(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$")


def symbol(value: str) -> str:
    value = value.upper().strip()
    if not _SYMBOL.fullmatch(value):
        raise ValueError(f"Use BASE-QUOTE symbols, e.g. BTC-USD: {value!r}")
    return value


def rfc3339_ns(value: str) -> int:
    match = _TIME.fullmatch(value)
    if not match:
        raise ValueError(f"Invalid RFC3339 timestamp: {value!r}")
    whole, fraction, zone = match.groups()
    dt = datetime.fromisoformat(whole + ("+00:00" if zone == "Z" else zone))
    return int(dt.timestamp()) * 1_000_000_000 + int((fraction or "").ljust(9, "0"))


def positive_decimal(value: object) -> str:
    # JSON floating-point values must be decoded as Decimal before reaching this layer.
    if isinstance(value, (float, bool)):
        raise ValueError("Decimal values must not arrive as binary floats or booleans")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal: {value!r}") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("Prices and quantities must be finite and positive")
    return format(number, "f")


@dataclass(frozen=True, slots=True)
class Trade:
    exchange: Exchange
    symbol: str
    trade_id: str
    price: str
    quantity: str
    taker_side: Literal["buy", "sell"]
    event_time_ns: int
    received_time_ns: int

    def __post_init__(self) -> None:
        symbol(self.symbol)
        if self.exchange not in ("coinbase", "kraken", "gemini"):
            raise ValueError("Unknown exchange")
        if not self.trade_id or self.taker_side not in ("buy", "sell"):
            raise ValueError("Invalid trade identity or side")
        if self.event_time_ns <= 0 or self.received_time_ns <= 0:
            raise ValueError("Timestamps must be positive")
        positive_decimal(self.price)
        positive_decimal(self.quantity)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.exchange, self.symbol, self.trade_id

    def as_dict(self) -> dict:
        return {"type": "trade", **asdict(self)}
