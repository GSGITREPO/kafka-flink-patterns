"""Shared event schema + (de)serialization.

One source of truth for the record that flows through the pipeline so the
producer and every consumer agree on the shape. Kept stdlib-only on purpose.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TradeEvent:
    """A single synthetic trade print.

    Attributes:
        symbol: Instrument identifier; also used as the Kafka partition key so
            all events for one symbol stay ordered on one partition.
        price: Trade price.
        size: Number of shares/contracts traded.
        event_ts: Epoch milliseconds when the trade occurred. This is the
            *event time* the windowing logic buckets on — never wall-clock.
    """

    symbol: str
    price: float
    size: int
    event_ts: int

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")

    @staticmethod
    def from_json(raw: bytes | str) -> "TradeEvent":
        data = json.loads(raw)
        return TradeEvent(
            symbol=str(data["symbol"]),
            price=float(data["price"]),
            size=int(data["size"]),
            event_ts=int(data["event_ts"]),
        )

    def partition_key(self) -> bytes:
        """Keying by symbol routes a symbol's events to a single partition."""
        return self.symbol.encode("utf-8")
