"""Event-time tumbling-window aggregation engine (pure Python, no Kafka).

This is the heart of the repo and the part the unit tests exercise. It is
deliberately decoupled from any transport so the windowing *semantics* can be
read and tested in isolation:

  * events are bucketed by **event time**, not arrival time;
  * a **watermark** = (max event time seen) - (allowed lateness) decides when a
    window is final;
  * an event whose window has already closed (older than the watermark) is
    **late** and dropped — in a real job it would go to a side output.

A window ``[start, start + size)`` is half-open: ``start`` inclusive, end
exclusive, so adjacent windows never double-count a boundary event.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class WindowKey:
    """Identifies one aggregation bucket: a symbol within a time window."""

    symbol: str
    window_start_ms: int
    window_size_ms: int

    @property
    def window_end_ms(self) -> int:
        return self.window_start_ms + self.window_size_ms


@dataclass
class WindowAggregate:
    """Running aggregate for a single :class:`WindowKey`."""

    key: WindowKey
    count: int = 0
    volume: int = 0          # sum of size
    notional: float = 0.0    # sum of price * size

    def add(self, price: float, size: int) -> None:
        self.count += 1
        self.volume += size
        self.notional += price * size

    @property
    def vwap(self) -> float:
        """Volume-weighted average price; 0.0 for an empty window."""
        return self.notional / self.volume if self.volume else 0.0

    def to_dict(self) -> dict:
        return {
            "symbol": self.key.symbol,
            "window_start_ms": self.key.window_start_ms,
            "window_end_ms": self.key.window_end_ms,
            "count": self.count,
            "volume": self.volume,
            "vwap": round(self.vwap, 6),
        }


def window_start_for(event_ts_ms: int, window_size_ms: int) -> int:
    """Floor an event timestamp to the start of its tumbling window."""
    if window_size_ms <= 0:
        raise ValueError("window_size_ms must be positive")
    return (event_ts_ms // window_size_ms) * window_size_ms


@dataclass
class TumblingWindowAggregator:
    """Accumulates event-time tumbling windows and emits them once final.

    Args:
        window_size_ms: Width of each tumbling window.
        allowed_lateness_ms: How far behind the max-seen event time the
            watermark trails. Larger values tolerate more out-of-order data at
            the cost of holding windows open longer.
    """

    window_size_ms: int
    allowed_lateness_ms: int = 0
    _buckets: dict[WindowKey, WindowAggregate] = field(default_factory=dict)
    _max_event_ts: int = -1
    _late_count: int = 0

    def __post_init__(self) -> None:
        if self.window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        if self.allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be non-negative")

    @property
    def watermark_ms(self) -> int:
        """Everything strictly below this timestamp is considered complete."""
        if self._max_event_ts < 0:
            return -1
        return self._max_event_ts - self.allowed_lateness_ms

    @property
    def late_count(self) -> int:
        return self._late_count

    def add_event(self, symbol: str, price: float, size: int, event_ts_ms: int) -> bool:
        """Route one event into its window.

        Returns:
            True if the event was aggregated, False if it was dropped as late.
        """
        start = window_start_for(event_ts_ms, self.window_size_ms)
        window_end = start + self.window_size_ms

        # A window is closed once its end is at or below the current watermark.
        # An event landing in an already-closed window is late.
        if self._max_event_ts >= 0 and window_end <= self.watermark_ms:
            self._late_count += 1
            return False

        self._max_event_ts = max(self._max_event_ts, event_ts_ms)

        key = WindowKey(symbol, start, self.window_size_ms)
        agg = self._buckets.get(key)
        if agg is None:
            agg = WindowAggregate(key)
            self._buckets[key] = agg
        agg.add(price, size)
        return True

    def emit_ready(self) -> list[WindowAggregate]:
        """Pop and return every window whose end <= watermark (final windows).

        Call this after each event (or on a timer). Returned windows are removed
        from internal state so memory stays bounded as time advances.
        """
        watermark = self.watermark_ms
        if watermark < 0:
            return []
        ready_keys = [
            key for key in self._buckets if key.window_end_ms <= watermark
        ]
        ready = [self._buckets.pop(key) for key in ready_keys]
        ready.sort(key=lambda a: (a.key.window_start_ms, a.key.symbol))
        return ready

    def flush(self) -> list[WindowAggregate]:
        """Emit all remaining open windows (call at end-of-stream)."""
        remaining = list(self._buckets.values())
        self._buckets.clear()
        remaining.sort(key=lambda a: (a.key.window_start_ms, a.key.symbol))
        return remaining
