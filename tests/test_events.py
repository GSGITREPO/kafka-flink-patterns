"""Round-trip tests for the shared event schema."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from events import TradeEvent  # noqa: E402


def test_json_round_trip_preserves_fields():
    event = TradeEvent(symbol="AAA", price=123.45, size=100, event_ts=1_700_000_000_000)
    restored = TradeEvent.from_json(event.to_json())
    assert restored == event


def test_from_json_coerces_types():
    raw = '{"symbol":"BBB","price":"9.5","size":"3","event_ts":"42"}'
    event = TradeEvent.from_json(raw)
    assert event.symbol == "BBB"
    assert event.price == 9.5
    assert event.size == 3
    assert event.event_ts == 42


def test_partition_key_is_symbol_bytes():
    event = TradeEvent(symbol="CCC", price=1.0, size=1, event_ts=0)
    assert event.partition_key() == b"CCC"


def test_event_is_frozen():
    event = TradeEvent(symbol="AAA", price=1.0, size=1, event_ts=0)
    try:
        event.price = 2.0  # type: ignore[misc]
    except Exception as exc:  # frozen dataclass raises FrozenInstanceError
        assert "FrozenInstanceError" in type(exc).__name__ or isinstance(exc, AttributeError)
    else:  # pragma: no cover
        raise AssertionError("TradeEvent should be immutable")
