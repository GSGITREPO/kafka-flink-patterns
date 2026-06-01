"""Unit tests for the event-time tumbling-window engine.

These run with no Kafka broker and no optional dependencies — they exercise the
pure windowing semantics in ``src/windowing.py``.
"""

import os
import sys

import pytest

# Make ``src`` importable when running ``pytest`` from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from windowing import (  # noqa: E402
    TumblingWindowAggregator,
    WindowKey,
    window_start_for,
)


SIZE_MS = 60_000  # 60-second windows


def test_window_start_floors_to_bucket():
    assert window_start_for(0, SIZE_MS) == 0
    assert window_start_for(59_999, SIZE_MS) == 0
    assert window_start_for(60_000, SIZE_MS) == 60_000
    assert window_start_for(125_000, SIZE_MS) == 120_000


def test_window_start_rejects_nonpositive_size():
    with pytest.raises(ValueError):
        window_start_for(1000, 0)


def test_boundary_event_belongs_to_next_window():
    # Half-open [start, end): an event exactly at end starts the next window.
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS)
    assert agg.add_event("AAA", 10.0, 1, event_ts_ms=59_999) is True
    assert agg.add_event("AAA", 10.0, 1, event_ts_ms=60_000) is True
    # Flush everything; two distinct windows must exist.
    windows = agg.flush()
    starts = sorted(w.key.window_start_ms for w in windows)
    assert starts == [0, 60_000]


def test_vwap_is_volume_weighted():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS)
    # Two trades in the same window: (price=10, size=100) and (price=20, size=300)
    agg.add_event("AAA", 10.0, 100, event_ts_ms=1_000)
    agg.add_event("AAA", 20.0, 300, event_ts_ms=2_000)
    [window] = agg.flush()
    assert window.count == 2
    assert window.volume == 400
    # VWAP = (10*100 + 20*300) / 400 = 7000/400 = 17.5
    assert window.vwap == pytest.approx(17.5)


def test_separate_symbols_get_separate_buckets():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS)
    agg.add_event("AAA", 10.0, 1, event_ts_ms=1_000)
    agg.add_event("BBB", 20.0, 1, event_ts_ms=1_000)
    windows = agg.flush()
    symbols = sorted(w.key.symbol for w in windows)
    assert symbols == ["AAA", "BBB"]


def test_watermark_trails_max_event_by_allowed_lateness():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS, allowed_lateness_ms=5_000)
    agg.add_event("AAA", 10.0, 1, event_ts_ms=100_000)
    assert agg.watermark_ms == 95_000


def test_emit_ready_returns_only_finalized_windows():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS, allowed_lateness_ms=0)
    # Fill the first window, then jump well into a later window so the first
    # window's end (60_000) drops at or below the watermark.
    agg.add_event("AAA", 10.0, 1, event_ts_ms=10_000)   # window [0, 60_000)
    agg.add_event("AAA", 11.0, 1, event_ts_ms=130_000)  # window [120k, 180k)
    ready = agg.emit_ready()
    ready_starts = [w.key.window_start_ms for w in ready]
    # Window [0,60k) is final (end 60k <= watermark 130k); [120k,180k) is open.
    assert 0 in ready_starts
    assert 120_000 not in ready_starts


def test_late_event_is_dropped_and_counted():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS, allowed_lateness_ms=0)
    # Advance the watermark far forward.
    agg.add_event("AAA", 10.0, 1, event_ts_ms=200_000)  # watermark -> 200_000
    # Now a straggler for an already-closed early window.
    accepted = agg.add_event("AAA", 10.0, 1, event_ts_ms=1_000)
    assert accepted is False
    assert agg.late_count == 1


def test_emit_ready_removes_emitted_windows():
    agg = TumblingWindowAggregator(window_size_ms=SIZE_MS, allowed_lateness_ms=0)
    agg.add_event("AAA", 10.0, 1, event_ts_ms=10_000)
    agg.add_event("AAA", 11.0, 1, event_ts_ms=130_000)
    first = agg.emit_ready()
    assert first  # something was emitted
    # A second call without new events must not re-emit the same window.
    assert agg.emit_ready() == []


def test_window_key_end_is_start_plus_size():
    key = WindowKey("AAA", window_start_ms=120_000, window_size_ms=SIZE_MS)
    assert key.window_end_ms == 180_000


def test_constructor_rejects_bad_params():
    with pytest.raises(ValueError):
        TumblingWindowAggregator(window_size_ms=0)
    with pytest.raises(ValueError):
        TumblingWindowAggregator(window_size_ms=SIZE_MS, allowed_lateness_ms=-1)
