"""Kafka consumer that drives the pure-Python tumbling-window aggregator.

Reads ``TradeEvent`` records from a topic, feeds them into
:class:`windowing.TumblingWindowAggregator`, and prints each window as it
becomes final (its end <= watermark). The interesting logic lives in
``windowing.py``; this file is just the I/O wiring.

Requires: pip install confluent-kafka

Run:
    python src/tumbling_window.py --topic trades --window-seconds 60
"""

from __future__ import annotations

import argparse
import json
import sys

from events import TradeEvent
from windowing import TumblingWindowAggregator

try:
    from confluent_kafka import Consumer
except ImportError:  # pragma: no cover - only hit when the optional dep is absent
    Consumer = None  # type: ignore[assignment]


_POLL_TIMEOUT_SECONDS = 1.0


def _build_consumer(bootstrap: str, group: str) -> "Consumer":
    if Consumer is None:
        sys.exit(
            "confluent-kafka is not installed. Run: pip install confluent-kafka"
        )
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            # Start from the beginning for a fresh group so the demo is
            # reproducible; switch to "latest" for live-tail behavior.
            "auto.offset.reset": "earliest",
            # Commit only after we've processed — at-least-once.
            "enable.auto.commit": True,
        }
    )


def _emit(aggregator: TumblingWindowAggregator) -> None:
    for agg in aggregator.emit_ready():
        print(json.dumps(agg.to_dict(), separators=(",", ":")))


def run(
    bootstrap: str,
    topic: str,
    group: str,
    window_seconds: int,
    allowed_lateness_seconds: int,
) -> int:
    consumer = _build_consumer(bootstrap, group)
    consumer.subscribe([topic])
    aggregator = TumblingWindowAggregator(
        window_size_ms=window_seconds * 1000,
        allowed_lateness_ms=allowed_lateness_seconds * 1000,
    )

    try:
        while True:
            msg = consumer.poll(_POLL_TIMEOUT_SECONDS)
            if msg is None:
                # Idle tick — still flush any windows the watermark has closed.
                _emit(aggregator)
                continue
            if msg.error():
                print(f"[consume-error] {msg.error()}", file=sys.stderr)
                continue

            event = TradeEvent.from_json(msg.value())
            aggregator.add_event(
                symbol=event.symbol,
                price=event.price,
                size=event.size,
                event_ts_ms=event.event_ts,
            )
            _emit(aggregator)
    except KeyboardInterrupt:
        print("\n[shutdown] flushing open windows...", file=sys.stderr)
        for agg in aggregator.flush():
            print(json.dumps(agg.to_dict(), separators=(",", ":")))
        print(f"[shutdown] dropped {aggregator.late_count} late event(s)", file=sys.stderr)
    finally:
        consumer.close()

    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tumbling-window trade aggregator")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="trades")
    parser.add_argument("--group", default="tumbling-window-demo")
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--allowed-lateness-seconds", type=int, default=5)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(
        run(
            args.bootstrap,
            args.topic,
            args.group,
            args.window_seconds,
            args.allowed_lateness_seconds,
        )
    )
