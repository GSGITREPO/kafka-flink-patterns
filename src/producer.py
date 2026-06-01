"""Idempotent, keyed Kafka producer for synthetic trade events.

Demonstrates the production-relevant knobs:
  * idempotent delivery (``enable.idempotence``) so retries don't duplicate;
  * keyed partitioning (by symbol) for per-key ordering;
  * delivery callbacks so failures are observed, not swallowed;
  * a graceful flush on shutdown so no in-flight records are lost.

Requires: pip install confluent-kafka

Run:
    python src/producer.py --topic trades --rate 50 --seconds 30
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from events import TradeEvent

# Imported lazily-friendly: the windowing tests never import this module, so a
# missing confluent-kafka install doesn't break `pytest`.
try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - only hit when the optional dep is absent
    Producer = None  # type: ignore[assignment]


_SYMBOLS = ("AAA", "BBB", "CCC", "DDD", "EEE")
_FLUSH_TIMEOUT_SECONDS = 10


def _build_producer(bootstrap: str) -> "Producer":
    if Producer is None:
        sys.exit(
            "confluent-kafka is not installed. Run: pip install confluent-kafka"
        )
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            # Exactly-once-into-the-log semantics for a single producer session.
            "enable.idempotence": True,
            "acks": "all",
            # Bounded local buffering; block instead of dropping when full.
            "linger.ms": 20,
            "queue.buffering.max.messages": 100_000,
        }
    )


def _on_delivery(err, msg) -> None:
    """Delivery callback — surface failures instead of silently losing data."""
    if err is not None:
        # In a real system this would increment a metric / route to an alert.
        print(f"[delivery-error] {err}", file=sys.stderr)


def _random_event(now_ms: int) -> TradeEvent:
    symbol = random.choice(_SYMBOLS)
    return TradeEvent(
        symbol=symbol,
        price=round(random.uniform(10.0, 500.0), 2),
        size=random.randint(1, 1_000),
        event_ts=now_ms,
    )


def run(bootstrap: str, topic: str, rate: int, seconds: int) -> int:
    producer = _build_producer(bootstrap)
    interval = 1.0 / rate if rate > 0 else 0.0
    deadline = time.time() + seconds
    sent = 0

    try:
        while time.time() < deadline:
            event = _random_event(int(time.time() * 1000))
            producer.produce(
                topic=topic,
                key=event.partition_key(),
                value=event.to_json(),
                on_delivery=_on_delivery,
            )
            # Serve delivery callbacks without blocking the produce loop.
            producer.poll(0)
            sent += 1
            if interval:
                time.sleep(interval)
    finally:
        # Block until every buffered record is acknowledged or fails.
        remaining = producer.flush(_FLUSH_TIMEOUT_SECONDS)
        if remaining:
            print(
                f"[warn] {remaining} message(s) still undelivered after flush",
                file=sys.stderr,
            )

    print(f"produced {sent} event(s) to topic '{topic}'")
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic trade-event producer")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="trades")
    parser.add_argument("--rate", type=int, default=50, help="events per second")
    parser.add_argument("--seconds", type=int, default=30, help="run duration")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(run(args.bootstrap, args.topic, args.rate, args.seconds))
