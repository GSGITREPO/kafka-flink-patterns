# Kafka + Flink Streaming Patterns

A small, self-contained reference for the streaming patterns I reach for most
often when building real-time data pipelines: a durable event **producer**, a
windowed **aggregation** consumer, and the operational scaffolding (topics,
partitions, consumer groups, watermarks) that makes them behave under load.

Everything here is original, dependency-light, and runnable on a laptop via
`docker-compose`. There is **no employer or proprietary code** — it is a
teaching repo.

## What's inside

| Path | Pattern | Notes |
|---|---|---|
| `docker-compose.yml` | Local single-broker Kafka (KRaft, no ZooKeeper) | One command to a working broker |
| `src/producer.py` | Idempotent event producer | Keyed partitioning, delivery callbacks, graceful flush |
| `src/tumbling_window.py` | Tumbling-window aggregation (pure Python) | Event-time windows + watermark + late-event drop |
| `src/flink_tumbling_window.py` | The same aggregation in PyFlink | Shows the managed-runtime equivalent |
| `src/events.py` | Shared event schema + JSON (de)serialization | One source of truth for the record shape |
| `tests/` | Unit tests for the windowing logic | Runs without a live broker |

## The mental model

```
producer  ──▶  Kafka topic (N partitions, keyed by symbol)  ──▶  windowed consumer
                                                                    │
                                            tumbling 60s event-time windows
                                                                    │
                                                       emit (window, symbol, count, sum, avg)
```

Two design choices worth calling out:

1. **Event time, not processing time.** Each record carries its own
   `event_ts`. Windows are bucketed by that timestamp, so a replay or a slow
   consumer produces the *same* aggregates it would have in real time. A
   **watermark** (`now - allowed_lateness`) decides when a window is closed;
   records older than the watermark are counted as *late* and dropped (or could
   be routed to a side output).

2. **Keyed partitioning.** The producer keys each event by `symbol`, so all
   events for a symbol land on the same partition and are processed in order by
   a single consumer. This is what lets per-key aggregation stay correct
   without a global shuffle.

## Quick start

```bash
# 1. Bring up a local broker
docker compose up -d

# 2. Produce a stream of synthetic trade events
python src/producer.py --topic trades --rate 50 --seconds 30

# 3. In another shell, run the pure-Python windowed aggregator
python src/tumbling_window.py --topic trades --window-seconds 60

# 4. (optional) Run the PyFlink equivalent instead
#    requires: pip install apache-flink
python src/flink_tumbling_window.py
```

## Why both a pure-Python and a Flink version?

The pure-Python consumer (`tumbling_window.py`) makes the *mechanics* of
event-time windowing explicit — you can read exactly how the watermark advances
and how a window flushes. The PyFlink version (`flink_tumbling_window.py`)
shows how the same semantics are expressed declaratively on a managed runtime
that handles state, checkpointing, and recovery for you. Seeing them
side-by-side is the whole point of the repo.

## Running the tests

```bash
python -m pytest -q
```

The tests exercise the windowing logic directly (assigning events to windows,
watermark advancement, late-event handling) and need **no running broker**.

## License

MIT — see [LICENSE](LICENSE).
