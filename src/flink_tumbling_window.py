"""The same event-time tumbling-window aggregation, expressed in PyFlink.

Where ``tumbling_window.py`` spells out the watermark + window mechanics by
hand, this version delegates state, checkpointing, and recovery to the Flink
runtime and declares only the *semantics*:

  * a Kafka source with a JSON deserializer,
  * an event-time watermark strategy with bounded out-of-orderness,
  * keyBy(symbol) + a TumblingEventTimeWindows assigner,
  * a ProcessWindowFunction that emits count / volume / VWAP per window.

This is intentionally illustrative — it shows the shape of the managed-runtime
equivalent rather than being a turnkey deployment.

Requires: pip install apache-flink

Run (after `docker compose up -d` and producing some events):
    python src/flink_tumbling_window.py
"""

from __future__ import annotations

import json

try:
    from pyflink.common import Duration, Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )
    from pyflink.datastream.functions import ProcessWindowFunction
    from pyflink.datastream.window import TumblingEventTimeWindows, Time
except ImportError as exc:  # pragma: no cover - optional heavy dependency
    raise SystemExit(
        "apache-flink is not installed. Run: pip install apache-flink"
    ) from exc


WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 5
BOOTSTRAP = "localhost:9092"
TOPIC = "trades"


class _EventTsAssigner(TimestampAssigner):
    """Extract event time from each record's ``event_ts`` field (epoch ms)."""

    def extract_timestamp(self, value, record_timestamp):
        return int(json.loads(value)["event_ts"])


class _VwapWindow(ProcessWindowFunction):
    """Aggregate count / volume / VWAP across all records in one window."""

    def process(self, key, context, elements):
        count = 0
        volume = 0
        notional = 0.0
        for raw in elements:
            rec = json.loads(raw)
            size = int(rec["size"])
            count += 1
            volume += size
            notional += float(rec["price"]) * size
        vwap = notional / volume if volume else 0.0
        window = context.window()
        yield json.dumps(
            {
                "symbol": key,
                "window_start_ms": window.start,
                "window_end_ms": window.end,
                "count": count,
                "volume": volume,
                "vwap": round(vwap, 6),
            },
            separators=(",", ":"),
        )


def build_job() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-tumbling-window-demo")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(ALLOWED_LATENESS_SECONDS)
        ).with_timestamp_assigner(_EventTsAssigner())
    )

    stream = env.from_source(
        source, watermark_strategy, "kafka-trades-source"
    )

    (
        stream.key_by(lambda raw: json.loads(raw)["symbol"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(_VwapWindow(), output_type=Types.STRING())
        .print()
    )

    return env


if __name__ == "__main__":
    build_job().execute("kafka-flink-tumbling-window")
