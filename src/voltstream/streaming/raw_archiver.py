"""The raw archiver: Kafka to the master dataset (T080, T081, §8.3).

**This job performs no transformation, no validation and no filtering.** It adds an
ingest timestamp and the two partition columns, and writes what it read. That restraint
is the entire point of the master dataset (§5.4): the property that justifies Lambda is
that the raw record survives a bug in every layer above it. A filter here — dropping
records that look invalid *today* — destroys exactly the recovery it exists to provide,
because tomorrow's fix cannot reprocess data that was never written.

Validation belongs to the speed layer and the batch layer, which read this data and may
be wrong about it without consequence. `split_valid_invalid` is deliberately not imported.

Partitioning is by **simulated** date and hour, derived from `event_ts`, so the batch job
reads one day by listing one directory (§10.2's column-pruning and partition-pruning
argument). Ingest time is recorded as a column, never as a partition key.
"""

from __future__ import annotations

import signal
import sys
from types import FrameType

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from voltstream.logging_setup import get_logger
from voltstream.metrics import events_consumed_total, start_metrics_server
from voltstream.storage.objectstore import raw_root
from voltstream.streaming.session import build_session, checkpoint_path
from voltstream.streaming.sinks import observe_e2e_latency
from voltstream.streaming.sources import KafkaLagListener, read_meter_stream

_JOB_NAME = "raw_archiver"
_LAYER = "archiver"

log = get_logger("raw-archiver")


def with_partition_columns(df: DataFrame) -> DataFrame:
    """Add `ingest_ts` and the simulated-time partition columns.

    `sim_date` and `hour` come from `event_ts`, which is simulated time, so a reading
    lands in the simulated day it describes regardless of when it arrived. Deriving them
    from arrival time instead would scatter one simulated day across several directories
    whenever a dropped-out meter flushed its buffer (D3).
    """
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("sim_date", F.to_date(F.col("event_ts")))
        .withColumn("hour", F.hour(F.col("event_ts")))
    )


def _write_batch(batch_df: DataFrame, batch_id: int, *, output_path: str) -> None:
    """Write one micro-batch, then emit its metrics and log line (T081).

    §5.7's honest limitation applies here: these metrics are emitted from the **driver**
    inside `foreachBatch`, not scraped from the executors that did the work. Executors are
    transient and would need a push gateway. The counts are exact; what is approximated is
    attribution to a specific executor, which nothing in this project needs.
    """
    batch_df.persist()
    try:
        rows_in = batch_df.count()

        (
            batch_df.write.mode("append")
            .partitionBy("sim_date", "hour")
            .option("compression", "snappy")
            .parquet(output_path)
        )

        if rows_in:
            # Per record, after the Parquet write has committed: Kafka to master dataset.
            # Consumer lag is not set here — KafkaLagListener derives it from the query's
            # progress, which knows the newest offset in Kafka; a micro-batch does not.
            observe_e2e_latency(batch_df, "kafka_timestamp", _LAYER)
            events_consumed_total.labels(layer=_LAYER).inc(rows_in)

        log.info(
            "micro-batch archived",
            extra={
                "stage": "archive",
                "batch_id": batch_id,
                "rows_in": rows_in,
                # No transformation, so rows_out == rows_in by construction. Emitted
                # anyway: the day these differ, the archiver has stopped being raw.
                "rows_out": rows_in,
                "rows_rejected": 0,
            },
        )
    finally:
        batch_df.unpersist()


def start(await_termination: bool = True) -> StreamingQuery:
    """Build the session, start the archiving query, and return it."""
    output_path = raw_root()

    spark = build_session(f"voltstream-{_JOB_NAME}")
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(KafkaLagListener({_JOB_NAME: _LAYER}))

    stream = with_partition_columns(read_meter_stream(spark))

    query = (
        stream.writeStream.queryName(_JOB_NAME)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(_JOB_NAME))
        .foreachBatch(lambda df, bid: _write_batch(df, bid, output_path=output_path))
        .start()
    )

    log.info(
        "archiver started",
        extra={
            "stage": "archive",
            "output_path": output_path,
            "checkpoint": checkpoint_path(_JOB_NAME),
        },
    )

    if await_termination:
        query.awaitTermination()
    return query


def main() -> None:
    start_metrics_server()

    def _stop(signum: int, _frame: FrameType | None) -> None:
        # Compose sends SIGTERM on `stop`; exiting non-zero would make an ordinary
        # shutdown look like a crash in the restart test (T083).
        log.info("shutdown requested", extra={"stage": "archive", "signal": signum})
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    start()


if __name__ == "__main__":
    main()
