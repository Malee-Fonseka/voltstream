"""Postgres and dead-letter sinks for the streaming jobs (T086, T087, §8.3).

JDBC has no upsert, and Structured Streaming's built-in JDBC sink can only append. Both
speed-view tables are keyed and revised in place — a window's totals grow as late data
arrives, and a household's running bill is rewritten every micro-batch — so the write has
to be `INSERT … ON CONFLICT … DO UPDATE`. That means going through psycopg on the driver
inside `foreachBatch` rather than through the JDBC sink.

Collecting a micro-batch to the driver is only defensible because the batches are small:
around 500 rows post-aggregation (§10.3, "one write of ~500 rows per micro-batch, not 500
round-trips"). `MAX_UPSERT_ROWS` makes that assumption fail loudly instead of quietly
turning the driver into a bottleneck if volume ever grows.

The DLQ write goes through Spark's own Kafka sink, not a Python Kafka client: the
connector JAR is already in the Spark image, and `confluent-kafka` is in the `sim` extra,
which that image does not install.
"""

from __future__ import annotations

from collections import Counter

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from voltstream.config import get_config
from voltstream.logging_setup import get_logger
from voltstream.metrics import e2e_latency_seconds, records_rejected_total
from voltstream.storage.postgres import connect

log = get_logger("sinks")

# Rows per micro-batch above which the collect-to-driver approach stops being reasonable.
# Post-aggregation batches are ~500 rows; an order of magnitude of headroom means this
# only fires if something has genuinely changed about the shape of the data.
MAX_UPSERT_ROWS = 5_000


def pg_connection_string() -> str:
    """libpq connection string from config. The password is env-only (T018)."""
    pg = get_config().postgres
    return f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"


def upsert_batch(
    df: DataFrame,
    table: str,
    conflict_cols: tuple[str, ...],
    *,
    max_rows: int = MAX_UPSERT_ROWS,
) -> int:
    """Upsert a micro-batch into `table`, keyed on `conflict_cols`. Returns rows written.

    Every non-key column is overwritten on conflict, which is what the speed view wants:
    the newest computation of a window or a running total is always the right one.
    """
    columns = list(df.columns)
    rows = [tuple(row) for row in df.collect()]
    if not rows:
        return 0

    if len(rows) > max_rows:
        # Loud, but not fatal: dropping the batch would lose data, and the write still
        # succeeds. The point is that the assumption behind this design is now false.
        log.warning(
            "micro-batch exceeds the collect-to-driver ceiling",
            extra={
                "stage": "sink",
                "table": table,
                "rows": len(rows),
                "ceiling": max_rows,
                "msg_detail": "upsert_batch collects to the driver; move this table to a "
                "partition-parallel writer if this persists",
            },
        )

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_cols)
    # updated_at is not in the DataFrame; the table defaults it on insert, so it has to be
    # set explicitly on update or a revised row keeps its original timestamp.
    updates += ", updated_at = now()"

    statement = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {updates}"
    )

    # psycopg 3 has no execute_values (that was psycopg2); executemany is pipelined and
    # sends the whole batch in one round trip, which is what the design note is after.
    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.executemany(statement, rows)
        conn.commit()

    return len(rows)


def observe_e2e_latency(df: DataFrame, produced_at_col: str, layer: str) -> int:
    """Observe `voltstream_e2e_latency_seconds{layer}` once per row of `df`: wall-clock
    now minus the Kafka record timestamp in `produced_at_col`. Returns observations made.

    Call it after the sink has committed, so the figure includes the write. The origin is
    the Kafka record timestamp (T030), never `event_ts`, which is simulated and would read
    288 times too large. Casting a timestamp to double keeps its microseconds, where
    `unix_timestamp()` truncated to whole seconds — below the histogram's first bucket.
    """
    latencies = df.select(
        (F.current_timestamp().cast("double") - F.col(produced_at_col).cast("double")).alias(
            "seconds"
        )
    ).collect()

    observed = 0
    for (seconds,) in latencies:
        # Negative only when the producer's clock runs ahead of the driver's. That is not
        # a latency, so it is dropped rather than clamped to zero.
        if seconds is not None and seconds >= 0:
            e2e_latency_seconds.labels(layer=layer).observe(seconds)
            observed += 1
    return observed


INSERT_REJECTED = (
    "INSERT INTO rejected_records (stage, reason, trace_id, raw_payload) VALUES (%s, %s, %s, %s)"
)

RejectedRow = tuple[str, str, str | None, str]


def _prepared_rejects(df: DataFrame, stage: str) -> DataFrame:
    # trace_id is nulled rather than assumed when the caller pruned it away. The column
    # is nullable in rejected_records precisely because not every reader carries it, and
    # a sink shared by two layers must not fail on the one that reads fewer columns.
    trace_col = (
        F.col("trace_id").cast("string") if "trace_id" in df.columns else F.lit(None).cast("string")
    )
    return df.select(
        F.lit(stage).alias("stage"),
        F.col("reason"),
        trace_col.alias("trace_id"),
        F.col("payload").alias("raw_payload"),
    )


def _count_rejects(rows: list[RejectedRow], stage: str) -> None:
    for reason, count in Counter(r[1] for r in rows).items():
        records_rejected_total.labels(layer=stage, reason=reason).inc(count)


def collect_rejected(df: DataFrame, stage: str) -> list[RejectedRow]:
    """Rejected records as `rejected_records` rows, counted but not written (R23).

    For the batch layer, which writes them in the same transaction as the day's bills:
    replacing the day's earlier batch rejects, so a retry or a restatement does not add
    another full set, and never publishing them to the DLQ, where the speed layer has
    already put every one of them.
    """
    rows: list[RejectedRow] = [
        (r["stage"], r["reason"], r["trace_id"], r["raw_payload"])
        for r in _prepared_rejects(df, stage).collect()
    ]
    _count_rejects(rows, stage)
    return rows


def write_rejected(df: DataFrame, stage: str) -> int:
    """Write the speed layer's rejected records to `rejected_records` and the DLQ topic.

    Both destinations, not one: the table is queryable for the report and joins against
    the rest of the serving layer, while the topic keeps a rejected record replayable
    once the reason it was rejected is fixed. They carry the same `trace_id`, so one
    identifier follows a bad record through both. The batch layer uses
    `collect_rejected` instead.
    """
    config = get_config()

    prepared = _prepared_rejects(df, stage)
    rows: list[RejectedRow] = [
        (r["stage"], r["reason"], r["trace_id"], r["raw_payload"]) for r in prepared.collect()
    ]
    if not rows:
        return 0

    with connect(pg_connection_string()) as conn, conn.cursor() as cur:
        cur.executemany(INSERT_REJECTED, rows)
        conn.commit()

    # Spark's Kafka sink, keyed by trace_id so a record's rejection sits in the same
    # partition every time it is replayed.
    (
        prepared.select(
            F.col("trace_id").alias("key"),
            F.to_json(F.struct("stage", "reason", "trace_id", "raw_payload")).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("topic", config.kafka.dlq_topic)
        .save()
    )

    _count_rejects(rows, stage)
    return len(rows)
