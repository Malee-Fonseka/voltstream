"""The single `SparkSession` builder (T077, §8.2).

All four Spark entrypoints — the raw archiver, the speed layer, the daily billing job and
the zone rollup — build their session here. One builder means the S3A wiring, the session
time zone and the shuffle sizing are stated once and cannot drift between jobs.

Two settings are load-bearing and easy to get wrong:

- **`path.style.access = true`.** MinIO serves buckets as `http://minio:9000/bucket/key`.
  The S3A default is virtual-host addressing (`http://bucket.minio:9000/key`), which does
  not resolve against MinIO and fails with an obscure `UnknownHostException`.
- **`spark.sql.session.timeZone = UTC`.** Every timestamp in the system is UTC
  (Appendix A). Left unset, Spark adopts the JVM default, so the same `event_ts` would
  derive a different `sim_date` partition on a developer laptop than in the container —
  silently splitting one simulated day across two partition directories.

Connection secrets are read from the process environment rather than through
`voltstream.config`: `config.py` deliberately holds no credentials (T018/T021), and
hadoop-aws needs them as Hadoop properties, not as YAML.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

# Checkpoints live on a named Docker volume, never in /tmp and never on S3A (T044). The
# default matches the volume mount in docker-compose.yml; overridden for local runs.
_DEFAULT_CHECKPOINT_ROOT = "/var/lib/voltstream/checkpoints"

# Single node, tiny post-aggregation volume. Spark's default of 200 shuffle partitions
# would produce 200 mostly-empty tasks per micro-batch — pure scheduling overhead, and on
# a file sink, 200 tiny files per batch (§10.2's small-file problem, self-inflicted).
_SHUFFLE_PARTITIONS = "4"


def checkpoint_root() -> Path:
    """Base directory for Structured Streaming checkpoints."""
    return Path(os.environ.get("VOLTSTREAM_CHECKPOINT_DIR", _DEFAULT_CHECKPOINT_ROOT))


def checkpoint_path(job_name: str) -> str:
    """Checkpoint location for one named streaming query.

    A query's checkpoint is its identity: it holds the committed Kafka offsets and the
    file-sink commit log. Changing this path for a running job silently restarts it from
    the beginning of the topic, so job names are fixed strings, never derived from a
    timestamp or a hostname.
    """
    return str(checkpoint_root() / job_name)


def _s3a_endpoint() -> str:
    """S3A endpoint, preferring the boto3 variable so both clients agree."""
    return (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("MINIO_ENDPOINT")
        or "http://minio:9000"
    )


def build_session(app_name: str, *, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Build (or return) the session for one job.

    `extra_conf` is for genuinely job-specific settings only. Anything that should hold
    across all four jobs belongs in this function, not at a call site.
    """
    endpoint = _s3a_endpoint()

    builder = (
        SparkSession.builder.appName(app_name)
        # ---- Time ----------------------------------------------------------------
        .config("spark.sql.session.timeZone", "UTC")
        # ---- Single-node sizing --------------------------------------------------
        .config("spark.sql.shuffle.partitions", _SHUFFLE_PARTITIONS)
        # ---- S3A / MinIO ---------------------------------------------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        # Required for MinIO — see the module docstring.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "true" if endpoint.startswith("https://") else "false",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # MinIO has no bucket-region lookup; without this S3A probes and fails.
        .config(
            "spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        )
        # ponytail: default (rename-based) commit protocol. Correct on MinIO for our
        # volume, but a copy rather than an atomic rename — §10.2 records it as a
        # limitation. Upgrade path is the S3A magic committer or a table format.
        .config("spark.hadoop.fs.s3a.committer.name", "file")
    )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
