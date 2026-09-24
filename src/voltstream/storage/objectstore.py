"""Object-store client and the single source of S3 path conventions (T099, §6.3).

**Every `s3a://` and every bucket key in the system is built here.** That is the point of
the module, not a stylistic preference: the archiver writes a partition, the batch job
reads it back, and the reconciliation job reads it again months later. A path formatted
in three places drifts in three places, and the failure is silent — a job reads an empty
directory and reports zero rows rather than erroring.

Two addressing schemes appear, for the same object:

- `s3a://` — Hadoop's S3A filesystem, which is what Spark reads and writes through.
- bucket + key — what boto3 takes, used by the simulators and health checks.

Both are built from the same bucket names in config, so they cannot disagree about where
something lives.

Credentials and the endpoint come from the environment rather than config: `config.py`
deliberately holds no secrets (T018/T021), and boto3 reads `AWS_ENDPOINT_URL`,
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` natively.
"""

from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from voltstream.config import get_config

if TYPE_CHECKING:  # pragma: no cover - import only for typing
    pass

_RAW_PREFIX = "meter_readings"
_TARIFF_PREFIX = "tariff"
_WEATHER_PREFIX = "weather"


# --------------------------------------------------------------------------------------
# Path conventions (§6.3)
# --------------------------------------------------------------------------------------


def raw_root() -> str:
    """The master dataset root, as Spark addresses it."""
    return f"s3a://{get_config().minio.bucket_raw}/{_RAW_PREFIX}"


def raw_partition_path(sim_date: date, hour: int | None = None) -> str:
    """One simulated day, or one hour within it.

    Partition columns are written by Spark as `sim_date=…/hour=…`, so these strings must
    match that layout exactly — a mismatch reads nothing rather than failing.
    """
    path = f"{raw_root()}/sim_date={sim_date.isoformat()}"
    return path if hour is None else f"{path}/hour={hour}"


def landing_tariff_key(sim_date: date) -> str:
    """Key of the day's tariff drop within the landing bucket."""
    return f"{_TARIFF_PREFIX}/tariff_{sim_date.isoformat()}.csv"


def landing_weather_key(sim_date: date) -> str:
    return f"{_WEATHER_PREFIX}/weather_{sim_date.isoformat()}.csv"


def landing_tariff_path(sim_date: date) -> str:
    """The same tariff file, as Spark addresses it."""
    return f"s3a://{get_config().minio.bucket_landing}/{landing_tariff_key(sim_date)}"


def archive_tariff_path(sim_date: date) -> str:
    """Where a consumed tariff file is archived, per §6.3.

    Partitioned Parquet (`tariff/sim_date=…/`) rather than the CSV it came from. The
    landing zone gets cleaned up; this is what a restatement months later reads, so it
    wants the same typed, columnar format as the master dataset rather than text whose
    types have to be re-inferred — which is exactly how a Decimal rate becomes a float.
    """
    return f"s3a://{get_config().minio.bucket_archive}/{_TARIFF_PREFIX}/sim_date={sim_date.isoformat()}"


# --------------------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------------------


@lru_cache(maxsize=1)
def get_client() -> Any:
    """A boto3 S3 client pointed at MinIO, built once per process.

    boto3 picks up the endpoint and credentials from the environment, so nothing is
    plumbed through here. Imported lazily because the Spark image installs `.[spark]`,
    which has no boto3 — the streaming jobs use the path helpers above and never the
    client, and importing boto3 at module scope would break them.
    """
    import boto3

    return boto3.client("s3")


def healthcheck() -> bool:
    """True when the object store answers and the master-dataset bucket exists.

    Used by `/health/ready` (T105). Checks the bucket rather than just the endpoint: a
    MinIO that is up but missing its buckets fails every write, and reporting that as
    healthy would be worse than reporting nothing.
    """
    try:
        get_client().head_bucket(Bucket=get_config().minio.bucket_raw)
    except Exception:  # noqa: BLE001 - any failure means "not ready", and why is logged by the caller
        return False
    return True


def object_exists(bucket: str, key: str) -> bool:
    """Whether one object is present — the tariff-file sensor's primitive (Phase 9)."""
    try:
        get_client().head_object(Bucket=bucket, Key=key)
    except Exception:  # noqa: BLE001 - boto3 raises ClientError for 404 and for auth alike
        return False
    return True


def endpoint_url() -> str:
    """The configured endpoint, for diagnostics and health-check detail."""
    return os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("MINIO_ENDPOINT") or ""
