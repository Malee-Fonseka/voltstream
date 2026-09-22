"""Shared pytest fixtures (decision T064) — currently just the session-scoped Spark
fixture used by the consistency test (`tests/consistency/`) and any future Spark tests, so
a full `pytest` run creates exactly one `SparkSession` (7-12s cold start) rather than one
per test.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest

# D1: on Windows, an unset PYSPARK_PYTHON makes Spark launch worker processes via the
# Microsoft Store `python` alias stub, and every task fails with "Python worker failed to
# connect back". Must be set BEFORE the first SparkSession is built. Harmless on Linux/CI.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402 (must follow the env vars above)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A local, in-memory `SparkSession` shared by every test in the run.

    `local[2]`, UI disabled, one shuffle partition — this is a unit/consistency-test
    session, not a performance one. Tests using this fixture must stay purely in-memory:
    `winutils.exe` / `HADOOP_HOME` are not installed, and are only needed for anything
    touching the local filesystem. A test that needs that is `integration`-marked and runs
    in the Spark container instead (see pyproject.toml's pytest markers).
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("voltstream-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
