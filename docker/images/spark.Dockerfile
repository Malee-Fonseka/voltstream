# voltstream — Spark image: speed layer, raw archiver, batch jobs (decision T049).
#
# Connector JARs are baked into the image rather than resolved via `--packages` at job
# start (slow, and fails without network). Versions are pinned to what T002 actually
# measured for pyspark==3.5.9 (not guessed): Hadoop 3.3.4, Scala 2.12. Kafka-connector
# and hadoop-aws transitive versions below are read directly from their Maven POMs
# (spark-sql-kafka-0-10_2.12:3.5.9 -> kafka-clients 3.4.1, commons-pool2 2.11.1;
# hadoop-aws:3.3.4 -> aws-java-sdk-bundle 1.12.262, confirming the design doc's estimate).
#
# Multi-stage: one stage builds the Python venv (pyspark + voltstream, non-editable
# install), a second stage only fetches JARs, and the final stage assembles JRE + venv +
# JARs with no leftover build toolchain.

ARG SPARK_KAFKA_CONNECTOR_VERSION=3.5.9
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRESQL_JDBC_VERSION=42.7.4

# ---------------------------------------------------------------------------
# Stage: builder — the Python venv (pyspark + voltstream[spark])
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build
RUN python -m venv /opt/venv
# pip gives up on a download that stalls for 15 s, and does not retry it: a cold build once
# died eleven minutes into pyspark's 318 MB download on a slow link (T175).
ENV PATH="/opt/venv/bin:${PATH}" \
    PIP_DEFAULT_TIMEOUT=120

# Dependencies first, source second. pyspark is a ~317 MB wheel and installing it takes
# the better part of an hour on a slow link; copying src/ before this step put every
# source edit ahead of that layer, so changing one line of Python rebuilt PySpark from
# scratch. Installing the dependencies from pyproject.toml alone keeps that layer cached
# across source changes, and only the (fast) package install below re-runs.
COPY pyproject.toml README.md ./
# The stub carries a __version__ line because [tool.hatch.version] reads it out of this
# file by regex; an empty placeholder fails metadata generation before pip resolves
# anything. The real file replaces it with the next COPY.
RUN mkdir -p src/voltstream \
    && printf "__version__ = \"0.0.0\"\n" > src/voltstream/__init__.py \
    && pip install --no-cache-dir ".[spark]" \
    && pip uninstall -y voltstream

COPY src/ ./src/
# --no-deps: everything above is already installed and pinned; without it pip re-resolves
# the whole tree and can silently pull a different pyspark than the JARs below match.
RUN pip install --no-cache-dir --no-deps ".[spark]"

# ---------------------------------------------------------------------------
# Stage: jars — fetch the connector JARs pyspark does not bundle
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS jars
ARG SPARK_KAFKA_CONNECTOR_VERSION
ARG KAFKA_CLIENTS_VERSION
ARG COMMONS_POOL2_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_BUNDLE_VERSION
ARG POSTGRESQL_JDBC_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /extra-jars
# Retry every failure, not just HTTP 5xx: a cold build once died on one dropped TLS handshake
# (curl exit 35) on a slow link, after the jar before it had taken two minutes (T175).
RUN set -eux; \
    base=https://repo1.maven.org/maven2; \
    fetch() { curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors --connect-timeout 30 -O "$base/$1"; }; \
    fetch "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_CONNECTOR_VERSION}.jar"; \
    fetch "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_CONNECTOR_VERSION}.jar"; \
    fetch "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    fetch "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    fetch "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    fetch "org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar"

# ---------------------------------------------------------------------------
# Final stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system voltstream && useradd --system --gid voltstream --create-home voltstream

COPY --from=builder /opt/venv /opt/venv
COPY --from=jars /extra-jars/*.jar /opt/venv/lib/python3.11/site-packages/pyspark/jars/

ENV PATH="/opt/venv/bin:${PATH}" \
    SPARK_HOME="/opt/venv/lib/python3.11/site-packages/pyspark" \
    PYSPARK_PYTHON="/opt/venv/bin/python" \
    PYSPARK_DRIVER_PYTHON="/opt/venv/bin/python"

# Checkpoint root, created and chowned BEFORE the volume is mounted. Docker seeds a
# fresh named volume from the image's contents at the mount path, ownership included;
# if the path does not exist in the image the volume is created root-owned and the
# voltstream user cannot mkdir inside it.
RUN mkdir -p /var/lib/voltstream/checkpoints \
    && chown -R voltstream:voltstream /var/lib/voltstream

# Versioned configuration, baked in. The long-running services mount ../config over this
# so it can be edited without a rebuild, but a container launched by DockerOperator gets
# no mounts — the DAG starts it through the socket proxy, which has no access to the
# repository on the host. Without a copy in the image the batch jobs cannot find
# base.yaml at all.
#
# Safe to bake because this file holds structure and defaults only: every secret comes
# from the environment (T018), and the package itself is already baked from the same
# commit, so the two cannot drift apart.
COPY config/ /app/config/

WORKDIR /app
USER voltstream

# No default CMD — the DAG's DockerOperator (D6) supplies the full
# `spark-submit ... --sim-date ...` command per job.
