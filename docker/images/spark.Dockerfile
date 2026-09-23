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
ENV PATH="/opt/venv/bin:${PATH}"

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir ".[spark]"

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
RUN set -eux; \
    base=https://repo1.maven.org/maven2; \
    curl -fsSL -O "$base/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSL -O "$base/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSL -O "$base/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -O "$base/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fsSL -O "$base/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -O "$base/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSL -O "$base/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar"

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

WORKDIR /app
USER voltstream

# No default CMD — the DAG's DockerOperator (D6) supplies the full
# `spark-submit ... --sim-date ...` command per job.
