#!/bin/bash
# voltstream — kafka-init (decision T046). Creates the two topics, idempotently, then
# exits 0. Run again on every `compose up`; --if-not-exists makes a second run a no-op.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-meter.readings}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-meter.readings.dlq}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"

KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "waiting for kafka at ${BOOTSTRAP}..."
until "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
  sleep 1
done

"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}"

"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${DLQ_TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}"

echo "topics present:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list
