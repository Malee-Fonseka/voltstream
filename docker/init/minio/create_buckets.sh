#!/bin/sh
# voltstream — minio-init (decision T047). Creates the three buckets, idempotently, then
# exits 0. Run again on every `compose up`; `mc mb --ignore-existing` makes a second run
# a no-op.
#
# Per T047: §6.3 also lists a fourth bucket, voltstream-checkpoints, for Spark
# checkpoints. T044 puts checkpoints on named Docker volumes instead (§8.5's own
# object-store commit caveat argues against checkpointing to S3A), so that bucket is
# deliberately NOT created here.
set -eu

mc alias set local "${MINIO_ENDPOINT:-http://minio:9000}" \
  "${MINIO_ROOT_USER:-voltstream}" "${MINIO_ROOT_PASSWORD:-voltstream-dev}"

mc mb --ignore-existing "local/${MINIO_BUCKET_RAW:-voltstream-raw}"
mc mb --ignore-existing "local/${MINIO_BUCKET_LANDING:-voltstream-landing}"
mc mb --ignore-existing "local/${MINIO_BUCKET_ARCHIVE:-voltstream-archive}"

echo "buckets present:"
mc ls local/
