#!/usr/bin/env bash
# Start local S3 (MinIO) and GCS (fake-gcs-server) emulators for testing.
# Idempotent — safe to run repeatedly.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[aresadb] Starting cloud emulators (MinIO + fake-gcs-server)..."
docker compose -f docker-compose.test.yml up -d minio fake-gcs

# Wait for both services to report healthy
echo "[aresadb] Waiting for emulators to become healthy..."
for _ in $(seq 1 30); do
  minio_healthy=$(docker inspect --format='{{.State.Health.Status}}' \
    "$(docker compose -f docker-compose.test.yml ps -q minio)" 2>/dev/null || echo "starting")
  gcs_healthy=$(docker inspect --format='{{.State.Health.Status}}' \
    "$(docker compose -f docker-compose.test.yml ps -q fake-gcs)" 2>/dev/null || echo "starting")
  if [ "$minio_healthy" = "healthy" ] && [ "$gcs_healthy" = "healthy" ]; then
    break
  fi
  sleep 1
done

# Run one-shot bucket setup containers
docker compose -f docker-compose.test.yml up --no-deps --exit-code-from minio-setup minio-setup
docker compose -f docker-compose.test.yml up --no-deps --exit-code-from fake-gcs-setup fake-gcs-setup

# Verify the buckets actually exist and writes land — we've had silent
# failures before (YAML-folded entrypoints dropping `-H` and friends,
# fake-gcs-server quirks with XML PUTs), so fail fast here.
#
# MinIO is anonymous-public on the test bucket, so an unauthenticated
# list returns 200. fake-gcs-server's /b/<bucket> returns 200 if the
# bucket exists.
echo "[aresadb] Verifying buckets..."

minio_bucket_check=$(curl -s -o /dev/null -w '%{http_code}' \
  "http://localhost:9000/aresadb-tests/?list-type=2" || true)
gcs_bucket_check=$(curl -s -o /dev/null -w '%{http_code}' \
  "http://localhost:4443/storage/v1/b/aresadb-tests" || true)
if [ "$minio_bucket_check" != "200" ]; then
  echo "[aresadb] ERROR: MinIO bucket 'aresadb-tests' not reachable (HTTP $minio_bucket_check)" >&2
  exit 1
fi
if [ "$gcs_bucket_check" != "200" ]; then
  echo "[aresadb] ERROR: fake-gcs bucket 'aresadb-tests' not reachable (HTTP $gcs_bucket_check)" >&2
  exit 1
fi

# Probe: can we actually PUT + GET an object against fake-gcs-server
# using the XML API path that object_store uses? Some older fake-gcs
# builds returned 404 here, which cascaded into cryptic test failures.
echo "[aresadb] Probing fake-gcs XML PUT..."
probe_key="_aresadb_probe.txt"
put_status=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
  --data 'probe' \
  "http://localhost:4443/aresadb-tests/${probe_key}" || true)
get_status=$(curl -s -o /dev/null -w '%{http_code}' \
  "http://localhost:4443/aresadb-tests/${probe_key}" || true)
echo "[aresadb]   XML PUT: ${put_status}, GET: ${get_status}"
if [ "$put_status" != "200" ] && [ "$put_status" != "201" ]; then
  echo "[aresadb] WARNING: fake-gcs-server XML PUT returned ${put_status} (expected 200/201)." >&2
  echo "[aresadb] GCS integration tests will be skipped; S3 tests will still run." >&2
  # When running inside GitHub Actions, propagate the skip to the test job
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "ARESADB_SKIP_GCS_TESTS=1" >> "$GITHUB_ENV"
  fi
fi

echo "[aresadb] Buckets OK."

cat <<EOF
[aresadb] Emulators ready.

  MinIO S3:        http://localhost:9000   (console: http://localhost:9001)
  fake-gcs-server: http://localhost:4443

To run the cloud integration tests:

  export AWS_ENDPOINT_URL=http://localhost:9000
  export AWS_ACCESS_KEY_ID=aresadb-test
  export AWS_SECRET_ACCESS_KEY=aresadb-test-secret
  export AWS_REGION=us-east-1
  export STORAGE_EMULATOR_HOST=http://localhost:4443
  export ARESADB_TEST_S3_BUCKET=aresadb-tests
  export ARESADB_TEST_GCS_BUCKET=aresadb-tests

  cargo test --test cloud_gcs --test cloud_s3 --test cloud_tiered

To stop them:   ./scripts/stop_emulators.sh
EOF
