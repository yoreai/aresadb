# AresaDB Test Suite

Integration and property tests live alongside the unit tests baked into each source module.

## Layout

| File | Purpose |
|------|---------|
| `integration_tests.rs` | End-to-end coverage across storage, query, and graph traversal |
| `property_tests.rs` | Proptest-driven invariants |
| `stress_tests.rs` | Concurrent read/write, large dataset, bulk insert |
| `cloud_gcs.rs` | GCS backend (fake-gcs-server emulator) |
| `cloud_s3.rs` | S3 backend (MinIO emulator) |
| `cloud_tiered.rs` | Tiered storage (evict / promote / cloud read / cache) — parameterized across both backends |
| `cloud_real.rs` | Gated smoke tests against real GCS / S3 |

## Running everything (no cloud)

```bash
cargo test
```

This runs all unit tests plus the integration / property / stress suites. The cloud test files **skip cleanly** when their emulator env vars are unset, so they're safe to include.

## Running cloud integration tests locally

Cloud tests exercise `src/storage/bucket.rs` and the tiered-storage cloud paths against live emulators. No credentials, no cost.

### Prereqs

- Docker (or a compatible container runtime)
- The first-time images are pulled; subsequent runs reuse them

### Start emulators

```bash
./scripts/start_emulators.sh
```

This spins up:
- **MinIO** on `http://localhost:9000` (S3-compatible, console on `:9001`)
- **fake-gcs-server** on `http://localhost:4443` (GCS-compatible)

Both store data in `tmpfs` — nothing persists between runs.

### Run the tests

```bash
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=aresadb-test
export AWS_SECRET_ACCESS_KEY=aresadb-test-secret
export AWS_REGION=us-east-1
export STORAGE_EMULATOR_HOST=http://localhost:4443
export ARESADB_TEST_S3_BUCKET=aresadb-tests
export ARESADB_TEST_GCS_BUCKET=aresadb-tests

cargo test --test cloud_gcs --test cloud_s3 --test cloud_tiered
```

### Tear down

```bash
./scripts/stop_emulators.sh
```

### Note on the GCS emulator

`object_store` 0.9 uses the GCS **XML** upload API, and current
`fake-gcs-server` builds respond to that API with 404 in some cases.
`start_emulators.sh` probes this at startup with a direct PUT; if it
fails, the script sets `ARESADB_SKIP_GCS_TESTS=1` (via `GITHUB_ENV`
when running in CI) and the GCS-write tests skip gracefully with a
clear reason. The S3 suite and the S3 half of the tiered-storage
tests are unaffected and run fully. The real-cloud smoke test
(`tests/cloud_real.rs`) covers the actual GCS XML API end-to-end when
you wire up secrets.

## Running real-cloud smoke tests

These tests close the remaining confidence gap that emulators can't cover — OAuth token refresh, IAM, production network paths. They're off by default and must be explicitly opted in.

### GCS

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export ARESADB_REAL_GCS_BUCKET=my-test-bucket
unset STORAGE_EMULATOR_HOST   # make sure we're not hitting the emulator

cargo test --test cloud_real smoke_test_real_gcs -- --nocapture
```

### S3

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export ARESADB_REAL_S3_BUCKET=my-test-bucket
unset AWS_ENDPOINT_URL

cargo test --test cloud_real smoke_test_real_s3 -- --nocapture
```

Each smoke test uses a unique UUID-namespaced prefix and cleans up after itself.

## CI

The `cloud-integration` job in `.github/workflows/ci.yml` runs the emulator-based tests on every push and PR. Real-cloud smoke tests are not run in CI by default; see `docs/cloud-testing-setup.md` for instructions on wiring them up with repository secrets.
