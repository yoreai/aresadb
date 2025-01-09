//! Real-cloud smoke tests.
//!
//! These tests run against *actual* GCS or S3 buckets, not emulators. They
//! close the last few percent of confidence that emulator-based tests can't
//! cover — OAuth token refresh, IAM edge cases, real network paths, and
//! production auth flows.
//!
//! They are **off by default**. To run them you must:
//!
//! 1. Unset `STORAGE_EMULATOR_HOST` / `AWS_ENDPOINT_URL` (or start a new shell).
//! 2. Set real credentials:
//!    - GCS: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json` pointing to
//!      a service account with `roles/storage.objectAdmin` on the test bucket.
//!    - S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
//!      (and optionally `AWS_SESSION_TOKEN`).
//! 3. Set the target bucket via `ARESADB_REAL_GCS_BUCKET` or `ARESADB_REAL_S3_BUCKET`.
//! 4. Run: `cargo test --test cloud_real -- --nocapture`.
//!
//! The tests clean up after themselves (deleting the test prefix on
//! teardown) and use a unique UUID-namespaced prefix to ensure parallel
//! runs don't collide.

use aresadb::BucketStorage;
use bytes::Bytes;
use uuid::Uuid;

fn real_gcs_url() -> Option<String> {
    let bucket = std::env::var("ARESADB_REAL_GCS_BUCKET").ok()?;
    if std::env::var("STORAGE_EMULATOR_HOST").is_ok() {
        eprintln!("[skip] STORAGE_EMULATOR_HOST is set — refusing to run real-GCS test");
        return None;
    }
    let prefix = format!("aresadb-smoke/{}", Uuid::new_v4());
    Some(format!("gs://{}/{}", bucket, prefix))
}

fn real_s3_url() -> Option<String> {
    let bucket = std::env::var("ARESADB_REAL_S3_BUCKET").ok()?;
    if std::env::var("AWS_ENDPOINT_URL").is_ok() {
        eprintln!("[skip] AWS_ENDPOINT_URL is set — refusing to run real-S3 test");
        return None;
    }
    let prefix = format!("aresadb-smoke/{}", Uuid::new_v4());
    Some(format!("s3://{}/{}", bucket, prefix))
}

/// Single smoke test: connect, put, get, delete, confirm deleted.
async fn smoke(url: &str) -> anyhow::Result<()> {
    let bucket = BucketStorage::connect(url).await?;
    bucket.check_connection().await?;

    let payload = Bytes::from_static(b"aresadb-real-cloud-smoke-test");
    bucket.put("smoke.bin", payload.clone()).await?;

    let got = bucket.get("smoke.bin").await?;
    assert_eq!(got, payload, "bytes round-trip intact against real cloud");

    bucket.delete("smoke.bin").await?;
    let after = bucket.get("smoke.bin").await;
    assert!(after.is_err(), "get after delete should error");

    Ok(())
}

#[tokio::test]
async fn smoke_test_real_gcs() {
    let Some(url) = real_gcs_url() else {
        eprintln!("[skip] ARESADB_REAL_GCS_BUCKET not set");
        return;
    };
    eprintln!("[real-gcs] target: {}", url);
    smoke(&url).await.expect("real-GCS smoke passes");
}

#[tokio::test]
async fn smoke_test_real_s3() {
    let Some(url) = real_s3_url() else {
        eprintln!("[skip] ARESADB_REAL_S3_BUCKET not set");
        return;
    };
    eprintln!("[real-s3] target: {}", url);
    smoke(&url).await.expect("real-S3 smoke passes");
}
