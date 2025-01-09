//! Helpers for cloud integration tests (GCS, S3, and tiered-storage cloud paths).
//!
//! Tests use local emulators (fake-gcs-server for GCS, MinIO for S3) by default.
//! Credentials and endpoints are read from environment variables; tests that
//! depend on them should call [`gcs_test_env`] or [`s3_test_env`] and skip
//! gracefully when the emulator isn't running.
//!
//! To run the tests locally:
//!
//! ```bash
//! ./scripts/start_emulators.sh
//! source <(./scripts/start_emulators.sh | grep export)  # or set env manually
//! cargo test --test cloud_gcs --test cloud_s3 --test cloud_tiered
//! ```

#![allow(dead_code)]

use uuid::Uuid;

/// Result of probing for a cloud emulator — either ready to go or a reason
/// to skip tests with a clear message.
pub enum EmulatorProbe {
    Ready(EmulatorEnv),
    Skip(&'static str),
}

/// Environment required to exercise a cloud backend.
#[derive(Clone)]
pub struct EmulatorEnv {
    /// Bucket name to use (pre-created by the setup scripts).
    pub bucket: String,
    /// Unique per-test prefix underneath the bucket — use it so parallel
    /// tests can't clobber each other.
    pub prefix: String,
}

impl EmulatorEnv {
    /// Build a `gs://` URL for the given prefix (optionally with a sub-path).
    pub fn gs_url(&self, subpath: &str) -> String {
        if subpath.is_empty() {
            format!("gs://{}/{}", self.bucket, self.prefix)
        } else {
            format!("gs://{}/{}/{}", self.bucket, self.prefix, subpath)
        }
    }

    /// Build an `s3://` URL for the given prefix (optionally with a sub-path).
    pub fn s3_url(&self, subpath: &str) -> String {
        if subpath.is_empty() {
            format!("s3://{}/{}", self.bucket, self.prefix)
        } else {
            format!("s3://{}/{}/{}", self.bucket, self.prefix, subpath)
        }
    }
}

/// Probe for a running fake-gcs-server emulator.
/// Returns `Skip` with a reason if `STORAGE_EMULATOR_HOST` is unset or
/// if `ARESADB_SKIP_GCS_TESTS` is truthy (set by the start-emulators
/// script when the XML PUT probe fails against the running container).
pub fn gcs_test_env() -> EmulatorProbe {
    if std::env::var("STORAGE_EMULATOR_HOST").is_err() {
        return EmulatorProbe::Skip(
            "STORAGE_EMULATOR_HOST not set — run ./scripts/start_emulators.sh and export vars",
        );
    }
    if is_truthy("ARESADB_SKIP_GCS_TESTS") {
        return EmulatorProbe::Skip(
            "ARESADB_SKIP_GCS_TESTS is set — emulator does not support XML PUTs used by object_store 0.9",
        );
    }
    let bucket =
        std::env::var("ARESADB_TEST_GCS_BUCKET").unwrap_or_else(|_| "aresadb-tests".to_string());
    EmulatorProbe::Ready(EmulatorEnv {
        bucket,
        prefix: format!("run-{}", Uuid::new_v4()),
    })
}

fn is_truthy(var: &str) -> bool {
    matches!(
        std::env::var(var).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

/// Probe for a running MinIO (S3) emulator.
/// Returns `Skip` if the required AWS env vars aren't present.
pub fn s3_test_env() -> EmulatorProbe {
    if std::env::var("AWS_ENDPOINT_URL").is_err() {
        return EmulatorProbe::Skip(
            "AWS_ENDPOINT_URL not set — run ./scripts/start_emulators.sh and export vars",
        );
    }
    if std::env::var("AWS_ACCESS_KEY_ID").is_err()
        || std::env::var("AWS_SECRET_ACCESS_KEY").is_err()
    {
        return EmulatorProbe::Skip("AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY not set for MinIO");
    }
    // AWS_REGION is required by the S3 client but MinIO doesn't care about the value.
    if std::env::var("AWS_REGION").is_err() {
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let bucket =
        std::env::var("ARESADB_TEST_S3_BUCKET").unwrap_or_else(|_| "aresadb-tests".to_string());
    EmulatorProbe::Ready(EmulatorEnv {
        bucket,
        prefix: format!("run-{}", Uuid::new_v4()),
    })
}

/// Unwrap an [`EmulatorProbe`] or return early with a skip message.
///
/// Kept as a plain function (not a macro) so it works uniformly across
/// Cargo's one-binary-per-test layout.
#[track_caller]
pub fn unwrap_or_skip(probe: EmulatorProbe) -> Option<EmulatorEnv> {
    match probe {
        EmulatorProbe::Ready(env) => Some(env),
        EmulatorProbe::Skip(reason) => {
            eprintln!("[aresadb::cloud-tests] skipping: {}", reason);
            None
        }
    }
}
