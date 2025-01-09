//! Cloud bucket storage backend (S3/GCS)
//!
//! Provides remote storage capabilities with intelligent chunking and caching.

#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::path::Path;
use std::sync::Arc;

use super::{DatabaseConfig, SyncStats};

/// Env var that, when set, points the S3 backend at a custom endpoint
/// (e.g. `http://localhost:9000` for a local MinIO emulator).
/// Also read by AWS SDKs; reusing the standard name keeps tooling compatible.
const S3_ENDPOINT_ENV: &str = "AWS_ENDPOINT_URL";

/// Env var that points the GCS backend at a custom endpoint
/// (e.g. `http://localhost:4443` for a local fake-gcs-server emulator).
/// The value should be a full URL including scheme; the official Google
/// Cloud SDKs read the same variable.
const GCS_ENDPOINT_ENV: &str = "STORAGE_EMULATOR_HOST";

/// Bucket storage backend for S3/GCS
#[derive(Clone)]
pub struct BucketStorage {
    store: Arc<dyn ObjectStore>,
    url: String,
    readonly: bool,
}

impl BucketStorage {
    /// Connect to a bucket storage URL.
    ///
    /// Supported URL schemes:
    /// - `s3://bucket/path` — AWS S3 or any S3-compatible service (MinIO, etc.)
    /// - `gs://bucket/path` — Google Cloud Storage
    ///
    /// Environment variables consulted:
    /// - S3:
    ///   - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` — standard AWS credentials
    ///   - `AWS_ENDPOINT_URL` — custom endpoint for S3-compatible services (MinIO, LocalStack).
    ///     When set, path-style addressing is enabled automatically and `allow_http` is permitted
    ///     for non-HTTPS emulator URLs.
    /// - GCS:
    ///   - `GOOGLE_APPLICATION_CREDENTIALS` — path to service account JSON
    ///   - `STORAGE_EMULATOR_HOST` — custom endpoint for fake-gcs-server. When set, the GCS client
    ///     skips OAuth and issues unauthenticated requests against the emulator.
    pub async fn connect(url: &str) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = if let Some(rest) = url.strip_prefix("s3://") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            let bucket = parts[0];

            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

            if let Ok(endpoint) = std::env::var(S3_ENDPOINT_ENV) {
                // S3-compatible emulators (MinIO, LocalStack) use path-style URLs
                // and are typically served over plain HTTP in local dev.
                let allow_http = endpoint.starts_with("http://");
                builder = builder
                    .with_endpoint(&endpoint)
                    .with_virtual_hosted_style_request(false)
                    .with_allow_http(allow_http);
            }

            let s3 = builder.build().context("Failed to build S3 client")?;
            Arc::new(s3)
        } else if let Some(rest) = url.strip_prefix("gs://") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            let bucket = parts[0];

            let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);

            if let Ok(endpoint) = std::env::var(GCS_ENDPOINT_ENV) {
                // fake-gcs-server doesn't speak OAuth. The object_store crate
                // supports this out of the box via a service-account JSON with
                // `disable_oauth: true` and a custom `gcs_base_url`.
                let emulator_url = normalize_gcs_emulator_url(&endpoint);
                let fake_sa = emulator_service_account_json(&emulator_url);
                builder = builder.with_config(GoogleConfigKey::ServiceAccountKey, fake_sa);
            }

            let gcs = builder.build().context("Failed to build GCS client")?;
            Arc::new(gcs)
        } else {
            bail!("Unsupported storage URL. Use s3://bucket/path or gs://bucket/path");
        };

        Ok(Self {
            store,
            url: url.to_string(),
            readonly: false,
        })
    }

    /// Set readonly mode
    pub fn set_readonly(&mut self, readonly: bool) {
        self.readonly = readonly;
    }

    /// Get the base path from URL
    fn base_path(&self) -> String {
        if self.url.starts_with("s3://") {
            let path = self.url.strip_prefix("s3://").unwrap();
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            parts.get(1).unwrap_or(&"").to_string()
        } else if self.url.starts_with("gs://") {
            let path = self.url.strip_prefix("gs://").unwrap();
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            parts.get(1).unwrap_or(&"").to_string()
        } else {
            String::new()
        }
    }

    /// Load database config from bucket
    pub async fn load_config(&self) -> Result<DatabaseConfig> {
        let base = self.base_path();
        let config_path = if base.is_empty() {
            ObjectPath::from(".aresadb/config.toml")
        } else {
            ObjectPath::from(format!("{}/.aresadb/config.toml", base))
        };

        let data = self.store.get(&config_path).await?;
        let bytes = data.bytes().await?;
        let config: DatabaseConfig = toml::from_str(std::str::from_utf8(&bytes)?)?;

        Ok(config)
    }

    /// Save database config to bucket
    pub async fn save_config(&self, config: &DatabaseConfig) -> Result<()> {
        if self.readonly {
            bail!("Cannot write to readonly bucket");
        }

        let base = self.base_path();
        let config_path = if base.is_empty() {
            ObjectPath::from(".aresadb/config.toml")
        } else {
            ObjectPath::from(format!("{}/.aresadb/config.toml", base))
        };

        let config_str = toml::to_string_pretty(config)?;
        self.store
            .put(&config_path, Bytes::from(config_str))
            .await?;

        Ok(())
    }

    /// Upload local database to bucket
    pub async fn upload_from_local(&self, local_path: &Path) -> Result<()> {
        if self.readonly {
            bail!("Cannot write to readonly bucket");
        }

        let base = self.base_path();

        // Upload all files in .aresadb directory
        let aresadb_dir = local_path.join(".aresadb");
        for entry in walkdir::WalkDir::new(&aresadb_dir) {
            let entry = entry?;
            if entry.file_type().is_file() {
                let relative = entry.path().strip_prefix(local_path)?;
                let object_path = if base.is_empty() {
                    ObjectPath::from(relative.to_string_lossy().to_string())
                } else {
                    ObjectPath::from(format!("{}/{}", base, relative.to_string_lossy()))
                };

                let data = tokio::fs::read(entry.path()).await?;
                self.store.put(&object_path, Bytes::from(data)).await?;
            }
        }

        Ok(())
    }

    /// Download bucket contents to local path
    pub async fn download_to_local(&self, local_path: &Path) -> Result<()> {
        let base = self.base_path();
        let prefix = if base.is_empty() {
            None
        } else {
            Some(ObjectPath::from(base.clone()))
        };

        // List all objects
        let mut stream = self.store.list(prefix.as_ref());

        while let Some(result) = stream.next().await {
            let meta = result?;
            let object_path = meta.location;

            // Calculate local path
            let relative = if base.is_empty() {
                object_path.to_string()
            } else {
                object_path
                    .to_string()
                    .strip_prefix(&format!("{}/", base))
                    .unwrap_or(object_path.as_ref())
                    .to_string()
            };

            let local_file = local_path.join(&relative);

            // Create parent directories
            if let Some(parent) = local_file.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            // Download file
            let data = self.store.get(&object_path).await?;
            let bytes = data.bytes().await?;
            tokio::fs::write(&local_file, bytes).await?;
        }

        Ok(())
    }

    /// Bidirectional sync with local path
    pub async fn sync_with_local(&self, local_path: &Path) -> Result<SyncStats> {
        let mut stats = SyncStats::default();
        let base = self.base_path();

        // Get list of remote files with their modification times
        let prefix = if base.is_empty() {
            None
        } else {
            Some(ObjectPath::from(base.clone()))
        };

        let mut remote_files = std::collections::HashMap::new();
        let mut stream = self.store.list(prefix.as_ref());

        while let Some(result) = stream.next().await {
            let meta = result?;
            let path = meta.location.to_string();
            let relative = if base.is_empty() {
                path.clone()
            } else {
                path.strip_prefix(&format!("{}/", base))
                    .unwrap_or(&path)
                    .to_string()
            };
            remote_files.insert(relative, meta.last_modified);
        }

        // Get list of local files
        let aresadb_dir = local_path.join(".aresadb");
        let mut local_files = std::collections::HashMap::new();

        if aresadb_dir.exists() {
            for entry in walkdir::WalkDir::new(&aresadb_dir) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    let relative = entry.path().strip_prefix(local_path)?;
                    let modified = entry.metadata()?.modified()?;
                    local_files.insert(relative.to_string_lossy().to_string(), modified);
                }
            }
        }

        // Upload newer local files
        if !self.readonly {
            for (path, local_time) in &local_files {
                let should_upload = if let Some(remote_time) = remote_files.get(path) {
                    let local_datetime = chrono::DateTime::<chrono::Utc>::from(*local_time);
                    local_datetime > *remote_time
                } else {
                    true
                };

                if should_upload {
                    let local_file = local_path.join(path);
                    let data = tokio::fs::read(&local_file).await?;

                    let object_path = if base.is_empty() {
                        ObjectPath::from(path.clone())
                    } else {
                        ObjectPath::from(format!("{}/{}", base, path))
                    };

                    self.store.put(&object_path, Bytes::from(data)).await?;
                    stats.uploaded += 1;
                }
            }
        }

        // Download newer remote files
        for (path, remote_time) in &remote_files {
            let should_download = if let Some(local_time) = local_files.get(path) {
                let local_datetime = chrono::DateTime::<chrono::Utc>::from(*local_time);
                *remote_time > local_datetime
            } else {
                true
            };

            if should_download {
                let object_path = if base.is_empty() {
                    ObjectPath::from(path.clone())
                } else {
                    ObjectPath::from(format!("{}/{}", base, path))
                };

                let local_file = local_path.join(path);

                // Create parent directories
                if let Some(parent) = local_file.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let data = self.store.get(&object_path).await?;
                let bytes = data.bytes().await?;
                tokio::fs::write(&local_file, bytes).await?;
                stats.downloaded += 1;
            }
        }

        Ok(stats)
    }

    /// Get a single object from bucket
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let base = self.base_path();
        let object_path = if base.is_empty() {
            ObjectPath::from(path.to_string())
        } else {
            ObjectPath::from(format!("{}/{}", base, path))
        };

        let data = self.store.get(&object_path).await?;
        let bytes = data.bytes().await?;
        Ok(bytes)
    }

    /// Put a single object to bucket
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        if self.readonly {
            bail!("Cannot write to readonly bucket");
        }

        let base = self.base_path();
        let object_path = if base.is_empty() {
            ObjectPath::from(path.to_string())
        } else {
            ObjectPath::from(format!("{}/{}", base, path))
        };

        self.store.put(&object_path, data).await?;
        Ok(())
    }

    /// Delete a single object from bucket
    pub async fn delete(&self, path: &str) -> Result<()> {
        if self.readonly {
            bail!("Cannot write to readonly bucket");
        }

        let base = self.base_path();
        let object_path = if base.is_empty() {
            ObjectPath::from(path.to_string())
        } else {
            ObjectPath::from(format!("{}/{}", base, path))
        };

        self.store.delete(&object_path).await?;
        Ok(())
    }

    /// Check if bucket is accessible
    pub async fn check_connection(&self) -> Result<()> {
        let base = self.base_path();
        let prefix = if base.is_empty() {
            None
        } else {
            Some(ObjectPath::from(base))
        };

        // Try to list objects (just get first one to verify access)
        let mut stream = self.store.list(prefix.as_ref());
        let _ = stream.next().await;

        Ok(())
    }
}

/// Normalize a fake-gcs-server endpoint so the `object_store` GCS client
/// targets the `/storage/v1/` base URL it expects.
///
/// Accepts either `http://host:port` or `http://host:port/storage/v1/b`.
fn normalize_gcs_emulator_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.contains("/storage/v1") {
        trimmed.to_string()
    } else {
        format!("{}/storage/v1/b", trimmed)
    }
}

/// Build the minimal service-account JSON that the `object_store` GCS
/// backend recognizes as "emulator mode": OAuth is skipped and requests
/// are issued against `gcs_base_url` unauthenticated.
fn emulator_service_account_json(base_url: &str) -> String {
    // All fields except `gcs_base_url` and `disable_oauth` are placeholders;
    // they satisfy the JSON schema but are never used when oauth is disabled.
    format!(
        r#"{{"gcs_base_url":"{}","disable_oauth":true,"client_email":"fake@emulator.local","private_key":"","private_key_id":""}}"#,
        base_url
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_appends_storage_v1_when_missing() {
        assert_eq!(
            normalize_gcs_emulator_url("http://localhost:4443"),
            "http://localhost:4443/storage/v1/b"
        );
        assert_eq!(
            normalize_gcs_emulator_url("http://localhost:4443/"),
            "http://localhost:4443/storage/v1/b"
        );
    }

    #[test]
    fn normalize_keeps_storage_v1_when_present() {
        assert_eq!(
            normalize_gcs_emulator_url("http://localhost:4443/storage/v1/b"),
            "http://localhost:4443/storage/v1/b"
        );
    }

    #[test]
    fn emulator_json_contains_required_fields() {
        let json = emulator_service_account_json("http://localhost:4443/storage/v1/b");
        assert!(json.contains(r#""gcs_base_url":"http://localhost:4443/storage/v1/b""#));
        assert!(json.contains(r#""disable_oauth":true"#));
        assert!(json.contains(r#""client_email""#));
    }
}
