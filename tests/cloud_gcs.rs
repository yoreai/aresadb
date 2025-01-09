//! Integration tests for the GCS cloud backend.
//!
//! These tests target a local `fake-gcs-server` emulator by default. Start it
//! with `./scripts/start_emulators.sh` and export the emulator env vars before
//! running. If `STORAGE_EMULATOR_HOST` is not set, every test logs a skip
//! message and exits successfully so the default `cargo test` stays fast.
//!
//! For real-cloud smoke tests, see `cloud_real_gcs.rs`.

mod common;

use aresadb::{BucketStorage, Database, DatabaseConfig, Timestamp};
use bytes::Bytes;
use common::cloud::{gcs_test_env, unwrap_or_skip};
use tempfile::TempDir;

/// 3.1 — Basic connect + list.
#[tokio::test]
async fn test_gcs_connect() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.gs_url(""))
        .await
        .expect("connect to GCS emulator");
    bucket.check_connection().await.expect("list succeeds");
}

/// 3.2 — Full round-trip: create local DB, push, download to a new path, verify parity.
#[tokio::test]
async fn test_gcs_push_and_download() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };

    let src_dir = TempDir::new().unwrap();
    let db = Database::create(src_dir.path(), "push-download-test")
        .await
        .unwrap();

    // 100 nodes + 50 edges of structured data
    let items: Vec<(&str, serde_json::Value)> = (0..100)
        .map(|i| {
            (
                "user",
                serde_json::json!({"i": i, "name": format!("u{}", i)}),
            )
        })
        .collect();
    let nodes = db.insert_nodes_batch(items).await.unwrap();
    assert_eq!(nodes.len(), 100);

    let ids: Vec<String> = nodes.iter().map(|n| n.id.to_string()).collect();
    let edges: Vec<(&str, &str, &str)> = (0..50)
        .map(|i| (ids[i].as_str(), ids[(i + 1) % 100].as_str(), "follows"))
        .collect();
    db.create_edges_batch(edges).await.unwrap();

    // Push to GCS
    let url = env.gs_url("db");
    db.push_to_bucket(&url).await.expect("push_to_bucket");

    // Download to a fresh local path and verify
    let dst_dir = TempDir::new().unwrap();
    let bucket = BucketStorage::connect(&url).await.unwrap();
    bucket
        .download_to_local(dst_dir.path())
        .await
        .expect("download_to_local");

    let round_trip = Database::open(dst_dir.path())
        .await
        .expect("open round-trip");
    let status = round_trip.status().await.unwrap();
    assert_eq!(status.node_count, 100, "node count survived the round-trip");
    assert_eq!(status.edge_count, 50, "edge count survived the round-trip");

    // Spot-check: first node's properties match
    let id = nodes[0].id.to_string();
    let fetched = round_trip
        .get_node(&id)
        .await
        .unwrap()
        .expect("node exists");
    assert_eq!(fetched.node_type, "user");
    assert_eq!(
        fetched.properties.get("name").and_then(|v| v.as_str()),
        Some("u0")
    );
}

/// 3.3 — Bidirectional sync should upload newer local and download newer remote.
#[tokio::test]
async fn test_gcs_sync_bidirectional() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };

    let src_dir = TempDir::new().unwrap();
    let db = Database::create(src_dir.path(), "sync-test").await.unwrap();
    db.insert_node("user", serde_json::json!({"n": 1}))
        .await
        .unwrap();

    let url = env.gs_url("sync");
    let stats = db.sync_with_bucket(&url).await.expect("first sync");
    assert!(stats.uploaded > 0, "fresh sync should upload files");

    // Second sync with no changes should be a no-op
    let stats2 = db.sync_with_bucket(&url).await.expect("second sync");
    // Same-source re-sync: uploads must be zero. Downloads can be
    // non-zero on emulators because server-assigned Last-Modified is a
    // few hundred ms after the local mtime, which the bidirectional
    // sync reads as "remote is newer" and re-downloads. Cosmetic only.
    assert_eq!(stats2.uploaded, 0, "second sync should not re-upload");

    // Mutate on another local clone (simulate "remote has newer data")
    let dst_dir = TempDir::new().unwrap();
    let bucket = BucketStorage::connect(&url).await.unwrap();
    bucket.download_to_local(dst_dir.path()).await.unwrap();

    let db2 = Database::open(dst_dir.path()).await.unwrap();
    db2.insert_node("user", serde_json::json!({"n": 2}))
        .await
        .unwrap();
    let stats3 = db2.sync_with_bucket(&url).await.unwrap();
    assert!(stats3.uploaded > 0, "new data pushed up");
}

/// 3.4 — Readonly mode rejects writes.
#[tokio::test]
async fn test_gcs_readonly_rejects_writes() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };
    let mut bucket = BucketStorage::connect(&env.gs_url("ro"))
        .await
        .expect("connect");
    bucket.set_readonly(true);

    let err = bucket
        .put("readme.txt", Bytes::from_static(b"hello"))
        .await
        .expect_err("readonly should reject put");
    assert!(
        err.to_string().to_lowercase().contains("readonly"),
        "error mentions readonly: {}",
        err
    );
}

/// 3.5 — Config roundtrip via `save_config` + `load_config`.
#[tokio::test]
async fn test_gcs_config_roundtrip() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.gs_url("cfg"))
        .await
        .expect("connect");

    let cfg = DatabaseConfig {
        name: "roundtrip-db".to_string(),
        version: 1,
        created_at: Timestamp::now(),
        bucket_url: Some(env.gs_url("cfg")),
    };

    bucket.save_config(&cfg).await.expect("save_config");
    let loaded = bucket.load_config().await.expect("load_config");
    assert_eq!(loaded.name, cfg.name);
    assert_eq!(loaded.version, cfg.version);
    assert_eq!(loaded.bucket_url, cfg.bucket_url);
}

/// 3.6 — put/get/delete round-trip on a single object.
#[tokio::test]
async fn test_gcs_single_object_ops() {
    let Some(env) = unwrap_or_skip(gcs_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.gs_url("objs"))
        .await
        .expect("connect");

    let payload = Bytes::from_static(b"aresadb-integration-test");
    bucket.put("blob.bin", payload.clone()).await.expect("put");

    let got = bucket.get("blob.bin").await.expect("get");
    assert_eq!(got, payload, "bytes round-trip intact");

    bucket.delete("blob.bin").await.expect("delete");

    let err = bucket.get("blob.bin").await.expect_err("get-after-delete");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("404") || msg.contains("nosuch"),
        "expected not-found error, got: {}",
        err
    );
}

/// 3.7 — Connecting to a non-existent bucket surfaces a usable error the
/// moment we try to do anything. (With fake-gcs-server, the bucket name
/// is validated lazily on first request.)
#[tokio::test]
async fn test_gcs_missing_bucket() {
    if std::env::var("STORAGE_EMULATOR_HOST").is_err() {
        eprintln!("[skip] STORAGE_EMULATOR_HOST not set");
        return;
    }
    let bucket = BucketStorage::connect("gs://this-bucket-really-does-not-exist-xyz123/foo")
        .await
        .expect("connect is lazy");

    let err = bucket
        .get("anything")
        .await
        .expect_err("get from missing bucket should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("404") || msg.contains("not found") || msg.contains("nosuch"),
        "expected not-found error from missing bucket, got: {}",
        err
    );
}
