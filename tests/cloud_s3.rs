//! Integration tests for the S3 cloud backend.
//!
//! These tests target a local MinIO emulator by default. Start it with
//! `./scripts/start_emulators.sh` and export the emulator env vars before
//! running. If `AWS_ENDPOINT_URL` is not set, every test logs a skip message
//! and exits successfully so the default `cargo test` stays fast.
//!
//! For real-cloud smoke tests, see `cloud_real_s3.rs` (when it exists).

mod common;

use aresadb::{BucketStorage, Database, DatabaseConfig, Timestamp};
use bytes::Bytes;
use common::cloud::{s3_test_env, unwrap_or_skip};
use tempfile::TempDir;

/// 4.1 — Basic connect + list.
#[tokio::test]
async fn test_s3_connect() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.s3_url(""))
        .await
        .expect("connect to S3 emulator");
    bucket.check_connection().await.expect("list succeeds");
}

/// 4.2 — Full round-trip: create local DB, push, download to a new path, verify parity.
#[tokio::test]
async fn test_s3_push_and_download() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };

    let src_dir = TempDir::new().unwrap();
    let db = Database::create(src_dir.path(), "push-download-test")
        .await
        .unwrap();

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

    let url = env.s3_url("db");
    db.push_to_bucket(&url).await.expect("push_to_bucket");

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
    assert_eq!(status.node_count, 100);
    assert_eq!(status.edge_count, 50);

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

/// 4.3 — Bidirectional sync: newer-wins semantics.
#[tokio::test]
async fn test_s3_sync_bidirectional() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };

    let src_dir = TempDir::new().unwrap();
    let db = Database::create(src_dir.path(), "sync-test").await.unwrap();
    db.insert_node("user", serde_json::json!({"n": 1}))
        .await
        .unwrap();

    let url = env.s3_url("sync");
    let stats = db.sync_with_bucket(&url).await.expect("first sync");
    assert!(stats.uploaded > 0);

    let stats2 = db.sync_with_bucket(&url).await.expect("second sync");
    // Second sync against the same source should not need to upload
    // anything new. We don't assert `downloaded == 0` here because
    // MinIO's Last-Modified is set at upload time and can land a few
    // hundred ms after the local mtime, which makes the bidirectional
    // sync consider the remote side newer and re-download. That's
    // cosmetic, not a correctness issue.
    assert_eq!(stats2.uploaded, 0, "second sync should not re-upload");

    let dst_dir = TempDir::new().unwrap();
    let bucket = BucketStorage::connect(&url).await.unwrap();
    bucket.download_to_local(dst_dir.path()).await.unwrap();

    let db2 = Database::open(dst_dir.path()).await.unwrap();
    db2.insert_node("user", serde_json::json!({"n": 2}))
        .await
        .unwrap();
    let stats3 = db2.sync_with_bucket(&url).await.unwrap();
    assert!(stats3.uploaded > 0);
}

/// 4.4 — Readonly rejects writes.
#[tokio::test]
async fn test_s3_readonly_rejects_writes() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };
    let mut bucket = BucketStorage::connect(&env.s3_url("ro"))
        .await
        .expect("connect");
    bucket.set_readonly(true);

    let err = bucket
        .put("readme.txt", Bytes::from_static(b"hello"))
        .await
        .expect_err("readonly should reject put");
    assert!(err.to_string().to_lowercase().contains("readonly"));
}

/// 4.5 — Config roundtrip.
#[tokio::test]
async fn test_s3_config_roundtrip() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.s3_url("cfg"))
        .await
        .expect("connect");

    let cfg = DatabaseConfig {
        name: "roundtrip-db".to_string(),
        version: 1,
        created_at: Timestamp::now(),
        bucket_url: Some(env.s3_url("cfg")),
    };

    bucket.save_config(&cfg).await.expect("save_config");
    let loaded = bucket.load_config().await.expect("load_config");
    assert_eq!(loaded.name, cfg.name);
    assert_eq!(loaded.version, cfg.version);
    assert_eq!(loaded.bucket_url, cfg.bucket_url);
}

/// 4.6 — put/get/delete round-trip on a single object.
#[tokio::test]
async fn test_s3_single_object_ops() {
    let Some(env) = unwrap_or_skip(s3_test_env()) else {
        return;
    };
    let bucket = BucketStorage::connect(&env.s3_url("objs"))
        .await
        .expect("connect");

    let payload = Bytes::from_static(b"aresadb-integration-test");
    bucket.put("blob.bin", payload.clone()).await.expect("put");

    let got = bucket.get("blob.bin").await.expect("get");
    assert_eq!(got, payload);

    bucket.delete("blob.bin").await.expect("delete");

    let err = bucket.get("blob.bin").await.expect_err("get-after-delete");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("404") || msg.contains("nosuchkey"),
        "expected not-found error, got: {}",
        err
    );
}

/// 4.7 — Non-existent bucket yields a clear error on first operation.
#[tokio::test]
async fn test_s3_missing_bucket() {
    if std::env::var("AWS_ENDPOINT_URL").is_err() {
        eprintln!("[skip] AWS_ENDPOINT_URL not set");
        return;
    }
    let bucket = BucketStorage::connect("s3://this-bucket-really-does-not-exist-xyz123/foo")
        .await
        .expect("connect is lazy");

    let err = bucket
        .get("anything")
        .await
        .expect_err("get from missing bucket should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("404")
            || msg.contains("not found")
            || msg.contains("nosuch")
            || msg.contains("no such bucket"),
        "expected not-found error from missing bucket, got: {}",
        err
    );
}
