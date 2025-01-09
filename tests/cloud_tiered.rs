//! Integration tests for tiered storage cloud paths: evict, promote,
//! transparent cloud reads, and cache behavior.
//!
//! Each test runs twice — once against fake-gcs-server (GCS) and once
//! against MinIO (S3) — so both backends are exercised through the same
//! code paths. Tests skip cleanly when the matching emulator isn't running.

mod common;

use aresadb::{
    BucketStorage, LocalStorage, Node, PayloadLocation, TieredConfig, TieredStorage, Value,
};
use common::cloud::{gcs_test_env, s3_test_env, unwrap_or_skip, EmulatorEnv};
use tempfile::TempDir;

/// Backend under test in a parameterized run.
#[derive(Clone, Copy)]
enum Backend {
    Gcs,
    S3,
}

impl Backend {
    fn url(self, env: &EmulatorEnv, subpath: &str) -> String {
        match self {
            Backend::Gcs => env.gs_url(subpath),
            Backend::S3 => env.s3_url(subpath),
        }
    }
}

/// Build a `TieredStorage` wired to the given cloud emulator plus a fresh
/// local store. Small payload threshold so we actually exercise eviction
/// with small test values.
async fn tiered_with_cloud(
    backend: Backend,
    env: &EmulatorEnv,
    subpath: &str,
) -> (TempDir, TieredStorage) {
    let temp = TempDir::new().unwrap();
    let local = LocalStorage::create(temp.path()).await.unwrap();
    let bucket = BucketStorage::connect(&backend.url(env, subpath))
        .await
        .expect("connect cloud emulator");
    let config = TieredConfig {
        min_cloud_payload_bytes: 0,
        ..TieredConfig::default()
    };
    let tiered = TieredStorage::with_bucket(local, bucket, config);
    (temp, tiered)
}

fn make_node(i: usize) -> Node {
    let props = Value::from_json(serde_json::json!({
        "i": i,
        "name": format!("node-{}", i),
        "payload": "x".repeat(512),  // 512B payload ensures we're above min_cloud_payload_bytes
    }))
    .unwrap();
    Node::new("doc", props)
}

/// 5.1 — Evicting a node moves its payload from local to cloud while
/// leaving the index record local.
async fn run_evict_to_cloud(backend: Backend) {
    let probe = match backend {
        Backend::Gcs => gcs_test_env(),
        Backend::S3 => s3_test_env(),
    };
    let Some(env) = unwrap_or_skip(probe) else {
        return;
    };
    let (_temp, tiered) = tiered_with_cloud(backend, &env, "evict").await;

    let node = make_node(1);
    let id = node.id.clone();
    tiered.insert_node(&node).await.unwrap();

    // Before eviction: payload is local
    let index = tiered.get_node_index(&id).await.unwrap().unwrap();
    assert_eq!(index.payload_location, PayloadLocation::Local);

    tiered.evict_to_cloud(&id).await.expect("evict_to_cloud");

    // After eviction: index location is Cloud, node is still readable
    let index = tiered.get_node_index(&id).await.unwrap().unwrap();
    assert_eq!(index.payload_location, PayloadLocation::Cloud);

    let stats = tiered.stats();
    assert!(stats.cloud_pushes >= 1, "at least one cloud push recorded");
}

/// 5.2 — After eviction, reading the node transparently fetches from cloud.
async fn run_read_from_cloud(backend: Backend) {
    let probe = match backend {
        Backend::Gcs => gcs_test_env(),
        Backend::S3 => s3_test_env(),
    };
    let Some(env) = unwrap_or_skip(probe) else {
        return;
    };
    let (_temp, tiered) = tiered_with_cloud(backend, &env, "read").await;

    let node = make_node(42);
    let id = node.id.clone();
    tiered.insert_node(&node).await.unwrap();
    tiered.evict_to_cloud(&id).await.unwrap();

    // Drop cache so the read definitely hits the cloud
    tiered.cache().clear();

    let stats_before = tiered.stats();
    let fetched = tiered
        .get_node(&id)
        .await
        .unwrap()
        .expect("node still readable after eviction");
    let stats_after = tiered.stats();

    assert_eq!(fetched.id, id);
    assert_eq!(fetched.get("i").and_then(|v| v.as_int()), Some(42));
    assert!(
        stats_after.cloud_fetches > stats_before.cloud_fetches,
        "read-after-evict should fetch from cloud"
    );
}

/// 5.3 — `promote_to_local` pulls a cloud payload back locally.
async fn run_promote_to_local(backend: Backend) {
    let probe = match backend {
        Backend::Gcs => gcs_test_env(),
        Backend::S3 => s3_test_env(),
    };
    let Some(env) = unwrap_or_skip(probe) else {
        return;
    };
    let (_temp, tiered) = tiered_with_cloud(backend, &env, "promote").await;

    let node = make_node(7);
    let id = node.id.clone();
    tiered.insert_node(&node).await.unwrap();
    tiered.evict_to_cloud(&id).await.unwrap();
    assert_eq!(
        tiered
            .get_node_index(&id)
            .await
            .unwrap()
            .unwrap()
            .payload_location,
        PayloadLocation::Cloud
    );

    tiered
        .promote_to_local(&id)
        .await
        .expect("promote_to_local");
    assert_eq!(
        tiered
            .get_node_index(&id)
            .await
            .unwrap()
            .unwrap()
            .payload_location,
        PayloadLocation::Local,
        "promote flips payload_location back to Local"
    );

    // After promotion, reading should no longer increment cloud_fetches
    tiered.cache().clear();
    let stats_before = tiered.stats();
    let _ = tiered.get_node(&id).await.unwrap().unwrap();
    let stats_after = tiered.stats();
    assert_eq!(
        stats_after.cloud_fetches, stats_before.cloud_fetches,
        "read after promote must not hit cloud"
    );
}

/// 5.4 — A second read of an evicted node hits the warm cache instead of
/// re-fetching from the cloud.
async fn run_cache_behavior(backend: Backend) {
    let probe = match backend {
        Backend::Gcs => gcs_test_env(),
        Backend::S3 => s3_test_env(),
    };
    let Some(env) = unwrap_or_skip(probe) else {
        return;
    };
    let (_temp, tiered) = tiered_with_cloud(backend, &env, "cache").await;

    let node = make_node(99);
    let id = node.id.clone();
    tiered.insert_node(&node).await.unwrap();
    tiered.evict_to_cloud(&id).await.unwrap();
    tiered.cache().clear();

    // First read — cloud miss, populates cache
    let _ = tiered.get_node(&id).await.unwrap();
    let stats_after_first = tiered.stats();

    // Second read — should be a cache hit, no new cloud fetch
    let _ = tiered.get_node(&id).await.unwrap();
    let stats_after_second = tiered.stats();

    assert_eq!(
        stats_after_second.cloud_fetches, stats_after_first.cloud_fetches,
        "second read must not hit cloud"
    );
    assert!(
        stats_after_second.cache_hits > stats_after_first.cache_hits,
        "second read should register a cache hit"
    );
}

// ========== GCS parameterization ==========

#[tokio::test]
async fn test_tiered_evict_to_cloud_gcs() {
    run_evict_to_cloud(Backend::Gcs).await;
}

#[tokio::test]
async fn test_tiered_read_from_cloud_gcs() {
    run_read_from_cloud(Backend::Gcs).await;
}

#[tokio::test]
async fn test_tiered_promote_to_local_gcs() {
    run_promote_to_local(Backend::Gcs).await;
}

#[tokio::test]
async fn test_tiered_cache_behavior_gcs() {
    run_cache_behavior(Backend::Gcs).await;
}

// ========== S3 parameterization ==========

#[tokio::test]
async fn test_tiered_evict_to_cloud_s3() {
    run_evict_to_cloud(Backend::S3).await;
}

#[tokio::test]
async fn test_tiered_read_from_cloud_s3() {
    run_read_from_cloud(Backend::S3).await;
}

#[tokio::test]
async fn test_tiered_promote_to_local_s3() {
    run_promote_to_local(Backend::S3).await;
}

#[tokio::test]
async fn test_tiered_cache_behavior_s3() {
    run_cache_behavior(Backend::S3).await;
}
