//! Integration test for Phase 2c-3c: range admin RPCs over tonic.
//!
//! Drives `AddRange`, `ListRanges`, and `RemoveRange` through a real
//! `ClusterAdminClient`, against a single running `ClusterNode`.
//!
//! Confirms:
//!   * the default range is registered on boot (Phase 2c-3b),
//!   * `AddRange` with `bootstrap_as_voter=true` opens a second range
//!     and makes the node its leader,
//!   * `ListRanges` returns both descriptors, sorted by `range_id`,
//!   * `AddRange` on an existing range returns `ALREADY_EXISTS`,
//!   * invalid descriptors are rejected with `INVALID_ARGUMENT`,
//!   * `RemoveRange` tears the runtime down and makes the range
//!     disappear from subsequent `ListRanges`,
//!   * `RemoveRange` on an unknown range returns `NOT_FOUND`.

use std::net::SocketAddr;
use std::time::Duration;

use aresadb_cluster::admin::pb;
use aresadb_cluster::{ClusterAdminClient, ClusterNode, NodeConfig, DEFAULT_RANGE_ID};
use aresadb_raft::NodeId;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

async fn pick_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn make_config(node_id: NodeId, listen: SocketAddr, dir: &TempDir) -> NodeConfig {
    NodeConfig::new(node_id, listen, dir.path().join(format!("node-{node_id}")))
        .with_cluster_name("aresadb-range-admin-rpcs")
}

async fn admin_client(addr: &str) -> ClusterAdminClient<Channel> {
    for _ in 0..20 {
        let endpoint = Endpoint::from_shared(addr.to_string())
            .unwrap()
            .connect_timeout(Duration::from_millis(500));
        if let Ok(channel) = endpoint.connect().await {
            return ClusterAdminClient::new(channel);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("gave up waiting for admin server at {addr}");
}

fn descriptor_for(range_id: u64, start: &[u8], end: &[u8]) -> pb::RangeDescriptor {
    pb::RangeDescriptor {
        range_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        replicas: vec![pb::ReplicaPlacement {
            node_id: 1,
            store_id: 1,
            role: pb::ReplicaRole::Voter as i32,
        }],
        raft_group_id: range_id,
        epoch: 0,
        generation: 0,
        lease: None,
    }
}

#[tokio::test]
async fn range_admin_rpcs_roundtrip_over_tonic() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let listen = pick_addr().await;
    let cfg = make_config(1, listen, &dir);
    let node = ClusterNode::bootstrap_single(cfg.clone())
        .await
        .expect("boot node 1");

    let mut client = admin_client(&cfg.effective_advertise_addr()).await;

    // --- step 1: ListRanges sees the default range only ----------------
    let list = client
        .list_ranges(pb::ListRangesRequest {})
        .await
        .expect("list after boot")
        .into_inner();
    assert_eq!(list.ranges.len(), 1);
    assert_eq!(list.ranges[0].range_id, DEFAULT_RANGE_ID);

    // --- step 2: AddRange with bootstrap_as_voter=true -----------------
    let new_desc = descriptor_for(42, b"x", b"");
    let added = client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(new_desc.clone()),
            bootstrap_as_voter: true,
        })
        .await
        .expect("add range 42")
        .into_inner();
    let added_desc = added.descriptor.expect("descriptor in response");
    assert_eq!(added_desc.range_id, 42);
    assert_eq!(added_desc.raft_group_id, 42);

    // --- step 3: ListRanges now sees both ranges, sorted by id ---------
    let list = client
        .list_ranges(pb::ListRangesRequest {})
        .await
        .expect("list after add")
        .into_inner();
    let ids: Vec<u64> = list.ranges.iter().map(|r| r.range_id).collect();
    assert_eq!(ids, vec![DEFAULT_RANGE_ID, 42]);

    // Range 42 is now leadable on this node — verify it shows up as a
    // live runtime in the underlying directory too.
    let directory_view = node.range_directory().descriptors();
    assert_eq!(directory_view.len(), 2);

    // --- step 4: duplicate AddRange is rejected ------------------------
    let err = client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(new_desc.clone()),
            bootstrap_as_voter: false,
        })
        .await
        .expect_err("duplicate add_range must fail");
    assert_eq!(err.code(), Code::AlreadyExists);

    // --- step 5: invalid descriptor → INVALID_ARGUMENT -----------------
    let zero = descriptor_for(0, b"a", b"b");
    let err = client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(zero),
            bootstrap_as_voter: false,
        })
        .await
        .expect_err("zero range_id must fail");
    assert_eq!(err.code(), Code::InvalidArgument);

    // Empty span (start == end) is rejected.
    let empty = descriptor_for(99, b"a", b"a");
    let err = client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(empty),
            bootstrap_as_voter: false,
        })
        .await
        .expect_err("empty span must fail");
    assert_eq!(err.code(), Code::InvalidArgument);

    // --- step 6: RemoveRange tears the runtime down --------------------
    client
        .remove_range(pb::RemoveRangeRequest {
            range_id: 42,
            force: false,
        })
        .await
        .expect("remove range 42");

    let list = client
        .list_ranges(pb::ListRangesRequest {})
        .await
        .expect("list after remove")
        .into_inner();
    assert_eq!(list.ranges.len(), 1);
    assert_eq!(list.ranges[0].range_id, DEFAULT_RANGE_ID);

    // --- step 7: RemoveRange on unknown id → NOT_FOUND -----------------
    let err = client
        .remove_range(pb::RemoveRangeRequest {
            range_id: 99_999,
            force: false,
        })
        .await
        .expect_err("unknown remove must fail");
    assert_eq!(err.code(), Code::NotFound);

    // --- step 8: zero range_id on remove → INVALID_ARGUMENT ------------
    let err = client
        .remove_range(pb::RemoveRangeRequest {
            range_id: 0,
            force: false,
        })
        .await
        .expect_err("remove with range_id=0 must fail");
    assert_eq!(err.code(), Code::InvalidArgument);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_ranges_on_one_node_are_storage_isolated() {
    // Multi-range isolation: the default range's data backend and a
    // freshly-added range's data backend are physically independent.
    // Writes to one don't show up in the other, their on-disk
    // directories are disjoint, and removing the new range doesn't
    // disturb the default range.

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let listen = pick_addr().await;
    let cfg = make_config(1, listen, &dir);
    let node = ClusterNode::bootstrap_single(cfg.clone()).await.unwrap();

    let mut client = admin_client(&cfg.effective_advertise_addr()).await;

    // Add a second range for the upper half of the keyspace.
    client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(descriptor_for(42, b"m", b"")),
            bootstrap_as_voter: true,
        })
        .await
        .expect("add range 42");

    // Write to the default range via the admin Write RPC (range 1 is
    // the admin-bound default range, so `Write` targets its Raft).
    let mut batch = aresadb_core::WriteBatch::new();
    batch.put(b"default-key".to_vec(), b"default-value".to_vec());
    let serialisable: aresadb_raft::SerializableWriteBatch = batch.into();
    let bytes = bincode::serialize(&serialisable).unwrap();
    client
        .write(pb::WriteRequest {
            batch: bytes,
            range_id: 0,
        })
        .await
        .expect("default range write");

    // Write directly into range 42's Raft, through the `RangeRuntime`
    // handle the directory exposes. Range 42 has its own Raft group
    // and its own backend — the admin `Write` RPC would target the
    // default range, so we bypass it here to prove isolation at the
    // range level.
    let range42 = node.range_directory().get_range(42).unwrap();
    let mut batch42 = aresadb_core::WriteBatch::new();
    batch42.put(b"range42-key".to_vec(), b"range42-value".to_vec());
    range42
        .raft()
        .client_write(aresadb_raft::AresaCommand::batch(batch42))
        .await
        .expect("range 42 replicated write");

    // Each range's backend contains only its own data.
    let default_range = node.range_directory().get_range(DEFAULT_RANGE_ID).unwrap();
    assert_eq!(
        &default_range
            .data_backend()
            .get(b"default-key")
            .await
            .unwrap()
            .unwrap()[..],
        b"default-value"
    );
    assert!(
        default_range
            .data_backend()
            .get(b"range42-key")
            .await
            .unwrap()
            .is_none(),
        "default range's backend must not see range 42's writes"
    );
    assert_eq!(
        &range42
            .data_backend()
            .get(b"range42-key")
            .await
            .unwrap()
            .unwrap()[..],
        b"range42-value"
    );
    assert!(
        range42
            .data_backend()
            .get(b"default-key")
            .await
            .unwrap()
            .is_none(),
        "range 42's backend must not see default range's writes"
    );

    // On-disk layout confirms physical separation.
    assert_ne!(
        cfg.range_data_path(DEFAULT_RANGE_ID),
        cfg.range_data_path(42)
    );
    assert_ne!(cfg.range_log_path(DEFAULT_RANGE_ID), cfg.range_log_path(42));
    assert!(cfg.range_data_path(DEFAULT_RANGE_ID).exists());
    assert!(cfg.range_data_path(42).exists());

    // Release our own reference to range 42 so `RemoveRange` can
    // consume the last `Arc<RangeRuntime>` cleanly.
    drop(range42);

    client
        .remove_range(pb::RemoveRangeRequest {
            range_id: 42,
            force: false,
        })
        .await
        .expect("remove range 42");

    // Default range survives untouched; range 42 is gone from the
    // directory.
    assert!(node.range_directory().get_range(42).is_none());
    assert!(node.range_directory().get_range(DEFAULT_RANGE_ID).is_some());

    // Default range's backend still serves its key after range 42's
    // removal.
    assert_eq!(
        &default_range
            .data_backend()
            .get(b"default-key")
            .await
            .unwrap()
            .unwrap()[..],
        b"default-value"
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn add_range_without_bootstrap_registers_but_does_not_initialize() {
    // Same flow but the second range is opened uninitialised —
    // simulating a follower/learner node that expects a leader
    // elsewhere to add it as a voter later.

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let listen = pick_addr().await;
    let cfg = make_config(1, listen, &dir);
    let node = ClusterNode::bootstrap_single(cfg.clone()).await.unwrap();

    let mut client = admin_client(&cfg.effective_advertise_addr()).await;

    client
        .add_range(pb::AddRangeRequest {
            descriptor: Some(descriptor_for(7, b"a", b"z")),
            bootstrap_as_voter: false,
        })
        .await
        .expect("add range 7 uninitialised");

    // Runtime is present in the directory…
    let list = client
        .list_ranges(pb::ListRangesRequest {})
        .await
        .unwrap()
        .into_inner();
    let range_ids: Vec<u64> = list.ranges.iter().map(|r| r.range_id).collect();
    assert!(range_ids.contains(&7));

    // …but the node sees no leader on it yet (uninitialised Raft
    // group, waiting to be added as a voter by someone else).
    let runtime = node.range_directory().get_range(7).unwrap();
    let metrics = runtime.raft().metrics().borrow().clone();
    assert!(
        metrics.current_leader.is_none(),
        "uninitialised range must not have a leader"
    );

    node.shutdown().await.unwrap();
}
