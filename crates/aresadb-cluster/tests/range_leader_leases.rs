//! End-to-end read-path test for Phase 2c-5.
//!
//! What this covers that no other cluster test does:
//!
//! * `Read(LINEARIZABLE)` against the leader returns the committed
//!   value with a positive `read_log_index`.
//! * `Read(LINEARIZABLE)` against a follower is rejected with
//!   `FAILED_PRECONDITION` and surfaces the current leader id via
//!   the `x-aresa-leader-id` gRPC metadata header.
//! * `Read(STALE)` on a follower eventually returns the value once
//!   the write has been applied there.
//! * After a leader failover, `Read(LINEARIZABLE)` against the new
//!   leader returns the value again — no stale routing, no stale
//!   reads.
//!
//! The harness mirrors `leader_failover.rs`: three redb-backed
//! nodes, real tonic transport, full openraft membership change.
//! We deliberately spend the setup cost so the test exercises the
//! same code paths the Docker Compose target does.

use std::net::SocketAddr;
use std::time::Duration;

use aresadb_cluster::admin::pb;
use aresadb_cluster::{ClusterAdminClient, ClusterNode, NodeConfig, DEFAULT_RANGE_ID};
use aresadb_core::WriteBatch;
use aresadb_raft::{NodeId, SerializableWriteBatch};
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
        .with_cluster_name("aresadb-range-leader-leases")
}

async fn admin_client(addr: &str) -> ClusterAdminClient<Channel> {
    for _ in 0..40 {
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

async fn current_leader(client: &mut ClusterAdminClient<Channel>) -> Option<NodeId> {
    let resp = client.status(pb::StatusRequest {}).await.ok()?.into_inner();
    let value: serde_json::Value = serde_json::from_slice(&resp.json).ok()?;
    value["current_leader"].as_u64()
}

/// Poll every endpoint until one reports a committed leader
/// that is not in `exclude`. Matches `leader_failover.rs`.
async fn wait_for_new_leader(endpoints: &[&str], exclude: &[NodeId]) -> NodeId {
    for _ in 0..120 {
        for ep in endpoints {
            let Ok(endpoint) = Endpoint::from_shared((*ep).to_string())
                .map(|e| e.connect_timeout(Duration::from_millis(250)))
            else {
                continue;
            };
            if let Ok(chan) = endpoint.connect().await {
                let mut c = ClusterAdminClient::new(chan);
                if let Some(id) = current_leader(&mut c).await {
                    if !exclude.contains(&id) {
                        return id;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "no new leader (excluding {:?}) emerged across {:?} within timeout",
        exclude, endpoints
    );
}

async fn write_kv(client: &mut ClusterAdminClient<Channel>, key: &str, value: &str) {
    let mut batch = WriteBatch::new();
    batch.put(key.to_string(), value.to_string());
    let serialisable: SerializableWriteBatch = batch.into();
    let bytes = bincode::serialize(&serialisable).unwrap();
    client
        .write(pb::WriteRequest {
            batch: bytes,
            range_id: 0,
        })
        .await
        .unwrap_or_else(|e| panic!("write {key}={value} failed: {e}"));
}

async fn linearizable_read(
    client: &mut ClusterAdminClient<Channel>,
    key: &str,
) -> Result<pb::ReadResponse, tonic::Status> {
    client
        .read(pb::ReadRequest {
            key: key.as_bytes().to_vec(),
            range_id: DEFAULT_RANGE_ID,
            consistency: pb::ReadConsistency::Linearizable as i32,
        })
        .await
        .map(|r| r.into_inner())
}

async fn stale_read(
    client: &mut ClusterAdminClient<Channel>,
    key: &str,
) -> Result<pb::ReadResponse, tonic::Status> {
    client
        .read(pb::ReadRequest {
            key: key.as_bytes().to_vec(),
            range_id: DEFAULT_RANGE_ID,
            consistency: pb::ReadConsistency::Stale as i32,
        })
        .await
        .map(|r| r.into_inner())
}

/// Poll a follower until its stale read sees the expected value —
/// replication is asynchronous so there is always a small apply
/// window after a successful write.
async fn wait_for_stale(client: &mut ClusterAdminClient<Channel>, key: &str, expected: &str) {
    for _ in 0..80 {
        if let Ok(resp) = stale_read(client, key).await {
            if resp.found && resp.value == expected.as_bytes() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("follower never applied key {key} = {expected}");
}

/// Boot three redb-backed nodes and promote them to a 3-voter
/// cluster. Returns (nodes, configs, bootstrap client) with
/// node[0] already the leader of the default range.
async fn bootstrap_three_node_cluster(
    dir: &TempDir,
) -> (
    [ClusterNode; 3],
    [NodeConfig; 3],
    ClusterAdminClient<Channel>,
) {
    let a1 = pick_addr().await;
    let a2 = pick_addr().await;
    let a3 = pick_addr().await;

    let cfg1 = make_config(1, a1, dir);
    let cfg2 = make_config(2, a2, dir);
    let cfg3 = make_config(3, a3, dir);

    let node1 = ClusterNode::bootstrap_single(cfg1.clone()).await.unwrap();
    let node2 = ClusterNode::start(cfg2.clone()).await.unwrap();
    let node3 = ClusterNode::start(cfg3.clone()).await.unwrap();

    let leader_addr = cfg1.effective_advertise_addr();
    let mut ctrl = admin_client(&leader_addr).await;

    ctrl.add_learner(pb::AddLearnerRequest {
        node: Some(pb::NodeDescriptor {
            node_id: 2,
            rpc_addr: cfg2.effective_advertise_addr(),
        }),
        blocking: true,
    })
    .await
    .unwrap();
    ctrl.add_learner(pb::AddLearnerRequest {
        node: Some(pb::NodeDescriptor {
            node_id: 3,
            rpc_addr: cfg3.effective_advertise_addr(),
        }),
        blocking: true,
    })
    .await
    .unwrap();
    ctrl.change_membership(pb::ChangeMembershipRequest {
        voter_ids: vec![1, 2, 3],
        retain_learners: false,
    })
    .await
    .unwrap();

    ([node1, node2, node3], [cfg1, cfg2, cfg3], ctrl)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_on_leader_returns_value() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let ([node1, node2, node3], [cfg1, _cfg2, _cfg3], mut leader) =
        bootstrap_three_node_cluster(&dir).await;

    write_kv(&mut leader, "point-key", "point-value").await;

    let resp = linearizable_read(&mut leader, "point-key")
        .await
        .expect("linearizable read on leader must succeed");
    assert!(resp.found);
    assert_eq!(resp.value, b"point-value");
    assert_eq!(resp.range_id, DEFAULT_RANGE_ID);
    assert!(
        resp.read_log_index > 0,
        "linearizable read must report a positive applied index, got {}",
        resp.read_log_index
    );

    // A missing key still succeeds — the linearizability guard is
    // orthogonal to key presence.
    let missing = linearizable_read(&mut leader, "never-written")
        .await
        .expect("linearizable read for absent key succeeds");
    assert!(!missing.found);
    assert!(missing.value.is_empty());

    let _ = cfg1; // keep the binding alive so the TempDir survives
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_on_follower_returns_not_leader_with_leader_hint() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let ([node1, node2, node3], [_cfg1, cfg2, _cfg3], mut leader) =
        bootstrap_three_node_cluster(&dir).await;

    write_kv(&mut leader, "guarded", "guarded-value").await;

    // Hit node 2 (a follower) with a linearizable read — it must
    // bounce the call back with a leader hint.
    let mut follower = admin_client(&cfg2.effective_advertise_addr()).await;
    let status = linearizable_read(&mut follower, "guarded")
        .await
        .expect_err("linearizable read on follower must fail");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "not-leader must map to FAILED_PRECONDITION, got {:?}",
        status.code()
    );

    // Leader hint is attached as a metadata header so the CLI /
    // SDK can re-route without regex-parsing the status message.
    let hint = status
        .metadata()
        .get("x-aresa-leader-id")
        .expect("follower must attach leader hint")
        .to_str()
        .unwrap();
    let hinted: NodeId = hint
        .parse()
        .expect("leader-id metadata must parse as a u64");
    assert_eq!(
        hinted, 1,
        "follower must name the bootstrap leader (1), got {hinted}"
    );

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_read_on_follower_eventually_reflects_write() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let ([node1, node2, node3], [_cfg1, cfg2, cfg3], mut leader) =
        bootstrap_three_node_cluster(&dir).await;

    write_kv(&mut leader, "bounded", "bounded-value").await;

    // Both followers must catch up — no leader guard on stale reads.
    let mut follower2 = admin_client(&cfg2.effective_advertise_addr()).await;
    let mut follower3 = admin_client(&cfg3.effective_advertise_addr()).await;
    wait_for_stale(&mut follower2, "bounded", "bounded-value").await;
    wait_for_stale(&mut follower3, "bounded", "bounded-value").await;

    // Absent keys come back as not-found, not as an error.
    let missing = stale_read(&mut follower3, "never-written").await.unwrap();
    assert!(!missing.found);

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_follows_leader_after_failover() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();
    let ([node1, node2, node3], [_cfg1, cfg2, cfg3], mut leader) =
        bootstrap_three_node_cluster(&dir).await;

    write_kv(&mut leader, "pre-failover", "old-leader").await;

    // Kill the old leader; election runs on nodes 2 and 3.
    node1.shutdown().await.unwrap();

    let survivors = [
        cfg2.effective_advertise_addr(),
        cfg3.effective_advertise_addr(),
    ];
    let survivor_refs: Vec<&str> = survivors.iter().map(String::as_str).collect();
    let new_leader = wait_for_new_leader(&survivor_refs, &[1]).await;

    let new_leader_addr = if new_leader == 2 {
        cfg2.effective_advertise_addr()
    } else {
        cfg3.effective_advertise_addr()
    };
    let mut new_client = admin_client(&new_leader_addr).await;

    // Write once more so we can assert the new leader actually
    // serves fresh data, not just replayed log from before the
    // failover.
    write_kv(&mut new_client, "post-failover", "new-leader").await;

    // The linearizable guard on the new leader holds — both old
    // and new values are readable. The old leader is gone, so if
    // the guard were tied to a specific member we'd see
    // `QuorumUnavailable` here instead. We don't assert strict
    // ordering of `read_log_index` across the two reads because
    // no write happens between them; the state machine's applied
    // index is monotonic but may be equal for back-to-back reads.
    let pre = linearizable_read(&mut new_client, "pre-failover")
        .await
        .unwrap();
    assert!(pre.found && pre.value == b"old-leader");
    assert!(
        pre.read_log_index > 0,
        "linearizable read must report a positive applied index"
    );

    let post = linearizable_read(&mut new_client, "post-failover")
        .await
        .unwrap();
    assert!(post.found && post.value == b"new-leader");
    assert!(
        post.read_log_index >= pre.read_log_index,
        "applied index is monotonic across reads: pre={}, post={}",
        pre.read_log_index,
        post.read_log_index
    );

    // A follower under the new leader still refuses linearizable
    // reads and offers a leader hint pointing at the new leader,
    // not the dead one.
    let follower_addr = if new_leader == 2 {
        cfg3.effective_advertise_addr()
    } else {
        cfg2.effective_advertise_addr()
    };
    let mut follower = admin_client(&follower_addr).await;
    let status = linearizable_read(&mut follower, "post-failover")
        .await
        .expect_err("follower under new leader must still refuse linearizable reads");
    assert_eq!(status.code(), Code::FailedPrecondition);
    let hint = status
        .metadata()
        .get("x-aresa-leader-id")
        .expect("follower attaches new leader hint")
        .to_str()
        .unwrap();
    let hinted: NodeId = hint.parse().expect("hint parses as u64");
    assert_eq!(
        hinted, new_leader,
        "leader hint must point at the new leader"
    );

    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}
