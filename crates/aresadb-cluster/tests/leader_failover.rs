//! End-to-end leader-failover test for Phase 1d.
//!
//! Covers the pieces `three_node_durable` doesn't: mid-flight loss of
//! the current leader, re-election on the surviving two voters,
//! successful writes after the failover, and successful catch-up when
//! the old leader rejoins.
//!
//! Every node runs on real redb, so "restart" is a full cold restart
//! from the data directory — the same code path the Docker Compose
//! setup exercises.

use std::net::SocketAddr;
use std::time::Duration;

use aresadb_cluster::admin::pb;
use aresadb_cluster::{ClusterAdminClient, ClusterNode, NodeConfig};
use aresadb_core::WriteBatch;
use aresadb_raft::{NodeId, SerializableWriteBatch};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint};

async fn pick_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn make_config(node_id: NodeId, listen: SocketAddr, dir: &TempDir) -> NodeConfig {
    NodeConfig::new(node_id, listen, dir.path().join(format!("node-{node_id}")))
        .with_cluster_name("aresadb-leader-failover")
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

/// Poll the admin status of each candidate endpoint until one reports
/// a committed leader that is NOT in `exclude`. Followers keep
/// advertising the last known leader until a new one is elected, so
/// simply asking "who's the leader?" right after killing the leader
/// would race.
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

async fn wait_for_key(node: &ClusterNode, key: &[u8], expected: &[u8]) {
    for _ in 0..100 {
        if let Some(v) = node.data().get(key).await.unwrap() {
            if &v[..] == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "node {} never saw expected value for key {:?}",
        node.node_id(),
        std::str::from_utf8(key).unwrap_or("<binary>")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_elects_new_leader_and_catches_up_rejoiner() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();

    // --- step 1: boot 3 nodes, promote to a 3-voter cluster ---------
    let a1 = pick_addr().await;
    let a2 = pick_addr().await;
    let a3 = pick_addr().await;

    let cfg1 = make_config(1, a1, &dir);
    let cfg2 = make_config(2, a2, &dir);
    let cfg3 = make_config(3, a3, &dir);

    let node1 = ClusterNode::bootstrap_single(cfg1.clone()).await.unwrap();
    let node2 = ClusterNode::start(cfg2.clone()).await.unwrap();
    let node3 = ClusterNode::start(cfg3.clone()).await.unwrap();

    let leader_addr_1 = cfg1.effective_advertise_addr();
    let mut ctrl = admin_client(&leader_addr_1).await;

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

    // --- step 2: write a first batch of keys through node 1 ---------
    for i in 0..10u32 {
        write_kv(&mut ctrl, &format!("pre-{i:02}"), &format!("v{i}")).await;
    }
    wait_for_key(&node2, b"pre-09", b"v9").await;
    wait_for_key(&node3, b"pre-09", b"v9").await;

    // --- step 3: drop node 1. Survivors must elect a new leader. ----
    // Graceful shutdown is honest here: killing the tokio runtime
    // mid-flight is what a real SIGTERM looks like.
    node1.shutdown().await.unwrap();

    let survivor_addrs = [
        cfg2.effective_advertise_addr(),
        cfg3.effective_advertise_addr(),
    ];
    let addr_refs: Vec<&str> = survivor_addrs.iter().map(String::as_str).collect();
    let new_leader = wait_for_new_leader(&addr_refs, &[1]).await;
    assert!(
        new_leader == 2 || new_leader == 3,
        "new leader must be one of the survivors, got {new_leader}"
    );

    // --- step 4: writes succeed on the new leader -------------------
    let new_leader_addr = if new_leader == 2 {
        cfg2.effective_advertise_addr()
    } else {
        cfg3.effective_advertise_addr()
    };
    let mut leader_client = admin_client(&new_leader_addr).await;
    for i in 0..10u32 {
        write_kv(
            &mut leader_client,
            &format!("post-{i:02}"),
            &format!("w{i}"),
        )
        .await;
    }

    // Every survivor sees every post-failover write.
    for node in [&node2, &node3] {
        wait_for_key(node, b"post-09", b"w9").await;
        wait_for_key(node, b"pre-00", b"v0").await;
    }

    // --- step 5: bring node 1 back and verify catch-up --------------
    let node1_b = ClusterNode::start(cfg1.clone()).await.unwrap();
    // It should catch every write, including the ones made while it was
    // offline. No manual rejoin — openraft replication does the work.
    wait_for_key(&node1_b, b"pre-00", b"v0").await;
    wait_for_key(&node1_b, b"post-09", b"w9").await;

    // --- step 6: write once more, confirm it lands on every node ----
    let mut latest_client = admin_client(&new_leader_addr).await;
    write_kv(&mut latest_client, "after-rejoin", "ok").await;
    for node in [&node1_b, &node2, &node3] {
        wait_for_key(node, b"after-rejoin", b"ok").await;
    }

    node1_b.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}
