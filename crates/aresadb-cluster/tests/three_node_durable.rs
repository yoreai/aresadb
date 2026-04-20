//! End-to-end integration test for Phase 1c.
//!
//! Spins up three real `ClusterNode`s, each with its own redb-backed
//! log + state machine, bootstraps them as a 3-voter cluster, writes
//! data, then restarts all three to verify durability and recovery.
//!
//! This is the narrowest test that exercises every piece of Phase 1c:
//!   * `NodeConfig` + `RedbBackend` (on-disk durability),
//!   * `ClusterNode::start` + `bootstrap_single` + `shutdown`
//!     (lifecycle),
//!   * `ClusterAdmin` gRPC service (add-learner, change-membership,
//!     write, read, status),
//!   * The Raft transport wiring it all together.

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
    // Ask the kernel for a free port, then drop the listener. There's
    // a small window where another test could grab the port before
    // tonic rebinds it, but for a two-digit number of parallel tests
    // it's fine. If this ever flakes we'll move to a named pipe or a
    // per-test-binary port range.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn make_config(node_id: NodeId, listen: SocketAddr, dir: &TempDir) -> NodeConfig {
    NodeConfig::new(node_id, listen, dir.path().join(format!("node-{node_id}")))
        .with_cluster_name("aresadb-three-node-durable")
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

async fn status_voters(client: &mut ClusterAdminClient<Channel>) -> Vec<NodeId> {
    let resp = client
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    let value: serde_json::Value = serde_json::from_slice(&resp.json).unwrap();
    value["membership"]["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect()
}

async fn wait_for_leader(raft: &openraft::Raft<aresadb_raft::TypeConfig>, expected: NodeId) {
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(expected, "expected leader")
        .await
        .expect("leader elected");
}

#[tokio::test]
async fn three_node_cluster_boots_replicates_and_recovers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let dir = TempDir::new().unwrap();

    // --- step 1: pick ports and boot all three nodes ----------------
    let a1 = pick_addr().await;
    let a2 = pick_addr().await;
    let a3 = pick_addr().await;

    let cfg1 = make_config(1, a1, &dir);
    let cfg2 = make_config(2, a2, &dir);
    let cfg3 = make_config(3, a3, &dir);

    // Node 1 bootstraps the cluster as a single voter. Nodes 2 and 3
    // just start and wait to be added. That's the production pattern.
    let node1 = ClusterNode::bootstrap_single(cfg1.clone()).await.unwrap();
    let node2 = ClusterNode::start(cfg2.clone()).await.unwrap();
    let node3 = ClusterNode::start(cfg3.clone()).await.unwrap();

    wait_for_leader(node1.raft(), 1).await;

    // --- step 2: promote nodes 2 and 3 to voters through the admin API
    let mut client_to_leader = admin_client(&cfg1.effective_advertise_addr()).await;

    client_to_leader
        .add_learner(pb::AddLearnerRequest {
            node: Some(pb::NodeDescriptor {
                node_id: 2,
                rpc_addr: cfg2.effective_advertise_addr(),
            }),
            blocking: true,
        })
        .await
        .expect("add node 2 as learner");

    client_to_leader
        .add_learner(pb::AddLearnerRequest {
            node: Some(pb::NodeDescriptor {
                node_id: 3,
                rpc_addr: cfg3.effective_advertise_addr(),
            }),
            blocking: true,
        })
        .await
        .expect("add node 3 as learner");

    client_to_leader
        .change_membership(pb::ChangeMembershipRequest {
            voter_ids: vec![1, 2, 3],
            retain_learners: false,
        })
        .await
        .expect("promote to 3-voter membership");

    let voters = status_voters(&mut client_to_leader).await;
    assert_eq!(voters, vec![1, 2, 3]);

    // --- step 3: push a handful of writes through the admin Write RPC
    for i in 0..15u32 {
        let mut batch = WriteBatch::new();
        batch.put(format!("key-{i:02}"), format!("val-{i:02}"));
        let serialisable: SerializableWriteBatch = batch.into();
        let bytes = bincode::serialize(&serialisable).unwrap();
        client_to_leader
            .write(pb::WriteRequest {
                batch: bytes,
                range_id: 0,
            })
            .await
            .expect("write");
    }

    // --- step 4: each follower catches up; their local data backends
    //             serve the same keys.
    for node in [&node2, &node3] {
        for _ in 0..50 {
            let got = node.data().get(b"key-14").await.unwrap();
            if got.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let v = node
            .data()
            .get(b"key-00")
            .await
            .unwrap()
            .expect("follower should have key-00");
        assert_eq!(&v[..], b"val-00");
    }

    // --- step 5: graceful shutdown of every node
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();

    // --- step 6: restart every node from the same data dirs; they
    //             must recover the prior state without help from the
    //             operator.
    let node1_b = ClusterNode::start(cfg1.clone()).await.unwrap();
    let node2_b = ClusterNode::start(cfg2.clone()).await.unwrap();
    let node3_b = ClusterNode::start(cfg3.clone()).await.unwrap();

    // A new leader needs to be elected. Give it a reasonable window.
    let mut leader_id: Option<NodeId> = None;
    for _ in 0..50 {
        let mut client = admin_client(&cfg1.effective_advertise_addr()).await;
        let resp = client
            .status(pb::StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        let value: serde_json::Value = serde_json::from_slice(&resp.json).unwrap();
        if let Some(id) = value["current_leader"].as_u64() {
            leader_id = Some(id);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        leader_id.is_some(),
        "cluster failed to elect a leader after restart"
    );

    // Every node's data backend still has every key we wrote.
    for (idx, node) in [&node1_b, &node2_b, &node3_b].iter().enumerate() {
        for i in 0..15u32 {
            let key = format!("key-{i:02}");
            let expected = format!("val-{i:02}");
            let got = node
                .data()
                .get(key.as_bytes())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("node {} lost key {} after restart", idx + 1, key));
            assert_eq!(&got[..], expected.as_bytes(), "node {} mismatch", idx + 1);
        }
    }

    // --- step 7: write one more entry *after* the restart; all three
    //             backends must pick it up. This proves the Raft log
    //             recovered consistently on every node, not just that
    //             the old committed data is on disk.
    let leader_addr = match leader_id.unwrap() {
        1 => cfg1.effective_advertise_addr(),
        2 => cfg2.effective_advertise_addr(),
        3 => cfg3.effective_advertise_addr(),
        other => panic!("unexpected leader id {other}"),
    };
    let mut leader_client = admin_client(&leader_addr).await;

    let mut batch = WriteBatch::new();
    batch.put("post-restart", "ok");
    let serialisable: SerializableWriteBatch = batch.into();
    let bytes = bincode::serialize(&serialisable).unwrap();
    leader_client
        .write(pb::WriteRequest {
            batch: bytes,
            range_id: 0,
        })
        .await
        .expect("post-restart write");

    for node in [&node1_b, &node2_b, &node3_b] {
        for _ in 0..50 {
            if node.data().get(b"post-restart").await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            &node.data().get(b"post-restart").await.unwrap().unwrap()[..],
            b"ok",
            "node {} missing post-restart write",
            node.node_id()
        );
    }

    node1_b.shutdown().await.unwrap();
    node2_b.shutdown().await.unwrap();
    node3_b.shutdown().await.unwrap();
}
