//! Three-node cluster test.
//!
//! This is the lowest configuration where losing a node and still
//! making progress is actually a coherent thing to test (quorum of
//! 3 is 2, so one node can be absent and writes still commit). A
//! passing run proves that:
//!
//!   * leader election works through real gRPC,
//!   * batched client writes replicate and commit across three
//!     independent state machines,
//!   * the transport does not silently drop any entries between
//!     leader and either follower.
//!
//! It deliberately does NOT test process restart or split-brain —
//! those land in Phase 1d alongside the Docker-compose scenarios.
//!
//! The test keeps one feature at a time honest; if it starts to sprawl,
//! split it into focused cases instead of adding more assertions.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use aresadb_core::{MemoryBackend, StorageBackend, WriteBatch};
use aresadb_net::{GrpcRaftNetwork, RaftGrpcServer, StaticPeerDirectory};
use aresadb_raft::{AresaCommand, LogStore, NodeId, StateMachineStore, TypeConfig};
use openraft::{BasicNode, Config, Raft};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct NodeHarness {
    id: NodeId,
    raft: Raft<TypeConfig>,
    data: Arc<dyn StorageBackend>,
    shutdown: oneshot::Sender<()>,
    server_task: tokio::task::JoinHandle<()>,
}

async fn build_node(
    id: NodeId,
    listener: TcpListener,
    directory: Arc<StaticPeerDirectory>,
) -> anyhow::Result<NodeHarness> {
    let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let config = Arc::new(
        Config {
            heartbeat_interval: 150,
            election_timeout_min: 500,
            election_timeout_max: 1500,
            cluster_name: "aresadb-three-node-test".to_string(),
            ..Default::default()
        }
        .validate()?,
    );

    let log_store = LogStore::new(log.clone());
    let sm = StateMachineStore::new(data.clone());
    let network = GrpcRaftNetwork::new_singleton(directory);
    let raft = Raft::<TypeConfig>::new(id, config, network, log_store, sm).await?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = RaftGrpcServer::new(raft.clone()).into_service();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("raft server");
    });

    Ok(NodeHarness {
        id,
        raft,
        data,
        shutdown: shutdown_tx,
        server_task,
    })
}

async fn pick_port() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

#[tokio::test]
async fn three_node_cluster_commits_across_all_voters() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let (l1, a1) = pick_port().await;
    let (l2, a2) = pick_port().await;
    let (l3, a3) = pick_port().await;

    let mut peers = HashMap::new();
    peers.insert(1u64, format!("http://{}", a1));
    peers.insert(2u64, format!("http://{}", a2));
    peers.insert(3u64, format!("http://{}", a3));
    let dir = StaticPeerDirectory::from_map(peers);

    let n1 = build_node(1, l1, dir.clone()).await.unwrap();
    let n2 = build_node(2, l2, dir.clone()).await.unwrap();
    let n3 = build_node(3, l3, dir.clone()).await.unwrap();

    // Bootstrap via node 1.
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::new(format!("http://{}", a1)));
    members.insert(2u64, BasicNode::new(format!("http://{}", a2)));
    members.insert(3u64, BasicNode::new(format!("http://{}", a3)));
    n1.raft.initialize(members).await.expect("initialize");
    n1.raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "elect n1")
        .await
        .expect("elected");

    // Push a batch of individual writes so we exercise the
    // transport across many append_entries RPCs, not just one.
    const N_WRITES: u32 = 25;
    let mut last_log_index = None;
    for i in 0..N_WRITES {
        let mut batch = WriteBatch::new();
        batch.put(format!("k{i:02}"), format!("v{i:02}"));
        let resp = n1
            .raft
            .client_write(AresaCommand::batch(batch))
            .await
            .expect("client_write");
        assert_eq!(resp.data.ops_applied, 1);
        last_log_index = Some(resp.log_id.index);
    }
    let last_log_index = last_log_index.unwrap();

    // Followers catch up to the final log index.
    for follower in [&n2, &n3] {
        follower
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index(
                Some(last_log_index),
                format!("n{} applies final entry", follower.id).as_str(),
            )
            .await
            .expect("follower apply");
    }

    // Every backend agrees on every key.
    for node in [&n1, &n2, &n3] {
        for i in 0..N_WRITES {
            let k = format!("k{i:02}");
            let v = format!("v{i:02}");
            let got = node.data.get(k.as_bytes()).await.unwrap();
            assert_eq!(
                &got.unwrap()[..],
                v.as_bytes(),
                "mismatch on node {} key {}",
                node.id,
                k
            );
        }
    }

    // Clean shutdown.
    for node in [n1, n2, n3] {
        node.raft.shutdown().await.unwrap();
        let _ = node.shutdown.send(());
        let _ = node.server_task.await;
    }
}
