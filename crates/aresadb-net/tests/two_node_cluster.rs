//! End-to-end two-node cluster test.
//!
//! Spins up two `openraft::Raft` instances that talk over real tonic
//! gRPC, bootstraps them as a two-voter cluster, writes through the
//! leader, and verifies the follower applies the same state. This is
//! the narrowest test that actually exercises every layer of Phase
//! 1b in the order the production bootstrap will use them.
//!
//! The test is marked `#[ignore]` by default on CI where binding
//! localhost ports is flaky; run it explicitly with
//! `cargo test -p aresadb-net -- --ignored`.
//!
//! Not using `#[ignore]` after all — the sockets we pick are
//! ephemeral and bound inside the test, so it's deterministic enough
//! to run in the default suite. If it ever flakes, re-enable the
//! ignore and move it to `--ignored`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use aresadb_core::{MemoryBackend, StorageBackend, WriteBatch};
use aresadb_net::{GrpcRaftNetwork, RaftGrpcServer, StaticPeerDirectory};
use aresadb_raft::{AresaCommand, LogStore, NodeId, StateMachineStore, TypeConfig};
use openraft::{BasicNode, Config, Raft};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Build a Raft instance wired to the gRPC network. Mirrors the
/// single-node harness in `aresadb-raft` but leaves bootstrap out so
/// the caller decides who owns the initial membership.
async fn build_node(
    node_id: NodeId,
    directory: Arc<StaticPeerDirectory>,
    log_backend: Arc<dyn StorageBackend>,
    data_backend: Arc<dyn StorageBackend>,
) -> anyhow::Result<(Raft<TypeConfig>, Arc<dyn StorageBackend>)> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 150,
            election_timeout_min: 500,
            election_timeout_max: 1500,
            cluster_name: "aresadb-two-node-test".to_string(),
            ..Default::default()
        }
        .validate()?,
    );

    let log = LogStore::new(log_backend.clone());
    let sm = StateMachineStore::new(data_backend.clone());
    let network = GrpcRaftNetwork::new_singleton(directory);

    let raft = Raft::<TypeConfig>::new(node_id, config, network, log, sm).await?;
    Ok((raft, data_backend))
}

/// Bind a random localhost port and return the listener along with
/// the address we can hand to tonic/Serve. We could ask tonic to bind
/// for us, but handling it manually lets the test hand the same
/// address back to peers without a second round of discovery.
async fn pick_port() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, addr)
}

#[tokio::test]
async fn two_node_cluster_replicates_write_over_grpc() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    // --- step 1: pick two ports ------------------------------------
    let (listener_1, addr_1) = pick_port().await;
    let (listener_2, addr_2) = pick_port().await;

    let mut peers = HashMap::new();
    peers.insert(1u64, format!("http://{}", addr_1));
    peers.insert(2u64, format!("http://{}", addr_2));
    let directory = StaticPeerDirectory::from_map(peers);

    // --- step 2: build the two Raft instances ----------------------
    let data_1: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log_1: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (raft_1, data_1_handle) = build_node(1, directory.clone(), log_1, data_1.clone())
        .await
        .unwrap();

    let data_2: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log_2: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (raft_2, data_2_handle) = build_node(2, directory.clone(), log_2, data_2.clone())
        .await
        .unwrap();

    // --- step 3: start the gRPC servers ----------------------------
    let (shutdown_1_tx, shutdown_1_rx) = oneshot::channel::<()>();
    let (shutdown_2_tx, shutdown_2_rx) = oneshot::channel::<()>();

    let server_1 = RaftGrpcServer::new(raft_1.clone()).into_service();
    let server_2 = RaftGrpcServer::new(raft_2.clone()).into_service();

    let incoming_1 = tokio_stream::wrappers::TcpListenerStream::new(listener_1);
    let incoming_2 = tokio_stream::wrappers::TcpListenerStream::new(listener_2);

    let h1 = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server_1)
            .serve_with_incoming_shutdown(incoming_1, async {
                let _ = shutdown_1_rx.await;
            })
            .await
            .expect("server 1");
    });
    let h2 = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server_2)
            .serve_with_incoming_shutdown(incoming_2, async {
                let _ = shutdown_2_rx.await;
            })
            .await
            .expect("server 2");
    });

    // --- step 4: bootstrap the cluster with both nodes -------------
    // Node 1 proposes the initial membership; openraft then
    // replicates it to node 2 through the network we just wired up.
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::new(format!("http://{}", addr_1)));
    members.insert(2u64, BasicNode::new(format!("http://{}", addr_2)));
    raft_1
        .initialize(members)
        .await
        .expect("initialize cluster");

    // Wait for node 1 to actually become leader (initialize returns
    // once the membership log entry is accepted, but not necessarily
    // once an election has resolved).
    raft_1
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "node 1 becomes leader")
        .await
        .expect("node 1 elected leader");

    // --- step 5: write a batch through the leader ------------------
    let mut batch = WriteBatch::new();
    batch.put("hello", "world").put("lang", "rust");
    let resp = raft_1
        .client_write(AresaCommand::batch(batch))
        .await
        .expect("client write on leader");
    assert_eq!(resp.data.ops_applied, 2);

    // --- step 6: wait for the follower to catch up -----------------
    raft_2
        .wait(Some(Duration::from_secs(5)))
        .applied_index(Some(resp.log_id.index), "node 2 applies same entry")
        .await
        .expect("node 2 catches up");

    // --- step 7: both data backends agree --------------------------
    assert_eq!(
        &data_1_handle.get(b"hello").await.unwrap().unwrap()[..],
        b"world"
    );
    assert_eq!(
        &data_1_handle.get(b"lang").await.unwrap().unwrap()[..],
        b"rust"
    );

    assert_eq!(
        &data_2_handle.get(b"hello").await.unwrap().unwrap()[..],
        b"world"
    );
    assert_eq!(
        &data_2_handle.get(b"lang").await.unwrap().unwrap()[..],
        b"rust"
    );

    // --- step 8: graceful shutdown ---------------------------------
    raft_1.shutdown().await.unwrap();
    raft_2.shutdown().await.unwrap();
    let _ = shutdown_1_tx.send(());
    let _ = shutdown_2_tx.send(());
    let _ = h1.await;
    let _ = h2.await;
}
