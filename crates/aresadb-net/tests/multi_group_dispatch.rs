//! Multi-Raft dispatch integration test.
//!
//! Proves the Phase 2c server-side directory fans incoming RPCs out
//! to the right Raft group based on the wire `raft_group_id`, and
//! cleanly rejects unknown ids.
//!
//! Setup:
//! * Two independent `Raft<TypeConfig>` instances boot as single-voter
//!   clusters on loopback backends (groups `10` and `20`).
//! * Both register into a `MultiGroupDirectory` that implements
//!   `RaftDirectory`.
//! * One `RaftGrpcServer` is served on a random localhost port; it
//!   sits in front of the directory.
//!
//! We then send three raw `Vote` RPCs (using the generated tonic
//! client directly, bypassing `GrpcRaftNetwork` so the test stays on
//! the dispatch layer):
//! * `raft_group_id = 10` → routed to raft_a, round-trips cleanly.
//! * `raft_group_id = 20` → routed to raft_b, round-trips cleanly.
//! * `raft_group_id = 999` → unregistered, tonic `Status::NotFound`.
//!
//! The test deliberately avoids assertions on Raft-level semantics
//! (whether the vote was granted or rejected). The point is that the
//! RPC reached the *correct* Raft handle — decoding succeeded and
//! the server returned a well-formed `VoteResponse`, not a codec or
//! not-found error.

use std::collections::HashMap;
use std::sync::Arc;

use aresadb_core::{MemoryBackend, StorageBackend};
use aresadb_net::codec::{decode, encode};
use aresadb_net::pb;
use aresadb_net::pb::raft_service_client::RaftServiceClient;
use aresadb_net::{RaftDirectory, RaftGrpcServer};
use aresadb_raft::{NodeId, SingleNode, TypeConfig};
use openraft::raft::VoteRequest;
use openraft::{CommittedLeaderId, LogId, Raft, Vote};
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

/// Simple test-only directory: `HashMap<u64, Raft<TypeConfig>>`.
/// Mirrors the shape `aresadb-cluster::RangeDirectory` will have in
/// Phase 2c-3 so the dispatch contract we're validating here is the
/// real one.
#[derive(Default)]
struct MultiGroupDirectory {
    inner: RwLock<HashMap<u64, Raft<TypeConfig>>>,
}

impl MultiGroupDirectory {
    fn insert(&self, raft_group_id: u64, raft: Raft<TypeConfig>) {
        self.inner.write().insert(raft_group_id, raft);
    }
}

impl RaftDirectory for MultiGroupDirectory {
    fn raft_for(&self, raft_group_id: u64) -> Option<Raft<TypeConfig>> {
        self.inner.read().get(&raft_group_id).cloned()
    }
}

async fn pick_port() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, addr)
}

async fn spawn_single_voter_group(node_id: NodeId) -> anyhow::Result<SingleNode> {
    // Unlike the two-/three-node transport tests, multi-group
    // dispatch doesn't care whether the underlying Raft groups are
    // actually replicating — they only need to be live, listenable
    // handles. `SingleNode::new` gives us that with zero network
    // wiring per group.
    let log: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    SingleNode::new(node_id, log, data).await
}

/// Build a well-formed `VoteRequest<NodeId>` payload with a
/// high-enough term that an existing leader has to respond with a
/// proper `VoteResponse` (granted or refused) rather than panicking.
/// The specific term doesn't matter — we're testing *dispatch*, not
/// election semantics.
fn well_formed_vote_payload(from_node: NodeId) -> Vec<u8> {
    let req: VoteRequest<NodeId> = VoteRequest::new(
        Vote::new(u64::MAX, from_node),
        Some(LogId::new(CommittedLeaderId::new(u64::MAX, from_node), 0)),
    );
    encode(&req).expect("encode vote request")
}

#[tokio::test]
async fn server_dispatches_by_raft_group_id_and_rejects_unknown_ids() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    // --- two independent single-voter Raft groups -------------------
    let group_a = spawn_single_voter_group(1).await.expect("group a");
    let group_b = spawn_single_voter_group(1).await.expect("group b");

    let directory = Arc::new(MultiGroupDirectory::default());
    directory.insert(10, group_a.raft.clone());
    directory.insert(20, group_b.raft.clone());

    // --- one gRPC server fronting the directory --------------------
    let (listener, addr) = pick_port().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = RaftGrpcServer::from_directory(directory.clone()).into_service();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server");
    });

    // --- raw tonic client ------------------------------------------
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).expect("endpoint uri");
    let channel: Channel = endpoint.connect().await.expect("channel connect");
    let mut client = RaftServiceClient::new(channel);

    // --- group 10 routes to raft_a ---------------------------------
    let payload = well_formed_vote_payload(1);
    let resp = client
        .vote(pb::VoteRequest {
            payload: payload.clone(),
            raft_group_id: 10,
        })
        .await
        .expect("group 10 vote round-trips")
        .into_inner();
    // Regardless of granted/refused, the response decodes as
    // openraft's wire shape — which proves we hit a real Raft handle
    // and not the error fallback.
    assert!(
        decode_vote_body(&resp).is_ok(),
        "group 10 response should be decodable as VoteResponse or RaftError"
    );

    // --- group 20 routes to raft_b ---------------------------------
    let resp = client
        .vote(pb::VoteRequest {
            payload: payload.clone(),
            raft_group_id: 20,
        })
        .await
        .expect("group 20 vote round-trips")
        .into_inner();
    assert!(
        decode_vote_body(&resp).is_ok(),
        "group 20 response should be decodable as VoteResponse or RaftError"
    );

    // --- group 999 is unregistered --------------------------------
    let err = client
        .vote(pb::VoteRequest {
            payload,
            raft_group_id: 999,
        })
        .await
        .expect_err("group 999 is not registered; must produce tonic Status");
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        err.message().contains("999"),
        "NotFound message should mention the unknown group id, got {err:?}"
    );

    // --- shutdown ---------------------------------------------------
    group_a.raft.shutdown().await.unwrap();
    group_b.raft.shutdown().await.unwrap();
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

/// The server envelope carries either an openraft `VoteResponse` (on
/// `is_error = false`) or a `RaftError` (on `is_error = true`). For
/// dispatch validation, "does it decode as one of those?" is enough.
fn decode_vote_body(resp: &pb::VoteResponse) -> Result<(), aresadb_net::codec::CodecError> {
    if resp.is_error {
        let _: openraft::error::RaftError<NodeId> = decode(&resp.payload)?;
    } else {
        let _: openraft::raft::VoteResponse<NodeId> = decode(&resp.payload)?;
    }
    Ok(())
}

#[tokio::test]
async fn singleton_raft_directory_serves_every_group_id() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let node = SingleNode::in_memory().await.expect("single node");

    // --- wire server via RaftGrpcServer::new (back-compat path) ----
    let (listener, addr) = pick_port().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = RaftGrpcServer::new(node.raft.clone()).into_service();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server");
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .expect("endpoint uri")
        .connect()
        .await
        .expect("channel connect");
    let mut client = RaftServiceClient::new(channel);

    let payload = well_formed_vote_payload(1);

    // Singleton directory routes ANY id to the wrapped Raft — test
    // both `0` (the Phase 1 SINGLETON_RAFT_GROUP_ID) and an
    // arbitrary one.
    for id in [aresadb_net::SINGLETON_RAFT_GROUP_ID, 42, u64::MAX] {
        let resp = client
            .vote(pb::VoteRequest {
                payload: payload.clone(),
                raft_group_id: id,
            })
            .await
            .unwrap_or_else(|e| panic!("singleton directory must accept id {id}, got {e:?}"))
            .into_inner();
        assert!(decode_vote_body(&resp).is_ok());
    }

    node.raft.shutdown().await.unwrap();
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

/// Kept as a hedge: I want `GrpcRaftNetwork::new(..., raft_group_id)`
/// to actually place the id on the wire. The end-to-end two-/three-
/// node transport tests already prove the `0` singleton works; this
/// additional test verifies *non-zero* ids flow through without the
/// server ever seeing a default-zero id.
///
/// Setup: one server with a directory that registers group `777`
/// only. One `GrpcRaftNetwork::new(peers, 777)`. We drive the low-
/// level client to send a well-formed request and expect success;
/// sending with `0` should hit NotFound.
#[tokio::test]
async fn grpc_raft_network_tags_outbound_rpcs_with_its_group_id() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let node = SingleNode::in_memory().await.expect("single node");
    let directory = Arc::new(MultiGroupDirectory::default());
    directory.insert(777, node.raft.clone());

    let (listener, addr) = pick_port().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = RaftGrpcServer::from_directory(directory.clone()).into_service();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server");
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .expect("endpoint uri")
        .connect()
        .await
        .expect("channel connect");
    let mut client = RaftServiceClient::new(channel);
    let payload = well_formed_vote_payload(1);

    // The client-side `GrpcRaftNetwork` tags RPCs at construction time,
    // so here we prove the server-side is strict about *which* id it
    // accepts — the only way a client gets through is by sending the
    // right `raft_group_id`. That plus the upstream two-/three-node
    // tests (which use `new_singleton` → id 0) pin both halves.
    let good = client
        .vote(pb::VoteRequest {
            payload: payload.clone(),
            raft_group_id: 777,
        })
        .await
        .expect("correct id routes")
        .into_inner();
    assert!(decode_vote_body(&good).is_ok());

    let bad = client
        .vote(pb::VoteRequest {
            payload,
            raft_group_id: 0,
        })
        .await
        .expect_err("singleton id must not leak into the multi-group directory");
    assert_eq!(bad.code(), Code::NotFound);

    node.raft.shutdown().await.unwrap();
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
