//! End-to-end integration test for the PD admin gRPC surface.
//!
//! Brings up a 3-node `PdCluster` (in-process Raft traffic over
//! [`PdRouter`]), wraps each member in a [`PdAdminService`] bound to
//! a real TCP listener, then drives the cluster exclusively through
//! the tonic admin client. Verifies:
//!
//! 1. Register + heartbeat + create range: every write replicates
//!    and followers converge on the same catalog state.
//! 2. Split / lease / membership updates land and round-trip via
//!    `ListRanges`.
//! 3. `ForwardToLeader` semantics: a mutating RPC sent to a
//!    follower surfaces as `Unavailable` with the `pd-leader-id`
//!    metadata hint, and the typed [`PdAdminClient`] folds that
//!    into [`PdAdminClientError::NotLeader`] so the caller can
//!    retry against the right endpoint.
//! 4. Catalog rejections (overlap, duplicate id, epoch regression)
//!    surface as `CatalogRejected` — not swallowed as transport
//!    errors.
//! 5. `HeartbeatLoop` drives a live cluster: spawning a loop and
//!    letting it run advances the catalog's `last_heartbeat_millis`
//!    without the test manually poking it.
//! 6. `Status` reports matching leader + range count across all
//!    members after the above writes quiesce.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aresadb_pd::admin::pb as admin_pb;
use aresadb_pd::{
    HeartbeatConfig, HeartbeatLoop, NodeId, NodeInfo, PdAdminClient, PdAdminClientError,
    PdAdminService, PdCluster, PdClusterMember, PlacementDriverAdminServer, RangeDescriptor,
    ReplicaPlacement,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Server};

const LEADER_TIMEOUT: Duration = Duration::from_secs(5);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(5);

fn voters(ids: &[NodeId]) -> Vec<ReplicaPlacement> {
    ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
}

fn genesis_range() -> RangeDescriptor {
    RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]))
}

/// A single admin-server task wrapping one [`PdClusterMember`].
struct MemberServer {
    node_id: NodeId,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl MemberServer {
    async fn spawn(member: &PdClusterMember) -> Self {
        // Pick a free local port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        drop(listener);

        let service = PdAdminService::new(
            member.raft.clone(),
            member.state_machine.clone(),
            member.data_backend.clone(),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(PlacementDriverAdminServer::new(service))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                eprintln!("pd admin server at {addr} exited: {e}");
            }
        });

        MemberServer {
            node_id: member.node_id,
            endpoint: format!("http://{addr}"),
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        }
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

/// Build a resolver that maps `NodeId -> endpoint` from the live
/// server map. Used by [`HeartbeatLoop`] to follow leader changes.
fn endpoint_resolver(
    endpoints: HashMap<NodeId, String>,
) -> Arc<dyn Fn(NodeId) -> Option<String> + Send + Sync> {
    Arc::new(move |id| endpoints.get(&id).cloned())
}

/// Brings up a 3-node PD cluster plus one tonic admin server per
/// member. Returns the cluster, a vector of servers, and the URL of
/// a client endpoint pointed at the current leader.
async fn setup_cluster(size: usize) -> (PdCluster, Vec<MemberServer>) {
    let cluster = PdCluster::in_memory(size).await.unwrap();
    cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    let mut servers = Vec::with_capacity(size);
    for id in cluster.ids() {
        let member = cluster.member(id).unwrap();
        servers.push(MemberServer::spawn(member).await);
    }
    (cluster, servers)
}

/// Dial an admin client at `endpoint`, retrying until the gRPC
/// server is actually accepting connections. tonic's `connect`
/// returns quickly when the server isn't ready yet, so we just
/// loop with a short sleep.
async fn connect_client(endpoint: &str) -> PdAdminClient {
    for _ in 0..40 {
        match Endpoint::from_shared(endpoint.to_string())
            .unwrap()
            .connect_timeout(Duration::from_millis(500))
            .connect()
            .await
        {
            Ok(channel) => return PdAdminClient::from_channel(channel),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("pd admin server at {endpoint} never accepted a connection");
}

/// Find the endpoint for the current leader among the provided
/// member-server set.
fn leader_endpoint(cluster: &PdCluster, servers: &[MemberServer]) -> String {
    let leader = cluster
        .leader()
        .expect("cluster has a leader during admin tests");
    servers
        .iter()
        .find(|s| s.node_id == leader)
        .map(|s| s.endpoint.clone())
        .expect("server map covers every cluster member")
}

/// Drop all member servers and the cluster itself.
async fn teardown(cluster: PdCluster, servers: Vec<MemberServer>) {
    for server in servers {
        server.shutdown().await;
    }
    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------
// 1) Register + heartbeat + create range via the admin surface
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_client_drives_catalog_on_three_node_cluster() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader_ep = leader_endpoint(&cluster, &servers);
    let mut client = connect_client(&leader_ep).await;

    // Register three nodes.
    for id in 1..=3u64 {
        client
            .register_node(NodeInfo {
                node_id: id,
                address: format!("127.0.0.1:70{id:02}"),
                stores: vec![1],
                last_heartbeat_millis: 0,
            })
            .await
            .unwrap();
    }

    // Heartbeat everyone.
    for id in 1..=3u64 {
        client
            .heartbeat_node(id, 1_700_000_000_000 + id)
            .await
            .unwrap();
    }

    // Create the genesis range.
    let stored = client.create_range(genesis_range()).await.unwrap();
    assert_eq!(stored.range_id, 1);
    assert_eq!(stored.raft_group_id, 1);

    // Let every follower catch up before probing their state.
    cluster
        .wait_for_replication(1, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Every member's state machine sees the genesis range and the 3
    // nodes. We check via the live catalog, not another RPC, so the
    // assertion is independent of which member would answer.
    for id in cluster.ids() {
        let m = cluster.member(id).unwrap();
        m.state_machine.read(|c| {
            assert_eq!(c.range_count(), 1);
            assert_eq!(c.get_range(1).unwrap().range_id, 1);
            assert_eq!(c.iter_nodes().count(), 3);
            for node_id in 1..=3 {
                let info = c.get_node(node_id).unwrap();
                assert_eq!(info.last_heartbeat_millis, 1_700_000_000_000 + node_id);
            }
        });
    }

    // `ListRanges` / `ListNodes` round-trip the same data.
    let ranges = client.list_ranges().await.unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].range_id, 1);

    let nodes = client.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.iter().all(|n| n.last_heartbeat_millis > 0));

    // `GetRangeByKey` returns the genesis range for any key.
    let hit = client.get_range_by_key(b"anything".to_vec()).await.unwrap();
    assert_eq!(hit.unwrap().range_id, 1);

    teardown(cluster, servers).await;
}

// ---------------------------------------------------------------
// 2) Split + lease + membership update
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_client_performs_splits_and_leases() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader_ep = leader_endpoint(&cluster, &servers);
    let mut client = connect_client(&leader_ep).await;

    client.create_range(genesis_range()).await.unwrap();
    cluster
        .wait_for_replication(1, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Split at "m".
    let rhs = client.split_range(1, b"m".to_vec()).await.unwrap();
    assert_eq!(rhs.start_key, b"m".to_vec());
    assert_eq!(rhs.end_key, Vec::<u8>::new());

    cluster
        .wait_for_replication(2, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Install a lease on the RHS.
    client
        .install_lease(
            rhs.range_id,
            aresadb_pd::LeaseInfo {
                holder: 2,
                expires_at_millis: 1_800_000_000_000,
            },
        )
        .await
        .unwrap();

    // Update membership on the LHS (range 1).
    client
        .update_membership(1, voters(&[1, 2, 3, 4]), 5)
        .await
        .unwrap();

    // Let all followers catch up to the latest wave of commands.
    cluster
        .wait_for_replication(2, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    for id in cluster.ids() {
        let m = cluster.member(id).unwrap();
        m.state_machine.read(|c| {
            let lhs = c.get_range(1).unwrap();
            assert_eq!(lhs.end_key, b"m".to_vec());
            assert_eq!(lhs.epoch, 5);
            assert_eq!(lhs.replicas.len(), 4);

            let rhs_live = c.get_range(rhs.range_id).unwrap();
            assert_eq!(rhs_live.lease.as_ref().unwrap().holder, 2);
        });
    }

    // ListRanges returns both in keyspace order.
    let listed = client.list_ranges().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].range_id, 1);
    assert_eq!(listed[1].range_id, rhs.range_id);

    teardown(cluster, servers).await;
}

// ---------------------------------------------------------------
// 3) ForwardToLeader semantics
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_to_follower_surface_not_leader_with_hint() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader = cluster.leader().unwrap();
    let follower_server = servers
        .iter()
        .find(|s| s.node_id != leader)
        .expect("at least one follower");
    let follower_ep = follower_server.endpoint.clone();

    let mut follower_client = connect_client(&follower_ep).await;

    let err = follower_client
        .create_range(genesis_range())
        .await
        .expect_err("follower must not accept writes");
    match err {
        PdAdminClientError::NotLeader(Some(hint)) => {
            assert_eq!(hint, leader, "hint should identify the actual leader");
        }
        other => panic!("expected NotLeader with hint, got {other:?}"),
    }

    // The admin client can follow the hint to produce a successful
    // write in one hop. (No retry helper exists yet — we just dial
    // the hinted endpoint manually.)
    let leader_ep = servers
        .iter()
        .find(|s| s.node_id == leader)
        .unwrap()
        .endpoint
        .clone();
    let mut leader_client = connect_client(&leader_ep).await;
    let stored = leader_client.create_range(genesis_range()).await.unwrap();
    assert_eq!(stored.range_id, 1);

    teardown(cluster, servers).await;
}

// ---------------------------------------------------------------
// 4) Catalog rejections surface as CatalogRejected, not transport
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlap_and_epoch_regression_surface_as_catalog_errors() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader_ep = leader_endpoint(&cluster, &servers);
    let mut client = connect_client(&leader_ep).await;

    client.create_range(genesis_range()).await.unwrap();
    cluster
        .wait_for_replication(1, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Overlap — same span, same id.
    let err = client
        .create_range(genesis_range())
        .await
        .expect_err("overlap must reject");
    assert!(matches!(err, PdAdminClientError::CatalogRejected(_)));

    // Update membership at epoch 5, then try to regress.
    client
        .update_membership(1, voters(&[1, 2, 3]), 5)
        .await
        .unwrap();
    let err = client
        .update_membership(1, voters(&[1, 2]), 3)
        .await
        .expect_err("epoch regression must reject");
    assert!(matches!(err, PdAdminClientError::CatalogRejected(_)));

    // Missing lease and clear=false — surfaces as InvalidArgument,
    // never touches Raft.
    let raw_resp = client
        .inner()
        .update_lease(admin_pb::UpdateLeaseRequest {
            range_id: 1,
            lease: None,
            clear: false,
        })
        .await;
    let status = raw_resp.expect_err("contradictory flags must reject");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    teardown(cluster, servers).await;
}

// ---------------------------------------------------------------
// 5) Heartbeat loop against a live cluster
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_loop_drives_catalog_timestamps() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader_ep = leader_endpoint(&cluster, &servers);
    let mut client = connect_client(&leader_ep).await;

    // Register node 42 so the heartbeat loop has a target.
    client
        .register_node(NodeInfo {
            node_id: 42,
            address: "127.0.0.1:4242".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        })
        .await
        .unwrap();

    // Endpoints keyed by node id so the heartbeat loop can follow
    // a leader hint when one arrives.
    let endpoints: HashMap<NodeId, String> = servers
        .iter()
        .map(|s| (s.node_id, s.endpoint.clone()))
        .collect();
    let resolver = endpoint_resolver(endpoints.clone());

    // Drive the loop off a monotonically increasing "clock" so the
    // catalog's `last_heartbeat_millis` moves forward in a way we
    // can check.
    let clock_tick = Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000));
    let clock_tick_for_fn = clock_tick.clone();
    let clock: Arc<dyn Fn() -> u64 + Send + Sync> =
        Arc::new(move || clock_tick_for_fn.fetch_add(1, std::sync::atomic::Ordering::SeqCst));

    let cfg = HeartbeatConfig {
        node_id: 42,
        interval: Duration::from_millis(40),
        endpoint: leader_ep.clone(),
        endpoint_for: Some(resolver),
        clock,
    };
    let handle = HeartbeatLoop::spawn(cfg);

    // Wait until the catalog's timestamp for node 42 moves forward
    // at least once. Poll a follower member's in-memory catalog —
    // that proves the heartbeat replicated across the quorum.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed = 0u64;
    while Instant::now() < deadline {
        let current = cluster
            .ids()
            .into_iter()
            .map(|id| {
                let m = cluster.member(id).unwrap();
                m.state_machine
                    .read(|c| c.get_node(42).map(|n| n.last_heartbeat_millis).unwrap_or(0))
            })
            .min()
            .unwrap_or(0);
        if current > 1_700_000_000_000 {
            observed = current;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        observed > 1_700_000_000_000,
        "heartbeat loop never advanced last_heartbeat_millis; saw {observed}"
    );

    handle.stop().await;
    teardown(cluster, servers).await;
}

// ---------------------------------------------------------------
// 6) Status reports reflect a settled cluster
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_reports_match_across_members_after_convergence() {
    let (cluster, servers) = setup_cluster(3).await;
    let leader_ep = leader_endpoint(&cluster, &servers);
    let mut leader_client = connect_client(&leader_ep).await;

    leader_client.create_range(genesis_range()).await.unwrap();
    let _ = leader_client.split_range(1, b"m".to_vec()).await.unwrap();
    cluster
        .wait_for_replication(2, CONVERGENCE_TIMEOUT)
        .await
        .unwrap();

    // Every member's status must report the same range_count and
    // a consistent current_leader. The leader reports itself; a
    // freshly-elected follower may briefly lag, but by this point
    // we've awaited replication so the view is stable.
    let leader = cluster.leader().unwrap();
    for server in &servers {
        let mut c = connect_client(&server.endpoint).await;
        let status = c.status().await.unwrap();
        assert_eq!(status["catalog"]["range_count"], serde_json::json!(2));
        assert_eq!(status["current_leader"], serde_json::json!(leader));
    }

    teardown(cluster, servers).await;
}
