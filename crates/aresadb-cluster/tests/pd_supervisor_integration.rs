//! Integration test for Phase 2c-4: PD-driven orchestration.
//!
//! Boots a 3-node [`PdCluster`] (in-process Raft) with one real
//! tonic admin server per member, plus a real [`ClusterNode`]
//! with its [`PdSupervisor`] attached. Writes flow:
//!
//!   client -> PD admin leader -> replicated through PD Raft
//!   -> supervisor reconcile tick -> local `RangeDirectory`
//!
//! Each test waits on the directory (with a timeout) rather than
//! trying to step the reconcile clock manually, mirroring how a
//! real operator would observe convergence.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use aresadb_cluster::{ClusterNode, NodeConfig, PdSupervisorConfig, DEFAULT_RANGE_ID};
use aresadb_pd::{
    NodeId, NodeInfo, PdAdminClient, PdAdminService, PdCluster, PdClusterMember,
    PlacementDriverAdminServer, RangeDescriptor, ReplicaPlacement,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Server};

const LEADER_TIMEOUT: Duration = Duration::from_secs(5);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(10);
const RECONCILE_TICK: Duration = Duration::from_millis(100);

/// Ask the OS for an unused local TCP port, then release it. Racy
/// in principle, but the window between here and the next
/// bind-for-real is small; the assertions downstream catch it.
async fn pick_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

// ---------------------------------------------------------------
// PD admin-server wrapper.
//
// The `aresadb-pd` crate has the same helper in `tests/`, but
// integration tests are separate compilation units — copying 30
// lines is cheaper than carving out a pub-crate test helper.
// ---------------------------------------------------------------

struct PdMemberServer {
    node_id: NodeId,
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl PdMemberServer {
    async fn spawn(member: &PdClusterMember) -> Self {
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

        PdMemberServer {
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

/// Spin up a 3-node in-memory `PdCluster` and one tonic admin
/// server per member. Returns everything the tests need to drive
/// the cluster.
async fn setup_pd_cluster() -> (PdCluster, Vec<PdMemberServer>) {
    let cluster = PdCluster::in_memory(3).await.unwrap();
    cluster.wait_for_leader(LEADER_TIMEOUT).await.unwrap();

    let mut servers = Vec::new();
    for id in cluster.ids() {
        let member = cluster.member(id).unwrap();
        servers.push(PdMemberServer::spawn(member).await);
    }
    (cluster, servers)
}

async fn teardown_pd(cluster: PdCluster, servers: Vec<PdMemberServer>) {
    for server in servers {
        server.shutdown().await;
    }
    cluster.shutdown().await.unwrap();
}

fn leader_endpoint(cluster: &PdCluster, servers: &[PdMemberServer]) -> String {
    let leader = cluster
        .leader()
        .expect("pd cluster has a leader at test time");
    servers
        .iter()
        .find(|s| s.node_id == leader)
        .map(|s| s.endpoint.clone())
        .expect("server map covers every pd member")
}

/// Dial the admin client at `endpoint`, retrying until the tonic
/// server is accepting. Matches the helper used in the aresadb-pd
/// admin integration test.
async fn connect_pd_client(endpoint: &str) -> PdAdminClient {
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

// ---------------------------------------------------------------
// ClusterNode + supervisor helpers
// ---------------------------------------------------------------

fn node_config(node_id: NodeId, listen: SocketAddr, dir: &TempDir) -> NodeConfig {
    NodeConfig::new(
        node_id,
        listen,
        dir.path().join(format!("cluster-node-{node_id}")),
    )
    .with_cluster_name("aresadb-pd-supervisor-test")
}

fn supervisor_config(
    node_id: NodeId,
    advertise_addr: String,
    pd_endpoints: Vec<String>,
) -> PdSupervisorConfig {
    PdSupervisorConfig::new(node_id, advertise_addr, pd_endpoints)
        // Aggressive cadences so the test converges quickly
        // without relying on production defaults.
        .with_heartbeat_interval(Duration::from_millis(200))
        .with_reconcile_interval(RECONCILE_TICK)
}

/// Poll `check` every 50ms up to `timeout`. Returns the first
/// `Some(x)` the closure yields; panics otherwise.
async fn wait_for<T, F>(mut check: F, timeout: Duration, label: &str) -> T
where
    F: FnMut() -> Option<T>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = check() {
            return v;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {label} (after {timeout:?})");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

/// One `ClusterNode` with a supervisor observes a PD `create_range`
/// and opens the matching `RangeRuntime` locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_opens_pd_created_range_locally() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    // 1. Bring up the PD cluster + admin servers, connect a client
    // to the leader.
    let (pd_cluster, pd_servers) = setup_pd_cluster().await;
    let leader_ep = leader_endpoint(&pd_cluster, &pd_servers);
    let mut pd_admin = connect_pd_client(&leader_ep).await;

    // 2. Pre-create a PD range assigned to node 1. Using range id
    // 42 (distinct from `DEFAULT_RANGE_ID`=1) so the supervisor's
    // skip-local protections for the default range don't mask
    // correctness.
    let pd_descriptor = RangeDescriptor::new(
        42,
        b"a".to_vec(),
        b"m".to_vec(),
        vec![ReplicaPlacement::voter(1, 1)],
    );
    pd_admin.create_range(pd_descriptor).await.unwrap();

    // 3. Bring up `ClusterNode` and attach the supervisor.
    // `attach_pd_supervisor` performs the initial `register_node`
    // synchronously, so on return the node is already in the
    // catalog; the reconcile task ticks afterwards.
    let tmp = TempDir::new().unwrap();
    let cluster_listen = pick_addr().await;
    let cluster_advertise = format!("http://{cluster_listen}");
    let node_cfg = node_config(1, cluster_listen, &tmp);
    let sup_cfg = supervisor_config(1, cluster_advertise.clone(), vec![leader_ep.clone()]);

    let mut node = ClusterNode::start(node_cfg.clone()).await.unwrap();
    // Bootstrap the default range so its Raft handle is alive.
    // The supervisor doesn't manage the default range (it's in
    // the `skip_local_ranges` set by default), so this stays
    // separate from the PD-driven path.
    node.default_range()
        .bootstrap_voter_with_addr(cluster_advertise.clone())
        .await
        .unwrap();
    node.attach_pd_supervisor(sup_cfg).await.unwrap();
    assert!(node.has_pd_supervisor());

    // 4. Wait for the local `RangeDirectory` to converge.
    wait_for(
        || node.range_directory().get_range(42).map(|_| ()),
        CONVERGE_TIMEOUT,
        "local RangeDirectory to open range 42 via supervisor",
    )
    .await;

    // 5. Integrity checks: the default range is still alive, and
    // both descriptors are distinguishable.
    let descriptors = node.range_directory().descriptors();
    let ids: Vec<_> = descriptors.iter().map(|d| d.range_id).collect();
    assert!(
        ids.contains(&DEFAULT_RANGE_ID),
        "default range missing after reconcile (ids = {ids:?})"
    );
    assert!(
        ids.contains(&42),
        "pd-created range missing after reconcile (ids = {ids:?})"
    );

    // 6. Verify the gRPC-layer dispatch also registered: the
    // range's raft_group_id should resolve to the same runtime.
    assert!(
        node.range_directory().get_group(42).is_some(),
        "range 42 not addressable via raft_group_id after reconcile"
    );

    node.shutdown().await.unwrap();
    teardown_pd(pd_cluster, pd_servers).await;
}

/// The supervisor ignores PD ranges that don't list this node as a
/// replica. A range assigned only to a different node id should
/// never show up on this node, even as we add a second range that
/// *is* assigned to us.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_ignores_ranges_not_assigned_to_this_node() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let (pd_cluster, pd_servers) = setup_pd_cluster().await;
    let leader_ep = leader_endpoint(&pd_cluster, &pd_servers);
    let mut pd_admin = connect_pd_client(&leader_ep).await;

    // Pre-register a ghost node 5 (address is never dialled; it
    // just needs to exist in the catalog for the narrative).
    // NodeInfo doesn't enforce reachability.
    pd_admin
        .register_node(NodeInfo {
            node_id: 5,
            address: "http://127.0.0.1:65535".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        })
        .await
        .unwrap();

    // Range 77 assigned ONLY to node 5. Our supervisor on node 1
    // must leave this alone.
    let ghost_range = RangeDescriptor::new(
        77,
        b"x".to_vec(),
        b"z".to_vec(),
        vec![ReplicaPlacement::voter(5, 1)],
    );
    pd_admin.create_range(ghost_range).await.unwrap();

    // Now bring up node 1.
    let tmp = TempDir::new().unwrap();
    let cluster_listen = pick_addr().await;
    let cluster_advertise = format!("http://{cluster_listen}");
    let node_cfg = node_config(1, cluster_listen, &tmp);
    let sup_cfg = supervisor_config(1, cluster_advertise.clone(), vec![leader_ep.clone()]);

    let mut node = ClusterNode::start(node_cfg).await.unwrap();
    node.default_range()
        .bootstrap_voter_with_addr(cluster_advertise.clone())
        .await
        .unwrap();
    node.attach_pd_supervisor(sup_cfg).await.unwrap();

    // Let several reconcile ticks pass. If the supervisor were
    // going to open range 77, it would have done so by now.
    tokio::time::sleep(RECONCILE_TICK * 10).await;

    assert!(
        node.range_directory().get_range(77).is_none(),
        "node 1 must not open range 77 (assigned to node 5)"
    );
    assert!(
        node.range_directory().get_range(DEFAULT_RANGE_ID).is_some(),
        "default range must still be alive"
    );

    // Now also add a range that *is* assigned to us: the
    // reconciler should open it while still ignoring 77.
    let my_range = RangeDescriptor::new(
        88,
        b"a".to_vec(),
        b"m".to_vec(),
        vec![ReplicaPlacement::voter(1, 1)],
    );
    pd_admin.create_range(my_range).await.unwrap();
    wait_for(
        || node.range_directory().get_range(88).map(|_| ()),
        CONVERGE_TIMEOUT,
        "node 1 to open range 88",
    )
    .await;

    // Node 5's range is still ignored after multiple reconcile
    // cycles have passed.
    assert!(
        node.range_directory().get_range(77).is_none(),
        "node 1 must still ignore range 77 after opening range 88"
    );

    node.shutdown().await.unwrap();
    teardown_pd(pd_cluster, pd_servers).await;
}
