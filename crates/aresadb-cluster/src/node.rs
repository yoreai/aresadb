//! Cluster node lifecycle.
//!
//! [`ClusterNode`] owns every piece of long-lived state for a running
//! AresaDB process:
//!   * a [`RangeDirectory`] of every range this node serves, each
//!     running its own `openraft::Raft<TypeConfig>` and its own
//!     per-range log + data backends,
//!   * the peer directory (for outgoing Raft traffic),
//!   * the gRPC server task that serves inbound Raft and admin RPCs
//!     for every range on this node — one listener, many groups,
//!   * a shutdown oneshot so the caller can tear it down cleanly.
//!
//! Phase 2c flips the node from a single Raft group to a full range-
//! aware multi-Raft layout. For back-compat with Phase 1 callers, a
//! well-known "default range" is always open: range_id =
//! [`DEFAULT_RANGE_ID`] with `raft_group_id` =
//! [`DEFAULT_RAFT_GROUP_ID`]. Legacy accessors ([`ClusterNode::raft`],
//! [`ClusterNode::data`], [`ClusterNode::log_backend`]) return the
//! default range's handles, so existing integration tests, admin
//! RPCs, and the CLI keep working unchanged.
//!
//! The public API is small on purpose. The hard part isn't the type —
//! it's making sure "bring up a node", "bring it down", and "restart
//! it" all route through the same construction path so recovery and
//! first boot share the same tested code.

use std::sync::Arc;

use aresadb_core::StorageBackend;
use aresadb_net::{GrpcRaftNetwork, RaftGrpcServer, StaticPeerDirectory};
use aresadb_pd::types::{RangeDescriptor, RangeId, ReplicaPlacement};
use aresadb_raft::{NodeId, TypeConfig};
use openraft::{Config, Raft};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::info;

use crate::admin::{AdminService, ClusterAdminServer};
use crate::config::NodeConfig;
use crate::error::{ClusterError, ClusterResult};
use crate::pd_supervisor::{PdSupervisor, PdSupervisorConfig, PdSupervisorHandle};
use crate::range::{RangeDirectory, RangeRuntime};

/// Range id reserved for the Phase 1 back-compat "default range" —
/// the keyspace-wide `[min, +infinity)` range that every `ClusterNode`
/// opens on boot. Real PD-assigned ranges start above this; the PD
/// counter will be bootstrapped accordingly in Phase 2c-4.
pub const DEFAULT_RANGE_ID: RangeId = 1;

/// `raft_group_id` reserved for the default range. Matches
/// `DEFAULT_RANGE_ID` because `RangeDescriptor::new` defaults
/// `raft_group_id = range_id`; both sides of the wire must agree for
/// RPCs to route through [`RangeDirectory`] correctly.
pub const DEFAULT_RAFT_GROUP_ID: u64 = DEFAULT_RANGE_ID;

/// A running cluster member.
///
/// Owns a `RangeDirectory` with at least the default range always
/// registered. More ranges can be added via future admin RPCs
/// (Phase 2c-3c) or the PD supervisor (Phase 2c-4); the gRPC server
/// dispatches inbound RPCs to the right range based on the wire-
/// level `raft_group_id` envelope introduced in Phase 2c-1.
pub struct ClusterNode {
    pub(crate) node_id: NodeId,
    pub(crate) range_directory: Arc<RangeDirectory>,
    pub(crate) default_range: Arc<RangeRuntime>,
    pub(crate) directory: Arc<StaticPeerDirectory>,
    pub(crate) node_config: NodeConfig,

    /// Channel to signal the server task to exit.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Handle to the gRPC server task. `None` once we've joined it.
    server_task: Option<JoinHandle<()>>,
    /// Handle to the membership watcher task. Keeps the in-memory
    /// peer directory in sync with whatever openraft reports as the
    /// committed membership — essential for graceful recovery, where
    /// the Raft log knows who the peers are but the directory is
    /// empty until we tell it.
    membership_task: Option<JoinHandle<()>>,
    /// Optional PD orchestration supervisor. Present only when
    /// [`ClusterNode::attach_pd_supervisor`] has been called. Owned
    /// by the node so shutdown tears it down before the gRPC
    /// server exits (otherwise an in-flight `AddRange` from the
    /// supervisor could race the `shutdown_tx` drop).
    pd_supervisor: Option<PdSupervisorHandle>,
}

impl ClusterNode {
    /// Open the default range's backends, spin up an uninitialised
    /// Raft instance for it, register it in the `RangeDirectory`, and
    /// start the gRPC server. Does NOT call `raft.initialize(...)`.
    ///
    /// Every code path that boots a node eventually ends up here —
    /// both cold starts and cluster bootstraps. Whether the node is a
    /// fresh member or a recovering one is decided by what
    /// subsequently happens to it (the caller either initialises it,
    /// or leaves it waiting for a leader to add it as a voter).
    pub async fn start(config: NodeConfig) -> ClusterResult<Self> {
        config.ensure_dirs()?;

        // Seed the peer directory with ourselves so outbound
        // traffic to our own id (e.g. openraft's self-replication
        // heartbeats) uses the right address.
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        peer_directory.upsert(config.node_id, config.effective_advertise_addr());

        let raft_config = Arc::new(
            Config {
                heartbeat_interval: 150,
                election_timeout_min: 500,
                election_timeout_max: 1500,
                cluster_name: config.cluster_name.clone(),
                ..Default::default()
            }
            .validate()
            .map_err(|e| ClusterError::Config(format!("invalid raft config: {e}")))?,
        );

        let range_directory = RangeDirectory::new();

        // Build the default range descriptor. `RangeDescriptor::new`
        // defaults `raft_group_id = range_id`, so the wire envelope
        // and the directory key line up automatically.
        let default_descriptor = RangeDescriptor::new(
            DEFAULT_RANGE_ID,
            b"".to_vec(),
            b"".to_vec(),
            vec![ReplicaPlacement::voter(config.node_id, 1)],
        );
        debug_assert_eq!(default_descriptor.raft_group_id, DEFAULT_RAFT_GROUP_ID);

        // One `GrpcRaftNetwork` per range — tagged with the range's
        // group id so outbound RPCs carry the right envelope. Every
        // range opened later does the same via the admin RPC path.
        let default_network = GrpcRaftNetwork::new(peer_directory.clone(), DEFAULT_RAFT_GROUP_ID);

        let default_runtime = Arc::new(
            RangeRuntime::open_on_disk(
                default_descriptor,
                config.node_id,
                &config,
                default_network,
                raft_config.clone(),
            )
            .await?,
        );

        range_directory
            .insert(default_runtime.clone())
            .map_err(|e| ClusterError::Config(format!("register default range: {e}")))?;

        // Rehydrate the peer directory from whatever openraft knows
        // about the committed membership on the default range. First
        // time this node boots (`bootstrap_single`) the membership is
        // empty and this is a no-op; on restart the Raft log replays
        // membership into metrics *before* start() returns, so we
        // pick up every peer that was part of the cluster before
        // shutdown.
        sync_directory(&peer_directory, default_runtime.raft());
        let membership_task =
            spawn_membership_watcher(peer_directory.clone(), default_runtime.raft().clone());

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = spawn_server(
            config.listen_addr,
            range_directory.clone(),
            default_runtime.raft().clone(),
            peer_directory.clone(),
            default_runtime.data_backend().clone(),
            config.clone(),
            shutdown_rx,
        );

        info!(
            node_id = config.node_id,
            listen_addr = %config.listen_addr,
            advertise_addr = %config.effective_advertise_addr(),
            default_range_id = DEFAULT_RANGE_ID,
            default_raft_group_id = DEFAULT_RAFT_GROUP_ID,
            "cluster node started"
        );

        Ok(Self {
            node_id: config.node_id,
            range_directory,
            default_range: default_runtime,
            directory: peer_directory,
            node_config: config,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
            membership_task: Some(membership_task),
            pd_supervisor: None,
        })
    }

    /// Convenience: start a node *and* initialise its default range
    /// as a brand-new single-voter cluster containing only itself.
    /// Good for the first node of a cluster or for single-node
    /// deployments.
    pub async fn bootstrap_single(config: NodeConfig) -> ClusterResult<Self> {
        let node = Self::start(config.clone()).await?;
        // Seed the membership record with our advertise address so
        // `spawn_membership_watcher` can populate the peer directory
        // from Raft metrics alone. `bootstrap_voter_with_addr` folds
        // the idempotent "already-initialised → wait for election"
        // path internally, so the same call is correct on fresh boot
        // and on recovery.
        node.default_range
            .bootstrap_voter_with_addr(config.effective_advertise_addr())
            .await?;
        Ok(node)
    }

    /// Attach a [`PdSupervisor`] to a running node. After this
    /// returns, the supervisor's heartbeat + reconcile tasks are
    /// alive and will stay alive until the node shuts down.
    ///
    /// Safe to call at most once per node. Calling it a second
    /// time returns [`ClusterError::Config`]; re-attachment would
    /// leak the existing supervisor's tasks.
    ///
    /// Most callers will prefer [`ClusterNode::start_with_pd`] —
    /// this method exists for the case where the supervisor's PD
    /// endpoints are discovered after the node is already up
    /// (e.g. via a service-discovery call).
    pub async fn attach_pd_supervisor(
        &mut self,
        supervisor_config: PdSupervisorConfig,
    ) -> ClusterResult<()> {
        if self.pd_supervisor.is_some() {
            return Err(ClusterError::Config(
                "pd supervisor is already attached to this node".to_string(),
            ));
        }
        let handle = PdSupervisor::spawn(
            supervisor_config,
            self.node_config.clone(),
            self.range_directory.clone(),
            self.directory.clone(),
        )
        .await
        .map_err(|e| ClusterError::Config(format!("pd supervisor spawn: {e}")))?;
        self.pd_supervisor = Some(handle);
        Ok(())
    }

    /// Convenience: [`ClusterNode::start`] followed by
    /// [`ClusterNode::attach_pd_supervisor`]. Returns the running
    /// node with the supervisor already live.
    pub async fn start_with_pd(
        config: NodeConfig,
        supervisor_config: PdSupervisorConfig,
    ) -> ClusterResult<Self> {
        let mut node = Self::start(config).await?;
        node.attach_pd_supervisor(supervisor_config).await?;
        Ok(node)
    }

    /// `true` if this node has a live PD supervisor. Useful for
    /// tests and for operator tooling that wants to surface the
    /// state without pattern-matching on internal fields.
    pub fn has_pd_supervisor(&self) -> bool {
        self.pd_supervisor.as_ref().is_some_and(|h| h.is_running())
    }

    /// Stable node id.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Range directory — the multi-Raft source of truth. Use this
    /// for range-aware operations (new admin RPCs, PD supervisor,
    /// tests that add ranges beyond the default). Cloning the
    /// returned `Arc` is cheap.
    pub fn range_directory(&self) -> &Arc<RangeDirectory> {
        &self.range_directory
    }

    /// Default range runtime. Back-compat handle for the Phase 1
    /// single-group code paths (admin RPCs, CLI, legacy integration
    /// tests). Returns an `Arc` so callers can stash it without
    /// another layer of indirection.
    pub fn default_range(&self) -> &Arc<RangeRuntime> {
        &self.default_range
    }

    /// Handle to the default range's state-machine data backend.
    /// Retained for back-compat; new code should prefer
    /// [`ClusterNode::default_range`] or
    /// [`ClusterNode::range_directory`].
    pub fn data(&self) -> &Arc<dyn StorageBackend> {
        self.default_range.data_backend()
    }

    /// Handle to the default range's Raft log backend. Retained for
    /// back-compat; new code should prefer
    /// [`ClusterNode::default_range`].
    pub fn log_backend(&self) -> &Arc<dyn StorageBackend> {
        self.default_range.log_backend()
    }

    /// Default range's Raft handle. Cloneable. Retained for
    /// back-compat so existing admin / status / write paths keep
    /// working; range-aware callers should reach for the directory
    /// instead.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        self.default_range.raft()
    }

    /// Peer directory. Mutations here take effect on the next outbound
    /// RPC, so operators updating it at runtime don't need to rebuild
    /// the transport.
    pub fn directory(&self) -> &Arc<StaticPeerDirectory> {
        &self.directory
    }

    /// Gracefully shut down the node: stop the PD supervisor (if
    /// attached), then the gRPC server, then shut down every range
    /// (Raft + backends) registered in the directory.
    pub async fn shutdown(mut self) -> ClusterResult<()> {
        // Stop the PD supervisor first so it can't open a new
        // range in the middle of shutdown. `stop()` awaits both the
        // heartbeat and reconcile tasks.
        if let Some(supervisor) = self.pd_supervisor.take() {
            supervisor.stop().await;
        }

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.server_task.take() {
            // Don't fail shutdown on server join errors — they're
            // rare and we're on the error path already.
            let _ = task.await;
        }
        if let Some(task) = self.membership_task.take() {
            task.abort();
            let _ = task.await;
        }

        // Drop the direct `default_range` handle so `Arc::try_unwrap`
        // below can succeed for it when the directory hands back the
        // last Arc. The directory's `drain()` returns every currently-
        // registered runtime; each one owns its Raft + backends.
        drop(self.default_range);

        let runtimes = self.range_directory.drain();
        for runtime in runtimes {
            match Arc::try_unwrap(runtime) {
                Ok(rt) => rt.shutdown().await?,
                Err(shared) => {
                    // Something else is still holding a reference
                    // (unexpected on shutdown) — log and skip
                    // `shutdown`, because calling it requires owned
                    // `self`. We still shut down the Raft portion
                    // because that only needs a handle clone.
                    tracing::warn!(
                        range_id = shared.descriptor().range_id,
                        "range runtime still has outstanding references; performing partial shutdown"
                    );
                    shared
                        .raft()
                        .clone()
                        .shutdown()
                        .await
                        .map_err(|e| ClusterError::Raft(e.to_string()))?;
                }
            }
        }

        Ok(())
    }
}

/// Copy every peer from openraft's current membership config into
/// `directory`. Called synchronously on startup so that by the time
/// this node's Raft replication task fires its first outbound RPC,
/// the directory already knows where to reach every peer.
fn sync_directory(directory: &StaticPeerDirectory, raft: &Raft<TypeConfig>) {
    let metrics = raft.metrics().borrow().clone();
    for (id, node) in metrics.membership_config.nodes() {
        if !node.addr.is_empty() {
            directory.upsert(*id, node.addr.clone());
        }
    }
}

/// Background task that watches openraft's metrics channel and keeps
/// the in-memory peer directory in lockstep with committed
/// membership changes. Exits when the metrics channel closes (which
/// openraft does when `raft.shutdown()` completes).
fn spawn_membership_watcher(
    directory: Arc<StaticPeerDirectory>,
    raft: Raft<TypeConfig>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        loop {
            {
                let metrics = rx.borrow_and_update().clone();
                for (id, node) in metrics.membership_config.nodes() {
                    if !node.addr.is_empty() {
                        directory.upsert(*id, node.addr.clone());
                    }
                }
            }
            if rx.changed().await.is_err() {
                break;
            }
        }
    })
}

/// Spawn the gRPC server that exposes both the multi-Raft transport
/// and the admin API on the same port. The Raft service fans out via
/// the `RangeDirectory`; the admin service holds the same directory
/// so range-aware admin RPCs (`AddRange`, `RemoveRange`,
/// `ListRanges`) can mutate it without a separate coordination
/// channel.
fn spawn_server(
    listen: std::net::SocketAddr,
    range_directory: Arc<RangeDirectory>,
    admin_raft: Raft<TypeConfig>,
    directory: Arc<StaticPeerDirectory>,
    admin_data: Arc<dyn StorageBackend>,
    node_config: NodeConfig,
    shutdown_rx: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    let raft_service = RaftGrpcServer::from_directory(range_directory.clone()).into_service();
    let admin_service = ClusterAdminServer::new(AdminService::new(
        admin_raft,
        directory,
        admin_data,
        range_directory,
        node_config,
    ));

    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(raft_service)
            .add_service(admin_service)
            .serve_with_shutdown(listen, async {
                let _ = shutdown_rx.await;
            })
            .await
        {
            tracing::error!(error = ?e, %listen, "cluster gRPC server exited with error");
        }
    })
}
