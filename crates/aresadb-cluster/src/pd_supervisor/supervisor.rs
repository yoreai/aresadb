//! PD-driven orchestration task.
//!
//! [`PdSupervisor::spawn`] starts two tokio tasks that live for the
//! lifetime of the supervisor:
//!
//! 1. **Heartbeat task** — a [`aresadb_pd::admin::HeartbeatLoop`]
//!    that pings the PD leader at
//!    [`PdSupervisorConfig::heartbeat_interval`] so the catalog's
//!    liveness timer stays fresh for this node.
//! 2. **Reconcile task** — an independent timer loop that calls
//!    `list_ranges` on PD, runs
//!    [`super::reconciler::plan_reconcile`] against the local
//!    `RangeDirectory`, and applies the result via
//!    [`super::executor::execute_plan`].
//!
//! Both tasks share one [`tokio::sync::watch::Sender<bool>`] — the
//! shutdown signal. [`PdSupervisorHandle::stop`] flips it to
//! `true` and awaits both tasks; dropping the handle also flips
//! the flag (fire-and-forget).
//!
//! The supervisor is intentionally forgiving: transient PD
//! unavailability, leader changes, malformed descriptors, or
//! collisions with racing admin RPCs all show up as logs and a
//! retry on the next tick. A node with broken PD connectivity
//! still serves its already-open ranges via the gRPC fan-out
//! server — only new ranges are stuck until PD is reachable again.

use std::sync::Arc;

use aresadb_net::StaticPeerDirectory;
use aresadb_pd::admin::client::{PdAdminClient, PdAdminClientError};
use aresadb_pd::admin::heartbeat::{HeartbeatConfig, HeartbeatHandle, HeartbeatLoop};
use aresadb_pd::types::NodeInfo;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::NodeConfig;
use crate::range::RangeDirectory;

use super::config::PdSupervisorConfig;
use super::executor::execute_plan;
use super::reconciler::plan_reconcile;

/// Error surface returned by [`PdSupervisor::spawn`].
///
/// Runtime errors inside the reconcile / heartbeat loops are logged
/// and retried rather than propagated; only setup-time failures
/// (bad config, unreachable PD on first dial, registration
/// rejected) come back through this type.
#[derive(Debug, Error)]
pub enum PdSupervisorError {
    /// `PdSupervisorConfig::pd_endpoints` was empty.
    #[error("pd supervisor config has no pd endpoints")]
    MissingEndpoints,

    /// The initial dial to any PD endpoint failed. The supervisor
    /// refuses to start so the caller can surface the error to
    /// ops immediately instead of having the node silently fail
    /// to register.
    #[error("initial dial to pd failed: {0}")]
    Dial(String),

    /// `register_node` rejected the node's identity. Usually
    /// indicates a catalog conflict (duplicate node id with a
    /// different address, for example).
    #[error("register_node failed: {0}")]
    Register(#[from] PdAdminClientError),
}

/// Handle returned by [`PdSupervisor::spawn`]. Drop to terminate
/// both tasks; call [`PdSupervisorHandle::stop`] to wait for the
/// in-flight work to drain.
pub struct PdSupervisorHandle {
    shutdown_tx: watch::Sender<bool>,
    heartbeat: Option<HeartbeatHandle>,
    reconcile_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for PdSupervisorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PdSupervisorHandle")
            .field("is_running", &self.is_running())
            .finish_non_exhaustive()
    }
}

impl PdSupervisorHandle {
    /// Signal shutdown and wait for both tasks to exit. Idempotent;
    /// safe to call on an already-stopped supervisor.
    pub async fn stop(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.heartbeat.take() {
            handle.stop().await;
        }
        if let Some(task) = self.reconcile_task.take() {
            let _ = task.await;
        }
    }

    /// `true` if the reconcile task is still running.
    pub fn is_running(&self) -> bool {
        self.reconcile_task
            .as_ref()
            .is_some_and(|t| !t.is_finished())
    }
}

impl Drop for PdSupervisorHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        // Both tasks observe the flag on their next `select!`
        // branch and exit. `HeartbeatHandle`'s own `Drop` impl
        // handles its task.
    }
}

/// Entry point for spawning a [`PdSupervisor`]. Static — the
/// supervisor has no persistent state outside the handle.
pub struct PdSupervisor;

impl PdSupervisor {
    /// Connect to PD, register this node, and spawn the heartbeat
    /// + reconcile background tasks. Returns a handle whose
    ///   `Drop` terminates everything.
    ///
    /// `node_config` is the [`ClusterNode`](crate::ClusterNode)'s
    /// own config — the executor needs it to compute per-range
    /// storage paths and cluster names. `peer_directory` is shared
    /// with the gRPC transport so newly-opened ranges inherit the
    /// existing peer map.
    pub async fn spawn(
        config: PdSupervisorConfig,
        node_config: NodeConfig,
        range_directory: Arc<RangeDirectory>,
        peer_directory: Arc<StaticPeerDirectory>,
    ) -> Result<PdSupervisorHandle, PdSupervisorError> {
        if config.pd_endpoints.is_empty() {
            return Err(PdSupervisorError::MissingEndpoints);
        }

        // First dial + register. Anything less strict (e.g. "retry
        // forever") would let a mis-configured node silently run
        // forever without anyone in the PD catalog — the operator
        // would see "why isn't this node visible?" with no signal.
        let primary = config.pd_endpoints[0].clone();
        let mut client = PdAdminClient::connect(primary.clone())
            .await
            .map_err(|e| PdSupervisorError::Dial(format!("{primary}: {e}")))?;

        let node_info = NodeInfo {
            node_id: config.node_id,
            address: config.advertise_addr.clone(),
            stores: vec![config.store_id],
            last_heartbeat_millis: 0,
        };
        // `register_node` is idempotent — a node id that re-registers
        // with the same address just refreshes `last_heartbeat_millis`
        // to zero (the heartbeat loop bumps it right afterwards).
        client.register_node(node_info).await?;

        // Heartbeat loop. No endpoint resolver for now — the
        // supervisor is the only thing driving this, and cross-
        // endpoint rotation can come in when dynamic peer
        // discovery lands (Phase 2c-5+).
        let heartbeat = HeartbeatLoop::spawn(HeartbeatConfig::new(
            config.node_id,
            primary,
            config.heartbeat_interval,
        ));

        // Shared shutdown. The reconcile task listens on
        // `shutdown_rx`; the heartbeat loop owns its own channel
        // under the hood and terminates when `heartbeat` is
        // dropped (by `PdSupervisorHandle::stop`).
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let log_node_id = config.node_id;
        let reconcile_task = spawn_reconcile_loop(
            config,
            node_config,
            range_directory,
            peer_directory,
            client,
            shutdown_rx,
        );

        info!(node_id = log_node_id, "pd supervisor started");

        Ok(PdSupervisorHandle {
            shutdown_tx,
            heartbeat: Some(heartbeat),
            reconcile_task: Some(reconcile_task),
        })
    }
}

/// Background reconcile loop. Extracted for readability; the only
/// caller is [`PdSupervisor::spawn`].
fn spawn_reconcile_loop(
    config: PdSupervisorConfig,
    node_config: NodeConfig,
    range_directory: Arc<RangeDirectory>,
    peer_directory: Arc<StaticPeerDirectory>,
    mut client: PdAdminClient,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = config.node_id;
        let mut ticker = tokio::time::interval(config.reconcile_interval);
        // The first tick of `interval` fires immediately; burn it
        // so the first reconcile pass happens after the first full
        // `reconcile_interval` rather than at t=0. Prevents a
        // spurious pass before the caller has even wired things
        // up, and matches the heartbeat loop's cadence.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!(node_id, "pd supervisor: shutdown requested");
                        return;
                    }
                }
                _ = ticker.tick() => {
                    if let Err(e) = reconcile_once(
                        node_id,
                        &config,
                        &node_config,
                        &range_directory,
                        &peer_directory,
                        &mut client,
                    ).await {
                        warn!(node_id, error = %e, "pd supervisor: reconcile tick failed");
                    }
                }
            }
        }
    })
}

/// Single reconcile pass. Extracted so the loop body reads cleanly
/// and so unit tests can exercise the logic once without timers.
async fn reconcile_once(
    node_id: aresadb_raft::NodeId,
    config: &PdSupervisorConfig,
    node_config: &NodeConfig,
    range_directory: &Arc<RangeDirectory>,
    peer_directory: &Arc<StaticPeerDirectory>,
    client: &mut PdAdminClient,
) -> Result<(), PdAdminClientError> {
    let pd_ranges = client.list_ranges().await?;
    let local = range_directory.descriptors();

    let plan = plan_reconcile(node_id, &pd_ranges, &local, &config.skip_local_ranges);
    if plan.is_empty() {
        return Ok(());
    }

    debug!(
        node_id,
        to_add = plan.to_add.len(),
        to_remove = plan.to_remove.len(),
        "pd supervisor: applying reconcile plan"
    );

    let report = execute_plan(plan, node_id, node_config, peer_directory, range_directory).await;

    if report.performed_work() {
        info!(
            node_id,
            added = ?report.added,
            removed = ?report.removed,
            "pd supervisor: reconcile applied"
        );
    }

    for err in report.errors {
        // Every executor error is already scoped to a single
        // range id, so log with structured context rather than
        // an opaque aggregate.
        warn!(node_id, error = %err, "pd supervisor: per-range error");
    }

    Ok(())
}

/// Expose the single reconcile pass for integration tests that
/// want to drive the supervisor logic synchronously (without
/// timers). Production code uses the [`PdSupervisor::spawn`] path.
#[doc(hidden)]
pub async fn reconcile_once_for_test(
    node_id: aresadb_raft::NodeId,
    config: &PdSupervisorConfig,
    node_config: &NodeConfig,
    range_directory: &Arc<RangeDirectory>,
    peer_directory: &Arc<StaticPeerDirectory>,
    client: &mut PdAdminClient,
) -> Result<(), PdAdminClientError> {
    reconcile_once(
        node_id,
        config,
        node_config,
        range_directory,
        peer_directory,
        client,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_refuses_empty_endpoint_list() {
        let tmp = tempfile::tempdir().unwrap();
        let node_config = NodeConfig::new(1, "127.0.0.1:0".parse().unwrap(), tmp.path());
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        let config = PdSupervisorConfig::new(1, "http://127.0.0.1:7001", vec![]);

        let result =
            PdSupervisor::spawn(config, node_config, range_directory, peer_directory).await;
        match result {
            Err(PdSupervisorError::MissingEndpoints) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_handle) => panic!("empty endpoints must fail"),
        }
    }

    #[tokio::test]
    async fn spawn_reports_dial_failure_for_unreachable_pd() {
        let tmp = tempfile::tempdir().unwrap();
        let node_config = NodeConfig::new(1, "127.0.0.1:0".parse().unwrap(), tmp.path());
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        // 127.0.0.1:1 is reserved for `tcpmux` on the TCP/IP
        // stack and is effectively guaranteed to refuse connections
        // on modern developer machines.
        let config = PdSupervisorConfig::new(
            1,
            "http://127.0.0.1:7001",
            vec!["http://127.0.0.1:1".to_string()],
        );

        let result =
            PdSupervisor::spawn(config, node_config, range_directory, peer_directory).await;
        match result {
            Err(PdSupervisorError::Dial(_)) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_handle) => panic!("unreachable pd must fail"),
        }
    }
}
