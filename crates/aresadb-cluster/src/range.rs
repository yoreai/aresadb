//! Per-range Raft runtime.
//!
//! [`RangeRuntime`] owns everything one range needs to replicate its
//! own slice of the keyspace:
//!   * the [`RangeDescriptor`] from the placement driver
//!     (`range_id`, `start_key`, `end_key`, replica placements,
//!     `raft_group_id`, `epoch`, `generation`),
//!   * the `openraft::Raft<TypeConfig>` handle for this range's group,
//!   * the per-range `StorageBackend` pair (log + data) the Raft log
//!     and state-machine adapters write through.
//!
//! One `RangeRuntime` maps 1:1 to one Raft group. A node running many
//! ranges will own a `HashMap<RangeId, Arc<RangeRuntime>>` — that's the
//! Phase 2c-3 `RangeDirectory`.
//!
//! ## Layout on disk
//!
//! Each range's state lives under `<data-dir>/ranges/<range_id>/` with
//! two sibling subdirectories:
//!
//! ```text
//! <data-dir>/
//!   ranges/
//!     <range_id>/
//!       log/      # Raft log backend (openraft's RaftLogStorage)
//!       data/     # state machine backend (openraft's RaftStateMachine)
//! ```
//!
//! Splitting log from data means the two can be tuned (or re-engined)
//! independently — the log backend is small, append-heavy, and
//! fsync-sensitive; the data backend is large, point-lookup-heavy, and
//! snapshot-friendly. Phase 2d will make the data engine pluggable
//! (redb vs. fjall-LSM) per range.
//!
//! ## Lifecycle
//!
//! * [`RangeRuntime::open`] — opens the backends, constructs the Raft
//!   handle, and rehydrates the state machine from disk. **Does not**
//!   call `raft.initialize(...)` — recovery paths rely on that.
//! * [`RangeRuntime::open_on_disk`] — convenience wrapper that derives
//!   the backend paths from a [`NodeConfig`] and opens redb backends.
//! * [`RangeRuntime::bootstrap_voter`] — wires `raft.initialize(...)`
//!   so the calling node becomes the sole voter and elects itself
//!   leader. Idempotent on restart (already-initialised is treated as
//!   a no-op).
//! * [`RangeRuntime::trigger_snapshot`] — request a snapshot from
//!   openraft. Useful for tests and for the future PD supervisor.
//! * [`RangeRuntime::shutdown`] — graceful Raft shutdown followed by
//!   backend `close()`.
//!
//! The runtime is generic over the network factory so tests can
//! substitute the in-process [`aresadb_raft::LoopbackNetwork`] while
//! production uses [`aresadb_net::GrpcRaftNetwork`]. Both satisfy
//! `RaftNetworkFactory<TypeConfig>`.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aresadb_core::StorageBackend;
use aresadb_engine_lsm::FjallBackend;
use aresadb_engine_redb::RedbBackend;
use aresadb_net::RaftDirectory;
use aresadb_pd::types::{GroupId, RangeDescriptor, RangeId};
use aresadb_raft::{LogStore, NodeId, StateMachineStore, TypeConfig};
use openraft::error::{InitializeError, RaftError};
use openraft::network::RaftNetworkFactory;
use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::config::{DataEngine, NodeConfig};
use crate::error::{ClusterError, ClusterResult, ReadResult};

/// Snapshot of a range's Raft leadership state, pulled from the
/// openraft metrics watch channel.
///
/// Cheap to produce — no I/O, no lock contention beyond a single
/// `watch::Receiver::borrow()`. Intended for observability hooks
/// (admin RPCs, the PD supervisor, Prometheus scrapers) rather than
/// for the read path itself; correctness checks go through
/// [`RangeRuntime::ensure_linearizable`] which talks to openraft.
///
/// All fields are plain data types so the struct is cheap to send
/// across an FFI or protobuf boundary — we deliberately avoid
/// leaking openraft types (`ServerState`, `Vote`) because they
/// change shape between minor versions and the cluster crate wants
/// a stable operator surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatus {
    /// Range id this status belongs to. Present because operators
    /// usually collect a batch of statuses and need to route them
    /// back to the range they describe.
    pub range_id: RangeId,

    /// The id of this local node. Avoids callers needing to carry
    /// the `NodeConfig` alongside the status.
    pub node_id: NodeId,

    /// `true` when this node was the leader at the time the
    /// metrics snapshot was taken. Based on `metrics.state ==
    /// Leader` — a follower with stale metrics will always report
    /// `false`, which is the conservative choice.
    pub is_leader: bool,

    /// Leader id as the metrics system understands it. May lag
    /// reality by up to one heartbeat interval during elections,
    /// which is fine for routing hints but *not* for
    /// linearizability. Linearizable reads must still call
    /// [`RangeRuntime::ensure_linearizable`].
    pub current_leader: Option<NodeId>,

    /// Current term. Useful when diagnosing storms of elections —
    /// a fast-rising term usually means network partitions or
    /// pre-vote rejections.
    pub current_term: u64,

    /// `last_log_index` from metrics. `None` if the node hasn't
    /// appended anything yet (a completely empty, never-initialised
    /// voter). Safe to use for monotonic lag tracking.
    pub last_log_index: Option<u64>,

    /// `last_applied` index from metrics (extracted out of the
    /// `Option<LogId>`). `None` for a brand-new member that has
    /// never applied. Paired with `last_log_index` to compute apply
    /// lag.
    pub last_applied_index: Option<u64>,

    /// Number of voters in the current committed membership. Useful
    /// as a reality check against the range descriptor — during a
    /// joint-consensus change the two can diverge briefly.
    pub voter_count: usize,
}

impl LeadershipStatus {
    /// Convenience: apply lag in log entries (`last_log_index -
    /// last_applied_index`), clamped to 0. Returns `None` if either
    /// index is missing.
    pub fn apply_lag(&self) -> Option<u64> {
        match (self.last_log_index, self.last_applied_index) {
            (Some(last), Some(applied)) => Some(last.saturating_sub(applied)),
            _ => None,
        }
    }
}

/// Running state for one range on one node.
///
/// Holds everything that must outlive an individual admin RPC: the
/// descriptor, the openraft handle, and the two backends. Cloneable
/// handles (`raft()`, `data_backend()`, etc.) are exposed so the
/// range-aware cluster node can share them without reparenting the
/// runtime itself.
pub struct RangeRuntime {
    descriptor: RangeDescriptor,
    node_id: NodeId,
    raft: Raft<TypeConfig>,
    log_backend: Arc<dyn StorageBackend>,
    data_backend: Arc<dyn StorageBackend>,
}

impl std::fmt::Debug for RangeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Keep the representation compact — the Raft handle and the
        // storage backends don't print usefully and the descriptor is
        // the thing operators actually want when diagnosing a process.
        f.debug_struct("RangeRuntime")
            .field("range_id", &self.descriptor.range_id)
            .field("raft_group_id", &self.descriptor.raft_group_id)
            .field("node_id", &self.node_id)
            .field("epoch", &self.descriptor.epoch)
            .field("generation", &self.descriptor.generation)
            .finish()
    }
}

impl RangeRuntime {
    /// Open a range on pre-existing storage backends.
    ///
    /// This is the low-level entry point; prefer
    /// [`RangeRuntime::open_on_disk`] when you want redb-backed storage
    /// under a [`NodeConfig`] data directory. `network` is consumed by
    /// openraft; pass a `GrpcRaftNetwork::new(directory, group_id)`
    /// (see `aresadb-net`) or a test-only `LoopbackNetwork`.
    ///
    /// Does **not** call `raft.initialize(...)`. Use
    /// [`RangeRuntime::bootstrap_voter`] on a fresh range, or leave it
    /// uninitialised if the caller will add this node as a voter from
    /// an existing leader.
    pub async fn open<N>(
        descriptor: RangeDescriptor,
        node_id: NodeId,
        log_backend: Arc<dyn StorageBackend>,
        data_backend: Arc<dyn StorageBackend>,
        network: N,
        raft_config: Arc<Config>,
    ) -> ClusterResult<Self>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let log = LogStore::new(log_backend.clone());
        let sm = StateMachineStore::open(data_backend.clone())
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        let raft = Raft::<TypeConfig>::new(node_id, raft_config, network, log, sm)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        info!(
            range_id = descriptor.range_id,
            raft_group_id = descriptor.raft_group_id,
            node_id,
            "range runtime opened"
        );

        Ok(Self {
            descriptor,
            node_id,
            raft,
            log_backend,
            data_backend,
        })
    }

    /// Open a range with on-disk backends rooted at
    /// `<cfg.data_dir>/ranges/<descriptor.range_id>/{log,data}/`.
    ///
    /// The log backend is always redb (append-heavy, one fsync per
    /// commit — redb's sweet spot). The data backend honours
    /// `cfg.data_engine`: `DataEngine::Redb` keeps every byte on
    /// redb (the default, unchanged from Phase 2c), and
    /// `DataEngine::Lsm` opens an `aresadb_engine_lsm::FjallBackend`
    /// at `.../data/data.lsm/` instead. Switching engines does not
    /// touch the log side, so a node may run a mixed deployment at
    /// the range level if callers supply per-range configs.
    ///
    /// Creates the directory structure on first open; subsequent opens
    /// are recovery and read from whatever the last shutdown flushed.
    /// The caller still owns a separate top-level `NodeConfig` (for
    /// listen address, advertised address, …) — this helper only uses
    /// the directory-layout accessors.
    pub async fn open_on_disk<N>(
        descriptor: RangeDescriptor,
        node_id: NodeId,
        cfg: &NodeConfig,
        network: N,
        raft_config: Arc<Config>,
    ) -> ClusterResult<Self>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        cfg.ensure_range_dirs(descriptor.range_id)?;
        let log_backend: Arc<dyn StorageBackend> =
            RedbBackend::open(cfg.range_log_path(descriptor.range_id)).await?;
        let data_backend: Arc<dyn StorageBackend> = match cfg.data_engine {
            DataEngine::Redb => RedbBackend::open(cfg.range_data_path(descriptor.range_id)).await?,
            DataEngine::Lsm => FjallBackend::open(cfg.range_data_path(descriptor.range_id)).await?,
        };
        Self::open(
            descriptor,
            node_id,
            log_backend,
            data_backend,
            network,
            raft_config,
        )
        .await
    }

    /// Initialise this range as a brand-new single-voter Raft group
    /// containing only the calling node, with no advertised address
    /// in the membership record. See
    /// [`RangeRuntime::bootstrap_voter_with_addr`] for the variant
    /// that seeds a peer address into the membership config — use
    /// that one from [`crate::ClusterNode`], because the peer
    /// directory depends on it.
    ///
    /// Idempotent on restart: pattern-matches
    /// [`InitializeError::NotAllowed`] on the return value and folds
    /// it into "already initialised, just wait for an election". That
    /// is more reliable than probing the error's display string (the
    /// wording changes across openraft releases) and more reliable
    /// than inspecting `RaftMetrics` up-front (the metrics channel
    /// takes a few ticks to report log state on a fresh reopen, so
    /// the state-machine driver may have the log loaded before the
    /// metrics snapshot catches up).
    pub async fn bootstrap_voter(&self) -> ClusterResult<()> {
        self.bootstrap_voter_with_addr("").await
    }

    /// Same as [`RangeRuntime::bootstrap_voter`] but seeds the
    /// membership entry for this node with `addr` so peer-directory
    /// consumers downstream (e.g. the `ClusterNode` membership
    /// watcher) can learn where to reach this node from Raft metrics
    /// alone.
    pub async fn bootstrap_voter_with_addr(&self, addr: impl Into<String>) -> ClusterResult<()> {
        let mut members = BTreeMap::new();
        members.insert(self.node_id, BasicNode::new(addr.into()));
        match self.raft.initialize(members).await {
            Ok(()) => {
                self.raft
                    .wait(Some(Duration::from_secs(10)))
                    .current_leader(self.node_id, "range voter self-becomes leader")
                    .await
                    .map_err(|e| ClusterError::Raft(e.to_string()))?;
            }
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                self.raft
                    .wait(Some(Duration::from_secs(10)))
                    .metrics(|m| m.current_leader.is_some(), "range leader elected")
                    .await
                    .map_err(|e| ClusterError::Raft(e.to_string()))?;
            }
            Err(e) => return Err(ClusterError::Raft(e.to_string())),
        }
        Ok(())
    }

    /// Range metadata this runtime was opened with. The descriptor is
    /// a snapshot of the catalog at open time; Phase 2c-3 keeps it
    /// refreshed as the PD reconfigures.
    pub fn descriptor(&self) -> &RangeDescriptor {
        &self.descriptor
    }

    /// Node id this runtime runs on. Convenience accessor so callers
    /// don't have to carry the id alongside the runtime.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Raft handle for this range. Cloneable — callers pass clones to
    /// the gRPC server, admin RPCs, and any background supervisors
    /// without needing an extra `Arc`.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    /// Application backend for this range. External readers (SQL /
    /// graph / vector layers) use this to serve committed reads
    /// without going through the Raft log.
    pub fn data_backend(&self) -> &Arc<dyn StorageBackend> {
        &self.data_backend
    }

    /// Raft log backend for this range. Exposed mostly for tests and
    /// diagnostics; production code should leave it to the Raft layer.
    pub fn log_backend(&self) -> &Arc<dyn StorageBackend> {
        &self.log_backend
    }

    /// Ask openraft to build a fresh snapshot. Returns after the
    /// request has been queued — building happens asynchronously on
    /// openraft's state-machine driver. Primarily a hook for the
    /// Phase 2c-4 PD supervisor (which triggers snapshots on apply-
    /// count thresholds) and for tests that want deterministic
    /// snapshot coverage.
    pub async fn trigger_snapshot(&self) -> ClusterResult<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))
    }

    /// Snapshot this range's leadership state.
    ///
    /// Observability-only; no network I/O, no linearizability
    /// guarantee. Pull this into admin `Status`, PD heartbeats, or
    /// Prometheus scrapers. For *read path* linearizability, use
    /// [`RangeRuntime::ensure_linearizable`] or
    /// [`RangeRuntime::linearizable_get`].
    pub fn leadership_status(&self) -> LeadershipStatus {
        let metrics = self.raft.metrics().borrow().clone();
        let is_leader = metrics.state.is_leader();
        let last_applied_index = metrics.last_applied.as_ref().map(|log_id| log_id.index);
        let voter_count = metrics.membership_config.membership().voter_ids().count();
        LeadershipStatus {
            range_id: self.descriptor.range_id,
            node_id: self.node_id,
            is_leader,
            current_leader: metrics.current_leader,
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            last_applied_index,
            voter_count,
        }
    }

    /// Linearizability guard for reads against this range's
    /// state machine.
    ///
    /// Wraps `openraft::Raft::ensure_linearizable`, which under
    /// openraft 0.9 runs the **ReadIndex** protocol: the leader
    /// sends heartbeats to a quorum of followers to confirm it is
    /// still the leader, and then waits for the state machine to
    /// apply at least up to the read log id. On success, a
    /// subsequent read against [`Self::data_backend`] is
    /// linearizable with respect to every write that acknowledged
    /// before this call returned.
    ///
    /// Callers should pair this with a data-backend read in the
    /// same task; see [`Self::linearizable_get`] for the common
    /// single-key shape.
    ///
    /// # Errors
    ///
    /// * [`ReadError::NotLeader`] — this node is not the leader.
    ///   The `Option<NodeId>` is the leader hint openraft attached
    ///   to the forward-to-leader error. Usually present, but may
    ///   be `None` during an election.
    /// * [`ReadError::QuorumUnavailable`] — the leader couldn't
    ///   reach a quorum during the heartbeat probe (minority
    ///   partition, slow peers). Transient; retry.
    /// * [`ReadError::Fatal`] — openraft reported a fatal state
    ///   (shutdown, corruption). The range is unusable without
    ///   operator intervention.
    pub async fn ensure_linearizable(&self) -> ReadResult<()> {
        self.raft.ensure_linearizable().await?;
        Ok(())
    }

    /// Linearizable point read for a single key.
    ///
    /// Runs [`Self::ensure_linearizable`] first, then reads the
    /// key out of [`Self::data_backend`]. Must be called on the
    /// range's Raft leader — routing decisions are the caller's
    /// responsibility (the PD catalog knows the current lease
    /// holder, and the admin RPC layer forwards to it).
    ///
    /// Returns `Ok(Some(value))` if the key exists at the
    /// linearization point, `Ok(None)` if it doesn't. Every other
    /// outcome maps to a [`ReadError`] variant — see
    /// [`Self::ensure_linearizable`] for the taxonomy.
    pub async fn linearizable_get(&self, key: &[u8]) -> ReadResult<Option<Vec<u8>>> {
        self.ensure_linearizable().await?;
        let value = self.data_backend.get(key).await?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    /// Bounded-staleness point read for a single key.
    ///
    /// Reads the range's state machine directly with **no**
    /// leadership guard. Safe to call on any member (leader or
    /// follower) but may return a value that has been committed
    /// but not yet applied on this node, or miss a write that
    /// was acknowledged elsewhere in the cluster.
    ///
    /// Intended for bounded-staleness workloads (analytics,
    /// warm caches, read-heavy scan fan-outs). For OLTP point
    /// reads, use [`Self::linearizable_get`].
    pub async fn stale_get(&self, key: &[u8]) -> ReadResult<Option<Vec<u8>>> {
        let value = self.data_backend.get(key).await?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    /// Gracefully shut down this range: stop Raft, then close both
    /// backends. Best-effort on the close step — we log and continue
    /// rather than fail the caller if a backend returns an error
    /// during teardown, because the process is normally exiting.
    pub async fn shutdown(self) -> ClusterResult<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        if let Err(e) = self.data_backend.close().await {
            warn!(range_id = self.descriptor.range_id, error = %e, "range data backend close returned error");
        }
        if let Err(e) = self.log_backend.close().await {
            warn!(range_id = self.descriptor.range_id, error = %e, "range log backend close returned error");
        }
        Ok(())
    }
}

/// Check that a path exists and is a directory. Exposed so tests and
/// the future PD supervisor can assert layout invariants without
/// duplicating the predicate.
pub fn is_range_dir(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    path.exists() && path.is_dir()
}

/// Error returned by [`RangeDirectory::insert`] when a range or group
/// id is already registered.
#[derive(Debug, thiserror::Error)]
pub enum RangeDirectoryError {
    /// A range with the same `range_id` is already registered.
    #[error("range id {0} is already registered")]
    DuplicateRangeId(RangeId),

    /// A range with the same `raft_group_id` is already registered
    /// (by a different range id). `raft_group_id` is allowed to
    /// diverge from `range_id` in the descriptor schema, but it must
    /// still be unique on this node — otherwise inbound RPCs can't be
    /// routed unambiguously.
    #[error("raft_group_id {0} is already registered")]
    DuplicateGroupId(GroupId),
}

/// Directory of [`RangeRuntime`]s running on a single node.
///
/// Implements [`aresadb_net::RaftDirectory`] so the Phase 2c-1 gRPC
/// server fans inbound RPCs out to the correct Raft group using just
/// the wire-level `raft_group_id` envelope — no per-range listener,
/// no extra process state.
///
/// Lookups are dual-indexed:
/// * `get_range(range_id)` — admin path, e.g. the cluster admin
///   RPCs addressed by `range_id`.
/// * `get_group(raft_group_id)` / [`RaftDirectory::raft_for`] — the
///   hot data-plane path; every inbound `AppendEntries` / `Vote` /
///   `InstallSnapshot` goes through it.
///
/// Both indexes point at the same `Arc<RangeRuntime>`, so lookups are
/// a single hash probe plus an `Arc` clone. Insertion is guarded —
/// duplicate ids can't sneak in — because a duplicate would silently
/// drop wire traffic into the wrong group.
pub struct RangeDirectory {
    inner: RwLock<RangeDirectoryInner>,
}

#[derive(Default)]
struct RangeDirectoryInner {
    by_range: HashMap<RangeId, Arc<RangeRuntime>>,
    by_group: HashMap<GroupId, Arc<RangeRuntime>>,
}

impl RangeDirectory {
    /// Allocate an empty directory wrapped in `Arc` so it can be
    /// shared with the gRPC server and background tasks without
    /// another layer of indirection. The common construction pattern
    /// is `let dir = RangeDirectory::new(); dir.insert(runtime)?;`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(RangeDirectoryInner::default()),
        })
    }

    /// Register a running range in both indexes. Fails if the
    /// `range_id` or `raft_group_id` collides with an already-
    /// registered runtime.
    pub fn insert(&self, runtime: Arc<RangeRuntime>) -> Result<(), RangeDirectoryError> {
        let range_id = runtime.descriptor().range_id;
        let group_id = runtime.descriptor().raft_group_id;

        let mut inner = self.inner.write();
        if inner.by_range.contains_key(&range_id) {
            return Err(RangeDirectoryError::DuplicateRangeId(range_id));
        }
        if inner.by_group.contains_key(&group_id) {
            return Err(RangeDirectoryError::DuplicateGroupId(group_id));
        }

        inner.by_range.insert(range_id, runtime.clone());
        inner.by_group.insert(group_id, runtime);
        Ok(())
    }

    /// Remove a range by id. Returns the last `Arc<RangeRuntime>`
    /// known to the directory so the caller can `shutdown()` it. The
    /// directory does not shut down the runtime itself — Phase 2c-3
    /// leaves that decision to the admin RPC / PD supervisor.
    pub fn remove(&self, range_id: RangeId) -> Option<Arc<RangeRuntime>> {
        let mut inner = self.inner.write();
        let runtime = inner.by_range.remove(&range_id)?;
        inner.by_group.remove(&runtime.descriptor().raft_group_id);
        Some(runtime)
    }

    /// Admin lookup by range id.
    pub fn get_range(&self, range_id: RangeId) -> Option<Arc<RangeRuntime>> {
        self.inner.read().by_range.get(&range_id).cloned()
    }

    /// Dispatch lookup by `raft_group_id`.
    pub fn get_group(&self, raft_group_id: GroupId) -> Option<Arc<RangeRuntime>> {
        self.inner.read().by_group.get(&raft_group_id).cloned()
    }

    /// Count of registered ranges. Exposed for diagnostics and tests.
    pub fn len(&self) -> usize {
        self.inner.read().by_range.len()
    }

    /// Whether the directory has no registered ranges.
    pub fn is_empty(&self) -> bool {
        self.inner.read().by_range.is_empty()
    }

    /// Snapshot of every registered `RangeDescriptor`. Useful for the
    /// `ListRanges` admin RPC and for the Phase 2c-4 PD supervisor's
    /// reconciliation loop. Cheap in practice (one descriptor clone
    /// per range, one short-lived read-lock).
    pub fn descriptors(&self) -> Vec<RangeDescriptor> {
        self.inner
            .read()
            .by_range
            .values()
            .map(|rt| rt.descriptor().clone())
            .collect()
    }

    /// Drain every runtime out of the directory and return them to
    /// the caller. The directory is empty afterwards. Used on process
    /// shutdown so each runtime can be consumed by its own
    /// `RangeRuntime::shutdown(self)` call.
    pub fn drain(&self) -> Vec<Arc<RangeRuntime>> {
        let mut inner = self.inner.write();
        let drained: Vec<_> = inner.by_range.drain().map(|(_, rt)| rt).collect();
        inner.by_group.clear();
        drained
    }
}

impl RaftDirectory for RangeDirectory {
    fn raft_for(&self, raft_group_id: u64) -> Option<Raft<TypeConfig>> {
        self.get_group(raft_group_id).map(|rt| rt.raft.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aresadb_core::WriteBatch;
    use aresadb_pd::types::{RangeDescriptor, ReplicaPlacement};
    use aresadb_raft::network::LoopbackNetwork;
    use aresadb_raft::AresaCommand;
    use openraft::Config;
    use tempfile::TempDir;

    use super::*;
    use crate::error::ReadError;

    fn voter_descriptor(range_id: u64, node_id: NodeId) -> RangeDescriptor {
        RangeDescriptor::new(
            range_id,
            b"".to_vec(),
            b"".to_vec(),
            vec![ReplicaPlacement::voter(node_id, 1)],
        )
    }

    fn test_raft_config() -> Arc<Config> {
        Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                cluster_name: "aresadb-range-runtime-test".to_string(),
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
    }

    fn node_cfg(dir: &TempDir) -> NodeConfig {
        NodeConfig::new(1, "127.0.0.1:0".parse().unwrap(), dir.path())
    }

    #[tokio::test]
    async fn open_on_disk_creates_range_layout() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let descriptor = voter_descriptor(7, 1);

        let runtime = RangeRuntime::open_on_disk(
            descriptor.clone(),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .expect("open range runtime");

        assert!(is_range_dir(cfg.range_log_dir(7)));
        assert!(is_range_dir(cfg.range_data_dir(7)));
        assert!(cfg.range_log_path(7).exists(), "log redb file created");
        assert!(cfg.range_data_path(7).exists(), "data redb file created");
        assert_eq!(runtime.descriptor().range_id, 7);
        assert_eq!(runtime.node_id(), 1);

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn bootstrap_voter_makes_node_leader_and_accepts_writes() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(1, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();

        runtime.bootstrap_voter().await.expect("bootstrap voter");

        let mut batch = WriteBatch::new();
        batch.put(b"key".to_vec(), b"value".to_vec());
        runtime
            .raft()
            .client_write(AresaCommand::batch(batch))
            .await
            .expect("replicated write");

        assert_eq!(
            &runtime.data_backend().get(b"key").await.unwrap().unwrap()[..],
            b"value"
        );

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reopen_rehydrates_applied_writes() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let descriptor = voter_descriptor(3, 1);

        {
            let runtime = RangeRuntime::open_on_disk(
                descriptor.clone(),
                1,
                &cfg,
                LoopbackNetwork,
                test_raft_config(),
            )
            .await
            .unwrap();
            runtime.bootstrap_voter().await.unwrap();

            let mut batch = WriteBatch::new();
            batch.put(b"durable".to_vec(), b"yes".to_vec());
            runtime
                .raft()
                .client_write(AresaCommand::batch(batch))
                .await
                .unwrap();

            runtime.shutdown().await.unwrap();
        }

        let runtime =
            RangeRuntime::open_on_disk(descriptor, 1, &cfg, LoopbackNetwork, test_raft_config())
                .await
                .expect("reopen range runtime");

        // Recovery replays the log before returning — `bootstrap_voter`
        // on the already-initialised range just waits for an election.
        runtime.bootstrap_voter().await.unwrap();

        assert_eq!(
            &runtime
                .data_backend()
                .get(b"durable")
                .await
                .unwrap()
                .unwrap()[..],
            b"yes",
            "rehydrated value survives shutdown → reopen"
        );

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn two_runtimes_with_different_range_ids_do_not_collide() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);

        let runtime_a = RangeRuntime::open_on_disk(
            voter_descriptor(10, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime_a.bootstrap_voter().await.unwrap();

        let runtime_b = RangeRuntime::open_on_disk(
            voter_descriptor(20, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime_b.bootstrap_voter().await.unwrap();

        let mut batch_a = WriteBatch::new();
        batch_a.put(b"shared".to_vec(), b"from-a".to_vec());
        runtime_a
            .raft()
            .client_write(AresaCommand::batch(batch_a))
            .await
            .unwrap();

        let mut batch_b = WriteBatch::new();
        batch_b.put(b"shared".to_vec(), b"from-b".to_vec());
        runtime_b
            .raft()
            .client_write(AresaCommand::batch(batch_b))
            .await
            .unwrap();

        assert_eq!(
            &runtime_a
                .data_backend()
                .get(b"shared")
                .await
                .unwrap()
                .unwrap()[..],
            b"from-a",
            "range 10's backend preserves its own value"
        );
        assert_eq!(
            &runtime_b
                .data_backend()
                .get(b"shared")
                .await
                .unwrap()
                .unwrap()[..],
            b"from-b",
            "range 20's backend preserves its own value"
        );

        assert_ne!(cfg.range_data_path(10), cfg.range_data_path(20));
        assert_ne!(cfg.range_log_path(10), cfg.range_log_path(20));

        runtime_a.shutdown().await.unwrap();
        runtime_b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn trigger_snapshot_runs_without_error() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(5, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime.bootstrap_voter().await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"a".to_vec(), b"b".to_vec());
        runtime
            .raft()
            .client_write(AresaCommand::batch(batch))
            .await
            .unwrap();

        runtime
            .trigger_snapshot()
            .await
            .expect("snapshot trigger queued");

        runtime.shutdown().await.unwrap();
    }

    async fn build_runtime(
        cfg: &NodeConfig,
        descriptor: RangeDescriptor,
        node_id: NodeId,
    ) -> Arc<RangeRuntime> {
        let runtime = RangeRuntime::open_on_disk(
            descriptor,
            node_id,
            cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime.bootstrap_voter().await.unwrap();
        Arc::new(runtime)
    }

    #[tokio::test]
    async fn directory_insert_get_remove_roundtrip() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let directory = RangeDirectory::new();
        assert!(directory.is_empty());

        let runtime = build_runtime(&cfg, voter_descriptor(1, 1), 1).await;
        directory.insert(runtime.clone()).unwrap();
        assert_eq!(directory.len(), 1);
        assert!(!directory.is_empty());

        let by_range = directory.get_range(1).expect("lookup by range id");
        assert_eq!(by_range.descriptor().range_id, 1);

        let by_group = directory.get_group(1).expect("lookup by raft_group_id");
        assert_eq!(by_group.descriptor().raft_group_id, 1);

        assert!(directory.get_range(999).is_none());
        assert!(directory.get_group(999).is_none());

        // Remove by range id also drops the group index entry.
        let removed = directory.remove(1).unwrap();
        assert_eq!(removed.descriptor().range_id, 1);
        assert!(directory.is_empty());
        assert!(directory.get_group(1).is_none());

        removed.raft().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn directory_rejects_duplicate_range_or_group_id() {
        // Each runtime needs its own data dir because redb takes an
        // exclusive file lock per database. The collision we're
        // testing here is logical (same range_id or same
        // raft_group_id in the directory), not physical.
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let dir_c = TempDir::new().unwrap();
        let dir_d = TempDir::new().unwrap();
        let cfg_a = node_cfg(&dir_a);
        let cfg_b = node_cfg(&dir_b);
        let cfg_c = node_cfg(&dir_c);
        let cfg_d = node_cfg(&dir_d);
        let directory = RangeDirectory::new();

        let original = build_runtime(&cfg_a, voter_descriptor(3, 1), 1).await;
        directory.insert(original).unwrap();

        // Same range_id + same group_id — DuplicateRangeId wins
        // (checked first).
        let dupe_same_ids = Arc::new(
            build_runtime_for_descriptor(
                &cfg_b,
                RangeDescriptor::new(3, b"".to_vec(), b"".to_vec(), vec![]),
                1,
                "3-same",
            )
            .await,
        );
        let err = directory
            .insert(dupe_same_ids)
            .expect_err("duplicate range id must reject");
        assert!(matches!(err, RangeDirectoryError::DuplicateRangeId(3)));

        // Different range_id but same raft_group_id as the original.
        let dupe_same_group = {
            let mut desc = RangeDescriptor::new(4, b"a".to_vec(), b"m".to_vec(), vec![]);
            desc.raft_group_id = 3;
            Arc::new(build_runtime_for_descriptor(&cfg_c, desc, 1, "4-group-3").await)
        };
        let err = directory
            .insert(dupe_same_group)
            .expect_err("duplicate group id must reject");
        assert!(matches!(err, RangeDirectoryError::DuplicateGroupId(3)));

        // Different range_id AND different group_id — accepted.
        let distinct = {
            let mut desc = RangeDescriptor::new(5, b"m".to_vec(), b"z".to_vec(), vec![]);
            desc.raft_group_id = 50;
            Arc::new(build_runtime_for_descriptor(&cfg_d, desc, 1, "5-group-50").await)
        };
        directory.insert(distinct).unwrap();

        assert_eq!(directory.len(), 2);

        for runtime in directory.drain() {
            drop(runtime);
        }
    }

    async fn build_runtime_for_descriptor(
        cfg: &NodeConfig,
        mut desc: RangeDescriptor,
        node_id: NodeId,
        tag: &str,
    ) -> RangeRuntime {
        // Give every range its own raft cluster_name so openraft
        // doesn't complain about duplicate names inside one process.
        let mut config = (*test_raft_config()).clone();
        config.cluster_name = format!("aresadb-range-test-{tag}");
        let config = Arc::new(config.validate().unwrap());
        // For the collision tests we don't need the runtime to be
        // initialised or writable — we just need it present in the
        // directory. Skip bootstrap_voter.
        desc.replicas
            .push(aresadb_pd::types::ReplicaPlacement::voter(node_id, 1));
        RangeRuntime::open_on_disk(desc, node_id, cfg, LoopbackNetwork, config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn directory_descriptors_snapshot_is_stable() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let directory = RangeDirectory::new();

        for range_id in [10u64, 20, 30] {
            let runtime = build_runtime(&cfg, voter_descriptor(range_id, 1), 1).await;
            directory.insert(runtime).unwrap();
        }

        let mut descriptors = directory.descriptors();
        descriptors.sort_by_key(|d| d.range_id);
        assert_eq!(descriptors.len(), 3);
        assert_eq!(
            descriptors.iter().map(|d| d.range_id).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );

        for runtime in directory.drain() {
            let rt = Arc::try_unwrap(runtime).expect("no outstanding references");
            rt.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn raft_directory_impl_routes_to_correct_group() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let directory = RangeDirectory::new();

        // Two ranges: (range_id=1, group_id=1) and (range_id=7, group_id=7).
        directory
            .insert(build_runtime(&cfg, voter_descriptor(1, 1), 1).await)
            .unwrap();
        directory
            .insert(build_runtime(&cfg, voter_descriptor(7, 1), 1).await)
            .unwrap();

        // Treat the directory as a RaftDirectory (the shape the gRPC
        // server sees it through).
        let as_raft_directory: &dyn RaftDirectory = directory.as_ref();

        assert!(
            as_raft_directory.raft_for(1).is_some(),
            "group 1 routes to range 1"
        );
        assert!(
            as_raft_directory.raft_for(7).is_some(),
            "group 7 routes to range 7"
        );
        assert!(
            as_raft_directory.raft_for(999).is_none(),
            "unregistered group returns None"
        );

        for runtime in directory.drain() {
            let rt = Arc::try_unwrap(runtime).expect("no outstanding references");
            rt.shutdown().await.unwrap();
        }
    }

    // ----- Phase 2c-5 — leadership + read path -----

    /// Before bootstrap, a freshly opened runtime is *not* a leader
    /// and reports a leaderless, empty metrics snapshot. This is
    /// the baseline: `leadership_status` must not panic on an
    /// uninitialised range.
    #[tokio::test]
    async fn leadership_status_before_bootstrap_reports_no_leader() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(11, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();

        let status = runtime.leadership_status();
        assert_eq!(status.range_id, 11);
        assert_eq!(status.node_id, 1);
        assert!(
            !status.is_leader,
            "never-initialised range must not claim leadership"
        );
        assert_eq!(status.current_leader, None);
        assert_eq!(status.voter_count, 0);
        // Apply lag is "unknown" (None) until the membership has
        // seen at least one log entry.
        assert_eq!(status.apply_lag(), None);

        runtime.shutdown().await.unwrap();
    }

    /// After bootstrap the single voter elects itself; the
    /// metrics snapshot must reflect that within a bounded number
    /// of heartbeats.
    #[tokio::test]
    async fn leadership_status_after_bootstrap_reports_leader() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(12, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime.bootstrap_voter().await.unwrap();

        // The openraft metrics channel lags by up to one heartbeat
        // interval; poll briefly instead of sleeping a fixed amount.
        let mut status = runtime.leadership_status();
        for _ in 0..50 {
            status = runtime.leadership_status();
            if status.is_leader && status.current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(status.is_leader, "single voter must elect itself");
        assert_eq!(status.current_leader, Some(1));
        assert_eq!(status.voter_count, 1);

        runtime.shutdown().await.unwrap();
    }

    /// After a replicated write, `linearizable_get` must return
    /// the value — this is the Phase 2c-5 happy path.
    #[tokio::test]
    async fn linearizable_get_returns_committed_value() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(13, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime.bootstrap_voter().await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"lin-key".to_vec(), b"lin-value".to_vec());
        runtime
            .raft()
            .client_write(AresaCommand::batch(batch))
            .await
            .unwrap();

        let value = runtime
            .linearizable_get(b"lin-key")
            .await
            .expect("linearizable read on leader must succeed");
        assert_eq!(value.as_deref(), Some(&b"lin-value"[..]));

        let missing = runtime
            .linearizable_get(b"does-not-exist")
            .await
            .expect("linearizable read must succeed even for absent keys");
        assert_eq!(missing, None);

        runtime.shutdown().await.unwrap();
    }

    /// `stale_get` must not require leadership and must mirror
    /// whatever the data backend has applied.
    #[tokio::test]
    async fn stale_get_reads_local_state_machine_without_guard() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(14, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();
        runtime.bootstrap_voter().await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"stale-key".to_vec(), b"stale-value".to_vec());
        runtime
            .raft()
            .client_write(AresaCommand::batch(batch))
            .await
            .unwrap();

        assert_eq!(
            runtime.stale_get(b"stale-key").await.unwrap().as_deref(),
            Some(&b"stale-value"[..])
        );
        assert_eq!(
            runtime.stale_get(b"absent").await.unwrap(),
            None,
            "absent key reads as None without error"
        );

        runtime.shutdown().await.unwrap();
    }

    /// Phase 2d — switching the data engine to `DataEngine::Lsm`
    /// routes through the fjall backend end-to-end. We commit a
    /// batch and then prove the value both round-trips through
    /// `stale_get` (state machine side) and survives a graceful
    /// shutdown → reopen, because fjall's fsync semantics after
    /// `OwnedWriteBatch::commit` + `persist(SyncAll)` must match
    /// redb's.
    #[tokio::test]
    async fn lsm_data_engine_persists_committed_writes_across_reopen() {
        let dir = TempDir::new().unwrap();
        let cfg = NodeConfig::new(1, "127.0.0.1:0".parse().unwrap(), dir.path())
            .with_data_engine(DataEngine::Lsm);
        let descriptor = voter_descriptor(31, 1);

        {
            let runtime = RangeRuntime::open_on_disk(
                descriptor.clone(),
                1,
                &cfg,
                LoopbackNetwork,
                test_raft_config(),
            )
            .await
            .expect("open range on LSM data engine");
            runtime.bootstrap_voter().await.unwrap();

            let mut batch = WriteBatch::new();
            batch.put(b"lsm-key".to_vec(), b"lsm-value".to_vec());
            runtime
                .raft()
                .client_write(AresaCommand::batch(batch))
                .await
                .unwrap();

            // Stale read hits the state-machine backend directly, so
            // if fjall is wired up correctly we see the value here.
            assert_eq!(
                runtime.stale_get(b"lsm-key").await.unwrap().as_deref(),
                Some(&b"lsm-value"[..]),
                "fjall-backed state machine serves stale reads",
            );

            runtime.shutdown().await.unwrap();
        }

        // Data layout check: the LSM suffix points at a directory
        // fjall manages, not a `.redb` file.
        assert_eq!(
            cfg.range_data_path(31).file_name().and_then(|s| s.to_str()),
            Some("data.lsm"),
        );
        assert!(cfg.range_data_path(31).is_dir());

        let reopened =
            RangeRuntime::open_on_disk(descriptor, 1, &cfg, LoopbackNetwork, test_raft_config())
                .await
                .expect("reopen range on LSM data engine");
        reopened.bootstrap_voter().await.unwrap();

        assert_eq!(
            reopened.stale_get(b"lsm-key").await.unwrap().as_deref(),
            Some(&b"lsm-value"[..]),
            "fjall-backed writes survive graceful shutdown + reopen",
        );

        reopened.shutdown().await.unwrap();
    }

    /// An uninitialised range is not a leader, so `ensure_linearizable`
    /// must fail with `ReadError::NotLeader`. This also exercises the
    /// `From<RaftError<_, CheckIsLeaderError>>` conversion.
    #[tokio::test]
    async fn ensure_linearizable_returns_not_leader_when_uninitialised() {
        let dir = TempDir::new().unwrap();
        let cfg = node_cfg(&dir);
        let runtime = RangeRuntime::open_on_disk(
            voter_descriptor(15, 1),
            1,
            &cfg,
            LoopbackNetwork,
            test_raft_config(),
        )
        .await
        .unwrap();

        let err = runtime
            .ensure_linearizable()
            .await
            .expect_err("uninitialised range cannot serve linearizable reads");
        // With loopback + no initialisation, openraft returns
        // ForwardToLeader with a `None` leader hint (nobody is
        // elected yet). Accept either variant — if a leader hint
        // is somehow present we still want the test to pass.
        match err {
            ReadError::NotLeader(_) => {}
            other => panic!("expected NotLeader, got {other:?}"),
        }

        runtime.shutdown().await.unwrap();
    }
}
