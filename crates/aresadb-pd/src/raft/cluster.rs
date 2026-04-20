//! Multi-node placement-driver Raft cluster harness.
//!
//! [`PdCluster`] brings up N [`SinglePdNode`]-grade members that all
//! share a single [`PdRouter`], so every RPC openraft issues is a
//! direct call against the target's in-process [`openraft::Raft`]
//! handle. No serialization, no tokio sockets — just a `HashMap`
//! lookup, an `.await`, and a return value.
//!
//! It's the harness every Phase 2b-3 integration test builds on:
//!
//! - **Elections** — spin up three voters, assert exactly one leader
//!   emerges, kill the leader, assert a new one takes over.
//! - **Splits & lease churn** — drive the catalog through hundreds of
//!   `PdCommand`s, then verify every follower's [`PdStateMachine`]
//!   converges to the same range / node table.
//! - **Snapshot + install_snapshot** — generate enough log entries
//!   that a lagging follower catches up via
//!   [`RaftStateMachine::install_snapshot`] rather than log replay.
//! - **Process restart** — shut a member down, drop and re-open its
//!   Raft handle against the same backends, watch it rejoin.
//!
//! The harness is deliberately synchronous-feeling from the outside:
//! callers call [`PdCluster::apply`] and the cluster figures out
//! which member is leader, forwards the command, and waits for
//! apply. It mirrors the [`SinglePdNode`] surface area so tests that
//! graduate from single-node to multi-node only swap the constructor.
//!
//! # Bootstrap flow
//!
//! openraft wants exactly one node to call
//! [`Raft::initialize`](openraft::Raft::initialize) on a fresh
//! cluster — the first member's `initialize` embeds the initial
//! membership in a log entry and replicates it to the rest. The
//! harness follows that rule:
//!
//! 1. Build every member's log store, state machine, and Raft handle
//!    — but **don't** call `initialize` yet. Register each with the
//!    router as soon as the handle exists so vote/append RPCs have
//!    a target.
//! 2. Once every member is up, call `initialize(all_members)` on the
//!    lowest-numbered node. Openraft replicates the membership entry
//!    everywhere, triggers an election, and the test can start
//!    applying commands.
//!
//! On restart ([`PdCluster::restart`]) we skip step 2: the member's
//! persisted log already contains a membership entry, so openraft
//! boots straight into the running cluster.
//!
//! [`SinglePdNode`]: super::single_node::SinglePdNode

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aresadb_core::{MemoryBackend, StorageBackend};
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use openraft::{BasicNode, Config, Raft};

use crate::command::{PdCommand, PdResponse};
use crate::state_machine::PdStateMachine;

use super::config::{typ, NodeId, PdTypeConfig};
use super::router::{PdRouter, PdRouterNetwork};
use super::state_machine::PdRaftStateMachine;
use super::PdLogStore;

/// `(node_id, log backend, data backend)` triple passed to the
/// multi-member constructors. Factored out as a type alias so the
/// function signatures stay readable (otherwise clippy trips on
/// `type_complexity`).
pub type MemberBackends = (NodeId, Arc<dyn StorageBackend>, Arc<dyn StorageBackend>);

/// A single member of a [`PdCluster`].
///
/// Owns its own log and data backends plus the Raft handle. The
/// backends outlive every individual Raft handle — they're the ground
/// truth across restarts. Reopening a node just drops `raft`,
/// `raft_state_machine`, and `state_machine`, then rebuilds them
/// pointing at the same backends.
pub struct PdClusterMember {
    /// This member's node id.
    pub node_id: NodeId,

    /// Cloneable Raft handle. Replaced on [`PdCluster::restart`].
    pub raft: typ::Raft,

    /// Persistent catalog state machine. Rehydrates from `data_backend`
    /// on every open, so restart preserves the catalog exactly.
    pub state_machine: Arc<PdStateMachine>,

    /// Adapter wrapping `state_machine` with Raft metadata accounting.
    pub raft_state_machine: Arc<PdRaftStateMachine>,

    /// Log-side backend. Stable across restarts so openraft finds its
    /// previously-persisted entries on reboot.
    pub log_backend: Arc<dyn StorageBackend>,

    /// Data-side backend. Stable across restarts so the catalog and
    /// PD Raft metadata rehydrate intact.
    pub data_backend: Arc<dyn StorageBackend>,
}

impl PdClusterMember {
    /// Open a member on the supplied backends and register it with the
    /// router. **Does not** call [`Raft::initialize`] — that's the
    /// [`PdCluster`] bootstrap's job.
    pub async fn open(
        node_id: NodeId,
        log_backend: Arc<dyn StorageBackend>,
        data_backend: Arc<dyn StorageBackend>,
        router: Arc<PdRouter>,
        config: Arc<Config>,
    ) -> anyhow::Result<Self> {
        let log = PdLogStore::new(log_backend.clone());
        let state_machine = PdStateMachine::open(data_backend.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        let raft_state_machine = PdRaftStateMachine::open(state_machine.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let network = PdRouterNetwork::new(node_id, router.clone());
        let raft =
            Raft::<PdTypeConfig>::new(node_id, config, network, log, raft_state_machine.clone())
                .await?;

        // Register the new handle *before* returning so any in-flight
        // RPCs from peers (e.g. a leader replicating membership the
        // moment we come back up) can reach us.
        router.register(node_id, raft.clone());

        Ok(Self {
            node_id,
            raft,
            state_machine,
            raft_state_machine,
            log_backend,
            data_backend,
        })
    }

    /// Gracefully shut down the member's Raft handle and drop it from
    /// the router. Backends are intentionally *not* closed — they may
    /// outlive the member (see [`PdCluster::restart`]).
    pub async fn shutdown(self, router: &PdRouter) -> anyhow::Result<()> {
        router.unregister(self.node_id);
        self.raft.shutdown().await?;
        Ok(())
    }
}

/// N-node in-process PD Raft cluster.
///
/// Every member shares a single [`PdRouter`] so RPCs route by node id
/// without touching the network. Each member gets its own independent
/// pair of backends.
pub struct PdCluster {
    /// Shared routing table. Cloneable; passed to every member on
    /// open and used by partition tests ([`Self::isolate`] /
    /// [`Self::reconnect`]).
    pub router: Arc<PdRouter>,

    config: Arc<Config>,
    members: BTreeMap<NodeId, PdClusterMember>,
}

impl PdCluster {
    /// Default openraft config for cluster harnesses. Fast enough for
    /// tests but still gives spurious-election-free elections on a
    /// busy CI box.
    pub fn default_config() -> Config {
        Config {
            heartbeat_interval: 150,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            cluster_name: "aresadb-pd".to_string(),
            // Let integration tests trigger snapshots manually via
            // `raft.trigger().snapshot()`. The default
            // `SnapshotPolicy::LogsSinceLast(5000)` is fine for
            // production but bigger than we want for multi-node
            // snapshot tests.
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
    }

    /// Spin up `size` PD Raft members on fresh in-memory backends.
    ///
    /// Members get node ids `1..=size as NodeId`. The lowest-numbered
    /// node bootstraps the cluster; every other node joins as a
    /// voter via the initial membership entry.
    pub async fn in_memory(size: usize) -> anyhow::Result<Self> {
        Self::in_memory_with_config(size, Self::default_config()).await
    }

    /// Variant of [`Self::in_memory`] that lets the caller override
    /// openraft config (snapshot policy, timeouts, …). Used by
    /// integration tests that want to exercise snapshot install or
    /// abnormal election behaviour.
    pub async fn in_memory_with_config(size: usize, config: Config) -> anyhow::Result<Self> {
        assert!(size > 0, "cluster must have at least one member");
        let router = PdRouter::new();
        let mut backends = Vec::with_capacity(size);
        for _ in 0..size {
            let log: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
            let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
            backends.push((log, data));
        }
        let members: Vec<MemberBackends> = backends
            .into_iter()
            .enumerate()
            .map(|(i, (log, data))| ((i as NodeId) + 1, log, data))
            .collect();
        Self::with_config(members, router, config).await
    }

    /// Build a cluster from explicit `(node_id, log, data)` triples.
    /// Used by restart / persistence tests that want to control the
    /// backends directly. Asserts unique, non-zero node ids.
    pub async fn new(members: Vec<MemberBackends>, router: Arc<PdRouter>) -> anyhow::Result<Self> {
        Self::with_config(members, router, Self::default_config()).await
    }

    /// Open every member from pre-existing backends **without**
    /// calling [`Raft::initialize`]. Use this after a full-cluster
    /// shutdown: the persisted log already contains the original
    /// membership entry, so openraft will bring the cluster back up
    /// on its own as soon as enough members are reachable.
    ///
    /// It is a logic error to call this against fresh backends — the
    /// cluster would boot up with no membership entry and never
    /// elect a leader. Use [`Self::new`] / [`Self::with_config`] for
    /// the fresh-bootstrap case.
    pub async fn open_existing(
        members: Vec<MemberBackends>,
        router: Arc<PdRouter>,
        config: Config,
    ) -> anyhow::Result<Self> {
        Self::build(members, router, config, false).await
    }

    /// [`Self::new`] with an explicit openraft config.
    pub async fn with_config(
        members: Vec<MemberBackends>,
        router: Arc<PdRouter>,
        config: Config,
    ) -> anyhow::Result<Self> {
        Self::build(members, router, config, true).await
    }

    async fn build(
        members: Vec<MemberBackends>,
        router: Arc<PdRouter>,
        config: Config,
        initialize: bool,
    ) -> anyhow::Result<Self> {
        assert!(!members.is_empty(), "cluster must have at least one member");
        let mut seen = std::collections::HashSet::new();
        for (id, _, _) in &members {
            assert!(*id != 0, "NodeId 0 is reserved");
            assert!(seen.insert(*id), "duplicate NodeId {id}");
        }

        let config = Arc::new(config.validate()?);

        // 1) Open every member. `open` registers each with the router
        // so peer RPCs during initialize have a target.
        let mut opened = BTreeMap::new();
        for (id, log_backend, data_backend) in members {
            let member = PdClusterMember::open(
                id,
                log_backend,
                data_backend,
                router.clone(),
                config.clone(),
            )
            .await?;
            opened.insert(id, member);
        }

        if initialize {
            // Fresh bootstrap: openraft wants exactly one node to
            // `initialize` a brand-new cluster. Use the lowest id.
            let bootstrap_id = *opened.keys().next().expect("non-empty");
            let membership: BTreeMap<NodeId, BasicNode> =
                opened.keys().map(|id| (*id, BasicNode::new(""))).collect();
            opened
                .get(&bootstrap_id)
                .expect("bootstrap member")
                .raft
                .initialize(membership)
                .await?;
        }
        // On `open_existing` we skip initialize entirely — the
        // persisted log already contains a membership entry and
        // openraft will bring itself up from it.

        Ok(Self {
            router,
            config,
            members: opened,
        })
    }

    /// Stable-sorted list of member ids.
    pub fn ids(&self) -> Vec<NodeId> {
        self.members.keys().copied().collect()
    }

    /// How many members are currently attached to this cluster. Does
    /// *not* consult openraft's view of membership — it just counts
    /// the handles the harness is tracking.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// `true` if no members are attached.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Borrow a specific member by id, if it's attached.
    pub fn member(&self, node_id: NodeId) -> Option<&PdClusterMember> {
        self.members.get(&node_id)
    }

    /// Mutable handle to a specific member. Used by [`Self::restart`]
    /// and a handful of low-level tests that want direct access to
    /// the state machine.
    pub fn member_mut(&mut self, node_id: NodeId) -> Option<&mut PdClusterMember> {
        self.members.get_mut(&node_id)
    }

    /// Scan every member's metrics; return the first id whose
    /// `current_leader` is itself. If no member agrees, returns
    /// `None` — usually because an election is in flight.
    pub fn leader(&self) -> Option<NodeId> {
        for (id, member) in &self.members {
            let metrics = member.raft.metrics().borrow().clone();
            if metrics.current_leader == Some(*id) {
                return Some(*id);
            }
        }
        None
    }

    /// Block until some member reports itself as leader, or `timeout`
    /// elapses. Poll interval is 10ms which is fast enough for tests
    /// and cheap enough on CI.
    pub async fn wait_for_leader(&self, timeout: Duration) -> anyhow::Result<NodeId> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(id) = self.leader() {
                return Ok(id);
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "no leader elected within {:?} (members: {:?}, router: {:?})",
                    timeout,
                    self.ids(),
                    self.router
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Replicate a command through the cluster and return the
    /// state-machine response.
    ///
    /// The call targets the last-known leader. If openraft forwards us
    /// because leadership has since moved, we retry against the new
    /// leader up to `MAX_HOPS` times. Any other Raft error fails the
    /// call.
    pub async fn apply(&self, cmd: PdCommand) -> anyhow::Result<PdResponse> {
        const MAX_HOPS: usize = 5;
        // First attempt: whoever we currently think is leader. If the
        // harness is fresh we may need to wait for election.
        let mut target = match self.leader() {
            Some(id) => id,
            None => self.wait_for_leader(Duration::from_secs(2)).await?,
        };

        for _ in 0..MAX_HOPS {
            let member = self.member(target).ok_or_else(|| {
                anyhow::anyhow!("apply: target {target} is not attached to this cluster")
            })?;
            match member.raft.client_write(cmd.clone()).await {
                Ok(resp) => return Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                    leader_id: Some(new_leader),
                    ..
                }))) => {
                    target = new_leader;
                    continue;
                }
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                    // Leader is currently unknown — wait and retry.
                    target = self.wait_for_leader(Duration::from_secs(2)).await?;
                    continue;
                }
                Err(other) => return Err(anyhow::anyhow!(other)),
            }
        }
        anyhow::bail!(
            "apply: exceeded {MAX_HOPS} leader hops — cluster appears unstable (members: {:?})",
            self.ids()
        )
    }

    /// Administratively drop the directed link `from -> to`. See
    /// [`PdRouter::isolate`] for the semantic detail. The caller is
    /// responsible for restoring it with [`Self::reconnect`].
    pub fn isolate(&self, from: NodeId, to: NodeId) {
        self.router.isolate(from, to);
    }

    /// Restore a previously-dropped link.
    pub fn reconnect(&self, from: NodeId, to: NodeId) {
        self.router.reconnect(from, to);
    }

    /// Symmetric partition: drop both `a -> b` and `b -> a`.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.router.isolate(a, b);
        self.router.isolate(b, a);
    }

    /// Heal a symmetric partition set by [`Self::partition`].
    pub fn heal(&self, a: NodeId, b: NodeId) {
        self.router.reconnect(a, b);
        self.router.reconnect(b, a);
    }

    /// Shut down the member with id `node_id` and reopen it on the
    /// same backends. Simulates a clean process restart: the Raft
    /// task stops, the state machine rehydrates from the on-disk
    /// catalog, the log store picks up where it left off.
    ///
    /// Returns `Ok(false)` if `node_id` isn't attached.
    pub async fn restart(&mut self, node_id: NodeId) -> anyhow::Result<bool> {
        let Some(old) = self.members.remove(&node_id) else {
            return Ok(false);
        };
        let log = old.log_backend.clone();
        let data = old.data_backend.clone();
        let router = self.router.clone();
        let config = self.config.clone();

        old.shutdown(&self.router).await?;

        let restarted = PdClusterMember::open(node_id, log, data, router, config).await?;
        self.members.insert(node_id, restarted);
        Ok(true)
    }

    /// Shut down every member.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        for (_, member) in self.members {
            member.shutdown(&self.router).await?;
        }
        Ok(())
    }

    /// Collect a per-member snapshot of `(last_applied_index,
    /// range_count)` — useful for asserting followers converge.
    pub fn catalog_snapshot(&self) -> HashMap<NodeId, (Option<u64>, usize)> {
        let mut out = HashMap::new();
        for (id, member) in &self.members {
            let metrics = member.raft.metrics().borrow().clone();
            let applied_index = metrics.last_applied.map(|li| li.index);
            let range_count = member.state_machine.read(|c| c.range_count());
            out.insert(*id, (applied_index, range_count));
        }
        out
    }

    /// Wait until every member reports the same `range_count`. Useful
    /// after a burst of [`Self::apply`]s to let followers catch up.
    pub async fn wait_for_replication(
        &self,
        expected_range_count: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.catalog_snapshot();
            if snapshot.values().all(|(_, n)| *n == expected_range_count) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "range_count did not converge to {expected_range_count} within {timeout:?}; saw {snapshot:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LeaseInfo, RangeDescriptor, ReplicaPlacement};

    fn voters(ids: &[NodeId]) -> Vec<ReplicaPlacement> {
        ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
    }

    fn genesis(members: &[NodeId]) -> RangeDescriptor {
        RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(members))
    }

    #[tokio::test]
    async fn three_node_cluster_elects_exactly_one_leader() {
        let cluster = PdCluster::in_memory(3).await.expect("bring up cluster");
        let leader = cluster
            .wait_for_leader(Duration::from_secs(3))
            .await
            .unwrap();
        assert!([1, 2, 3].contains(&leader));

        // Exactly one member should claim leadership.
        let claims: Vec<NodeId> = cluster
            .ids()
            .into_iter()
            .filter(|id| {
                let m = cluster.member(*id).unwrap();
                m.raft.metrics().borrow().current_leader == Some(*id)
            })
            .collect();
        assert_eq!(claims, vec![leader]);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn apply_replicates_to_all_members() {
        let cluster = PdCluster::in_memory(3).await.unwrap();
        cluster
            .wait_for_leader(Duration::from_secs(3))
            .await
            .unwrap();

        cluster
            .apply(PdCommand::CreateRange(genesis(&[1, 2, 3])))
            .await
            .unwrap();

        cluster
            .wait_for_replication(1, Duration::from_secs(2))
            .await
            .unwrap();

        for id in cluster.ids() {
            let m = cluster.member(id).unwrap();
            m.state_machine.read(|c| {
                assert_eq!(c.range_count(), 1);
                assert_eq!(c.get_range(1).unwrap().range_id, 1);
            });
        }

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn forward_to_leader_is_transparent_from_follower() {
        let cluster = PdCluster::in_memory(3).await.unwrap();
        let leader = cluster
            .wait_for_leader(Duration::from_secs(3))
            .await
            .unwrap();

        // Find a follower and call `client_write` on it directly. The
        // returned error should tell us where the leader is; `apply`
        // hides that, so we issue the raw call to make sure the
        // plumbing is sane.
        let follower = cluster.ids().into_iter().find(|id| *id != leader).unwrap();
        let follower_raft = cluster.member(follower).unwrap().raft.clone();
        let err = follower_raft
            .client_write(PdCommand::CreateRange(genesis(&[1, 2, 3])))
            .await
            .expect_err("follower must reject writes");
        match err {
            RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
                assert_eq!(f.leader_id, Some(leader));
            }
            other => panic!("expected ForwardToLeader, got {other:?}"),
        }

        // And the high-level `apply` succeeds regardless of where the
        // leader currently sits.
        cluster
            .apply(PdCommand::CreateRange(genesis(&[1, 2, 3])))
            .await
            .unwrap();
        cluster
            .wait_for_replication(1, Duration::from_secs(2))
            .await
            .unwrap();

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn split_and_lease_updates_converge_on_all_followers() {
        let cluster = PdCluster::in_memory(3).await.unwrap();
        cluster
            .wait_for_leader(Duration::from_secs(3))
            .await
            .unwrap();

        cluster
            .apply(PdCommand::CreateRange(genesis(&[1, 2, 3])))
            .await
            .unwrap();

        // Walk-right split chain: see the parallel test in
        // single_node.rs for the reasoning.
        let mut parent = 1u64;
        for key in [b"e" as &[u8], b"j", b"o"] {
            let resp = cluster
                .apply(PdCommand::SplitRange {
                    parent_range_id: parent,
                    split_key: key.to_vec(),
                })
                .await
                .unwrap();
            let rhs = match resp {
                PdResponse::Range(r) => r,
                other => panic!("expected Range, got {other:?}"),
            };
            parent = rhs.range_id;
        }

        cluster
            .apply(PdCommand::UpdateLease {
                range_id: parent,
                lease: Some(LeaseInfo {
                    holder: 2,
                    expires_at_millis: 1_800_000_000_000,
                }),
            })
            .await
            .unwrap();

        cluster
            .wait_for_replication(4, Duration::from_secs(2))
            .await
            .unwrap();

        for id in cluster.ids() {
            let m = cluster.member(id).unwrap();
            m.state_machine.read(|c| {
                assert_eq!(c.range_count(), 4);
                let last = c.get_range(parent).unwrap();
                assert_eq!(last.lease.as_ref().unwrap().holder, 2);
            });
        }

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn member_restart_rehydrates_catalog_from_backends() {
        let mut cluster = PdCluster::in_memory(3).await.unwrap();
        cluster
            .wait_for_leader(Duration::from_secs(3))
            .await
            .unwrap();

        cluster
            .apply(PdCommand::CreateRange(genesis(&[1, 2, 3])))
            .await
            .unwrap();
        cluster
            .wait_for_replication(1, Duration::from_secs(2))
            .await
            .unwrap();

        // Pick a follower to bounce — restarting the leader would
        // trigger a re-election and muddies the test's intent.
        let leader = cluster.leader().expect("have leader");
        let follower = cluster.ids().into_iter().find(|id| *id != leader).unwrap();

        cluster.restart(follower).await.unwrap();

        // The follower's in-memory catalog was thrown away when we
        // dropped its Raft handle; opening a fresh PdStateMachine
        // rehydrates it from `data_backend`.
        let m = cluster.member(follower).unwrap();
        m.state_machine.read(|c| {
            assert_eq!(c.range_count(), 1);
            assert_eq!(c.get_range(1).unwrap().range_id, 1);
        });

        // Cluster should still be healthy for more writes.
        cluster
            .apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
        cluster
            .wait_for_replication(2, Duration::from_secs(2))
            .await
            .unwrap();

        cluster.shutdown().await.unwrap();
    }
}
