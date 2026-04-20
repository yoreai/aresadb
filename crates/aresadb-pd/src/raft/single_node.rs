//! Single-member placement-driver Raft cluster.
//!
//! Mirrors [`aresadb_raft::SingleNode`] but for the PD Raft group:
//! bundles a [`PdLogStore`], a [`PdRaftStateMachine`], and a
//! [`PdRouterNetwork`] (with the node registered in its own router)
//! into a running [`openraft::Raft<PdTypeConfig>`] handle already
//! initialized as a one-voter cluster.
//!
//! Useful in two places:
//!
//! 1. **Tests**: drives every PD flow (create / split / merge /
//!    heartbeat) through real Raft without needing a multi-node
//!    harness.
//! 2. **Phase 2b-4 bootstrapping**: the cluster CLI's
//!    `aresadb-cluster init pd` subcommand can start a single-
//!    member PD group on the first node and add learners as more
//!    nodes join, reusing the exact same plumbing that powers
//!    multi-node tests.
//!
//! The single-node harness uses the in-process [`PdRouter`] with
//! only one entry (itself), so every RPC openraft issues routes to
//! the local handle. That's functionally equivalent to a loopback
//! factory but keeps us on one transport path, which simplifies the
//! Phase 2c story when the PD group grows past one member.

use std::collections::BTreeMap;
use std::sync::Arc;

use aresadb_core::{MemoryBackend, StorageBackend};
use openraft::{BasicNode, Config, Raft};

use crate::command::{PdCommand, PdResponse};
use crate::state_machine::PdStateMachine;

use super::config::{typ, NodeId, PdTypeConfig};
use super::router::{PdRouter, PdRouterNetwork};
use super::state_machine::PdRaftStateMachine;

/// A single-member PD Raft cluster ready for client writes.
pub struct SinglePdNode {
    /// This node's id. Defaults to `1` for the in-memory harness.
    pub node_id: NodeId,

    /// Handle to the Raft task. Cloneable; `shutdown()` it when done.
    pub raft: Raft<PdTypeConfig>,

    /// The router this node registered itself with. Kept around so
    /// tests can add more nodes to the same cluster without re-
    /// opening the harness (`PdCluster` builds on this).
    pub router: Arc<PdRouter>,

    /// The inner persistent catalog state machine. Reads go directly
    /// against its in-memory catalog via [`PdStateMachine::read`].
    pub state_machine: Arc<PdStateMachine>,

    /// The Raft state-machine adapter wrapping `state_machine`.
    pub raft_state_machine: Arc<PdRaftStateMachine>,

    /// The log-side backend. Distinct from the catalog backend so
    /// fsync-heavy log writes don't sit in the same tree as
    /// sorted-run-friendly catalog rows.
    pub log_backend: Arc<dyn StorageBackend>,

    /// The data backend — where the catalog rows and Raft meta live.
    pub data_backend: Arc<dyn StorageBackend>,
}

impl SinglePdNode {
    /// Spin up a single-node PD cluster on brand-new memory backends.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let log: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        Self::new(1, log, data, PdRouter::new()).await
    }

    /// Boot a single-node PD cluster on the provided backends and
    /// router. The node registers itself in `router` so callers that
    /// want to grow the cluster later can add more nodes pointing at
    /// the same routing table.
    ///
    /// Returns after `initialize` has resolved — i.e. the node is
    /// elected leader and the first membership entry has been
    /// applied. The caller can immediately issue [`Self::apply`].
    pub async fn new(
        node_id: NodeId,
        log_backend: Arc<dyn StorageBackend>,
        data_backend: Arc<dyn StorageBackend>,
        router: Arc<PdRouter>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 150,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                cluster_name: "aresadb-pd".to_string(),
                ..Default::default()
            }
            .validate()?,
        );

        let log = super::PdLogStore::new(log_backend.clone());
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

        // Register ourselves in the router *before* initialize so any
        // RPCs openraft issues as part of bootstrap (there generally
        // aren't any on a single-voter cluster, but be defensive)
        // find the local handle.
        router.register(node_id, raft.clone());

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(""));
        raft.initialize(members).await?;

        Ok(Self {
            node_id,
            raft,
            router,
            state_machine,
            raft_state_machine,
            log_backend,
            data_backend,
        })
    }

    /// Replicate a [`PdCommand`] through Raft and wait for the
    /// state machine to apply it.
    pub async fn apply(&self, cmd: PdCommand) -> anyhow::Result<PdResponse> {
        let resp = self.raft.client_write(cmd).await?;
        Ok(resp.data)
    }

    /// Borrow the current [`typ::Raft`] handle. Useful when a test
    /// wants to call openraft methods directly (e.g. `metrics`,
    /// `change_membership`) without going through the harness.
    pub fn raft(&self) -> &typ::Raft {
        &self.raft
    }

    /// Gracefully shut down the Raft task. Consumes `self` so you
    /// can't accidentally keep using the harness after shutdown.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.unregister(self.node_id);
        self.raft.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LeaseInfo, RangeDescriptor, ReplicaPlacement};

    fn voters(ids: &[NodeId]) -> Vec<ReplicaPlacement> {
        ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
    }

    fn genesis() -> RangeDescriptor {
        RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1]))
    }

    #[tokio::test]
    async fn single_pd_node_applies_create_range() {
        let node = SinglePdNode::in_memory().await.expect("start node");

        let resp = node.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        assert!(matches!(resp, PdResponse::Range(r) if r.range_id == 1));

        // Range also reachable from the in-memory catalog.
        node.state_machine.read(|c| {
            assert_eq!(c.range_count(), 1);
            assert_eq!(c.get_range(1).unwrap(), &genesis());
        });

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_pd_node_splits_and_updates_lease() {
        let node = SinglePdNode::in_memory().await.unwrap();

        node.apply(PdCommand::CreateRange(genesis())).await.unwrap();
        let rhs = node
            .apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .await
            .unwrap();
        let rhs = match rhs {
            PdResponse::Range(r) => r,
            other => panic!("expected Range response, got {other:?}"),
        };
        assert_eq!(rhs.range_id, 2);

        node.apply(PdCommand::UpdateLease {
            range_id: 2,
            lease: Some(LeaseInfo {
                holder: 1,
                expires_at_millis: 1_700_000_000_000,
            }),
        })
        .await
        .unwrap();

        node.state_machine.read(|c| {
            assert_eq!(c.range_count(), 2);
            let r2 = c.get_range(2).unwrap();
            assert_eq!(r2.lease.as_ref().unwrap().holder, 1);
        });

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_pd_node_rejects_invalid_command_via_error_response() {
        let node = SinglePdNode::in_memory().await.unwrap();

        // Splitting a nonexistent range — catalog-level rejection.
        let resp = node
            .apply(PdCommand::SplitRange {
                parent_range_id: 99,
                split_key: b"x".to_vec(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, PdResponse::Error(_)), "got {resp:?}");

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_pd_node_metrics_report_leadership() {
        let node = SinglePdNode::in_memory().await.unwrap();

        let _ = node
            .raft
            .wait(Some(std::time::Duration::from_secs(2)))
            .current_leader(node.node_id, "become leader")
            .await
            .unwrap();

        let metrics = node.raft.metrics().borrow().clone();
        assert_eq!(metrics.current_leader, Some(node.node_id));

        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_pd_node_snapshot_roundtrips_through_raft() {
        let node = SinglePdNode::in_memory().await.unwrap();

        node.apply(PdCommand::CreateRange(genesis())).await.unwrap();

        // Walk right: each iteration splits the right-hand range
        // produced by the previous step. That keeps every split key
        // inside the target range's span and ends with 5 ranges.
        let mut parent = 1u64;
        for key in [b"e" as &[u8], b"j", b"o", b"t"] {
            let resp = node
                .apply(PdCommand::SplitRange {
                    parent_range_id: parent,
                    split_key: key.to_vec(),
                })
                .await
                .unwrap();
            let rhs = match resp {
                PdResponse::Range(r) => r,
                other => panic!("expected Range response, got {other:?}"),
            };
            parent = rhs.range_id;
        }

        node.raft.trigger().snapshot().await.unwrap();

        // Wait for openraft to wire up the snapshot metadata.
        for _ in 0..50 {
            if node.raft.metrics().borrow().snapshot.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(node.raft.metrics().borrow().snapshot.is_some());

        // Catalog still responds correctly post-snapshot.
        node.state_machine.read(|c| {
            assert_eq!(c.range_count(), 5);
        });

        node.shutdown().await.unwrap();
    }
}
