//! Applies a [`ReconcilePlan`] against the node's
//! [`RangeDirectory`].
//!
//! The executor is intentionally chatty in its error surface: a
//! single reconcile pass can touch multiple ranges, and one failed
//! open shouldn't prevent the others from proceeding. Instead of
//! returning on first error, [`execute_plan`] collects all errors
//! into an [`ExecutorReport`] and hands them back to the caller.
//! The supervisor logs them at `warn` and retries on the next tick.

use std::sync::Arc;

use aresadb_net::{GrpcRaftNetwork, StaticPeerDirectory};
use aresadb_pd::types::{RangeDescriptor, RangeId};
use aresadb_raft::NodeId;
use openraft::Config;
use thiserror::Error;

use crate::config::NodeConfig;
use crate::error::ClusterError;
use crate::range::{RangeDirectory, RangeDirectoryError, RangeRuntime};

use super::reconciler::ReconcilePlan;

/// Per-range failure surfaced by [`execute_plan`].
#[derive(Debug, Error)]
pub enum ExecutorError {
    /// `RangeRuntime::open_on_disk` failed while trying to
    /// materialise a new range. Usually a disk / permissions /
    /// redb-lock problem.
    #[error("open range {range_id}: {source}")]
    OpenFailed {
        /// Range id the open was for.
        range_id: RangeId,
        /// Underlying cluster-level error.
        #[source]
        source: ClusterError,
    },

    /// `RangeDirectory::insert` rejected the new runtime because
    /// another reconcile (or a racing admin RPC) added the same id
    /// first. Usually benign — the next tick will observe the
    /// already-open state and skip the add.
    #[error("register range {range_id}: {source}")]
    InsertFailed {
        /// Range id the insert was for.
        range_id: RangeId,
        /// Underlying directory error.
        #[source]
        source: RangeDirectoryError,
    },

    /// `RangeRuntime::shutdown` returned an error. The runtime has
    /// already been removed from the directory at this point, so
    /// the plan still advanced; the error is reported so the
    /// supervisor can log it.
    #[error("shutdown range {range_id}: {source}")]
    ShutdownFailed {
        /// Range id the shutdown was for.
        range_id: RangeId,
        /// Underlying cluster-level error.
        #[source]
        source: ClusterError,
    },

    /// `Arc::try_unwrap` on the removed runtime failed because
    /// somebody else is still holding a reference. The supervisor
    /// still shuts the Raft handle down (best-effort) but the
    /// backends live on until the last `Arc` drops.
    #[error("range {range_id} still has outstanding references; skipped storage shutdown")]
    ForceShutdown {
        /// Range id affected.
        range_id: RangeId,
    },
}

/// Summary of what the executor did and which errors (if any) the
/// caller should log / retry. Always returned by value so callers
/// can match against specific actions in tests.
#[derive(Debug, Default)]
pub struct ExecutorReport {
    /// Ids of ranges successfully opened and inserted into the
    /// directory this pass.
    pub added: Vec<RangeId>,
    /// Ids of ranges successfully shut down and removed from the
    /// directory this pass.
    pub removed: Vec<RangeId>,
    /// Per-range errors encountered during this pass. The plan
    /// continues past them; the supervisor retries on the next
    /// tick.
    pub errors: Vec<ExecutorError>,
}

impl ExecutorReport {
    /// Convenience: `true` iff at least one action succeeded (add
    /// or remove). Used by the supervisor to decide whether to log
    /// the pass at `info` or `trace`.
    pub fn performed_work(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty()
    }
}

/// Apply `plan` against `range_directory`. Safe to call
/// concurrently with admin RPCs: each operation is a single
/// directory mutation and failures are collected rather than
/// propagated, so a parallel `AddRange` / `RemoveRange` from
/// another caller just shows up as a duplicate / not-found and
/// gets logged.
pub async fn execute_plan(
    plan: ReconcilePlan,
    node_id: NodeId,
    node_config: &NodeConfig,
    peer_directory: &Arc<StaticPeerDirectory>,
    range_directory: &Arc<RangeDirectory>,
) -> ExecutorReport {
    let mut report = ExecutorReport::default();

    for descriptor in plan.to_add {
        let range_id = descriptor.range_id;
        match open_and_register(
            &descriptor,
            node_id,
            node_config,
            peer_directory,
            range_directory,
        )
        .await
        {
            Ok(()) => report.added.push(range_id),
            Err(e) => report.errors.push(e),
        }
    }

    for range_id in plan.to_remove {
        match remove_and_shutdown(range_id, range_directory).await {
            Ok(()) => report.removed.push(range_id),
            Err(e) => report.errors.push(e),
        }
    }

    report
}

/// Open a new [`RangeRuntime`] for `descriptor` and register it in
/// the directory. Extracted into a helper so the happy path is
/// trivially readable and error mapping stays colocated.
async fn open_and_register(
    descriptor: &RangeDescriptor,
    node_id: NodeId,
    node_config: &NodeConfig,
    peer_directory: &Arc<StaticPeerDirectory>,
    range_directory: &Arc<RangeDirectory>,
) -> Result<(), ExecutorError> {
    let range_id = descriptor.range_id;
    let raft_group_id = descriptor.raft_group_id;

    // Same Raft config shape as the admin `AddRange` path and
    // `ClusterNode::start` use. Distinct `cluster_name` per range
    // keeps openraft's log-replication heuristics from conflating
    // groups inside one process — essential when the same node
    // runs many ranges.
    let raft_config = Config {
        heartbeat_interval: 150,
        election_timeout_min: 500,
        election_timeout_max: 1500,
        cluster_name: format!("{}-range-{}", node_config.cluster_name, range_id),
        ..Default::default()
    }
    .validate()
    .map_err(|e| ExecutorError::OpenFailed {
        range_id,
        source: ClusterError::Config(format!("invalid raft config: {e}")),
    })?;

    let network = GrpcRaftNetwork::new(peer_directory.clone(), raft_group_id);

    let runtime = RangeRuntime::open_on_disk(
        descriptor.clone(),
        node_id,
        node_config,
        network,
        Arc::new(raft_config),
    )
    .await
    .map_err(|e| ExecutorError::OpenFailed {
        range_id,
        source: e,
    })?;

    let runtime = Arc::new(runtime);
    range_directory
        .insert(runtime)
        .map_err(|e| ExecutorError::InsertFailed {
            range_id,
            source: e,
        })?;

    Ok(())
}

/// Remove `range_id` from the directory and shut down its runtime.
/// Mirrors the admin `RemoveRange` handler but skips the
/// `force=true` escape hatch — the supervisor fires on a timer, so
/// a transient reference (e.g. an in-flight RPC) will resolve on
/// the next tick.
async fn remove_and_shutdown(
    range_id: RangeId,
    range_directory: &Arc<RangeDirectory>,
) -> Result<(), ExecutorError> {
    let Some(runtime) = range_directory.remove(range_id) else {
        // Already gone — harmless race (another tick / admin RPC).
        return Ok(());
    };

    match Arc::try_unwrap(runtime) {
        Ok(rt) => rt
            .shutdown()
            .await
            .map_err(|e| ExecutorError::ShutdownFailed {
                range_id,
                source: e,
            }),
        Err(shared) => {
            // Best-effort: shut down the Raft portion so we stop
            // accepting RPCs for this group. Backends will drop
            // when the last `Arc` is released.
            let _ = shared.raft().clone().shutdown().await;
            Err(ExecutorError::ForceShutdown { range_id })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd_supervisor::reconciler::plan_reconcile;
    use crate::range::RangeDirectory;
    use aresadb_pd::types::{RangeDescriptor, ReplicaPlacement};
    use std::collections::BTreeSet;
    use std::net::SocketAddr;

    fn test_node_config(tmp: &tempfile::TempDir) -> NodeConfig {
        NodeConfig::new(1, "127.0.0.1:0".parse::<SocketAddr>().unwrap(), tmp.path())
            .with_cluster_name("test-cluster")
    }

    #[tokio::test]
    async fn execute_plan_opens_new_range_and_registers_it() {
        let tmp = tempfile::tempdir().unwrap();
        let node_config = test_node_config(&tmp);
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        peer_directory.upsert(1, "http://127.0.0.1:0".to_string());

        let descriptor = RangeDescriptor::new(
            42,
            b"a".to_vec(),
            b"m".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        );

        let plan = ReconcilePlan {
            to_add: vec![descriptor.clone()],
            to_remove: vec![],
        };

        let report = execute_plan(plan, 1, &node_config, &peer_directory, &range_directory).await;

        assert_eq!(report.added, vec![42]);
        assert!(report.removed.is_empty());
        assert!(
            report.errors.is_empty(),
            "unexpected errors: {:?}",
            report.errors
        );

        let live = range_directory
            .get_range(42)
            .expect("range 42 should be registered");
        assert_eq!(live.descriptor().range_id, 42);

        // Clean up: shut down so the test temp dir can be removed.
        drop(range_directory);
    }

    #[tokio::test]
    async fn execute_plan_removes_registered_range() {
        let tmp = tempfile::tempdir().unwrap();
        let node_config = test_node_config(&tmp);
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        peer_directory.upsert(1, "http://127.0.0.1:0".to_string());

        let descriptor = RangeDescriptor::new(
            77,
            b"x".to_vec(),
            b"z".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        );
        let plan_add = ReconcilePlan {
            to_add: vec![descriptor],
            to_remove: vec![],
        };
        let r1 = execute_plan(plan_add, 1, &node_config, &peer_directory, &range_directory).await;
        assert!(r1.errors.is_empty());
        assert!(range_directory.get_range(77).is_some());

        let plan_remove = ReconcilePlan {
            to_add: vec![],
            to_remove: vec![77],
        };
        let r2 = execute_plan(
            plan_remove,
            1,
            &node_config,
            &peer_directory,
            &range_directory,
        )
        .await;
        assert_eq!(r2.removed, vec![77]);
        assert!(r2.errors.is_empty(), "unexpected errors: {:?}", r2.errors);
        assert!(range_directory.get_range(77).is_none());
    }

    #[tokio::test]
    async fn execute_plan_idempotent_on_unknown_remove() {
        // Removing a range that isn't registered is a no-op, not
        // an error. The supervisor may observe "PD says this
        // range is gone" for several ticks before the directory
        // actually clears it, so double-removes are normal.
        let tmp = tempfile::tempdir().unwrap();
        let node_config = test_node_config(&tmp);
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());

        let plan = ReconcilePlan {
            to_add: vec![],
            to_remove: vec![9999],
        };
        let report = execute_plan(plan, 1, &node_config, &peer_directory, &range_directory).await;
        assert_eq!(report.removed, vec![9999]);
        assert!(report.errors.is_empty());
    }

    #[tokio::test]
    async fn execute_plan_reports_errors_per_range_and_keeps_going() {
        // Simulate a race where the same id is "added" twice: the
        // second attempt should fail with `InsertFailed` but the
        // executor should still process the rest of the plan.
        let tmp = tempfile::tempdir().unwrap();
        let node_config = test_node_config(&tmp);
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        peer_directory.upsert(1, "http://127.0.0.1:0".to_string());

        // Pre-populate range 5 so the second add collides.
        let pre = RangeDescriptor::new(
            5,
            b"a".to_vec(),
            b"b".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        );
        execute_plan(
            ReconcilePlan {
                to_add: vec![pre],
                to_remove: vec![],
            },
            1,
            &node_config,
            &peer_directory,
            &range_directory,
        )
        .await;

        // Now ask the executor to add 5 again (collision) and 6
        // (fresh). The colliding add fails on
        // `RangeRuntime::open_on_disk` because redb's file lock
        // is already held; the fresh add succeeds.
        let new_a = RangeDescriptor::new(
            5,
            b"a".to_vec(),
            b"b".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        );
        let new_b = RangeDescriptor::new(
            6,
            b"c".to_vec(),
            b"d".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        );
        let report = execute_plan(
            ReconcilePlan {
                to_add: vec![new_a, new_b],
                to_remove: vec![],
            },
            1,
            &node_config,
            &peer_directory,
            &range_directory,
        )
        .await;

        assert!(
            report.added.contains(&6),
            "range 6 should have been added despite the collision on 5"
        );
        assert!(
            !report.errors.is_empty(),
            "collision on 5 should produce an error"
        );
    }

    #[tokio::test]
    async fn plan_plus_execute_converges_local_directory() {
        // End-to-end: build a plan from (pd, local) snapshots and
        // apply it; check the directory reflects the target.
        let tmp = tempfile::tempdir().unwrap();
        let node_config = test_node_config(&tmp);
        let range_directory = RangeDirectory::new();
        let peer_directory = StaticPeerDirectory::from_map(Default::default());
        peer_directory.upsert(1, "http://127.0.0.1:0".to_string());

        let target = vec![
            RangeDescriptor::new(
                100,
                b"a".to_vec(),
                b"m".to_vec(),
                vec![ReplicaPlacement::voter(1, 1)],
            ),
            RangeDescriptor::new(
                200,
                b"m".to_vec(),
                b"z".to_vec(),
                vec![ReplicaPlacement::voter(1, 1)],
            ),
        ];
        let mut skip = BTreeSet::new();
        skip.insert(crate::DEFAULT_RANGE_ID);

        let plan = plan_reconcile(1, &target, &[], &skip);
        assert_eq!(plan.to_add.len(), 2);

        let report = execute_plan(plan, 1, &node_config, &peer_directory, &range_directory).await;
        assert!(report.errors.is_empty(), "errors: {:?}", report.errors);
        assert_eq!(report.added.len(), 2);
        assert!(range_directory.get_range(100).is_some());
        assert!(range_directory.get_range(200).is_some());
    }
}
