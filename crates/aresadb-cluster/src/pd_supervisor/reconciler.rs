//! Pure-logic reconciliation planner.
//!
//! Takes two snapshots — what PD thinks this node should be
//! running, and what the local [`RangeDirectory`](crate::RangeDirectory)
//! actually is — and produces a [`ReconcilePlan`]: the list of
//! ranges to open, the list of ranges to close, and any descriptor
//! updates (deferred to Phase 2c-5).
//!
//! Every decision here is a pure function of the inputs. The real
//! I/O happens in the [`executor`](super::executor) module; keeping
//! the planner pure means every edge case can be covered by unit
//! tests without standing up a PD cluster or spinning disks.

use std::collections::{BTreeMap, BTreeSet};

use aresadb_pd::types::{RangeDescriptor, RangeId};
use aresadb_raft::NodeId;

/// Concrete list of actions the executor must apply to bring the
/// local directory in line with the PD catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilePlan {
    /// Descriptors that PD assigns to this node but the local
    /// directory doesn't currently own. Sorted by `range_id` for
    /// deterministic execution order.
    pub to_add: Vec<RangeDescriptor>,

    /// Range ids present in the local directory that PD no longer
    /// assigns to this node (or that PD has dropped entirely).
    /// Sorted ascending for deterministic execution order.
    pub to_remove: Vec<RangeId>,
}

impl ReconcilePlan {
    /// Returns `true` when there's nothing to do. The supervisor
    /// skips the executor entirely on an empty plan to avoid
    /// logging noise.
    pub fn is_empty(&self) -> bool {
        self.to_add.is_empty() && self.to_remove.is_empty()
    }

    /// Convenience accessor for the total number of actions the
    /// plan will emit.
    pub fn action_count(&self) -> usize {
        self.to_add.len() + self.to_remove.len()
    }
}

/// Compute the reconciliation plan for a single node.
///
/// `pd_ranges` is the authoritative catalog (result of
/// `list_ranges` against the PD leader). `local_descriptors` is the
/// snapshot from `RangeDirectory::descriptors()`.
/// `skip_local_ranges` is the set of range ids that must never
/// appear in `to_remove` — every `ClusterNode` passes
/// `{DEFAULT_RANGE_ID}` here so its back-compat default range
/// survives every reconcile tick.
///
/// The function's full contract:
///
/// * `plan.to_add` contains every PD range whose replica list
///   includes `node_id` and whose `range_id` is not already present
///   locally or in `skip_local_ranges`. (Skipped ids never get
///   opened on top of a local runtime that shares the id.)
/// * `plan.to_remove` contains every `local_descriptors` id that
///   PD either doesn't know about or that PD has reassigned to a
///   different replica set no longer including `node_id`.
/// * `plan.to_remove` never contains an id from `skip_local_ranges`.
/// * `plan.to_add` and `plan.to_remove` are disjoint.
pub fn plan_reconcile(
    node_id: NodeId,
    pd_ranges: &[RangeDescriptor],
    local_descriptors: &[RangeDescriptor],
    skip_local_ranges: &BTreeSet<RangeId>,
) -> ReconcilePlan {
    // Build the set of ranges PD wants this node to own. A range
    // is "assigned to us" iff this node id appears in any
    // `ReplicaPlacement` regardless of role — voters and learners
    // both materialise on-disk.
    let mut pd_assigned: BTreeMap<RangeId, RangeDescriptor> = BTreeMap::new();
    for r in pd_ranges {
        if r.replicas.iter().any(|p| p.node_id == node_id) {
            pd_assigned.insert(r.range_id, r.clone());
        }
    }

    let local_ids: BTreeSet<RangeId> = local_descriptors.iter().map(|d| d.range_id).collect();

    // `to_add` = PD-assigned minus local (minus skip list). Skip-
    // list entries are treated as "already opened locally and not
    // managed by PD" — we never duplicate them.
    let mut to_add: Vec<RangeDescriptor> = pd_assigned
        .values()
        .filter(|d| !local_ids.contains(&d.range_id) && !skip_local_ranges.contains(&d.range_id))
        .cloned()
        .collect();
    to_add.sort_by_key(|d| d.range_id);

    // `to_remove` = local minus PD-assigned, minus skip list.
    let mut to_remove: Vec<RangeId> = local_descriptors
        .iter()
        .filter_map(|d| {
            let skip =
                skip_local_ranges.contains(&d.range_id) || pd_assigned.contains_key(&d.range_id);
            if skip {
                None
            } else {
                Some(d.range_id)
            }
        })
        .collect();
    to_remove.sort_unstable();

    ReconcilePlan { to_add, to_remove }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aresadb_pd::types::{RangeDescriptor, ReplicaPlacement};

    fn skip_default() -> BTreeSet<RangeId> {
        let mut s = BTreeSet::new();
        s.insert(crate::DEFAULT_RANGE_ID);
        s
    }

    fn range(id: RangeId, start: &[u8], end: &[u8], voters: &[NodeId]) -> RangeDescriptor {
        let replicas: Vec<_> = voters
            .iter()
            .map(|n| ReplicaPlacement::voter(*n, 1))
            .collect();
        RangeDescriptor::new(id, start.to_vec(), end.to_vec(), replicas)
    }

    #[test]
    fn empty_inputs_empty_plan() {
        let plan = plan_reconcile(1, &[], &[], &skip_default());
        assert!(plan.is_empty());
        assert_eq!(plan.action_count(), 0);
    }

    #[test]
    fn pd_assigns_new_range_to_this_node() {
        let pd = vec![range(100, b"a", b"m", &[1])];
        let plan = plan_reconcile(1, &pd, &[], &skip_default());
        assert_eq!(plan.to_add.len(), 1);
        assert_eq!(plan.to_add[0].range_id, 100);
        assert!(plan.to_remove.is_empty());
    }

    #[test]
    fn pd_assigns_range_to_other_node_we_ignore_it() {
        let pd = vec![range(100, b"a", b"m", &[2, 3])];
        let plan = plan_reconcile(1, &pd, &[], &skip_default());
        assert!(plan.is_empty());
    }

    #[test]
    fn pd_assigns_range_to_multi_node_we_add_if_included() {
        let pd = vec![range(100, b"a", b"m", &[1, 2, 3])];
        let plan = plan_reconcile(1, &pd, &[], &skip_default());
        assert_eq!(plan.to_add.len(), 1);
        let plan3 = plan_reconcile(3, &pd, &[], &skip_default());
        assert_eq!(plan3.to_add.len(), 1);
        let plan9 = plan_reconcile(9, &pd, &[], &skip_default());
        assert!(plan9.is_empty());
    }

    #[test]
    fn skip_list_prevents_both_add_and_remove_for_the_id() {
        // PD doesn't know about range 1 at all; a naive planner
        // would propose removing it. The skip list rescues it.
        let local = vec![range(1, b"", b"", &[1])];
        let plan = plan_reconcile(1, &[], &local, &skip_default());
        assert!(
            plan.is_empty(),
            "default range must never be removed: {:?}",
            plan
        );

        // PD somehow knows about range 1 with us as the replica;
        // we must not try to open it on top of the local one.
        let pd = vec![range(1, b"", b"a", &[1])];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        assert!(
            plan.is_empty(),
            "skip-listed id must never be re-added: {:?}",
            plan
        );
    }

    #[test]
    fn pd_drops_us_from_range_we_remove_locally() {
        let pd = vec![range(42, b"a", b"m", &[2, 3])]; // we (id=1) not in replicas
        let local = vec![
            range(1, b"", b"", &[1]),    // default
            range(42, b"a", b"m", &[1]), // stale local copy
        ];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        assert!(plan.to_add.is_empty());
        assert_eq!(plan.to_remove, vec![42]);
    }

    #[test]
    fn pd_removes_range_entirely_we_remove_locally() {
        let pd: Vec<RangeDescriptor> = vec![];
        let local = vec![
            range(1, b"", b"", &[1]),    // default, survives
            range(50, b"a", b"m", &[1]), // was PD-assigned, now gone
        ];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        assert_eq!(plan.to_remove, vec![50]);
        assert!(plan.to_add.is_empty());
    }

    #[test]
    fn multiple_adds_are_sorted_by_range_id() {
        let pd = vec![
            range(300, b"x", b"z", &[1]),
            range(100, b"a", b"c", &[1]),
            range(200, b"d", b"m", &[1]),
        ];
        let plan = plan_reconcile(1, &pd, &[], &skip_default());
        assert_eq!(
            plan.to_add.iter().map(|d| d.range_id).collect::<Vec<_>>(),
            vec![100, 200, 300],
        );
    }

    #[test]
    fn multiple_removes_are_sorted_ascending() {
        let local = vec![
            range(1, b"", b"", &[1]),
            range(300, b"x", b"z", &[1]),
            range(100, b"a", b"c", &[1]),
            range(200, b"d", b"m", &[1]),
        ];
        let plan = plan_reconcile(1, &[], &local, &skip_default());
        assert_eq!(plan.to_remove, vec![100, 200, 300]);
    }

    #[test]
    fn add_and_remove_can_coexist_in_one_plan() {
        // PD's view: range 50 is ours, range 99 is not.
        let pd = vec![range(50, b"a", b"m", &[1]), range(99, b"n", b"z", &[2, 3])];
        // Local view: default + old range 99 (stale) + no range 50.
        let local = vec![range(1, b"", b"", &[1]), range(99, b"n", b"z", &[1])];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        assert_eq!(plan.to_add.len(), 1);
        assert_eq!(plan.to_add[0].range_id, 50);
        assert_eq!(plan.to_remove, vec![99]);
    }

    #[test]
    fn add_and_remove_are_disjoint_invariant_holds() {
        // Cross-check the disjoint invariant across a random-ish
        // mix: an id in both `pd_assigned` and `local` is neither
        // added nor removed.
        let pd = vec![range(10, b"a", b"b", &[1]), range(20, b"c", b"d", &[1])];
        let local = vec![
            range(1, b"", b"", &[1]),
            range(10, b"a", b"b", &[1]), // already open
            range(30, b"e", b"f", &[1]), // stale
        ];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        let add_ids: BTreeSet<RangeId> = plan.to_add.iter().map(|d| d.range_id).collect();
        let remove_ids: BTreeSet<RangeId> = plan.to_remove.iter().copied().collect();
        assert!(add_ids.is_disjoint(&remove_ids));
        assert_eq!(add_ids.into_iter().collect::<Vec<_>>(), vec![20]);
        assert_eq!(remove_ids.into_iter().collect::<Vec<_>>(), vec![30]);
    }

    #[test]
    fn learner_replicas_are_also_materialised() {
        // A learner on our node still needs a local `RangeRuntime`
        // so the leader has somewhere to replicate into. The
        // planner must not discriminate on role.
        let mut d = range(77, b"a", b"m", &[2]); // voter on 2
        d.replicas
            .push(aresadb_pd::types::ReplicaPlacement::learner(1, 1));
        let plan = plan_reconcile(1, &[d], &[], &skip_default());
        assert_eq!(plan.to_add.len(), 1);
        assert_eq!(plan.to_add[0].range_id, 77);
    }

    #[test]
    fn no_op_when_pd_and_local_agree() {
        let pd = vec![range(10, b"a", b"b", &[1])];
        let local = vec![range(1, b"", b"", &[1]), range(10, b"a", b"b", &[1])];
        let plan = plan_reconcile(1, &pd, &local, &skip_default());
        assert!(plan.is_empty());
    }

    #[test]
    fn custom_skip_list_honours_multiple_ids() {
        let mut skip = BTreeSet::new();
        skip.insert(1);
        skip.insert(2);
        let local = vec![
            range(1, b"", b"", &[1]),
            range(2, b"", b"", &[1]),
            range(3, b"", b"", &[1]), // not skip-listed
        ];
        // PD knows about none of them.
        let plan = plan_reconcile(1, &[], &local, &skip);
        assert_eq!(plan.to_remove, vec![3]);
    }
}
