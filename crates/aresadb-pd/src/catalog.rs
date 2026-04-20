//! Placement-driver catalog — pure-logic index over every range /
//! node descriptor.
//!
//! The [`Catalog`] is deliberately independent of Raft, storage, and
//! networking. It owns an in-memory representation of the cluster's
//! current placement state and exposes one way to mutate it:
//! [`Catalog::apply`], driven by a [`PdCommand`]. Typed helpers
//! ([`Catalog::create_range`], [`Catalog::split_range`], …) exist for
//! direct use in tests and in the future admin layer.
//!
//! Keeping the catalog pure-logic means every invariant below is
//! unit-testable in isolation:
//!
//! 1. Ranges' `[start_key, end_key)` spans never overlap.
//! 2. Raft group ids are unique across ranges.
//! 3. Epoch is strictly monotonically increasing for a given range.
//! 4. Splitting a range preserves total coverage of the keyspace.
//! 5. Merging two ranges requires them to be adjacent and share
//!    their replica set.
//! 6. Allocated `RangeId`s are monotonic and never re-used.
//!
//! Phase 2b-2 will wrap this type in a Raft state machine; that layer
//! is responsible for translating the serialized `PdCommand` log
//! entries back into calls against a [`Catalog`] instance, plus
//! snapshotting.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::{
    command::{PdCommand, PdResponse},
    error::CatalogError,
    types::{GroupId, LeaseInfo, NodeId, NodeInfo, RangeDescriptor, RangeId, ReplicaPlacement},
};

/// The placement-driver catalog.
///
/// A `Catalog` is cheap to construct; the intended lifecycle is
/// "create one, apply commands, serialize on snapshot". Nothing here
/// is thread-safe — wrap in a mutex at the Raft state-machine layer.
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    /// Primary storage: `range_id -> descriptor`.
    ranges: BTreeMap<RangeId, RangeDescriptor>,
    /// Secondary index: `start_key -> range_id`. Ordered by the span's
    /// lower bound so overlap checks and "which range owns this key?"
    /// lookups run in `O(log n)`.
    by_start: BTreeMap<Vec<u8>, RangeId>,
    /// Secondary index: `raft_group_id -> range_id`. Lets the catalog
    /// enforce group-id uniqueness cheaply.
    by_group: BTreeMap<GroupId, RangeId>,
    /// Node inventory. Populated via `RegisterNode` / `HeartbeatNode`.
    nodes: BTreeMap<NodeId, NodeInfo>,
    /// Next range id to hand out on [`PdCommand::SplitRange`]. Also
    /// advanced past any explicit id used in `CreateRange`.
    next_range_id: RangeId,
}

impl Catalog {
    /// Construct a fresh, empty catalog. Range ids will be handed out
    /// starting at `1`; `0` is reserved as "unassigned".
    pub fn new() -> Self {
        Self {
            ranges: BTreeMap::new(),
            by_start: BTreeMap::new(),
            by_group: BTreeMap::new(),
            nodes: BTreeMap::new(),
            next_range_id: 1,
        }
    }

    // ------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------

    /// Look up a range by id.
    pub fn get_range(&self, range_id: RangeId) -> Option<&RangeDescriptor> {
        self.ranges.get(&range_id)
    }

    /// Iterate every range in the catalog, ordered by `range_id`.
    pub fn iter_ranges(&self) -> impl Iterator<Item = &RangeDescriptor> {
        self.ranges.values()
    }

    /// Iterate every range in the catalog, ordered by `start_key`.
    /// Useful for the "draw me a picture of the keyspace" admin view.
    pub fn iter_ranges_by_start(&self) -> impl Iterator<Item = &RangeDescriptor> {
        self.by_start
            .values()
            .map(|id| self.ranges.get(id).expect("by_start index out of sync"))
    }

    /// Find the range whose `[start_key, end_key)` contains `key`.
    /// Returns `None` if no range covers the key (the catalog allows
    /// gaps — total coverage is a Phase 2c bootstrap invariant, not a
    /// catalog-level one).
    pub fn find_range_for_key(&self, key: &[u8]) -> Option<&RangeDescriptor> {
        // Find the candidate predecessor: the range whose start_key is
        // the largest entry <= key. BTreeMap gives this in O(log n).
        let (_, range_id) = self
            .by_start
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()?;
        let candidate = self.ranges.get(range_id)?;
        if candidate.contains(key) {
            Some(candidate)
        } else {
            None
        }
    }

    /// Iterate the node inventory.
    pub fn iter_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    /// Look up a node by id.
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    /// Hydrate the catalog from a pre-built set of descriptors and
    /// nodes. Used by the persistent state machine's recovery path —
    /// callers must pass rows that are themselves consistent (no
    /// overlap, no duplicate group ids, etc.). This routine does
    /// **not** re-run invariant checks; it trusts the on-disk data.
    /// `next_range_id_hint` seeds the internal counter and is then
    /// advanced past every range id that appears in `ranges`.
    pub fn load(
        ranges: impl IntoIterator<Item = RangeDescriptor>,
        nodes: impl IntoIterator<Item = NodeInfo>,
        next_range_id_hint: RangeId,
    ) -> Self {
        let mut c = Self::new();
        for desc in ranges {
            c.by_start.insert(desc.start_key.clone(), desc.range_id);
            c.by_group.insert(desc.raft_group_id, desc.range_id);
            c.next_range_id = c.next_range_id.max(desc.range_id + 1);
            c.ranges.insert(desc.range_id, desc);
        }
        for info in nodes {
            c.nodes.insert(info.node_id, info);
        }
        c.next_range_id = c.next_range_id.max(next_range_id_hint);
        c
    }

    /// Peek at the next range id the catalog would hand out on a
    /// split. Does **not** advance the counter.
    pub fn peek_next_range_id(&self) -> RangeId {
        self.next_range_id
    }

    /// Current range count. Useful for tests and admin dashboards.
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    // ------------------------------------------------------------
    // Command dispatch
    // ------------------------------------------------------------

    /// Apply a replicated command. Every successful mutation returns
    /// a [`PdResponse`]; rejections surface as [`CatalogError`] so
    /// callers can distinguish programmatically. The state-machine
    /// layer (Phase 2b-2) converts `Err` into
    /// [`PdResponse::Error`] for the on-wire protocol.
    pub fn apply(&mut self, cmd: PdCommand) -> Result<PdResponse, CatalogError> {
        match cmd {
            PdCommand::RegisterNode(info) => {
                self.register_node(info);
                Ok(PdResponse::Ok)
            }
            PdCommand::HeartbeatNode {
                node_id,
                last_seen_millis,
            } => {
                self.heartbeat_node(node_id, last_seen_millis)?;
                Ok(PdResponse::Ok)
            }
            PdCommand::CreateRange(desc) => {
                let stored = self.create_range(desc)?;
                Ok(PdResponse::Range(stored))
            }
            PdCommand::SplitRange {
                parent_range_id,
                split_key,
            } => {
                let new_rhs = self.split_range(parent_range_id, split_key)?;
                Ok(PdResponse::Range(new_rhs))
            }
            PdCommand::MergeRanges { left, right } => {
                self.merge_ranges(left, right)?;
                Ok(PdResponse::Ok)
            }
            PdCommand::UpdateMembership {
                range_id,
                new_replicas,
                new_epoch,
            } => {
                self.update_membership(range_id, new_replicas, new_epoch)?;
                Ok(PdResponse::Ok)
            }
            PdCommand::UpdateLease { range_id, lease } => {
                self.update_lease(range_id, lease)?;
                Ok(PdResponse::Ok)
            }
        }
    }

    // ------------------------------------------------------------
    // Typed mutators
    // ------------------------------------------------------------

    /// Register (or refresh) a node in the cluster inventory. If the
    /// node is already present its address / store list are replaced
    /// but the last-heartbeat timestamp is preserved — heartbeats
    /// arrive via [`Self::heartbeat_node`] on their own log entries.
    pub fn register_node(&mut self, mut info: NodeInfo) {
        if let Some(existing) = self.nodes.get(&info.node_id) {
            info.last_heartbeat_millis = existing.last_heartbeat_millis;
        }
        self.nodes.insert(info.node_id, info);
    }

    /// Mark a node as alive at `last_seen_millis`. Rejected if the
    /// node has never registered. A heartbeat timestamp never moves
    /// backwards — older timestamps are silently dropped.
    pub fn heartbeat_node(
        &mut self,
        node_id: NodeId,
        last_seen_millis: u64,
    ) -> Result<(), CatalogError> {
        let entry = self
            .nodes
            .get_mut(&node_id)
            .ok_or(CatalogError::NodeNotRegistered(node_id))?;
        entry.last_heartbeat_millis = last_seen_millis.max(entry.last_heartbeat_millis);
        Ok(())
    }

    /// Insert a brand-new range. Rejects overlap, duplicate ids, and
    /// zero-width / inverted spans. On success, returns the stored
    /// descriptor (equal to the input).
    pub fn create_range(&mut self, desc: RangeDescriptor) -> Result<RangeDescriptor, CatalogError> {
        if !desc.has_non_empty_span() {
            return Err(CatalogError::InvalidSpan);
        }
        if self.ranges.contains_key(&desc.range_id) {
            return Err(CatalogError::RangeAlreadyExists(desc.range_id));
        }
        if let Some(existing_owner) = self.by_group.get(&desc.raft_group_id) {
            if *existing_owner != desc.range_id {
                return Err(CatalogError::GroupIdInUse {
                    group_id: desc.raft_group_id,
                    owner: *existing_owner,
                });
            }
        }
        if let Some(conflict) = self.find_overlap(&desc.start_key, &desc.end_key) {
            return Err(CatalogError::OverlappingRange {
                start_hex: to_hex(&desc.start_key),
                end_hex: end_hex(&desc.end_key),
                conflict,
            });
        }

        self.by_start.insert(desc.start_key.clone(), desc.range_id);
        self.by_group.insert(desc.raft_group_id, desc.range_id);
        self.next_range_id = self.next_range_id.max(desc.range_id + 1);
        self.ranges.insert(desc.range_id, desc.clone());
        Ok(desc)
    }

    /// Split `parent_range_id` at `split_key`. The parent's span
    /// shrinks to `[parent.start_key, split_key)`; a new range
    /// covering `[split_key, parent.end_key)` is created with a
    /// freshly-allocated id and Raft group id.
    ///
    /// Returns the newly-created right-hand-side descriptor.
    pub fn split_range(
        &mut self,
        parent_range_id: RangeId,
        split_key: Vec<u8>,
    ) -> Result<RangeDescriptor, CatalogError> {
        let parent = self
            .ranges
            .get(&parent_range_id)
            .ok_or(CatalogError::RangeNotFound(parent_range_id))?
            .clone();

        // split_key must be strictly inside parent's span.
        let strictly_above_start = split_key.as_slice() > parent.start_key.as_slice();
        let strictly_below_end =
            parent.end_key.is_empty() || split_key.as_slice() < parent.end_key.as_slice();
        if !(strictly_above_start && strictly_below_end) {
            return Err(CatalogError::SplitKeyOutOfBounds {
                range_id: parent_range_id,
                key_hex: to_hex(&split_key),
            });
        }

        let new_range_id = self.next_range_id;
        let new_group_id: GroupId = new_range_id;
        self.next_range_id += 1;

        let new_generation = parent.generation + 1;

        // Build the RHS first so we can still reference the parent's
        // original span.
        let rhs = RangeDescriptor {
            range_id: new_range_id,
            start_key: split_key.clone(),
            end_key: parent.end_key.clone(),
            replicas: parent.replicas.clone(),
            raft_group_id: new_group_id,
            epoch: parent.epoch,
            generation: new_generation,
            lease: None,
        };

        // Mutate the parent in place.
        {
            let parent_mut = self
                .ranges
                .get_mut(&parent_range_id)
                .expect("parent existed above");
            parent_mut.end_key = split_key;
            parent_mut.generation = new_generation;
            // Splitting drops any stale lease — a new leader for the
            // right-hand span will be elected anyway, and the parent's
            // old lease covered keys that are no longer its own.
            parent_mut.lease = None;
        }

        // Insert RHS into the indices and the primary map.
        self.by_start.insert(rhs.start_key.clone(), rhs.range_id);
        self.by_group.insert(rhs.raft_group_id, rhs.range_id);
        self.ranges.insert(rhs.range_id, rhs.clone());

        Ok(rhs)
    }

    /// Merge two adjacent ranges. `left.end_key` must equal
    /// `right.start_key`; both ranges must share the same replica set
    /// (safety invariant — rebalancing to a common placement is a
    /// precondition the coordinator is responsible for).
    pub fn merge_ranges(&mut self, left: RangeId, right: RangeId) -> Result<(), CatalogError> {
        let left_desc = self
            .ranges
            .get(&left)
            .ok_or(CatalogError::RangeNotFound(left))?
            .clone();
        let right_desc = self
            .ranges
            .get(&right)
            .ok_or(CatalogError::RangeNotFound(right))?
            .clone();

        if left_desc.end_key != right_desc.start_key || left_desc.end_key.is_empty() {
            return Err(CatalogError::NotAdjacent { left, right });
        }

        if !replica_sets_equal(&left_desc.replicas, &right_desc.replicas) {
            return Err(CatalogError::ReplicaSetMismatch { left, right });
        }

        let new_generation = left_desc.generation.max(right_desc.generation) + 1;

        // Drop the right range from every index.
        self.by_start.remove(&right_desc.start_key);
        self.by_group.remove(&right_desc.raft_group_id);
        self.ranges.remove(&right);

        // Extend the left range to cover the merged span.
        let left_mut = self.ranges.get_mut(&left).expect("left existed above");
        left_mut.end_key = right_desc.end_key;
        left_mut.generation = new_generation;
        left_mut.lease = None;
        Ok(())
    }

    /// Replace the replica set of `range_id` atomically. `new_epoch`
    /// must be strictly greater than the range's current epoch.
    pub fn update_membership(
        &mut self,
        range_id: RangeId,
        new_replicas: Vec<ReplicaPlacement>,
        new_epoch: u64,
    ) -> Result<(), CatalogError> {
        let desc = self
            .ranges
            .get_mut(&range_id)
            .ok_or(CatalogError::RangeNotFound(range_id))?;
        if new_epoch <= desc.epoch {
            return Err(CatalogError::EpochRegression {
                range_id,
                existing: desc.epoch,
                attempted: new_epoch,
            });
        }
        desc.replicas = new_replicas;
        desc.epoch = new_epoch;
        Ok(())
    }

    /// Install, renew, or clear the leader lease on a range.
    pub fn update_lease(
        &mut self,
        range_id: RangeId,
        lease: Option<LeaseInfo>,
    ) -> Result<(), CatalogError> {
        let desc = self
            .ranges
            .get_mut(&range_id)
            .ok_or(CatalogError::RangeNotFound(range_id))?;
        desc.lease = lease;
        Ok(())
    }

    // ------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------

    /// Return the id of any existing range that overlaps the span
    /// `[start_key, end_key)`, or `None` if the span is unoccupied.
    fn find_overlap(&self, start_key: &[u8], end_key: &[u8]) -> Option<RangeId> {
        // Look at the range immediately at or before `start_key`. Its
        // end_key must not reach past the proposed start.
        if let Some((_, pred_id)) = self
            .by_start
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(start_key)))
            .next_back()
        {
            let pred = &self.ranges[pred_id];
            // We matched on start_key ≤ our start. If the predecessor
            // actually starts *at* our start, it occupies the slot.
            // If it starts before, it only overlaps when its end_key
            // bleeds past.
            let pred_covers_start = pred.end_key.is_empty() || pred.end_key.as_slice() > start_key;
            if pred_covers_start {
                return Some(*pred_id);
            }
        }

        // Look at the range immediately after `start_key`. Its
        // start_key must be ≥ our end_key (or we have an unbounded
        // end, in which case any successor overlaps).
        if let Some((suc_start, suc_id)) = self
            .by_start
            .range::<[u8], _>((Bound::Excluded(start_key), Bound::Unbounded))
            .next()
        {
            let overlaps = end_key.is_empty() || suc_start.as_slice() < end_key;
            if overlaps {
                return Some(*suc_id);
            }
        }

        None
    }
}

/// Lowercase hex rendering of a byte slice. Used for error reporting.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 * bytes.len() + 2);
    s.push_str("0x");
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Render an end_key, mapping the empty vector to `+inf`.
fn end_hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        "+inf".to_string()
    } else {
        to_hex(bytes)
    }
}

/// Compare two replica lists as multisets. The catalog does not
/// impose an ordering on replicas, but merge correctness requires
/// them to match set-for-set.
fn replica_sets_equal(a: &[ReplicaPlacement], b: &[ReplicaPlacement]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted = a.to_vec();
    let mut b_sorted = b.to_vec();
    let key = |r: &ReplicaPlacement| (r.node_id, r.store_id, r.role as u8 as u64);
    a_sorted.sort_by_key(key);
    b_sorted.sort_by_key(key);
    a_sorted == b_sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(ids: &[NodeId]) -> Vec<ReplicaPlacement> {
        ids.iter().map(|n| ReplicaPlacement::voter(*n, 1)).collect()
    }

    fn genesis() -> RangeDescriptor {
        RangeDescriptor::new(1, Vec::<u8>::new(), Vec::<u8>::new(), voters(&[1, 2, 3]))
    }

    // -------- bootstrap --------

    #[test]
    fn new_catalog_is_empty() {
        let c = Catalog::new();
        assert_eq!(c.range_count(), 0);
        assert_eq!(c.peek_next_range_id(), 1);
        assert!(c.find_range_for_key(b"anything").is_none());
    }

    #[test]
    fn create_genesis_range_succeeds() {
        let mut c = Catalog::new();
        let stored = c.create_range(genesis()).unwrap();
        assert_eq!(stored.range_id, 1);
        assert_eq!(c.range_count(), 1);
        assert_eq!(c.peek_next_range_id(), 2);

        // Genesis range covers every key.
        assert!(c.find_range_for_key(b"").is_some());
        assert!(c.find_range_for_key(b"arbitrary").is_some());
        assert!(c.find_range_for_key(&[0xff; 32]).is_some());
    }

    // -------- create / overlap --------

    #[test]
    fn create_rejects_overlap_with_existing() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"c".to_vec(),
            b"m".to_vec(),
            vec![],
        ))
        .unwrap();

        // Completely inside.
        let err = c
            .create_range(RangeDescriptor::new(
                2,
                b"f".to_vec(),
                b"h".to_vec(),
                vec![],
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::OverlappingRange { conflict: 1, .. }
        ));

        // Straddles the right boundary.
        let err = c
            .create_range(RangeDescriptor::new(
                3,
                b"k".to_vec(),
                b"z".to_vec(),
                vec![],
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::OverlappingRange { conflict: 1, .. }
        ));

        // Straddles the left boundary.
        let err = c
            .create_range(RangeDescriptor::new(
                4,
                b"a".to_vec(),
                b"d".to_vec(),
                vec![],
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::OverlappingRange { conflict: 1, .. }
        ));

        // Exactly the same span.
        let err = c
            .create_range(RangeDescriptor::new(
                5,
                b"c".to_vec(),
                b"m".to_vec(),
                vec![],
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::OverlappingRange { conflict: 1, .. }
        ));
    }

    #[test]
    fn create_allows_adjacent_non_overlapping_ranges() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"m".to_vec(),
            vec![],
        ))
        .unwrap();
        // Adjacent on the right boundary: end of left == start of new.
        c.create_range(RangeDescriptor::new(
            2,
            b"m".to_vec(),
            b"z".to_vec(),
            vec![],
        ))
        .unwrap();
        assert_eq!(c.range_count(), 2);
    }

    #[test]
    fn create_rejects_duplicate_range_id() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            7,
            b"a".to_vec(),
            b"m".to_vec(),
            vec![],
        ))
        .unwrap();
        let err = c
            .create_range(RangeDescriptor::new(
                7,
                b"n".to_vec(),
                b"z".to_vec(),
                vec![],
            ))
            .unwrap_err();
        assert_eq!(err, CatalogError::RangeAlreadyExists(7));
    }

    #[test]
    fn create_rejects_duplicate_group_id() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor {
            range_id: 1,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            replicas: vec![],
            raft_group_id: 99,
            epoch: 0,
            generation: 0,
            lease: None,
        })
        .unwrap();

        let err = c
            .create_range(RangeDescriptor {
                range_id: 2,
                start_key: b"n".to_vec(),
                end_key: b"z".to_vec(),
                replicas: vec![],
                raft_group_id: 99,
                epoch: 0,
                generation: 0,
                lease: None,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::GroupIdInUse {
                group_id: 99,
                owner: 1
            }
        ));
    }

    #[test]
    fn create_rejects_zero_width_and_inverted_spans() {
        let mut c = Catalog::new();
        assert_eq!(
            c.create_range(RangeDescriptor::new(
                1,
                b"a".to_vec(),
                b"a".to_vec(),
                vec![]
            )),
            Err(CatalogError::InvalidSpan)
        );
        assert_eq!(
            c.create_range(RangeDescriptor::new(
                2,
                b"z".to_vec(),
                b"a".to_vec(),
                vec![]
            )),
            Err(CatalogError::InvalidSpan)
        );
    }

    #[test]
    fn create_advances_next_range_id_past_explicit_ids() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            42,
            b"a".to_vec(),
            b"m".to_vec(),
            vec![],
        ))
        .unwrap();
        assert_eq!(c.peek_next_range_id(), 43);
    }

    // -------- split --------

    #[test]
    fn split_divides_span_and_preserves_coverage() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();

        let rhs = c.split_range(1, b"m".to_vec()).unwrap();
        assert_eq!(rhs.range_id, 2);
        assert_eq!(rhs.raft_group_id, 2);
        assert_eq!(rhs.start_key, b"m".to_vec());
        assert_eq!(rhs.end_key, Vec::<u8>::new());

        let lhs = c.get_range(1).unwrap();
        assert_eq!(lhs.start_key, Vec::<u8>::new());
        assert_eq!(lhs.end_key, b"m".to_vec());

        // Every key still resolves to some range.
        assert_eq!(c.find_range_for_key(b"a").unwrap().range_id, 1);
        assert_eq!(c.find_range_for_key(b"l").unwrap().range_id, 1);
        assert_eq!(c.find_range_for_key(b"m").unwrap().range_id, 2);
        assert_eq!(c.find_range_for_key(b"zzz").unwrap().range_id, 2);
    }

    #[test]
    fn split_bumps_generation_on_both_sides() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        let original_generation = c.get_range(1).unwrap().generation;

        let rhs = c.split_range(1, b"m".to_vec()).unwrap();
        assert_eq!(rhs.generation, original_generation + 1);
        assert_eq!(c.get_range(1).unwrap().generation, original_generation + 1);
    }

    #[test]
    fn split_inherits_replica_set_and_epoch() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        c.update_membership(1, voters(&[1, 2, 3, 4]), 5).unwrap();

        let rhs = c.split_range(1, b"m".to_vec()).unwrap();
        assert_eq!(rhs.replicas, c.get_range(1).unwrap().replicas);
        assert_eq!(rhs.epoch, 5, "RHS inherits parent's epoch at split time");
    }

    #[test]
    fn split_drops_stale_leases() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        c.update_lease(
            1,
            Some(LeaseInfo {
                holder: 1,
                expires_at_millis: 1_700_000_000_000,
            }),
        )
        .unwrap();

        let rhs = c.split_range(1, b"m".to_vec()).unwrap();
        assert!(rhs.lease.is_none());
        assert!(
            c.get_range(1).unwrap().lease.is_none(),
            "parent's lease is dropped on split too"
        );
    }

    #[test]
    fn split_rejects_out_of_range_keys() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"c".to_vec(),
            b"m".to_vec(),
            vec![],
        ))
        .unwrap();

        // Equal to start: not strictly inside.
        let err = c.split_range(1, b"c".to_vec()).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::SplitKeyOutOfBounds { range_id: 1, .. }
        ));

        // Equal to end: not strictly inside.
        let err = c.split_range(1, b"m".to_vec()).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::SplitKeyOutOfBounds { range_id: 1, .. }
        ));

        // Below start.
        let err = c.split_range(1, b"a".to_vec()).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::SplitKeyOutOfBounds { range_id: 1, .. }
        ));

        // Above end.
        let err = c.split_range(1, b"z".to_vec()).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::SplitKeyOutOfBounds { range_id: 1, .. }
        ));
    }

    #[test]
    fn split_unknown_parent_errors() {
        let mut c = Catalog::new();
        let err = c.split_range(99, b"m".to_vec()).unwrap_err();
        assert_eq!(err, CatalogError::RangeNotFound(99));
    }

    #[test]
    fn split_advances_next_range_id() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        assert_eq!(c.peek_next_range_id(), 2);
        c.split_range(1, b"m".to_vec()).unwrap();
        assert_eq!(c.peek_next_range_id(), 3);
        c.split_range(1, b"f".to_vec()).unwrap();
        assert_eq!(c.peek_next_range_id(), 4);
    }

    // -------- merge --------

    #[test]
    fn merge_adjacent_ranges_with_shared_replicas_succeeds() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        c.split_range(1, b"m".to_vec()).unwrap();
        let gen_before = c.get_range(1).unwrap().generation;

        c.merge_ranges(1, 2).unwrap();
        assert_eq!(c.range_count(), 1);
        let merged = c.get_range(1).unwrap();
        assert_eq!(merged.start_key, Vec::<u8>::new());
        assert_eq!(merged.end_key, Vec::<u8>::new());
        assert!(merged.generation > gen_before);
    }

    #[test]
    fn merge_rejects_non_adjacent_ranges() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"f".to_vec(),
            voters(&[1]),
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            2,
            b"n".to_vec(),
            b"z".to_vec(),
            voters(&[1]),
        ))
        .unwrap();

        let err = c.merge_ranges(1, 2).unwrap_err();
        assert_eq!(err, CatalogError::NotAdjacent { left: 1, right: 2 });
    }

    #[test]
    fn merge_rejects_mismatched_replica_sets() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"m".to_vec(),
            voters(&[1, 2, 3]),
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            2,
            b"m".to_vec(),
            b"z".to_vec(),
            voters(&[1, 2, 4]),
        ))
        .unwrap();

        let err = c.merge_ranges(1, 2).unwrap_err();
        assert_eq!(err, CatalogError::ReplicaSetMismatch { left: 1, right: 2 });
    }

    #[test]
    fn merge_ignores_replica_order() {
        // Same replicas in different order should still match.
        let a_order = vec![
            ReplicaPlacement::voter(1, 1),
            ReplicaPlacement::voter(2, 1),
            ReplicaPlacement::voter(3, 1),
        ];
        let b_order = vec![
            ReplicaPlacement::voter(3, 1),
            ReplicaPlacement::voter(1, 1),
            ReplicaPlacement::voter(2, 1),
        ];
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"m".to_vec(),
            a_order,
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            2,
            b"m".to_vec(),
            b"z".to_vec(),
            b_order,
        ))
        .unwrap();
        c.merge_ranges(1, 2).unwrap();
    }

    // -------- membership --------

    #[test]
    fn update_membership_bumps_epoch_and_replaces_replicas() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        c.update_membership(1, voters(&[1, 2, 3, 4]), 1).unwrap();

        let desc = c.get_range(1).unwrap();
        assert_eq!(desc.epoch, 1);
        assert_eq!(desc.replicas.len(), 4);
    }

    #[test]
    fn update_membership_rejects_epoch_regression() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        c.update_membership(1, voters(&[1, 2, 3]), 5).unwrap();

        let err = c.update_membership(1, voters(&[1, 2]), 3).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::EpochRegression {
                range_id: 1,
                existing: 5,
                attempted: 3
            }
        ));

        let err = c.update_membership(1, voters(&[1, 2]), 5).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::EpochRegression {
                range_id: 1,
                existing: 5,
                attempted: 5
            }
        ));
    }

    #[test]
    fn update_membership_unknown_range_errors() {
        let mut c = Catalog::new();
        let err = c.update_membership(99, voters(&[1]), 1).unwrap_err();
        assert_eq!(err, CatalogError::RangeNotFound(99));
    }

    // -------- lease --------

    #[test]
    fn update_lease_installs_and_clears() {
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();

        let lease = LeaseInfo {
            holder: 2,
            expires_at_millis: 1_700_000_000_000,
        };
        c.update_lease(1, Some(lease.clone())).unwrap();
        assert_eq!(c.get_range(1).unwrap().lease, Some(lease));

        c.update_lease(1, None).unwrap();
        assert!(c.get_range(1).unwrap().lease.is_none());
    }

    #[test]
    fn update_lease_unknown_range_errors() {
        let mut c = Catalog::new();
        let err = c.update_lease(77, None).unwrap_err();
        assert_eq!(err, CatalogError::RangeNotFound(77));
    }

    // -------- find_range_for_key --------

    #[test]
    fn find_range_for_key_returns_none_on_gap() {
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"f".to_vec(),
            vec![],
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            2,
            b"n".to_vec(),
            b"z".to_vec(),
            vec![],
        ))
        .unwrap();

        // [f, n) is a gap.
        assert!(c.find_range_for_key(b"g").is_none());
        assert!(c.find_range_for_key(b"m").is_none());

        // Boundary check.
        assert_eq!(c.find_range_for_key(b"a").unwrap().range_id, 1);
        assert!(c.find_range_for_key(b"f").is_none(), "end is exclusive");
        assert_eq!(c.find_range_for_key(b"n").unwrap().range_id, 2);
    }

    #[test]
    fn find_range_for_key_after_many_splits() {
        // Stress the BTreeMap predecessor lookup after a pile of
        // splits — the by_start index had better stay consistent.
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();
        for split_at in [b"c" as &[u8], b"h", b"m", b"r", b"w"] {
            let parent = c.find_range_for_key(split_at).unwrap().range_id;
            c.split_range(parent, split_at.to_vec()).unwrap();
        }
        assert_eq!(c.range_count(), 6);

        // Every key resolves to some range; ranges pack contiguously.
        let checks: &[&[u8]] = &[
            b"", b"a", b"c", b"f", b"h", b"j", b"m", b"q", b"r", b"v", b"w", b"y", b"\xff",
        ];
        for k in checks {
            assert!(c.find_range_for_key(k).is_some(), "key {k:?} had no range");
        }

        // The six ranges together cover the full keyspace with no
        // overlaps. Confirm by walking them in start-key order.
        let mut last_end: Vec<u8> = Vec::new();
        let mut seen = 0usize;
        let mut covers_infinity = false;
        for r in c.iter_ranges_by_start() {
            assert_eq!(
                r.start_key, last_end,
                "gap detected at range {}",
                r.range_id
            );
            if r.end_key.is_empty() {
                covers_infinity = true;
            }
            last_end = r.end_key.clone();
            seen += 1;
        }
        assert_eq!(seen, 6);
        assert!(covers_infinity, "last range should extend to +inf");
    }

    // -------- apply() dispatch --------

    #[test]
    fn apply_dispatches_to_typed_helpers() {
        let mut c = Catalog::new();

        let resp = c.apply(PdCommand::CreateRange(genesis())).unwrap();
        assert!(matches!(resp, PdResponse::Range(d) if d.range_id == 1));

        let resp = c
            .apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"m".to_vec(),
            })
            .unwrap();
        assert!(matches!(resp, PdResponse::Range(d) if d.range_id == 2));

        // Lease installs even though the merge below will wipe it —
        // we just want to prove the command plumbing.
        let resp = c
            .apply(PdCommand::UpdateLease {
                range_id: 1,
                lease: Some(LeaseInfo {
                    holder: 1,
                    expires_at_millis: 1,
                }),
            })
            .unwrap();
        assert!(matches!(resp, PdResponse::Ok));

        // Merge first — both sides still share the post-split
        // replica set, so this succeeds. Afterwards range 2 is gone.
        let resp = c
            .apply(PdCommand::MergeRanges { left: 1, right: 2 })
            .unwrap();
        assert!(matches!(resp, PdResponse::Ok));

        // Then prove membership update plumbing on the surviving range.
        let resp = c
            .apply(PdCommand::UpdateMembership {
                range_id: 1,
                new_replicas: voters(&[1, 2, 3, 4]),
                new_epoch: 1,
            })
            .unwrap();
        assert!(matches!(resp, PdResponse::Ok));
    }

    #[test]
    fn apply_catches_every_error_path() {
        // Belt-and-suspenders: every command variant must be able to
        // return its corresponding rejection through `apply`.
        let mut c = Catalog::new();
        c.create_range(genesis()).unwrap();

        // CreateRange overlap.
        let err = c.apply(PdCommand::CreateRange(genesis())).unwrap_err();
        assert!(matches!(err, CatalogError::RangeAlreadyExists(1)));

        // SplitRange out-of-bounds.
        let err = c
            .apply(PdCommand::SplitRange {
                parent_range_id: 1,
                split_key: b"".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(
            err,
            CatalogError::SplitKeyOutOfBounds { range_id: 1, .. }
        ));

        // UpdateMembership epoch regression.
        c.update_membership(1, voters(&[1, 2]), 3).unwrap();
        let err = c
            .apply(PdCommand::UpdateMembership {
                range_id: 1,
                new_replicas: voters(&[1]),
                new_epoch: 3,
            })
            .unwrap_err();
        assert!(matches!(err, CatalogError::EpochRegression { .. }));

        // UpdateLease unknown.
        let err = c
            .apply(PdCommand::UpdateLease {
                range_id: 99,
                lease: None,
            })
            .unwrap_err();
        assert_eq!(err, CatalogError::RangeNotFound(99));

        // MergeRanges on a non-existent right range.
        let err = c
            .apply(PdCommand::MergeRanges { left: 1, right: 99 })
            .unwrap_err();
        assert_eq!(err, CatalogError::RangeNotFound(99));

        // HeartbeatNode on an unregistered node.
        let err = c
            .apply(PdCommand::HeartbeatNode {
                node_id: 42,
                last_seen_millis: 1,
            })
            .unwrap_err();
        assert_eq!(err, CatalogError::NodeNotRegistered(42));
    }

    // -------- node inventory --------

    #[test]
    fn register_node_then_heartbeat_updates_timestamp() {
        let mut c = Catalog::new();
        c.register_node(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        });
        c.heartbeat_node(1, 1_700_000_000_000).unwrap();
        assert_eq!(
            c.iter_nodes().next().unwrap().last_heartbeat_millis,
            1_700_000_000_000
        );
    }

    #[test]
    fn heartbeat_is_monotonic() {
        let mut c = Catalog::new();
        c.register_node(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        });
        c.heartbeat_node(1, 1_000).unwrap();
        c.heartbeat_node(1, 500).unwrap(); // out-of-order heartbeat
        assert_eq!(
            c.iter_nodes().next().unwrap().last_heartbeat_millis,
            1_000,
            "older heartbeats must not regress the timestamp"
        );
    }

    #[test]
    fn register_existing_node_preserves_heartbeat() {
        let mut c = Catalog::new();
        c.register_node(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        });
        c.heartbeat_node(1, 1_000).unwrap();

        c.register_node(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1, 2],       // added a store
            last_heartbeat_millis: 0, // should NOT clobber
        });
        assert_eq!(
            c.iter_nodes().next().unwrap().last_heartbeat_millis,
            1_000,
            "re-register must preserve the last heartbeat timestamp"
        );
        assert_eq!(c.iter_nodes().next().unwrap().stores, vec![1, 2]);
    }

    // -------- ordering / indices --------

    #[test]
    fn iter_ranges_by_start_follows_keyspace_order() {
        // Create ranges out of key order; iter_ranges_by_start should
        // still walk them in lex order.
        let mut c = Catalog::new();
        c.create_range(RangeDescriptor::new(
            10,
            b"m".to_vec(),
            b"t".to_vec(),
            vec![],
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            11,
            b"a".to_vec(),
            b"f".to_vec(),
            vec![],
        ))
        .unwrap();
        c.create_range(RangeDescriptor::new(
            12,
            b"t".to_vec(),
            b"z".to_vec(),
            vec![],
        ))
        .unwrap();

        let ids: Vec<_> = c.iter_ranges_by_start().map(|r| r.range_id).collect();
        assert_eq!(ids, vec![11, 10, 12]);
    }
}
