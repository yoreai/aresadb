//! Core placement-driver types. All types here are the on-the-wire /
//! on-disk representation and round-trip through `bincode`.
//!
//! Everything that the catalog stores about a range or a node lives in
//! these structs — the catalog, the state machine, and (later) the
//! admin gRPC service all agree on this shape.

use serde::{Deserialize, Serialize};

/// Identifier of a physical cluster node. Equal to the `NodeId` used
/// by `aresadb-raft`; redeclared as a plain `u64` alias here so this
/// crate does not need to depend on `aresadb-raft`.
pub type NodeId = u64;

/// Identifier of a physical store (disk / volume) attached to a node.
/// A single node may host several stores — the catalog tracks them
/// individually so replicas can be placed for write-amplification and
/// failure-domain reasons.
pub type StoreId = u64;

/// Identifier of a single range. The PD hands these out monotonically
/// from a counter row (`/m/pd/seq/range_id`).
pub type RangeId = u64;

/// Identifier of a single Raft group. Typically equal to the
/// [`RangeId`] of the range the group replicates; kept distinct so
/// future layouts (e.g. multiple groups per range for metadata) can
/// diverge without changing the type.
pub type GroupId = u64;

/// Role of a replica in its Raft group. Learners receive log entries
/// but do not vote; voters participate in elections and quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Participates in elections and counts toward quorum.
    Voter,
    /// Receives replication traffic but does not vote. Used while
    /// catching a new replica up before promoting it to voter.
    Learner,
}

/// Placement of a single replica: which node holds it, which store
/// within that node, and what role it plays in the Raft group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    /// Node hosting this replica.
    pub node_id: NodeId,
    /// Store within [`Self::node_id`] that owns the replica's data.
    pub store_id: StoreId,
    /// Role of this replica in its Raft group.
    pub role: ReplicaRole,
}

impl ReplicaPlacement {
    /// Convenience constructor for a voter replica on a single-store
    /// node (by far the most common case in tests).
    pub fn voter(node_id: NodeId, store_id: StoreId) -> Self {
        Self {
            node_id,
            store_id,
            role: ReplicaRole::Voter,
        }
    }

    /// Convenience constructor for a learner replica on a single-store
    /// node.
    pub fn learner(node_id: NodeId, store_id: StoreId) -> Self {
        Self {
            node_id,
            store_id,
            role: ReplicaRole::Learner,
        }
    }
}

/// Information about the current range-leader lease. Leaders serve
/// reads locally (without a Raft round-trip) only while their lease is
/// live; lease expiry is measured against the cluster's hybrid logical
/// clock, serialized here as a Unix-millis timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseInfo {
    /// Node currently holding the lease.
    pub holder: NodeId,
    /// Lease expiry, milliseconds since the Unix epoch.
    pub expires_at_millis: u64,
}

/// A single range descriptor — the placement driver's authoritative
/// record of where one `[start_key, end_key)` slice of the keyspace
/// lives.
///
/// The span is half-open: `start_key` inclusive, `end_key` exclusive.
/// An empty `end_key` byte-vector denotes +infinity, i.e. the range
/// extends to the top of the keyspace.
///
/// `epoch` bumps on every membership change (add / remove / promote
/// replica). `generation` bumps on every split / merge. Together they
/// let the catalog and the range leaders detect stale reconfig
/// attempts without a heavier Lamport clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeDescriptor {
    /// Globally unique range identifier.
    pub range_id: RangeId,
    /// Inclusive lower bound of the range's keyspace.
    pub start_key: Vec<u8>,
    /// Exclusive upper bound of the range's keyspace. Empty means
    /// "+infinity" (the top of the keyspace).
    pub end_key: Vec<u8>,
    /// Replica placements for this range's Raft group.
    pub replicas: Vec<ReplicaPlacement>,
    /// Raft group replicating this range. Usually equal to
    /// [`Self::range_id`], but kept distinct for flexibility.
    pub raft_group_id: GroupId,
    /// Monotonically-increasing counter — bumps on every membership
    /// change.
    pub epoch: u64,
    /// Monotonically-increasing counter — bumps on every split /
    /// merge.
    pub generation: u64,
    /// Current range-leader lease, if any.
    pub lease: Option<LeaseInfo>,
}

impl RangeDescriptor {
    /// Build a brand-new range descriptor in "generation 0, epoch 0"
    /// form. Convenience for tests and bootstrap code; production
    /// paths normally construct via [`crate::PdCommand::CreateRange`].
    pub fn new(
        range_id: RangeId,
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
        replicas: Vec<ReplicaPlacement>,
    ) -> Self {
        Self {
            range_id,
            start_key: start_key.into(),
            end_key: end_key.into(),
            replicas,
            raft_group_id: range_id,
            epoch: 0,
            generation: 0,
            lease: None,
        }
    }

    /// Returns `true` if `key` lies in `[start_key, end_key)`.
    ///
    /// An empty `end_key` is treated as +infinity.
    pub fn contains(&self, key: &[u8]) -> bool {
        let ge_start = key >= self.start_key.as_slice();
        let lt_end = self.end_key.is_empty() || key < self.end_key.as_slice();
        ge_start && lt_end
    }

    /// Returns `true` if this range's span is non-empty. A range with
    /// `start_key == end_key` (neither open-ended) covers zero keys
    /// and is rejected by the catalog on insert.
    pub fn has_non_empty_span(&self) -> bool {
        if self.end_key.is_empty() {
            // +infinity upper bound → always non-empty.
            true
        } else {
            self.start_key.as_slice() < self.end_key.as_slice()
        }
    }
}

/// Lightweight description of a physical cluster node, keyed by
/// [`NodeId`]. Populated by the registration / heartbeat path in
/// Phase 2b-4; included here so the catalog already knows the shape
/// of the future row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique id of this node.
    pub node_id: NodeId,
    /// gRPC endpoint (`host:port`) used for Raft + admin traffic.
    pub address: String,
    /// Stores this node exposes.
    pub stores: Vec<StoreId>,
    /// Last heartbeat, Unix-millis. `0` means "never heartbeated".
    pub last_heartbeat_millis: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_constructors_build_expected_roles() {
        assert_eq!(ReplicaPlacement::voter(1, 1).role, ReplicaRole::Voter);
        assert_eq!(ReplicaPlacement::learner(1, 1).role, ReplicaRole::Learner);
    }

    #[test]
    fn range_descriptor_new_zero_initialises_counters() {
        let r = RangeDescriptor::new(7, b"a".to_vec(), b"z".to_vec(), vec![]);
        assert_eq!(r.range_id, 7);
        assert_eq!(r.raft_group_id, 7);
        assert_eq!(r.epoch, 0);
        assert_eq!(r.generation, 0);
        assert!(r.lease.is_none());
    }

    #[test]
    fn range_descriptor_contains_respects_bounds() {
        let r = RangeDescriptor::new(1, b"c".to_vec(), b"m".to_vec(), vec![]);
        assert!(r.contains(b"c"), "start is inclusive");
        assert!(r.contains(b"f"));
        assert!(!r.contains(b"m"), "end is exclusive");
        assert!(!r.contains(b"z"));
        assert!(!r.contains(b"a"));
    }

    #[test]
    fn range_descriptor_empty_end_key_is_infinity() {
        let r = RangeDescriptor::new(1, b"c".to_vec(), Vec::<u8>::new(), vec![]);
        assert!(r.contains(b"c"));
        assert!(r.contains(&[0xff; 64]));
        assert!(!r.contains(b"a"));
    }

    #[test]
    fn range_descriptor_has_non_empty_span_catches_zero_width() {
        // +infinity upper bound is always non-empty.
        let inf = RangeDescriptor::new(1, b"x".to_vec(), Vec::<u8>::new(), vec![]);
        assert!(inf.has_non_empty_span());

        // Ordinary non-empty span.
        let ok = RangeDescriptor::new(2, b"a".to_vec(), b"b".to_vec(), vec![]);
        assert!(ok.has_non_empty_span());

        // Zero-width span is rejected.
        let bad = RangeDescriptor::new(3, b"a".to_vec(), b"a".to_vec(), vec![]);
        assert!(!bad.has_non_empty_span());

        // Inverted span is rejected.
        let inverted = RangeDescriptor::new(4, b"b".to_vec(), b"a".to_vec(), vec![]);
        assert!(!inverted.has_non_empty_span());
    }

    #[test]
    fn range_descriptor_bincode_round_trip() {
        let original = RangeDescriptor {
            range_id: 42,
            start_key: b"alice".to_vec(),
            end_key: b"bob".to_vec(),
            replicas: vec![
                ReplicaPlacement::voter(1, 10),
                ReplicaPlacement::voter(2, 11),
                ReplicaPlacement::learner(3, 12),
            ],
            raft_group_id: 42,
            epoch: 3,
            generation: 1,
            lease: Some(LeaseInfo {
                holder: 1,
                expires_at_millis: 1_700_000_000_000,
            }),
        };
        let bytes = bincode::serialize(&original).unwrap();
        let restored: RangeDescriptor = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn node_info_bincode_round_trip() {
        let n = NodeInfo {
            node_id: 7,
            address: "192.168.1.10:7001".to_string(),
            stores: vec![1, 2],
            last_heartbeat_millis: 1_700_000_001_234,
        };
        let bytes = bincode::serialize(&n).unwrap();
        assert_eq!(bincode::deserialize::<NodeInfo>(&bytes).unwrap(), n);
    }
}
