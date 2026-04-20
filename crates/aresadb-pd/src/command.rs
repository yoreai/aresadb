//! Placement-driver replicated commands.
//!
//! Every mutation to the placement catalog goes through one of these
//! commands. Phase 2b-2 wraps them in `aresadb_raft::AresaCommand`
//! payloads so the PD Raft group can replicate them across its 3-5
//! replicas; Phase 2b-1 (this file) only defines the wire format and
//! proves bincode round-trip.

use serde::{Deserialize, Serialize};

use crate::types::{LeaseInfo, NodeId, NodeInfo, RangeDescriptor, RangeId, ReplicaPlacement};

/// A single mutation applied to the placement catalog.
///
/// Every variant is independently idempotent given the catalog's
/// epoch / generation counters — replaying the same command twice is
/// either a no-op or a controlled error (e.g.
/// [`crate::CatalogError::EpochRegression`]). This is what lets the
/// PD Raft group safely replay its log on recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PdCommand {
    /// Register or update a physical node in the cluster inventory.
    /// Re-registering an existing node updates its address / store
    /// list but preserves its last-heartbeat timestamp.
    RegisterNode(NodeInfo),

    /// Mark a node as alive at `last_seen_millis`. Liveness decisions
    /// compare this against the current time minus an operator-
    /// configurable threshold.
    HeartbeatNode {
        /// Node reporting in.
        node_id: NodeId,
        /// Wall-clock timestamp of the heartbeat (Unix millis).
        last_seen_millis: u64,
    },

    /// Create a brand-new range. Used at cluster bootstrap (to create
    /// the genesis range covering the whole keyspace) and when a
    /// split produces a right-hand-side range. Rejected if the new
    /// range overlaps any existing range or re-uses an existing id.
    CreateRange(RangeDescriptor),

    /// Split an existing range at `split_key`. The parent range's
    /// `end_key` shrinks to `split_key`; a new range covering
    /// `[split_key, old_end)` is created with an id allocated from
    /// the catalog's `next_range_id` counter (equal to the new Raft
    /// group id by default). Both ranges' `generation` bumps.
    ///
    /// The id is **not** part of the command — the catalog allocates
    /// it during apply. This keeps the command deterministic across
    /// Raft replicas: every replica's counter is part of the
    /// replicated state, so every replica's apply produces the same
    /// new id.
    SplitRange {
        /// Range being split.
        parent_range_id: RangeId,
        /// Split point. Must lie strictly inside the parent's span
        /// (i.e. `parent.start_key < split_key < parent.end_key`
        /// when the parent is bounded; `parent.start_key < split_key`
        /// when the parent is unbounded on the right).
        split_key: Vec<u8>,
    },

    /// Merge two adjacent ranges. `left.end_key` must equal
    /// `right.start_key` and both must share a replica set (enforced
    /// later, in Phase 2b-3, by the coordinator; this variant just
    /// records the intent).
    MergeRanges {
        /// Left range (retained).
        left: RangeId,
        /// Right range (dissolved into `left`).
        right: RangeId,
    },

    /// Replace the replica set of a range atomically. Caller supplies
    /// the new epoch; the catalog rejects the command unless
    /// `new_epoch > existing.epoch`.
    UpdateMembership {
        /// Range whose replica set is changing.
        range_id: RangeId,
        /// Replacement replica placements.
        new_replicas: Vec<ReplicaPlacement>,
        /// New epoch for the range. Must be strictly greater than the
        /// range's current epoch.
        new_epoch: u64,
    },

    /// Update (or clear) the leader lease for a range.
    UpdateLease {
        /// Range whose lease is changing.
        range_id: RangeId,
        /// `Some` to install / renew a lease, `None` to clear it.
        lease: Option<LeaseInfo>,
    },
}

/// Response produced by applying a single [`PdCommand`]. Deliberately
/// stays serde-serializable so it can ride back over gRPC once the
/// admin surface lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PdResponse {
    /// Command applied successfully with no interesting payload.
    Ok,
    /// The range descriptor that the command produced or mutated. For
    /// [`PdCommand::SplitRange`] the response carries the *new* range
    /// (RHS); call [`crate::Catalog::get_range`] to see the mutated
    /// parent.
    Range(RangeDescriptor),
    /// Command rejected. The payload is a human-readable rendering of
    /// the underlying [`crate::CatalogError`].
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LeaseInfo, NodeInfo, ReplicaPlacement};

    fn sample_range() -> RangeDescriptor {
        RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"z".to_vec(),
            vec![ReplicaPlacement::voter(1, 1), ReplicaPlacement::voter(2, 1)],
        )
    }

    #[test]
    fn register_node_round_trips() {
        let cmd = PdCommand::RegisterNode(NodeInfo {
            node_id: 1,
            address: "127.0.0.1:7001".to_string(),
            stores: vec![1],
            last_heartbeat_millis: 0,
        });
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn heartbeat_round_trips() {
        let cmd = PdCommand::HeartbeatNode {
            node_id: 3,
            last_seen_millis: 1_700_000_000_000,
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn create_range_round_trips() {
        let cmd = PdCommand::CreateRange(sample_range());
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn split_range_round_trips() {
        let cmd = PdCommand::SplitRange {
            parent_range_id: 1,
            split_key: b"m".to_vec(),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn merge_ranges_round_trips() {
        let cmd = PdCommand::MergeRanges { left: 1, right: 2 };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn update_membership_round_trips() {
        let cmd = PdCommand::UpdateMembership {
            range_id: 1,
            new_replicas: vec![
                ReplicaPlacement::voter(1, 1),
                ReplicaPlacement::voter(2, 1),
                ReplicaPlacement::voter(3, 1),
            ],
            new_epoch: 5,
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn update_lease_round_trips() {
        let cmd = PdCommand::UpdateLease {
            range_id: 1,
            lease: Some(LeaseInfo {
                holder: 2,
                expires_at_millis: 1_700_000_005_000,
            }),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn update_lease_none_round_trips() {
        let cmd = PdCommand::UpdateLease {
            range_id: 1,
            lease: None,
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        assert_eq!(bincode::deserialize::<PdCommand>(&bytes).unwrap(), cmd);
    }

    #[test]
    fn response_variants_round_trip() {
        let ok = PdResponse::Ok;
        let range = PdResponse::Range(sample_range());
        let err = PdResponse::Error("range 7 not found".to_string());
        for resp in [ok, range, err] {
            let bytes = bincode::serialize(&resp).unwrap();
            assert_eq!(bincode::deserialize::<PdResponse>(&bytes).unwrap(), resp);
        }
    }
}
