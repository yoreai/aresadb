//! Error type for catalog operations.
//!
//! Every mutation goes through a single `Catalog::apply` entry point
//! (or one of the typed helpers); every rejection produces a variant
//! of [`CatalogError`]. The variants are deliberately narrow so a
//! caller can react programmatically — e.g. retrying on
//! `NewRangeIdTaken` but giving up on `OverlappingRange`.

use thiserror::Error;

use crate::types::{NodeId, RangeId, StoreId};

/// Reason the catalog rejected a mutation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogError {
    /// Caller tried to create a range whose id is already in use.
    #[error("range {0} already exists")]
    RangeAlreadyExists(RangeId),

    /// Caller referenced a range id that does not exist.
    #[error("range {0} not found")]
    RangeNotFound(RangeId),

    /// Caller referenced a node id that was never registered.
    #[error("node {0} has not been registered")]
    NodeNotRegistered(NodeId),

    /// Caller tried to create a range whose raft-group id is already
    /// in use by a different range.
    #[error("raft group {group_id} already owned by range {owner}")]
    GroupIdInUse {
        /// The group id that clashed.
        group_id: RangeId,
        /// The range that already owns `group_id`.
        owner: RangeId,
    },

    /// The proposed span overlaps an existing range.
    #[error("range span [{start_hex}, {end_hex}) overlaps existing range {conflict}")]
    OverlappingRange {
        /// Lower bound of the rejected span (hex-encoded).
        start_hex: String,
        /// Upper bound of the rejected span (hex-encoded). `+inf` if
        /// the original span was +infinity.
        end_hex: String,
        /// Range that the span collided with.
        conflict: RangeId,
    },

    /// The proposed span has `start_key >= end_key` (with end_key
    /// bounded). A zero-width or inverted range is never valid.
    #[error("range span is empty or inverted: start_key >= end_key")]
    InvalidSpan,

    /// The split key is not strictly inside the parent range's span.
    #[error("split key {key_hex} must be strictly inside range {range_id}")]
    SplitKeyOutOfBounds {
        /// Range that was being split.
        range_id: RangeId,
        /// The rejected split key (hex-encoded).
        key_hex: String,
    },

    /// A membership change attempted to regress the range's epoch.
    #[error("epoch regression on range {range_id}: existing {existing}, attempted {attempted}")]
    EpochRegression {
        /// Range whose membership was being updated.
        range_id: RangeId,
        /// Existing epoch on the catalog.
        existing: u64,
        /// Epoch the command attempted to install.
        attempted: u64,
    },

    /// Caller tried to merge two ranges that are not adjacent in the
    /// keyspace.
    #[error("ranges {left} and {right} are not adjacent — left.end_key != right.start_key")]
    NotAdjacent {
        /// Left-hand operand of the merge.
        left: RangeId,
        /// Right-hand operand of the merge.
        right: RangeId,
    },

    /// Merge was rejected because the two ranges have different
    /// replica sets. Catalog invariant: merge must be preceded by
    /// reconfigurations that make the two ranges' placements
    /// identical.
    #[error("ranges {left} and {right} have different replica sets — rebalance before merge")]
    ReplicaSetMismatch {
        /// Left-hand operand of the merge.
        left: RangeId,
        /// Right-hand operand of the merge.
        right: RangeId,
    },

    /// A replica placement referenced an unknown store. Reserved
    /// for a later enforcement pass that validates replica placements
    /// against the `NodeInfo.stores` inventory at `create_range` /
    /// `split_range` / `update_membership` time; kept in the error
    /// enum now so the on-wire shape is stable across that future
    /// rollout.
    #[error("replica placement references unknown store {0}")]
    UnknownStore(StoreId),
}
