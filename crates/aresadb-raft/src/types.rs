//! openraft type configuration.
//!
//! [`TypeConfig`] is the single place every other piece of the Raft
//! stack reaches for when it needs the concrete types a generic
//! openraft trait requires. Per openraft's convention, we define it
//! via [`openraft::declare_raft_types!`], which emits the boilerplate
//! `impl RaftTypeConfig` along with the associated-type equalities.

use std::io::Cursor;

use openraft::{BasicNode, TokioRuntime};

use crate::command::{AresaCommand, AresaResponse};

/// 64-bit monotonically-assigned node identifier.
///
/// We pick `u64` (openraft's default) for compactness on the wire and
/// in the log. Phase 1c's cluster bootstrapper allocates these from a
/// persistent counter so a re-added node keeps its old identity.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Openraft type configuration for AresaDB.
    ///
    /// Every openraft generic (log entries, requests, responses, the
    /// async runtime) is pinned here. Downstream crates can reach for
    /// the concrete types via the [`typ`] module without having to
    /// restate the bindings.
    pub TypeConfig:
        D            = AresaCommand,
        R            = AresaResponse,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

/// Concrete aliases for common openraft types, so callers don't have
/// to restate `openraft::SomeType<TypeConfig>` everywhere.
pub mod typ {
    use openraft::BasicNode;

    use super::{NodeId, TypeConfig};

    /// A Raft log entry carrying an [`crate::AresaCommand`].
    pub type Entry = openraft::Entry<TypeConfig>;

    /// Openraft's storage-error type parameterized to our `NodeId`.
    pub type StorageError = openraft::StorageError<NodeId>;

    /// A Raft log identifier (term + index).
    pub type LogId = openraft::LogId<NodeId>;

    /// A Raft vote (term + node-id + committed-marker).
    pub type Vote = openraft::Vote<NodeId>;

    /// Metadata accompanying a snapshot on the wire.
    pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, BasicNode>;

    /// First/last log id pair, returned by `get_log_state`.
    pub type LogState = openraft::LogState<TypeConfig>;

    /// A snapshot handle returned by the state machine.
    pub type Snapshot = openraft::Snapshot<TypeConfig>;

    /// Applied membership, as stored in the state machine.
    pub type StoredMembership = openraft::StoredMembership<NodeId, BasicNode>;

    /// The `Raft` handle, pre-parameterized for AresaDB's type config.
    ///
    /// Binding this lets downstream code type `Raft::new(...)` without
    /// having to restate the generic every time.
    pub type Raft = openraft::Raft<TypeConfig>;
}
