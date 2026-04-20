//! Openraft type configuration for the placement-driver Raft group.
//!
//! The PD runs its own small (3-5 node) Raft group, separate from the
//! per-range user-data groups defined in [`aresadb_raft::types`]. It
//! replicates only [`PdCommand`]s — the cluster's view of which range
//! lives where — and therefore needs its own [`RaftTypeConfig`]
//! pinning `D = PdCommand` / `R = PdResponse` instead of the user-data
//! batch types.
//!
//! The configuration is intentionally parallel to the user-data one:
//!
//! - `NodeId = u64`: same dense, monotonically-assigned id space the
//!   user-data groups use. A single physical node can therefore join
//!   both the PD group and any number of user-data groups under the
//!   same identity — which is what Phase 2b-4's heartbeat loop and
//!   Phase 2c's range-aware `ClusterNode` will need.
//! - `Node = BasicNode`: `{ addr: String }`. Rich enough for the
//!   in-process tests in Phase 2b-3 and for the gRPC admin surface in
//!   Phase 2b-4.
//! - `SnapshotData = Cursor<Vec<u8>>`: snapshots are bincode-encoded
//!   [`Catalog`] dumps, small by construction (one descriptor per
//!   range, one row per node). An in-memory cursor is the simplest
//!   transport.
//! - `AsyncRuntime = TokioRuntime`: the rest of the stack is Tokio, so
//!   the PD group is too.
//!
//! Keeping the config here — instead of in `aresadb-raft` — lets the
//! PD crate own its wire format without leaking `PdCommand` /
//! `PdResponse` into the user-data Raft module.
//!
//! [`PdCommand`]: crate::PdCommand
//! [`Catalog`]: crate::Catalog
//! [`RaftTypeConfig`]: openraft::RaftTypeConfig

use std::io::Cursor;

use openraft::{BasicNode, TokioRuntime};

use crate::command::{PdCommand, PdResponse};

/// 64-bit monotonically-assigned node identifier.
///
/// Shared keyspace with [`aresadb_raft::types::NodeId`] so that a
/// single physical node carries one identity across every Raft group
/// it participates in. Redeclared as an alias here (instead of a
/// re-export) so `aresadb-pd` does not need to depend on
/// `aresadb-raft`.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Openraft type configuration for the PD Raft group.
    ///
    /// Mirrors [`aresadb_raft::types::TypeConfig`] but replaces the
    /// user-data command / response payloads with the placement
    /// driver's [`PdCommand`] and [`PdResponse`]. Downstream code
    /// (state-machine adapter, router, cluster harness) reaches for
    /// the concrete openraft generics via the [`typ`] module.
    pub PdTypeConfig:
        D            = PdCommand,
        R            = PdResponse,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<PdTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

/// Concrete aliases for common openraft types pinned to
/// [`PdTypeConfig`], so callers don't have to restate
/// `openraft::SomeType<PdTypeConfig>` everywhere.
pub mod typ {
    use openraft::BasicNode;

    use super::{NodeId, PdTypeConfig};

    /// A Raft log entry carrying a [`crate::PdCommand`].
    pub type Entry = openraft::Entry<PdTypeConfig>;

    /// Openraft's storage-error type parameterized to our `NodeId`.
    pub type StorageError = openraft::StorageError<NodeId>;

    /// A Raft log identifier (term + index).
    pub type LogId = openraft::LogId<NodeId>;

    /// A Raft vote (term + node-id + committed-marker).
    pub type Vote = openraft::Vote<NodeId>;

    /// Metadata accompanying a snapshot on the wire.
    pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, BasicNode>;

    /// First/last log id pair, returned by `get_log_state`.
    pub type LogState = openraft::LogState<PdTypeConfig>;

    /// A snapshot handle returned by the state machine.
    pub type Snapshot = openraft::Snapshot<PdTypeConfig>;

    /// Applied membership, as stored in the state machine.
    pub type StoredMembership = openraft::StoredMembership<NodeId, BasicNode>;

    /// The `Raft` handle, pre-parameterized for the PD type config.
    ///
    /// Binding this lets downstream code type `Raft::new(...)` without
    /// having to restate the generic every time.
    pub type Raft = openraft::Raft<PdTypeConfig>;
}

#[cfg(test)]
mod tests {
    use openraft::RaftTypeConfig;

    use super::*;

    /// Sanity check: the declared type config actually resolves to the
    /// command / response payloads we expect. If this ever breaks, the
    /// `declare_raft_types!` macro has shifted under us and every
    /// downstream `D`/`R` bound will need revisiting.
    #[test]
    fn pd_type_config_binds_pd_command_and_response() {
        fn assert_d_eq<T: RaftTypeConfig<D = PdCommand>>() {}
        fn assert_r_eq<T: RaftTypeConfig<R = PdResponse>>() {}
        fn assert_node_id_eq<T: RaftTypeConfig<NodeId = NodeId>>() {}

        assert_d_eq::<PdTypeConfig>();
        assert_r_eq::<PdTypeConfig>();
        assert_node_id_eq::<PdTypeConfig>();
    }

    /// Entry round-trip through bincode — catches accidental changes
    /// to the log wire format.
    #[test]
    fn pd_entry_bincode_round_trips() {
        use crate::types::{RangeDescriptor, ReplicaPlacement};

        let payload = PdCommand::CreateRange(RangeDescriptor::new(
            1,
            b"a".to_vec(),
            b"z".to_vec(),
            vec![ReplicaPlacement::voter(1, 1)],
        ));
        let entry = typ::Entry {
            log_id: typ::LogId::new(openraft::CommittedLeaderId::new(1, 1), 7),
            payload: openraft::EntryPayload::Normal(payload.clone()),
        };
        let bytes = bincode::serialize(&entry).expect("serialize");
        let restored: typ::Entry = bincode::deserialize(&bytes).expect("deserialize");
        match restored.payload {
            openraft::EntryPayload::Normal(cmd) => assert_eq!(cmd, payload),
            other => panic!("expected Normal payload, got {:?}", other),
        }
        assert_eq!(restored.log_id.index, 7);
    }
}
