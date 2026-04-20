//! Raft integration for the placement-driver state machine.
//!
//! This module glues the pure-catalog [`PdStateMachine`] into openraft:
//!
//! - [`config`] pins the `RaftTypeConfig` for the PD Raft group
//!   ([`PdTypeConfig`], [`typ`]).
//! - [`state_machine`] provides [`PdRaftStateMachine`], the wrapper
//!   that implements `RaftStateMachine<PdTypeConfig>` and
//!   `RaftSnapshotBuilder<PdTypeConfig>` on top of the
//!   catalog-owned [`PdStateMachine`], persisting
//!   `last_applied` / `last_membership` atomically with each
//!   catalog mutation.
//!
//! Design notes:
//!
//! - The PD Raft group has its **own** `RaftTypeConfig` — distinct
//!   from the user-data [`aresadb_raft::TypeConfig`] — because its
//!   replicated payload is [`crate::PdCommand`] rather than user
//!   writes. Keeping them separate means the PD group can evolve its
//!   wire format without touching user-data groups and vice versa.
//! - The log side of the PD group reuses [`aresadb_raft::LogStoreGeneric`]
//!   via the [`PdLogStore`] alias — the generic log store was
//!   explicitly parameterized in Phase 2b-3 step 1 so this crate
//!   could drop it in without a fork.
//! - The state-machine side is a thin adapter: it delegates catalog
//!   mutations to [`PdStateMachine`] and only owns the Raft-specific
//!   meta (applied log id, membership) plus the current snapshot.
//!
//! [`PdStateMachine`]: crate::PdStateMachine

pub mod cluster;
pub mod config;
pub mod router;
pub mod single_node;
pub mod state_machine;

pub use cluster::{MemberBackends, PdCluster, PdClusterMember};
pub use config::{typ, NodeId, PdTypeConfig};
pub use router::{PdRouter, PdRouterConnection, PdRouterNetwork};
pub use single_node::SinglePdNode;
pub use state_machine::{PdRaftStateMachine, PersistedPdMeta, SnapshotPayload, StoredSnapshot};

/// Raft log store parameterized to [`PdTypeConfig`].
///
/// A type alias over [`aresadb_raft::LogStoreGeneric`] so the PD
/// cluster harness can write `PdLogStore::new(backend)` without
/// restating the generic. Gives the log side of the PD Raft group
/// the same semantics (open/append/truncate/purge) as the user-data
/// log store.
pub type PdLogStore = aresadb_raft::LogStoreGeneric<PdTypeConfig>;
