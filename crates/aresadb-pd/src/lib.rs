//! # aresadb-pd
//!
//! Placement-driver catalog for AresaDB v2. This crate owns the
//! cluster's view of *which range lives where*: every range has a
//! [`RangeDescriptor`] (id, span, replica placement, Raft group id,
//! epoch, generation, lease), and the [`Catalog`] is the pure-logic
//! index over those descriptors. Catalog mutations go through a
//! single replicated command type [`PdCommand`] so Phase 2b-2 can
//! wrap the catalog in a Raft state machine with zero interface
//! churn.
//!
//! Split into layers the same way `aresadb-raft` is:
//!
//! - [`types`] — `RangeDescriptor`, `ReplicaPlacement`, `NodeInfo`,
//!   etc. Pure data, serde-serializable, bincode round-trip.
//! - [`command`] — `PdCommand` / `PdResponse`, the replicated log
//!   entry.
//! - [`error`] — `CatalogError`, one variant per rejection reason.
//! - [`catalog`] — `Catalog`, the in-memory, non-thread-safe,
//!   invariant-enforcing catalog.
//! - [`persist`] — on-disk key layout (`/m/pd/r/*`, `/m/pd/n/*`) and
//!   the encode / decode helpers that wrap it.
//! - [`state_machine`] — `PdStateMachine`, the persistent adapter
//!   that binds a [`Catalog`] to an
//!   [`aresadb_core::StorageBackend`]. Applies every command
//!   atomically to backend + memory and rehydrates on `open`.
//! - [`raft`] — openraft [`RaftTypeConfig`] and state-machine
//!   adapter wiring [`PdStateMachine`] into a PD Raft group.
//! - [`admin`] — tonic admin service, typed client, and node-side
//!   heartbeat loop. This is the control-plane surface the operator
//!   CLI talks to.
//!
//! See `docs/architecture-v2.md` §3.2 for the high-level design of
//! the placement driver and `docs/phase-status.md` for what each
//! Phase 2 slice ships.
//!
//! [`RaftTypeConfig`]: openraft::RaftTypeConfig

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod admin;
pub mod catalog;
pub mod command;
pub mod error;
pub mod persist;
pub mod raft;
pub mod state_machine;
pub mod types;

pub use admin::{
    HeartbeatConfig, HeartbeatHandle, HeartbeatLoop, PdAdminClient, PdAdminClientError,
    PdAdminService, PlacementDriverAdmin, PlacementDriverAdminClient, PlacementDriverAdminServer,
};
pub use catalog::Catalog;
pub use command::{PdCommand, PdResponse};
pub use error::CatalogError;
pub use raft::{
    MemberBackends, PdCluster, PdClusterMember, PdLogStore, PdRaftStateMachine, PdRouter,
    PdRouterConnection, PdRouterNetwork, PdTypeConfig, PersistedPdMeta, SinglePdNode,
    SnapshotPayload, StoredSnapshot,
};
pub use state_machine::{PdApplyError, PdStateMachine};
pub use types::{
    GroupId, LeaseInfo, NodeId, NodeInfo, RangeDescriptor, RangeId, ReplicaPlacement, ReplicaRole,
    StoreId,
};
