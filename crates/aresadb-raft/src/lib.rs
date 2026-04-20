//! # aresadb-raft
//!
//! Raft consensus for AresaDB v2 — a thin adapter layer that connects
//! [openraft]'s split `RaftLogStorage` + `RaftStateMachine` traits to
//! the engine-agnostic [`aresadb_core::StorageBackend`] trait.
//!
//! One instance of this crate represents **one Raft group replicating
//! one logical shard**. Phase 1 uses exactly one group per node (the
//! whole database). Phase 2's multi-raft scheduler instantiates many
//! of these per node, one per range.
//!
//! See [`crate::types::TypeConfig`] for the openraft type
//! configuration, [`crate::LogStore`] for the log-side adapter, and
//! [`crate::StateMachineStore`] for the application-side adapter.
//!
//! ## Separation of backends
//!
//! The log and the state machine each take their own
//! `Arc<dyn StorageBackend>`. Co-locating them on the same engine is
//! allowed but discouraged — the log wants fsync-heavy sequential
//! writes while the state machine wants sorted-run-friendly storage,
//! and we'll pick different engines for each in Phase 2 / 5.
//!
//! See the repository's `docs/architecture-v2.md` for the big-picture
//! design, and `docs/phase-status.md` for what's shipped versus
//! pending.
//!
//! [openraft]: https://docs.rs/openraft

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod command;
pub mod error;
pub mod log_store;
pub mod network;
pub mod single_node;
pub mod state_machine;
pub mod types;

pub use command::{AresaCommand, AresaResponse, SerializableWriteBatch, SerializableWriteOp};
pub use error::{storage_err, storage_err_ctx, BincodeError};
pub use log_store::{LogStore, LogStoreGeneric};
pub use network::{LoopbackConnection, LoopbackNetwork};
pub use single_node::SingleNode;
pub use state_machine::{SnapshotPayload, StateMachineStore, StoredSnapshot};
pub use types::{typ, NodeId, TypeConfig};
