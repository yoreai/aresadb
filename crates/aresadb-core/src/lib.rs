//! # aresadb-core
//!
//! Core types and traits for the AresaDB v2 distributed architecture.
//!
//! This crate contains the abstract contract that every storage backend
//! implements. The goal is that the rest of the database — the Raft
//! state machine, the range sharder, the query engine — can be written
//! against this trait and swap the underlying engine (redb, fjall,
//! thread-per-core LSM, in-memory for tests) without any code changes.
//!
//! See `docs/architecture-v2.md` in the repository for the full design.
//!
//! ## What's in this crate
//!
//! - [`StorageBackend`] — the async trait every engine implements.
//! - [`WriteBatch`] — the unit of atomic write.
//! - [`KeyRange`] — byte-lexicographic range used for scans and snapshots.
//! - [`Snapshot`] — a point-in-time read view.
//! - [`KeyValueStream`] — async iterator over scan results.
//! - [`MemoryBackend`] — reference in-memory implementation for tests.
//! - [`keys`] — unified keyspace encoder/decoder for all five models.
//! - [`Error`] / [`Result`] — backend-level error type.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod error;
pub mod keys;
mod memory;
mod range;
mod snapshot;
mod stream;
mod trait_;
mod write_batch;

pub use error::{Error, Result};
pub use memory::MemoryBackend;
pub use range::KeyRange;
pub use snapshot::Snapshot;
pub use stream::{KeyValue, KeyValueStream};
pub use trait_::StorageBackend;
pub use write_batch::{WriteBatch, WriteOp};

/// Format version for on-disk data emitted by backends that implement
/// this trait. Bump when the binary layout of keys or values changes in
/// a way that is not backward-compatible.
pub const FORMAT_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_version_is_two() {
        assert_eq!(FORMAT_VERSION, 2);
    }
}
