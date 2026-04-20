//! Error-mapping helpers.
//!
//! openraft's `RaftLogStorage` and `RaftStateMachine` traits both
//! return `StorageError<C::NodeId>`, while our [`aresadb_core::Error`]
//! is an enum that speaks the backend's language. The helpers here
//! convert between the two so implementers can use `?` on backend
//! calls without spelling out the conversion every time.
//!
//! The helpers are generic over any node-id type that satisfies
//! openraft's [`openraft::NodeId`] bound, so the same
//! `LogStoreGeneric<C>` can serve both the user-data
//! [`crate::types::TypeConfig`] and the placement-driver
//! PdTypeConfig without duplicating this error-mapping code.
//!
//! They're also exposed to downstream crates (notably `aresadb-pd`'s
//! Raft state-machine adapter) — the mapping logic is identical there,
//! and duplicating it would be a subtle maintenance hazard (drift on
//! one side produces asymmetric error subjects / verbs that confuse
//! openraft's recovery path).
//!
//! ## Why the bound is `openraft::NodeId` specifically
//!
//! Every openraft type we funnel errors through — `StorageError`,
//! `StorageIOError`, `ErrorSubject`, `LogId` — has its own
//! `NID: openraft::NodeId` constraint, and `NodeId` is itself a
//! super-trait combining `NodeIdEssential` (Default, Display, Hash,
//! Ord, Clone, Copy in practice) with serde. Matching that one bound
//! here avoids a pile of `where` clauses at every call site.

use std::fmt::Debug;

use openraft::{ErrorSubject, ErrorVerb, NodeId, StorageError, StorageIOError};

/// Turn a backend or serialization error into a Raft `StorageError`.
///
/// We flag every backend failure as a "read/write state machine"
/// error by default; the caller can override via [`storage_err_ctx`]
/// when a more specific subject/verb pairing is known.
pub fn storage_err<N, E>(err: E) -> StorageError<N>
where
    N: NodeId,
    E: std::error::Error + 'static,
{
    let io = StorageIOError::<N>::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        openraft::AnyError::new(&err),
    );
    io.into()
}

/// Convert an error with explicit subject/verb.
pub fn storage_err_ctx<N, E>(subject: ErrorSubject<N>, verb: ErrorVerb, err: E) -> StorageError<N>
where
    N: NodeId,
    E: std::error::Error + 'static,
{
    let io = StorageIOError::<N>::new(subject, verb, openraft::AnyError::new(&err));
    io.into()
}

/// Tiny wrapper around `bincode` errors so the `Display` and `Debug`
/// impls compose with `AnyError::new`.
#[derive(Debug, thiserror::Error)]
#[error("bincode: {0}")]
pub struct BincodeError(#[from] pub Box<bincode::ErrorKind>);
