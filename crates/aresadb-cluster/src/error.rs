//! Error types for cluster operations.
//!
//! Every failure mode the cluster crate surfaces lives here. We keep
//! the taxonomy small — callers almost never need to match specific
//! variants; they either log the error or bubble it up to the CLI.

use std::io;

use aresadb_raft::{NodeId, TypeConfig};
use openraft::error::{ChangeMembershipError, CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::BasicNode;

/// Result alias.
pub type ClusterResult<T, E = ClusterError> = std::result::Result<T, E>;

/// Errors returned by cluster lifecycle and admin operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// Problem reading or writing node state on disk.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// The storage backend (redb or memory) returned an error.
    #[error("storage: {0}")]
    Storage(#[from] aresadb_core::Error),

    /// openraft reported a problem (initialisation, election, write,
    /// membership change, etc.). We fold all of them into a single
    /// variant because the CLI treats them uniformly.
    #[error("raft: {0}")]
    Raft(String),

    /// Invalid configuration — usually a NodeConfig parameter.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Admin operation failed validation before reaching Raft.
    #[error("invalid admin request: {0}")]
    InvalidRequest(String),
}

/// Result alias for read-path operations.
pub type ReadResult<T> = std::result::Result<T, ReadError>;

/// Errors returned by the Phase 2c-5 range read path.
///
/// Every variant maps to a distinct operator-visible outcome. The
/// admin RPC layer turns these into tonic status codes
/// (`NotLeader` → `FAILED_PRECONDITION` with a leader-id hint in
/// metadata, `QuorumUnavailable` → `UNAVAILABLE`, storage → `INTERNAL`).
///
/// Kept deliberately separate from [`ClusterError`] so call sites
/// that fan out a read path don't have to match on write-path
/// variants like `Config` / `InvalidRequest`.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// This node isn't the leader for the range. The Option is the
    /// current leader as reported by openraft's `ForwardToLeader`
    /// error — present most of the time, `None` during an election
    /// when no leader is known.
    ///
    /// Callers should re-route the read (directly or via the PD
    /// catalog) rather than retry against the same member.
    #[error("not leader for this range; current leader: {0:?}")]
    NotLeader(Option<NodeId>),

    /// The linearizability guard failed because heartbeats couldn't
    /// reach a quorum of followers. Usually transient — a minority
    /// partition or a collection of slow peers.
    #[error("quorum unavailable for linearizable read: {0}")]
    QuorumUnavailable(String),

    /// openraft reported a fatal state (e.g. shutdown, storage
    /// corruption). The range is no longer usable without operator
    /// intervention.
    #[error("raft fatal: {0}")]
    Fatal(String),

    /// The underlying [`aresadb_core::StorageBackend`] returned an
    /// error while fetching the key.
    #[error("storage: {0}")]
    Storage(#[from] aresadb_core::Error),
}

impl From<RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>> for ReadError {
    fn from(err: RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>) -> Self {
        match err {
            RaftError::APIError(CheckIsLeaderError::ForwardToLeader(fw)) => {
                ReadError::NotLeader(fw.leader_id)
            }
            RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(q)) => {
                ReadError::QuorumUnavailable(q.to_string())
            }
            RaftError::Fatal(f) => ReadError::Fatal(f.to_string()),
        }
    }
}

/// Required only to keep the compiler from complaining about the
/// unused generic — explicit re-export so callers can name the
/// openraft type config that `ReadError`'s conversion depends on
/// without reaching into the openraft namespace themselves.
///
/// (We don't actually *need* to parameterize `ReadError` on the
/// type config because the cluster crate only has one:
/// [`TypeConfig`]. This private shim exists so a future split into
/// multiple type configs can re-type the `From` impl without an
/// API break.)
#[allow(dead_code)]
type ClusterTypeConfig = TypeConfig;

/// Result alias for range-aware write-path operations.
pub type WriteResult<T> = std::result::Result<T, WriteError>;

/// Errors returned by the Phase 2c-6 range write path.
///
/// Mirrors the shape of [`ReadError`] so operators see consistent
/// taxonomy across reads and writes. The admin RPC layer maps:
///
/// * `NotLeader` → `FAILED_PRECONDITION` with the current leader's
///   id in an `x-aresa-leader-id` metadata header.
/// * `RangeNotFound` → `NOT_FOUND` so clients can tell the range
///   isn't hosted here rather than re-trying.
/// * `InvalidMembership` → `FAILED_PRECONDITION`.
/// * `Fatal` → `INTERNAL`.
/// * `Storage` → `INTERNAL`.
///
/// Kept separate from [`ClusterError`] because write-path call sites
/// (the admin `Write` RPC, future SDK helpers) benefit from a narrow
/// enum they can pattern-match on without carrying CLI-level
/// variants like `Config` or `InvalidRequest` along for the ride.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The node receiving the write isn't the leader for the target
    /// range. The `Option` is the current leader as reported by
    /// openraft's `ForwardToLeader` — present most of the time,
    /// `None` during an election.
    #[error("not leader for this range; current leader: {0:?}")]
    NotLeader(Option<NodeId>),

    /// The caller targeted a `range_id` that isn't registered on
    /// this node. Fatal to the write — operators should route to
    /// a node that actually hosts the range, via the PD catalog.
    #[error("range {0} is not registered on this node")]
    RangeNotFound(u64),

    /// Raft-level membership check failed while replicating the
    /// batch (e.g. the membership change the caller piggy-backed on
    /// contradicts the current config). Surfaced verbatim so the
    /// operator can inspect the rejection reason.
    #[error("membership change rejected: {0}")]
    InvalidMembership(String),

    /// openraft reported a fatal state (shutdown, storage corruption).
    #[error("raft fatal: {0}")]
    Fatal(String),

    /// The underlying [`aresadb_core::StorageBackend`] returned an
    /// error while applying the batch.
    #[error("storage: {0}")]
    Storage(#[from] aresadb_core::Error),
}

impl From<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>> for WriteError {
    fn from(err: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        match err {
            RaftError::APIError(ClientWriteError::ForwardToLeader(fw)) => {
                WriteError::NotLeader(fw.leader_id)
            }
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::InProgress(p),
            )) => WriteError::InvalidMembership(p.to_string()),
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::LearnerNotFound(e),
            )) => WriteError::InvalidMembership(e.to_string()),
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::EmptyMembership(e),
            )) => WriteError::InvalidMembership(e.to_string()),
            RaftError::Fatal(f) => WriteError::Fatal(f.to_string()),
        }
    }
}
