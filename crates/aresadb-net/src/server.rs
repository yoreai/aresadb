//! gRPC server that dispatches Raft RPCs into `openraft::Raft`.
//!
//! Each handler does the same four things:
//!   1. look up the target `Raft<TypeConfig>` by `raft_group_id`,
//!   2. bincode-decode the payload into the typed openraft request,
//!   3. call the matching method on `openraft::Raft`,
//!   4. bincode-encode the response or logical error back into the
//!      protobuf envelope, using `is_error` to discriminate.
//!
//! Transport-level failures (bad bytes, cancelled request, missing
//! group) come out as `tonic::Status`; logical Raft failures travel
//! as a serialized `RaftError` with `is_error = true`. The client
//! side (`client.rs`) mirrors that split so both halves stay honest.
//!
//! Phase 2c — multi-Raft: the server holds a [`RaftDirectory`] that
//! resolves a wire `raft_group_id` to a specific `Raft<TypeConfig>`
//! handle. Phase 1 single-group callers send
//! [`SINGLETON_RAFT_GROUP_ID`][crate::client::SINGLETON_RAFT_GROUP_ID];
//! the [`SingletonRaftDirectory`] adapter just returns the same
//! handle for every id so existing single-shard deployments work
//! unchanged.

// `tonic::Status` is ~176 bytes and every RPC handler plus the
// `resolve` helper returns a `Result<_, Status>`. Boxing `Status`
// would force us to fight tonic's generated trait signatures, so we
// accept the lint at the module boundary instead — same trade-off
// as `aresadb-pd::admin::server`.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::Raft;
use tonic::{async_trait, Request, Response, Status};
use tracing::{debug, instrument};

use aresadb_raft::{NodeId, TypeConfig};

use crate::codec::{decode, encode, to_status};
use crate::pb;
use crate::pb::raft_service_server::{RaftService, RaftServiceServer};

/// Resolves a wire `raft_group_id` to a live `Raft<TypeConfig>`
/// handle on this node.
///
/// The directory is consulted on every inbound RPC and therefore
/// must be safe to share across tasks and cheap to call. In Phase 2c
/// the `aresadb-cluster::RangeDirectory` will implement this trait
/// directly so the gRPC dispatch path is "dictionary lookup → one
/// openraft method call" with no extra locking.
pub trait RaftDirectory: Send + Sync + 'static {
    /// Return the Raft handle registered under `raft_group_id`, or
    /// `None` if the node does not host that group (which the server
    /// translates into `tonic::Status::not_found` so the caller can
    /// reconcile with the PD). Implementations should be O(1) or
    /// close to it — this lands on the critical path of every
    /// `AppendEntries`.
    fn raft_for(&self, raft_group_id: u64) -> Option<Raft<TypeConfig>>;
}

/// Directory adapter for Phase 1 single-group deployments: every
/// wire id (including `0` = [`SINGLETON_RAFT_GROUP_ID`]) routes to
/// the same `Raft` handle.
///
/// Kept as a distinct type rather than a closure so construction
/// paths that used to pass a bare `Raft<TypeConfig>` don't have to
/// learn about directory trait objects.
///
/// [`SINGLETON_RAFT_GROUP_ID`]: crate::client::SINGLETON_RAFT_GROUP_ID
#[derive(Clone)]
pub struct SingletonRaftDirectory {
    raft: Raft<TypeConfig>,
}

impl SingletonRaftDirectory {
    /// Wrap a single Raft handle.
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
    }
}

impl RaftDirectory for SingletonRaftDirectory {
    fn raft_for(&self, _raft_group_id: u64) -> Option<Raft<TypeConfig>> {
        Some(self.raft.clone())
    }
}

/// Adapter that turns a [`RaftDirectory`] into a tonic service. One
/// instance per node — it fans every incoming RPC out to the Raft
/// group whose id the wire envelope carries.
#[derive(Clone)]
pub struct RaftGrpcServer {
    directory: Arc<dyn RaftDirectory>,
}

impl RaftGrpcServer {
    /// Phase 1 convenience: wrap a single `Raft<TypeConfig>` and
    /// serve every incoming RPC from it, regardless of wire
    /// `raft_group_id`.
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self::from_directory(Arc::new(SingletonRaftDirectory::new(raft)))
    }

    /// Phase 2c: wrap a directory so one listener can fan out across
    /// many Raft groups.
    pub fn from_directory(directory: Arc<dyn RaftDirectory>) -> Self {
        Self { directory }
    }

    /// Convert into the tonic service so callers can plug into
    /// `Server::builder().add_service(...)`.
    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
    }

    fn resolve(&self, raft_group_id: u64) -> Result<Raft<TypeConfig>, Status> {
        self.directory.raft_for(raft_group_id).ok_or_else(|| {
            Status::not_found(format!(
                "no raft group registered on this node for id {raft_group_id}"
            ))
        })
    }
}

#[async_trait]
impl RaftService for RaftGrpcServer {
    #[instrument(level = "debug", skip_all)]
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let body = request.into_inner();
        let raft = self.resolve(body.raft_group_id)?;

        let req: AppendEntriesRequest<TypeConfig> = decode(&body.payload).map_err(to_status)?;
        debug!(
            raft_group_id = body.raft_group_id,
            entries = req.entries.len(),
            "append_entries request"
        );

        match raft.append_entries(req).await {
            Ok(resp) => {
                let payload = encode(&resp).map_err(to_status)?;
                Ok(Response::new(pb::AppendEntriesResponse {
                    payload,
                    is_error: false,
                }))
            }
            Err(e) => {
                let payload = encode(&e).map_err(to_status)?;
                Ok(Response::new(pb::AppendEntriesResponse {
                    payload,
                    is_error: true,
                }))
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let body = request.into_inner();
        let raft = self.resolve(body.raft_group_id)?;

        let req: VoteRequest<NodeId> = decode(&body.payload).map_err(to_status)?;

        match raft.vote(req).await {
            Ok(resp) => {
                let payload = encode(&resp).map_err(to_status)?;
                Ok(Response::new(pb::VoteResponse {
                    payload,
                    is_error: false,
                }))
            }
            Err(e) => {
                let payload = encode(&e).map_err(to_status)?;
                Ok(Response::new(pb::VoteResponse {
                    payload,
                    is_error: true,
                }))
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let body = request.into_inner();
        let raft = self.resolve(body.raft_group_id)?;

        let req: InstallSnapshotRequest<TypeConfig> = decode(&body.payload).map_err(to_status)?;

        match raft.install_snapshot(req).await {
            Ok(resp) => {
                let payload = encode(&resp).map_err(to_status)?;
                Ok(Response::new(pb::InstallSnapshotResponse {
                    payload,
                    is_error: false,
                }))
            }
            Err(e) => {
                let payload = encode(&e).map_err(to_status)?;
                Ok(Response::new(pb::InstallSnapshotResponse {
                    payload,
                    is_error: true,
                }))
            }
        }
    }
}
