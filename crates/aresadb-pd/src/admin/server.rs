//! Placement-driver admin tonic service.
//!
//! Wraps a Raft handle + the local [`PdStateMachine`] and exposes
//! them to the outside world via the `PlacementDriverAdmin` gRPC
//! service.
//!
//! Every mutating RPC boils down to: parse the protobuf →
//! translate into a [`PdCommand`] → call `raft.client_write(cmd)` →
//! fold the [`PdResponse`] back into a typed protobuf reply. Reads
//! skip Raft entirely and are served from this member's local
//! catalog.
//!
//! Error mapping is deliberate. There are three classes:
//!
//! 1. **Invalid arguments** (malformed pb, missing required fields,
//!    contradictory flags) — return `InvalidArgument`. These never
//!    hit the Raft log.
//! 2. **Catalog rejections** (overlap, duplicate id, non-adjacent
//!    merge, epoch regression, …) — return `FailedPrecondition`
//!    with the human-readable catalog error in the message. The
//!    command *did* replicate through Raft but the state machine
//!    declined to apply it.
//! 3. **Raft errors** (`ForwardToLeader`, `Shutdown`, network
//!    failures while replicating) — return `Unavailable`. For
//!    `ForwardToLeader` we attach the suggested leader id in the
//!    `pd-leader-id` response trailer so the admin client can
//!    retry in one RPC rather than probing every member.
//
// Every handler returns `Result<_, tonic::Status>` because that's
// what the tonic service trait requires. `Status` is ~176 bytes so
// clippy's `result_large_err` fires on every signature; the trait
// bounds make it impossible to box without rewriting tonic itself.
// Mirror the targeted allows used elsewhere in the crate for
// trait-adjacent APIs.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use aresadb_core::StorageBackend;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use openraft::Raft;
use serde_json::json;
use tonic::{async_trait, metadata::MetadataValue, Request, Response, Status};

use crate::command::{PdCommand, PdResponse};
use crate::raft::PdTypeConfig;
use crate::state_machine::PdStateMachine;

use super::convert::replica_role_from_i32;
use super::pb;
use super::pb::placement_driver_admin_server::PlacementDriverAdmin;

/// gRPC metadata key we attach to `Unavailable` responses when the
/// receiving node is a follower. The admin client reads it to route
/// the next attempt at the current leader.
pub const LEADER_HINT_METADATA: &str = "pd-leader-id";

/// Adapter wiring the local PD Raft handle + state machine to the
/// tonic service trait.
///
/// Cheap to construct and cheap to clone — both `Raft` and
/// `Arc<PdStateMachine>` are already internally `Arc`-backed.
pub struct PdAdminService {
    raft: Raft<PdTypeConfig>,
    state_machine: Arc<PdStateMachine>,
    /// Data backend. Kept so future RPCs can raw-read PD rows for
    /// consistency checks; unused by the current handlers.
    #[allow(dead_code)]
    data: Arc<dyn StorageBackend>,
}

impl PdAdminService {
    /// Construct a service bound to the given Raft handle + state
    /// machine. Typically called once per PD member.
    pub fn new(
        raft: Raft<PdTypeConfig>,
        state_machine: Arc<PdStateMachine>,
        data: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            raft,
            state_machine,
            data,
        }
    }

    /// Replicate `cmd` through the PD Raft group and return the
    /// resulting [`PdResponse`]. Folds the openraft error matrix
    /// down to tonic `Status` per the class table in the module
    /// docstring.
    async fn replicate(&self, cmd: PdCommand) -> Result<PdResponse, Status> {
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id,
                ..
            }))) => {
                let mut status = Status::unavailable(match leader_id {
                    Some(id) => format!("not leader; current leader is node {id}"),
                    None => "no leader currently elected".to_string(),
                });
                if let Some(id) = leader_id {
                    status
                        .metadata_mut()
                        .insert(LEADER_HINT_METADATA, MetadataValue::from(id));
                }
                Err(status)
            }
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => Err(
                Status::failed_precondition(format!("change_membership: {e}")),
            ),
            Err(RaftError::Fatal(f)) => Err(Status::internal(format!("raft fatal: {f}"))),
        }
    }

    /// Convert a [`PdResponse`] into either a typed payload or a
    /// tonic error. Catalog rejections become `FailedPrecondition`
    /// so clients can distinguish "your request was bad" from
    /// "I'm not the leader" or "I'm unreachable".
    fn unwrap_response(resp: PdResponse) -> Result<PdResponse, Status> {
        match &resp {
            PdResponse::Error(msg) => Err(Status::failed_precondition(msg.clone())),
            _ => Ok(resp),
        }
    }
}

#[async_trait]
impl PlacementDriverAdmin for PdAdminService {
    // ------------------------------------------------------------
    // Mutations
    // ------------------------------------------------------------

    async fn register_node(
        &self,
        request: Request<pb::RegisterNodeRequest>,
    ) -> Result<Response<pb::RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let node = req
            .node
            .ok_or_else(|| Status::invalid_argument("register_node: node is required"))?;
        if node.address.is_empty() {
            return Err(Status::invalid_argument(
                "register_node: address must be non-empty",
            ));
        }

        let resp = self.replicate(PdCommand::RegisterNode(node.into())).await?;
        let _ = Self::unwrap_response(resp)?;
        Ok(Response::new(pb::RegisterNodeResponse {}))
    }

    async fn heartbeat_node(
        &self,
        request: Request<pb::HeartbeatNodeRequest>,
    ) -> Result<Response<pb::HeartbeatNodeResponse>, Status> {
        let req = request.into_inner();
        if req.node_id == 0 {
            return Err(Status::invalid_argument(
                "heartbeat_node: node_id must be non-zero",
            ));
        }

        let resp = self
            .replicate(PdCommand::HeartbeatNode {
                node_id: req.node_id,
                last_seen_millis: req.last_seen_millis,
            })
            .await?;
        let _ = Self::unwrap_response(resp)?;
        Ok(Response::new(pb::HeartbeatNodeResponse {}))
    }

    async fn create_range(
        &self,
        request: Request<pb::CreateRangeRequest>,
    ) -> Result<Response<pb::CreateRangeResponse>, Status> {
        let req = request.into_inner();
        let pb_range = req
            .range
            .ok_or_else(|| Status::invalid_argument("create_range: range is required"))?;
        let desc = pb_range.try_into()?;

        let resp = self.replicate(PdCommand::CreateRange(desc)).await?;
        match Self::unwrap_response(resp)? {
            PdResponse::Range(stored) => Ok(Response::new(pb::CreateRangeResponse {
                range: Some(stored.into()),
            })),
            other => Err(Status::internal(format!(
                "create_range returned unexpected response: {other:?}"
            ))),
        }
    }

    async fn split_range(
        &self,
        request: Request<pb::SplitRangeRequest>,
    ) -> Result<Response<pb::SplitRangeResponse>, Status> {
        let req = request.into_inner();
        if req.parent_range_id == 0 {
            return Err(Status::invalid_argument(
                "split_range: parent_range_id must be non-zero",
            ));
        }
        if req.split_key.is_empty() {
            return Err(Status::invalid_argument(
                "split_range: split_key must be non-empty",
            ));
        }

        let resp = self
            .replicate(PdCommand::SplitRange {
                parent_range_id: req.parent_range_id,
                split_key: req.split_key,
            })
            .await?;
        match Self::unwrap_response(resp)? {
            PdResponse::Range(rhs) => Ok(Response::new(pb::SplitRangeResponse {
                new_range: Some(rhs.into()),
            })),
            other => Err(Status::internal(format!(
                "split_range returned unexpected response: {other:?}"
            ))),
        }
    }

    async fn merge_ranges(
        &self,
        request: Request<pb::MergeRangesRequest>,
    ) -> Result<Response<pb::MergeRangesResponse>, Status> {
        let req = request.into_inner();
        if req.left_range_id == 0 || req.right_range_id == 0 {
            return Err(Status::invalid_argument(
                "merge_ranges: left and right range ids must be non-zero",
            ));
        }

        let resp = self
            .replicate(PdCommand::MergeRanges {
                left: req.left_range_id,
                right: req.right_range_id,
            })
            .await?;
        let _ = Self::unwrap_response(resp)?;
        Ok(Response::new(pb::MergeRangesResponse {}))
    }

    async fn update_membership(
        &self,
        request: Request<pb::UpdateMembershipRequest>,
    ) -> Result<Response<pb::UpdateMembershipResponse>, Status> {
        let req = request.into_inner();
        if req.range_id == 0 {
            return Err(Status::invalid_argument(
                "update_membership: range_id must be non-zero",
            ));
        }
        if req.new_replicas.is_empty() {
            return Err(Status::invalid_argument(
                "update_membership: new_replicas must be non-empty",
            ));
        }

        let mut new_replicas = Vec::with_capacity(req.new_replicas.len());
        for rp in req.new_replicas {
            // Inline translation because the field is already parsed
            // from the outer request; we want all invalid-argument
            // errors to surface *before* contacting Raft so we don't
            // waste a round trip on obviously bad input.
            new_replicas.push(crate::types::ReplicaPlacement {
                node_id: rp.node_id,
                store_id: rp.store_id,
                role: replica_role_from_i32(rp.role)?,
            });
        }

        let resp = self
            .replicate(PdCommand::UpdateMembership {
                range_id: req.range_id,
                new_replicas,
                new_epoch: req.new_epoch,
            })
            .await?;
        let _ = Self::unwrap_response(resp)?;
        Ok(Response::new(pb::UpdateMembershipResponse {}))
    }

    async fn update_lease(
        &self,
        request: Request<pb::UpdateLeaseRequest>,
    ) -> Result<Response<pb::UpdateLeaseResponse>, Status> {
        let req = request.into_inner();
        if req.range_id == 0 {
            return Err(Status::invalid_argument(
                "update_lease: range_id must be non-zero",
            ));
        }
        if req.clear && req.lease.is_some() {
            return Err(Status::invalid_argument(
                "update_lease: cannot set both lease and clear",
            ));
        }
        if !req.clear && req.lease.is_none() {
            return Err(Status::invalid_argument(
                "update_lease: must either set lease or set clear=true",
            ));
        }

        let lease = req.lease.map(Into::into);
        let resp = self
            .replicate(PdCommand::UpdateLease {
                range_id: req.range_id,
                lease,
            })
            .await?;
        let _ = Self::unwrap_response(resp)?;
        Ok(Response::new(pb::UpdateLeaseResponse {}))
    }

    // ------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------

    async fn get_range(
        &self,
        request: Request<pb::GetRangeRequest>,
    ) -> Result<Response<pb::GetRangeResponse>, Status> {
        let req = request.into_inner();
        if req.range_id == 0 {
            return Err(Status::invalid_argument(
                "get_range: range_id must be non-zero",
            ));
        }
        let found: Option<crate::types::RangeDescriptor> = self
            .state_machine
            .read(|c| c.get_range(req.range_id).cloned());
        Ok(Response::new(pb::GetRangeResponse {
            found: found.is_some(),
            range: found.map(Into::into),
        }))
    }

    async fn get_range_by_key(
        &self,
        request: Request<pb::GetRangeByKeyRequest>,
    ) -> Result<Response<pb::GetRangeByKeyResponse>, Status> {
        let req = request.into_inner();
        // Empty key == "the start of the keyspace", which is a valid
        // lookup target (it resolves to the range starting at "").
        // Don't reject it.
        let found: Option<crate::types::RangeDescriptor> = self
            .state_machine
            .read(|c| c.find_range_for_key(&req.key).cloned());
        Ok(Response::new(pb::GetRangeByKeyResponse {
            found: found.is_some(),
            range: found.map(Into::into),
        }))
    }

    async fn list_ranges(
        &self,
        _request: Request<pb::ListRangesRequest>,
    ) -> Result<Response<pb::ListRangesResponse>, Status> {
        let ranges: Vec<pb::RangeDescriptorPb> = self
            .state_machine
            .read(|c| c.iter_ranges_by_start().cloned().collect::<Vec<_>>())
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(Response::new(pb::ListRangesResponse { ranges }))
    }

    async fn get_node(
        &self,
        request: Request<pb::GetNodeRequest>,
    ) -> Result<Response<pb::GetNodeResponse>, Status> {
        let req = request.into_inner();
        if req.node_id == 0 {
            return Err(Status::invalid_argument(
                "get_node: node_id must be non-zero",
            ));
        }
        let found: Option<crate::types::NodeInfo> = self
            .state_machine
            .read(|c| c.get_node(req.node_id).cloned());
        Ok(Response::new(pb::GetNodeResponse {
            found: found.is_some(),
            node: found.map(Into::into),
        }))
    }

    async fn list_nodes(
        &self,
        _request: Request<pb::ListNodesRequest>,
    ) -> Result<Response<pb::ListNodesResponse>, Status> {
        let nodes: Vec<pb::NodeInfoPb> = self
            .state_machine
            .read(|c| c.iter_nodes().cloned().collect::<Vec<_>>())
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(Response::new(pb::ListNodesResponse { nodes }))
    }

    async fn status(
        &self,
        _request: Request<pb::PdStatusRequest>,
    ) -> Result<Response<pb::PdStatusResponse>, Status> {
        // Drop the metrics guard before any await by cloning.
        let metrics = self.raft.metrics().borrow().clone();
        let membership_voters: Vec<_> =
            metrics.membership_config.membership().voter_ids().collect();
        let membership_learners: Vec<_> = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect();

        let (range_count, node_count, next_range_id) = self.state_machine.read(|c| {
            (
                c.range_count(),
                c.iter_nodes().count(),
                c.peek_next_range_id(),
            )
        });

        let payload = json!({
            "node_id": metrics.id,
            "current_leader": metrics.current_leader,
            "current_term": metrics.current_term,
            "last_log_index": metrics.last_log_index,
            "last_applied": metrics.last_applied.map(|l| json!({
                "term": l.leader_id.term,
                "index": l.index,
            })),
            "state": format!("{:?}", metrics.state),
            "membership": {
                "voters": membership_voters,
                "learners": membership_learners,
            },
            "catalog": {
                "range_count": range_count,
                "node_count": node_count,
                "next_range_id": next_range_id,
            },
        });

        let bytes = serde_json::to_vec(&payload)
            .map_err(|e| Status::internal(format!("status json encode: {e}")))?;
        Ok(Response::new(pb::PdStatusResponse { json: bytes }))
    }
}
