//! Cluster admin gRPC service.
//!
//! This is the control plane. The CLI talks to it, and eventually the
//! client SDK and Kubernetes operator will too. The service sits on
//! the same TCP port as the Raft peer transport so each node needs
//! only a single listener.
//!
//! Every handler is thin: parse the protobuf, call one openraft
//! method, package the reply. Anything more complex belongs in
//! helpers — keep the wire surface honest.

// `tonic::Status` is ~176 bytes, which trips
// `clippy::result_large_err` on every fallible helper whose error
// path we want to surface to tonic. Boxing would force us to
// re-wrap on the trait-impl side (tonic's generated signatures take
// `Result<Response<T>, Status>` by value), so we take the same
// module-wide allow that `aresadb-net::server` uses.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aresadb_core::StorageBackend;
use aresadb_net::{GrpcRaftNetwork, StaticPeerDirectory};
use aresadb_pd::types::{
    LeaseInfo, RangeDescriptor as CatalogRangeDescriptor, RangeId as PdRangeId,
    ReplicaPlacement as CatalogReplicaPlacement, ReplicaRole,
};
use aresadb_raft::{AresaCommand, NodeId, SerializableWriteBatch, TypeConfig};
use openraft::{BasicNode, Config, Raft};
use serde_json::json;
use tonic::{async_trait, Request, Response, Status};

use crate::config::NodeConfig;
use crate::error::{ReadError, WriteError};
use crate::node::DEFAULT_RANGE_ID;
use crate::range::{RangeDirectory, RangeDirectoryError, RangeRuntime};

// Generated protobuf types don't come with doc comments; suppress the
// crate-wide `missing_docs` lint just for this module.
#[allow(missing_docs)]
pub mod pb {
    tonic::include_proto!("aresadb.cluster.v1");
}

pub use pb::cluster_admin_client::ClusterAdminClient;
pub use pb::cluster_admin_server::{ClusterAdmin, ClusterAdminServer};

/// Implementation of the `ClusterAdmin` tonic service. Holds a Raft
/// handle and the peer directory so membership changes can update
/// both at once.
pub struct AdminService {
    raft: Raft<TypeConfig>,
    directory: Arc<StaticPeerDirectory>,
    data: Arc<dyn StorageBackend>,
    range_directory: Arc<RangeDirectory>,
    node_config: NodeConfig,
}

impl AdminService {
    /// Construct an admin service bound to the given default-range
    /// Raft instance and the node's `RangeDirectory`. The directory
    /// is shared with the gRPC fan-out server (Phase 2c-1), so
    /// mutations here are visible to inbound Raft RPCs immediately.
    ///
    /// `node_config` is used by the range admin RPCs to derive per-
    /// range storage paths and to stamp peer-directory entries.
    pub fn new(
        raft: Raft<TypeConfig>,
        directory: Arc<StaticPeerDirectory>,
        data: Arc<dyn StorageBackend>,
        range_directory: Arc<RangeDirectory>,
        node_config: NodeConfig,
    ) -> Self {
        Self {
            raft,
            directory,
            data,
            range_directory,
            node_config,
        }
    }

    /// Translate a [`ReadError`] into a tonic [`Status`] suitable
    /// for the `Read` RPC.
    ///
    /// Mapping:
    /// * `NotLeader(Some(id))` → `FAILED_PRECONDITION` with the
    ///   leader id in the status message. (A follow-up change can
    ///   attach the id as a structured metadata header; for now
    ///   the human-readable form is enough for the CLI.)
    /// * `NotLeader(None)` → `FAILED_PRECONDITION` with
    ///   "no current leader" so clients know to retry rather than
    ///   re-route.
    /// * `QuorumUnavailable` → `UNAVAILABLE` (transient; safe to
    ///   retry against the same member).
    /// * `Fatal` / `Storage` → `INTERNAL` (operator intervention).
    fn read_status(err: ReadError) -> Status {
        match err {
            ReadError::NotLeader(Some(id)) => {
                let mut s = Status::failed_precondition(format!(
                    "not leader for range; current leader: {id}"
                ));
                // Surface the leader id as a metadata header so the
                // CLI and SDK can re-route without parsing the
                // human message. `x-aresa-leader-id` is the same
                // convention the Phase 2b-4 PD admin uses.
                if let Ok(value) = id.to_string().parse() {
                    s.metadata_mut().insert("x-aresa-leader-id", value);
                }
                s
            }
            ReadError::NotLeader(None) => {
                Status::failed_precondition("not leader for range; no current leader")
            }
            ReadError::QuorumUnavailable(msg) => Status::unavailable(msg),
            ReadError::Fatal(msg) => Status::internal(format!("raft fatal: {msg}")),
            ReadError::Storage(e) => Status::internal(format!("storage: {e}")),
        }
    }

    /// Translate a [`WriteError`] into a tonic [`Status`] for the
    /// `Write` RPC. Mirrors [`Self::read_status`]: `NotLeader` gets
    /// the `x-aresa-leader-id` metadata header so the CLI can
    /// re-route without parsing the human message, and every other
    /// variant maps to the closest grpc status the caller can
    /// distinguish on.
    fn write_status(err: WriteError) -> Status {
        match err {
            WriteError::NotLeader(Some(id)) => {
                let mut s = Status::failed_precondition(format!(
                    "not leader for range; current leader: {id}"
                ));
                if let Ok(value) = id.to_string().parse() {
                    s.metadata_mut().insert("x-aresa-leader-id", value);
                }
                s
            }
            WriteError::NotLeader(None) => {
                Status::failed_precondition("not leader for range; no current leader")
            }
            WriteError::RangeNotFound(id) => {
                Status::not_found(format!("range {id} is not registered on this node"))
            }
            WriteError::InvalidMembership(msg) => {
                Status::failed_precondition(format!("membership: {msg}"))
            }
            WriteError::Fatal(msg) => Status::internal(format!("raft fatal: {msg}")),
            WriteError::Storage(e) => Status::internal(format!("storage: {e}")),
        }
    }
}

fn descriptor_to_pb(d: &CatalogRangeDescriptor) -> pb::RangeDescriptor {
    pb::RangeDescriptor {
        range_id: d.range_id,
        start_key: d.start_key.clone(),
        end_key: d.end_key.clone(),
        replicas: d
            .replicas
            .iter()
            .map(|r| pb::ReplicaPlacement {
                node_id: r.node_id,
                store_id: r.store_id,
                role: match r.role {
                    ReplicaRole::Voter => pb::ReplicaRole::Voter as i32,
                    ReplicaRole::Learner => pb::ReplicaRole::Learner as i32,
                },
            })
            .collect(),
        raft_group_id: d.raft_group_id,
        epoch: d.epoch,
        generation: d.generation,
        lease: d.lease.as_ref().map(|l| pb::RangeLease {
            holder: l.holder,
            expires_at_millis: l.expires_at_millis,
        }),
    }
}

fn descriptor_from_pb(p: pb::RangeDescriptor) -> Result<CatalogRangeDescriptor, Status> {
    if p.range_id == 0 {
        return Err(Status::invalid_argument("range_id must be non-zero"));
    }
    let replicas = p
        .replicas
        .into_iter()
        .map(|r| {
            let role = pb::ReplicaRole::try_from(r.role).map_err(|_| {
                Status::invalid_argument(format!("unknown replica role {}", r.role))
            })?;
            let role = match role {
                pb::ReplicaRole::Unspecified => {
                    return Err(Status::invalid_argument(format!(
                        "replica on node {} has unspecified role",
                        r.node_id
                    )));
                }
                pb::ReplicaRole::Voter => ReplicaRole::Voter,
                pb::ReplicaRole::Learner => ReplicaRole::Learner,
            };
            Ok(CatalogReplicaPlacement {
                node_id: r.node_id,
                store_id: r.store_id,
                role,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let raft_group_id = if p.raft_group_id == 0 {
        p.range_id
    } else {
        p.raft_group_id
    };
    let lease = p.lease.map(|l| LeaseInfo {
        holder: l.holder,
        expires_at_millis: l.expires_at_millis,
    });
    Ok(CatalogRangeDescriptor {
        range_id: p.range_id,
        start_key: p.start_key,
        end_key: p.end_key,
        replicas,
        raft_group_id,
        epoch: p.epoch,
        generation: p.generation,
        lease,
    })
}

#[async_trait]
impl ClusterAdmin for AdminService {
    async fn initialize(
        &self,
        request: Request<pb::InitializeRequest>,
    ) -> Result<Response<pb::InitializeResponse>, Status> {
        let req = request.into_inner();
        if req.members.is_empty() {
            return Err(Status::invalid_argument(
                "initialize requires at least one member",
            ));
        }

        let mut members = BTreeMap::new();
        for m in req.members {
            if m.rpc_addr.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "member {} has empty rpc_addr",
                    m.node_id
                )));
            }
            self.directory.upsert(m.node_id, m.rpc_addr.clone());
            members.insert(m.node_id, BasicNode::new(m.rpc_addr));
        }

        self.raft
            .initialize(members)
            .await
            .map_err(|e| Status::failed_precondition(format!("initialize: {e}")))?;
        Ok(Response::new(pb::InitializeResponse {}))
    }

    async fn add_learner(
        &self,
        request: Request<pb::AddLearnerRequest>,
    ) -> Result<Response<pb::AddLearnerResponse>, Status> {
        let req = request.into_inner();
        let node = req
            .node
            .ok_or_else(|| Status::invalid_argument("node descriptor missing"))?;
        if node.rpc_addr.is_empty() {
            return Err(Status::invalid_argument("node rpc_addr is empty"));
        }

        // Tell our own transport how to reach the new peer before we
        // ask Raft to replicate to it, otherwise the first
        // `append_entries` would fail with "no endpoint known".
        self.directory.upsert(node.node_id, node.rpc_addr.clone());

        self.raft
            .add_learner(node.node_id, BasicNode::new(node.rpc_addr), req.blocking)
            .await
            .map_err(|e| Status::failed_precondition(format!("add_learner: {e}")))?;

        Ok(Response::new(pb::AddLearnerResponse {}))
    }

    async fn change_membership(
        &self,
        request: Request<pb::ChangeMembershipRequest>,
    ) -> Result<Response<pb::ChangeMembershipResponse>, Status> {
        let req = request.into_inner();
        let voters: BTreeSet<NodeId> = req.voter_ids.into_iter().collect();
        if voters.is_empty() {
            return Err(Status::invalid_argument(
                "change_membership requires at least one voter",
            ));
        }

        self.raft
            .change_membership(voters, req.retain_learners)
            .await
            .map_err(|e| Status::failed_precondition(format!("change_membership: {e}")))?;

        Ok(Response::new(pb::ChangeMembershipResponse {}))
    }

    async fn write(
        &self,
        request: Request<pb::WriteRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let req = request.into_inner();
        if req.batch.is_empty() {
            return Err(Status::invalid_argument("empty write batch"));
        }

        let serialisable: SerializableWriteBatch = bincode::deserialize(&req.batch)
            .map_err(|e| Status::invalid_argument(format!("bad write batch: {e}")))?;
        let batch: aresadb_core::WriteBatch = serialisable.into();

        // Phase 2c-6 range routing. `range_id = 0` preserves the
        // Phase 1c wire contract — we route to the default range's
        // Raft handle (same object as `self.range_directory` holds
        // under id `DEFAULT_RANGE_ID`, but going through
        // `self.raft` keeps the call path identical to earlier
        // versions so integration tests don't have to be rewritten).
        // Non-zero `range_id` goes through the directory — that's
        // the knob the CLI exposes (`write --range-id N`) and the
        // `docker/cluster/multi-range.sh` smoke exercises.
        let range_id = if req.range_id == 0 {
            DEFAULT_RANGE_ID
        } else {
            req.range_id
        };

        let raft_handle = if range_id == DEFAULT_RANGE_ID {
            self.raft.clone()
        } else {
            self.range_directory
                .get_range(range_id)
                .ok_or_else(|| {
                    Status::not_found(format!("range {range_id} is not registered on this node"))
                })?
                .raft()
                .clone()
        };

        let resp = raft_handle
            .client_write(AresaCommand::batch(batch))
            .await
            .map_err(|e| Self::write_status(WriteError::from(e)))?;

        Ok(Response::new(pb::WriteResponse {
            log_index: resp.log_id.index,
            ops_applied: u64::from(resp.data.ops_applied),
            range_id,
        }))
    }

    async fn read(
        &self,
        request: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("read key is empty"));
        }

        // Resolve the target range. `0` means "caller didn't set it"
        // (protobuf scalar default) — map to the default range for
        // Phase 1c back-compat.
        let range_id = if req.range_id == 0 {
            DEFAULT_RANGE_ID
        } else {
            req.range_id
        };
        let consistency = pb::ReadConsistency::try_from(req.consistency)
            .unwrap_or(pb::ReadConsistency::Unspecified);

        // Resolve the range. If the caller targeted the default
        // range explicitly (or fell through the `== 0` branch above)
        // and the `UNSPECIFIED` consistency path is picked, we keep
        // the Phase 1c fast path — a raw read against `self.data`
        // without a `RangeDirectory` lookup. That path existed
        // before the directory did, and we preserve it so
        // regression tests don't need to be rewritten.
        if range_id == DEFAULT_RANGE_ID && consistency == pb::ReadConsistency::Unspecified {
            let val = self
                .data
                .get(&req.key)
                .await
                .map_err(|e| Status::internal(format!("storage: {e}")))?;
            return Ok(Response::new(pb::ReadResponse {
                found: val.is_some(),
                value: val.map(|b| b.to_vec()).unwrap_or_default(),
                range_id,
                read_log_index: 0,
            }));
        }

        let range = self
            .range_directory
            .get_range(range_id)
            .ok_or_else(|| Status::not_found(format!("range {range_id} not found on this node")))?;

        let (value, read_log_index) = match consistency {
            pb::ReadConsistency::Linearizable => {
                let value = range
                    .linearizable_get(&req.key)
                    .await
                    .map_err(Self::read_status)?;
                // Report the applied index as of the snapshot we
                // pulled *after* the linearizability guard — this
                // is an upper bound on "what the leader had applied
                // when we returned". Good enough for client-side
                // read-my-writes heuristics; clients that need
                // stronger guarantees should pair with the log
                // index returned by `Write`.
                let last_applied = range.leadership_status().last_applied_index.unwrap_or(0);
                (value, last_applied)
            }
            pb::ReadConsistency::Stale => {
                let value = range.stale_get(&req.key).await.map_err(Self::read_status)?;
                (value, 0)
            }
            pb::ReadConsistency::Unspecified => {
                // Non-default range, unspecified consistency ==
                // treat as stale. Keeps the wire surface uniform
                // without breaking Phase 1c default-range callers.
                let value = range.stale_get(&req.key).await.map_err(Self::read_status)?;
                (value, 0)
            }
        };

        Ok(Response::new(pb::ReadResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
            range_id,
            read_log_index,
        }))
    }

    async fn status(
        &self,
        _request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        // Build a compact, human-friendly JSON payload. We render it
        // ourselves (rather than using `serde_json::to_value(&metrics)`
        // directly) because some of openraft's internal types aren't
        // stable under serialization — they change shape between
        // minor versions, which would break CLI parsing unnecessarily.
        let membership_voters: Vec<_> =
            metrics.membership_config.membership().voter_ids().collect();
        let membership_learners: Vec<_> = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect();
        let snapshot = metrics
            .snapshot
            .as_ref()
            .map(|s| json!({"term": s.leader_id.term, "index": s.index}))
            .unwrap_or(serde_json::Value::Null);

        let json_value = json!({
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
            "snapshot": snapshot,
            "cluster_name": metrics.running_state.map(|_| "running").unwrap_or("not-running"),
        });

        let bytes = serde_json::to_vec(&json_value)
            .map_err(|e| Status::internal(format!("json encode: {e}")))?;

        Ok(Response::new(pb::StatusResponse { json: bytes }))
    }

    async fn add_range(
        &self,
        request: Request<pb::AddRangeRequest>,
    ) -> Result<Response<pb::AddRangeResponse>, Status> {
        let req = request.into_inner();
        let descriptor = req
            .descriptor
            .ok_or_else(|| Status::invalid_argument("descriptor is required"))?;
        let descriptor = descriptor_from_pb(descriptor)?;
        if !descriptor.has_non_empty_span() {
            return Err(Status::invalid_argument(
                "range descriptor span is empty or inverted",
            ));
        }

        let range_id: PdRangeId = descriptor.range_id;
        let raft_group_id = descriptor.raft_group_id;

        // Pre-flight duplicate check. `RangeDirectory::insert` would
        // catch the same collision below, but a duplicate also makes
        // `RangeRuntime::open_on_disk` fail on redb's exclusive file
        // lock *first*, masking the real reason with a
        // `Status::internal`. Checking the directory first turns
        // "logical duplicate" into a clean `ALREADY_EXISTS`.
        if self.range_directory.get_range(range_id).is_some() {
            return Err(Status::already_exists(format!(
                "range {} is already registered",
                range_id
            )));
        }
        if self.range_directory.get_group(raft_group_id).is_some() {
            return Err(Status::already_exists(format!(
                "raft_group_id {} is already registered",
                raft_group_id
            )));
        }

        // Build a per-range Raft config identical in shape to the
        // one `ClusterNode::start` uses. A distinct `cluster_name`
        // per range keeps openraft's log-replication heuristics from
        // conflating groups inside the same process (important for
        // tests that run several ranges on one node).
        let raft_config = Arc::new(
            Config {
                heartbeat_interval: 150,
                election_timeout_min: 500,
                election_timeout_max: 1500,
                cluster_name: format!("{}-range-{}", self.node_config.cluster_name, range_id),
                ..Default::default()
            }
            .validate()
            .map_err(|e| Status::internal(format!("invalid raft config: {e}")))?,
        );

        let network = GrpcRaftNetwork::new(self.directory.clone(), raft_group_id);

        let runtime = RangeRuntime::open_on_disk(
            descriptor.clone(),
            self.node_config.node_id,
            &self.node_config,
            network,
            raft_config,
        )
        .await
        .map_err(|e| Status::internal(format!("open range: {e}")))?;

        let runtime = Arc::new(runtime);
        // The second insert check exists because another AddRange
        // could have raced in between the pre-flight probe and here.
        match self.range_directory.insert(runtime.clone()) {
            Ok(()) => {}
            Err(RangeDirectoryError::DuplicateRangeId(id)) => {
                drop(runtime);
                return Err(Status::already_exists(format!(
                    "range {} is already registered (raced with concurrent AddRange)",
                    id
                )));
            }
            Err(RangeDirectoryError::DuplicateGroupId(id)) => {
                drop(runtime);
                return Err(Status::already_exists(format!(
                    "raft_group_id {} is already registered (raced with concurrent AddRange)",
                    id
                )));
            }
        }

        if req.bootstrap_as_voter {
            runtime
                .bootstrap_voter_with_addr(self.node_config.effective_advertise_addr())
                .await
                .map_err(|e| Status::failed_precondition(format!("bootstrap voter: {e}")))?;
        }

        Ok(Response::new(pb::AddRangeResponse {
            descriptor: Some(descriptor_to_pb(runtime.descriptor())),
        }))
    }

    async fn remove_range(
        &self,
        request: Request<pb::RemoveRangeRequest>,
    ) -> Result<Response<pb::RemoveRangeResponse>, Status> {
        let req = request.into_inner();
        if req.range_id == 0 {
            return Err(Status::invalid_argument("range_id must be non-zero"));
        }

        let runtime = self.range_directory.remove(req.range_id).ok_or_else(|| {
            Status::not_found(format!("range {} is not registered", req.range_id))
        })?;

        let range_id = runtime.descriptor().range_id;

        match Arc::try_unwrap(runtime) {
            Ok(rt) => rt
                .shutdown()
                .await
                .map_err(|e| Status::internal(format!("shutdown range: {e}")))?,
            Err(shared) => {
                if !req.force {
                    // Put it back — tearing down a runtime with live
                    // references is a caller-visible hazard, so we
                    // surface it rather than hide it.
                    self.range_directory.insert(shared).map_err(|e| {
                        Status::internal(format!(
                            "re-insert range {} after aborted remove: {e}",
                            range_id
                        ))
                    })?;
                    return Err(Status::failed_precondition(format!(
                        "range {} has outstanding references; retry with force=true",
                        range_id
                    )));
                }
                // Force path: shut down the Raft side at least, even
                // though the backends will live on until the last
                // `Arc` is dropped elsewhere.
                shared
                    .raft()
                    .clone()
                    .shutdown()
                    .await
                    .map_err(|e| Status::internal(format!("force shutdown raft: {e}")))?;
            }
        }

        Ok(Response::new(pb::RemoveRangeResponse {}))
    }

    async fn list_ranges(
        &self,
        _request: Request<pb::ListRangesRequest>,
    ) -> Result<Response<pb::ListRangesResponse>, Status> {
        let mut descriptors = self.range_directory.descriptors();
        descriptors.sort_by_key(|d| d.range_id);
        let ranges = descriptors.iter().map(descriptor_to_pb).collect();
        Ok(Response::new(pb::ListRangesResponse { ranges }))
    }
}
