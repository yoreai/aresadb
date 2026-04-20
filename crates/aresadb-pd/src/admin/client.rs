//! Typed wrapper around the generated tonic admin client.
//!
//! The raw `PlacementDriverAdminClient<Channel>` takes and returns
//! protobuf structs and surfaces every transport / Raft failure as a
//! tonic `Status`. That is fine for plumbing but annoying to use
//! directly from Rust callers: you want Rust types back,
//! `ForwardToLeader` to look like a dedicated error variant, and one
//! place to parse the `pd-leader-id` metadata hint.
//!
//! [`PdAdminClient`] wraps the raw client and does all of that.
//! Every mutating method returns its natural Rust result; every read
//! method returns `Option<Rust-type>` (or a `Vec`), mirroring the
//! catalog. Errors collapse into [`PdAdminClientError`].

use std::time::Duration;

use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::types::{LeaseInfo, NodeId, NodeInfo, RangeDescriptor, RangeId, ReplicaPlacement};

use super::pb;
use super::pb::placement_driver_admin_client::PlacementDriverAdminClient;
use super::server::LEADER_HINT_METADATA;

/// Error returned by every [`PdAdminClient`] method.
#[derive(Debug, Error)]
pub enum PdAdminClientError {
    /// The receiving node is not the current leader. If the
    /// response carried the `pd-leader-id` metadata hint we surface
    /// it so the caller can retry against the right endpoint in
    /// one hop. `None` means leadership is unresolved right now
    /// (mid-election); callers typically sleep + retry.
    #[error("not leader; hint = {0:?}")]
    NotLeader(Option<NodeId>),

    /// The catalog rejected the command (overlap, duplicate id,
    /// non-adjacent merge, epoch regression, …). Contains the
    /// server-side rendered message.
    #[error("catalog rejected: {0}")]
    CatalogRejected(String),

    /// A programmer error: a required field was missing or a
    /// contradictory flag was set. Contains the server-side rendered
    /// message.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Any other tonic `Status` (transport failure, server-side
    /// internal error, …). Boxed so [`PdAdminClientError`] stays
    /// small enough to move cheaply on hot paths (tonic `Status`
    /// is ~176 bytes on its own).
    #[error("rpc failed: {0}")]
    Rpc(Box<Status>),

    /// The server returned a response with an unset `range` or
    /// `node` field where one was required. Should never happen with
    /// a well-behaved server, but we fail loudly rather than panic.
    #[error("server response missing required field: {0}")]
    MalformedResponse(&'static str),
}

impl PdAdminClientError {
    /// Extract the suggested leader id from a `NotLeader` error.
    /// Returns `None` for every other variant.
    pub fn leader_hint(&self) -> Option<NodeId> {
        match self {
            Self::NotLeader(hint) => *hint,
            _ => None,
        }
    }
}

fn map_status(status: Status) -> PdAdminClientError {
    match status.code() {
        Code::Unavailable => {
            let hint: Option<NodeId> = status
                .metadata()
                .get(LEADER_HINT_METADATA)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<NodeId>().ok());
            PdAdminClientError::NotLeader(hint)
        }
        Code::FailedPrecondition => {
            PdAdminClientError::CatalogRejected(status.message().to_string())
        }
        Code::InvalidArgument => PdAdminClientError::InvalidArgument(status.message().to_string()),
        _ => PdAdminClientError::Rpc(Box::new(status)),
    }
}

/// Typed admin client — construct via [`PdAdminClient::connect`] or
/// wrap a pre-built [`Channel`] with [`PdAdminClient::from_channel`].
#[derive(Clone)]
pub struct PdAdminClient {
    inner: PlacementDriverAdminClient<Channel>,
}

impl PdAdminClient {
    /// Dial `endpoint` with a 5-second connect timeout and return a
    /// ready client. `endpoint` must be a full URL (`http://host:port`).
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let endpoint =
            Endpoint::from_shared(endpoint.into())?.connect_timeout(Duration::from_secs(5));
        let channel = endpoint.connect().await?;
        Ok(Self::from_channel(channel))
    }

    /// Wrap an existing tonic channel. Useful in tests that spin up
    /// the server on an in-memory transport.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: PlacementDriverAdminClient::new(channel),
        }
    }

    /// Borrow the inner tonic client, in case a caller needs a
    /// field that we haven't wrapped.
    pub fn inner(&mut self) -> &mut PlacementDriverAdminClient<Channel> {
        &mut self.inner
    }

    // ------------------------------------------------------------
    // Mutations
    // ------------------------------------------------------------

    /// Register (or refresh) a node in the cluster inventory.
    pub async fn register_node(&mut self, node: NodeInfo) -> Result<(), PdAdminClientError> {
        self.inner
            .register_node(pb::RegisterNodeRequest {
                node: Some(node.into()),
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Mark a node as alive at `last_seen_millis`.
    pub async fn heartbeat_node(
        &mut self,
        node_id: NodeId,
        last_seen_millis: u64,
    ) -> Result<(), PdAdminClientError> {
        self.inner
            .heartbeat_node(pb::HeartbeatNodeRequest {
                node_id,
                last_seen_millis,
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Insert a brand-new range. Returns the stored descriptor.
    pub async fn create_range(
        &mut self,
        range: RangeDescriptor,
    ) -> Result<RangeDescriptor, PdAdminClientError> {
        let resp = self
            .inner
            .create_range(pb::CreateRangeRequest {
                range: Some(range.into()),
            })
            .await
            .map_err(map_status)?;
        let pb = resp
            .into_inner()
            .range
            .ok_or(PdAdminClientError::MalformedResponse("range"))?;
        pb.try_into().map_err(map_status)
    }

    /// Split `parent_range_id` at `split_key`. Returns the newly
    /// created right-hand-side descriptor.
    pub async fn split_range(
        &mut self,
        parent_range_id: RangeId,
        split_key: Vec<u8>,
    ) -> Result<RangeDescriptor, PdAdminClientError> {
        let resp = self
            .inner
            .split_range(pb::SplitRangeRequest {
                parent_range_id,
                split_key,
            })
            .await
            .map_err(map_status)?;
        let pb = resp
            .into_inner()
            .new_range
            .ok_or(PdAdminClientError::MalformedResponse("new_range"))?;
        pb.try_into().map_err(map_status)
    }

    /// Merge two adjacent ranges.
    pub async fn merge_ranges(
        &mut self,
        left: RangeId,
        right: RangeId,
    ) -> Result<(), PdAdminClientError> {
        self.inner
            .merge_ranges(pb::MergeRangesRequest {
                left_range_id: left,
                right_range_id: right,
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Replace the replica set of a range atomically.
    pub async fn update_membership(
        &mut self,
        range_id: RangeId,
        new_replicas: Vec<ReplicaPlacement>,
        new_epoch: u64,
    ) -> Result<(), PdAdminClientError> {
        self.inner
            .update_membership(pb::UpdateMembershipRequest {
                range_id,
                new_replicas: new_replicas.into_iter().map(Into::into).collect(),
                new_epoch,
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Install or renew the leader lease on a range.
    pub async fn install_lease(
        &mut self,
        range_id: RangeId,
        lease: LeaseInfo,
    ) -> Result<(), PdAdminClientError> {
        self.inner
            .update_lease(pb::UpdateLeaseRequest {
                range_id,
                lease: Some(lease.into()),
                clear: false,
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    /// Clear the leader lease on a range.
    pub async fn clear_lease(&mut self, range_id: RangeId) -> Result<(), PdAdminClientError> {
        self.inner
            .update_lease(pb::UpdateLeaseRequest {
                range_id,
                lease: None,
                clear: true,
            })
            .await
            .map_err(map_status)?;
        Ok(())
    }

    // ------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------

    /// Look up a single range by id.
    pub async fn get_range(
        &mut self,
        range_id: RangeId,
    ) -> Result<Option<RangeDescriptor>, PdAdminClientError> {
        let resp = self
            .inner
            .get_range(pb::GetRangeRequest { range_id })
            .await
            .map_err(map_status)?;
        let resp = resp.into_inner();
        if !resp.found {
            return Ok(None);
        }
        let pb = resp
            .range
            .ok_or(PdAdminClientError::MalformedResponse("range"))?;
        Ok(Some(pb.try_into().map_err(map_status)?))
    }

    /// Look up the range that owns `key` right now.
    pub async fn get_range_by_key(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Option<RangeDescriptor>, PdAdminClientError> {
        let resp = self
            .inner
            .get_range_by_key(pb::GetRangeByKeyRequest { key })
            .await
            .map_err(map_status)?;
        let resp = resp.into_inner();
        if !resp.found {
            return Ok(None);
        }
        let pb = resp
            .range
            .ok_or(PdAdminClientError::MalformedResponse("range"))?;
        Ok(Some(pb.try_into().map_err(map_status)?))
    }

    /// Dump every range in keyspace order.
    pub async fn list_ranges(&mut self) -> Result<Vec<RangeDescriptor>, PdAdminClientError> {
        let resp = self
            .inner
            .list_ranges(pb::ListRangesRequest {})
            .await
            .map_err(map_status)?;
        resp.into_inner()
            .ranges
            .into_iter()
            .map(|r| RangeDescriptor::try_from(r).map_err(map_status))
            .collect()
    }

    /// Look up a single node by id.
    pub async fn get_node(
        &mut self,
        node_id: NodeId,
    ) -> Result<Option<NodeInfo>, PdAdminClientError> {
        let resp = self
            .inner
            .get_node(pb::GetNodeRequest { node_id })
            .await
            .map_err(map_status)?;
        let resp = resp.into_inner();
        if !resp.found {
            return Ok(None);
        }
        let pb = resp
            .node
            .ok_or(PdAdminClientError::MalformedResponse("node"))?;
        Ok(Some(pb.into()))
    }

    /// Dump every registered node in id order.
    pub async fn list_nodes(&mut self) -> Result<Vec<NodeInfo>, PdAdminClientError> {
        let resp = self
            .inner
            .list_nodes(pb::ListNodesRequest {})
            .await
            .map_err(map_status)?;
        Ok(resp
            .into_inner()
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Fetch the server's JSON status dump. The payload schema is
    /// described in the service handler.
    pub async fn status(&mut self) -> Result<serde_json::Value, PdAdminClientError> {
        let resp = self
            .inner
            .status(pb::PdStatusRequest {})
            .await
            .map_err(map_status)?;
        let bytes = resp.into_inner().json;
        serde_json::from_slice(&bytes).map_err(|e| {
            PdAdminClientError::Rpc(Box::new(Status::internal(format!(
                "malformed status json from server: {e}"
            ))))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn map_status_classifies_leader_hint() {
        let mut status = Status::unavailable("not leader");
        status
            .metadata_mut()
            .insert(LEADER_HINT_METADATA, MetadataValue::from(7u64));
        let err = map_status(status);
        assert!(matches!(err, PdAdminClientError::NotLeader(Some(7))));
    }

    #[test]
    fn map_status_unavailable_without_hint_is_notleader_none() {
        let status = Status::unavailable("no leader");
        let err = map_status(status);
        assert!(matches!(err, PdAdminClientError::NotLeader(None)));
    }

    #[test]
    fn map_status_failed_precondition_is_catalog() {
        let status = Status::failed_precondition("RangeNotFound(7)");
        let err = map_status(status);
        assert!(matches!(err, PdAdminClientError::CatalogRejected(_)));
    }

    #[test]
    fn map_status_invalid_argument_is_invalid() {
        let status = Status::invalid_argument("address must be non-empty");
        let err = map_status(status);
        assert!(matches!(err, PdAdminClientError::InvalidArgument(_)));
    }

    #[test]
    fn map_status_other_is_rpc() {
        let status = Status::internal("oh no");
        let err = map_status(status);
        assert!(matches!(err, PdAdminClientError::Rpc(_)));
    }

    #[test]
    fn leader_hint_accessor() {
        let err = PdAdminClientError::NotLeader(Some(3));
        assert_eq!(err.leader_hint(), Some(3));
        let err = PdAdminClientError::CatalogRejected("x".into());
        assert_eq!(err.leader_hint(), None);
    }
}
