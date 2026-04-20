//! gRPC client that implements `openraft::RaftNetwork`.
//!
//! The client is split into two concerns:
//!
//! * [`PeerDirectory`] — tells the transport how to reach a node id.
//!   Today that's a static map, but in Phase 1c the cluster
//!   membership store owns the same trait so nodes can be moved at
//!   runtime without re-instantiating the transport.
//! * [`GrpcRaftNetwork`] — an `openraft::RaftNetworkFactory` whose
//!   connections are one-shot tonic gRPC channels. Each factory
//!   instance carries a `raft_group_id` so one node running many
//!   Raft groups (Phase 2c) can hand a different factory to each
//!   group; the group id is forwarded on every outbound RPC so the
//!   receiving server routes it to the matching `Raft` handle.
//!   Phase 1 single-group deployments use
//!   [`GrpcRaftNetwork::new_singleton`] which sends `raft_group_id = 0`
//!   and is what existing tests still construct.
//!
//! Each connection is cheap because tonic reuses HTTP/2 streams
//! under the hood, and openraft calls `new_client` infrequently
//! (once per replication target per epoch).

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint};

use aresadb_raft::{NodeId, TypeConfig};

use crate::codec::{decode, encode};
use crate::pb;
use crate::pb::raft_service_client::RaftServiceClient;

/// Resolves a node id into a gRPC endpoint URI. Implementations must
/// be cheap to clone and safe to call concurrently.
pub trait PeerDirectory: Send + Sync + 'static {
    /// Return the endpoint URI (e.g. `http://10.0.0.5:7020`) for
    /// `node_id`, or `None` if the node is unknown.
    fn endpoint(&self, node_id: NodeId) -> Option<String>;
}

/// In-process peer directory backed by a `RwLock<HashMap>`. Good for
/// tests and single-shard deployments. Cluster membership updates
/// that propagate through Raft will also update this map.
#[derive(Debug, Default)]
pub struct StaticPeerDirectory {
    peers: RwLock<HashMap<NodeId, String>>,
}

impl StaticPeerDirectory {
    /// Build a directory from a static seed.
    pub fn from_map(peers: HashMap<NodeId, String>) -> Arc<Self> {
        Arc::new(Self {
            peers: RwLock::new(peers),
        })
    }

    /// Insert or update the endpoint for `node_id`.
    pub fn upsert(&self, node_id: NodeId, endpoint: impl Into<String>) {
        self.peers.write().insert(node_id, endpoint.into());
    }

    /// Remove `node_id` from the directory.
    pub fn remove(&self, node_id: NodeId) -> Option<String> {
        self.peers.write().remove(&node_id)
    }
}

impl PeerDirectory for StaticPeerDirectory {
    fn endpoint(&self, node_id: NodeId) -> Option<String> {
        self.peers.read().get(&node_id).cloned()
    }
}

/// The `raft_group_id` sent on every outbound RPC when the caller is
/// a Phase 1 single-group cluster member. Matching constant on the
/// server side routes incoming RPCs with this id to the
/// `Raft<TypeConfig>` registered under `SingletonRaftDirectory`.
///
/// `0` is chosen because it's the protobuf default for a `uint64`,
/// so a Phase 1 peer that ignores the field entirely will still
/// arrive with the right id.
pub const SINGLETON_RAFT_GROUP_ID: u64 = 0;

/// `RaftNetworkFactory` whose connections speak the `aresadb.raft.v1`
/// protocol. Constructed once per node **per Raft group** (Phase 2c)
/// and handed to `openraft::Raft::new`.
///
/// In Phase 1 deployments — one Raft group per node — use
/// [`GrpcRaftNetwork::new_singleton`]; every outbound RPC will carry
/// `raft_group_id = SINGLETON_RAFT_GROUP_ID` and the server will
/// dispatch it to the default group.
#[derive(Clone)]
pub struct GrpcRaftNetwork {
    directory: Arc<dyn PeerDirectory>,
    raft_group_id: u64,
}

impl GrpcRaftNetwork {
    /// Create a new transport tagged with `raft_group_id`. Every
    /// outbound RPC on this factory is routed to the target node's
    /// Raft group with this id.
    ///
    /// Phase 2c — callers (one per range) pass the range's
    /// `raft_group_id` from the PD catalog.
    pub fn new(directory: Arc<dyn PeerDirectory>, raft_group_id: u64) -> Self {
        Self {
            directory,
            raft_group_id,
        }
    }

    /// Create a new transport for a Phase 1 single-group cluster.
    /// All outbound RPCs carry [`SINGLETON_RAFT_GROUP_ID`], which the
    /// server routes via the `SingletonRaftDirectory` adapter.
    ///
    /// Equivalent to `GrpcRaftNetwork::new(directory, SINGLETON_RAFT_GROUP_ID)`.
    pub fn new_singleton(directory: Arc<dyn PeerDirectory>) -> Self {
        Self::new(directory, SINGLETON_RAFT_GROUP_ID)
    }

    /// Raft group id this factory tags onto outbound RPCs.
    pub fn raft_group_id(&self) -> u64 {
        self.raft_group_id
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcRaftNetwork {
    type Network = GrpcRaftConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        GrpcRaftConnection {
            target,
            directory: self.directory.clone(),
            raft_group_id: self.raft_group_id,
            channel: RwLock::new(None),
        }
    }
}

/// One connection per (replication target, Raft group) pair. The
/// channel is lazily established on first use so the transport
/// tolerates peers that come up in any order, and re-established on
/// failure so a flake on one RPC doesn't permanently break the pair.
pub struct GrpcRaftConnection {
    target: NodeId,
    directory: Arc<dyn PeerDirectory>,
    raft_group_id: u64,
    channel: RwLock<Option<Channel>>,
}

impl GrpcRaftConnection {
    async fn client(&self) -> Result<RaftServiceClient<Channel>, io::Error> {
        if let Some(ch) = self.channel.read().clone() {
            return Ok(RaftServiceClient::new(ch));
        }

        let endpoint_uri = self.directory.endpoint(self.target).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "aresadb-net: no endpoint known for node {} — peer directory is empty?",
                    self.target
                ),
            )
        })?;

        let endpoint = Endpoint::from_shared(endpoint_uri.clone()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid endpoint uri {endpoint_uri:?}: {e}"),
            )
        })?;

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        *self.channel.write() = Some(channel.clone());
        Ok(RaftServiceClient::new(channel))
    }

    /// Invalidate the cached channel so the next call reconnects.
    fn reset(&self) {
        *self.channel.write() = None;
    }
}

/// Convert an `io::Error` from the connect path into a Unreachable
/// RPC error, which openraft treats as "retry later".
fn unreachable<E>(target: NodeId, err: io::Error) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    let _ = target;
    RPCError::Unreachable(Unreachable::new(&err))
}

/// Convert any other `std::error::Error` into a `NetworkError`. Used
/// for codec / tonic::Status failures that are not retriable on their
/// own.
fn net_err<E>(err: impl std::error::Error + 'static) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&err))
}

// openraft's network traits use native `async fn` signatures (no
// `#[async_trait]` shim), so we follow the same pattern to avoid the
// lifetime-parameter mismatch that `#[async_trait]` would introduce.
#[allow(clippy::result_large_err)]
impl RaftNetwork<TypeConfig> for GrpcRaftConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = encode(&rpc).map_err(net_err)?;
        let mut client = self
            .client()
            .await
            .map_err(|e| unreachable(self.target, e))?;

        let result = client
            .append_entries(pb::AppendEntriesRequest {
                payload,
                raft_group_id: self.raft_group_id,
            })
            .await;

        let response = match result {
            Ok(r) => r,
            Err(status) => {
                self.reset();
                return Err(net_err(StatusError(status)));
            }
        };

        let body = response.into_inner();
        if body.is_error {
            let logical: RaftError<NodeId> = decode(&body.payload).map_err(net_err)?;
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                logical,
            )))
        } else {
            decode(&body.payload).map_err(net_err)
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload = encode(&rpc).map_err(net_err)?;
        let mut client = self
            .client()
            .await
            .map_err(|e| unreachable(self.target, e))?;

        let result = client
            .install_snapshot(pb::InstallSnapshotRequest {
                payload,
                raft_group_id: self.raft_group_id,
            })
            .await;

        let response = match result {
            Ok(r) => r,
            Err(status) => {
                self.reset();
                return Err(net_err(StatusError(status)));
            }
        };

        let body = response.into_inner();
        if body.is_error {
            let logical: RaftError<NodeId, InstallSnapshotError> =
                decode(&body.payload).map_err(net_err)?;
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                logical,
            )))
        } else {
            decode(&body.payload).map_err(net_err)
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = encode(&rpc).map_err(net_err)?;
        let mut client = self
            .client()
            .await
            .map_err(|e| unreachable(self.target, e))?;

        let result = client
            .vote(pb::VoteRequest {
                payload,
                raft_group_id: self.raft_group_id,
            })
            .await;
        let response = match result {
            Ok(r) => r,
            Err(status) => {
                self.reset();
                return Err(net_err(StatusError(status)));
            }
        };

        let body = response.into_inner();
        if body.is_error {
            let logical: RaftError<NodeId> = decode(&body.payload).map_err(net_err)?;
            Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                logical,
            )))
        } else {
            decode(&body.payload).map_err(net_err)
        }
    }
}

/// Newtype so `tonic::Status` can flow through `NetworkError::new`,
/// which expects a `&dyn std::error::Error`.
#[derive(Debug)]
struct StatusError(tonic::Status);

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "grpc status: {}", self.0)
    }
}

impl std::error::Error for StatusError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_directory_roundtrip() {
        let dir = StaticPeerDirectory::default();
        dir.upsert(1, "http://127.0.0.1:7001");
        dir.upsert(2, "http://127.0.0.1:7002");
        assert_eq!(dir.endpoint(1), Some("http://127.0.0.1:7001".to_string()));
        assert_eq!(dir.endpoint(2), Some("http://127.0.0.1:7002".to_string()));
        assert_eq!(dir.endpoint(3), None);
    }

    #[test]
    fn static_directory_remove_clears_entry() {
        let dir = StaticPeerDirectory::default();
        dir.upsert(1, "http://127.0.0.1:7001");
        assert!(dir.remove(1).is_some());
        assert_eq!(dir.endpoint(1), None);
    }
}
