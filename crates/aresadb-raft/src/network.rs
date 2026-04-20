//! Network transport for Raft RPCs.
//!
//! Phase 1a ships only the **loopback** factory ([`LoopbackNetwork`]) —
//! it's enough to bring up a single-node cluster for the end-to-end
//! test that exercises the log store, state machine, and client
//! path. In single-node mode openraft never replicates to another
//! peer, so calling any of the `vote` / `append_entries` /
//! `install_snapshot` methods through the loopback is a bug in the
//! caller, and the factory panics to surface it loudly.
//!
//! Phase 1b replaces this with a real tonic gRPC implementation in
//! the `aresadb-net` crate. That implementation will also satisfy
//! [`openraft::network::RaftNetworkFactory`] so swapping it in at
//! boot time is a one-line change in the server bootstrap.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::types::{NodeId, TypeConfig};

/// Network factory that refuses every peer connection.
///
/// Designed for single-node clusters where openraft never actually
/// calls `vote` / `append_entries` / `install_snapshot` on a remote.
/// If the single-node invariant is violated, the RPC returns
/// [`openraft::error::NetworkError`] with a helpful diagnostic rather
/// than hanging, so tests fail fast.
#[derive(Clone, Debug, Default)]
pub struct LoopbackNetwork;

impl RaftNetworkFactory<TypeConfig> for LoopbackNetwork {
    type Network = LoopbackConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        LoopbackConnection { target }
    }
}

/// Connection handle returned by [`LoopbackNetwork`]. Every method
/// short-circuits with a `NetworkError` explaining that only
/// single-node operation is supported.
pub struct LoopbackConnection {
    target: NodeId,
}

// `RPCError` is a large enum from openraft; boxing it would change
// the trait signatures it must live inside, so a targeted allow is
// the right call here.
#[allow(clippy::result_large_err)]
fn unsupported<T, E>(target: NodeId) -> Result<T, RPCError<NodeId, BasicNode, E>>
where
    E: std::error::Error,
{
    let err = std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "aresadb-raft: LoopbackNetwork rejected an RPC to node {target} — only \
             single-node operation is supported. Wire up `aresadb-net` (Phase 1b) for \
             multi-node replication."
        ),
    );
    Err(RPCError::Network(NetworkError::new(&err)))
}

impl RaftNetwork<TypeConfig> for LoopbackConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unsupported::<AppendEntriesResponse<NodeId>, RaftError<NodeId>>(self.target)
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        // Explicit type parameter: silences an inference error because
        // `InstallSnapshotError` only shows up in this variant.
        let e = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "install_snapshot not supported in single-node loopback (target={})",
                self.target
            ),
        );
        Err(RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // Single-node clusters never call `vote` on another peer;
        // openraft wins the election locally. Any RPC that reaches
        // here is a bug in the caller (e.g. they added a learner
        // without swapping in a real network) — surface it instead
        // of hanging.
        unsupported::<VoteResponse<NodeId>, RaftError<NodeId>>(self.target)
    }
}
