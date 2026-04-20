//! In-process network transport for the PD Raft group.
//!
//! `aresadb-net` ships a tonic gRPC transport for the user-data Raft
//! group (and will grow a PD-specific flavor in Phase 2b-4). For the
//! Phase 2b-3 integration tests that bring up a 3-5 node PD cluster
//! *inside a single process* we need something simpler: a shared
//! handle lookup keyed on node id that calls the remote
//! [`openraft::Raft::append_entries`] / `vote` / `install_snapshot`
//! directly, with no serialization.
//!
//! The router is intentionally minimal:
//!
//! - Peers register themselves by id after their [`openraft::Raft`]
//!   handle is constructed (registration is idempotent — reconnecting
//!   a peer just overwrites the handle).
//! - Lookups are `Arc<RwLock<HashMap<_,_>>>` reads; the factory's
//!   `new_client` is cheap.
//! - An RPC to a target that isn't registered yet surfaces as
//!   [`openraft::error::Unreachable`], so openraft retries the
//!   operation once the peer comes up instead of hard-failing the
//!   election.
//! - There's an explicit "drop this link" hook
//!   ([`PdRouter::isolate`] / [`PdRouter::reconnect`]) that future
//!   split-brain / partition tests will use. Today the hook exists
//!   but isn't exercised — it's here so the shape of the test
//!   harness stops evolving when partitions become interesting.
//!
//! When the gRPC flavor of the PD network lands, everything below
//! becomes the "fast, in-process" transport used by integration
//! tests, and the gRPC transport becomes the default for deployed
//! clusters. Both implement the same openraft traits, so swapping is
//! a one-line change.

use std::collections::{HashMap, HashSet};
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

use super::config::{typ, NodeId, PdTypeConfig};

/// Shared routing table for an in-process PD cluster.
///
/// Every member of the cluster registers its [`openraft::Raft`]
/// handle under its node id. RPCs dispatched through
/// [`PdRouterNetwork`] clone the target's handle out of the router
/// and invoke the corresponding `Raft::append_entries` / `vote` /
/// `install_snapshot` method directly, skipping the network layer
/// entirely. Good for integration tests; not for production.
#[derive(Default)]
pub struct PdRouter {
    // openraft::Raft<_> intentionally doesn't implement Debug (the
    // handle is a bag of channels), so we can't derive `Debug` on the
    // router. Manual impl below skips the handles themselves and
    // only surfaces node ids.
    nodes: RwLock<HashMap<NodeId, typ::Raft>>,
    /// Pairs of `(from, to)` links that are administratively isolated.
    /// The router returns `Unreachable` on matching RPCs so openraft
    /// treats them like a temporary network error.
    isolated: RwLock<HashSet<(NodeId, NodeId)>>,
}

impl std::fmt::Debug for PdRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids: Vec<NodeId> = self.nodes.read().keys().copied().collect();
        let isolated: Vec<(NodeId, NodeId)> = self.isolated.read().iter().copied().collect();
        f.debug_struct("PdRouter")
            .field("nodes", &ids)
            .field("isolated", &isolated)
            .finish()
    }
}

impl PdRouter {
    /// Build an empty router. Clones of the returned `Arc` are safe
    /// to share across threads and nodes.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register (or replace) the `Raft` handle for `node_id`.
    ///
    /// Idempotent: calling `register` twice for the same id simply
    /// overwrites the previous handle. That's useful when a test
    /// drops and re-opens a member: the new handle takes over and
    /// peers see the change on their next lookup.
    pub fn register(&self, node_id: NodeId, raft: typ::Raft) {
        self.nodes.write().insert(node_id, raft);
    }

    /// Remove `node_id` from the routing table. Subsequent RPCs
    /// addressed to it return [`Unreachable`].
    pub fn unregister(&self, node_id: NodeId) -> Option<typ::Raft> {
        self.nodes.write().remove(&node_id)
    }

    /// Administratively drop the directed link `from -> to`. RPCs
    /// originating at `from` for `to` surface as
    /// [`openraft::error::Unreachable`] until
    /// [`Self::reconnect`] is called. Used by future partition /
    /// split-brain tests.
    pub fn isolate(&self, from: NodeId, to: NodeId) {
        self.isolated.write().insert((from, to));
    }

    /// Restore a previously-isolated directed link.
    pub fn reconnect(&self, from: NodeId, to: NodeId) {
        self.isolated.write().remove(&(from, to));
    }

    /// How many members are currently registered. Exposed for tests /
    /// diagnostics.
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// `true` if no members are registered.
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }

    /// Snapshot the set of registered ids. Stable-sorted for
    /// deterministic test output.
    pub fn ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.nodes.read().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn lookup(&self, from: NodeId, to: NodeId) -> Option<typ::Raft> {
        if self.isolated.read().contains(&(from, to)) {
            return None;
        }
        self.nodes.read().get(&to).cloned()
    }
}

/// `RaftNetworkFactory` that delivers RPCs through a [`PdRouter`].
///
/// Each node holds exactly one instance of this factory, tagged with
/// its own id, and shares the router with every other node. On
/// `new_client(target)` the factory returns a connection that knows
/// how to call the remote handle directly.
#[derive(Clone)]
pub struct PdRouterNetwork {
    from: NodeId,
    router: Arc<PdRouter>,
}

impl PdRouterNetwork {
    /// Build a factory for node `from` sharing the routing table
    /// `router`. The factory is cheap to clone and safe to share.
    pub fn new(from: NodeId, router: Arc<PdRouter>) -> Self {
        Self { from, router }
    }
}

impl RaftNetworkFactory<PdTypeConfig> for PdRouterNetwork {
    type Network = PdRouterConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        PdRouterConnection {
            from: self.from,
            target,
            router: self.router.clone(),
        }
    }
}

/// A single "connection" to a peer. Stateless — every RPC just looks
/// up the target's handle from the router and invokes it.
pub struct PdRouterConnection {
    from: NodeId,
    target: NodeId,
    router: Arc<PdRouter>,
}

#[allow(clippy::result_large_err)]
fn unreachable_err<E>(from: NodeId, target: NodeId) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    let err = io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "PdRouter: node {target} is not registered (seen from node {from}); \
             returning `Unreachable` so openraft retries."
        ),
    );
    RPCError::Unreachable(Unreachable::new(&err))
}

impl RaftNetwork<PdTypeConfig> for PdRouterConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<PdTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let Some(peer) = self.router.lookup(self.from, self.target) else {
            return Err(unreachable_err::<RaftError<NodeId>>(self.from, self.target));
        };
        peer.append_entries(rpc).await.map_err(|e| {
            // `Fatal` and `APIError` both surface as RemoteError —
            // it's "the remote returned a logical Raft error",
            // regardless of which kind.
            RPCError::RemoteError(RemoteError::new(self.target, e))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<PdTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let Some(peer) = self.router.lookup(self.from, self.target) else {
            return Err(unreachable_err::<RaftError<NodeId, InstallSnapshotError>>(
                self.from,
                self.target,
            ));
        };
        peer.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let Some(peer) = self.router.lookup(self.from, self.target) else {
            // Return a plain `NetworkError`, not `Unreachable`, so
            // openraft's leader-election path treats this as a
            // genuine network error. `Unreachable` would also work
            // but keeps the election probe in flight; for the vote
            // path we prefer a clear fail so callers see the missing
            // peer immediately.
            let err = io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "PdRouter: node {} not registered — can't vote (seen from node {})",
                    self.target, self.from
                ),
            );
            return Err(RPCError::Network(NetworkError::new(&err)));
        };
        peer.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_router_is_empty() {
        let router = PdRouter::new();
        assert!(router.is_empty());
        assert_eq!(router.len(), 0);
        assert_eq!(router.ids(), Vec::<NodeId>::new());
    }

    #[tokio::test]
    async fn factory_returns_connection_for_unknown_target() {
        // Target lookup is lazy — the factory hands out a connection
        // even for unregistered ids so openraft's retry loop can do
        // its thing when the peer comes up later.
        let router = PdRouter::new();
        let mut factory = PdRouterNetwork::new(1, router.clone());
        let _ = factory.new_client(42, &BasicNode::default()).await;
    }

    #[test]
    fn isolate_round_trip_is_symmetric_in_shape() {
        // Structural test only — no Raft instances yet. We're just
        // making sure `isolate` / `reconnect` toggle the flag and
        // don't mix up directions.
        let router = PdRouter::new();
        router.isolate(1, 2);
        assert!(router.isolated.read().contains(&(1, 2)));
        assert!(!router.isolated.read().contains(&(2, 1)));
        router.reconnect(1, 2);
        assert!(!router.isolated.read().contains(&(1, 2)));
    }
}
