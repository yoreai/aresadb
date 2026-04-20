//! Convenience harness for a one-node AresaDB cluster.
//!
//! [`SingleNode`] bundles a [`LogStore`], a [`StateMachineStore`], and
//! a [`LoopbackNetwork`] into a running [`openraft::Raft`] instance
//! already initialized as a single-voter cluster. It's what the
//! end-to-end tests (and later the `aresadb --standalone` CLI mode)
//! lean on for a fully functional Raft node without any wire
//! transport.
//!
//! In Phase 1c the binary's cluster-start path will use the exact
//! same building blocks but swap [`LoopbackNetwork`] for the
//! `aresadb-net` gRPC factory.

use std::sync::Arc;

use aresadb_core::{MemoryBackend, StorageBackend, WriteBatch};
use openraft::{BasicNode, Config, Raft};

use crate::command::{AresaCommand, AresaResponse};
use crate::log_store::LogStore;
use crate::network::LoopbackNetwork;
use crate::state_machine::StateMachineStore;
use crate::types::{NodeId, TypeConfig};

/// A ready-to-use single-node Raft cluster.
pub struct SingleNode {
    /// This node's id. Fixed at `1` for the loopback harness.
    pub node_id: NodeId,

    /// Handle to the Raft task. Cloneable and safe to share.
    pub raft: Raft<TypeConfig>,

    /// Direct handle to the application backend. Readers use this to
    /// serve committed reads without going through consensus.
    pub data: Arc<dyn StorageBackend>,

    /// Direct handle to the log backend. Primarily exposed for tests
    /// and introspection.
    pub log_backend: Arc<dyn StorageBackend>,
}

impl SingleNode {
    /// Spin up a single-node cluster backed by [`MemoryBackend`]s.
    ///
    /// The node is already initialized — i.e. `raft.initialize(...)`
    /// has returned, a leader election has resolved, and the first
    /// membership entry has been applied — so callers can start
    /// issuing `client_write` immediately.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let log_backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        Self::new(1, log_backend, data).await
    }

    /// Boot a single-node cluster on the provided backends.
    pub async fn new(
        node_id: NodeId,
        log_backend: Arc<dyn StorageBackend>,
        data: Arc<dyn StorageBackend>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 150,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                cluster_name: "aresadb-single-node".to_string(),
                ..Default::default()
            }
            .validate()?,
        );

        let log = LogStore::new(log_backend.clone());
        let sm = StateMachineStore::new(data.clone());

        let raft = Raft::<TypeConfig>::new(node_id, config, LoopbackNetwork, log, sm).await?;

        let mut members = std::collections::BTreeMap::new();
        members.insert(node_id, BasicNode::new(""));
        raft.initialize(members).await?;

        Ok(Self {
            node_id,
            raft,
            data,
            log_backend,
        })
    }

    /// Convenience: replicate a write batch through Raft and wait
    /// until the state machine has applied it.
    pub async fn write(&self, batch: WriteBatch) -> anyhow::Result<AresaResponse> {
        let resp = self.raft.client_write(AresaCommand::batch(batch)).await?;
        Ok(resp.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_end_to_end_write_and_read() {
        let node = SingleNode::in_memory().await.expect("start node");

        let mut batch = WriteBatch::new();
        batch.put("hello", "world").put("lang", "rust");
        let resp = node.write(batch).await.expect("client write");
        assert_eq!(resp.ops_applied, 2);

        // Application backend reflects the write after the state
        // machine applied it; client_write only returns after apply.
        assert_eq!(
            &node.data.get(b"hello").await.unwrap().unwrap()[..],
            b"world"
        );
        assert_eq!(&node.data.get(b"lang").await.unwrap().unwrap()[..], b"rust");

        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_node_delete_propagates() {
        let node = SingleNode::in_memory().await.unwrap();

        let mut setup = WriteBatch::new();
        setup.put("k", "v");
        node.write(setup).await.unwrap();
        assert!(node.data.get(b"k").await.unwrap().is_some());

        let mut del = WriteBatch::new();
        del.delete("k");
        node.write(del).await.unwrap();
        assert!(node.data.get(b"k").await.unwrap().is_none());

        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_node_noop_command_applies() {
        let node = SingleNode::in_memory().await.unwrap();
        let resp = node.raft.client_write(AresaCommand::Noop).await.unwrap();
        assert_eq!(resp.data.ops_applied, 0);
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_node_metrics_report_leadership() {
        let node = SingleNode::in_memory().await.unwrap();
        // Wait until we see ourselves as leader.
        let _ = node
            .raft
            .wait(Some(std::time::Duration::from_secs(2)))
            .current_leader(node.node_id, "become leader")
            .await
            .unwrap();

        let metrics = node.raft.metrics().borrow().clone();
        assert_eq!(metrics.current_leader, Some(node.node_id));
        node.raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_node_committed_writes_survive_into_snapshot() {
        let node = SingleNode::in_memory().await.unwrap();

        for i in 0u32..20 {
            let mut b = WriteBatch::new();
            b.put(format!("k{i}"), format!("v{i}"));
            node.write(b).await.unwrap();
        }

        // Trigger a snapshot and then verify it's observable on the
        // underlying state machine.
        node.raft.trigger().snapshot().await.unwrap();

        // The snapshot is built asynchronously — wait briefly for
        // openraft to finish wiring it up.
        for _ in 0..50 {
            if node.raft.metrics().borrow().snapshot.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(node.raft.metrics().borrow().snapshot.is_some());

        // Data is still served from the live backend.
        for i in 0u32..20 {
            let got = node.data.get(format!("k{i}").as_bytes()).await.unwrap();
            assert_eq!(&got.unwrap()[..], format!("v{i}").as_bytes());
        }

        node.raft.shutdown().await.unwrap();
    }
}
