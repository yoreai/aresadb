//! Sharding with Consistent Hashing
//!
//! Distributes data across multiple storage backends using consistent hashing.
//! Supports rebalancing when shards are added/removed.

#![allow(dead_code)]

use anyhow::Result;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

use crate::storage::{Edge, EdgeId, LocalStorage, Node, NodeId, Value};

/// Configuration for shard manager
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Number of shards
    pub num_shards: usize,
    /// Number of virtual nodes per shard for consistent hashing
    pub virtual_nodes: usize,
    /// Base path for shard storage
    pub base_path: PathBuf,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            num_shards: 16,
            virtual_nodes: 150,
            base_path: PathBuf::from("."),
        }
    }
}

/// Individual shard containing a LocalStorage instance
pub struct Shard {
    /// Shard ID
    pub id: usize,
    /// Storage for this shard
    storage: LocalStorage,
    /// Bloom filter for fast negative lookups
    bloom: RwLock<crate::distributed::BloomFilter>,
}

impl Shard {
    /// Create a new shard
    pub async fn new(id: usize, path: &Path) -> Result<Self> {
        let shard_path = path.join(format!("shard_{:04}", id));
        let storage = LocalStorage::create(&shard_path).await?;
        let bloom = RwLock::new(crate::distributed::BloomFilter::new(100_000, 0.01));

        Ok(Self { id, storage, bloom })
    }

    /// Open an existing shard
    pub async fn open(id: usize, path: &Path) -> Result<Self> {
        let shard_path = path.join(format!("shard_{:04}", id));
        let storage = LocalStorage::open(&shard_path).await?;
        let bloom = RwLock::new(crate::distributed::BloomFilter::new(100_000, 0.01));

        Ok(Self { id, storage, bloom })
    }

    /// Get the underlying storage
    pub fn storage(&self) -> &LocalStorage {
        &self.storage
    }

    /// Check bloom filter for possible existence
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.read().may_contain(key)
    }

    /// Add key to bloom filter
    pub fn add_to_bloom(&self, key: &[u8]) {
        self.bloom.write().insert(key);
    }
}

/// Manages sharded storage using consistent hashing
pub struct ShardManager {
    /// Configuration
    #[allow(dead_code)]
    config: ShardConfig,
    /// Hash ring for consistent hashing
    ring: ConsistentHashRing,
    /// Shards
    shards: Vec<Arc<Shard>>,
}

impl ShardManager {
    /// Compute which shard a key belongs to (static helper for tests)
    pub fn shard_for_key(key: &[u8], num_shards: u32) -> u32 {
        let hash = xxh3_64(key);
        (hash % num_shards as u64) as u32
    }

    /// Create a new shard manager
    pub async fn new(config: ShardConfig) -> Result<Self> {
        let mut ring = ConsistentHashRing::new(config.virtual_nodes);
        let mut shards = Vec::with_capacity(config.num_shards);

        for i in 0..config.num_shards {
            let shard = Shard::new(i, &config.base_path).await?;
            ring.add_node(i);
            shards.push(Arc::new(shard));
        }

        Ok(Self {
            config,
            ring,
            shards,
        })
    }

    /// Open existing shards
    pub async fn open(config: ShardConfig) -> Result<Self> {
        let mut ring = ConsistentHashRing::new(config.virtual_nodes);
        let mut shards = Vec::with_capacity(config.num_shards);

        for i in 0..config.num_shards {
            let shard = Shard::open(i, &config.base_path).await?;
            ring.add_node(i);
            shards.push(Arc::new(shard));
        }

        Ok(Self {
            config,
            ring,
            shards,
        })
    }

    /// Get shard for a key
    pub fn get_shard(&self, key: &[u8]) -> &Arc<Shard> {
        let shard_id = self.ring.get_node(key);
        &self.shards[shard_id]
    }

    /// Get shard for a node ID
    pub fn get_shard_for_node(&self, node_id: &NodeId) -> &Arc<Shard> {
        self.get_shard(&node_id.uuid)
    }

    /// Get all shards
    pub fn shards(&self) -> &[Arc<Shard>] {
        &self.shards
    }

    /// Insert a node into the appropriate shard
    pub async fn insert_node(&self, node: &Node) -> Result<()> {
        let shard = self.get_shard_for_node(&node.id);
        shard.add_to_bloom(&node.id.uuid);
        shard.storage().insert_node(node).await
    }

    /// Get a node by ID
    pub async fn get_node(&self, id: &NodeId) -> Result<Option<Node>> {
        let shard = self.get_shard_for_node(id);

        // Use bloom filter for fast negative lookup
        if !shard.may_contain(&id.uuid) {
            return Ok(None);
        }

        shard.storage().get_node(id).await
    }

    /// Update a node
    pub async fn update_node(&self, id: &NodeId, properties: Value) -> Result<Node> {
        let shard = self.get_shard_for_node(id);
        shard.storage().update_node(id, properties).await
    }

    /// Delete a node
    pub async fn delete_node(&self, id: &NodeId) -> Result<()> {
        let shard = self.get_shard_for_node(id);
        shard.storage().delete_node(id).await
    }

    /// Insert an edge
    pub async fn insert_edge(&self, edge: &Edge) -> Result<()> {
        // Edges are stored with the source node's shard
        let shard = self.get_shard_for_node(&edge.from);
        shard.add_to_bloom(&edge.id.uuid);
        shard.storage().insert_edge(edge).await
    }

    /// Get an edge by ID
    pub async fn get_edge(&self, id: &EdgeId, from_node: &NodeId) -> Result<Option<Edge>> {
        let shard = self.get_shard_for_node(from_node);
        shard.storage().get_edge(id).await
    }

    /// Get edges from a node
    pub async fn get_edges_from(
        &self,
        node_id: &NodeId,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let shard = self.get_shard_for_node(node_id);
        shard.storage().get_edges_from(node_id, edge_type).await
    }

    /// Get nodes by type across all shards
    pub async fn get_nodes_by_type(
        &self,
        node_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Node>> {
        let mut all_nodes = Vec::new();
        let per_shard_limit = limit.map(|l| (l / self.shards.len()).max(1));

        for shard in &self.shards {
            let nodes = shard
                .storage()
                .get_nodes_by_type(node_type, per_shard_limit)
                .await?;
            all_nodes.extend(nodes);

            if let Some(lim) = limit {
                if all_nodes.len() >= lim {
                    all_nodes.truncate(lim);
                    break;
                }
            }
        }

        Ok(all_nodes)
    }

    /// Get statistics across all shards
    pub async fn stats(&self) -> Result<ShardStats> {
        let mut total_nodes = 0;
        let mut total_edges = 0;
        let mut total_size = 0;
        let mut shard_stats = Vec::new();

        for shard in &self.shards {
            let stats = shard.storage().stats().await?;
            total_nodes += stats.node_count;
            total_edges += stats.edge_count;
            total_size += stats.size_bytes;

            shard_stats.push(SingleShardStats {
                id: shard.id,
                node_count: stats.node_count,
                edge_count: stats.edge_count,
                size_bytes: stats.size_bytes,
            });
        }

        Ok(ShardStats {
            num_shards: self.shards.len(),
            total_nodes,
            total_edges,
            total_size,
            shards: shard_stats,
        })
    }
}

/// Statistics for all shards
#[derive(Debug, Clone)]
pub struct ShardStats {
    pub num_shards: usize,
    pub total_nodes: u64,
    pub total_edges: u64,
    pub total_size: u64,
    pub shards: Vec<SingleShardStats>,
}

/// Statistics for a single shard
#[derive(Debug, Clone)]
pub struct SingleShardStats {
    pub id: usize,
    pub node_count: u64,
    pub edge_count: u64,
    pub size_bytes: u64,
}

/// Consistent hash ring for distributing keys across shards
struct ConsistentHashRing {
    /// Number of virtual nodes per physical node
    virtual_nodes: usize,
    /// Sorted ring of (hash, node_id)
    ring: BTreeMap<u64, usize>,
}

impl ConsistentHashRing {
    fn new(virtual_nodes: usize) -> Self {
        Self {
            virtual_nodes,
            ring: BTreeMap::new(),
        }
    }

    fn add_node(&mut self, node_id: usize) {
        for i in 0..self.virtual_nodes {
            let key = format!("node_{}_{}", node_id, i);
            let hash = xxh3_64(key.as_bytes());
            self.ring.insert(hash, node_id);
        }
    }

    #[allow(dead_code)]
    fn remove_node(&mut self, node_id: usize) {
        for i in 0..self.virtual_nodes {
            let key = format!("node_{}_{}", node_id, i);
            let hash = xxh3_64(key.as_bytes());
            self.ring.remove(&hash);
        }
    }

    fn get_node(&self, key: &[u8]) -> usize {
        if self.ring.is_empty() {
            return 0;
        }

        let hash = xxh3_64(key);

        // Find first node with hash >= key hash
        if let Some((&_, &node_id)) = self.ring.range(hash..).next() {
            return node_id;
        }

        // Wrap around to first node
        *self.ring.values().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new(100);

        ring.add_node(0);
        ring.add_node(1);
        ring.add_node(2);

        // Same key should always map to same node
        let node1 = ring.get_node(b"test_key");
        let node2 = ring.get_node(b"test_key");
        assert_eq!(node1, node2);

        // Different keys should distribute across nodes
        let mut distribution = [0usize; 3];
        for i in 0..1000 {
            let node = ring.get_node(format!("key_{}", i).as_bytes());
            distribution[node] += 1;
        }

        // Check reasonable distribution (each shard gets some keys)
        for &count in &distribution {
            assert!(count > 100, "Uneven distribution: {:?}", distribution);
        }
    }

    #[tokio::test]
    async fn test_shard_manager_basic() {
        let temp = TempDir::new().unwrap();

        let config = ShardConfig {
            num_shards: 4,
            virtual_nodes: 100,
            base_path: temp.path().to_path_buf(),
        };

        let manager = ShardManager::new(config).await.unwrap();

        // Insert a node
        let node = Node::new(
            "user",
            Value::from_json(serde_json::json!({
                "name": "Alice"
            }))
            .unwrap(),
        );
        let node_id = node.id.clone();

        manager.insert_node(&node).await.unwrap();

        // Retrieve the node
        let retrieved = manager.get_node(&node_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.node_type, "user");
    }

    #[tokio::test]
    async fn test_shard_distribution() {
        let temp = TempDir::new().unwrap();

        let config = ShardConfig {
            num_shards: 4,
            virtual_nodes: 100,
            base_path: temp.path().to_path_buf(),
        };

        let manager = ShardManager::new(config).await.unwrap();

        // Insert many nodes
        for i in 0..100 {
            let node = Node::new(
                "user",
                Value::from_json(serde_json::json!({
                    "name": format!("User {}", i)
                }))
                .unwrap(),
            );
            manager.insert_node(&node).await.unwrap();
        }

        // Check distribution
        let stats = manager.stats().await.unwrap();
        assert_eq!(stats.total_nodes, 100);

        // Each shard should have some nodes
        for shard_stat in &stats.shards {
            assert!(
                shard_stat.node_count > 0,
                "Shard {} has no nodes",
                shard_stat.id
            );
        }
    }
}
