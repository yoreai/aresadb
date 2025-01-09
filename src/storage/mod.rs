//! Storage Engine
//!
//! Unified storage layer supporting local filesystem and cloud bucket backends.

#![allow(dead_code)]
#![allow(unused_imports)]

mod bucket;
mod cache;
mod local;
mod node;
mod parallel;
pub mod tiered;
pub mod vector;
pub mod vector_index;

pub use bucket::BucketStorage;
pub use cache::CacheLayer;
pub use local::LocalStorage;
pub use node::{DistanceMetric, Edge, EdgeId, Node, NodeId, SimilarityResult, Timestamp, Value};
pub use parallel::{ParallelExecutor, ParallelTraversalResult, SnapshotReader};
pub use tiered::{NodeIndex, PayloadLocation, TieredConfig, TieredStats, TieredStorage};
pub use vector::{VectorNodeBuilder, VectorSearch};
pub use vector_index::{IndexStats, VectorIndex};

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database name
    pub name: String,
    /// Storage format version
    pub version: u32,
    /// When the database was created
    pub created_at: Timestamp,
    /// Remote cloud bucket URL for tiered storage (e.g. `s3://...` or `gs://...`)
    pub bucket_url: Option<String>,
}

/// Database status information
#[derive(Debug, Clone)]
pub struct DatabaseStatus {
    /// Database name
    pub name: String,
    /// Filesystem path to the database directory
    pub path: String,
    /// Total number of nodes
    pub node_count: u64,
    /// Total number of edges
    pub edge_count: u64,
    /// Number of distinct node types
    pub schema_count: u64,
    /// Total on-disk size in bytes
    pub size_bytes: u64,
}

/// Sync statistics
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Number of objects pushed to the remote bucket
    pub uploaded: u64,
    /// Number of objects pulled from the remote bucket
    pub downloaded: u64,
}

/// Graph representation for visualization
#[derive(Debug, Clone)]
pub struct GraphView {
    /// Nodes in the view
    pub nodes: Vec<Node>,
    /// Edges connecting the nodes
    pub edges: Vec<Edge>,
}

/// Key-value representation for visualization
#[derive(Debug, Clone)]
pub struct KvView {
    /// Key-value pairs where keys are node IDs and values are property maps
    pub entries: Vec<(String, Value)>,
}

/// Key for a vector index: (node_type, embedding_field)
type VectorIndexKey = (String, String);

/// Main database handle.
///
/// Uses tiered storage: graph index stays local (sub-ms), node payloads
/// can live locally or in cloud storage (S3/GCS) with transparent caching.
/// Includes managed HNSW vector indexes for fast approximate nearest neighbor search.
#[derive(Clone)]
pub struct Database {
    /// Path to the database
    path: PathBuf,
    /// Database configuration
    config: Arc<RwLock<DatabaseConfig>>,
    /// Tiered storage engine (local + cache + optional cloud)
    tiered: TieredStorage,
    /// Direct local storage handle (for legacy/edge operations)
    local: LocalStorage,
    /// Managed HNSW vector indexes, keyed by (node_type, embedding_field)
    vector_indexes: Arc<RwLock<HashMap<VectorIndexKey, Arc<VectorIndex>>>>,
}

impl Database {
    /// Create a new database at the given path
    pub async fn create(path: impl AsRef<Path>, name: &str) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory structure
        std::fs::create_dir_all(&path).context("Failed to create database directory")?;
        std::fs::create_dir_all(path.join(".aresadb"))
            .context("Failed to create .aresadb directory")?;

        let config = DatabaseConfig {
            name: name.to_string(),
            version: crate::FORMAT_VERSION,
            created_at: Timestamp::now(),
            bucket_url: None,
        };

        // Write config file
        let config_path = path.join(".aresadb/config.toml");
        let config_str = toml::to_string_pretty(&config)?;
        std::fs::write(&config_path, config_str)?;

        // Initialize local storage
        let local = LocalStorage::create(&path).await?;
        let tiered_config = TieredConfig::default();
        let tiered = TieredStorage::new(local.clone(), tiered_config);

        Ok(Self {
            path,
            config: Arc::new(RwLock::new(config)),
            tiered,
            local,
            vector_indexes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Open an existing database
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Load config
        let config_path = path.join(".aresadb/config.toml");
        let config_str = std::fs::read_to_string(&config_path)
            .context("Failed to read database config. Is this an aresadb database?")?;
        let config: DatabaseConfig = toml::from_str(&config_str)?;

        // Open local storage
        let local = LocalStorage::open(&path).await?;

        // Auto-migrate legacy databases to tiered format
        let migrated = local.migrate_to_tiered().await?;
        if migrated > 0 {
            tracing::info!("Migrated {} nodes to tiered storage format", migrated);
        }

        let tiered_config = TieredConfig::default();

        // Connect to bucket if configured
        let tiered = if let Some(ref url) = config.bucket_url {
            let bucket = BucketStorage::connect(url).await?;
            TieredStorage::with_bucket(local.clone(), bucket, tiered_config)
        } else {
            TieredStorage::new(local.clone(), tiered_config)
        };

        Ok(Self {
            path,
            config: Arc::new(RwLock::new(config)),
            tiered,
            local,
            vector_indexes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Connect to a remote bucket database
    pub async fn connect_bucket(url: &str, _readonly: bool) -> Result<Self> {
        let bucket = BucketStorage::connect(url).await?;
        let config = bucket.load_config().await?;

        // Create temporary local cache
        let temp_path = std::env::temp_dir().join(format!("aresadb-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_path)?;

        let local = LocalStorage::create(&temp_path).await?;
        let tiered_config = TieredConfig {
            cache_max_bytes: 500 * 1024 * 1024, // 500MB for remote-primary mode
            ..TieredConfig::default()
        };
        let tiered = TieredStorage::with_bucket(local.clone(), bucket, tiered_config);

        Ok(Self {
            path: temp_path,
            config: Arc::new(RwLock::new(config)),
            tiered,
            local,
            vector_indexes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get database status
    pub async fn status(&self) -> Result<DatabaseStatus> {
        let name = self.config.read().name.clone();
        let stats = self.local.stats().await?;

        Ok(DatabaseStatus {
            name,
            path: self.path.display().to_string(),
            node_count: stats.node_count,
            edge_count: stats.edge_count,
            schema_count: stats.schema_count,
            size_bytes: stats.size_bytes,
        })
    }

    // ========== Node Operations (via tiered storage) ==========

    /// Insert a new node
    pub async fn insert_node(
        &self,
        node_type: &str,
        properties: serde_json::Value,
    ) -> Result<Node> {
        let props = Value::from_json(properties)?;
        let node = Node::new(node_type, props);
        self.tiered.insert_node(&node).await?;
        Ok(node)
    }

    /// Batch insert multiple nodes in a single transaction.
    /// Returns the inserted nodes. Orders of magnitude faster than individual inserts.
    pub async fn insert_nodes_batch(
        &self,
        items: Vec<(&str, serde_json::Value)>,
    ) -> Result<Vec<Node>> {
        let mut nodes = Vec::with_capacity(items.len());
        for (node_type, properties) in items {
            let props = Value::from_json(properties)?;
            nodes.push(Node::new(node_type, props));
        }
        self.tiered.insert_nodes_batch(&nodes).await?;
        Ok(nodes)
    }

    /// Get a node by ID
    pub async fn get_node(&self, id: &str) -> Result<Option<Node>> {
        let node_id = NodeId::parse(id)?;
        self.tiered.get_node(&node_id).await
    }

    /// Update a node's properties
    pub async fn update_node(&self, id: &str, properties: serde_json::Value) -> Result<Node> {
        let node_id = NodeId::parse(id)?;
        let props = Value::from_json(properties)?;
        self.tiered.update_node(&node_id, props).await
    }

    /// Delete a node and its edges
    pub async fn delete_node(&self, id: &str) -> Result<()> {
        let node_id = NodeId::parse(id)?;
        self.tiered.delete_node(&node_id).await
    }

    /// Get all nodes of a specific type
    pub async fn get_all_by_type(
        &self,
        node_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Node>> {
        self.tiered.get_nodes_by_type(node_type, limit).await
    }

    // ========== Edge Operations ==========

    /// Create an edge between two nodes
    pub async fn create_edge(
        &self,
        from_id: &str,
        to_id: &str,
        edge_type: &str,
        properties: Option<serde_json::Value>,
    ) -> Result<Edge> {
        let from = NodeId::parse(from_id)?;
        let to = NodeId::parse(to_id)?;
        let props = properties
            .map(Value::from_json)
            .transpose()?
            .unwrap_or(Value::Object(Default::default()));

        let edge = Edge::new(from, to, edge_type, props);
        self.tiered.insert_edge(&edge).await?;
        Ok(edge)
    }

    /// Batch create edges in a single transaction.
    /// Much faster for bulk graph construction.
    pub async fn create_edges_batch(&self, edges: Vec<(&str, &str, &str)>) -> Result<Vec<Edge>> {
        let mut edge_objects = Vec::with_capacity(edges.len());
        for (from_id, to_id, edge_type) in edges {
            let from = NodeId::parse(from_id)?;
            let to = NodeId::parse(to_id)?;
            edge_objects.push(Edge::new(from, to, edge_type, Value::Null));
        }
        self.tiered.insert_edges_batch(&edge_objects).await?;
        Ok(edge_objects)
    }

    /// Get edges from a node
    pub async fn get_edges_from(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let id = NodeId::parse(node_id)?;
        self.tiered.get_edges_from(&id, edge_type).await
    }

    /// Get edges to a node
    pub async fn get_edges_to(&self, node_id: &str, edge_type: Option<&str>) -> Result<Vec<Edge>> {
        let id = NodeId::parse(node_id)?;
        self.tiered.get_edges_to(&id, edge_type).await
    }

    /// Delete an edge
    pub async fn delete_edge(&self, edge_id: &str) -> Result<()> {
        let id = EdgeId::parse(edge_id)?;
        self.tiered.delete_edge(&id).await
    }

    // ========== View Operations ==========

    /// Get data as a graph view
    pub async fn get_as_graph(&self, node_type: &str, limit: Option<usize>) -> Result<GraphView> {
        let nodes = self.get_all_by_type(node_type, limit).await?;
        let mut edges = Vec::new();

        for node in &nodes {
            let node_edges = self.tiered.get_edges_from(&node.id, None).await?;
            edges.extend(node_edges);
        }

        Ok(GraphView { nodes, edges })
    }

    /// Get data as key-value pairs
    pub async fn get_as_kv(&self, node_type: &str, limit: Option<usize>) -> Result<KvView> {
        let nodes = self.get_all_by_type(node_type, limit).await?;
        let entries: Vec<(String, Value)> = nodes
            .into_iter()
            .map(|n| (n.id.to_string(), Value::Object(n.properties)))
            .collect();

        Ok(KvView { entries })
    }

    // ========== Cloud Tiering Operations ==========

    /// Push database to a cloud bucket
    pub async fn push_to_bucket(&self, url: &str) -> Result<()> {
        let bucket = BucketStorage::connect(url).await?;

        // Save config
        let config = self.config.read().clone();
        bucket.save_config(&config).await?;

        // Upload data files
        bucket.upload_from_local(&self.path).await?;

        // Update local config with bucket URL
        drop(config);
        self.config.write().bucket_url = Some(url.to_string());
        self.save_config()?;

        Ok(())
    }

    /// Sync local database with remote bucket
    pub async fn sync_with_bucket(&self, url: &str) -> Result<SyncStats> {
        let bucket = BucketStorage::connect(url).await?;
        let stats = bucket.sync_with_local(&self.path).await?;
        Ok(stats)
    }

    /// Run eviction: move cold node payloads from local to cloud storage.
    /// Returns the number of payloads evicted.
    pub async fn run_eviction(&self) -> Result<u64> {
        self.tiered.run_eviction().await
    }

    /// Get tiered storage statistics (cache hits, cloud fetches, etc.)
    pub fn tiered_stats(&self) -> TieredStats {
        self.tiered.stats()
    }

    /// Get the tiered storage engine directly
    pub fn tiered(&self) -> &TieredStorage {
        &self.tiered
    }

    /// Save config to disk
    fn save_config(&self) -> Result<()> {
        let config = self.config.read();
        let config_str = toml::to_string_pretty(&*config)?;
        std::fs::write(self.path.join(".aresadb/config.toml"), config_str)?;
        Ok(())
    }

    /// Get the local storage handle
    pub fn local(&self) -> &LocalStorage {
        &self.local
    }

    /// Get database path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get database name
    pub fn name(&self) -> String {
        self.config.read().name.clone()
    }

    // ========== Secondary Index Operations ==========

    /// Create a secondary index on a property field for faster SQL lookups.
    /// Returns the number of existing nodes that were indexed.
    pub async fn create_index(&self, node_type: &str, field: &str) -> Result<u64> {
        self.local.create_property_index(node_type, field).await
    }

    /// Drop a secondary index
    pub async fn drop_index(&self, node_type: &str, field: &str) -> Result<()> {
        self.local.drop_property_index(node_type, field).await
    }

    /// List all secondary indexes
    pub fn list_indexes(&self) -> Result<Vec<(String, String)>> {
        self.local.list_indexes()
    }

    /// Look up nodes by an indexed property value.
    /// Returns None if no index exists, Some(nodes) if indexed.
    pub async fn index_lookup(
        &self,
        node_type: &str,
        field: &str,
        value: &Value,
    ) -> Result<Option<Vec<Node>>> {
        let node_ids = self.local.index_lookup(node_type, field, value).await?;

        match node_ids {
            None => Ok(None),
            Some(ids) => {
                let mut nodes = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(node) = self.tiered.get_node(&id).await? {
                        nodes.push(node);
                    }
                }
                Ok(Some(nodes))
            }
        }
    }

    // ========== Full-Text Search Operations ==========

    /// Create a full-text search index on a string property.
    /// Tokenizes and indexes all existing values using an inverted index with BM25 scoring.
    pub async fn create_fulltext_index(&self, node_type: &str, field: &str) -> Result<u64> {
        self.local.create_fulltext_index(node_type, field).await
    }

    /// Execute a full-text search query using BM25 ranking.
    /// Returns nodes sorted by relevance score.
    pub async fn fulltext_search(
        &self,
        node_type: &str,
        field: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(Node, f64)>> {
        let results = self
            .local
            .fulltext_search(node_type, field, query, limit)
            .await?;

        let mut nodes_with_scores = Vec::with_capacity(results.len());
        for (id, score) in results {
            if let Some(node) = self.tiered.get_node(&id).await? {
                nodes_with_scores.push((node, score));
            }
        }

        Ok(nodes_with_scores)
    }

    /// List all full-text indexes
    pub fn list_fulltext_indexes(&self) -> Result<Vec<(String, String)>> {
        self.local.list_fulltext_indexes()
    }

    // ========== Vector/Embedding Operations ==========

    /// Insert a node with a vector embedding.
    /// Automatically adds the vector to the managed HNSW index.
    pub async fn insert_with_embedding(
        &self,
        node_type: &str,
        properties: serde_json::Value,
        embedding_field: &str,
        embedding: Vec<f32>,
    ) -> Result<Node> {
        let dim = embedding.len();
        let mut props = Value::from_json(properties)?;

        if let Value::Object(ref mut map) = props {
            map.insert(
                embedding_field.to_string(),
                Value::Vector(embedding.clone()),
            );
        }

        let node = Node::new(node_type, props);
        self.tiered.insert_node(&node).await?;

        // Add to HNSW index
        let key = (node_type.to_string(), embedding_field.to_string());
        let index = self.get_or_create_vector_index(&key, dim);
        if let Err(e) = index.insert(node.id.clone(), embedding) {
            tracing::warn!("Failed to add vector to HNSW index: {}", e);
        }

        Ok(node)
    }

    /// Perform similarity search using the HNSW index (fast, approximate)
    /// or falls back to brute-force scan if the index hasn't been built yet.
    pub async fn similarity_search(
        &self,
        query_vector: &[f32],
        node_type: &str,
        embedding_field: &str,
        k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SimilarityResult>> {
        let key = (node_type.to_string(), embedding_field.to_string());

        // Try HNSW index first
        {
            let indexes = self.vector_indexes.read();
            if let Some(index) = indexes.get(&key) {
                if !index.is_empty() {
                    let ann_results = index.search(query_vector, k)?;

                    let results: Vec<SimilarityResult> = ann_results
                        .into_iter()
                        .map(|(node_id, distance)| {
                            let score = match metric {
                                DistanceMetric::Cosine => 1.0 - distance as f64,
                                DistanceMetric::DotProduct => -(distance as f64),
                                _ => 1.0 / (1.0 + distance as f64),
                            };
                            SimilarityResult {
                                node_id,
                                score,
                                distance: distance as f64,
                            }
                        })
                        .collect();

                    return Ok(results);
                }
            }
        }

        // Fall back: build index from stored data, then search
        let nodes = self.tiered.get_nodes_by_type(node_type, None).await?;

        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        // Detect vector dimension from first node with the field
        let dim = nodes
            .iter()
            .filter_map(|n| n.properties.get(embedding_field))
            .filter_map(|v| v.vector_dimension())
            .next();

        if let Some(dim) = dim {
            let index = self.get_or_create_vector_index(&key, dim);

            // Populate index if empty (lazy build)
            if index.is_empty() {
                for node in &nodes {
                    if let Some(Value::Vector(v)) = node.properties.get(embedding_field) {
                        let _ = index.insert(node.id.clone(), v.clone());
                    }
                }
            }

            // Search using HNSW
            let ann_results = index.search(query_vector, k)?;
            let results: Vec<SimilarityResult> = ann_results
                .into_iter()
                .map(|(node_id, distance)| {
                    let score = match metric {
                        DistanceMetric::Cosine => 1.0 - distance as f64,
                        DistanceMetric::DotProduct => -(distance as f64),
                        _ => 1.0 / (1.0 + distance as f64),
                    };
                    SimilarityResult {
                        node_id,
                        score,
                        distance: distance as f64,
                    }
                })
                .collect();

            Ok(results)
        } else {
            // No vectors found, use brute force as last resort
            let search = VectorSearch::new(metric);
            Ok(search.search(query_vector, &nodes, embedding_field, k))
        }
    }

    /// Find similar nodes within a distance threshold (brute-force, exact)
    pub async fn similarity_search_radius(
        &self,
        query_vector: &[f32],
        node_type: &str,
        embedding_field: &str,
        max_distance: f64,
        metric: DistanceMetric,
    ) -> Result<Vec<SimilarityResult>> {
        let nodes = self.tiered.get_nodes_by_type(node_type, None).await?;
        let search = VectorSearch::new(metric);
        let results = search.search_radius(query_vector, &nodes, embedding_field, max_distance);
        Ok(results)
    }

    /// Get a node and its embedding
    pub async fn get_node_with_embedding(
        &self,
        id: &str,
        embedding_field: &str,
    ) -> Result<Option<(Node, Option<Vec<f32>>)>> {
        let node = self.get_node(id).await?;

        Ok(node.map(|n| {
            let embedding = n
                .properties
                .get(embedding_field)
                .and_then(|v| v.as_vector())
                .map(|v| v.to_vec());
            (n, embedding)
        }))
    }

    /// Get or create a managed HNSW vector index for a (node_type, field) pair
    fn get_or_create_vector_index(
        &self,
        key: &VectorIndexKey,
        dimension: usize,
    ) -> Arc<VectorIndex> {
        {
            let indexes = self.vector_indexes.read();
            if let Some(index) = indexes.get(key) {
                return Arc::clone(index);
            }
        }

        let mut indexes = self.vector_indexes.write();
        // Double-check after acquiring write lock
        if let Some(index) = indexes.get(key) {
            return Arc::clone(index);
        }

        let index = Arc::new(VectorIndex::with_params(
            dimension,
            16,
            4,
            DistanceMetric::Cosine,
        ));
        indexes.insert(key.clone(), Arc::clone(&index));
        index
    }

    /// Manually rebuild the HNSW index for a (node_type, field) pair.
    /// Useful after bulk inserts without embeddings going through insert_with_embedding.
    pub async fn rebuild_vector_index(
        &self,
        node_type: &str,
        embedding_field: &str,
    ) -> Result<IndexStats> {
        let nodes = self.tiered.get_nodes_by_type(node_type, None).await?;

        let dim = nodes
            .iter()
            .filter_map(|n| n.properties.get(embedding_field))
            .filter_map(|v| v.vector_dimension())
            .next()
            .ok_or_else(|| {
                anyhow::anyhow!("No vectors found in {}.{}", node_type, embedding_field)
            })?;

        let key = (node_type.to_string(), embedding_field.to_string());

        // Create fresh index
        let index = Arc::new(VectorIndex::with_params(dim, 16, 4, DistanceMetric::Cosine));

        for node in &nodes {
            if let Some(Value::Vector(v)) = node.properties.get(embedding_field) {
                index.insert(node.id.clone(), v.clone())?;
            }
        }

        let stats = index.stats();
        self.vector_indexes.write().insert(key, index);
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_database() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();
        assert_eq!(db.name(), "testdb");
    }

    #[tokio::test]
    async fn test_insert_and_get_node() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        let props = serde_json::json!({
            "name": "John",
            "age": 30
        });
        let node = db.insert_node("user", props).await.unwrap();

        let retrieved = db.get_node(&node.id.to_string()).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.node_type, "user");
    }

    #[tokio::test]
    async fn test_batch_insert_nodes() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        let items: Vec<(&str, serde_json::Value)> = (0..100)
            .map(|i| {
                (
                    "item",
                    serde_json::json!({"index": i, "name": format!("item_{}", i)}),
                )
            })
            .collect();

        let nodes = db.insert_nodes_batch(items).await.unwrap();
        assert_eq!(nodes.len(), 100);

        let all = db.get_all_by_type("item", None).await.unwrap();
        assert_eq!(all.len(), 100);

        let status = db.status().await.unwrap();
        assert_eq!(status.node_count, 100);
    }

    #[tokio::test]
    async fn test_batch_insert_edges() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        let n1 = db
            .insert_node("user", serde_json::json!({"name": "A"}))
            .await
            .unwrap();
        let n2 = db
            .insert_node("user", serde_json::json!({"name": "B"}))
            .await
            .unwrap();
        let n3 = db
            .insert_node("user", serde_json::json!({"name": "C"}))
            .await
            .unwrap();

        let id1 = n1.id.to_string();
        let id2 = n2.id.to_string();
        let id3 = n3.id.to_string();

        let edges = db
            .create_edges_batch(vec![
                (&id1, &id2, "follows"),
                (&id2, &id3, "follows"),
                (&id1, &id3, "knows"),
            ])
            .await
            .unwrap();

        assert_eq!(edges.len(), 3);

        let from_1 = db.get_edges_from(&id1, None).await.unwrap();
        assert_eq!(from_1.len(), 2);
    }

    #[tokio::test]
    async fn test_managed_hnsw_insert_and_search() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        for i in 0..50 {
            let embedding = vec![(i as f32) / 50.0, 1.0 - (i as f32) / 50.0, 0.5];
            db.insert_with_embedding(
                "doc",
                serde_json::json!({"title": format!("doc_{}", i)}),
                "embedding",
                embedding,
            )
            .await
            .unwrap();
        }

        let query = vec![0.5, 0.5, 0.5];
        let results = db
            .similarity_search(&query, "doc", "embedding", 5, DistanceMetric::Cosine)
            .await
            .unwrap();

        assert_eq!(results.len(), 5);
        // Results should be sorted by score descending (distance ascending)
        for i in 1..results.len() {
            assert!(results[i].distance >= results[i - 1].distance);
        }
    }

    #[tokio::test]
    async fn test_rebuild_vector_index() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        // Bulk insert without using insert_with_embedding
        let items: Vec<(&str, serde_json::Value)> = (0..20)
            .map(|i| {
                let embedding: Vec<serde_json::Value> = vec![
                    serde_json::json!((i as f64) / 20.0),
                    serde_json::json!(1.0 - (i as f64) / 20.0),
                ];
                (
                    "doc",
                    serde_json::json!({
                        "title": format!("doc_{}", i),
                        "embedding": { "$vector": embedding }
                    }),
                )
            })
            .collect();

        db.insert_nodes_batch(items).await.unwrap();

        // Rebuild index explicitly
        let stats = db.rebuild_vector_index("doc", "embedding").await.unwrap();
        assert_eq!(stats.num_vectors, 20);

        // Search should work
        let results = db
            .similarity_search(&[0.5, 0.5], "doc", "embedding", 3, DistanceMetric::Cosine)
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_status_node_count_tiered() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        db.insert_node("user", serde_json::json!({"name": "A"}))
            .await
            .unwrap();
        db.insert_node("user", serde_json::json!({"name": "B"}))
            .await
            .unwrap();
        db.insert_node("product", serde_json::json!({"name": "P"}))
            .await
            .unwrap();

        let status = db.status().await.unwrap();
        assert_eq!(status.node_count, 3);
    }

    #[tokio::test]
    async fn test_secondary_index_create_and_lookup() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        // Insert test data
        db.insert_node("user", serde_json::json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        db.insert_node("user", serde_json::json!({"name": "Bob", "age": 25}))
            .await
            .unwrap();
        db.insert_node("user", serde_json::json!({"name": "Charlie", "age": 30}))
            .await
            .unwrap();

        // Before index: lookup returns None
        let result = db
            .index_lookup("user", "age", &Value::Int(30))
            .await
            .unwrap();
        assert!(result.is_none());

        // Create index — should back-fill existing data
        let indexed = db.create_index("user", "age").await.unwrap();
        assert_eq!(indexed, 3);

        // Now lookup should return results
        let result = db
            .index_lookup("user", "age", &Value::Int(30))
            .await
            .unwrap();
        assert!(result.is_some());
        let nodes = result.unwrap();
        assert_eq!(nodes.len(), 2);

        // Lookup for age=25 should return 1
        let result = db
            .index_lookup("user", "age", &Value::Int(25))
            .await
            .unwrap();
        assert_eq!(result.unwrap().len(), 1);

        // List indexes
        let indexes = db.list_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0], ("user".to_string(), "age".to_string()));
    }

    #[tokio::test]
    async fn test_secondary_index_auto_maintain() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        // Create index first, then insert data
        db.create_index("user", "city").await.unwrap();

        db.insert_node("user", serde_json::json!({"name": "Alice", "city": "NYC"}))
            .await
            .unwrap();
        db.insert_node("user", serde_json::json!({"name": "Bob", "city": "LA"}))
            .await
            .unwrap();
        db.insert_node(
            "user",
            serde_json::json!({"name": "Charlie", "city": "NYC"}),
        )
        .await
        .unwrap();

        // Lookup should find nodes inserted after index creation
        let result = db
            .index_lookup("user", "city", &Value::String("NYC".to_string()))
            .await
            .unwrap();
        assert_eq!(result.unwrap().len(), 2);

        let result = db
            .index_lookup("user", "city", &Value::String("LA".to_string()))
            .await
            .unwrap();
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_secondary_index_drop() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        db.insert_node(
            "product",
            serde_json::json!({"name": "Widget", "category": "tools"}),
        )
        .await
        .unwrap();

        db.create_index("product", "category").await.unwrap();

        let result = db
            .index_lookup("product", "category", &Value::String("tools".to_string()))
            .await
            .unwrap();
        assert_eq!(result.unwrap().len(), 1);

        // Drop the index
        db.drop_index("product", "category").await.unwrap();

        // Should return None now
        let result = db
            .index_lookup("product", "category", &Value::String("tools".to_string()))
            .await
            .unwrap();
        assert!(result.is_none());

        // List should be empty
        assert!(db.list_indexes().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_fulltext_search_basic() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        // Insert documents
        db.insert_node("article", serde_json::json!({
            "title": "Introduction to Machine Learning",
            "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms"
        })).await.unwrap();
        db.insert_node("article", serde_json::json!({
            "title": "Deep Learning Networks",
            "content": "Deep learning uses neural networks with many layers for complex pattern recognition"
        })).await.unwrap();
        db.insert_node("article", serde_json::json!({
            "title": "Database Systems",
            "content": "Modern database systems provide ACID transactions and efficient query processing"
        })).await.unwrap();

        // Create full-text index on content
        let indexed = db
            .create_fulltext_index("article", "content")
            .await
            .unwrap();
        assert_eq!(indexed, 3);

        // Search for "machine learning"
        let results = db
            .fulltext_search("article", "content", "machine learning", 10)
            .await
            .unwrap();
        assert!(!results.is_empty());
        // The ML article should rank highest
        assert!(results[0]
            .0
            .properties
            .get("title")
            .unwrap()
            .to_string()
            .contains("Machine"));

        // Search for "neural networks"
        let results = db
            .fulltext_search("article", "content", "neural networks", 10)
            .await
            .unwrap();
        assert!(!results.is_empty());
        assert!(results[0]
            .0
            .properties
            .get("title")
            .unwrap()
            .to_string()
            .contains("Deep"));

        // Search for "database"
        let results = db
            .fulltext_search("article", "content", "database", 10)
            .await
            .unwrap();
        assert!(!results.is_empty());
        assert!(results[0]
            .0
            .properties
            .get("title")
            .unwrap()
            .to_string()
            .contains("Database"));
    }

    #[tokio::test]
    async fn test_fulltext_search_auto_index() {
        let temp = TempDir::new().unwrap();
        let db = Database::create(temp.path(), "testdb").await.unwrap();

        // Create index first, then insert
        db.create_fulltext_index("doc", "body").await.unwrap();

        db.insert_node(
            "doc",
            serde_json::json!({
                "body": "Rust programming language is fast and safe"
            }),
        )
        .await
        .unwrap();
        db.insert_node(
            "doc",
            serde_json::json!({
                "body": "Python programming language is easy to learn"
            }),
        )
        .await
        .unwrap();

        // Search should find docs inserted after index creation
        let results = db
            .fulltext_search("doc", "body", "rust fast", 10)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0]
            .0
            .properties
            .get("body")
            .unwrap()
            .to_string()
            .contains("Rust"));
    }
}
