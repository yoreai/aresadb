//! Local filesystem storage backend using redb
//!
//! Provides ACID-compliant persistent storage with B+ tree indexes.

#![allow(dead_code)]

use anyhow::{Context, Result};
use parking_lot::RwLock;
use redb::{
    Database as RedbDatabase, MultimapTableDefinition, ReadableMultimapTable, ReadableTable,
    ReadableTableMetadata, TableDefinition,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::node::{Edge, EdgeId, Node, NodeId, Timestamp, Value};
use super::tiered::{NodeIndex, PayloadLocation};

// Table definitions for redb

// Legacy table: full JSON nodes (used for backward compatibility / migration)
const NODES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("nodes");

// Tiered storage tables
const NODE_INDEX_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("node_indexes");
const NODE_PAYLOADS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("node_payloads");

const EDGES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("edges");
const NODE_TYPE_INDEX: MultimapTableDefinition<&str, &[u8]> =
    MultimapTableDefinition::new("node_type_index");
const EDGE_FROM_INDEX: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("edge_from_index");
const EDGE_TO_INDEX: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("edge_to_index");
const EDGE_TYPE_INDEX: MultimapTableDefinition<&str, &[u8]> =
    MultimapTableDefinition::new("edge_type_index");
const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

// Secondary property index: composite key "type\0field\0value" → NodeId bytes
const PROPERTY_INDEX: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("property_index");
// Tracks which indexes exist: key "type\0field" → empty value
const INDEX_REGISTRY: TableDefinition<&str, &[u8]> = TableDefinition::new("index_registry");

// Full-text inverted index: key "type\0field\0token" → NodeId bytes
const FULLTEXT_INDEX: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("fulltext_index");
// Tracks which full-text indexes exist: key "type\0field" → empty value
const FULLTEXT_REGISTRY: TableDefinition<&str, &[u8]> = TableDefinition::new("fulltext_registry");
// Document term frequency: key = node_id_bytes ++ "type\0field" → JSON {token: count}
const FULLTEXT_DOC_FREQ: TableDefinition<&[u8], &[u8]> = TableDefinition::new("fulltext_doc_freq");

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub schema_count: u64,
    pub size_bytes: u64,
}

/// Local storage backend using redb
#[derive(Clone)]
pub struct LocalStorage {
    /// Path to the database directory
    path: PathBuf,
    /// redb database handle
    db: Arc<RwLock<RedbDatabase>>,
}

impl LocalStorage {
    /// Create a new local storage at the given path
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db_path = path.join(".aresadb/data.redb");

        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = RedbDatabase::create(&db_path).context("Failed to create redb database")?;

        // Initialize tables (including tiered storage tables)
        {
            let write_txn = db.begin_write()?;
            {
                let _ = write_txn.open_table(NODES_TABLE)?;
                let _ = write_txn.open_table(NODE_INDEX_TABLE)?;
                let _ = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
                let _ = write_txn.open_table(EDGES_TABLE)?;
                let _ = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
                let _ = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
                let _ = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
                let _ = write_txn.open_multimap_table(EDGE_TYPE_INDEX)?;
                let _ = write_txn.open_table(METADATA_TABLE)?;
                let _ = write_txn.open_multimap_table(PROPERTY_INDEX)?;
                let _ = write_txn.open_table(INDEX_REGISTRY)?;
                let _ = write_txn.open_multimap_table(FULLTEXT_INDEX)?;
                let _ = write_txn.open_table(FULLTEXT_REGISTRY)?;
                let _ = write_txn.open_table(FULLTEXT_DOC_FREQ)?;
            }
            write_txn.commit()?;
        }

        // Initialize metadata
        {
            let write_txn = db.begin_write()?;
            {
                let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
                let now = Timestamp::now();
                let created_bytes = serde_json::to_vec(&now)?;
                meta_table.insert("created_at", created_bytes.as_slice())?;

                let version_bytes = serde_json::to_vec(&crate::FORMAT_VERSION)?;
                meta_table.insert("version", version_bytes.as_slice())?;
            }
            write_txn.commit()?;
        }

        Ok(Self {
            path,
            db: Arc::new(RwLock::new(db)),
        })
    }

    /// Open an existing local storage
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db_path = path.join(".aresadb/data.redb");

        let db = RedbDatabase::open(&db_path).context("Failed to open redb database")?;

        Ok(Self {
            path,
            db: Arc::new(RwLock::new(db)),
        })
    }

    /// Get storage statistics
    pub async fn stats(&self) -> Result<StorageStats> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        // Prefer tiered index table for node count, fall back to legacy
        let node_count = if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            let tiered_count = index_table.len()?;
            if tiered_count > 0 {
                tiered_count
            } else {
                let nodes_table = read_txn.open_table(NODES_TABLE)?;
                nodes_table.len()?
            }
        } else {
            let nodes_table = read_txn.open_table(NODES_TABLE)?;
            nodes_table.len()?
        };

        let edges_table = read_txn.open_table(EDGES_TABLE)?;
        let edge_count = edges_table.len()?;

        let db_path = self.path.join(".aresadb/data.redb");
        let size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

        Ok(StorageStats {
            node_count,
            edge_count,
            schema_count: 0,
            size_bytes,
        })
    }

    // ========== Node Operations ==========

    /// Insert a new node
    pub async fn insert_node(&self, node: &Node) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            // Serialize node
            let node_bytes = serde_json::to_vec(node)?;
            let id_bytes = node.id.uuid;

            // Insert into nodes table
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            nodes_table.insert(id_bytes.as_slice(), node_bytes.as_slice())?;

            // Update type index
            let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
            type_index.insert(node.node_type.as_str(), id_bytes.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get a node by ID
    pub async fn get_node(&self, id: &NodeId) -> Result<Option<Node>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        if let Some(data) = nodes_table.get(id.uuid.as_slice())? {
            let node: Node = serde_json::from_slice(data.value())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    /// Update a node's properties
    pub async fn update_node(&self, id: &NodeId, properties: Value) -> Result<Node> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        let node = {
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;

            // Get existing node - clone data to release borrow
            let node_data = {
                let guard = nodes_table
                    .get(id.uuid.as_slice())?
                    .ok_or_else(|| anyhow::anyhow!("Node not found: {}", id))?;
                guard.value().to_vec()
            };

            let mut node: Node = serde_json::from_slice(&node_data)?;

            // Update properties
            if let Value::Object(new_props) = properties {
                for (k, v) in new_props {
                    node.properties.insert(k, v);
                }
            }
            node.updated_at = Timestamp::now();

            // Save updated node
            let node_bytes = serde_json::to_vec(&node)?;
            nodes_table.insert(id.uuid.as_slice(), node_bytes.as_slice())?;

            node
        };

        write_txn.commit()?;
        Ok(node)
    }

    /// Delete a node and its edges
    pub async fn delete_node(&self, id: &NodeId) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            // Get node to find its type
            let nodes_table = write_txn.open_table(NODES_TABLE)?;
            if let Some(data) = nodes_table.get(id.uuid.as_slice())? {
                let node: Node = serde_json::from_slice(data.value())?;

                // Remove from type index
                let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
                type_index.remove(node.node_type.as_str(), id.uuid.as_slice())?;
            }
            drop(nodes_table);

            // Remove node
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            nodes_table.remove(id.uuid.as_slice())?;

            // Remove edges from this node
            let edge_from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
            let edge_ids: Vec<Vec<u8>> = edge_from_index
                .get(id.uuid.as_slice())?
                .map(|r| r.map(|v| v.value().to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(edge_from_index);

            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_from = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
            let _edge_to = write_txn.open_multimap_table(EDGE_TO_INDEX)?;

            for edge_id in edge_ids {
                edges_table.remove(edge_id.as_slice())?;
                edge_from.remove(id.uuid.as_slice(), edge_id.as_slice())?;
            }

            // Remove edges to this node
            drop(edges_table);
            drop(edge_from);
            drop(_edge_to);

            let edge_to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
            let edge_ids: Vec<Vec<u8>> = edge_to_index
                .get(id.uuid.as_slice())?
                .map(|r| r.map(|v| v.value().to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(edge_to_index);

            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_to = write_txn.open_multimap_table(EDGE_TO_INDEX)?;

            for edge_id in edge_ids {
                edges_table.remove(edge_id.as_slice())?;
                edge_to.remove(id.uuid.as_slice(), edge_id.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get all nodes of a specific type
    pub async fn get_nodes_by_type(
        &self,
        node_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Node>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let type_index = read_txn.open_multimap_table(NODE_TYPE_INDEX)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let mut nodes = Vec::new();
        let max_count = limit.unwrap_or(usize::MAX);

        for result in type_index.get(node_type)? {
            if nodes.len() >= max_count {
                break;
            }

            let id_bytes = result?.value().to_vec();
            if let Some(data) = nodes_table.get(id_bytes.as_slice())? {
                let node: Node = serde_json::from_slice(data.value())?;
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Get all nodes (with optional limit)
    pub async fn get_all_nodes(&self, limit: Option<usize>) -> Result<Vec<Node>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let mut nodes = Vec::new();
        let max_count = limit.unwrap_or(usize::MAX);

        for result in nodes_table.iter()? {
            if nodes.len() >= max_count {
                break;
            }

            let (_, data) = result?;
            let node: Node = serde_json::from_slice(data.value())?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    // ========== Edge Operations ==========

    /// Insert a new edge
    pub async fn insert_edge(&self, edge: &Edge) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            // Serialize edge
            let edge_bytes = serde_json::to_vec(edge)?;
            let id_bytes = edge.id.uuid;

            // Insert into edges table
            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            edges_table.insert(id_bytes.as_slice(), edge_bytes.as_slice())?;

            // Update from index
            let mut from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
            from_index.insert(edge.from.uuid.as_slice(), id_bytes.as_slice())?;

            // Update to index
            let mut to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
            to_index.insert(edge.to.uuid.as_slice(), id_bytes.as_slice())?;

            // Update type index
            let mut type_index = write_txn.open_multimap_table(EDGE_TYPE_INDEX)?;
            type_index.insert(edge.edge_type.as_str(), id_bytes.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Batch insert multiple edges in a single transaction
    pub async fn insert_edges_batch(&self, edges: &[Edge]) -> Result<()> {
        if edges.is_empty() {
            return Ok(());
        }

        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            let mut from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
            let mut to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
            let mut type_index = write_txn.open_multimap_table(EDGE_TYPE_INDEX)?;

            for edge in edges {
                let edge_bytes = serde_json::to_vec(edge)?;
                let id_bytes = edge.id.uuid;

                edges_table.insert(id_bytes.as_slice(), edge_bytes.as_slice())?;
                from_index.insert(edge.from.uuid.as_slice(), id_bytes.as_slice())?;
                to_index.insert(edge.to.uuid.as_slice(), id_bytes.as_slice())?;
                type_index.insert(edge.edge_type.as_str(), id_bytes.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get an edge by ID
    pub async fn get_edge(&self, id: &EdgeId) -> Result<Option<Edge>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let edges_table = read_txn.open_table(EDGES_TABLE)?;

        if let Some(data) = edges_table.get(id.uuid.as_slice())? {
            let edge: Edge = serde_json::from_slice(data.value())?;
            Ok(Some(edge))
        } else {
            Ok(None)
        }
    }

    /// Get edges from a node
    pub async fn get_edges_from(
        &self,
        node_id: &NodeId,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let from_index = read_txn.open_multimap_table(EDGE_FROM_INDEX)?;
        let edges_table = read_txn.open_table(EDGES_TABLE)?;

        let mut edges = Vec::new();

        for result in from_index.get(node_id.uuid.as_slice())? {
            let edge_id = result?.value().to_vec();
            if let Some(data) = edges_table.get(edge_id.as_slice())? {
                let edge: Edge = serde_json::from_slice(data.value())?;

                // Filter by edge type if specified
                if let Some(et) = edge_type {
                    if edge.edge_type != et {
                        continue;
                    }
                }

                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Get edges to a node
    pub async fn get_edges_to(
        &self,
        node_id: &NodeId,
        edge_type: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let to_index = read_txn.open_multimap_table(EDGE_TO_INDEX)?;
        let edges_table = read_txn.open_table(EDGES_TABLE)?;

        let mut edges = Vec::new();

        for result in to_index.get(node_id.uuid.as_slice())? {
            let edge_id = result?.value().to_vec();
            if let Some(data) = edges_table.get(edge_id.as_slice())? {
                let edge: Edge = serde_json::from_slice(data.value())?;

                // Filter by edge type if specified
                if let Some(et) = edge_type {
                    if edge.edge_type != et {
                        continue;
                    }
                }

                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Delete an edge
    pub async fn delete_edge(&self, id: &EdgeId) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            // Get edge to find its from/to nodes
            let edges_table = write_txn.open_table(EDGES_TABLE)?;
            if let Some(data) = edges_table.get(id.uuid.as_slice())? {
                let edge: Edge = serde_json::from_slice(data.value())?;

                // Remove from indexes
                let mut from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
                from_index.remove(edge.from.uuid.as_slice(), id.uuid.as_slice())?;

                let mut to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
                to_index.remove(edge.to.uuid.as_slice(), id.uuid.as_slice())?;

                let mut type_index = write_txn.open_multimap_table(EDGE_TYPE_INDEX)?;
                type_index.remove(edge.edge_type.as_str(), id.uuid.as_slice())?;
            }
            drop(edges_table);

            // Remove edge
            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            edges_table.remove(id.uuid.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get all edges of a specific type
    pub async fn get_edges_by_type(
        &self,
        edge_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Edge>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let type_index = read_txn.open_multimap_table(EDGE_TYPE_INDEX)?;
        let edges_table = read_txn.open_table(EDGES_TABLE)?;

        let mut edges = Vec::new();
        let max_count = limit.unwrap_or(usize::MAX);

        for result in type_index.get(edge_type)? {
            if edges.len() >= max_count {
                break;
            }

            let id_bytes = result?.value().to_vec();
            if let Some(data) = edges_table.get(id_bytes.as_slice())? {
                let edge: Edge = serde_json::from_slice(data.value())?;
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    // ========== Tiered Storage Operations ==========

    /// Insert a node using the tiered index/payload split
    pub async fn insert_node_tiered(
        &self,
        id: &NodeId,
        index: &NodeIndex,
        payload: &[u8],
    ) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            let id_bytes = id.uuid;
            let index_bytes = serde_json::to_vec(index)?;

            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            index_table.insert(id_bytes.as_slice(), index_bytes.as_slice())?;

            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            payload_table.insert(id_bytes.as_slice(), payload)?;

            let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
            type_index.insert(index.node_type.as_str(), id_bytes.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Batch insert multiple nodes in a single transaction (much faster for bulk loads)
    pub async fn insert_nodes_tiered_batch(
        &self,
        nodes: &[(NodeId, NodeIndex, Vec<u8>)],
    ) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;

            for (id, index, payload) in nodes {
                let id_bytes = id.uuid;
                let index_bytes = serde_json::to_vec(index)?;

                index_table.insert(id_bytes.as_slice(), index_bytes.as_slice())?;
                payload_table.insert(id_bytes.as_slice(), payload.as_slice())?;
                type_index.insert(index.node_type.as_str(), id_bytes.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get the index record for a node
    pub async fn get_node_index(&self, id: &NodeId) -> Result<Option<NodeIndex>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        // Try the tiered index table first
        if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            if let Some(data) = index_table.get(id.uuid.as_slice())? {
                let index: NodeIndex = serde_json::from_slice(data.value())?;
                return Ok(Some(index));
            }
        }

        // Fall back to legacy NODES_TABLE (migration path)
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        if let Some(data) = nodes_table.get(id.uuid.as_slice())? {
            let node: Node = serde_json::from_slice(data.value())?;
            let payload_bytes = serde_json::to_vec(&node.properties)?;
            let index = NodeIndex::from_node(&node, PayloadLocation::Local, payload_bytes.len());
            return Ok(Some(index));
        }

        Ok(None)
    }

    /// Get the raw payload bytes for a node
    pub async fn get_payload(&self, id: &NodeId) -> Result<Option<Vec<u8>>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        // Try tiered payloads table
        if let Ok(payload_table) = read_txn.open_table(NODE_PAYLOADS_TABLE) {
            if let Some(data) = payload_table.get(id.uuid.as_slice())? {
                return Ok(Some(data.value().to_vec()));
            }
        }

        // Fall back to legacy NODES_TABLE
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        if let Some(data) = nodes_table.get(id.uuid.as_slice())? {
            let node: Node = serde_json::from_slice(data.value())?;
            let payload = serde_json::to_vec(&node.properties)?;
            return Ok(Some(payload));
        }

        Ok(None)
    }

    /// Write a payload to the local payloads table
    pub async fn put_payload(&self, id: &NodeId, payload: &[u8]) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;
        {
            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            payload_table.insert(id.uuid.as_slice(), payload)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Delete a payload from the local payloads table
    pub async fn delete_payload(&self, id: &NodeId) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;
        {
            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            payload_table.remove(id.uuid.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Update a node's index record
    pub async fn update_node_index(&self, id: &NodeId, index: &NodeIndex) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;
        {
            let index_bytes = serde_json::to_vec(index)?;
            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            index_table.insert(id.uuid.as_slice(), index_bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Update a node using the tiered split
    pub async fn update_node_tiered(
        &self,
        id: &NodeId,
        index: &NodeIndex,
        payload: &[u8],
    ) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            let id_bytes = id.uuid;
            let index_bytes = serde_json::to_vec(index)?;

            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            index_table.insert(id_bytes.as_slice(), index_bytes.as_slice())?;

            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            payload_table.insert(id_bytes.as_slice(), payload)?;

            // Keep legacy table in sync
            let node = index.to_node(id.clone(), serde_json::from_slice(payload)?);
            let node_bytes = serde_json::to_vec(&node)?;
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            nodes_table.insert(id_bytes.as_slice(), node_bytes.as_slice())?;
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Delete a node using tiered storage (index + payload + edges)
    pub async fn delete_node_tiered(&self, id: &NodeId) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        {
            // Get node type for index cleanup
            let node_type = {
                let index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
                let index_data = index_table
                    .get(id.uuid.as_slice())?
                    .map(|d| d.value().to_vec());
                drop(index_table);

                if let Some(data) = index_data {
                    let index: NodeIndex = serde_json::from_slice(&data)?;
                    Some(index.node_type.clone())
                } else {
                    let nodes_table = write_txn.open_table(NODES_TABLE)?;
                    let node_data = nodes_table
                        .get(id.uuid.as_slice())?
                        .map(|d| d.value().to_vec());
                    drop(nodes_table);

                    if let Some(data) = node_data {
                        let node: Node = serde_json::from_slice(&data)?;
                        Some(node.node_type.clone())
                    } else {
                        None
                    }
                }
            };

            // Remove from type index
            if let Some(nt) = node_type {
                let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
                type_index.remove(nt.as_str(), id.uuid.as_slice())?;
            }

            // Remove index record
            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            index_table.remove(id.uuid.as_slice())?;
            drop(index_table);

            // Remove payload
            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;
            payload_table.remove(id.uuid.as_slice())?;
            drop(payload_table);

            // Remove legacy node
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            nodes_table.remove(id.uuid.as_slice())?;
            drop(nodes_table);

            // Remove edges from this node
            let edge_from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
            let edge_ids: Vec<Vec<u8>> = edge_from_index
                .get(id.uuid.as_slice())?
                .map(|r| r.map(|v| v.value().to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(edge_from_index);

            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_from = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;

            for edge_id in &edge_ids {
                edges_table.remove(edge_id.as_slice())?;
                edge_from.remove(id.uuid.as_slice(), edge_id.as_slice())?;
            }
            drop(edges_table);
            drop(edge_from);

            // Remove edges to this node
            let edge_to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
            let to_edge_ids: Vec<Vec<u8>> = edge_to_index
                .get(id.uuid.as_slice())?
                .map(|r| r.map(|v| v.value().to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(edge_to_index);

            let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_to = write_txn.open_multimap_table(EDGE_TO_INDEX)?;

            for edge_id in &to_edge_ids {
                edges_table.remove(edge_id.as_slice())?;
                edge_to.remove(id.uuid.as_slice(), edge_id.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Get all node indexes of a specific type
    pub async fn get_node_indexes_by_type(
        &self,
        node_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(NodeId, NodeIndex)>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let type_index = read_txn.open_multimap_table(NODE_TYPE_INDEX)?;

        let mut results = Vec::new();
        let max_count = limit.unwrap_or(usize::MAX);

        // Try tiered index table first
        let has_index_table = read_txn.open_table(NODE_INDEX_TABLE).is_ok();

        for result in type_index.get(node_type)? {
            if results.len() >= max_count {
                break;
            }

            let id_bytes = result?.value().to_vec();
            let id = NodeId {
                uuid: id_bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid node ID bytes"))?,
            };

            if has_index_table {
                let index_table = read_txn.open_table(NODE_INDEX_TABLE)?;
                if let Some(data) = index_table.get(id_bytes.as_slice())? {
                    let index: NodeIndex = serde_json::from_slice(data.value())?;
                    results.push((id, index));
                    continue;
                }
            }

            // Fall back to legacy table
            let nodes_table = read_txn.open_table(NODES_TABLE)?;
            if let Some(data) = nodes_table.get(id_bytes.as_slice())? {
                let node: Node = serde_json::from_slice(data.value())?;
                let payload_bytes = serde_json::to_vec(&node.properties)?;
                let index =
                    NodeIndex::from_node(&node, PayloadLocation::Local, payload_bytes.len());
                results.push((id, index));
            }
        }

        Ok(results)
    }

    /// Get all node indexes (with optional limit)
    pub async fn get_all_node_indexes(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(NodeId, NodeIndex)>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let mut results = Vec::new();
        let max_count = limit.unwrap_or(usize::MAX);

        // Try tiered index table
        if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            for result in index_table.iter()? {
                if results.len() >= max_count {
                    break;
                }
                let (key, data) = result?;
                let id_bytes: [u8; 16] = key
                    .value()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid node ID bytes"))?;
                let id = NodeId { uuid: id_bytes };
                let index: NodeIndex = serde_json::from_slice(data.value())?;
                results.push((id, index));
            }

            if !results.is_empty() {
                return Ok(results);
            }
        }

        // Fall back to legacy table
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        for result in nodes_table.iter()? {
            if results.len() >= max_count {
                break;
            }
            let (key, data) = result?;
            let id_bytes: [u8; 16] = key
                .value()
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid node ID bytes"))?;
            let id = NodeId { uuid: id_bytes };
            let node: Node = serde_json::from_slice(data.value())?;
            let payload_bytes = serde_json::to_vec(&node.properties)?;
            let index = NodeIndex::from_node(&node, PayloadLocation::Local, payload_bytes.len());
            results.push((id, index));
        }

        Ok(results)
    }

    /// Get payload statistics (count, total_bytes) for eviction decisions
    pub fn payload_stats(&self) -> (u64, u64) {
        let db = self.db.read();
        let read_txn = match db.begin_read() {
            Ok(t) => t,
            Err(_) => return (0, 0),
        };

        if let Ok(table) = read_txn.open_table(NODE_PAYLOADS_TABLE) {
            let count = table.len().unwrap_or(0);
            // Estimate bytes from file size (actual per-row size tracking would need iteration)
            let db_path = self.path.join(".aresadb/data.redb");
            let size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
            (count, size)
        } else {
            (0, 0)
        }
    }

    /// Get eviction candidates: nodes with local payloads, sorted by update time (oldest first).
    /// Returns (NodeId, payload_size) pairs.
    pub async fn get_eviction_candidates(&self, min_size: u32) -> Result<Vec<(NodeId, u32)>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let index_table = read_txn.open_table(NODE_INDEX_TABLE)?;

        let mut candidates: Vec<(NodeId, NodeIndex)> = Vec::new();

        for result in index_table.iter()? {
            let (key, data) = result?;
            let id_bytes: [u8; 16] = key
                .value()
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid node ID bytes"))?;
            let index: NodeIndex = serde_json::from_slice(data.value())?;

            if index.payload_location == PayloadLocation::Local && index.payload_size >= min_size {
                candidates.push((NodeId { uuid: id_bytes }, index));
            }
        }

        // Sort by updated_at ascending (oldest updates first = coldest data)
        candidates.sort_by_key(|(_, idx)| idx.updated_at.millis);

        Ok(candidates
            .into_iter()
            .map(|(id, idx)| (id, idx.payload_size))
            .collect())
    }

    // ========== Secondary Property Indexes ==========

    /// Create a secondary index on a property field.
    /// Scans all existing nodes of the type and builds the index.
    pub async fn create_property_index(&self, node_type: &str, field: &str) -> Result<u64> {
        let registry_key = format!("{}\0{}", node_type, field);

        // Check if index already exists
        {
            let db = self.db.read();
            let read_txn = db.begin_read()?;
            if let Ok(registry) = read_txn.open_table(INDEX_REGISTRY) {
                if registry.get(registry_key.as_str()).ok().flatten().is_some() {
                    return Ok(0); // Already indexed
                }
            }
        }

        // Scan existing nodes and build the index
        let db = self.db.write();
        let read_txn = db.begin_read()?;

        let mut entries: Vec<(Vec<u8>, [u8; 16])> = Vec::new();

        if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            let payload_table = read_txn.open_table(NODE_PAYLOADS_TABLE)?;
            let type_index = read_txn.open_multimap_table(NODE_TYPE_INDEX)?;

            for result in type_index.get(node_type)? {
                let id_bytes: [u8; 16] = result?
                    .value()
                    .to_vec()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid ID bytes"))?;

                // Check this is a valid indexed node
                if index_table.get(id_bytes.as_slice())?.is_none() {
                    continue;
                }

                // Get the payload to extract the field value
                if let Some(payload_data) = payload_table.get(id_bytes.as_slice())? {
                    let properties: std::collections::BTreeMap<String, Value> =
                        serde_json::from_slice(payload_data.value())?;

                    if let Some(value) = properties.get(field) {
                        let index_key = make_property_index_key(node_type, field, value);
                        entries.push((index_key, id_bytes));
                    }
                }
            }
        }
        drop(read_txn);

        let indexed_count = entries.len() as u64;

        // Write the index entries
        let write_txn = db.begin_write()?;
        {
            let mut prop_index = write_txn.open_multimap_table(PROPERTY_INDEX)?;
            for (key, id_bytes) in &entries {
                prop_index.insert(key.as_slice(), id_bytes.as_slice())?;
            }

            let mut registry = write_txn.open_table(INDEX_REGISTRY)?;
            registry.insert(registry_key.as_str(), &[] as &[u8])?;
        }
        write_txn.commit()?;

        Ok(indexed_count)
    }

    /// Drop a secondary index
    pub async fn drop_property_index(&self, node_type: &str, field: &str) -> Result<()> {
        let registry_key = format!("{}\0{}", node_type, field);

        let db = self.db.write();

        // First, collect all index keys to remove
        let keys_to_remove: Vec<Vec<u8>> = {
            let read_txn = db.begin_read()?;
            let mut keys = Vec::new();

            if let Ok(prop_index) = read_txn.open_multimap_table(PROPERTY_INDEX) {
                let prefix = format!("{}\0{}\0", node_type, field).into_bytes();
                for result in prop_index.iter()? {
                    let (key, _) = result?;
                    let key_bytes = key.value().to_vec();
                    if key_bytes.starts_with(&prefix) {
                        keys.push(key_bytes);
                    }
                }
            }

            keys
        };

        // Now remove them
        let write_txn = db.begin_write()?;
        {
            let mut registry = write_txn.open_table(INDEX_REGISTRY)?;
            registry.remove(registry_key.as_str())?;

            let mut prop_index = write_txn.open_multimap_table(PROPERTY_INDEX)?;
            for key in &keys_to_remove {
                prop_index.remove_all(key.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Look up nodes by a property value using the secondary index.
    /// Returns None if no index exists for this field.
    pub async fn index_lookup(
        &self,
        node_type: &str,
        field: &str,
        value: &Value,
    ) -> Result<Option<Vec<NodeId>>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        // Check if index exists
        let registry_key = format!("{}\0{}", node_type, field);
        if let Ok(registry) = read_txn.open_table(INDEX_REGISTRY) {
            if registry.get(registry_key.as_str()).ok().flatten().is_none() {
                return Ok(None); // No index for this field
            }
        } else {
            return Ok(None);
        }

        let index_key = make_property_index_key(node_type, field, value);

        let prop_index = read_txn.open_multimap_table(PROPERTY_INDEX)?;
        let mut ids = Vec::new();

        for result in prop_index.get(index_key.as_slice())? {
            let id_bytes = result?.value().to_vec();
            let uuid: [u8; 16] = id_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid node ID in index"))?;
            ids.push(NodeId { uuid });
        }

        Ok(Some(ids))
    }

    /// Get the list of indexed fields for a given node type (cached read)
    pub fn get_indexed_fields_for_type(&self, node_type: &str) -> Result<Vec<String>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let prefix = format!("{}\0", node_type);
        let mut fields = Vec::new();

        if let Ok(registry) = read_txn.open_table(INDEX_REGISTRY) {
            for result in registry.iter()? {
                let (key, _) = result?;
                let key_str = key.value();
                if key_str.starts_with(&prefix) {
                    fields.push(key_str[prefix.len()..].to_string());
                }
            }
        }

        Ok(fields)
    }

    /// Update secondary property indexes for a node.
    /// Must be called after the node is inserted/updated in the tiered tables.
    pub async fn update_property_indexes(
        &self,
        id: &NodeId,
        node_type: &str,
        properties: &std::collections::BTreeMap<String, Value>,
    ) -> Result<()> {
        let indexed_fields = self.get_indexed_fields_for_type(node_type)?;
        if indexed_fields.is_empty() {
            return Ok(());
        }

        let db = self.db.write();
        let write_txn = db.begin_write()?;
        {
            let mut prop_index = write_txn.open_multimap_table(PROPERTY_INDEX)?;

            for field in &indexed_fields {
                if let Some(value) = properties.get(field) {
                    let index_key = make_property_index_key(node_type, field, value);
                    prop_index.insert(index_key.as_slice(), id.uuid.as_slice())?;
                }
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Get list of all registered indexes
    pub fn list_indexes(&self) -> Result<Vec<(String, String)>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let mut indexes = Vec::new();

        if let Ok(registry) = read_txn.open_table(INDEX_REGISTRY) {
            for result in registry.iter()? {
                let (key, _) = result?;
                let parts: Vec<&str> = key.value().splitn(2, '\0').collect();
                if parts.len() == 2 {
                    indexes.push((parts[0].to_string(), parts[1].to_string()));
                }
            }
        }

        Ok(indexes)
    }

    // ========== Full-Text Search Index ==========

    /// Create a full-text index on a string property field.
    /// Tokenizes and indexes all existing nodes of the given type.
    pub async fn create_fulltext_index(&self, node_type: &str, field: &str) -> Result<u64> {
        let registry_key = format!("{}\0{}", node_type, field);

        // Check if already exists
        {
            let db = self.db.read();
            let read_txn = db.begin_read()?;
            if let Ok(reg) = read_txn.open_table(FULLTEXT_REGISTRY) {
                if reg.get(registry_key.as_str()).ok().flatten().is_some() {
                    return Ok(0);
                }
            }
        }

        // Scan existing nodes and build inverted index
        let db = self.db.write();
        let read_txn = db.begin_read()?;

        #[allow(clippy::type_complexity)]
        let mut entries: Vec<(Vec<u8>, [u8; 16], HashMap<String, u32>)> = Vec::new();

        if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            let payload_table = read_txn.open_table(NODE_PAYLOADS_TABLE)?;
            let type_index = read_txn.open_multimap_table(NODE_TYPE_INDEX)?;

            for result in type_index.get(node_type)? {
                let id_bytes: [u8; 16] = result?
                    .value()
                    .to_vec()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid ID bytes"))?;

                if index_table.get(id_bytes.as_slice())?.is_none() {
                    continue;
                }

                if let Some(payload_data) = payload_table.get(id_bytes.as_slice())? {
                    let properties: std::collections::BTreeMap<String, Value> =
                        serde_json::from_slice(payload_data.value())?;

                    if let Some(Value::String(text)) = properties.get(field) {
                        let term_freqs = tokenize_and_count(text);
                        for token in term_freqs.keys() {
                            let key = make_fulltext_key(node_type, field, token);
                            entries.push((key, id_bytes, term_freqs.clone()));
                        }
                    }
                }
            }
        }
        drop(read_txn);

        // Deduplicate: collect unique (id, term_freqs) and all (key, id) pairs
        let mut doc_freqs: HashMap<[u8; 16], HashMap<String, u32>> = HashMap::new();
        let mut index_entries: Vec<(Vec<u8>, [u8; 16])> = Vec::new();

        for (key, id_bytes, term_freq) in entries {
            doc_freqs
                .entry(id_bytes)
                .or_insert_with(|| term_freq.clone());
            index_entries.push((key, id_bytes));
        }

        let indexed_count = doc_freqs.len() as u64;

        let write_txn = db.begin_write()?;
        {
            let mut ft_index = write_txn.open_multimap_table(FULLTEXT_INDEX)?;
            let mut ft_registry = write_txn.open_table(FULLTEXT_REGISTRY)?;
            let mut ft_doc_freq = write_txn.open_table(FULLTEXT_DOC_FREQ)?;

            for (key, id_bytes) in &index_entries {
                ft_index.insert(key.as_slice(), id_bytes.as_slice())?;
            }

            for (id_bytes, term_freq) in &doc_freqs {
                let doc_key = make_doc_freq_key(id_bytes, node_type, field);
                let freq_json = serde_json::to_vec(term_freq)?;
                ft_doc_freq.insert(doc_key.as_slice(), freq_json.as_slice())?;
            }

            ft_registry.insert(registry_key.as_str(), &[] as &[u8])?;
        }
        write_txn.commit()?;

        Ok(indexed_count)
    }

    /// Search the full-text index. Returns (NodeId, BM25 score) pairs.
    pub async fn fulltext_search(
        &self,
        node_type: &str,
        field: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(NodeId, f64)>> {
        let registry_key = format!("{}\0{}", node_type, field);

        let db = self.db.read();
        let read_txn = db.begin_read()?;

        // Check index exists
        if let Ok(reg) = read_txn.open_table(FULLTEXT_REGISTRY) {
            if reg.get(registry_key.as_str()).ok().flatten().is_none() {
                anyhow::bail!("No full-text index on {}.{}", node_type, field);
            }
        } else {
            anyhow::bail!("No full-text index on {}.{}", node_type, field);
        }

        let ft_index = read_txn.open_multimap_table(FULLTEXT_INDEX)?;
        let ft_doc_freq = read_txn.open_table(FULLTEXT_DOC_FREQ)?;

        // Get total document count for BM25
        let total_docs = {
            let type_index = read_txn.open_multimap_table(NODE_TYPE_INDEX)?;
            type_index.get(node_type)?.count() as f64
        };

        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Collect candidate documents and their term frequencies
        let mut scores: HashMap<[u8; 16], f64> = HashMap::new();

        for token in &query_tokens {
            let key = make_fulltext_key(node_type, field, token);

            // Count documents containing this token (df)
            let df = ft_index.get(key.as_slice())?.count() as f64;
            if df == 0.0 {
                continue;
            }

            // IDF component (BM25)
            let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();

            // Iterate matching documents
            let key2 = make_fulltext_key(node_type, field, token);
            for result in ft_index.get(key2.as_slice())? {
                let id_bytes: [u8; 16] = result?
                    .value()
                    .to_vec()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid ID"))?;

                // Get term frequency for this doc
                let doc_key = make_doc_freq_key(&id_bytes, node_type, field);
                let tf = if let Some(freq_data) = ft_doc_freq.get(doc_key.as_slice())? {
                    let freq_map: HashMap<String, u32> = serde_json::from_slice(freq_data.value())?;
                    *freq_map.get(token).unwrap_or(&0) as f64
                } else {
                    0.0
                };

                // BM25 scoring: k1=1.2, b=0.75, avgdl=100 (approximation)
                let k1 = 1.2;
                let b = 0.75;
                let avg_dl = 100.0;
                let dl = 100.0; // Approximate doc length
                let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avg_dl));

                *scores.entry(id_bytes).or_insert(0.0) += idf * tf_component;
            }
        }

        // Sort by score descending
        let mut results: Vec<([u8; 16], f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results
            .into_iter()
            .map(|(id_bytes, score)| (NodeId { uuid: id_bytes }, score))
            .collect())
    }

    /// Index a single document's text field for full-text search.
    /// Called on node insert when a full-text index exists.
    pub async fn update_fulltext_index(
        &self,
        id: &NodeId,
        node_type: &str,
        properties: &std::collections::BTreeMap<String, Value>,
    ) -> Result<()> {
        // Get all full-text indexed fields for this type
        let indexed_fields = self.get_fulltext_fields_for_type(node_type)?;
        if indexed_fields.is_empty() {
            return Ok(());
        }

        let db = self.db.write();
        let write_txn = db.begin_write()?;
        {
            let mut ft_index = write_txn.open_multimap_table(FULLTEXT_INDEX)?;
            let mut ft_doc_freq = write_txn.open_table(FULLTEXT_DOC_FREQ)?;

            for field in &indexed_fields {
                if let Some(Value::String(text)) = properties.get(field) {
                    let term_freqs = tokenize_and_count(text);

                    for token in term_freqs.keys() {
                        let key = make_fulltext_key(node_type, field, token);
                        ft_index.insert(key.as_slice(), id.uuid.as_slice())?;
                    }

                    let doc_key = make_doc_freq_key(&id.uuid, node_type, field);
                    let freq_json = serde_json::to_vec(&term_freqs)?;
                    ft_doc_freq.insert(doc_key.as_slice(), freq_json.as_slice())?;
                }
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Get full-text indexed fields for a node type
    fn get_fulltext_fields_for_type(&self, node_type: &str) -> Result<Vec<String>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let prefix = format!("{}\0", node_type);
        let mut fields = Vec::new();

        if let Ok(reg) = read_txn.open_table(FULLTEXT_REGISTRY) {
            for result in reg.iter()? {
                let (key, _) = result?;
                if key.value().starts_with(&prefix) {
                    fields.push(key.value()[prefix.len()..].to_string());
                }
            }
        }

        Ok(fields)
    }

    /// List all full-text indexes
    pub fn list_fulltext_indexes(&self) -> Result<Vec<(String, String)>> {
        let db = self.db.read();
        let read_txn = db.begin_read()?;

        let mut indexes = Vec::new();
        if let Ok(reg) = read_txn.open_table(FULLTEXT_REGISTRY) {
            for result in reg.iter()? {
                let (key, _) = result?;
                let parts: Vec<&str> = key.value().splitn(2, '\0').collect();
                if parts.len() == 2 {
                    indexes.push((parts[0].to_string(), parts[1].to_string()));
                }
            }
        }

        Ok(indexes)
    }

    /// Migrate a legacy database to tiered format.
    /// Reads all nodes from NODES_TABLE and creates corresponding
    /// entries in NODE_INDEX_TABLE and NODE_PAYLOADS_TABLE.
    pub async fn migrate_to_tiered(&self) -> Result<u64> {
        let db = self.db.write();
        let read_txn = db.begin_read()?;

        // Check if already migrated (index table has entries)
        if let Ok(index_table) = read_txn.open_table(NODE_INDEX_TABLE) {
            if index_table.len()? > 0 {
                return Ok(0);
            }
        }

        // Read all legacy nodes
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        let mut entries: Vec<(Vec<u8>, Node)> = Vec::new();

        for result in nodes_table.iter()? {
            let (key, data) = result?;
            let id_bytes = key.value().to_vec();
            let node: Node = serde_json::from_slice(data.value())?;
            entries.push((id_bytes, node));
        }
        drop(nodes_table);
        drop(read_txn);

        // Write tiered entries
        let write_txn = db.begin_write()?;
        let count = entries.len() as u64;

        {
            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            let mut payload_table = write_txn.open_table(NODE_PAYLOADS_TABLE)?;

            for (id_bytes, node) in &entries {
                let payload = serde_json::to_vec(&node.properties)?;
                let index = NodeIndex::from_node(node, PayloadLocation::Local, payload.len());
                let index_bytes = serde_json::to_vec(&index)?;

                index_table.insert(id_bytes.as_slice(), index_bytes.as_slice())?;
                payload_table.insert(id_bytes.as_slice(), payload.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(count)
    }

    // ========== Transaction Support ==========

    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<Transaction> {
        Transaction::new(self.db.clone())
    }

    /// Get database path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A database transaction for atomic operations
pub struct Transaction {
    db: Arc<RwLock<RedbDatabase>>,
    operations: Vec<TransactionOp>,
}

#[derive(Debug)]
enum TransactionOp {
    InsertNode(Node),
    UpdateNode(NodeId, Value),
    DeleteNode(NodeId),
    InsertEdge(Edge),
    DeleteEdge(EdgeId),
}

impl Transaction {
    fn new(db: Arc<RwLock<RedbDatabase>>) -> Result<Self> {
        Ok(Self {
            db,
            operations: Vec::new(),
        })
    }

    /// Insert a node in this transaction
    pub fn insert_node(&mut self, node: Node) {
        self.operations.push(TransactionOp::InsertNode(node));
    }

    /// Update a node in this transaction
    pub fn update_node(&mut self, id: NodeId, properties: Value) {
        self.operations
            .push(TransactionOp::UpdateNode(id, properties));
    }

    /// Delete a node in this transaction
    pub fn delete_node(&mut self, id: NodeId) {
        self.operations.push(TransactionOp::DeleteNode(id));
    }

    /// Insert an edge in this transaction
    pub fn insert_edge(&mut self, edge: Edge) {
        self.operations.push(TransactionOp::InsertEdge(edge));
    }

    /// Delete an edge in this transaction
    pub fn delete_edge(&mut self, id: EdgeId) {
        self.operations.push(TransactionOp::DeleteEdge(id));
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<()> {
        let db = self.db.write();
        let write_txn = db.begin_write()?;

        for op in self.operations {
            match op {
                TransactionOp::InsertNode(node) => {
                    let node_bytes = serde_json::to_vec(&node)?;
                    let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
                    nodes_table.insert(node.id.uuid.as_slice(), node_bytes.as_slice())?;

                    let mut type_index = write_txn.open_multimap_table(NODE_TYPE_INDEX)?;
                    type_index.insert(node.node_type.as_str(), node.id.uuid.as_slice())?;
                }
                TransactionOp::UpdateNode(id, properties) => {
                    let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
                    let node_data = {
                        nodes_table
                            .get(id.uuid.as_slice())?
                            .map(|d| d.value().to_vec())
                    };
                    if let Some(data) = node_data {
                        let mut node: Node = serde_json::from_slice(&data)?;
                        if let Value::Object(new_props) = properties {
                            for (k, v) in new_props {
                                node.properties.insert(k, v);
                            }
                        }
                        node.updated_at = Timestamp::now();
                        let node_bytes = serde_json::to_vec(&node)?;
                        nodes_table.insert(id.uuid.as_slice(), node_bytes.as_slice())?;
                    }
                }
                TransactionOp::DeleteNode(id) => {
                    let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
                    nodes_table.remove(id.uuid.as_slice())?;
                }
                TransactionOp::InsertEdge(edge) => {
                    let edge_bytes = serde_json::to_vec(&edge)?;
                    let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
                    edges_table.insert(edge.id.uuid.as_slice(), edge_bytes.as_slice())?;

                    let mut from_index = write_txn.open_multimap_table(EDGE_FROM_INDEX)?;
                    from_index.insert(edge.from.uuid.as_slice(), edge.id.uuid.as_slice())?;

                    let mut to_index = write_txn.open_multimap_table(EDGE_TO_INDEX)?;
                    to_index.insert(edge.to.uuid.as_slice(), edge.id.uuid.as_slice())?;
                }
                TransactionOp::DeleteEdge(id) => {
                    let mut edges_table = write_txn.open_table(EDGES_TABLE)?;
                    edges_table.remove(id.uuid.as_slice())?;
                }
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Rollback the transaction (simply drop it)
    pub fn rollback(self) {
        // Operations are discarded when transaction is dropped
    }
}

/// Build a composite key for the property index: "type\0field\0value_canonical"
fn make_property_index_key(node_type: &str, field: &str, value: &Value) -> Vec<u8> {
    let value_str = match value {
        Value::String(s) => format!("s:{}", s),
        Value::Int(i) => format!("i:{}", i),
        Value::Float(f) => format!("f:{:.10}", f),
        Value::Bool(b) => format!("b:{}", b),
        Value::Null => "n:".to_string(),
        _ => format!("j:{}", serde_json::to_string(value).unwrap_or_default()),
    };

    format!("{}\0{}\0{}", node_type, field, value_str).into_bytes()
}

fn make_fulltext_key(node_type: &str, field: &str, token: &str) -> Vec<u8> {
    format!("{}\0{}\0{}", node_type, field, token).into_bytes()
}

fn make_doc_freq_key(id_bytes: &[u8; 16], node_type: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + node_type.len() + field.len() + 2);
    key.extend_from_slice(id_bytes);
    key.push(0);
    key.extend_from_slice(node_type.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key
}

use std::collections::HashMap;

const STOP_WORDS: &[&str] = &[
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "is",
    "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "shall", "can", "it", "its", "this", "that",
    "these", "those", "i", "me", "my", "we", "our", "you", "your", "he", "she", "they", "them",
    "their", "not", "no", "so", "if", "as",
];

/// Tokenize text into lowercase, non-stopword tokens
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 1)
        .map(|s| s.to_lowercase())
        .filter(|s| !STOP_WORDS.contains(&s.as_str()))
        .collect()
}

/// Tokenize and count term frequencies
fn tokenize_and_count(text: &str) -> HashMap<String, u32> {
    let mut counts = HashMap::new();
    for token in tokenize(text) {
        *counts.entry(token).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_open() {
        let temp = TempDir::new().unwrap();

        // Create database
        let storage = LocalStorage::create(temp.path()).await.unwrap();
        drop(storage);

        // Reopen database
        let storage = LocalStorage::open(temp.path()).await.unwrap();
        let stats = storage.stats().await.unwrap();
        assert_eq!(stats.node_count, 0);
    }

    #[tokio::test]
    async fn test_node_crud() {
        let temp = TempDir::new().unwrap();
        let storage = LocalStorage::create(temp.path()).await.unwrap();

        // Insert
        let props = Value::from_json(serde_json::json!({"name": "Alice", "age": 25})).unwrap();
        let node = Node::new("user", props);
        let node_id = node.id.clone();
        storage.insert_node(&node).await.unwrap();

        // Read
        let retrieved = storage.get_node(&node_id).await.unwrap().unwrap();
        assert_eq!(retrieved.node_type, "user");
        assert_eq!(retrieved.get("name").unwrap().as_str(), Some("Alice"));

        // Update
        let new_props = Value::from_json(serde_json::json!({"age": 26})).unwrap();
        let updated = storage.update_node(&node_id, new_props).await.unwrap();
        assert_eq!(updated.get("age").unwrap().as_int(), Some(26));

        // Delete
        storage.delete_node(&node_id).await.unwrap();
        let deleted = storage.get_node(&node_id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_edge_crud() {
        let temp = TempDir::new().unwrap();
        let storage = LocalStorage::create(temp.path()).await.unwrap();

        // Create nodes
        let node1 = Node::new(
            "user",
            Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
        );
        let node2 = Node::new(
            "user",
            Value::from_json(serde_json::json!({"name": "Bob"})).unwrap(),
        );
        storage.insert_node(&node1).await.unwrap();
        storage.insert_node(&node2).await.unwrap();

        // Create edge
        let edge = Edge::new(node1.id.clone(), node2.id.clone(), "follows", Value::Null);
        let edge_id = edge.id.clone();
        storage.insert_edge(&edge).await.unwrap();

        // Get edges from node1
        let edges = storage.get_edges_from(&node1.id, None).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].edge_type, "follows");

        // Get edges to node2
        let edges = storage.get_edges_to(&node2.id, None).await.unwrap();
        assert_eq!(edges.len(), 1);

        // Delete edge
        storage.delete_edge(&edge_id).await.unwrap();
        let edges = storage.get_edges_from(&node1.id, None).await.unwrap();
        assert_eq!(edges.len(), 0);
    }

    #[tokio::test]
    async fn test_transaction() {
        let temp = TempDir::new().unwrap();
        let storage = LocalStorage::create(temp.path()).await.unwrap();

        // Create multiple nodes in a transaction
        let mut txn = storage.begin_transaction().unwrap();

        let node1 = Node::new(
            "user",
            Value::from_json(serde_json::json!({"name": "Alice"})).unwrap(),
        );
        let node2 = Node::new(
            "user",
            Value::from_json(serde_json::json!({"name": "Bob"})).unwrap(),
        );

        txn.insert_node(node1.clone());
        txn.insert_node(node2.clone());

        txn.commit().unwrap();

        // Verify nodes were created
        let nodes = storage.get_nodes_by_type("user", None).await.unwrap();
        assert_eq!(nodes.len(), 2);
    }
}
