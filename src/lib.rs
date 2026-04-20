//! # AresaDB — High-Performance Multi-Model Database Engine
//!
//! AresaDB is a blazing-fast embedded database that unifies five data models
//! under a single property graph: **Key-Value**, **Graph**, **SQL**,
//! **Vector Search** (HNSW), and **Full-Text Search** (BM25).
//!
//! ## Features
//!
//! - **Five models, one binary** — KV, Graph, SQL, Vector, and Full-Text Search
//! - **Pure SQL** — standard SQL interface; LLMs can generate queries from natural language
//! - **HNSW vector search** — approximate k-NN with cosine, euclidean, dot, manhattan
//! - **BM25 full-text search** — inverted index with relevance ranking
//! - **Secondary B-tree indexes** — O(log n) property lookups
//! - **Blazing fast** — lock-free reads, parallel traversal, zero-copy serialization
//! - **Transparent cloud tiering** — graph index local, payloads on S3/GCS
//! - **ACID compliant** — full transaction support via redb
//! - **Distributed building blocks** — sharding, replication, WAL (V2)
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                     CLI  /  Rust SDK  /  Python SDK              │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                         Query Engine                             │
//! │   ├── SQL Parser (sqlparser-rs)                                  │
//! │   ├── Query Planner & Optimizer                                  │
//! │   ├── Graph Traversal (BFS, shortest path, components)           │
//! │   └── Vector & Full-Text Query Execution                         │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                        Index Layer                                │
//! │   ├── Structural Indexes (node type, edge adjacency)             │
//! │   ├── Secondary B-tree Indexes (property lookups)                │
//! │   ├── HNSW Vector Index (approximate k-NN)                       │
//! │   └── Inverted Full-Text Index (BM25 scoring)                    │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                   Tiered Storage Engine                           │
//! │   ├── Node Index (always local, sub-µs)                          │
//! │   ├── Payload Store (local or cloud-tiered)                      │
//! │   ├── Edge Store (adjacency lists)                               │
//! │   └── LRU Cache (moka, read-through)                             │
//! ├──────────────────────────────────────────────────────────────────┤
//! │                    Storage Backends                               │
//! │   ├── Local: redb (embedded B+ tree, ACID)                       │
//! │   └── Cloud: S3 / GCS via object_store                           │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use aresadb::{Database, QueryEngine, DistanceMetric};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let db = Database::create("./mydata", "myapp").await?;
//!
//!     // Insert nodes
//!     let user = db.insert_node("user", serde_json::json!({
//!         "name": "Alice", "email": "alice@example.com"
//!     })).await?;
//!
//!     // SQL queries
//!     let engine = QueryEngine::new(db.clone());
//!     let result = engine.execute_sql("SELECT * FROM user WHERE name = 'Alice'", None).await?;
//!
//!     // Vector search
//!     let hits = db.similarity_search(&[1.0, 0.0, 0.0, 0.0],
//!         "document", "embedding", 10, DistanceMetric::Cosine).await?;
//!
//!     // Full-text search
//!     db.create_fulltext_index("article", "body").await?;
//!     let fts = db.fulltext_search("article", "body", "Rust database", 5).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## SQL Interface
//!
//! ```sql
//! INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30);
//! SELECT * FROM users WHERE age > 25 ORDER BY name LIMIT 10;
//! UPDATE users SET age = 31 WHERE name = 'Alice';
//! DELETE FROM users WHERE age < 18;
//!
//! -- Secondary indexes
//! CREATE INDEX ON users (age);
//!
//! -- Full-text search
//! CREATE FULLTEXT INDEX ON articles (body);
//! FULLTEXT SEARCH articles FIELD body FOR 'distributed databases' LIMIT 10;
//!
//! -- Vector search
//! VECTOR SEARCH documents FIELD embedding FOR [1.0, 0.0, 0.0, 0.0] METRIC cosine LIMIT 10;
//! ```

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

// Core modules
pub mod cli;
pub mod output;
pub mod query;
pub mod schema;
pub mod storage;

// V2: Distributed modules
pub mod distributed;

/// RAG (Retrieval-Augmented Generation) utilities.
pub mod rag;

// V2: Server/Client modules (behind feature flags)
#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "server")]
pub mod client;

// Re-exports for convenience
#[allow(unused_imports)]
pub use storage::{
    BucketStorage, CacheLayer, Database, DatabaseConfig, DatabaseStatus, DistanceMetric, Edge,
    EdgeId, GraphView, IndexStats, KvView, LocalStorage, Node, NodeId, NodeIndex, ParallelExecutor,
    ParallelTraversalResult, PayloadLocation, SimilarityResult, SnapshotReader, SyncStats,
    TieredConfig, TieredStats, TieredStorage, Timestamp, Value, VectorIndex, VectorNodeBuilder,
    VectorSearch,
};

#[allow(unused_imports)]
pub use query::{
    Condition, Operator, OrderBy, ParsedQuery, QueryEngine, QueryOperation, QueryParser,
    QueryResult, TraversalResult,
};

#[allow(unused_imports)]
pub use schema::{
    FieldType, Migration, MigrationAction, MigrationGenerator, Schema, SchemaField, SchemaManager,
};

#[allow(unused_imports)]
pub use distributed::{
    BloomFilter, CompressionStats, Compressor, CountingBloomFilter, Cursor, ReplicaConfig,
    ReplicaSet, ReplicaState, ResultStream, ShardConfig, ShardManager, StreamSender,
};

#[allow(unused_imports)]
pub use rag::{
    ChunkStrategy, Chunker, ContextChunk, ContextRetriever, DocumentChunk, EmbeddingManager,
    EmbeddingProvider, HybridSearch, HybridSearchConfig, HybridSearchResult, OpenAIModel,
    RetrievedContext,
};

#[cfg(feature = "server")]
pub use server::{Server, ServerConfig};

#[cfg(feature = "server")]
pub use client::{Client, ClientBuilder};

/// Database format version for compatibility checking
#[allow(dead_code)]
pub const FORMAT_VERSION: u32 = 1;

/// Maximum number of nodes to return in a single query by default
#[allow(dead_code)]
pub const DEFAULT_QUERY_LIMIT: usize = 1000;

/// Default cache size in bytes (100MB)
#[allow(dead_code)]
pub const DEFAULT_CACHE_SIZE: usize = 100 * 1024 * 1024;

/// Default number of shards
#[allow(dead_code)]
pub const DEFAULT_SHARD_COUNT: usize = 16;

/// Prelude module — common types for working with AresaDB
#[allow(unused_imports)]
pub mod prelude {

    pub use crate::storage::{Database, Edge, EdgeId, Node, NodeId, Timestamp, Value};

    pub use crate::query::{QueryEngine, QueryResult, TraversalResult};

    pub use crate::schema::{Schema, SchemaManager};

    pub use crate::distributed::{BloomFilter, Compressor, ShardManager};

    #[cfg(feature = "server")]
    pub use crate::{Client, Server};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert_eq!(FORMAT_VERSION, 1);
    }

    #[test]
    fn test_constants() {
        assert!(DEFAULT_QUERY_LIMIT > 0);
        assert!(DEFAULT_CACHE_SIZE > 0);
        assert!(DEFAULT_SHARD_COUNT > 0);
    }
}
