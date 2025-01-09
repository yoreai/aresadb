# AresaDB Architecture

> Technical reference for the internal architecture of AresaDB.
> This document is maintained alongside the codebase and serves as a companion
> to the [publication](paper/README.md).

---

## System Overview

AresaDB is an embedded multi-model database that unifies five data paradigms —
key-value, graph, relational (SQL), vector search, and full-text search — under
a single property graph foundation. Its distinguishing architectural feature is
**transparent cloud tiering**: the graph index remains local for sub-microsecond
traversals while node payloads can be transparently offloaded to cloud object
storage (S3/GCS) for infinite scalability.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Access Layer                              │
│  CLI . REPL . Rust Library . Python (PyO3) . TCP Wire Protocol  │
├─────────────────────────────────────────────────────────────────┤
│                       Query Engine                               │
│  ┌───────────┐   ┌──────────┐   ┌──────────────────────────┐   │
│  │  Parser   │──→│ Planner  │──→│       Executor           │   │
│  │(sqlparser)│   │(cost-    │   │ - Table scan / Index     │   │
│  │           │   │ based)   │   │ - Filter / Sort / Limit  │   │
│  └───────────┘   └──────────┘   │ - Aggregation            │   │
│                                  │ - VECTOR SEARCH          │   │
│                                  │ - FULLTEXT SEARCH        │   │
│                                  └──────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│   Graph Engine        │    Index Subsystem                       │
│   ┌──────────────┐    │    ┌────────────────────────────────┐   │
│   │ BFS/DFS      │    │    │ Type index (multimap)          │   │
│   │ Neighbor      │    │    │ Edge index (from/to multimap)  │   │
│   │ traversal     │    │    │ Secondary B-tree index         │   │
│   │ (index-only   │    │    │ Full-text inverted index       │   │
│   │  hops)        │    │    │ HNSW vector index              │   │
│   └──────────────┘    │    └────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                   Tiered Storage Engine                           │
│                                                                   │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────┐   │
│  │  Local       │    │  Cache        │    │  Cloud            │   │
│  │  (redb)      │←──→│  (moka LRU)  │←──→│  (S3/GCS via     │   │
│  │              │    │              │    │   object_store)   │   │
│  │  NodeIndex   │    │  Hot payload │    │  Cold payloads    │   │
│  │  + hot       │    │  eviction    │    │  Infinite scale   │   │
│  │    payloads  │    │              │    │                   │   │
│  └─────────────┘    └──────────────┘    └───────────────────┘   │
│                                                                   │
│  Invariant: NodeIndex and all graph structure always local.       │
│  Only serialized payloads (properties, embeddings) are tiered.   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Data Model

### 1.1 Property Graph Foundation

The property graph is the unifying abstraction. Every piece of data in AresaDB
is either a **Node** (entity with typed properties) or an **Edge** (directed
relationship between two nodes).

```rust
struct Node {
    id: NodeId,                           // UUID v4
    node_type: String,                    // e.g., "user", "product"
    properties: BTreeMap<String, Value>,  // Flexible schema
    created_at: Timestamp,
    updated_at: Timestamp,
}

struct Edge {
    id: EdgeId,
    from: NodeId,
    to: NodeId,
    edge_type: String,                    // e.g., "purchased", "follows"
    properties: BTreeMap<String, Value>,
    created_at: Timestamp,
    updated_at: Timestamp,
}
```

### 1.2 Multi-Model Mapping

The same graph data is accessible through five query paradigms:

| Paradigm | Mapping | Access Pattern |
|----------|---------|----------------|
| **Key-Value** | `NodeId → Node` | Direct get/put by ID |
| **Relational** | `node_type` as table, properties as columns | SQL queries |
| **Graph** | Nodes + Edges | BFS/DFS traversal |
| **Vector** | Properties with `$vector` annotation | ANN similarity search |
| **Full-Text** | String properties | Inverted index + BM25 |

---

## 2. Tiered Storage Architecture

This is AresaDB's primary architectural contribution. Traditional databases
store all data in one tier. AresaDB splits each node into a lightweight
**index record** (always local) and a heavier **payload** (tiered).

### 2.1 Node Index

```rust
struct NodeIndex {
    node_type: String,
    created_at: Timestamp,
    updated_at: Timestamp,
    payload_location: PayloadLocation,  // Local | Cloud(url)
    payload_size: u32,
    property_count: u16,
}
```

The `NodeIndex` is ~100–200 bytes and resides in the local redb B+ tree. Graph
traversal reads only these records, achieving sub-microsecond per-hop latency
(**measured: 0.04 µs mean**).

### 2.2 Storage Tiers

| Tier | Backing | Latency | Capacity | Contents |
|------|---------|---------|----------|----------|
| **Hot** | redb (local SSD) | 4–12 µs | Bounded by disk | Full payloads |
| **Warm** | moka LRU cache | < 1 µs | Configurable (default 10K entries) | Recently accessed |
| **Cold** | S3 / GCS | 50–200 ms | Unbounded | Evicted payloads |
| **Index** | redb (local SSD) | < 1 µs | Always local | NodeIndex + edges |

### 2.3 Read/Write Paths

**Read path**: `cache hit → local payload table → cloud fetch + cache populate`

**Write path**: `local index + local payload → optional async cloud replication`

**Eviction**: When local storage exceeds a configurable threshold, payloads with
the oldest `updated_at` are migrated to cloud. The NodeIndex's
`payload_location` field is updated to `Cloud(url)`. Subsequent reads
transparently fetch from cloud and populate the cache.

### 2.4 Local Storage (redb)

AresaDB uses [redb](https://github.com/cberner/redb), a pure-Rust embedded
B+ tree database providing ACID transactions and memory-mapped I/O.

**Table Layout**:

| Table | Type | Key | Value |
|-------|------|-----|-------|
| `NODE_INDEX_TABLE` | Table | NodeId bytes | Serialized NodeIndex |
| `NODE_PAYLOADS_TABLE` | Table | NodeId bytes | Serialized properties |
| `NODES_TABLE` | Table | NodeId bytes | Full JSON node (compat) |
| `EDGES` | Table | EdgeId bytes | Serialized Edge |
| `TYPE_INDEX` | Multimap | node_type (str) | NodeId bytes |
| `EDGE_FROM_INDEX` | Multimap | from NodeId | EdgeId bytes |
| `EDGE_TO_INDEX` | Multimap | to NodeId | EdgeId bytes |
| `PROPERTY_INDEX` | Multimap | `type\0field\0value` | NodeId bytes |
| `INDEX_REGISTRY` | Table | `type\0field` | empty |
| `FULLTEXT_INDEX` | Multimap | `type\0field\0token` | NodeId bytes |
| `FULLTEXT_REGISTRY` | Table | `type\0field` | empty |
| `FULLTEXT_DOC_FREQ` | Table | NodeId + `type\0field` | JSON {token: count} |
| `METADATA_TABLE` | Table | key (str) | config bytes |

---

## 3. Index Subsystem

### 3.1 Structural Indexes (Built-in)

- **Type Index**: O(1) lookup by `node_type` via `TYPE_INDEX` multimap
- **Edge Indexes**: O(1) neighbor discovery via `EDGE_FROM_INDEX` / `EDGE_TO_INDEX`
- **Bloom Filters**: Probabilistic pre-filter for distributed key lookups

### 3.2 Secondary Property Indexes

B-tree indexes on arbitrary node properties. Composite key encoding:

```
Key:   type\0field\0value   (null-byte separated)
Value: [NodeId, NodeId, ...]   (multimap)
```

- Created via `CREATE INDEX ON table (field)` or `db.create_index(type, field)`
- Back-fills existing data at creation time (**measured: 67ms for 12,500 entries**)
- Auto-maintained on node insertion
- Query planner routes equality predicates through index automatically
- **Measured speedup: 24.7x** (51ms → 2.1ms for equality query on 12.5K records)

### 3.3 Full-Text Inverted Index

BM25-ranked text search over string properties.

**Tokenization pipeline**: input → lowercase → whitespace split → stopword removal → min 2 chars

**Index structure**:
- Forward posting: `type\0field\0token` → `[NodeId]` (multimap)
- Document term frequency: `NodeId + type\0field` → `{token: count}` (per-doc TF)
- Corpus statistics: total document count, per-token document frequency

**BM25 scoring** (k₁ = 1.2, b = 0.75):

```
score(q, d) = Σ IDF(t) · (tf(t,d) · (k₁ + 1)) / (tf(t,d) + k₁ · (1 - b + b · |d|/avgdl))
```

Where `IDF(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)`.

- Created via `CREATE FULLTEXT INDEX ON table (field)`
- Queried via `FULLTEXT SEARCH table FIELD field FOR 'query' LIMIT n`
- **Measured: 29ms search over 12,500 documents**

### 3.4 HNSW Vector Index

Approximate Nearest Neighbor search using Hierarchical Navigable Small World
graphs. Managed per `(node_type, field)` pair.

| Parameter | Default | Description |
|-----------|---------|-------------|
| M | 16 | Max connections per node |
| ef_construction | 100 | Search width during build |
| max_layers | 4 | HNSW hierarchy depth |
| Metrics | Cosine, Euclidean, Dot, Manhattan | Distance functions |

**Lifecycle**:
1. `insert_with_embedding()` adds vector incrementally
2. `similarity_search()` lazy-builds on first call
3. `rebuild_vector_index()` explicit rebuild after bulk loads

**Filtered vector search** pre-filters by SQL WHERE, then runs ANN on the subset:
```sql
VECTOR SEARCH docs FIELD embedding FOR [0.1, 0.2, ...] WHERE topic = 'ai' LIMIT 10
```

**Measured performance** (10K × 128D vectors):
- HNSW build: 142ms
- HNSW top-10 search: **6.5 µs** (98.7x faster than brute-force 642 µs)

---

## 4. Query Engine

### 4.1 SQL Parser

Extends `sqlparser-rs` with custom syntax for vector search, full-text search,
and index management:

```
Standard SQL:     SELECT * FROM user WHERE age > 25 ORDER BY name LIMIT 10
Vector search:    VECTOR SEARCH docs FIELD emb FOR [0.1, ...] LIMIT 10
Full-text:        FULLTEXT SEARCH docs FIELD title FOR 'query' LIMIT 10
Index mgmt:       CREATE INDEX ON user (email)
                  CREATE FULLTEXT INDEX ON docs (content)
                  DROP INDEX ON user (email)
```

### 4.2 Query Planner

Cost-based planner that considers available indexes:

```rust
enum PlanStep {
    FullScan { node_type },           // Full scan: O(n)
    IndexLookup { node_type, field, value },  // B-tree: O(log n)
    Filter { conditions },            // Predicate evaluation
    Sort { field, descending },       // In-memory sort
    Limit { count, offset },          // Early termination
    Project { columns },              // Column selection
    Traverse { start_node, depth, edge_types }, // Graph BFS
    InsertNode { node_type, data },   // Node creation
    UpdateNodes { data },             // Batch update
    DeleteNodes,                      // Batch delete
}
```

The planner automatically selects `IndexLookup` over `FullScan` when a
secondary index exists for the queried field.

### 4.3 Executor

Executes plan steps sequentially, with each step operating on the result
set from the previous step. Returns `QueryResult` with columns, rows,
execution time, and rows affected.

---

## 5. Wire Protocol and Server

### 5.1 Protocol

Binary protocol using `bincode` serialization over TCP. Request/Response
types are Rust enums covering all database operations.

### 5.2 Server Architecture

- TCP listener with configurable connection pool (atomic counter-based)
- Per-connection `RequestHandler` with embedded `QueryEngine`
- Supports all operations: CRUD, SQL queries, graph traversal, vector search,
  full-text search, index management, transactions

### 5.3 Batch APIs

Single-transaction bulk operations amortize WAL flush overhead:

| Method | Throughput | Speedup |
|--------|-----------|---------|
| `insert_nodes_batch()` | 37,720 nodes/sec | 345x vs individual |
| `create_edges_batch()` | 28,219 edges/sec | — |

---

## 6. Module Map

```
src/
├── lib.rs                    Public API re-exports
├── main.rs                   CLI entry point
├── storage/
│   ├── mod.rs                Database struct, high-level API
│   ├── local.rs              redb backend, all table definitions
│   ├── tiered.rs             TieredStorage orchestrator
│   ├── cloud.rs              S3/GCS via object_store
│   ├── cache.rs              moka LRU cache layer
│   ├── vector_index.rs       HNSW implementation
│   └── parallel.rs           Parallel scan utilities
├── query/
│   ├── mod.rs                QueryOperation, ParsedQuery types
│   ├── parser.rs             SQL + custom syntax parser
│   ├── planner.rs            Cost-based query planner
│   └── executor.rs           QueryEngine execution
├── server/                   TCP server (feature = "server")
│   ├── mod.rs                Server bootstrap
│   ├── protocol.rs           Request/Response wire types
│   ├── handler.rs            RequestHandler dispatch
│   └── pool.rs               Connection pool
├── client/                   TCP client
├── distributed/              WAL, sharding, replication, compression
├── schema/                   Schema registry and migrations
├── rag/                      Document chunking and context retrieval
├── cli/                      Command definitions
├── output/                   Table, JSON, CSV formatters
└── repl/                     Interactive shell
```

---

## 7. Testing

330+ tests across four categories:

| Category | Count | What |
|----------|-------|------|
| Unit tests | ~280 | Per-module `#[cfg(test)]` blocks |
| Integration tests | ~30 | `tests/` directory |
| Stress tests | ~20 | Concurrent R/W, scale, crash recovery |
| Criterion benchmarks | ~20 | Storage, query, distributed micro-benchmarks |

---

## 8. Dependencies

| Crate | Purpose | Why |
|-------|---------|-----|
| `redb` | Embedded B+ tree storage | Pure Rust, ACID, zero-copy |
| `object_store` | S3/GCS abstraction | Apache Arrow ecosystem |
| `moka` | Concurrent LRU cache | Lock-free, high throughput |
| `rkyv` | Zero-copy serialization | Near-instant deserialization |
| `sqlparser` | SQL parsing | Robust, PostgreSQL-compatible |
| `tokio` | Async runtime | Industry standard |
| `serde` / `bincode` | Wire protocol serialization | Compact binary format |
| `parking_lot` | Synchronization | Faster than std mutexes |
| `uuid` | Node/Edge identifiers | Universally unique |
| `chrono` | Timestamps | RFC 3339 compatibility |
| `lz4_flex` | Compression | Fast encode/decode |
| `xxhash-rust` | Hashing | Fast consistent hashing |
| `crc32fast` | Checksums | WAL integrity |

---

*This document is maintained alongside the codebase. Last updated: April 2026.*
*See also: [BENCHMARKS.md](BENCHMARKS.md) for measured performance data.*
*See also: [paper/](paper/) for the publication draft.*
