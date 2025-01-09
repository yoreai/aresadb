# AresaDB Benchmarks

Reproducible benchmark results for AresaDB's multi-model storage engine.

> **Hardware**: Apple M2 Pro (aarch64), 16GB RAM, 512GB SSD  
> **OS**: macOS 15  
> **Rust**: 1.85+ (release profile, `opt-level = 3`)  
> **Date**: April 11, 2026  
> **Dataset**: 50K nodes, 250K edges, 10K 128-dimensional vectors

---

## Reproducibility

All numbers below are produced by a single deterministic script:

```bash
cargo run --example benchmark_suite --release
```

The benchmark runner writes results to `/tmp/aresadb-benchmark-results.json`.
Archived copies are saved to `benchmarks/results-YYYY-MM-DD.json`.
The benchmark uses 10 samples per measurement after 3 warmup iterations.
Each run uses a fresh database on a temporary filesystem path.

---

## Summary

| Operation | Latency / Throughput |
|-----------|---------------------|
| Batch insert | **37,720 nodes/sec** |
| Edge insert | **28,219 edges/sec** |
| Point lookup (payload) | **4.9 µs** mean, 4.0 µs p50 |
| Index-only lookup | **< 1 µs** (0.04 µs mean) |
| Graph traversal (depth-2, 30 edges) | **92 µs** |
| HNSW vector search (10K × 128D, top-10) | **6.5 µs** |
| Brute-force vector search | 642 µs |
| Indexed SQL query | **2.1 ms** |
| Full-text search (BM25) | **29 ms** |

---

## 1. Insert Throughput

| Method | Rate | Notes |
|--------|------|-------|
| Individual insert | 109 nodes/sec | One redb transaction per node |
| **Batch insert** | **37,720 nodes/sec** | Single transaction, 5K batch |
| **Batch edges** | **28,219 edges/sec** | Single transaction, 5K batch |

Batch insert achieves a **345x speedup** over individual inserts by amortizing
the redb write-ahead log flush across thousands of entries in a single transaction.

## 2. Point Lookups

| Type | Mean | p50 | p99 |
|------|------|-----|-----|
| Full node (payload + properties) | 4.9 µs | 4.0 µs | 12.0 µs |
| Index-only (NodeIndex metadata) | 0.04 µs | < 1 µs | 1.0 µs |

**Index-only lookups are ~100x faster** than full payload fetches. This is what
graph traversal uses — only structural metadata (type, timestamps, payload
location), no property deserialization. When payloads reside on cloud storage
(S3/GCS), the index lookup remains local and sub-microsecond.

## 3. Graph Traversal

BFS traversal from a single node across a graph with average fan-out of 5:

| Depth | Nodes Visited | Edges Traversed | Latency |
|-------|--------------|-----------------|---------|
| 1 | 6 | 5 | 29 µs |
| 2 | 20 | 30 | 92 µs |
| 3 | 47 | 100 | 255 µs |

All traversals complete in **sub-millisecond time** on a 50K-node, 250K-edge graph.
Traversal uses index-only lookups for neighbor discovery, fetching full payloads
only for result materialization.

## 4. SQL Query Engine

Queries over 12,500 nodes of each type (50K total):

| Query | Latency | Notes |
|-------|---------|-------|
| `SELECT * FROM user LIMIT 10` | 50.5 ms | Full type scan |
| `SELECT * WHERE score > 50 LIMIT 10` | 51.0 ms | Filter scan |
| `SELECT * ORDER BY name LIMIT 10` | 54.0 ms | Sort + limit |
| **With secondary index** (`category = 'cat_7'`) | **2.1 ms** | B-tree index lookup |

### Secondary Index Impact

| Metric | Value |
|--------|-------|
| Index build time (12,500 entries) | 67 ms |
| Unindexed query | 51.2 ms |
| Indexed query | 2.1 ms |
| **Speedup** | **24.7x** |

Secondary indexes use a B-tree structure in redb (composite key:
`type\0field\0value` → `[NodeId]`). The query planner automatically routes
equality predicates through available indexes.

## 5. Full-Text Search

BM25-ranked search over 12,500 documents:

| Metric | Value |
|--------|-------|
| Index build time | 268 ms |
| Search latency ("entity topic details") | 29 ms |
| Results returned | 10 |

The inverted index stores per-document term frequencies. Scoring uses
BM25 with k1=1.2, b=0.75. Tokenization includes lowercase normalization,
whitespace splitting, and stopword removal.

## 6. Vector Search (HNSW)

Approximate nearest neighbor search over 10,000 128-dimensional vectors:

| Method | Latency | Speedup |
|--------|---------|---------|
| Brute-force linear scan | 642 µs | 1x |
| **HNSW ANN** | **6.5 µs** | **98.7x** |
| Filtered (`WHERE topic = 'x'`) | 61 ms | Pre-filter + brute |

| Metric | Value |
|--------|-------|
| HNSW build time (10K vectors) | 142 ms |
| Memory overhead per vector | ~512 bytes |

The managed HNSW index is built lazily on first search or explicitly via
`rebuild_vector_index()`. It supports incremental updates via
`insert_with_embedding()`.

---

## Comparison Context

AresaDB is an embedded multi-model database. For fair comparison, note:

| System | Strength | AresaDB Differentiator |
|--------|----------|----------------------|
| SQLite | Mature OLTP, ecosystem | AresaDB adds graph, vector, FTS, cloud tiering |
| DuckDB | Analytics, columnar | AresaDB is OLTP-oriented, graph-native |
| LanceDB | Vector-first, columnar | AresaDB adds graph, SQL, full-text, cloud tiering |
| Neo4j | Graph queries, Cypher | AresaDB is embedded, adds vector + FTS + SQL |
| Redis | In-memory speed | AresaDB is persistent, multi-model |

**AresaDB's unique position**: No other embedded database combines KV + Graph +
SQL + Vector Search + Full-Text Search with transparent cloud tiering in a
single binary.

---

## Running Benchmarks

```bash
# Full reproducible suite (writes JSON)
cargo run --example benchmark_suite --release

# Criterion micro-benchmarks
cargo bench --bench storage_bench
cargo bench --bench query_bench
cargo bench --bench distributed_bench

# Interactive demo with all features
cargo run --example tiered_storage_demo --release
```

---

## Benchmark History

| Date | Version | Nodes/sec (batch) | Point Lookup | HNSW (10K) | Tests |
|------|---------|--------------------|--------------|------------|-------|
| 2026-04-11 | 0.2.0-dev | 37,720 | 4.9 µs | 6.5 µs | 330 |
