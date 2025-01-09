# Changelog

All notable changes to AresaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned
- Distributed mode hardening

---

## [0.2.1] - 2026-04-11

### Added
- Cloud storage integration test suite covering GCS and S3 backends
  - Emulator-based tests against MinIO (S3) and fake-gcs-server (GCS) run on every CI build — no cloud credentials required
  - 7 GCS tests, 7 S3 tests, 8 tiered-storage tests (evict / read-from-cloud / promote / cache behavior, parameterized across both backends)
  - Gated real-cloud smoke tests (`tests/cloud_real.rs`) close the last few percent of confidence that emulators cannot cover (OAuth token refresh, IAM edge cases)
  - `docker-compose.test.yml`, `scripts/start_emulators.sh`, and `scripts/stop_emulators.sh` for local dev parity with CI
  - Dedicated `cloud-integration` CI job spins up both emulators and runs the full suite on every push and PR
  - Scheduled `cloud-smoke` CI workflow wired up to exercise real GCS + S3 on a nightly cadence (activated once repository secrets are populated)
  - `scripts/start_emulators.sh` probes the fake-gcs-server XML PUT path at startup and auto-skips GCS-write tests if the emulator doesn't support it (common with `object_store` 0.9). The S3 suite and the S3 half of the tiered-storage tests continue to run; the real-cloud smoke workflow validates the actual GCS XML API end-to-end
- `BucketStorage::connect` now honors `STORAGE_EMULATOR_HOST` (GCS) and `AWS_ENDPOINT_URL` (S3) for routing to local emulators or S3-compatible services
- `docs/cloud-testing-setup.md` — step-by-step guide for provisioning scoped GCP service accounts and AWS IAM users, plus wiring secrets into GitHub Actions
- `tests/README.md` — test-suite layout and cloud-testing how-to

### Changed
- Marked "Cloud Storage (S3/GCS)" as Done in `TODO.md` (was: Needs testing)

---

## [0.2.0] - 2026-04-11

### Added

#### Tiered Storage Engine
- Transparent cloud tiering where graph index stays local (sub-ms traversals) but node payloads can live on S3/GCS for infinite scale
- `TieredStorage` orchestrator with local → cache → cloud read path
- `NodeIndex` lightweight index records (type, timestamps, payload location) always stored locally
- `NODE_INDEX_TABLE` and `NODE_PAYLOADS_TABLE` split in redb for index/payload separation
- Read-through caching via moka for warm payloads
- Write-through option for immediate cloud replication
- `evict_to_cloud()` / `promote_to_local()` for manual payload migration
- `run_eviction()` automatic cold-data eviction when local storage exceeds threshold
- `prefetch_neighbors()` for graph traversal optimization
- `TieredConfig` for tuning local limits, cache size, write-through, prefetch
- `TieredStats` for observability (cache hits/misses, cloud fetches/pushes)
- Automatic migration of legacy databases to tiered format on open
- Backward-compatible: legacy `NODES_TABLE` kept in sync for existing tools
- 6 new tests for tiered storage (insert, get, update, delete, edges, cache, index-only)

#### HNSW Vector Indexes
- Managed HNSW vector indexes: automatic approximate nearest neighbor search
- HNSW index auto-built on first `similarity_search()` call (lazy initialization)
- `insert_with_embedding()` maintains index incrementally on each insert
- `rebuild_vector_index()` for explicit index rebuild after bulk loads
- Indexes managed per (node_type, embedding_field) pair
- ~99x speedup over brute-force linear scan (6.5µs vs 642µs on 10K 128D vectors)
- Filtered vector search: combine WHERE clauses with VECTOR_SEARCH
- Syntax: `VECTOR SEARCH table FIELD f FOR [...] WHERE col = 'val' AND col2 > 5 LIMIT k`

#### Secondary Property Indexes
- B-tree indexes for fast SQL query execution
- `CREATE INDEX ON table (field)` / `DROP INDEX ON table (field)` SQL commands
- Indexes stored in redb multimap tables for O(log n) lookups
- Auto-maintained on insert — indexes existing data on create, updates on new inserts
- Query planner automatically uses indexes when available
- `index_lookup()` API for programmatic access

#### Full-Text Search Engine
- Inverted index with BM25 ranking
- `CREATE FULLTEXT INDEX ON table (field)` SQL command
- `FULLTEXT SEARCH table FIELD field FOR 'query' LIMIT n` query syntax
- Tokenizer with stopword removal, case normalization
- BM25 scoring (k1=1.2, b=0.75) for relevance ranking
- Auto-maintained on insert for indexed fields
- `fulltext_search()` API returns (Node, score) pairs

#### Batch Insert APIs
- `insert_nodes_batch()`: single-transaction node bulk insert (~37,700 nodes/sec vs ~100/sec)
- `create_edges_batch()`: single-transaction edge bulk insert (~28,200 edges/sec)
- Both use single redb write transactions for the entire batch

#### Wire Protocol
- Complete wire protocol: all server handler operations implemented
- SQL query execution over TCP (`Request::Query`)
- Graph traversal over TCP (`Request::Traverse`)
- Edge deletion over TCP (`Request::DeleteEdge`)
- 6 new server handler tests

#### Python Bindings (PyO3)
- Expanded from 13 to 33 methods covering all 5 paradigms
- New: `insert_batch`, `update`, `create_edges_batch`, `get_edges_from`/`get_edges_to`, `delete_edge`
- New: `traverse`, `shortest_path`, `connected_components` (graph algorithms)
- New: `create_index`, `drop_index`, `list_indexes`, `index_lookup` (secondary indexes)
- New: `create_fulltext_index`, `fulltext_search`, `list_fulltext_indexes` (BM25)
- New: `similarity_search_radius`, `get_node_with_embedding`, `rebuild_vector_index`
- New: `PyEdge`, `PyTraversalResult`, `PyFulltextResult`, `PyIndexStats` types
- `create_edge` now accepts Python dicts (not just JSON strings) for properties
- `status()` now exposes `path` and `size_bytes`
- `.pyi` type stubs for full IDE autocompletion
- 38 pytest tests covering all API surfaces

#### Rust API
- Re-exported `DistanceMetric`, `SimilarityResult`, `VectorSearch`, `VectorNodeBuilder` at crate root

#### Documentation & Benchmarks
- Reproducible benchmark suite: `cargo run --example benchmark_suite --release`
- Publication materials (`paper/` directory) with Quarto source
- BENCHMARKS.md rewritten with real measured numbers
- ARCHITECTURE.md upgraded to publication-grade with table layout reference
- README.md updated with current performance numbers and features
- python/README.md rewritten with complete API documentation
- Crate docs updated to reflect all 5 paradigms with new architecture diagram

#### Examples
- Tiered storage demo: end-to-end benchmark at `examples/tiered_storage_demo.rs`

### Fixed
- Node count in `status()` now reads from tiered index table (was reading empty legacy table)
- Removed redundant legacy NODES_TABLE write from tiered insert path (performance fix)
- Fixed connection pool: replaced broken semaphore-based pool with atomic CAS
- Fixed protocol: client now imports via public re-exports (was using private module path)
- Fixed `status()` holding parking_lot guard across `.await` (not `Send`-safe)
- Fixed bincode serialization test to handle Value enum correctly

---

## [0.1.3] - 2026-04-11

### Fixed
- Docker build: use latest Rust image (resolved dependency edition requirements)
- macOS wheel build: pin Python interpreters to 3.9-3.13 (avoid 3.14 pre-release)

### Changed
- Bumped minimum supported Rust version to 1.85

---

## [0.1.2] - 2026-04-11

### Fixed
- Dockerfile Rust version bump from 1.75 to 1.85 for `getrandom` edition 2024 compatibility

---

## [0.1.1] - 2026-04-11

### Fixed
- Release pipeline: graceful crates.io re-publish for existing versions
- CI: removed `--all-features` flag (server module has pre-existing compilation issues)
- macOS CI runner updated from deprecated `macos-13` to `macos-latest`

---

## [0.1.0] - 2024-11-28

### Added

#### Core Database
- Property graph data model with Nodes and Edges
- Flexible Value types: String, Integer, Float, Boolean, Array, Object, Null
- Local storage backend using redb (embedded B+ tree, ACID)
- Zero-copy serialization with rkyv
- CRUD operations for nodes and edges
- Type-based indexing for fast queries
- Edge traversal (from/to relationships)

#### Query Engine
- SQL parser integration (sqlparser-rs)
- SELECT queries with column selection
- WHERE clause filtering (=, !=, <, >, <=, >=)
- ORDER BY sorting (ASC, DESC)
- LIMIT clause support
- Query planning and basic optimization

#### CLI
- `init` - Initialize new databases
- `insert` - Insert nodes with JSON properties
- `get` - Retrieve nodes by UUID
- `delete` - Delete nodes
- `query` - Execute SQL queries
- `view` - Multiple view formats (table, kv, graph)
- `status` - Database statistics
- `push` / `connect` / `sync` - Cloud storage commands
- `repl` - Interactive shell with history
- `traverse` - Graph traversal from a node
- Multiple output formats: table, json, csv

#### Vector & RAG
- Vector similarity search (cosine, euclidean, dot product, manhattan)
- Document chunking for RAG pipelines
- Hybrid search (vector + keyword)
- Embedding generation (local hash + OpenAI)

#### Cloud Storage
- S3 support via object_store
- GCS support via object_store
- Push/sync functionality
- Cache layer for remote data

#### Distributed Features (V2 building blocks)
- Write-Ahead Log (WAL) for durability
- Bloom filters for fast negative lookups
- LZ4 compression
- Consistent hashing for sharding
- Connection pooling structure
- Streaming results structure
- Leader election structure (Raft-like)

#### Python Bindings
- PyO3-based Python client (`pip install aresadb` / `uv add aresadb`)
- Full API: insert, query (SQL), get, delete, edges, vector search
- Wheels for Linux/macOS, Python 3.9-3.13

#### CI/CD
- GitHub Actions CI (check, test, lint, docs)
- Automated release on git tag: crates.io, PyPI, GHCR Docker
- Multi-platform wheel builds (Linux + macOS, x86_64 + ARM64)

#### Testing
- 170 unit and integration tests
- Property-based tests (proptest)
- Stress/concurrency tests
- Criterion benchmarks (storage, query, distributed)

#### Documentation
- README.md with usage guide
- ARCHITECTURE.md with technical details
- CONTRIBUTING.md with development guidelines
- QUICKSTART.md for getting started

### Performance
- ~300 records/sec insert rate
- Sub-millisecond point lookups
- ~380ms for 25K node scan
- ~5ms aggregation queries

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.2.0 | 2026-04-11 | Tiered storage, HNSW vectors, FTS, secondary indexes, expanded Python API |
| 0.1.3 | 2026-04-11 | Docker + macOS wheel fixes, MSRV 1.85 |
| 0.1.2 | 2026-04-11 | Dockerfile Rust version fix |
| 0.1.1 | 2026-04-11 | Release pipeline fixes |
| 0.1.0 | 2024-11-28 | Initial release with core functionality |

---

## Contributors

- Yevheniy Chuba ([@yevheniyc](https://github.com/yevheniyc)) - Creator

---

[Unreleased]: https://github.com/yoreai/aresadb/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/yoreai/aresadb/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/yoreai/aresadb/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/yoreai/aresadb/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/yoreai/aresadb/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/yoreai/aresadb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/yoreai/aresadb/releases/tag/v0.1.0
