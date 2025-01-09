# AresaDB Roadmap

> Last updated: April 2026

## Status

| Area | Status |
|------|--------|
| Core Database Engine | Done |
| Query Engine (SQL) | Done |
| CLI + REPL | Done |
| Vector / RAG | Done |
| Docker | Done |
| CI/CD | Done |
| Distributed (structure) | Done |
| Cloud Storage (S3/GCS) | Done (emulator + real-cloud smoke) |
| Tiered Storage (local+cloud) | Done |
| Secondary Indexes | Done |
| Full-Text Search (BM25) | Done |
| Wire Protocol (TCP server) | Done |
| Python Client (PyO3) | Done |
| crates.io Publishing | Done |
| PyPI Publishing | Done |
| GHCR Docker Images | Done |
| Automated Release Pipeline | Done |
| Reproducible Benchmarks | Done |
| Publication Draft | Done |

---

## Next Up

### Testing
- [x] Cloud storage integration tests (S3, GCS) — emulator-based (MinIO + fake-gcs-server) on every CI run, plus gated real-cloud smoke tests
- [ ] Expand test coverage (target 80%+)
- [ ] Scale tests (100K+ records)
- [x] Concurrent read/write stress tests (330+ tests)

### Engine Improvements
- [x] Tiered storage: index/payload split with cloud tiering
- [x] Read-through cache for cloud payloads
- [x] Eviction/promotion between local and cloud storage
- [x] Auto-migration of legacy databases to tiered format
- [x] HNSW vector index: managed ANN indexes per (node_type, field), lazy-build, incremental updates
- [x] Filtered vector search: `VECTOR SEARCH ... WHERE col = 'val'` syntax with pre-filtering
- [x] Batch insert API: `insert_nodes_batch()`, `create_edges_batch()` for high-throughput ingestion
- [x] Secondary property indexes: B-tree indexes via `CREATE INDEX ON table (field)`, auto-maintained
- [x] Full-text search engine: inverted index with BM25 ranking, `CREATE FULLTEXT INDEX` + `FULLTEXT SEARCH`
- [x] Server wire protocol: query, traversal, edge delete all implemented over TCP
- [ ] Advanced SQL (subqueries, CTEs, JOINs)

### Distributed (future)
- [ ] Multi-master replication
- [ ] Auto-sharding
- [ ] Cross-shard queries

### Publication
- [x] Reproducible benchmark suite (`cargo run --example benchmark_suite --release`)
- [x] BENCHMARKS.md with measured data
- [x] ARCHITECTURE.md (publication-grade)
- [x] Paper drafted (rendered PDF at `paper/aresadb-paper.pdf`)
- [ ] Generate vector figures from benchmark JSON
- [ ] Submit to arXiv preprint
- [ ] Submit to VLDB/SIGMOD/CIDR

### Ecosystem (future)
- [ ] JavaScript/TypeScript client
- [ ] Apache Arrow support
- [ ] Parquet import/export
