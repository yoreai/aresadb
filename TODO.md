# AresaDB Roadmap

> Last updated: April 2026
>
> **v2 distributed architecture is underway.** See
> [`docs/architecture-v2.md`](docs/architecture-v2.md) for the full spec
> and [`docs/phase-status.md`](docs/phase-status.md) for live execution
> status. This file still tracks v1 areas; v2 roadmap lives in
> `phase-status.md` so we don't double-maintain it.

## v1 status (embedded, single-node)

| Area | Status |
|------|--------|
| Core Database Engine | Done |
| Query Engine (SQL) | Done |
| CLI + REPL | Done |
| Vector / RAG | Done |
| Docker | Done |
| CI/CD | Done |
| Distributed (building blocks) | Done (scaffolding only — real distribution lives in v2) |
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

## v2 status (distributed)

| Phase | Status |
|-------|--------|
| Phase 0 — Foundations (workspace, `StorageBackend`, madsim harness) | Done |
| Phase 1 — Single-shard cluster (openraft + gRPC + durable redb + 3-node compose) | Done — tagged `v2.0.0-alpha.1` |
| Phase 2 — Multi-Raft + range sharding + LSM backend | Done — tagged `v2.0.0-alpha.2` |
| Phase 3 — Distributed query execution | Planned |
| Phase 4 — Distributed transactions (MVCC + parallel commit + SSI) | Planned |
| Phase 5 — Thread-per-core LSM engine (headline benchmark numbers) | Planned |
| Phase 6 — CDC change feeds + online distributed schema changes | Planned |

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

### Distributed — moved to v2

The v1 "distributed building blocks" (`src/distributed/`) were always a
scaffolding placeholder: a shard manager with in-process sharding, a
Raft-like state machine that had no transport, a WAL stub, bloom filters,
compressors. None of them are wired into a real multi-node cluster.

The real distributed story is v2. See `docs/architecture-v2.md`.

- Multi-master replication → v2 Phase 4 (cross-shard transactions).
- Auto-sharding → v2 Phase 2 (range-based splits/merges/rebalancing).
- Cross-shard queries → v2 Phase 3 (scatter-gather, distributed BFS /
  vector / full-text).

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
