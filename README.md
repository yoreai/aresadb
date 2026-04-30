# AresaDB

<div align="center">

**High-Performance Multi-Model Database Engine in Rust**

*Key-Value . Graph . SQL . Vector . Full-Text Search -- All in One Binary*

[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/aresadb.svg)](https://crates.io/crates/aresadb)
[![PyPI](https://img.shields.io/pypi/v/aresadb.svg)](https://pypi.org/project/aresadb/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/yoreai/aresadb/actions/workflows/ci.yml/badge.svg)](https://github.com/yoreai/aresadb/actions)

</div>

> **v2 distributed architecture in progress.** v0.2.1 remains the latest
> stable embedded release. The `main` branch is now tracking
> `2.0.0-alpha.2` — the full multi-Raft + range-sharded cluster, with a
> replicated placement driver (`aresadb-pd`), range-aware
> `ClusterNode` (many Raft groups per node, PD-driven add/remove,
> leader-lease reads), range-aware write / read RPCs, deterministic-
> simulation coverage for cross-range isolation, a 3-node Docker
> Compose smoke, and an opt-in fjall-backed LSM data engine
> alongside the default redb. The full Phase-2 arc (2a keyspace, 2b
> PD, 2c range-aware cluster, 2d LSM) is on disk, tested, and
> clippy-clean. See
> [`docs/architecture-v2.md`](docs/architecture-v2.md) for the full
> design and [`docs/phase-status.md`](docs/phase-status.md) for live
> progress. Distributed query (Phase 3) is next.

---

## Install

**Rust:**
```bash
cargo install aresadb
# or: cargo add aresadb
```

**Python:**
```bash
pip install aresadb
# or: uv add aresadb
```

**Docker:**
```bash
docker pull ghcr.io/yoreai/aresadb:latest
docker run -it -v $(pwd)/data:/data ghcr.io/yoreai/aresadb
```

---

## 30-Second Quickstart

```bash
# Create a database, insert data, query it
aresadb init ./mydata --name demo
aresadb -d ./mydata insert user --props '{"name": "Alice", "age": 30}'
aresadb -d ./mydata query "SELECT * FROM user WHERE age > 25"
```

---

## Why AresaDB?

Most databases force you to choose: relational OR graph OR key-value OR vector. AresaDB unifies all five models under a single property graph with SQL, vector, and full-text search.

- **Single binary, zero config** -- embedded, no servers, no setup
- **Sub-microsecond index lookups** -- graph traversal at < 1µs per hop
- **HNSW vector search** -- 99x faster than brute force (6.5µs for 10K vectors)
- **Full-text search** -- inverted index with BM25 ranking
- **Secondary indexes** -- B-tree property indexes, 25x query speedup
- **Transparent cloud tiering** -- graph index stays local, payloads scale to S3/GCS
- **37K+ batch inserts/sec** -- single-transaction bulk loading
- **330+ tests** -- unit, integration, stress, and concurrent

---

## Usage

### CLI

```bash
aresadb init ./mydata --name myapp

aresadb -d ./mydata insert user --props '{"name": "John", "email": "john@example.com", "age": 30}'

aresadb -d ./mydata query "SELECT * FROM user"
aresadb -d ./mydata query "SELECT name, email FROM user WHERE age > 25 ORDER BY age DESC"

# Output formats
aresadb -d ./mydata -f json query "SELECT * FROM user"
aresadb -d ./mydata -f csv query "SELECT * FROM user" > export.csv

# Graph operations
aresadb -d ./mydata traverse <node-id> --depth 3

# Interactive REPL
aresadb -d ./mydata repl
```

### As a Rust Library

```rust
use aresadb::{Database, DistanceMetric};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Database::create("./mydata", "myapp").await?;

    let user = db.insert_node("user", serde_json::json!({
        "name": "Alice", "email": "alice@example.com"
    })).await?;

    let users = db.get_all_by_type("user", Some(100)).await?;

    let results = db.similarity_search(
        &[1.0, 0.0, 0.0, 0.0],
        "document", "embedding", 10,
        DistanceMetric::Cosine,
    ).await?;

    Ok(())
}
```

### As a Python Library

```python
from aresadb_python import Database

db = Database.create("./mydata", "demo")
# db = Database.open("./mydata")  # re-open existing

# Nodes — single, batch, update, delete
user = db.insert_dict("user", {"name": "Alice", "age": 30})
nodes = db.insert_batch([("user", {"name": "Bob"}), ("user", {"name": "Charlie"})])
db.update(user.id, {"age": 31})

# SQL
result = db.query("SELECT * FROM user WHERE age > 25")

# Edges & graph traversal
edge = db.create_edge(user.id, nodes[0].id, "follows", {"since": "2024"})
path = db.shortest_path(user.id, nodes[1].id)
traversal = db.traverse(user.id, max_depth=3)

# Secondary indexes
db.create_index("user", "age")
hits = db.index_lookup("user", "age", 30)

# Full-text search (BM25)
db.create_fulltext_index("user", "name")
fts = db.fulltext_search("user", "name", "Alice", limit=5)

# Vector search (HNSW)
results = db.similarity_search([1.0, 0.0, 0.0, 0.0], "document", "embedding", 10)
```

See [python/README.md](python/README.md) for the full Python API (33 methods, type stubs, 38 tests).

### Vector Search

```bash
aresadb -d ./mydata embed document \
  --props '{"title": "ML Intro", "content": "Machine learning is..."}' \
  --vector '[0.9, 0.1, 0.0, 0.0]' \
  --field embedding

aresadb -d ./mydata search document \
  --vector '[1.0, 0.0, 0.0, 0.0]' \
  --k 10 --metric cosine

aresadb -d ./mydata query \
  "VECTOR SEARCH document FIELD embedding FOR [1.0, 0.0, 0.0, 0.0] METRIC cosine LIMIT 10"
```

### Cloud Storage

```bash
aresadb -d ./mydata push s3://mybucket/databases/myapp
aresadb connect gs://mybucket/databases/myapp
aresadb -d ./mydata sync s3://mybucket/databases/myapp
```

---

## Architecture

```
┌──────────────────────────────────────────────┐
│           CLI / REPL / Library / TCP          │
├──────────────────────────────────────────────┤
│              Query Engine (SQL)               │
│    Parser → Planner → Executor               │
│    Secondary Indexes . Full-Text Search      │
├──────────────────────────────────────────────┤
│     Graph Engine      │   Vector Engine       │
│  BFS/DFS Traversal    │  HNSW ANN Index       │
│  Index-only hops      │  Filtered Search      │
├──────────────────────────────────────────────┤
│           Tiered Storage Engine               │
│  ┌─────────┐  ┌──────────┐  ┌─────────────┐ │
│  │  Local   │  │  Cache   │  │   Cloud     │ │
│  │  (redb)  │←→│  (moka)  │←→│  (S3/GCS)  │ │
│  │  B+ Tree │  │   LRU    │  │ object_store│ │
│  └─────────┘  └──────────┘  └─────────────┘ │
│     Index always local; payloads tiered       │
└──────────────────────────────────────────────┘
```

### Data Model

Everything is a **property graph**: nodes with typed properties, connected by edges.
The same data is queryable as tables (SQL), key-value pairs, or graph traversals.

| Crate | Purpose |
|-------|---------|
| `redb` | Embedded B+ tree storage (ACID) |
| `rkyv` | Zero-copy serialization |
| `sqlparser` | SQL parsing |
| `object_store` | S3/GCS abstraction |
| `petgraph` | Graph algorithms |
| `moka` | High-performance LRU cache |
| `tokio` | Async runtime |

---

## Performance

> Measured on Apple M2 Pro, 16GB RAM. See [BENCHMARKS.md](BENCHMARKS.md) for full methodology.

| Operation | Latency / Throughput |
|-----------|---------------------|
| Batch insert | **37,720 nodes/sec** |
| Point lookup | **4.9 µs** mean (p99: 12 µs) |
| Index-only lookup | **< 1 µs** |
| Graph traversal (depth-2) | **92 µs** |
| HNSW vector search (10K × 128D) | **6.5 µs** (99x vs brute) |
| Indexed SQL query | **2.1 ms** (25x vs scan) |
| Full-text search (BM25) | **29 ms** |

```bash
# Reproduce these numbers:
cargo run --example benchmark_suite --release
```

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `init` | Create new database |
| `insert` | Insert a node |
| `get` | Get node by ID |
| `delete` | Delete a node |
| `query` | Execute SQL query |
| `view` | View data (table/kv/graph) |
| `status` | Database statistics |
| `traverse` | Graph traversal |
| `embed` | Insert with vector embedding |
| `search` | Vector similarity search |
| `chunk` | Split document for RAG |
| `context` | Retrieve RAG context |
| `ingest` | Chunk + embed + store |
| `push/connect/sync` | Cloud storage operations |
| `repl` | Interactive shell |

---

## Docker

```bash
docker run -it -v $(pwd)/data:/data ghcr.io/yoreai/aresadb

docker-compose up -d
```

See [Dockerfile](Dockerfile) for the multi-stage build.

---

## Testing

```bash
cargo test
cargo test -- --nocapture
cargo bench
```

Cloud storage (S3, GCS) is integration-tested on every CI run against local MinIO and fake-gcs-server emulators, plus gated real-cloud smoke tests. See [tests/README.md](tests/README.md) for how to run them locally.

---

## Documentation

- [QUICKSTART.md](QUICKSTART.md) -- Get running in 5 minutes
- [ARCHITECTURE.md](ARCHITECTURE.md) -- Technical deep-dive
- [BENCHMARKS.md](BENCHMARKS.md) -- Performance methodology and results
- [EXAMPLES.md](EXAMPLES.md) -- Real-world use cases
- [CONTRIBUTING.md](CONTRIBUTING.md) -- How to contribute
- [CHANGELOG.md](CHANGELOG.md) -- Version history
- [tests/README.md](tests/README.md) -- Test suite layout and how to run cloud integration tests

---

## Roadmap

**v1 (embedded, single-node) — stable.** Core engine, SQL, vectors,
HNSW, secondary indexes, full-text/BM25, tiered cloud storage,
batch insert APIs, filtered vector search, TCP wire protocol,
Python client, Docker images, cloud-storage CI on emulators + real
clouds. See [`CHANGELOG.md`](CHANGELOG.md) for the released history.

**v2 (distributed) — `2.0.0-alpha.2` shipped on `main`.**

- [x] Phase 0 — workspace, `StorageBackend` trait, madsim harness
- [x] Phase 1 — single-shard cluster (openraft + gRPC + redb + 3-node compose), tagged `v2.0.0-alpha.1`
- [x] Phase 2 — multi-Raft + range sharding + opt-in fjall LSM, tagged `v2.0.0-alpha.2`
- [ ] Phase 3 — distributed query execution (router + planner + scatter-gather)
- [ ] Phase 4 — distributed transactions (HLC + MVCC + parallel commit + SSI)
- [ ] Phase 5 — thread-per-core LSM engine
- [ ] Phase 6 — CDC change feeds + online distributed schema changes

Implementation tracker: [`docs/phase-status.md`](docs/phase-status.md).
Architecture: [`docs/architecture-v2.md`](docs/architecture-v2.md).
Operator runbook: [`docs/operations.md`](docs/operations.md).

---

## License

MIT -- see [LICENSE](LICENSE).

<div align="center">

Built by [YoreAI](https://github.com/yoreai)

</div>
