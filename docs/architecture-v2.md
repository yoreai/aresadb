# AresaDB v2 — Distributed Architecture

> Status: **Ratified — execution in progress**
> Author: AresaDB team
> Created: April 2026
> Target release: v2.0
> Companion doc: [ARCHITECTURE.md](../ARCHITECTURE.md) (current single-node design)
>
> See [§15 Ratified Decisions](#15-ratified-decisions) for the locked-in choices.
> See [PHASE_STATUS.md](./phase-status.md) for live execution status.

---

## 1. Vision and Goals

AresaDB v1 is an embedded multi-model engine: KV, SQL, Graph, Vector, Full-Text
in a single Rust binary, with transparent cloud tiering for payloads. v2 keeps
everything that makes v1 useful and adds a real distributed mode that can
honestly claim:

> *AresaDB is one of the fastest open-source multi-model databases — embedded
> on a single node, **and** linearizable across a cluster.*

### 1.1 Goals (in priority order)

1. **Correctness first.** Linearizable single-key writes, serializable
   single-shard transactions, serializable cross-shard transactions (Phase 4),
   verified by deterministic simulation and Jepsen-style testing.
2. **Single-node performance unchanged or better.** v1 numbers are the floor.
   Embedded mode keeps the redb backend and current API.
3. **Sub-millisecond p50 reads** in a 3-node cluster on commodity hardware.
4. **>100 K writes/sec/node** sustained for batch inserts, >50 K random KV
   writes/sec/node in cluster mode.
5. **All five models work distributed** — KV, SQL, Graph, Vector, FTS.
6. **Operationally trivial** — single binary, optional in-process placement
   driver, no external ZooKeeper/etcd.
7. **Open source, MIT.** No proprietary cluster tier.

### 1.2 Non-goals (explicitly out of scope for v2.0)

- **Multi-region / global consistency** (Spanner / TrueTime) — defer to v3.
- **Time-travel queries** (AS OF SYSTEM TIME) beyond the MVCC GC window — defer.
- **Geo-distributed replicas with local read replicas** — defer to v3.
- **Active-active conflict resolution** — defer.
- **A "cluster manager" web UI.** CLI + Prometheus + Grafana JSON only for v2.0.
- **SQL feature parity with Postgres** (JOINs across tables, CTEs, subqueries,
  stored procedures) — see v1 backlog; orthogonal to distribution.

In scope (by user decision):
- **Logical CDC / change feeds** (Phase 6).
- **Online schema migration for distributed indexes** (Phase 6).
- **Thread-per-core LSM engine for headline performance** (Phase 5).

### 1.3 What stays the same

- Property-graph data model (`Node`, `Edge`, properties as `BTreeMap<String, Value>`).
- Public Rust API surface (`Database::create`, `db.insert_node`, `db.query`).
- Python bindings.
- SQL syntax (`VECTOR SEARCH`, `FULLTEXT SEARCH`, `CREATE INDEX`).
- Embedded single-process mode — works exactly as today, no networking.
- redb as the default local engine.
- Tiered cloud storage for payloads (S3 / GCS).

### 1.4 What changes

- A new **cluster mode** (`aresadb cluster …`) that runs the same binary as
  a node in a multi-machine cluster.
- A new **storage backend abstraction** (`StorageBackend` trait) — redb is
  one implementation; a thread-per-core LSM engine is another (Phase 5).
- A **range-based** internal keyspace replacing the per-table-name redb
  tables. Range-based partitioning is what enables horizontal scaling for
  graph/SQL workloads (hash partitioning destroys range scans).
- **Multi-Raft** replication: one Raft consensus group per range, default
  RF=3.
- A small **Placement Driver** (PD) that owns cluster metadata and
  coordinates range splits, merges, and rebalancing.
- An inter-node **gRPC transport** (in addition to the existing client TCP
  protocol).
- An **MVCC value layer** with **Hybrid Logical Clocks** (HLC) for
  cross-shard transactions (Phase 4).

---

## 2. Topology and Roles

A cluster is a set of self-organizing nodes. Each node can hold one or
more of three roles, and by default holds all three:

| Role | Responsibility |
|------|----------------|
| `data` | Serves Raft groups for ranges assigned to it. Holds the actual data. |
| `gateway` | Accepts client connections, parses queries, routes/scatters to data nodes. |
| `pd` | Member of the Placement Driver Raft group. Owns cluster metadata: nodes, ranges, leases. |

The PD role runs on a small odd-numbered set (default 3) of nodes and is
itself a Raft group whose state machine is the cluster catalog. Every
other role can scale out arbitrarily.

```
                     ┌────────────────────────────────────────┐
                     │            Clients (Rust / Py)         │
                     └──────────────────┬─────────────────────┘
                                        │ TCP wire protocol (length-prefixed bincode)
                                        ▼
              ┌─────────────────────────────────────────────────────┐
              │                     Gateway Layer                   │
              │   parse SQL / route by range / scatter-gather       │
              └─────────────────────────────────────────────────────┘
                                        │ gRPC (tonic) inter-node
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
      ┌──────────┐                ┌──────────┐                ┌──────────┐
      │ Data N1  │                │ Data N2  │                │ Data N3  │
      │ ranges:  │   Multi-Raft   │ ranges:  │  Multi-Raft    │ ranges:  │
      │ A,B,C    │ ◄───────────► │ A,B,D    │ ◄────────────► │ B,C,D    │
      └────┬─────┘                └────┬─────┘                └────┬─────┘
           │                            │                            │
           └────────────────────────────┼────────────────────────────┘
                                        │ gRPC
                                        ▼
                         ┌─────────────────────────────┐
                         │   Placement Driver (PD)     │
                         │  3-node Raft group          │
                         │  catalog: nodes, ranges,    │
                         │  leases, schemas            │
                         └─────────────────────────────┘
```

Discovery: bootstrap with a list of seed addresses. Membership is then
maintained via a lightweight gossip protocol (SWIM-style) for liveness
detection only; authoritative membership lives in the PD.

---

## 3. Keyspace and Sharding

### 3.1 Unified keyspace

All data lives in a single sorted byte-keyspace. Prefixes namespace the
five models:

```
/m/<key>                                    cluster metadata
/n/<NodeId>                                 node payloads
/i/<NodeId>                                 node index entries (small)
/e/<EdgeId>                                 edge records
/ef/<from_node><edge_id>                    edge-by-from index
/et/<to_node><edge_id>                      edge-by-to index
/p/<type>/<field>/<value>/<NodeId>          secondary B-tree property index
/ft/<type>/<field>/<token>/<NodeId>         full-text inverted index
/v/<type>/<field>/<NodeId>                  HNSW vector entries
/s/<type>                                   schema registry
/x/<TxId>                                   transaction records (Phase 4)
```

This is the same pattern CockroachDB and TiKV use. It makes range scans
(`SELECT … WHERE x BETWEEN a AND b`, BFS frontiers, FTS posting list
walks) efficient, because keys that need to be read together end up in
the same physical range most of the time.

### 3.2 Range-based partitioning

The keyspace is split into contiguous ranges. Each range:

- Has a `[start_key, end_key)` half-open interval.
- Is replicated by exactly one Raft group (default RF=3).
- Targets a soft size limit of **64 MB** and soft load limit (configurable).
- Is split when it exceeds 2× target size or when load metrics flag a hot spot.
- Adjacent ranges below 16 MB and below load thresholds are merged.

Range descriptors live in the PD's catalog:

```rust
struct RangeDescriptor {
    range_id: RangeId,          // monotonic u64
    start_key: Vec<u8>,
    end_key: Vec<u8>,           // exclusive; empty => +infinity
    replicas: Vec<ReplicaPlacement>,  // [{node_id, store_id, voter|learner}]
    raft_group_id: GroupId,     // typically == range_id
    epoch: u64,                 // bumps on every membership change
    generation: u64,            // bumps on split/merge
    lease: Option<LeaseInfo>,   // current leader lease
}
```

Splits are atomic operations replicated through the parent range's Raft
log; the new right-hand-side range starts with the same replica
placement as the parent.

### 3.3 Why not consistent hashing?

The existing `ShardManager` uses xxhash + 150 virtual nodes. That works
fine for KV, but it kills:

- **Range scans** in SQL (`WHERE age BETWEEN 25 AND 40`).
- **Graph traversal locality** (a node and its outgoing edges should
  ideally co-locate).
- **Sorted iteration** (`ORDER BY name LIMIT 10` over a property index).

Range partitioning + co-location heuristics outperform hash partitioning
for every workload AresaDB cares about *except* perfectly random uniform
KV writes — and for that case the auto-splitter ends up spreading load
just as well anyway.

### 3.4 Co-location

Two heuristics encourage related data to land on the same range:

1. **Edge-by-from** uses the from-node's `NodeId` as its key prefix, so
   walking outgoing edges of a node usually stays in one range.
2. **Property index** uses `type/field/value` prefix, so equality
   lookups land in one range and range scans walk a small contiguous
   slice.

---

## 4. Replication: Multi-Raft via openraft

### 4.1 Why Multi-Raft?

A single Raft group across the cluster bottlenecks on the leader. Every
real-world distributed Rust DB (TiKV, CockroachDB-style designs, Databend,
RisingWave) runs **one Raft group per data range**, scheduled across many
nodes. Hundreds-to-thousands of independent Raft groups per cluster is
normal.

### 4.2 Library: `openraft`

We standardize on [openraft](https://github.com/datafuselabs/openraft):

- Modern async-first Rust Raft, no `unsafe`, mature.
- Used in production by Databend, Greptime, RisingWave.
- Modular: pluggable storage, pluggable network, pluggable types.
- Implements joint consensus, pre-vote, leader leases, learners, snapshots.

Rolling our own Raft would be a 3-month project on its own. openraft saves
us that effort and is battle-tested by people doing exactly this.

### 4.3 Reads

Three read modes, opt-in per query:

| Mode | Latency | Consistency | When to use |
|------|---------|-------------|-------------|
| **Leader-lease read** | ~1 RTT to leader, no Raft round-trip | Linearizable | Default for OLTP |
| **Read-index** | 1 RTT to leader, then quorum check | Linearizable | When lease expired |
| **Bounded staleness** | Local follower read | Stale up to N seconds | Analytics, warm caches |

Leader leases (default 9 s, renewable) make reads as fast as a single
node-to-leader RPC plus the leader's local read.

### 4.4 Writes

Standard Raft commit path:

1. Gateway routes write to the range's leader (cached from the PD).
2. Leader appends to its log, sends `AppendEntries` to followers.
3. On quorum ack, leader commits, applies to its state machine, returns to client.
4. Followers apply asynchronously after their local commit advances.

### 4.5 Membership changes

Joint consensus (the safe Raft variant). Adding/removing a replica
goes through a transitional configuration (C_old ∪ C_new) before
finalizing C_new. No risk of split-brain during transitions.

### 4.6 Failure semantics

- A range with f failures out of 2f+1 replicas continues serving reads
  and writes (default RF=3 ⇒ tolerates 1 failure).
- Loss of quorum: range becomes unavailable for writes. Reads can still
  use stale-read mode if any replica is reachable.
- The PD watches replica liveness via gossip and triggers replacement
  when a replica is suspected down for > T (default 5 min).

---

## 5. Storage Backend Abstraction

### 5.1 The `StorageBackend` trait

We extract a minimal trait for what a Raft state machine needs from local
storage:

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn scan(&self, range: KeyRange) -> Result<Box<dyn KeyValueStream>>;
    async fn batch_write(&self, batch: WriteBatch) -> Result<()>;
    async fn snapshot(&self) -> Result<Box<dyn Snapshot>>;
    async fn ingest_snapshot(&self, snap: Box<dyn Snapshot>) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    fn approximate_size(&self, range: &KeyRange) -> u64;
}
```

### 5.2 Backends

| Backend | Status | Notes |
|---------|--------|-------|
| `redb` | Phase 1 | Wraps existing `LocalStorage`. Default. |
| `lsm` | Phase 5 | Thread-per-core LSM, glommio + io_uring. New, optional. |
| `memory` | Phase 1 | For tests. |

The redb backend is what ships in v2.0. The LSM backend is the path to
"genuinely fastest" — it's optional and feature-gated so we can land a
correct cluster first and optimize later.

### 5.3 The thread-per-core LSM engine (Phase 5)

This is what unlocks ScyllaDB / Aerospike-class single-node throughput:

- One executor per CPU core via [`glommio`](https://github.com/DataDog/glommio).
- Each core owns a fixed slice of the local key-space (sub-shards).
- All I/O via **io_uring** with fixed buffers and direct I/O.
- LSM with leveled compaction, Bloom filters, prefix Bloom filters.
- Memtable: skip list per core, no cross-core locks.
- SSTables: mmap'd for reads, fixed-block format with prefix compression.
- Zero-copy reads via `bytes::Bytes` for shared buffers.
- Background compaction on dedicated executors.

Targets:

- 1 M+ random KV reads/sec/node on a 16-core machine.
- 200 K+ random KV writes/sec/node.
- p99 read latency < 200 µs from cache, < 1 ms from SSTable.

This is the "fastest single-node" play. It also helps cluster mode
because the per-node engine is what serves Raft's state machine.

---

## 6. Transactions and Consistency

### 6.1 Phase 1-3: single-key + single-shard

- Every Raft commit is its own atomic operation → single-key
  linearizable.
- Single-shard transactions = a single Raft proposal containing a write
  batch → atomic by construction.

### 6.2 Phase 4: cross-shard transactions (MVCC + parallel commit)

Inspired directly by CockroachDB.

**Hybrid Logical Clocks (HLC):** Every value is tagged
`{commit_ts: HLC}`. HLC = `(physical_ms, logical_counter, node_id)`.
HLC behaves like a wall clock but is monotonic across the cluster within
a clock-skew bound (we require ≤ 500 ms NTP skew, configurable).

**MVCC value encoding:**

```
key:   /n/<NodeId>/<reverse_ts>          // newest first
value: <committed payload bytes>

key:   /n/<NodeId>/<reverse_ts>/intent   // optional intent for in-flight tx
value: { tx_id, payload }
```

Snapshot reads pick a timestamp `t`, scan committed values where
`commit_ts ≤ t`, ignore intents. GC removes versions older than the
configured retention (default 25 hours).

**Parallel commit (CockroachDB optimization):**

A transaction is logically committed once all of:

1. The transaction record exists (initial state = `STAGING`).
2. All intent writes are durable on their respective ranges.

…are visible to readers. The actual `STAGING → COMMITTED` flip happens
asynchronously. Readers that find a `STAGING` record check whether all
intents are present; if so, they treat the transaction as committed and
opportunistically promote the record. This shaves one Raft round-trip
off the critical path for cross-shard writes.

**Isolation:** Serializable Snapshot Isolation (SSI) by default. We
detect read-write conflicts via timestamp-cache tracking on each leader.
Optionally Read Committed for compatibility.

**Conflict detection:** Each leader keeps a small in-memory timestamp
cache: "the highest read timestamp observed for each key range." Writes
that would commit at `t < cache[key]` are bumped to a higher timestamp
or aborted, depending on isolation level.

### 6.3 What this gives us

- Single-key writes: linearizable, < 5 ms p99 in a 3-node cluster.
- Single-shard transactions: serializable, < 5 ms p99.
- Cross-shard transactions: serializable, ~ 2× single-shard latency.
- Snapshot reads: locally evaluable, very fast, slightly stale.

---

## 7. Distributed Query Execution

The current `QueryEngine` runs against `Database`. In v2 we split it:

```
SQL string
   │
   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Parser     │ →  │   Planner    │ →  │  Distributor │
│ (sqlparser)  │    │ (logical)    │    │  (physical)  │
└──────────────┘    └──────────────┘    └──────┬───────┘
                                               │
                ┌──────────────────────────────┼──────────────────────────────┐
                ▼                              ▼                              ▼
        ┌──────────────┐               ┌──────────────┐               ┌──────────────┐
        │ Local Exec   │               │ Local Exec   │               │ Local Exec   │
        │ on Range R1  │               │ on Range R2  │               │ on Range R3  │
        └──────┬───────┘               └──────┬───────┘               └──────┬───────┘
               └──────────────────┬───────────┴──────────────────────────────┘
                                  ▼
                          ┌──────────────┐
                          │  Aggregator  │
                          │  + sort/limit│
                          └──────────────┘
```

### 7.1 Routing

- **Point lookup by `NodeId`**: PD lookup → 1 RPC to range leader → done.
- **Equality on indexed property**: 1 RPC to the range owning that
  property index slice.
- **Range scan / unindexed query**: scatter to all ranges of the
  relevant prefix, gather, merge.
- **Graph traversal**: BFS frontier maintained at the gateway; each
  step batches per-range RPCs and merges results.
- **Vector search**: each range maintains its partition of the HNSW
  index; gateway broadcasts the query, gathers per-range top-k,
  merges to global top-k.
- **Full-text search**: per-range inverted index; gateway gathers
  postings + per-range BM25 scores, then re-ranks using cluster-wide
  document frequency (cached at the gateway, refreshed periodically).

### 7.2 Push-down

Filters, projections, and limits push to the data node executing the
local scan. We never ship a full table to the gateway just to filter it
there.

### 7.3 Streaming

Results stream from data nodes to the gateway, gateway streams to the
client. Memory-bounded execution via backpressure.

---

## 8. Wire Protocols

### 8.1 Client ↔ Gateway: existing TCP protocol

The current binary protocol (length-prefixed bincode `Request`/`Response`)
stays as the public client interface. We add:

- `Connect { client_id }` for connection identity.
- `RouteHint { range_id, leader_node_id }` so smart clients can cache
  leader locations and skip the gateway hop.

### 8.2 Inter-node: gRPC (tonic)

openraft already uses tonic. We use it for:

- Raft RPCs (`AppendEntries`, `RequestVote`, snapshots).
- PD ↔ Data: range descriptor lookups, lease grants, range split/merge
  coordination.
- Gateway ↔ Data: scan, scatter-gather, traversal-step RPCs.

### 8.3 Why not pure custom protocol everywhere?

We benchmarked it; gRPC is "fast enough" (single-digit-µs framing
overhead) and the tooling/observability win is substantial. The hot
path in Phase 5 (storage I/O) is io_uring directly anyway, not RPC.
Optionally swap to QUIC in v2.1 if HOL blocking shows up.

---

## 9. Operations and Observability

### 9.1 Single binary, multiple modes

```bash
# Embedded (unchanged)
aresadb init ./data --name myapp
aresadb -d ./data query "SELECT * FROM user"

# Cluster
aresadb cluster init --name prod --pd-peers n1:6000,n2:6000,n3:6000
aresadb cluster start --node-id n1 --bind 0.0.0.0:7432 \
  --gossip 0.0.0.0:7433 --raft 0.0.0.0:7434 --pd 0.0.0.0:6000 \
  --data-dir ./data --roles data,gateway,pd

# Admin
aresadb admin nodes
aresadb admin ranges --hot
aresadb admin balance start
```

### 9.2 Observability

- **OpenTelemetry** traces with W3C trace-context propagation through
  RPCs and Raft commits.
- **Prometheus** metrics on `/metrics` (HTTP server on each node):
  - Per-range Raft state, log lag, leader lease state.
  - Per-RPC latency histograms (p50/p99/p999).
  - Storage backend: read/write/compaction stats.
  - Query engine: planner cost, scatter fan-out, gather merge time.
- Structured tracing logs via the existing `tracing` crate.
- Per-cluster grafana dashboards shipped as JSON.

### 9.3 Backups and restore

- `aresadb admin backup --to s3://…` — consistent snapshot via Raft
  snapshots from each range, written as a manifest + per-range files
  to object storage.
- `aresadb admin restore --from s3://…` — restore into a fresh
  cluster with matching topology (or auto-rebalance if smaller).
- Continuous backup via incremental Raft log shipping to object storage
  (Phase 6).

---

## 10. Testing Strategy

We treat distributed correctness as the single biggest risk and invest
proportionally.

### 10.1 Layers

| Layer | Tool | What it catches |
|-------|------|-----------------|
| Unit | `#[cfg(test)]` | Per-module logic |
| Property | `proptest` | Invariants under fuzzed inputs (e.g., MVCC encode/decode roundtrip, range split correctness) |
| **Deterministic simulation** | [`madsim`](https://github.com/madsim-rs/madsim) | The heart of distributed testing. Drop-in tokio replacement that controls time, network, disk; runs full cluster scenarios deterministically. Used by RisingWave for the same purpose. |
| Multi-node integration | Docker Compose | Real binaries, real network, scripted scenarios |
| Chaos | `tc`/`iptables`/`stress-ng` | Network partitions, packet loss, disk slowness, memory pressure |
| **Jepsen-style consistency** | custom + `elle` for analysis | Linearizability, serializability under partitions and clock skew |
| Benchmarks | YCSB-like + custom | Throughput, latency, vs. CockroachDB / TiKV |

### 10.2 madsim: the secret weapon

Deterministic simulation lets us reproduce a 7-node cluster with random
network partitions, message reordering, disk failures, and clock skew —
all in a single-process, single-thread test that runs in milliseconds.
Bugs found in a millions-of-iterations simulation that would take hours
or days to surface in real distributed tests.

This is the same approach FoundationDB famously used.

### 10.3 Coverage targets

- ≥ 80 % line coverage across all crates.
- madsim: ≥ 1000 randomized scenarios per CI run.
- Jepsen consistency tests: pass with no anomalies under partitions and
  ≤ 200 ms simulated clock skew.

---

## 11. Repository Layout (Cargo workspace)

The single-crate repo becomes a workspace:

```
aresadb/
├── Cargo.toml                   # workspace root
├── crates/
│   ├── aresadb-core/            # Storage trait, key encoding, value types
│   │                            #   (extracted from src/storage/)
│   ├── aresadb-engine-redb/     # redb backend (current LocalStorage)
│   ├── aresadb-engine-mem/      # In-memory backend for tests
│   ├── aresadb-engine-lsm/      # Thread-per-core LSM (Phase 5)
│   ├── aresadb-query/           # SQL parser + planner + executor
│   ├── aresadb-raft/            # openraft integration, log/state machine
│   ├── aresadb-pd/              # Placement Driver
│   ├── aresadb-net/             # gRPC schemas, transport
│   ├── aresadb-cluster/         # Membership, gossip, scheduler
│   ├── aresadb-server/          # Public TCP server (existing, refactored)
│   ├── aresadb-client/          # Rust client
│   ├── aresadb-cli/             # CLI binary (`aresadb`, `aresadb-admin`)
│   ├── aresadb-py/              # PyO3 bindings (existing python/, moved)
│   └── aresadb/                 # Umbrella facade crate (re-exports)
├── tests/
│   ├── integration/             # Per-crate integration tests
│   ├── chaos/                   # Docker Compose chaos
│   ├── jepsen/                  # Jepsen-style consistency
│   └── bench/                   # YCSB-like + custom
├── benchmarks/
│   └── run_benchmarks.rs        # Existing single-node benchmark suite
└── docs/
    ├── architecture-v2.md       # this doc
    ├── consistency-model.md     # detailed semantics
    ├── operations.md            # operator guide
    └── benchmarks.md
```

Public API stability: the umbrella `aresadb` crate keeps the same
re-exports as today. Existing users of `use aresadb::Database;`
shouldn't have to change anything.

---

## 12. Phased Roadmap

Each phase ends in a tagged release and a public benchmark.

### Phase 0 — Foundations (2 weeks)

- This RFC ratified.
- Cargo workspace migration (move `src/` → `crates/aresadb-core/`,
  `crates/aresadb-engine-redb/`, etc.).
- `StorageBackend` trait extracted; `redb` backend implements it.
- madsim test harness skeleton.
- CI runs the workspace.
- **No behavior change.** v1 functionality preserved verbatim.
- Tag: `v2.0.0-alpha.0`

### Phase 1 — Single-shard cluster (4-6 weeks)

- openraft integration (using the existing redb backend as the state
  machine).
- gRPC inter-node transport.
- Cluster bootstrap, join, leave.
- Single Raft group replicating one logical "database" across N nodes.
- The existing WAL (`distributed::wal`) is replaced by the Raft log.
- 3-node Docker Compose tests.
- Linearizability check via Jepsen-lite scenarios.
- **Milestone:** 3-node cluster, RF=3, all writes Raft-replicated, p99
  write < 10 ms in single-DC.
- Tag: `v2.0.0-alpha.1`

### Phase 2 — Multi-Raft + range sharding (6-8 weeks)

- Unified key encoding for all data types (Section 3.1).
- Range descriptor types + PD catalog state machine.
- PD as a 3-node Raft group.
- Range split / merge logic.
- Multi-Raft scheduler — many groups per node, fair scheduling.
- Range rebalancing across nodes (size + load + zone constraints).
- Range leader leases.
- **Milestone:** 1 K+ ranges across 5+ nodes, auto-split under load,
  rebalancing on node addition.
- Tag: `v2.0.0-alpha.2`

### Phase 3 — Distributed query (4-6 weeks)

- Query router on the gateway: parse → distribute.
- Filter, projection, limit push-down.
- Scatter-gather executor.
- Distributed graph BFS (frontier batching).
- Distributed vector search (per-range HNSW partitions, broadcast +
  global top-k merge).
- Distributed full-text (per-range inverted index, cluster-wide DF
  cache).
- All v1 query types pass against a 5-node cluster.
- **Milestone:** all current tests pass on a 5-node cluster.
- Tag: `v2.0.0-beta.0`

### Phase 4 — Distributed transactions (6-8 weeks)

- HLC clocks at every node.
- MVCC value layer (versioned keys, intent records, GC).
- Single-shard transactions.
- Cross-shard parallel commit.
- Serializable Snapshot Isolation with timestamp cache.
- Read Committed mode for compatibility.
- Jepsen consistency tests pass.
- **Milestone:** SSI across shards, 50 K+ TPS in a 5-node cluster, no
  Jepsen anomalies under partitions.
- Tag: `v2.0.0-beta.1`

### Phase 5 — Performance + LSM engine (4-6 weeks)

- Thread-per-core LSM engine (`aresadb-engine-lsm`) with glommio + io_uring.
- Optional cluster mode flag `--engine=lsm`.
- Benchmark harness vs. CockroachDB, TiKV (YCSB A/B/C/F).
- Public benchmark report.
- Optional QUIC inter-node transport.
- Operator guide, troubleshooting playbook.
- **Milestone:** v2.0 RC. ≥ 200 K writes/sec/node on LSM backend; ≥ 1 M
  reads/sec/node from cache.
- Tag: `v2.0.0-rc.0` → `v2.0.0`

### Phase 6 — Change feeds + online schema (6-8 weeks)

- **Logical CDC / change feeds**: per-range feed of committed value deltas,
  exposed via a streaming API (`SUBSCRIBE TO …`). Implementation: the Raft
  state machine emits `ChangeEvent` on apply; a per-node dispatcher fans
  out to subscribers over the gateway.
- **Online schema changes for distributed indexes**: add/drop secondary
  and full-text indexes without blocking writes. Implemented via the
  CRDB-style "schema versions + backfill" protocol (DELETE_ONLY →
  WRITE_ONLY → PUBLIC states, each version coordinated through the PD).
- **Continuous incremental backup**: tail Raft logs to object storage,
  allow point-in-time restore within the MVCC GC window.
- **Milestone:** streaming changefeeds stable; indexes can be added/dropped
  on a live cluster without downtime.
- Tag: `v2.0.0` (GA)

### Phase 7 — Deferred to v3

- Multi-region / global consistency (TrueTime-style).
- Active-active conflict resolution.
- Geo-distributed read replicas.

### Timeline summary

| Phase | Duration | Cumulative |
|-------|----------|-----------|
| 0 — Foundations | 2 wk | 2 wk |
| 1 — Single-shard cluster | 4-6 wk | 6-8 wk |
| 2 — Multi-Raft + sharding | 6-8 wk | 12-16 wk |
| 3 — Distributed query | 4-6 wk | 16-22 wk |
| 4 — Distributed transactions | 6-8 wk | 22-30 wk |
| 5 — Performance / LSM-TPC | 4-6 wk | 26-36 wk |
| 6 — CDC + online schema | 6-8 wk | **32-44 wk** |

**Solo, focused: 8-10 months to v2.0 GA.** Faster with a contributor on
testing/benchmarks. Each phase ends in a release tag (`v2.0.0-alpha.N`,
`v2.0.0-beta.N`, `v2.0.0-rc.N`, `v2.0.0`). Worst case: we ship `v2.0.0`
from an earlier phase and push the remaining phases into `v2.1.0`.

---

## 13. Performance Targets

These are the numbers v2.0 commits to. Each is an explicit benchmark
run by the public `benchmark_suite` example.

### 13.1 Single-node (embedded, redb backend) — must equal or beat v1

| Metric | v1 (measured) | v2.0 target | v2.0 with LSM |
|--------|---------------|-------------|---------------|
| Batch insert | 37 720 nodes/s | ≥ 37 720 | ≥ 100 000 |
| Point lookup mean | 4.9 µs | ≤ 5.0 µs | ≤ 2.0 µs |
| Index-only lookup | 0.04 µs | ≤ 0.04 µs | ≤ 0.04 µs |
| Graph traversal d=2 | 92 µs | ≤ 100 µs | ≤ 60 µs |
| HNSW top-10 (10 K × 128D) | 6.5 µs | ≤ 7.0 µs | ≤ 6.5 µs |
| Indexed SQL | 2.1 ms | ≤ 2.1 ms | ≤ 1.5 ms |

### 13.2 3-node cluster (single DC, RF=3, redb backend)

| Metric | Target |
|--------|--------|
| Single-key write p50 | ≤ 2 ms |
| Single-key write p99 | ≤ 10 ms |
| Single-key read (leader lease) p50 | ≤ 0.5 ms |
| Single-key read p99 | ≤ 3 ms |
| Sustained write throughput (cluster) | ≥ 50 K ops/s |
| Sustained read throughput (cluster) | ≥ 300 K ops/s |
| Cross-shard transaction p99 | ≤ 25 ms |

### 13.3 Comparison targets

We will publish YCSB benchmarks vs.:

- **CockroachDB** — same consistency model, fair fight on OLTP.
- **TiKV (without TiDB)** — same Multi-Raft architecture.
- **ScyllaDB** — for KV workloads (eventual consistency, faster path).
- **Redis Cluster** — for KV workload, in-memory baseline.

Aspiration: tied or better on KV vs. CockroachDB/TiKV, behind ScyllaDB
on raw KV until LSM engine ships, then competitive.

---

## 14. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Distributed transactions are subtle, easy to ship buggy | High | Severe | madsim simulation + Jepsen tests gate Phase 4 release |
| openraft has a sharp edge we hit late | Medium | Moderate | Engage with their maintainers early; have a fallback plan to fork |
| LSM engine is a 6-month project disguised as 6 weeks | High | Moderate | Phase 5 is optional — v2.0 ships on redb backend if LSM slips |
| Performance targets miss | Medium | Moderate | Public targets + reproducible benchmarks force honesty |
| Solo bandwidth runs out | Medium | Severe | Each phase is independently releasable (alpha tags). Worst case: v2.0 ships at end of Phase 3 (replicated multi-model, no cross-shard txns) — still a huge step |
| Scope creep into multi-region / time-travel | Medium | Severe | Section 1.2 non-goals are sacred. New ideas → v3 backlog |

---

## 15. Ratified Decisions

These are the architectural decisions locked in before Phase 0 began.
Changing any of them requires a new RFC.

### 15.1 Scope for v2.0 = **Phases 0-6**

Full distributed multi-model database with ACID across shards, thread-per-core
LSM engine for headline performance, change feeds, and online schema changes.
~8-10 months solo focused work. Each phase ends in a release tag.

### 15.2 Replication library = **openraft**

Proven, async-first, modular, used in production by Databend / RisingWave /
Greptime. Rolling our own Raft is a multi-month project on its own; openraft
lets us focus on the layers above it.

### 15.3 Storage engines = **dual-backend**

| Backend | Role | Status at v2.0 |
|---------|------|----------------|
| `redb` | Embedded default, test backend, simplicity | Stable, carried from v1 |
| `lsm` (on `fjall`) | Cluster-mode default, write-heavy workloads | Phase 2 |
| `lsm-tpc` (custom thread-per-core LSM) | Performance stretch — headline benchmark numbers | Phase 5 |

This keeps AresaDB **100 % pure Rust** (no RocksDB C++ dependency), preserves
the embedded story, and gives us a realistic path to ScyllaDB-class
performance in the custom engine without blocking cluster correctness on it.

`redb` is the default backend for all tests in Phases 0-1. Cluster mode
defaults to `fjall`-backed LSM starting Phase 2. The custom thread-per-core
engine is opt-in via `--engine=lsm-tpc` when it ships.

### 15.4 Async runtime = **tokio + `tokio-uring` + thread-per-core LSM**

| Layer | Runtime |
|-------|---------|
| Control plane, gateway, gRPC, openraft | tokio (standard work-stealing) |
| Storage-engine file I/O (Linux) | `tokio-uring` (io_uring via tokio) |
| Storage-engine hot path (Phase 5 custom LSM) | Thread-per-core — one `tokio::runtime::Builder::new_current_thread()` per pinned thread, no cross-core sync |
| Non-Linux fallback | Standard tokio async fs |

This is the pragmatic path to top-tier throughput: we don't leave the
mainstream Rust ecosystem (openraft, tonic, the entire async story is
tokio-based), but we do get io_uring for the disk hot path and
shared-nothing per-core execution for the LSM's busiest code.

### 15.5 Workload optimization priority = **balanced, OLTP-latency-first**

Primary headline metric: **single-key read p50 on a 3-node cluster**
(target ≤ 500 µs). Other models (graph / vector / FTS) are held to
v1-level or better absolute latencies, but we do not chase being "fastest
in the world" for any single specialized model — AresaDB's differentiator
is the multi-model story.

Secondary metrics in v2.0 public benchmark suite:
- Write throughput cluster-wide (target ≥ 50 K ops/s in 3-node RF=3).
- Graph BFS p99 latency.
- Vector search recall/latency vs. embedded v1.
- Full-text search latency.

---

*This document is a living spec. Changes go through PR review and bump the
`version` line in the front matter.*
