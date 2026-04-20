# aresadb-engine-redb

redb-backed implementation of `aresadb_core::StorageBackend`. This is
the default durable engine for AresaDB v2 — Raft log entries and state
machine data both live on an instance of this backend.

## Why redb

* Pure Rust, zero-syscall hot path, ACID by default.
* Single-writer / many-reader MVCC — perfect match for openraft's
  append-heavy log workload (one writer = the Raft task) and the
  read-mostly state machine.
* Stable on-disk format (v2+), so recovery semantics are clear.
* We already depend on it from the v1 tree.

## Layout

Each backend opens one `redb::Database` file and exposes a single table
named `default`. Applications that need to partition the keyspace (e.g.
log entries vs. metadata in `aresadb-raft`) do so by prefixing the keys
themselves — that keeps the engine trait simple and engine-agnostic.

## Usage

```rust,ignore
use std::sync::Arc;
use aresadb_core::StorageBackend;
use aresadb_engine_redb::RedbBackend;

# tokio_test::block_on(async {
let dir = tempfile::tempdir()?;
let backend: Arc<dyn StorageBackend> =
    RedbBackend::open(dir.path().join("state.redb")).await?;
backend.write_batch(/* ... */).await?;
# Ok::<_, Box<dyn std::error::Error>>(())
# });
```

## Durability model

Every `write_batch` runs inside a redb write transaction, and
`flush()` commits it. The backend currently does *not* batch commits
across calls — Raft needs one fsync per log-append to be correct, and
batching would hide a bug during replay. Phase 2 adds an explicit
group-commit mode for higher write throughput.
