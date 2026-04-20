# aresadb-engine-lsm

fjall-backed implementation of `aresadb_core::StorageBackend`. The
**write-heavy** engine for AresaDB v2 — intended for hot ranges that
see sustained ingest traffic, while `aresadb-engine-redb` remains the
default for small / embedded deployments and the metadata stores
(Raft log, PD catalog).

## Why fjall

- LSM-tree (memtable → levelled SSTables, RocksDB-shaped on disk) sized
  for the write workload AresaDB's data ranges produce after Phase 2c —
  bounded per-write latency, cheap range scans, and per-block LZ4 on
  disk.
- Pure Rust, 100% safe, no RocksDB C++ dep.
- Cheap `Clone` of `Database` / `Keyspace` handles so we can share one
  fjall handle across many `spawn_blocking` calls without reopening.
- Crash-safe by default (journal + controlled flushes); durability is
  opt-in via `PersistMode::SyncAll` on each batch commit.

## Layout

Each backend opens one `fjall::Database` rooted at a caller-supplied
directory and uses a single keyspace named `default`. Higher layers
that need a sub-keyspace (Raft log vs state-machine data, per-range
namespacing, etc.) prefix keys themselves — mirrors how
`aresadb-engine-redb` works, so the two engines are behaviourally
equivalent from the trait's perspective.

## Durability model

- `write_batch` runs inside `fjall::OwnedWriteBatch::commit()` (atomic
  cross-keyspace commit) and then calls `Database::persist(SyncAll)`
  so the journal is fsynced before we return `Ok(())`.
- `flush()` is a separate `persist(SyncAll)` for callers that buffered
  writes via some other path. Redundant after a fsync'd batch commit
  but kept to honour the trait.
- Dropping the `Database` handle triggers a best-effort sync as well,
  so process exit without an explicit `close()` still preserves
  committed data.

## Usage

```rust,ignore
use std::sync::Arc;
use aresadb_core::StorageBackend;
use aresadb_engine_lsm::FjallBackend;

# tokio_test::block_on(async {
let dir = tempfile::tempdir()?;
let backend: Arc<dyn StorageBackend> =
    FjallBackend::open(dir.path().join("state.lsm")).await?;
backend.write_batch(/* ... */).await?;
# Ok::<_, Box<dyn std::error::Error>>(())
# });
```

## Why not the default engine?

- Range-delete is O(N) in the range today: fjall doesn't expose a
  cheap range-tombstone that lets us delete a whole segment in one
  shot, so we materialise keys and remove them one-by-one (same as the
  redb backend). Fine for Raft log purges; would be a bottleneck for
  a "drop tenant" admin.
- Snapshots eagerly materialise the keyspace into memory. This is
  identical to the redb backend's behaviour and is OK for the places
  `Snapshot` is used today (Raft bootstrap, admin commands). A Phase 4
  streaming-snapshot rework will clean this up once the query engine
  actually needs it.
- `approximate_size` returns 0. We could plumb
  `Keyspace::disk_space` through, but the range sharder in Phase 2
  treats this as an advisory hint and has a fallback, so we keep the
  impl honest and return the "no estimate" value until we have
  per-range metadata.

redb wins for the small-deployment / single-node / metadata use case
because it's simpler, single-file, and has one fsync-per-commit
semantics out of the box. fjall wins once ranges are big and writes
are hot.
