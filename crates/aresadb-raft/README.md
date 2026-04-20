# aresadb-raft

Raft consensus integration for AresaDB v2.

This crate wires [openraft](https://docs.rs/openraft) onto the
[`StorageBackend`] trait defined in `aresadb-core`. One Raft group
replicates exactly one logical shard. Higher layers (multi-raft
scheduler, range sharder) instantiate many instances of this crate —
one per range — in Phase 2.

## What lives here

- `TypeConfig` / `NodeId` — the openraft type config used by AresaDB.
- `AresaCommand` — the replicated command (a serialized `WriteBatch`).
- `LogStore<B>` — `RaftLogStorage` + `RaftLogReader` over a
  `StorageBackend` keyspace prefix.
- `StateMachineStore<B>` — `RaftStateMachine` over a separate
  `StorageBackend` (the user data).
- `AresaRaft<B>` — a ready-to-use wrapper that boots an openraft
  instance with both stores bound to the same network.

## Design choices

- **Two backends per group.** The Raft log and the state machine live
  in separate `StorageBackend` instances. That keeps the log hot and
  lets us later swap in a log-optimized engine (fsync-heavy,
  sequential writes) independently of the data engine (LSM / sorted-run
  friendly).
- **Commands are `WriteBatch`es.** The state machine's `apply` is just
  `backend.write_batch(...)`, which keeps the trait surface small.
  Higher layers that need richer commands (schema changes, DDL) will
  extend `AresaCommand` — not the state machine boundary.
- **Object-safe backends.** We store `Arc<dyn StorageBackend>` so the
  same code path works for `MemoryBackend` (tests / sim), the redb
  wrapper (Phase 1), and the fjall/TPC LSM (Phase 2/5).
- **openraft 0.9's storage-v2 split traits.** We never implement the
  deprecated unified `RaftStorage` — only `RaftLogStorage` +
  `RaftStateMachine`, which is what openraft itself recommends.

See `docs/architecture-v2.md` in the repository root for the big-picture
design.
