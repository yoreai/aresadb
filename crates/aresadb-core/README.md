# aresadb-core

Core types and traits for the AresaDB v2 distributed architecture.

This crate defines the contract that every storage backend must implement
(`StorageBackend`), along with the supporting types (`WriteBatch`,
`KeyRange`, `Snapshot`, `KeyValueStream`) that the higher-level database
engine uses to talk to storage.

The crate is deliberately small and has no redb / LSM / network
dependencies. That lets each concrete backend live in its own crate
(`aresadb-engine-redb`, `aresadb-engine-lsm`, `aresadb-engine-lsm-tpc`)
and be composed in or out.

See [../../docs/architecture-v2.md](../../docs/architecture-v2.md) for
the full design.

## Status

Phase 0 — trait surface only. Concrete backends will move here in later
phases as we extract them from the main `aresadb` crate.
