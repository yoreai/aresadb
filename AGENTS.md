# Agent Instructions

Instructions for AI coding agents working on AresaDB.

## Source-of-truth files

- **`CHANGELOG.md`** — released-version history (Keep a Changelog format)
- **`docs/phase-status.md`** — v2 implementation phase tracker + decision log
- **`docs/architecture-v2.md`** — v2 distributed architecture spec
- **`docs/operations.md`** — operator runbook
- **`README.md` Roadmap section** — public-facing direction
- **`../aresadb.md`** (in the local `yoreai/` workspace, not in this repo)
  — cross-repo open-work plan and next-move recommendations

There is intentionally **no `TODO.md`**. Released history → `CHANGELOG.md`.
Implementation status → `docs/phase-status.md`. Forward direction →
README Roadmap + the cross-repo `../aresadb.md`.

## After every code change

1. **Update `CHANGELOG.md`** — add entry under `[Unreleased]`.
2. **Update or add tests** — every changed behavior needs a test.
3. **Run `cargo check`** before committing (default features only, not
   `--all-features`).
4. **Update `docs/phase-status.md`** if the change advances or closes a
   v2 phase checkpoint.
5. **Update the README Roadmap** if a phase ships or a new phase opens.

## Before a version release

1. Bump `version` in both `Cargo.toml` and `python/pyproject.toml`.
2. Move `[Unreleased]` entries in `CHANGELOG.md` to a new version section.
3. Update compare links at the bottom of `CHANGELOG.md`.
4. Update `org.opencontainers.image.version` in `Dockerfile`.
5. Commit, then `git tag vX.Y.Z && git push origin vX.Y.Z`.

## Architecture quick reference

- **Storage engine**: `src/storage/` — redb-backed, `Database` type is `Clone` (Arc internals)
- **Tiered storage**: `src/storage/tiered.rs` — `TieredStorage` wraps local+cache+bucket; graph index always local, payloads tiered
- **Key types**: `NodeIndex` (lightweight local index), `PayloadLocation` (Local/Cloud), `TieredConfig`
- **Tables**: `NODE_INDEX_TABLE` + `NODE_PAYLOADS_TABLE` (tiered), `NODES_TABLE` (legacy compat), `EDGES_TABLE`
- **Read path**: cache → local payload → cloud bucket (transparent)
- **Write path**: local index + payload → optional write-through to cloud
- **SQL queries**: `QueryEngine::new(db.clone()).execute_sql(sql, limit)` — parser → planner → executor
- **Python bindings**: `python/src/lib.rs` — PyO3 0.22, module name `aresadb_python`
- **CI**: default features only — `server` feature has stub handlers, do not use `--all-features`
- **Release CI**: tag `v*.*.*` auto-publishes to crates.io, PyPI, and GHCR

## Known issues

- `query/executor.rs` ~line 145: `IndexLookup` falls back to full scan (TODO)
- `server/handler.rs`: all operations (insert, get, update, delete, query, traverse, edges, status, transactions) are fully implemented
- `storage/bucket.rs`: no unit tests for S3/GCS paths
- `benches/` files have formatting issues (`cargo fmt` will flag them)
