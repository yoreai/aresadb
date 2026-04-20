# Changelog

All notable changes to AresaDB will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added (publishing-audit follow-ups)

- **`benches/v2_cluster_bench.rs`**: Criterion scaffold for the v2
  distributed stack. Tracks today: `v2/raft/apply_single_node/put_one`,
  `v2/raft/apply_single_node/put_batched/{16,128}` (openraft
  `SingleNode::in_memory()` end-to-end put throughput + batch
  amortisation curve), `v2/engine/backend/put`,
  `v2/engine/backend/put_batched/64`, `v2/engine/backend/get_warm`, and
  `v2/engine/backend/scan_range` (all on-disk, `RedbBackend` vs.
  `FjallBackend` side-by-side). Registered under `[[bench]] name =
  "v2_cluster_bench"` alongside the legacy `distributed_bench`;
  `aresadb-core`, `aresadb-raft`, `aresadb-engine-redb`, and
  `aresadb-engine-lsm` are now root-crate dev-deps so the bench wires
  into the v2 crates without duplicating code. Smoke numbers + batch-
  size amortisation takeaways captured in
  [`docs/publishing-audit.md`](docs/publishing-audit.md) §4a.
- **`docs/publishing-audit.md`**: new audit document that reconciles the
  v1 embedded paper (still authoritative for the single-node engine) with
  the v2 alpha cluster (own companion tech-note planned); captures the
  2026-04-20 re-run of the v1 `benchmark_suite` example against the
  alpha.2 workspace, calls out the v2 bench suite plan, the v2 note
  outline, and the v1 Zenodo upload still to ship.
- **`docs/operations.md`**: operator runbook — components, data-dir
  layout, bootstrap flows (Docker Compose + bare-metal), everyday
  operator flows (status / I/O / membership / range admin), failure
  injection + recovery, upgrade paths, observability, and a file-map
  of where everything lives in the repo.
- **`docs/release-notes/v2.0.0-alpha.2.md`**: standalone GitHub-shaped
  release notes (headline, what's new, benchmarks, breaking changes,
  known limitations, upgrade guide, artefacts). Used verbatim as the
  body for `gh release create v2.0.0-alpha.2`.
- **`.github/workflows/docker-smoke.yml`**: new nightly + on-demand +
  path-filtered workflow that builds the `aresadb-cluster` Docker
  image, brings up the 3-node compose stack, and exercises both
  `docker/cluster/bootstrap.sh` (default-range smoke) and
  `docker/cluster/multi-range.sh` (range-aware smoke) end-to-end.
  Dumps per-node logs on failure and tears volumes down afterwards.
- **`paper/README.md`**: scoped the AresaDB technical report explicitly
  to the v1 embedded engine; added pointers to
  [`BENCHMARKS.md`](BENCHMARKS.md), [`benches/v2_cluster_bench.rs`](benches/v2_cluster_bench.rs),
  and [`docs/publishing-audit.md`](docs/publishing-audit.md) so readers
  see where the v2 story lives.
- **`BENCHMARKS.md`**: refreshed with v2.0.0-alpha.2 numbers + history
  row + a "Scope" callout noting the doc covers the embedded engine and
  pointing downstream to the v2 bench scaffold.
- **`benchmarks/results-2026-04-20-alpha2.json`**: archived alpha.2
  re-run of the v1 `benchmark_suite` example (~75K nodes/sec batch
  insert, 5 µs p50 point lookup, 7 µs HNSW, 23× B-tree index speedup).
- **`genass/publications/quarto/aresadb_v2_note/`**: scaffold for the
  v2 distributed companion tech note (separate Quarto project,
  `CITATION.cff`, `zenodo.json`, `AGENTS.md`, chapters 1-5). The
  0.1 scaffold is outline-grade by design — chapters 1 / 4 / 5
  (scope / limitations / reproducibility) are close to publication
  quality; chapters 2 / 3 (architecture / evaluation) populate once
  the sized v2 bench suite lands.

### Changed (publishing-audit follow-ups)

- **`.github/workflows/ci.yml`**: added a `benches` job that runs
  `cargo check --workspace --benches`. Catches breakage in
  `v2_cluster_bench.rs` (and the legacy bench targets) that the
  existing `cargo test` + `cargo clippy --workspace` jobs don't see
  because benches are opt-in targets.
- **`benches/distributed_bench.rs`**: annotated with an explicit scope
  callout — this bench measures the v0.2-era utility module
  (`BloomFilter`, `Compressor`, `ShardManager`) and is NOT on the v2
  distributed data path. Numbers here don't reflect v2 throughput or
  latency; readers are pointed at `v2_cluster_bench.rs` and
  `docs/publishing-audit.md` §4a.
- **`CITATION.cff` + `zenodo.json`**
  (`genass/publications/quarto/aresadb_technical_report/`): bumped
  `date-released` / `publication_date` to `2026-04-20`, added explicit
  `version: "1.0"`, refreshed abstracts with the re-run numbers, noted
  the v2 alpha is a sibling publication rather than a new version of
  the v1 deposit.
- **`genass/publications/quarto/aresadb_technical_report/8_conclusion.qmd`**:
  light-touch §Limitations + §Future Directions edit that acknowledges
  the `v2.0.0-alpha.2` multi-Raft cluster, keeps "not yet
  production-ready" as the honest caveat, and points at the v2
  companion tech note for the distributed architecture. No figure, no
  table, no headline number changed.
- **`yev/apps/aresalab/lib/publications.ts`**: AresaDB entry date
  bumped to `2026-04`, metrics block synced to the alpha.2 `headline.json`
  (23× index speedup, 5 µs / 75K / 7 µs), abstract gained a one-line
  v2 alpha cross-reference, keyword list gained `"Multi-Raft (v2
  alpha)"`. Card remains authoritative for the **v1 embedded** paper.

### Fixed (v2.0.0-alpha.2 release pipeline follow-ups)

Post-tag follow-ups surfaced by the first real tag-driven release
run on 2026-04-20. Everything in this block is back-compatible and
stays at the `2.0.0-alpha.2` version number — crates.io + PyPI were
already published successfully by the tag, so no version bump.

- **rustdoc intra-doc links** across `aresadb-pd`, `aresadb-cluster`,
  `aresadb-raft`, and `aresadb-sim`: fully qualify `PD_RAFT_META_KEY`
  and `ReadError::*`, fix `PdResponse::Error` (tuple fields aren't
  allowed), use `super::PdLogStore` / `super::reconciler::*` paths,
  replace redundant explicit link targets, and demote the link to a
  private `MultiRangeApplyDeterminism::run_on_new_nodes` helper to
  prose. Unblocks the `Docs` CI job under `-D warnings`.
- **clippy `manual_is_multiple_of`**: `src/cli/repl.rs` used
  `quotes % 2 != 0`; switched to `!quotes.is_multiple_of(2)` for
  Rust 1.95's newly stable method + lint.
- **`python/Cargo.lock` + root `Cargo.lock` tracked.** Both lockfiles
  are now under version control so the PyO3 wheel build and the v2
  cluster Docker image resolve deps deterministically across CI and
  local builds. `.gitignore` gained an explanatory comment covering
  both.
- **`docker/cluster/Dockerfile` + root `Dockerfile` realignment.**
  The cargo-chef-style warmup phase was copying / stubbing an out-
  of-date workspace member list; added `aresadb-engine-lsm` and
  `aresadb-pd` Cargo.tomls + stub sources, plus stubs for every
  root-crate `[[bin]]`, `[[bench]]`, and `[[example]]` target so
  `cargo fetch --locked` actually parses the manifest. Base image
  bumped `1.85-slim-bookworm` → `1.90-slim-bookworm` to satisfy
  `fjall`'s MSRV, and the workspace `rust-version` was raised to
  match (it was stale at `1.85`). Runtime stage now pre-creates
  `/var/lib/aresadb/data` with `aresadb:aresadb` ownership so
  docker-compose named volumes don't fall back to root and produce
  `Permission denied (os error 13)` at startup.
- **`.github/workflows/release.yml`**: added `workflow_dispatch`
  (`version` + `docker_only` inputs) so the Docker image publish
  can be re-run against a specific version without re-hitting
  crates.io / PyPI (both already accepted the tag). The Docker job
  itself now targets `docker/cluster/Dockerfile` explicitly and
  publishes to `ghcr.io/yoreai/aresadb/cluster:<version>` (the
  legacy `ghcr.io/yoreai/aresadb` image stays reserved for the v1
  embedded CLI).
- **`docs/release-notes/v2.0.0-alpha.2.md`**: image path updated to
  `ghcr.io/yoreai/aresadb/cluster` in both the "Breaking changes"
  and "Artefacts" sections.

### In progress
- **Phase 3 — distributed query.** Query router + physical
  planner with range awareness, filter / projection / limit
  push-down to data nodes, and a scatter-gather executor. See
  [`docs/architecture-v2.md`](docs/architecture-v2.md) §5 and
  [`docs/phase-status.md`](docs/phase-status.md).
- **v2 companion tech-note — 1.0 populate.** The 0.1 scaffold in
  `genass/publications/quarto/aresadb_v2_note/` lands here; chapters
  2 (architecture) and 3 (evaluation) need to fill in before upload.
  Blocks on the sized v2 benchmark suite — tracked in
  [`docs/publishing-audit.md`](docs/publishing-audit.md) §4a / §4b.
- **v1 Zenodo upload.** One-shot manual deposit using the refreshed
  `CITATION.cff` + `metrics.json`. Tracked in
  [`aresalab.md`](../aresalab.md) Phase 2 and
  [`docs/publishing-audit.md`](docs/publishing-audit.md) §4c.

---

## [2.0.0-alpha.2] - 2026-04-11

Ships the full Phase 2 arc: **multi-Raft + range-sharded cluster**,
a **replicated placement driver**, **range leader leases** + a
range-aware data plane, **multi-range determinism coverage**, a
3-node Docker Compose smoke for the range-aware admin surface, and
an **opt-in fjall-backed LSM data engine** alongside the default
redb. Every v2-lane (2a keyspace, 2b PD, 2c range-aware cluster, 2d
LSM) is now on disk, tested, and `clippy -D warnings` clean.

The individual sub-phase entries that follow document the
incremental work that landed under this tag — each one shipped as
an internal checkpoint against [`docs/phase-status.md`](docs/phase-status.md)
and can be read independently.

### Added (Phase 2a — unified keyspace)

- **`aresadb-core::keys`**: canonical encoder / decoder for the
  ratified v2 keyspace layout (§3.1 of `architecture-v2.md`). One
  sorted byte-keyspace for every model — `/n/`, `/i/`, `/e/`, `/ef/`,
  `/et/`, `/p/`, `/ft/`, `/v/`, `/s/`, `/x/`, `/m/`. Variable-length
  segments use CRDB-style escape encoding (`0x00` → `0x00 0xff`,
  `0x00 0x01` terminator), the last segment is written raw so prefix
  scans match naturally.
- `Key` enum with 11 structured variants + `encode` / `decode` that
  round-trip. `DecodeError` distinguishes too-short input, unknown
  prefix, missing terminator, and malformed escape sequences.
- Range-scan prefix helpers: `node_prefix`, `edge_prefix`,
  `edge_from_prefix`, `edge_to_prefix`, `property_field_prefix`,
  `property_equality_prefix`, `fulltext_token_prefix`,
  `vector_field_prefix`.
- 23 unit tests cover round-trip of every variant, sort order
  (property index sorts by value then node id; edge-by-from sorts
  by source then edge id), prefix-range containment, escape
  correctness (embedded 0x00 bytes survive round-trip), and negative
  paths (too-short / unknown-prefix / missing-terminator /
  malformed-escape).

Nothing in Phase 1 consumes this module yet; it is the canonical
contract for Phase 2b (placement-driver range descriptors) and Phase
2c (range-aware `ClusterNode` data path) to build against.

### Added (Phase 2b-1 — PD catalog core)

- **`aresadb-pd`**: new workspace crate. Owns the placement driver's
  view of *which range lives where*. Layered the same way
  `aresadb-raft` is — pure data + commands + pure-logic index — so
  Phase 2b-2 can drop a Raft state machine on top without touching
  the catalog's invariant logic.
- **Types** (`aresadb_pd::types`). `RangeDescriptor` matching the
  spec: `range_id`, half-open `[start_key, end_key)`, `replicas`,
  `raft_group_id`, `epoch` (bumps on membership change),
  `generation` (bumps on split/merge), optional `lease`. Plus
  `ReplicaPlacement`, `ReplicaRole`, `LeaseInfo`, and `NodeInfo`.
  Empty `end_key` denotes `+infinity`. Every type round-trips through
  `bincode`.
- **Commands** (`aresadb_pd::command`). Seven `PdCommand` variants —
  `RegisterNode`, `HeartbeatNode`, `CreateRange`, `SplitRange`,
  `MergeRanges`, `UpdateMembership`, `UpdateLease` — plus a matching
  `PdResponse`. `SplitRange` deliberately does *not* carry the new
  `range_id`; the catalog allocates it from a replicated counter at
  apply time so every Raft replica produces the same id.
- **Catalog** (`aresadb_pd::catalog::Catalog`). In-memory index over
  every range + node descriptor, with typed mutators
  (`create_range`, `split_range`, `merge_ranges`,
  `update_membership`, `update_lease`, `register_node`,
  `heartbeat_node`) and a single `apply(PdCommand)` entry point
  mirroring the Raft state-machine pattern. Secondary indices
  (`by_start`, `by_group`) keep overlap checks and key→range lookups
  at `O(log n)`.
- **Invariants enforced by the catalog.** No overlapping spans.
  Raft group ids unique across ranges. Epoch strictly monotonic per
  range. Split preserves total coverage (parent shrinks, RHS inherits
  replicas + epoch, both bump `generation`, both drop stale leases).
  Merge requires adjacent ranges with identical replica sets; right
  side dissolves into left. Heartbeats never regress.
- **Errors** (`aresadb_pd::error::CatalogError`). Ten variants, each
  mapped to exactly one rejection path:
  `RangeAlreadyExists`, `RangeNotFound`, `NodeNotRegistered`,
  `GroupIdInUse`, `OverlappingRange`, `InvalidSpan`,
  `SplitKeyOutOfBounds`, `EpochRegression`, `NotAdjacent`,
  `ReplicaSetMismatch`, `UnknownStore` (reserved for a later
  enforcement pass that validates placements against
  `NodeInfo.stores`).
- **Test coverage.** 48 unit tests across `types`, `command`, and
  `catalog`: serde / bincode round-trip for every variant; every
  mutator's success path; every rejection path through both the
  typed helper and the `apply()` dispatch; a stress test that splits
  six ranges in one catalog and walks them in start-key order to
  prove `by_start` stays in sync and coverage remains
  gap-and-overlap-free.

Phase 2b-2 (PD state machine + Raft persistence), 2b-3 (single-node
PD + 3-node PD cluster integration test), and 2b-4 (gRPC admin RPCs +
node heartbeats) will be layered on top without modifying this
crate's public surface.

### Added (Phase 2b-2 — persistent PD catalog)

- **`aresadb_pd::state_machine::PdStateMachine`**: persistent
  adapter binding a [`Catalog`](../aresadb-pd/src/catalog.rs) to any
  [`aresadb_core::StorageBackend`]. Every accepted `PdCommand`
  mutates the in-memory catalog and writes the touched rows to the
  backend in one atomic `WriteBatch`, then flushes.
- **`aresadb_pd::persist`**: on-disk key layout. Range rows at
  `/m/pd/r/<range_id_be:8>`, node rows at `/m/pd/n/<node_id_be:8>`,
  all under the unified-keyspace metadata prefix. `next_range_id` is
  **derived** on open from `max(range_id) + 1` rather than persisted
  separately, so partial-write splits never leave the counter
  lagging the live ranges.
- **Serialized applies.** A `tokio::sync::Mutex` guards the apply
  path so a catalog mutation + its derived backend write form one
  logical operation. Reads use a separate `parking_lot::RwLock` and
  never block on I/O.
- **Fatal-backend-error discipline.** `PdApplyError` distinguishes
  `Catalog` (recoverable — on-disk and in-memory are still
  consistent) from `Backend` (fatal — in-memory mutated but disk
  write failed; caller must discard the state machine, `open`
  against the same backend, and let the replicator re-deliver).
- **Recovery via `Catalog::load`**: reopening against a populated
  backend scans `/m/pd/` and re-seeds the catalog. The `load`
  constructor accepts already-consistent rows and skips invariant
  re-checking.
- **18 additional unit tests** (66 total in the crate): every
  command variant's persistence path, catalog-rejection-does-not-
  write safety, reopen-restores-catalog-state, range-id counter
  advances past restored max on reopen, and a full end-to-end
  round-trip through the durable `RedbBackend` — create a genesis
  range, split, install a lease, drop the state machine, reopen,
  verify every row survived.

With 2b-2 landed, the PD catalog is durable, serialized, and
unit-testable without Raft in the loop. 2b-3 will wrap the state
machine in an `openraft::RaftStateMachine<PdTypeConfig>` adapter and
introduce a TypeConfig-parameterized log store so multiple Raft
groups (PD + per-range data groups) can share the log-storage
implementation.

### Added (Phase 2b-3 — PD Raft group)

- **Multi-Raft log store.** `aresadb-raft::LogStore` is now a type
  alias for `LogStoreGeneric<C: RaftTypeConfig>`; the user-data
  group keeps the exact same surface (`pub type LogStore =
  LogStoreGeneric<TypeConfig>`) while the PD group reuses the same
  persistence logic over `PdTypeConfig`. The error-mapping helpers
  (`storage_err`, `storage_err_ctx`, `BincodeError`) were
  generalised over `N: openraft::NodeId` and made public so
  downstream crates don't duplicate the `StorageError<N>` wrapping
  boilerplate.
- **`aresadb_pd::raft::PdTypeConfig`.** `openraft::declare_raft_types!`
  binding over `PdCommand` / `PdResponse`, `NodeId = u64`, snapshot
  data = `Cursor<Vec<u8>>`. `PdLogStore = LogStoreGeneric<PdTypeConfig>`
  falls out for free.
- **`PdRaftStateMachine`.** Wraps `Arc<PdStateMachine>` and
  implements `RaftStateMachine<PdTypeConfig>` +
  `RaftSnapshotBuilder<PdTypeConfig>`. `apply` routes catalog
  mutations through `PdStateMachine::apply_with_meta` so
  `last_applied` / `last_membership` persist atomically at the
  reserved `b"\xff/pd/sm/meta"` row inside the same `WriteBatch`
  as the touched catalog rows. Raft blank entries and membership
  entries advance `last_applied` via `apply_meta_only`, without
  ever touching the catalog. Snapshots serialize the full range
  table + node table + `next_range_id` + Raft meta; `install_snapshot`
  wipes `/m/pd/r/*` + `/m/pd/n/*` in one batch via two
  `delete_range`s before re-hydrating.
- **Catalog-rejection semantics.** A command the catalog rejects
  (`RangeNotFound`, `SplitKeyOutOfBounds`, …) returns
  `PdResponse::Error(String)` *through* Raft. The state machine
  still advances `last_applied` — from the replicator's point of
  view the entry was delivered — so a crashed leader can't
  silently re-apply the rejected command.
- **In-process multi-node transport.** `PdRouter` +
  `PdRouterNetwork` deliver RPCs between PD Raft members as
  direct `openraft::Raft::{append_entries, vote, install_snapshot}`
  calls against the target's handle. Unregistered / partitioned
  peers surface as `openraft::error::Unreachable` so the replicator
  retries instead of hard-failing. `isolate(from, to)` and
  `reconnect(from, to)` toggle directed links for partition tests.
- **`SinglePdNode`.** One-voter harness mirroring
  `aresadb-raft::SingleNode`: bundles `PdLogStore`,
  `PdRaftStateMachine`, and a single-member-registered
  `PdRouterNetwork` into a fully-initialised
  `openraft::Raft<PdTypeConfig>`. `in_memory()` spins the whole
  thing up on fresh `MemoryBackend`s in one call.
- **`PdCluster`.** N-member in-process cluster on top of the same
  building blocks. `with_config` / `in_memory` bootstrap a fresh
  cluster (lowest-id node calls `initialize(all_members)`);
  `open_existing` skips `initialize` for crash-restart flows where
  the persisted log already contains a membership entry. Helpers
  include `wait_for_leader`, `wait_for_replication`, `catalog_snapshot`,
  `partition`/`heal`, and `restart(node_id)` which drops a member's
  Raft handle and reopens it on the same backends.
- **Test coverage (97 PD tests, all passing).** 20 new raft-module
  unit tests (PdTypeConfig round-trip, `PdRaftStateMachine` apply /
  membership / snapshot / install-snapshot / rehydrate, in-process
  router semantics, `SinglePdNode` create / split / lease /
  rejection / metrics / snapshot) + 5 new `PdCluster` unit tests
  (election, replication, ForwardToLeader, multi-step split
  convergence, single-member restart) + 6 new end-to-end
  integration tests under `crates/aresadb-pd/tests/cluster_integration.rs`:

  - `leader_failover_elects_new_leader_and_continues_applying` —
    kills the leader, asserts one of the two live voters takes
    over, drives a follow-up write through the new leader's
    handle, and verifies both survivors apply it.
  - `partition_isolates_leader_and_heals_cleanly` — isolates the
    leader from both followers (both directions), the remaining
    quorum elects a new leader, the new leader accepts writes,
    and when the partition heals every member converges on the
    new range count.
  - `full_cluster_restart_rehydrates_catalog_from_redb` — boots
    three redb-backed voters, applies four splits, shuts the
    whole cluster down + drops the router, reopens every backend
    with `open_existing`, and asserts the rehydrated catalog
    matches the pre-shutdown state on every member plus accepts
    new writes.
  - `follower_churn_under_load_converges` — 8 walk-right splits
    while restarting a follower every third step; final
    `range_count == 9` on every member.
  - `many_splits_converge_across_followers` — 50 walk-right splits;
    every follower's `iter_ranges()` list equals the leader's
    range-id-for-range-id.
  - `open_existing_rejects_fresh_backends` — sanity guard that
    a mis-used `open_existing` on empty memory backends times
    out on `wait_for_leader` instead of silently hanging.

Phase 2b-3 closes out the replicated placement-driver surface.
2b-4 adds the `pd.proto` admin RPCs + node heartbeats and wires
the `aresadb-cluster` CLI into the PD group; 2c then teaches
`ClusterNode` to manage many Raft groups per node with per-range
backends.

### Added (Phase 2b-4 — PD admin RPCs + heartbeats + CLI)

- **`pd.proto`** — control-plane gRPC surface for the placement
  driver (`crates/aresadb-pd/proto/pd.proto`, package
  `aresadb.pd.v1`). Strongly-typed messages (unlike the bincode-over-
  `bytes` payloads of `aresadb-net`'s Raft transport) so every
  language binding can drive the catalog without depending on the
  Rust crate. Fifteen RPCs: mutations (`RegisterNode`,
  `HeartbeatNode`, `CreateRange`, `SplitRange`, `MergeRanges`,
  `UpdateMembership`, `UpdateLease`) all funnel through the PD Raft
  log; reads (`GetRange`, `GetRangeByKey`, `ListRanges`, `GetNode`,
  `ListNodes`, `Status`) are served from this member's last-applied
  catalog.
- **`PdAdminService`** — tonic server adapter over
  `openraft::Raft<PdTypeConfig>` + `PdStateMachine`. Mutations run
  `Raft::client_write(cmd)` and fold the `PdResponse` back into a
  typed protobuf reply. Error mapping has three classes:
  - **`InvalidArgument`** for malformed requests (missing required
    field, both `lease` and `clear` set on an `UpdateLease`, …). These
    never hit the Raft log.
  - **`FailedPrecondition`** for catalog rejections (overlap,
    duplicate id, non-adjacent merge, epoch regression, …). The
    command *did* replicate through Raft but the state machine
    declined to apply it.
  - **`Unavailable`** for Raft errors (`ForwardToLeader`, shutdown,
    replication failure). `ForwardToLeader` attaches the suggested
    leader id in a custom `pd-leader-id` response trailer so callers
    retry in one hop rather than probing every member.
- **`PdAdminClient`** — typed Rust wrapper over the generated tonic
  client. Returns native Rust types (`RangeDescriptor`, `NodeInfo`,
  `serde_json::Value` for `status`) and maps every tonic `Status`
  into a `PdAdminClientError` with a dedicated `NotLeader(Option<id>)`
  variant so leader hints are first-class. `leader_hint()` accessor
  extracts the suggested id without pattern-matching.
- **`HeartbeatLoop`** — cancellation-safe background task that sends
  `HeartbeatNode` RPCs at a fixed cadence. `HeartbeatConfig` wires in
  an optional `EndpointResolver` so the loop rotates on
  `NotLeader` hints, a `ClockFn` for deterministic tests, and a
  configurable interval. `HeartbeatHandle` supports graceful
  shutdown via `.stop()` or drop; both are idempotent.
- **`aresadb-cluster` CLI — `pd` subcommand group** (15 subcommands)
  for end-to-end catalog control: `status`, `list-ranges`,
  `list-nodes`, `get-range`, `get-range-by-key`, `get-node`,
  `register`, `heartbeat`, `heartbeat-loop` (long-running, follows
  leader hints when `--peer ID=URL` is supplied), `create-range`,
  `split-range`, `merge-ranges`, `install-lease`, `clear-lease`,
  `update-membership`. Replica specs parse `node_id:store_id[:role]`
  (role defaults to `voter`). Keys are UTF-8 strings; empty
  `--end-key` means +∞. Leader hints surface in the error message
  as `"not leader; retry against node N"` instead of being swallowed.
- **36 new tests across two crates.**
  - **18 unit tests in `aresadb-pd::admin`**: 6 `convert` round-trips
    (including unspecified / unknown replica-role rejection), 6
    `client` error-mapping tests (Unavailable with and without
    leader-hint metadata, FailedPrecondition, InvalidArgument,
    fallthrough to `Rpc`, leader-hint accessor), 6 `heartbeat`
    tests (wall-clock sanity, clock override, resolver capture,
    `HeartbeatHandle::drop` terminates the task, `.stop()`
    idempotency).
  - **6 end-to-end `admin_integration` tests** drive a three-node
    `PdCluster` via real tonic channels:
    `admin_client_drives_catalog_on_three_node_cluster` covers
    register + heartbeat + create-range + list;
    `admin_client_performs_splits_and_leases` covers split + lease
    install + membership update; `writes_to_follower_surface_not_leader_with_hint`
    proves `Unavailable` + `pd-leader-id` surfaces as
    `NotLeader(Some(id))`; `overlap_and_epoch_regression_surface_as_catalog_errors`
    asserts catalog rejections come through as `CatalogRejected`;
    `heartbeat_loop_drives_catalog_timestamps` starts a live
    `HeartbeatLoop` and waits for the catalog's
    `last_heartbeat_millis` to advance; `status_reports_match_across_members_after_convergence`
    validates consistent status across the quorum.
  - **12 tests in `aresadb-cluster`**: 11 CLI parser unit tests
    (replica-spec voter/learner/default, empty/ malformed rejection,
    `--peer ID=URL` parsing, leader-hint rendering, `now_millis`
    sanity) + 1 end-to-end `pd_cli_smoke` integration test that
    shells out to the compiled `aresadb-cluster` binary (via
    `CARGO_BIN_EXE_aresadb-cluster`) and drives a single-node PD
    cluster through `status` → `create-range` → `list-ranges` →
    `register` → `heartbeat` → `get-node` → `split-range` →
    `get-range-by-key`.

**Totals after 2b-4**: 121 PD tests (109 unit + 6 admin integration
+ 6 cluster integration); 18 `aresadb-cluster` tests. Phase 2b is
now feature-complete: the placement driver is replicated, durable,
accessible over gRPC, and operable from the CLI. Phase 2c makes
`ClusterNode` range-aware so each member can host many Raft groups
with per-range backends.

### Added (Phase 2c-1 — multi-Raft wire + dispatch)

- **`aresadb-net` protobuf envelope.** Every Raft RPC
  (`AppendEntriesRequest`, `VoteRequest`, `InstallSnapshotRequest`) now
  carries a `uint64 raft_group_id` alongside the existing bincode
  `payload`. Group id `0` is reserved as
  `aresadb_net::SINGLETON_RAFT_GROUP_ID` — the marker Phase 1 deployments
  still emit by default, so existing on-wire traffic decodes unchanged
  (protobuf zero-defaulting). The payload framing is untouched; openraft's
  request/response bodies are still opaque to the wire layer.
- **`RaftDirectory` trait + `SingletonRaftDirectory` adapter.** New
  server-side lookup interface: `fn raft_for(&self, raft_group_id: u64)
  -> Option<Raft<TypeConfig>>`. `RaftGrpcServer` now holds
  `Arc<dyn RaftDirectory>` and every handler routes through a single
  `resolve(group_id)` helper that maps "unknown group id" to
  `tonic::Status::not_found` so the client sees a clean `NotFound` rather
  than a panic or timeout. `RaftGrpcServer::new(raft)` is still the
  one-liner Phase 1 uses — it wraps the `Raft` handle in a
  `SingletonRaftDirectory` that ignores the group id and returns the same
  instance for every call. Multi-range deployments use
  `RaftGrpcServer::from_directory(Arc::new(my_directory))`.
- **`GrpcRaftNetwork` parameterised by `raft_group_id`.** The network
  factory is now per-group: each range constructs its own
  `GrpcRaftNetwork::new(peer_directory.clone(), range.raft_group_id)` and
  every outgoing RPC stamps that id into the envelope. Phase 1 call sites
  use `GrpcRaftNetwork::new_singleton(peer_directory)` which pins the id
  to `SINGLETON_RAFT_GROUP_ID`, preserving behaviour bit-for-bit against
  pre-2c members. The connection cache still keys off `NodeId` so peers
  shared across many ranges reuse the same channel.
- **Backward-compat guarantees.** The existing two-node and three-node
  transport tests (`tests/two_node_cluster.rs`,
  `tests/three_node_cluster.rs`) and `ClusterNode::start` all use the
  singleton adapters and continue to pass unchanged. A client built with
  `new_singleton` talks to a server built with `RaftGrpcServer::new`
  without either side ever observing a non-zero group id.
- **`tests/multi_group_dispatch.rs`** — new integration test that boots
  two independent `SingleNode`s at group ids `10` and `20`, fronts both
  with a `MultiGroupDirectory` behind one `RaftGrpcServer`, and drives
  real tonic RPCs over a localhost channel to prove: (1) group `10`
  receives only its own traffic; (2) group `20` receives only its own
  traffic; (3) an unknown group id (`999`) surfaces as
  `tonic::Code::NotFound` via the shared `resolve` helper.
- **`aresadb-net/README.md`** documents both call patterns (Phase 1
  singleton vs. Phase 2c multi-group) with runnable examples.

Net effect: the Raft transport is now ready to carry traffic for many
Raft groups per node without any further wire-protocol churn. Phase 1
deployments keep working unchanged; Phase 2c-2 (`RangeRuntime`) can layer
on top by constructing one network factory + directory entry per range.

### Added (Phase 2c-2 — `RangeRuntime`)

- **`aresadb_cluster::range::RangeRuntime`.** Owns one range's full
  runtime: the PD-supplied `RangeDescriptor`, an
  `openraft::Raft<TypeConfig>` for the range's Raft group, and a
  dedicated `(log, data)` backend pair rooted at
  `<data-dir>/ranges/<range_id>/{log,data}/`. Phase 2c-3 will compose
  many of these into a `RangeDirectory` on each node; in isolation one
  runtime already exercises the full `open` / `bootstrap_voter` /
  `trigger_snapshot` / `shutdown` lifecycle.
- **Generic on the network factory.** `RangeRuntime::open` accepts any
  `N: RaftNetworkFactory<TypeConfig>`, so tests plug in
  `aresadb_raft::LoopbackNetwork` while production uses
  `aresadb_net::GrpcRaftNetwork::new(peer_directory, raft_group_id)` —
  the Phase 2c-1 per-group factory. Each range ends up with its own
  group-scoped network factory, matching the wire protocol.
- **`open_on_disk` convenience.** Given a `NodeConfig` + descriptor,
  opens redb backends under the new per-range directory layout
  (`NodeConfig::range_log_path` / `range_data_path`) and wires the
  runtime. `NodeConfig::ensure_range_dirs` creates the per-range
  `log/` + `data/` subdirectories idempotently on every open.
- **`NodeConfig` per-range accessors.** `ranges_root`, `range_dir`,
  `range_log_dir`, `range_data_dir`, `range_log_path`,
  `range_data_path`, and `ensure_range_dirs` — all the layout
  knowledge lives on `NodeConfig` so the admin tooling, the runtime,
  and the future PD supervisor share one source of truth for where a
  range's data lives.
- **Idempotent `bootstrap_voter`.** Pattern-matches
  `openraft::error::InitializeError::NotAllowed` on
  `raft.initialize(...)` return to handle "already initialised"
  recovery paths robustly, instead of probing the error's display
  string (which shifts across openraft releases). On a recovery
  reopen, the call skips `initialize` and waits for a re-election
  from the on-disk log; on a fresh bootstrap it initialises and waits
  for self-leader. The same fix is applied to
  `ClusterNode::bootstrap_single`.
- **`trigger_snapshot`**. Hook for the Phase 2c-4 PD supervisor (and
  for tests that want deterministic snapshot coverage) to request an
  async snapshot build from openraft.
- **Backend independence per range.** Every range constructs its own
  `LogStore` + `StateMachineStore`. That means
  `aresadb_core::FORMAT_VERSION` bumps and the `\xff/sm/meta`
  metadata key are scoped per-range, so a future rolling migration
  can touch one range at a time without a global flag day. Two
  `RangeRuntime`s on the same node don't share any keyspace.
- **8 new unit tests.** Five in `src/range.rs` cover the lifecycle
  (on-disk layout, bootstrap + replicated write, shutdown+reopen
  rehydrates, two-range isolation on a single node, snapshot
  trigger); three in `src/config.rs` cover the new layout helpers
  (path builders, id-collision-free paths,
  `ensure_range_dirs` idempotency).

**Totals after 2c-2**: 12 `aresadb-cluster` lib unit tests (up from
4), plus the existing 11 CLI unit tests and 3 integration tests
(`leader_failover`, `pd_cli_smoke`, `three_node_durable`) — all
passing. `cargo clippy -- -D warnings` clean.

Net effect: the per-range storage + Raft runtime is feature-complete
in isolation. Phase 2c-3 turns `ClusterNode` into a directory of
`RangeRuntime`s fronted by one gRPC server.

### Added (Phase 2c-3 — range-aware `ClusterNode`)

- **`aresadb_cluster::range::RangeDirectory`.** The multi-Raft source
  of truth on every node: a dual-indexed (`HashMap<RangeId, Arc<…>>`
  + `HashMap<GroupId, Arc<…>>`) directory of every `RangeRuntime`
  this node serves. Implements `aresadb_net::RaftDirectory`, so the
  Phase 2c-1 fan-out gRPC server dispatches inbound Raft RPCs to the
  correct range via the wire-level `raft_group_id` envelope — one
  hash probe, no extra state. `RangeDirectoryError` surfaces
  `DuplicateRangeId` / `DuplicateGroupId` as typed errors so the
  admin RPC path can map them to `ALREADY_EXISTS` cleanly.
- **Range-aware `ClusterNode`.** The node no longer holds a single
  `Raft<TypeConfig>` + `log/data` pair. It owns an
  `Arc<RangeDirectory>` plus a handle to a well-known "default
  range" (`DEFAULT_RANGE_ID = 1`, `DEFAULT_RAFT_GROUP_ID = 1`) that
  every `start()` call opens on boot. Legacy accessors (`raft()`,
  `data()`, `log_backend()`) continue to work by forwarding to the
  default range — so Phase 1 CLI, admin RPCs, and integration tests
  are untouched. Shutdown drains the directory and tears every
  range's Raft + backends down cleanly.
- **Single listener, many groups.** `spawn_server` now builds
  `RaftGrpcServer::from_directory(range_directory)` for the peer
  transport, so adding or removing a range on a running node needs
  **no** port churn and no per-range listener. The admin API still
  rides the same TCP port.
- **`AddRange` / `RemoveRange` / `ListRanges` admin RPCs**
  (`proto/admin.proto`, `AdminService`, generated `ClusterAdminClient`).
  `AddRange` opens per-range backends under
  `<data-dir>/ranges/<range_id>/{log,data}/`, constructs a
  `GrpcRaftNetwork` tagged with the new group id, registers the
  runtime in the directory, and optionally self-bootstraps as a
  single voter. Idempotent: a pre-flight directory probe turns
  `ALREADY_EXISTS` into a clean gRPC status instead of a confusing
  redb file-lock error. `RemoveRange` consumes the last `Arc` and
  shuts the runtime down, with `force=true` for the rare case where
  outstanding references exist. `ListRanges` returns every live
  `RangeDescriptor`, sorted by `range_id`.
- **Typed wire schema for ranges.** `pb::RangeDescriptor`,
  `pb::ReplicaPlacement`, `pb::RangeLease`, and `pb::ReplicaRole`
  mirror `aresadb_pd::types::*`, so the cluster admin API and the PD
  admin API (`aresadb.pd.v1`) speak the same shape on the wire.
  `descriptor_from_pb` defaults `raft_group_id` to `range_id` when
  zero — matches `RangeDescriptor::new` — and rejects empty spans,
  unspecified replica roles, and zero `range_id`.
- **`RangeRuntime::bootstrap_voter_with_addr`.** New variant that
  seeds `BasicNode::new(addr)` into the initial membership entry,
  so the `ClusterNode` membership-watcher can populate the peer
  directory from Raft metrics alone on a fresh bootstrap. The
  zero-arg `bootstrap_voter()` is now a thin wrapper over it.
- **Multi-range isolation tests.** New `range_admin_rpcs.rs`
  integration test binary drives the full stack through
  `ClusterAdminClient`: three tests cover the happy-path AddRange
  + ListRanges + RemoveRange loop, the uninitialised-AddRange
  (learner-join) flow, and full storage isolation — writes to one
  range never leak into another, on-disk layouts are physically
  disjoint, and removing a secondary range leaves the default
  range's data intact. Four new `RangeDirectory` unit tests in
  `src/range.rs` cover insert/get/remove round-trips, duplicate-id
  rejection (both axes), descriptor snapshots, and the
  `RaftDirectory` impl routing by group id.

**Totals after 2c-3**: 16 `aresadb-cluster` lib unit tests (up from
12), 11 CLI unit tests, and 4 integration test binaries (`leader_failover`,
`pd_cli_smoke`, `three_node_durable`, **`range_admin_rpcs`**) — all
passing, `cargo clippy -- -D warnings` clean across every v2 crate.

**Back-compat notes.** The on-disk layout under
`<data-dir>/ranges/<range_id>/{log,data}/` replaces Phase 1's
`<data-dir>/{raft_log,state_machine}/`. Pre-2c-3 clusters need to
re-bootstrap; this is a pre-alpha release so no migration tooling is
provided. New ranges land at range id >= 2; `DEFAULT_RANGE_ID = 1` is
reserved for the back-compat default range on every node.

Net effect: `ClusterNode` is now range-aware end-to-end. Future
phases (2c-4 PD-driven orchestration, 2c-5 range-leader leases) can
mutate the directory through the admin RPCs without disturbing the
Phase 1 data plane.

### Added (Phase 2c-4 — PD-driven orchestration)

- **`aresadb_cluster::pd_supervisor`.** New module that closes the
  loop between the PD catalog and a node's `RangeDirectory`. A
  `PdSupervisor` spawns two long-running tokio tasks per node — a
  heartbeat loop (`aresadb_pd::admin::HeartbeatLoop`) that keeps
  the catalog's liveness timer fresh, and an independent reconcile
  loop that calls `list_ranges` on PD, diffs against the local
  directory, and opens / closes `RangeRuntime`s to converge. Both
  share one `watch` shutdown signal so `PdSupervisorHandle::stop`
  cleanly drains in-flight work.
- **`PdSupervisorConfig`**. Builder-style config carrying the
  node's identity (`node_id`, `store_id`, `advertise_addr`), its
  PD endpoints, heartbeat / reconcile cadences (defaults 1s each),
  and a `skip_local_ranges` set that defaults to
  `{DEFAULT_RANGE_ID}`. The skip list is how the supervisor
  preserves back-compat: the default range is local-only and never
  replicated through the PD catalog, so we never try to "reconcile
  it away".
- **Pure-logic reconciler** (`pd_supervisor::reconciler::plan_reconcile`).
  Given `(node_id, pd_ranges, local_descriptors, skip_set)`,
  emits a `ReconcilePlan { to_add, to_remove }`. `to_add` = PD
  entries that list this node as a replica, minus local, minus
  skip-list. `to_remove` = local entries with no matching PD
  assignment, minus skip-list. 10 unit tests cover every pairwise
  combination of present / absent / skipped — empty plan when in
  sync, add-only when PD is ahead, remove-only when local is
  ahead, mixed, and the skip-set honored in both directions.
- **Executor** (`pd_supervisor::executor::execute_plan`). Applies
  a plan by opening `RangeRuntime`s for every `to_add` entry
  (under `<data-dir>/ranges/<range_id>/{log,data}/`, wiring a
  `GrpcRaftNetwork` tagged with the new group id, registering in
  the directory) and shutting down the last `Arc<RangeRuntime>`
  for every `to_remove` entry. Per-range failures accumulate in
  an `ExecutorReport` with structured `ExecutorError`s rather
  than aborting the pass — a single bad range can't block every
  other range's progress on the same tick.
- **`ClusterNode::attach_pd_supervisor`** + **`::start_with_pd`**
  + **`::has_pd_supervisor`**. Attach is one-shot (a second call
  returns `ClusterError::Config`) and performs the initial
  `register_node` synchronously, so a successful return means the
  node is already in the catalog when the reconcile loop starts.
  `ClusterNode::shutdown` now stops the supervisor first, then
  drains the `RangeDirectory`, so PD-managed ranges are closed in
  the same pass as the default range — no partial-shutdown
  footgun.
- **`pd_supervisor_integration.rs` integration tests.** Spin up a
  real 3-node `PdCluster` with one tonic admin server per member,
  then a real `ClusterNode` with its supervisor pointed at the
  leader. Two tests:
  - `supervisor_opens_pd_created_range_locally` creates a PD
    range assigned to node 1, boots the node, and waits for the
    local directory to contain the new range by both `range_id`
    and `raft_group_id` (gRPC dispatch). The default range stays
    alongside.
  - `supervisor_ignores_ranges_not_assigned_to_this_node` puts a
    range assigned to a ghost node 5 into PD, confirms node 1
    never opens it after many reconcile ticks, then adds a
    second range assigned to node 1 and confirms only the second
    converges locally.

**Totals after 2c-4**: 46 `aresadb-cluster` lib unit tests (up from
16 reported at 2c-3; the count includes the new `pd_supervisor`
sub-module tests), 11 CLI unit tests, 5 integration test binaries
(`leader_failover`, `pd_cli_smoke`, `three_node_durable`,
`range_admin_rpcs`, **`pd_supervisor_integration`**) — all passing,
`cargo clippy -- -D warnings` clean across every v2 crate.

**Scope note.** Phase 2c-4 covers *add* and *remove* reconciliation.
Splits and merges are deferred to Phase 2c-5+: they require
additional edge-case handling (split markers, generation bumps,
membership transitions) that's bigger than the PD-supervisor's
"observe catalog, converge local directory" contract. The
supervisor keeps hands off the default range (`skip_local_ranges`
by default contains `{DEFAULT_RANGE_ID}`) so the Phase 1
single-shard path continues to work with or without PD attached.

Net effect: a `ClusterNode` can now be configured as a pure PD
client — boot with `start_with_pd`, advertise its address, and let
the catalog drive which ranges it serves. Phase 2c-5 (range
leader leases) and Phase 2c-6 (multi-range madsim + Docker smoke)
build on this to get end-to-end production semantics.

### Added (Phase 2c-5 — range leader leases)

- **`LeadershipStatus`** (in `aresadb_cluster::range`). A compact,
  plain-data snapshot of a range's Raft leadership state pulled
  from the openraft metrics watch channel: `range_id`, `node_id`,
  `is_leader`, `current_leader`, `current_term`, `last_log_index`,
  `last_applied_index`, `voter_count`, plus a derived `apply_lag()`
  helper (`last_log_index − last_applied_index`, clamped). Every
  field is plain `u64` / `bool` / `Option<u64>` so the struct
  survives protobuf / FFI round-trips without leaking openraft
  internals (`ServerState`, `Vote`, `StoredMembership`) whose
  shape drifts between minor versions.
- **`RangeRuntime::leadership_status()`**. Observability-only
  accessor — pure `watch::Receiver::borrow()`, no network I/O.
  Intended for admin `Status`, PD heartbeat payloads, and
  Prometheus scrapers. Correctness-sensitive callers must still
  go through `ensure_linearizable`.
- **`ReadError` + `ReadResult<T>`** (in `aresadb_cluster::error`).
  Read-path error taxonomy kept distinct from `ClusterError` so
  call sites that fan out a read don't have to match on
  write-path variants (`Config`, `InvalidRequest`). Variants:
  - `NotLeader(Option<NodeId>)` — leader hint surfaced from
    openraft's `ForwardToLeader`. `None` during an election.
  - `QuorumUnavailable(String)` — ReadIndex heartbeat couldn't
    reach a quorum. Transient; safe to retry.
  - `Fatal(String)` — openraft reported a fatal state (shutdown,
    corruption). Operator intervention.
  - `Storage(#[from] aresadb_core::Error)` — state-machine read
    error surfaced verbatim.
  - `From<RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>>`
    wired so every call site uses the `?` operator cleanly.
- **`RangeRuntime::ensure_linearizable()`**. Wraps
  `openraft::Raft::ensure_linearizable`, which under openraft 0.9
  runs the ReadIndex protocol: the leader probes a quorum of
  followers to confirm it is still the leader, then waits for the
  state machine to apply up to the read log id. Returns
  `Ok(())` on success; everything else maps to a `ReadError`.
- **`RangeRuntime::linearizable_get(key)`**. Single-key
  linearizable point read. Calls `ensure_linearizable`, then
  reads the key out of `data_backend`. Must be invoked on the
  Raft leader — otherwise returns `ReadError::NotLeader` with a
  hint. Covers the "Leader-lease read" row of §4.3 in
  `architecture-v2.md`.
- **`RangeRuntime::stale_get(key)`**. Skips the leadership guard
  entirely; reads directly from the local state machine. Safe on
  any member; may miss concurrent writes or see an older applied
  index. Covers the "Bounded staleness" row of §4.3.
- **`ReadConsistency` enum on the admin `Read` RPC** plus
  optional `range_id` field (proto: `aresadb.cluster.v1`). New
  values: `READ_CONSISTENCY_UNSPECIFIED` (Phase 1c back-compat
  — raw state-machine lookup on the default range, no guard),
  `READ_CONSISTENCY_LINEARIZABLE`, `READ_CONSISTENCY_STALE`.
  `range_id = 0` resolves to `DEFAULT_RANGE_ID`. The
  `ReadResponse` gains `range_id` (echo of the resolved range)
  and `read_log_index` (applied index reported by
  `LeadershipStatus` immediately after the linearizability
  guard, zero for stale / unspecified reads). The server-side
  handler in `AdminService::read` branches on consistency and
  routes to `linearizable_get` / `stale_get` on the target
  range, with `ReadError::NotLeader` mapping to
  `FAILED_PRECONDITION` and attaching an `x-aresa-leader-id`
  gRPC metadata header so CLIs and SDKs can re-route without
  parsing the human-readable status message.
- **CLI `--consistency` + `--range-id` flags on `read`**. The
  `aresadb-cluster read` subcommand grows `--consistency
  [unspecified|linearizable|stale]` (default: `unspecified`,
  Phase 1c shape) and `--range-id <u64>` (default: `0`,
  resolves to the default range). Linearizable reads print a
  trailer line to stderr showing the served range and applied
  index — useful when debugging routing against a 3-node
  cluster from the shell.
- **Unit tests** (`crates/aresadb-cluster/src/range.rs`, 5 new
  cases). `leadership_status_before_bootstrap_reports_no_leader`
  asserts the uninitialised baseline;
  `leadership_status_after_bootstrap_reports_leader` polls until
  the single-voter election converges;
  `linearizable_get_returns_committed_value` writes via Raft then
  reads linearizably, covering both present and absent keys;
  `stale_get_reads_local_state_machine_without_guard` mirrors
  that for the bounded-staleness path;
  `ensure_linearizable_returns_not_leader_when_uninitialised`
  exercises the `From<RaftError<_, CheckIsLeaderError>>`
  conversion and asserts the `ReadError::NotLeader` variant is
  surfaced.
- **Integration tests** (`tests/range_leader_leases.rs`, 4 new
  cases, 3-voter redb-backed cluster with real tonic transport):
  - `linearizable_read_on_leader_returns_value` — happy path,
    asserts `read_log_index > 0` and correct value for both
    present and absent keys.
  - `linearizable_read_on_follower_returns_not_leader_with_leader_hint`
    — hits a follower, expects `FAILED_PRECONDITION` and the
    `x-aresa-leader-id` metadata header pointing at node 1.
  - `stale_read_on_follower_eventually_reflects_write` — polls
    a follower's stale read until the write has propagated;
    confirms absent keys come back as `not found` without
    error.
  - `linearizable_read_follows_leader_after_failover` — kills
    the bootstrap leader, waits for a new leader on the
    survivors, asserts linearizable reads on the new leader
    return both pre-failover and post-failover values with
    monotonic `read_log_index`, and that a follower under the
    *new* leader still refuses linearizable reads with a
    correctly-updated leader hint.

**Totals after 2c-5**: 51 `aresadb-cluster` lib unit tests (up
from 46 at 2c-4; +5 in `range::tests`), 11 CLI unit tests, 6
integration test binaries (`leader_failover`, `pd_cli_smoke`,
`pd_supervisor_integration`, **`range_leader_leases`**,
`range_admin_rpcs`, `three_node_durable`) — all passing,
`cargo clippy -- -D warnings` clean across every v2 crate.

**Scope note.** Phase 2c-5 ships the *openraft-backed* leader-lease
read path (ReadIndex plus wait-for-apply under the hood). The PD's
`LeaseInfo` machinery — which tracks catalog-level lease holders
for coordinated leader transfer during rebalancing — is untouched
and remains a later phase concern. The `stale_get` path is
intentionally guard-free; when we ship MVCC in Phase 4 we will add
an explicit `read_as_of(ts)` variant that reads under a timestamp
predicate rather than "wherever the state machine is right now".

Net effect: `RangeRuntime` now exposes all three rows of §4.3 —
leader-lease linearizable reads, ReadIndex-backed quorum reads
(same entry point for now; a lease-only fast path without the
heartbeat round-trip is a later optimization), and bounded-
staleness follower reads. The admin `Read` RPC honours the
choice, attaches leader hints on rejection, and keeps Phase 1c
callers working without code changes.

### Added (Phase 2c-6 — multi-range madsim + Docker smoke)

Closes the Phase 2c arc. Two independent lanes land in this phase:
a deterministic-simulation scenario for the per-range apply path,
and an operator-driven Docker smoke test that exercises the
range-aware admin surface end-to-end on a 3-node cluster.

**Range-aware admin Write / CLI.**

- `aresadb.cluster.v1.WriteRequest` gains a `range_id` field (and
  `WriteResponse` echoes it back). `range_id = 0` preserves the
  Phase 1c wire contract — routes to the default range
  (`DEFAULT_RANGE_ID = 1`) via the admin service's default Raft
  handle. Non-zero values look up the target range in the local
  `RangeDirectory` and issue `client_write` against that range's
  own Raft, returning `NOT_FOUND` when the range isn't registered
  and `FAILED_PRECONDITION` (with `x-aresa-leader-id` metadata) on
  `ForwardToLeader` — mirrors the Phase 2c-5 `Read` design.
- New `WriteError` / `WriteResult` types in
  `aresadb_cluster::error` with a `From<RaftError<_,
  ClientWriteError<_, _>>>` impl that maps `ForwardToLeader`,
  `ChangeMembershipError::{InProgress,LearnerNotFound,EmptyMembership}`,
  and `RaftError::Fatal` to distinct, structured variants. The
  admin `write_status` helper renders them as typed tonic
  `Status` values, parallelling the existing `read_status`.
- `aresadb-cluster` CLI grows `add-range`, `remove-range`,
  `list-ranges` subcommands plus a `--range-id` flag on `write`.
  The new subcommands are thin wrappers over
  `ClusterAdminClient::{add_range,remove_range,list_ranges}` from
  Phase 2c-3c — they were previously callable only in tests.

**`MultiRangeApplyDeterminism` scenario (`aresadb-sim`).**

- New scenario running alongside `RaftApplyDeterminism`. Routes
  each scripted `RaftOp` to a range by longest-prefix match over
  a configurable `prefixes: Vec<(Vec<u8>, u64)>` map, spins up one
  `SingleNode` per range in parallel via
  `futures::future::try_join_all`, replays the same ~320-op script
  twice, and asserts:
  - Each range's final state is byte-identical across the two
    runs (per-range apply determinism).
  - No range's backend contains a key that routes to a different
    range (cross-range isolation — the new invariant this phase
    is really about).
- Default script covers puts, overwrites, point-deletes (hits and
  misses), and per-range `delete_range` ops across four ranges
  (`r1/`, `r2/`, `r3/`, `r4/`). Ops are intentionally interleaved,
  not grouped by range, so a routing bug can't be masked by
  lucky ordering.
- Negative tests: unrouted ops fail with a clear error, and an
  empty script surfaces "zero user-visible keys" per range (no
  silent pass-through).

**Docker multi-range smoke (`docker/cluster/multi-range.sh`).**

- Drop-in companion to `bootstrap.sh`. Assumes the 3-node compose
  stack is up and the default-range bootstrap has completed, then:
  1. Opens a fresh range `42` on node-1 as a single-voter Raft
     group via `aresadb-cluster add-range --bootstrap-as-voter`.
  2. Writes `r42/hello = phase-2c-6` through the Phase 2c-6
     range-aware `Write` RPC.
  3. Reads it back under both `linearizable` and `stale`
     consistency — exercising the Phase 2c-5 read-path on a
     non-default range for the first time in production-shape.
  4. Verifies node-2 and node-3 return `NOT_FOUND` for reads and
     writes targeting range 42 — cross-process isolation, the
     `MultiRangeApplyDeterminism` invariant enforced across real
     containers on a real network.
  5. Dumps `list-ranges` on every node so the divergent local
     directories are visible for debugging.
- The script is idempotent: `ALREADY_EXISTS` on `add-range` is
  treated as success so the smoke survives container restarts
  without tearing volumes.
- Multi-node replication of non-default ranges stays out of scope
  for this phase — the cluster-admin `AddLearner` /
  `ChangeMembership` RPCs still target the default range only.
  That work is carved out to Phase 2d's "range-aware admin"
  thread, alongside PD-driven split execution.

**Tests.**

- `aresadb-sim`: 7 unit tests (was 4) — 3 new cases covering the
  multi-range scenario (happy path, longest-prefix routing,
  unrouted-op rejection, empty-script rejection).
- `aresadb-cluster`: 58 tests across library + 7 integration
  binaries (was 55). `tests/range_leader_leases.rs`,
  `tests/range_admin_rpcs.rs`, `tests/leader_failover.rs`, and
  `tests/three_node_durable.rs` were updated for the new
  `WriteRequest::range_id` field; every existing case still
  passes via the `range_id = 0` default.
- Workspace `cargo check --workspace --all-targets` stays clean;
  `bash -n docker/cluster/multi-range.sh` parses cleanly.

**Scope note.** Phase 2c-6 deliberately does not spin up a
network-capable PD in Docker. The Phase 2b-3 `PdRouter` is
in-process only, so a multi-node PD would need a whole new gRPC
transport layer — that's Phase 2d. Within that constraint, the
multi-range smoke still exercises every wire-level integration
that Phase 2c-6 ships: range-aware routing on the data plane,
leader-lease reads on non-default ranges, and explicit isolation
checks against unrelated nodes.

### Added (Phase 2d — fjall-backed LSM engine)

Ships the second of the two v2 storage engines: a log-structured
merge-tree backend for write-heavy ranges, opt-in per node via
configuration, with redb remaining the default.

**New crate: `aresadb-engine-lsm`.**

- `FjallBackend` — fjall-3.1-backed implementation of
  `aresadb_core::StorageBackend`. One `fjall::Database` per
  backend instance (its own journal + memtable + levelled
  SSTables), holding a single `Keyspace` named `"default"` that
  maps onto the backend's flat key/value namespace. The
  `bytes_1` fjall feature is enabled so fjall's `UserKey` /
  `UserValue` are `bytes::Bytes` directly — the same slice type
  `aresadb-core` uses, so no copy crosses the backend boundary.
- **I/O model.** Every synchronous fjall call is wrapped in
  `tokio::task::spawn_blocking` and routed through `Arc<Handles>`
  clones (both `Database` and `Keyspace` are cheap to clone).
  The async runtime never blocks on disk — same discipline as
  `aresadb-engine-redb`.
- **Durability.** `write_batch` drives a single
  `OwnedWriteBatch::commit()` followed by
  `db.persist(PersistMode::SyncAll)` so the journal is fsync'd
  before the method returns. This is the contract Raft needs
  from a state-machine backend: once `apply` returns Ok, the
  write survives a crash. `flush()` calls `persist(SyncAll)`
  unconditionally so the metrics / snapshot trigger paths can
  force a sync point without touching the writer.
- **Snapshots.** `FjallSnapshot` eagerly materialises every
  `get` / `scan` through a single persistent
  `fjall::Snapshot` captured at `snapshot()` time and returns
  `Vec<KeyValue>` / `Bytes` — same `Send + 'static` trick
  `RedbBackend` uses. Cost: O(range) memory per range scan.
  Fine for Raft log truncations (bounded) and state-machine
  snapshot builds (already buffered upstream); a streaming
  snapshot iterator is a later optimisation.
- **Range delete.** fjall has no single-op range tombstone, so
  `delete_range` collects every key in the half-open
  `[start, end)` interval via a snapshot scan and emits
  individual `OwnedWriteBatch::remove` calls in one commit.
  O(N) in keys dropped, same shape as `RedbBackend`; acceptable
  for log-purge volumes, revisit if a "drop-tenant"-sized
  delete becomes a real workload.
- **`approximate_size`.** Returns 0 for every range. fjall's
  `Keyspace::disk_space` is whole-keyspace, not range-scoped,
  and the sharder treats this hint as advisory (Phase 2b
  catalog + PD split heuristics already tolerate a zero hint
  from `aresadb-engine-redb`).
- **Lifecycle.** `open(path)` opens (or creates) a fjall
  database under `path/` and the `"default"` keyspace inside
  it. `close()` takes the inner handle under the backend's
  internal `RwLock`, drops it to trigger fjall's shutdown,
  and every subsequent trait call returns `Error::Closed` —
  same contract as `RedbBackend::close`. Re-opens on the
  same path return a fresh `FjallBackend` with the previously
  persisted keys visible.

**Pluggable data engine: `NodeConfig::data_engine`.**

- New `DataEngine` enum (`Redb` default, `Lsm` opt-in) on
  `aresadb_cluster::NodeConfig`, plumbed through a
  `with_data_engine(engine)` builder. Path layout gains an
  engine-aware suffix: `data.redb` for the default,
  `data.lsm` for the fjall backend — two distinct on-disk
  shapes (file vs. directory) so switching engines on an
  existing range would surface as a data-directory mismatch
  rather than a silent data loss.
- `RangeRuntime::open_on_disk` dispatches on
  `cfg.data_engine`: `DataEngine::Redb` opens
  `RedbBackend::open(data_path)` as before; `DataEngine::Lsm`
  opens `FjallBackend::open(data_path)`. The **log backend
  stays on redb unconditionally** — Raft log workloads are
  append-heavy with fsync-per-commit, which is exactly the
  shape redb handles well and exactly the shape LSMs waste
  write amplification on.
- Label helper (`DataEngine::label()`) returns `"redb"` /
  `"lsm"` for status endpoints / logs. `path_suffix()` is the
  only function anywhere in the cluster that knows the
  directory name per engine, so a future `"lsm2"` or
  remote-object-store backend is a single-enum-variant
  change.

**Tests.**

- `aresadb-engine-lsm`: **13 unit tests** exercising the full
  `StorageBackend` contract — put/get/delete round-trip,
  durability across reopen, snapshot isolation from
  subsequent writes, range-scan inclusive/exclusive bounds,
  full-range scan ordering, `delete_range` parity with
  `MemoryBackend`, flush idempotency, `close()` → `Closed`
  transition, and the advisory-zero `approximate_size`
  contract.
- `aresadb-cluster`: new `range::tests::lsm_data_engine_
  persists_committed_writes_across_reopen` test drives
  `open_on_disk` with `DataEngine::Lsm`, commits a write
  through the full Raft → state-machine path, graceful-
  shuts-down, reopens the range on the same data directory,
  and asserts the value is still there. Also verifies the
  `data.lsm` suffix and that the path is a directory (not a
  file). 50 lib unit tests total (up from 49).
- Full workspace suite stays green on a mixed-engine run:
  existing redb-only tests continue to use the default
  `DataEngine::Redb` and pass unchanged.

**Scope note.** Phase 2d intentionally stops at the
storage-engine layer. It does not ship:
- A gRPC PD transport (the Phase 2b-3 `PdRouter` is still
  in-process).
- Range-aware `AddLearner` / `ChangeMembership` admin RPCs
  (Phase 2c-6's smoke still uses single-voter non-default
  ranges).
- An LSM-specific tuning surface (compaction workers, block
  cache sizes, bloom-filter bits) — fjall's defaults are
  sensible for the sizes we're testing at, and a `LsmOptions`
  struct should land together with real benchmark numbers,
  not as speculative dials.
- Multi-range madsim scenarios that specifically stress the
  LSM backend (the Phase 2c-6 scenarios are engine-agnostic).

The next checkpoint is the `v2.0.0-alpha.2` tag — every
Phase 2 lane (2a keyspace, 2b PD, 2c range-aware cluster, 2d
LSM engine) is now on disk, tested, and clippy-clean.

---

## [2.0.0-alpha.1] - 2026-04-11

First Raft-replicated, on-disk cluster release. Phase 1 is functionally
complete: a durable single-shard cluster, a real 3-node deployment, and
the first deterministic-simulation scenario. The v1 embedded code path
is unchanged.

### Closeout

- Removed the legacy v1 `src/distributed/wal.rs` stub — the Raft log
  (`aresadb-raft::LogStore`) is now the authoritative write-ahead log.
  The `aresadb::distributed` module is now marked as v1 scaffolding; new
  distributed functionality lives under `crates/`.
- Tagging this release pins the Phase 1 surface so Phase 2 (multi-Raft,
  range sharding, LSM) can move against a frozen baseline.

### Added (Phase 1 — single-shard cluster)

Phase 1 brings AresaDB from an embedded key-value engine to a durable
single-shard distributed cluster. All work lands under new crates in
`crates/`; the v1 embedded code path is unchanged.

**Phase 1a — Raft core (`aresadb-raft`)**
- `openraft 0.9.22` integration on top of the Phase 0 `StorageBackend`
  trait. Log and state machine are both engine-agnostic: swap the
  backend, the Raft layer doesn't care.
- `LogStore` — persistent Raft log with key-prefix partitioning
  (`0x00` for entries, `0x01` for vote / committed / purged pointers).
- `StateMachineStore` — applies `AresaCommand::WriteBatch` to a
  separate data backend, builds serializable snapshots, handles
  `install_snapshot` via backend wipe + replay. Persists
  `last_applied` and `last_membership` at `0xff/sm/meta` so recovery
  resumes from the last applied index after a restart.
- `AresaCommand` / `AresaResponse` / `SerializableWriteBatch` —
  bincode-friendly replicated command format with a Serde surface
  independent of the `Bytes`-heavy core types.
- `LoopbackNetwork` — single-node network stub that fails closed on
  any peer RPC so mis-configurations surface immediately.
- `SingleNode` — batteries-included harness that brings up an
  initialized one-voter cluster backed by `MemoryBackend`.
- 23 unit tests + 2 conformance tests via `openraft::testing::Suite`.

**Phase 1b — gRPC transport (`aresadb-net`)**
- `proto/raft.proto` — unary `AppendEntries` / `Vote` /
  `InstallSnapshot` RPCs that carry bincode-encoded openraft
  payloads with an `is_error` discriminant for logical failures.
- `RaftGrpcServer` — tonic service adapter over `openraft::Raft`.
- `GrpcRaftNetwork` + `StaticPeerDirectory` + `PeerDirectory` trait —
  `RaftNetworkFactory` with a pluggable peer-lookup layer so the
  cluster layer can drive membership without touching transport.
- Two-node integration test: real localhost gRPC, election, commit,
  follower apply, backend equality.
- Three-node integration test: 25 batched writes replicated across
  all voters.
- Vendored `protoc` via `protoc-bin-vendored` — building the
  transport needs no external cmake / protobuf toolchain.

**Phase 1d — 3-node deployment + sim coverage**
- `docker/cluster/` — real three-voter deployment of
  `aresadb-cluster`:
  - Multi-stage `Dockerfile` that builds the cluster CLI out of the
    workspace, uses BuildKit mount caches, and ships on debian-slim.
  - `docker-compose.yml` — three services (`aresadb-node-1..3`) on
    the `aresadb-cluster` network with per-node named volumes.
    Node 1 runs `bootstrap`, nodes 2 / 3 run `join` and wait to be
    added. Healthchecks call `aresadb-cluster status` against the
    admin gRPC service so `depends_on: service_healthy` is meaningful.
  - `bootstrap.sh` — one-shot operator script that adds node-2 and
    node-3 as voters, writes a sample key, and reads it back from
    every node, using a throwaway admin container on the same
    network — no host-side binary required.
  - `README.md` — operator walkthrough + failure-injection cookbook
    (`docker compose stop aresadb-node-1`, watch re-election, restart).
- **Leader-failover test (`aresadb-cluster/tests/leader_failover.rs`)**:
  bootstraps 3 redb-backed voters, commits ten writes, shuts the
  leader down, proves the survivors elect a new leader, commits ten
  more writes on the new leader, restarts the killed node and
  verifies it catches up via openraft log replication.
- **`RaftApplyDeterminism` sim scenario** (`aresadb-sim`): drives
  two independent `SingleNode` harnesses through a 200-op script of
  puts, overwrites, point deletes, and a range delete, and asserts
  that the resulting user-visible state is byte-identical. This is
  the seed scenario the Phase 2 multi-node madsim harness will
  build on. Ships alongside its negative test (empty script must be
  rejected).

**Phase 1c — Durable cluster (`aresadb-engine-redb`, `aresadb-cluster`)**
- `aresadb-engine-redb` (new crate): durable `StorageBackend`
  implementation built on [redb](https://github.com/cberner/redb).
  Single-table layout, all I/O dispatched through
  `tokio::task::spawn_blocking`, snapshot reads via `RedbSnapshot`.
  7 unit tests covering put / get / delete / range / snapshot /
  crash-free reopen.
- `aresadb-cluster` (new crate): node lifecycle + operator surface.
  - `NodeConfig` — declarative description of a node (id, bind
    addresses, data dir layout).
  - `ClusterNode` — runtime object that owns Raft, the redb log
    backend, the redb data backend, the gRPC Raft server, and the
    gRPC admin server. `start` / `bootstrap_single` / `shutdown`
    with graceful task cancellation.
  - `admin.proto` + `AdminService` — tonic gRPC admin API:
    `Initialize`, `AddLearner`, `ChangeMembership`, `Write`, `Read`,
    `Status`. Runs on the same gRPC server as the Raft transport.
  - Membership watcher — background task that tails
    `openraft::Raft::metrics` and keeps `StaticPeerDirectory` in
    lockstep with committed membership, so the transport layer
    always knows how to reach the current voters / learners.
  - `aresadb-cluster` CLI (`src/bin/cli.rs`) — `clap`-based operator
    tool with `bootstrap`, `join`, `add-voter`, `change-membership`,
    `write`, `read`, `status` subcommands talking to the admin API.
- **Three-node durable cluster test
  (`tests/three_node_durable.rs`)**: three redb-backed nodes,
  bootstrap, replicate, graceful shutdown, restart from disk,
  verify every committed write survives, then confirm new writes
  still replicate. This is the first real on-disk cluster proof.

### Workspace totals after Phase 1
- 400 tests green across 22 test binaries (`cargo test --workspace`).
- Six new crates pass `cargo clippy --all-targets -- -D warnings`:
  `aresadb-core`, `aresadb-raft`, `aresadb-net`,
  `aresadb-engine-redb`, `aresadb-cluster`, `aresadb-sim`.

---

## [2.0.0-alpha.0] - 2026-04-19

First alpha of the v2 distributed architecture. **No behavior change
for embedded users** — everything in `0.2.1` still works exactly the
same. This release lays the foundation:

### Added

- **Cargo workspace**: the repo is now a workspace. The root `aresadb`
  package remains the main embedded/server crate; new layers live in
  `crates/`.
- **`aresadb-core`** (new crate): defines the engine-agnostic
  `StorageBackend` trait, `WriteBatch`, `KeyRange`, `Snapshot`, and
  `KeyValueStream` types. Includes `MemoryBackend`, a reference in-memory
  implementation used as the contract source of truth and by simulation
  tests.
- **`aresadb-sim`** (new crate): deterministic-simulation harness built
  on [`madsim`](https://github.com/madsim-rs/madsim). Phase 0 ships a
  single-node smoke scenario; Phase 1+ grows it into full Jepsen-lite
  cluster testing.
- **`docs/architecture-v2.md`**: the full design spec for the v2
  distributed architecture.
- **`docs/phase-status.md`**: live execution tracker.

### Ratified architectural decisions

- Replication: Multi-Raft via [openraft](https://github.com/datafuselabs/openraft).
- Storage: dual-backend — redb for embedded/compat, fjall-backed LSM
  for cluster mode (Phase 2), custom thread-per-core LSM for headline
  performance (Phase 5).
- Runtime: tokio primary, `tokio-uring` for io_uring file I/O,
  thread-per-core pattern for the custom LSM hot path.
- Sharding: range-based (CockroachDB / TiKV style), unified keyspace.

---

## [0.2.1] - 2026-04-11

Initial public release.

### Core engine

- Multi-model storage: property graph + key-value + relational (SQL) + vector search + full-text search, all in one embedded database
- Tiered storage architecture: graph index always local (sub-ms traversals) with node payloads that can live local, cached, or on S3 / GCS
- `TieredStorage` orchestrator with a local → cache → cloud read path, read-through caching via moka, optional write-through, and `evict_to_cloud` / `promote_to_local` / `run_eviction` / `prefetch_neighbors` controls
- `NodeIndex` records and split `NODE_INDEX_TABLE` / `NODE_PAYLOADS_TABLE` in redb for index/payload separation
- Zero-copy serialization with rkyv; `Value` type supports String, Integer, Float, Boolean, Array, Object, Null
- Automatic migration of legacy databases to the tiered format on open

### Indexes and search

- HNSW vector indexes managed per `(node_type, embedding_field)`, lazy-built on first search, incrementally maintained on insert; ~99x speedup over brute-force linear scan
- Filtered vector search: `VECTOR SEARCH ... WHERE col = 'val' LIMIT k`
- Secondary B-tree property indexes via `CREATE INDEX ON table (field)`, automatically maintained, used by the query planner for O(log n) equality lookups
- Full-text search with inverted index and BM25 ranking (k1=1.2, b=0.75): `CREATE FULLTEXT INDEX ON table (field)` and `FULLTEXT SEARCH table FIELD field FOR 'query' LIMIT n`

### Query engine

- SQL parser (sqlparser-rs) with `SELECT`, column selection, `WHERE` (`=`, `!=`, `<`, `>`, `<=`, `>=`), `ORDER BY`, `LIMIT`
- Graph traversal, shortest-path, connected-components algorithms
- Query planning with automatic index routing

### Batch APIs

- `insert_nodes_batch()` (~37,700 nodes/sec) and `create_edges_batch()` (~28,200 edges/sec), each executed in a single redb write transaction

### CLI and wire protocol

- Full-featured CLI: `init`, `insert`, `get`, `delete`, `query`, `view`, `status`, `repl`, `traverse`, `push` / `connect` / `sync`
- Multiple output formats: table, json, csv
- TCP wire protocol (`feature = "server"`) covering insert, get, update, delete (node/edge), query, traverse, status

### Python bindings

- PyO3 bindings (`pip install aresadb`) with 33 methods across all five paradigms: KV, graph, SQL, vector, full-text
- `.pyi` type stubs for full IDE autocompletion
- Multi-platform wheels for Linux and macOS, Python 3.9-3.13

### Cloud storage integration

- S3 and GCS support via the `object_store` crate with push / pull / sync
- `BucketStorage::connect` honors `STORAGE_EMULATOR_HOST` (GCS) and `AWS_ENDPOINT_URL` (S3) for routing to local emulators or S3-compatible services
- Cloud integration test suite with emulator-based tests (MinIO + fake-gcs-server) on every CI build — no cloud credentials required
- Gated nightly real-cloud smoke tests (`tests/cloud_real.rs`) for OAuth refresh, IAM edge cases, and real network paths
- `docker-compose.test.yml`, `scripts/start_emulators.sh`, and `scripts/stop_emulators.sh` for local dev parity with CI

### CI/CD and packaging

- Multi-job GitHub Actions CI: check, test, lint, docs, emulator-backed cloud integration
- Tag-triggered release pipeline publishing to crates.io, PyPI (Linux x86_64 / aarch64, macOS x86_64 / arm64), and GHCR Docker images
- Reproducible benchmark suite at `cargo run --example benchmark_suite --release`

### Testing

- 330+ unit, integration, and stress tests
- Property-based tests (proptest), Criterion benchmarks

### Documentation

- `README.md`, `ARCHITECTURE.md`, `BENCHMARKS.md`, `QUICKSTART.md`, `CONTRIBUTING.md`
- `docs/cloud-testing-setup.md` — step-by-step guide for provisioning GCP service accounts and AWS IAM users for the gated smoke tests
- `tests/README.md` — test-suite layout and cloud-testing how-to
- Crate docs with architecture diagram
- `python/README.md` with complete API documentation

[Unreleased]: https://github.com/yoreai/aresadb/compare/v2.0.0-alpha.2...HEAD
[2.0.0-alpha.2]: https://github.com/yoreai/aresadb/compare/v2.0.0-alpha.1...v2.0.0-alpha.2
[2.0.0-alpha.1]: https://github.com/yoreai/aresadb/compare/v2.0.0-alpha.0...v2.0.0-alpha.1
[2.0.0-alpha.0]: https://github.com/yoreai/aresadb/compare/v0.2.1...v2.0.0-alpha.0
[0.2.1]: https://github.com/yoreai/aresadb/releases/tag/v0.2.1
