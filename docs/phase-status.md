# AresaDB v2 — Phase Status

> Live execution tracker for the distributed v2 build. See
> [architecture-v2.md](./architecture-v2.md) for the full spec.

## Legend

- ☐ Not started
- ◐ In progress
- ☑ Done
- ⚠ Blocked
- ✗ Cancelled / descoped

## Current phase

**Phase 2 — Multi-Raft + range sharding + LSM** ☑ complete, tagged
`v2.0.0-alpha.2`.

- Phase 2a (unified keyspace encoder/decoder for all five models) ☑
- Phase 2b (replicated placement driver — PD catalog + state
  machine + Raft group + admin gRPC + heartbeats + CLI) ☑
- Phase 2c (range-aware `ClusterNode` — multi-Raft wire + dispatch,
  `RangeRuntime`, `RangeDirectory` admin RPCs, PD supervisor,
  range leader leases, multi-range madsim + Docker smoke) ☑
- Phase 2d (opt-in fjall-backed LSM data engine alongside the
  default redb; `NodeConfig::data_engine` + `open_on_disk`
  dispatch; conformance parity with `RedbBackend`) ☑
- Phase 2 closeout (bump workspace + Docker artifacts to
  `2.0.0-alpha.2`; CHANGELOG tagline; phase-status roll-up) ☑

Jepsen-lite linearizability coverage and multi-range madsim
scenarios that specifically stress split/leader-loss/merge-
correctness remain on the cross-phase checklist — they grow
incrementally alongside the consistency / transaction semantics
each later phase introduces. **Phase 3 — distributed query** is the
next active phase.

Cross-repo handoff / remaining-work view: `../../aresadb.md` in the local
`yoreai/` workspace.
Short version after the alpha.2 closeout:

- **Manual publication work:** v1 technical-report Zenodo upload + DOI
  stamp-back through Genass and Aresalab.
- **Benchmark work:** grow `benches/v2_cluster_bench.rs` from the
  alpha scaffold into a production-shaped 3-node suite before writing
  the v2 companion note.
- **Implementation work:** Phase 3 distributed query, then Phase 4
  transactions, Phase 5 performance / custom LSM, and Phase 6 CDC +
  online schema.

---

## Phase 0 — Foundations ☑

Goal: set up the workspace, introduce the `StorageBackend` trait,
stand up the `madsim` test harness. **No behavior change.** Every
existing test must still pass against the new layout.

Phase 0 was deliberately scoped small: rather than moving thousands
of lines of existing code into new crates, we added the new crates
as siblings and kept the existing `aresadb` crate untouched. Big
moves (`aresadb-query`, `aresadb-server`, …) happen naturally as
each subsequent phase touches the relevant code.

- [x] Cargo workspace root (`Cargo.toml` now has a `[workspace]` section)
- [x] `crates/aresadb-core` — `StorageBackend` trait + `WriteBatch`,
      `KeyRange`, `Snapshot`, `KeyValueStream`, `MemoryBackend`
- [x] `crates/aresadb-sim` — `Scenario` trait + smoke scenario,
      madsim as first-class dependency
- [x] Version bump: `2.0.0-alpha.0` (root crate, `python/Cargo.toml`,
      `python/pyproject.toml`)
- [x] CHANGELOG entry for `2.0.0-alpha.0`
- [x] `docs/architecture-v2.md` ratified
- [x] This status doc created

**Exit criteria achieved:**

1. `cargo test --workspace` — ☑ 353 tests pass (up from 339 in v1 due
   to 14 new core tests + 1 sim test).
2. `cargo clippy -p aresadb-core -p aresadb-sim --all-targets -D warnings` — ☑ clean.
3. `cargo check --workspace` — ☑ clean.

**Deferred to natural boundaries in later phases:**

- `crates/aresadb-engine-redb/`, `crates/aresadb-query/`,
  `crates/aresadb-server/`, `crates/aresadb-client/`, `crates/aresadb/`
  — extracted one at a time as Phase 1 / 3 touch each subsystem.
- CI workflow updates for workspace benches — done when Phase 1 adds
  the gRPC / openraft matrix.
- Release pipeline update for pre-releases — done when Phase 1 tags
  `v2.0.0-alpha.1`.

---

## Phase 1 — Single-shard cluster (4-6 wk)

- [x] `crates/aresadb-raft/` — openraft integration
    - [x] Raft log on top of `StorageBackend` (`LogStore`)
    - [x] Raft state machine = `WriteBatch` apply hook (`StateMachineStore`)
    - [x] Single-node loopback network + end-to-end harness (`SingleNode`)
    - [x] Conformance tests via `openraft::testing::Suite`
- [x] `crates/aresadb-net/` — tonic gRPC transport
    - [x] `raft.proto` + vendored protoc (no cmake dep)
    - [x] `RaftGrpcServer` — adapter over `openraft::Raft`
    - [x] `GrpcRaftNetwork` — `RaftNetworkFactory` over tonic
    - [x] Two-node integration test — real gRPC, real election, real commit
    - [x] Three-node integration test — 25 batched writes replicate across all voters
- [x] `crates/aresadb-engine-redb/` — durable `StorageBackend`
      on top of redb (single table, async wrapper over
      `spawn_blocking`, `RedbSnapshot` for point-in-time reads)
- [x] `crates/aresadb-cluster/` — cluster lifecycle + admin surface
    - [x] `NodeConfig` — declarative node identity + paths
    - [x] `ClusterNode` — bootstrap / start / shutdown with Raft +
          log backend + data backend + dual gRPC server (Raft +
          admin) + membership watcher
    - [x] `StateMachineStore` persists `last_applied` /
          `last_membership` at `0xff/sm/meta` so recovery is durable
    - [x] `admin.proto` — `Initialize` / `AddLearner` /
          `ChangeMembership` / `Write` / `Read` / `Status`
    - [x] `aresadb-cluster` CLI — operator commands over admin gRPC
- [x] Three-node **durable** cluster test: redb-backed, graceful
      shutdown, cold restart, all committed writes survive, new
      writes still replicate (`aresadb-cluster/tests/three_node_durable.rs`)
- [x] **Single Raft group replicating one "database" across N nodes**
      — delivered by `ClusterNode` + `three_node_durable` +
      `leader_failover`. Range-based sharding arrives in Phase 2.
- [x] **3-node Docker Compose integration** —
      `docker/cluster/{Dockerfile,docker-compose.yml,bootstrap.sh}`
      brings up a real three-voter deployment with named volumes,
      healthchecks that hit the admin gRPC service, and a one-shot
      bootstrap script.
- [x] **Leader failover chaos test** —
      `aresadb-cluster/tests/leader_failover.rs` kills the current
      leader, waits for re-election on the surviving voters,
      confirms writes land on the new leader, then rejoins the
      killed node and verifies it catches up.
- [x] **Raft apply-determinism sim scenario** —
      `aresadb-sim::RaftApplyDeterminism` runs the same command
      script on two independent `SingleNode` harnesses and proves
      their final user-visible state is byte-identical.
- [ ] Leader lease reads (deferred — current read path goes
      through the state machine on every node, which is already
      linearizable-after-apply; leader leases land with Phase 4's
      transaction path)
- [x] Replace `src/distributed/wal.rs` stub — Raft
      (`aresadb-raft::LogStore`) is now the authoritative write-ahead log
- [ ] Jepsen-lite linearizability run (promoted to the cross-phase
      checklist — scope grows with every later phase)
- [x] Tag: `v2.0.0-alpha.1`

## Phase 2 — Multi-Raft + range sharding (6-8 wk)

- [x] **Phase 2a — Unified key encoding for all five models**
      (`aresadb-core::keys`). 11-variant `Key` enum per §3.1, CRDB-style
      escape + terminator encoding between variable segments so lex
      order matches logical order and prefix scans are exact. 23 unit
      tests cover round-trip / sort order / prefix containment /
      malformed-input rejection.
- [x] **Phase 2b — Placement Driver** (split into four slices so each
      lands a checkpoint without growing the blast radius) — complete
    - [x] **Phase 2b-1 — Catalog core (`aresadb-pd`).** Pure-logic
          catalog over `RangeDescriptor`, with `PdCommand` /
          `PdResponse`, secondary indices (`by_start`, `by_group`),
          and invariant enforcement: no overlapping ranges, unique
          group ids, monotonic epoch / heartbeat, split preserves
          coverage + bumps generation, merge requires adjacency +
          matching replica set. `apply(PdCommand)` mirrors the Raft
          state-machine pattern so 2b-2 becomes a thin adapter. 48
          unit tests (serde / bincode round-trip, every mutator
          success path, every rejection path, six-range split-walk
          stress test).
    - [x] **Phase 2b-2 — Persistent PD state machine
          (`aresadb_pd::state_machine`).** `PdStateMachine` binds a
          `Catalog` to any `aresadb_core::StorageBackend`: every
          accepted command mutates the in-memory catalog and writes
          the touched rows atomically, then flushes. Key layout
          `/m/pd/r/<range_id_be>` / `/m/pd/n/<node_id_be>` under the
          unified keyspace's `/m/` metadata prefix.
          `next_range_id` is derived on open from `max(range_id)+1`
          rather than persisted, so a partial-write split never
          leaves the counter lagging live ranges. Apply path
          serializes on a `tokio` mutex; read path uses
          `parking_lot::RwLock` and never blocks on I/O. 18
          additional unit tests including a full redb round-trip
          (create → split → lease → drop → reopen → verify).
    - [x] **Phase 2b-3 — Single-node PD + 3-node PD Raft cluster.**
          `aresadb_pd::raft::PdTypeConfig` replicates `PdCommand` /
          `PdResponse` through its own openraft group. The Phase 1
          `aresadb-raft::LogStore` was generalized to
          `LogStoreGeneric<C: RaftTypeConfig>` so the PD group reuses
          the same battle-tested log persistence (the previous
          `LogStore` type alias keeps Phase 1 callers unchanged).
          `PdRaftStateMachine` wraps `PdStateMachine` and implements
          `RaftStateMachine<PdTypeConfig>` + `RaftSnapshotBuilder`:
          `last_applied` / `last_membership` persist at
          `b"\xff/pd/sm/meta"` in the **same `WriteBatch`** as each
          catalog mutation (`apply_with_meta`) so a crash can never
          advance Raft's applied pointer past a catalog row that
          didn't land. Snapshots serialize the full catalog +
          `next_range_id` + Raft meta and, on install, wipe
          `/m/pd/r/*` + `/m/pd/n/*` with a pair of `delete_range`s
          inside one batch before re-hydrating. An in-process
          `PdRouter` / `PdRouterNetwork` delivers RPCs between
          members as direct `openraft::Raft::{append_entries, vote,
          install_snapshot}` calls; `isolate` / `reconnect` hooks
          simulate partitions. `SinglePdNode` bundles the pieces
          into a one-voter harness (mirrors
          `aresadb-raft::SingleNode`) and `PdCluster` grows it to N
          voters, including `wait_for_leader`,
          `wait_for_replication`, `partition` / `heal`, and
          `restart(node_id)` (drops one member's Raft handle and
          reopens on the same backends). 20 new raft-module unit
          tests + 6 end-to-end integration tests cover elections,
          follower convergence, ForwardToLeader forwarding,
          partition + heal, single-member restart, full-cluster
          process restart on redb, follower churn under load, and
          a 50-split stress. **97 PD tests total, all passing.**
    - [x] **Phase 2b-4 — Admin RPCs + heartbeats + CLI.**
          `pd.proto` (package `aresadb.pd.v1`) defines 15 strongly-
          typed RPCs — 7 mutations that funnel through the PD Raft
          log (`RegisterNode`, `HeartbeatNode`, `CreateRange`,
          `SplitRange`, `MergeRanges`, `UpdateMembership`,
          `UpdateLease`) and 8 reads served from the local
          state machine (`GetRange`, `GetRangeByKey`, `ListRanges`,
          `GetNode`, `ListNodes`, `Status`). Unlike `aresadb-net`'s
          bincode-over-`bytes` Raft transport, the admin API is
          fully typed so any language binding can drive it.
          `PdAdminService` adapts `openraft::Raft<PdTypeConfig>` +
          `PdStateMachine` to the tonic service trait; error mapping
          is deliberate: `InvalidArgument` for malformed requests
          (never touches Raft), `FailedPrecondition` for catalog
          rejections (did replicate, state machine declined),
          `Unavailable` for Raft errors with a `pd-leader-id`
          metadata trailer on `ForwardToLeader` so clients retry in
          one hop. `PdAdminClient` is a typed Rust wrapper that
          returns native types (`RangeDescriptor`, `NodeInfo`,
          `serde_json::Value`) and surfaces leader hints as a
          dedicated `PdAdminClientError::NotLeader(Option<id>)`
          variant — the `Status` payload is boxed so the error enum
          stays small on hot paths. `HeartbeatLoop` spawns a
          cancellation-safe background task: configurable interval,
          optional `EndpointResolver` for following leader hints, a
          swappable `ClockFn` for deterministic tests, and graceful
          shutdown via `HeartbeatHandle::stop()` or drop.
          `aresadb-cluster` grows a 15-subcommand `pd` group
          (`status`, `list-ranges`, `list-nodes`, `get-range`,
          `get-range-by-key`, `get-node`, `register`, `heartbeat`,
          `heartbeat-loop`, `create-range`, `split-range`,
          `merge-ranges`, `install-lease`, `clear-lease`,
          `update-membership`) with UTF-8-string keys, `N:S[:role]`
          replica specs, `ID=URL` peer maps, and leader-hint-aware
          error messages. 36 new tests: 18 admin unit tests +
          6 end-to-end admin-integration tests (3-node cluster
          driven over real tonic channels; covers register /
          heartbeat / create / split / lease / membership, forward-
          to-leader with `pd-leader-id` hint, catalog-rejection
          classification, a live `HeartbeatLoop` advancing
          `last_heartbeat_millis` in the quorum, and cross-member
          `Status` consistency) + 12 CLI tests (11 parser units,
          1 `CARGO_BIN_EXE_aresadb-cluster`-driven smoke test that
          round-trips `status → create-range → list-ranges → register
          → heartbeat → get-node → split-range → get-range-by-key`
          through the compiled binary). **Totals after 2b-4**: 121
          PD tests, 18 `aresadb-cluster` tests, all passing;
          `clippy -D warnings` clean on both crates.
- [ ] **Phase 2c — Range-aware `ClusterNode`** (split into six slices
      so each lands a tested checkpoint without destabilizing the
      single-Raft behavior that Phase 1 shipped)
    - [x] **Phase 2c-1 — Multi-Raft wire + dispatch.** Extended
          `aresadb-net/proto/raft.proto` with a `raft_group_id`
          envelope field on every RPC (`AppendEntries`, `Vote`,
          `InstallSnapshot`); added `aresadb_net::SINGLETON_RAFT_GROUP_ID
          = 0` so pre-2c members continue to emit/accept the default.
          Introduced the `RaftDirectory` trait (one lookup:
          `raft_for(group_id) -> Option<Raft<TypeConfig>>`) and
          refactored `RaftGrpcServer` to route every handler through a
          single `resolve(group_id)` helper that returns
          `tonic::Code::NotFound` for unregistered ids.
          `RaftGrpcServer::new(raft)` now wraps the handle in a
          `SingletonRaftDirectory` so the one-liner Phase 1 construction
          keeps working; multi-range deployments call
          `RaftGrpcServer::from_directory(Arc::new(…))`. On the client
          side, `GrpcRaftNetwork::new(directory, raft_group_id)`
          stamps the id into every outgoing RPC, with
          `GrpcRaftNetwork::new_singleton(directory)` pinning the id to
          `SINGLETON_RAFT_GROUP_ID` for back-compat. **Tests**: new
          `multi_group_dispatch.rs` integration test boots two
          independent `SingleNode`s at group ids 10/20, fronts them
          with a `MultiGroupDirectory` behind one `RaftGrpcServer`,
          and drives real tonic traffic — verifying (a) each group sees
          only its own RPCs, (b) unknown group id surfaces as
          `NotFound`; the existing `two_node_cluster.rs` and
          `three_node_cluster.rs` transport tests still pass via the
          singleton adapters. **Totals after 2c-1**: 9 `aresadb-net`
          tests (4 unit + 3 multi-group dispatch + 1+1 single-group
          integration) alongside the full 560-test workspace suite,
          all green; `cargo clippy -- -D warnings` clean on every v2
          crate.
    - [x] **Phase 2c-2 — `RangeRuntime`.** Shipped
          `aresadb_cluster::range::RangeRuntime` owning one range's
          `(RangeDescriptor, openraft::Raft<TypeConfig>, log backend,
          data backend)` under `<data-dir>/ranges/<range_id>/{log,
          data}/`, generic over `N: RaftNetworkFactory<TypeConfig>`
          so production wires it with a group-scoped
          `aresadb_net::GrpcRaftNetwork::new(directory, group_id)`
          while tests plug in `aresadb_raft::LoopbackNetwork`.
          Lifecycle: `open` / `open_on_disk` (derives paths from
          `NodeConfig`, opens redb backends, calls `open`) /
          `bootstrap_voter` (idempotent on recovery — matches
          `InitializeError::NotAllowed` on the typed error variant
          instead of string-probing the error's display, and applies
          the same fix to `ClusterNode::bootstrap_single`) /
          `trigger_snapshot` / `shutdown`. Per-range backends mean
          `aresadb_core::FORMAT_VERSION` bumps and the `\xff/sm/meta`
          key are both scoped per-range, so rolling format migrations
          land one range at a time. New `NodeConfig` accessors
          (`ranges_root`, `range_dir`, `range_log_dir`,
          `range_data_dir`, `range_log_path`, `range_data_path`,
          `ensure_range_dirs`) give every caller one source of truth
          for layout. **Totals after 2c-2**: 12 `aresadb-cluster` lib
          unit tests (up from 4) — 5 new in `range::tests` (on-disk
          layout, bootstrap + replicated write, reopen rehydration,
          two-range isolation on one node, snapshot trigger) + 3
          new in `config::tests` (path builders, id-collision-free
          paths, `ensure_range_dirs` idempotency). The existing 11
          CLI unit tests + 3 integration tests (`leader_failover`,
          `pd_cli_smoke`, `three_node_durable`) still pass;
          `cargo clippy -- -D warnings` is clean on every v2 crate.
    - [x] **Phase 2c-3 — Range-aware `ClusterNode`.** `ClusterNode`
          now owns an `Arc<RangeDirectory>` — a dual-indexed
          (`HashMap<RangeId, Arc<RangeRuntime>>` +
          `HashMap<GroupId, Arc<RangeRuntime>>`) registry of every
          range this node serves, exposed as an `aresadb_net::RaftDirectory`
          so the one gRPC listener fans inbound Raft RPCs out by
          `raft_group_id`. Back-compat default range: every
          `start()` call opens range id `DEFAULT_RANGE_ID = 1`
          (raft_group_id `1`) spanning `[min, +inf)`; legacy
          accessors (`raft()`, `data()`, `log_backend()`) forward to
          it, so Phase 1 CLI / admin / integration tests are
          unchanged. New admin RPCs `AddRange` / `RemoveRange` /
          `ListRanges` (`proto/admin.proto` +
          `ClusterAdminClient`) let external operators (and
          eventually the Phase 2c-4 PD supervisor) mutate the
          directory over tonic — with a pre-flight duplicate probe
          so `ALREADY_EXISTS` wins over redb's file-lock error on
          duplicates, `INVALID_ARGUMENT` for zero / empty-span
          descriptors, `NOT_FOUND` for removing unknown ranges, and
          `FAILED_PRECONDITION` for removes with live references
          (bypassable via `force=true`). `AddRange` defaults
          `raft_group_id = range_id` when zero and optionally
          self-bootstraps the new range as a single voter via
          `RangeRuntime::bootstrap_voter_with_addr` (which seeds the
          peer directory via `BasicNode::new(addr)` for the
          membership watcher). **Totals after 2c-3**: 16
          `aresadb-cluster` lib unit tests (up from 12; 4 new
          `RangeDirectory` tests), 11 CLI unit tests, 4 integration
          test binaries (`leader_failover`, `pd_cli_smoke`,
          `three_node_durable`, **`range_admin_rpcs`** with 3
          tests — full RPC round-trip, uninitialised-AddRange,
          two-range storage isolation). `cargo clippy -- -D warnings`
          clean across every v2 crate. **On-disk break**: the new
          layout (`<data-dir>/ranges/<range_id>/{log,data}/`)
          supersedes Phase 1's
          `<data-dir>/{raft_log,state_machine}/`; pre-alpha
          clusters need to re-bootstrap.
    - [x] **Phase 2c-4 — PD-driven orchestration (add / remove).**
          Shipped `aresadb-cluster::pd_supervisor`: a configurable
          supervisor task that owns the heartbeat loop
          (`aresadb_pd::admin::HeartbeatLoop`) plus an independent
          reconcile loop calling `list_ranges` on PD, diffing
          against the local `RangeDirectory` via the pure-logic
          `plan_reconcile` function, and applying the plan with
          the shared executor (open per-range backends under
          `<data-dir>/ranges/<range_id>/`, register into the
          directory; or drop the last `Arc<RangeRuntime>` on
          remove). `ClusterNode::attach_pd_supervisor` + `::
          start_with_pd` + `::has_pd_supervisor` wire it into the
          lifecycle — attach performs the first `register_node`
          synchronously so a successful return means the node is
          already in the catalog, and `ClusterNode::shutdown`
          stops the supervisor before draining the directory.
          Tests: 10 new `reconciler` unit tests (every pairwise
          combination of present / absent / skipped), 6 new
          `executor` + `config` + `supervisor` unit tests, and a
          new `pd_supervisor_integration.rs` binary with 2 tests
          against a real 3-node PD cluster + a real
          `ClusterNode` (happy-path convergence and
          filter-ranges-not-assigned-to-this-node). `cargo clippy
          -- -D warnings` clean across every v2 crate. **Scope
          note**: splits and merges deferred to Phase 2c-5+;
          `skip_local_ranges` defaults to `{DEFAULT_RANGE_ID}` so
          the back-compat default range is never reconciled
          away.
    - [x] **Phase 2c-5 — Range leader leases.** `RangeRuntime`
          now exposes the §4.3 read taxonomy:
          `leadership_status()` returns a plain-data snapshot
          (`is_leader`, `current_leader`, `current_term`,
          `last_log_index`, `last_applied_index`, `voter_count`,
          plus derived `apply_lag()`) pulled from the openraft
          metrics watch — cheap, no network I/O;
          `ensure_linearizable()` wraps openraft's ReadIndex +
          wait-for-apply and returns a typed `ReadError`
          (`NotLeader(Option<NodeId>)` with leader hint,
          `QuorumUnavailable`, `Fatal`, `Storage`); convenience
          `linearizable_get(key)` and `stale_get(key)` cover the
          leader-lease and bounded-staleness rows of the §4.3
          table. The admin `Read` RPC gains a `ReadConsistency`
          enum (`UNSPECIFIED` — Phase 1c back-compat raw read on
          the default range, `LINEARIZABLE`, `STALE`), an
          optional `range_id` (0 resolves to `DEFAULT_RANGE_ID`),
          a `read_log_index` field on `ReadResponse`, and a
          `x-aresa-leader-id` metadata header on
          `FAILED_PRECONDITION` rejections so clients can
          re-route without string-parsing. CLI `read` grows
          `--consistency [unspecified|linearizable|stale]` and
          `--range-id <u64>`. Tests: 5 new unit tests in
          `range::tests` (leadership before/after bootstrap,
          linearizable_get, stale_get, ensure_linearizable
          not-leader conversion) and a new
          `range_leader_leases.rs` integration binary with 4
          tests on a real 3-voter redb-backed cluster —
          linearizable read on leader, not-leader rejection on
          follower with leader-id header, stale read propagation
          to followers, and linearizable reads following a
          leader failover with updated hints on the surviving
          followers. Totals: **51 lib unit tests** (up from 46),
          6 integration binaries, `cargo clippy -- -D warnings`
          clean. **Scope note**: this ships the openraft-backed
          ReadIndex + wait-for-apply path; a pure lease-only
          fast path that skips heartbeat round-trips, and the PD
          `LeaseInfo` machinery for coordinated leader transfer
          during rebalancing, are both later-phase concerns.
    - [x] **Phase 2c-6 — Multi-range madsim + Docker smoke.**
          Closes the Phase 2c arc. Two independent lanes land
          together: a deterministic-simulation scenario that
          exercises the per-range apply path and proves cross-
          range isolation, plus an operator-driven Docker smoke
          test that drives the range-aware admin surface on a
          real 3-node cluster.
          **Data-plane surface.** `aresadb.cluster.v1.WriteRequest`
          gains a `range_id` field (and `WriteResponse` echoes it
          back). `range_id = 0` preserves the Phase 1c wire —
          routes to the default range via the admin service's
          default Raft handle. Non-zero values look up the range
          in the local `RangeDirectory` and fire `client_write`
          against that range's own Raft, returning `NOT_FOUND`
          for unregistered ranges and `FAILED_PRECONDITION` with
          `x-aresa-leader-id` metadata on `ForwardToLeader`.
          A new `WriteError` / `WriteResult` pair in
          `aresadb_cluster::error` (plus `write_status` helper in
          `admin.rs`) mirrors the Phase 2c-5 read-path design for
          every `ClientWriteError` variant openraft can produce.
          **CLI surface.** `aresadb-cluster` grows `add-range`,
          `remove-range`, `list-ranges` subcommands plus a
          `--range-id` flag on `write`. The range-admin
          subcommands are thin wrappers over Phase 2c-3c's
          `ClusterAdminClient::{add_range,remove_range,list_ranges}` —
          previously callable only in tests.
          **Madsim scenario.** `MultiRangeApplyDeterminism` drives
          N independent `SingleNode` Raft groups in parallel
          (one per range) through a ~320-op interleaved script,
          routing each op by longest-prefix match. Asserts
          per-range determinism *and* cross-range isolation:
          no range's backend contains a key that routes to a
          different range. Default layout is four ranges on
          `r1/`, `r2/`, `r3/`, `r4/`; unrouted ops fail loudly.
          **Docker smoke** (`docker/cluster/multi-range.sh`).
          Assumes `bootstrap.sh` has formed the default-range
          cluster, then opens range `42` on node-1 as a single
          voter, writes `r42/hello = phase-2c-6` through the
          range-aware `Write`, reads it back under both
          `linearizable` and `stale`, and verifies node-2 /
          node-3 return `NOT_FOUND` for range 42 — cross-process
          isolation, same invariant as the madsim scenario but
          across real containers on a real gRPC network.
          Multi-node replication of non-default ranges stays
          out of scope: the admin `AddLearner` /
          `ChangeMembership` RPCs still target the default range
          only. That's on Phase 2d alongside PD-driven split
          execution.
          **Tests.** `aresadb-sim` grew from 4 to 7 passing unit
          tests (happy path + longest-prefix routing + unrouted-
          op rejection + empty-script rejection). `aresadb-
          cluster` still passes all 58 tests across library + 7
          integration binaries — integration tests updated for
          `WriteRequest::range_id: 0` default, every existing
          case unchanged. `cargo check --workspace --all-targets`
          and `bash -n docker/cluster/multi-range.sh` clean.
- [x] **Phase 2d — `crates/aresadb-engine-lsm/`** — fjall-3.1-backed
      LSM implements `aresadb_core::StorageBackend`. The new
      `aresadb_cluster::NodeConfig::data_engine` (`DataEngine::{Redb,
      Lsm}`) opts a node into the LSM *data* backend at
      `<data-dir>/ranges/<range_id>/data.lsm/`; the Raft log stays
      on redb (`data.redb`) because append-heavy fsync-per-commit
      workloads don't benefit from an LSM's write amplification.
      `FjallBackend` wraps one `fjall::Database` + single `"default"`
      `Keyspace`, routes every synchronous fjall call through
      `tokio::task::spawn_blocking`, persists batches with
      `OwnedWriteBatch::commit()` + `Database::persist(PersistMode::
      SyncAll)` so durability matches redb's fsync-per-commit
      contract, and eagerly materialises `Snapshot` / `scan` results
      into `Vec<KeyValue>` for `Send + 'static` parity with the
      `RedbBackend`. Range-delete is O(N) via batched individual
      deletes (fjall has no native range tombstone); acceptable for
      Raft log purges and the log suffix's `delete_range` pattern.
      `approximate_size` returns `0` — fjall's `disk_space()` is
      whole-keyspace, not range-aware, and the PD split heuristics
      already treat the hint as advisory. **Tests**: 13 new unit
      tests in `aresadb-engine-lsm::tests` (put/get round-trip,
      durability across reopen, snapshot isolation, range-scan
      bounds inclusive/exclusive, full-range scan ordering,
      `delete_range` parity with `MemoryBackend`, `flush`
      idempotency, `close` → `Closed` transition); 1 new
      integration-shaped test in `aresadb-cluster`
      (`range::tests::lsm_data_engine_persists_committed_writes_
      across_reopen`) drives the full `open_on_disk` →
      `bootstrap_voter` → Raft `client_write` → `shutdown` →
      reopen path on `DataEngine::Lsm` and asserts the write
      survives + the `data.lsm` suffix is a directory. `cargo
      test -p aresadb-cluster -p aresadb-engine-lsm` green; 63
      tests (50 cluster lib + 13 engine-lsm) plus every existing
      integration binary still passing on the default redb
      engine. **Scope note**: an `LsmOptions` knob surface
      (compaction workers, block-cache size, bloom-filter bits)
      is deliberately deferred — fjall defaults are adequate at
      the sizes we run in CI, and real tuning should land with
      real benchmark numbers rather than speculative dials. Multi-
      range madsim scenarios that specifically stress the LSM
      backend are left to a later phase.
- [ ] Multi-range madsim scenarios in `aresadb-sim` that
      specifically stress split-during-writes, leader-loss +
      rebalance, and merge correctness (Phase 2c-6 covered cross-
      range apply determinism + isolation; the split / rebalance /
      merge scenarios belong with Phase 3's distributed-query
      work since they require PD-driven split execution, which is
      not yet wired up at the node side).
- [x] Tag: `v2.0.0-alpha.2`

## Phase 3 — Distributed query (4-6 wk)

- [ ] Query router + physical planner with range awareness
- [ ] Filter / projection / limit push-down to data nodes
- [ ] Scatter-gather executor
- [ ] Distributed graph BFS (frontier batching)
- [ ] Distributed vector search (per-range HNSW + global top-k merge)
- [ ] Distributed full-text (cluster-wide DF cache)
- [ ] Tag: `v2.0.0-beta.0`

## Phase 4 — Distributed transactions (6-8 wk)

- [ ] HLC clocks
- [ ] MVCC value layer (versioned keys, intents, GC)
- [ ] Single-shard transactions
- [ ] Cross-shard parallel commit
- [ ] Serializable Snapshot Isolation with timestamp cache
- [ ] Read Committed mode for compatibility
- [ ] Jepsen consistency tests pass under partitions
- [ ] Tag: `v2.0.0-beta.1`

## Phase 5 — Custom thread-per-core LSM engine (4-6 wk)

- [ ] `crates/aresadb-engine-lsm-tpc/` — custom engine
- [ ] Thread-per-core runtime (pinned threads with `tokio::runtime::Builder::new_current_thread`)
- [ ] io_uring file I/O via `tokio-uring`
- [ ] Memtable / SSTable / Bloom filters / leveled compaction
- [ ] Opt-in flag `--engine=lsm-tpc`
- [ ] Public benchmark report vs. CockroachDB, TiKV, ScyllaDB
- [ ] Tag: `v2.0.0-rc.0`

## Phase 6 — CDC + online schema changes (6-8 wk)

- [ ] Per-range Raft change events → dispatcher → subscribers
- [ ] `SUBSCRIBE` API over gateway
- [ ] CRDB-style multi-version schema changes (DELETE_ONLY → WRITE_ONLY → PUBLIC)
- [ ] Online `CREATE INDEX` / `CREATE FULLTEXT INDEX` / `DROP INDEX` on distributed data
- [ ] Continuous incremental backup (Raft log tailing to object storage)
- [ ] Operator guide, troubleshooting playbook
- [ ] Tag: `v2.0.0`

---

## Cross-phase work

- [ ] Jepsen test suite (Phase 1 initial, grows every phase)
- [ ] madsim simulation coverage expands every phase (goal: ≥ 1 000 scenarios per CI run by Phase 4)
- [ ] Public benchmark harness vs. CockroachDB, TiKV, ScyllaDB (starts Phase 1 baseline, final Phase 5)
- [ ] Docs kept in sync — `architecture-v2.md`, `consistency-model.md` (new, Phase 4), `operations.md` (new, Phase 1)

---

## Decision log

Non-trivial decisions made during execution that deviate from or refine the
architecture doc go here.

| Date | Decision | Context |
|------|----------|---------|
| 2026-04-11 | Raft log entries and state machine data live on **separate `StorageBackend` instances** (see `aresadb-raft/src/log_store.rs`). | Lets the log engine get tuned independently (future: append-only log-structured backend) while the data engine keeps whatever layout the user picks. Tradeoff: two backends per node; acceptable since v1 already supports in-process construction of multiple engines. |
| 2026-04-11 | Raft RPCs wrap a **single `bytes payload` field in protobuf**, with bincode-encoded openraft types inside. | Faithful proto schemas for `Entry<TypeConfig>` / `RaftError<…>` would require conversion code that tracks every openraft release. Wire consumers are always AresaDB nodes, so the client protocol (SQL/gRPC) will get its own typed schema later. |
| 2026-04-11 | `protoc` ships via **`protoc-bin-vendored`** instead of `protobuf-src`. | `protobuf-src` requires cmake; `protoc-bin-vendored` ships a prebuilt binary per target. Zero external toolchain on macOS/Linux CI. |
| 2026-04-11 | State-machine metadata (`last_applied`, `last_membership`) is persisted to the **data backend** under the reserved `0xff/sm/meta` key rather than a third backend. | Keeps the atomicity story simple: every `apply` batch writes user ops + SM meta in the same `WriteBatch`, so either the apply is fully durable or none of it is. User-visible snapshots exclude the `0xff`-prefixed reserved range. |
| 2026-04-11 | Peer discovery is a **`StaticPeerDirectory` + membership-watcher task** that tails `openraft::Raft::metrics` and upserts each voter/learner as soon as it commits. | openraft's `RaftNetworkFactory` wants synchronous `get_endpoint(NodeId)`; we can't block the replication loop on async service discovery. Watching metrics keeps the directory correct across joins, leaves, and restarts without adding a new RPC path. |
| 2026-04-11 | Raft admin and Raft transport share **one gRPC server** per node. | Avoids binding two ports per node and simplifies `NodeConfig`. Admin is an internal-facing API; placing it behind a separate gateway service is Phase 3+. |
| 2026-04-11 | Phase 2a ships the unified keyspace codec as **ASCII-prefixed** (`/n/`, `/ef/`, `/p/`, …) matching architecture-v2.md §3.1 verbatim, rather than compact 1-byte binary tags. | The 2-byte overhead per key is modest (1-2%) and the human-readable prefixes make operator dumps (`aresadb-cluster status`, range-debug logs) trivially greppable. Re-encoding to binary later is a pure data migration and can wait until a profile justifies it. |
| 2026-04-11 | Phase 2a encodes variable segments with **CRDB-style escape + terminator** (`0x00` → `0x00 0xff`; segments separated by `0x00 0x01`; last segment written raw). | Gives lexicographic sort on the encoded bytes == logical sort on the structured key, without reserving any byte the caller cannot emit. Prefix-range scans on partial keys (`/ef/<from><0x00 0x01>…`) are exact. Length-prefix alternatives sort wrong on variable-length segments. |
| 2026-04-11 | Phase 2b-1 **splits `RangeId` allocation off the `SplitRange` command** — the catalog owns a replicated `next_range_id` counter and hands out the new id during `apply()`. | The command is the replicated log entry; every replica must produce the same post-apply state deterministically. Carrying the id on the command makes the client an id-allocation source of truth, which races with other clients; carrying it in the catalog keeps the id stream serializable through the PD Raft group for free. |
| 2026-04-11 | Phase 2b-1 ships the placement-driver catalog as **pure logic first** (`aresadb_pd::catalog::Catalog`); Raft wrapping happens in 2b-2 with zero interface churn. | Mirrors the Phase 1 split between `StateMachineStore` (invariant logic) and `ClusterNode` (Raft + transport + admin). Lets the catalog's invariants — no overlap, monotonic epoch, split preserves coverage, merge preconditions — be property-tested against a plain `Catalog` instance with zero async or I/O in the test loop. |
| 2026-04-11 | Phase 2b-1 makes **split drop the parent's lease** (and never carry a lease on the new RHS). | The lease covers the parent's *old* span; once `end_key` shrinks, the part of the keyspace that used to be covered is now in the RHS with a fresh Raft group. Rather than try to surgically split the lease window, the catalog invalidates it and lets the new leaders re-elect on both halves. Cost is a few extra round-trips after every split, paid by whoever initiated the split. |
| 2026-04-11 | Phase 2b-2 **derives `next_range_id` from disk on open** (`max(range_id) + 1`) instead of persisting a dedicated counter row. | A dedicated counter row risks lagging reality on a partial-write crash (counter committed, the range that used it not committed, or vice versa — either way, allocating the next split would re-use or skip ids). Deriving from the max observed id means the counter is always at least one past every persisted range, regardless of partial writes. Cost: an `O(range_count)` scan at open, which is dominated by rehydration anyway. |
| 2026-04-11 | Phase 2b-2 **gives up on the state machine** if a backend write fails mid-apply. | Once the in-memory catalog mutated and the disk write returned an error, the two are out of sync and no cheap reconciliation exists (we'd need to either rollback the catalog — no transaction API — or redo the write on an unknown backend state). Instead, return `PdApplyError::Backend`, let the caller (the 2b-3 Raft state-machine adapter) propagate it to openraft, and rely on the node-restart path to rehydrate from the last-durable disk state. Matches Phase 1's `StateMachineStore` discipline. |
| 2026-04-11 | Phase 2b-2 uses a **`tokio::sync::Mutex` for apply serialization** rather than funneling applies through a single task's channel. | Channels add a hop and an extra allocation per apply. The mutex lets openraft's state-machine driver call `apply` directly; serialization is provided by the lock's FIFO discipline. Reads go through a separate `parking_lot::RwLock` so admin / range-lookup traffic never blocks on the apply lock. |
| 2026-04-11 | Phase 2b-3 **generalizes `aresadb-raft::LogStore` to `LogStoreGeneric<C: RaftTypeConfig>`** and keeps `pub type LogStore = LogStoreGeneric<TypeConfig>` as a Phase 1 alias. The error-mapping helpers (`storage_err`, `storage_err_ctx`, `BincodeError`) are made `pub` and generic over `N: openraft::NodeId`. | The PD Raft group needs the same byte-layout / fsync discipline as the user-data group, but over `PdCommand` / `PdResponse` instead of `AresaCommand` / `AresaResponse`. Duplicating the 500-line `LogStore` would immediately drift. Parameterizing on `C` keeps both groups on the same proven path; exposing the error helpers means `aresadb-pd` doesn't reinvent `StorageError<N>` wrapping. The trade-off — a `C::NodeId: Copy` bound on `LogStoreGeneric` — matches openraft's own expectations for `LogId<N>` and costs nothing for the `u64` node ids we actually use. |
| 2026-04-11 | Phase 2b-3 **persists Raft meta (`last_applied`, `last_membership`) inside the catalog's `WriteBatch`** rather than in a sibling flush. | `last_applied` must monotonically lead on-disk catalog rows: if the catalog row landed but Raft meta didn't, recovery replays the command and the catalog applies it twice (fine for CreateRange/idempotent ops, broken for SplitRange / UpdateLease). If Raft meta landed but the catalog row didn't, recovery advances past a command that never actually mutated the catalog. `apply_with_meta` bundles both rows into one atomic batch so neither direction is possible. Matches the Phase 1 `StateMachineStore` contract. |
| 2026-04-11 | Phase 2b-3 gives the PD Raft group its **own reserved meta key** `b"\xff/pd/sm/meta"`, distinct from Phase 1's user-data key `b"\xff/sm/meta"`. | A PD Raft member's catalog backend is guaranteed to live on a different `StorageBackend` from any user-data state machine (PD is a singleton cluster per AresaDB deployment; user data is sharded across ranges). The two keys only co-exist in unit tests, but keeping them separate now prevents a future refactor from accidentally packing both state machines into one backend and having them trample each other. |
| 2026-04-11 | Phase 2b-3 uses an **in-process `PdRouter`** for the N-node harness instead of the existing tonic gRPC transport in `aresadb-net`. | `aresadb-net`'s Raft RPC codec is hard-coded to `TypeConfig` (the user-data type config) and growing a PD-flavored gRPC surface is Phase 2b-4 work. In the meantime a `HashMap<NodeId, Raft<PdTypeConfig>>` + direct `.append_entries(...)` calls give us the exact same semantics — election timeouts, ForwardToLeader, install_snapshot — minus serialization. It also unlocks `isolate` / `reconnect` partition tests for free, which a gRPC transport would require test-only middleware to simulate. |
| 2026-04-11 | Phase 2b-3 `PdCluster` **only calls `initialize` once**, on the lowest-numbered node. `open_existing` skips `initialize` entirely. | openraft rejects repeated `initialize` calls on the same cluster. A fresh three-node cluster bootstraps by having one voter write the initial membership entry to its log; replication handles the rest. On restart, the persisted log already contains that membership entry, so openraft wakes up into the existing cluster without any bootstrap call. Splitting the two flows into `with_config` (fresh) vs `open_existing` (restart) makes the crash-restart integration test trivially correct. |
| 2026-04-11 | `PdCluster::default_config` sets **`SnapshotPolicy::Never`** so integration tests that care about snapshot timing drive them explicitly via `raft.trigger().snapshot()`. | Openraft's default policy is `LogsSinceLast(5000)`, which never fires in tests and makes install-snapshot coverage impossible. `Never` gives tests full control: `triggered`-snapshot tests call `trigger().snapshot()`, while the rest of the suite stays on log replay. Production clusters can override `default_config` through `PdCluster::with_config`. |
| 2026-04-11 | Phase 2b-4 makes the PD admin gRPC API **strongly typed** (`aresadb.pd.v1.RangeDescriptorPb`, `NodeInfoPb`, …) rather than reusing `aresadb-net`'s bincode-over-`bytes` Raft envelope. | The Raft transport is always AresaDB-to-AresaDB and must carry openraft types that change shape across releases, so an opaque payload is the right call there. The admin surface is the opposite: it's the contract every operator tool, client library, and future non-Rust binding talks to. Typed messages mean protoc-generated clients in any language can call `CreateRange` / `SplitRange` / `Status` without depending on Rust bincode layout, and schema evolution is a matter of adding fields the normal protobuf way. The one-way conversions (`From<Rust>` for pb) are total and the fallible direction (`TryFrom<pb>` for Rust) funnels through a single module with clear `InvalidArgument` rejections. |
| 2026-04-11 | Phase 2b-4 admin-error mapping pins **three tonic `Code`s**: `InvalidArgument` for malformed requests (never touches Raft), `FailedPrecondition` for catalog rejections (replicated, then declined), `Unavailable` for Raft errors including `ForwardToLeader` with a custom `pd-leader-id` metadata trailer. | Collapsing everything into a generic `Internal` would lose the information the operator needs to react. `InvalidArgument` tells the client its request is structurally wrong — don't retry. `FailedPrecondition` tells it the catalog reached consensus and still rejected — don't retry, fix the request. `Unavailable` with a leader-hint tells it to dial the leader and retry in one hop. The alternative — requiring clients to probe every member until one accepts the write — costs a round-trip per member on every not-leader write. `pd-leader-id` is a custom metadata trailer (not a standard gRPC convention) because there isn't a standard; documenting the key at the server boundary and parsing it in the typed client keeps the surface self-contained. |
| 2026-04-11 | `PdAdminClientError::Rpc` boxes the inner `tonic::Status` (`Rpc(Box<Status>)`). | `Status` is ~176 bytes on its own, which makes every `Result<T, PdAdminClientError>` the admin client returns 176+ bytes on the error side and trips `clippy::result_large_err`. The client is on the hot path (e.g. one RPC per heartbeat × N nodes × many seconds), so keeping the `Result` small matters. The `convert` and `server` modules can't box their error types — they implement trait signatures dictated by tonic — so those use targeted `#[allow(clippy::result_large_err)]` module attributes instead. |
| 2026-04-11 | `HeartbeatLoop` **drops its client connection** on any non-`NotLeader` error and reconnects on the next tick, instead of reusing the original channel. | The alternative — hold on to the channel and retry — means a heartbeat that failed because the server restarted keeps firing into a stale connection until tonic's keepalive notices. Dropping the channel turns every transient failure into a fresh dial, which pays a one-off ~RTT cost but converges on healthy members immediately when the PD cluster recovers from a restart / partition. `NotLeader` is treated differently because the channel is still live; the loop just rotates to the hinted endpoint if an `EndpointResolver` was supplied. |
| 2026-04-11 | `HeartbeatConfig::endpoint_for` is a user-supplied `Arc<dyn Fn(NodeId) -> Option<String>>` rather than a static peer map. | Static peer lists bake in the cluster topology at construction time, which is wrong for clusters that grow or shrink. A closure lets the CLI / node supervisor plug in whatever discovery source it wants — a static map today, a shared `watch::Receiver<HashMap<NodeId, String>>` once live membership reconfiguration lands in Phase 2c. Returning `Option<String>` means the loop silently no-ops when the hint references an unknown id (e.g. a leader the caller hasn't configured a route to yet), falling back to retrying the same endpoint. |
| 2026-04-11 | The `aresadb-cluster pd …` subcommand group accepts **UTF-8 strings** for keys (with empty `--end-key` denoting +∞) instead of hex, and **replica specs as `node_id:store_id[:role]`** (role defaults to `voter`). | The operator CLI is a smoke-test / demo tool; 99% of real keys during development are UTF-8 (e.g. `"m"` for a mid-alphabet split). Forcing hex encoding (`--split-key-hex`) would make every tutorial example unreadable. Binary key support is a future `--key-is-hex` flag when a real use case appears. The replica spec format keeps each placement on one CLI token (comma-separated) so scripts can interpolate a list without juggling nested delimiters, and defaulting to `voter` means the common case (`1:1,2:1,3:1`) stays typo-proof. |
| 2026-04-11 | `pd heartbeat-loop` is the **only PD subcommand that stays up until SIGINT**; every other subcommand is one-shot. | Mirrors the `bootstrap` / `join` (daemon) vs. `add-voter` / `write` (one-shot) split that's already established on the `aresadb-cluster` CLI. The heartbeat loop is a daemon because it has to outlive the invocation — a single heartbeat RPC is useful for smoke tests, but real node processes need continuous heartbeats for the catalog's liveness timer to stay fresh. Keeping every other subcommand one-shot means operators can script them the obvious way (`aresadb-cluster pd create-range … && aresadb-cluster pd split-range …`) without spawning background processes. |
| 2026-04-11 | Phase 2c-1 adds `raft_group_id` as a **field on the existing Raft RPC messages** (`AppendEntriesRequest`, `VoteRequest`, `InstallSnapshotRequest`) rather than wrapping them in a new envelope message. | A new outer envelope would force every existing caller and test to re-pack payloads and would bump the on-wire format in a way pre-2c members couldn't decode. A plain `uint64` field defaults to zero under protobuf's zero-value semantics, so any 2c member talking to a 2b-or-earlier peer sees `raft_group_id = 0` and routes it to the singleton group — which is exactly what that peer owns. It also keeps the bincode payload codec untouched, so the openraft type layouts that the Raft crate cares about are not affected. |
| 2026-04-11 | Server-side dispatch uses a dedicated `RaftDirectory` trait instead of overloading `openraft::RaftNetworkFactory` on the server. | `RaftNetworkFactory` is designed for the outbound direction (node A wants to talk to node B for group G); reusing it as an inbound dispatcher conflates two responsibilities and would force every test fixture to build a full factory just to register one `Raft` handle. The `RaftDirectory` trait is a single-method lookup — trivially mockable (the multi-group test implements it with a `HashMap` in ten lines), and `SingletonRaftDirectory` makes single-group deployments keep their one-liner `RaftGrpcServer::new(raft)` construction. |
| 2026-04-11 | Unknown `raft_group_id`s are mapped to **`tonic::Code::NotFound`**, not `Unimplemented` or `Internal`. | `NotFound` is the closest match in the gRPC status taxonomy for "the group id you asked for isn't hosted on this member" — it's the same code a REST API would return for a missing resource. `Unimplemented` implies the RPC itself isn't offered (the client should stop calling it entirely); `Internal` would tell the caller to retry against the same endpoint indefinitely. `NotFound` lets a future range-aware client inspect the error, invalidate its cache of "which node hosts group G", and re-query the placement driver to find the current home — the exact flow Phase 2c-3's `RangeDirectory` will need. |
| 2026-04-11 | The connection pool in `GrpcRaftNetwork` continues to key on `NodeId` alone, even though each range now constructs its own network factory with its own `raft_group_id`. | Peers are almost always shared across many ranges on the same node (e.g. every range's replica set is a subset of the same 3-5 physical nodes), so keying the channel cache on `(NodeId, group_id)` would open N channels per peer pair where one would do. Tonic channels multiplex every call through one HTTP/2 stream regardless of which group id the payload carries, so the sharing is safe. The factory knows which group id to stamp on every RPC because it's constructed per-group; the connection cache just forwards bytes. |
| 2026-04-11 | Phase 2c-1 keeps the `#[allow(clippy::result_large_err)]` module attribute in `aresadb-net::server` rather than boxing `tonic::Status`. | Mirrors the 2b-4 decision for `aresadb-pd::admin`. The `tonic::server::Server` trait signatures on generated service traits return `Result<Response<T>, Status>` by value; the internal `resolve()` helper has to return the same shape to plug into them. Boxing the inner `Status` would require a one-off conversion at every call site, which is uglier than a single allow-pragma. |
| 2026-04-11 | Phase 2c-2 places per-range storage under `<data-dir>/ranges/<range_id>/{log,data}/` instead of a flat `<data-dir>/range-<range_id>-log/` layout. | Makes operator enumeration obvious (`ls <data-dir>/ranges/` shows every range on the node; `du -sh <data-dir>/ranges/<range_id>/` sizes one range). Keeps `log/` and `data/` as separate directories so each can migrate to a different engine (redb vs. fjall-LSM in Phase 2d) without renaming sibling files. Also isolates the per-range state from the Phase 1 single-group paths (`<data-dir>/raft-log/`, `<data-dir>/state-machine/`) so both can coexist on one filesystem during the Phase 2c → Phase 3 transition. |
| 2026-04-11 | Phase 2c-2 makes `RangeRuntime::open` generic on `N: RaftNetworkFactory<TypeConfig>` instead of hard-coding `GrpcRaftNetwork`. | `GrpcRaftNetwork` is the production factory, but its tests must stand up a gRPC server and a peer directory before the runtime can replicate anything — an expensive setup when what we actually need is a unit test of the lifecycle (`open` / `bootstrap_voter` / `shutdown` / reopen). Making `open` generic lets tests pass `aresadb_raft::LoopbackNetwork`, a no-op factory that keeps a single-voter range bootstrapping in <50ms with zero bind races. The generic parameter lives on the method, not the struct, so callers never see it at the type level — the runtime is a plain `RangeRuntime` once constructed. |
| 2026-04-11 | Phase 2c-2 `bootstrap_voter` pattern-matches `openraft::error::InitializeError::NotAllowed` on the typed return value instead of probing `e.to_string()` for substrings like `"already initialized"` or `"NotAllowed"`. Same fix applied retroactively to `ClusterNode::bootstrap_single`. | The original substring check was fragile: openraft 0.9's `NotAllowed` display string is "not allowed to initialize due to current raft state: …" — which contains neither literal substring. The typed match is guaranteed to track openraft's type surface even when its error wording moves. An alternative — inspect `RaftMetrics::last_log_index` to decide whether to call `initialize` at all — is racy on a fresh reopen because the metrics watch channel lags behind the state-machine driver for a few ticks after `Raft::new` returns; that would produce a false-negative "fresh boot" path and a duplicate `initialize` call. |
| 2026-04-11 | Phase 2c-2 keeps `RangeRuntime` and `ClusterNode` as peers rather than folding the runtime into a `ClusterNode::range_for(range_id)` method. | The runtime is the *reusable* unit — one per range, created by the PD supervisor, destroyed on split/merge/decommission. `ClusterNode` is the *per-process* container that will eventually own many of them. Keeping the types separate now means Phase 2c-3 can introduce `RangeDirectory: HashMap<RangeId, Arc<RangeRuntime>>` without refactoring the runtime's surface, and it makes the Phase 2d migration story simpler (swap `RangeRuntime`'s backend factory per range without touching `ClusterNode`). The cost is one extra public type in the `aresadb-cluster` API, which every Phase 2c consumer will pay exactly once. |
| 2026-04-11 | Phase 2c-3 dual-indexes `RangeDirectory` on `(RangeId, GroupId)` instead of storing one `HashMap<RangeId, _>` and mapping every lookup through the descriptor. | The hot path is `RaftDirectory::raft_for(raft_group_id)` — it fires on every inbound `AppendEntries` RPC, which on a well-loaded multi-range node is thousands per second per peer. Routing that through `HashMap<RangeId, _>::values()` + a linear scan on `descriptor().raft_group_id` would be O(N) per RPC. Two hash maps over the same `Arc<RangeRuntime>` keep both lookup shapes O(1) with exactly one `Arc` clone on each, and the memory cost is two `u64` keys per range. The invariant that `range_id ≠ group_id` is *allowed* by the descriptor schema (kept distinct precisely for future layouts where one range needs multiple metadata groups), so a single index keyed on only one of them is structurally unsafe. |
| 2026-04-11 | `ClusterNode` **always** opens a "default range" (`DEFAULT_RANGE_ID = 1`, `DEFAULT_RAFT_GROUP_ID = 1`) on `start()` rather than exposing an empty directory that the caller has to populate. | Phase 1 callers (the CLI, admin RPCs, `three_node_durable`, `leader_failover`) all assume a single Raft group with `node.raft()` / `node.data()` / `node.log_backend()` accessors. Making the default range mandatory lets those accessors forward to it unconditionally — zero changes to any downstream test or tool — while still giving range-aware callers a `range_directory()` handle to add more ranges through the admin API. The alternative (empty directory on boot; operator must call `AddRange` to get a data plane) is strictly more flexible but would have required updating every Phase 1 consumer in lockstep, and it breaks the one-line `ClusterNode::bootstrap_single` contract. A future refactor can relax this once the Phase 2c-4 PD supervisor is the only thing populating directories. |
| 2026-04-11 | The back-compat default range takes `range_id = 1` (not `0`). | `SINGLETON_RAFT_GROUP_ID = 0` from Phase 2c-1 is deliberately **the wire-level default** for clients that haven't upgraded to the multi-Raft protocol — it's consumed by `SingletonRaftDirectory`, which routes every inbound RPC to one Raft handle regardless of the envelope's group id. The `RangeDirectory` is the *real* multi-Raft dispatcher, so its keys are actual range ids. Using `range_id = 1` (and `raft_group_id = 1` by default) keeps `id = 0` reserved as a sentinel for "no group specified" at the transport layer; two `ClusterNode`s talking to each other both advertise group `1` on the wire, so they match. Integration tests that use the lower-level `SingleNode` harness with `GrpcRaftNetwork::new_singleton` (which still sends `raft_group_id = 0`) continue to test the `SingletonRaftDirectory` path independently. |
| 2026-04-11 | `AddRange` runs a **pre-flight duplicate probe** against the directory before opening backends, even though `RangeDirectory::insert` would catch the collision anyway. | `RangeRuntime::open_on_disk` takes redb's exclusive file lock on `<data-dir>/ranges/<range_id>/{log,data}/` the moment it opens the backends. Hitting a logical duplicate through this path returns `StorageError::Backend("Database already open. Cannot acquire lock.")`, which the admin handler maps to `Status::internal` — masking a perfectly recoverable "that range exists" with a generic server error. The pre-flight probe turns the two most common error shapes (`DuplicateRangeId`, `DuplicateGroupId`) into clean `ALREADY_EXISTS` gRPC codes, and the post-open `insert` call still covers the narrow window where two concurrent `AddRange`s race past the probe. |
| 2026-04-11 | The cluster admin API defines its **own** `pb::RangeDescriptor` / `pb::ReplicaPlacement` / `pb::ReplicaRole` types even though `aresadb.pd.v1` already has equivalents. | Keeping the cluster and PD wire schemas independent means each can evolve on its own schedule — the cluster admin API is operator-facing (CLI, Kubernetes operator, SDK) while the PD admin API is catalog-facing (PD-aware tools, future split/merge orchestration). Sharing a protobuf crate would couple both release cycles: a change to the PD's catalog shape would ripple into every operator tool, and vice versa. The one-way conversions live in `descriptor_to_pb` / `descriptor_from_pb` helpers inside `AdminService`; each one is <20 lines, totally mechanical, and the fallible direction funnels through a single `Status::invalid_argument` path. |
| 2026-04-11 | `RemoveRange` on a runtime with **outstanding `Arc` references** rolls the directory back and returns `FAILED_PRECONDITION` rather than detaching the runtime half-silently. | `RangeRuntime::shutdown(self)` consumes ownership so it can flush and close the Raft + backends deterministically — if the directory hands back an `Arc<RangeRuntime>` that another thread (an in-flight admin RPC, a test, a debug tool) is still holding, `Arc::try_unwrap` fails and we can't call `shutdown()`. Silently returning success would leave an undead runtime with live Raft state but a stale directory; silently dropping the runtime would skip the `close()` path. Returning `FAILED_PRECONDITION` with a `force=true` escape hatch lets the caller decide: wait, cancel, or accept a partial shutdown. On `force=true` we still `raft.shutdown()` the Raft side (that only needs a handle clone), and the backends live on until the last `Arc` drops — which is a best-effort but documented outcome. |
| 2026-04-11 | `AdminService` takes the full `NodeConfig` rather than just the fields it needs (`node_id`, advertise-addr closure, `ranges_root`). | The admin service needs to stamp per-range paths, cluster names, and advertise addresses into every `AddRange` call; extracting those fields at construction time would mean freezing them at that moment, and operators tweaking `NodeConfig` (e.g. rotating `advertise_addr` on a dual-homed node) wouldn't see the change without a full node restart. Passing the whole `NodeConfig` through is cheap (it's `Clone`, <200 bytes), keeps the admin handler and the `ClusterNode::start` path pointed at the same source of truth, and trivially extends when Phase 2c-4 adds PD-supplied addresses to `NodeConfig`. |
| 2026-04-20 | Phase 2c-4 splits the PD supervisor into three sharply-scoped modules — `reconciler` (pure logic), `executor` (directory side-effects), and `supervisor` (task lifecycle) — instead of a single monolithic task that does all three. | The reconciler is the only part with interesting semantics (assignment filtering, skip list, add/remove symmetry); keeping it pure makes every edge case a one-line unit test against fixtures rather than a multi-second integration test with a live PD cluster. The executor owns the "open backend + register in directory" sequence exactly once, so a future Phase 2c-5 caller that wants to add a range via a different trigger (e.g. a split-driven reconfiguration) reuses the same entry point. The supervisor is left with the task-lifecycle boilerplate (shutdown channel, timer, error logging), which is the part that's hardest to unit test and easiest to get right by inspection. The alternative — one big `async fn run_supervisor_loop()` — would make each of those concerns harder to reason about in isolation and harder to test without standing up real gRPC servers. |
| 2026-04-20 | `PdSupervisorConfig::skip_local_ranges` defaults to `{DEFAULT_RANGE_ID}` and the reconciler treats the skip-list identically for both `to_add` and `to_remove`. | The back-compat default range is owned by `ClusterNode::start` — it's bootstrapped locally, it's not in the PD catalog, and its data lives under `<data-dir>/ranges/1/`. If the reconciler didn't skip it, every tick would see "range 1 is local, PD doesn't have it" and try to close the default runtime, breaking every Phase 1 smoke test and CLI invocation that expects `node.default_range()` to be alive. Skipping symmetrically (both directions) means the supervisor can't accidentally create a *duplicate* range with id 1 either — if PD did somehow gain an entry with `range_id = 1`, we'd ignore it on the local side. Operators who eventually want PD to manage the default range can pass `with_skip_local_ranges(Default::default())` to the config to clear the set. |
| 2026-04-20 | `ClusterNode::attach_pd_supervisor` performs the initial `register_node` synchronously and refuses to spawn if PD is unreachable, rather than spawning the supervisor unconditionally and letting the first reconcile tick discover the outage. | A mis-configured PD endpoint is a "don't boot silently" error, not a "retry forever" one. If the supervisor spawned optimistically, an operator looking at node logs would see periodic `reconcile tick failed` warnings but would have to grep to discover *why* — and in the meantime the node would be serving its local default range without the cluster catalog ever knowing it exists. Failing fast at attach-time means `ClusterNode::start_with_pd` either returns a fully-functional PD-connected node or bubbles the dial error up to the caller (CLI / deployment controller / test harness), which is the same "fail on startup" pattern every production system uses for its critical downstream dependencies. |
| 2026-04-20 | The executor accumulates per-range errors in an `ExecutorReport` rather than short-circuiting on the first failure. | Different ranges on the same reconcile tick are independent — a backend-open error for range 42 has no bearing on whether range 43 can be added. Short-circuiting would let a single bad range block convergence on every other range indefinitely. The report model lets the supervisor log each failure with structured context (`tracing::warn!(range_id, error, …)`) and still apply the remaining plan; the next tick will retry the failed entries naturally. This also matches how Kubernetes-style reconcilers are written in every other production system — "converge what you can, log what you can't, retry next tick". |
| 2026-04-20 | The supervisor's heartbeat loop pins to a single PD endpoint (the first entry in `pd_endpoints`) instead of using `HeartbeatConfig::endpoint_for` to follow leader churn. | Following leader churn is valuable *eventually*, but it requires a live view of every PD member's address — which in Phase 2c-4 comes from the PD catalog itself via `list_nodes`, which we don't consult in the heartbeat loop. Plumbing that through would make the supervisor reach into its own reconcile client on every heartbeat tick, which is an ordering constraint (the first reconcile has to succeed before heartbeats can rotate) that's harder to reason about than "pin to endpoint 0, retry on failure, rely on PD's own forward-to-leader replication for writes". Phase 2c-5 can upgrade this when the dynamic-endpoint watch channel lands. |
| 2026-04-20 | Phase 2c-4 ships `add` and `remove` only; splits and merges stay with the Phase 2b-4 admin API (`PdAdminClient::split_range` / `merge_ranges`) and don't drive local runtime changes on node-side. | A split operation isn't just "create a new range" — it's "stop writes to the parent at `split_key`, materialize the right-hand-side data under a new range id, atomically cut over, and make both halves available at the same `epoch + 1`." Every one of those steps is a sub-phase's worth of work (split markers, epoch fencing, data migration). Shoehorning split execution into the supervisor loop would hide all that machinery behind a `reconcile_once` call that silently grew five pages long. Keeping splits as an explicit admin-initiated PD operation (Phase 2c-5+) means the supervisor's contract stays "observe catalog, converge directory" — a one-paragraph specification that's easy to reason about, easy to test, and easy to extend. |
| 2026-04-11 | Phase 2c-5 routes `RangeRuntime::linearizable_get` through openraft's `ensure_linearizable` — the ReadIndex + wait-for-apply path — instead of implementing a pure lease-only fast path that skips the heartbeat round-trip. | openraft 0.9.22 does not ship a standalone "leader lease, no quorum probe" read API; `ensure_linearizable` is the only linearizable read primitive it exposes and it always runs ReadIndex internally (quorum heartbeat + apply wait). Implementing a pure lease-only path ourselves would mean tracking heartbeat timestamps inside `RangeRuntime`, maintaining a lease-expiry watch, and side-stepping openraft's state machine — three pieces of concurrency-sensitive code whose bugs are *exactly* the safety violations ("serve stale reads from an ousted leader") that leader leases are supposed to prevent. ReadIndex is already fast on a healthy cluster (one cheap heartbeat RPC per read) and is provably safe; the lease-only fast path is a later optimization that becomes worth the audit cost when we have p99 numbers that demand it. `architecture-v2.md` §4.3 promises both modes — this phase ships "Leader-lease read" and "Read-index" via the same openraft entry point and reserves the right to branch them later. |
| 2026-04-11 | `ReadError` lives next to `ClusterError` in `aresadb_cluster::error` as a *separate* enum rather than adding `NotLeader` / `QuorumUnavailable` / `Fatal` variants to `ClusterError`. | `ClusterError` is the lifecycle/admin/write error taxonomy — it includes `Config`, `InvalidRequest`, and transport-flavoured `Raft(String)` variants that no read-path caller will ever produce or want to pattern-match on. Fat-finger adding read variants would force every existing `?` call site to widen its match or its docblock. Keeping `ReadError` distinct lets the admin `Read` handler exhaustively match four focused variants and map each to a *specific* tonic `Status::code`: `NotLeader` → `FAILED_PRECONDITION` (you called the wrong member), `QuorumUnavailable` → `UNAVAILABLE` (retry against me), `Fatal` → `INTERNAL` (oncall), `Storage` → `INTERNAL` (oncall). Mixing the taxonomies would make the status-code mapping a guess on every handler. A shared supertrait or `From<ReadError> for ClusterError` is easy to add later if any call site needs to bridge them. |
| 2026-04-11 | `linearizable_get` signals "caller hit the wrong member" with `ReadError::NotLeader(Option<NodeId>)` and the admin RPC attaches the id as an `x-aresa-leader-id` gRPC metadata header on the `FAILED_PRECONDITION` status, rather than embedding it in the human-readable message only. | Clients that re-route on leader hints — the CLI, the forthcoming SDK, the future Vercel-adjacent gateway, and the `aresadb-cluster` integration tests themselves — need a *stable, machine-readable* way to extract the hint. Status messages are free-form, subject to translation, and change between minor openraft versions; metadata headers are typed key-value pairs that tonic round-trips intact. The `x-aresa-` prefix matches the Phase 2b-4 PD admin convention and keeps our control-plane metadata under a single namespace. The human-readable message remains ("not leader for range; current leader: N") as a debugging aid, but no programmatic consumer needs to parse it. |
| 2026-04-11 | The admin `Read` RPC preserves its **Phase 1c fast path** (`range_id == DEFAULT_RANGE_ID && consistency == UNSPECIFIED` → raw `self.data.get(key)` with no directory lookup) instead of unconditionally routing through the `RangeDirectory`. | Every Phase 1c caller — `leader_failover`, `three_node_durable`, the existing CLI, and probably unreleased internal tooling — issued `Read` with a bare key and no consistency specifier and expected a raw state-machine lookup on the default range. Those call sites were written before `ReadError` existed; flipping the default to a stale-or-linearizable path would either change the error taxonomy they observe or slow them down with a leadership-status read. Preserving the raw fast path means the UNSPECIFIED branch is byte-for-byte the Phase 1c code we shipped at `v2.0.0-alpha.1`, and clients opt into the new semantics by *naming* them (`READ_CONSISTENCY_LINEARIZABLE` / `READ_CONSISTENCY_STALE`). Non-default `range_id` never had a Phase 1c shape, so there we treat UNSPECIFIED as `STALE` — still a raw read, but routed through the directory. |
| 2026-04-11 | `LeadershipStatus` is a flat, plain-data struct (`u64` / `bool` / `Option<u64>`) rather than a thin newtype wrapper around `openraft::RaftMetrics`. | Operators consume this status from four different places — admin `Status` JSON, PD heartbeat payloads, future Prometheus scrapers, and the `aresadb-cluster` CLI — and they all want a stable shape. `RaftMetrics` embeds `ServerState`, `Vote<NID>`, and `StoredMembership<NID, N>`, all of which have changed layout between openraft minor versions in the past (`committed` field moved, `Vote` field ordering, membership serialization). Exposing any of those directly would make a minor openraft bump a breaking API change for AresaDB operators. The flat struct is ~64 bytes of primitives, trivial to serialize to JSON / protobuf / Prometheus text, and cheap to derive new fields on when we need them. The raw `RaftMetrics` channel is still available via `RangeRuntime::raft().metrics()` for callers that explicitly want it. |
| 2026-04-11 | `stale_get` lives on `RangeRuntime` as a formal method even though it's a one-line wrapper around `data_backend().get(key)`. | Reading the state machine directly is *correct* on any member, but the ergonomics of telling callers "call `data_backend().get()` yourself" are actively harmful: they have to remember the ordering contract ("no guard, may miss concurrent writes"), they have to thread the `Arc<dyn StorageBackend>` through their code, and worst of all, they lose the *symmetry* with `linearizable_get` that makes the §4.3 consistency table legible in code. Having both methods on the same type, both returning `ReadResult<Option<Vec<u8>>>`, turns "pick your consistency level" into a two-line diff. When Phase 4 adds MVCC, `read_as_of(ts)` and `stale_get` can coexist as named variants in the same module without anyone having to remember which accessor skipped which guard. |
| 2026-04-11 | Phase 2c-6 ships the range-aware admin `Write` with an explicit **`WriteError`** enum in `aresadb_cluster::error`, parallelling Phase 2c-5's `ReadError`, rather than reusing the generic `ClusterError::Raft(String)` variant on the write path. | The write path has the same caller-visible failure shapes as the read path — `ForwardToLeader` (wrong member, re-route), `ChangeMembershipError::*` (replicated and declined), `Fatal` (oncall). Collapsing them into a `String` variant would erase every distinction the CLI and SDK need to make sensible retry decisions, and would make attaching the `x-aresa-leader-id` metadata header conditional on parsing the error's `Display` output — exactly the brittleness that Phase 2c-5 spent a decision log entry avoiding for reads. Keeping `WriteError` separate from `ReadError` (rather than a shared `RangeError`) mirrors the existing split between the admin `Write` and `Read` handlers' status-mapping helpers: each handler owns one enum, one mapping function, one test surface. |
| 2026-04-11 | The `WriteRequest` wire contract defaults `range_id = 0` to the default range (`DEFAULT_RANGE_ID = 1`) and goes through the admin service's cached `self.raft` handle, instead of looking every request up through `RangeDirectory` unconditionally. | Phase 1c wire compatibility is the explicit constraint. Every `WriteRequest` sent by `v2.0.0-alpha.1` clients has `range_id = 0` under protobuf zero-value semantics; those clients must keep working after the Phase 2c-6 upgrade without any code change. Routing a zero-valued request through the directory would work semantically, but it would take the `RangeDirectory::get_range` lock on every write on the hot Phase 1c path — a visible regression with no benefit, because the default-range `Arc<RangeRuntime>` and the admin service's `self.raft` point at the exact same openraft handle. Non-zero `range_id` takes the directory path, which is the only one that can target a non-default range. The cost is one extra branch per write and a ~5-line equivalence note in the admin handler. |
| 2026-04-11 | Phase 2c-6's madsim scenario (`MultiRangeApplyDeterminism`) drives N **independent** `SingleNode` Raft groups rather than a shared multi-Raft harness on one openraft instance. | The invariant this scenario is really about is cross-range *isolation*: writes routed to range A must not leak into range B's state machine, even when their schedules interleave. A shared harness that plugged multiple ranges into one `Raft::new(…)` instance would be a stronger test (it would also catch multi-Raft scheduling bugs inside openraft) but openraft 0.9.22 is one-group-per-handle — there is no shared harness today. Spinning up one `SingleNode` per range mirrors exactly what `RangeRuntime` does in production, and `futures::future::try_join_all` on the startup path gives us concurrent apply (and therefore real interleaving) without building a new scheduling layer on top of openraft. The alternative — sequential ranges — would silently mask any routing bug whose failure mode depends on ordering, which is exactly the class of bugs this scenario is meant to catch. |
| 2026-04-11 | `MultiRangeApplyDeterminism::route` resolves ops by **longest-prefix match** instead of first-match or a hash-based bucketing. | Real range layouts nest — a hot range like `r2-hot/` can carve out of `r2/` after a split, and the scenario's routing must not regress the moment the prefix map gains an overlapping entry. First-match would make the declaration order of `prefixes` load-bearing; hash bucketing would decouple the routing from the actual keyspace layout and would silently pass if a routing bug put keys in the wrong *set* of ranges as long as the cardinality matched. Longest-prefix is what the production PD catalog does (via `by_start` secondary index on `Catalog`), so the scenario is testing the same resolution strategy callers will see in Phase 3+. The cost is O(P × L) per op — tiny for the default 4-range layout, and still fine for the hundred-range stress layout we'd write next. |
| 2026-04-11 | Phase 2c-6 Docker smoke (`multi-range.sh`) uses a **single-voter range** (replicas = `1:1`) rather than a 3-way-replicated range that mirrors the default range's topology. | The cluster-admin `AddLearner` / `ChangeMembership` RPCs still target the default range only (Phase 2c-3c kept them range-unaware to avoid a premature API split). Extending them to take a `range_id` parameter is a real change — it touches the admin proto, the tonic server, the typed client, and every integration test — and it should land together with PD-driven split execution in Phase 2d rather than as a one-off in the docker smoke. In the meantime, single-voter is enough to exercise every wire-level change Phase 2c-6 actually ships: range-aware `Write` routing, the Phase 2c-5 `Read` path on a non-default range, and cross-process isolation (node-2 and node-3 genuinely don't know range 42 exists). The smoke script is explicit about this trade-off so nobody mistakes it for a full multi-range replication test. |
| 2026-04-11 | The `add-range` / `remove-range` / `list-ranges` CLI subcommands take `--leader` / `--addr` like every other cluster-admin command, even though these operations aren't replicated through Raft and could in principle target any healthy node. | The Phase 2c-3c admin RPCs (`AddRange`, `RemoveRange`, `ListRanges`) live on the same tonic service as the Phase 1c `AddLearner` / `ChangeMembership` commands that *do* require a leader. Mixing "send to any node" and "send to leader" flags in the same CLI would force operators to remember which one each subcommand needs, which is both a UX regression and a footgun when scripting rollouts. Naming the flag consistently (`--leader` for mutating subcommands that happen to be leader-agnostic today) keeps the shape uniform, and if a future phase makes range opens catalog-replicated (e.g. a PD-driven split that propagates through Raft), no flag rename is needed. The alternative — `--addr` everywhere with a runtime check — would trade a doc-string inconsistency for a footgun, which is the worse trade. |
| 2026-04-11 | Phase 2c-6 does not add a PD container to `docker/cluster/docker-compose.yml` even though the `aresadb-cluster` image already knows how to talk to a PD via the Phase 2c-4 supervisor. | `aresadb-pd`'s Raft transport is the in-process `PdRouter` — there is no gRPC network layer for PD Raft RPCs yet. A single-node PD container would work for catalog reads and heartbeats, but the moment it restarted (or a compose user scaled the service) the catalog would be lost, which is strictly worse than not having one at all. Building out a PD gRPC transport is a phase of its own; shoehorning it into the Phase 2c-6 smoke would push that work forward by a full sub-phase with no benefit to Phase 2c-6's actual goal, which is to exercise the node-side range-aware data plane over a real network. Pushing the PD story to Phase 2d keeps the compose file honest: every service in it is production-shaped, none are single-point-of-failure placeholders. |
| 2026-04-11 | Phase 2d picks **fjall 3.1** over RocksDB, LevelDB, sled, and a hand-rolled in-house LSM for the write-heavy `StorageBackend` engine. | fjall is pure-Rust (no C++ toolchain on CI, no bindings drift), ships a genuine levelled LSM on top of a bounded journal + memtable + SSTables, and its `Database` / `Keyspace` handles are cheap `Arc`-under-the-hood clones — which maps cleanly onto our `tokio::task::spawn_blocking` model. Its MSRV (1.90) is comfortably under the workspace's 1.95 toolchain. RocksDB is the obvious alternative but the bindings story (`rocksdb` crate, whose maintenance follows a separate release cadence than the C++ core) and the statically-linked `libstdc++` footprint are both real costs for a v2-alpha tag we want to be portable. sled is in the middle of an on-disk format transition and explicitly labels itself as not production-ready. LevelDB's Rust bindings are unmaintained. A hand-rolled LSM is a phase-5 project that AresaDB explicitly has on the roadmap for its thread-per-core engine, but Phase 2d needs *an* LSM today, not the best one eventually. fjall is the smallest move that gets every write-heavy test into a write-optimized engine without bringing a C++ toolchain along. |
| 2026-04-11 | The `FjallBackend` uses exactly **one fjall keyspace named `"default"`** rather than mapping each `aresadb-core` "logical tenant" or Raft group to its own keyspace. | `aresadb-core::StorageBackend` presents one flat key/value namespace per instance — there is no concept of "column family" or "tenant" at the trait layer. Every callsite in `aresadb-cluster` (including the per-range data backend and the per-range log backend) opens its own `FjallBackend` under a distinct directory, so isolation between Raft groups comes from separate databases on disk, not separate keyspaces within one database. Introducing multiple keyspaces would be strictly worse: the `StorageBackend` trait would need a column-family dimension that no current caller supplies, and fjall's resource model (one journal + compaction worker pool per `Keyspace`) wouldn't let us shrink memory by sharing them anyway. If Phase 5's custom engine wants per-range column families inside one LSM, that redesign is a whole trait change, not a fjall configuration choice. |
| 2026-04-11 | `FjallBackend::write_batch` drives `OwnedWriteBatch::commit()` + `Database::persist(PersistMode::SyncAll)` on **every** batch rather than buffering multiple batches per fsync or trusting fjall's default group-commit cadence. | The caller is almost always openraft's `RaftStateMachine::apply` or the Raft log `LogStore::append`, both of which have a hard contract: once the call returns `Ok`, the write must survive a crash. fjall's default `PersistMode::Buffer` is much faster (no fsync per commit — amortized over multiple journal flushes), but a crash after a Buffer-mode commit returns success can lose the write — which would silently violate Raft's durability invariant on the state-machine side and corrupt the log on the log side. Forcing `SyncAll` mirrors what `RedbBackend` does via `redb::WriteTransaction::commit` + redb's own fsync, so both backends now have the same "returned Ok ⇒ durable" contract. The cost is one fsync per Raft apply — identical to what the redb path already pays, and still dominated by the openraft scheduler's cadence rather than by the engine layer. |
| 2026-04-11 | `FjallBackend`'s `Snapshot` impl **eagerly materialises** both `get` and `scan` into `Bytes` / `Vec<KeyValue>` at call time, rather than holding a long-lived `fjall::Snapshot` cursor and streaming entries out. | The `aresadb_core::Snapshot` trait requires `Send + 'static`, because its values cross `tokio::task::spawn_blocking` / `spawn` boundaries. fjall's `Snapshot` itself is `Send`, but its iterator yields `Guard` values whose lifetime is tied to the snapshot object — turning that into a `KeyValueStream<'static>` would require either (a) a self-referential struct, which stable Rust does not support, or (b) an unsafe transmute of the iterator's borrow, which is exactly the wrong kind of cleverness for a phase whose contract is "this backend is safe to Raft-replicate against". Eager materialisation is what `RedbBackend` already does, so Raft snapshot builders and `stale_get` callers see the same timing profile regardless of engine. The cost is O(range_bytes) memory at snapshot-call time; acceptable for log-purge volumes and for the state-machine snapshots openraft emits on a schedule, and fixable later with a streaming trait extension if a workload ever demands it. |
| 2026-04-11 | `FjallBackend::delete_range` **collects every key in the half-open interval** and issues individual `OwnedWriteBatch::remove` calls in one commit, rather than using a native range tombstone. | fjall 3.1 does not expose a single-op range tombstone — `OwnedWriteBatch::remove` is per-key, and its SSTables don't yet encode range markers in a way queries can skip over. The O(N) batched delete matches the `RedbBackend` shape (redb also has no range tombstone) and is acceptable for every caller Phase 2d actually has: Raft log suffix deletes are bounded to the committed index gap, and PD-driven range drops during split/merge haven't landed yet. When the latter become a real workload, either fjall will have grown native range tombstones or we'll add a dedicated fast path; until then, the shared O(N) implementation keeps both engines on parity semantics. |
| 2026-04-11 | `FjallBackend::approximate_size` returns **0 for every range** rather than reporting `Keyspace::disk_space()` or a range-proportional estimate derived from it. | `Keyspace::disk_space` is the total on-disk footprint of the whole keyspace — it has no notion of where a given `[start_key, end_key)` window sits inside the SSTables. Reporting it verbatim would tell the PD split heuristics that every range on the node is the same enormous size, which is strictly worse than "no information" (it would bias splits toward nodes with the fewest ranges regardless of actual hot-key distribution). A range-proportional estimate (say, total_bytes × (range_end - range_start) / keyspace_span) is worse still — it pretends the data is uniformly distributed, which is the exact pathology ranges exist to handle. The `approximate_size` contract on `StorageBackend` is already documented as advisory and the PD catalog tolerates zero-size hints from `RedbBackend`, so returning `0` keeps both engines truthful. A genuine range-aware size estimator belongs with the Phase 5 custom LSM, where we own the SSTable format. |
| 2026-04-11 | Phase 2d introduces the `DataEngine::{Redb, Lsm}` enum as a `NodeConfig` field and **not** per-range (per-range engine selection via an `Option<DataEngine>` on `RangeDescriptor` was considered and rejected for Phase 2d). | Per-range engine selection is the right long-term answer — a metadata range benefits from redb's crash-simple ACID story while a write-heavy log-shaped range benefits from an LSM — but ratifying it in Phase 2d would require plumbing the choice through `RangeDescriptor` (serialized bytes on the PD Raft log), through every `pd.proto` / `admin.proto` RPC, through the PD catalog's overlap-check machinery, and through the `pd_supervisor::reconciler` planning surface. That's four sub-phases of API churn for a feature that has zero operational drivers today (nobody is running AresaDB yet; the benchmarks we're about to publish on `v2.0.0-alpha.2` all pick *one* engine per run). Node-level selection is one line of config, opt-in per node, and fully forward-compatible: a future `per_range_engine: HashMap<RangeId, DataEngine>` override can layer on top without breaking any current caller. |
| 2026-04-11 | The Raft log backend **stays on redb** unconditionally; `DataEngine::Lsm` applies to the state-machine data backend only. | Raft's log is a strictly append-only, fsync-per-commit workload with frequent suffix deletes (log truncation after snapshot install). LSMs spend their write-amplification budget on compaction to turn random writes into sequential ones — value that's entirely wasted on a workload that's already sequential. redb's append-heavy behaviour on a single file matches the Raft log's access pattern exactly (one fsync per commit, O(log N) point reads on replay, O(N) range deletes for truncation — same shape as `RedbBackend::delete_range`). Running the log on fjall would add one extra compaction worker pool and one extra journal per range with zero measurable gain. Splitting the two axes now means a future `LogEngine` enum can experiment with alternatives (e.g. a dedicated Raft-log-shaped append-only engine) without moving the data-engine needle. |
| 2026-04-11 | The on-disk layout per range uses **distinct directory / file names per engine** (`data.redb` file vs. `data.lsm` directory) rather than a single `data/` path the engine interprets. | redb is one file; fjall is a directory containing journal + levels + metadata. Using the same path name for both would either (a) require one engine to adopt the other's shape (fjall-in-a-file is not a supported mode; redb-in-a-directory is a nonsense layout for a single-file store), or (b) silently corrupt on engine switch — imagine `data_engine: Redb` writing a file called `data/`, then the operator setting `data_engine: Lsm`, and fjall happily creating its metadata next to a stray redb artifact. Distinct suffixes mean a mis-configured switch surfaces as an obvious "engine says file, disk says directory" error at open time, not as silent data loss three commits later. It also makes `ls <data-dir>/ranges/<id>/` an immediate answer to "which engine is this range on", which is exactly what oncall will want when they're paged. |
| 2026-04-11 | `aresadb_cluster::range::tests::lsm_data_engine_persists_committed_writes_across_reopen` asserts the LSM path through **the full openraft `client_write` → `apply` → `Database::persist(SyncAll)` chain**, not just a raw `FjallBackend` round-trip. | The fjall-only unit tests inside `aresadb-engine-lsm` already cover the `StorageBackend` trait conformance in isolation — they're good at pinning `FjallBackend` to the redb-shaped invariants that the `MemoryBackend` also satisfies, but they can't catch integration bugs like "the state-machine driver calls `apply` on the wrong backend" or "the Raft log and the state machine accidentally point at the same fjall database". The cluster-level test opens a real `RangeRuntime` with `DataEngine::Lsm`, commits a write through Raft, graceful-shuts-down, reopens on the same data directory, and asserts the value is still visible through `stale_get`. That proves every plumb point (`open_on_disk`, the `RangeRuntime` constructor's backend dispatch, the `shutdown` sequence, the reopen path, and the durability contract) is consistent with `DataEngine::Redb` — which is the bar Phase 2d's "opt-in, same semantics" promise has to clear before `v2.0.0-alpha.2`. |
| 2026-04-20 | Publishing-audit follow-up: the v1 technical report stays authoritative for the **embedded** engine; the v2 distributed cluster gets its **own companion tech-note** (separate slug, Zenodo deposit, Aresalab card) rather than a rewrite of the v1 paper. | The v1 paper measures a single-process data structure — sub-10-µs point lookups, 113× HNSW speedup, 24× B-tree index speedup. Folding network-bound measurements (Raft commit, range throughput, compaction tail) into its evaluation would make both stories harder to evaluate against their own baselines. Zenodo's version model keeps both DOIs stable and mutually citable. See [`docs/publishing-audit.md`](./publishing-audit.md) for the full scope split and the v2-note outline. |
| 2026-04-20 | The v1 `benchmark_suite` example was **re-run on the alpha.2 workspace** (2026-04-20) and the numbers were promoted into `BENCHMARKS.md`, `CITATION.cff`, `zenodo.json`, and the Aresalab publications card, even though no paper text was re-submitted. | The paper's figures live in the same repo as the source they measure; shipping alpha.2 without re-running the regression suite would leave `BENCHMARKS.md`'s history table silent on whether Phase 2 regressed the embedded engine. Re-running confirmed every metric is at least as good as 2026-04-11 (batch insert ~2× faster), and promoting the numbers into the card + metadata means every downstream consumer (Aresalab landing, Zenodo preview, CLI `about`) sees a consistent story at the alpha.2 boundary. The paper body is not re-cut because its conclusions are qualitative — the numbers moved the right direction within the noise floor the paper itself names. |
| 2026-04-20 | A first-pass **`benches/v2_cluster_bench.rs`** scaffold lands alongside the publishing audit: just `v2/raft/apply_single_node` + `v2/engine/backend` (put / warm-get) on redb and fjall, not the full sized suite planned for the v2 note. | Landing a scaffold now — rather than blocking on the complete suite — pins the workflow (`[[bench]]` registration, root-crate dev-dep on `aresadb-core` / `aresadb-raft` / `aresadb-engine-redb` / `aresadb-engine-lsm`, criterion benchmark-group naming convention) so every follow-up track (cluster writes, linearizable reads, failover, range create, batched writes on both engines) just adds cases to an existing `benches::bench_*` function. Running it on 2026-04-20 surfaced two immediately-useful qualitative findings (Raft apply ~25 µs dominates until fsync kicks in; redb vs fjall at single-commit granularity is a wash, with ~3 ms per put on both due to per-commit fsync) that are carried into the v2-note outline so the reader doesn't arrive expecting an LSM blowout on the point-write axis. |
| 2026-04-20 | Root-crate dev-dependencies for the v2 bench scaffold (`aresadb-core`, `aresadb-raft`, `aresadb-engine-redb`, `aresadb-engine-lsm`) use `path = "crates/…"` **even though the workspace already declares them as members**, and the bench is placed in the **root** crate's `benches/` directory rather than a dedicated `crates/aresadb-sim/benches/`. | The root crate already houses every Criterion bench in the repo (`storage_bench`, `query_bench`, `distributed_bench`), and every `cargo bench` invocation in CI / the paper's `experiments/run.py` targets the root crate by name. Splitting v2 benches into a sibling crate would bifurcate that invocation — every consumer would need to know to also run `cargo bench --package aresadb-sim`. Keeping the bench at the root-crate level with explicit `path =` dev-deps is additive: `cargo check --bench v2_cluster_bench` still works, `cargo bench` without a filter runs v1 + v2 together, and the root crate's release build is untouched because dev-deps never leak out. |
| 2026-04-20 | CI gains a **`benches` job** (`cargo check --workspace --benches`) as a compile-only gate and a **`docker-smoke` workflow** (nightly + push-to-main on cluster/docker path filters) that runs `bootstrap.sh` + `multi-range.sh` end-to-end; both sit outside the existing `test` / `lint` / `docs` gates. | `cargo test --workspace` doesn't compile opt-in bench targets, so without the `benches` job a misnamed helper or a removed type would only surface at the next `cargo bench` weeks later. Running the docker-compose smoke on every PR would push cold-build minutes onto every contributor (the `aresadb-cluster` image is ~3-5 min on GHA runners); pinning it to nightly + path-filtered main pushes keeps PR latency unchanged while still guaranteeing at most a 24-hour window between a regression and a red build. Both jobs run in parallel with the existing gates, so neither one extends the PR critical path. |
| 2026-04-20 | The distributed-stack benchmarks grow along **both** axes — Raft `put_batched/{16,128}` on the apply loop, and `put_batched/64` + `scan_range/1000` on the engine side — in a single expansion pass rather than trickling tracks in one at a time. | Each new track costs zero dev-deps and zero new bench files — the scaffold from 2026-04-20 already wires `aresadb-core`, `aresadb-raft`, `aresadb-engine-redb`, and `aresadb-engine-lsm` into the root crate's dev-dep tree and registers `v2_cluster_bench` in `[[bench]]`. Adding tracks in one sweep produces a single publishable smoke table in `docs/publishing-audit.md` §4a (one hardware snapshot, one measurement window, one criterion version), which is strictly easier to reason about than a grab-bag of tracks measured weeks apart on drifting dependencies. The batch-size amortisation curve it surfaces (per-put cost dropping from ~23 µs at `put_one` to ~0.59 µs at `put_batched/128`) is also one of the qualitative findings the v2 companion tech-note's §3 will quote directly. |
| 2026-04-20 | **`docs/operations.md`** and **`docs/release-notes/v2.0.0-alpha.2.md`** land inside the `aresadb` repo rather than in a separate `yev` / `genass` site. | Both documents pin hard to the exact semantics of `v2.0.0-alpha.2`: the operations runbook references the CLI flags, data-dir layout, and PD supervisor that this tag ships; the release notes reference the exact benchmark numbers captured on this workspace. Keeping them in the repo means the tag ships with its own documentation (they can be browsed directly on the repo's file tree and copied verbatim to `gh release create` / Aresalab), and any future refactor that changes the CLI surface, adds a new port, or renames a sub-phase flows through the same commit that updates these docs. The alternative — scoping operational docs to a sibling site — would introduce a staleness race every release. The markdown files are kept in `docs/` alongside `architecture-v2.md` and `phase-status.md`; operators who already know those two documents find the new ones by browsing the same directory. |
| 2026-04-20 | The **v2 companion tech-note** scaffolds as a Quarto *book* (`genass/publications/quarto/aresadb_v2_note/`) with its own `CITATION.cff`, `zenodo.json`, and `AGENTS.md` from day one, even though chapters 2 (architecture) and 3 (evaluation) are outline-grade. | A paper that's mostly outline is still useful scaffolding if every *non-outline* piece is complete: scope, known limitations, reproducibility rig, citation metadata, Zenodo deposit plan, and Aresalab integration story. Shipping those pieces now — even as a 0.1 — means the moment the sized v2 benchmark suite finishes (blocker in `docs/publishing-audit.md` §4a), filling in §2 and §3 is purely additive and the paper can go to Zenodo without rebuilding its metadata plumbing. The explicit "do not upload the 0.1 scaffold" note in the `CITATION.cff` and `AGENTS.md` prevents an accidental deposit that would have to be superseded. Scaffolding late would mean every publication ends up reinventing the same 8-10 files; scaffolding now puts the v2 note on the exact same render path the v1 paper already uses (`_quarto.yml` → `quarto render` → `aresadb-v2-distributed-note.pdf`). |
| 2026-04-20 | The v1 paper's §8 limitations + §Future Directions get a **light-touch edit** that names the `v2.0.0-alpha.2` cluster but changes zero figures, tables, or headline numbers, rather than either (a) leaving the paper silent on v2 or (b) rewriting §8 with v2 content. | Option (a) — silence — is now factually stale: readers who browse the repo or the Aresalab card will see a multi-Raft cluster and wonder why the paper doesn't acknowledge it. Option (c) — rewrite — would turn this into a v2 paper, conflict with the separate companion tech-note, and invalidate the paper's own scope statement in §1. The middle path (one paragraph under "Distributed mode" and one bullet under "Future Directions") lets the paper stay authoritative for the embedded surface it measures while pointing every serious reader at the v2 note for the cluster story. No figure is touched, no number is re-stated, and the Zenodo deposit for the v1 paper can still describe itself as "v1.0" with a straight face. |
| 2026-04-20 | The v2 Docker image is published as **`ghcr.io/yoreai/aresadb/cluster`** (note the `/cluster` suffix), leaving `ghcr.io/yoreai/aresadb` as the legacy v1 embedded-CLI image rather than overwriting it with the new cluster binary. | The v1 `ghcr.io/yoreai/aresadb` image has existed since `v0.2.0` and is what every previous README snippet documents — `docker run -it -v $(pwd)/data:/data ghcr.io/yoreai/aresadb` should keep launching the embedded CLI for users who haven't adopted the cluster yet. The v2 cluster binary (`aresadb-cluster`) is a genuinely different entrypoint with different ports, a different data-dir layout, and different lifecycle guarantees, so pointing it at the same namespace would silently break every v1 `docker pull` the next time someone does `:latest`. The `/cluster` suffix makes the artefact self-identifying: `aresadb-cluster` is to AresaDB what `etcd` is to etcd — a related but distinct runtime concern. The legacy image also keeps working because we still maintain the root `Dockerfile` with full workspace-member coverage and the same workspace `rust-version` pin. |
| 2026-04-20 | The release workflow gains a **`workflow_dispatch`** escape hatch with a `version` input and a `docker_only` toggle rather than relying on tag-re-pushes when the Docker leg of a release needs to be retried. | The tag-driven path succeeded for crates.io and PyPI — both registries now have `aresadb 2.0.0-alpha.2` — but the Docker publish failed because the Dockerfile stub phase was out-of-date with the v2 workspace (missing `aresadb-engine-lsm` / `aresadb-pd` Cargo.tomls; rustc 1.85 base too old for fjall's MSRV). Fixing forward by moving the tag would re-trigger crates.io (which has a one-shot version contract) and re-trigger PyPI (which does accept duplicates but logs them as errors). The `workflow_dispatch` with `docker_only: true` lets the Docker leg re-run against the fixed `main` without touching any of the other publishers and without forcing a tag rewrite. The `version` input is required so the dispatched run tags the image correctly — it won't derive from `GITHUB_REF_NAME` because `workflow_dispatch` fires from a branch, not a tag. |
| 2026-04-20 | The `v2.0.0-alpha.2` release-pipeline follow-ups (rustdoc fixes, Dockerfile updates, `Cargo.lock` tracking, clippy `is_multiple_of`, workflow_dispatch) ship on `main` **without a version bump** — they stay under the `[Unreleased] / ### Fixed` banner in the CHANGELOG and the published `aresadb 2.0.0-alpha.2` artefacts on crates.io + PyPI are untouched. | All the follow-ups are back-compatible, non-API changes: rustdoc-only, clippy-only, or Docker-image-only. Users who already installed `aresadb 2.0.0-alpha.2` from crates.io or PyPI do not need a re-install — their wheels + source dist are identical to what a fresh install would produce today. Bumping to `alpha.3` *only* to ship a Docker-image tag would confuse every consumer who had to re-resolve + re-test to pick up literally the same library. When the next batch of real API-affecting work (Phase 3 query router, real multi-voter replication, etc.) lands, the next crates.io / PyPI publish becomes `alpha.3` and the CHANGELOG promotes these entries into `[2.0.0-alpha.3]`. |
| 2026-04-20 | The workspace `rust-version` is bumped from `1.85` → `1.90` **even though** CI's `dtolnay/rust-toolchain@stable` has been running 1.95 for a week, and the v1 `Cargo.toml` claim of 1.85 had been silently wrong since `fjall` 3.1 landed in Phase 2d. | The MSRV in `Cargo.toml` is visible to every downstream crate that depends on `aresadb` — it's how Dependabot decides which rustc to matrix-test on, and it's what `cargo` consults when `--min-rust-version` is enabled. A stale `1.85` claim was a latent promise that anyone on rustc 1.85 could build this workspace, which has been materially false since the LSM engine's transitive deps landed. Bumping to `1.90` makes the claim match the truth (1.90 is the highest of the transitive MSRVs: fjall 3.1 = 1.90, lsm-tree 3.1 = 1.90, sfa 1.0 = 1.89, byteview 0.10 = 1.87). The Dockerfiles pin to the same `1.90` tag so a downstream builder who follows the MSRV claim gets a container that actually compiles — which is how the Docker smoke surfaced the mismatch in the first place. |
| 2026-04-20 | The cluster image's runtime stage drops to the unprivileged `aresadb` user via a `gosu`-based `docker-entrypoint.sh` rather than a plain `USER aresadb` directive in the Dockerfile. | The `USER aresadb` + image-time `chown` pattern only worked on Linux Docker hosts: there the engine copies the image-path ownership into freshly created named volumes, so the volume mount inherited `aresadb:aresadb`. Docker Desktop for macOS and Windows materialises every named volume as `root:root` regardless of the underlying image, so the unprivileged user couldn't write redb / fjall files and the container failed its healthcheck with `Permission denied (os error 13)` the moment a clean operator pulled `ghcr.io/yoreai/aresadb/cluster:2.0.0-alpha.2`. The init-script-then-`gosu` pattern is the same one Postgres / Redis / MariaDB use; it works portably across Linux runners, Docker Desktop, and Kubernetes, and an `ARESADB_RUN_AS_ROOT=1` escape hatch keeps bind-mount UID-preservation scenarios functional. The cost is one extra fork/exec at container start (negligible compared to a Raft cold-boot). |
| 2026-04-20 | The cluster ships an opt-in `docker/cluster/docker-compose.ghcr.yml` override that drops `build:` and forces the GHCR image, instead of either (a) flipping the default compose to `image:`-only or (b) adding two compose files for two different cluster sizes. | The default `docker-compose.yml` is the developer loop — `docker compose up --build` rebuilds incrementally on every code change, which is what every Phase 2c / 2d iteration spent weeks doing. Flipping the default to a `pull` would make `cargo edit` → `docker compose up` silently re-run the *previous* image, hiding genuine changes behind a tag-equality check; that's the worst possible default for a pre-1.0 codebase. A separate override file lets operators (and the not-yet-existing public docs site) document the production path with a one-line YAML composition, and the helper scripts honour `IMAGE=` so the same `bootstrap.sh` and `multi-range.sh` drive both modes. The "two compose files for different cluster sizes" alternative was rejected because cluster size is already a `docker compose up --scale` knob — splitting it across files would be redundant. |
