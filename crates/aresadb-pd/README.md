# aresadb-pd

Placement driver catalog for AresaDB v2. Owns the cluster's view of
*which range lives where*:

- Every range in the cluster is a [`RangeDescriptor`] (id, span, replica
  placement, Raft group id, epoch, generation, lease).
- The [`Catalog`] is the pure-logic index over those descriptors. It
  enforces the invariants that matter for correctness — no overlapping
  spans, split preserves coverage, epoch / generation are monotonic —
  and nothing else.
- [`PdCommand`] / [`PdResponse`] are the serializable commands that the
  (future) placement-driver Raft group replicates across PD replicas.

## Layering

The catalog is deliberately independent of Raft, storage, and
networking so its invariants can be unit-tested in isolation. Follow-up
phases layer around it:

- **2b-2** ☑ — `PdStateMachine`: applies `PdCommand` to a
  `StorageBackend`, mirrors the `aresadb-raft::StateMachineStore`
  pattern.
- **2b-3** ☑ — `aresadb_pd::raft::{PdTypeConfig, PdRaftStateMachine,
  PdLogStore, PdRouter, SinglePdNode, PdCluster}` — a full
  openraft group replicating `PdCommand` / `PdResponse`, with an
  in-process transport for multi-node integration tests.
- **2b-4** ☑ — `aresadb_pd::admin::{PdAdminService, PdAdminClient,
  HeartbeatLoop}` + `proto/pd.proto` (`aresadb.pd.v1`). Tonic server
  adapts `openraft::Raft<PdTypeConfig>` + `PdStateMachine` to 15
  strongly-typed RPCs; typed client surfaces `ForwardToLeader` as a
  first-class `NotLeader(Option<id>)` error variant;
  cancellation-safe heartbeat loop follows leader hints through an
  `EndpointResolver`. `aresadb-cluster` CLI ships a 15-subcommand
  `pd` group so operators can drive the catalog end-to-end.
- **2c** — wire into `aresadb-cluster::ClusterNode` so range leaders
  report back to PD.

Keeping the catalog pure-logic means Phase 2c can drive it from unit
tests, `madsim`, and production PD replicas with zero behavior
difference.

## PD Raft group (Phase 2b-3)

The 2b-3 layer wraps the persistent catalog in a proper openraft
group:

- `PdTypeConfig` binds openraft to `PdCommand` / `PdResponse`.
- `PdLogStore = aresadb_raft::LogStoreGeneric<PdTypeConfig>` reuses
  the Phase 1 log persistence — the underlying
  `LogStoreGeneric<C: RaftTypeConfig>` refactor means the same log
  storage serves both the user-data group and the PD group.
- `PdRaftStateMachine` bridges `PdStateMachine` to
  `RaftStateMachine<PdTypeConfig>`: every apply atomically
  persists the catalog row **and** Raft meta (`last_applied`,
  `last_membership`) in a single `WriteBatch` at
  `b"\xff/pd/sm/meta"`.
- `PdRouter` / `PdRouterNetwork` is the in-process transport:
  every peer registers its `openraft::Raft<PdTypeConfig>` handle
  and RPCs dispatch as direct method calls. Integration tests
  use it via `SinglePdNode` (one voter) and `PdCluster` (N
  voters, with `wait_for_leader`, `wait_for_replication`,
  `partition`/`heal`, and `restart(node_id)` helpers).

## Admin gRPC surface (Phase 2b-4)

The 2b-4 layer puts a production-grade control plane in front of the
Raft-replicated catalog:

- `proto/pd.proto` (`package aresadb.pd.v1`) — 15 strongly-typed
  RPCs: 7 mutations that replicate through the PD Raft log and
  8 reads served from the local state machine. Unlike
  `aresadb-net`'s bincode-over-`bytes` Raft envelope, the admin
  schema is fully typed so any language binding can drive it.
- `PdAdminService` — tonic server adapter over
  `openraft::Raft<PdTypeConfig>` + `PdStateMachine`. Error mapping
  is deliberate: `InvalidArgument` for malformed requests
  (never touches Raft), `FailedPrecondition` for catalog
  rejections (replicated, then declined), `Unavailable` for Raft
  errors. `ForwardToLeader` attaches the suggested leader id in a
  custom `pd-leader-id` metadata trailer so clients retry in
  one hop.
- `PdAdminClient` — typed Rust wrapper over the generated tonic
  client. Returns native Rust types and surfaces leader hints as
  a dedicated `PdAdminClientError::NotLeader(Option<id>)` variant.
- `HeartbeatLoop` — cancellation-safe background task that sends
  `HeartbeatNode` RPCs at a fixed cadence. Optional
  `EndpointResolver` closure lets the loop rotate when it
  receives a leader hint; swappable `ClockFn` keeps tests
  deterministic; `HeartbeatHandle::stop()` / drop both trigger
  graceful shutdown.
- Operator CLI: `aresadb-cluster pd <subcommand>` (see
  `crates/aresadb-cluster/src/bin/cli.rs`) wraps all 15 RPCs plus a
  long-running `heartbeat-loop` that follows leader hints when
  `--peer ID=URL` pairs are supplied.

## Why Raft-replicate the catalog at all

A placement driver that stores range placement in memory is a single
point of failure: one crash and the cluster forgets where every range
lives. CockroachDB / TiKV / YugabyteDB all replicate their placement
metadata via Raft for exactly this reason. We piggyback on
`aresadb-raft` to get that for free.

## Persistence keyspace

PD catalog rows live under the unified keyspace's `/m/` prefix (see
[`aresadb_core::keys::prefix::METADATA`]); Raft-adapter state lives
under the reserved `0xff` prefix:

```
/m/pd/r/<range_id_be:8>    -> bincode(RangeDescriptor)
/m/pd/n/<node_id_be:8>     -> bincode(NodeInfo)
\xff/pd/sm/meta            -> bincode({ last_applied, last_membership })
```

`next_range_id` is **derived** on open from
`max(stored range_id) + 1` rather than persisted separately; see the
Phase 2b-2 decision log entry for why. The `\xff`-prefixed Raft meta
row is written atomically in the same `WriteBatch` as the touched
catalog rows (see `PdStateMachine::apply_with_meta`), so a crash can
never leave Raft's applied pointer ahead of durable catalog state.

Row layout is stable across Phase 2b releases — adding fields happens
via `bincode`'s structural compatibility or an explicit on-disk
version bump, tracked by [`aresadb_core::FORMAT_VERSION`].
