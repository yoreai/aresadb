# aresadb-cluster

Cluster lifecycle and admin API for AresaDB v2. Everything you need
to go from "I have three empty machines" to "I have a replicated
AresaDB cluster" lives here.

## Layers

* [`NodeConfig`] — declarative description of a node's identity,
  listen address, peer directory, and on-disk layout.
* [`ClusterNode`] — the runtime object. Owns the `openraft::Raft`
  handle, both persistent backends (Raft log + state machine data),
  the gRPC transport server task, and the admin API.
* [`admin`] — protobuf-defined admin service (bootstrap, add-voter,
  remove-voter, change-membership, write, status) on top of which the
  CLI is built.
* `aresadb-cluster` binary — thin CLI wrapper for operators. Example:

      aresadb-cluster bootstrap --node-id 1 --listen 127.0.0.1:7001 --data-dir /var/lib/aresa/1
      aresadb-cluster status    --addr    127.0.0.1:7001

* `aresadb-cluster pd <subcommand>` — operator frontend for the
  [`aresadb-pd`] placement-driver admin gRPC surface. All 15
  `PlacementDriverAdmin` RPCs are exposed one-for-one, plus a
  long-running `heartbeat-loop` that follows leader hints through
  `--peer ID=URL` entries. Examples:

      aresadb-cluster pd status --addr http://127.0.0.1:8601
      aresadb-cluster pd create-range \
          --start-key ""  --end-key zz \
          --replica 1:1:voter --replica 2:1:voter --replica 3:1:voter \
          --raft-group-id 42 --addr http://127.0.0.1:8601
      aresadb-cluster pd heartbeat-loop \
          --node-id 3 --interval-ms 500 \
          --addr http://127.0.0.1:8601 \
          --peer 1=http://127.0.0.1:8601 \
          --peer 2=http://127.0.0.1:8602

  Errors surface `ForwardToLeader` hints (`[hint: current leader is
  node N]`) so scripts can retry against the right replica in one
  hop.

## Durability

Every node gets its own directory laid out like this:

```
<data-dir>/
├── raft-log/        # redb file holding raft log entries + meta
├── state-machine/   # redb file holding user keyspace
└── ranges/          # per-range backends (Phase 2c)
    └── <range_id>/
        ├── log/     # per-range Raft log backend
        └── data/    # per-range state-machine backend
```

Both top-level directories (`raft-log/` + `state-machine/`) are
created on first `bootstrap` / `join` and used by the Phase 1
single-group `ClusterNode`. The new `ranges/<range_id>/` layout is
owned by `RangeRuntime` (below) and is created on demand the first
time a range opens on this node.

Restarting the node reopens the exact same files and replays any
post-snapshot log entries — `aresadb-raft` state machine `apply` is
idempotent, so this works without operator intervention.

## `RangeRuntime` — per-range Raft (Phase 2c-2)

`aresadb_cluster::range::RangeRuntime` owns one range's full runtime
state: its [`RangeDescriptor`](../aresadb-pd/src/types.rs), the
`openraft::Raft<TypeConfig>` handle, and its own `(log, data)`
backend pair under `<data-dir>/ranges/<range_id>/{log,data}/`.
Phase 2c-3 will compose many of these into a `RangeDirectory` on
each node; in isolation, one runtime already exercises every
lifecycle transition:

* `RangeRuntime::open` — low-level constructor taking pre-opened
  backends + any `RaftNetworkFactory<TypeConfig>`. Tests use
  `aresadb_raft::LoopbackNetwork`; production uses
  `aresadb_net::GrpcRaftNetwork::new(directory, raft_group_id)` so
  every range ends up with its own group-scoped network factory on
  the 2c-1 multi-Raft wire.
* `RangeRuntime::open_on_disk` — convenience wrapper that derives
  the backend paths from a `NodeConfig` (`range_log_path` /
  `range_data_path`), opens `RedbBackend`s under
  `<data-dir>/ranges/<range_id>/`, and calls `open`.
* `RangeRuntime::bootstrap_voter` — initialises a brand-new single-
  voter Raft group with this node as the sole member. Idempotent:
  pattern-matches `openraft::error::InitializeError::NotAllowed` on
  the return value so the same call works on fresh bootstrap and
  recovery (recovery just waits for re-election from the on-disk
  log). Multi-voter ranges will use the same primitive on node 1
  plus admin RPCs to grow membership.
* `RangeRuntime::trigger_snapshot` — hook for the Phase 2c-4 PD
  supervisor (and for tests that need deterministic snapshot
  coverage) to request a snapshot build from openraft.
* `RangeRuntime::shutdown` — `raft.shutdown()` followed by
  best-effort `close()` on each backend.

The 5 unit tests in `src/range.rs` verify: (1) `open_on_disk`
creates the on-disk layout; (2) `bootstrap_voter` makes the node
leader and accepts replicated writes; (3) reopen rehydrates
committed writes from disk; (4) two `RangeRuntime`s on the same node
with different `range_id`s operate on fully independent backends;
(5) `trigger_snapshot` queues without error.

## Split between `aresadb-cluster` and `aresadb-net`

`aresadb-net` is strictly peer-to-peer (Raft RPCs). The admin service
is client-facing — different authentication story, different schema
evolution discipline — so it lives here. Both services can coexist on
the same TCP port via `tonic`'s multi-service server.
