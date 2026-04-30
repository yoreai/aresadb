# AresaDB v2 — Operations Runbook

> **Scope.** What an operator actually needs to stand up, inspect, and
> recover a `v2.0.0-alpha.2` cluster — bootstrap, ports, data directories,
> the `aresadb-cluster` / `aresadb-pd` admin surfaces, failure injection,
> upgrades, and the knobs that matter most often.
>
> This runbook is the operational counterpart to
> [`architecture-v2.md`](./architecture-v2.md) (design) and
> [`phase-status.md`](./phase-status.md) (what's in / what's out of each
> phase). When a behaviour here is covered in more depth by one of those
> docs, the section links down to it instead of restating.
>
> **Status** — alpha. The data plane is stable enough for deterministic
> simulation and a 3-node Docker Compose smoke; it is **not** a
> production deployment target yet. Known limitations are listed in
> [§8](#8-known-limitations).

---

## 1. Components

| Component | What it is | Crate(s) | Default listen port |
|-----------|------------|----------|---------------------|
| **Cluster node** | A storage + compute node. Hosts one or more ranges (Raft groups) and serves admin RPCs. | `aresadb-cluster` (bin: `aresadb-cluster`) | `7001` |
| **Placement driver (PD)** | Replicated metadata service. Owns the range→nodes map, leases, and cluster-wide admin. Runs as its own 3-node Raft group. | `aresadb-pd` (exposed via `aresadb-cluster pd ...`) | `8001` |
| **Admin CLI** | Single binary. `aresadb-cluster` for node lifecycle + range-aware I/O; `aresadb-cluster pd ...` for the placement-driver control plane. | `aresadb-cluster` | — |

A full cluster is **N cluster nodes + 3 PD voters + 1 admin CLI**. The
simplest honest deployment is `N=3` nodes + `3` PD voters, which is what
the Docker Compose smoke stands up.

---

## 2. Data directory layout

Every cluster node owns a single data directory (`--data-dir` /
`ARESADB_DATA_DIR`, default varies by deployment). Everything is under
that root; nothing is kept in `$HOME` or `/tmp`. The layout is:

```
<data-dir>/
├── raft-log/                   # redb — Raft log for the default range (range_id=1)
├── state-machine/              # redb — default-range state machine
└── ranges/
    ├── <range_id>/
    │   ├── log/                # redb — Raft log for this range
    │   └── data/
    │       ├── data.redb       # when NodeConfig::data_engine = Redb
    │       └── data.lsm/       # when NodeConfig::data_engine = Lsm  (fjall dir)
    └── ...
```

- **Durability.** redb is single-file, `fsync`-per-commit. fjall is
  journal + memtable + LSM tree, `PersistMode::SyncAll`. Both are safe
  across `kill -9`; replaying the Raft log on restart rebuilds any
  in-memory state.
- **Storage engine knob.** `DataEngine::Lsm` (opt-in, Phase 2d) only
  affects the **data** backend under `ranges/<id>/data/`. The Raft log
  stays on redb because append-heavy, single-writer, one-fsync-per-commit
  is redb's sweet spot — an LSM there would only add write
  amplification for no win. See
  [`docs/architecture-v2.md`](./architecture-v2.md) §4.4.
- **Node-id stability.** The data directory encodes the node's identity
  (it contains that node's Raft log). Never move a data directory to a
  different `--node-id` — that will corrupt the Raft log's view of
  membership. If you need a fresh node, wipe the directory and start with
  `join`.

---

## 3. Bringing up a cluster

### 3a. Local Docker Compose (3 nodes, single-node PD)

This is the canonical smoke harness. It's what runs in CI and what the
Phase 1d / Phase 2c-6 decision logs in
[`phase-status.md`](./phase-status.md) depend on. Two variants:

**Build from source** (developer loop, default):

```bash
docker compose -f docker/cluster/docker-compose.yml up -d --build
bash docker/cluster/bootstrap.sh        # promotes nodes 2 + 3 to voters
bash docker/cluster/multi-range.sh      # opens range 42, exercises range isolation
```

**Pull from GHCR** (operator path, no Rust toolchain on the host):

```bash
docker compose \
  -f docker/cluster/docker-compose.yml \
  -f docker/cluster/docker-compose.ghcr.yml \
  up -d
IMAGE=ghcr.io/yoreai/aresadb/cluster:2.0.0-alpha.2 \
  bash docker/cluster/bootstrap.sh
IMAGE=ghcr.io/yoreai/aresadb/cluster:2.0.0-alpha.2 \
  bash docker/cluster/multi-range.sh
```

Both modes are idempotent — re-running either script is safe once the
cluster is healthy. The image runs an entrypoint that chowns the
data directory at startup and drops to the unprivileged `aresadb`
user via `gosu`, which is what makes the volume mount work the same
on Linux runners and Docker Desktop for Mac/Windows. Set
`ARESADB_RUN_AS_ROOT=1` only when you need to bind-mount a host
directory whose UID/GID you want to preserve.

See [`docker/cluster/README.md`](../docker/cluster/README.md) for a
full walk-through including expected output and the operator flows.

### 3b. Bare-metal / systemd (1-node-per-host)

The single binary is `aresadb-cluster`. Every subcommand takes
`--node-id`, `--listen`, `--advertise`, and `--data-dir` (or reads them
from `ARESADB_NODE_ID`, `ARESADB_LISTEN`, `ARESADB_ADVERTISE`,
`ARESADB_DATA_DIR` so systemd units stay one line).

The pattern is always **one node bootstraps; the rest join**:

```bash
# Host A (seed) — bootstraps a single-voter Raft group.
aresadb-cluster bootstrap \
  --node-id 1 \
  --listen 0.0.0.0:7001 \
  --advertise http://host-a.internal:7001 \
  --data-dir /var/lib/aresadb/data

# Host B — starts fresh and waits to be added.
aresadb-cluster join \
  --node-id 2 \
  --listen 0.0.0.0:7001 \
  --advertise http://host-b.internal:7001 \
  --data-dir /var/lib/aresadb/data

# From any fourth machine — promote the joiner to a voter.
aresadb-cluster add-voter \
  --leader http://host-a.internal:7001 \
  --node-id 2 \
  --addr http://host-b.internal:7001
```

Repeat the `join` + `add-voter` pair for host C, then run
`aresadb-cluster status --addr http://host-a.internal:7001` and
verify `membership.voters == [1, 2, 3]`.

### 3c. Tuning the data engine

LSM on hot ranges, redb everywhere else:

```bash
# Before starting the binary: set the engine via the config layer.
# (Today the CLI inherits the default DataEngine::Redb — flipping the
# knob happens via the NodeConfig::with_data_engine builder in any
# embedding that wraps the cluster binary.)
```

See [`phase-status.md`](./phase-status.md) "Phase 2d" for the
full rollout path. The decision log entry for the LSM engine spells
out the invariants (Raft log stays on redb, data backend per-range can
be either).

---

## 4. Everyday operator flows

All commands here are idempotent; running them twice is never worse
than running them once.

### 4a. Read-only inspection

```bash
# Cluster node state (leader, term, membership, last applied log id).
aresadb-cluster status --addr http://host-a.internal:7001

# Every range registered locally on this node (with Raft metrics + lease info).
aresadb-cluster list-ranges --addr http://host-a.internal:7001

# PD's view of the entire cluster (authoritative catalog).
aresadb-cluster pd status     --addr http://pd-a.internal:8001
aresadb-cluster pd list-ranges --addr http://pd-a.internal:8001
aresadb-cluster pd list-nodes  --addr http://pd-a.internal:8001
```

A useful sanity check: `pd list-ranges` must agree with
`list-ranges` run on every voter for that range. Divergence means the
`pd_supervisor` on some node hasn't reconciled — wait ~one heartbeat
period and re-diff.

### 4b. Single-key I/O (back-compat default range)

```bash
aresadb-cluster write --leader http://host-a.internal:7001 --key foo --value bar
aresadb-cluster read  --addr   http://host-b.internal:7001 --key foo
```

`write` hits the leader's default range (`range_id=1`) and returns
once the entry commits. `read` defaults to `unspecified` consistency
(the Phase 1c raw backend read). Pass `--consistency linearizable` to
go through the leader-lease + ReadIndex path, or
`--consistency stale` for a bounded-staleness follower read.

### 4c. Range-aware I/O

Once `pd create-range` has placed a range and the `pd_supervisor` on
each node has converged, every `write` / `read` can target that range
explicitly with `--range-id <id>`:

```bash
aresadb-cluster write \
  --leader http://host-a.internal:7001 \
  --key "r42/hello" --value "world" \
  --range-id 42

aresadb-cluster read \
  --addr http://host-a.internal:7001 \
  --key "r42/hello" \
  --range-id 42 \
  --consistency linearizable
```

Writes must hit that range's leader — the admin server responds with
`FAILED_PRECONDITION` and an `x-aresa-leader-id` metadata header
otherwise, so re-routing is one hop.

### 4d. Changing the voter set

```bash
# Promote an existing learner to a voter.
aresadb-cluster add-voter \
  --leader http://host-a.internal:7001 \
  --node-id 4 --addr http://host-d.internal:7001

# Replace the voter set wholesale (drop node 1, keep 2 / 3 / 4).
aresadb-cluster change-membership \
  --leader http://host-a.internal:7001 \
  --voters 2,3,4 \
  --retain-learners
```

Both commands are openraft `ChangeMembership` RPCs under the hood, so
they go through a proper joint-consensus commit. The CLI waits for
the second commit (the post-consensus membership) before returning.

### 4e. Creating, splitting, merging ranges (via PD)

```bash
# Create a brand-new range owned by nodes 1 / 2 / 3.
aresadb-cluster pd create-range \
  --addr http://pd-a.internal:8001 \
  --range-id 42 \
  --start-key "r42/" \
  --end-key   "r42/~" \
  --replicas 1:1,2:1,3:1

# Split an existing range at a middle key.
aresadb-cluster pd split-range \
  --addr http://pd-a.internal:8001 \
  --range-id 7 \
  --at-key "r7/mid"

# Merge two adjacent ranges.
aresadb-cluster pd merge-range \
  --addr http://pd-a.internal:8001 \
  --left 8 --right 9
```

These commands return **after the PD Raft log commits** the intent. The
data-plane convergence (opening per-range backends on each target
node, bootstrapping as voter) is asynchronous; watch `list-ranges` on
the target nodes to confirm the `pd_supervisor` has finished.

---

## 5. Failure injection & recovery

The cluster is alpha but the data plane survives the usual failure
primitives — the test harness exercises each one.

### 5a. Kill the leader

```bash
docker compose -f docker/cluster/docker-compose.yml stop aresadb-node-1
aresadb-cluster status --addr http://aresadb-node-2:7001 | jq .current_leader
```

A new leader (2 or 3) wins the election within one election timeout
(~150-300 ms by default). Linearizable reads against the old leader's
address now fail fast — point the CLI at a live voter instead.

### 5b. Revive the former leader

```bash
docker compose -f docker/cluster/docker-compose.yml start aresadb-node-1
aresadb-cluster status --addr http://aresadb-node-1:7001 | jq .current_leader
```

It rejoins as a follower and catches up via openraft's log replication
+ `InstallSnapshot` RPCs. No operator action required.

### 5c. Lose a data directory

If a node's `<data-dir>` is gone — disk failure, accidental wipe — the
only safe recovery is to **treat the node as a new learner**:

1. Remove the voter on the leader:
   `aresadb-cluster change-membership --leader ... --voters <remaining>`
2. Delete the remaining on-disk cruft on the failed host.
3. Start the host with `aresadb-cluster join --node-id <new-id> ...`
   using a **fresh node id** (reusing the old id confuses openraft's
   log indexing).
4. Promote: `aresadb-cluster add-voter --leader ... --node-id <new-id>`.

Do **not** move another node's data directory onto the failed host to
"rebuild" — it will fork the Raft log and break linearizability.

### 5d. Split-brain / clock skew

openraft is designed around the standard Raft assumption that clocks
are monotonic and relatively close (within one heartbeat). If a host's
clock jumps backward mid-term, the election timer can fire too late
and the node will flap between follower and candidate. Run `chronyd`
or `systemd-timesyncd` on every node and monitor `ntp` drift with your
usual observability stack.

---

## 6. Upgrades

### 6a. Rolling upgrade within a patch line (e.g. 2.0.0-alpha.2 → alpha.3)

Alpha releases are **not** covered by a compatibility promise. Within a
single alpha line the pattern is:

1. Bring up the new binary on one follower at a time. `ClusterNode::start`
   re-hydrates the state machine's `last_applied` / `last_membership`
   from disk, so no special handling is needed.
2. After each upgrade, run `aresadb-cluster status --addr <host>` and
   confirm `current_leader`, `membership`, and `last_applied` are
   consistent across voters.
3. Leave the leader for last — transfer leadership off it (kill ⇒
   election is the simplest primitive today; explicit
   `aresadb-cluster transfer-leader` is tracked for Phase 3).
4. Upgrade the PD voters the same way, one at a time.

### 6b. Major upgrades (v1 embedded ⇄ v2 cluster)

Not supported in-place. The v1 embedded engine uses a different on-disk
layout (`redb` under `data/aresadb.redb`) than a v2 cluster node
(Raft log + state machine + per-range sub-directories). The supported
migration path is:

1. Dump from v1 via the embedded APIs (`StorageEngine::scan`).
2. Bootstrap a v2 cluster.
3. Replay writes through the admin `Write` RPC (range-aware where
   appropriate).

A bulk import path is on the Phase 3+ roadmap; until then, treat v1
and v2 as siblings, not parent/child. See
[`docs/publishing-audit.md`](./publishing-audit.md) §1 for why these
surfaces are kept deliberately separate.

---

## 7. Observability

### 7a. Logs

Every binary honours `RUST_LOG`. The recommended baseline for alpha
deployments:

```
RUST_LOG=info,aresadb_cluster=debug,aresadb_pd=debug,openraft=info
```

- `openraft=info` keeps leader-election / membership-change logs
  visible without drowning in the per-append-entries log spam.
- Bumping `aresadb_cluster=trace` is useful when debugging admin RPC
  routing (range-id mismatch, not-leader redirects); don't leave it
  on under load.

### 7b. Metrics

Structured metrics over `/metrics` are planned for Phase 3. Until then,
every `status` / `list-ranges` response is JSON and cheap to poll on a
fixed interval (a few hundred ms). A minimal Prometheus-shaped bridge
lives in `docker/cluster/multi-range.sh` — grep `jq .current_leader`
and `jq '.ranges[] | select(.id == N)'` style spelunking works today.

### 7c. Dashboards

None ship with alpha.2. The `BENCHMARKS.md` numbers and the v2 cluster
bench scaffold (`benches/v2_cluster_bench.rs`) are the current
quantitative baselines for operator-side performance expectations.

---

## 8. Known limitations

From [`phase-status.md`](./phase-status.md), accurate as of
`v2.0.0-alpha.2`:

- **Single region, single tenant.** No cross-region replication, no
  RBAC, no multi-tenant isolation.
- **No distributed query router.** Every admin RPC targets one range
  at a time. Phase 3 adds a scatter-gather executor.
- **No MVCC / transactions across ranges.** Writes to a single range
  are linearizable; anything broader is on the Phase 4 roadmap.
- **Range replication through the admin wire is single-voter.**
  `aresadb-cluster add-range` bootstraps a range as a single voter;
  replicating a non-default range across all voters still goes
  through the PD supervisor, which is covered by Phase 2c-4 but not
  yet by a Docker-compose smoke.
- **Runtime reconfig of `DataEngine` is per-range, at open time.** You
  cannot flip a running range from redb to fjall; create a new range
  on the desired engine and migrate data through the admin wire.
- **Raft log is always on redb.** By design — see §2.

For the full list, see [`phase-status.md`](./phase-status.md)
"Current phase" and the most recent decision-log entries.

---

## 9. Where things live

| Surface | File |
|---------|------|
| Cluster binary | `crates/aresadb-cluster/src/bin/cli.rs` |
| Node runtime | `crates/aresadb-cluster/src/node.rs`, `range.rs` |
| PD control plane | `crates/aresadb-pd/src/**` |
| Config shape | `crates/aresadb-cluster/src/config.rs` |
| Docker smoke | `docker/cluster/{docker-compose.yml,bootstrap.sh,multi-range.sh,README.md}` |
| Deterministic sim | `crates/aresadb-sim/` |
| Benchmarks | `benches/v2_cluster_bench.rs`, `benchmarks/` |
| Architecture | `docs/architecture-v2.md` |
| Phase log | `docs/phase-status.md` |
| Publishing audit | `docs/publishing-audit.md` |
| This runbook | `docs/operations.md` |
