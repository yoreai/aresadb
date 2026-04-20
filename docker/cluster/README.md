# AresaDB v2 — 3-node cluster (Docker Compose)

Phase 1d integration harness. Brings up a real three-voter AresaDB v2
cluster on your laptop using the `aresadb-cluster` operator CLI. Every
node runs in its own container with its own redb-backed log and state
machine, talks to its peers over gRPC on the `aresadb-cluster` Docker
network, and persists data to a named volume so container restarts
recover the cluster rather than bootstrapping a new one.

This is the first "production-shaped" deployment of AresaDB v2: real
hostnames, real network, real on-disk storage. The same binary and
same config shape run in Kubernetes later.

---

## Layout

```
docker/cluster/
├── Dockerfile           # multi-stage build of the aresadb-cluster binary
├── docker-compose.yml   # 3 services: aresadb-node-1..3
├── bootstrap.sh         # one-shot: promote nodes 2 and 3 to voters, smoke test
├── multi-range.sh       # Phase 2c-6: multi-range smoke (open range 42, write/read, assert isolation)
└── README.md            # you are here
```

Ports on the host:

| Node         | Container port | Host port |
|--------------|----------------|-----------|
| node-1       | `7001`         | `7001`    |
| node-2       | `7001`         | `7002`    |
| node-3       | `7001`         | `7003`    |

All inter-node RPCs (Raft + admin) use the container hostnames
(`http://aresadb-node-{1,2,3}:7001`) over the `aresadb-cluster`
network.

---

## Quick start

From the repo root (`aresadb/`):

```bash
docker compose -f docker/cluster/docker-compose.yml up -d --build
bash    docker/cluster/bootstrap.sh
```

`bootstrap.sh` does the operator work you'd otherwise do by hand:

1. Waits for node-1 to finish `bootstrap` and become the single-voter
   leader.
2. Adds node-2 and node-3 as voters (learner + `ChangeMembership`, in
   one shot, via the admin `AddVoter` flow).
3. Writes one key through Raft (`aresadb-cluster write`).
4. Reads that key back from all three nodes, retrying briefly while
   followers catch up.
5. Prints `aresadb-cluster status` so you can see the cluster
   topology.

Expected output ends with something like:

```
bootstrap: reading 'hello' back from every node
  http://aresadb-node-1:7001: world
  http://aresadb-node-2:7001: world
  http://aresadb-node-3:7001: world
bootstrap: final status:
{
  "node_id": 1,
  "current_leader": 1,
  "membership": { "voters": [1, 2, 3], "learners": [] },
  ...
}
```

---

## Operator flows

All commands below run a throwaway `aresadb-cluster` container on the
cluster network, so you never need the binary on the host.

```bash
admin() {
  docker run --rm --network aresadb-cluster \
    --entrypoint /usr/local/bin/aresadb-cluster \
    aresadb-cluster:2.0.0-alpha.2 "$@"
}
```

Write a value:

```bash
admin write --leader http://aresadb-node-1:7001 --key foo --value bar
```

Read from any node (followers too — reads go to the local state
machine):

```bash
admin read --addr http://aresadb-node-2:7001 --key foo
```

Dump cluster status (JSON):

```bash
admin status --addr http://aresadb-node-1:7001
```

Replace the voter set (example: shrink to {1, 2} while keeping 3 as a
learner):

```bash
admin change-membership \
  --leader http://aresadb-node-1:7001 \
  --voters 1,2 \
  --retain-learners
```

---

## Multi-range smoke (Phase 2c-6)

Once the default-range cluster is up, open a second range and
exercise the range-aware admin surface:

```bash
bash docker/cluster/multi-range.sh
```

This script:

1. Opens a brand-new range `42` on node-1 as a single-voter Raft
   group via `aresadb-cluster add-range --bootstrap-as-voter`.
2. Writes `r42/hello = phase-2c-6` through the Phase 2c-6 range-
   aware `Write` RPC (`--range-id 42`).
3. Reads the key back from node-1 under both `linearizable` and
   `stale` consistency.
4. Confirms node-2 / node-3 return `NOT_FOUND` for range 42 since
   the range isn't registered there — this is the cross-process
   isolation check that matches the `MultiRangeApplyDeterminism`
   scenario in `aresadb-sim`.
5. Dumps `list-ranges` on every node to show the divergent local
   views.

Multi-node replication of non-default ranges is deferred to Phase 2d
because the cluster-admin `AddLearner` / `ChangeMembership` RPCs
still target the default range only. The Phase 2c-6 smoke is honest
about what the current wire can guarantee — single-voter range
isolation plus the full Phase 2c-5 read-path for non-default ranges.

---

## Failure injection by hand

Kill the current leader and watch 2 or 3 take over:

```bash
docker compose -f docker/cluster/docker-compose.yml stop aresadb-node-1
admin status --addr http://aresadb-node-2:7001 | jq .current_leader
```

Bring it back — it rejoins and catches up from the Raft log:

```bash
docker compose -f docker/cluster/docker-compose.yml start aresadb-node-1
admin status --addr http://aresadb-node-1:7001 | jq .current_leader
```

Follower catch-up is automatic because the cluster ships state via
openraft's log replication + `InstallSnapshot` RPCs.

---

## Teardown

```bash
docker compose -f docker/cluster/docker-compose.yml down           # keep data
docker compose -f docker/cluster/docker-compose.yml down -v        # wipe volumes
```

---

## Design notes

- **Single gRPC port per node.** Raft transport and the admin API
  share `7001` inside the container. Admin is considered internal —
  external clients come later via the gateway (Phase 3+).
- **`bootstrap` vs. `join`.** Node 1 uses `bootstrap` (init a
  single-voter Raft group); nodes 2 and 3 use `join` (start a fresh
  node and wait to be added). This mirrors the production pattern for
  growing a cluster from one node.
- **Durability.** Each container mounts a named volume at
  `/var/lib/aresadb/data`. The Raft log and state machine both live
  there, and `ClusterNode::start` re-hydrates the state machine's
  `last_applied` / `last_membership` from disk via `StateMachineStore::open`.
- **Healthchecks.** Each node's healthcheck calls
  `aresadb-cluster status --addr http://127.0.0.1:7001`, which hits the
  admin gRPC service and returns only when the Raft layer is fully
  initialised. That's why `depends_on: { condition: service_healthy }`
  on node-1 is meaningful: node-2 and node-3 only start `join`'ing
  once node-1's admin server is actually up.
