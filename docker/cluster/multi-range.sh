#!/usr/bin/env bash
# AresaDB v2 — multi-range smoke test (Phase 2c-6)
#
# Assumes the 3-node compose stack is already running *and* the
# default-range bootstrap has completed:
#
#   docker compose -f docker/cluster/docker-compose.yml up -d
#   bash    docker/cluster/bootstrap.sh
#   bash    docker/cluster/multi-range.sh
#
# What this exercises:
#
#   1. Open a brand-new range (id 42, key prefix `r42/`) on node-1 as
#      a single-voter Raft group via `aresadb-cluster add-range
#      --bootstrap-as-voter true`.
#   2. Write a key to that range through the admin `Write` RPC with
#      `--range-id 42` — this drives the Phase 2c-6 range-aware
#      routing added to the admin handler.
#   3. Read the key back from node-1 under linearizable consistency —
#      proves the Phase 2c-5 lease-based read path works on a
#      non-default range.
#   4. Verify that node-2, which hasn't registered range 42, returns
#      `NOT_FOUND` for both reads and writes targeting that range —
#      proves cross-range isolation across processes.
#   5. Dump `list-ranges` on every node and show the expected
#      divergence: node-1 has the default range + range 42, nodes 2
#      and 3 still only have the default range.
#
# Multi-node replication of range 42 is intentionally out of scope for
# this smoke. The cluster-admin `AddLearner` / `ChangeMembership` RPCs
# still target the default range only (noted in the Phase 2c-3c
# decision log), so extending this script to replicate range 42
# across all three nodes is part of Phase 2d's "range-aware admin"
# work. Keeping the smoke honest means the failure modes we exercise
# here are exactly the ones the production wire supports today.

set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"
IMAGE="aresadb-cluster:2.0.0-alpha.2"
NETWORK="aresadb-cluster"

NODE_1="http://aresadb-node-1:7001"
NODE_2="http://aresadb-node-2:7001"
NODE_3="http://aresadb-node-3:7001"

RANGE_ID=42
RANGE_KEY="r42/hello"
RANGE_VALUE="phase-2c-6"

# Run a one-shot aresadb-cluster command inside a disposable
# container on the cluster network. Mirrors `bootstrap.sh`.
admin() {
    docker run --rm \
        --network "${NETWORK}" \
        --entrypoint /usr/local/bin/aresadb-cluster \
        "${IMAGE}" \
        "$@"
}

# Like `admin` but captures stderr too so we can assert on gRPC
# error payloads (NOT_FOUND, FAILED_PRECONDITION, …).
admin_with_stderr() {
    docker run --rm \
        --network "${NETWORK}" \
        --entrypoint /usr/local/bin/aresadb-cluster \
        "${IMAGE}" \
        "$@" 2>&1
}

wait_for_default_range() {
    local attempt
    for attempt in $(seq 1 30); do
        if admin status --addr "${NODE_1}" >/dev/null 2>&1; then
            # Double-check the default range is leader-elected. The
            # cluster admin `status` returns JSON whose
            # `current_leader` field is null until an election
            # resolves.
            local leader
            leader="$(admin status --addr "${NODE_1}" 2>/dev/null \
                      | grep -E '"current_leader":' \
                      | head -1 \
                      | sed -E 's/.*"current_leader":[[:space:]]*([0-9]+|null).*/\1/')"
            if [[ "${leader}" != "null" ]] && [[ -n "${leader}" ]]; then
                return 0
            fi
        fi
        sleep 1
    done
    echo "multi-range: timed out waiting for default range to elect a leader on ${NODE_1}" >&2
    return 1
}

echo "multi-range: prerequisites — waiting for default range on ${NODE_1}"
wait_for_default_range

echo "multi-range: step 1 — opening range ${RANGE_ID} on node-1 as single voter"
set +e
add_range_output="$(admin_with_stderr add-range \
    --leader "${NODE_1}" \
    --range-id "${RANGE_ID}" \
    --start-key "r${RANGE_ID}/" \
    --end-key "r${RANGE_ID}/~" \
    --replicas "1:1" \
    --bootstrap-as-voter)"
add_range_rc=$?
set -e
if [[ ${add_range_rc} -ne 0 ]]; then
    # Treat ALREADY_EXISTS as success so the smoke is re-runnable —
    # the range persists on disk across compose runs once it's
    # bootstrapped, so a second execution should skip straight to
    # the read/write checks.
    if grep -q 'already registered\|already exists' <<<"${add_range_output}"; then
        echo "  ${NODE_1}: range ${RANGE_ID} was already registered; continuing"
    else
        echo "multi-range: add-range failed unexpectedly: ${add_range_output}" >&2
        exit 1
    fi
else
    echo "  ${NODE_1}: ${add_range_output}"
fi

echo "multi-range: step 2 — writing ${RANGE_KEY}=${RANGE_VALUE} through the range-aware Write RPC"
admin write \
    --leader "${NODE_1}" \
    --key "${RANGE_KEY}" \
    --value "${RANGE_VALUE}" \
    --range-id "${RANGE_ID}"

echo "multi-range: step 3 — reading back from node-1 under linearizable consistency"
got_linearizable="$(admin read \
    --addr "${NODE_1}" \
    --key "${RANGE_KEY}" \
    --range-id "${RANGE_ID}" \
    --consistency linearizable)"
if [[ "${got_linearizable}" != "${RANGE_VALUE}" ]]; then
    echo "multi-range: linearizable read returned '${got_linearizable}', expected '${RANGE_VALUE}'" >&2
    exit 1
fi
echo "  ${NODE_1} (linearizable): ${got_linearizable}"

echo "multi-range: step 4a — stale read of the same key from node-1"
got_stale="$(admin read \
    --addr "${NODE_1}" \
    --key "${RANGE_KEY}" \
    --range-id "${RANGE_ID}" \
    --consistency stale)"
if [[ "${got_stale}" != "${RANGE_VALUE}" ]]; then
    echo "multi-range: stale read on leader returned '${got_stale}', expected '${RANGE_VALUE}'" >&2
    exit 1
fi
echo "  ${NODE_1} (stale): ${got_stale}"

echo "multi-range: step 4b — node-2 must reject range ${RANGE_ID} reads with NOT_FOUND"
set +e
node2_read_err="$(admin_with_stderr read \
    --addr "${NODE_2}" \
    --key "${RANGE_KEY}" \
    --range-id "${RANGE_ID}" \
    --consistency linearizable)"
node2_read_rc=$?
set -e
if [[ ${node2_read_rc} -eq 0 ]]; then
    echo "multi-range: node-2 accepted a read for unregistered range ${RANGE_ID}: ${node2_read_err}" >&2
    exit 1
fi
if ! grep -q 'range .* not found' <<<"${node2_read_err}"; then
    echo "multi-range: node-2 returned unexpected error: ${node2_read_err}" >&2
    exit 1
fi
echo "  ${NODE_2}: correctly refused range ${RANGE_ID} read"

echo "multi-range: step 4c — node-3 must reject range ${RANGE_ID} writes with NOT_FOUND"
set +e
node3_write_err="$(admin_with_stderr write \
    --leader "${NODE_3}" \
    --key "${RANGE_KEY}" \
    --value "${RANGE_VALUE}-forbidden" \
    --range-id "${RANGE_ID}")"
node3_write_rc=$?
set -e
if [[ ${node3_write_rc} -eq 0 ]]; then
    echo "multi-range: node-3 accepted a write for unregistered range ${RANGE_ID}: ${node3_write_err}" >&2
    exit 1
fi
if ! grep -q 'is not registered on this node' <<<"${node3_write_err}"; then
    echo "multi-range: node-3 returned unexpected error: ${node3_write_err}" >&2
    exit 1
fi
echo "  ${NODE_3}: correctly refused range ${RANGE_ID} write"

echo "multi-range: step 5 — listing ranges on each node"
for addr in "${NODE_1}" "${NODE_2}" "${NODE_3}"; do
    echo "  ${addr}:"
    admin list-ranges --addr "${addr}"
done

echo "multi-range: smoke passed. Useful follow-ups:"
echo "  docker compose -f ${COMPOSE_FILE} logs -f aresadb-node-1"
echo "  bash $(dirname "${BASH_SOURCE[0]}")/multi-range.sh       # idempotent after the first run"
