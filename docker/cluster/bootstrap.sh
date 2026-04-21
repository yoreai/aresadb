#!/usr/bin/env bash
# AresaDB v2 — 3-node cluster bootstrap script (Phase 1d)
#
# Assumes the compose stack is already running:
#   docker compose -f docker/cluster/docker-compose.yml up -d
#
# Then exercises the full Phase 1c admin path end to end:
#   1. add node-2 and node-3 as learners on the leader,
#   2. promote them to voters,
#   3. write a couple of keys,
#   4. read them back from each node,
#   5. dump cluster status.
#
# Every admin RPC goes through a throwaway aresadb-cluster container
# attached to the same Docker network, so you don't need the binary on
# the host.

set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"
# IMAGE is overridable so the same script can drive the locally-built
# `aresadb-cluster:2.0.0-alpha.2` (default, matches docker-compose.yml)
# or the published `ghcr.io/yoreai/aresadb/cluster:2.0.0-alpha.2`
# (used together with docker-compose.ghcr.yml). Operators on a clean
# machine should `export IMAGE=ghcr.io/yoreai/aresadb/cluster:2.0.0-alpha.2`
# before invoking this script.
IMAGE="${IMAGE:-aresadb-cluster:2.0.0-alpha.2}"
NETWORK="aresadb-cluster"

LEADER="http://aresadb-node-1:7001"
NODE_2="http://aresadb-node-2:7001"
NODE_3="http://aresadb-node-3:7001"

# Run a one-shot admin command inside a disposable container on the
# cluster network. Each call here is a single `aresadb-cluster ...`
# invocation.
admin() {
    docker run --rm \
        --network "${NETWORK}" \
        --entrypoint /usr/local/bin/aresadb-cluster \
        "${IMAGE}" \
        "$@"
}

wait_for_leader() {
    local attempt
    for attempt in $(seq 1 30); do
        if admin status --addr "${LEADER}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "bootstrap: timed out waiting for ${LEADER}" >&2
    return 1
}

echo "bootstrap: waiting for node-1 admin API at ${LEADER}"
wait_for_leader

echo "bootstrap: adding node-2 and node-3 as voters"
admin add-voter --leader "${LEADER}" --node-id 2 --addr "${NODE_2}"
admin add-voter --leader "${LEADER}" --node-id 3 --addr "${NODE_3}"

echo "bootstrap: committing a sample write on the leader"
admin write --leader "${LEADER}" --key hello --value world

echo "bootstrap: reading 'hello' back from every node"
for addr in "${LEADER}" "${NODE_2}" "${NODE_3}"; do
    # Replication is async; a brief spin gives followers time to apply.
    for _ in $(seq 1 20); do
        val="$(admin read --addr "${addr}" --key hello 2>/dev/null || true)"
        if [[ "${val}" == "world" ]]; then
            echo "  ${addr}: ${val}"
            break
        fi
        sleep 0.5
    done
    if [[ "${val}" != "world" ]]; then
        echo "bootstrap: node at ${addr} never saw the write" >&2
        exit 1
    fi
done

echo "bootstrap: final status:"
admin status --addr "${LEADER}"

echo "bootstrap: cluster is live. Useful commands:"
echo "  docker compose -f ${COMPOSE_FILE} logs -f"
echo "  docker compose -f ${COMPOSE_FILE} stop aresadb-node-1"
echo "  bash $(dirname "${BASH_SOURCE[0]}")/bootstrap.sh       # idempotent"
