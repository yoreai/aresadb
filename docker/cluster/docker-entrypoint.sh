#!/usr/bin/env bash
# AresaDB v2 cluster image — runtime entrypoint.
#
# This script is invoked by `tini --` (PID 1) and is responsible for
# making the on-disk state writable before exec'ing the
# `aresadb-cluster` binary as the unprivileged `aresadb` user.
#
# Why this exists:
#
# Docker named volumes are created as root:root regardless of the
# image's pre-existing ownership, *except* on Linux hosts where the
# mount point in the image already contains files (Docker then copies
# both content and ownership). The aresadb image ships an empty
# `/var/lib/aresadb/data` directory, so on Docker Desktop for
# Mac/Windows the mounted volume materialises as root:root and the
# non-root `aresadb` user inside the container can't write redb /
# fjall files there. The fix is to start as root, chown the data
# directory to aresadb, and drop privileges via `gosu` before exec.
#
# On hosts where the volume already had the right ownership (most
# Linux runners, including the GitHub Actions docker-smoke job), the
# chown is a no-op and the only observable side-effect is one extra
# fork/exec on container start.
#
# Operators who want to keep running as root (e.g. when bind-mounting
# a host directory whose UID/GID they want to preserve) can set
# `ARESADB_RUN_AS_ROOT=1` to skip the privilege drop.

set -euo pipefail

DATA_DIR="${ARESADB_DATA_DIR:-/var/lib/aresadb/data}"

# Path-existence check is intentionally cheap: we always run mkdir +
# chown so that a clean named-volume bring-up converges to the right
# ownership, and a re-run on an existing volume is a fast no-op.
if [[ "$(id -u)" -eq 0 ]]; then
    mkdir -p "${DATA_DIR}"
    chown -R aresadb:aresadb "${DATA_DIR}"

    if [[ "${ARESADB_RUN_AS_ROOT:-0}" == "1" ]]; then
        exec /usr/local/bin/aresadb-cluster "$@"
    fi

    exec gosu aresadb /usr/local/bin/aresadb-cluster "$@"
fi

# Already running as a non-root user (e.g. someone passed
# `--user 1000:1000` on `docker run`): nothing to chown, just exec.
exec /usr/local/bin/aresadb-cluster "$@"
