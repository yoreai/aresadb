#!/usr/bin/env bash
# Stop and remove the local cloud emulators (volumes are tmpfs, nothing persists).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[aresadb] Stopping cloud emulators..."
docker compose -f docker-compose.test.yml down -v --remove-orphans
echo "[aresadb] Done."
