# Multi-stage build for the legacy v1 `aresadb` single-process CLI.
#
# This is the standalone embedded-engine image that predates the v2
# distributed cluster. For the v2 multi-Raft cluster image see
# `docker/cluster/Dockerfile`. The workflow-level release publishes
# the cluster image; this Dockerfile is kept for local dev and
# back-compat `docker build .` flows.

# === Build ===
# Rust 1.90+ is required: `fjall` (used by aresadb-engine-lsm in the
# workspace) has an MSRV of 1.90.
FROM rust:1.90-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Workspace manifest + every member's manifest first (dependency cache).
# Keep this list in lockstep with `aresadb/Cargo.toml` `[workspace] members`.
COPY Cargo.toml Cargo.lock ./
COPY crates/aresadb-core/Cargo.toml        crates/aresadb-core/Cargo.toml
COPY crates/aresadb-raft/Cargo.toml        crates/aresadb-raft/Cargo.toml
COPY crates/aresadb-net/Cargo.toml         crates/aresadb-net/Cargo.toml
COPY crates/aresadb-engine-redb/Cargo.toml crates/aresadb-engine-redb/Cargo.toml
COPY crates/aresadb-engine-lsm/Cargo.toml  crates/aresadb-engine-lsm/Cargo.toml
COPY crates/aresadb-cluster/Cargo.toml     crates/aresadb-cluster/Cargo.toml
COPY crates/aresadb-pd/Cargo.toml          crates/aresadb-pd/Cargo.toml
COPY crates/aresadb-sim/Cargo.toml         crates/aresadb-sim/Cargo.toml

# Dummy srcs for each member so `cargo build` resolves/builds deps.
# The root crate declares additional `[[bin]]`, `[[bench]]`, and
# `[[example]]` targets; we stub them all to keep manifest
# validation happy before the real sources land.
RUN set -eux; \
    mkdir -p src src/bin \
             benches examples benchmarks \
             crates/aresadb-core/src crates/aresadb-raft/src \
             crates/aresadb-net/src crates/aresadb-engine-redb/src \
             crates/aresadb-engine-lsm/src \
             crates/aresadb-cluster/src crates/aresadb-cluster/src/bin \
             crates/aresadb-pd/src \
             crates/aresadb-sim/src; \
    echo "fn main() {}"    > src/main.rs; \
    echo "fn main() {}"    > src/bin/server.rs; \
    echo "pub fn lib() {}" > src/lib.rs; \
    for c in aresadb-core aresadb-raft aresadb-net aresadb-engine-redb \
             aresadb-engine-lsm aresadb-cluster aresadb-pd aresadb-sim; do \
        echo "pub fn lib() {}" > "crates/${c}/src/lib.rs"; \
    done; \
    echo "fn main() {}" > crates/aresadb-cluster/src/bin/cli.rs; \
    for b in storage_bench query_bench distributed_bench v2_cluster_bench; do \
        echo "fn main() {}" > "benches/${b}.rs"; \
    done; \
    echo "fn main() {}" > examples/fire_safety_test.rs; \
    echo "fn main() {}" > examples/tiered_storage_demo.rs; \
    echo "fn main() {}" > benchmarks/run_benchmarks.rs; \
    cargo fetch --locked

COPY src ./src
COPY crates ./crates
COPY tests ./tests
COPY benches ./benches
COPY examples ./examples

RUN cargo build --release --locked --bin aresadb

# === Runtime ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 -s /bin/bash aresadb

WORKDIR /app

COPY --from=builder /app/target/release/aresadb /usr/local/bin/aresadb

RUN mkdir -p /data && chown aresadb:aresadb /data

USER aresadb

ENV ARESADB_DATA_DIR=/data

VOLUME /data

CMD ["aresadb", "repl"]

EXPOSE 6379

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD aresadb --version || exit 1

LABEL org.opencontainers.image.title="AresaDB" \
      org.opencontainers.image.description="High-performance multi-model database in Rust" \
      org.opencontainers.image.version="2.0.0-alpha.2" \
      org.opencontainers.image.source="https://github.com/yoreai/aresadb" \
      org.opencontainers.image.licenses="MIT"
