# syntax=docker/dockerfile:1.7

# -----------------------------------------------------------------------------
# AresaDB v2 — 3-node cluster image
#
# Builds the `aresadb-cluster` operator binary out of the workspace and
# ships it as a minimal debian-slim runtime image. Used by
# `docker/cluster/docker-compose.yml` to bring up a real 3-voter cluster
# on a single host for integration testing.
# -----------------------------------------------------------------------------

FROM rust:1.85-slim-bookworm AS builder

# aresadb-net uses protoc-bin-vendored, so no system protoc is strictly
# required. But pkg-config + libssl stay useful for openraft/tonic's
# transitive crates. (Keeping this minimal; add if a future dep needs it.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy just enough to resolve the workspace and cache dependency builds
# before the real source lands. Every workspace member's Cargo.toml has
# to be present; the source trees are stubbed until the final COPY.
COPY Cargo.toml Cargo.lock ./
COPY crates/aresadb-core/Cargo.toml        crates/aresadb-core/Cargo.toml
COPY crates/aresadb-raft/Cargo.toml        crates/aresadb-raft/Cargo.toml
COPY crates/aresadb-net/Cargo.toml         crates/aresadb-net/Cargo.toml
COPY crates/aresadb-engine-redb/Cargo.toml crates/aresadb-engine-redb/Cargo.toml
COPY crates/aresadb-engine-lsm/Cargo.toml  crates/aresadb-engine-lsm/Cargo.toml
COPY crates/aresadb-cluster/Cargo.toml     crates/aresadb-cluster/Cargo.toml
COPY crates/aresadb-pd/Cargo.toml          crates/aresadb-pd/Cargo.toml
COPY crates/aresadb-sim/Cargo.toml         crates/aresadb-sim/Cargo.toml

# Stubs so `cargo fetch` and the warmup build see valid crates.
# The root `aresadb` crate declares additional `[[bin]]`,
# `[[bench]]`, and `[[example]]` targets; we stub them all to keep
# Cargo manifest validation happy during the warmup phase. The real
# sources land with the later `COPY crates / COPY src ... ./` steps.
RUN set -eux; \
    mkdir -p src src/bin \
             benches examples benchmarks \
             crates/aresadb-core/src crates/aresadb-raft/src \
             crates/aresadb-net/src crates/aresadb-engine-redb/src \
             crates/aresadb-engine-lsm/src \
             crates/aresadb-cluster/src crates/aresadb-cluster/src/bin \
             crates/aresadb-pd/src \
             crates/aresadb-sim/src; \
    echo 'fn main() {}'      > src/main.rs; \
    echo 'fn main() {}'      > src/bin/server.rs; \
    echo 'pub fn lib() {}'   > src/lib.rs; \
    for c in aresadb-core aresadb-raft aresadb-net aresadb-engine-redb \
             aresadb-engine-lsm aresadb-cluster aresadb-pd aresadb-sim; do \
        echo 'pub fn lib() {}' > "crates/${c}/src/lib.rs"; \
    done; \
    echo 'fn main() {}' > crates/aresadb-cluster/src/bin/cli.rs; \
    for b in storage_bench query_bench distributed_bench v2_cluster_bench; do \
        echo 'fn main() {}' > "benches/${b}.rs"; \
    done; \
    echo 'fn main() {}' > examples/fire_safety_test.rs; \
    echo 'fn main() {}' > examples/tiered_storage_demo.rs; \
    echo 'fn main() {}' > benchmarks/run_benchmarks.rs

# Warm the cargo registry cache. The aresadb-cluster bin itself needs
# build.rs + proto files we haven't copied yet, so we just fetch here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo fetch --locked

# Now lay down the real sources and build the cluster bin.
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin aresadb-cluster \
        -p aresadb-cluster \
    && cp /src/target/release/aresadb-cluster /aresadb-cluster

# -----------------------------------------------------------------------------
# Runtime image
# -----------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/aresadb --create-home --shell /usr/sbin/nologin aresadb

COPY --from=builder /aresadb-cluster /usr/local/bin/aresadb-cluster

USER aresadb
WORKDIR /var/lib/aresadb

# Default env; individual compose services override these.
ENV RUST_LOG=info \
    ARESADB_DATA_DIR=/var/lib/aresadb/data

VOLUME ["/var/lib/aresadb/data"]

EXPOSE 7001

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/aresadb-cluster"]
CMD ["--help"]

LABEL org.opencontainers.image.title="AresaDB cluster" \
      org.opencontainers.image.description="AresaDB v2 distributed cluster node (aresadb-cluster CLI)" \
      org.opencontainers.image.version="2.0.0-alpha.2" \
      org.opencontainers.image.source="https://github.com/yoreai/aresadb" \
      org.opencontainers.image.licenses="MIT"
