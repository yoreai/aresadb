# Multi-stage build for minimal image size

# === Build ===
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Workspace manifest + every member's manifest first (dependency cache).
COPY Cargo.toml ./
COPY crates/aresadb-core/Cargo.toml crates/aresadb-core/Cargo.toml
COPY crates/aresadb-sim/Cargo.toml crates/aresadb-sim/Cargo.toml

# Dummy srcs for each member so `cargo build` resolves/builds deps.
RUN mkdir -p src crates/aresadb-core/src crates/aresadb-sim/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs && \
    echo "pub fn lib() {}" > crates/aresadb-core/src/lib.rs && \
    echo "pub fn lib() {}" > crates/aresadb-sim/src/lib.rs && \
    cargo build --release --bin aresadb 2>/dev/null || true && \
    rm -rf src crates/aresadb-core/src crates/aresadb-sim/src \
           target/release/deps/aresadb*

COPY src ./src
COPY crates ./crates
COPY tests ./tests
COPY benches ./benches
COPY examples ./examples

RUN cargo build --release --bin aresadb

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
