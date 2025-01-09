# Multi-stage build for minimal image size

# === Build ===
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml ./

# Dummy src for dependency caching
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src target/release/deps/aresadb*

COPY src ./src
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
      org.opencontainers.image.version="0.2.1" \
      org.opencontainers.image.source="https://github.com/yoreai/aresadb" \
      org.opencontainers.image.licenses="MIT"
