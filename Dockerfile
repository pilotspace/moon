# =============================================================================
# Moon - Optimized Multi-Stage Docker Build
# =============================================================================
#
# Build arguments:
#   FEATURES  - Cargo feature flags (default: runtime-monoio,jemalloc)
#               Use runtime-tokio,jemalloc for broader compatibility.
#
# Build examples:
#   docker build .
#   docker build --build-arg FEATURES=runtime-tokio,jemalloc .
#   docker buildx build --platform linux/amd64,linux/arm64 .
# =============================================================================

ARG RUST_VERSION=1.88
ARG DEBIAN_VERSION=bookworm

# =============================================================================
# Stage 1: chef-planner
# Computes the dependency recipe from manifests and src/ structure.
# Cached as long as Cargo.toml / Cargo.lock / src/ layout doesn't change.
# =============================================================================
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS chef-planner

# Install cargo-chef for reproducible dependency caching
RUN cargo install cargo-chef --locked

WORKDIR /app

# Copy manifests and source structure (cargo-chef needs bench targets to resolve Cargo.toml)
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/

RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 2: chef-cook (dependency cache layer)
# Downloads and compiles all dependencies.
# This layer is cached until Cargo.toml / Cargo.lock changes, even when src/
# changes. This is the main speedup for incremental builds.
# =============================================================================
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS chef-cook

# Build-time dependencies:
#   cmake        - required by aws-lc-rs (TLS backend)
#   pkg-config   - required by several sys crates
#   build-essential - C compiler for mlua vendored Lua 5.4
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Speed up registry fetching
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
# Disable incremental compilation (not useful in Docker layer builds)
ENV CARGO_INCREMENTAL=0

RUN cargo install cargo-chef --locked

WORKDIR /app

COPY --from=chef-planner /app/recipe.json recipe.json

ARG FEATURES=runtime-monoio,jemalloc

# Cook (compile) all dependencies. This is the cached layer.
RUN cargo chef cook --release --features ${FEATURES} --recipe-path recipe.json

# =============================================================================
# Stage 3: builder
# Compiles only the project source. Dependencies are already compiled above.
# =============================================================================
FROM chef-cook AS builder

# Source code and bench targets (Cargo.toml references bench entries)
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/

ARG FEATURES=runtime-monoio,jemalloc

# Release profile already has: LTO fat, codegen-units=1, strip=true
RUN cargo build --release --features ${FEATURES}

# =============================================================================
# Stage 4: runtime
# Minimal debian-slim image with only the binary and essential runtime libs.
# We use debian-slim (not scratch/distroless) because:
#   - mlua vendored Lua 5.4 links to glibc dynamically
#   - libgcc_s is needed for Rust panic unwinding
#   - ca-certificates are needed for TLS outbound connections
# =============================================================================
FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with /data as home directory (used for AOF persistence)
RUN groupadd -r moon && \
    useradd -r -g moon -d /data -s /sbin/nologin moon && \
    mkdir -p /data && \
    chown moon:moon /data

COPY --from=builder /app/target/release/moon /usr/local/bin/moon

USER moon
WORKDIR /data

# /data is used for AOF persistence files
VOLUME ["/data"]

# Plain Redis protocol port + TLS port
EXPOSE 6379 6443

# Lightweight health check: just verifies the binary runs (no network needed)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD moon --help > /dev/null 2>&1 || exit 1

CMD ["moon", "--bind", "0.0.0.0", "--protected-mode", "no"]
