# =============================================================================
# Moon - Optimized Multi-Stage Docker Build
# =============================================================================
#
# Build arguments:
#   FEATURES  - Cargo feature flags (default: runtime-monoio,jemalloc)
#               Use runtime-tokio,jemalloc for tokio runtime.
#               NOTE: runtime-monoio and runtime-tokio are mutually exclusive.
#
# Build examples:
#   docker build .
#   docker build --build-arg FEATURES=runtime-tokio,jemalloc .
#   docker buildx build --platform linux/amd64,linux/arm64 .
# =============================================================================

ARG RUST_VERSION=1.94
ARG DEBIAN_VERSION=bookworm

# =============================================================================
# Stage 1: chef-base
# Shared base for cargo-chef stages. Installs cargo-chef and build deps once.
# =============================================================================
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS chef-base

# Build-time dependencies:
#   cmake          - required by aws-lc-rs (TLS backend)
#   pkg-config     - required by several -sys crates
#   build-essential - C compiler for mlua vendored Lua 5.4
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_INCREMENTAL=0

RUN cargo install cargo-chef --locked

WORKDIR /app

# =============================================================================
# Stage 2: chef-planner
# Computes dependency recipe from manifests. Cargo.toml references [[bench]]
# entries, so we create dummy stubs to satisfy the manifest without copying
# real bench source (avoids cache invalidation when benchmarks change).
# =============================================================================
FROM chef-base AS chef-planner

COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# Create minimal bench stubs so cargo-chef can resolve Cargo.toml
RUN mkdir -p benches && \
    grep -oP 'name = "\K[^"]+' Cargo.toml | while read -r name; do \
        echo "fn main() {}" > "benches/${name}.rs"; \
    done

RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 3: chef-cook (dependency cache layer)
# Compiles all dependencies. Cached until Cargo.toml/Cargo.lock changes.
# Source changes do NOT invalidate this layer — the main rebuild speedup.
# =============================================================================
FROM chef-base AS chef-cook

COPY --from=chef-planner /app/recipe.json recipe.json

ARG FEATURES=runtime-monoio,jemalloc

# --no-default-features is required: runtime-monoio and runtime-tokio are
# mutually exclusive (compile_error! if both enabled). Without this flag,
# building with FEATURES=runtime-tokio,jemalloc would fail.
RUN cargo chef cook --release --no-default-features --features ${FEATURES} --recipe-path recipe.json

# =============================================================================
# Stage 4: builder
# Compiles only project source. Dependencies are pre-compiled in chef-cook.
# =============================================================================
FROM chef-cook AS builder

COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# Create bench stubs (same as planner — real bench code is not needed for binary)
RUN mkdir -p benches && \
    grep -oP 'name = "\K[^"]+' Cargo.toml | while read -r name; do \
        echo "fn main() {}" > "benches/${name}.rs"; \
    done

ARG FEATURES=runtime-monoio,jemalloc

RUN cargo build --release --no-default-features --features ${FEATURES}

# =============================================================================
# Stage 5: runtime
# Distroless cc image — contains only glibc, libgcc_s, libm, and ca-certs.
# Binary linkage (verified via ldd):
#   libgcc_s.so.1, libm.so.6, libc.so.6 — all present in distroless/cc
#
# Why distroless/cc, not scratch:
#   Moon links dynamically to glibc (mlua vendored Lua, jemalloc, io-uring).
#   Static musl builds are fragile with aws-lc-rs cmake and mlua.
#
# Why not debian-slim:
#   debian-slim ships 96 packages (97MB). Moon needs exactly 3 shared libs.
#   Distroless provides those in 34MB with zero attack surface.
# =============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /app/target/release/moon /usr/local/bin/moon

# /data is used for AOF persistence files
VOLUME ["/data"]
WORKDIR /data

# Plain Redis protocol port + TLS port
EXPOSE 6379 6443

# Exec-form healthcheck (no shell in distroless). Verifies binary executes.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD ["/usr/local/bin/moon", "--help"]

# Distroless :nonroot tag runs as uid 65534 by default.
# --protected-mode no: Docker networking is non-loopback; protected mode
# blocks all connections from forwarded ports.
CMD ["moon", "--bind", "0.0.0.0", "--protected-mode", "no"]
