# Stage 1: Build
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/

# Build release binary
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r rustredis && useradd -r -g rustredis -d /data -s /sbin/nologin rustredis
RUN mkdir -p /data && chown rustredis:rustredis /data

# Copy binary from builder
COPY --from=builder /app/target/release/rust-redis /usr/local/bin/rust-redis

USER rustredis
WORKDIR /data

EXPOSE 6379

CMD ["rust-redis", "--bind", "0.0.0.0"]
