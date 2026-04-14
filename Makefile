# Moon — development commands (all run inside OrbStack moon-dev VM)
ORB = orb run -m moon-dev bash -c
CARGO_ENV = source $$HOME/.cargo/env
REPO = /Users/tindang/workspaces/tind-repo/moon
RUSTFLAGS_NATIVE = RUSTFLAGS="-C target-cpu=native"

PORT ?= 6399
SHARDS ?= 4
ADMIN_PORT ?= 9100
EXTRA_ARGS ?=

.PHONY: build build-tokio check check-tokio clippy fmt test test-tokio start stop restart

# --- Build ---

build:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(RUSTFLAGS_NATIVE) cargo build --release --features console'

build-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(RUSTFLAGS_NATIVE) cargo build --release --no-default-features --features runtime-tokio,jemalloc,console'

# --- Check / Lint ---

check:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo check --features console'

check-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo check --no-default-features --features runtime-tokio,jemalloc,console'

clippy:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings'

fmt:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo fmt --check'

# --- Test ---

test:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo test --release'

test-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo test --no-default-features --features runtime-tokio,jemalloc'

# --- Server ---

DATA_DIR ?= /tmp/moon-data

PERSIST_ARGS = --appendonly yes --dir $(DATA_DIR) --disk-offload enable --disk-offload-dir $(DATA_DIR)/offload --save "3600 1 300 100"

start: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -x moon 2>/dev/null; sleep 0.5; mkdir -p $(DATA_DIR) && ./target/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(PERSIST_ARGS) $(EXTRA_ARGS)'

start-bg: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -x moon 2>/dev/null; sleep 0.5; mkdir -p $(DATA_DIR) && nohup ./target/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(PERSIST_ARGS) $(EXTRA_ARGS) &>/tmp/moon.log & echo "Moon started (PID $$!), log: /tmp/moon.log"'

start-ephemeral: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -x moon 2>/dev/null; sleep 0.5; ./target/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(EXTRA_ARGS)'

stop:
	$(ORB) 'pkill -x moon 2>/dev/null && echo "Moon stopped" || echo "Moon not running"'

restart: stop build start

# --- CI parity ---

ci:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && cargo fmt --check && cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings && cargo test --release && cargo test --no-default-features --features runtime-tokio,jemalloc'
