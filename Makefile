# Moon — development commands (all run inside OrbStack moon-dev VM)
ORB = orb run -m moon-dev bash -c
CARGO_ENV = source $$HOME/.cargo/env
# The LIVE checkout. /Users/tindang/workspaces/tind-repo/moon is a STALE second
# checkout stuck in the hash-ttl era (#126) — pointing REPO there makes every
# target below silently build, test and run ~350 PRs of the wrong code, and
# `make ci` reports green on code nobody is shipping. See CLAUDE.md.
REPO = /Volumes/Games/tindang-repo/moon
# Linux ELF artifacts must not clobber the macOS Mach-O ones in the shared
# checkout — the VM and the host compile the same source tree. (CLAUDE.md)
# `export` (not a bare VAR=x prefix) so it survives every `&&` in a chain.
TARGET_DIR = export CARGO_TARGET_DIR=target-linux &&
RUSTFLAGS_NATIVE = RUSTFLAGS="-C target-cpu=native"

PORT ?= 6399
SHARDS ?= 4
ADMIN_PORT ?= 9100
EXTRA_ARGS ?=

.PHONY: build build-tokio check check-tokio clippy fmt test test-tokio start stop restart

# --- Build ---

build:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) $(RUSTFLAGS_NATIVE) cargo build --release --features console'

build-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) $(RUSTFLAGS_NATIVE) cargo build --release --no-default-features --features runtime-tokio,jemalloc,console'

# --- Check / Lint ---

check:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo check --features console'

check-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo check --no-default-features --features runtime-tokio,jemalloc,console'

clippy:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings'

fmt:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo fmt --check'

# --- Test ---

test:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo test --release'

test-tokio:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo test --no-default-features --features runtime-tokio,jemalloc'

# --- Server ---

DATA_DIR ?= /tmp/moon-data

PERSIST_ARGS = --appendonly yes --dir $(DATA_DIR) --disk-offload enable --disk-offload-dir $(DATA_DIR)/offload --save "3600 1 300 100"

start: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -f "$(REPO)/target-linux/release/moon" 2>/dev/null; sleep 0.5; mkdir -p $(DATA_DIR) && ./target-linux/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(PERSIST_ARGS) $(EXTRA_ARGS)'

start-bg: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -f "$(REPO)/target-linux/release/moon" 2>/dev/null; sleep 0.5; mkdir -p $(DATA_DIR) && nohup ./target-linux/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(PERSIST_ARGS) $(EXTRA_ARGS) &>/tmp/moon.log & echo "Moon started (PID $$!), log: /tmp/moon.log"'

start-ephemeral: build
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && pkill -f "$(REPO)/target-linux/release/moon" 2>/dev/null; sleep 0.5; ./target-linux/release/moon --port $(PORT) --shards $(SHARDS) --admin-port $(ADMIN_PORT) $(EXTRA_ARGS)'

stop:
	$(ORB) 'pkill -f "$(REPO)/target-linux/release/moon" 2>/dev/null && echo "Moon stopped" || echo "Moon not running"'

restart: stop build start

# --- CI parity ---

ci:
	$(ORB) '$(CARGO_ENV) && cd $(REPO) && $(TARGET_DIR) cargo fmt --check && cargo clippy -- -D warnings && cargo clippy --no-default-features --features runtime-tokio,jemalloc -- -D warnings && cargo test --release && cargo test --no-default-features --features runtime-tokio,jemalloc'
