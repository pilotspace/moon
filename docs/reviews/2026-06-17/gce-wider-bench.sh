#!/usr/bin/env bash
# GCloud WIDER benchmark: shard scaling (1/4/12) × all-commands/pipeline/datasize (bench-compare.sh)
# + production scenarios (bench-production.sh). Moon vs Redis, per-arch. Reuses the repo-canonical scripts.
#
# FAIRNESS PATCH: the canonical scripts start Redis with `--appendonly no` but Moon with persistence
# defaults (AOF on) and no log redirect — under SET load Moon's AOF channel saturates, floods
# `AOF append dropped … channel full` WARNs (998K lines), pins port 6400, and handicaps Moon vs a
# no-AOF Redis. We patch the cloned scripts to start Moon with `--appendonly no --disk-offload disable
# --dir <fresh>` + RUST_LOG=error + stdout/stderr → /dev/null, so the comparison is AOF-off on both sides.
set -uo pipefail
ARCH="$(uname -m)"; BR="${MOON_BRANCH:-main}"
REQ="${REQ:-200000}"; PROD_DUR="${PROD_DUR:-12}"
ulimit -n 65536 2>/dev/null || true
echo "================ WIDER BENCH START · arch=$ARCH · $(date -u) ================"

# ---------- deps ----------
command -v cargo >/dev/null 2>&1 || { curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1 >/dev/null 2>&1; }
source "$HOME/.cargo/env"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq >/dev/null 2>&1
sudo apt-get install -y -qq build-essential pkg-config libssl-dev git redis-server redis-tools >/dev/null 2>&1
sudo systemctl stop redis-server >/dev/null 2>&1 || true
sudo systemctl disable redis-server >/dev/null 2>&1 || true

# ---------- build Moon from main ----------
rm -rf "$HOME/moon"; git clone --depth 1 --branch "$BR" https://github.com/pilotspace/moon.git "$HOME/moon" >/dev/null 2>&1
cd "$HOME/moon" || { echo "CLONE FAILED"; echo "WIDER BENCH DONE · $ARCH"; exit 1; }
echo "HEAD: $(git log --oneline -1)"
RUSTFLAGS="-C target-cpu=native" cargo build --release 2>&1 | tail -1
[ -x ./target/release/moon ] || { echo "BUILD FAILED"; echo "WIDER BENCH DONE · $ARCH"; exit 1; }
echo "CPU: $(lscpu | awk -F: '/Model name/{print $2}' | xargs) · cores=$(nproc)"
redis-server --version | head -1

# ---------- fairness patch (AOF off on Moon, no flood) ----------
sed -i 's#RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no &#RUST_LOG=error "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no --appendonly no --disk-offload disable --dir /tmp/moon-cmp >/dev/null 2>\&1 \&#' scripts/bench-compare.sh
sed -i 's#$RUST_BINARY --port "$PORT_RUST" --shards "$SHARDS" &#RUST_LOG=error $RUST_BINARY --port "$PORT_RUST" --shards "$SHARDS" --appendonly no --disk-offload disable --dir /tmp/moon-prod >/dev/null 2>\&1 \&#' scripts/bench-production.sh
echo "PATCH check (compare): $(grep -c 'disk-offload disable --dir /tmp/moon-cmp' scripts/bench-compare.sh) line(s)"
echo "PATCH check (production): $(grep -c 'disk-offload disable --dir /tmp/moon-prod' scripts/bench-production.sh) line(s)"

cleanup_ports(){ pkill -9 -x moon 2>/dev/null || true; pkill -9 -f 'redis-server --port 6399' 2>/dev/null || true; sleep 2; rm -rf /tmp/moon-cmp /tmp/moon-prod; }

# ---------- 1) shard scaling × all-commands/pipeline/datasize/connections ----------
for S in 1 4 12; do
  cleanup_ports
  echo ""
  echo "########## COMPARE shards=$S START (req=$REQ, AOF off) ##########"
  bash scripts/bench-compare.sh --requests "$REQ" --shards "$S" 2>/dev/null \
    || echo "(bench-compare shards=$S FAILED)"
  echo "########## COMPARE shards=$S END ##########"
done

# ---------- 2) production scenarios ----------
for S in 1 12; do
  cleanup_ports
  echo ""
  echo "########## PRODUCTION shards=$S START (dur=${PROD_DUR}s, AOF off) ##########"
  OUT="/tmp/prod-s${S}.md"
  bash scripts/bench-production.sh --shards "$S" --duration "$PROD_DUR" --output "$OUT" >/dev/null 2>&1 || true
  if [ -s "$OUT" ]; then cat "$OUT"; else echo "(bench-production shards=$S produced no output)"; fi
  echo "########## PRODUCTION shards=$S END ##########"
done

cleanup_ports
echo "================ WIDER BENCH DONE · arch=$ARCH · $(date -u) ================"
