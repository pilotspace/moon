#!/usr/bin/env bash
# Moon vs Redis vs Qdrant — Server-Mode Vector Benchmark
#
# Runs all three systems as actual servers with identical workloads.
# Generates BENCHMARK-REPORT.md with QPS, latency, memory, recall tables.
#
# Usage:
#   ./scripts/bench-server-mode.sh                 # Full: 100K vectors, 768d
#   ./scripts/bench-server-mode.sh 10000 128       # Quick: 10K vectors, 128d
#   ./scripts/bench-server-mode.sh 100000 768 50   # Custom: 100K, 768d, 50 queries
#
# Prerequisites:
#   - Redis 8.x installed (redis-server, redis-cli)
#   - Docker (for Qdrant)
#   - Python3 with: numpy, redis-py, requests
#   - Rust toolchain with target-cpu=native support

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
N_VECTORS="${1:-100000}"
DIM="${2:-768}"
N_QUERIES="${3:-200}"
K=10
EF=128

MOON_PORT=16379
REDIS_PORT=16400
QDRANT_PORT=16333

RESULTS_DIR="target/bench-results"
DATA_DIR="target/bench-data"
REPORT_PATH=".planning/BENCHMARK-REPORT.md"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

mkdir -p "$RESULTS_DIR" "$DATA_DIR"

# ── Cleanup Trap ─────────────────────────────────────────────────────────
MOON_PID=""
REDIS_PID=""
cleanup() {
    echo ""
    echo ">>> Cleaning up..."
    [ -n "$MOON_PID" ] && kill "$MOON_PID" 2>/dev/null && wait "$MOON_PID" 2>/dev/null || true
    [ -n "$REDIS_PID" ] && kill "$REDIS_PID" 2>/dev/null && wait "$REDIS_PID" 2>/dev/null || true
    docker rm -f qdrant-bench 2>/dev/null || true
    echo ">>> Cleanup complete."
}
trap cleanup EXIT

# ── System Info ──────────────────────────────────────────────────────────
echo "================================================================="
echo " Moon vs Redis vs Qdrant — Server-Mode Benchmark"
echo "================================================================="
echo " Vectors: $N_VECTORS | Dimensions: $DIM | K: $K | ef: $EF"
echo " Queries: $N_QUERIES (sequential, single-threaded)"

if [[ "$(uname)" == "Darwin" ]]; then
    HW_CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
    HW_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "?")
    HW_MEM=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))
else
    HW_CPU=$(lscpu 2>/dev/null | grep "Model name" | cut -d: -f2 | xargs || echo "unknown")
    HW_CORES=$(nproc 2>/dev/null || echo "?")
    HW_MEM=$(( $(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0) / 1024 / 1024 ))
fi

echo " CPU: $HW_CPU"
echo " Cores: $HW_CORES | RAM: ${HW_MEM}GB"
echo " OS: $(uname -s) $(uname -r) $(uname -m)"
echo " Date: $(date -u +"%Y-%m-%d %H:%M UTC")"
echo "================================================================="

# ── Step 1: Build Moon Release ───────────────────────────────────────────
echo ""
echo ">>> Building Moon (release, target-cpu=native)..."
RUSTFLAGS="-C target-cpu=native" cargo build --release \
    --no-default-features --features runtime-tokio,jemalloc 2>&1 | tail -3

MOON_VERSION=$(git rev-parse --short HEAD)
echo "  Moon version: $MOON_VERSION"

# ── Step 2: Generate Test Data ───────────────────────────────────────────
echo ""
echo ">>> Generating test data: ${N_VECTORS} vectors, ${DIM}d..."
python3 "$SCRIPT_DIR/bench-vs-competitors.py" \
    --generate-only \
    --vectors "$N_VECTORS" --dim "$DIM" --queries "$N_QUERIES" \
    --output "$DATA_DIR"

echo "  Data files in $DATA_DIR/"

# ── Step 3: Moon Benchmark (Server Mode) ─────────────────────────────────
echo ""
echo "================================================================="
echo " MOON (Server Mode, port $MOON_PORT)"
echo "================================================================="

# Kill any existing process on our benchmark port
EXISTING_PID=$(lsof -ti :"$MOON_PORT" 2>/dev/null || true)
[ -n "$EXISTING_PID" ] && kill "$EXISTING_PID" 2>/dev/null && sleep 1 || true

# Use --shards 1 for correct FT.SEARCH results (multi-shard merge has known issues).
# Single-shard gives best per-key throughput for non-pipelined workloads anyway.
./target/release/moon --port "$MOON_PORT" --shards 1 &
MOON_PID=$!
echo "  Started Moon server (PID=$MOON_PID)"

# Wait for startup
for i in $(seq 1 10); do
    if redis-cli -p "$MOON_PORT" PING 2>/dev/null | grep -q PONG; then
        echo "  Moon ready (attempt $i)"
        break
    fi
    sleep 1
done

python3 "$SCRIPT_DIR/bench-vs-competitors.py" \
    --bench-moon --port "$MOON_PORT" \
    --dim "$DIM" --k "$K" --ef "$EF" \
    --input "$DATA_DIR" --output "$RESULTS_DIR/moon.json"

# Capture memory
MOON_RSS=$(ps -o rss= -p "$MOON_PID" 2>/dev/null | tr -d ' ' || echo "0")
echo "  Moon RSS after benchmark: $((MOON_RSS / 1024)) MB"

kill "$MOON_PID" 2>/dev/null && wait "$MOON_PID" 2>/dev/null || true
MOON_PID=""
echo "  Moon server stopped."

# ── Step 4: Redis Benchmark ──────────────────────────────────────────────
echo ""
echo "================================================================="
echo " REDIS 8.x (port $REDIS_PORT)"
echo "================================================================="

REDIS_VERSION=$(redis-server --version 2>/dev/null | head -1 || echo "not installed")
echo "  Version: $REDIS_VERSION"

if command -v redis-server &>/dev/null; then
    python3 "$SCRIPT_DIR/bench-vs-competitors.py" \
        --bench-redis --port "$REDIS_PORT" \
        --dim "$DIM" --k "$K" --ef "$EF" \
        --input "$DATA_DIR" --output "$RESULTS_DIR/redis.json"
else
    echo "  SKIPPED: redis-server not found"
    echo '{"skipped": true, "reason": "redis-server not installed"}' > "$RESULTS_DIR/redis.json"
fi

# ── Step 5: Qdrant Benchmark ────────────────────────────────────────────
echo ""
echo "================================================================="
echo " QDRANT (Docker, port $QDRANT_PORT)"
echo "================================================================="

if command -v docker &>/dev/null; then
    python3 "$SCRIPT_DIR/bench-vs-competitors.py" \
        --bench-qdrant \
        --qdrant-port "$QDRANT_PORT" \
        --dim "$DIM" --k "$K" --ef "$EF" \
        --input "$DATA_DIR" --output "$RESULTS_DIR/qdrant.json"
else
    echo "  SKIPPED: docker not found"
    echo '{"skipped": true, "reason": "docker not installed"}' > "$RESULTS_DIR/qdrant.json"
fi

# ── Step 6: Generate Report ──────────────────────────────────────────────
echo ""
echo "================================================================="
echo " GENERATING REPORT"
echo "================================================================="

python3 "$SCRIPT_DIR/bench-vs-competitors.py" \
    --report \
    --results-dir "$RESULTS_DIR" \
    --output "$REPORT_PATH" \
    --vectors "$N_VECTORS" --dim "$DIM" --k "$K" --ef "$EF" \
    --queries "$N_QUERIES" \
    --hw-cpu "$HW_CPU" --hw-cores "$HW_CORES" --hw-mem "${HW_MEM}GB" \
    --hw-os "$(uname -s) $(uname -r) $(uname -m)" \
    --moon-version "$MOON_VERSION" \
    --redis-version "$REDIS_VERSION"

echo ""
echo ">>> Report written to: $REPORT_PATH"
echo ">>> Raw results in: $RESULTS_DIR/"
echo ">>> Done."
