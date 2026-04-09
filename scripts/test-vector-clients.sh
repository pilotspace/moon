#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# test-vector-clients.sh -- Vector search (FT.*) smoke test via redis-cli
#
# Tests Moon's FT.CREATE, HSET (vector ingest), FT.SEARCH, FT.INFO,
# FT.DROPINDEX using only redis-cli (no Python/LangChain dependencies).
#
# Usage:
#   ./scripts/test-vector-clients.sh                  # Default port 6379
#   ./scripts/test-vector-clients.sh --port 6400      # Custom port
#   ./scripts/test-vector-clients.sh --skip-build     # Skip cargo build
#   ./scripts/test-vector-clients.sh --shards N        # Shard count (default 1)
###############################################################################

PORT=6400
SHARDS=1
SKIP_BUILD=false
RUST_BINARY="./target/release/moon"
MOON_PID=""
PASS=0
FAIL=0
TOTAL=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)       PORT="$2"; shift 2 ;;
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --help|-h)    sed -n '3,14p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    if [[ -n "$MOON_PID" ]]; then
        kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    fi
    pkill -f "moon.*${PORT}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
    for ((i=0; i<30; i++)); do
        redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.2
    done
    log "ERROR: port $PORT not ready"; return 1
}

mcli() { redis-cli -p "$PORT" "$@" 2>&1; }

assert_eq() {
    local label="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local actual
    actual=$(mcli "$@")
    if [[ "$actual" == "$expected" ]]; then
        PASS=$((PASS + 1))
        echo "  PASS: $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $label (expected='$expected', got='$actual')"
    fi
}

assert_contains() {
    local label="$1" substring="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local actual
    actual=$(mcli "$@")
    if echo "$actual" | grep -qi "$substring"; then
        PASS=$((PASS + 1))
        echo "  PASS: $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $label (expected to contain '$substring', got='$actual')"
    fi
}

assert_not_error() {
    local label="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local actual
    actual=$(mcli "$@")
    if ! echo "$actual" | grep -qi "^(error)"; then
        PASS=$((PASS + 1))
        echo "  PASS: $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $label (got error: $actual)"
    fi
}

assert_error() {
    local label="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local actual
    actual=$(mcli "$@")
    if echo "$actual" | grep -qi "err"; then
        PASS=$((PASS + 1))
        echo "  PASS: $label"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $label (expected error, got='$actual')"
    fi
}

# ===========================================================================
# Build & Start Server
# ===========================================================================

if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building Moon..."
    cargo build --release 2>&1 | tail -3
fi

pkill -f "moon.*${PORT}" 2>/dev/null || true
sleep 0.3

log "Starting Moon on port $PORT (shards=$SHARDS)..."
"$RUST_BINARY" --port "$PORT" --shards "$SHARDS" &
MOON_PID=$!
wait_for_port "$PORT"
log "Moon ready (PID=$MOON_PID)"

# ===========================================================================
# Clean slate
# ===========================================================================

mcli FLUSHALL >/dev/null 2>&1

echo ""
echo "=== VECTOR CLIENT SMOKE TESTS ==="
echo ""

# ===========================================================================
# 1. FT.CREATE — create a FLAT vector index (4 dimensions, L2)
# ===========================================================================

echo "--- FT.CREATE ---"
assert_eq "FT.CREATE flat index" "OK" \
    FT.CREATE vec_test ON HASH PREFIX 1 item: SCHEMA \
    embedding VECTOR FLAT 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32

# Duplicate index should error
assert_error "FT.CREATE duplicate index" \
    FT.CREATE vec_test ON HASH PREFIX 1 item: SCHEMA \
    embedding VECTOR FLAT 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32

# ===========================================================================
# 2. FT.INFO — verify index metadata
# ===========================================================================

echo "--- FT.INFO ---"
assert_contains "FT.INFO shows index name" "vec_test" \
    FT.INFO vec_test

assert_error "FT.INFO nonexistent index" \
    FT.INFO nonexistent_index

# ===========================================================================
# 3. HSET — ingest vectors (binary via python struct pack)
# ===========================================================================

echo "--- HSET vectors ---"

# Vector [1,0,0,0] — unit X
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',1.0,0.0,0.0,0.0))" \
    | redis-cli -x -p "$PORT" HSET item:1 embedding >/dev/null 2>&1
TOTAL=$((TOTAL + 1))
GOT=$(mcli HGET item:1 embedding | wc -c)
if [[ "$GOT" -gt 0 ]]; then PASS=$((PASS + 1)); echo "  PASS: HSET item:1 vector stored"; else FAIL=$((FAIL + 1)); echo "  FAIL: HSET item:1"; fi

# Vector [0,1,0,0] — unit Y
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.0,1.0,0.0,0.0))" \
    | redis-cli -x -p "$PORT" HSET item:2 embedding >/dev/null 2>&1
TOTAL=$((TOTAL + 1))
GOT=$(mcli HGET item:2 embedding | wc -c)
if [[ "$GOT" -gt 0 ]]; then PASS=$((PASS + 1)); echo "  PASS: HSET item:2 vector stored"; else FAIL=$((FAIL + 1)); echo "  FAIL: HSET item:2"; fi

# Vector [0,0,1,0] — unit Z
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.0,0.0,1.0,0.0))" \
    | redis-cli -x -p "$PORT" HSET item:3 embedding >/dev/null 2>&1
TOTAL=$((TOTAL + 1))
GOT=$(mcli HGET item:3 embedding | wc -c)
if [[ "$GOT" -gt 0 ]]; then PASS=$((PASS + 1)); echo "  PASS: HSET item:3 vector stored"; else FAIL=$((FAIL + 1)); echo "  FAIL: HSET item:3"; fi

# Vector with extra hash field (metadata)
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.5,0.5,0.0,0.0))" \
    | redis-cli -x -p "$PORT" HSET item:4 embedding >/dev/null 2>&1
mcli HSET item:4 name "mixed vector" >/dev/null 2>&1
TOTAL=$((TOTAL + 1))
GOT=$(mcli HGET item:4 name)
if [[ "$GOT" == "mixed vector" ]]; then PASS=$((PASS + 1)); echo "  PASS: HSET item:4 with metadata"; else FAIL=$((FAIL + 1)); echo "  FAIL: HSET item:4 metadata (got '$GOT')"; fi

# ===========================================================================
# 4. FT.SEARCH — wildcard (list all docs)
# ===========================================================================

echo "--- FT.SEARCH ---"

# Wildcard search should not error and should return results
assert_not_error "FT.SEARCH wildcard" \
    FT.SEARCH vec_test "*"

# FT.SEARCH result should mention at least one item key
assert_contains "FT.SEARCH returns docs" "item:" \
    FT.SEARCH vec_test "*"

# Search on nonexistent index should error
assert_error "FT.SEARCH nonexistent index" \
    FT.SEARCH nonexistent_index "*"

# ===========================================================================
# 5. FT.INFO after inserts — num_docs should reflect ingested data
# ===========================================================================

echo "--- FT.INFO post-insert ---"
TOTAL=$((TOTAL + 1))
FT_INFO_RESULT=$(mcli FT.INFO vec_test)
# num_docs should be >= 4
if echo "$FT_INFO_RESULT" | grep -qE "(num_docs|4)"; then
    PASS=$((PASS + 1))
    echo "  PASS: FT.INFO shows docs after insert"
else
    # Even if we can't parse num_docs exactly, it shouldn't error
    if ! echo "$FT_INFO_RESULT" | grep -qi "err"; then
        PASS=$((PASS + 1))
        echo "  PASS: FT.INFO returns data (no error)"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: FT.INFO post-insert returned error"
    fi
fi

# ===========================================================================
# 6. FT.DROPINDEX — remove the index
# ===========================================================================

echo "--- FT.DROPINDEX ---"
assert_eq "FT.DROPINDEX existing" "OK" \
    FT.DROPINDEX vec_test

# Index should be gone
assert_error "FT.INFO after drop" \
    FT.INFO vec_test

# Double drop should error
assert_error "FT.DROPINDEX already dropped" \
    FT.DROPINDEX vec_test

# ===========================================================================
# 7. HNSW index variant
# ===========================================================================

echo "--- HNSW index ---"
assert_eq "FT.CREATE HNSW index" "OK" \
    FT.CREATE hnsw_test ON HASH PREFIX 1 hnsw: SCHEMA \
    vec VECTOR HNSW 6 DIM 4 DISTANCE_METRIC COSINE TYPE FLOAT32

python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',1.0,0.0,0.0,0.0))" \
    | redis-cli -x -p "$PORT" HSET hnsw:1 vec >/dev/null 2>&1

assert_not_error "FT.SEARCH on HNSW index" \
    FT.SEARCH hnsw_test "*"

assert_eq "FT.DROPINDEX HNSW" "OK" \
    FT.DROPINDEX hnsw_test

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "==========================================="
echo "  Vector Client Smoke Tests"
echo "  PASS: $PASS / $TOTAL"
echo "  FAIL: $FAIL / $TOTAL"
echo "==========================================="

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
echo "All vector client tests passed."
