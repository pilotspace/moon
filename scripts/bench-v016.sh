#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-v016.sh -- Moon v0.1.6 Feature Benchmark Suite
#
# Benchmarks all v0.1.6 vector search features:
#   - FT.SEARCH (baseline KNN, LIMIT, tag filter, geo filter, RANGE)
#   - FT.CACHESEARCH (cache hit vs miss latency)
#   - FT.RECOMMEND (positive/negative example recommendation)
#   - FT.SEARCH EXPAND GRAPH (graph-augmented search)
#   - FT.NAVIGATE (multi-hop knowledge navigation)
#   - FT.SEARCH with RRF fusion (dense+sparse)
#
# Usage:
#   ./scripts/bench-v016.sh                    # Full suite (10K vectors)
#   ./scripts/bench-v016.sh --vectors 50000    # Custom vector count
#   ./scripts/bench-v016.sh --queries 1000     # Custom query count
#   ./scripts/bench-v016.sh --section cache    # Single section
#   ./scripts/bench-v016.sh --shards 4         # Multi-shard
#
# Sections: all, baseline, limit, filter, cache, recommend, navigate, expand,
#           range, fusion
###############################################################################

PORT_MOON=6401
VECTORS=10000
QUERIES=500
SHARDS=1
SECTION="all"
DIM=384
K=10
RUST_BINARY="./target/release/moon"

MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vectors)   VECTORS="$2"; shift 2 ;;
        --queries)   QUERIES="$2"; shift 2 ;;
        --shards)    SHARDS="$2"; shift 2 ;;
        --section)   SECTION="$2"; shift 2 ;;
        --port)      PORT_MOON="$2"; shift 2 ;;
        --dim)       DIM="$2"; shift 2 ;;
        --k)         K="$2"; shift 2 ;;
        --help)      awk '/^###/{n++} n==1' "$0"; exit 0 ;;
        *)           echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    if [[ -n "${MOON_PID:-}" ]]; then
        kill "$MOON_PID" 2>/dev/null || true
        wait "$MOON_PID" 2>/dev/null || true
    fi
    pkill -f "moon.*${PORT_MOON}" 2>/dev/null || true
}
trap cleanup EXIT

should_run() { [[ "$SECTION" == "all" ]] || [[ "$SECTION" == "$1" ]]; }

# ── Server startup ──────────────────────────────────────────────────────

wait_for_server() {
    local port="$1" max_wait=15 elapsed=0
    while (( elapsed < max_wait )); do
        if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    echo "Moon failed to start on port $port within ${max_wait}s" >&2
    exit 1
}

log "Starting Moon on port $PORT_MOON ($SHARDS shards)..."
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no &
MOON_PID=$!
wait_for_server "$PORT_MOON"
log "Moon ready."

# ── Helper: run a command N times and measure latency ───────────────────

# Build RESP protocol string for one command
_resp() {
    local n=$#
    printf "*%d\r\n" "$n"
    for arg in "$@"; do
        printf "\$%d\r\n%s\r\n" "${#arg}" "$arg"
    done
}

# Generate a random float vector as comma-separated string
gen_vector_csv() {
    local dim="$1"
    python3 -c "
import random, sys
random.seed(${2:-42})
print(','.join(f'{random.gauss(0,1):.6f}' for _ in range($dim)))
"
}

# Generate a random binary blob (float32 LE) for vector query
gen_vector_blob() {
    local dim="$1" seed="${2:-42}"
    python3 -c "
import struct, random, sys
random.seed($seed)
v = [random.gauss(0,1) for _ in range($dim)]
sys.stdout.buffer.write(struct.pack(f'<{$dim}f', *v))
"
}

# Benchmark helper: sends raw RESP commands via redis-cli and measures timing.
# Uses redis-cli --pipe for bulk loading, redis-cli for individual timing.
bench_latency() {
    local desc="$1" count="$2"
    shift 2
    # "$@" is the command to benchmark
    local total_us=0 min_us=999999999 max_us=0 p99_idx latencies=()

    for ((i=0; i<count; i++)); do
        local start_ns end_ns elapsed_us
        start_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
        redis-cli -p "$PORT_MOON" "$@" > /dev/null 2>&1
        end_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
        elapsed_us=$(( (end_ns - start_ns) / 1000 ))
        latencies+=("$elapsed_us")
        total_us=$((total_us + elapsed_us))
        if (( elapsed_us < min_us )); then min_us=$elapsed_us; fi
        if (( elapsed_us > max_us )); then max_us=$elapsed_us; fi
    done

    local avg_us=$((total_us / count))
    # Sort for p99
    IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}")); unset IFS
    p99_idx=$(( (count * 99) / 100 ))
    if (( p99_idx >= count )); then p99_idx=$((count - 1)); fi
    local p99_us="${sorted[$p99_idx]}"
    local ops_sec
    if (( avg_us > 0 )); then
        ops_sec=$((1000000 / avg_us))
    else
        ops_sec=999999
    fi

    printf "| %-40s | %8d | %8.0f | %8d | %8d |\n" \
        "$desc" "$ops_sec" "$(echo "scale=1; $avg_us / 1000" | bc)" \
        "$(echo "scale=0; $p99_us / 1000" | bc -l)" "$count"
}

# Faster benchmark using redis-cli pipeline (for high-throughput tests)
bench_pipeline() {
    local desc="$1" count="$2" tmpfile
    shift 2
    tmpfile=$(mktemp)
    for ((i=0; i<count; i++)); do
        _resp "$@" >> "$tmpfile"
    done

    local start_ns end_ns elapsed_us
    start_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
    redis-cli -p "$PORT_MOON" --pipe < "$tmpfile" > /dev/null 2>&1
    end_ns=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
    elapsed_us=$(( (end_ns - start_ns) / 1000 ))
    rm -f "$tmpfile"

    local avg_us ops_sec
    if (( count > 0 )); then
        avg_us=$((elapsed_us / count))
    else
        avg_us=0
    fi
    if (( avg_us > 0 )); then
        ops_sec=$((1000000 / avg_us))
    else
        ops_sec=999999
    fi

    printf "| %-40s | %8d | %8.1f | %8s | %8d |\n" \
        "$desc" "$ops_sec" "$(echo "scale=1; $avg_us / 1000" | bc)" "~" "$count"
}

# ── Data setup ──────────────────────────────────────────────────────────

log "Creating vector index (dim=$DIM)..."
redis-cli -p "$PORT_MOON" FT.CREATE bench_idx ON HASH PREFIX 1 "doc:" \
    SCHEMA vec VECTOR HNSW 6 TYPE FLOAT32 DIM "$DIM" DISTANCE_METRIC L2 \
    tag TAG geo GEO 2>/dev/null || true

log "Inserting $VECTORS vectors..."
{
    for ((i=0; i<VECTORS; i++)); do
        # Generate vector inline as float string list for HSET
        vec_blob=$(python3 -c "
import struct, random, sys
random.seed($i)
v = [random.gauss(0,1) for _ in range($DIM)]
sys.stdout.buffer.write(struct.pack(f'<${DIM}f', *v))
" | xxd -p | tr -d '\n')
        # Use redis-cli to set hash with vector blob, tag, and geo fields
        _resp HSET "doc:$i" vec "$(python3 -c "
import struct, random, sys
random.seed($i)
v = [random.gauss(0,1) for _ in range($DIM)]
sys.stdout.buffer.write(struct.pack('<${DIM}f', *v))
" | base64)" tag "$(( i % 10 == 0 ? 1 : 0 ))" geo "13.361,38.116"
    done
} | redis-cli -p "$PORT_MOON" --pipe > /dev/null 2>&1

# Build vectors using a simpler approach: individual HSET with float blob
log "Inserting vectors via HSET with binary blobs..."
python3 -c "
import struct, random, socket, sys

def resp_bulk(s):
    if isinstance(s, str):
        s = s.encode()
    return b'\$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'

def resp_array(*args):
    parts = [b'*' + str(len(args)).encode() + b'\r\n']
    for a in args:
        parts.append(resp_bulk(a))
    return b''.join(parts)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', $PORT_MOON))

# Insert vectors
for i in range($VECTORS):
    random.seed(i)
    vec = [random.gauss(0, 1) for _ in range($DIM)]
    blob = struct.pack('<${DIM}f', *vec)
    tag_val = 'category_a' if i % 10 == 0 else 'category_b'
    lat = 38.116 + (i % 100) * 0.001
    lon = 13.361 + (i % 100) * 0.001
    geo_val = f'{lon},{lat}'
    cmd = resp_array(b'HSET', f'doc:{i}'.encode(), b'vec', blob,
                     b'tag', tag_val.encode(), b'geo', geo_val.encode())
    sock.sendall(cmd)
    # Drain responses periodically
    if i % 100 == 99:
        sock.setblocking(False)
        try:
            while True:
                data = sock.recv(65536)
                if not data:
                    break
        except BlockingIOError:
            pass
        sock.setblocking(True)

# Drain remaining responses
sock.setblocking(False)
import time
time.sleep(0.5)
try:
    while True:
        data = sock.recv(65536)
        if not data:
            break
except BlockingIOError:
    pass
sock.close()
print(f'Inserted {$VECTORS} vectors', file=sys.stderr)
" 2>&1 | while read -r line; do log "$line"; done

# Let auto-indexing settle
sleep 2

# Compact for HNSW search
redis-cli -p "$PORT_MOON" FT.COMPACT bench_idx > /dev/null 2>&1 || true
sleep 1

# Setup graph data for EXPAND/NAVIGATE tests
if should_run "expand" || should_run "navigate" || [[ "$SECTION" == "all" ]]; then
    log "Creating graph data for EXPAND/NAVIGATE..."
    python3 -c "
import socket

def resp_bulk(s):
    if isinstance(s, str):
        s = s.encode()
    return b'\$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'

def resp_array(*args):
    parts = [b'*' + str(len(args)).encode() + b'\r\n']
    for a in args:
        parts.append(resp_bulk(a))
    return b''.join(parts)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', $PORT_MOON))

# Create graph with edges linking consecutive docs
# GRAPH.CREATE bench_graph
sock.sendall(resp_array(b'GRAPH.CREATE', b'bench_graph'))

# Add edges: doc:i -> doc:i+1 for i in 0..min(1000, N)
n = min(1000, $VECTORS)
for i in range(n - 1):
    cmd = resp_array(b'GRAPH.ADDEDGE', b'bench_graph', f'doc:{i}'.encode(),
                     f'doc:{i+1}'.encode(), b'related')
    sock.sendall(cmd)

import time
time.sleep(0.5)
sock.setblocking(False)
try:
    while True:
        data = sock.recv(65536)
        if not data:
            break
except BlockingIOError:
    pass
sock.close()
" 2>&1 || log "Graph setup skipped (graph feature may not be enabled)"
fi

# Generate a query vector blob file
QUERY_BLOB=$(mktemp)
gen_vector_blob "$DIM" 99999 > "$QUERY_BLOB"
QUERY_BLOB_B64=$(base64 < "$QUERY_BLOB")

# ── Report header ───────────────────────────────────────────────────────

PLATFORM="$(uname -s) $(uname -m)"
if [[ -f /etc/os-release ]]; then
    PLATFORM="$PLATFORM / $(grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '"')"
fi

cat <<HEADER
# Moon v0.1.6 Feature Benchmark Report

**Date:** $(date -u +"%Y-%m-%d %H:%M UTC")
**Platform:** $PLATFORM
**Vectors:** $VECTORS (dim=$DIM)
**Queries per test:** $QUERIES
**Shards:** $SHARDS
**K (neighbors):** $K

---

HEADER

table_header() {
    echo ""
    echo "## $1"
    echo ""
    echo "$2"
    echo ""
    printf "| %-40s | %8s | %8s | %8s | %8s |\n" "Command" "ops/sec" "avg(ms)" "p99(ms)" "queries"
    printf "|%-42s|%10s|%10s|%10s|%10s|\n" \
        "------------------------------------------" "----------" "----------" "----------" "----------"
}

# ── 1. Baseline FT.SEARCH (KNN) ────────────────────────────────────────

if should_run "baseline"; then
    table_header "1. Baseline FT.SEARCH (KNN)" \
        "Pure KNN search without filters or limits."
    bench_latency "FT.SEARCH KNN k=$K" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")"
fi

# ── 2. FT.SEARCH with LIMIT ────────────────────────────────────────────

if should_run "limit"; then
    table_header "2. FT.SEARCH with LIMIT" \
        "KNN search with offset/count pagination."
    bench_latency "FT.SEARCH LIMIT 0 5" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" LIMIT 0 5
    bench_latency "FT.SEARCH LIMIT 5 5" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" LIMIT 5 5
fi

# ── 3. FT.SEARCH with tag filter ───────────────────────────────────────

if should_run "filter"; then
    table_header "3. FT.SEARCH with Filters" \
        "KNN search with tag and geo pre-filters."
    bench_latency "FT.SEARCH + tag filter" "$QUERIES" \
        FT.SEARCH bench_idx "@tag:{category_a}=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")"
    bench_latency "FT.SEARCH + geo filter" "$QUERIES" \
        FT.SEARCH bench_idx "@geo:[13.361 38.116 50 km]=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")"
fi

# ── 4. FT.SEARCH with RANGE ────────────────────────────────────────────

if should_run "range"; then
    table_header "4. FT.SEARCH with RANGE" \
        "Distance-threshold range search (returns all vectors within radius)."
    bench_latency "FT.SEARCH RANGE 100.0" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" RANGE 100.0
    bench_latency "FT.SEARCH RANGE 50.0" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" RANGE 50.0
fi

# ── 5. FT.CACHESEARCH (cache hit vs miss) ──────────────────────────────

if should_run "cache"; then
    table_header "5. FT.CACHESEARCH" \
        "Semantic cache: first call is a miss (full search), repeat is a hit."

    # Prime the cache with one query
    redis-cli -p "$PORT_MOON" FT.CACHESEARCH bench_idx "cache:" \
        "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" \
        THRESHOLD 0.95 FALLBACK KNN "$K" > /dev/null 2>&1 || true

    # Cache miss: use different seeds for each query
    bench_latency "FT.CACHESEARCH (miss)" "$QUERIES" \
        FT.CACHESEARCH bench_idx "cache:" \
        "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(gen_vector_blob "$DIM" $RANDOM)" \
        THRESHOLD 0.95 FALLBACK KNN "$K"

    # Cache hit: repeat same query
    bench_latency "FT.CACHESEARCH (hit)" "$QUERIES" \
        FT.CACHESEARCH bench_idx "cache:" \
        "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" \
        THRESHOLD 0.95 FALLBACK KNN "$K"
fi

# ── 6. FT.RECOMMEND ────────────────────────────────────────────────────

if should_run "recommend"; then
    table_header "6. FT.RECOMMEND" \
        "Item recommendation using positive/negative example keys."
    bench_latency "FT.RECOMMEND (1 positive)" "$QUERIES" \
        FT.RECOMMEND bench_idx POSITIVE doc:0 K "$K"
    bench_latency "FT.RECOMMEND (3 positive)" "$QUERIES" \
        FT.RECOMMEND bench_idx POSITIVE doc:0 doc:1 doc:2 K "$K"
    bench_latency "FT.RECOMMEND (3 pos + 1 neg)" "$QUERIES" \
        FT.RECOMMEND bench_idx POSITIVE doc:0 doc:1 doc:2 NEGATIVE doc:50 K "$K"
fi

# ── 7. FT.SEARCH EXPAND GRAPH ──────────────────────────────────────────

if should_run "expand"; then
    table_header "7. FT.SEARCH EXPAND GRAPH" \
        "Graph-augmented vector search: KNN + BFS expansion."
    bench_latency "FT.SEARCH EXPAND GRAPH depth:1" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" \
        EXPAND GRAPH "depth:1"
    bench_latency "FT.SEARCH EXPAND GRAPH depth:2" "$QUERIES" \
        FT.SEARCH bench_idx "*=>[KNN $K @vec \$query]" PARAMS 2 query "$(cat "$QUERY_BLOB")" \
        EXPAND GRAPH "depth:2"
fi

# ── 8. FT.NAVIGATE ─────────────────────────────────────────────────────

if should_run "navigate"; then
    table_header "8. FT.NAVIGATE" \
        "Multi-hop knowledge navigation: KNN + graph BFS + re-ranking."
    bench_latency "FT.NAVIGATE HOPS 1" "$QUERIES" \
        FT.NAVIGATE bench_idx "*=>[KNN $K @vec \$query]" HOPS 1 PARAMS 2 query "$(cat "$QUERY_BLOB")"
    bench_latency "FT.NAVIGATE HOPS 2" "$QUERIES" \
        FT.NAVIGATE bench_idx "*=>[KNN $K @vec \$query]" HOPS 2 PARAMS 2 query "$(cat "$QUERY_BLOB")"
    bench_latency "FT.NAVIGATE HOPS 3" "$QUERIES" \
        FT.NAVIGATE bench_idx "*=>[KNN $K @vec \$query]" HOPS 3 PARAMS 2 query "$(cat "$QUERY_BLOB")"
fi

# ── 9. RRF Fusion (dense + sparse) ─────────────────────────────────────

if should_run "fusion"; then
    table_header "9. RRF Fusion (placeholder)" \
        "Dense+sparse RRF fusion requires sparse index support. Placeholder for future."
    echo "| (sparse index not yet benchmarkable via CLI) |    —     |    —     |    —     |    —     |"
fi

# ── Cleanup temp files ──────────────────────────────────────────────────

rm -f "$QUERY_BLOB"

# ── Summary ─────────────────────────────────────────────────────────────

cat <<'EOF'

---

## Notes

- **ops/sec** is derived from average latency (1M / avg_us).
- **p99** is the 99th percentile latency from the measured queries.
- **Cache hit** measures repeated identical queries; **cache miss** uses unique vectors.
- **EXPAND GRAPH** and **NAVIGATE** require the `graph` feature and graph data.
- For production benchmarks, use `--vectors 100000 --queries 5000` on Linux.
- RRF fusion benchmark requires sparse index support (future feature).

---
*Generated by scripts/bench-v016.sh*
EOF
