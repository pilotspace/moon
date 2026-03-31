#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-vector.sh -- Vector engine benchmark suite
#
# Orchestrates Criterion HNSW benchmarks at multiple scales and dimensions,
# then formats results into a markdown report. Optionally runs server-path
# benchmarks (FT.CREATE + FT.SEARCH) via a Moon server instance.
#
# Usage:
#   ./scripts/bench-vector.sh                  # Full run (Criterion + server)
#   ./scripts/bench-vector.sh --criterion-only # Criterion benchmarks only
#   ./scripts/bench-vector.sh --server-only    # Server-path benchmarks only
#   ./scripts/bench-vector.sh --dim 768        # Override dimension
#   ./scripts/bench-vector.sh --scale 50000    # Override vector count
#   ./scripts/bench-vector.sh --output FILE    # Custom output file
#   ./scripts/bench-vector.sh --help           # Show usage
###############################################################################

# ── Configuration ──────────────────────────────────────────────────────

PORT_MOON=6400
REQUESTS=1000
SHARDS=1
DIMENSIONS=128
SCALE=10000
EF_SEARCH=64
RUST_BINARY="./target/release/moon"
OUTPUT_FILE="BENCHMARK-VECTOR.md"

MODE="both"   # "both", "criterion", "server"

MOON_PID=""

# ── Argument parsing ──────────────────────────────────────────────────

usage() {
    cat <<'USAGE'
bench-vector.sh -- Vector engine benchmark suite

OPTIONS:
  --requests N       Number of search requests for server-path bench (default: 1000)
  --shards N         Moon shard count (default: 1)
  --dim N            Vector dimension for server-path bench (default: 128)
  --scale N          Number of vectors to insert (default: 10000)
  --ef N             ef_search parameter (default: 64)
  --output FILE      Output markdown file (default: BENCHMARK-VECTOR.md)
  --criterion-only   Run only Criterion benchmarks (no server)
  --server-only      Run only server-path benchmarks
  --help             Show this help

EXAMPLES:
  ./scripts/bench-vector.sh                        # Full run
  ./scripts/bench-vector.sh --dim 768 --scale 5000 # 768d at 5K vectors
  ./scripts/bench-vector.sh --criterion-only       # Criterion only

OUTPUT:
  Generates a markdown report (BENCHMARK-VECTOR.md) with:
  - Criterion HNSW build throughput (vectors/sec) at 128d and 768d
  - Criterion HNSW search QPS at multiple scales and ef_search values
  - Server-path FT.SEARCH latency and throughput (optional)
  - System information and configuration
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --requests requires a numeric value"; exit 1
            fi
            REQUESTS="$2"; shift 2 ;;
        --shards)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --shards requires a numeric value"; exit 1
            fi
            SHARDS="$2"; shift 2 ;;
        --dim)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --dim requires a numeric value"; exit 1
            fi
            DIMENSIONS="$2"; shift 2 ;;
        --scale)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --scale requires a numeric value"; exit 1
            fi
            SCALE="$2"; shift 2 ;;
        --ef)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --ef requires a numeric value"; exit 1
            fi
            EF_SEARCH="$2"; shift 2 ;;
        --output)
            if [[ -z "${2:-}" ]] || [[ "$2" == --* ]]; then
                echo "Error: --output requires a file path"; exit 1
            fi
            OUTPUT_FILE="$2"; shift 2 ;;
        --criterion-only)
            MODE="criterion"; shift ;;
        --server-only)
            MODE="server"; shift ;;
        --help|-h)
            usage ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${MOON_PID:-}" ]] && kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    pkill -f "moon.*${PORT_MOON}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_server() {
    local port="$1" name="$2" max_wait=15 elapsed=0
    while (( elapsed < max_wait )); do
        if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    echo "$name failed to start on port $port within ${max_wait}s"
    exit 1
}

# ── System info ────────────────────────────────────────────────────────

collect_system_info() {
    echo "## System Information"
    echo ""
    echo "- **Date:** $(date +%Y-%m-%d)"
    echo "- **Platform:** $(uname -s) $(uname -m)"
    echo "- **CPU:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu 2>/dev/null | grep 'Model name' | sed 's/Model name:\s*//' || echo 'unknown')"
    echo "- **Memory:** $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GB", $1/1073741824}' || free -h 2>/dev/null | awk '/Mem:/{print $2}' || echo 'unknown')"
    echo "- **Rust:** $(rustc --version 2>/dev/null || echo 'unknown')"
    echo ""
}

# ── Criterion benchmark section ────────────────────────────────────────

run_criterion_benchmarks() {
    log "Building release binary..."
    cargo build --release 2>&1 | tail -3

    log "Running Criterion HNSW benchmarks (this may take several minutes)..."
    local raw_output
    raw_output=$(cargo bench --bench hnsw_bench -- --output-format=bencher 2>&1 || true)

    echo "## Criterion HNSW Benchmarks"
    echo ""
    echo "Criterion micro-benchmarks measure pure HNSW performance (no network overhead)."
    echo ""

    # ── Build throughput ──
    echo "### Build Throughput"
    echo ""
    printf "| %-25s | %18s | %18s |\n" "Configuration" "Time/iter" "Throughput"
    printf "|%-27s|%20s|%20s|\n" "---------------------------" "--------------------" "--------------------"

    echo "$raw_output" | grep "^test " | grep "hnsw_build" | while IFS= read -r line; do
        local name ns_iter
        name=$(echo "$line" | awk '{print $2}')
        ns_iter=$(echo "$line" | awk '{print $5}' | tr -d ',')

        if [[ -n "$ns_iter" ]] && [[ "$ns_iter" != "0" ]]; then
            # Extract scale from name (e.g., hnsw_build/build/1000)
            local scale
            scale=$(echo "$name" | grep -oE '[0-9]+$' || echo "?")
            local ms_iter
            ms_iter=$(awk "BEGIN { printf \"%.2f ms\", $ns_iter / 1000000 }")
            local vecs_per_sec
            if [[ "$scale" != "?" ]]; then
                vecs_per_sec=$(awk "BEGIN { printf \"%.0f vec/s\", $scale / ($ns_iter / 1000000000) }")
            else
                vecs_per_sec="N/A"
            fi
            printf "| %-25s | %18s | %18s |\n" "$name" "$ms_iter" "$vecs_per_sec"
        fi
    done

    echo ""

    # ── Search QPS ──
    echo "### Search QPS"
    echo ""
    printf "| %-35s | %14s | %14s |\n" "Configuration" "Latency" "QPS"
    printf "|%-37s|%16s|%16s|\n" "-------------------------------------" "----------------" "----------------"

    echo "$raw_output" | grep "^test " | grep "hnsw_search" | while IFS= read -r line; do
        local name ns_iter
        name=$(echo "$line" | awk '{print $2}')
        ns_iter=$(echo "$line" | awk '{print $5}' | tr -d ',')

        if [[ -n "$ns_iter" ]] && [[ "$ns_iter" != "0" ]]; then
            local us_iter qps
            us_iter=$(awk "BEGIN { printf \"%.1f us\", $ns_iter / 1000 }")
            qps=$(awk "BEGIN { printf \"%.0f\", 1000000000 / $ns_iter }")
            printf "| %-35s | %14s | %14s |\n" "$name" "$us_iter" "$qps"
        fi
    done

    echo ""

    # ── Raw bencher output (collapsed) ──
    echo "<details>"
    echo "<summary>Raw Criterion output</summary>"
    echo ""
    echo '```'
    echo "$raw_output" | grep "^test " || echo "(no bencher output captured)"
    echo '```'
    echo ""
    echo "</details>"
    echo ""
}

# ── Server-path benchmark section ──────────────────────────────────────

run_server_benchmarks() {
    if ! command -v redis-cli &>/dev/null; then
        log "WARNING: redis-cli not found, skipping server-path benchmarks"
        echo "## Server-Path Benchmarks"
        echo ""
        echo "*Skipped: redis-cli not found in PATH.*"
        echo ""
        return
    fi

    log "Building release binary..."
    cargo build --release 2>&1 | tail -3

    log "Starting Moon server on port $PORT_MOON ($SHARDS shards)..."
    RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no &
    MOON_PID=$!
    wait_for_server "$PORT_MOON" "Moon"

    echo "## Server-Path Benchmarks"
    echo ""
    echo "End-to-end benchmarks including network, parsing, and command dispatch."
    echo ""
    echo "- **Port:** $PORT_MOON"
    echo "- **Shards:** $SHARDS"
    echo "- **Dimension:** $DIMENSIONS"
    echo "- **Scale:** $SCALE vectors"
    echo "- **ef_search:** $EF_SEARCH"
    echo ""

    # Create index
    log "Creating vector index (dim=$DIMENSIONS)..."
    redis-cli -p "$PORT_MOON" FT.CREATE bench_idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 TYPE FLOAT32 DIM "$DIMENSIONS" DISTANCE_METRIC L2 2>/dev/null || true

    # Insert vectors via pipeline
    log "Inserting $SCALE vectors (dim=$DIMENSIONS)..."
    local insert_start insert_end insert_duration
    insert_start=$(date +%s%N)

    # Generate and insert vectors in batches via redis-cli pipe
    python3 -c "
import struct, random, sys
random.seed(42)
for i in range($SCALE):
    vec_bytes = struct.pack('<${DIMENSIONS}f', *[random.gauss(0,1) for _ in range($DIMENSIONS)])
    hex_str = vec_bytes.hex()
    # Use HSET with hex-encoded vector (redis-cli --pipe expects RESP)
    cmd = f'HSET doc:{i} vec {hex_str}\r\n'
    sys.stdout.write(f'*4\r\n\$4\r\nHSET\r\n\${len(f\"doc:{i}\")}\r\ndoc:{i}\r\n\$3\r\nvec\r\n\${len(hex_str)}\r\n{hex_str}\r\n')
" | redis-cli -p "$PORT_MOON" --pipe 2>/dev/null || true

    insert_end=$(date +%s%N)
    insert_duration=$(( (insert_end - insert_start) / 1000000 ))

    local insert_rate
    if [[ "$insert_duration" -gt 0 ]]; then
        insert_rate=$(awk "BEGIN { printf \"%.0f\", $SCALE / ($insert_duration / 1000.0) }")
    else
        insert_rate="N/A"
    fi

    echo "### Insert Performance"
    echo ""
    printf "| %-20s | %-20s |\n" "Metric" "Value"
    printf "|%-22s|%-22s|\n" "----------------------" "----------------------"
    printf "| %-20s | %-20s |\n" "Vectors inserted" "$SCALE"
    printf "| %-20s | %-20s |\n" "Total time" "${insert_duration}ms"
    printf "| %-20s | %-20s |\n" "Insert rate" "${insert_rate} vec/s"
    echo ""

    # Search benchmark: generate a query vector and time repeated searches
    log "Running $REQUESTS search queries..."
    local query_hex
    query_hex=$(python3 -c "
import struct, random
random.seed(999)
vec = struct.pack('<${DIMENSIONS}f', *[random.gauss(0,1) for _ in range($DIMENSIONS)])
print(vec.hex(), end='')
")

    local search_start search_end search_duration
    search_start=$(date +%s%N)

    for _ in $(seq 1 "$REQUESTS"); do
        redis-cli -p "$PORT_MOON" FT.SEARCH bench_idx "*=>[KNN 10 @vec \$BLOB]" PARAMS 2 BLOB "$query_hex" >/dev/null 2>&1 || true
    done

    search_end=$(date +%s%N)
    search_duration=$(( (search_end - search_start) / 1000000 ))

    local search_qps avg_latency_us
    if [[ "$search_duration" -gt 0 ]]; then
        search_qps=$(awk "BEGIN { printf \"%.0f\", $REQUESTS / ($search_duration / 1000.0) }")
        avg_latency_us=$(awk "BEGIN { printf \"%.0f\", ($search_duration * 1000.0) / $REQUESTS }")
    else
        search_qps="N/A"
        avg_latency_us="N/A"
    fi

    echo "### Search Performance (FT.SEARCH)"
    echo ""
    printf "| %-20s | %-20s |\n" "Metric" "Value"
    printf "|%-22s|%-22s|\n" "----------------------" "----------------------"
    printf "| %-20s | %-20s |\n" "Queries" "$REQUESTS"
    printf "| %-20s | %-20s |\n" "Total time" "${search_duration}ms"
    printf "| %-20s | %-20s |\n" "QPS" "$search_qps"
    printf "| %-20s | %-20s |\n" "Avg latency" "${avg_latency_us}us"
    printf "| %-20s | %-20s |\n" "ef_search" "$EF_SEARCH"
    printf "| %-20s | %-20s |\n" "k (top-K)" "10"
    echo ""

    # Cleanup index
    redis-cli -p "$PORT_MOON" FT.DROPINDEX bench_idx 2>/dev/null || true

    # Stop server
    kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    MOON_PID=""
}

# ── Main ───────────────────────────────────────────────────────────────

{
    echo "# Vector Engine Benchmark Report"
    echo ""
    echo "**Generated by:** \`scripts/bench-vector.sh\`"
    echo "**Mode:** $MODE"
    echo ""

    collect_system_info

    if [[ "$MODE" == "both" ]] || [[ "$MODE" == "criterion" ]]; then
        run_criterion_benchmarks
    fi

    if [[ "$MODE" == "both" ]] || [[ "$MODE" == "server" ]]; then
        run_server_benchmarks
    fi

    echo "---"
    echo "*Generated by bench-vector.sh on $(date +%Y-%m-%d\ %H:%M:%S)*"
} > "$OUTPUT_FILE"

log "Report written to $OUTPUT_FILE"
log "Done."
