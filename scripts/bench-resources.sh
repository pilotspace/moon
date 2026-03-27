#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-resources.sh -- CPU & Memory comparison: moon vs Redis
#
# Starts fresh server instances per test for accurate RSS measurement.
# Measures absolute RSS, per-key memory, throughput, and CPU efficiency.
#
# Usage:
#   ./scripts/bench-resources.sh                       # Run all tests
#   ./scripts/bench-resources.sh --shards N            # moon shard count
#   ./scripts/bench-resources.sh --skip-build          # Skip cargo build
#   ./scripts/bench-resources.sh --output FILE         # Output file path
#   ./scripts/bench-resources.sh --quick               # Smaller test matrix
###############################################################################

PORT_REDIS=6399
PORT_RUST=6400
SHARDS=0
SKIP_BUILD=false
OUTPUT_FILE="BENCHMARK-RESOURCES.md"
RUST_BINARY="./target/release/moon"
QUICK=false

RUST_PID=""
REDIS_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --output)     OUTPUT_FILE="$2"; shift 2 ;;
        --quick)      QUICK=true; shift ;;
        --help|-h)    sed -n '3,14p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
    local port=$1
    for ((i=0; i<30; i++)); do
        redis-cli -p "$port" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.2
    done
    log "ERROR: port $port not ready"; return 1
}

get_rss_kb() {
    ps -o rss= -p "$1" 2>/dev/null | tr -d ' '
}

get_cpu_pct() {
    ps -o %cpu= -p "$1" 2>/dev/null | tr -d ' '
}

human_kb() {
    local kb=${1:-0}
    if (( kb < 0 )); then echo "${kb}KB"
    elif (( kb >= 1048576 )); then echo "$(echo "scale=1; $kb / 1048576" | bc)GB"
    elif (( kb >= 1024 )); then echo "$(echo "scale=1; $kb / 1024" | bc)MB"
    else echo "${kb}KB"
    fi
}

start_servers() {
    # Kill any lingering instances
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
    sleep 0.3

    redis-server --port "$PORT_REDIS" --save "" --appendonly no \
        --loglevel warning --daemonize no &>/dev/null &
    REDIS_PID=$!

    "$RUST_BINARY" --port "$PORT_RUST" --shards "$SHARDS" &>/dev/null &
    RUST_PID=$!

    wait_for_port "$PORT_REDIS"
    wait_for_port "$PORT_RUST"
}

stop_servers() {
    kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    RUST_PID=""
    REDIS_PID=""
    sleep 0.5
}

# ===========================================================================
# Main benchmark: fresh servers per data point for accurate RSS
# ===========================================================================

bench_memory_row() {
    local num_keys=$1 val_size=$2 label=$3

    start_servers

    # Measure baseline RSS right after startup
    sleep 0.5
    local redis_base rust_base
    redis_base=$(get_rss_kb "$REDIS_PID")
    rust_base=$(get_rss_kb "$RUST_PID")

    # Load data
    log "  [$label] Loading $num_keys keys (${val_size}B) ..."
    local redis_rps rust_rps
    # -r ensures unique random keys (key:000000XXXXXX with random suffix)
    redis_rps=$(redis-benchmark -p "$PORT_REDIS" -t set -n "$num_keys" -r "$num_keys" -d "$val_size" \
        -P 16 -c 50 --csv -q 2>&1 | tr '\r' '\n' | grep -i '"SET"' | head -1 | cut -d, -f2 | tr -d '"') || true
    rust_rps=$(redis-benchmark -p "$PORT_RUST" -t set -n "$num_keys" -r "$num_keys" -d "$val_size" \
        -P 16 -c 50 --csv -q 2>&1 | tr '\r' '\n' | grep -i '"SET"' | head -1 | cut -d, -f2 | tr -d '"') || true

    sleep 1  # let allocator settle

    # Measure loaded RSS
    local redis_rss rust_rss
    redis_rss=$(get_rss_kb "$REDIS_PID")
    rust_rss=$(get_rss_kb "$RUST_PID")

    # DBSIZE
    local redis_db rust_db
    redis_db=$(redis-cli -p "$PORT_REDIS" DBSIZE 2>/dev/null | grep -oE '[0-9]+') || true
    rust_db=$(redis-cli -p "$PORT_RUST" DBSIZE 2>/dev/null | grep -oE '[0-9]+') || true
    redis_db=${redis_db:-0}
    rust_db=${rust_db:-0}

    # used_memory from INFO (Redis reports this; moon may not)
    local redis_used
    redis_used=$(redis-cli -p "$PORT_REDIS" INFO memory 2>/dev/null | grep "^used_memory:" | cut -d: -f2 | tr -d '\r') || true
    redis_used=${redis_used:-0}

    # Delta = loaded - baseline
    local redis_delta=$((redis_rss - redis_base))
    local rust_delta=$((rust_rss - rust_base))

    # Per-key bytes
    local redis_per_key=0 rust_per_key=0
    if (( redis_db > 0 && redis_delta > 0 )); then
        redis_per_key=$(( redis_delta * 1024 / redis_db ))
    fi
    if (( rust_db > 0 && rust_delta > 0 )); then
        rust_per_key=$(( rust_delta * 1024 / rust_db ))
    fi

    # Memory ratio (Redis/Rust — >1 means rust uses less)
    local mem_ratio="N/A"
    if (( rust_delta > 100 && redis_delta > 100 )); then
        mem_ratio=$(echo "scale=2; $redis_delta * 100 / $rust_delta" | bc)"%"
    fi

    # CPU efficiency: run GET benchmark and sample CPU
    log "  [$label] Measuring CPU during GET..."
    local redis_cpu rust_cpu
    redis-benchmark -p "$PORT_REDIS" -t get -n "$num_keys" -r "$num_keys" -P 16 -c 50 -q &>/dev/null &
    local bp=$!
    sleep 2
    redis_cpu=$(get_cpu_pct "$REDIS_PID")
    wait "$bp" 2>/dev/null || true

    redis-benchmark -p "$PORT_RUST" -t get -n "$num_keys" -r "$num_keys" -P 16 -c 50 -q &>/dev/null &
    bp=$!
    sleep 2
    rust_cpu=$(get_cpu_pct "$RUST_PID")
    wait "$bp" 2>/dev/null || true

    stop_servers

    echo "| ${label} | ${redis_db} | ${rust_db} | $(human_kb "$redis_base") | $(human_kb "$rust_base") | $(human_kb "$redis_rss") | $(human_kb "$rust_rss") | $(human_kb "$redis_delta") | $(human_kb "$rust_delta") | ${redis_per_key}B | ${rust_per_key}B | ${mem_ratio} | ${redis_rps:-?} | ${rust_rps:-?} | ${redis_cpu:-?}% | ${rust_cpu:-?}% |"
}

bench_ttl_overhead() {
    # Test 1: fresh servers, load keys WITHOUT TTL
    start_servers
    sleep 0.5
    local redis_base_no rust_base_no
    redis_base_no=$(get_rss_kb "$REDIS_PID")
    rust_base_no=$(get_rss_kb "$RUST_PID")

    log "  [TTL] Loading 500K keys without TTL..."
    redis-benchmark -p "$PORT_REDIS" -t set -n 500000 -r 500000 -d 64 -P 16 -c 50 -q &>/dev/null
    redis-benchmark -p "$PORT_RUST" -t set -n 500000 -r 500000 -d 64 -P 16 -c 50 -q &>/dev/null
    sleep 1

    local redis_no_ttl rust_no_ttl
    redis_no_ttl=$(get_rss_kb "$REDIS_PID")
    rust_no_ttl=$(get_rss_kb "$RUST_PID")
    local redis_data_no=$((redis_no_ttl - redis_base_no))
    local rust_data_no=$((rust_no_ttl - rust_base_no))
    stop_servers

    # Test 2: fresh servers, load keys WITH TTL
    start_servers
    sleep 0.5
    local redis_base_ttl rust_base_ttl
    redis_base_ttl=$(get_rss_kb "$REDIS_PID")
    rust_base_ttl=$(get_rss_kb "$RUST_PID")

    log "  [TTL] Loading 500K keys with SETEX (TTL=3600)..."
    redis-benchmark -p "$PORT_REDIS" -n 500000 -r 500000 -d 64 -P 16 -c 50 -q \
        SETEX __rand_key__ 3600 __rand_val__ &>/dev/null 2>&1 || true
    redis-benchmark -p "$PORT_RUST" -n 500000 -r 500000 -d 64 -P 16 -c 50 -q \
        SETEX __rand_key__ 3600 __rand_val__ &>/dev/null 2>&1 || true
    sleep 1

    local redis_ttl rust_ttl
    redis_ttl=$(get_rss_kb "$REDIS_PID")
    rust_ttl=$(get_rss_kb "$RUST_PID")
    local redis_data_ttl=$((redis_ttl - redis_base_ttl))
    local rust_data_ttl=$((rust_ttl - rust_base_ttl))

    local redis_db_ttl rust_db_ttl
    redis_db_ttl=$(redis-cli -p "$PORT_REDIS" DBSIZE 2>/dev/null | grep -oE '[0-9]+') || true
    rust_db_ttl=$(redis-cli -p "$PORT_RUST" DBSIZE 2>/dev/null | grep -oE '[0-9]+') || true

    # TTL overhead = data_with_ttl - data_without_ttl
    local redis_overhead=$((redis_data_ttl - redis_data_no))
    local rust_overhead=$((rust_data_ttl - rust_data_no))

    stop_servers

    echo "| Metric | Redis | moon | Notes |"
    echo "|--------|-------|------------|-------|"
    echo "| Keys loaded (SETEX) | ${redis_db_ttl:-?} | ${rust_db_ttl:-?} | |"
    echo "| RSS data (no TTL) | $(human_kb "$redis_data_no") | $(human_kb "$rust_data_no") | Fresh server, 500K x 64B SET |"
    echo "| RSS data (with TTL) | $(human_kb "$redis_data_ttl") | $(human_kb "$rust_data_ttl") | Fresh server, 500K x 64B SETEX |"
    echo "| **TTL extra cost** | **$(human_kb "$redis_overhead")** | **$(human_kb "$rust_overhead")** | Difference |"
    if (( redis_data_no > 0 )); then
        local redis_pct=$(echo "scale=1; $redis_overhead * 100 / $redis_data_no" | bc 2>/dev/null || echo "?")
        local rust_pct=$(echo "scale=1; $rust_overhead * 100 / $rust_data_no" | bc 2>/dev/null || echo "?")
        echo "| TTL overhead % | ${redis_pct}% | ${rust_pct}% | % of base data |"
    fi
}

bench_cpu_efficiency() {
    start_servers

    # Pre-load 200K keys
    redis-benchmark -p "$PORT_REDIS" -t set -n 200000 -r 200000 -d 128 -P 16 -c 50 -q &>/dev/null
    redis-benchmark -p "$PORT_RUST" -t set -n 200000 -r 200000 -d 128 -P 16 -c 50 -q &>/dev/null
    sleep 0.5

    echo "| Pipeline | Redis CPU% | Rust CPU% | Redis RPS | Rust RPS | RPS Ratio | Redis CPU/100K-ops | Rust CPU/100K-ops |"
    echo "|----------|------------|-----------|-----------|----------|-----------|--------------------|--------------------|"

    for pipeline in 1 8 16 64; do
        log "  [CPU] Pipeline=$pipeline..."

        # Redis: run sustained workload, sample CPU mid-flight
        redis-benchmark -p "$PORT_REDIS" -t get,set -n 1000000 -r 200000 -P "$pipeline" -c 50 -q &>/dev/null &
        local bp=$!
        sleep 3
        local rcpu
        rcpu=$(get_cpu_pct "$REDIS_PID")
        wait "$bp" 2>/dev/null || true

        # Get RPS for this pipeline depth
        local rrps
        rrps=$(redis-benchmark -p "$PORT_REDIS" -t get -n 500000 -r 200000 -P "$pipeline" -c 50 --csv -q 2>&1 \
            | tr '\r' '\n' | grep -i '"GET"' | head -1 | cut -d, -f2 | tr -d '"') || true

        # moon
        redis-benchmark -p "$PORT_RUST" -t get,set -n 1000000 -r 200000 -P "$pipeline" -c 50 -q &>/dev/null &
        bp=$!
        sleep 3
        local ucpu
        ucpu=$(get_cpu_pct "$RUST_PID")
        wait "$bp" 2>/dev/null || true

        local urps
        urps=$(redis-benchmark -p "$PORT_RUST" -t get -n 500000 -r 200000 -P "$pipeline" -c 50 --csv -q 2>&1 \
            | tr '\r' '\n' | grep -i '"GET"' | head -1 | cut -d, -f2 | tr -d '"') || true

        # RPS ratio
        local rps_ratio="N/A"
        if [[ -n "${rrps:-}" ]] && [[ -n "${urps:-}" ]]; then
            rps_ratio=$(echo "scale=2; ${urps} / ${rrps}" | bc 2>/dev/null || echo "N/A")
        fi

        # CPU efficiency: CPU% per 100K ops/sec
        local redis_eff="N/A" rust_eff="N/A"
        if [[ -n "${rcpu:-}" ]] && [[ -n "${rrps:-}" ]] && (( $(echo "$rrps > 0" | bc) )); then
            redis_eff=$(echo "scale=2; ${rcpu} / (${rrps} / 100000)" | bc 2>/dev/null || echo "N/A")
        fi
        if [[ -n "${ucpu:-}" ]] && [[ -n "${urps:-}" ]] && (( $(echo "$urps > 0" | bc) )); then
            rust_eff=$(echo "scale=2; ${ucpu} / (${urps} / 100000)" | bc 2>/dev/null || echo "N/A")
        fi

        echo "| P=$pipeline | ${rcpu:-?}% | ${ucpu:-?}% | ${rrps:-?} | ${urps:-?} | ${rps_ratio}x | ${redis_eff}% | ${rust_eff}% |"
    done

    stop_servers
}

# ===========================================================================
# Main
# ===========================================================================

main() {
    log "============================================"
    log "  Resource Benchmark: moon vs Redis"
    log "============================================"

    if [[ "$SKIP_BUILD" == false ]]; then
        log "Building moon (release)..."
        cargo build --release 2>&1 | tail -3
    fi

    command -v redis-server &>/dev/null || { log "ERROR: redis-server not found"; exit 1; }
    command -v redis-benchmark &>/dev/null || { log "ERROR: redis-benchmark not found"; exit 1; }

    # Get Redis version from quick start
    start_servers
    local redis_ver
    redis_ver=$(redis-cli -p "$PORT_REDIS" INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '\r')
    stop_servers

    local nproc os_info
    nproc=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo "?")
    os_info=$(uname -srm)

    log "Redis: $redis_ver | System: $os_info, $nproc cores"
    log "Fresh server per data point for accurate RSS"
    log ""

    # Test matrix
    local key_counts val_sizes
    if [[ "$QUICK" == true ]]; then
        key_counts=(100000 1000000)
        val_sizes=(64 1024)
    else
        key_counts=(100000 500000 1000000)
        val_sizes=(32 256 1024 4096)
    fi

    {
        echo "# Resource Benchmark: moon vs Redis"
        echo ""
        echo "**Date:** $(date '+%Y-%m-%d %H:%M:%S')"
        echo "**System:** ${os_info}, ${nproc} cores"
        echo "**Redis:** ${redis_ver}"
        echo "**moon shards:** ${SHARDS} (0=auto)"
        echo "**Method:** Fresh server per data point (accurate RSS, no allocator hysteresis)"
        echo ""

        # --- String keys ---
        echo "## String Keys: Memory & Throughput"
        echo ""
        echo "| Test | Redis Keys | Rust Keys | Redis Base | Rust Base | Redis RSS | Rust RSS | Redis Data | Rust Data | Redis/Key | Rust/Key | Rust as % of Redis | Redis SET/s | Rust SET/s | Redis CPU | Rust CPU |"
        echo "|------|-----------|-----------|------------|-----------|-----------|----------|------------|-----------|-----------|----------|--------------------:|-------------|------------|-----------|----------|"

        for nk in "${key_counts[@]}"; do
            for vs in "${val_sizes[@]}"; do
                local nk_label
                if (( nk >= 1000000 )); then nk_label="$((nk/1000000))M"
                else nk_label="$((nk/1000))K"; fi
                bench_memory_row "$nk" "$vs" "${nk_label} x ${vs}B"
            done
        done

        echo ""

        # --- TTL overhead ---
        echo "## TTL Memory Overhead (500K keys x 64B)"
        echo ""
        bench_ttl_overhead
        echo ""
        echo "> Redis stores TTL in a separate \`expires\` dict (extra dictEntry per key)."
        echo "> moon packs TTL as a 4-byte delta inside CompactEntry (zero extra allocation)."
        echo ""

        # --- CPU efficiency ---
        echo "## CPU Efficiency (200K pre-loaded keys, GET+SET mixed)"
        echo ""
        echo "CPU/100K-ops = CPU% normalized by throughput. Lower = more efficient."
        echo ""
        bench_cpu_efficiency

        echo ""
        echo "---"
        echo "*Generated by bench-resources.sh on $(date '+%Y-%m-%d %H:%M:%S')*"
    } > "$OUTPUT_FILE"

    log ""
    log "Done! Report: $OUTPUT_FILE"
    log ""
    cat "$OUTPUT_FILE" >&2
}

main
