#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-v2.sh -- Phase 23: End-to-End Performance Validation Benchmark Suite
#
# Primary tool: memtier_benchmark (industry standard, HdrHistogram, Zipfian)
# Do NOT use redis-benchmark for primary results (lacks Zipfian + rate-limiting)
#
# Usage:
#   ./bench-v2.sh                         # Full suite: rust-redis + Redis 7.x comparison
#   ./bench-v2.sh --smoke-test            # Fast validation: 1K keys, 10s runs
#   ./bench-v2.sh --rust-only             # Skip Redis 7.x comparison
#   ./bench-v2.sh --redis-only            # Only benchmark Redis 7.x
#   ./bench-v2.sh --memory-only           # Memory overhead section only
#   ./bench-v2.sh --snapshot-only         # Snapshot spike section only
#   ./bench-v2.sh --open-loop-only        # Open-loop rate-limited section only
#   ./bench-v2.sh --server-host HOST      # Point at remote server (for target hardware)
#   ./bench-v2.sh --output FILE           # Write BENCHMARK-v2.md to FILE
#   ./bench-v2.sh --keys N                # Override key count (default: 10000000)
#   ./bench-v2.sh --test-time N           # Override test duration in seconds (default: 180)
#
# Methodology:
#   - Pre-populate with P:P (sequential) to ensure ALL keys are loaded
#   - Benchmark with Z:Z (Zipfian alpha ~0.99) to simulate real-world hot-spot access
#   - 5 iterations per configuration; report median ops/sec with min/max range
#   - Open-loop (--rate-limiting) tests for coordinated-omission-aware latency curves
#   - Compare against Redis 7.x and N-shard Redis simulation (same machine, same core count)
#   - Client co-location caveat documented when client runs on same machine as server
###############################################################################

# ===========================================================================
# Configuration
# ===========================================================================

PORT_RUST=6399
PORT_REDIS=6400
REDIS_CLUSTER_BASE_PORT=7000

# Benchmark parameters (overridden by --smoke-test)
KEY_MAX=10000000
TEST_TIME=180
WARMUP_TIME=30
RUN_COUNT=5
THREADS=4
CLIENTS_LIST=(1 10 50 100)
PIPELINE_LIST=(1 16 64)
DATA_SIZE=256

# Smoke test overrides
SMOKE_TEST=false
SMOKE_KEY_MAX=1000
SMOKE_TEST_TIME=10
SMOKE_WARMUP_TIME=5
SMOKE_RUN_COUNT=1

# Flags
RUN_RUST=true
RUN_REDIS=true
RUN_MEMORY=true
RUN_SNAPSHOT=true
RUN_OPEN_LOOP=true
RUN_THROUGHPUT=true
SERVER_HOST="127.0.0.1"
OUTPUT_FILE="BENCHMARK-v2.md"
RESULTS_DIR=""

RUST_BINARY="./target/release/rust-redis"
RUST_PID=""
REDIS_PID=""
REDIS_CLUSTER_PIDS=()

# ===========================================================================
# Argument Parsing
# ===========================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --smoke-test)
                SMOKE_TEST=true
                shift
                ;;
            --rust-only)
                RUN_RUST=true
                RUN_REDIS=false
                shift
                ;;
            --redis-only)
                RUN_RUST=false
                RUN_REDIS=true
                shift
                ;;
            --memory-only)
                RUN_THROUGHPUT=false
                RUN_SNAPSHOT=false
                RUN_OPEN_LOOP=false
                RUN_MEMORY=true
                shift
                ;;
            --snapshot-only)
                RUN_THROUGHPUT=false
                RUN_MEMORY=false
                RUN_OPEN_LOOP=false
                RUN_SNAPSHOT=true
                shift
                ;;
            --open-loop-only)
                RUN_THROUGHPUT=false
                RUN_MEMORY=false
                RUN_SNAPSHOT=false
                RUN_OPEN_LOOP=true
                shift
                ;;
            --server-host)
                SERVER_HOST="${2:?--server-host requires a HOST argument}"
                shift 2
                ;;
            --output)
                OUTPUT_FILE="${2:?--output requires a FILE argument}"
                shift 2
                ;;
            --keys)
                KEY_MAX="${2:?--keys requires a number}"
                shift 2
                ;;
            --test-time)
                TEST_TIME="${2:?--test-time requires a number}"
                shift 2
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --smoke-test          Fast validation: 1K keys, 10s runs, 1 iteration"
                echo "  --rust-only           Only benchmark rust-redis (skip Redis baseline)"
                echo "  --redis-only          Only benchmark Redis 7.x"
                echo "  --memory-only         Run memory overhead section only"
                echo "  --snapshot-only       Run snapshot spike section only"
                echo "  --open-loop-only      Run open-loop rate-limited section only"
                echo "  --server-host HOST    Point at remote server (default: 127.0.0.1)"
                echo "  --output FILE         Output file (default: BENCHMARK-v2.md)"
                echo "  --keys N              Override key count (default: 10000000)"
                echo "  --test-time N         Override test duration in seconds (default: 180)"
                echo "  --help, -h            Show this help"
                exit 0
                ;;
            *)
                echo "Unknown option: $1" >&2
                echo "Use --help for usage information." >&2
                exit 1
                ;;
        esac
    done

    # Apply smoke test overrides
    if [[ "$SMOKE_TEST" == "true" ]]; then
        KEY_MAX="$SMOKE_KEY_MAX"
        TEST_TIME="$SMOKE_TEST_TIME"
        WARMUP_TIME="$SMOKE_WARMUP_TIME"
        RUN_COUNT="$SMOKE_RUN_COUNT"
    fi
}

# ===========================================================================
# Helper Functions
# ===========================================================================

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    # Kill rust-redis if started by this script
    if [[ -n "${RUST_PID:-}" ]] && kill -0 "$RUST_PID" 2>/dev/null; then
        kill "$RUST_PID" 2>/dev/null || true; wait "$RUST_PID" 2>/dev/null || true
    fi
    # Kill redis comparison instance
    if [[ -n "${REDIS_PID:-}" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
        kill "$REDIS_PID" 2>/dev/null || true; wait "$REDIS_PID" 2>/dev/null || true
    fi
    # Kill Redis cluster simulation instances
    for pid in "${REDIS_CLUSTER_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    # Keep RESULTS_DIR: raw data is useful for offline analysis
}
trap cleanup EXIT

check_memtier() {
    if ! command -v memtier_benchmark &>/dev/null; then
        echo "ERROR: memtier_benchmark is not installed." >&2
        echo "Install:" >&2
        echo "  macOS:  brew install memtier_benchmark" >&2
        echo "  Ubuntu: sudo apt-get install -y memtier-benchmark" >&2
        echo "  Source: git clone https://github.com/redis/memtier_benchmark.git" >&2
        echo "          cd memtier_benchmark && autoreconf -ivf && ./configure && make && sudo make install" >&2
        exit 1
    fi
    log "memtier_benchmark: $(memtier_benchmark --version 2>&1 | head -1)"
}

check_redis_cli() {
    if ! command -v redis-cli &>/dev/null; then
        log "WARNING: redis-cli not found. Memory measurements will be skipped."
        RUN_MEMORY=false
        RUN_SNAPSHOT=false
    fi
}

check_rust_binary() {
    if [[ ! -x "$RUST_BINARY" ]]; then
        log "rust-redis binary not found. Building with: cargo build --release"
        cargo build --release
    fi
}

wait_for_server() {
    local port=$1
    local max_attempts=100
    local attempt=0
    while ! redis-cli -p "$port" PING &>/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            log "ERROR: Server on port $port failed to start within 10 seconds"
            return 1
        fi
        sleep 0.1
    done
    log "Server ready on port $port"
}

get_rss() {
    local pid=$1
    if [[ "$(uname)" == "Linux" ]]; then
        awk '/VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0
    else
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo 0
    fi
}

start_rust_server() {
    local port=$1
    local ncpu
    ncpu=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo "unknown")
    log "Starting rust-redis on port $port (shards=$ncpu)"
    "$RUST_BINARY" --port "$port" &
    RUST_PID=$!
    wait_for_server "$port"
}

stop_rust_server() {
    if [[ -n "${RUST_PID:-}" ]] && kill -0 "$RUST_PID" 2>/dev/null; then
        log "Stopping rust-redis (PID $RUST_PID)"
        kill "$RUST_PID" 2>/dev/null || true
        wait "$RUST_PID" 2>/dev/null || true
        RUST_PID=""
    fi
}

start_redis_server() {
    local port=$1
    log "Starting redis-server on port $port"
    redis-server --port "$port" --save "" --daemonize no &
    REDIS_PID=$!
    wait_for_server "$port"
}

stop_redis_server() {
    if [[ -n "${REDIS_PID:-}" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
        log "Stopping redis-server (PID $REDIS_PID)"
        kill "$REDIS_PID" 2>/dev/null || true
        wait "$REDIS_PID" 2>/dev/null || true
        REDIS_PID=""
    fi
}

# ===========================================================================
# Pre-population
# ===========================================================================

run_prepopulate() {
    local port=$1 label=$2
    log "Pre-populating $label with $KEY_MAX keys (${DATA_SIZE}B values, sequential P:P)..."
    # MUST use P:P (sequential) not Z:Z (Zipfian) for pre-population.
    # Zipfian concentrates on a subset -- using it here would leave most keys absent.
    memtier_benchmark -s "$SERVER_HOST" -p "$port" \
        -t 4 -c 30 \
        --ratio=1:0 \
        --key-pattern=P:P \
        --key-maximum="$KEY_MAX" \
        -d "$DATA_SIZE" \
        -n allkeys \
        --hide-histogram \
        2>&1 | grep -E "^\[|Ops|RPS|KB/sec" >&2 || true
    log "Pre-population complete. Verifying key count..."
    local actual_keys
    actual_keys=$(redis-cli -h "$SERVER_HOST" -p "$port" DBSIZE 2>/dev/null || echo "unknown")
    log "Keys loaded: $actual_keys (target: $KEY_MAX)"
}

# ===========================================================================
# Primary Throughput Benchmark
# ===========================================================================

run_benchmark_v2() {
    local port=$1 label=$2 clients=$3 pipeline=$4
    local outprefix="$RESULTS_DIR/${label}_c${clients}_p${pipeline}"
    log "  Benchmarking $label: clients=$clients pipeline=$pipeline data=${DATA_SIZE}B"
    # Z:Z = Zipfian distribution (hot-spot access, matches real-world workloads)
    # --run-count=5: 5 iterations, memtier reports median
    # --warmup-time: excluded from stats
    # --distinct-client-seed: different clients use different random seeds
    memtier_benchmark -s "$SERVER_HOST" -p "$port" \
        -t "$THREADS" -c "$clients" \
        --ratio=1:10 \
        --key-pattern=Z:Z \
        --key-maximum="$KEY_MAX" \
        -d "$DATA_SIZE" \
        --test-time="$TEST_TIME" \
        --warmup-time="$WARMUP_TIME" \
        --distinct-client-seed \
        --run-count="$RUN_COUNT" \
        --pipeline="$pipeline" \
        --print-percentiles 50,90,95,99,99.9,99.99 \
        --hdr-file-prefix="${outprefix}_hdr" \
        > "${outprefix}.txt" 2>&1
    log "  Done: ${outprefix}.txt"
}

# ===========================================================================
# Throughput Suite (pre-populate + sweep all concurrency x pipeline)
# ===========================================================================

run_throughput_suite() {
    local port=$1 label=$2
    log "=== Throughput suite: $label ==="
    run_prepopulate "$port" "$label"
    for clients in "${CLIENTS_LIST[@]}"; do
        for pipeline in "${PIPELINE_LIST[@]}"; do
            run_benchmark_v2 "$port" "$label" "$clients" "$pipeline"
        done
    done
}

# ===========================================================================
# Result Extraction Helpers
# ===========================================================================

# Extract total ops/sec from memtier output file (last "Totals" line)
extract_ops_sec() {
    local file=$1
    grep -m1 "^Totals" "$file" 2>/dev/null | awk '{print $2}' || echo "N/A"
}

# Extract p99 latency (ms) from memtier output file
extract_p99() {
    local file=$1
    grep -m1 "^Totals" "$file" 2>/dev/null | awk '{print $9}' || echo "N/A"
}

# Extract p99.9 latency (ms) from memtier output file
extract_p999() {
    local file=$1
    grep -m1 "^Totals" "$file" 2>/dev/null | awk '{print $10}' || echo "N/A"
}

# ===========================================================================
# Memory Overhead Measurement
# ===========================================================================

measure_memory_overhead() {
    # Measures per-key overhead using INFO MEMORY used_memory (allocator-tracked).
    # MUST use used_memory, not RSS (RSS includes TCP buffers, FD tables, OS overhead).
    # Source: CONTEXT.md Memory benchmarks
    local port=$1 label=$2 num_keys=$3

    # Baseline memory before loading any keys
    local baseline
    baseline=$(redis-cli -p "$port" INFO MEMORY 2>/dev/null \
        | awk -F: '/^used_memory:/{gsub(/[[:space:]]/, "", $2); print $2}')

    log "  Memory baseline ($label): ${baseline} bytes"

    # Load num_keys keys with DATA_SIZE values using sequential pattern
    memtier_benchmark -s "$SERVER_HOST" -p "$port" \
        -t 1 -c 1 \
        --ratio=1:0 \
        --key-pattern=P:P \
        --key-maximum="$num_keys" \
        -d "$DATA_SIZE" \
        -n allkeys \
        --hide-histogram \
        &>/dev/null || true

    local loaded
    loaded=$(redis-cli -p "$port" INFO MEMORY 2>/dev/null \
        | awk -F: '/^used_memory:/{gsub(/[[:space:]]/, "", $2); print $2}')

    log "  Memory loaded ($label): ${loaded} bytes"

    if [[ -n "$baseline" ]] && [[ -n "$loaded" ]] && [[ "$loaded" -gt "$baseline" ]]; then
        local overhead=$(( (loaded - baseline) / num_keys ))
        log "  Per-key overhead ($label): ${overhead} bytes (target: <=24 bytes)"
        echo "${label},${baseline},${loaded},${num_keys},${overhead}" \
            >> "$RESULTS_DIR/memory_overhead.csv"
        echo "$overhead"  # return value for callers
    else
        log "  WARNING: Could not compute per-key overhead (baseline=$baseline loaded=$loaded)"
        echo "N/A"
    fi

    # Clean up keys loaded by this function (Pitfall 7: FLUSHALL between runs)
    redis-cli -p "$port" FLUSHALL &>/dev/null || true
}

run_memory_suite() {
    log "=== Memory overhead suite ==="
    local mem_keys=${KEY_MAX}
    # Cap at 100K for smoke test
    [[ "$SMOKE_TEST" == "true" ]] && mem_keys=10000

    echo "label,baseline_bytes,loaded_bytes,num_keys,per_key_overhead_bytes" \
        > "$RESULTS_DIR/memory_overhead.csv"

    if [[ "$RUN_RUST" == "true" ]]; then
        start_rust_server "$PORT_RUST"
        measure_memory_overhead "$PORT_RUST" "rust-redis" "$mem_keys"
        stop_rust_server
    fi

    if [[ "$RUN_REDIS" == "true" ]] && command -v redis-server &>/dev/null; then
        start_redis_server "$PORT_REDIS"
        measure_memory_overhead "$PORT_REDIS" "redis-7x" "$mem_keys"
        stop_redis_server
    fi
}

# ===========================================================================
# Snapshot RSS Spike Measurement
# ===========================================================================

measure_snapshot_spike() {
    # Measures peak RSS increase during async snapshot (BGSAVE).
    # Uses RSS (not used_memory) here because we want to capture the full
    # system-visible memory pressure during snapshot, not just allocator usage.
    local port=$1 pid=$2 label=$3

    # Steady-state RSS before snapshot
    local steady_rss
    steady_rss=$(get_rss "$pid")
    log "  Steady-state RSS ($label): ${steady_rss} KB"

    # Record LASTSAVE time before triggering snapshot
    local before_save
    before_save=$(redis-cli -p "$port" LASTSAVE 2>/dev/null || echo "0")

    # Trigger async snapshot
    redis-cli -p "$port" BGSAVE &>/dev/null || true
    log "  BGSAVE triggered ($label)"

    # Poll RSS every 100ms until snapshot completes (LASTSAVE changes)
    local peak_rss=$steady_rss
    local poll_count=0
    local max_polls=300  # 30 second timeout
    while [[ $poll_count -lt $max_polls ]]; do
        sleep 0.1
        poll_count=$((poll_count + 1))
        local current_rss
        current_rss=$(get_rss "$pid")
        (( current_rss > peak_rss )) && peak_rss=$current_rss

        local current_save
        current_save=$(redis-cli -p "$port" LASTSAVE 2>/dev/null || echo "0")
        if [[ "$current_save" != "$before_save" ]]; then
            log "  Snapshot complete after ${poll_count} polls"
            break
        fi
    done

    if [[ "$steady_rss" -gt 0 ]]; then
        # Use awk for floating point percentage calculation
        local spike_pct
        spike_pct=$(awk "BEGIN {printf \"%.2f\", ($peak_rss - $steady_rss) * 100.0 / $steady_rss}")
        log "  Snapshot RSS spike ($label): ${spike_pct}% (target: <5%)"
        echo "${label},${steady_rss},${peak_rss},${spike_pct}" \
            >> "$RESULTS_DIR/snapshot_spike.csv"
    else
        log "  WARNING: Could not measure snapshot spike (steady_rss=$steady_rss)"
    fi
}

run_snapshot_suite() {
    log "=== Snapshot spike suite ==="
    echo "label,steady_rss_kb,peak_rss_kb,spike_pct" \
        > "$RESULTS_DIR/snapshot_spike.csv"

    # Load enough keys to make snapshot non-trivial
    local snap_keys=${KEY_MAX}
    [[ "$SMOKE_TEST" == "true" ]] && snap_keys=10000

    if [[ "$RUN_RUST" == "true" ]]; then
        start_rust_server "$PORT_RUST"
        # Pre-load keys
        memtier_benchmark -s "$SERVER_HOST" -p "$PORT_RUST" \
            -t 1 -c 1 --ratio=1:0 --key-pattern=P:P \
            --key-maximum="$snap_keys" -d "$DATA_SIZE" -n allkeys \
            --hide-histogram &>/dev/null || true
        measure_snapshot_spike "$PORT_RUST" "$RUST_PID" "rust-redis"
        stop_rust_server
    fi

    if [[ "$RUN_REDIS" == "true" ]] && command -v redis-server &>/dev/null; then
        start_redis_server "$PORT_REDIS"
        memtier_benchmark -s "$SERVER_HOST" -p "$PORT_REDIS" \
            -t 1 -c 1 --ratio=1:0 --key-pattern=P:P \
            --key-maximum="$snap_keys" -d "$DATA_SIZE" -n allkeys \
            --hide-histogram &>/dev/null || true
        measure_snapshot_spike "$PORT_REDIS" "$REDIS_PID" "redis-7x"
        stop_redis_server
    fi
}

# ===========================================================================
# Open-Loop (Coordinated-Omission-Aware) Rate-Limited Tests
# ===========================================================================

run_open_loop() {
    # Runs a single rate-limited memtier test at a fixed ops/sec per connection.
    # rate = ops/sec per connection (total throughput = rate * threads * clients)
    # Source: CONTEXT.md Open-loop testing
    local port=$1 label=$2 rate=$3
    local outfile="$RESULTS_DIR/${label}_openloop_${rate}rps.txt"
    log "  Open-loop ($label): rate-limiting=${rate} ops/sec/conn"
    memtier_benchmark -s "$SERVER_HOST" -p "$port" \
        -t "$THREADS" -c 50 \
        --ratio=1:10 \
        --key-pattern=Z:Z \
        --key-maximum="$KEY_MAX" \
        -d "$DATA_SIZE" \
        --test-time="$TEST_TIME" \
        --rate-limiting="$rate" \
        --print-percentiles 50,90,95,99,99.9,99.99 \
        --hdr-file-prefix="$RESULTS_DIR/${label}_openloop_${rate}rps_hdr" \
        > "$outfile" 2>&1
    local ops p99
    ops=$(extract_ops_sec "$outfile")
    p99=$(extract_p99 "$outfile")
    log "  Open-loop result: ${ops} ops/sec, p99=${p99}ms at rate=${rate}/conn"
    echo "${label},${rate},${ops},${p99}" >> "$RESULTS_DIR/open_loop_results.csv"
}

run_open_loop_suite() {
    log "=== Open-loop rate-limited suite (coordinated-omission-aware) ==="
    # Coordinated omission: use closed-loop throughput as peak denominator.
    # Then test at 10/25/50/75/90% of peak to produce the throughput-latency curve.
    # WARNING: if rate > peak throughput, memtier reverts to closed-loop behavior.
    # Source: 23-RESEARCH.md Pitfall 4

    echo "label,rate_per_conn,actual_ops_sec,p99_ms" > "$RESULTS_DIR/open_loop_results.csv"

    # Determine peak ops/sec from prior closed-loop results (if available)
    # Falls back to a conservative default if no prior run exists
    local peak_ops=0
    local closed_loop_file
    # Look for the highest-throughput closed-loop result (50 clients, pipeline 1)
    closed_loop_file=$(ls "$RESULTS_DIR"/rust-redis_c50_p1.txt 2>/dev/null | head -1 || true)
    if [[ -n "$closed_loop_file" ]] && [[ -f "$closed_loop_file" ]]; then
        peak_ops=$(extract_ops_sec "$closed_loop_file" | grep -E '^[0-9]+' || echo 0)
    fi

    # If no prior results, use a conservative default for rate calculation
    # On dev hardware (12-core Mac): ~130K ops/sec typical
    # On 64-core Linux target: ~5M ops/sec
    if [[ "$peak_ops" -eq 0 ]] || [[ "$peak_ops" == "N/A" ]]; then
        log "  No closed-loop results found. Using conservative peak estimate of 100000 ops/sec."
        log "  Run full suite first for accurate open-loop targets."
        peak_ops=100000
    fi

    log "  Using peak ops/sec = ${peak_ops} as denominator"

    # Calculate per-connection rates (rate-limiting applies per connection)
    # Total connections = THREADS * 50 clients
    local total_conns=$(( THREADS * 50 ))
    local rate_10pct=$(( peak_ops / 10 / total_conns + 1 ))
    local rate_25pct=$(( peak_ops / 4  / total_conns + 1 ))
    local rate_50pct=$(( peak_ops / 2  / total_conns + 1 ))
    local rate_75pct=$(( peak_ops * 3 / 4 / total_conns + 1 ))
    local rate_90pct=$(( peak_ops * 9 / 10 / total_conns + 1 ))

    log "  Per-connection rates: 10%=${rate_10pct} 25%=${rate_25pct} 50%=${rate_50pct} 75%=${rate_75pct} 90%=${rate_90pct}"

    # Smoke test: only run the 50% rate point to validate script correctness
    if [[ "$SMOKE_TEST" == "true" ]]; then
        log "  SMOKE TEST: running only 50% rate point"
        if [[ "$RUN_RUST" == "true" ]]; then
            start_rust_server "$PORT_RUST"
            run_prepopulate "$PORT_RUST" "rust-redis-openloop"
            run_open_loop "$PORT_RUST" "rust-redis" "$rate_50pct"
            stop_rust_server
        fi
        return 0
    fi

    # Full suite: all 5 rate points
    if [[ "$RUN_RUST" == "true" ]]; then
        start_rust_server "$PORT_RUST"
        run_prepopulate "$PORT_RUST" "rust-redis"
        for rate in "$rate_10pct" "$rate_25pct" "$rate_50pct" "$rate_75pct" "$rate_90pct"; do
            run_open_loop "$PORT_RUST" "rust-redis" "$rate"
        done
        stop_rust_server
    fi

    if [[ "$RUN_REDIS" == "true" ]] && command -v redis-server &>/dev/null; then
        start_redis_server "$PORT_REDIS"
        run_prepopulate "$PORT_REDIS" "redis-7x"
        for rate in "$rate_10pct" "$rate_25pct" "$rate_50pct" "$rate_75pct" "$rate_90pct"; do
            run_open_loop "$PORT_REDIS" "redis-7x" "$rate"
        done
        stop_redis_server
    fi
}

# Generate BENCHMARK-v2.md from raw result files.
# Includes methodology, throughput tables, latency tables, memory comparison.
generate_report() { log "Report generation: not yet implemented (Plan 03)"; }

# ===========================================================================
# Main
# ===========================================================================

main() {
    parse_args "$@"
    RESULTS_DIR=$(mktemp -d "/tmp/bench-v2-XXXXXX")
    log "Results directory: $RESULTS_DIR"
    log "Configuration: KEY_MAX=$KEY_MAX TEST_TIME=${TEST_TIME}s WARMUP=${WARMUP_TIME}s RUN_COUNT=$RUN_COUNT"
    [[ "$SMOKE_TEST" == "true" ]] && log "SMOKE TEST MODE: reduced parameters"

    check_memtier
    check_redis_cli
    [[ "$SERVER_HOST" == "127.0.0.1" ]] && check_rust_binary

    # If --server-host is a remote host, skip starting local servers
    local local_mode=true
    [[ "$SERVER_HOST" != "127.0.0.1" ]] && local_mode=false

    # --- Primary throughput suite ---
    if [[ "$RUN_THROUGHPUT" == "true" ]]; then

        # --- rust-redis throughput suite ---
        if [[ "$RUN_RUST" == "true" ]]; then
            [[ "$local_mode" == "true" ]] && start_rust_server "$PORT_RUST"
            run_throughput_suite "$PORT_RUST" "rust-redis"
            # FLUSHALL between suite runs to reset key state
            redis-cli -h "$SERVER_HOST" -p "$PORT_RUST" FLUSHALL &>/dev/null || true
            [[ "$local_mode" == "true" ]] && stop_rust_server
        fi

        # --- Redis 7.x throughput suite ---
        if [[ "$RUN_REDIS" == "true" ]] && command -v redis-server &>/dev/null; then
            start_redis_server "$PORT_REDIS"
            run_throughput_suite "$PORT_REDIS" "redis-7x"
            stop_redis_server
        elif [[ "$RUN_REDIS" == "true" ]]; then
            log "WARNING: redis-server not found. Skipping Redis 7.x comparison."
            log "Install: brew install redis (macOS) | apt-get install redis-server (Linux)"
            RUN_REDIS=false
        fi

    fi

    # Memory, snapshot, open-loop sections -- implemented in Plan 02
    # Each section is called here and guarded by its flag:
    if [[ "$RUN_MEMORY" == "true" ]]; then
        run_memory_suite
    fi
    if [[ "$RUN_SNAPSHOT" == "true" ]]; then
        run_snapshot_suite
    fi
    if [[ "$RUN_OPEN_LOOP" == "true" ]]; then
        run_open_loop_suite
    fi

    # Generate BENCHMARK-v2.md -- implemented in Plan 03
    generate_report

    log "=== bench-v2.sh complete. Results: $RESULTS_DIR ==="
    log "=== Output file (when report is implemented): $OUTPUT_FILE ==="
}

main "$@"
