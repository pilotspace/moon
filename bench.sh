#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench.sh -- Comprehensive benchmark runner for rust-redis
#
# Runs redis-benchmark against rust-redis and optionally Redis 7.x,
# measuring throughput, latency percentiles, CPU, memory, and persistence
# overhead. Outputs formatted BENCHMARK.md with side-by-side comparison.
#
# Usage:
#   ./bench.sh                    # Benchmark both rust-redis and Redis 7.x
#   ./bench.sh --rust-only        # Skip Redis baseline comparison
#   ./bench.sh --redis-only       # Only benchmark Redis 7.x
#   ./bench.sh --output FILE      # Write results to FILE (default: BENCHMARK.md)
###############################################################################

# ===========================================================================
# Configuration
# ===========================================================================

PORT_RUST=6399
PORT_REDIS=6400
REQUESTS=100000
KEYSPACE=100000
TESTS="set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zrangebyscore,mset"
CLIENTS=(1 10 50)
PIPELINES=(1 16 64)
DATASIZES=(3 256 1024 4096)
RESULTS_DIR=""
OUTPUT_FILE="BENCHMARK.md"
RUST_BINARY="./target/release/rust-redis"

# Flags
RUN_RUST=true
RUN_REDIS=true

# PIDs for cleanup
RUST_PID=""
REDIS_PID=""

# ===========================================================================
# Helper Functions
# ===========================================================================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

cleanup() {
    log "Cleaning up..."
    if [[ -n "${RUST_PID:-}" ]] && kill -0 "$RUST_PID" 2>/dev/null; then
        kill "$RUST_PID" 2>/dev/null || true
        wait "$RUST_PID" 2>/dev/null || true
    fi
    if [[ -n "${REDIS_PID:-}" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
        kill "$REDIS_PID" 2>/dev/null || true
        wait "$REDIS_PID" 2>/dev/null || true
    fi
    if [[ -n "${RESULTS_DIR:-}" ]] && [[ -d "$RESULTS_DIR" ]]; then
        rm -rf "$RESULTS_DIR"
    fi
}
trap cleanup EXIT

check_command() {
    if ! command -v redis-benchmark &>/dev/null; then
        echo "ERROR: redis-benchmark is not installed." >&2
        echo "Install Redis tools:" >&2
        echo "  macOS:  brew install redis" >&2
        echo "  Ubuntu: sudo apt-get install redis-tools" >&2
        echo "  Fedora: sudo dnf install redis" >&2
        exit 1
    fi
}

check_rust_binary() {
    if [[ ! -x "$RUST_BINARY" ]]; then
        log "rust-redis binary not found at $RUST_BINARY"
        log "Building with: cargo build --release"
        cargo build --release
    fi
}

check_redis_server() {
    if ! command -v redis-server &>/dev/null; then
        log "WARNING: redis-server not found. Skipping Redis baseline."
        log "Install: brew install redis (macOS) or apt-get install redis-server (Linux)"
        RUN_REDIS=false
    fi
}

wait_for_server() {
    local port=$1
    local max_attempts=50
    local attempt=0
    while ! redis-cli -p "$port" PING &>/dev/null; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            log "ERROR: Server on port $port failed to start within 5 seconds"
            return 1
        fi
        sleep 0.1
    done
    log "Server on port $port is ready"
}

start_rust_server() {
    local port=$1
    shift
    local extra_args=("$@")
    log "Starting rust-redis on port $port ${extra_args[*]:-}"
    "$RUST_BINARY" --port "$port" "${extra_args[@]}" &
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
    shift
    local extra_args=("$@")
    log "Starting redis-server on port $port ${extra_args[*]:-}"
    redis-server --port "$port" --save "" --daemonize no "${extra_args[@]}" &
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

# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------

measure_memory() {
    # Returns RSS in KB for the given PID
    local pid=$1
    ps -o rss= -p "$pid" 2>/dev/null | tr -d ' '
}

measure_cpu() {
    # Captures CPU utilization percentage snapshot for a PID
    # Appends to $RESULTS_DIR/cpu_${label}.txt
    local pid=$1
    local label=$2
    local cpu
    cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null | tr -d ' ')
    echo "$cpu" >> "$RESULTS_DIR/cpu_${label}.txt"
}

# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

run_benchmark() {
    local port=$1
    local label=$2
    local clients=$3
    local pipeline=$4
    local datasize=$5

    local outfile="$RESULTS_DIR/result_${label}_c${clients}_p${pipeline}_d${datasize}.csv"

    log "  Benchmarking ${label}: clients=$clients pipeline=$pipeline datasize=$datasize"
    redis-benchmark \
        -p "$port" \
        -c "$clients" \
        -P "$pipeline" \
        -d "$datasize" \
        -n "$REQUESTS" \
        -r "$KEYSPACE" \
        -t "$TESTS" \
        --csv \
        > "$outfile" 2>/dev/null

    echo "$outfile"
}

# ---------------------------------------------------------------------------
# CSV to Markdown conversion
# ---------------------------------------------------------------------------

csv_to_markdown() {
    # Parse CSV files from redis-benchmark --csv output and produce markdown tables.
    # redis-benchmark --csv columns:
    #   "test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms",
    #   "p95_latency_ms","p99_latency_ms","p99.9_latency_ms","max_latency_ms"
    #
    # Arguments:
    #   $1 = label (e.g., "rust-redis" or "redis")
    #   $2 = results directory
    #   $3 = output file to append to
    local label=$1
    local results_dir=$2
    local outfile=$3

    # Collect unique commands from CSV files
    local commands
    commands=$(find "$results_dir" -name "result_${label}_*.csv" -exec tail -n +2 {} \; \
        | cut -d',' -f1 | tr -d '"' | sort -u)

    for cmd in $commands; do
        {
            echo ""
            echo "### ${cmd} -- ${label}"
            echo ""
            echo "| Clients | Pipeline | Data Size | ops/sec | p50 (ms) | p99 (ms) | p99.9 (ms) |"
            echo "|---------|----------|-----------|---------|----------|----------|------------|"
        } >> "$outfile"

        for csvfile in "$results_dir"/result_"${label}"_c*_p*_d*.csv; do
            [[ -f "$csvfile" ]] || continue
            # Extract params from filename: result_label_cN_pN_dN.csv
            local basename
            basename=$(basename "$csvfile" .csv)
            local c p d
            c=$(echo "$basename" | sed -E 's/.*_c([0-9]+)_.*/\1/')
            p=$(echo "$basename" | sed -E 's/.*_p([0-9]+)_.*/\1/')
            d=$(echo "$basename" | sed -E 's/.*_d([0-9]+).*/\1/')

            # Find the row for this command (skip header, match command name)
            local row
            row=$(grep -i "\"${cmd}\"" "$csvfile" 2>/dev/null | head -1) || true
            if [[ -z "$row" ]]; then
                continue
            fi

            # Parse CSV fields: "test","rps","avg","min","p50","p95","p99","p99.9","max"
            local rps p50 p99 p999
            rps=$(echo "$row" | cut -d',' -f2 | tr -d '"' | tr -d ' ')
            p50=$(echo "$row" | cut -d',' -f5 | tr -d '"' | tr -d ' ')
            p99=$(echo "$row" | cut -d',' -f7 | tr -d '"' | tr -d ' ')
            p999=$(echo "$row" | cut -d',' -f8 | tr -d '"' | tr -d ' ')

            echo "| $c | $p | ${d}B | $rps | $p50 | $p99 | $p999 |" >> "$outfile"
        done
    done
}

csv_to_comparison_markdown() {
    # Generate side-by-side comparison tables for rust-redis vs Redis 7.x
    # Arguments:
    #   $1 = results directory
    #   $2 = output file to append to
    local results_dir=$1
    local outfile=$2

    # Collect unique commands from rust-redis CSV files
    local commands
    commands=$(find "$results_dir" -name "result_rust_*.csv" -exec tail -n +2 {} \; \
        | cut -d',' -f1 | tr -d '"' | sort -u)

    for cmd in $commands; do
        {
            echo ""
            echo "### ${cmd} -- Comparison"
            echo ""
            echo "| Clients | Pipeline | Data Size | rust-redis (ops/sec) | p50 | p99 | p99.9 | Redis 7.x (ops/sec) | p50 | p99 | p99.9 | Ratio |"
            echo "|---------|----------|-----------|---------------------|-----|-----|-------|---------------------|-----|-----|-------|-------|"
        } >> "$outfile"

        for rustfile in "$results_dir"/result_rust_c*_p*_d*.csv; do
            [[ -f "$rustfile" ]] || continue
            local basename
            basename=$(basename "$rustfile" .csv)
            local c p d
            c=$(echo "$basename" | sed -E 's/.*_c([0-9]+)_.*/\1/')
            p=$(echo "$basename" | sed -E 's/.*_p([0-9]+)_.*/\1/')
            d=$(echo "$basename" | sed -E 's/.*_d([0-9]+).*/\1/')

            local redisfile="$results_dir/result_redis_c${c}_p${p}_d${d}.csv"

            # Rust row
            local rust_row
            rust_row=$(grep -i "\"${cmd}\"" "$rustfile" 2>/dev/null | head -1) || true
            if [[ -z "$rust_row" ]]; then
                continue
            fi
            local rust_rps rust_p50 rust_p99 rust_p999
            rust_rps=$(echo "$rust_row" | cut -d',' -f2 | tr -d '"' | tr -d ' ')
            rust_p50=$(echo "$rust_row" | cut -d',' -f5 | tr -d '"' | tr -d ' ')
            rust_p99=$(echo "$rust_row" | cut -d',' -f7 | tr -d '"' | tr -d ' ')
            rust_p999=$(echo "$rust_row" | cut -d',' -f8 | tr -d '"' | tr -d ' ')

            # Redis row (if comparison file exists)
            local redis_rps="N/A" redis_p50="N/A" redis_p99="N/A" redis_p999="N/A" ratio="N/A"
            if [[ -f "$redisfile" ]]; then
                local redis_row
                redis_row=$(grep -i "\"${cmd}\"" "$redisfile" 2>/dev/null | head -1) || true
                if [[ -n "$redis_row" ]]; then
                    redis_rps=$(echo "$redis_row" | cut -d',' -f2 | tr -d '"' | tr -d ' ')
                    redis_p50=$(echo "$redis_row" | cut -d',' -f5 | tr -d '"' | tr -d ' ')
                    redis_p99=$(echo "$redis_row" | cut -d',' -f7 | tr -d '"' | tr -d ' ')
                    redis_p999=$(echo "$redis_row" | cut -d',' -f8 | tr -d '"' | tr -d ' ')
                    # Calculate ratio (rust / redis), handle division by zero
                    if [[ "$redis_rps" != "0" ]] && [[ "$redis_rps" != "N/A" ]]; then
                        ratio=$(awk "BEGIN { printf \"%.2fx\", ${rust_rps} / ${redis_rps} }")
                    fi
                fi
            fi

            echo "| $c | $p | ${d}B | $rust_rps | $rust_p50 | $rust_p99 | $rust_p999 | $redis_rps | $redis_p50 | $redis_p99 | $redis_p999 | $ratio |" >> "$outfile"
        done
    done
}

# ===========================================================================
# Phase A: Throughput and Latency Benchmarks
# ===========================================================================

phase_a_throughput() {
    log "=== Phase A: Throughput and Latency Benchmarks ==="

    if [[ "$RUN_RUST" == true ]]; then
        check_rust_binary
        start_rust_server "$PORT_RUST"
        local server_pid="$RUST_PID"

        log "Benchmarking rust-redis..."
        for clients in "${CLIENTS[@]}"; do
            for pipeline in "${PIPELINES[@]}"; do
                for datasize in "${DATASIZES[@]}"; do
                    measure_cpu "$server_pid" "rust"
                    run_benchmark "$PORT_RUST" "rust" "$clients" "$pipeline" "$datasize"
                    measure_cpu "$server_pid" "rust"
                done
            done
        done

        stop_rust_server
    fi

    if [[ "$RUN_REDIS" == true ]]; then
        start_redis_server "$PORT_REDIS"
        local server_pid="$REDIS_PID"

        log "Benchmarking Redis 7.x..."
        for clients in "${CLIENTS[@]}"; do
            for pipeline in "${PIPELINES[@]}"; do
                for datasize in "${DATASIZES[@]}"; do
                    measure_cpu "$server_pid" "redis"
                    run_benchmark "$PORT_REDIS" "redis" "$clients" "$pipeline" "$datasize"
                    measure_cpu "$server_pid" "redis"
                done
            done
        done

        stop_redis_server
    fi
}

# ===========================================================================
# Phase B: Memory Benchmarks
# ===========================================================================

phase_b_memory() {
    log "=== Phase B: Memory Benchmarks ==="

    if [[ "$RUN_RUST" == true ]]; then
        check_rust_binary
        start_rust_server "$PORT_RUST"

        # Measure RSS at rest (after 2-second warmup)
        sleep 2
        local rust_rss_rest
        rust_rss_rest=$(measure_memory "$RUST_PID")
        log "rust-redis RSS at rest: ${rust_rss_rest} KB"

        # Load 100K keys
        log "Loading 100K keys into rust-redis..."
        redis-benchmark -p "$PORT_RUST" -n 100000 -r 100000 -t set -q >/dev/null 2>&1

        # Wait for RSS to stabilize
        sleep 1
        local rust_rss_loaded
        rust_rss_loaded=$(measure_memory "$RUST_PID")
        log "rust-redis RSS with 100K keys: ${rust_rss_loaded} KB"

        echo "rust-redis,$rust_rss_rest,$rust_rss_loaded" > "$RESULTS_DIR/memory_rust.csv"
        stop_rust_server
    fi

    if [[ "$RUN_REDIS" == true ]]; then
        start_redis_server "$PORT_REDIS"

        sleep 2
        local redis_rss_rest
        redis_rss_rest=$(measure_memory "$REDIS_PID")
        log "Redis 7.x RSS at rest: ${redis_rss_rest} KB"

        log "Loading 100K keys into Redis 7.x..."
        redis-benchmark -p "$PORT_REDIS" -n 100000 -r 100000 -t set -q >/dev/null 2>&1

        sleep 1
        local redis_rss_loaded
        redis_rss_loaded=$(measure_memory "$REDIS_PID")
        log "Redis 7.x RSS with 100K keys: ${redis_rss_loaded} KB"

        echo "redis,$redis_rss_rest,$redis_rss_loaded" > "$RESULTS_DIR/memory_redis.csv"
        stop_redis_server
    fi
}

# ===========================================================================
# Phase C: Persistence Overhead Benchmarks
# ===========================================================================

phase_c_persistence() {
    log "=== Phase C: Persistence Overhead Benchmarks ==="

    if [[ "$RUN_RUST" != true ]]; then
        log "Skipping persistence benchmarks (--redis-only mode)"
        return
    fi

    check_rust_binary

    local persist_modes=("none" "everysec" "always")
    local persist_args_none=()
    local persist_args_everysec=("--appendonly" "yes" "--appendfsync" "everysec")
    local persist_args_always=("--appendonly" "yes" "--appendfsync" "always")

    for mode in "${persist_modes[@]}"; do
        local tmpdir
        tmpdir=$(mktemp -d)

        local -a args=("--dir" "$tmpdir")
        case "$mode" in
            none)      ;;
            everysec)  args+=("${persist_args_everysec[@]}") ;;
            always)    args+=("${persist_args_always[@]}") ;;
        esac

        log "Persistence mode: $mode"
        start_rust_server "$PORT_RUST" "${args[@]}"

        redis-benchmark \
            -p "$PORT_RUST" \
            -c 50 \
            -n "$REQUESTS" \
            -t set,get \
            --csv \
            > "$RESULTS_DIR/persist_${mode}.csv" 2>/dev/null

        stop_rust_server

        # Clean up persistence temp dir
        rm -rf "$tmpdir"
    done
}

# ===========================================================================
# Phase D: Generate BENCHMARK.md
# ===========================================================================

get_hw_info() {
    local os
    os=$(uname -s)

    echo "**Date:** $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo "**OS:** $(uname -a)"

    if [[ "$os" == "Darwin" ]]; then
        local ncpu mem_bytes mem_gb
        ncpu=$(sysctl -n hw.ncpu 2>/dev/null || echo "unknown")
        mem_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
        mem_gb=$((mem_bytes / 1073741824))
        echo "**CPU cores:** $ncpu"
        echo "**Memory:** ${mem_gb} GB"
        echo "**CPU model:** $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'unknown')"
    else
        local ncpu mem_kb mem_gb
        ncpu=$(nproc 2>/dev/null || echo "unknown")
        mem_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "0")
        mem_gb=$((mem_kb / 1048576))
        echo "**CPU cores:** $ncpu"
        echo "**Memory:** ${mem_gb} GB"
        echo "**CPU model:** $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || echo 'unknown')"
    fi

    echo "**Requests per test:** $REQUESTS"
    echo "**Key space:** $KEYSPACE random keys"
}

generate_cpu_section() {
    local outfile=$1

    {
        echo ""
        echo "## CPU Utilization"
        echo ""
        echo "| Server | Benchmark Phase | Avg CPU % |"
        echo "|--------|-----------------|-----------|"
    } >> "$outfile"

    for label in rust redis; do
        local cpufile="$RESULTS_DIR/cpu_${label}.txt"
        if [[ -f "$cpufile" ]]; then
            local avg
            avg=$(awk '{ sum += $1; n++ } END { if (n > 0) printf "%.1f", sum/n; else print "N/A" }' "$cpufile")
            local display_label
            if [[ "$label" == "rust" ]]; then
                display_label="rust-redis"
            else
                display_label="Redis 7.x"
            fi
            echo "| $display_label | Throughput suite | $avg |" >> "$outfile"
        fi
    done
}

generate_memory_section() {
    local outfile=$1

    {
        echo ""
        echo "## Memory Usage (RSS)"
        echo ""
        echo "| Server | RSS at rest (KB) | RSS with 100K keys (KB) |"
        echo "|--------|------------------|------------------------|"
    } >> "$outfile"

    for label in rust redis; do
        local memfile="$RESULTS_DIR/memory_${label}.csv"
        if [[ -f "$memfile" ]]; then
            local display_label rest loaded
            if [[ "$label" == "rust" ]]; then
                display_label="rust-redis"
            else
                display_label="Redis 7.x"
            fi
            rest=$(cut -d',' -f2 "$memfile")
            loaded=$(cut -d',' -f3 "$memfile")
            echo "| $display_label | $rest | $loaded |" >> "$outfile"
        fi
    done
}

generate_persistence_section() {
    local outfile=$1

    {
        echo ""
        echo "## Persistence Overhead"
        echo ""
        echo "| Mode | SET ops/sec | GET ops/sec |"
        echo "|------|-------------|-------------|"
    } >> "$outfile"

    for mode in none everysec always; do
        local csvfile="$RESULTS_DIR/persist_${mode}.csv"
        if [[ -f "$csvfile" ]]; then
            local set_rps get_rps
            local set_row get_row
            set_row=$(grep -i '"SET"' "$csvfile" 2>/dev/null | head -1) || true
            get_row=$(grep -i '"GET"' "$csvfile" 2>/dev/null | head -1) || true

            set_rps="N/A"
            get_rps="N/A"
            if [[ -n "$set_row" ]]; then
                set_rps=$(echo "$set_row" | cut -d',' -f2 | tr -d '"' | tr -d ' ')
            fi
            if [[ -n "$get_row" ]]; then
                get_rps=$(echo "$get_row" | cut -d',' -f2 | tr -d '"' | tr -d ' ')
            fi

            local display_mode
            case "$mode" in
                none)      display_mode="No persistence" ;;
                everysec)  display_mode="AOF everysec" ;;
                always)    display_mode="AOF always" ;;
            esac

            echo "| $display_mode | $set_rps | $get_rps |" >> "$outfile"
        fi
    done
}

phase_d_generate_report() {
    log "=== Phase D: Generating $OUTPUT_FILE ==="

    {
        echo "# Benchmark Results"
        echo ""
        get_hw_info
        echo ""
        echo "---"
        echo ""
        echo "## Throughput and Latency"
    } > "$OUTPUT_FILE"

    # Generate throughput tables
    if [[ "$RUN_RUST" == true ]] && [[ "$RUN_REDIS" == true ]]; then
        csv_to_comparison_markdown "$RESULTS_DIR" "$OUTPUT_FILE"
    else
        if [[ "$RUN_RUST" == true ]]; then
            csv_to_markdown "rust" "$RESULTS_DIR" "$OUTPUT_FILE"
        fi
        if [[ "$RUN_REDIS" == true ]]; then
            csv_to_markdown "redis" "$RESULTS_DIR" "$OUTPUT_FILE"
        fi
    fi

    # CPU section
    generate_cpu_section "$OUTPUT_FILE"

    # Memory section
    generate_memory_section "$OUTPUT_FILE"

    # Persistence section
    generate_persistence_section "$OUTPUT_FILE"

    # Bottleneck analysis placeholder
    {
        echo ""
        echo "## Top 3 Bottlenecks"
        echo ""
        echo "_To be filled after analysis in Plan 03._"
        echo ""
        echo "1. **TBD** -- ..."
        echo "2. **TBD** -- ..."
        echo "3. **TBD** -- ..."
    } >> "$OUTPUT_FILE"

    log "Report written to $OUTPUT_FILE"
}

# ===========================================================================
# Argument Parsing
# ===========================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
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
            --output)
                OUTPUT_FILE="${2:-BENCHMARK.md}"
                shift 2
                ;;
            --port-rust)
                PORT_RUST="${2:-6399}"
                shift 2
                ;;
            --port-redis)
                PORT_REDIS="${2:-6400}"
                shift 2
                ;;
            --requests)
                REQUESTS="${2:-100000}"
                shift 2
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --rust-only        Only benchmark rust-redis (skip Redis baseline)"
                echo "  --redis-only       Only benchmark Redis 7.x"
                echo "  --output FILE      Output file (default: BENCHMARK.md)"
                echo "  --port-rust PORT   Port for rust-redis (default: 6399)"
                echo "  --port-redis PORT  Port for Redis 7.x (default: 6400)"
                echo "  --requests N       Requests per benchmark (default: 100000)"
                echo "  --help, -h         Show this help"
                exit 0
                ;;
            *)
                echo "Unknown option: $1" >&2
                echo "Use --help for usage information." >&2
                exit 1
                ;;
        esac
    done
}

# ===========================================================================
# Main
# ===========================================================================

main() {
    parse_args "$@"

    log "Starting benchmark suite"
    log "  rust-redis: $RUN_RUST (port $PORT_RUST)"
    log "  Redis 7.x:  $RUN_REDIS (port $PORT_REDIS)"
    log "  Output:      $OUTPUT_FILE"
    log "  Requests:    $REQUESTS"

    # Create temp results directory
    RESULTS_DIR=$(mktemp -d)
    log "Results directory: $RESULTS_DIR"

    # Check prerequisites
    check_command
    if [[ "$RUN_REDIS" == true ]]; then
        check_redis_server
    fi

    # Run benchmark phases
    phase_a_throughput
    phase_b_memory
    phase_c_persistence
    phase_d_generate_report

    log "Benchmark suite complete!"
    log "Results: $OUTPUT_FILE"
}

main "$@"
