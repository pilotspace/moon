#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# profile.sh -- Automated CPU & Memory Profiling Suite
#
# Measures per-key memory overhead, RSS growth, memory fragmentation, and CPU
# efficiency for rust-redis (Tokio & Monoio) vs Redis. Runs Criterion micro-
# benchmarks. Generates PROFILING-REPORT.md.
#
# Usage:
#   ./profile.sh                          # Run all sections
#   ./profile.sh --output FILE            # Custom output file
#   ./profile.sh --sizes "10000 50000"    # Custom dataset sizes
#   ./profile.sh --skip-monoio            # Skip monoio runtime
#   ./profile.sh --section NAME           # Run one section only
#                                         #   (memory|fragmentation|cpu|criterion)
###############################################################################

# ===========================================================================
# Configuration
# ===========================================================================

PORT_REDIS=6399
PORT_RUST_TOKIO=6400
PORT_RUST_MONOIO=6401
RUST_BINARY="./target/release/rust-redis"
RUST_BINARY_MONOIO="./target/release/rust-redis-monoio"
OUTPUT_FILE="PROFILING-REPORT.md"
SIZES=(10000 50000 100000 500000 1000000)
SKIP_MONOIO=false
SECTION_FILTER=""
VALUE_SIZE=256
RESULTS_DIR=""

REDIS_PID=""
TOKIO_PID=""
MONOIO_PID=""

# ===========================================================================
# Argument Parsing
# ===========================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)      OUTPUT_FILE="$2"; shift 2 ;;
        --sizes)       IFS=' ' read -ra SIZES <<< "$2"; shift 2 ;;
        --skip-monoio) SKIP_MONOIO=true; shift ;;
        --section)     SECTION_FILTER="$2"; shift 2 ;;
        --help|-h)
            head -16 "$0" | tail -10
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ===========================================================================
# Helpers
# ===========================================================================

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${TOKIO_PID:-}" ]]  && kill "$TOKIO_PID"  2>/dev/null; wait "$TOKIO_PID"  2>/dev/null || true
    [[ -n "${MONOIO_PID:-}" ]] && kill "$MONOIO_PID" 2>/dev/null; wait "$MONOIO_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]]  && kill "$REDIS_PID"  2>/dev/null; wait "$REDIS_PID"  2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "rust-redis.*${PORT_RUST_TOKIO}" 2>/dev/null || true
    pkill -f "rust-redis.*${PORT_RUST_MONOIO}" 2>/dev/null || true
    [[ -n "${RESULTS_DIR:-}" ]] && [[ -d "$RESULTS_DIR" ]] && rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

wait_for_server() {
    local port=$1 name=$2 retries=30
    while ! redis-cli -p "$port" PING &>/dev/null; do
        retries=$((retries - 1))
        [[ $retries -le 0 ]] && { log "ERROR: $name on port $port not responding"; return 1; }
        sleep 0.2
    done
    log "$name ready on port $port"
}

flush_server() {
    redis-cli -p "$1" FLUSHALL &>/dev/null || true
}

get_rss_kb() {
    local pid=$1
    [[ -z "$pid" ]] && echo "0" && return
    ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
}

get_used_memory() {
    local port=$1
    redis-cli -p "$port" INFO memory 2>/dev/null | tr '\r' '\n' | grep "^used_memory:" | cut -d: -f2 || echo "0"
}

get_cpu_pct() {
    local pid=$1
    [[ -z "$pid" ]] && echo "0" && return
    ps -o %cpu= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0"
}

format_number() {
    local n="${1%%.*}"
    [[ -z "$n" ]] && echo "0" && return
    printf "%'d" "$n" 2>/dev/null || echo "$n"
}

ratio() {
    local a="${1%%.*}" b="${2%%.*}"
    [[ -z "$a" ]] && a=0
    [[ -z "$b" ]] && b=0
    if [[ "$b" == "0" ]]; then echo "N/A"; return; fi
    awk "BEGIN { printf \"%.2fx\", $a / $b }"
}

parse_rps() {
    tr '\r' '\n' | grep "requests per second" | tail -1 | awk '{print $2}' | sed 's/,//g'
}

bytes_to_mb() {
    local b="${1%%.*}"
    [[ -z "$b" ]] && echo "0" && return
    awk "BEGIN { printf \"%.1f\", $b / 1048576 }"
}

kb_to_mb() {
    local kb="${1%%.*}"
    [[ -z "$kb" ]] && echo "0" && return
    awk "BEGIN { printf \"%.1f\", $kb / 1024 }"
}

# ===========================================================================
# Server Lifecycle
# ===========================================================================

start_redis() {
    log "Starting Redis on port $PORT_REDIS..."
    redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning &>/dev/null &
    REDIS_PID=$!
    wait_for_server "$PORT_REDIS" "Redis"
}

start_rust_tokio() {
    log "Starting rust-redis (Tokio) on port $PORT_RUST_TOKIO..."
    "$RUST_BINARY" --port "$PORT_RUST_TOKIO" --shards 1 &>/dev/null &
    TOKIO_PID=$!
    wait_for_server "$PORT_RUST_TOKIO" "rust-redis (Tokio)"
}

start_rust_monoio() {
    if [[ "$SKIP_MONOIO" == "true" ]]; then
        log "Skipping monoio (--skip-monoio)"
        return 1
    fi
    if [[ ! -f "$RUST_BINARY_MONOIO" ]]; then
        log "Monoio binary not found at $RUST_BINARY_MONOIO, skipping"
        return 1
    fi
    log "Starting rust-redis (Monoio) on port $PORT_RUST_MONOIO..."
    "$RUST_BINARY_MONOIO" --port "$PORT_RUST_MONOIO" --shards 1 &>/dev/null &
    MONOIO_PID=$!
    wait_for_server "$PORT_RUST_MONOIO" "rust-redis (Monoio)"
}

stop_server() {
    local pid_var=$1
    local pid="${!pid_var}"
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
        eval "$pid_var=''"
    fi
}

stop_all() {
    stop_server REDIS_PID
    stop_server TOKIO_PID
    stop_server MONOIO_PID
    sleep 0.5
}

# ===========================================================================
# Build
# ===========================================================================

build_binaries() {
    log "Building rust-redis (Tokio, release)..."
    cargo build --release --features runtime-tokio 2>&1 | tail -3

    if [[ "$SKIP_MONOIO" != "true" ]]; then
        log "Building rust-redis (Monoio, release)..."
        if cargo build --release --features runtime-monoio --no-default-features 2>&1 | tail -3; then
            cp "$RUST_BINARY" "$RUST_BINARY_MONOIO"
        else
            log "WARNING: Monoio build failed, skipping monoio tests"
            SKIP_MONOIO=true
        fi
        # Rebuild Tokio binary (since release binary was overwritten)
        cargo build --release --features runtime-tokio 2>&1 | tail -3
    fi
}

# ===========================================================================
# Measurement: per-server loop helper
# ===========================================================================

# measure_per_server callback_fn
# Calls callback_fn with args: server_label port pid
# for Redis and rust-redis (Tokio).
measure_per_server() {
    local callback=$1
    # Redis
    "$callback" "Redis" "$PORT_REDIS" "$REDIS_PID"
    # rust-redis (Tokio)
    "$callback" "rust-redis" "$PORT_RUST_TOKIO" "$TOKIO_PID"
}

# ===========================================================================
# Section: Per-Key Memory Overhead
# ===========================================================================

_measure_memory_for_server() {
    local server_label=$1 port=$2 pid=$3
    local count=$_CURRENT_COUNT dtype=$_CURRENT_DTYPE

    flush_server "$port"
    sleep 0.3

    # Baseline measurements
    local baseline_rss
    baseline_rss=$(get_rss_kb "$pid")
    local baseline_mem
    baseline_mem=$(get_used_memory "$port")

    # Insert keys
    case "$dtype" in
        string)
            redis-benchmark -p "$port" -t set -n "$count" -r "$count" -d "$VALUE_SIZE" -q 2>/dev/null | tail -1 >&2
            ;;
        hash)
            (
                set +eo pipefail
                for ((i=0; i<count; i++)); do
                    printf "HSET key:%d f1 %0${VALUE_SIZE}d f2 %0${VALUE_SIZE}d f3 val3 f4 val4 f5 val5\r\n" "$i" "$i" "$i"
                done
            ) | redis-cli -p "$port" --pipe 2>/dev/null | tail -1 >&2 || true
            ;;
        list)
            (
                set +eo pipefail
                for ((i=0; i<count; i++)); do
                    printf "LPUSH list:%d e1 e2 e3 e4 e5 e6 e7 e8 e9 e10\r\n" "$i"
                done
            ) | redis-cli -p "$port" --pipe 2>/dev/null | tail -1 >&2 || true
            ;;
        zset)
            (
                set +eo pipefail
                for ((i=0; i<count; i++)); do
                    printf "ZADD zset:%d 1 m1 2 m2 3 m3 4 m4 5 m5 6 m6 7 m7 8 m8 9 m9 10 m10\r\n" "$i"
                done
            ) | redis-cli -p "$port" --pipe 2>/dev/null | tail -1 >&2 || true
            ;;
    esac

    sleep 0.3

    # Final measurements
    local final_rss
    final_rss=$(get_rss_kb "$pid")
    local final_mem
    final_mem=$(get_used_memory "$port")

    # Compute per-key overhead
    local rss_delta_kb=$(( final_rss - baseline_rss ))
    local mem_delta=$(( final_mem - baseline_mem ))
    local rss_per_key=0
    local mem_per_key=0
    if [[ $count -gt 0 ]]; then
        rss_per_key=$(awk "BEGIN { printf \"%.1f\", ($rss_delta_kb * 1024.0) / $count }")
        mem_per_key=$(awk "BEGIN { printf \"%.1f\", $mem_delta / $count }")
    fi

    echo "$count,$dtype,$server_label,$rss_per_key,$mem_per_key" >> "$_MEMORY_FILE"
    log "  $server_label $dtype@${count}: RSS delta=${rss_delta_kb}KB, per_key=${rss_per_key}B (RSS) / ${mem_per_key}B (used_memory)"
}

section_memory() {
    log "=== Section: Per-Key Memory Overhead ==="

    RESULTS_DIR="${RESULTS_DIR:-$(mktemp -d)}"
    _MEMORY_FILE="$RESULTS_DIR/memory_results.csv"
    echo "size,type,server,rss_bytes_per_key,used_memory_bytes_per_key" > "$_MEMORY_FILE"

    start_redis
    start_rust_tokio

    for count in "${SIZES[@]}"; do
        for dtype in string hash list zset; do
            log "Measuring $dtype at $count keys..."
            _CURRENT_COUNT=$count
            _CURRENT_DTYPE=$dtype
            measure_per_server _measure_memory_for_server
        done
    done

    stop_all

    MEMORY_RESULTS_FILE="$_MEMORY_FILE"
}

# ===========================================================================
# Section: Memory Fragmentation
# ===========================================================================

_measure_fragmentation_for_server() {
    local server_label=$1 port=$2 pid=$3
    local frag_count=1000000
    local del_count=500000

    flush_server "$port"
    sleep 0.3

    # Insert 1M string keys
    log "Inserting ${frag_count} keys into $server_label..."
    redis-benchmark -p "$port" -t set -n "$frag_count" -r "$frag_count" -d "$VALUE_SIZE" -q 2>/dev/null | tail -1 >&2
    sleep 0.5

    local rss_after_insert
    rss_after_insert=$(get_rss_kb "$pid")
    local mem_after_insert
    mem_after_insert=$(get_used_memory "$port")
    local frag_after_insert
    if [[ "$mem_after_insert" -gt 0 ]]; then
        frag_after_insert=$(awk "BEGIN { printf \"%.2f\", ($rss_after_insert * 1024.0) / $mem_after_insert }")
    else
        frag_after_insert="N/A"
    fi

    echo "after_insert,$server_label,$rss_after_insert,$mem_after_insert,$frag_after_insert" >> "$_FRAG_FILE"
    log "  $server_label after insert: RSS=$(kb_to_mb "$rss_after_insert")MB, used_memory=$(bytes_to_mb "$mem_after_insert")MB, frag=$frag_after_insert"

    # Delete 500K random keys
    log "Deleting ${del_count} keys from $server_label..."
    (
        set +eo pipefail
        for ((i=0; i<del_count; i++)); do
            printf "DEL key:%012d\r\n" "$((RANDOM * RANDOM % frag_count))"
        done
    ) | redis-cli -p "$port" --pipe 2>/dev/null | tail -1 >&2 || true
    sleep 0.5

    local rss_after_delete
    rss_after_delete=$(get_rss_kb "$pid")
    local mem_after_delete
    mem_after_delete=$(get_used_memory "$port")
    local frag_after_delete
    if [[ "$mem_after_delete" -gt 0 ]]; then
        frag_after_delete=$(awk "BEGIN { printf \"%.2f\", ($rss_after_delete * 1024.0) / $mem_after_delete }")
    else
        frag_after_delete="N/A"
    fi

    echo "after_delete,$server_label,$rss_after_delete,$mem_after_delete,$frag_after_delete" >> "$_FRAG_FILE"
    log "  $server_label after delete: RSS=$(kb_to_mb "$rss_after_delete")MB, used_memory=$(bytes_to_mb "$mem_after_delete")MB, frag=$frag_after_delete"
}

section_fragmentation() {
    log "=== Section: Memory Fragmentation ==="

    RESULTS_DIR="${RESULTS_DIR:-$(mktemp -d)}"
    _FRAG_FILE="$RESULTS_DIR/fragmentation.csv"
    echo "phase,server,rss_kb,used_memory,frag_ratio" > "$_FRAG_FILE"

    start_redis
    start_rust_tokio

    measure_per_server _measure_fragmentation_for_server

    stop_all

    FRAGMENTATION_RESULTS_FILE="$_FRAG_FILE"
}

# ===========================================================================
# Section: CPU Efficiency
# ===========================================================================

_measure_cpu_for_server() {
    local server_label=$1 port=$2 pid=$3
    local bench_requests=200000
    local bench_clients=50

    flush_server "$port"
    sleep 0.3

    # Background CPU sampler
    local cpu_samples_file="$RESULTS_DIR/cpu_samples_${server_label}.txt"
    > "$cpu_samples_file"
    (
        while true; do
            get_cpu_pct "$pid" >> "$cpu_samples_file"
            sleep 1
        done
    ) &
    local sampler_pid=$!

    # Run mixed workload
    log "Benchmarking CPU efficiency for $server_label..."
    local bench_output
    bench_output=$(redis-benchmark -p "$port" -c "$bench_clients" -n "$bench_requests" \
        -t set,get -r 100000 -d "$VALUE_SIZE" -q 2>/dev/null)

    # Stop sampler
    kill "$sampler_pid" 2>/dev/null; wait "$sampler_pid" 2>/dev/null || true

    # Parse ops/sec (sum of SET and GET) — look for "requests per second" summary lines
    local set_rps get_rps
    set_rps=$(echo "$bench_output" | tr '\r' '\n' | grep "SET:.*requests per second" | awk '{print $2}' | sed 's/,//g' | head -1)
    get_rps=$(echo "$bench_output" | tr '\r' '\n' | grep "GET:.*requests per second" | awk '{print $2}' | sed 's/,//g' | head -1)
    [[ -z "$set_rps" ]] && set_rps=0
    [[ -z "$get_rps" ]] && get_rps=0
    local total_rps
    total_rps=$(awk "BEGIN { printf \"%.0f\", $set_rps + $get_rps }")

    # Average CPU%
    local avg_cpu
    avg_cpu=$(awk '{ sum += $1; n++ } END { if (n>0) printf "%.1f", sum/n; else print "0" }' "$cpu_samples_file")

    # Efficiency: ops/sec per CPU%
    local efficiency
    if [[ "$avg_cpu" != "0" ]] && [[ "$avg_cpu" != "0.0" ]]; then
        efficiency=$(awk "BEGIN { printf \"%.0f\", $total_rps / $avg_cpu }")
    else
        efficiency="N/A"
    fi

    echo "$server_label,$total_rps,$avg_cpu,$efficiency" >> "$_CPU_FILE"
    log "  $server_label: ${total_rps} ops/sec, ${avg_cpu}% CPU, efficiency=$efficiency ops/sec/%CPU"
}

section_cpu() {
    log "=== Section: CPU Efficiency ==="

    RESULTS_DIR="${RESULTS_DIR:-$(mktemp -d)}"
    _CPU_FILE="$RESULTS_DIR/cpu_results.csv"
    echo "server,ops_per_sec,avg_cpu_pct,efficiency" > "$_CPU_FILE"

    # Redis
    start_redis
    _measure_cpu_for_server "Redis" "$PORT_REDIS" "$REDIS_PID"
    stop_all

    # rust-redis (Tokio)
    start_rust_tokio
    _measure_cpu_for_server "rust-redis-Tokio" "$PORT_RUST_TOKIO" "$TOKIO_PID"
    stop_all

    # Monoio (optional)
    if [[ "$SKIP_MONOIO" != "true" ]]; then
        if start_rust_monoio; then
            _measure_cpu_for_server "rust-redis-Monoio" "$PORT_RUST_MONOIO" "$MONOIO_PID"
            stop_all
        fi
    fi

    CPU_RESULTS_FILE="$_CPU_FILE"
}

# ===========================================================================
# Section: Criterion Benchmarks
# ===========================================================================

section_criterion() {
    log "=== Section: Criterion Benchmarks ==="

    RESULTS_DIR="${RESULTS_DIR:-$(mktemp -d)}"
    local criterion_file="$RESULTS_DIR/criterion_output.txt"
    > "$criterion_file"

    for bench_name in compact_key entry_memory get_hotpath; do
        log "Running cargo bench --bench $bench_name ..."
        echo "### $bench_name" >> "$criterion_file"
        echo '```' >> "$criterion_file"
        if cargo bench --bench "$bench_name" 2>&1 | grep -E "^[a-zA-Z0-9_/]|time:" >> "$criterion_file"; then
            :
        else
            echo "(bench $bench_name failed or produced no output)" >> "$criterion_file"
        fi
        echo '```' >> "$criterion_file"
        echo "" >> "$criterion_file"
    done

    CRITERION_RESULTS_FILE="$criterion_file"
}

# ===========================================================================
# Report Generation
# ===========================================================================

generate_report() {
    log "Generating $OUTPUT_FILE ..."

    local hw_ncpu hw_memsize os_version
    hw_ncpu=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo "unknown")
    hw_memsize=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
    local hw_mem_gb
    hw_mem_gb=$(awk "BEGIN { printf \"%.0f\", $hw_memsize / 1073741824 }")
    os_version=$(sw_vers -productVersion 2>/dev/null || uname -r 2>/dev/null || echo "unknown")

    cat > "$OUTPUT_FILE" <<HEADER
# Profiling Report

**Generated:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Hardware:** ${hw_ncpu} cores, ${hw_mem_gb}GB RAM
**OS:** macOS ${os_version}
**rust-redis version:** $(git describe --tags --always 2>/dev/null || echo "dev")
**Redis version:** $(redis-server --version 2>/dev/null | awk '{print $3}' | sed 's/v=//' || echo "unknown")

---

HEADER

    # -- Per-Key Memory Overhead --
    if [[ -n "${MEMORY_RESULTS_FILE:-}" ]] && [[ -f "$MEMORY_RESULTS_FILE" ]]; then
        cat >> "$OUTPUT_FILE" <<'SECTION'
## Per-Key Memory Overhead

Measures RSS growth per key inserted, comparing Redis vs rust-redis across data types and dataset sizes.

SECTION

        echo "| Keys | Type | Redis (B/key) | rust-redis (B/key) | Ratio |" >> "$OUTPUT_FILE"
        echo "|-----:|------|-------------:|-----------------:|------:|" >> "$OUTPUT_FILE"

        # Parse CSV results: each type has Redis line followed by rust-redis line
        while IFS=, read -r size dtype server rss_per_key mem_per_key; do
            [[ "$size" == "size" ]] && continue  # skip header
            if [[ "$server" == "Redis" ]]; then
                local redis_val="$rss_per_key"
                # Read next line for rust-redis
                IFS=, read -r _ _ server2 rust_val _ || true
                if [[ "$server2" == "rust-redis" ]]; then
                    local r
                    r=$(ratio "${rust_val%%.*}" "${redis_val%%.*}")
                    printf "| %s | %s | %s | %s | %s |\n" \
                        "$(format_number "$size")" "$dtype" "$redis_val" "$rust_val" "$r" >> "$OUTPUT_FILE"
                fi
            fi
        done < "$MEMORY_RESULTS_FILE"

        echo "" >> "$OUTPUT_FILE"

        # RSS Growth Curve
        echo "### RSS Growth Curve (Strings, ${VALUE_SIZE}B values)" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "| Keys | Redis RSS (MB) | rust-redis RSS (MB) | Ratio |" >> "$OUTPUT_FILE"
        echo "|-----:|--------------:|-------------------:|------:|" >> "$OUTPUT_FILE"

        grep ",string," "$MEMORY_RESULTS_FILE" | while IFS=, read -r size dtype server rss_per_key mem_per_key; do
            if [[ "$server" == "Redis" ]]; then
                local redis_total
                redis_total=$(awk "BEGIN { printf \"%.1f\", ($rss_per_key * $size) / 1048576 }")
                IFS=, read -r _ _ _ rust_rss_per_key _ || true
                local rust_total
                rust_total=$(awk "BEGIN { printf \"%.1f\", ($rust_rss_per_key * $size) / 1048576 }")
                local r
                r=$(ratio "${rust_total%%.*}" "${redis_total%%.*}")
                printf "| %s | %s | %s | %s |\n" \
                    "$(format_number "$size")" "$redis_total" "$rust_total" "$r" >> "$OUTPUT_FILE"
            fi
        done

        echo "" >> "$OUTPUT_FILE"
    fi

    # -- Memory Fragmentation --
    if [[ -n "${FRAGMENTATION_RESULTS_FILE:-}" ]] && [[ -f "$FRAGMENTATION_RESULTS_FILE" ]]; then
        cat >> "$OUTPUT_FILE" <<'SECTION'
## Memory Fragmentation

Insert 1M keys, delete 500K, measure RSS vs used_memory ratio.

SECTION

        echo "| Phase | Metric | Redis | rust-redis |" >> "$OUTPUT_FILE"
        echo "|-------|--------|------:|-----------:|" >> "$OUTPUT_FILE"

        local redis_insert_rss="" redis_insert_mem="" redis_insert_frag=""
        local rust_insert_rss="" rust_insert_mem="" rust_insert_frag=""
        local redis_delete_rss="" redis_delete_mem="" redis_delete_frag=""
        local rust_delete_rss="" rust_delete_mem="" rust_delete_frag=""

        while IFS=, read -r phase server rss mem frag; do
            [[ "$phase" == "phase" ]] && continue
            case "${phase}_${server}" in
                after_insert_Redis)        redis_insert_rss=$rss; redis_insert_mem=$mem; redis_insert_frag=$frag ;;
                after_insert_rust-redis)   rust_insert_rss=$rss;  rust_insert_mem=$mem;  rust_insert_frag=$frag ;;
                after_delete_Redis)        redis_delete_rss=$rss; redis_delete_mem=$mem; redis_delete_frag=$frag ;;
                after_delete_rust-redis)   rust_delete_rss=$rss;  rust_delete_mem=$mem;  rust_delete_frag=$frag ;;
            esac
        done < "$FRAGMENTATION_RESULTS_FILE"

        printf "| After Insert (1M) | RSS (MB) | %s | %s |\n" \
            "$(kb_to_mb "$redis_insert_rss")" "$(kb_to_mb "$rust_insert_rss")" >> "$OUTPUT_FILE"
        printf "| After Insert (1M) | used_memory (MB) | %s | %s |\n" \
            "$(bytes_to_mb "$redis_insert_mem")" "$(bytes_to_mb "$rust_insert_mem")" >> "$OUTPUT_FILE"
        printf "| After Insert (1M) | Fragmentation | %s | %s |\n" \
            "$redis_insert_frag" "$rust_insert_frag" >> "$OUTPUT_FILE"
        printf "| After Delete (500K) | RSS (MB) | %s | %s |\n" \
            "$(kb_to_mb "$redis_delete_rss")" "$(kb_to_mb "$rust_delete_rss")" >> "$OUTPUT_FILE"
        printf "| After Delete (500K) | used_memory (MB) | %s | %s |\n" \
            "$(bytes_to_mb "$redis_delete_mem")" "$(bytes_to_mb "$rust_delete_mem")" >> "$OUTPUT_FILE"
        printf "| After Delete (500K) | Fragmentation | %s | %s |\n" \
            "$redis_delete_frag" "$rust_delete_frag" >> "$OUTPUT_FILE"

        echo "" >> "$OUTPUT_FILE"
    fi

    # -- CPU Efficiency --
    if [[ -n "${CPU_RESULTS_FILE:-}" ]] && [[ -f "$CPU_RESULTS_FILE" ]]; then
        cat >> "$OUTPUT_FILE" <<'SECTION'
## CPU Efficiency

Mixed SET+GET workload: 200K requests, 50 clients, 256B values.

SECTION

        echo "| Server | ops/sec | Avg CPU% | ops/sec per CPU% |" >> "$OUTPUT_FILE"
        echo "|--------|-------:|--------:|----------------:|" >> "$OUTPUT_FILE"

        while IFS=, read -r server ops cpu eff; do
            [[ "$server" == "server" ]] && continue
            printf "| %s | %s | %s | %s |\n" \
                "$server" "$(format_number "$ops")" "${cpu}%" "$(format_number "$eff")" >> "$OUTPUT_FILE"
        done < "$CPU_RESULTS_FILE"

        echo "" >> "$OUTPUT_FILE"
    fi

    # -- Criterion Benchmarks --
    if [[ -n "${CRITERION_RESULTS_FILE:-}" ]] && [[ -f "$CRITERION_RESULTS_FILE" ]]; then
        echo "## Criterion Micro-Benchmarks" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        cat "$CRITERION_RESULTS_FILE" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    fi

    # -- Optimization Recommendations --
    cat >> "$OUTPUT_FILE" <<'SECTION'
## Optimization Recommendations

Based on profiling results:

1. **Per-key overhead:** Compare RSS/key between Redis and rust-redis. If rust-redis uses more, investigate CompactEntry/CompactKey overhead vs jemalloc metadata.
2. **Fragmentation:** If fragmentation ratio > 1.5 after deletes, consider implementing active defragmentation or memory compaction.
3. **CPU efficiency:** Higher ops/sec per CPU% indicates better instruction-level efficiency. Monoio should show improvement over Tokio due to reduced async runtime overhead.
4. **Inline key threshold:** CompactKey SSO threshold is 23 bytes. If most keys exceed this, consider increasing inline capacity (trade struct size for fewer allocations).

SECTION

    log "Report written to $OUTPUT_FILE ($(wc -l < "$OUTPUT_FILE") lines)"
}

# ===========================================================================
# Main
# ===========================================================================

main() {
    log "=== Profile.sh: CPU & Memory Profiling Suite ==="
    log "Output: $OUTPUT_FILE"
    log "Sizes: ${SIZES[*]}"
    log "Skip monoio: $SKIP_MONOIO"

    RESULTS_DIR=$(mktemp -d)

    # Initialize result file variables
    MEMORY_RESULTS_FILE=""
    FRAGMENTATION_RESULTS_FILE=""
    CPU_RESULTS_FILE=""
    CRITERION_RESULTS_FILE=""

    # Build if needed
    if [[ ! -f "$RUST_BINARY" ]]; then
        build_binaries
    fi

    # Run sections
    if [[ -z "$SECTION_FILTER" ]] || [[ "$SECTION_FILTER" == "memory" ]]; then
        section_memory
    fi

    if [[ -z "$SECTION_FILTER" ]] || [[ "$SECTION_FILTER" == "fragmentation" ]]; then
        section_fragmentation
    fi

    if [[ -z "$SECTION_FILTER" ]] || [[ "$SECTION_FILTER" == "cpu" ]]; then
        section_cpu
    fi

    if [[ -z "$SECTION_FILTER" ]] || [[ "$SECTION_FILTER" == "criterion" ]]; then
        section_criterion
    fi

    # Generate report
    generate_report

    log "=== Profiling complete ==="
}

main
