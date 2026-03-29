#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-scaling.sh -- Multi-shard scaling benchmark with statistical runs
#
# Tests Moon vs Redis across all shard/client/pipeline/command combinations.
# Each configuration runs N times (default 3) for statistical significance.
# Outputs structured markdown tables with RPS, p50, p99, ratio, and scaling
# efficiency metrics.
#
# Usage:
#   ./scripts/bench-scaling.sh                     # Full run (local servers)
#   ./scripts/bench-scaling.sh --dry-run            # Print test matrix only
#   ./scripts/bench-scaling.sh --host 10.0.0.5      # Remote server mode
#   ./scripts/bench-scaling.sh --skip-redis          # Moon-only mode
#   ./scripts/bench-scaling.sh --aof                 # Enable AOF testing
#   ./scripts/bench-scaling.sh --shards-list "1 4"   # Custom shard counts
#   ./scripts/bench-scaling.sh --runs 5              # 5 statistical runs
###############################################################################

# ===========================================================================
# Configuration Defaults
# ===========================================================================

HOST="127.0.0.1"
MOON_PORT=6400
REDIS_PORT=6399
RUNS=3
REQUESTS=100000
OUTPUT_FILE="BENCHMARK-SCALING.md"
SHARDS_LIST="1 2 4 8 12"
CLIENTS_LIST="50 200 500"
PIPELINE_LIST="1 8 16 64"
COMMANDS="GET SET MGET"
RUST_BINARY="./target/release/moon"
SKIP_REDIS=false
AOF_MODE=false
DRY_RUN=false

MOON_PID=""
REDIS_PID=""
RESULTS_DIR=""

# ===========================================================================
# CLI Parsing
# ===========================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            [[ -z "${2:-}" ]] && { echo "Error: --host requires a value"; exit 1; }
            HOST="$2"; shift 2 ;;
        --moon-port)
            [[ -z "${2:-}" ]] && { echo "Error: --moon-port requires a value"; exit 1; }
            MOON_PORT="$2"; shift 2 ;;
        --redis-port)
            [[ -z "${2:-}" ]] && { echo "Error: --redis-port requires a value"; exit 1; }
            REDIS_PORT="$2"; shift 2 ;;
        --runs)
            [[ -z "${2:-}" ]] && { echo "Error: --runs requires a value"; exit 1; }
            RUNS="$2"; shift 2 ;;
        --requests)
            [[ -z "${2:-}" ]] && { echo "Error: --requests requires a value"; exit 1; }
            REQUESTS="$2"; shift 2 ;;
        --output)
            [[ -z "${2:-}" ]] && { echo "Error: --output requires a value"; exit 1; }
            OUTPUT_FILE="$2"; shift 2 ;;
        --shards-list)
            [[ -z "${2:-}" ]] && { echo "Error: --shards-list requires a value"; exit 1; }
            SHARDS_LIST="$2"; shift 2 ;;
        --clients-list)
            [[ -z "${2:-}" ]] && { echo "Error: --clients-list requires a value"; exit 1; }
            CLIENTS_LIST="$2"; shift 2 ;;
        --pipeline-list)
            [[ -z "${2:-}" ]] && { echo "Error: --pipeline-list requires a value"; exit 1; }
            PIPELINE_LIST="$2"; shift 2 ;;
        --aof)
            AOF_MODE=true; shift ;;
        --skip-redis)
            SKIP_REDIS=true; shift ;;
        --dry-run)
            DRY_RUN=true; shift ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --host HOST          Server host (default: 127.0.0.1)"
            echo "  --moon-port PORT     Moon port (default: 6400)"
            echo "  --redis-port PORT    Redis port (default: 6399)"
            echo "  --runs N             Runs per config (default: 3)"
            echo "  --requests N         Requests per run (default: 100000)"
            echo "  --output FILE        Output file (default: BENCHMARK-SCALING.md)"
            echo "  --shards-list LIST   Shard counts (default: \"1 2 4 8 12\")"
            echo "  --clients-list LIST  Client counts (default: \"50 200 500\")"
            echo "  --pipeline-list LIST Pipeline depths (default: \"1 8 16 64\")"
            echo "  --aof                Enable AOF testing"
            echo "  --skip-redis         Moon-only mode"
            echo "  --dry-run            Print test matrix without executing"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

LOCAL_MODE=true
[[ "$HOST" != "127.0.0.1" ]] && [[ "$HOST" != "localhost" ]] && LOCAL_MODE=false

# ===========================================================================
# Helper Functions
# ===========================================================================

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${MOON_PID:-}" ]] && kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    if $LOCAL_MODE; then
        pkill -f "redis-server.*${REDIS_PORT}" 2>/dev/null || true
        pkill -f "moon.*${MOON_PORT}" 2>/dev/null || true
    fi
    [[ -n "${RESULTS_DIR:-}" ]] && [[ -d "$RESULTS_DIR" ]] && rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

parse_rps() {
    # Redis-benchmark 8.x uses \r for progress, final line has "requests per second"
    local input
    input=$(tr '\r' '\n')
    local result
    result=$(echo "$input" | awk '/[Rr]equests per second/ { found=1; for (i=1; i<=NF; i++) { gsub(/,/, "", $i); if ($i+0 == $i && $i > 0) { print $i; exit } } } END { if (!found) exit 1 }')
    if [[ -n "$result" ]]; then
        echo "$result"
    else
        echo "$input" | sed -n 's/.*[[:space:]]\([0-9][0-9.]*\)[[:space:]]*requests per second.*/\1/p' \
            | sed 's/,//g' | tail -1
    fi
}

parse_latency() {
    # Parse p50 and p99 from redis-benchmark --csv output
    # CSV format: "command","rps","avg","min","p50","p99","max"
    local csv_output="$1"
    local percentile="$2"  # "p50" or "p99"

    case "$percentile" in
        p50)
            echo "$csv_output" | tr '\r' '\n' | grep -v '^"' | head -1 | awk -F',' '{print $5}' 2>/dev/null \
                || echo "$csv_output" | tr '\r' '\n' | tail -1 | awk -F',' '{print $5}' 2>/dev/null \
                || echo "N/A"
            ;;
        p99)
            echo "$csv_output" | tr '\r' '\n' | grep -v '^"' | head -1 | awk -F',' '{print $6}' 2>/dev/null \
                || echo "$csv_output" | tr '\r' '\n' | tail -1 | awk -F',' '{print $6}' 2>/dev/null \
                || echo "N/A"
            ;;
    esac
}

median() {
    # Compute median from a list of numbers (one per line on stdin)
    sort -n | awk '{ a[NR] = $1 } END { if (NR % 2) print a[(NR+1)/2]; else print (a[NR/2] + a[NR/2+1]) / 2 }'
}

scaling_eff() {
    # scaling_eff = (rps_N_shards / rps_1_shard) / N_shards * 100
    local rps_n="$1" rps_1="$2" n_shards="$3"
    if [[ "$rps_1" == "0" ]] || [[ "$rps_1" == "N/A" ]] || [[ -z "$rps_1" ]]; then
        echo "N/A"
        return
    fi
    awk "BEGIN { printf \"%.1f\", ($rps_n / $rps_1) / $n_shards * 100 }"
}

wait_for_server() {
    local host="$1" port="$2" name="$3" max_wait=20 elapsed=0
    while (( elapsed < max_wait )); do
        if redis-cli -h "$host" -p "$port" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    log "ERROR: $name on $host:$port not responding after ${max_wait} attempts"
    return 1
}

start_moon() {
    local shards="$1"
    local aof_flags=""
    $AOF_MODE && aof_flags="--aof"

    log "Starting Moon on port $MOON_PORT ($shards shards)..."
    RUST_LOG=warn "$RUST_BINARY" --port "$MOON_PORT" --shards "$shards" --protected-mode no $aof_flags &
    MOON_PID=$!
    wait_for_server "$HOST" "$MOON_PORT" "Moon"
}

stop_moon() {
    if [[ -n "${MOON_PID:-}" ]]; then
        kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null || true
        MOON_PID=""
        sleep 0.5
    fi
}

start_redis() {
    local aof_flags="--appendonly no"
    $AOF_MODE && aof_flags="--appendonly yes"

    log "Starting Redis on port $REDIS_PORT..."
    redis-server --port "$REDIS_PORT" --save "" $aof_flags --loglevel warning --protected-mode no &
    REDIS_PID=$!
    wait_for_server "$HOST" "$REDIS_PORT" "Redis"
}

stop_redis() {
    if [[ -n "${REDIS_PID:-}" ]]; then
        kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
        REDIS_PID=""
        sleep 0.5
    fi
}

# bench_single: run one redis-benchmark invocation, capture RPS and CSV output
# Returns: "rps csv_line" on stdout
bench_single() {
    local host="$1" port="$2" clients="$3" pipeline="$4" cmd="$5"
    local bench_args="-h $host -p $port -n $REQUESTS -c $clients -P $pipeline"
    local raw_output csv_output rps

    case "$cmd" in
        GET|SET)
            raw_output=$(redis-benchmark $bench_args -t "${cmd,,}" -q 2>/dev/null)
            csv_output=$(redis-benchmark $bench_args -t "${cmd,,}" --csv 2>/dev/null)
            ;;
        MGET)
            # Use hash-tagged keys for co-location: {bench}:key:0 .. {bench}:key:9
            local mget_cmd="MGET"
            for i in $(seq 0 9); do
                mget_cmd="$mget_cmd {bench}:key:$i"
            done
            raw_output=$(redis-benchmark $bench_args -q "$mget_cmd" 2>/dev/null)
            csv_output=$(redis-benchmark $bench_args --csv "$mget_cmd" 2>/dev/null)
            ;;
    esac

    rps=$(echo "$raw_output" | parse_rps)
    rps="${rps:-0}"

    local p50 p99
    p50=$(parse_latency "$csv_output" "p50")
    p99=$(parse_latency "$csv_output" "p99")
    p50="${p50:-N/A}"
    p99="${p99:-N/A}"

    echo "$rps $p50 $p99"
}

# bench_config: run N iterations of a benchmark config, compute median stats
# Outputs: "median_rps min_rps max_rps p50 p99"
bench_config() {
    local host="$1" port="$2" clients="$3" pipeline="$4" cmd="$5"
    local rps_values=() p50_values=() p99_values=()
    local run rps p50 p99 result

    for (( run=1; run<=RUNS; run++ )); do
        result=$(bench_single "$host" "$port" "$clients" "$pipeline" "$cmd")
        rps=$(echo "$result" | awk '{print $1}')
        p50=$(echo "$result" | awk '{print $2}')
        p99=$(echo "$result" | awk '{print $3}')
        rps_values+=("$rps")
        p50_values+=("$p50")
        p99_values+=("$p99")
    done

    local median_rps min_rps max_rps median_p50 median_p99

    median_rps=$(printf '%s\n' "${rps_values[@]}" | median)
    min_rps=$(printf '%s\n' "${rps_values[@]}" | sort -n | head -1)
    max_rps=$(printf '%s\n' "${rps_values[@]}" | sort -n | tail -1)

    # Use last run's latency as representative (median of p50/p99 across runs)
    median_p50=$(printf '%s\n' "${p50_values[@]}" | grep -v 'N/A' | median 2>/dev/null || echo "N/A")
    median_p99=$(printf '%s\n' "${p99_values[@]}" | grep -v 'N/A' | median 2>/dev/null || echo "N/A")

    echo "$median_rps $min_rps $max_rps $median_p50 $median_p99"
}

# Seed GET/MGET data so benchmarks read real values
seed_data() {
    local host="$1" port="$2" clients="$3"
    log "  Seeding data on $host:$port..."
    redis-benchmark -h "$host" -p "$port" -n "$REQUESTS" -c "$clients" -t set -q >/dev/null 2>&1

    # Seed MGET hash-tagged keys
    for i in $(seq 0 9); do
        redis-cli -h "$host" -p "$port" SET "{bench}:key:$i" "value$i" >/dev/null 2>&1
    done
}

# ===========================================================================
# Pre-flight Checks
# ===========================================================================

# Raise ulimit for high client counts
ulimit -n 65536 2>/dev/null || log "WARNING: Could not set ulimit -n 65536 (may fail with high client counts)"

if ! command -v redis-benchmark &>/dev/null; then
    echo "Error: redis-benchmark not found in PATH"
    exit 1
fi

if $LOCAL_MODE && [[ ! -x "$RUST_BINARY" ]]; then
    echo "Error: Moon binary not found at $RUST_BINARY"
    echo "Build first: cargo build --release"
    exit 1
fi

RESULTS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/bench-scaling.XXXXXX")

# ===========================================================================
# Calculate Test Matrix Size
# ===========================================================================

num_shards=$(echo $SHARDS_LIST | wc -w | tr -d ' ')
num_clients=$(echo $CLIENTS_LIST | wc -w | tr -d ' ')
num_pipelines=$(echo $PIPELINE_LIST | wc -w | tr -d ' ')
num_commands=$(echo $COMMANDS | wc -w | tr -d ' ')
total_configs=$(( num_shards * num_clients * num_pipelines * num_commands ))
total_runs=$(( total_configs * RUNS ))
servers=1
$SKIP_REDIS || servers=2
# Estimate ~5 seconds per run (conservative)
est_seconds=$(( total_runs * servers * 5 ))
est_minutes=$(( est_seconds / 60 ))

# ===========================================================================
# Dry Run
# ===========================================================================

if $DRY_RUN; then
    echo "# Benchmark Scaling Test Matrix (DRY RUN)"
    echo ""
    echo "**Host:** $HOST ($(if $LOCAL_MODE; then echo "local"; else echo "remote"; fi))"
    echo "**Shards:** $SHARDS_LIST"
    echo "**Clients:** $CLIENTS_LIST"
    echo "**Pipeline depths:** $PIPELINE_LIST"
    echo "**Commands:** $COMMANDS"
    echo "**Runs per config:** $RUNS"
    echo "**Requests per run:** $REQUESTS"
    echo "**AOF mode:** $AOF_MODE"
    echo "**Skip Redis:** $SKIP_REDIS"
    echo ""
    echo "**Total configurations:** $total_configs"
    echo "**Total benchmark runs:** $total_runs (x$servers servers)"
    echo "**Estimated runtime:** ~${est_minutes} minutes"
    echo ""
    echo "## Test Matrix"
    echo ""
    printf "| %-8s | %-8s | %-10s | %-8s |\n" "Shards" "Clients" "Pipeline" "Command"
    printf "|%-10s|%-10s|%-12s|%-10s|\n" "----------" "----------" "------------" "----------"
    for s in $SHARDS_LIST; do
        for c in $CLIENTS_LIST; do
            for p in $PIPELINE_LIST; do
                for cmd in $COMMANDS; do
                    printf "| %-8s | %-8s | %-10s | %-8s |\n" "$s" "$c" "$p" "$cmd"
                done
            done
        done
    done
    echo ""
    echo "Total: $total_configs configs x $RUNS runs = $total_runs benchmark invocations"
    exit 0
fi

# ===========================================================================
# Remote Mode Instructions
# ===========================================================================

if ! $LOCAL_MODE; then
    log "=========================================="
    log "REMOTE MODE: Server management is disabled"
    log "=========================================="
    log ""
    log "Ensure the following before running:"
    log "  - Moon is running on $HOST:$MOON_PORT"
    log "  - Redis is running on $HOST:$REDIS_PORT (unless --skip-redis)"
    log "  - You will need to restart Moon manually for each shard configuration"
    log ""
    log "The script will pause between shard configs for you to restart servers."
    log ""
fi

# ===========================================================================
# Main Benchmark Loop
# ===========================================================================

log "Starting scaling benchmark suite"
log "  Shards: $SHARDS_LIST"
log "  Clients: $CLIENTS_LIST"
log "  Pipeline: $PIPELINE_LIST"
log "  Commands: $COMMANDS"
log "  Runs/config: $RUNS"
log "  Requests/run: $REQUESTS"
log "  Estimated runtime: ~${est_minutes} minutes"
log ""

# Associative arrays for results keyed by "cmd|pipeline|shards|clients"
declare -A MOON_RPS MOON_MIN MOON_MAX MOON_P50 MOON_P99
declare -A REDIS_RPS REDIS_MIN REDIS_MAX REDIS_P50 REDIS_P99
# Track 1-shard baseline for scaling efficiency
declare -A BASELINE_MOON_RPS BASELINE_REDIS_RPS

config_count=0

for shards in $SHARDS_LIST; do
    log "=== Shard configuration: $shards ==="

    # Start/restart servers for this shard count
    if $LOCAL_MODE; then
        stop_moon
        start_moon "$shards"

        if ! $SKIP_REDIS && [[ -z "${REDIS_PID:-}" ]]; then
            start_redis
        fi
    else
        log "Remote mode: Ensure Moon is running with --shards $shards on $HOST:$MOON_PORT"
        log "Press Enter to continue (or Ctrl-C to abort)..."
        read -r
    fi

    for clients in $CLIENTS_LIST; do
        # Seed data for GET/MGET benchmarks
        seed_data "$HOST" "$MOON_PORT" "$clients"
        if ! $SKIP_REDIS; then
            seed_data "$HOST" "$REDIS_PORT" "$clients"
        fi

        for pipeline in $PIPELINE_LIST; do
            for cmd in $COMMANDS; do
                config_count=$((config_count + 1))
                local_key="${cmd}|${pipeline}|${shards}|${clients}"

                log "  [$config_count/$total_configs] $cmd s=$shards c=$clients p=$pipeline ($RUNS runs)..."

                # Moon benchmark
                moon_result=$(bench_config "$HOST" "$MOON_PORT" "$clients" "$pipeline" "$cmd")
                MOON_RPS[$local_key]=$(echo "$moon_result" | awk '{print $1}')
                MOON_MIN[$local_key]=$(echo "$moon_result" | awk '{print $2}')
                MOON_MAX[$local_key]=$(echo "$moon_result" | awk '{print $3}')
                MOON_P50[$local_key]=$(echo "$moon_result" | awk '{print $4}')
                MOON_P99[$local_key]=$(echo "$moon_result" | awk '{print $5}')

                # Redis benchmark
                if ! $SKIP_REDIS; then
                    redis_result=$(bench_config "$HOST" "$REDIS_PORT" "$clients" "$pipeline" "$cmd")
                    REDIS_RPS[$local_key]=$(echo "$redis_result" | awk '{print $1}')
                    REDIS_P50[$local_key]=$(echo "$redis_result" | awk '{print $4}')
                fi

                # Track 1-shard baseline
                if [[ "$shards" == "1" ]]; then
                    BASELINE_MOON_RPS["${cmd}|${pipeline}|${clients}"]="${MOON_RPS[$local_key]}"
                    if ! $SKIP_REDIS; then
                        BASELINE_REDIS_RPS["${cmd}|${pipeline}|${clients}"]="${REDIS_RPS[$local_key]}"
                    fi
                fi
            done
        done
    done
done

log "All benchmarks complete. Generating report..."

# ===========================================================================
# Output Generation
# ===========================================================================

{
    echo "# Multi-Shard Scaling Benchmark"
    echo ""
    echo "**Date:** $(date +%Y-%m-%d)"
    echo "**Hardware:** $(uname -srm)"

    if $LOCAL_MODE; then
        local_moon_ver=$("$RUST_BINARY" --version 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo "unknown")
        echo "**Moon binary:** $local_moon_ver"
    else
        echo "**Moon host:** $HOST:$MOON_PORT"
    fi

    if ! $SKIP_REDIS; then
        redis_ver=$(redis-cli -h "$HOST" -p "$REDIS_PORT" INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '\r' || echo "unknown")
        echo "**Redis version:** $redis_ver"
    fi

    echo "**Runs per config:** $RUNS"
    echo "**Requests per run:** $REQUESTS"
    echo "**Test host:** $HOST ($(if $LOCAL_MODE; then echo "local"; else echo "remote"; fi))"
    echo "**AOF mode:** $AOF_MODE"
    echo ""

    # Generate scaling matrices per command x pipeline
    for cmd in $COMMANDS; do
        for pipeline in $PIPELINE_LIST; do
            echo "## Scaling Matrix: $cmd (p=$pipeline)"
            echo ""

            if $SKIP_REDIS; then
                printf "| %-8s | %-8s | %18s | %10s | %10s | %10s | %10s | %13s |\n" \
                    "Shards" "Clients" "Moon RPS (median)" "Min" "Max" "Moon p50" "Moon p99" "Scaling Eff %"
                printf "|%-10s|%-10s|%-20s|%-12s|%-12s|%-12s|%-12s|%-15s|\n" \
                    "----------" "----------" "--------------------" "------------" "------------" "------------" "------------" "---------------"
            else
                printf "| %-8s | %-8s | %18s | %10s | %10s | %12s | %12s | %8s | %13s |\n" \
                    "Shards" "Clients" "Moon RPS (median)" "Moon p50" "Moon p99" "Redis RPS" "Redis p50" "Ratio" "Scaling Eff %"
                printf "|%-10s|%-10s|%-20s|%-12s|%-12s|%-14s|%-14s|%-10s|%-15s|\n" \
                    "----------" "----------" "--------------------" "------------" "------------" "--------------" "--------------" "----------" "---------------"
            fi

            for shards in $SHARDS_LIST; do
                for clients in $CLIENTS_LIST; do
                    local_key="${cmd}|${pipeline}|${shards}|${clients}"
                    baseline_key="${cmd}|${pipeline}|${clients}"

                    moon_rps="${MOON_RPS[$local_key]:-0}"
                    moon_p50="${MOON_P50[$local_key]:-N/A}"
                    moon_p99="${MOON_P99[$local_key]:-N/A}"
                    base_moon="${BASELINE_MOON_RPS[$baseline_key]:-0}"
                    eff=$(scaling_eff "$moon_rps" "$base_moon" "$shards")

                    if $SKIP_REDIS; then
                        moon_min="${MOON_MIN[$local_key]:-0}"
                        moon_max="${MOON_MAX[$local_key]:-0}"
                        printf "| %-8s | %-8s | %18s | %10s | %10s | %10s | %10s | %13s |\n" \
                            "$shards" "$clients" "$moon_rps" "$moon_min" "$moon_max" "$moon_p50" "$moon_p99" "$eff"
                    else
                        redis_rps="${REDIS_RPS[$local_key]:-0}"
                        redis_p50="${REDIS_P50[$local_key]:-N/A}"

                        if [[ "$redis_rps" != "0" ]] && [[ "$moon_rps" != "0" ]]; then
                            ratio=$(awk "BEGIN { printf \"%.2f\", $moon_rps / $redis_rps }")
                        else
                            ratio="N/A"
                        fi

                        printf "| %-8s | %-8s | %18s | %10s | %10s | %12s | %12s | %8sx | %13s |\n" \
                            "$shards" "$clients" "$moon_rps" "$moon_p50" "$moon_p99" "$redis_rps" "$redis_p50" "$ratio" "$eff"
                    fi
                done
            done
            echo ""
        done
    done

    # Pipeline scaling section (1 shard, first client count)
    first_clients=$(echo $CLIENTS_LIST | awk '{print $1}')
    echo "## Pipeline Scaling (1 shard, c=$first_clients)"
    echo ""

    if $SKIP_REDIS; then
        printf "| %-10s | %12s | %12s |\n" "Pipeline" "Moon GET" "Moon SET"
        printf "|%-12s|%-14s|%-14s|\n" "------------" "--------------" "--------------"
    else
        printf "| %-10s | %12s | %12s | %12s | %12s | %10s | %10s |\n" \
            "Pipeline" "Moon GET" "Moon SET" "Redis GET" "Redis SET" "GET Ratio" "SET Ratio"
        printf "|%-12s|%-14s|%-14s|%-14s|%-14s|%-12s|%-12s|\n" \
            "------------" "--------------" "--------------" "--------------" "--------------" "------------" "------------"
    fi

    for pipeline in $PIPELINE_LIST; do
        get_key="GET|${pipeline}|1|${first_clients}"
        set_key="SET|${pipeline}|1|${first_clients}"

        moon_get="${MOON_RPS[$get_key]:-0}"
        moon_set="${MOON_RPS[$set_key]:-0}"

        if $SKIP_REDIS; then
            printf "| %-10s | %12s | %12s |\n" "$pipeline" "$moon_get" "$moon_set"
        else
            redis_get="${REDIS_RPS[$get_key]:-0}"
            redis_set="${REDIS_RPS[$set_key]:-0}"

            get_ratio="N/A"
            set_ratio="N/A"
            [[ "$redis_get" != "0" ]] && [[ "$moon_get" != "0" ]] && \
                get_ratio=$(awk "BEGIN { printf \"%.2fx\", $moon_get / $redis_get }")
            [[ "$redis_set" != "0" ]] && [[ "$moon_set" != "0" ]] && \
                set_ratio=$(awk "BEGIN { printf \"%.2fx\", $moon_set / $redis_set }")

            printf "| %-10s | %12s | %12s | %12s | %12s | %10s | %10s |\n" \
                "$pipeline" "$moon_get" "$moon_set" "$redis_get" "$redis_set" "$get_ratio" "$set_ratio"
        fi
    done
    echo ""

    # AOF impact section
    if $AOF_MODE; then
        echo "## AOF Impact"
        echo ""
        echo "*AOF was enabled for all tests above. Re-run without --aof and compare results.*"
        echo ""
    fi

    echo "---"
    echo "*Generated by bench-scaling.sh on $(date +%Y-%m-%dT%H:%M:%S)*"

} > "$OUTPUT_FILE"

log "Report written to $OUTPUT_FILE"
log "Done."
