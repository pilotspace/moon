#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-scaling-distributed.sh — Sprint 1.1 re-baseline for v0.2 plan
#
# Distributed-key scaling curve. Differs from bench-scaling.sh in two ways:
#   1. Uses redis-benchmark `-r <N>` so writes hit a uniform keyspace, not
#      one hot key. Without -r, all SETs target __rand_key__ → measures lock
#      fairness on a single cache line, not real scaling. See:
#      memory/feedback_memory_benchmarking.md
#      memory/benchmark_shard_scaling_2026_04_22.md
#   2. Outputs a single-comparison table directly comparable to the
#      2026-04-22 baseline so the Sprint 4 gate decision is mechanical.
#
# Sprint 4 gate (.planning/rfcs/v02-enterprise-architecture.md):
#   8-shard SET p=1 distributed-keys ≥ 150K → defer S4 to v0.3
#   50K–150K                                → ship S2+S3, S4 in v0.3 with go/no-go
#   < 50K                                   → S4 non-negotiable for v0.2
#
# Usage:
#   ./scripts/bench-scaling-distributed.sh                # full matrix
#   ./scripts/bench-scaling-distributed.sh --requests N   # default 500000 per cell
#   ./scripts/bench-scaling-distributed.sh --keyspace N   # default 10000000
#   ./scripts/bench-scaling-distributed.sh --shards "1 4" # subset of shard counts
#   ./scripts/bench-scaling-distributed.sh --pipes "1 64" # subset of pipelines
###############################################################################

PORT=6399
REQUESTS=500000
CLIENTS_LIST="50 200"
KEYSPACE=10000000
SHARDS_LIST="1 2 4 8 12"
PIPES_LIST="1 16 64"
RUST_BINARY="./target/release/moon"
RESULT_DIR="tmp/bench-scaling-distributed-$(date +%Y%m%d-%H%M%S)"

# Note: ALWAYS run c=50 AND c=200. Below ~25 clients/shard the syscall path
# under-amortizes and SET p=1 appears to "collapse" — it's a methodology
# artifact, not architectural. See memory/benchmark_scaling_concurrency_2026_04_26.md.

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests) REQUESTS="$2"; shift 2 ;;
        --keyspace) KEYSPACE="$2"; shift 2 ;;
        --shards)   SHARDS_LIST="$2"; shift 2 ;;
        --pipes)    PIPES_LIST="$2"; shift 2 ;;
        --clients)  CLIENTS_LIST="$2"; shift 2 ;;
        --port)     PORT="$2"; shift 2 ;;
        -h|--help)  sed -n '4,29p' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$RESULT_DIR"
CSV="$RESULT_DIR/results.csv"
MD="$RESULT_DIR/results.md"
LOG="$RESULT_DIR/run.log"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG" >&2; }

MOON_PID=""
cleanup() {
    if [[ -n "$MOON_PID" ]] && kill -0 "$MOON_PID" 2>/dev/null; then
        kill "$MOON_PID" 2>/dev/null || true
        wait "$MOON_PID" 2>/dev/null || true
    fi
    pkill -f "moon.*--port ${PORT}" 2>/dev/null || true
    sleep 0.3
}
trap cleanup EXIT

start_moon() {
    local n="$1"
    cleanup
    sleep 0.3
    log "Starting Moon: --shards $n --port $PORT --appendonly no"
    "$RUST_BINARY" --shards "$n" --port "$PORT" --appendonly no \
        >"$RESULT_DIR/moon-$n.log" 2>&1 &
    MOON_PID=$!
    for _ in {1..50}; do
        if redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG; then
            log "Moon ready (pid=$MOON_PID)"
            return 0
        fi
        sleep 0.2
    done
    log "ERROR: Moon failed to come up; log tail:"
    tail -20 "$RESULT_DIR/moon-$n.log" >&2
    return 1
}

parse_rps() {
    tr '\r' '\n' \
        | awk '/[Rr]equests per second/ { for (i=1; i<=NF; i++) { gsub(/,/, "", $i); if ($i+0 == $i && $i > 0) { print $i; exit } } }' \
        | head -1
}

bench_one() {
    local op="$1" pipe="$2" clients="$3"
    redis-benchmark -p "$PORT" -n "$REQUESTS" -c "$clients" \
        -P "$pipe" -r "$KEYSPACE" -t "$op" -q 2>/dev/null \
        | parse_rps
}

echo "shards,op,pipeline,clients,rps" >"$CSV"
: >"$RESULT_DIR/cells.csv"

# Restart moon per (shards × clients) cell so the DashTable starts empty —
# otherwise split_segment cost from prior SET runs taints later cells.
for n in $SHARDS_LIST; do
    for c in $CLIENTS_LIST; do
        start_moon "$n" || { log "Skipping shards=$n c=$c"; continue; }
        for op in SET GET; do
            for pipe in $PIPES_LIST; do
                log "Bench shards=$n op=$op pipe=$pipe c=$c"
                rps=$(bench_one "$op" "$pipe" "$c")
                rps="${rps:-0}"
                echo "$n,$op,$pipe,$c,$rps" | tee -a "$CSV" >>"$RESULT_DIR/cells.csv"
                log "  → $rps RPS"
            done
        done
        cleanup
    done
done

format_rps() {
    local rps="$1"
    if ! [[ "$rps" =~ ^[0-9.]+$ ]] || (( $(echo "$rps == 0" | bc -l) )); then
        echo "NA"; return
    fi
    if (( $(echo "$rps >= 1000000" | bc -l) )); then
        printf "%.2fM" "$(echo "$rps / 1000000" | bc -l)"
    elif (( $(echo "$rps >= 1000" | bc -l) )); then
        printf "%.0fK" "$(echo "$rps / 1000" | bc -l)"
    else
        printf "%.0f" "$rps"
    fi
}

{
    echo "# Moon shard-scaling re-baseline $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "Distributed keys (\`redis-benchmark -r $KEYSPACE\`), $REQUESTS req/cell, clients=[$CLIENTS_LIST]."
    echo "Binary: \`$RUST_BINARY\`"
    if [[ -r /proc/cpuinfo ]]; then
        echo "Host: $(uname -srm), $(grep -c '^processor' /proc/cpuinfo) CPUs, $(grep -m1 'model name' /proc/cpuinfo | sed 's/^.*: //')"
    else
        echo "Host: $(uname -srm)"
    fi
    echo
    echo "## Results"
    base_n=$(echo $SHARDS_LIST | awk '{print $1}')
    for c in $CLIENTS_LIST; do
        echo
        echo "### c=$c"
        echo
        printf "| op | pipe |"
        for n in $SHARDS_LIST; do printf " %d shards |" "$n"; done
        printf "\n|---|---|"
        for _ in $SHARDS_LIST; do printf "---|"; done
        printf "\n"
        for op in SET GET; do
            for pipe in $PIPES_LIST; do
                base_rps=$(awk -F, -v n="$base_n" -v o="$op" -v p="$pipe" -v cc="$c" '$1==n && $2==o && $3==p && $4==cc {print $5}' "$RESULT_DIR/cells.csv")
                base_rps="${base_rps:-0}"
                printf "| %s | %d |" "$op" "$pipe"
                for n in $SHARDS_LIST; do
                    rps=$(awk -F, -v n="$n" -v o="$op" -v p="$pipe" -v cc="$c" '$1==n && $2==o && $3==p && $4==cc {print $5}' "$RESULT_DIR/cells.csv")
                    rps="${rps:-0}"
                    cell=$(format_rps "$rps")
                    if [[ "$n" != "$base_n" ]] && [[ "$base_rps" =~ ^[0-9.]+$ ]] && (( $(echo "$base_rps > 0" | bc -l) )); then
                        ratio=$(echo "scale=2; $rps / $base_rps" | bc -l)
                        printf " %s (%.2fx) |" "$cell" "$ratio"
                    else
                        printf " %s |" "$cell"
                    fi
                done
                printf "\n"
            done
        done
    done

    echo
    echo "## Sprint 4 gate (use c=200 production-realistic concurrency)"
    echo
    set8_p1_c200=$(awk -F, '$1==8 && $2=="SET" && $3==1 && $4==200 {print $5}' "$RESULT_DIR/cells.csv")
    set8_p1_c200="${set8_p1_c200:-0}"
    echo "8-shard SET p=1 distributed-keys @ c=200: **${set8_p1_c200}** RPS"
    echo
    if [[ "$set8_p1_c200" =~ ^[0-9.]+$ ]] && (( $(echo "$set8_p1_c200 > 0" | bc -l) )); then
        if (( $(echo "$set8_p1_c200 >= 150000" | bc -l) )); then
            echo "→ **Verdict: defer Sprint 4 (storage refactor) to v0.3.** Ship Sprints 2+3 as v0.2."
        elif (( $(echo "$set8_p1_c200 >= 50000" | bc -l) )); then
            echo "→ **Verdict: ship Sprints 2+3 as v0.2 with operator docs.** Schedule Sprint 4 for v0.3 with go/no-go."
        else
            echo "→ **Verdict: Sprint 4 storage refactor non-negotiable for v0.2.**"
        fi
    fi
    echo
    echo "_Note: c=50 numbers are typically 2–3× lower at 8 shards. Below ~25 clients/shard the syscall path under-amortizes — methodology artifact, not architectural._ See \`memory/benchmark_scaling_concurrency_2026_04_26.md\`."
    echo
    echo "## 2026-04-22 baseline (for comparison)"
    echo
    echo "| op | p | 1 shard | 2 shards | 4 shards | 8 shards |"
    echo "|---|---|---|---|---|---|"
    echo "| GET | 64 | 6.67M | 4.20M | 5.26M | 5.15M |"
    echo "| SET | 64 | 1.36M | 1.35M | 1.17M | 1.14M |"
    echo "| GET | 1  | 371K  | 327K  | 292K  | 275K  |"
    echo "| SET | 1  | 332K  | 41K   | 27K   | 24K   |"
    echo
    echo "Source: \`memory/benchmark_shard_scaling_2026_04_22.md\` (branch \`perf/shard-dispatch-hot-path\` @938ab44, OrbStack aarch64)."
} >"$MD"

log "Done. Results: $RESULT_DIR/"
log "  CSV:  $CSV"
log "  MD:   $MD"
echo
cat "$MD"
