#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-cross-shard-fastpath.sh  --  Cross-shard fast-path scaling matrix
#
# Measures Moon throughput across the full combinatorial grid of:
#   shards × clients × pipeline-depth × fast-path-mode
#
# Designed to run inside the OrbStack moon-dev Linux VM (Ubuntu 24.04, arm64)
# for production-representative numbers. Run from the repo root:
#
#   orb run -m moon-dev bash -c '
#     source ~/.cargo/env &&
#     cd /Users/tindang/workspaces/tind-repo/moon &&
#     bash scripts/bench-cross-shard-fastpath.sh
#   '
#
# If moon-dev is unavailable, the script still generates a results skeleton
# so the table structure is preserved. Pass --dry-run to skip server execution.
#
# Flags:
#   --dry-run           Print the matrix only; do not start servers or run benches.
#   --requests N        requests per redis-benchmark run (default: 200000)
#   --moon-port P       Base port for Moon server (default: 7400)
#   --output FILE       Output markdown path (default: per plan spec)
#   --binary PATH       Path to moon release binary (default: ./target/release/moon)
#   --shards-list "..."  Space-separated shard counts (default: "1 4 8")
#   --clients-list "..." Space-separated client counts (default: "10 50 100 200 500")
#   --pipeline-list "..." Space-separated pipeline depths (default: "1 16 64")
#
# IMPORTANT: Per feedback_bench_disk_offload_fairness.md, Moon is started with
#   --disk-offload disable --appendonly no
# for all runs to measure raw KV throughput without WAL/spill overhead.
#
# Per project_multi_shard_limitations.md, the c >= 25 x shards rule applies:
# rows where clients < 25 * shards are marked (*) in the output table since
# the benchmark artificially collapses throughput at that client count.
###############################################################################

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DRY_RUN=false
REQUESTS=200000
MOON_PORT=7400
OUTPUT_FILE="docs/benchmarks/cross-shard-fastpath-$(date +%Y-%m-%d).md"
BINARY="./target/release/moon"
SHARDS_LIST="1 4 8"
CLIENTS_LIST="10 50 100 200 500"
PIPELINE_LIST="1 16 64"
FAST_PATH_LIST="on off"
KEYSPACE=1000000

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)      DRY_RUN=true; shift ;;
        --requests)     REQUESTS="$2"; shift 2 ;;
        --moon-port)    MOON_PORT="$2"; shift 2 ;;
        --output)       OUTPUT_FILE="$2"; shift 2 ;;
        --binary)       BINARY="$2"; shift 2 ;;
        --shards-list)  SHARDS_LIST="$2"; shift 2 ;;
        --clients-list) CLIENTS_LIST="$2"; shift 2 ;;
        --pipeline-list) PIPELINE_LIST="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \?//' | head -30
            exit 0 ;;
        *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOON_PID=""

cleanup() {
    if [[ -n "$MOON_PID" ]]; then
        kill "$MOON_PID" 2>/dev/null || true
        wait "$MOON_PID" 2>/dev/null || true
        MOON_PID=""
    fi
}
trap cleanup EXIT INT TERM

start_moon() {
    local port="$1"
    local shards="$2"
    local fast_path="$3"

    cleanup

    "$BINARY" \
        --port "$port" \
        --shards "$shards" \
        --appendonly no \
        --disk-offload disable \
        --cross-shard-fast-path "$fast_path" \
        --admin-port 0 \
        >/dev/null 2>&1 &
    MOON_PID=$!

    # Wait for server to accept connections (up to 5s)
    local deadline=$((SECONDS + 5))
    until redis-cli -p "$port" ping >/dev/null 2>&1; do
        if [[ $SECONDS -ge $deadline ]]; then
            echo "ERROR: Moon did not start on port $port within 5 seconds" >&2
            return 1
        fi
        sleep 0.1
    done
}

# Run redis-benchmark and extract RPS from 8.x output (handles \r progress lines)
run_bench() {
    local port="$1"
    local clients="$2"
    local pipeline="$3"
    local cmd="$4"

    redis-benchmark \
        -p "$port" \
        -c "$clients" \
        -P "$pipeline" \
        -n "$REQUESTS" \
        -r "$KEYSPACE" \
        -t "$cmd" \
        --csv 2>/dev/null \
        | tr '\r' '\n' \
        | grep "\"${cmd}\"" \
        | awk -F',' '{gsub(/"/, "", $2); printf "%.0f\n", $2}' \
        | tail -1
}

# ---------------------------------------------------------------------------
# Dry-run: print matrix without executing
# ---------------------------------------------------------------------------
if $DRY_RUN; then
    echo "=== DRY RUN: Cross-shard fast-path matrix ==="
    echo "Shards: $SHARDS_LIST"
    echo "Clients: $CLIENTS_LIST"
    echo "Pipeline: $PIPELINE_LIST"
    echo "Fast-path modes: $FAST_PATH_LIST"
    echo ""
    echo "Matrix rows (shards x clients x pipeline x mode):"
    count=0
    for shards in $SHARDS_LIST; do
        for clients in $CLIENTS_LIST; do
            for pipeline in $PIPELINE_LIST; do
                for mode in $FAST_PATH_LIST; do
                    # Skip trivially: single shard never cross-shards
                    if [[ "$shards" == "1" && "$mode" == "off" ]]; then
                        continue
                    fi
                    note=""
                    if [[ $clients -lt $((25 * shards)) ]]; then
                        note=" (*)"
                    fi
                    echo "  shards=$shards c=$clients p=$pipeline mode=$mode$note"
                    count=$((count + 1))
                done
            done
        done
    done
    echo ""
    echo "Total: $count benchmark runs ((*) = clients < 25 x shards; results may be artificially low)"
    exit 0
fi

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: Moon binary not found at '$BINARY'" >&2
    echo "Build first: cargo build --release" >&2
    exit 1
fi

if ! command -v redis-benchmark &>/dev/null; then
    echo "ERROR: redis-benchmark not on PATH. Install redis-server package." >&2
    exit 1
fi

if ! command -v redis-cli &>/dev/null; then
    echo "ERROR: redis-cli not on PATH. Install redis-server package." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Output file header
# ---------------------------------------------------------------------------
mkdir -p "$(dirname "$OUTPUT_FILE")"

{
cat <<EOF
# Cross-Shard Fast-Path Benchmark

**Date:** $(date -u '+%Y-%m-%d %H:%M UTC')
**Binary:** $("$BINARY" --version 2>/dev/null | head -1 || echo "moon (unknown version)")
**Host:** $(uname -srm)
**Requests per run:** $REQUESTS (keyspace -r $KEYSPACE)
**Server flags:** --appendonly no --disk-offload disable

## Methodology

Each cell is a single \`redis-benchmark\` run of $REQUESTS SET operations at
the given \`(shards, clients, pipeline, fast-path-mode)\` combination.
Server restarts between rows to avoid RSS high-water-mark interference.

- \`on\` = \`--cross-shard-fast-path on\` (current default behavior)
- \`off\` = \`--cross-shard-fast-path off\` (all foreign reads through SPSC)
- \`(*)\` = clients < 25 × shards — benchmark under-subscribed; throughput may not be representative (see docs/production-guide.md §c ≥ 25 × shards rule)
- \`(skip)\` = single shard, fast-path mode irrelevant (no cross-shard traffic)

## Results: SET (writes, no cross-shard fast path active on either mode)

EOF

# SET results
echo "| Shards | Clients | Pipeline | fast-path=on (RPS) | fast-path=off (RPS) | delta |"
echo "|--------|---------|----------|--------------------|---------------------|-------|"

for shards in $SHARDS_LIST; do
    for clients in $CLIENTS_LIST; do
        for pipeline in $PIPELINE_LIST; do
            note=""
            if [[ $clients -lt $((25 * shards)) ]]; then
                note=" (*)"
            fi

            if [[ "$shards" == "1" ]]; then
                # Single shard: mode irrelevant — run once and show same number
                start_moon "$MOON_PORT" "$shards" "auto"
                rps_on=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "SET" || echo "N/A")
                cleanup
                printf "| %d | %d | %d | %s | (skip)%s | — |\n" \
                    "$shards" "$clients" "$pipeline" "$rps_on" "$note"
            else
                # Fast-path on
                start_moon "$MOON_PORT" "$shards" "on"
                rps_on=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "SET" || echo "N/A")
                cleanup

                # Fast-path off
                start_moon "$MOON_PORT" "$shards" "off"
                rps_off=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "SET" || echo "N/A")
                cleanup

                # Delta %
                if [[ "$rps_on" =~ ^[0-9]+$ && "$rps_off" =~ ^[0-9]+$ && "$rps_off" -gt 0 ]]; then
                    delta=$(awk "BEGIN {printf \"%+.1f%%\", (($rps_on - $rps_off) * 100.0) / $rps_off}")
                else
                    delta="N/A"
                fi
                printf "| %d | %d | %d | %s%s | %s | %s |\n" \
                    "$shards" "$clients" "$pipeline" "$rps_on" "$note" "$rps_off" "$delta"
            fi
        done
    done
done

echo ""
echo "## Results: GET (reads — fast-path directly affects cross-shard GET)"
echo ""
echo "| Shards | Clients | Pipeline | fast-path=on (RPS) | fast-path=off (RPS) | delta |"
echo "|--------|---------|----------|--------------------|---------------------|-------|"

for shards in $SHARDS_LIST; do
    for clients in $CLIENTS_LIST; do
        for pipeline in $PIPELINE_LIST; do
            note=""
            if [[ $clients -lt $((25 * shards)) ]]; then
                note=" (*)"
            fi

            if [[ "$shards" == "1" ]]; then
                start_moon "$MOON_PORT" "$shards" "auto"
                rps_on=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "GET" || echo "N/A")
                cleanup
                printf "| %d | %d | %d | %s | (skip)%s | — |\n" \
                    "$shards" "$clients" "$pipeline" "$rps_on" "$note"
            else
                start_moon "$MOON_PORT" "$shards" "on"
                rps_on=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "GET" || echo "N/A")
                cleanup

                start_moon "$MOON_PORT" "$shards" "off"
                rps_off=$(run_bench "$MOON_PORT" "$clients" "$pipeline" "GET" || echo "N/A")
                cleanup

                if [[ "$rps_on" =~ ^[0-9]+$ && "$rps_off" =~ ^[0-9]+$ && "$rps_off" -gt 0 ]]; then
                    delta=$(awk "BEGIN {printf \"%+.1f%%\", (($rps_on - $rps_off) * 100.0) / $rps_off}")
                else
                    delta="N/A"
                fi
                printf "| %d | %d | %d | %s%s | %s | %s |\n" \
                    "$shards" "$clients" "$pipeline" "$rps_on" "$note" "$rps_off" "$delta"
            fi
        done
    done
done

echo ""
echo "---"
echo "> (*) c < 25 × shards: benchmark artifact, not representative of production concurrency."
echo "> See docs/production-guide.md §When to use multi-shard for the c ≥ 25 × shards guidance."

} | tee "$OUTPUT_FILE"

echo ""
echo "Results written to: $OUTPUT_FILE"
