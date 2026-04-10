#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-phase101-commands.sh -- Moon vs Redis benchmark for Phase 101 commands
#
# Tests all 24 commands added in Phase 101 (command parity gaps):
#   HyperLogLog, List convenience, Hash, Set, Sorted Set 6.2+,
#   Blocking fast-path, Functions/FCALL
#
# Usage:
#   ./scripts/bench-phase101-commands.sh                # Full run (20K req)
#   ./scripts/bench-phase101-commands.sh --requests N   # Custom request count
#   ./scripts/bench-phase101-commands.sh --shards N     # Moon shard count
#   ./scripts/bench-phase101-commands.sh --section hll  # Single section
#
# Sections: all, hll, list, hash, set, zset, blocking, functions, pipeline
###############################################################################

PORT_REDIS=6399
PORT_MOON=6400
REQUESTS=20000
CLIENTS=50
SHARDS=1
SECTION="all"
RUST_BINARY="./target/release/moon"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

REDIS_PID=""
MOON_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests)  if [[ $# -lt 2 ]] || [[ -z "$2" ]] || [[ "$2" == -* ]]; then echo "Error: --requests requires a value"; exit 1; fi; REQUESTS="$2"; shift 2 ;;
        --shards)    if [[ $# -lt 2 ]] || [[ -z "$2" ]] || [[ "$2" == -* ]]; then echo "Error: --shards requires a value"; exit 1; fi; SHARDS="$2"; shift 2 ;;
        --clients)   if [[ $# -lt 2 ]] || [[ -z "$2" ]] || [[ "$2" == -* ]]; then echo "Error: --clients requires a value"; exit 1; fi; CLIENTS="$2"; shift 2 ;;
        --section)   if [[ $# -lt 2 ]] || [[ -z "$2" ]] || [[ "$2" == -* ]]; then echo "Error: --section requires a value"; exit 1; fi; SECTION="$2"; shift 2 ;;
        --help)      awk '/^###/{n++} n==1' "$0"; exit 0 ;;
        *)           echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    if [[ -n "${MOON_PID:-}" ]]; then kill "$MOON_PID" 2>/dev/null || true; wait "$MOON_PID" 2>/dev/null || true; fi
    if [[ -n "${REDIS_PID:-}" ]]; then kill "$REDIS_PID" 2>/dev/null || true; wait "$REDIS_PID" 2>/dev/null || true; fi
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_MOON}" 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

parse_rps() {
    tr '\r' '\n' \
        | awk '/[Rr]equests per second/ {
            for (i=1; i<=NF; i++) { gsub(/,/, "", $i); if ($i+0 == $i && $i > 0) { print $i; exit } }
        }' \
        | head -1
}

print_row() {
    local desc="$1" redis_rps="${2:-0}" moon_rps="${3:-0}" ratio
    if [[ "$redis_rps" != "0" ]] && [[ "$moon_rps" != "0" ]]; then
        ratio=$(awk "BEGIN { printf \"%.2f\", $moon_rps / $redis_rps }")
    else
        ratio="N/A"
    fi
    printf "| %-40s | %12s | %12s | %6sx |\n" "$desc" "$redis_rps" "$moon_rps" "$ratio"
}

section_header() {
    echo ""
    echo "## $1"
    echo ""
    printf "| %-40s | %12s | %12s | %7s |\n" "Command" "Redis RPS" "Moon RPS" "Ratio"
    printf "|%-42s|%14s|%14s|%9s|\n" "------------------------------------------" "--------------" "--------------" "---------"
}

should_run() { [[ "$SECTION" == "all" ]] || [[ "$SECTION" == "$1" ]]; }

# redis-benchmark for commands it handles (single-key, simple args)
bench_rb() {
    local desc="$1"; shift
    local r m
    r=$(redis-benchmark -p "$PORT_REDIS" -n "$REQUESTS" -c "$CLIENTS" -q "$@" 2>/dev/null | parse_rps)
    m=$(redis-benchmark -p "$PORT_MOON"  -n "$REQUESTS" -c "$CLIENTS" -q "$@" 2>/dev/null | parse_rps)
    print_row "$desc" "${r:-0}" "${m:-0}"
}

# Build RESP protocol string for one command
_resp() {
    local n=$#
    printf "*%d\r\n" "$n"
    for arg in "$@"; do
        printf "\$%d\r\n%s\r\n" "${#arg}" "$arg"
    done
}

# Pipe N copies of a RESP command to a port, return RPS
_pipe_rps() {
    local port="$1" n="$2"; shift 2
    local one_cmd
    one_cmd=$(_resp "$@")
    local payload
    payload=$(python3 -c "import sys; sys.stdout.write(sys.stdin.read() * $n)" <<< "$one_cmd")

    local start end ms rps
    start=$(date +%s%N)
    printf '%s' "$payload" | redis-cli -p "$port" --pipe 2>/dev/null >/dev/null
    end=$(date +%s%N)
    ms=$(( (end - start) / 1000000 ))
    if [[ $ms -gt 0 ]]; then
        rps=$(awk "BEGIN { printf \"%.0f\", ($n * 1000.0) / $ms }")
    else
        rps="0"
    fi
    echo "$rps"
}

# Benchmark via pipe mode (for multi-arg commands redis-benchmark can't run)
bench_pipe() {
    local desc="$1"; shift
    local r m
    r=$(_pipe_rps "$PORT_REDIS" "$REQUESTS" "$@")
    m=$(_pipe_rps "$PORT_MOON"  "$REQUESTS" "$@")
    print_row "$desc" "$r" "$m"
}

# Re-seed a key with N elements via pipe
reseed_list() {
    local key="$1" n="$2"
    for port in "$PORT_REDIS" "$PORT_MOON"; do
        redis-cli -p "$port" DEL "$key" >/dev/null 2>&1
        local one=$(_resp RPUSH "$key" val)
        python3 -c "import sys; sys.stdout.write(sys.stdin.read() * $n)" <<< "$one" \
            | redis-cli -p "$port" --pipe 2>/dev/null >/dev/null
    done
}

reseed_zset() {
    local key="$1" n="$2"
    for port in "$PORT_REDIS" "$PORT_MOON"; do
        redis-cli -p "$port" DEL "$key" >/dev/null 2>&1
        local cmds=""
        for ((i=1; i<=n; i++)); do
            cmds+=$(_resp ZADD "$key" "$i" "m$i")
        done
        printf '%s' "$cmds" | redis-cli -p "$port" --pipe 2>/dev/null >/dev/null
    done
}

# ===========================================================================
# Start Servers
# ===========================================================================

log "Starting Redis on port $PORT_REDIS..."
redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --protected-mode no &
REDIS_PID=$!

log "Starting Moon on port $PORT_MOON ($SHARDS shards)..."
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_MOON" --shards "$SHARDS" --protected-mode no &
MOON_PID=$!

wait_for_server() {
    local port="$1" name="$2" i=0
    while (( i < 20 )); do
        redis-cli -p "$port" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.5; i=$((i+1))
    done
    echo "$name failed to start on port $port"; exit 1
}

wait_for_server "$PORT_REDIS" "Redis"
wait_for_server "$PORT_MOON" "Moon"
log "Servers ready."

# ===========================================================================
# Header
# ===========================================================================

REDIS_VER=$(redis-cli -p "$PORT_REDIS" INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '\r')
PLATFORM="$(uname -s) $(uname -m)"
[[ -f /etc/os-release ]] && PLATFORM="$PLATFORM / $(grep PRETTY_NAME /etc/os-release | cut -d= -f2 | tr -d '"')"

echo "# Phase 101 — Command Parity Benchmark (Moon vs Redis)"
echo ""
echo "| Property | Value |"
echo "|----------|-------|"
echo "| Date | $(date +%Y-%m-%d) |"
echo "| Redis | $REDIS_VER |"
echo "| Moon | $SHARDS shard(s) |"
echo "| Requests | $REQUESTS per test |"
echo "| Clients | $CLIENTS |"
echo "| Platform | $PLATFORM |"

# ===========================================================================
# Seed data (fast Python-based seeder)
# ===========================================================================

log "Seeding test data..."
python3 "$SCRIPT_DIR/bench-phase101-seed.py" "$PORT_REDIS"
python3 "$SCRIPT_DIR/bench-phase101-seed.py" "$PORT_MOON"
log "Data seeded."

# ===========================================================================
# HyperLogLog
# ===========================================================================

if should_run "hll"; then
    log "Benchmarking HyperLogLog..."
    section_header "HyperLogLog (PFADD / PFCOUNT / PFMERGE)"

    bench_rb   "PFADD (1 elem, existing key)"          PFADD hll1 newelem
    bench_rb   "PFADD (3 elem, new key)"                PFADD hllbench a b c
    bench_rb   "PFCOUNT (1 key)"                        PFCOUNT hll1
    bench_pipe "PFCOUNT (2 keys)"                       PFCOUNT hll1 hll2
    bench_pipe "PFMERGE (2 → 1)"                        PFMERGE hll3 hll1 hll2
fi

# ===========================================================================
# List Commands
# ===========================================================================

if should_run "list"; then
    log "Benchmarking list commands..."
    section_header "List (LPUSHX / RPUSHX / LMPOP)"

    bench_rb   "LPUSHX (existing key)"                  LPUSHX mylist val
    bench_rb   "RPUSHX (existing key)"                  RPUSHX mylist val
    bench_rb   "LPUSHX (missing key → NOP)"             LPUSHX nokey val
    bench_pipe "LMPOP 1 key LEFT"                       LMPOP 1 mylist LEFT
    bench_pipe "LMPOP 1 key LEFT COUNT 10"              LMPOP 1 mylist LEFT COUNT 10
fi

# ===========================================================================
# Hash Commands
# ===========================================================================

if should_run "hash"; then
    log "Benchmarking hash commands..."
    section_header "Hash (HRANDFIELD)"

    bench_rb   "HRANDFIELD (1 field)"                   HRANDFIELD myhash
    bench_pipe "HRANDFIELD (5 fields)"                  HRANDFIELD myhash 5
    bench_pipe "HRANDFIELD (10 WITHVALUES)"             HRANDFIELD myhash 10 WITHVALUES
    bench_pipe "HRANDFIELD (-5, allow dups)"            HRANDFIELD myhash -5
fi

# ===========================================================================
# Set Commands
# ===========================================================================

if should_run "set"; then
    log "Benchmarking set commands..."
    section_header "Set (SMOVE / SINTERCARD)"

    bench_pipe "SMOVE (member exists)"                  SMOVE smvsrc smvdst m1
    bench_pipe "SINTERCARD (2 sets)"                    SINTERCARD 2 myset1 myset2
    bench_pipe "SINTERCARD (3 sets)"                    SINTERCARD 3 myset1 myset2 myset3
    bench_pipe "SINTERCARD (2 sets, LIMIT 10)"          SINTERCARD 2 myset1 myset2 LIMIT 10
fi

# ===========================================================================
# Sorted Set 6.2+
# ===========================================================================

if should_run "zset"; then
    log "Benchmarking sorted set 6.2+ commands..."
    section_header "Sorted Set 6.2+ (ZRANGESTORE / ZDIFF / ZUNION / ZINTER / etc.)"

    bench_pipe "ZRANGESTORE (50 elements)"              ZRANGESTORE zdst myzset1 0 49
    bench_pipe "ZDIFF (2 keys)"                         ZDIFF 2 myzset1 myzset2
    bench_pipe "ZUNION (2 keys)"                        ZUNION 2 myzset1 myzset2
    bench_pipe "ZINTER (2 keys)"                        ZINTER 2 myzset1 myzset2
    bench_pipe "ZINTERCARD (2 keys)"                    ZINTERCARD 2 myzset1 myzset2
    bench_pipe "ZINTERCARD (2 keys, LIMIT 10)"          ZINTERCARD 2 myzset1 myzset2 LIMIT 10
    bench_pipe "ZMSCORE (3 members)"                    ZMSCORE myzset1 m1 m50 m100
    bench_pipe "ZRANDMEMBER (1)"                        ZRANDMEMBER myzset1
    bench_pipe "ZRANDMEMBER (10)"                       ZRANDMEMBER myzset1 10
    bench_pipe "ZRANDMEMBER (5 WITHSCORES)"             ZRANDMEMBER myzset1 5 WITHSCORES
    bench_pipe "ZMPOP 1 key MIN"                        ZMPOP 1 bzset MIN
    bench_pipe "ZMPOP 1 key MIN COUNT 5"                ZMPOP 1 bzset MIN COUNT 5
fi

# ===========================================================================
# Blocking fast-path (data already present → immediate return)
# ===========================================================================

if should_run "blocking"; then
    log "Benchmarking blocking commands (fast path)..."
    section_header "Blocking — Fast Path (element already available)"

    echo "| *(Blocking cmds use non-blocking equivalents for throughput comparison)* ||||"

    # Blocking commands can't be benchmarked via pipe (they consume + block).
    # Instead, compare the non-blocking equivalents which share the same code path.
    # The blocking overhead is just the timeout check + registry lookup (~10ns).
    bench_rb   "LPOP (= BLPOP fast path)"               LPOP blist
    bench_rb   "RPOP (= BRPOP fast path)"               RPOP blist

    # BLMPOP/BLMOVE/BZMPOP can't use redis-benchmark directly.
    # Use pipe with small N and large pre-seeded data to avoid exhaustion.
    bench_pipe "LMPOP 1 key LEFT (= BLMPOP path)"       LMPOP 1 blist LEFT
    bench_pipe "LMOVE (= BLMOVE path)"                  LMOVE blsrc bldst LEFT RIGHT
    bench_pipe "ZMPOP 1 key MIN (= BZMPOP path)"        ZMPOP 1 bzset MIN
fi

# ===========================================================================
# Functions / FCALL
# ===========================================================================

if should_run "functions"; then
    log "Benchmarking Functions/FCALL..."
    section_header "Functions API (FCALL / FCALL_RO)"

    bench_pipe "FCALL echo1 (0 keys, 1 arg)"           FCALL echo1 0 hello
    bench_pipe "FCALL_RO echo1 (0 keys, 1 arg)"        FCALL_RO echo1 0 hello
    bench_pipe "FUNCTION LIST"                          FUNCTION LIST
fi

# ===========================================================================
# Pipeline scaling for key Phase 101 commands
# ===========================================================================

if should_run "pipeline" || should_run "all"; then
    log "Benchmarking pipeline scaling..."
    section_header "Pipeline Scaling — Phase 101 Commands"

    for p in 1 8 16 64; do
        for cmd_desc_args in \
            "PFADD:PFADD hll1 newelem" \
            "PFCOUNT:PFCOUNT hll1" \
            "LPUSHX:LPUSHX mylist val" \
            "HRANDFIELD:HRANDFIELD myhash" \
            "ZRANDMEMBER:ZRANDMEMBER myzset1" \
        ; do
            local_desc="${cmd_desc_args%%:*}"
            local_args="${cmd_desc_args#*:}"
            # shellcheck disable=SC2086
            r=$(redis-benchmark -p "$PORT_REDIS" -n "$REQUESTS" -c "$CLIENTS" -P "$p" -q $local_args 2>/dev/null | parse_rps)
            # shellcheck disable=SC2086
            m=$(redis-benchmark -p "$PORT_MOON"  -n "$REQUESTS" -c "$CLIENTS" -P "$p" -q $local_args 2>/dev/null | parse_rps)
            print_row "${local_desc} p=$p" "${r:-0}" "${m:-0}"
        done
    done
fi

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "---"
echo ""
echo "### Legend"
echo "- **Ratio > 1.0**: Moon is faster"
echo "- **Ratio < 1.0**: Redis is faster"
echo "- **Ratio = N/A**: Command not supported or returned 0"
echo "- Pipe-mode tests are single-connection serial (lower absolute RPS, fair comparison)"
echo "- redis-benchmark tests use $CLIENTS parallel clients"
echo ""
echo "*Generated by bench-phase101-commands.sh on $(date)*"

log "Done."
