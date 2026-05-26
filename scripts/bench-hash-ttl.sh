#!/usr/bin/env bash
# bench-hash-ttl.sh — Hash-field TTL stack benchmark (Moon vs Redis 8.x)
#
# Scenarios:
#   A — baseline regression (plain Hash commands, Moon vs Redis)
#   B — new HEXPIRE-family / HGETDEL / HGETEX commands (Moon-only; Redis 8.x unsupported)
#   C — HashWithTtl path overhead vs plain Hash (Moon-only)
#   D — Redis 8.x comparison for supported commands (cross-reference A with explicit ratios)
#
# Usage: bash scripts/bench-hash-ttl.sh [--requests N] [--clients N]
# Requires: ./target/release/moon, redis-server, redis-benchmark
set -euo pipefail

###############################################################################
# Defaults
###############################################################################
PORT_MOON=6399
PORT_REDIS=6398
REQUESTS=200000
CLIENTS=50
RUST_BINARY="./target/release/moon"
MOON_PID=""
REDIS_PID=""
SEED_FIELDS=1000   # fields per hash for pre-seed; must match -r N in redis-benchmark
BENCH_HASH="myhash"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requests) REQUESTS="$2"; shift 2 ;;
        --clients)  CLIENTS="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

###############################################################################
# Helpers
###############################################################################
log() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2; }

cleanup() {
    log "Cleaning up servers..."
    [[ -n "${MOON_PID:-}"  ]] && kill "$MOON_PID"  2>/dev/null; wait "$MOON_PID"  2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "moon.*${PORT_MOON}"          2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
}
trap cleanup EXIT

wait_for() {
    local port="$1" name="$2"
    local tries=0
    until redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; do
        tries=$(( tries + 1 ))
        if [[ $tries -ge 30 ]]; then
            log "ERROR: $name failed to start on port $port within 15s"
            exit 1
        fi
        sleep 0.5
    done
}

# Parse RPS from redis-benchmark 8.x output.
# redis-benchmark 8.x uses \r for progress lines; the final result line is:
#   "<cmd>: 769230.75 requests per second, p50=0.103 msec"
# Strategy:
#   1. tr '\r' '\n' — flatten \r progress lines into separate lines
#   2. awk — find the line with "requests per second" and print it (exit 0 always)
#   3. sed — extract the number immediately before "requests per second"
# NOTE: grep is NOT used because grep exits non-zero on no-match under
#       set -o pipefail. awk exits 0 even when no line matches.
parse_rps() {
    tr '\r' '\n' \
        | awk '/requests per second/ { print; exit }' \
        | sed 's/.*[[:space:]]\([0-9][0-9.]*\)[[:space:]]requests per second.*/\1/'
}

# Run redis-benchmark once, return RPS string (never fails under set -e)
bench_once() {
    local port="$1"; shift
    # Capture output to avoid parse_rps pipeline exit-code propagation
    local raw
    raw=$(redis-benchmark -p "$port" -n "$REQUESTS" -c "$CLIENTS" -q "$@" 2>/dev/null) || true
    printf '%s\n' "$raw" | parse_rps || true
}

# Run redis-benchmark 3× on a port, return median RPS
bench_median() {
    local port="$1"; shift
    local v1 v2 v3
    v1=$(bench_once "$port" "$@") || true; v1="${v1:-0}"
    v2=$(bench_once "$port" "$@") || true; v2="${v2:-0}"
    v3=$(bench_once "$port" "$@") || true; v3="${v3:-0}"
    # Sort three floats, return middle value
    printf '%s\n%s\n%s\n' "$v1" "$v2" "$v3" \
        | awk 'BEGIN{n=0} {a[n++]=$1+0}
               END{ if(a[0]>a[1]){t=a[0];a[0]=a[1];a[1]=t}
                    if(a[1]>a[2]){t=a[1];a[1]=a[2];a[2]=t}
                    if(a[0]>a[1]){t=a[0];a[0]=a[1];a[1]=t}
                    printf "%.0f\n", a[1] }'
}

# Emit one result line
# Usage: scenario <id> <cmd_desc> <pipeline> <moon_rps> <redis_rps_or_NA>
scenario() {
    local id="$1" desc="$2" pipe="$3" moon="$4" redis="$5"
    local ratio="N/A"
    if [[ "$redis" != "N/A" ]] && [[ "${redis:-0}" != "0" ]] && [[ "${moon:-0}" != "0" ]]; then
        ratio=$(awk "BEGIN { printf \"%.2fx\", $moon / $redis }")
    fi
    printf '%-6s %-44s p=%-4s moon=%-12s redis=%-12s ratio=%s\n' \
        "$id" "$desc" "$pipe" "${moon:-0}" "${redis:-N/A}" "$ratio"
}

# FLUSHALL both servers (idempotent)
flush_all() {
    redis-cli -p "$PORT_MOON"  FLUSHALL >/dev/null 2>&1 || true
    redis-cli -p "$PORT_REDIS" FLUSHALL >/dev/null 2>&1 || true
}

# Pre-seed SEED_FIELDS fields into BENCH_HASH on a given port.
# Uses a single variadic HSET command: HSET key f0 v f1 v ... f999 v
seed_hash() {
    local port="$1"
    local args=""
    for i in $(seq 0 $(( SEED_FIELDS - 1 ))); do args="$args field${i} v"; done
    # shellcheck disable=SC2086
    redis-cli -p "$port" HSET "$BENCH_HASH" $args >/dev/null 2>&1
}

# Promote all SEED_FIELDS fields in BENCH_HASH to HashWithTtl via a single HEXPIRE call.
expire_all_fields() {
    local port="$1" seconds="$2"
    local field_list=""
    for i in $(seq 0 $(( SEED_FIELDS - 1 ))); do field_list="$field_list field${i}"; done
    # shellcheck disable=SC2086
    redis-cli -p "$port" HEXPIRE "$BENCH_HASH" "$seconds" FIELDS "$SEED_FIELDS" $field_list >/dev/null 2>&1
}

###############################################################################
# Start Servers
###############################################################################
log "Starting Moon on port $PORT_MOON (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)..."
RUST_LOG=warn "$RUST_BINARY" \
    --port              "$PORT_MOON" \
    --shards            1 \
    --appendonly        no \
    --disk-offload      disable \
    --disk-free-min-pct 0 \
    --protected-mode    no \
    >/tmp/moon-hash-ttl-bench.log 2>&1 &
MOON_PID=$!

log "Starting Redis on port $PORT_REDIS (no save, no AOF)..."
redis-server \
    --port "$PORT_REDIS" \
    --save "" \
    --appendonly no \
    --loglevel warning \
    --protected-mode no \
    >/tmp/redis-hash-ttl-bench.log 2>&1 &
REDIS_PID=$!

wait_for "$PORT_MOON"  "Moon"
wait_for "$PORT_REDIS" "Redis"
log "Both servers ready."

# Capture metadata
MOON_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
REDIS_VER=$(redis-cli -p "$PORT_REDIS" INFO server 2>/dev/null \
    | grep redis_version | tr -d '\r' | cut -d: -f2 | tr -d ' ')
REDIS_BENCH_VER=$(redis-benchmark --version 2>/dev/null | head -1 || echo "unknown")
PLATFORM="$(uname -s) $(uname -m)"
[[ -f /etc/os-release ]] && PLATFORM+=" / $(grep PRETTY_NAME /etc/os-release | cut -d= -f2 | tr -d '"')"

###############################################################################
# Header
###############################################################################
echo "==========================================================================="
echo "  Hash-field TTL stack — Moon vs Redis 8.x benchmark"
echo "  Moon commit  : $MOON_COMMIT (shards=1, appendonly=no, disk-offload=disable, disk-free-min-pct=0)"
echo "  Redis        : $REDIS_VER (save=\"\" appendonly=no)"
echo "  redis-bench  : $REDIS_BENCH_VER"
echo "  Platform     : $PLATFORM"
echo "  Requests     : $REQUESTS   Clients: $CLIENTS"
echo "  Methodology  : median of 3 runs; FLUSHALL + re-seed between scenarios"
echo "  Seed         : HSET $BENCH_HASH with $SEED_FIELDS fields (field0..field$((SEED_FIELDS-1)))"
echo "  Note         : -r $SEED_FIELDS ensures __rand_int__ covers 0..$(( SEED_FIELDS - 1 ))"
echo "==========================================================================="
echo ""

###############################################################################
# A — Baseline regression (plain Hash commands, Moon vs Redis)
###############################################################################
echo "--- A: Baseline regression (plain Hash commands) ---"
echo ""

# A.1  HSET  p=1
flush_all
log "A.1 HSET p=1 (Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HSET "$BENCH_HASH" "field__rand_int__" "v")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HSET "$BENCH_HASH" "field__rand_int__" "v")
scenario "A.1" "HSET myhash field__rand_int__ v" "1" "$M" "$R"

# A.2  HSET  p=16
flush_all
log "A.2 HSET p=16 (Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 16 HSET "$BENCH_HASH" "field__rand_int__" "v")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 16 HSET "$BENCH_HASH" "field__rand_int__" "v")
scenario "A.2" "HSET myhash field__rand_int__ v" "16" "$M" "$R"

# A.3  HGET  p=1  (pre-seeded)
flush_all
seed_hash "$PORT_MOON"
seed_hash "$PORT_REDIS"
log "A.3 HGET p=1 (pre-seeded $SEED_FIELDS fields, Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HGET "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HGET "$BENCH_HASH" "field__rand_int__")
scenario "A.3" "HGET myhash field__rand_int__" "1" "$M" "$R"
A3_MOON_RPS="$M"   # Save for C.2 comparison

# A.4  HGET  p=16  (same seed, no re-flush)
log "A.4 HGET p=16 (same seed, Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 16 HGET "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 16 HGET "$BENCH_HASH" "field__rand_int__")
scenario "A.4" "HGET myhash field__rand_int__" "16" "$M" "$R"
A4_MOON_RPS="$M"   # Save for C.2 comparison

# A.5  HDEL  p=1
log "A.5 HDEL p=1 (Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HDEL "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HDEL "$BENCH_HASH" "field__rand_int__")
scenario "A.5" "HDEL myhash field__rand_int__" "1" "$M" "$R"

# A.6  HLEN  p=16  (plain Hash — O(1) on Moon)
flush_all
seed_hash "$PORT_MOON"
seed_hash "$PORT_REDIS"
log "A.6 HLEN p=16 (plain Hash, Moon + Redis)..."
M=$(bench_median "$PORT_MOON"  -P 16 HLEN "$BENCH_HASH")
R=$(bench_median "$PORT_REDIS" -P 16 HLEN "$BENCH_HASH")
scenario "A.6" "HLEN myhash (plain Hash, O(1))" "16" "$M" "$R"
A6_MOON_RPS="$M"

echo ""

###############################################################################
# B — New commands: HEXPIRE-family, HGETDEL, HGETEX (Moon-only)
###############################################################################
echo "--- B: New commands (Moon-only — Redis 8.x has no HEXPIRE support) ---"
echo ""

# B.1  HEXPIRE FIELDS 1  p=1  (pre-seeded plain Hash → promotes on first call)
flush_all
seed_hash "$PORT_MOON"
log "B.1 HEXPIRE FIELDS 1 p=1 (pre-seeded $SEED_FIELDS plain Hash fields)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HEXPIRE "$BENCH_HASH" 3600 FIELDS 1 "field__rand_int__")
scenario "B.1" "HEXPIRE myhash 3600 FIELDS 1 <f>" "1" "$M" "N/A"

# B.2  HEXPIRE FIELDS 1  p=16
log "B.2 HEXPIRE FIELDS 1 p=16 (same seed)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 16 \
    HEXPIRE "$BENCH_HASH" 3600 FIELDS 1 "field__rand_int__")
scenario "B.2" "HEXPIRE myhash 3600 FIELDS 1 <f>" "16" "$M" "N/A"

# B.3  HTTL FIELDS 1  p=1  — first bulk-expire all fields so type is HashWithTtl
log "B.3 pre-step: HEXPIRE all $SEED_FIELDS fields to promote to HashWithTtl..."
expire_all_fields "$PORT_MOON" 3600
log "B.3 HTTL FIELDS 1 p=1..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HTTL "$BENCH_HASH" FIELDS 1 "field__rand_int__")
scenario "B.3" "HTTL myhash FIELDS 1 <f>" "1" "$M" "N/A"

# B.4  HTTL FIELDS 1  p=16
log "B.4 HTTL FIELDS 1 p=16..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 16 \
    HTTL "$BENCH_HASH" FIELDS 1 "field__rand_int__")
scenario "B.4" "HTTL myhash FIELDS 1 <f>" "16" "$M" "N/A"

# B.5  HPERSIST FIELDS 1  p=1  (all fields have TTL from B.3 setup)
log "B.5 HPERSIST FIELDS 1 p=1..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HPERSIST "$BENCH_HASH" FIELDS 1 "field__rand_int__")
scenario "B.5" "HPERSIST myhash FIELDS 1 <f>" "1" "$M" "N/A"

# B.6  HGETDEL FIELDS 1  p=1  — re-seed (HPERSIST leaves fields intact)
flush_all
seed_hash "$PORT_MOON"
log "B.6 HGETDEL FIELDS 1 p=1 (re-seeded plain Hash)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HGETDEL "$BENCH_HASH" FIELDS 1 "field__rand_int__")
scenario "B.6" "HGETDEL myhash FIELDS 1 <f>" "1" "$M" "N/A"

# B.7  HGETEX EX 600 FIELDS 1  p=1  — re-seed + promote to HashWithTtl
flush_all
seed_hash "$PORT_MOON"
expire_all_fields "$PORT_MOON" 3600
log "B.7 HGETEX EX 600 FIELDS 1 p=1 (HashWithTtl)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HGETEX "$BENCH_HASH" EX 600 FIELDS 1 "field__rand_int__")
scenario "B.7" "HGETEX myhash EX 600 FIELDS 1 <f>" "1" "$M" "N/A"
B7_MOON_RPS="$M"

# B.8  HGETEX FIELDS 1  p=1  — no-mode fast path (no TTL change, pure read)
log "B.8 HGETEX FIELDS 1 p=1 (no-mode — pure read, compare to A.3)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 1 \
    HGETEX "$BENCH_HASH" FIELDS 1 "field__rand_int__")
scenario "B.8" "HGETEX myhash FIELDS 1 <f> (no-mode)" "1" "$M" "N/A"
B8_MOON_RPS="$M"

# Commentary: B.8 vs A.3 (HGET p=1)
if [[ -n "${A3_MOON_RPS:-}" ]] && [[ "$A3_MOON_RPS" != "0" ]] && [[ "$B8_MOON_RPS" != "0" ]]; then
    B8_OVERHEAD=$(awk "BEGIN { printf \"%.1f\", (1 - $B8_MOON_RPS / $A3_MOON_RPS) * 100 }")
    echo ""
    echo "  [B.8 vs A.3] HGETEX no-mode overhead vs plain HGET p=1: ${B8_OVERHEAD}%"
fi

echo ""

###############################################################################
# C — HashWithTtl path overhead vs plain Hash (Moon-only)
###############################################################################
echo "--- C: HashWithTtl path overhead vs plain Hash (Moon-only) ---"
echo ""

# C.1 — reference from A.4 (HGET p=16 plain Hash)
echo "C.1 HGET p=16 plain Hash (reference — see A.4): moon=${A4_MOON_RPS:-N/A}"

# C.2 — HashWithTtl + HGET p=16  (seed + HEXPIRE every field → HGET)
flush_all
seed_hash "$PORT_MOON"
expire_all_fields "$PORT_MOON" 3600
log "C.2 HGET p=16 (HashWithTtl — all $SEED_FIELDS fields have TTL)..."
M=$(bench_median "$PORT_MOON" -r "$SEED_FIELDS" -P 16 \
    HGET "$BENCH_HASH" "field__rand_int__")
scenario "C.2" "HGET myhash (HashWithTtl, all fields TTL'd)" "16" "$M" "N/A"
C2_MOON_RPS="$M"

# Compute HashWithTtl HGET overhead vs plain Hash
if [[ -n "${A4_MOON_RPS:-}" ]] && [[ "$A4_MOON_RPS" != "0" ]] && [[ "$C2_MOON_RPS" != "0" ]]; then
    C2_OVERHEAD=$(awk "BEGIN { printf \"%.1f\", (1 - $C2_MOON_RPS / $A4_MOON_RPS) * 100 }")
    echo "  [C.2 vs A.4] HGET overhead on HashWithTtl vs plain Hash p=16: ${C2_OVERHEAD}%"
fi

# C.3 — HLEN on plain Hash (O(1)) p=16
flush_all
seed_hash "$PORT_MOON"
log "C.3 HLEN p=16 (plain Hash, O(1))..."
M_C3=$(bench_median "$PORT_MOON" -P 16 HLEN "$BENCH_HASH")
scenario "C.3" "HLEN myhash (plain Hash, O(1))" "16" "$M_C3" "N/A"

# C.4 — HLEN on HashWithTtl (O(N)) p=16
flush_all
seed_hash "$PORT_MOON"
expire_all_fields "$PORT_MOON" 3600
log "C.4 HLEN p=16 (HashWithTtl, O(N), $SEED_FIELDS fields)..."
M_C4=$(bench_median "$PORT_MOON" -P 16 HLEN "$BENCH_HASH")
scenario "C.4" "HLEN myhash (HashWithTtl, O(N))" "16" "$M_C4" "N/A"

# Compute HLEN slowdown
if [[ "$M_C3" != "0" ]] && [[ "$M_C4" != "0" ]]; then
    HLEN_RATIO=$(awk "BEGIN { printf \"%.2f\", $M_C3 / $M_C4 }")
    echo "  [C.4 vs C.3] HLEN on HashWithTtl is ${HLEN_RATIO}x slower than plain Hash (expected O(N) vs O(1))"
fi

echo ""

###############################################################################
# D — Redis 8.x comparison (explicit re-run for cross-section clarity)
###############################################################################
echo "--- D: Redis 8.x comparison (commands Redis supports) ---"
echo ""

# D.1 HSET p=1
flush_all
log "D.1/D.2 HSET p=1 + p=16 (Moon vs Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HSET "$BENCH_HASH" "field__rand_int__" "v")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HSET "$BENCH_HASH" "field__rand_int__" "v")
scenario "D.1" "HSET p=1" "1" "$M" "$R"

flush_all
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 16 HSET "$BENCH_HASH" "field__rand_int__" "v")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 16 HSET "$BENCH_HASH" "field__rand_int__" "v")
scenario "D.2" "HSET p=16" "16" "$M" "$R"

# D.3/D.4 HGET p=1 + p=16 (pre-seeded)
flush_all
seed_hash "$PORT_MOON"
seed_hash "$PORT_REDIS"
log "D.3/D.4 HGET p=1 + p=16 (pre-seeded, Moon vs Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HGET "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HGET "$BENCH_HASH" "field__rand_int__")
scenario "D.3" "HGET p=1" "1" "$M" "$R"

M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 16 HGET "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 16 HGET "$BENCH_HASH" "field__rand_int__")
scenario "D.4" "HGET p=16" "16" "$M" "$R"

# D.5 HDEL p=1
log "D.5 HDEL p=1 (Moon vs Redis)..."
M=$(bench_median "$PORT_MOON"  -r "$SEED_FIELDS" -P 1  HDEL "$BENCH_HASH" "field__rand_int__")
R=$(bench_median "$PORT_REDIS" -r "$SEED_FIELDS" -P 1  HDEL "$BENCH_HASH" "field__rand_int__")
scenario "D.5" "HDEL p=1" "1" "$M" "$R"

# D.6 HLEN p=16 (plain Hash)
flush_all
seed_hash "$PORT_MOON"
seed_hash "$PORT_REDIS"
log "D.6 HLEN p=16 (plain Hash, Moon vs Redis)..."
M=$(bench_median "$PORT_MOON"  -P 16 HLEN "$BENCH_HASH")
R=$(bench_median "$PORT_REDIS" -P 16 HLEN "$BENCH_HASH")
scenario "D.6" "HLEN p=16 (plain Hash)" "16" "$M" "$R"

echo ""
echo "==========================================================================="
echo "  HEXPIRE/HTTL/HPERSIST/HGETDEL/HGETEX — Moon-only (B.*)"
echo "  Redis 8.x does not support HEXPIRE-family."
echo "  Valkey 9.x would be the apples-to-apples comparison for B.*; not in scope."
echo "==========================================================================="
echo ""
log "Benchmark complete."
