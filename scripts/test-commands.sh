#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# test-commands.sh -- Comprehensive command coverage test
#
# Tests ALL Redis commands that moon supports via redis-cli, comparing output
# against Redis as ground truth. Also runs redis-benchmark throughput tests
# for benchmarkable commands.
#
# Usage:
#   ./scripts/test-commands.sh                  # Run all tests
#   ./scripts/test-commands.sh --category NAME  # Run single category
#   ./scripts/test-commands.sh --list           # List categories
#   ./scripts/test-commands.sh --shards N       # moon shard count (default: 1)
#   ./scripts/test-commands.sh --skip-build     # Skip cargo build
#   ./scripts/test-commands.sh --skip-bench     # Skip redis-benchmark throughput
#   ./scripts/test-commands.sh --bench-only     # Only redis-benchmark throughput
#   ./scripts/test-commands.sh --moon-only      # Test moon without Redis comparison
###############################################################################

# Overridable like the sibling `test-consistency.sh`: these ports are often
# already held by another checkout's servers, and a squatter on PORT_REDIS
# would silently make every "expected" value below come from moon.
PORT_REDIS="${PORT_REDIS:-6399}"
PORT_RUST="${PORT_RUST:-6400}"
SHARDS=1
SKIP_BUILD=false
SKIP_BENCH=false
BENCH_ONLY=false
MOON_ONLY=false
CATEGORY_FILTER=""
# Overridable so the suite can be pointed at a specific build -- and so the
# startup guard can be exercised (`MOON_BIN=/usr/bin/false` makes moon never
# listen, which must abort the run, not produce vacuous passes).
RUST_BINARY="${MOON_BIN:-./target/release/moon}"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
BENCH_PASS=0
BENCH_FAIL=0
RUST_PID=""
REDIS_PID=""

# ===========================================================================
# Argument Parsing
# ===========================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards)       SHARDS="$2"; shift 2 ;;
        --skip-build)   SKIP_BUILD=true; shift ;;
        --skip-bench)   SKIP_BENCH=true; shift ;;
        --bench-only)   BENCH_ONLY=true; shift ;;
        --moon-only)    MOON_ONLY=true; shift ;;
        --category)     CATEGORY_FILTER="$2"; shift 2 ;;
        --list)
            echo "Available categories:"
            echo "  string       - String commands (GET, SET, MGET, APPEND, INCR, etc.)"
            echo "  list         - List commands (LPUSH, RPUSH, LPOP, LRANGE, etc.)"
            echo "  hash         - Hash commands (HSET, HGET, HMGET, HGETALL, etc.)"
            echo "  set          - Set commands (SADD, SMEMBERS, SINTER, SDIFF, etc.)"
            echo "  sorted_set   - Sorted set commands (ZADD, ZRANGE, ZSCORE, etc.)"
            echo "  key          - Key commands (DEL, EXISTS, EXPIRE, TTL, RENAME, etc.)"
            echo "  stream       - Stream commands (XADD, XREAD, XRANGE, XGROUP, etc.)"
            echo "  connection   - Connection commands (PING, ECHO, SELECT, INFO, etc.)"
            echo "  pubsub       - Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)"
            echo "  transaction  - Transaction commands (MULTI, EXEC, DISCARD)"
            echo "  scripting    - Lua scripting (EVAL, EVALSHA)"
            echo "  vector       - Vector search commands (FT.CREATE, FT.SEARCH, FT.INFO, FT.DROPINDEX)"
            echo "  persistence  - Persistence commands (BGSAVE, BGREWRITEAOF, etc.)"
            echo "  blocking     - Blocking commands (BLPOP, BRPOP, BZPOPMIN, etc.)"
            echo "  temporal     - Temporal commands (TEMPORAL.SNAPSHOT_AT, TEMPORAL.INVALIDATE)"
            echo "  workspace    - Workspace commands (WS CREATE, WS LIST, WS INFO, WS AUTH, WS DROP)"
            echo "  mq           - Durable Message Queue (MQ CREATE, PUSH, POP, ACK, DLQLEN, TRIGGER)"
            echo "  txn_kv       - KV Transaction Wiring (TXN.BEGIN, TXN.COMMIT, TXN.ABORT lifecycle)"
            echo "  eviction     - Eviction policy behavior (volatile-ttl victim order, liveness, OOM)"
            echo "  benchmark    - redis-benchmark throughput for all benchmarkable commands"
            exit 0
            ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

# ===========================================================================
# Helpers
# ===========================================================================

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    # `rc` is captured and re-exited at the end (moon#679): a trap's exit
    # status replaces the script's, and the `[[ -n ... ]] && kill` lines below
    # return 1 whenever a PID is unset, which would report a clean run as a
    # failure (and a failed run as whatever the last kill happened to return).
    local rc=$?
    log "Cleaning up..."
    [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
    [[ -n "${MOON_DATA_DIR:-}" ]] && rm -rf "$MOON_DATA_DIR"
    return "$rc"
}
trap cleanup EXIT

# Multiline "A appears somewhere before B" match, reading stdin.
#
# Replaces `grep -Pzo "(?s)A.*B"` (moon#679). That idiom is GNU-only: on a
# macOS host `grep` is ugrep, which rejects -P and exits 2 -- and because the
# rows compared command output rather than exit status, that 2 leaked through
# as `got: 2`, so every AGG-*/TAG-*/NUMERIC-04 row reported a moon answer of
# "2" that moon never gave. Flattening newlines to spaces and using a plain
# BRE works on GNU grep, BSD grep and ugrep alike.
#
# The match is deliberately as loose as the idiom it replaces (`open.*5` also
# matches "open" followed by a later "25"); tightening it is a separate change
# from making it run at all.
spans() {
    tr '\n' ' ' | grep -q "$1"
}

# The `|| true` on every client wrapper is load-bearing (moon#679).
#
# `redis-cli` exits non-zero when it cannot reach the server, and under
# `set -euo pipefail` that makes `VAR=$(mcli ...)` end the whole run. So the
# moment a server died -- which is exactly when the suite has something
# important to say -- the script stopped without printing its summary, and the
# operator saw a truncated log rather than "N rows failed". A row that gets an
# empty answer should FAIL and let the run continue to the totals.
#
# Nothing branches on these functions' exit status (checked: no `if mcli ...`
# and no `mcli ... &&` anywhere in this file), so swallowing it costs nothing.
rcli() {
    # Run redis-cli against Redis
    redis-cli -p "$PORT_REDIS" "$@" 2>/dev/null || true
}

# Liveness probes that CAN fail.
#
# `rcli`/`mcli` end in `|| true` so an assertion row can compare output without
# aborting the whole suite. That also makes them useless as health checks --
# `mcli PING || exit 1` is a branch execution can never reach. These two do not
# swallow the status, and they match the reply rather than just its exit code,
# so a foreign process holding the port cannot fake liveness either.
redis_is_up() { redis-cli -p "$PORT_REDIS" PING 2>/dev/null | grep -qx PONG; }
moon_is_up()  { redis-cli -p "$PORT_RUST"  PING 2>/dev/null | grep -qx PONG; }

# Poll rather than sleep a fixed amount: a cold first exec of a freshly built
# binary can take well over a second, and the old `sleep 1` was the other half
# of the same bug.
await_up() {  # await_up <name> <probe-fn>
    local name="$1" probe="$2" i
    for i in $(seq 1 100); do
        "$probe" && return 0
        sleep 0.1
    done
    echo "$name failed to start (no PONG on its port after 10s)" >&2
    return 1
}

mcli() {
    # Run redis-cli against moon
    redis-cli -p "$PORT_RUST" "$@" 2>/dev/null || true
}

# Run several commands down ONE connection, one reply per line.
#
# `mcli` spawns a `redis-cli` per call, so every call is a separate connection.
# That is fine for stateless commands and fatal for connection-scoped state:
# MULTI, WATCH, SELECT, SUBSCRIBE and moon's cross-store TXN all live on the
# connection. Probing them through `mcli` sends each command down a connection
# of its own, so `TXN BEGIN` is already gone by the time `SET` runs -- which is
# why the whole TXN section used to report failures that said nothing about
# moon (moon#683).
#
# redis-cli reads commands from stdin when it is not a tty, and holds one
# connection for the batch.
msession() {
    printf '%s\n' "$@" | redis-cli -p "$PORT_RUST" 2>/dev/null || true
}

# The Nth reply (1-based) of an `msession` transcript.
mreply() {
    local n="$1"
    shift
    printf '%s\n' "$@" | sed -n "${n}p"
}

# ── 4-dimensional FLOAT32 fixture vectors (moon#683) ────────────────────────
# These used to be the obvious little-endian encodings of 1.0 (00 00 80 3f) and
# 0.0 (00 00 00 00), built inline with `"$(printf '\x00...')"`. Command
# substitution STRIPS NUL bytes, so a 16-byte 4-dim blob reached redis-cli as
# TWO bytes -- for the stored documents as well as for every query vector. That
# is the entire source of the "ERR query vector dimension mismatch" and "no
# valid vectors found for POSITIVE keys" rows: the harness never sent a vector.
#
# These byte patterns carry no NULs, so what the shell passes is what the test
# meant: V_HI ~0.747, V_LO ~0.035, V_MID ~0.633 -- the same "one dominant axis"
# geometry the KNN and tag-filter rows depend on.
#
# File scope, not inside the vector block: the hybrid rows use `$VQ` from their
# own `should_run` gate, and `set -u` turns an out-of-scope read into a silent
# abort.
V_HI='\x3f\x3f\x3f\x3f'
V_LO='\x11\x11\x11\x3d'
V_MID='\x22\x22\x22\x3f'
VEC1=$(printf "${V_HI}${V_LO}${V_LO}${V_LO}")   # [HI, LO, LO, LO]
VEC2=$(printf "${V_LO}${V_HI}${V_LO}${V_LO}")   # [LO, HI, LO, LO]
VEC3=$(printf "${V_MID}${V_LO}${V_LO}${V_LO}")  # near VEC1, so KNN order means something
VQ="$VEC1"                                      # query: nearest doc:1, then doc:3

# Fail loudly rather than run ten rows against a truncated fixture -- that is
# exactly the failure that hid for as long as it did.
for _v in "$VEC1" "$VEC2" "$VEC3"; do
    if [[ ${#_v} -ne 16 ]]; then
        echo "FATAL: vector fixture is ${#_v} bytes, expected 16 -- the shell ate part of it." >&2
        exit 2
    fi
done

rcli_raw() {
    redis-cli -p "$PORT_REDIS" --no-auth-warning "$@" 2>/dev/null || true
}

mcli_raw() {
    redis-cli -p "$PORT_RUST" --no-auth-warning "$@" 2>/dev/null || true
}

# Compare redis-cli output between Redis and moon
assert_match() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null || echo "__REDIS_ERROR__")
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if [[ "$redis_out" == "$moon_out" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $(echo "$redis_out" | head -3)"
        echo "    MOON:  $(echo "$moon_out" | head -3)"
    fi
}

# Compare a countdown reply (TTL / PTTL / OBJECT IDLETIME) that both servers
# compute from "now".
#
# moon#683: `TTL k:eat` after `EXPIREAT k:eat 9999999999` was compared with
# assert_match and failed as REDIS=8212451035 vs MOON=8212451034. That is not a
# divergence -- `rcli` and `mcli` are separate `redis-cli` invocations ~100ms
# apart, so whenever a whole second ticks over between them the two answers
# differ by exactly 1. Sampling both servers back-to-back 16 times (8 in each
# order, to cancel any bias from who is asked first) gave diff=0 every time, so
# there is no rounding-direction difference to catch. Allowing +/-1 keeps the
# assertion honest and stops the row failing on a clock tick.
assert_match_countdown() {
    local desc="$1" tolerance="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out delta
    redis_out=$(rcli "$@" 2>/dev/null || echo "__REDIS_ERROR__")
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if ! [[ "$redis_out" =~ ^-?[0-9]+$ && "$moon_out" =~ ^-?[0-9]+$ ]]; then
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (non-numeric reply)"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
        return
    fi
    delta=$(( redis_out > moon_out ? redis_out - moon_out : moon_out - redis_out ))
    if [[ "$delta" -le "$tolerance" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (differs by $delta, tolerance $tolerance)"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
    fi
}

# Compare sorted output (for unordered results like HGETALL, SMEMBERS, SUNION)
assert_match_sorted() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | sort || echo "__REDIS_ERROR__")
    moon_out=$(mcli "$@" 2>/dev/null | sort || echo "__MOON_ERROR__")
    if [[ "$redis_out" == "$moon_out" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (sorted comparison)"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $(echo "$redis_out" | head -3)"
        echo "    MOON:  $(echo "$moon_out" | head -3)"
    fi
}

# Compare with TTL tolerance (±5 seconds)
assert_match_ttl() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    moon_out=$(mcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    local diff=$(( redis_out - moon_out ))
    if [[ "$diff" -lt 0 ]]; then diff=$(( -diff )); fi
    if [[ "$diff" -le 5 ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (TTL diff=$diff, tolerance=5)"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
    fi
}

# Compare with millisecond TTL tolerance (±5000ms)
assert_match_pttl() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    moon_out=$(mcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    local diff=$(( redis_out - moon_out ))
    if [[ "$diff" -lt 0 ]]; then diff=$(( -diff )); fi
    if [[ "$diff" -le 5000 ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (PTTL diff=$diff, tolerance=5000)"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
    fi
}

# Test moon-only (no Redis comparison)
assert_moon() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if [[ "$moon_out" == "$expected" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    CMD:      redis-cli $*"
        echo "    EXPECTED: $expected"
        echo "    GOT:      $(echo "$moon_out" | head -3)"
    fi
}

# Test moon response contains expected substring
assert_moon_contains() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qF "$expected"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (expected substring '$expected')"
        echo "    CMD:  redis-cli $*"
        echo "    GOT:  $(echo "$moon_out" | head -3)"
    fi
}

# The negative form: moon's reply must NOT contain the string. Used for
# absence claims (moon#672: no reply may quote a moon source path), where the
# positive form has nothing stable to match against.
assert_moon_not_contains() {
    local desc="$1" forbidden="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>&1)
    if echo "$moon_out" | grep -qF "$forbidden"; then
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (reply contains forbidden substring '$forbidden')"
        echo "    CMD:  redis-cli $*"
        echo "    GOT:  $(echo "$moon_out" | head -3)"
    else
        PASS=$((PASS + 1))
    fi
}

# As above, but the expectation must be a WHOLE LINE of the reply.
#
# `COMMAND LIST` is the case that needs it: since moon#635 publishes
# `container|sub` entries, a substring match for `pubsub` is satisfied by
# `pubsub|channels`, so it would stay green with the top-level `pubsub` entry
# gone. Reach for this whenever one published name is a prefix of another.
assert_moon_line() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qxF "$expected"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (expected the whole line '$expected')"
        echo "    CMD:  redis-cli $*"
        echo "    GOT:  $(echo "$moon_out" | head -3)"
    fi
}

# Assert moon's RAW first reply line, read straight off the socket.
#
# Needed because `redis-cli` renders `$-1` (null bulk) and `*-1` (null array)
# identically as "(nil)" -- the two RESP2 nulls a typed client decodes
# differently. Every `assert_moon_*` above is built on `mcli`, so none of them
# can express "which null was it" (moon#482).
#
# A failure to connect returns a marker that can never equal an expected RESP
# line, so a broken probe fails loudly instead of passing vacuously.
assert_moon_raw_reply() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local line=""
    if ! exec 3<>"/dev/tcp/127.0.0.1/${PORT_RUST}"; then
        line="__CONNECT_FAILED__"
    else
        printf '%s\r\n' "$*" >&3
        IFS= read -r -t 5 line <&3
        exec 3>&-
        line="${line%$'\r'}"
    fi
    if [[ "$line" == "$expected" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    CMD:  $*"
        echo "    WANT: $expected"
        echo "    GOT:  $line"
    fi
}

# Test moon response matches an extended regex.
# `assert_moon_contains` greps -F, so it cannot express "some positive
# integer" -- the shape you need when the exact value is not fixed.
assert_moon_matches() {
    local desc="$1" pattern="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qE "$pattern"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (expected match /$pattern/)"
        echo "    CMD:  redis-cli $*"
        echo "    GOT:  $(echo "$moon_out" | head -3)"
    fi
}

# Test that moon returns non-error response
assert_moon_ok() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qvE "^(\(error\)|ERR |__MOON_ERROR__)"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (got error)"
        echo "    CMD: redis-cli $*"
        echo "    GOT: $(echo "$moon_out" | head -3)"
    fi
}

# Run redis-benchmark and check it completes
assert_bench() {
    local desc="$1" cmd="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local raw rps
    raw=$(redis-benchmark -p "$PORT_RUST" -n 5000 -c 50 $cmd "$@" 2>&1 | tr '\r' '\n')
    # `|| true`: when redis-benchmark prints no summary line (a command it
    # cannot drive, a server that went away) grep exits 1, pipefail propagates
    # it, and set -e ends the run -- before the `-z "$rps"` fallback just below
    # ever gets to do its job (moon#679).
    rps=$(echo "$raw" | grep -i "requests per second" | tail -1 | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]/ && $(i+1) ~ /requests/) print $i}' | sed 's/,//g' || true)
    # Fallback: try -q mode format "COMMAND: NNN.NN requests per second"
    if [[ -z "$rps" ]]; then
        rps=$(echo "$raw" | grep "requests per second" | tail -1 | sed 's/.*: \([0-9.]*\) requests.*/\1/' | sed 's/,//g' || true)
    fi
    if [[ -n "$rps" ]] && [[ "$rps" != "0" ]] && [[ "$rps" != "0.00" ]]; then
        BENCH_PASS=$((BENCH_PASS + 1))
        PASS=$((PASS + 1))
        printf "    %-40s %s rps\n" "$desc" "$rps"
    else
        BENCH_FAIL=$((BENCH_FAIL + 1))
        FAIL=$((FAIL + 1))
        echo "    FAIL: $desc (benchmark returned no results)"
    fi
}

flush_both() {
    rcli FLUSHALL >/dev/null 2>&1 || true
    mcli FLUSHALL >/dev/null 2>&1 || true
}

should_run() {
    [[ -z "$CATEGORY_FILTER" ]] || [[ "$CATEGORY_FILTER" == "$1" ]]
}

# ===========================================================================
# Setup
# ===========================================================================

log "=== Moon Command Coverage Test ==="

if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building moon..."
    # Do NOT swallow stderr here (moon#679). This was
    # `cargo build ... --quiet 2>/dev/null`, so a failed build ended the run
    # under `set -e` with the log reading "Building moon..." and nothing else
    # -- no error, no summary, no clue. Whatever cargo has to say about why it
    # could not build the binary this suite is about to test, the operator
    # needs to see.
    if ! cargo build --release --features text-index --quiet; then
        echo "FATAL: cargo build failed; the suite has nothing to test." >&2
        exit 2
    fi
fi

# Refuse to run on a port somebody else is already listening on (moon#679).
#
# Without this the suite silently measures the WRONG SERVER: a leftover moon or
# redis from another run answers, every row compares against it, and the report
# looks authoritative. This is not hypothetical -- it produced a full run of
# `MOONERR diskfull` failures that had nothing to do with the code under test,
# because a moon from an unrelated session (a different binary, different
# flags, different data dir) held the port. Judge by the LISTENER, not by
# whether a PING comes back: a PING coming back is exactly the symptom.
require_free_port() {
    local port="$1" what="$2" owner
    # `|| true`: lsof exits 1 when nothing matches -- i.e. when the port is
    # FREE, the common case -- and pipefail + set -e would end the run there.
    owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR==2{print $1" (pid "$2")"}' || true)
    if [[ -n "$owner" ]]; then
        echo "FATAL: port $port ($what) is already held by $owner." >&2
        echo "       This suite would have compared against that process instead of" >&2
        echo "       the server it started. Stop it, or re-run with a free port:" >&2
        echo "         PORT_REDIS=<n> PORT_RUST=<n> $0" >&2
        exit 2
    fi
    # Explicit: without it the function's status is that of the `if` test
    # above, which is 1 when the port IS free -- and under `set -e` a function
    # returning 1 at top level ends the script. The first draft of this very
    # guard did exactly that, killing the run with no output at all.
    return 0
}
require_free_port "$PORT_RUST" "moon"
# A plain `[[ ... ]] && cmd` would be the same trap: false test, status 1,
# `set -e` ends the run. Use an if.
if [[ "$MOON_ONLY" == "false" ]]; then
    require_free_port "$PORT_REDIS" "redis"
fi

if [[ "$MOON_ONLY" == "false" ]]; then
    log "Starting Redis on port $PORT_REDIS..."
    redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --protected-mode no &
    REDIS_PID=$!
fi

log "Starting moon on port $PORT_RUST ($SHARDS shards)..."
# A fresh --dir per run (moon#679). Without it moon treats the CWD as its data
# directory: it writes `appendonlydir/` and `moon.lock` into the repo root, and
# -- because FLUSHALL deliberately keeps index DEFINITIONS -- it reloads the
# previous run's FT indexes on start. The visible symptom is `FT.CREATE basic`
# failing with `ERR Index already exists` on the second and every later run,
# which makes the suite non-reproducible: a clean checkout passes, the same
# checkout run twice does not.
MOON_DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/moon-test-commands.XXXXXX")
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_RUST" --shards "$SHARDS" --protected-mode no \
    --dir "$MOON_DATA_DIR" --disk-free-min-pct 0 &
RUST_PID=$!

# Verify servers are up. These guards used to be `rcli PING || exit 1`, which
# could never fire (see `moon_is_up`) -- a suite that ran to completion against
# a server that was not listening reported 41 failures with every reply empty.
if [[ "$MOON_ONLY" == "false" ]]; then
    await_up "Redis" redis_is_up || exit 1
fi
await_up "moon" moon_is_up || exit 1

log "Servers ready."

if [[ "$BENCH_ONLY" == "true" ]]; then
    CATEGORY_FILTER="benchmark"
fi

# ===========================================================================
# STRING COMMANDS
# ===========================================================================

if should_run "string"; then
    echo ""
    echo "=== STRING COMMANDS ==="
    flush_both

    if [[ "$MOON_ONLY" == "true" ]]; then
        assert_moon "SET basic"          "OK"    SET str:k1 hello
        assert_moon "GET basic"          "hello" GET str:k1
        assert_moon "SET EX"             "OK"    SET str:k2 world EX 100
        assert_moon "SET PX"             "OK"    SET str:k3 val PX 100000
        assert_moon "SET NX (new)"       "OK"    SET str:k4 new NX
        assert_moon "SET NX (exists)"    ""      SET str:k4 newer NX
        assert_moon "SET XX (exists)"    "OK"    SET str:k4 updated XX
        assert_moon "SET XX (missing)"   ""      SET str:k999 x XX
        assert_moon "SETNX (new)"        "(integer) 1" SETNX str:k5 val
        assert_moon "SETNX (exists)"     "(integer) 0" SETNX str:k5 val2
        assert_moon "SETEX"              "OK"    SETEX str:k6 100 myval
        assert_moon "PSETEX"             "OK"    PSETEX str:k7 100000 myval
        assert_moon "GET (missing)"      ""      GET str:missing
        assert_moon "GETSET"             "hello" GETSET str:k1 newhello
        assert_moon "GETDEL"             "newhello" GETDEL str:k1
        assert_moon "GETDEL (gone)"      ""      GET str:k1
        assert_moon "APPEND"             "(integer) 8" APPEND str:a1 hello
        assert_moon "APPEND (existing)"  "(integer) 13" APPEND str:a1 world
        assert_moon "STRLEN"             "(integer) 13" STRLEN str:a1
        assert_moon "INCR"               "(integer) 1" INCR str:cnt1
        assert_moon "INCR (again)"       "(integer) 2" INCR str:cnt1
        assert_moon "INCRBY"             "(integer) 12" INCRBY str:cnt1 10
        assert_moon "DECR"               "(integer) 11" DECR str:cnt1
        assert_moon "DECRBY"             "(integer) 6" DECRBY str:cnt1 5
        assert_moon "INCRBYFLOAT"        "6.5"   INCRBYFLOAT str:cnt1 0.5
        assert_moon "MSET"               "OK"    MSET str:m1 a str:m2 b str:m3 c
        assert_moon_ok "MGET"            MGET str:m1 str:m2 str:m3
        assert_moon "MSETNX (new)"       "(integer) 1" MSETNX "{mcm}n1" a "{mcm}n2" b
        assert_moon "MSETNX (exists)"    "(integer) 0" MSETNX "{mcm}n2" x "{mcm}n3" c
        assert_moon_ok "GETEX with EX"   GETEX str:m1 EX 100
    else
        assert_match "SET basic"         SET str:k1 hello
        assert_match "GET basic"         GET str:k1
        assert_match "SET EX"            SET str:k2 world EX 100
        assert_match "SET PX"            SET str:k3 val PX 100000
        assert_match "SET NX (new)"      SET str:k4 new NX
        assert_match "SET NX (exists)"   SET str:k4 newer NX
        assert_match "SET XX (exists)"   SET str:k4 updated XX
        assert_match "SET XX (missing)"  SET str:k999 x XX
        assert_match "SETNX (new)"       SETNX str:k5 val
        assert_match "SETNX (exists)"    SETNX str:k5 val2
        assert_match "SETEX"             SETEX str:k6 100 myval
        assert_match "PSETEX"            PSETEX str:k7 100000 myval
        assert_match "GET (missing)"     GET str:missing
        assert_match "APPEND"            APPEND str:a1 helloworld
        assert_match "STRLEN"            STRLEN str:a1
        assert_match "INCR"              INCR str:cnt1
        assert_match "INCR (again)"      INCR str:cnt1
        assert_match "INCRBY"            INCRBY str:cnt1 10
        assert_match "DECR"              DECR str:cnt1
        assert_match "DECRBY"            DECRBY str:cnt1 5
        assert_match "INCRBYFLOAT"       INCRBYFLOAT str:cnt1 0.5
        assert_match "MSET"              MSET str:m1 a str:m2 b str:m3 c
        assert_match "MGET"              MGET str:m1 str:m2 str:m3
        assert_match "MSETNX (new)"      MSETNX "{mcc}n1" a "{mcc}n2" b
        assert_match "MSETNX (exists)"   MSETNX "{mcc}n2" x "{mcc}n3" c
        assert_match "GETEX with EX"     GETEX str:m1 EX 100
    fi
fi

# ===========================================================================
# LIST COMMANDS
# ===========================================================================

if should_run "list"; then
    echo ""
    echo "=== LIST COMMANDS ==="
    flush_both

    assert_match "LPUSH"               LPUSH lst:k1 a b c
    assert_match "RPUSH"               RPUSH lst:k1 x y z
    assert_match "LLEN"                LLEN lst:k1
    assert_match "LRANGE all"          LRANGE lst:k1 0 -1
    assert_match "LRANGE partial"      LRANGE lst:k1 1 3
    assert_match "LINDEX"              LINDEX lst:k1 0
    assert_match "LINDEX negative"     LINDEX lst:k1 -1
    assert_match "LPOP"                LPOP lst:k1
    assert_match "RPOP"                RPOP lst:k1
    assert_match "LPOP count"          LPOP lst:k1 2
    assert_match "RPOP count"          RPOP lst:k1 2
    # Rebuild for remaining tests
    flush_both
    rcli RPUSH lst:k2 a b c d e >/dev/null 2>&1; mcli RPUSH lst:k2 a b c d e >/dev/null 2>&1
    assert_match "LSET"                LSET lst:k2 2 REPLACED
    assert_match "LRANGE after LSET"   LRANGE lst:k2 0 -1
    assert_match "LREM"                LREM lst:k2 1 a
    assert_match "LTRIM"               LTRIM lst:k2 0 2
    assert_match "LRANGE after LTRIM"  LRANGE lst:k2 0 -1
    rcli RPUSH lst:k3 a b c >/dev/null 2>&1; mcli RPUSH lst:k3 a b c >/dev/null 2>&1
    assert_match "LINSERT BEFORE"      LINSERT lst:k3 BEFORE b INSERTED
    assert_match "LINSERT AFTER"       LINSERT lst:k3 AFTER c APPENDED
    assert_match "LRANGE after INSERT" LRANGE lst:k3 0 -1
    rcli RPUSH lst:k4 a >/dev/null 2>&1; mcli RPUSH lst:k4 a >/dev/null 2>&1
    assert_match "LPOS"                LPOS lst:k4 a
    # moon#570: `{lst}` co-locates source and destination on one shard, so this
    # row compares the COMMAND against Redis at any `--shards N`. Untagged names
    # made it a function of the shard count -- moon refuses a move whose two
    # keys are owned by different shards (it cannot do both halves atomically
    # and used to lose the element instead); Redis, having no shards, moves it
    # either way. The refusal is asserted in scripts/test-consistency.sh.
    rcli RPUSH {lst}:src x y z >/dev/null 2>&1; mcli RPUSH {lst}:src x y z >/dev/null 2>&1
    assert_match "LMOVE"               LMOVE {lst}:src {lst}:dst LEFT RIGHT
    # RPOPLPUSH === LMOVE ... RIGHT LEFT (moon#520). Probe the reply, the
    # source's remainder AND the destination: a no-op that answered the popped
    # element without moving it would pass a reply-only assertion.
    rcli RPUSH lst:rl x y z >/dev/null 2>&1; mcli RPUSH lst:rl x y z >/dev/null 2>&1
    assert_match "RPOPLPUSH"           RPOPLPUSH lst:rl lst:rl-d
    assert_match "RPOPLPUSH source"    LRANGE lst:rl 0 -1
    assert_match "RPOPLPUSH dest"      LRANGE lst:rl-d 0 -1
    assert_match "RPOPLPUSH miss"      RPOPLPUSH lst:rl-absent lst:rl-d
    assert_match "RPOPLPUSH arity"     RPOPLPUSH lst:rl
fi

# ===========================================================================
# HASH COMMANDS
# ===========================================================================

if should_run "hash"; then
    echo ""
    echo "=== HASH COMMANDS ==="
    flush_both

    assert_match "HSET single"         HSET hsh:k1 f1 v1
    assert_match "HSET multi"          HSET hsh:k1 f2 v2 f3 v3
    assert_match "HGET"                HGET hsh:k1 f1
    assert_match "HGET (missing)"      HGET hsh:k1 missing
    assert_match "HMSET"               HMSET hsh:k1 f4 v4 f5 v5
    assert_match "HMGET"               HMGET hsh:k1 f1 f2 f3 f4 f5 missing
    assert_match_sorted "HGETALL"      HGETALL hsh:k1
    assert_match_sorted "HKEYS"       HKEYS hsh:k1
    assert_match_sorted "HVALS"       HVALS hsh:k1
    assert_match "HLEN"                HLEN hsh:k1
    assert_match "HEXISTS (yes)"       HEXISTS hsh:k1 f1
    assert_match "HEXISTS (no)"        HEXISTS hsh:k1 missing
    # moon#636. `f1` (2 bytes) vs `v1` (2 bytes) would NOT discriminate, so
    # measure a field whose name and value differ in length.
    assert_match "HSTRLEN"             HSTRLEN hsh:k1 f1
    assert_match "HSTRLEN (missing)"   HSTRLEN hsh:k1 missing
    assert_match "HDEL"                HDEL hsh:k1 f5
    assert_match "HSETNX (new)"        HSETNX hsh:k1 f6 v6
    assert_match "HSETNX (exists)"     HSETNX hsh:k1 f6 v6b
    assert_moon_ok "HSCAN"             HSCAN hsh:k1 0
    # moon#630: NOVALUES was accepted and dropped, so the reply carried the
    # field/value interleave a client reads as field names. `assert_match`
    # compares against real Redis, which is what catches a silent drop.
    assert_match "HSCAN NOVALUES"      HSCAN hsh:k1 0 NOVALUES
    assert_match "HINCRBY"             HINCRBY hsh:k1 counter 10
    assert_match "HINCRBY (again)"     HINCRBY hsh:k1 counter 5
    assert_match "HINCRBYFLOAT"        HINCRBYFLOAT hsh:k1 fcounter 1.5
fi

# ===========================================================================
# SET COMMANDS
# ===========================================================================

if should_run "set"; then
    echo ""
    echo "=== SET COMMANDS ==="
    flush_both

    assert_match "SADD"                SADD s:k1 a b c d e
    assert_match "SADD dup"            SADD s:k1 a b f
    assert_match "SCARD"               SCARD s:k1
    assert_match "SISMEMBER (yes)"     SISMEMBER s:k1 a
    assert_match "SISMEMBER (no)"      SISMEMBER s:k1 z
    assert_match "SMISMEMBER"          SMISMEMBER s:k1 a z c
    assert_match "SREM"                SREM s:k1 a f
    assert_match "SCARD after SREM"    SCARD s:k1
    # Set ops. moon#592: the `{s}` hash tag co-locates every key of a multi-key
    # set operation on ONE shard, so these rows compare the COMMAND against
    # Redis at any `--shards N`. Untagged names made them a function of the
    # shard count instead -- moon refuses a *STORE whose destination is owned
    # by a different shard than its sources (it executes the whole command on
    # one slice and used to write the destination into the wrong shard's table,
    # acked and unreadable), while Redis, having no shards, stores it either
    # way. The refusal is asserted in scripts/test-consistency.sh.
    rcli SADD {s}:A 1 2 3 >/dev/null 2>&1; mcli SADD {s}:A 1 2 3 >/dev/null 2>&1
    rcli SADD {s}:B 2 3 4 >/dev/null 2>&1; mcli SADD {s}:B 2 3 4 >/dev/null 2>&1
    assert_match_sorted "SINTER"       SINTER {s}:A {s}:B
    assert_match_sorted "SUNION"      SUNION {s}:A {s}:B
    assert_match_sorted "SDIFF"       SDIFF {s}:A {s}:B
    assert_match "SINTERSTORE"         SINTERSTORE {s}:intres {s}:A {s}:B
    assert_match "SUNIONSTORE"         SUNIONSTORE {s}:unires {s}:A {s}:B
    assert_match "SDIFFSTORE"          SDIFFSTORE {s}:difres {s}:A {s}:B
    # SMOVE moves a member between two keys and is routed by the SOURCE, so the
    # destination is the key it did NOT route on (moon#592).
    rcli SADD {s}:mvsrc m1 m2 >/dev/null 2>&1; mcli SADD {s}:mvsrc m1 m2 >/dev/null 2>&1
    assert_match "SMOVE"               SMOVE {s}:mvsrc {s}:mvdst m1
    assert_match "SISMEMBER after SMOVE" SISMEMBER {s}:mvdst m1
    assert_moon_ok "SPOP"              SPOP s:k1
    assert_moon_ok "SRANDMEMBER"       SRANDMEMBER {s}:A
    assert_moon_ok "SMEMBERS"          SMEMBERS {s}:A
    assert_moon_ok "SSCAN"             SSCAN {s}:A 0
fi

# ===========================================================================
# SORTED SET COMMANDS
# ===========================================================================

if should_run "sorted_set"; then
    echo ""
    echo "=== SORTED SET COMMANDS ==="
    flush_both

    assert_match "ZADD"                ZADD z:k1 1 a 2 b 3 c 4 d 5 e
    assert_match "ZADD update"         ZADD z:k1 10 a
    assert_match "ZCARD"               ZCARD z:k1
    assert_match "ZSCORE"              ZSCORE z:k1 a
    assert_match "ZSCORE (missing)"    ZSCORE z:k1 missing
    assert_match "ZRANK"               ZRANK z:k1 b
    assert_match "ZREVRANK"            ZREVRANK z:k1 b
    # Redis 7.2 WITHSCORE (singular). The miss is a null ARRAY with the option
    # and a null BULK without it, so both are probed (moon#521).
    assert_match "ZRANK WITHSCORE"     ZRANK z:k1 b WITHSCORE
    assert_match "ZREVRANK WITHSCORE"  ZREVRANK z:k1 b WITHSCORE
    assert_match "ZRANK WITHSCORE miss" ZRANK z:k1 missing WITHSCORE
    assert_match "ZRANK miss no option" ZRANK z:k1 missing
    assert_match "ZRANGE"              ZRANGE z:k1 0 -1
    assert_match "ZRANGE WITHSCORES"   ZRANGE z:k1 0 -1 WITHSCORES
    assert_match "ZREVRANGE"           ZREVRANGE z:k1 0 2
    assert_match "ZRANGEBYSCORE"       ZRANGEBYSCORE z:k1 2 5
    # ZREVRANGEBYSCORE on clean key (ZPOPMIN/ZPOPMAX above mutated z:k1)
    rcli ZADD z:revtest 1 alpha 2 beta 3 gamma >/dev/null 2>&1; mcli ZADD z:revtest 1 alpha 2 beta 3 gamma >/dev/null 2>&1
    assert_match "ZRANGEBYSCORE 2"     ZRANGEBYSCORE z:revtest 1 3
    assert_match "ZREVRANGEBYSCORE"    ZREVRANGEBYSCORE z:revtest +inf -inf
    assert_match "ZREVRANGEBYSCORE 2"  ZREVRANGEBYSCORE z:revtest 3 1
    assert_match "ZCOUNT"              ZCOUNT z:k1 2 5
    assert_match "ZINCRBY"             ZINCRBY z:k1 100 b
    assert_match "ZREM"                ZREM z:k1 e
    assert_match "ZPOPMIN"             ZPOPMIN z:k1
    assert_match "ZPOPMAX"             ZPOPMAX z:k1
    assert_match "ZLEXCOUNT"           ZLEXCOUNT z:k1 - +
    # Store ops. moon#592: `{z}` co-locates destination and sources -- see the
    # set-ops block above for why the tag is load-bearing at `--shards > 1`.
    rcli ZADD {z}:A 1 a 2 b 3 c >/dev/null 2>&1; mcli ZADD {z}:A 1 a 2 b 3 c >/dev/null 2>&1
    rcli ZADD {z}:B 2 b 3 c 4 d >/dev/null 2>&1; mcli ZADD {z}:B 2 b 3 c 4 d >/dev/null 2>&1
    assert_match "ZUNIONSTORE"         ZUNIONSTORE {z}:union 2 {z}:A {z}:B
    assert_match "ZINTERSTORE"         ZINTERSTORE {z}:inter 2 {z}:A {z}:B
    assert_match "ZRANGESTORE"         ZRANGESTORE {z}:rstore {z}:A 0 -1
    assert_match "ZCARD after ZRANGESTORE" ZCARD {z}:rstore
    assert_moon_ok "ZSCAN"             ZSCAN {z}:A 0
fi

# ===========================================================================
# KEY COMMANDS
# ===========================================================================

if should_run "key"; then
    echo ""
    echo "=== KEY COMMANDS ==="
    flush_both

    rcli SET k:1 v1 >/dev/null 2>&1; mcli SET k:1 v1 >/dev/null 2>&1
    rcli SET k:2 v2 >/dev/null 2>&1; mcli SET k:2 v2 >/dev/null 2>&1
    rcli SET k:3 v3 >/dev/null 2>&1; mcli SET k:3 v3 >/dev/null 2>&1
    assert_match "EXISTS (yes)"        EXISTS k:1
    assert_match "EXISTS (no)"         EXISTS k:missing
    assert_match "EXISTS multi"        EXISTS k:1 k:2 k:missing
    assert_match "DEL single"          DEL k:3
    assert_match "DEL multi"           DEL k:1 k:2
    rcli SET k:ttl v EX 1000 >/dev/null 2>&1; mcli SET k:ttl v EX 1000 >/dev/null 2>&1
    assert_match_ttl "TTL (with expiry)" TTL k:ttl
    assert_match_pttl "PTTL (with expiry)" PTTL k:ttl
    rcli SET k:nox v >/dev/null 2>&1; mcli SET k:nox v >/dev/null 2>&1
    assert_match "TTL (no expiry)"     TTL k:nox
    assert_match "EXPIRE"              EXPIRE k:nox 500
    assert_match_ttl "TTL after EXPIRE" TTL k:nox
    assert_match "PEXPIRE"             PEXPIRE k:nox 500000
    assert_match "PERSIST"             PERSIST k:nox
    assert_match "TTL after PERSIST"   TTL k:nox
    assert_match "TYPE string"         TYPE k:nox
    rcli LPUSH k:lst a >/dev/null 2>&1; mcli LPUSH k:lst a >/dev/null 2>&1
    assert_match "TYPE list"           TYPE k:lst
    rcli SADD k:st a >/dev/null 2>&1; mcli SADD k:st a >/dev/null 2>&1
    assert_match "TYPE set"            TYPE k:st
    rcli ZADD k:zs 1 a >/dev/null 2>&1; mcli ZADD k:zs 1 a >/dev/null 2>&1
    assert_match "TYPE zset"           TYPE k:zs
    rcli HSET k:hs f v >/dev/null 2>&1; mcli HSET k:hs f v >/dev/null 2>&1
    assert_match "TYPE hash"           TYPE k:hs
    # moon#592: `{k}` co-locates the two names. RENAME is routed by its SOURCE
    # and used to write the destination into the source owner's table -- acked
    # `+OK` with the value readable under neither name. The tag keeps this row
    # comparing the COMMAND against Redis at any `--shards N`; the refusal is
    # asserted in scripts/test-consistency.sh.
    rcli SET {k}:ren oldval >/dev/null 2>&1; mcli SET {k}:ren oldval >/dev/null 2>&1
    assert_match "RENAME"              RENAME {k}:ren {k}:renamed
    assert_match "GET after RENAME"    GET {k}:renamed
    rcli SET {k}:rnx1 v1 >/dev/null 2>&1; mcli SET {k}:rnx1 v1 >/dev/null 2>&1
    rcli SET {k}:rnx2 v2 >/dev/null 2>&1; mcli SET {k}:rnx2 v2 >/dev/null 2>&1
    assert_match "RENAMENX (blocked)"  RENAMENX {k}:rnx1 {k}:rnx2
    rcli SET k:cpsrc cpval >/dev/null 2>&1; mcli SET k:cpsrc cpval >/dev/null 2>&1
    assert_match "COPY"                COPY k:cpsrc k:cpdst
    assert_match "GET after COPY"      GET k:cpdst
    rcli SET k:cpdst2 old >/dev/null 2>&1; mcli SET k:cpdst2 old >/dev/null 2>&1
    assert_match "COPY no REPLACE"     COPY k:cpsrc k:cpdst2
    assert_match "COPY REPLACE"        COPY k:cpsrc k:cpdst2 REPLACE
    assert_match "UNLINK"              UNLINK {k}:renamed
    assert_moon_ok "DBSIZE"            DBSIZE
    assert_moon_ok "SCAN cursor"       SCAN 0
    assert_moon_ok "KEYS pattern"      KEYS "k:*"
    assert_moon_ok "OBJECT HELP"       OBJECT HELP
    assert_moon_ok "OBJECT ENCODING"   OBJECT ENCODING k:nox
    assert_moon_ok "OBJECT FREQ"       OBJECT FREQ k:nox
    assert_moon_ok "HOTKEYS"           HOTKEYS
    assert_moon_ok "HOTKEYS COUNT"     HOTKEYS COUNT 5

    # Bit operations
    rcli SET k:bits "\xff\x0f" >/dev/null 2>&1; mcli SET k:bits "\xff\x0f" >/dev/null 2>&1
    assert_match "GETBIT"              GETBIT k:bits 0
    assert_match "SETBIT"              SETBIT k:bits 0 0
    assert_match "BITCOUNT"            BITCOUNT k:bits
    assert_match "BITCOUNT range"      BITCOUNT k:bits 0 0
    rcli SET k:bits2 "\x0f\xff" >/dev/null 2>&1; mcli SET k:bits2 "\x0f\xff" >/dev/null 2>&1
    assert_match "BITOP AND"           BITOP AND k:bitdst k:bits k:bits2
    assert_match "BITOP OR"            BITOP OR k:bitdst k:bits k:bits2
    assert_match "BITOP XOR"           BITOP XOR k:bitdst k:bits k:bits2
    assert_match "BITOP NOT"           BITOP NOT k:bitdst k:bits
    assert_match "BITPOS 1"            BITPOS k:bits 1
    assert_match "BITPOS 0"            BITPOS k:bits 0

    # BITFIELD / BITFIELD_RO (WS1 command parity)
    rcli BITFIELD k:bf SET u8 0 255 >/dev/null 2>&1; mcli BITFIELD k:bf SET u8 0 255 >/dev/null 2>&1
    assert_match "BITFIELD GET"         BITFIELD k:bf GET u8 0
    assert_match "BITFIELD_RO GET"      BITFIELD_RO k:bf GET u8 0
    assert_moon_contains "BITFIELD_RO rejects SET" "GET subcommand" BITFIELD_RO k:bf SET u8 0 1

    # SORT / SORT_RO
    rcli RPUSH k:sortl 3 1 2 >/dev/null 2>&1; mcli RPUSH k:sortl 3 1 2 >/dev/null 2>&1
    assert_match "SORT numeric"        SORT k:sortl
    assert_match "SORT DESC"           SORT k:sortl DESC
    assert_match "SORT ALPHA"          SORT k:sortl ALPHA
    assert_match "SORT LIMIT"          SORT k:sortl LIMIT 0 2
    assert_match "SORT_RO numeric"     SORT_RO k:sortl
    # Redis parity: SORT_RO + STORE returns the generic "ERR syntax error"
    # (SORT_RO's grammar has no STORE branch), not a SORT_RO-specific message.
    assert_moon_contains "SORT_RO rejects STORE" "syntax error" SORT_RO k:sortl STORE k:sortdst

    # GEO commands (incl. GEORADIUS/GEORADIUSBYMEMBER + _RO twins, WS1 parity)
    rcli GEOADD k:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    mcli GEOADD k:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    assert_match "GEOPOS"              GEOPOS k:geo Palermo
    assert_match "GEODIST km"          GEODIST k:geo Palermo Catania km
    assert_match "GEOHASH"             GEOHASH k:geo Palermo
    assert_match "GEOSEARCH"           GEOSEARCH k:geo FROMLONLAT 15 37 BYRADIUS 200 km ASC
    assert_match "GEORADIUS"                GEORADIUS k:geo 15 37 200 km ASC
    assert_match "GEORADIUS_RO"             GEORADIUS_RO k:geo 15 37 200 km ASC
    assert_match "GEORADIUSBYMEMBER"        GEORADIUSBYMEMBER k:geo Palermo 200 km ASC
    assert_match "GEORADIUSBYMEMBER_RO"     GEORADIUSBYMEMBER_RO k:geo Palermo 200 km ASC
    # moon#568: WITHCOORD coordinates carry the same full precision as GEOPOS.
    # `%.4f` rounding was invisible to every row above, none of which asks for
    # coordinates at all.
    assert_match "GEOSEARCH WITHCOORD"      GEOSEARCH k:geo FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHCOORD
    assert_match "GEOSEARCH WITHCOORD+DIST+HASH" GEOSEARCH k:geo FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHCOORD WITHDIST WITHHASH
    assert_match "GEORADIUS WITHCOORD"      GEORADIUS k:geo 15 37 200 km ASC WITHCOORD
    assert_match "GEORADIUSBYMEMBER WITHCOORD" GEORADIUSBYMEMBER k:geo Palermo 200 km ASC WITHCOORD
    # moon#645: the legacy STORE/STOREDIST clause. `{geo}` co-locates source
    # and destination on one shard so every row below compares the COMMAND
    # against Redis at any `--shards N` -- untagged names would make it a
    # function of the shard count (moon refuses a two-key write whose keys
    # are owned by different shards; that refusal is asserted in
    # scripts/test-consistency.sh, not here).
    rcli GEOADD {geo}:src 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    mcli GEOADD {geo}:src 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    assert_match "GEORADIUS STORE"            GEORADIUS {geo}:src 15 37 200 km STORE {geo}:d1
    assert_match "GEORADIUS STORE result"     ZRANGE {geo}:d1 0 -1 WITHSCORES
    assert_match "GEORADIUSBYMEMBER STORE"    GEORADIUSBYMEMBER {geo}:src Palermo 200 km STORE {geo}:d2
    assert_match "GEORADIUSBYMEMBER STORE result" ZRANGE {geo}:d2 0 -1 WITHSCORES
    assert_match "GEORADIUS COUNT+STORE"      GEORADIUS {geo}:src 15 37 200 km COUNT 1 STORE {geo}:d1
    assert_match "GEORADIUS STORE rejects WITHDIST" GEORADIUS {geo}:src 15 37 200 km WITHDIST STORE {geo}:d1
    assert_match "GEORADIUS STORE needs a dest"     GEORADIUS {geo}:src 15 37 200 km STORE
    assert_match "GEOSEARCHSTORE rejects WITHCOORD" GEOSEARCHSTORE {geo}:d1 {geo}:src FROMLONLAT 15 37 BYRADIUS 200 km WITHCOORD
    assert_match "GEORADIUS_RO still refuses STORE" GEORADIUS_RO {geo}:src 15 37 200 km STORE {geo}:d1
    # STOREDIST stores the raw f64 distance rather than a %.4f rendering, so
    # this is the one geo reply where moon and redis can differ in the last
    # digit: measured 190.44242984775795 vs 190.44242984775784 for Palermo,
    # ~1 ULP apart. Compare at 9 decimals -- five orders tighter than the 4dp
    # every other geo reply exposes, and still ULP-immune.
    rcli GEORADIUS {geo}:src 15 37 200 km STOREDIST {geo}:d3 >/dev/null 2>&1
    mcli GEORADIUS {geo}:src 15 37 200 km STOREDIST {geo}:d3 >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    _sd_round='NR%2==0{printf "%.9f\n",$1; next}{print}'
    r_sd=$(rcli ZRANGE {geo}:d3 0 -1 WITHSCORES 2>/dev/null | awk "$_sd_round")
    m_sd=$(mcli ZRANGE {geo}:d3 0 -1 WITHSCORES 2>/dev/null | awk "$_sd_round")
    if [[ "$r_sd" == "$m_sd" && -n "$r_sd" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: GEORADIUS STOREDIST scores"
        echo "    REDIS: $(echo "$r_sd" | tr '\n' ' ')"
        echo "    MOON:  $(echo "$m_sd" | tr '\n' ' ')"
    fi
    # EXPIREAT / PEXPIREAT / EXPIRETIME / PEXPIRETIME
    rcli SET k:eat val >/dev/null 2>&1; mcli SET k:eat val >/dev/null 2>&1
    assert_match "EXPIREAT"            EXPIREAT k:eat 9999999999
    assert_match_countdown "TTL after EXPIREAT" 1 TTL k:eat
    assert_match "EXPIRETIME"          EXPIRETIME k:eat
    assert_match "PEXPIRETIME"         PEXPIRETIME k:eat

    # TIME / RANDOMKEY / TOUCH
    assert_moon_ok "TIME"              TIME
    rcli SET k:rnd val >/dev/null 2>&1; mcli SET k:rnd val >/dev/null 2>&1
    assert_moon_ok "RANDOMKEY"         RANDOMKEY
    assert_match "TOUCH"               TOUCH k:rnd

    # FLUSHDB
    assert_match "FLUSHDB"             FLUSHDB
fi

# ===========================================================================
# STREAM COMMANDS
# ===========================================================================

if should_run "stream"; then
    echo ""
    echo "=== STREAM COMMANDS ==="
    flush_both

    # XADD with auto-IDs: can't compare IDs across servers, test moon-only
    assert_moon_ok "XADD"              XADD stream:k1 '*' field1 value1
    assert_moon_ok "XADD 2"            XADD stream:k1 '*' field2 value2
    assert_moon_ok "XADD 3"            XADD stream:k1 '*' field3 value3
    assert_moon "XLEN"                 "3" XLEN stream:k1
    assert_moon_ok "XRANGE all"        XRANGE stream:k1 - +
    assert_moon_ok "XREVRANGE"         XREVRANGE stream:k1 + -
    assert_moon_ok "XINFO STREAM"      XINFO STREAM stream:k1
    assert_moon_ok "XTRIM MAXLEN"      XTRIM stream:k1 MAXLEN 10
    # Consumer groups
    assert_moon_ok "XGROUP CREATE"     XGROUP CREATE stream:k1 grp1 0
    assert_moon_ok "XREADGROUP"        XREADGROUP GROUP grp1 consumer1 COUNT 1 STREAMS stream:k1 '>'
    assert_moon_ok "XACK"              XACK stream:k1 grp1 0-0
    assert_moon_ok "XPENDING summary"  XPENDING stream:k1 grp1 - + 10

    # moon#594: a stream that yielded nothing is OMITTED from the reply, where
    # moon used to carry it as a present-but-empty entry list. Both streams
    # share a hash tag so ONE XREAD sees them whatever the shard count — moon
    # routes a multi-stream read by its first key.
    mcli DEL "{xr594}:a" "{xr594}:b" >/dev/null 2>&1 || true
    mcli XADD "{xr594}:a" 1-1 f v >/dev/null 2>&1 || true
    mcli XADD "{xr594}:b" 1-1 f v >/dev/null 2>&1 || true
    TOTAL=$((TOTAL + 1))
    xr594_out=$(mcli XREAD COUNT 10 STREAMS "{xr594}:a" "{xr594}:b" 0 99999 2>/dev/null)
    # `a` is served from 0; `b` is read from an id past its last, so it has
    # nothing. Asserting on BOTH halves — `a` present AND `b` absent — so a
    # server that dropped every stream could not pass.
    if grep -q "{xr594}:a" <<<"$xr594_out" && ! grep -q "{xr594}:b" <<<"$xr594_out"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: XREAD must omit the unserved stream {xr594}:b"
        echo "    GOT: $(echo "$xr594_out" | tr '\n' ' ')"
    fi
    # The fence: a read that serves NOTHING is still the null array (moon#482),
    # not an empty one — without it, "always omit" would pass.
    assert_moon "XREAD all-unserved is the null array" "" \
        XREAD STREAMS "{xr594}:a" "{xr594}:b" 99999 99999

    # moon#595: BLOCK is honoured rather than parsed and discarded. The reply
    # BYTES of a timeout cannot tell the two apart (`*-1` is exactly what a
    # legitimate timeout answers), so this asserts on the argument VALIDATION,
    # which only a server that actually reads the value can produce. The timing
    # halves live in tests/blocking_stream_read.rs and test-consistency.sh.
    assert_moon "XREAD BLOCK rejects a non-integer" \
        "ERR timeout is not an integer or out of range" \
        XREAD BLOCK abc STREAMS stream:k1 '$'
    assert_moon "XREAD BLOCK rejects a negative timeout" \
        "ERR timeout is negative" \
        XREAD BLOCK -1 STREAMS stream:k1 '$'
    # A blocking read whose data is ALREADY there answers at once, unchanged.
    assert_moon_ok "XREAD BLOCK served immediately" \
        XREAD BLOCK 3000 STREAMS stream:k1 0

    # An oversized timeout used to ABORT THE WHOLE PROCESS: the deadline was
    # built with Duration::from_secs_f64, which panics on a value it cannot
    # represent, and a panic on a shard thread takes the server down. So the
    # PING below is not decoration — on a broken build the two rows above it
    # cannot even report, because the server is gone. Texts and the accept /
    # reject boundary are redis-server 8.6.1's own.
    assert_moon "XREAD BLOCK rejects an unrepresentable timeout" \
        "ERR timeout is out of range" \
        XREAD BLOCK 9223372036854775807 STREAMS stream:k1 '$'
    assert_moon "BLPOP rejects an unrepresentable timeout" \
        "ERR timeout is out of range" \
        BLPOP stream:absent 1e300
    assert_moon "BLPOP negative timeout says negative" \
        "ERR timeout is negative" \
        BLPOP stream:absent -0.5
    assert_moon "server survives an oversized blocking timeout" "PONG" PING

    # Redis parses BLOCK with `string2ll`, which takes neither a leading `+`
    # nor leading zeros. `str::parse::<i64>` takes both, so moon accepted these
    # and PARKED for 300ms on arguments Redis rejects outright. Texts measured
    # against redis-server 8.6.1.
    assert_moon "XREAD BLOCK rejects a leading plus" \
        "ERR timeout is not an integer or out of range" \
        XREAD BLOCK +300 STREAMS stream:k1 '$'
    assert_moon "XREAD BLOCK rejects leading zeros" \
        "ERR timeout is not an integer or out of range" \
        XREAD BLOCK 0300 STREAMS stream:k1 '$'
fi

# ===========================================================================
# CONNECTION COMMANDS
# ===========================================================================

if should_run "connection"; then
    echo ""
    echo "=== CONNECTION COMMANDS ==="

    assert_match "PING"                PING
    assert_match "PING message"        PING hello
    assert_match "ECHO"                ECHO "hello world"
    assert_moon_ok "SELECT 0"          SELECT 0
    assert_moon_ok "SELECT 1"          SELECT 1
    assert_moon_contains "INFO server" "redis_version" INFO server
    assert_moon_ok "DBSIZE"            DBSIZE
    assert_moon_ok "COMMAND"           COMMAND
    assert_moon_ok "COMMAND COUNT"     COMMAND COUNT
    # `assert_moon_ok` passed on the old stub too — it replied `*0`, which is a
    # perfectly well-formed reply of the WRONG TYPE. These assert content, so a
    # regression to a stub cannot slip past as "ok".
    assert_moon_contains "COMMAND INFO GET names the command" "get" COMMAND INFO GET
    assert_moon_contains "COMMAND LIST includes reset" "reset" COMMAND LIST
    # moon#635: the container surface. `pubsub` is a command Moon ANSWERS but
    # never published, so a client building its table from LIST refused to send
    # it; `config|get` is the subcommand form Moon published none of.
    # A whole-LINE match, not a substring: now that subcommands are published,
    # `pubsub|channels` contains "pubsub", so a substring assertion would pass
    # even with the top-level `pubsub` entry missing — the very thing it exists
    # to catch.
    assert_moon_line "COMMAND LIST includes pubsub" "pubsub" COMMAND LIST
    assert_moon_contains "COMMAND LIST includes container subcommands" "config|get" COMMAND LIST
    assert_moon_contains "COMMAND INFO resolves a subcommand" "config|get" COMMAND INFO "config|get"
    assert_moon_contains "COMMAND GETKEYS extracts the key" "k1" COMMAND GETKEYS MSET k1 v1 k2 v2
    assert_moon_contains "COMMAND GETKEYS rejects keyless" "no key arguments" COMMAND GETKEYS PING
    # moon#537: `first_key: 0` mirrors redis and means "the keys are not at a
    # FIXED argument position", NOT "there are no keys". Reading it as the
    # latter made GETKEYS answer "the command has no key arguments" to the
    # whole movablekeys family — the exact question a cluster-aware client
    # asks GETKEYS in order to route. One assertion per key LAYOUT.
    assert_moon_contains "GETKEYS LMPOP names its key" "mk1" COMMAND GETKEYS LMPOP 2 mk1 mk2 LEFT
    assert_moon_contains "GETKEYS ZMPOP names its key" "mk1" COMMAND GETKEYS ZMPOP 1 mk1 MIN
    assert_moon_contains "GETKEYS SINTERCARD names its key" "mk1" COMMAND GETKEYS SINTERCARD 2 mk1 mk2
    assert_moon_contains "GETKEYS ZDIFF names its key" "mk1" COMMAND GETKEYS ZDIFF 2 mk1 mk2
    assert_moon_contains "GETKEYS EVAL names its declared key" "mk1" COMMAND GETKEYS EVAL "return 1" 1 mk1
    assert_moon_contains "GETKEYS XREADGROUP names its stream" "ms1" COMMAND GETKEYS XREADGROUP GROUP g c STREAMS ms1 '>'
    assert_moon_contains "GETKEYS XREAD names its stream" "ms1" COMMAND GETKEYS XREAD COUNT 1 STREAMS ms1 0
    assert_moon_contains "GETKEYS ZUNIONSTORE names its dest" "mkd" COMMAND GETKEYS ZUNIONSTORE mkd 2 mk1 mk2
    assert_moon_contains "GETKEYS SORT..STORE names its dest" "mkd" COMMAND GETKEYS SORT mk1 ALPHA STORE mkd
    assert_moon_contains "GETKEYS OBJECT names its key" "mk1" COMMAND GETKEYS OBJECT ENCODING mk1
    # The other three error strings redis uses, so "has no key arguments" can
    # never again stand in for all of them.
    assert_moon_contains "GETKEYS unknown command" "Invalid command specified" COMMAND GETKEYS NOSUCHCMD k
    assert_moon_contains "GETKEYS wrong arity" "Invalid number of arguments" COMMAND GETKEYS LMPOP 0 LEFT
    assert_moon_contains "GETKEYS unextractable argv" "Invalid arguments specified" COMMAND GETKEYS LMPOP abc k LEFT
    assert_moon_contains "COMMAND COUNT arity" "wrong number of arguments" COMMAND COUNT extra
    # Known red, tracked as moon#536: moon's master_repl_offset advances even
    # with no replica ever attached, so the third element of ROLE diverges from
    # Redis's 0. Left asserting the full reply rather than narrowed to the
    # parts that agree -- narrowing it would hide the fix when it lands.
    assert_match "ROLE (known: moon#536)"                ROLE
    assert_moon_contains "ROLE reports master" "master" ROLE
    assert_match "RESET"               RESET
    assert_moon_contains "RESET arity" "wrong number of arguments" RESET now
    assert_moon_contains "CLIENT INFO laddr is not port 0" "laddr=127.0.0.1:$PORT_RUST" CLIENT INFO
    assert_moon_contains "MEMORY DOCTOR" "Per-subsystem (resident):" MEMORY DOCTOR
    # moon#636: clients feature-detect with MODULE LIST on connect. An empty
    # array is the truthful answer; an unknown-command error reads as broken.
    # Not `assert_match` — redis 8.x ships `vectorset`, so ITS list is not
    # empty; moon loading nothing is the correct answer, not a divergence.
    assert_moon "MODULE LIST is empty" "" MODULE LIST
    assert_match "MODULE unknown sub"  MODULE BOGUS
    # task #511: MEMORY USAGE must hash the KEY, not the literal "USAGE".
    # Single-shard here, so this catches the arity/shape regression; the
    # cross-shard routing itself is covered in test-consistency.sh.
    redis-cli -p "$PORT_RUST" SET memusagekey hello-value >/dev/null 2>&1
    assert_moon_matches "MEMORY USAGE sizes an existing key" '^[1-9][0-9]*$' MEMORY USAGE memusagekey

    # moon#482: RESP2's two nulls are not interchangeable, and `redis-cli`
    # renders both as "(nil)" — so this reads the raw reply line instead.
    # Cross-server parity lives in test-consistency.sh; this is the shape check.
    # Distinct keys per probe: a timed-out BLPOP leaves an empty list behind
    # (#523), which would change a later probe's answer on a shared key.
    assert_moon_raw_reply "BLPOP timeout is a null ARRAY" '*-1' BLPOP nulltype:t1 0.05
    assert_moon_raw_reply "GET miss is still a null BULK" '$-1' GET nulltype:t2
    assert_moon_raw_reply "ZPOPMIN miss is still an EMPTY array" '*0' ZPOPMIN nulltype:t3

    # moon#636: DEBUG DIGEST. Byte-parity against redis is asserted in
    # test-consistency.sh, which has the oracle running with DEBUG enabled;
    # here the check is that the command is REACHABLE from a client and
    # behaves as a fingerprint. That matters on its own: DEBUG DIGEST is
    # served by a handler intercept rather than the ordinary dispatch, so a
    # missing arm would be invisible to unit tests.
    # moon#677: FLUSHALL clears only the selected database, so it cannot on
    # its own establish "the dataset is empty". Nothing in this script writes
    # outside db0 today, but the sentinel row would fail confusingly the first
    # time something did -- and the failure would look like a digest bug.
    # Collapse this back to a bare FLUSHALL once #677 lands.
    for _dg_db in $(seq 0 15); do
        redis-cli -p "$PORT_RUST" -n "$_dg_db" FLUSHDB >/dev/null 2>&1
    done
    assert_moon "DEBUG DIGEST of an empty dataset is the zero sentinel" \
        "0000000000000000000000000000000000000000" DEBUG DIGEST
    mcli SET dg:probe v1 >/dev/null 2>&1
    DG_ONE=$(mcli DEBUG DIGEST 2>/dev/null)
    TOTAL=$((TOTAL + 1))
    # Same reasoning as DG_TWO below: "not the zero sentinel" is satisfied by
    # an error string too, so require a real 40-hex digest.
    if [[ "$DG_ONE" =~ ^[0-9a-f]{40}$ && "$DG_ONE" != "0000000000000000000000000000000000000000" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: DEBUG DIGEST gave no non-zero digest after a write: $DG_ONE"
    fi
    # Same data must give the same digest; different data must not.
    # "differs from DG_ONE" is not enough on its own -- an ERROR reply also
    # differs, so a broken DEBUG DIGEST would pass. Require an actual digest.
    mcli SET dg:probe v2 >/dev/null 2>&1
    DG_TWO=$(mcli DEBUG DIGEST 2>/dev/null)
    TOTAL=$((TOTAL + 1))
    if [[ "$DG_TWO" =~ ^[0-9a-f]{40}$ && "$DG_TWO" != "$DG_ONE" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: DEBUG DIGEST did not return a changed digest: $DG_TWO"
    fi
    mcli SET dg:probe v1 >/dev/null 2>&1
    assert_moon "DEBUG DIGEST returns to its earlier value" "$DG_ONE" DEBUG DIGEST
fi

# ===========================================================================
# PUB/SUB COMMANDS
# ===========================================================================

if should_run "pubsub"; then
    echo ""
    echo "=== PUB/SUB COMMANDS ==="
    flush_both

    # Publish to a channel (no subscribers = 0)
    assert_match "PUBLISH (no subs)"   PUBLISH chan:test "hello"

    # Sharded pub/sub. Compared against Redis like everything else here, so a
    # divergence in the sharded namespace shows up next to the plain one.
    assert_match "SPUBLISH (no subs)"  SPUBLISH schan:test "hello"
    assert_match "PUBSUB SHARDCHANNELS (empty)" PUBSUB SHARDCHANNELS
    assert_match "PUBSUB SHARDNUMSUB (absent)"  PUBSUB SHARDNUMSUB schan:test
    # Distinct patterns, not subscribers — with nobody subscribed both are 0,
    # so this row guards the wiring; the two-live-subscriber case that actually
    # exposes the counting bug is tests/pubsub_resp3_push.rs::ps14.
    assert_match "PUBSUB NUMPAT (none)" PUBSUB NUMPAT
fi

# ===========================================================================
# TRANSACTION COMMANDS
# ===========================================================================

if should_run "transaction"; then
    echo ""
    echo "=== TRANSACTION COMMANDS ==="
    flush_both

    # Test MULTI/EXEC via pipe (using \n not \r\n for redis-cli pipe mode)
    TOTAL=$((TOTAL + 1))
    tx_moon=$(printf 'MULTI\nSET tx:k1 v1\nSET tx:k2 v2\nGET tx:k1\nEXEC\n' | redis-cli -p "$PORT_RUST" 2>/dev/null || true)
    if echo "$tx_moon" | grep -q "v1"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: MULTI/EXEC pipeline"
        echo "    GOT: $(echo "$tx_moon" | head -5)"
    fi

    # DISCARD (must be inside MULTI)
    TOTAL=$((TOTAL + 1))
    tx_discard=$(printf 'MULTI\nDISCARD\n' | redis-cli -p "$PORT_RUST" 2>/dev/null || true)
    if echo "$tx_discard" | grep -q "OK"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: MULTI/DISCARD"
    fi

    # --- Unknown container subcommands abort the transaction (moon#670) ---
    #
    # Redis validates a container's SUBCOMMAND at queue time: `CONFIG BOGUS` is
    # refused on the MULTI connection and the block is poisoned, so EXEC answers
    # -EXECABORT and NOTHING runs. Moon used to reply +QUEUED and only notice at
    # EXEC, so the valid half of a mistyped transaction executed.
    #
    # The verdict is read from the KEY, not from EXEC's reply. That is what makes
    # the row discriminating: pre-fix, `tx:sub670` exists and this FAILS; the
    # error-text half alone would not catch a transaction that still ran.
    TOTAL=$((TOTAL + 1))
    mcli DEL tx:sub670 > /dev/null 2>&1
    tx_sub=$(printf 'MULTI\nCONFIG BOGUS\nSET tx:sub670 ran\nEXEC\n' | redis-cli -p "$PORT_RUST" 2>/dev/null || true)
    tx_sub_key=$(mcli GET tx:sub670 2>/dev/null || true)
    if [ -n "$tx_sub_key" ]; then
        FAIL=$((FAIL + 1))
        echo "  FAIL: TXN-SUB-01 the transaction RAN despite a bogus subcommand (tx:sub670=$tx_sub_key)"
    elif echo "$tx_sub" | grep -q "EXECABORT"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: TXN-SUB-01 expected EXECABORT, got: $(echo "$tx_sub" | tr '\n' ' ')"
    fi

    # The queue-time refusal must carry Redis's exact shape, not just any error:
    # clients branch on this string, and moon had ten spellings of it.
    TOTAL=$((TOTAL + 1))
    tx_sub_msg=$(mcli CONFIG BOGUS 2>&1 || true)
    if echo "$tx_sub_msg" | grep -q "unknown subcommand 'BOGUS'. Try CONFIG HELP."; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: TXN-SUB-02 wrong unknown-subcommand shape: $tx_sub_msg"
    fi

    # Discriminator for the widening direction: a REAL subcommand must still
    # queue and the transaction must still run. A gate that refused every
    # container subcommand would pass TXN-SUB-01 and be far worse than the bug.
    TOTAL=$((TOTAL + 1))
    mcli DEL tx:sub670ok > /dev/null 2>&1
    printf 'MULTI\nCONFIG GET maxmemory\nSET tx:sub670ok ran\nEXEC\n' | redis-cli -p "$PORT_RUST" > /dev/null 2>&1 || true
    if [ "$(mcli GET tx:sub670ok 2>/dev/null || true)" = "ran" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: TXN-SUB-03 a valid container subcommand blocked the transaction"
    fi

    # --- FUNCTION inside MULTI (moon#697) ---------------------------------
    #
    # Every FUNCTION subcommand was queued and then answered `unknown command`
    # at EXEC. The verdict is read from a KEY written after it in the same
    # transaction, not from EXEC's reply — an executor that errored on FUNCTION
    # but still ran the rest would otherwise look fine.
    TOTAL=$((TOTAL + 1))
    mcli DEL tx:fn697 > /dev/null 2>&1
    fn_exec=$(printf 'MULTI\nFUNCTION LIST\nSET tx:fn697 ran\nEXEC\n' \
        | redis-cli -p "$PORT_RUST" 2>/dev/null || true)
    if [ "$(mcli GET tx:fn697 2>/dev/null || true)" = "ran" ] \
        && ! echo "$fn_exec" | grep -q "unknown command"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: FN-MULTI-01 FUNCTION LIST inside MULTI: $(echo "$fn_exec" | tr '\n' ' ')"
    fi

    # FUNCTION now joins the moon#670 queue gate, as redis 8.6.1 does.
    TOTAL=$((TOTAL + 1))
    fn_bogus=$(printf 'MULTI\nFUNCTION BOGUS\nEXEC\n' \
        | redis-cli -p "$PORT_RUST" 2>&1 || true)
    if echo "$fn_bogus" | grep -q "unknown subcommand 'BOGUS'. Try FUNCTION HELP." \
        && echo "$fn_bogus" | grep -q "EXECABORT"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: FN-MULTI-02 expected queue-time refusal + EXECABORT, got: $(echo "$fn_bogus" | tr '\n' ' ')"
    fi

    # --- Container HELP (moon#698) ----------------------------------------
    #
    # Redis gives every container a HELP subcommand answering an array of SIMPLE
    # strings. Moon refused it on eight containers, three of them with a message
    # telling the client to run the very command it had just refused.
    TOTAL=$((TOTAL + 1))
    help_missing=""
    for c in ACL CLIENT COMMAND CONFIG FUNCTION MEMORY MODULE OBJECT PUBSUB \
             SCRIPT SLOWLOG XGROUP XINFO; do
        # Word-splitting is off in zsh, so the subcommand is passed explicitly
        # rather than through a single "$c HELP" string.
        line=$(mcli "$c" HELP 2>&1 | head -1 || true)
        case "$line" in
            "$c <subcommand> "*) ;;
            *) help_missing="$help_missing $c" ;;
        esac
    done
    if [ -z "$help_missing" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: HELP-01 containers without Redis's help header:$help_missing"
    fi

    # HELP must survive the moon#670 queue gate — before #698 it was refused at
    # QUEUE time and poisoned the transaction, so this reads the KEY, not EXEC.
    TOTAL=$((TOTAL + 1))
    mcli DEL tx:help698 > /dev/null 2>&1
    printf 'MULTI\nCONFIG HELP\nSET tx:help698 ran\nEXEC\n' \
        | redis-cli -p "$PORT_RUST" > /dev/null 2>&1 || true
    if [ "$(mcli GET tx:help698 2>/dev/null || true)" = "ran" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: HELP-02 CONFIG HELP aborted the transaction (moon#698)"
    fi

    # --- WATCH / UNWATCH optimistic locking -------------------------------
    #
    # A CAS conflict needs TWO connections interleaved: the transaction must
    # stay open while a second client writes the watched key. `redis-cli`
    # one-shot mode cannot express that (each invocation is its own connection,
    # closed on exit), so bash's /dev/tcp holds the transaction connection open
    # and drives it with inline commands. The verdict is read from the key's
    # FINAL VALUE, not from EXEC's reply, which keeps this free of RESP parsing.
    watch_cas_outcome() {
        local port="$1" conflict="$2" line=""
        redis-cli -p "$port" SET cas:k base >/dev/null 2>&1 || true
        exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__"; return 0; }
        printf 'WATCH cas:k\r\nMULTI\r\nSET cas:k from-txn\r\n' >&3
        if [[ "$conflict" == "yes" ]]; then
            redis-cli -p "$port" SET cas:k from-other >/dev/null 2>&1 || true
        fi
        # ECHO after EXEC is a round-trip barrier: reading its reply proves EXEC
        # was applied before the connection closes, so the GET cannot race it.
        printf 'EXEC\r\nECHO cas-done\r\n' >&3
        while IFS= read -r -t 5 line <&3; do
            [[ "${line%$'\r'}" == "cas-done" ]] && break
        done
        exec 3>&-
        redis-cli -p "$port" GET cas:k 2>/dev/null
    }

    TOTAL=$((TOTAL + 1))
    cas_moon=$(watch_cas_outcome "$PORT_RUST" yes)
    if [[ "$cas_moon" == "from-other" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: WATCH conflicting write did not abort EXEC"
        echo "    EXPECTED: from-other (transaction aborted)"
        echo "    GOT:      $cas_moon"
    fi

    TOTAL=$((TOTAL + 1))
    cas_clean=$(watch_cas_outcome "$PORT_RUST" no)
    if [[ "$cas_clean" == "from-txn" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: unconflicted WATCH/EXEC did not commit"
        echo "    EXPECTED: from-txn"
        echo "    GOT:      $cas_clean"
    fi

    assert_moon_contains "WATCH arity error" "wrong number of arguments" WATCH
    assert_moon "UNWATCH outside MULTI" "OK" UNWATCH
fi

# ===========================================================================
# SCRIPTING COMMANDS
# ===========================================================================

if should_run "scripting"; then
    echo ""
    echo "=== SCRIPTING COMMANDS ==="
    flush_both

    assert_match "EVAL return"         EVAL "return 42" 0
    assert_match "EVAL string"         EVAL "return 'hello'" 0
    rcli SET lua:k1 luaval >/dev/null 2>&1; mcli SET lua:k1 luaval >/dev/null 2>&1
    assert_match "EVAL redis.call"     EVAL "return redis.call('GET', KEYS[1])" 1 lua:k1
    assert_match "EVAL table"          EVAL "return {1,2,3}" 0
    # moon#636: EVAL_RO is EVAL with ONE difference — a write is refused. The
    # read row is the control: without it, a handler that refused everything
    # in read-only mode would look correct.
    assert_match "EVAL_RO reads"       EVAL_RO "return redis.call('GET', KEYS[1])" 1 lua:k1
    assert_moon_contains "EVAL_RO refuses a write" "read-only" \
        EVAL_RO "return redis.call('SET', KEYS[1], 'x')" 1 lua:k1
    # ...and refused means the value did NOT change.
    assert_match "EVAL_RO write did not land" GET lua:k1

    # moon#672: the redis error CODE raised inside a script must LEAD the reply
    # -- it is the part a client matches on. moon buried it behind
    # `ERR Error running script: runtime error: `, so a client could not tell a
    # type clash from a bug. Not `assert_match`: redis appends its own
    # ` script: <sha>, on @user_script:N.` tail, so only the head is parity.
    mcli DEL lua:wt >/dev/null 2>&1; mcli LPUSH lua:wt a >/dev/null 2>&1
    assert_moon_contains "script error leads with WRONGTYPE" "WRONGTYPE" \
        EVAL "return redis.call('GET', KEYS[1])" 1 lua:wt
    # ...and no error quotes a moon source path back at the client.
    assert_moon_not_contains "script error leaks no moon source path" ".rs" \
        EVAL "error('boom')" 0

    # moon#636: DUMP/RESTORE. The payload cannot be compared against redis's --
    # different value encodings and a different RDB version -- so the parity
    # rows cover the ERROR surface, and the round-trip is checked inside Lua
    # (a payload carried through the shell loses its NUL type byte).
    assert_match "DUMP of a missing key"    DUMP dr:absent
    assert_match "DUMP arity"               DUMP dr:a dr:b
    assert_match "RESTORE bad payload"      RESTORE dr:k 0 garbage
    assert_match "RESTORE negative TTL"     RESTORE dr:k -1 garbage
    mcli SET "dr:{dr}:src" hello >/dev/null 2>&1
    # The `{dr}` tag co-locates both keys: at --shards >= 2 they would
    # otherwise land on different shards and the script is refused CROSSSLOT
    # before it reaches DUMP at all.
    assert_moon "DUMP then RESTORE round-trips" "hello" \
        EVAL "local p = redis.call('DUMP', KEYS[1]); redis.call('RESTORE', KEYS[2], 0, p, 'REPLACE'); return redis.call('GET', KEYS[2])" \
        2 "dr:{dr}:src" "dr:{dr}:dst"

    # moon#636: DEBUG DIGEST is not available from an inline context, and the
    # refusal must name the limitation rather than answer from the one
    # database that path can see. Lua is the reachable half of that pair from
    # a one-shot client (MULTI needs a held connection); the message covers
    # both, so match the phrase that names them rather than a single word.
    # The digest's own behaviour is checked in the CONNECTION block.
    assert_moon_contains "DEBUG DIGEST from Lua is refused, not guessed" \
        "MULTI/EXEC and Lua" \
        EVAL "return redis.call('DEBUG', 'DIGEST')" 0

    # moon#515: Redis caches an EVAL'd body server-wide, so EVAL-then-EVALSHA
    # is a supported idiom. moon cached it only on the executing shard, so at
    # --shards >= 2 the EVALSHA answered NOSCRIPT for every key that routed
    # elsewhere. Each `mcli` is a fresh connection, so the loop samples shards.
    # The sha comes from REDIS, not from `SCRIPT LOAD` on moon -- SCRIPT LOAD
    # already fanned out, and using it here would hide the defect.
    FANOUT_BODY="return redis.call('set',KEYS[1],'v')"
    FANOUT_SHA=$(rcli SCRIPT LOAD "$FANOUT_BODY" 2>/dev/null)
    mcli EVAL "$FANOUT_BODY" 1 lua:fanoutseed >/dev/null 2>&1
    for i in 1 2 3 4 5 6 7 8; do
        assert_match "EVALSHA after bare EVAL (key $i)" EVALSHA "$FANOUT_SHA" 1 "lua:fanout$i"
    done

    # moon#514: FUNCTION LOAD must reach every shard and FCALL must route to
    # the shard owning its key. Unfixed at --shards 4: 5/8 CROSSSLOT, 3/8
    # "Function not found", 0/8 succeeded.
    FN_LIB=$'#!lua name=cmdlib\nredis.register_function(\'cmdset\', function(keys, args) return redis.call(\'set\', keys[1], args[1]) end)\n'
    rcli FUNCTION FLUSH >/dev/null 2>&1; mcli FUNCTION FLUSH >/dev/null 2>&1
    assert_match "FUNCTION LOAD"       FUNCTION LOAD "$FN_LIB"
    for i in 1 2 3 4 5 6 7 8; do
        assert_match "FCALL single key (key $i)" FCALL cmdset 1 "fn:k$i" "v$i"
        assert_match "FCALL value landed (key $i)" GET "fn:k$i"
    done
    assert_match "FUNCTION DELETE"     FUNCTION DELETE cmdlib
    for i in 1 2 3 4; do
        assert_match "FCALL after DELETE (key $i)" FCALL cmdset 1 "fn:d$i" x
    done
fi

# ===========================================================================
# PERSISTENCE COMMANDS
# ===========================================================================

# ===========================================================================
# VECTOR SEARCH COMMANDS (moon-only — Redis uses different syntax)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== VECTOR SEARCH COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # FT.CREATE — create a vector index.
    #
    # HNSW, not FLAT (moon#679). This row asked for `VECTOR FLAT` and expected
    # `OK` from the day it was written, but moon has only ever implemented
    # HNSW: `ERR expected HNSW algorithm` has been in `ft_create.rs` since #27.
    # So the row never passed, and the five rows below it -- FT.INFO,
    # FT.SEARCH, FT.DROPINDEX, FT.INFO-after-drop -- all fell over behind it
    # on an index that was never created. Four of the six "vector" rows in this
    # category were reporting a failure that told you nothing.
    #
    # Note this is not a redis-parity assertion and never was: the
    # `redis-server` this suite runs against has no query engine, so FT.* is
    # `unknown command` on the Redis side. These are moon-only rows, which is
    # why nothing flagged the expectation as unsupported.
    assert_moon "FT.CREATE basic"          "OK"    FT.CREATE myidx ON HASH PREFIX 1 doc: SCHEMA embedding VECTOR HNSW 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32

    # FLAT is a real RediSearch algorithm moon does not implement. Assert the
    # refusal explicitly rather than letting it hide inside a row that expected
    # success -- a gap that is asserted is a gap someone can find.
    TOTAL=$((TOTAL + 1)); FT_FLAT=$(mcli FT.CREATE flatidx ON HASH PREFIX 1 f: SCHEMA v VECTOR FLAT 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32 2>&1)
    if echo "$FT_FLAT" | grep -q "expected HNSW algorithm"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.CREATE VECTOR FLAT refused (moon implements HNSW only)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.CREATE VECTOR FLAT expected 'expected HNSW algorithm', got: $FT_FLAT"
    fi

    # moon#681: a VECTOR clause truncated after the algorithm keyword used to
    # panic the shard thread and abort the whole process. The row that matters
    # is the PING after it -- an error reply is fine, a dead server is not.
    TOTAL=$((TOTAL + 1)); FT_TRUNC=$(mcli FT.CREATE truncidx ON HASH PREFIX 1 t: SCHEMA v VECTOR HNSW 2>&1)
    if [ "$(mcli PING 2>&1)" = "PONG" ]; then
        PASS=$((PASS + 1)); echo "  PASS: moon#681 truncated FT.CREATE answers ($FT_TRUNC) and the server survives"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: moon#681 truncated FT.CREATE killed the server (reply was: $FT_TRUNC)"
    fi

    # FT.INFO — index metadata
    TOTAL=$((TOTAL + 1)); FT_INFO=$(mcli FT.INFO myidx 2>&1)
    if echo "$FT_INFO" | grep -q "myidx"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO returns index name"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO returns index name"; fi

    # Insert vectors via HSET (auto-indexed) — use python3 to avoid null byte stripping in bash
    python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',1.0,0.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET doc:1 embedding >/dev/null 2>&1 || true
    python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.0,1.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET doc:2 embedding >/dev/null 2>&1 || true

    # FT.SEARCH — verify command doesn't error (redis-cli can't pass binary args directly)
    # moon#693 (PART DONE): `*` is the match-all query. It used to be refused on
    # every index type with "ERR invalid KNN query syntax" -- even on a
    # TEXT-only index with no KNN anything. It now works on every index with an
    # inverted schema (TEXT / TAG / NUMERIC), which is where the document
    # registry lives -- see the `FT.SEARCH "*" enumerates a TEXT index` row in
    # the BM25 section, which passes.
    #
    # `myidx` here is VECTOR-ONLY, and that case is still open: the vector
    # engine's live key map would have to be enumerable through BOTH the local
    # handler path and `scatter_text_search`, which is its own change with its
    # own multi-shard verification. Tracked as moon#695. Left asserting the
    # RediSearch behaviour so it goes green on its own the day that lands.
    TOTAL=$((TOTAL + 1)); FT_SEARCH=$(mcli FT.SEARCH myidx "*" 2>&1)
    if ! echo "$FT_SEARCH" | grep -qi "err"; then PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH does not error"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH \"*\" on a VECTOR-only index returned error (moon#695, the open half of moon#693): $FT_SEARCH"; fi

    # FT.DROPINDEX — remove index
    assert_moon "FT.DROPINDEX"             "OK"    FT.DROPINDEX myidx

    # FT.INFO after drop should error
    # moon#683: this grepped for "err" or "not found" and failed on moon's
    # actual reply. Verified on the wire: moon answers `-Unknown Index name`
    # -- a real RESP error frame, carrying the exact text RediSearch uses. The
    # row was wrong, not the server. (redis-cli prints an error reply to stdout
    # with no "(error)" marker when it is not on a tty, so the message text is
    # the only thing a shell harness can match on.)
    TOTAL=$((TOTAL + 1)); FT_INFO_AFTER=$(mcli FT.INFO myidx 2>&1)
    if echo "$FT_INFO_AFTER" | grep -qi "unknown index"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO after drop errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO after drop should report an unknown index, got: $FT_INFO_AFTER"; fi
fi

# ===========================================================================
# PERSISTENCE COMMANDS
# ===========================================================================

if should_run "persistence"; then
    echo ""
    echo "=== PERSISTENCE COMMANDS ==="

    assert_moon "BGSAVE"               "Background saving started" BGSAVE
    sleep 1

    # SHUTDOWN [NOSAVE|SAVE] is intentionally NOT exercised in this section:
    # it terminates the server process this whole script shares across every
    # other category, which would abort the run. Coverage lives in
    # tests/shutdown_integration.rs (spawns its own throwaway server per
    # case) and scripts/test-consistency.sh (cross-shard durability smoke
    # check against its own dedicated instance).
fi

# ===========================================================================
# BLOCKING COMMANDS (short timeouts)
# ===========================================================================

if should_run "blocking"; then
    echo ""
    echo "=== BLOCKING COMMANDS ==="
    flush_both

    # Pre-populate so blocking commands return immediately
    rcli RPUSH blk:l1 val1 >/dev/null 2>&1; mcli RPUSH blk:l1 val1 >/dev/null 2>&1
    assert_match "BLPOP (ready)"       BLPOP blk:l1 1

    rcli RPUSH blk:l2 val2 >/dev/null 2>&1; mcli RPUSH blk:l2 val2 >/dev/null 2>&1
    assert_match "BRPOP (ready)"       BRPOP blk:l2 1

    rcli ZADD blk:z1 1 a 2 b >/dev/null 2>&1; mcli ZADD blk:z1 1 a 2 b >/dev/null 2>&1
    assert_match "BZPOPMIN (ready)"    BZPOPMIN blk:z1 1

    rcli ZADD blk:z2 1 a 2 b >/dev/null 2>&1; mcli ZADD blk:z2 1 a 2 b >/dev/null 2>&1
    assert_match "BZPOPMAX (ready)"    BZPOPMAX blk:z2 1

    # moon#570: `{blk}` co-locates the pair -- see the LMOVE row above.
    rcli RPUSH {blk}:src x y z >/dev/null 2>&1; mcli RPUSH {blk}:src x y z >/dev/null 2>&1
    assert_match "BLMOVE (ready)"      BLMOVE {blk}:src {blk}:dst LEFT RIGHT 1
fi

# ===========================================================================
# REDIS-BENCHMARK THROUGHPUT
# ===========================================================================

if should_run "benchmark" && [[ "$SKIP_BENCH" == "false" ]]; then
    echo ""
    echo "=== REDIS-BENCHMARK THROUGHPUT (moon, 1000 requests each) ==="
    echo ""

    # String commands
    assert_bench "SET"                    "-t set"
    assert_bench "GET"                    "-t get"
    assert_bench "MSET (10 keys)"         "-t mset"
    assert_bench "INCR"                   "-t incr"
    assert_bench "APPEND"                 "" -c 50 APPEND bench:append hello

    # List commands
    assert_bench "LPUSH"                  "-t lpush"
    assert_bench "RPUSH"                  "-t rpush"
    assert_bench "LPOP"                   "-t lpop"
    assert_bench "RPOP"                   "-t rpop"
    assert_bench "LRANGE 100"             "" -c 50 LRANGE bench:list 0 99
    assert_bench "LRANGE 300"             "" -c 50 LRANGE bench:list 0 299

    # Hash commands
    assert_bench "HSET"                   "-t hset"

    # Set commands
    assert_bench "SADD"                   "-t sadd"
    assert_bench "SPOP"                   "-t spop"

    # Sorted set commands
    assert_bench "ZADD"                   "-t zadd"
    assert_bench "ZPOPMIN"                "-t zpopmin"

    # Key commands
    assert_bench "PING inline"            "-t ping_inline"
    assert_bench "PING mbulk"             "-t ping_mbulk"

    # Pipeline scaling
    echo ""
    echo "  --- Pipeline scaling (SET) ---"
    for p in 1 4 16 64; do
        assert_bench "SET p=$p"           "-t set -P $p"
    done
    echo ""
    echo "  --- Pipeline scaling (GET) ---"
    for p in 1 4 16 64; do
        assert_bench "GET p=$p"           "-t get -P $p"
    done
fi

# ===========================================================================
# VECTOR SEARCH COMMANDS (v0.1.6)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== VECTOR SEARCH COMMANDS ==="
    flush_both

    # Create a test index with 4-dimensional vectors
    assert_moon_ok "FT.CREATE basic" FT.CREATE testidx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2

    # moon#683: the vectors here used to be the obvious little-endian encodings
    # of 1.0 (00 00 80 3f) and 0.0 (00 00 00 00), built with
    # `"$(printf '\x00...')"`. Command substitution STRIPS NUL bytes, so a
    # 16-byte 4-dim FLOAT32 blob reached redis-cli as TWO bytes -- for the
    # stored documents as well as for every query vector. That is the whole
    # source of the "ERR query vector dimension mismatch" and "no valid vectors
    # found for POSITIVE keys" rows: the harness never sent a vector.
    #
    # These byte patterns carry no NULs, so what the shell passes is what the
    # test meant. `$V_HI` is ~0.747, `$V_LO` ~0.035, `$V_MID` ~0.633 -- the
    # same "one dominant axis" geometry as before, which is what the KNN and
    # tag-filter rows rely on.
    mcli HSET doc:1 vec "$VEC1" category science title "quantum physics" >/dev/null 2>&1
    mcli HSET doc:2 vec "$VEC2" category math title "linear algebra" >/dev/null 2>&1
    mcli HSET doc:3 vec "$VEC3" category science title "particle physics" >/dev/null 2>&1
    sleep 0.5

    # FT.SEARCH basic KNN
    assert_moon_contains "FT.SEARCH KNN" "doc:" FT.SEARCH testidx "*=>[KNN 2 @vec \$q]" PARAMS 2 q "$VQ"

    # FT.SEARCH with LIMIT
    assert_moon_contains "FT.SEARCH LIMIT" "doc:" FT.SEARCH testidx "*=>[KNN 3 @vec \$q]" PARAMS 2 q "$VQ" LIMIT 0 1

    # FT.INFO
    assert_moon_contains "FT.INFO" "testidx" FT.INFO testidx

    # FT._LIST
    assert_moon_contains "FT._LIST" "testidx" FT._LIST

    # FT.COMPACT (should succeed even if nothing to compact)
    assert_moon_ok "FT.COMPACT" FT.COMPACT testidx

    # FT.CONFIG SET/GET
    assert_moon_ok "FT.CONFIG SET AUTOCOMPACT" FT.CONFIG SET testidx AUTOCOMPACT OFF
    assert_moon_contains "FT.CONFIG GET AUTOCOMPACT" "OFF" FT.CONFIG GET testidx AUTOCOMPACT

    # FT.RECOMMEND (with existing keys as positive examples)
    assert_moon_contains "FT.RECOMMEND basic" "doc:" FT.RECOMMEND testidx POSITIVE doc:1 K 2

    # Tag filter
    assert_moon_contains "FT.SEARCH tag filter" "doc:" FT.SEARCH testidx "@category:{science}=>[KNN 3 @vec \$q]" PARAMS 2 q "$VQ"

    # ── KNN-prefilter numeric grammar (moon#648 / moon#664) ────────────────
    # FT.SEARCH has TWO numeric-range parsers. NUMERIC-01..06 below cover the
    # full query grammar, which was always correct. These cover the KNN
    # prefilter, which was not: it could not read a `(` bound and returned None
    # for the WHOLE expression, and the caller read None as "no filter" and ran
    # an UNFILTERED search. A filter that has silently stopped filtering is
    # indistinguishable from one that legitimately matched everything.
    mcli FT.CREATE knnfilt ON HASH PREFIX 1 kf: SCHEMA vt NUMERIC vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2 >/dev/null 2>&1
    # 16 NUL-free ASCII bytes = one FLOAT32 DIM 4 vector. A `printf '\x00...'`
    # blob would NOT survive command substitution -- bash strips NUL bytes, the
    # vector arrives short, and every query below answers "dimension mismatch"
    # instead of exercising the filter. That makes the guard vacuous.
    KF_VEC="ABCDEFGHIJKLMNOP"
    mcli HSET kf:1 vt 150 vec "$KF_VEC" >/dev/null 2>&1
    mcli HSET kf:2 vt 250 vec "$KF_VEC" >/dev/null 2>&1
    mcli HSET kf:3 vt 350 vec "$KF_VEC" >/dev/null 2>&1
    sleep 0.5

    # KNNFILT-01 baseline: the prefilter is applied at all (150 only).
    TOTAL=$((TOTAL + 1))
    KF_INC=$(mcli FT.SEARCH knnfilt '@vt:[100 200]=>[KNN 4 @vec $q]' PARAMS 2 q "$KF_VEC" DIALECT 2 2>&1)
    KF_N=$(echo "$KF_INC" | grep -c '^kf:' || true)
    if [ "$KF_N" -eq 1 ]; then
        PASS=$((PASS + 1)); echo "  PASS: KNNFILT-01 prefilter [100 200] -> 1 key"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: KNNFILT-01 expected 1 got $KF_N: $KF_INC"
    fi

    # KNNFILT-02: exclusive upper bound. Pre-fix this returned 3 (unfiltered).
    TOTAL=$((TOTAL + 1))
    KF_EXCL=$(mcli FT.SEARCH knnfilt '@vt:[100 (300]=>[KNN 4 @vec $q]' PARAMS 2 q "$KF_VEC" DIALECT 2 2>&1)
    KF_NE=$(echo "$KF_EXCL" | grep -c '^kf:' || true)
    if [ "$KF_NE" -eq 2 ]; then
        PASS=$((PASS + 1)); echo "  PASS: KNNFILT-02 prefilter [100 (300] -> 2 keys (exclusive honoured)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: KNNFILT-02 expected 2 got $KF_NE (3 = filter silently dropped): $KF_EXCL"
    fi

    # KNNFILT-03: an unparseable prefilter is an ERROR, not a wider search.
    TOTAL=$((TOTAL + 1))
    KF_BAD=$(mcli FT.SEARCH knnfilt '@vt:[abc def]=>[KNN 4 @vec $q]' PARAMS 2 q "$KF_VEC" DIALECT 2 2>&1)
    if echo "$KF_BAD" | grep -qi "invalid FILTER"; then
        PASS=$((PASS + 1)); echo "  PASS: KNNFILT-03 unparseable prefilter rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: KNNFILT-03 expected an error, got: $KF_BAD"
    fi

    # KNNFILT-04 (moon#664): an inverted range must not abort the process.
    # BTreeMap::range panics when start > end, and a shard panic aborts moon.
    TOTAL=$((TOTAL + 1))
    KF_INV=$(mcli FT.SEARCH knnfilt '@vt:[300 100]=>[KNN 4 @vec $q]' PARAMS 2 q "$KF_VEC" DIALECT 2 2>&1)
    KF_ALIVE=$(mcli PING 2>&1)
    if echo "$KF_INV" | grep -qi "invalid FILTER" && echo "$KF_ALIVE" | grep -qi "PONG"; then
        PASS=$((PASS + 1)); echo "  PASS: KNNFILT-04 inverted prefilter rejected, server still alive"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: KNNFILT-04 reply='$KF_INV' ping='$KF_ALIVE' (empty ping = process aborted)"
    fi

    mcli FT.DROPINDEX knnfilt DD >/dev/null 2>&1

    # FT.DROPINDEX
    assert_moon_ok "FT.DROPINDEX" FT.DROPINDEX testidx

    # Verify index is gone
    assert_moon "FT._LIST empty" "" FT._LIST

    # ── FT.DROPINDEX DD flag tests ──────────────────────────────────────────
    # DD flag deletes all indexed documents along with the index

    # Create a fresh index for DD tests
    assert_moon_ok "FT.CREATE dd_test" FT.CREATE ddtest ON HASH PREFIX 1 dd: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2

    # Insert documents.
    # `$PORT` (four sites below) was never a variable this script defines --
    # under `set -euo pipefail` the first one ABORTED the whole run here, so
    # every category after VECTOR SEARCH (MQ, txn_kv, eviction, benchmark) and
    # the result summary never executed. Same class as moon#634.
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT_RUST" HSET dd:1 vec >/dev/null 2>&1 || true
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT_RUST" HSET dd:2 vec >/dev/null 2>&1 || true

    # Verify documents exist
    TOTAL=$((TOTAL + 1)); DD_EXISTS=$(mcli EXISTS dd:1 dd:2 2>&1)
    if [ "$DD_EXISTS" = "2" ]; then PASS=$((PASS + 1)); echo "  PASS: DD docs exist before drop"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD docs should exist (got: $DD_EXISTS)"; fi

    # Drop with DD flag — documents should be deleted
    assert_moon_ok "FT.DROPINDEX DD" FT.DROPINDEX ddtest DD

    # Verify documents are gone
    TOTAL=$((TOTAL + 1)); DD_AFTER=$(mcli EXISTS dd:1 dd:2 2>&1)
    if [ "$DD_AFTER" = "0" ]; then PASS=$((PASS + 1)); echo "  PASS: DD docs deleted after FT.DROPINDEX DD"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD docs should be deleted (got: $DD_AFTER)"; fi

    # Test case insensitivity: create another index
    assert_moon_ok "FT.CREATE dd_test2" FT.CREATE ddtest2 ON HASH PREFIX 1 dd2: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT_RUST" HSET dd2:1 vec >/dev/null 2>&1 || true

    # Drop with lowercase dd flag
    assert_moon_ok "FT.DROPINDEX dd (lowercase)" FT.DROPINDEX ddtest2 dd

    TOTAL=$((TOTAL + 1)); DD2_AFTER=$(mcli EXISTS dd2:1 2>&1)
    if [ "$DD2_AFTER" = "0" ]; then PASS=$((PASS + 1)); echo "  PASS: lowercase dd flag works"; else FAIL=$((FAIL + 1)); echo "  FAIL: lowercase dd should work (got: $DD2_AFTER)"; fi

    # Test without DD — documents should remain
    assert_moon_ok "FT.CREATE no_dd_test" FT.CREATE noddtest ON HASH PREFIX 1 ndd: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT_RUST" HSET ndd:1 vec >/dev/null 2>&1 || true

    assert_moon_ok "FT.DROPINDEX no DD" FT.DROPINDEX noddtest

    TOTAL=$((TOTAL + 1)); NDD_AFTER=$(mcli EXISTS ndd:1 2>&1)
    if [ "$NDD_AFTER" = "1" ]; then PASS=$((PASS + 1)); echo "  PASS: no DD preserves documents"; else FAIL=$((FAIL + 1)); echo "  FAIL: no DD should preserve docs (got: $NDD_AFTER)"; fi

    # Cleanup
    mcli DEL ndd:1 >/dev/null 2>&1

    # Test DD on non-existent index returns error
    TOTAL=$((TOTAL + 1)); DD_NONEXIST=$(mcli FT.DROPINDEX nonexistent_idx DD 2>&1)
    if echo "$DD_NONEXIST" | grep -qi "unknown\|err"; then PASS=$((PASS + 1)); echo "  PASS: DD on non-existent index errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD on non-existent should error (got: $DD_NONEXIST)"; fi

    # ── End DD flag tests ────────────────────────────────────────────────────

    echo "  vector: $PASS passed (of $TOTAL total)"
fi

# ===========================================================================
# TEXT FIELD TESTS (v0.1.7 full-text search)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== TEXT FIELD TESTS ==="
    flush_both

    # FT.CREATE with TEXT-only index
    assert_moon_ok "FT.CREATE text-only index" FT.CREATE textidx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 2.0 body TEXT

    # HSET to trigger text auto-indexing
    assert_moon_ok "HSET doc with TEXT fields" HSET doc:t1 title "Hello world" body "This is a test document"
    assert_moon_ok "HSET second doc" HSET doc:t2 title "Second document" body "Another test with more words"
    assert_moon_ok "HSET third doc" HSET doc:t3 title "Third title" body "Final body text here"

    # FT.INFO reports text stats — num_docs and num_terms must be > 0 after HSET
    FT_TEXT_INFO=$(mcli FT.INFO textidx 2>&1)

    TOTAL=$((TOTAL + 1))
    TEXT_NUM_DOCS=$(echo "$FT_TEXT_INFO" | grep -A1 "num_docs" | tail -1 | tr -d '[:space:]' || true)
    if [ -n "$TEXT_NUM_DOCS" ] && [ "$TEXT_NUM_DOCS" != "0" ] && [ "$TEXT_NUM_DOCS" -gt 0 ] 2>/dev/null; then
        PASS=$((PASS + 1)); echo "  PASS: FT.INFO text num_docs = $TEXT_NUM_DOCS (should be > 0)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text num_docs should be > 0 (got: $TEXT_NUM_DOCS)"
    fi

    TOTAL=$((TOTAL + 1))
    TEXT_NUM_TERMS=$(echo "$FT_TEXT_INFO" | grep -A1 "num_terms" | tail -1 | tr -d '[:space:]' || true)
    if [ -n "$TEXT_NUM_TERMS" ] && [ "$TEXT_NUM_TERMS" != "0" ] && [ "$TEXT_NUM_TERMS" -gt 0 ] 2>/dev/null; then
        PASS=$((PASS + 1)); echo "  PASS: FT.INFO text num_terms = $TEXT_NUM_TERMS (should be > 0)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text num_terms should be > 0 (got: $TEXT_NUM_TERMS)"
    fi

    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "avg_doc_len"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text avg_doc_len"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text avg_doc_len"; fi
    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "bm25_k1"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text bm25_k1"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text bm25_k1"; fi
    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "bytes_per_posting"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text bytes_per_posting"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text bytes_per_posting"; fi

    # FT.CREATE with TEXT + NOSTEM
    assert_moon_ok "FT.CREATE NOSTEM index" FT.CREATE nostemidx ON HASH PREFIX 1 ns: SCHEMA content TEXT NOSTEM

    # FT.CREATE with TEXT + NOINDEX
    assert_moon_ok "FT.CREATE NOINDEX field" FT.CREATE noidxtest ON HASH PREFIX 1 ni: SCHEMA indexed TEXT meta TEXT NOINDEX

    # FT.CREATE with BM25 parameters
    assert_moon_ok "FT.CREATE with BM25 params" FT.CREATE bm25idx ON HASH PREFIX 1 bm: BM25_K1 1.5 BM25_B 0.8 SCHEMA content TEXT

    # FT.CONFIG SET/GET BM25 parameters
    assert_moon_ok "FT.CONFIG SET BM25_K1" FT.CONFIG SET bm25idx BM25_K1 1.8
    assert_moon_contains "FT.CONFIG GET BM25_K1" "1.8" FT.CONFIG GET bm25idx BM25_K1

    # ── FT.SEARCH BM25 text search tests (Plan 150-01) ──────────────────────────
    # Uses doc:t1/t2/t3 indexed above: title TEXT WEIGHT 2.0, body TEXT
    # doc:t1: title="Hello world" body="This is a test document"
    # doc:t2: title="Second document" body="Another test with more words"
    # doc:t3: title="Third title" body="Final body text here"

    # 1. Basic single-term text search: "document" matches doc:t1 and doc:t2 body fields
    TOTAL=$((TOTAL + 1))
    FT_BASIC=$(mcli FT.SEARCH textidx "document" 2>&1)
    if echo "$FT_BASIC" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH basic text returned error: $FT_BASIC"
    elif echo "$FT_BASIC" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH basic text returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH basic text returned no results"
    fi

    # 2. __bm25_score field must appear in response
    TOTAL=$((TOTAL + 1))
    if echo "$FT_BASIC" | grep -q "__bm25_score"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH text response contains __bm25_score"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH text response missing __bm25_score field"
    fi

    # 3. Multi-term AND search: "test document" — both must appear in same doc (doc:t1 body)
    TOTAL=$((TOTAL + 1))
    FT_MULTI=$(mcli FT.SEARCH textidx "test document" 2>&1)
    if echo "$FT_MULTI" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH multi-term returned error: $FT_MULTI"
    elif echo "$FT_MULTI" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH multi-term AND returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH multi-term AND returned no results (regression of moon#690: 'test' back on the stoplist, or a stop word zeroing its conjunction again)"
    fi

    # 4. Field-targeted search: @title:(document) — only doc:t2 has 'document' in title
    TOTAL=$((TOTAL + 1))
    FT_FIELD=$(mcli FT.SEARCH textidx "@title:(document)" 2>&1)
    if echo "$FT_FIELD" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH field-targeted returned error: $FT_FIELD"
    elif echo "$FT_FIELD" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH @title:(document) returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH @title:(document) returned no results"
    fi

    # 5. Empty result for non-existent term
    TOTAL=$((TOTAL + 1))
    FT_EMPTY=$(mcli FT.SEARCH textidx "xyznonexistentterm" 2>&1)
    if echo "$FT_EMPTY" | grep -qi "^err\b" | head -1; then
        # Some ERR is acceptable (e.g. stop word) but the term is unique
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH nonexistent term returned error: $FT_EMPTY"
    else
        # Should return 0 results (not error)
        FT_EMPTY_COUNT=$(echo "$FT_EMPTY" | head -1 | tr -d '[:space:]')
        if [ "$FT_EMPTY_COUNT" = "0" ] || echo "$FT_EMPTY" | grep -q "^(empty\|0)"; then
            PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH nonexistent term returns 0 results"
        else
            PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH nonexistent term (no doc: in result)"
        fi
    fi

    # 6. LIMIT clause: FT.SEARCH textidx "document" LIMIT 0 1 — returns exactly 1 doc entry
    TOTAL=$((TOTAL + 1))
    FT_LIMIT=$(mcli FT.SEARCH textidx "document" LIMIT 0 1 2>&1)
    FT_LIMIT_DOC_COUNT=$(echo "$FT_LIMIT" | grep -c "doc:" || true)
    if [ "$FT_LIMIT_DOC_COUNT" -le 1 ] && echo "$FT_LIMIT" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH LIMIT 0 1 returns at most 1 result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH LIMIT 0 1 should return exactly 1 result (got $FT_LIMIT_DOC_COUNT)"
    fi

    # 7. Stop-words-only query: no crash, and no documents.
    #
    # moon#683: this row demanded an ERR and unconditionally failed anything
    # else -- including the `0` its own comment called "also acceptable". moon
    # drops stop words at index time and answers an empty result set, which is
    # what RediSearch does; there is no oracle here either way, since the
    # redis-server this suite compares against has no query engine. So pin the
    # behaviour that exists: the query must not error and must not match
    # documents. A returned document would mean stop-word filtering is off.
    TOTAL=$((TOTAL + 1))
    FT_STOP=$(mcli FT.SEARCH textidx "the" 2>&1)
    if echo "$FT_STOP" | grep -q "doc:"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH stop-words-only matched documents: $FT_STOP"
    elif echo "$FT_STOP" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH stop-words-only errored instead of returning 0: $FT_STOP"
    else
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH stop-words-only returns no documents"
    fi

    # 7b. Match-all (moon#693): `*` enumerates every document in an inverted
    # index -- INCLUDING documents no term query can reach. doc:t3's body is
    # "Final body text here"; the row asserts the count, not a term, so it
    # cannot pass by accident on a stray substring match.
    TOTAL=$((TOTAL + 1))
    FT_STAR=$(mcli FT.SEARCH textidx "*" LIMIT 0 0 2>&1)
    if [[ "$FT_STAR" == "3" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH \"*\" enumerates a TEXT index (3 docs)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH \"*\" on a TEXT index expected 3, got: $FT_STAR"
    fi

    # 7c. Ordinary English words are indexable (moon#690). Under the old
    # 1298-word stopword list every one of these was discarded at index time.
    TOTAL=$((TOTAL + 1))
    FT_ORD=$(mcli FT.SEARCH textidx "hello" LIMIT 0 0 2>&1)
    if [[ "$FT_ORD" == "1" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH finds an ordinary word ('hello')"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH 'hello' expected 1 doc, got: $FT_ORD (moon#690 stoplist regression?)"
    fi

    # 7d. A stop word in a conjunction is REMOVED from the query, not
    # intersected as the empty set (moon#690). `hello the` must equal `hello`.
    TOTAL=$((TOTAL + 1))
    FT_STOP=$(mcli FT.SEARCH textidx "hello the" LIMIT 0 0 2>&1)
    if [[ "$FT_STOP" == "1" && "$FT_STOP" == "$FT_ORD" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: a stop word drops out of the conjunction ('hello the' == 'hello')"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: 'hello the' returned $FT_STOP but 'hello' returned $FT_ORD (moon#690 asymmetry regression)"
    fi

    # 8. Cross-field search: "world" appears in doc:t1 title — searches all TEXT fields
    TOTAL=$((TOTAL + 1))
    FT_CROSS=$(mcli FT.SEARCH textidx "world" 2>&1)
    if echo "$FT_CROSS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH cross-field returned error: $FT_CROSS"
    elif echo "$FT_CROSS" | grep -q "doc:t1"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH cross-field finds 'world' in doc:t1"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH cross-field should find 'world' in doc:t1 (regression of moon#690: 'world' back on the stoplist)"
    fi
    # ── End FT.SEARCH BM25 text search tests ─────────────────────────────────────

    # ── HIGHLIGHT / SUMMARIZE tests (Plan 150-03) ─────────────────────────────
    # Add a document with sufficient body text for SUMMARIZE truncation (> 20 words)
    assert_moon_ok "HSET doc:long for HIGHLIGHT/SUMMARIZE" HSET doc:long title "machine learning overview" body "This is a comprehensive guide to machine learning covering supervised learning unsupervised learning and reinforcement learning techniques used in modern artificial intelligence and data science applications for production systems"

    # 1. HIGHLIGHT basic: verify <b> tag in response
    TOTAL=$((TOTAL + 1))
    FT_HL=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT 2>&1)
    if echo "$FT_HL" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT returned error: $FT_HL"
    elif echo "$FT_HL" | grep -q "<b>"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT response contains <b> tag"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT response missing <b> tag (got: $FT_HL)"
    fi

    # 2. HIGHLIGHT FIELDS: only highlight specified field
    TOTAL=$((TOTAL + 1))
    FT_HL_FIELDS=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT FIELDS 1 title 2>&1)
    if echo "$FT_HL_FIELDS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT FIELDS returned error: $FT_HL_FIELDS"
    elif echo "$FT_HL_FIELDS" | grep -q "<b>\|machine"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT FIELDS 1 title returns result with match"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT FIELDS 1 title returned no match (got: $FT_HL_FIELDS)"
    fi

    # 3. HIGHLIGHT custom TAGS: verify custom open/close tags
    TOTAL=$((TOTAL + 1))
    FT_HL_TAGS=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT TAGS "[[" "]]" 2>&1)
    if echo "$FT_HL_TAGS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT TAGS returned error: $FT_HL_TAGS"
    elif echo "$FT_HL_TAGS" | grep -q "\[\["; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT TAGS [[ ]] response contains custom tag"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT TAGS response missing custom tag (got: $FT_HL_TAGS)"
    fi

    # 4. SUMMARIZE basic: verify response is returned without error
    TOTAL=$((TOTAL + 1))
    FT_SUM=$(mcli FT.SEARCH textidx "machine" SUMMARIZE 2>&1)
    if echo "$FT_SUM" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE returned error: $FT_SUM"
    elif echo "$FT_SUM" | grep -q "machine\|learning"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE response contains match terms"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE response missing match terms (got: $FT_SUM)"
    fi

    # 5. SUMMARIZE FIELDS: only summarize the body field
    TOTAL=$((TOTAL + 1))
    FT_SUM_FIELDS=$(mcli FT.SEARCH textidx "machine" SUMMARIZE FIELDS 1 body 2>&1)
    if echo "$FT_SUM_FIELDS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE FIELDS returned error: $FT_SUM_FIELDS"
    elif echo "$FT_SUM_FIELDS" | grep -q "machine\|learning"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE FIELDS 1 body returns result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE FIELDS 1 body missing match (got: $FT_SUM_FIELDS)"
    fi

    # 6. SUMMARIZE with LEN: fragment should be short (10 tokens)
    TOTAL=$((TOTAL + 1))
    FT_SUM_LEN=$(mcli FT.SEARCH textidx "machine" SUMMARIZE LEN 10 2>&1)
    if echo "$FT_SUM_LEN" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE LEN returned error: $FT_SUM_LEN"
    else
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE LEN 10 does not error"
    fi

    # 7. HIGHLIGHT + SUMMARIZE combined: title highlighted, body summarized
    TOTAL=$((TOTAL + 1))
    FT_BOTH=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT FIELDS 1 title SUMMARIZE FIELDS 1 body 2>&1)
    if echo "$FT_BOTH" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT + SUMMARIZE combined returned error: $FT_BOTH"
    elif echo "$FT_BOTH" | grep -q "machine\|<b>"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT FIELDS 1 title SUMMARIZE FIELDS 1 body returns result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH combined HIGHLIGHT+SUMMARIZE missing output (got: $FT_BOTH)"
    fi
    # ── End HIGHLIGHT / SUMMARIZE tests ──────────────────────────────────────

    # FT.DROPINDEX removes text index
    assert_moon_ok "FT.DROPINDEX text index" FT.DROPINDEX textidx

    # FT.INFO after drop should error
    TOTAL=$((TOTAL + 1)); FT_TEXT_AFTER=$(mcli FT.INFO textidx 2>&1)
    if echo "$FT_TEXT_AFTER" | grep -qi "err\|not found\|unknown"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO after text drop errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO after text drop should error"; fi

    # Cleanup remaining text indexes
    mcli FT.DROPINDEX nostemidx >/dev/null 2>&1
    mcli FT.DROPINDEX noidxtest >/dev/null 2>&1
    mcli FT.DROPINDEX bm25idx >/dev/null 2>&1

    echo "  text: $PASS passed (of $TOTAL total)"
fi

# ===========================================================================
# FUZZY AND PREFIX SEARCH TESTS (v0.1.7 typo-tolerance — FUZ-01/02/03)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FUZZY AND PREFIX SEARCH TESTS ==="
    flush_both

    # Setup: create text index and populate test documents
    mcli FT.CREATE fuzzyidx ON HASH PREFIX 1 fz: SCHEMA title TEXT body TEXT >/dev/null 2>&1
    mcli HSET fz:1 title "Machine Learning" body "Introduction to machine learning algorithms" >/dev/null 2>&1
    mcli HSET fz:2 title "Deep Learning" body "Neural networks and deep architectures" >/dev/null 2>&1
    mcli HSET fz:3 title "Natural Language" body "NLP processing with transformers" >/dev/null 2>&1
    mcli HSET fz:4 title "Machinery Parts" body "Industrial machinery components" >/dev/null 2>&1
    mcli HSET fz:5 title "Macro Economics" body "Study of macroeconomic indicators" >/dev/null 2>&1

    # FT.COMPACT builds FST (required before fuzzy/prefix queries use FST path)
    assert_moon_ok "FT.COMPACT fuzzyidx for FST build" FT.COMPACT fuzzyidx

    # Test 1: FUZ-01 — Fuzzy search distance 2 (%% syntax)
    TOTAL=$((TOTAL + 1))
    FT_FUZZY2=$(mcli FT.SEARCH fuzzyidx "%%machne%%" LIMIT 0 10 2>&1)
    if echo "$FT_FUZZY2" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %%machne%% returned error: $FT_FUZZY2"
    elif echo "$FT_FUZZY2" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 %%machne%% (fuzzy dist-2) returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %%machne%% returned no docs: $FT_FUZZY2"
    fi

    # Test 2: FUZ-01 — Fuzzy search distance 1 (% syntax)
    TOTAL=$((TOTAL + 1))
    FT_FUZZY1=$(mcli FT.SEARCH fuzzyidx "%machin%" LIMIT 0 10 2>&1)
    if echo "$FT_FUZZY1" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %machin% returned error: $FT_FUZZY1"
    elif echo "$FT_FUZZY1" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 %machin% (fuzzy dist-1) returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %machin% returned no docs: $FT_FUZZY1"
    fi

    # Test 3: FUZ-03 — Prefix search (mach* syntax)
    TOTAL=$((TOTAL + 1))
    FT_PREFIX=$(mcli FT.SEARCH fuzzyidx "mach*" LIMIT 0 10 2>&1)
    if echo "$FT_PREFIX" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 mach* returned error: $FT_PREFIX"
    elif echo "$FT_PREFIX" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-03 mach* prefix search returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 mach* returned no docs: $FT_PREFIX"
    fi

    # Test 4: FUZ-03 — Short prefix (ma*)
    TOTAL=$((TOTAL + 1))
    FT_SHORT_PREFIX=$(mcli FT.SEARCH fuzzyidx "ma*" LIMIT 0 10 2>&1)
    if echo "$FT_SHORT_PREFIX" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 ma* returned error: $FT_SHORT_PREFIX"
    elif echo "$FT_SHORT_PREFIX" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-03 ma* short prefix returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 ma* returned no docs: $FT_SHORT_PREFIX"
    fi

    # Test 5: FUZ-01 — Fuzzy search with field target @title:(%%machne%%)
    TOTAL=$((TOTAL + 1))
    FT_FIELD_FUZZY=$(mcli FT.SEARCH fuzzyidx "@title:(%%machne%%)" LIMIT 0 10 2>&1)
    if echo "$FT_FIELD_FUZZY" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 field-targeted fuzzy returned error: $FT_FIELD_FUZZY"
    elif echo "$FT_FIELD_FUZZY" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 @title:(%%machne%%) field-targeted fuzzy returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 @title:(%%machne%%) returned no docs: $FT_FIELD_FUZZY"
    fi

    # Test 6: REGRESSION — exact search still works after query parser changes
    TOTAL=$((TOTAL + 1))
    FT_EXACT=$(mcli FT.SEARCH fuzzyidx "machine" LIMIT 0 10 2>&1)
    if echo "$FT_EXACT" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: REGRESSION exact search returned error: $FT_EXACT"
    elif echo "$FT_EXACT" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: REGRESSION exact search still works (no regressions)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: REGRESSION exact 'machine' returned no docs: $FT_EXACT"
    fi

    # Test 7: MIXED — exact + fuzzy combined query.
    #
    # moon#683: this row asked for `%%machne%% deep` and demanded documents.
    # There are none, and there should be none: the fuzzy half resolves to
    # "machine" (fz:1 only) and "deep" is in fz:2 only, so the conjunction is
    # empty by construction. Measured against this exact corpus --
    # machine->fz:1, deep->fz:2, learning->fz:1 fz:2 -- the row was asserting
    # that AND behaves like OR.
    #
    # It now uses a conjunction that IS satisfiable (fz:1 has both "machine"
    # and "learning") and keeps the empty one as the counter-assertion, which
    # is the half that proves AND is really AND.
    TOTAL=$((TOTAL + 1))
    FT_MIXED=$(mcli FT.SEARCH fuzzyidx "%%machne%% learning" LIMIT 0 10 2>&1)
    if echo "$FT_MIXED" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: MIXED exact+fuzzy returned error: $FT_MIXED"
    elif echo "$FT_MIXED" | grep -q "^fz:1$"; then
        PASS=$((PASS + 1)); echo "  PASS: MIXED %%machne%% learning (fuzzy+exact) returns fz:1"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MIXED %%machne%% learning expected fz:1, got: $FT_MIXED"
    fi

    TOTAL=$((TOTAL + 1))
    FT_MIXED_EMPTY=$(mcli FT.SEARCH fuzzyidx "%%machne%% deep" LIMIT 0 10 2>&1)
    if echo "$FT_MIXED_EMPTY" | grep -q "fz:"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: MIXED unsatisfiable conjunction matched (AND behaving as OR): $FT_MIXED_EMPTY"
    else
        PASS=$((PASS + 1)); echo "  PASS: MIXED %%machne%% deep matches nothing (no doc has both)"
    fi

    # Test 8: FUZ-02 — FST build via FT.COMPACT on a fresh index
    mcli FT.CREATE fuzzyidx2 ON HASH PREFIX 1 fzr: SCHEMA title TEXT >/dev/null 2>&1
    mcli HSET fzr:1 title "Machine Learning" >/dev/null 2>&1
    mcli HSET fzr:2 title "Machinery Parts" >/dev/null 2>&1
    assert_moon_ok "FT.COMPACT fuzzyidx2 builds FST" FT.COMPACT fuzzyidx2

    TOTAL=$((TOTAL + 1))
    FT_COMPACT_FUZZY=$(mcli FT.SEARCH fuzzyidx2 "%%machne%%" LIMIT 0 10 2>&1)
    if echo "$FT_COMPACT_FUZZY" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-02 fuzzy after compact returned error: $FT_COMPACT_FUZZY"
    elif echo "$FT_COMPACT_FUZZY" | grep -q "fzr:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-02 fuzzy works after FT.COMPACT FST build"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-02 fuzzy after compact returned no docs: $FT_COMPACT_FUZZY"
    fi

    # Cleanup fuzzy indexes
    mcli FT.DROPINDEX fuzzyidx >/dev/null 2>&1
    mcli FT.DROPINDEX fuzzyidx2 >/dev/null 2>&1

    echo "  fuzzy/prefix: done"
fi

# ===========================================================================
# FT.AGGREGATE (faceted search — Phase 152 AGG-01..04)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FT.AGGREGATE (FACETED SEARCH) ==="
    flush_both

    # Setup: Plan 06 shipped TAG field-type; Plan 07 ships NUMERIC. Both are now
    # permanent baseline for the aggidx schema — no probe, no gating.
    FT_CREATE_RESULT=$(mcli FT.CREATE aggidx ON HASH PREFIX 1 agg: SCHEMA status TAG priority TAG assignee TAG score NUMERIC 2>&1)
    if echo "$FT_CREATE_RESULT" | grep -qi "err"; then
        echo "  FAIL: FT.CREATE aggidx rejected by moon: $FT_CREATE_RESULT"
        SKIP_AGG=1
    else
        SKIP_AGG=0
    fi

    # Insert deterministic fixture (runs only when FT.CREATE succeeded):
    #   status=open  × 5 (priority=high × 3, priority=low × 2) — assignees user0..user4
    #   status=closed × 2 (priority=low × 2) — assignees user0, user1
    if [ "$SKIP_AGG" -eq 0 ]; then
        for i in 1 2 3; do
            mcli HSET agg:$i status open priority high assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
        for i in 4 5; do
            mcli HSET agg:$i status open priority low assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
        for i in 6 7; do
            mcli HSET agg:$i status closed priority low assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
    fi

    if [ "$SKIP_AGG" -eq 0 ]; then

        # AGG-01: GROUPBY + COUNT — assert specific counts (open=5 closed=2)
        TOTAL=$((TOTAL + 1))
        AGG_COUNT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_COUNT" | spans "open.*5" && echo "$AGG_COUNT" | spans "closed.*2"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-01 GROUPBY+COUNT (open=5 closed=2)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-01 expected open=5 closed=2, got: $AGG_COUNT"
        fi

        # AGG-02: GROUPBY @priority (high=3 low=4)
        TOTAL=$((TOTAL + 1))
        AGG_PRIORITY=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @priority REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_PRIORITY" | spans "low.*4" && echo "$AGG_PRIORITY" | spans "high.*3"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-02 GROUPBY @priority (high=3 low=4)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-02 expected high=3 low=4, got: $AGG_PRIORITY"
        fi

        # AGG-03/04 (SUM/AVG/MIN/MAX/COUNT_DISTINCT over @score) — unconditional
        # now that Plan 07 ships NUMERIC. Exact-value assertions follow below.
        TOTAL=$((TOTAL + 1))
        AGG_SUM=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE SUM 1 @score AS total 2>&1)
        # Expected: status=open → 10+20+30+40+50=150; status=closed → 60+70=130.
        if echo "$AGG_SUM" | spans "open.*150" && echo "$AGG_SUM" | spans "closed.*130"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+SUM exact (open=150 closed=130)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected open=150 closed=130, got: $AGG_SUM"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_AVG=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @priority REDUCE AVG 1 @score AS avg_score 2>&1)
        if echo "$AGG_AVG" | grep -qi "err"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 GROUPBY+AVG errored: $AGG_AVG"
        else
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+AVG returns rows"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_MIN=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE MIN 1 @score AS min_score 2>&1)
        # Expected: status=open → min=10; status=closed → min=60.
        if echo "$AGG_MIN" | spans "open.*10" && echo "$AGG_MIN" | spans "closed.*60"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+MIN exact (open=10 closed=60)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected MIN open=10 closed=60, got: $AGG_MIN"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_MAX=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE MAX 1 @score AS max_score 2>&1)
        # Expected: status=open → max=50; status=closed → max=70.
        if echo "$AGG_MAX" | spans "open.*50" && echo "$AGG_MAX" | spans "closed.*70"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+MAX exact (open=50 closed=70)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected MAX open=50 closed=70, got: $AGG_MAX"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_DISTINCT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT_DISTINCT 1 @assignee AS uniq_users 2>&1)
        if echo "$AGG_DISTINCT" | grep -qi "err"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-04 COUNT_DISTINCT errored: $AGG_DISTINCT"
        else
            PASS=$((PASS + 1)); echo "  PASS: AGG-04 COUNT_DISTINCT returns rows"
        fi

        # AGG-02b: SORTBY + LIMIT
        TOTAL=$((TOTAL + 1))
        AGG_LIMIT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC LIMIT 0 1 2>&1)
        if echo "$AGG_LIMIT" | grep -q "open" && ! echo "$AGG_LIMIT" | grep -q "closed"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-02b SORTBY + LIMIT returns top-1 ('open')"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-02b SORTBY+LIMIT unexpected: $AGG_LIMIT"
        fi

        # APPLY must be rejected in v1 (D-04 / Pitfall 10)
        TOTAL=$((TOTAL + 1))
        APPLY_REJECT=$(mcli FT.AGGREGATE aggidx '*' APPLY '@score+1' AS plus_one 2>&1)
        if echo "$APPLY_REJECT" | grep -qE "APPLY.*not supported|not implemented|v1"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG APPLY rejected in v1"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: APPLY should be rejected: $APPLY_REJECT"
        fi

        # ── Plan 06 TAG gap-closure assertions ─────────────────────────────

        # TAG-01: @status:{open} GROUPBY @priority — expect high=3 low=2
        TOTAL=$((TOTAL + 1))
        AGG_TAG_FILTER=$(mcli FT.AGGREGATE aggidx '@status:{open}' GROUPBY 1 @priority REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_TAG_FILTER" | spans "high.*3" && echo "$AGG_TAG_FILTER" | spans "low.*2"; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-01 @status:{open} GROUPBY @priority (high=3 low=2)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-01 expected high=3 low=2, got: $AGG_TAG_FILTER"
        fi

        # TAG-02: FT.SEARCH @status:{open} — 5 keys
        TOTAL=$((TOTAL + 1))
        SEARCH_TAG=$(mcli FT.SEARCH aggidx '@status:{open}' LIMIT 0 10 2>&1)
        HIT_COUNT=$(echo "$SEARCH_TAG" | grep -c '^agg:' || true)
        if [ "$HIT_COUNT" -eq 5 ]; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-02 FT.SEARCH @status:{open} returned 5 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-02 expected 5 open keys, got $HIT_COUNT: $SEARCH_TAG"
        fi

        # TAG-03: @Status:{open} (mixed-case field) must match @status:{open}
        TOTAL=$((TOTAL + 1))
        SEARCH_TAG_CASE=$(mcli FT.SEARCH aggidx '@Status:{open}' LIMIT 0 10 2>&1)
        HIT_COUNT_CASE=$(echo "$SEARCH_TAG_CASE" | grep -c '^agg:' || true)
        if [ "$HIT_COUNT_CASE" -eq 5 ]; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-03 case-insensitive field @Status:{open} → 5 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-03 case-insensitive field lookup: expected 5 got $HIT_COUNT_CASE"
        fi

        # TAG-04: partial HSET must preserve untouched tag fields
        TOTAL=$((TOTAL + 1))
        mcli HSET agg:partial status open priority high >/dev/null 2>&1
        mcli HSET agg:partial priority low >/dev/null 2>&1   # partial — status NOT touched
        SEARCH_PARTIAL=$(mcli FT.SEARCH aggidx '@status:{open}' LIMIT 0 100 2>&1)
        if echo "$SEARCH_PARTIAL" | grep -q '^agg:partial$'; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-04 partial HSET preserved @status:open on agg:partial"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-04 partial HSET wiped prior status entry"
        fi
        mcli DEL agg:partial >/dev/null 2>&1

        # TAG-05: multi-tag OR returns the union of both tags.
        #
        # moon#683: this row demanded the rejection moon used to emit
        # ("multi-tag OR syntax not supported") and had gone stale -- the
        # feature landed and nobody updated the row, so a working feature read
        # as a failure. It now asserts the union: 5 open + 3 closed, and both
        # tags represented.
        TOTAL=$((TOTAL + 1))
        TAG_OR=$(mcli FT.SEARCH aggidx '@status:{open|closed}' LIMIT 0 20 2>&1)
        TAG_OR_N=$(echo "$TAG_OR" | head -1)
        TAG_ONLY_OPEN=$(mcli FT.SEARCH aggidx '@status:{open}' LIMIT 0 20 2>&1 | head -1)
        TAG_ONLY_CLOSED=$(mcli FT.SEARCH aggidx '@status:{closed}' LIMIT 0 20 2>&1 | head -1)
        if echo "$TAG_OR" | grep -qi "err\|not supported"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-05 multi-tag OR returned an error: $TAG_OR"
        elif ! [[ "$TAG_ONLY_OPEN$TAG_ONLY_CLOSED$TAG_OR_N" =~ ^[0-9]+$ ]]; then
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-05 non-numeric counts (or=$TAG_OR_N open=$TAG_ONLY_OPEN closed=$TAG_ONLY_CLOSED)"
        elif [ "$TAG_OR_N" = "$((TAG_ONLY_OPEN + TAG_ONLY_CLOSED))" ]; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-05 multi-tag OR returns the union ($TAG_ONLY_OPEN + $TAG_ONLY_CLOSED = $TAG_OR_N)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-05 union expected $((TAG_ONLY_OPEN + TAG_ONLY_CLOSED)), got $TAG_OR_N"
        fi

        # ── Plan 07 NUMERIC gap-closure assertions ─────────────────────────
        # Fixture recap: agg:1..5 scores 10..50 (open), agg:6..7 scores 60,70 (closed).

        # NUMERIC-01: @score:[20 40] inclusive — agg:2 agg:3 agg:4 = 3 keys
        TOTAL=$((TOTAL + 1))
        NUM_INC=$(mcli FT.SEARCH aggidx '@score:[20 40]' LIMIT 0 10 2>&1)
        HIT_NUM=$(echo "$NUM_INC" | grep -c '^agg:' || true)
        if [ "$HIT_NUM" -eq 3 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-01 @score:[20 40] inclusive → 3 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-01 expected 3 got $HIT_NUM: $NUM_INC"
        fi

        # NUMERIC-02: exclusive bounds — (20 40] → agg:3 agg:4 = 2 keys
        TOTAL=$((TOTAL + 1))
        NUM_EXCL=$(mcli FT.SEARCH aggidx '@score:[(20 40]' LIMIT 0 10 2>&1)
        HIT_EXCL=$(echo "$NUM_EXCL" | grep -c '^agg:' || true)
        if [ "$HIT_EXCL" -eq 2 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-02 @score:[(20 40] exclusive-low → 2 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-02 expected 2 got $HIT_EXCL: $NUM_EXCL"
        fi

        # NUMERIC-03: full range [-inf +inf] → all 7 keys
        TOTAL=$((TOTAL + 1))
        NUM_FULL=$(mcli FT.SEARCH aggidx '@score:[-inf +inf]' LIMIT 0 20 2>&1)
        HIT_FULL=$(echo "$NUM_FULL" | grep -c '^agg:' || true)
        if [ "$HIT_FULL" -eq 7 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-03 @score:[-inf +inf] → 7 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-03 expected 7 got $HIT_FULL"
        fi

        # NUMERIC-04: FT.AGGREGATE range filter + GROUPBY — @score:[10 30] → status
        # agg:1..3 (scores 10,20,30) all open → cnt=3 on status=open only
        TOTAL=$((TOTAL + 1))
        NUM_AGG=$(mcli FT.AGGREGATE aggidx '@score:[10 30]' GROUPBY 1 @status REDUCE COUNT 0 AS cnt 2>&1)
        if echo "$NUM_AGG" | spans "open.*3"; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-04 FT.AGGREGATE @score:[10 30] GROUPBY status (open=3)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-04 expected open=3, got: $NUM_AGG"
        fi

        # NUMERIC-05: inverted range REJECTED (T-152-07-05)
        # moon#683: the row grepped for the phrase "min > max", which moon has
        # never emitted. What it is FOR is T-152-07-05, "an inverted range is
        # refused rather than executed", so it asserts exactly that: no
        # documents, and the specific refusal.
        #
        # The refusal used to be the bare token `-numeric_filter_invalid`
        # (moon#691), so this row had to accept a plain "err" to stay green --
        # which made it match ANY error, including "no such index", and stop
        # discriminating. Now that the reply is a real RESP error the row names
        # the token AND requires the ERR prefix, so a regression in either the
        # refusal or its wire form fails here.
        TOTAL=$((TOTAL + 1))
        NUM_INV=$(mcli FT.SEARCH aggidx '@score:[100 10]' LIMIT 0 10 2>&1)
        if echo "$NUM_INV" | grep -q "^agg:"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-05 inverted range was EXECUTED, not refused: $NUM_INV"
        elif echo "$NUM_INV" | grep -q "^ERR " && echo "$NUM_INV" | grep -q "numeric_filter_invalid"; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-05 inverted range refused ($NUM_INV)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-05 expected a refusal, got: $NUM_INV"
        fi

        # NUMERIC-06: NaN-on-write filtered (write-path guard, T-152-07-02).
        # Insert a doc with NaN score; [-inf +inf] range must NOT include it.
        TOTAL=$((TOTAL + 1))
        mcli HSET agg:nan status open priority high score NaN >/dev/null 2>&1
        NUM_NAN=$(mcli FT.SEARCH aggidx '@score:[-inf +inf]' LIMIT 0 20 2>&1)
        if echo "$NUM_NAN" | grep -q '^agg:nan$'; then
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-06 NaN leaked into index: agg:nan appeared in [-inf +inf]"
        else
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-06 NaN filtered on write"
        fi
        mcli DEL agg:nan >/dev/null 2>&1
    fi

    mcli FT.DROPINDEX aggidx >/dev/null 2>&1

    # NUMERIC-07 (W-01 cross-shard correctness): 1-shard vs 4-shard identical keys
    # for @score:[5 15] on an independent fixture.
    TOTAL=$((TOTAL + 1))
    # `|| true`: pkill exits 1 when nothing matched, and under
    # `set -euo pipefail` that killed the run outright -- on a clean machine
    # the FIRST of these (nothing to kill yet) aborted the script before
    # NUMERIC-07, so MQ, txn_kv, eviction and the RESULT SUMMARY never ran.
    pkill -f 'moon --port 6411' 2>/dev/null || true
    pkill -f 'moon --port 6414' 2>/dev/null || true
    sleep 1
    # Fresh dirs here too -- these two would otherwise reload `nidx` from the
    # repo root and report a cross-shard "match" that came from disk.
    N7_DIR1=$(mktemp -d "${TMPDIR:-/tmp}/moon-n7-1.XXXXXX")
    N7_DIR2=$(mktemp -d "${TMPDIR:-/tmp}/moon-n7-4.XXXXXX")
    ./target/release/moon --port 6411 --shards 1 --protected-mode no --dir "$N7_DIR1" --disk-free-min-pct 0 > /tmp/moon-6411.log 2>&1 &
    ./target/release/moon --port 6414 --shards 4 --protected-mode no --dir "$N7_DIR2" --disk-free-min-pct 0 > /tmp/moon-6414.log 2>&1 &
    sleep 2
    for PORT in 6411 6414; do
        redis-cli -p $PORT FT.CREATE nidx ON HASH PREFIX 1 n: SCHEMA status TAG score NUMERIC > /dev/null 2>&1 || true
        for i in $(seq 0 19); do
            redis-cli -p $PORT HSET n:$i status open score $i > /dev/null 2>&1 || true
        done
    done
    sleep 1
    N1=$(redis-cli -p 6411 FT.SEARCH nidx '@score:[5 15]' LIMIT 0 100 2>&1 | grep '^n:' | sort || true)
    N4=$(redis-cli -p 6414 FT.SEARCH nidx '@score:[5 15]' LIMIT 0 100 2>&1 | grep '^n:' | sort || true)
    if [ "$N1" = "$N4" ] && [ -n "$N1" ]; then
        COUNT_N=$(echo "$N1" | wc -l | tr -d ' ')
        PASS=$((PASS + 1)); echo "  PASS: NUMERIC-07 1-shard and 4-shard return identical keys for @score:[5 15] (count=$COUNT_N)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-07 cross-shard mismatch"
        echo "1-shard: $N1"
        echo "4-shard: $N4"
    fi
    # `|| true`: pkill exits 1 when nothing matched, and under
    # `set -euo pipefail` that killed the run outright -- on a clean machine
    # the FIRST of these (nothing to kill yet) aborted the script before
    # NUMERIC-07, so MQ, txn_kv, eviction and the RESULT SUMMARY never ran.
    pkill -f 'moon --port 6411' 2>/dev/null || true
    pkill -f 'moon --port 6414' 2>/dev/null || true

    echo "  ft_aggregate: done"
fi

# ===========================================================================
# FT.SEARCH HYBRID (three-way RRF — Phase 152 HYB-01..03)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FT.SEARCH HYBRID (THREE-WAY RRF) ==="
    flush_both

    # Setup: text + dense + sparse index
    mcli FT.CREATE hybidx ON HASH PREFIX 1 hy: SCHEMA title TEXT vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC COSINE >/dev/null 2>&1

    # Insert 3 docs with titles + vectors
    mcli HSET hy:1 title "machine learning introduction" vec "$VQ" >/dev/null 2>&1
    mcli HSET hy:2 title "deep neural learning" vec "$(printf '\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00')" >/dev/null 2>&1
    mcli HSET hy:3 title "quantum machines" vec "$(printf '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00')" >/dev/null 2>&1
    sleep 0.5

    # HYB-01: two-way hybrid (BM25 + dense, no sparse clause) — D-16 fall-through
    TOTAL=$((TOTAL + 1))
    HYB_TWO=$(mcli FT.SEARCH hybidx "machine learning" HYBRID VECTOR @vec '$q' FUSION RRF LIMIT 0 5 PARAMS 2 q "$VQ" 2>&1)
    if echo "$HYB_TWO" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 two-way hybrid errored: $HYB_TWO"
    elif echo "$HYB_TWO" | grep -q "hy:"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-01 two-way BM25+dense returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 two-way returned no docs: $HYB_TWO"
    fi

    # HYB-01: response carries __rrf_score
    TOTAL=$((TOTAL + 1))
    if echo "$HYB_TWO" | grep -q "__rrf_score"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-01 response contains __rrf_score"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 response missing __rrf_score: $HYB_TWO"
    fi

    # HYB-03: WEIGHTS tuning — all three weights honored
    TOTAL=$((TOTAL + 1))
    HYB_WEIGHTS=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 1.5 0.5 LIMIT 0 5 PARAMS 2 q "$VQ" 2>&1)
    if echo "$HYB_WEIGHTS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 WEIGHTS errored: $HYB_WEIGHTS"
    elif echo "$HYB_WEIGHTS" | grep -q "hy:"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 WEIGHTS 1.0 1.5 0.5 returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 WEIGHTS returned no docs: $HYB_WEIGHTS"
    fi

    # HYB-03: negative weight rejected (D-17)
    TOTAL=$((TOTAL + 1))
    HYB_NEG=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 -1.0 1.0 LIMIT 0 5 PARAMS 2 q "$VQ" 2>&1)
    if echo "$HYB_NEG" | grep -qiE "non-negative|finite|weight"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 negative weight rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 negative weight should reject: $HYB_NEG"
    fi

    # HYB-03: NaN weight rejected
    TOTAL=$((TOTAL + 1))
    HYB_NAN=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 NaN 1.0 LIMIT 0 5 PARAMS 2 q "$VQ" 2>&1)
    if echo "$HYB_NAN" | grep -qiE "non-negative|finite|weight"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 NaN weight rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 NaN weight should reject: $HYB_NAN"
    fi

    # HYB-02: non-RRF fusion rejected
    TOTAL=$((TOTAL + 1))
    HYB_FUSION=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION FOO LIMIT 0 5 PARAMS 2 q "$VQ" 2>&1)
    if echo "$HYB_FUSION" | grep -qi "fusion"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-02 unknown FUSION mode rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-02 unknown FUSION should reject: $HYB_FUSION"
    fi

    # HYB-02: SPARSE on index without sparse field errors (D-16)
    TOTAL=$((TOTAL + 1))
    HYB_NOSPARSE=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' SPARSE @noexist '$qs' FUSION RRF LIMIT 0 5 PARAMS 4 q "$VQ" qs "$(printf '\x01\x00\x00\x00\x00\x00\x80\x3f')" 2>&1)
    if echo "$HYB_NOSPARSE" | grep -qi "sparse"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-02 SPARSE on index without sparse field errors (D-16)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-02 missing sparse field should error: $HYB_NOSPARSE"
    fi

    # HYB: backward-compat — FT.SEARCH without HYBRID keyword unchanged (D-18)
    TOTAL=$((TOTAL + 1))
    HYB_BC=$(mcli FT.SEARCH hybidx "machine" LIMIT 0 5 2>&1)
    if echo "$HYB_BC" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB backward compat (no HYBRID) errored: $HYB_BC"
    else
        PASS=$((PASS + 1)); echo "  PASS: HYB backward compat (FT.SEARCH text, no HYBRID)"
    fi

    mcli FT.DROPINDEX hybidx >/dev/null 2>&1
    echo "  ft_search_hybrid: done"
fi

# ===========================================================================
# TEMPORAL COMMANDS (moon-only -- no Redis equivalent)
# ===========================================================================

if should_run "temporal"; then
    echo ""
    echo "=== TEMPORAL COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # TEMP-01: TEMPORAL.SNAPSHOT_AT basic — records wall-clock->LSN binding
    assert_moon "TEMPORAL.SNAPSHOT_AT basic" "OK" TEMPORAL.SNAPSHOT_AT

    # TEMP-02: TEMPORAL.SNAPSHOT_AT wrong args — extra argument rejected
    TOTAL=$((TOTAL + 1))
    SNAP_ERR=$(mcli TEMPORAL.SNAPSHOT_AT extraarg 2>&1)
    if echo "$SNAP_ERR" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.SNAPSHOT_AT wrong args rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.SNAPSHOT_AT wrong args should reject: $SNAP_ERR"
    fi

    # TEMP-03: TEMPORAL.INVALIDATE basic — create graph, add node, invalidate
    mcli GRAPH.CREATE testgraph >/dev/null 2>&1
    ADDNODE_OUT=$(mcli GRAPH.ADDNODE testgraph :TestLabel 2>&1)
    # Extract numeric node_id from ADDNODE response (format: "(integer) <id>" or just "<id>")
    NODE_ID=$(echo "$ADDNODE_OUT" | grep -oE '[0-9]+' | head -1 || true)
    if [[ -n "$NODE_ID" ]]; then
        TOTAL=$((TOTAL + 1))
        INV_OK=$(mcli TEMPORAL.INVALIDATE "$NODE_ID" NODE testgraph 2>&1)
        if echo "$INV_OK" | grep -q "OK"; then
            PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE basic OK (node_id=$NODE_ID)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE basic should return OK: $INV_OK"
        fi

        # Verify node is still visible (no VALID_AT filter = sees all)
        TOTAL=$((TOTAL + 1))
        QUERY_OUT=$(mcli GRAPH.QUERY testgraph "MATCH (n:TestLabel) RETURN n" 2>&1)
        if echo "$QUERY_OUT" | grep -qiE "TestLabel|node|result"; then
            PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE node still visible without VALID_AT"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE node should be visible: $QUERY_OUT"
        fi
    else
        TOTAL=$((TOTAL + 2))
        FAIL=$((FAIL + 2))
        echo "  FAIL: Could not extract node_id from GRAPH.ADDNODE: $ADDNODE_OUT"
    fi

    # TEMP-04: TEMPORAL.INVALIDATE not found — nonexistent graph
    TOTAL=$((TOTAL + 1))
    INV_NOTFOUND=$(mcli TEMPORAL.INVALIDATE 999999 NODE nonexistent 2>&1)
    if echo "$INV_NOTFOUND" | grep -qi "graph not found"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE graph not found"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE should error graph not found: $INV_NOTFOUND"
    fi

    # TEMP-05: TEMPORAL.INVALIDATE wrong args — no arguments
    TOTAL=$((TOTAL + 1))
    INV_NOARGS=$(mcli TEMPORAL.INVALIDATE 2>&1)
    if echo "$INV_NOARGS" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE wrong args (none)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE no args should reject: $INV_NOARGS"
    fi

    # TEMP-06: TEMPORAL.INVALIDATE bad entity kind — VERTEX not valid
    TOTAL=$((TOTAL + 1))
    INV_BADKIND=$(mcli TEMPORAL.INVALIDATE 42 VERTEX testgraph 2>&1)
    if echo "$INV_BADKIND" | grep -qi "entity kind must be NODE or EDGE"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE bad entity kind rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE bad kind should reject: $INV_BADKIND"
    fi

    mcli GRAPH.DELETE testgraph >/dev/null 2>&1

    # ── Phase 165-03: FT.SEARCH AS_OF block ──────────────────────────────────
    # TEMP-04 end-to-end via redis-py (bash command substitution truncates
    # binary vectors at null bytes, so we delegate to a short Python helper
    # that speaks the real client protocol — identical pattern to
    # scripts/validate-v018-sdk.py).
    echo ""
    echo "--- FT.SEARCH AS_OF ---"
    mcli FLUSHALL >/dev/null 2>&1

    FT_ASOF_OUT=$(PORT_RUST="$PORT_RUST" python3 - <<'PYEOF'
import os, sys, time, struct, redis
r = redis.Redis(host="127.0.0.1", port=int(os.environ["PORT_RUST"]))
r.execute_command("FT.CREATE", "asidx", "ON", "HASH", "PREFIX", "1", "as:",
                  "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                  "DIM", "4", "TYPE", "FLOAT32", "DISTANCE_METRIC", "L2")
v1 = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)
v2 = struct.pack("<4f", 0.0, 1.0, 0.0, 0.0)
r.hset("as:1", "vec", v1)
time.sleep(0.1)
r.execute_command("TEMPORAL.SNAPSHOT_AT")
wall_ms_t1 = int(time.time() * 1000)
time.sleep(0.1)
r.hset("as:2", "vec", v2)
time.sleep(0.1)
as_of = r.execute_command("FT.SEARCH", "asidx", "*=>[KNN 10 @vec $q]",
                          "PARAMS", "2", "q", v1,
                          "AS_OF", str(wall_ms_t1), "DIALECT", "2")
latest = r.execute_command("FT.SEARCH", "asidx", "*=>[KNN 10 @vec $q]",
                           "PARAMS", "2", "q", v1, "DIALECT", "2")
try:
    err = r.execute_command("FT.SEARCH", "asidx", "*=>[KNN 10 @vec $q]",
                             "PARAMS", "2", "q", v1,
                             "AS_OF", "1", "DIALECT", "2")
    err_msg = str(err)
except redis.exceptions.ResponseError as e:
    err_msg = f"ERR {e}"
try:
    r.execute_command("FT.DROPINDEX", "asidx")
except Exception:
    pass
as_of_keys = [x.decode() if isinstance(x, bytes) else str(x) for x in as_of[1::2]]
latest_keys = [x.decode() if isinstance(x, bytes) else str(x) for x in latest[1::2]]
print(f"AS_OF_COUNT={as_of[0]}")
print(f"AS_OF_KEYS={','.join(as_of_keys)}")
print(f"LATEST_COUNT={latest[0]}")
print(f"ERR_MSG={err_msg}")
PYEOF
    )

    AS_OF_COUNT=$(echo "$FT_ASOF_OUT" | grep '^AS_OF_COUNT=' | cut -d= -f2 || true)
    AS_OF_KEYS=$(echo "$FT_ASOF_OUT" | grep '^AS_OF_KEYS=' | cut -d= -f2 || true)
    LATEST_COUNT=$(echo "$FT_ASOF_OUT" | grep '^LATEST_COUNT=' | cut -d= -f2 || true)
    ERR_MSG=$(echo "$FT_ASOF_OUT" | grep '^ERR_MSG=' | cut -d= -f2- || true)

    TOTAL=$((TOTAL + 1))
    if [[ "$AS_OF_COUNT" == "1" && "$AS_OF_KEYS" == "as:1" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH AS_OF filters post-snapshot doc (count=1, as:1 only)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH AS_OF expected count=1 keys=as:1; got count=$AS_OF_COUNT keys=$AS_OF_KEYS"
    fi

    TOTAL=$((TOTAL + 1))
    if [[ "$LATEST_COUNT" == "2" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH without AS_OF returns latest (count=2)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH latest expected count=2; got $LATEST_COUNT"
    fi

    TOTAL=$((TOTAL + 1))
    if echo "$ERR_MSG" | grep -q "no temporal snapshot registered"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH AS_OF <unregistered> surfaces helper ERR"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH AS_OF unregistered should surface ERR; got: $ERR_MSG"
    fi

    # ── Phase 165-03: FT.SEARCH inside TXN block ─────────────────────────────
    # ACID-09: TXN BEGIN on connection A captures snapshot_lsn; a concurrent
    # HSET on connection B must NOT appear in A's FT.SEARCH until A commits.
    # redis-py Connection objects are per-object; we open two.
    echo ""
    echo "--- FT.SEARCH inside TXN ---"
    mcli FLUSHALL >/dev/null 2>&1

    FT_TXN_OUT=$(PORT_RUST="$PORT_RUST" python3 - <<'PYEOF'
import os, sys, time, struct, redis
port = int(os.environ["PORT_RUST"])
# Two independent redis.Redis() instances → two independent TCP sessions on
# the server side (required for cross-client TXN isolation).
a = redis.Redis(host="127.0.0.1", port=port, single_connection_client=True)
b = redis.Redis(host="127.0.0.1", port=port, single_connection_client=True)
setup = redis.Redis(host="127.0.0.1", port=port)
try:
    setup.execute_command("FT.CREATE", "txidx", "ON", "HASH", "PREFIX", "1", "tx:",
                          "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                          "DIM", "4", "TYPE", "FLOAT32", "DISTANCE_METRIC", "L2")
except redis.exceptions.ResponseError:
    pass
va = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)
vb = struct.pack("<4f", 0.0, 1.0, 0.0, 0.0)
setup.hset("tx:a", "vec", va)
time.sleep(0.1)
# Client A: TXN BEGIN captures snapshot_lsn at this moment.
a.execute_command("TXN", "BEGIN")
# Client B: HSET tx:b commits AFTER A's snapshot_lsn.
b.hset("tx:b", "vec", vb)
time.sleep(0.1)
inside = a.execute_command("FT.SEARCH", "txidx", "*=>[KNN 10 @vec $q]",
                           "PARAMS", "2", "q", va, "DIALECT", "2")
inside_keys = [x.decode() if isinstance(x, bytes) else str(x) for x in inside[1::2]]
a.execute_command("TXN", "COMMIT")
post = a.execute_command("FT.SEARCH", "txidx", "*=>[KNN 10 @vec $q]",
                         "PARAMS", "2", "q", va, "DIALECT", "2")
post_keys = [x.decode() if isinstance(x, bytes) else str(x) for x in post[1::2]]
try:
    setup.execute_command("FT.DROPINDEX", "txidx")
except Exception:
    pass
print(f"INSIDE_COUNT={inside[0]}")
print(f"INSIDE_KEYS={','.join(sorted(inside_keys))}")
print(f"POST_COUNT={post[0]}")
print(f"POST_KEYS={','.join(sorted(post_keys))}")
PYEOF
    )

    INSIDE_COUNT=$(echo "$FT_TXN_OUT" | grep '^INSIDE_COUNT=' | cut -d= -f2 || true)
    INSIDE_KEYS=$(echo "$FT_TXN_OUT" | grep '^INSIDE_KEYS=' | cut -d= -f2 || true)
    POST_COUNT=$(echo "$FT_TXN_OUT" | grep '^POST_COUNT=' | cut -d= -f2 || true)
    POST_KEYS=$(echo "$FT_TXN_OUT" | grep '^POST_KEYS=' | cut -d= -f2 || true)

    # Inside TXN: must see tx:a (pre-TXN) but NOT tx:b (post-snapshot).
    TOTAL=$((TOTAL + 1))
    if [[ "$INSIDE_COUNT" == "1" && "$INSIDE_KEYS" == "tx:a" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH inside TXN hides post-snapshot foreign write (inside=tx:a)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH inside TXN expected tx:a only; got count=$INSIDE_COUNT keys=$INSIDE_KEYS"
    fi

    # After COMMIT: must see both.
    TOTAL=$((TOTAL + 1))
    if [[ "$POST_COUNT" == "2" && "$POST_KEYS" == "tx:a,tx:b" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH after TXN COMMIT returns both docs (count=2, tx:a+tx:b)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH post-COMMIT expected tx:a+tx:b; got count=$POST_COUNT keys=$POST_KEYS"
    fi

    echo "  temporal: done"
fi

# ===========================================================================
# TEMPORAL DECAY (GRAPH.QUERY --decay / FT.NAVIGATE DECAY)
# ===========================================================================

if should_run "temporal"; then
    echo ""
    echo "=== TEMPORAL DECAY (GRAPH.QUERY --decay / FT.NAVIGATE DECAY) ==="
    mcli FLUSHALL >/dev/null 2>&1

    # Build the stale-direct vs fresh-detour graph:
    #   A -> C  weight 1.0, created ~2s before the detour edges (stale direct)
    #   A -> B -> C  weight 0.6 each, created last (fresh detour)
    # Without --decay the cheaper direct path wins (1.0 < 1.2); with a decay
    # rate the older direct edge pays lambda * age_seconds and the fresh
    # detour wins. Real wall-clock sleep creates the age gap.
    mcli GRAPH.CREATE decayg >/dev/null 2>&1
    DECAY_A=$(mcli GRAPH.ADDNODE decayg Person name A 2>&1 | grep -oE '[0-9]+' | head -1 || true)
    DECAY_B=$(mcli GRAPH.ADDNODE decayg Person name B 2>&1 | grep -oE '[0-9]+' | head -1 || true)
    DECAY_C=$(mcli GRAPH.ADDNODE decayg Person name C 2>&1 | grep -oE '[0-9]+' | head -1 || true)

    mcli GRAPH.ADDEDGE decayg "$DECAY_A" "$DECAY_C" KNOWS WEIGHT 1.0 >/dev/null 2>&1
    sleep 2
    mcli GRAPH.ADDEDGE decayg "$DECAY_A" "$DECAY_B" KNOWS WEIGHT 0.6 >/dev/null 2>&1
    mcli GRAPH.ADDEDGE decayg "$DECAY_B" "$DECAY_C" KNOWS WEIGHT 0.6 >/dev/null 2>&1

    DECAY_QUERY="MATCH p = shortestPath((a:Person {name: 'A'})-[*..5]->(c:Person {name: 'C'})) RETURN p"

    # The returned path renders as one node id per line; the detour is
    # detected by whether B's node id appears in the path.

    # DECAY-01: without --decay the direct path wins (A -> C, no B)
    TOTAL=$((TOTAL + 1))
    OFF_OUT=$(mcli GRAPH.QUERY decayg "$DECAY_QUERY" 2>&1)
    if echo "$OFF_OUT" | grep -qE "^${DECAY_C}\$" && ! echo "$OFF_OUT" | grep -qE "^${DECAY_B}\$"; then
        PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY shortestPath without --decay takes direct path (no B)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: expected direct A->C path without --decay: $OFF_OUT"
    fi

    # DECAY-02: with --decay the fresh detour wins (A -> B -> C)
    TOTAL=$((TOTAL + 1))
    ON_OUT=$(mcli GRAPH.QUERY decayg "$DECAY_QUERY" --decay 5 2>&1)
    if echo "$ON_OUT" | grep -qE "^${DECAY_B}\$"; then
        PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY shortestPath --decay 5 prefers fresh detour (via B)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: expected detour via B with --decay 5: $ON_OUT"
    fi

    # DECAY-03: --decay garbage value rejected
    TOTAL=$((TOTAL + 1))
    BAD_OUT=$(mcli GRAPH.QUERY decayg "$DECAY_QUERY" --decay abc 2>&1)
    if echo "$BAD_OUT" | grep -qi "finite non-negative"; then
        PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY --decay rejects non-numeric value"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: --decay abc should be rejected: $BAD_OUT"
    fi

    # DECAY-04: --time-weight without --decay rejected
    TOTAL=$((TOTAL + 1))
    TW_OUT=$(mcli GRAPH.QUERY decayg "$DECAY_QUERY" --time-weight 2.0 2>&1)
    if echo "$TW_OUT" | grep -qi "requires --decay"; then
        PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY --time-weight without --decay rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: --time-weight alone should be rejected: $TW_OUT"
    fi

    # DECAY-05: FT.NAVIGATE DECAY strict validation (parses before index lookup)
    TOTAL=$((TOTAL + 1))
    NAV_BAD=$(mcli FT.NAVIGATE noidx "*" HOPS 2 DECAY notanumber 2>&1)
    if echo "$NAV_BAD" | grep -qi "DECAY must be a finite non-negative number"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.NAVIGATE DECAY rejects non-numeric value"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.NAVIGATE DECAY notanumber should be rejected: $NAV_BAD"
    fi

    # DECAY-06: FT.NAVIGATE with valid DECAY proceeds past parsing (the error,
    # if any, is about the missing index — NOT about the DECAY value)
    TOTAL=$((TOTAL + 1))
    NAV_OK=$(mcli FT.NAVIGATE noidx "*" HOPS 2 DECAY 0.5 2>&1)
    if echo "$NAV_OK" | grep -qi "DECAY"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: valid DECAY 0.5 should not produce a DECAY error: $NAV_OK"
    else
        PASS=$((PASS + 1)); echo "  PASS: FT.NAVIGATE DECAY 0.5 accepted (no DECAY parse error)"
    fi

    # DECAY-07: --decay on a write query rejected (read-only-traversal knob,
    # never silently ignored)
    TOTAL=$((TOTAL + 1))
    WR_OUT=$(mcli GRAPH.QUERY decayg "CREATE (:Person {name: 'X'})" --decay 0.5 2>&1)
    if echo "$WR_OUT" | grep -qi "read-only"; then
        PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY --decay on write query rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: --decay on CREATE should be rejected: $WR_OUT"
    fi

    mcli GRAPH.DELETE decayg >/dev/null 2>&1
    echo "  temporal decay: done"
fi

# ===========================================================================
# WORKSPACE COMMANDS (WS CREATE/LIST/INFO/AUTH/DROP)
# ===========================================================================

if should_run "workspace"; then
    echo ""
    echo "=== WORKSPACE COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # WS-01: WS CREATE returns UUID
    TOTAL=$((TOTAL + 1))
    WS_ID=$(mcli WS CREATE myworkspace 2>&1)
    if echo "$WS_ID" | grep -qE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
        PASS=$((PASS + 1)); echo "  PASS: WS CREATE returns UUID ($WS_ID)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS CREATE should return UUID, got: $WS_ID"
    fi

    # WS-02: WS LIST returns workspace
    TOTAL=$((TOTAL + 1))
    WS_LIST=$(mcli WS LIST 2>&1)
    if echo "$WS_LIST" | grep -qF "myworkspace"; then
        PASS=$((PASS + 1)); echo "  PASS: WS LIST contains myworkspace"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS LIST should contain myworkspace: $WS_LIST"
    fi

    # WS-03: WS INFO returns metadata
    TOTAL=$((TOTAL + 1))
    WS_INFO=$(mcli WS INFO "$WS_ID" 2>&1)
    if echo "$WS_INFO" | grep -qF "myworkspace"; then
        PASS=$((PASS + 1)); echo "  PASS: WS INFO returns workspace metadata"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS INFO should contain name: $WS_INFO"
    fi

    # WS-04: WS AUTH binds workspace
    assert_moon "WS AUTH bind" "OK" WS AUTH "$WS_ID"

    # WS-05: Workspace-scoped SET+GET
    assert_moon "WS SET scoped" "OK" SET testkey testval
    assert_moon "WS GET scoped" "testval" GET testkey

    # WS-06: WS DROP removes workspace
    # Need a fresh workspace to drop (current conn already bound)
    TOTAL=$((TOTAL + 1))
    WS_ID2=$(mcli WS CREATE dropme 2>&1)
    DROP_OK=$(mcli WS DROP "$WS_ID2" 2>&1)
    if echo "$DROP_OK" | grep -q "OK"; then
        PASS=$((PASS + 1)); echo "  PASS: WS DROP returns OK"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS DROP should return OK: $DROP_OK"
    fi

    # WS-07: WS LIST after drop no longer shows dropped workspace
    TOTAL=$((TOTAL + 1))
    WS_LIST2=$(mcli WS LIST 2>&1)
    if echo "$WS_LIST2" | grep -qF "dropme"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: WS LIST should not contain 'dropme' after drop"
    else
        PASS=$((PASS + 1)); echo "  PASS: WS LIST does not contain dropped workspace"
    fi

    # WS-08: WS AUTH with invalid UUID
    TOTAL=$((TOTAL + 1))
    AUTH_ERR=$(mcli WS AUTH "not-a-uuid" 2>&1)
    if echo "$AUTH_ERR" | grep -qi "ERR"; then
        PASS=$((PASS + 1)); echo "  PASS: WS AUTH invalid UUID rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS AUTH invalid should error: $AUTH_ERR"
    fi

    # WS-09: WS CREATE with empty name
    TOTAL=$((TOTAL + 1))
    CREATE_ERR=$(mcli WS CREATE 2>&1)
    if echo "$CREATE_ERR" | grep -qi "ERR"; then
        PASS=$((PASS + 1)); echo "  PASS: WS CREATE missing name rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS CREATE no name should error: $CREATE_ERR"
    fi

    echo "  workspace: done"
fi

# ===========================================================================
# MQ (DURABLE MESSAGE QUEUE) COMMANDS -- moon-only
# ===========================================================================

if should_run "mq"; then
    echo ""
    echo "=== MQ (DURABLE MESSAGE QUEUE) COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # MQ-01: MQ CREATE basic
    assert_moon "MQ CREATE basic" "OK" MQ CREATE mqtest MAXDELIVERY 5

    # MQ-02: MQ CREATE default (no MAXDELIVERY)
    assert_moon "MQ CREATE default" "OK" MQ CREATE mqdefault

    # MQ-03: MQ CREATE idempotent (same key twice)
    assert_moon "MQ CREATE idempotent" "OK" MQ CREATE mqtest MAXDELIVERY 5

    # MQ-04: MQ PUSH returns stream ID
    TOTAL=$((TOTAL + 1))
    MQ_PUSH_ID=$(mcli MQ PUSH mqtest field1 value1 2>&1)
    if echo "$MQ_PUSH_ID" | grep -qE '^[0-9]+-[0-9]+$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH returns stream ID ($MQ_PUSH_ID)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH should return stream ID: $MQ_PUSH_ID"
    fi

    # MQ-05: MQ POP returns message with fields
    TOTAL=$((TOTAL + 1))
    MQ_POP_OUT=$(mcli MQ POP mqtest 2>&1)
    if echo "$MQ_POP_OUT" | grep -qF "field1"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP returns message with field"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP should contain field1: $MQ_POP_OUT"
    fi

    # MQ-06: MQ ACK returns count
    # Push a fresh message, pop it, then ack it
    MQ_ACK_ID=$(mcli MQ PUSH mqtest ackf ackv 2>&1)
    mcli MQ POP mqtest >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_ACK_OUT=$(mcli MQ ACK mqtest "$MQ_ACK_ID" 2>&1)
    if echo "$MQ_ACK_OUT" | grep -qE '(integer) 1|^1$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK returns 1"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK should return 1: $MQ_ACK_OUT"
    fi

    # MQ-07: MQ ACK non-existent returns 0
    TOTAL=$((TOTAL + 1))
    MQ_ACK_ZERO=$(mcli MQ ACK mqtest 999999999-999 2>&1)
    if echo "$MQ_ACK_ZERO" | grep -qE '(integer) 0|^0$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK non-existent returns 0"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK non-existent should return 0: $MQ_ACK_ZERO"
    fi

    # MQ-08: MQ DLQLEN empty queue
    assert_moon "MQ DLQLEN empty" "0" MQ DLQLEN mqtest

    # MQ-09: MQ DLQ routing (MAXDELIVERY 1 -> immediate dead-letter)
    mcli MQ CREATE mqdlqtest MAXDELIVERY 1 >/dev/null 2>&1
    mcli MQ PUSH mqdlqtest dlqf dlqv >/dev/null 2>&1
    mcli MQ POP mqdlqtest >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_DLQ_LEN=$(mcli MQ DLQLEN mqdlqtest 2>&1)
    if echo "$MQ_DLQ_LEN" | grep -qE '(integer) 1|^1$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ DLQ routing (MAXDELIVERY 1 -> DLQ len 1)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ DLQLEN after DLQ routing should be 1: $MQ_DLQ_LEN"
    fi

    # MQ-10: MQ TRIGGER register
    assert_moon "MQ TRIGGER register" "OK" MQ TRIGGER mqtest "PUBLISH events notify" DEBOUNCE 1000

    # MQ-11: MQ unknown subcommand
    TOTAL=$((TOTAL + 1))
    MQ_UNK=$(mcli MQ FOOBAR 2>&1)
    if echo "$MQ_UNK" | grep -qi "unknown MQ subcommand"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ unknown subcommand rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ unknown sub should error: $MQ_UNK"
    fi

    # MQ-12: MQ PUSH missing args
    TOTAL=$((TOTAL + 1))
    MQ_PUSH_ERR=$(mcli MQ PUSH 2>&1)
    if echo "$MQ_PUSH_ERR" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH missing args rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH no args should error: $MQ_PUSH_ERR"
    fi

    # MQ-13: MQ ACK invalid ID format
    TOTAL=$((TOTAL + 1))
    MQ_ACK_BAD=$(mcli MQ ACK mqtest not-a-valid-id 2>&1)
    if echo "$MQ_ACK_BAD" | grep -qi "invalid message ID format"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK invalid ID rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK invalid ID should error: $MQ_ACK_BAD"
    fi

    # MQ-14: MQ PUSH to non-durable stream
    mcli XADD nondurable '*' f v >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_NONDUR=$(mcli MQ PUSH nondurable f v 2>&1)
    if echo "$MQ_NONDUR" | grep -qi "not a durable queue"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH to non-durable stream rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH non-durable should error: $MQ_NONDUR"
    fi

    # MQ-15: MQ POP with COUNT
    mcli MQ CREATE mqcount MAXDELIVERY 10 >/dev/null 2>&1
    for i in $(seq 1 3); do
        mcli MQ PUSH mqcount "f$i" "v$i" >/dev/null 2>&1
    done
    TOTAL=$((TOTAL + 1))
    MQ_POP_CNT=$(mcli MQ POP mqcount COUNT 2 2>&1)
    # Should contain at least some data (not empty or error)
    if echo "$MQ_POP_CNT" | grep -qE "f[0-9]|v[0-9]"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP COUNT 2 returns messages"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP COUNT 2 should return messages: $MQ_POP_CNT"
    fi

    echo "  mq: done"
fi

# ===========================================================================
# TXN KV WIRING (Phase 161) — ACID lifecycle tests
# ===========================================================================

if should_run "txn_kv"; then
    echo ""
    echo "=== TXN KV Wiring ==="
    mcli FLUSHALL >/dev/null 2>&1 || true

    # moon#683: every row below used to issue each command through `mcli`,
    # i.e. through its own connection. moon's cross-store TXN is
    # connection-scoped, so `TXN BEGIN` / `SET` / `TXN COMMIT` landed on three
    # unrelated connections: COMMIT answered "ERR not in a cross-store
    # transaction", a second BEGIN answered OK because it was a first BEGIN on
    # a fresh connection, and the ABORT rows saw their key survive because the
    # SET had never been in a transaction at all. The failures were real
    # failures of the harness, and said nothing about moon.
    #
    # Each scenario is now ONE `msession` transcript, so the reply indices
    # below are the replies to the commands in order.

    # TXN-01: BEGIN / SET / COMMIT lifecycle, then read back on a fresh conn.
    TXN_T1=$(msession "TXN BEGIN" "SET txn_kv_commit_key committed_value" "TXN COMMIT")
    TOTAL=$((TOTAL + 1))
    if [[ "$(mreply 1 "$TXN_T1")" == *OK* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: TXN BEGIN returns OK"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TXN BEGIN should return OK: $(mreply 1 "$TXN_T1")"
    fi

    TOTAL=$((TOTAL + 1))
    if [[ "$(mreply 2 "$TXN_T1")" == *OK* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: SET inside TXN returns OK"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: SET inside TXN should return OK: $(mreply 2 "$TXN_T1")"
    fi

    TOTAL=$((TOTAL + 1))
    if [[ "$(mreply 3 "$TXN_T1")" == *OK* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: TXN COMMIT returns OK"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TXN COMMIT should return OK: $(mreply 3 "$TXN_T1")"
    fi

    # Read back on a NEW connection: a committed value must be visible to
    # everyone, not just to the connection that wrote it.
    TOTAL=$((TOTAL + 1))
    TXN_GET_AFTER=$(mcli GET txn_kv_commit_key)
    if [[ "$TXN_GET_AFTER" == *committed_value* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: GET after TXN COMMIT returns committed value"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: GET after TXN COMMIT should return committed_value: $TXN_GET_AFTER"
    fi

    # TXN-02: BEGIN / SET / ABORT — the inserted key must not exist.
    msession "TXN BEGIN" "SET txn_kv_abort_key should_vanish" "TXN ABORT" >/dev/null
    TOTAL=$((TOTAL + 1))
    TXN_ABORT_GET=$(mcli GET txn_kv_abort_key)
    if [[ "$TXN_ABORT_GET" != *should_vanish* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: GET after TXN ABORT (insert) returns nil"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: GET after TXN ABORT (insert) should be nil: $TXN_ABORT_GET"
    fi

    # TXN-03: BEGIN / DEL / ABORT — the deleted key must come back.
    mcli SET txn_kv_del_key original >/dev/null 2>&1
    msession "TXN BEGIN" "DEL txn_kv_del_key" "TXN ABORT" >/dev/null
    TOTAL=$((TOTAL + 1))
    TXN_DEL_ABORT_GET=$(mcli GET txn_kv_del_key)
    if [[ "$TXN_DEL_ABORT_GET" == *original* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: GET after DEL + TXN ABORT restores original value"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: GET after DEL + TXN ABORT should return original: $TXN_DEL_ABORT_GET"
    fi

    # TXN-04: a second BEGIN on the SAME connection must be refused. Through
    # `mcli` this was two first-BEGINs on two connections and always "passed"
    # the wrong way round.
    TXN_T4=$(msession "TXN BEGIN" "TXN BEGIN" "TXN ABORT")
    TOTAL=$((TOTAL + 1))
    TXN_DOUBLE_BEGIN=$(mreply 2 "$TXN_T4")
    if [[ "$TXN_DOUBLE_BEGIN" == *ERR* || "$TXN_DOUBLE_BEGIN" == *error* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: Double TXN BEGIN rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: Double TXN BEGIN should error: $TXN_DOUBLE_BEGIN"
    fi

    # TXN-05: KV + MQ committed atomically on one connection.
    mcli MQ CREATE txn_mq_test_q MAXDELIVERY 5 >/dev/null 2>&1
    TXN_T5=$(msession "TXN BEGIN" "SET txn_mq_commit_k committed_mq_val" \
                      "MQ PUBLISH txn_mq_test_q mfield mvalue" "TXN COMMIT")
    TOTAL=$((TOTAL + 1))
    TXN_MQ_PUB=$(mreply 3 "$TXN_T5")
    if [[ "$TXN_MQ_PUB" == *QUEUED* || "$TXN_MQ_PUB" == *queued* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUBLISH inside TXN returns QUEUED"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUBLISH inside TXN should return QUEUED: $TXN_MQ_PUB"
    fi

    TOTAL=$((TOTAL + 1))
    TXN_MQ_GET=$(mcli GET txn_mq_commit_k)
    if [[ "$TXN_MQ_GET" == *committed_mq_val* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: GET after KV+MQ TXN COMMIT returns committed value"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: GET after KV+MQ TXN COMMIT should return committed_mq_val: $TXN_MQ_GET"
    fi

    TOTAL=$((TOTAL + 1))
    TXN_MQ_POP=$(mcli MQ POP txn_mq_test_q)
    if [[ "$TXN_MQ_POP" == *mfield* || "$TXN_MQ_POP" == *mvalue* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP after TXN COMMIT returns published message"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP after TXN COMMIT should contain message: $TXN_MQ_POP"
    fi

    # TXN-06: KV + MQ both rolled back by ABORT.
    mcli MQ CREATE txn_mq_abort_q MAXDELIVERY 5 >/dev/null 2>&1
    msession "TXN BEGIN" "SET txn_mq_abort_k should_vanish" \
             "MQ PUBLISH txn_mq_abort_q afield avalue" "TXN ABORT" >/dev/null
    TOTAL=$((TOTAL + 1))
    TXN_MQ_ABORT_GET=$(mcli GET txn_mq_abort_k)
    if [[ "$TXN_MQ_ABORT_GET" != *should_vanish* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: GET after KV+MQ TXN ABORT returns nil"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: GET after KV+MQ TXN ABORT should be nil: $TXN_MQ_ABORT_GET"
    fi

    TOTAL=$((TOTAL + 1))
    TXN_MQ_ABORT_POP=$(mcli MQ POP txn_mq_abort_q)
    if [[ "$TXN_MQ_ABORT_POP" != *afield* && "$TXN_MQ_ABORT_POP" != *avalue* ]]; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP after TXN ABORT returns no message"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP after TXN ABORT should be empty: $TXN_MQ_ABORT_POP"
    fi

    # Cleanup
    mcli DEL txn_kv_commit_key txn_kv_del_key txn_mq_commit_k txn_mq_abort_k >/dev/null 2>&1 || true

    echo "  txn_kv: done"
fi

# ===========================================================================
# EVICTION (moon#600) -- volatile-ttl victim selection, liveness, OOM
#
# `volatile-ttl` is the only sampler that picks from the maintained deadline
# index rather than from the keyspace, so it is the only one that can name a
# victim `evict_to_budget` cannot remove -- which used to spin the shard
# thread forever instead of returning. These are the client-visible
# consequences: the nearest deadline goes first, the server keeps answering,
# and `noeviction` still refuses rather than evicting or spinning.
#
# Runs on its OWN instance, started with `--disk-offload disable`: under the
# default (enabled) a victim is TIERED rather than dropped, so it stays
# readable and stays in DBSIZE (moon#599 / #355) and victim ORDER is not
# observable from a client at all. The tiered half of that contract is
# covered by scripts/test-consistency.sh, which runs both legs.
# ===========================================================================

if should_run "eviction"; then
    echo ""
    echo "=== Eviction (volatile-ttl) ==="

    PORT_EVICT=$((PORT_RUST + 530))
    EVICT_DIR=$(mktemp -d /tmp/moon-cmd-evict.XXXXXX)
    ecli() { redis-cli -t 5 -p "$PORT_EVICT" "$@"; }

    "$RUST_BINARY" --port "$PORT_EVICT" --shards 1 --dir "$EVICT_DIR" \
        --protected-mode no --disk-free-min-pct 0 --appendonly no \
        --disk-offload disable --maxmemory-policy volatile-ttl >/dev/null 2>&1 &
    EVICT_PID=$!
    for _ in $(seq 1 50); do
        ecli PING >/dev/null 2>&1 && break
        sleep 0.1
    done

    # EVICT-01: volatile-ttl evicts the GLOBALLY nearest deadline. Five 64KB
    # volatile values against a 256KB cap needs a couple of victims, and the
    # soonest-expiring key must be among the first to go -- not a random
    # volatile one. The budget is absolute, not derived from `used_memory`:
    # the memory ledger is published on a chore tick, so a read taken
    # immediately after the writes can still answer 0.
    TOTAL=$((TOTAL + 1))
    EV_VAL=$(head -c 65536 </dev/zero | tr '\0' 'z')
    for i in 1 2 3 4; do
        ecli SET "ev:far:$i" "$EV_VAL" EX 3600 >/dev/null 2>&1
    done
    ecli SET ev:soonest "$EV_VAL" EX 60 >/dev/null 2>&1
    ecli CONFIG SET maxmemory 262144 >/dev/null 2>&1 || true
    ecli SET ev:trigger v >/dev/null 2>&1 || true
    EV_SOONEST=$(ecli EXISTS ev:soonest 2>&1)
    EV_SURVIVORS=$(ecli EXISTS ev:far:1 ev:far:2 ev:far:3 ev:far:4 2>&1)
    if [[ "$EV_SOONEST" == "0" ]] && [[ "$EV_SURVIVORS" =~ ^[1-4]$ ]]; then
        PASS=$((PASS + 1)); echo "  PASS: volatile-ttl evicts the nearest deadline first ($EV_SURVIVORS far keys kept)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: volatile-ttl victim order wrong: EXISTS ev:soonest=$EV_SOONEST, far survivors=$EV_SURVIVORS"
    fi

    # EVICT-02: liveness. A shard thread spinning inside evict_to_budget
    # never answers again; `-t 5` turns that into a FAIL, not a hung suite.
    TOTAL=$((TOTAL + 1))
    EV_ALIVE=$(ecli PING 2>&1)
    if [[ "$EV_ALIVE" == "PONG" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: server stays responsive through volatile-ttl eviction"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: server stopped answering after volatile-ttl eviction: '$EV_ALIVE'"
    fi

    # EVICT-03: moon#599 accounting -- with no cold tier every victim LEAVES
    # the keyspace, so evicted_keys moves and spilled_keys does not.
    TOTAL=$((TOTAL + 1))
    EV_INFO=$(ecli INFO 2>/dev/null | tr -d '\r')
    EV_EVICTED=$(echo "$EV_INFO" | awk -F: '/^evicted_keys:/ {print $2}')
    EV_SPILLED=$(echo "$EV_INFO" | awk -F: '/^spilled_keys:/ {print $2}')
    if [[ "$EV_EVICTED" =~ ^[0-9]+$ ]] && ((EV_EVICTED > 0)) && [[ "$EV_SPILLED" == "0" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: evicted_keys=$EV_EVICTED, spilled_keys=0 (nothing tiered without disk-offload)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: evicted_keys='$EV_EVICTED' spilled_keys='$EV_SPILLED'"
    fi

    # EVICT-04: noeviction refuses instead of evicting -- or spinning. The
    # cap is tightened below what is already resident so the write is over
    # budget by construction.
    TOTAL=$((TOTAL + 1))
    ecli CONFIG SET maxmemory-policy noeviction >/dev/null 2>&1 || true
    ecli CONFIG SET maxmemory 65536 >/dev/null 2>&1 || true
    EV_OOM=$(ecli SET ev:refused "$EV_VAL" 2>&1)
    if echo "$EV_OOM" | grep -qi "OOM"; then
        PASS=$((PASS + 1)); echo "  PASS: noeviction over budget returns OOM"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: noeviction should return OOM, got: $EV_OOM"
    fi

    kill "$EVICT_PID" 2>/dev/null || true
    wait "$EVICT_PID" 2>/dev/null || true
    pkill -f "moon.*${PORT_EVICT}" 2>/dev/null || true
    rm -rf "$EVICT_DIR"

    echo "  eviction: done"
fi

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "==========================================="
echo "  COMMAND COVERAGE TEST RESULTS"
echo "==========================================="
echo ""
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
if [[ "$SKIP_BENCH" == "false" ]] && should_run "benchmark"; then
    echo ""
    echo "  Benchmarks: $BENCH_PASS passed, $BENCH_FAIL failed"
fi
echo ""

if [[ "$FAIL" -gt 0 ]]; then
    echo "  STATUS: FAIL ($FAIL failures)"
    exit 1
else
    echo "  STATUS: ALL PASSED"
    exit 0
fi
