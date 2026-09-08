#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-ab-matrix.sh -- interleaved Moon-vs-Redis throughput matrix
#
# The method BENCHMARK.md §2.11 adopted after a single-pass matrix produced
# three apparent double-digit "regressions" that were drift: Redis -- unchanged
# code, benchmarked in both legs -- had itself moved 27%. When the control moves
# further than the subject, no per-row number means anything.
#
# So: legs alternate order every rep, BOTH engines are restarted and
# re-measured every rep, and a ratio is only reported as signal when it clears
# the noise floor (the worst within-leg CV of the two series being compared).
#
# Emits one CSV row per measurement to stdout; provenance to the header.
# Analyse with scripts/bench-ab-report.py.
#
# Usage:
#   ./scripts/bench-ab-matrix.sh --moon-bin ./target/release/moon --reps 5
#
# Every family is an EXPLICIT keyed command over __rand_int__. Do not switch
# these back to `redis-benchmark -t lpush|sadd|hset|zadd`: those drive ONE
# literal key (`mylist`/`myset`/...) and `-r` randomises the element, not the
# key, which is how a shard-scaling matrix once asked eight of twelve families
# to demonstrate an impossibility and got the tautological answer (§2.14).
###############################################################################

MOON_BIN="./target/release/moon"
REDIS_BIN="redis-server"
REPS=5
CLIENTS=50
SHARDS=1
KEYSPACE=100000
PORT=7501
MIN_DBSIZE=50000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --moon-bin) MOON_BIN="$2"; shift 2 ;;
    --reps)     REPS="$2"; shift 2 ;;
    --clients)  CLIENTS="$2"; shift 2 ;;
    --shards)   SHARDS="$2"; shift 2 ;;
    --port)     PORT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

# Request count scales with depth so a p=64 point measures a second of steady
# work rather than process startup (§2.12).
requests_for() {
  case "$1" in
    1)  echo 100000 ;;
    8)  echo 400000 ;;
    64) echo 1500000 ;;
  esac
}

# family|command template. Order matters: SADD runs before SPOP so SPOP pops
# from populated sets on both engines identically.
FAMILIES=(
  "SET|set key:__rand_int__ xxxxxxxx"
  "GET|get key:__rand_int__"
  "INCR|incr ctr:__rand_int__"
  "LPUSH|lpush list:__rand_int__ xxxxxxxx"
  "SADD|sadd set:__rand_int__ __rand_int__"
  "SPOP|spop set:__rand_int__"
  "HSET|hset hash:__rand_int__ f __rand_int__"
  "ZADD|zadd z:__rand_int__ 1 m:__rand_int__"
)
DEPTHS=(1 8 64)

SERVER_PID=""
SERVER_DIR=""

stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    # Bounded: moon under REUSEPORT has hung on SIGTERM before, and a benchmark
    # that waits forever on teardown looks exactly like a benchmark that hangs.
    for _ in $(seq 1 50); do kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
  [[ -n "$SERVER_DIR" ]] && rm -rf "$SERVER_DIR"
  SERVER_DIR=""
}
trap stop_server EXIT

start_server() { # start_server <moon|redis>
  SERVER_DIR="$(mktemp -d)"
  if [[ "$1" == "moon" ]]; then
    MOON_DISK_FREE_MIN_PCT=0 "$MOON_BIN" --port "$PORT" --shards "$SHARDS" \
      --dir "$SERVER_DIR" --protected-mode no \
      --appendonly no --disk-offload disable >"$SERVER_DIR/log" 2>&1 &
  else
    "$REDIS_BIN" --port "$PORT" --dir "$SERVER_DIR" \
      --save '' --appendonly no >"$SERVER_DIR/log" 2>&1 &
  fi
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]]; then return 0; fi
    sleep 0.1
  done
  echo "FATAL: $1 never answered PING on $PORT" >&2
  cat "$SERVER_DIR/log" >&2 || true
  exit 1
}

# Position-based parse. `awk '{print $2}'` yields "summary:" on redis-benchmark
# 8.x, and a first-numeric-field scan picks up the literal `1` in the ZADD
# template -- so anchor on the words "requests per second" and take the field
# before them.
parse_rps() {
  tr '\r' '\n' | awk '{
    for (i = 1; i <= NF; i++)
      if ($i == "requests" && $(i+1) == "per") { v = $(i-1); gsub(/,/, "", v); print v; exit }
  }'
}

bench_one() { # bench_one <depth> <command...>
  local depth="$1"; shift
  local n; n="$(requests_for "$depth")"
  redis-benchmark -p "$PORT" -n "$n" -c "$CLIENTS" -P "$depth" \
    -r "$KEYSPACE" -q "$@" 2>/dev/null | parse_rps
}

# ── provenance (§2.12: binary sha, versions, CPU into the header) ────────────
moon_ver="$("$MOON_BIN" --version 2>/dev/null | head -1 || echo unknown)"
moon_sha="$(sha256sum "$MOON_BIN" 2>/dev/null | cut -c1-16 || echo unknown)"
redis_ver="$($REDIS_BIN --version 2>/dev/null | head -1 || echo unknown)"
cpu_model="$(awk -F': ' '/model name|Model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || echo unknown)"
[[ -z "$cpu_model" ]] && cpu_model="$(lscpu 2>/dev/null | awk -F': +' '/Model name/{print $2; exit}')"

cat <<EOF
# moon: $moon_ver sha256:$moon_sha
# redis: $redis_ver
# cpu: ${cpu_model:-unknown} ($(nproc) cores)
# kernel: $(uname -sr)
# shards: $SHARDS clients: $CLIENTS keyspace: $KEYSPACE reps: $REPS
# date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')
rep,order,engine,family,depth,rps
EOF

run_leg() { # run_leg <rep> <order> <engine>
  local rep="$1" order="$2" engine="$3"
  start_server "$engine"

  # Seed so the keyspace is real before the first timed point, and prove it.
  # A leg that silently benchmarks 15 keys is the §2.14 failure.
  redis-benchmark -p "$PORT" -n 120000 -c "$CLIENTS" -P 16 -r "$KEYSPACE" -q \
    set key:__rand_int__ xxxxxxxx >/dev/null 2>&1
  redis-benchmark -p "$PORT" -n 120000 -c "$CLIENTS" -P 16 -r "$KEYSPACE" -q \
    sadd set:__rand_int__ __rand_int__ >/dev/null 2>&1
  local dbsize
  dbsize="$(timeout 5 redis-cli -p "$PORT" dbsize 2>/dev/null || echo 0)"
  if (( dbsize < MIN_DBSIZE )); then
    echo "FATAL: $engine rep=$rep dbsize=$dbsize < $MIN_DBSIZE -> LEG_ABORT" >&2
    exit 1
  fi

  local depth entry family cmd rps
  for depth in "${DEPTHS[@]}"; do
    for entry in "${FAMILIES[@]}"; do
      family="${entry%%|*}"; cmd="${entry#*|}"
      # shellcheck disable=SC2086
      rps="$(bench_one "$depth" $cmd)"
      [[ -z "$rps" ]] && rps="NA"
      echo "$rep,$order,$engine,$family,$depth,$rps"
    done
  done
  stop_server
}

for rep in $(seq 1 "$REPS"); do
  if (( rep % 2 == 1 )); then order="moon-first"; first=moon; second=redis
  else                        order="redis-first"; first=redis; second=moon; fi
  log "rep $rep/$REPS ($order)"
  run_leg "$rep" "$order" "$first"
  run_leg "$rep" "$order" "$second"
done

log "done"
