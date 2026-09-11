#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-zset-read-encoding.sh -- does a READ destroy a small zset's compact
# encoding?
#
# moon#928 / PR #932: fourteen sorted-set READ handlers used to flatten a
# listpack zset to a skiplist just by looking at it. A throughput matrix cannot
# see this -- it only writes -- so the ZADD "+38%" measured on 2026-09-10 was a
# write-only figure that one intervening GET-shaped command could erase.
#
# This measures the thing directly, twice over, because either alone is
# arguable:
#
#   OBJECT ENCODING -- the engine's own claim about the key. Direct, but it is
#                      a self-report, and self-reports have been wrong here.
#   RSS             -- the kernel's number. A listpack -> skiplist promotion
#                      cannot happen for free; if the encoding really changed,
#                      /proc has to show it.
#
# A run where the two disagree is reported as a disagreement, not resolved in
# favour of whichever is convenient.
#
# WHICH DISPATCH PATH THE READ TAKES IS THE WHOLE EXPERIMENT. The defect was
# never reachable from a plain connection: a bare ZSCORE goes through
# `command::dispatch_read`, which hands out an immutable view and cannot
# upgrade anything. It is the MUTABLE path -- MULTI/EXEC, Lua, inline dispatch
# -- that took `&mut` and flattened. So `--read-mode plain` on a buggy binary
# is a clean run that proves nothing, which is why all three modes are swept
# and the plain leg is kept as the negative control.
###############################################################################

MOON_BIN="./target/release/moon"
REDIS_BIN="redis-server"
PORT=7505
KEYS=200000
SHARDS=1
MEMBERS=4
ENGINES=(moon redis)
READ_MODES=(plain multi eval)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --moon-bin) MOON_BIN="$2"; shift 2 ;;
    --keys)     KEYS="$2"; shift 2 ;;
    --shards)   SHARDS="$2"; shift 2 ;;
    --engines)  IFS=',' read -r -a ENGINES <<< "$2"; shift 2 ;;
    --read-modes) IFS=',' read -r -a READ_MODES <<< "$2"; shift 2 ;;
    --port)     PORT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

for _tool in timeout redis-cli; do
  command -v "$_tool" >/dev/null 2>&1 || { echo "FATAL: '$_tool' not found" >&2; exit 1; }
done

SERVER_PID=""; SERVER_DIR=""
stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
  [[ -n "$SERVER_DIR" ]] && rm -rf "$SERVER_DIR"; SERVER_DIR=""
}
trap stop_server EXIT

start_server() {
  if [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]]; then
    echo "FATAL: port $PORT occupied -- a foreign listener would be measured as ours." >&2
    exit 1
  fi
  SERVER_DIR="$(mktemp -d)"
  if [[ "$1" == "moon" ]]; then
    MOON_DISK_FREE_MIN_PCT=0 "$MOON_BIN" --port "$PORT" --shards "$SHARDS" \
      --dir "$SERVER_DIR" --protected-mode no \
      --appendonly no --disk-offload disable >"$SERVER_DIR/log" 2>&1 &
  else
    "$REDIS_BIN" --port "$PORT" --dir "$SERVER_DIR" --save '' --appendonly no \
      >"$SERVER_DIR/log" 2>&1 &
  fi
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "FATAL: $1 exited at startup" >&2; cat "$SERVER_DIR/log" >&2; exit 1; }
    [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]] && return 0
    sleep 0.1
  done
  echo "FATAL: $1 never answered PING" >&2; cat "$SERVER_DIR/log" >&2; exit 1
}

rss_kb() { awk '/VmRSS/ {print $2}' "/proc/$SERVER_PID/status"; }

gen() { # gen <seed|plain|multi|eval>
  awk -v n="$KEYS" -v e="$MEMBERS" -v mode="$1" '
    function pad(s, w,  r) { r = s; while (length(r) < w) r = r "a"; return substr(r, 1, w) }
    function emit(c) { printf "*%d\r\n", c }
    function arg(s) { printf "$%d\r\n%s\r\n", length(s), s }
    BEGIN {
      lua = "return redis.call(\047ZSCORE\047, KEYS[1], ARGV[1])"
      for (i = 0; i < n; i++) {
        key = sprintf("z:%012d", i)
        if (mode == "seed") {
          emit(2 + 2*e); arg("ZADD"); arg(key)
          for (j = 0; j < e; j++) { arg(sprintf("%d", j+1)); arg(pad(sprintf("m%d", j), 8)) }
        } else if (mode == "plain") {
          # Negative control: the read-only dispatch path. Must not flatten on
          # ANY binary, buggy or fixed.
          emit(3); arg("ZSCORE"); arg(key); arg(pad("m0", 8))
        } else if (mode == "multi") {
          # The mutable path. This is the one that flattened.
          emit(1); arg("MULTI")
          emit(3); arg("ZSCORE"); arg(key); arg(pad("m0", 8))
          emit(1); arg("EXEC")
        } else if (mode == "eval") {
          emit(5); arg("EVAL"); arg(lua); arg("1"); arg(key); arg(pad("m0", 8))
        }
      }
    }'
}

encoding_of() { timeout 5 redis-cli -p "$PORT" object encoding "z:000000000000" 2>/dev/null | tr -d '\r'; }

# ── provenance (§2.12: binary sha, versions, CPU into the header) ────────────
emit_provenance() { # emit_provenance <extra-params-line>
  local moon_sha redis_ver cpu_model
  moon_sha="$(sha256sum "$MOON_BIN" 2>/dev/null | cut -c1-16 || echo unknown)"
  redis_ver="$($REDIS_BIN --version 2>/dev/null | head -1 || echo unknown)"
  cpu_model="$(awk -F': ' '/model name|Model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null)"
  [[ -z "$cpu_model" ]] && cpu_model="$(lscpu 2>/dev/null | awk -F': +' '/Model name/{print $2; exit}')"
  cat <<PROV
# moon: sha256:$moon_sha ($MOON_BIN)
# redis: $redis_ver
# cpu: ${cpu_model:-unknown} ($(nproc) cores)
# kernel: $(uname -sr)
# $1
# date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')
PROV
}

emit_provenance "shards: $SHARDS keys: $KEYS members: $MEMBERS read_modes: ${READ_MODES[*]}"
echo "engine,read_mode,phase,encoding,rss_kb,dbsize,per_key_bytes"

for engine in "${ENGINES[@]}"; do
  for mode in "${READ_MODES[@]}"; do
    # Fresh server per (engine, mode): the promotion is irreversible, so a
    # second mode run against the same keyspace would inherit the first mode's
    # damage and every later mode would look guilty.
    start_server "$engine"
    sleep 1
    idle="$(rss_kb)"

    gen seed | redis-cli -p "$PORT" --pipe >/dev/null 2>&1
    sleep 1
    before_rss="$(rss_kb)"
    before_enc="$(encoding_of)"
    dbsize="$(timeout 10 redis-cli -p "$PORT" dbsize 2>/dev/null || echo 0)"
    if (( dbsize < KEYS / 2 )); then
      echo "FATAL: $engine dbsize=$dbsize -- keyspace never materialised" >&2; exit 1
    fi
    echo "$engine,$mode,before_read,${before_enc:-unsupported},$before_rss,$dbsize,$(awk "BEGIN{printf \"%.1f\", ($before_rss-$idle)*1024/$dbsize}")"

    gen "$mode" | redis-cli -p "$PORT" --pipe >/dev/null 2>&1
    sleep 1
    after_rss="$(rss_kb)"
    after_enc="$(encoding_of)"
    echo "$engine,$mode,after_read,${after_enc:-unsupported},$after_rss,$dbsize,$(awk "BEGIN{printf \"%.1f\", ($after_rss-$idle)*1024/$dbsize}")"

    delta="$(awk "BEGIN{printf \"%.1f\", ($after_rss-$before_rss)*100.0/($before_rss-$idle)}")"
    echo "# $engine/$mode: encoding $before_enc -> $after_enc, RSS-over-idle ${delta}% after a pure read" >&2
    stop_server
  done
done
