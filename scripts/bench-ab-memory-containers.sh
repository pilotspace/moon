#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-ab-memory-containers.sh -- per-key RSS for hash/list/set/zset,
# moon vs Redis, fresh server per data point.
#
# Why this exists: bench-ab-memory.sh measures STRING values only, so the
# container memory advantage that the whole compact-encoding campaign
# (moon#897, #830 -> PRs #920/#921/#922/#932) exists to deliver was measured
# nowhere. A string row cannot see a listpack.
#
# Two figures per container type, because only the pair is honest:
#
#   untouched -- the container as first written. This is the best case: the
#                compact form has never been asked to survive a mutation.
#   touched   -- after ONE secondary write per key, using the very mutator
#                each PR taught to mutate in place (HINCRBY, LSET, SREM,
#                ZINCRBY). Before those PRs this write flattened the
#                container; a workload that writes twice sees THIS number.
#
# Publishing only `untouched` would be the best case that no real workload
# sees. Both are emitted, per key, with the element count each was measured
# at, so the pair recomputes without a re-run.
#
# Inherits the discipline of bench-ab-memory.sh: fail-closed tool check,
# occupied-port refusal, our-PID liveness, a DBSIZE guard, and an arithmetic
# floor per row. A row under its own floor is physically impossible and aborts
# rather than being published.
###############################################################################

MOON_BIN="./target/release/moon"
REDIS_BIN="redis-server"
PORT=7503
KEYS=200000
SHARDS=1
REPS=3
ELEMS=4
ELEMSZ=8
TYPES=(hash list set zset)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --moon-bin) MOON_BIN="$2"; shift 2 ;;
    --shards)   SHARDS="$2"; shift 2 ;;
    --reps)     REPS="$2"; shift 2 ;;
    --keys)     KEYS="$2"; shift 2 ;;
    --types)    IFS=',' read -r -a TYPES <<< "$2"; shift 2 ;;
    --port)     PORT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

for _tool in timeout redis-cli redis-benchmark; do
  command -v "$_tool" >/dev/null 2>&1 || {
    echo "FATAL: '$_tool' not found. These harnesses are Linux-only;" >&2
    echo "       on macOS, 'timeout' comes from coreutils (brew install coreutils)." >&2
    exit 1
  }
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

preflight_port() {
  if [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]]; then
    echo "FATAL: something already answers PING on port $PORT -- refusing to start." >&2
    echo "       A foreign listener would be measured as if it were ours." >&2
    exit 1
  fi
}

start_server() {
  preflight_port
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
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "FATAL: $1 exited during startup (port $PORT)" >&2
      cat "$SERVER_DIR/log" >&2 || true; exit 1
    fi
    [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]] && return 0
    sleep 0.1
  done
  echo "FATAL: $1 never answered PING" >&2; cat "$SERVER_DIR/log" >&2; exit 1
}

# The kernel's number, not a self-report -- moon's own INFO memory has been
# wrong before (BENCHMARK.md §2.14).
rss_kb() { awk '/VmRSS/ {print $2}' "/proc/$SERVER_PID/status"; }

# Deterministic load: exactly $KEYS keys of exactly $ELEMS elements. A
# `redis-benchmark -r` load cannot promise that -- it randomises, so coverage
# is uneven and a "per-key" divisor becomes a guess. Emitted as real RESP
# arrays rather than inline commands (inline needs exact \r\n framing and
# silently truncates on a long argv).
resp() { # resp <arg>...
  printf '*%d\r\n' "$#"
  local a; for a in "$@"; do printf '$%d\r\n%s\r\n' "${#a}" "$a"; done
}

gen_seed() { # gen_seed <type> -> RESP on stdout
  local t="$1"
  awk -v n="$KEYS" -v t="$t" -v e="$ELEMS" -v sz="$ELEMSZ" '
    function pad(s, w,  r) { r = s; while (length(r) < w) r = r "a"; return substr(r, 1, w) }
    function emit(argc,   i) { printf "*%d\r\n", argc }
    function arg(s) { printf "$%d\r\n%s\r\n", length(s), s }
    BEGIN {
      for (i = 0; i < n; i++) {
        key = sprintf("%s:%012d", t, i)
        if (t == "hash") {
          emit(2 + 2*e); arg("HSET"); arg(key)
          for (j = 0; j < e; j++) {
            arg(sprintf("f%d", j))
            # f(e-1) is numeric so HINCRBY can mutate it in place later.
            if (j == e-1) arg("0"); else arg(pad(sprintf("v%d", j), sz))
          }
        } else if (t == "list") {
          emit(2 + e); arg("RPUSH"); arg(key)
          for (j = 0; j < e; j++) arg(pad(sprintf("v%d", j), sz))
        } else if (t == "set") {
          emit(2 + e); arg("SADD"); arg(key)
          for (j = 0; j < e; j++) arg(pad(sprintf("m%d", j), sz))
        } else if (t == "zset") {
          emit(2 + 2*e); arg("ZADD"); arg(key)
          for (j = 0; j < e; j++) { arg(sprintf("%d", j+1)); arg(pad(sprintf("m%d", j), sz)) }
        }
      }
    }'
}

# ONE secondary write per key -- the mutator each PR taught to mutate in place.
# hash: HINCRBY (#920)   list: LSET (#921)
# set:  SREM    (#921)   zset: ZINCRBY (#922)
# SREM is the only one that changes cardinality (4 -> 3); it runs identically
# on both engines, and the resulting element count is recorded per row.
gen_touch() { # gen_touch <type> -> RESP on stdout
  local t="$1"
  awk -v n="$KEYS" -v t="$t" -v e="$ELEMS" -v sz="$ELEMSZ" '
    function pad(s, w,  r) { r = s; while (length(r) < w) r = r "a"; return substr(r, 1, w) }
    function emit(argc) { printf "*%d\r\n", argc }
    function arg(s) { printf "$%d\r\n%s\r\n", length(s), s }
    BEGIN {
      for (i = 0; i < n; i++) {
        key = sprintf("%s:%012d", t, i)
        if (t == "hash")      { emit(4); arg("HINCRBY"); arg(key); arg(sprintf("f%d", e-1)); arg("1") }
        else if (t == "list") { emit(4); arg("LSET");    arg(key); arg("0"); arg(pad("y", sz)) }
        else if (t == "set")  { emit(3); arg("SREM");    arg(key); arg(pad(sprintf("m%d", e-1), sz)) }
        else if (t == "zset") { emit(4); arg("ZINCRBY"); arg(key); arg("1"); arg(pad("m0", sz)) }
      }
    }'
}

elems_after_touch() { # the cardinality the `touched` figure is measured at
  case "$1" in set) echo "$((ELEMS - 1))" ;; *) echo "$ELEMS" ;; esac
}

pipe_load() { # pipe_load <generator> <type>
  local errs
  errs="$("$1" "$2" | redis-cli -p "$PORT" --pipe 2>&1 | tr -d '\r' | grep -c 'ERR' || true)"
  if [[ "$errs" != "0" ]]; then
    echo "FATAL: $1 $2 -- $errs error replies from the pipe load" >&2
    exit 1
  fi
}

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

emit_provenance "shards: $SHARDS keys: $KEYS elems: $ELEMS elemsz: $ELEMSZ reps: $REPS types: ${TYPES[*]}"
echo "engine,rep,order,type,phase,elems,idle_rss_kb,loaded_rss_kb,dbsize,per_key_bytes,floor_bytes"

for rep in $(seq 1 "$REPS"); do
  for t in "${TYPES[@]}"; do
    if (( rep % 2 == 1 )); then order="moon-first"; engines=(moon redis)
    else                        order="redis-first"; engines=(redis moon); fi
    for engine in "${engines[@]}"; do
      start_server "$engine"
      sleep 1
      idle="$(rss_kb)"

      pipe_load gen_seed "$t"
      sleep 1
      untouched="$(rss_kb)"
      dbsize="$(timeout 10 redis-cli -p "$PORT" dbsize 2>/dev/null || echo 0)"
      if (( dbsize < KEYS / 2 )); then
        echo "FATAL: $engine $t dbsize=$dbsize -- keyspace never materialised" >&2; exit 1
      fi

      pipe_load gen_touch "$t"
      sleep 1
      touched="$(rss_kb)"

      for phase in untouched touched; do
        if [[ "$phase" == "untouched" ]]; then loaded="$untouched"; ne="$ELEMS"
        else                                   loaded="$touched";   ne="$(elems_after_touch "$t")"; fi
        per_key="$(awk "BEGIN{printf \"%.1f\", ($loaded - $idle) * 1024 / $dbsize}")"
        # key is "<type>:<12 digits>"; elements are $ELEMSZ bytes each.
        floor="$(( ${#t} + 1 + 12 + ne * ELEMSZ + 24 ))"
        below="$(awk "BEGIN{print ($per_key < $floor) ? 1 : 0}")"
        if [[ "$below" == "1" ]]; then
          echo "FATAL: $engine $t/$phase per_key=$per_key < floor=$floor -- impossible, harness is wrong" >&2
          exit 1
        fi
        echo "$engine,$rep,$order,$t,$phase,$ne,$idle,$loaded,$dbsize,$per_key,$floor"
      done
      stop_server
    done
  done
  log "rep $rep/$REPS done"
done
