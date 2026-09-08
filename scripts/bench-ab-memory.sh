#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-ab-memory.sh -- per-key RSS and idle RSS, moon vs Redis, fresh server
# per data point.
#
# Two traps this script exists to not fall into, both of which produced
# published-and-later-retracted numbers (BENCHMARK.md §2.14):
#
#  1. `redis-benchmark -t set` defaults to a THREE-byte value. A per-key figure
#     measured that way is measuring `CompactValue` inlining (values <= 12 B
#     live inside the entry), which is a band, not a trend. `-d` is explicit
#     here and swept.
#  2. Without `-r`, every write lands on `__rand_key__` -- one real key -- so
#     "per-key" divides by a DBSIZE that has nothing to do with the load.
#
# Every row is checked against the arithmetic floor (key + value + 24). A leg
# that reports below its own floor is physically impossible and aborts, which
# is exactly what should have happened to the retired 0.90x row.
###############################################################################

MOON_BIN="./target/release/moon"
REDIS_BIN="redis-server"
PORT=7502
KEYS=200000
CLIENTS=50
SHARDS=1
REPS=3
SIZES=(8 64 256 1024)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --moon-bin) MOON_BIN="$2"; shift 2 ;;
    --shards)   SHARDS="$2"; shift 2 ;;
    --reps)     REPS="$2"; shift 2 ;;
    --sizes)    IFS=',' read -r -a SIZES <<< "$2"; shift 2 ;;
    --port)     PORT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

# Fail closed on a missing tool. The port and readiness checks below are built
# on `timeout`, which is absent on stock macOS -- and a missing `timeout` makes
# `$(timeout 2 redis-cli ... )` expand to empty, which is not "PONG", so the
# occupied-port guard would silently pass instead of refusing. A guard that
# cannot fire is worse than no guard, so require the tools up front.
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


# A foreign process already holding $PORT answers PING while our server fails
# to bind, and the harness then attributes a stranger's numbers to both
# engines. This has happened in this repo: a stray redis-server PONGed on a
# probe port after moon had aborted. So: refuse to start on an occupied port,
# and treat a dead SERVER_PID during the wait as fatal rather than waiting for
# someone else to answer.
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

# VmRSS of the whole process tree (moon runs several threads but one process;
# redis likewise). Read from /proc so it is the kernel's number, not a
# self-report -- moon's own INFO memory has been wrong before.
rss_kb() { awk '/VmRSS/ {print $2}' "/proc/$SERVER_PID/status"; }

echo "engine,rep,order,value_size,idle_rss_kb,loaded_rss_kb,dbsize,per_key_bytes,floor_bytes"

# Alternate the engine order per (rep, size), not per rep. Running every moon
# leg and then every redis leg lets a host-state drift over the run land
# entirely on one engine. Fresh-server-per-point makes RSS far less drift-prone
# than throughput, but the ordering costs nothing and the alternative is
# unfalsifiable, so it is recorded in the CSV as `order`.
for rep in $(seq 1 "$REPS"); do
  for size in "${SIZES[@]}"; do
    if (( rep % 2 == 1 )); then order="moon-first"; engines=(moon redis)
    else                        order="redis-first"; engines=(redis moon); fi
    for engine in "${engines[@]}"; do
      start_server "$engine"
      sleep 1
      idle="$(rss_kb)"
      redis-benchmark -p "$PORT" -n "$((KEYS * 2))" -c "$CLIENTS" -P 16 \
        -r "$KEYS" -d "$size" -t set -q >/dev/null 2>&1
      sleep 1
      loaded="$(rss_kb)"
      dbsize="$(timeout 5 redis-cli -p "$PORT" dbsize 2>/dev/null || echo 0)"
      if (( dbsize < KEYS / 2 )); then
        echo "FATAL: $engine dbsize=$dbsize -- keyspace never materialised" >&2; exit 1
      fi
      per_key="$(awk "BEGIN{printf \"%.1f\", ($loaded - $idle) * 1024 / $dbsize}")"
      # redis-benchmark keys are `key:000000000000` = 16 bytes.
      floor="$((16 + size + 24))"
      below="$(awk "BEGIN{print ($per_key < $floor) ? 1 : 0}")"
      if [[ "$below" == "1" ]]; then
        echo "FATAL: $engine d=$size per_key=$per_key < floor=$floor -- impossible, harness is wrong" >&2
        exit 1
      fi
      echo "$engine,$rep,$order,$size,$idle,$loaded,$dbsize,$per_key,$floor"
      stop_server
    done
  done
  log "rep $rep/$REPS done"
done
