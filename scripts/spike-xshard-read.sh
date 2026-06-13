#!/usr/bin/env bash
# spike-xshard-read.sh — focused cross-shard read latency spike (NOT the full matrix).
# Measures the cells that matter for xshard-read-fastpath ⚠1 (spin viability):
#   s4 c1  P1 GET  — the −85% regression cell (≈ mean cross-shard read latency; c1 ⇒ RPS=1/latency)
#   s4 c100 P1 GET — neighbour-throughput proxy (does a per-conn spin starve co-located conns?)
#   s4 c1  P1 SET  — write control (cross-shard writes already route; should be ~flat)
# Fresh server per rep ×REPS (the shardslice §6 phantom-regression lesson). VM-local --dir on /tmp.
#
# Usage (inside moon-dev): bash scripts/spike-xshard-read.sh <binary> <label> [reps] [requests]
set -euo pipefail

BINARY="${1:?need binary path}"
LABEL="${2:?need label}"
REPS="${3:-3}"
REQUESTS="${4:-200000}"
SHARDS=4
PORT=7411
KEYSPACE=1000000

[[ -x "$BINARY" ]] || { echo "ERROR: binary not executable: $BINARY" >&2; exit 1; }

MOON_PID=""
cleanup() { [[ -n "$MOON_PID" ]] && { kill "$MOON_PID" 2>/dev/null||true; wait "$MOON_PID" 2>/dev/null||true; MOON_PID=""; }; return 0; }
trap cleanup EXIT INT TERM

start_moon() {
  cleanup
  local dir; dir="$(mktemp -d /tmp/moon-spike.XXXXXX)"   # fresh VM-local dir — dodges diskfull + CWD-reload trap
  "$BINARY" --port "$PORT" --shards "$SHARDS" --dir "$dir" \
    --appendonly no --disk-offload disable --admin-port 0 >/dev/null 2>&1 &
  MOON_PID=$!
  local deadline=$((SECONDS+8))
  until redis-cli -p "$PORT" ping >/dev/null 2>&1; do
    [[ $SECONDS -ge $deadline ]] && { echo "ERROR: moon did not start" >&2; return 1; }
    sleep 0.1
  done
}

# RPS for one redis-benchmark run (8.x \r-safe).
bench_rps() {
  local c="$1" p="$2" cmd="$3"
  redis-benchmark -p "$PORT" -c "$c" -P "$p" -n "$REQUESTS" -r "$KEYSPACE" -t "$cmd" --csv 2>/dev/null \
    | tr '\r' '\n' | grep "\"${cmd}\"" | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1
}

# median of N reps (fresh server each)
cell() {
  local c="$1" p="$2" cmd="$3" name="$4"
  local vals=()
  for ((i=1;i<=REPS;i++)); do
    start_moon
    vals+=("$(bench_rps "$c" "$p" "$cmd" || echo 0)")
    cleanup
  done
  local sorted; sorted=$(printf '%s\n' "${vals[@]}" | sort -n)
  local med; med=$(echo "$sorted" | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}')
  printf '%-22s c=%-3d P=%-2d %-3s  reps=[%s]  median=%s RPS\n' "$name" "$c" "$p" "$cmd" "$(IFS=,;echo "${vals[*]}")" "$med"
}

echo "===== SPIKE [$LABEL]  binary=$BINARY  shards=$SHARDS reqs=$REQUESTS reps=$REPS ====="
echo "host: $(uname -srm)  ver: $("$BINARY" --version 2>/dev/null | head -1)"
cell 1   1 GET "s4-c1-GET (regression cell)"
cell 100 1 GET "s4-c100-GET (neighbours)"
cell 1   1 SET "s4-c1-SET (write control)"
echo "===== END [$LABEL] ====="
