#!/usr/bin/env bash
# moon#1297: bytes held in cold spill files over time under a no-AOF write
# flood over maxmemory with DEL churn, for one or more moon binaries (A/B).
#
#   scripts/bench-cold-disk-held.sh [--shards N] [--secs S] [--port P] BIN [BIN...]
#
# Per binary: a fresh server (--appendonly no, --save that never fires,
# 8 MB allkeys-lru, disk offload, orphan sweep every 5 s), then for S seconds
# two redis-benchmark clients at once — SET -r 50000 -d 600 (-c 16 -P 16) and
# DEL key:__rand_int__ -r 50000 (-c 4 -P 16) — then S more seconds idle.
# Every 2 s it prints `binary,t_secs,phase,heap_bytes,heap_files` (CSV on
# stdout): the sum and count of heap-*.mpf under the offload dir.
#
# redis-benchmark's payload is one repeated byte, which the spill path
# compresses: absolute bytes understate a real workload, the A/B ratio is
# what this measures. tests/cold_block_reclaim_no_aof_1297.rs
# (disk_held_*) measures incompressible values.
#
# Needs redis-benchmark and redis-cli on PATH. Linux numbers only.
set -euo pipefail

SHARDS=4
SECS=60
PORT=7650
while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards) SHARDS="$2"; shift 2 ;;
        --secs) SECS="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) break ;;
    esac
done
[[ $# -ge 1 ]] || { echo "usage: $0 [--shards N] [--secs S] [--port P] BIN [BIN...]" >&2; exit 2; }
if redis-cli -p "$PORT" ping >/dev/null 2>&1; then
    echo "port $PORT is in use" >&2
    exit 2
fi

heap_stats() {
    find "$1/off" -name 'heap-*.mpf' -printf '%s\n' 2>/dev/null |
        awk '{b += $1; n += 1} END {printf "%d,%d", b, n}'
}

echo "binary,t_secs,phase,heap_bytes,heap_files"
for bin in "$@"; do
    dir=$(mktemp -d "${TMPDIR:-/tmp}/moon-disk-held.XXXXXX")
    mkdir -p "$dir/off"
    MOON_DISK_FREE_MIN_PCT=0 "$bin" --port "$PORT" --shards "$SHARDS" \
        --maxmemory 8388608 --maxmemory-policy allkeys-lru \
        --disk-offload enable --disk-offload-dir "$dir/off" \
        --appendonly no --save "3600 100000000" --dir "$dir" \
        --cold-orphan-sweep-interval-secs 5 --disk-free-min-pct 0 \
        >"$dir/moon.log" 2>&1 &
    pid=$!
    for _ in $(seq 1 80); do
        redis-cli -p "$PORT" ping >/dev/null 2>&1 && break
        sleep 0.1
    done
    n=$((SECS * 400000))
    redis-benchmark -p "$PORT" -t set -r 50000 -d 600 -n "$n" -c 16 -P 16 -q >/dev/null 2>&1 &
    set_pid=$!
    redis-benchmark -p "$PORT" -r 50000 -n "$n" -c 4 -P 16 -q DEL 'key:__rand_int__' >/dev/null 2>&1 &
    del_pid=$!
    name=$(basename "$bin")
    for t in $(seq 0 2 $((2 * SECS))); do
        phase=flood
        if [[ $t -ge $SECS ]]; then
            phase=idle
            kill "$set_pid" "$del_pid" 2>/dev/null || true
        fi
        echo "$name,$t,$phase,$(heap_stats "$dir")"
        sleep 2
    done
    wait "$set_pid" "$del_pid" 2>/dev/null || true
    redis-cli -p "$PORT" info | grep -E '^cold_reclaim_|^cold_keys|^cold_grave_slots' |
        tr -d '\r' | sed "s/^/# $name /" >&2 || true
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    rm -rf "$dir"
done
