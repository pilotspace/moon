#!/usr/bin/env bash
# probe-xshard.sh — quick ungated discriminator: does the VM collapse single-client RPS
# even for a LOCAL (single-shard) read? Separates cross-shard contamination from total
# host starvation. Not a baseline; a diagnostic. Args: BINS dir (default ~/moon-baseline-bins).
set -uo pipefail
BINS="${1:-$HOME/moon-baseline-bins}"
PORT=7413

probe() {  # <bin> <shards> <label>
  local bin="$1" shards="$2" label="$3" dir pid rps d
  dir=$(mktemp -d /tmp/probe.XXXXXX)
  taskset -c 0-3 "$bin" --port "$PORT" --shards "$shards" --dir "$dir" --appendonly no --admin-port 0 >/dev/null 2>&1 &
  pid=$!
  d=$((SECONDS+10))
  until redis-cli -p "$PORT" ping >/dev/null 2>&1; do
    [[ $SECONDS -ge $d ]] && { printf '%-30s shards=%s  NOSTART\n' "$label" "$shards"; kill -9 $pid 2>/dev/null; return; }
    sleep 0.1
  done
  rps=$(taskset -c 4,5 redis-benchmark -p "$PORT" -c 1 -P 1 -n 100000 -r 1000000 -t GET --csv 2>/dev/null \
        | tr '\r' '\n' | grep '"GET"' | awk -F',' '{gsub(/"/,"",$2); printf "%.0f",$2}' | tail -1)
  printf '%-30s shards=%s  c1-GET = %8s RPS\n' "$label" "$shards" "$rps"
  kill -9 $pid 2>/dev/null; wait $pid 2>/dev/null; rm -rf "$dir"; sleep 1
}

echo "load before: $(cat /proc/loadavg)"
for r in 1 2 3; do
  probe "$BINS/moon-3e376a1-monoio" 1 "3e376a1 (v0.3.0 fast) LOCAL"
  probe "$BINS/moon-3e376a1-monoio" 4 "3e376a1 (v0.3.0 fast) XSHARD"
  probe "$BINS/moon-a497602-monoio" 1 "a497602 (HEAD slow)  LOCAL"
  probe "$BINS/moon-a497602-monoio" 4 "a497602 (HEAD slow)  XSHARD"
  echo "  --- round $r done, load: $(cat /proc/loadavg) ---"
done
