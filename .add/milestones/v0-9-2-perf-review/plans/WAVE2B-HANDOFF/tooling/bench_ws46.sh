#!/usr/bin/env bash
# WS46 (moon#1266 1A) A/B: interleaved arms, fresh server per cell.
# bench2b copy: arm order rotates by one each rep; data dirs under scratchpad/bench2b/ws46data.
# Arm spec: <binary>[@1]  — "@1" sets MOON_AOF_SHARD_WRITE=1 (1A on).
# Usage: bench_ws46.sh <reps> <shards> "<cells P:c:n ...>" <out.csv> [arms...]
set -u
REPS=${1:-3}; SHARDS=${2:-1}; CELLS=${3:-"16:50:2000000 1:50:500000 1:1:100000"}; OUT=${4:-/dev/stdout}
shift 4 || true
ARMS=("$@")
[ ${#ARMS[@]} -eq 0 ] && ARMS=(/home/user/wt/bin/ws42-final-monoio /home/user/wt/bin/ws46-v1-monoio /home/user/wt/bin/ws46-v1-monoio@1)
PORT=${PORT:-7624}
TCK=$(getconf CLK_TCK)
[ -s "$OUT" ] || echo "rep,arm,shards,P,c,n,rps,p50_ms,p99_ms,server_core_us_per_op,vol_cs_per_kop,lane_writes_per_kop,load1,shard_us_per_op,aofw_us_per_op" > "$OUT"
# Summed utime+stime ticks of the threads whose comm starts with $2.
tticks() { local t s=0; for t in /proc/$1/task/*; do grep -q "^$2" "$t/comm" 2>/dev/null && s=$((s + $(awk '{print $14+$15}' "$t/stat"))); done; echo $s; }
for rep in $(seq 1 "$REPS"); do
 for cell in $CELLS; do
  IFS=: read -r P C N <<< "$cell"
  n=${#ARMS[@]}; ord=(); for i in $(seq 0 $((n-1))); do ord+=("${ARMS[$(( (i+rep-1) % n ))]}"); done
  for spec in "${ord[@]}"; do
   bin=${spec%@*}; on=""; [ "$spec" != "$bin" ] && on=${spec#*@}
   d=$(mktemp -d ${SCRATCH:-/tmp/moon-work}/bench2b/ws46data/ws46.XXXX)
   env ${EXTRA_ENV:-} MOON_AOF_SHARD_WRITE=$on MOON_DISK_FREE_MIN_PCT=0 "$bin" --port $PORT --shards "$SHARDS" --appendonly yes --appendfsync everysec \
     --auto-aof-rewrite-percentage 0 --disk-free-min-pct 0 --save "" --maxmemory 0 --dir "$d" >/dev/null 2>&1 &
   pid=$!
   for _ in $(seq 1 200); do redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.05; done
   t0=$(awk '{print $14+$15}' /proc/$pid/stat); sh0=$(tticks $pid shard-); aw0=$(tticks $pid aof-)
   cs0=$(awk '/^voluntary_ctxt_switches/{v+=$2} END{print v}' /proc/$pid/task/*/status)
   line=$(redis-benchmark -p $PORT -t set -r 1000000 -d 16 -P "$P" -c "$C" -n "$N" --csv 2>/dev/null | tr '\r' '\n' | grep '^"SET"' | tail -1)
   t1=$(awk '{print $14+$15}' /proc/$pid/stat); sh1=$(tticks $pid shard-); aw1=$(tticks $pid aof-)
   cs1=$(awk '/^voluntary_ctxt_switches/{v+=$2} END{print v}' /proc/$pid/task/*/status)
   lw=$(redis-cli -p $PORT info persistence 2>/dev/null | tr -d '\r' | awk -F: '/^aof_shard_writes:/{print $2}')
   kill -9 $pid; wait $pid 2>/dev/null; rm -rf "$d"
   rps=$(echo "$line" | cut -d, -f2 | tr -d '"'); p50=$(echo "$line" | cut -d, -f5 | tr -d '"'); p99=$(echo "$line" | cut -d, -f7 | tr -d '"')
   cpu=$(awk -v a="$t0" -v b="$t1" -v n="$N" -v t="$TCK" 'BEGIN{printf "%.2f",(b-a)/t*1e6/n}')
   cs=$(awk -v a="$cs0" -v b="$cs1" -v n="$N" 'BEGIN{printf "%.1f",(b-a)*1000/n}')
   lwk=$(awk -v w="${lw:-0}" -v n="$N" 'BEGIN{printf "%.1f",w*1000/n}')
   shu=$(awk -v a="$sh0" -v b="$sh1" -v n="$N" -v t="$TCK" 'BEGIN{printf "%.2f",(b-a)/t*1e6/n}')
   awu=$(awk -v a="$aw0" -v b="$aw1" -v n="$N" -v t="$TCK" 'BEGIN{printf "%.2f",(b-a)/t*1e6/n}')
   echo "$rep,$(basename "$bin")${on:+@$on},$SHARDS,$P,$C,$N,$rps,$p50,$p99,$cpu,$cs,$lwk,$(cut -d' ' -f1 /proc/loadavg),$shu,$awu" >> "$OUT"
  done
 done
done
