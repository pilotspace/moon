#!/bin/bash
# Copy of WS43 ab.sh (same server flags, same redis-benchmark command) + server CPU per op split by thread name.
# ws43hold.sh: + arm 'i2b-fe6fb20@hold' = 2b with MOON_TEST_COLD_RECLAIM_HOLD_FILE present (no new compactions).
# arms rotate per rep. usage: ws43hold.sh <reps> "<rts>" "<shards>"  -> ws43hold.csv ; arm order alternates per rep.
B=${SCRATCH:-/tmp/moon-work}/bench2b; BIN=/home/user/wt/bin
REPS=$1; RTS=$2; SHS=$3; P=7652; TCK=$(getconf CLK_TCK); N=${N:-100000}
[ -s $B/ws43hold.csv ] || echo "rt,shards,n,rep,arm,rps,cpu_us_op,shard_us_op,spill_us_op,other_us_op,compactions,evicted,files_unlinked,load1" > $B/ws43hold.csv
tt(){ local s=0 t; for t in /proc/$1/task/*; do grep -q "^$2" $t/comm 2>/dev/null && s=$((s+$(awk '{print $14+$15}' $t/stat))); done; echo $s; }
for rep in $(seq 1 $REPS); do for rt in $RTS; do for sh in $SHS; do
  A=(w2a-fa3f751 i2b-fe6fb20 i2b-fe6fb20@hold); arms=""; for i in 0 1 2; do arms="$arms ${A[$(( (i+rep-1) % 3 ))]}"; done
  for a in $arms; do
    d=$(mktemp -d $B/ab.XXXX); mkdir -p $d/off
    bin=${a%@hold}; hold=""; [ "$a" != "$bin" ] && { touch $B/reclaim.hold; hold="MOON_TEST_COLD_RECLAIM_HOLD_FILE=$B/reclaim.hold"; }
    env $hold MOON_DISK_FREE_MIN_PCT=0 $BIN/$bin-$rt --port $P --shards $sh --maxmemory 8388608 --maxmemory-policy allkeys-lru \
      --disk-offload enable --disk-offload-dir $d/off --appendonly no --save "3600 100000000" --dir $d \
      --disk-free-min-pct 0 > $d/log 2>&1 &
    pid=$!
    for _ in $(seq 1 80); do redis-cli -p $P ping >/dev/null 2>&1 && break; sleep 0.1; done
    t0=$(awk '{print $14+$15}' /proc/$pid/stat); s0=$(tt $pid shard-); p0=$(tt $pid spill-)
    rps=$(redis-benchmark -p $P -t set -r 50000 -d 600 -n $N -c 16 -P 16 -q 2>/dev/null | tr '\r' '\n' | grep -E "^SET: " | tail -1 | awk '{print $2}')
    t1=$(awk '{print $14+$15}' /proc/$pid/stat); s1=$(tt $pid shard-); p1=$(tt $pid spill-)
    inf=$(redis-cli -p $P info | tr -d '\r')
    cc=$(echo "$inf" | awk -F: '/^cold_reclaim_compactions:/{print $2}'); ev=$(echo "$inf" | awk -F: '/^evicted_keys:/{print $2}'); fu=$(echo "$inf" | awk -F: '/^cold_reclaim_files_unlinked:/{print $2}')
    kill -9 $pid; wait $pid 2>/dev/null; rm -rf $d
    f(){ awk -v a=$1 -v t=$TCK -v n=$N 'BEGIN{printf "%.2f", a/t*1e6/n}'; }
    echo "$rt,$sh,$N,$rep,$a,$rps,$(f $((t1-t0))),$(f $((s1-s0))),$(f $((p1-p0))),$(f $(( (t1-t0)-(s1-s0)-(p1-p0) ))),${cc:-0},${ev:-0},${fu:-0},$(cut -d' ' -f1 /proc/loadavg)" >> $B/ws43hold.csv
  done
done; done; done
