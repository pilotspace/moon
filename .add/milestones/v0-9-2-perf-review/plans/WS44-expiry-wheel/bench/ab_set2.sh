#!/bin/bash
# ws44 A/B: SET key:<rand> v EX 3600, wheel OFF vs ON, same binary (or two binaries), interleaved.
# Reports client-side rps AND server CPU us/op (utime+stime from /proc, robust to a shared box).
# usage: ab_set2.sh PORT REPS N KEYSPACE LABEL=BIN:ENV [LABEL=BIN:ENV ...]
PORT=$1; REPS=$2; N=$3; R=$4; shift 4
export MOON_DISK_FREE_MIN_PCT=0
cpu_ticks() { awk '{print $14+$15}' /proc/$1/stat; }
run_one() { # label bin envv P
  local label=$1 bin=$2 envv=$3 P=$4
  local d; d=$(mktemp -d /tmp/ws44ab.XXXXXX)
  env MOON_EXPIRY_WHEEL=$envv $bin --port $PORT --dir $d --shards 1 --appendonly no --save "" --disk-offload disable --maxmemory 0 >/dev/null 2>&1 &
  local pid=$!
  for i in $(seq 100); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
  redis-benchmark -p $PORT -c 50 -P $P -n $((N/2)) -r $R -q SET key:__rand_int__ v EX 3600 >/dev/null 2>&1
  local c0; c0=$(cpu_ticks $pid)
  local out; out=$(redis-benchmark -p $PORT -c 50 -P $P -n $N -r $R -q SET key:__rand_int__ v EX 3600 2>&1 | tr '\r' '\n' | grep "requests per second" | tail -1 | sed -n 's/.*: \([0-9.]*\) requests per second.*/\1/p')
  local c1; c1=$(cpu_ticks $pid)
  local us; us=$(awk -v a=$c0 -v b=$c1 -v n=$N 'BEGIN{printf "%.3f", (b-a)*10000/n}')
  echo "$label P$P rps=$out srv_cpu_us_per_op=$us load=$(cut -d' ' -f1 /proc/loadavg)"
  kill $pid 2>/dev/null; wait $pid 2>/dev/null; rm -rf $d
}
VARIANTS=("$@")
for rep in $(seq $REPS); do
  for P in 1 16; do
    if (( rep % 2 )); then order=("${VARIANTS[@]}"); else
      order=(); for ((i=${#VARIANTS[@]}-1;i>=0;i--)); do order+=("${VARIANTS[$i]}"); done; fi
    for v in "${order[@]}"; do
      label=${v%%=*}; rest=${v#*=}; bin=${rest%%:*}; envv=${rest#*:}
      run_one $label $bin $envv $P
    done
  done
done
