#!/bin/bash
# moon#1322: 4-shard spanning MSET latency (r2b3-ab/lat.py, 300 MSETs x2 per shape per run),
# alternating base (w2a-fa3f751) / 2b (i2b-fe6fb20) per pair; order flips each pair.
# usage: lat_ab.sh <pairs>   -> appends to lat.txt
SP=${SCRATCH:-/tmp/moon-work}; B=$SP/bench2b; BIN=/home/user/wt/bin
PAIRS=${1:-6}; PORT=7630
for pair in $(seq 1 $PAIRS); do for rt in monoio tokio; do for pol in always everysec; do
  if [ $((pair % 2)) -eq 1 ]; then arms="w2a-fa3f751 i2b-fe6fb20"; else arms="i2b-fe6fb20 w2a-fa3f751"; fi
  for a in $arms; do
    timeout 300 python3 $SP/r2b3-ab/lat.py $BIN/$a-$rt $PORT $pol 300 2>&1 | sed "s/^/$pair $rt $a $(cut -d' ' -f1 /proc/loadavg) /" >> $B/lat.txt
  done
done; done; done
