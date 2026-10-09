#!/bin/bash
# moon#1297: WS43 ab.sh (-t set -r 50000 -d 600 -n 100000 -c 16 -P 16, 8 MB maxmemory, disk offload, --save "3600 100000000")
# called one rep at a time so the arm order alternates per rep.
SP=${SCRATCH:-/tmp/moon-work}; B=$SP/bench2b; BIN=/home/user/wt/bin
RTS=${RTS:-monoio tokio}; REPS=${REPS:-3}
for rep in $(seq 1 $REPS); do for rt in $RTS; do for sh in 1 4; do
  if [ $((rep % 2)) -eq 1 ]; then o="$BIN/w2a-fa3f751-$rt $BIN/i2b-fe6fb20-$rt"; else o="$BIN/i2b-fe6fb20-$rt $BIN/w2a-fa3f751-$rt"; fi
  $SP/ab.sh $sh 1 $o | sed "s/rep1/rep$rep/" >> $B/ws43.txt
done; done; done
