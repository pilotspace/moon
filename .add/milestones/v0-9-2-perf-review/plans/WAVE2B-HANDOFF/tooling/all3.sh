#!/bin/bash
cd "$(dirname "$0")"
V=$1   # binary prefix e.g. /home/user/wt/bin/r2bfb2-v2
OUT=$2
KEYED="mset msetnx del unlink bitop copy set incr eval multi txn"
CNT="flushall flushdb mflush eflush erflush mrflush swapdb"
BOOTK="mset msetnx set incr eval multi txn flushall mflush eflush erflush mrflush swapdb"
{
for rt in tokio monoio; do
  BIN=$V-$rt
  if [ $rt = monoio ]; then export EXTRA_ENV="MOON_NO_URING=1"; else export EXTRA_ENV=""; fi
  for mode in always after_always; do
    ./run3.sh $rt $BIN 28200 4 $mode $KEYED $CNT
  done
  ./run3.sh $rt $BIN 28200 4 boot $BOOTK
  ./run3.sh $rt $BIN 28200 1 always set incr multi eval swapdb flushall mflush
  ./run3.sh $rt $BIN 28200 1 after_always set incr swapdb flushall
  ./run3.sh $rt $BIN 28200 1 boot set incr swapdb flushall
done
} > $OUT 2>&1
echo DONE >> $OUT
