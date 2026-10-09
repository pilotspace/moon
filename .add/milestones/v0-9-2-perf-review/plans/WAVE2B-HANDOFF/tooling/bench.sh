#!/bin/bash
# Wave-2b quiet-window A/B: base = wave 2a (fa3f751) vs final = wave 2b (fe6fb20). Copy of bench2a/bench.sh; only arms, port, out dir changed.
# Interleaved (order alternates by rep parity), fresh server per run. Linux container, relative evidence only.
set -u
O=$(dirname $0); BIN=/home/user/wt/bin
BASE=$BIN/w2a-fa3f751-monoio; FINAL=$BIN/i2b-fe6fb20-monoio
[ -n "${RT:-}" ] && { BASE=$BIN/w2a-fa3f751-$RT; FINAL=$BIN/i2b-fe6fb20-$RT; }
P=${PORT:-7610}; REPS=${REPS:-3}; N=${N:-200000}
out=$O/results-${RT:-monoio}${TAG:-}.csv; echo "cell,rep,arm,test,rps,load1" > $out
run(){ # arm bin shards aof fsync P clients test
  local arm=$1 bin=$2 sh=$3 aof=$4 fs=$5 pl=$6 c=$7 t=$8
  local d=$O/data-$arm
  rm -rf $d; mkdir -p $d
  MOON_DISK_FREE_MIN_PCT=0 $bin --port $P --shards $sh --dir $d --appendonly $aof --appendfsync $fs --save "" --maxmemory 0 >$O/srv.log 2>&1 &
  local pid=$!; for i in $(seq 1 100); do redis-cli -p $P ping >/dev/null 2>&1 && break; sleep 0.05; done
  local r=$(redis-benchmark -p $P -t $t -n $N -c $c -P $pl -r 100000 -d 64 -q 2>/dev/null | tr '\r' '\n' | grep -E "^[A-Z]+:" | tail -1 | awk '{print $2}')
  redis-cli -p $P shutdown nosave >/dev/null 2>&1; wait $pid 2>/dev/null
  echo "s${sh}-aof${aof}-${fs}-p${pl}-c${c},$rep,$arm,$t,$r,$(cut -d" " -f1 /proc/loadavg)" >> $out
}
CELLS="${CELLS_OVERRIDE:-1:no:everysec:1:50:set 1:no:everysec:16:50:set 1:no:everysec:1:50:get 1:yes:everysec:1:1:set 1:yes:everysec:1:50:set 1:yes:everysec:16:1:set 1:yes:everysec:16:50:set 4:yes:everysec:1:50:set 4:yes:everysec:16:50:set 1:yes:always:16:50:set}"
for rep in $(seq 1 $REPS); do
  for cell in $CELLS; do
    IFS=: read sh aof fs pl c t <<< "$cell"
    if [ $((rep % 2)) -eq 1 ]; then run base $BASE $sh $aof $fs $pl $c $t; run final $FINAL $sh $aof $fs $pl $c $t
    else run final $FINAL $sh $aof $fs $pl $c $t; run base $BASE $sh $aof $fs $pl $c $t; fi
  done
done
python3 - $out <<'PY'
import csv,sys,statistics as st,collections
rows=list(csv.DictReader(open(sys.argv[1]))); d=collections.defaultdict(list)
for r in rows:
    try: d[(r['cell'],r['test'],r['arm'])].append(float(r['rps']))
    except: pass
cells=sorted({(r['cell'],r['test']) for r in rows})
print(f"{'cell':32} {'base median (min-max)':28} {'final median (min-max)':28} delta")
for c,t in cells:
    b=d[(c,t,'base')]; f=d[(c,t,'final')]
    if not b or not f: continue
    mb,mf=st.median(b),st.median(f)
    print(f"{c+' '+t:32} {mb:9.0f} ({min(b):.0f}-{max(b):.0f})   {mf:9.0f} ({min(f):.0f}-{max(f):.0f})   {100*(mf-mb)/mb:+.1f}%")
PY
