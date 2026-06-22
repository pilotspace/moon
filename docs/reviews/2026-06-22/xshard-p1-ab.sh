#!/usr/bin/env bash
# Controlled A/B: isolate WHY s=12 p=1 collapses vs Redis.
#
# Single co-located VM run. Fixed: p=1, c=50, n=200000, GET+SET.
# Varied axes (the experiment):
#   shards  : 1  vs 12          (cross-shard probability 0% vs 91.7%)
#   keymode : single vs random  (affinity self-heal CAN vs CANNOT fire)
#
# Hypothesis: the 0.46x collapse needs BOTH 12 shards AND random keys.
#   - random keys (-r) defeat AffinityTracker (needs >=10/16 to one shard),
#     so the connection never migrates local -> permanent cross-shard tax.
#   - single key (__rand_key__, no -r) -> one dominant shard -> migrates
#     local after 16 cmds -> recovers toward the s1 number.
#
# All servers: in-memory, AOF off, fresh /tmp dir (matches the fair scripts).
set -u

MOON=/Volumes/Games/tindang-repo/moon/target-linux/release/moon
N=200000
C=50
PORT_REDIS=7000
PORT_MOON=7001
RKEYS=100000   # -r keyspace for the "random" arm

rps() { # parse redis-benchmark -q output robustly (8.x uses \r)
  tr '\r' '\n' | grep -i "requests per second" | tail -1 | awk '{print $2}'
}

bench() { # $1=port $2=test(get|set) $3=keymode(single|random)
  local port=$1 t=$2 mode=$3 ropt=""
  [ "$mode" = random ] && ropt="-r $RKEYS"
  redis-benchmark -p "$port" -n "$N" -c "$C" -P 1 -t "$t" $ropt -q 2>/dev/null | rps
}

start_redis() {
  redis-server --port $PORT_REDIS --save "" --appendonly no --daemonize yes >/dev/null 2>&1
  sleep 1
}

start_moon() { # $1=shards
  local s=$1 dir
  dir=$(mktemp -d /tmp/moon-ab.XXXXXX)
  RUST_LOG=error "$MOON" --port $PORT_MOON --shards "$s" \
    --appendonly no --disk-offload disable --dir "$dir" >/dev/null 2>&1 &
  MOON_PID=$!
  sleep 2
}

stop_moon() { kill -9 "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null; sleep 1; }

echo "# xshard p=1 A/B  (n=$N c=$C P=1)  $(date -u +%FT%TZ)"
echo

start_redis
echo "## Redis baselines (port $PORT_REDIS)"
R_GET_S=$(bench $PORT_REDIS get single);  echo "redis GET single = $R_GET_S"
R_GET_R=$(bench $PORT_REDIS get random);  echo "redis GET random = $R_GET_R"
R_SET_S=$(bench $PORT_REDIS set single);  echo "redis SET single = $R_SET_S"
R_SET_R=$(bench $PORT_REDIS set random);  echo "redis SET random = $R_SET_R"
echo

ratio() { awk -v m="$1" -v r="$2" 'BEGIN{ if(r>0) printf "%.2f", m/r; else print "n/a" }'; }

for S in 1 12; do
  start_moon "$S"
  echo "## Moon shards=$S (port $PORT_MOON)"
  M=$(bench $PORT_MOON get single); echo "moon s$S GET single = $M   ratio=$(ratio "$M" "$R_GET_S")x"
  M=$(bench $PORT_MOON get random); echo "moon s$S GET random = $M   ratio=$(ratio "$M" "$R_GET_R")x"
  M=$(bench $PORT_MOON set single); echo "moon s$S SET single = $M   ratio=$(ratio "$M" "$R_SET_S")x"
  M=$(bench $PORT_MOON set random); echo "moon s$S SET random = $M   ratio=$(ratio "$M" "$R_SET_R")x"
  stop_moon
  echo
done

redis-cli -p $PORT_REDIS shutdown nosave 2>/dev/null
echo "# done"
