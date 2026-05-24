#!/usr/bin/env bash
# PR #100 multi-shard perf: Moon at s=1/2/4 vs Redis on same VM (6 cores).
# GET + SET, p=1/16/64, c=400, median-of-3. Random keys (-r 100000) so SET
# write rate isn't artificially capped to 1 hot key.
set -euo pipefail

REPO=/Volumes/Games/tindang-repo/moon
MOON_PORT=7540
REDIS_PORT=7541
RESULTS=/tmp/pr100-multi.tsv
SUMMARY=/tmp/pr100-multi.md
CLIENTS=400

cd "$REPO"
printf "engine\tshards\top\tpipeline\tr1\tr2\tr3\tmedian\n" > "$RESULTS"

extract_rps () { tr '\r' '\n' | grep -oE "[0-9]+\.[0-9]+ requests" | tail -1 | awk '{print $1}'; }
median3 () { printf "%s\n" "$@" | sort -n | sed -n '2p'; }

run_cell () {
  local engine=$1 port=$2 shards=$3 op=$4 pipe=$5
  local n=$((300000 * pipe / 4))
  [[ $n -lt 100000 ]] && n=100000
  [[ $n -gt 1500000 ]] && n=1500000
  local r1 r2 r3 med
  r1=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  r2=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  r3=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  med=$(median3 "$r1" "$r2" "$r3")
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$engine" "$shards" "$op" "$pipe" "$r1" "$r2" "$r3" "$med" >> "$RESULTS"
  printf "%-6s s=%-1s %-3s p=%-3s [%s %s %s] median=%s\n" "$engine" "$shards" "$op" "$pipe" "$r1" "$r2" "$r3" "$med"
}

run_moon () {
  local shards=$1
  echo "=== Moon (s=$shards, --appendonly no --disk-offload disable) ==="
  ./target/release/moon --port $MOON_PORT --shards $shards --appendonly no --disk-offload disable > /tmp/moon-multi-s$shards.log 2>&1 &
  local pid=$!
  sleep 2
  redis-cli -p $MOON_PORT PING > /dev/null || { echo "moon s=$shards start fail"; kill $pid; exit 1; }
  for op in get set; do for p in 1 16 64; do run_cell moon $MOON_PORT $shards $op $p; done; done
  kill $pid 2>/dev/null; wait 2>/dev/null || true
  sleep 1
}

# --- Moon s=1, s=2, s=4 ---
for s in 1 2 4; do run_moon $s; done

# --- Redis (single-process baseline) ---
echo "=== Redis (single-process, appendonly no) ==="
redis-server --port $REDIS_PORT --appendonly no --save "" --daemonize no > /tmp/redis-multi.log 2>&1 &
RPID=$!
sleep 2
redis-cli -p $REDIS_PORT PING > /dev/null || { echo "redis start fail"; kill $RPID; exit 1; }
for op in get set; do for p in 1 16 64; do run_cell redis $REDIS_PORT 1 $op $p; done; done
kill $RPID 2>/dev/null; wait 2>/dev/null || true

# --- Summary table ---
echo ""
echo "=== Summary ==="
{
  echo "| cell        | Redis      | Moon s=1   | Moon s=2   | Moon s=4   | best vs Redis |"
  echo "|-------------|------------|------------|------------|------------|---------------|"
} | tee "$SUMMARY"
for op in get set; do
  for p in 1 16 64; do
    r=$(awk -v op="$op" -v p="$p" '$1=="redis" && $3==op && $4==p {print $8}' "$RESULTS")
    m1=$(awk -v op="$op" -v p="$p" '$1=="moon" && $2==1 && $3==op && $4==p {print $8}' "$RESULTS")
    m2=$(awk -v op="$op" -v p="$p" '$1=="moon" && $2==2 && $3==op && $4==p {print $8}' "$RESULTS")
    m4=$(awk -v op="$op" -v p="$p" '$1=="moon" && $2==4 && $3==op && $4==p {print $8}' "$RESULTS")
    best=$(awk -v a="$m1" -v b="$m2" -v c="$m4" 'BEGIN {x=a; if(b>x)x=b; if(c>x)x=c; print x}')
    ratio=$(awk -v b="$best" -v r="$r" 'BEGIN {printf "%.2f", b/r}')
    printf "| %-3s p=%-3s | %-10s | %-10s | %-10s | %-10s | %sx |\n" "$op" "$p" "$r" "$m1" "$m2" "$m4" "$ratio" | tee -a "$SUMMARY"
  done
done
echo ""
echo "Full TSV: $RESULTS"
echo "Summary:  $SUMMARY"
