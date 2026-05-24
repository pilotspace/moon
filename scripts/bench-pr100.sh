#!/usr/bin/env bash
# PR #100 final perf check: Moon (current HEAD) vs Redis on the same VM.
# s=1, GET + SET, p=1/16/64, c=400, median-of-3. ~15 min total.
set -euo pipefail

REPO=/Users/tindang/workspaces/tind-repo/moon
MOON_PORT=7530
REDIS_PORT=7531
RESULTS=/tmp/pr100-bench.tsv
SUMMARY=/tmp/pr100-bench.md
CLIENTS=400

cd "$REPO"
printf "engine\top\tpipeline\tr1\tr2\tr3\tmedian\n" > "$RESULTS"

extract_rps () { tr '\r' '\n' | grep -oE "[0-9]+\.[0-9]+ requests" | tail -1 | awk '{print $1}'; }
median3 () { printf "%s\n" "$@" | sort -n | sed -n '2p'; }

run_cell () {
  local engine=$1 port=$2 op=$3 pipe=$4
  local n=$((300000 * pipe / 4))
  [[ $n -lt 100000 ]] && n=100000
  [[ $n -gt 1500000 ]] && n=1500000
  local r1 r2 r3 med
  r1=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  r2=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  r3=$(redis-benchmark -h 127.0.0.1 -p $port -t $op -P $pipe -c $CLIENTS -n $n -r 100000 -q 2>/dev/null | extract_rps)
  med=$(median3 "$r1" "$r2" "$r3")
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$engine" "$op" "$pipe" "$r1" "$r2" "$r3" "$med" >> "$RESULTS"
  printf "%-6s %-3s p=%-3s [%s %s %s] median=%s\n" "$engine" "$op" "$pipe" "$r1" "$r2" "$r3" "$med"
}

# --- Moon ---
echo "=== Moon (s=1, --appendonly no --disk-offload disable) ==="
./target/release/moon --port $MOON_PORT --shards 1 --appendonly no --disk-offload disable > /tmp/moon-pr100.log 2>&1 &
MPID=$!
sleep 2
redis-cli -p $MOON_PORT PING > /dev/null || { echo "moon start fail"; kill $MPID; exit 1; }
for op in get set; do for p in 1 16 64; do run_cell moon $MOON_PORT $op $p; done; done
kill $MPID 2>/dev/null; wait 2>/dev/null || true
sleep 1

# --- Redis ---
echo "=== Redis (single-process, appendonly no) ==="
redis-server --port $REDIS_PORT --appendonly no --save "" --daemonize no > /tmp/redis-pr100.log 2>&1 &
RPID=$!
sleep 2
redis-cli -p $REDIS_PORT PING > /dev/null || { echo "redis start fail"; kill $RPID; exit 1; }
for op in get set; do for p in 1 16 64; do run_cell redis $REDIS_PORT $op $p; done; done
kill $RPID 2>/dev/null; wait 2>/dev/null || true

# --- Summary table ---
echo ""
echo "=== Summary ==="
echo "| cell        | Moon       | Redis      | Moon vs Redis |" | tee "$SUMMARY"
echo "|-------------|------------|------------|---------------|" | tee -a "$SUMMARY"
for op in get set; do
  for p in 1 16 64; do
    m=$(awk -v op="$op" -v p="$p" '$1=="moon" && $2==op && $3==p {print $7}' "$RESULTS")
    r=$(awk -v op="$op" -v p="$p" '$1=="redis" && $2==op && $3==p {print $7}' "$RESULTS")
    ratio=$(awk -v m="$m" -v r="$r" 'BEGIN {printf "%.2f", m/r}')
    printf "| %-3s p=%-3s | %-10s | %-10s | %sx |\n" "$op" "$p" "$m" "$r" "$ratio" | tee -a "$SUMMARY"
  done
done
echo ""
echo "Full TSV: $RESULTS"
echo "Summary:  $SUMMARY"
