#!/usr/bin/env bash
# remeasure-xshard-fix.sh — validate the batch-gate fix (xshard-read-fastpath).
# 3-way same-conditions matrix: no-mech / old-mech (P16-regressed) / new-mech (fixed).
# Cells: s4-c1-GET (the win), s4-c100-GET (guard), s4-P16-GET (the regression), s1-P16 (noise control).
# best-of-5, quiesced, core-pinned. RELATIVE ratios only (VM absolute is untrusted).
set -u
WORK="$HOME/moon-baseline"; BINS="$HOME/moon-baseline-bins"
REQ="${REQ:-300000}"; KEYSPACE=1000000; PORT=7480
build_bin() { # commit -> path on stdout
  local c="$1"
  local out="$BINS/moon-${c}-monoio"
  [ -x "$out" ] && { echo "$out"; return 0; }
  git -C "$WORK" fetch -q origin 2>/dev/null || true
  git -C "$WORK" checkout -q "$c" || { echo "CHECKOUT-FAIL:$c" >&2; return 1; }
  ( cd "$WORK" && CARGO_TARGET_DIR="$WORK/target" RUSTFLAGS="-C target-cpu=native" cargo build --release >/dev/null 2>&1 ) || { echo "BUILD-FAIL:$c" >&2; return 1; }
  mkdir -p "$BINS"; cp "$WORK/target/release/moon" "$out"; echo "$out"
}
wait_q() { for _ in $(seq 1 40); do l=$(awk '{print $1}' /proc/loadavg); awk -v a="$l" 'BEGIN{exit !(a<0.7)}' && return 0; sleep 2; done; }
bench() { taskset -c 4,5 redis-benchmark -p $PORT -c "$1" -P "$2" -n "$REQ" -r "$KEYSPACE" -t "$3" --csv 2>/dev/null | tr '\r' '\n' | grep "\"${3^^}\"" | awk -F',' '{gsub(/"/,"",$2);printf "%.0f",$2}'; }
measure() { # bin cores shards c P cmd label
  local vals=()
  for _ in 1 2 3 4 5; do
    wait_q
    local dir; dir=$(mktemp -d /tmp/rmx.XXXXXX)
    taskset -c "$2" "$1" --port $PORT --shards "$3" --dir "$dir" --appendonly no --admin-port 0 >/dev/null 2>&1 &
    local pid=$!; for _ in $(seq 1 50); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.1; done
    vals+=("$(bench "$4" "$5" "$6")")
    kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  done
  local best med
  best=$(printf '%s\n' "${vals[@]}" | sort -n | tail -1)
  med=$(printf '%s\n' "${vals[@]}" | sort -n | awk '{a[NR]=$1}END{print a[int((NR+1)/2)]}')
  printf '%-14s best=%-9s med=%-9s reps=[%s]\n' "$7" "$best" "$med" "$(IFS=,;echo "${vals[*]}")"
}
NM=$(build_bin a497602) || exit 1
OM=$(build_bin 2bfc5bc) || exit 1
FM=$(build_bin 7048e8a) || exit 1
echo "###### XSHARD BATCH-GATE FIX RE-MEASURE  reqs=$REQ best-of-5 quiesced  host=$(uname -m)"
for tag in "no-mech:$NM" "old-mech:$OM" "new-mech:$FM"; do
  echo "### ${tag%%:*}"
  measure "${tag##*:}" 0-3 4 1   1  GET "s4-c1-GET"
  measure "${tag##*:}" 0-3 4 100 1  GET "s4-c100-GET"
  measure "${tag##*:}" 0-3 4 50  16 GET "s4-P16-GET"
  measure "${tag##*:}" 0   1 50  16 GET "s1-P16-CTRL"
done
echo "###### END"
