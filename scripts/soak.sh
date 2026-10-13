#!/usr/bin/env bash
# soak.sh — continuous mixed-load soak with explicit pass criteria (Linux).
#
# The v0.8.10 release gate. It found two release blockers that every other
# gate passed: #1055's allkeys-* write lockout (reverted in #1157) and #1158,
# an AOF rewrite that was dispatched and never committed under sustained
# mixed writes on a real disk. Neither shows up in a suite that runs for
# seconds; both show up here within minutes.
#
#   scripts/soak.sh --bin <moon> --duration <secs> [--stress] [--port N]
#                   [--dir D] [--out O] [--sample SECS] [--tolerate CLASS]...
#
#   --tolerate CLASS : do not fail on this server error class (e.g. OOM while
#              #1156 is open). Still counted and logged. Repeatable.
#
#   default  : the shipped config (--shards 4, AOF everysec, maxmemory 512mb
#              allkeys-lru), 1M-key preload, 5-minute samples. The nightly.
#   --stress : the same load with auto-aof-rewrite-min-size 4mb and 1-minute
#              samples, so AOF rewrites fire back to back. The ci-local leg:
#              #1158 reproduces in minutes here instead of hours.
#
# Four loads run for the whole duration — SET/GET, HSET over 3M fields,
# XADD and XREADGROUP on 16 streams — and each restarts when it exits, with
# every exit counted, so a dead load is REPORTED rather than silently absent
# (the first soak driver's side loads died on NOGROUP and nobody noticed).
#
# Criteria (any one fails the run):
#   * moon answers PING at every sample, and never panics
#   * a SET probe lands at >=95% of samples and at the last 3 (the #1055
#     lockout: an evicting policy must never refuse writes for good)
#   * DBSIZE grows between the first and last sample
#   * RSS in the last quarter <= 1.3x the second quarter; FD delta <= 50;
#     GET p99 in the last quarter <= 2x the first
#   * AOF rewrites: at least one committed, and once the load stops every
#     dispatched rewrite commits within 120s (the #1158 stall); no append
#     LOST; auto-rewrite never reports a rewrite that has not finished
#   * kill -9 then restart: DBSIZE and DEBUG DIGEST unchanged
#   * no load received a server error, by class (the first word after
#     "Error from server:"), unless that class is --tolerate'd. The v0.8.10
#     RC3 soak passed every sampled criterion while its stream loads took
#     3966 IOERRs (#1201) — the samples never looked at the loads' replies.
#
# Exit 0 = PASS, 1 = a criterion failed, 2 = usage, 3 = moon never started.
# Linux only: RSS and FDs are read from /proc.
set -uo pipefail

BIN=""; DUR=""; STRESS=0; PORT=6500; DIR=""; OUT=""; SAMPLE=""; TOLERATE=" "
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN=$2; shift 2 ;;
    --duration) DUR=$2; shift 2 ;;
    --stress) STRESS=1; shift ;;
    --port) PORT=$2; shift 2 ;;
    --dir) DIR=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    --sample) SAMPLE=$2; shift 2 ;;
    --tolerate) TOLERATE="$TOLERATE$2 "; shift 2 ;;
    *) echo "usage: $0 --bin <moon> --duration <secs> [--stress] [--port N] [--dir D] [--out O] [--sample S]" >&2; exit 2 ;;
  esac
done
[ -x "$BIN" ] && [ -n "$DUR" ] || { echo "soak: --bin <executable> and --duration are required" >&2; exit 2; }
for t in redis-cli redis-benchmark python3 timeout; do
  command -v $t >/dev/null || { echo "soak: $t not on PATH" >&2; exit 2; }
done
# Default to $HOME, not /tmp: /tmp is tmpfs on moon-dev, and #1158 only
# reproduced on a real disk.
DIR=${DIR:-$HOME/soak-data-$PORT}; OUT=${OUT:-$HOME/soak-out-$PORT}
if [ $STRESS = 1 ]; then
  SAMPLE=${SAMPLE:-60}; PRELOAD=200000
  EXTRA=(--auto-aof-rewrite-min-size 4mb --auto-aof-rewrite-percentage 50)
else
  SAMPLE=${SAMPLE:-300}; PRELOAD=1000000; EXTRA=()
fi
rm -rf "$DIR" "$OUT"; mkdir -p "$DIR" "$OUT"
log() { echo "[$(date '+%F %T')] $*" | tee -a "$OUT/driver.log"; }
cli() { timeout 10 redis-cli -p "$PORT" "$@"; }
mlog() { sed 's/\x1b\[[0-9;]*m//g' "$OUT/moon.log"; }
count() { mlog | grep -c -- "$1"; }

MOON_PID=""
start_moon() {
  "$BIN" --port "$PORT" --shards 4 --dir "$DIR" \
    --appendonly yes --appendfsync everysec \
    --maxmemory 512mb --maxmemory-policy allkeys-lru "${EXTRA[@]}" \
    >>"$OUT/moon.log" 2>&1 &
  MOON_PID=$!
  for _ in $(seq 1 1200); do cli PING 2>/dev/null | grep -q PONG && return 0; sleep 0.5; done
  log "FAIL: moon did not become ready"; return 1
}
LOADS=()
cleanup() {
  [ ${#LOADS[@]} -gt 0 ] && kill "${LOADS[@]}" 2>/dev/null
  pkill -P $$ 2>/dev/null
  [ -n "$MOON_PID" ] && kill -9 "$MOON_PID" 2>/dev/null
}
trap cleanup EXIT

log "binary: $BIN duration=${DUR}s stress=$STRESS port=$PORT sample=${SAMPLE}s"
start_moon || exit 3
redis-benchmark -p "$PORT" -t set -n $PRELOAD -r $PRELOAD -d 64 -c 50 -P 32 -q >/dev/null 2>&1
# 12-digit names: redis-benchmark expands __rand_int__ to 12 digits, so the
# groups must exist on exactly those keys or every XREADGROUP is NOGROUP.
for i in $(seq 0 15); do cli XGROUP CREATE "st:$(printf %012d "$i")" g '$' MKSTREAM >/dev/null; done
DEADLINE=$(( $(date +%s) + DUR ))

loop() {  # <name> <cmd...>
  local name=$1; shift
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    "$@" >/dev/null 2>>"$OUT/load-$name.err"
    echo "$(date '+%T') exit=$?" >> "$OUT/load-$name.exits"
    sleep 1
  done
}
loop kv    redis-benchmark -p "$PORT" -t set,get -c 40 -P 16 -r 1000000 -d 64 -n 20000000 -q & LOADS+=($!)
loop hset  redis-benchmark -p "$PORT" -c 4 -P 8 -r 3000000 -n 5000000 HSET 'h:__rand_int__' f '__rand_int__' & LOADS+=($!)
loop xadd  redis-benchmark -p "$PORT" -c 2 -r 16 -n 2000000 XADD 'st:__rand_int__' MAXLEN '~' 10000 '*' f v & LOADS+=($!)
loop xread redis-benchmark -p "$PORT" -c 2 -r 16 -n 2000000 XREADGROUP GROUP g c COUNT 10 STREAMS 'st:__rand_int__' '>' & LOADS+=($!)

echo "ts,rss_kb,fds,dbsize,used_memory,get_p99_ms,alive,write_ok,rw_dispatched,rw_committed" > "$OUT/samples.csv"
ALIVE_FAILS=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  sleep "$SAMPLE"
  if kill -0 "$MOON_PID" 2>/dev/null && cli PING 2>/dev/null | grep -q PONG; then alive=1; else alive=0; ALIVE_FAILS=$((ALIVE_FAILS+1)); fi
  rss=$(ps -o rss= -p "$MOON_PID" 2>/dev/null | tr -d ' ')
  fds=$(ls "/proc/$MOON_PID/fd" 2>/dev/null | wc -l)
  dbs=$(cli DBSIZE 2>/dev/null)
  um=$(cli INFO memory 2>/dev/null | tr -d '\r' | awk -F: '/^used_memory:/{print $2}')
  p99=$(timeout 60 redis-benchmark -p "$PORT" -t get -c 20 -P 16 -r 1000000 -n 200000 --csv 2>/dev/null | tr -d '"\r' | awk -F, '$1=="GET"{print $7}')
  wok=0; for _ in $(seq 1 10); do cli SET "soak:probe:$(date +%s%N)" v 2>/dev/null | grep -q '^OK' && { wok=1; break; }; sleep 0.5; done
  echo "$(date '+%T'),$rss,$fds,$dbs,$um,$p99,$alive,$wok,$(count 'rewrite dispatched'),$(count 'per-shard rewrite complete')" >> "$OUT/samples.csv"
  [ $alive = 1 ] || log "ALERT: moon not answering at sample"
done

kill "${LOADS[@]}" 2>/dev/null; LOADS=(); pkill -P $$ redis-benchmark 2>/dev/null; sleep 2
kill -0 "$MOON_PID" 2>/dev/null || { log "FAIL: moon died during the soak"; log "SOAK VERDICT: FAIL"; exit 1; }
for n in kv hset xadd xread; do log "load $n exits: $(wc -l < "$OUT/load-$n.exits" 2>/dev/null || echo 0); stderr tail: $(tail -c 200 "$OUT/load-$n.err" 2>/dev/null | tr '\n\r' '  ')"; done
# Every error reply the loads saw, by class. redis-benchmark stops a run at
# its first error, so a count is a count of runs that hit one — a floor.
cat "$OUT"/load-*.err 2>/dev/null | tr '\r' '\n' | grep -o 'Error from server: [A-Za-z]*' \
  | awk '{print $4}' | sort | uniq -c | awk '{print $2, $1}' > "$OUT/errors-by-class.txt"
while read -r cls n; do log "load errors: $cls x$n"; done < "$OUT/errors-by-class.txt"

# #1158: a stalled rewrite never commits, however long the quiet. Give an
# in-flight one 120s with the load off before calling it stuck.
for _ in $(seq 1 120); do [ "$(count 'rewrite dispatched')" -le "$(count 'per-shard rewrite complete')" ] && break; sleep 1; done
RW_D=$(count 'rewrite dispatched'); RW_C=$(count 'per-shard rewrite complete')
RW_LOST=$(count 'append LOST'); RW_STUCK=$(count 'has not finished')
log "aof rewrites dispatched=$RW_D committed=$RW_C lost=$RW_LOST stuck_reports=$RW_STUCK aof_mb=$(du -sm "$DIR/appendonlydir" 2>/dev/null | cut -f1)"

sleep 5
BEFORE_SIZE=$(cli DBSIZE); BEFORE_DIGEST=$(cli DEBUG DIGEST)
log "before kill -9: dbsize=$BEFORE_SIZE digest=$BEFORE_DIGEST"
kill -9 "$MOON_PID"; wait "$MOON_PID" 2>/dev/null
start_moon || exit 3
for _ in $(seq 1 2400); do cli INFO persistence 2>/dev/null | grep -q '^loading:0' && break; sleep 0.5; done
AFTER_SIZE=$(cli DBSIZE); AFTER_DIGEST=$(cli DEBUG DIGEST)
log "after restart:  dbsize=$AFTER_SIZE digest=$AFTER_DIGEST"
cli SHUTDOWN NOSAVE >/dev/null 2>&1; sleep 2; kill -9 "$MOON_PID" 2>/dev/null; MOON_PID=""

PANICS=$(mlog | grep -ciE 'panicked|SIGSEGV|fatal')
N=$(( $(wc -l < "$OUT/samples.csv") - 1 ))
read -r RSS_Q2 RSS_Q4 FD_FIRST FD_LAST P99_Q1 P99_Q4 WFAIL WLAST DB_FIRST DB_LAST < <(python3 - "$OUT/samples.csv" <<'EOF'
import csv,sys,statistics as st
r=[x for x in csv.DictReader(open(sys.argv[1])) if x['rss_kb']]
if not r: print("0 0 0 0 0 0 0 0 0 0"); sys.exit()
w=max(1,len(r)//4)
f=lambda k,rows:[float(x[k]) for x in rows if x[k] not in ('',None)]
rq2=max(f('rss_kb',r[w:2*w]) or f('rss_kb',r)); rq4=max(f('rss_kb',r[-w:]))
p1=st.median(f('get_p99_ms',r[:w]) or [0]); p4=st.median(f('get_p99_ms',r[-w:]) or [0])
wf=sum(1 for x in r if x['write_ok']!='1'); wl=all(x['write_ok']=='1' for x in r[-3:])
print(int(rq2),int(rq4),r[0]['fds'],r[-1]['fds'],p1,p4,wf,1 if wl else 0,r[0]['dbsize'] or 0,r[-1]['dbsize'] or 0)
EOF
)
log "samples=$N alive_fails=$ALIVE_FAILS rss q2=${RSS_Q2}KB q4=${RSS_Q4}KB fds ${FD_FIRST}->${FD_LAST} get_p99 q1=${P99_Q1}ms q4=${P99_Q4}ms write_fail=$WFAIL dbsize ${DB_FIRST}->${DB_LAST} panics=$PANICS"

VERDICT=PASS
fail() { VERDICT=FAIL; log "criterion: $*"; }
[ "$N" -ge 4 ] || fail "only $N samples — the run was too short to judge"
[ "$ALIVE_FAILS" = 0 ] || fail "moon unresponsive at $ALIVE_FAILS samples"
[ "$PANICS" = 0 ] || fail "panics in moon.log"
python3 -c "import sys; sys.exit(0 if $WFAIL <= max(1, 0.05*$N) else 1)" || fail "write probe failed at >5% of samples"
[ "$WLAST" = 1 ] || fail "writes refused at the end of the soak (lockout)"
[ "$DB_LAST" -gt "$DB_FIRST" ] || fail "DBSIZE never grew — writes are not landing"
python3 -c "import sys; sys.exit(0 if $RSS_Q4 <= 1.3*$RSS_Q2 else 1)" || fail "RSS grew >30% between quarter 2 and quarter 4"
[ $(( FD_LAST - FD_FIRST )) -le 50 ] || fail "FD leak (${FD_FIRST}->${FD_LAST})"
python3 -c "import sys; sys.exit(0 if $P99_Q4 <= 2*max($P99_Q1,0.5) else 1)" || fail "GET p99 more than doubled"
[ "$RW_C" -ge 1 ] || fail "no AOF rewrite ever committed — the rewrite path was not exercised"
[ "$RW_D" -le "$RW_C" ] || fail "$((RW_D - RW_C)) AOF rewrite(s) dispatched and never committed (#1158)"
[ "$RW_LOST" = 0 ] || fail "$RW_LOST AOF appends LOST"
[ "$RW_STUCK" = 0 ] || fail "auto-rewrite reported a rewrite that has not finished"
[ "$BEFORE_SIZE" = "$AFTER_SIZE" ] || fail "DBSIZE changed across kill -9 ($BEFORE_SIZE -> $AFTER_SIZE)"
[ "$BEFORE_DIGEST" = "$AFTER_DIGEST" ] || fail "DEBUG DIGEST changed across kill -9"
while read -r cls n; do
  case "$TOLERATE" in
    *" $cls "*) log "tolerated: $n load runs ended in $cls (--tolerate $cls)" ;;
    *) fail "$n load runs ended in a $cls error reply" ;;
  esac
done < "$OUT/errors-by-class.txt"
log "SOAK VERDICT: $VERDICT"
# Keep the data only when it is evidence: a passing run's AOF is gigabytes
# of nothing on a VM whose full root wedges OrbStack.
[ $VERDICT = PASS ] && rm -rf "$DIR"
[ $VERDICT = PASS ]
