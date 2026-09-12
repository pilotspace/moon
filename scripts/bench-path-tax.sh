#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# bench-path-tax.sh -- how much does moon's GENERIC command path cost, over and
# above its inline path, for IDENTICAL handler work?
#
# moon serves `GET` and plain `SET key value` from a byte-level inline path
# (`server::conn::try_inline_dispatch`) that builds no `Frame` and never touches
# the dispatch table. Everything else takes the generic path. Measured per-
# command costs say the inline path is ~0.56-0.58x of Redis's while the generic
# path is 1.3-1.65x -- but those compare DIFFERENT COMMANDS, so the gap could be
# the path or could be the datatype. This separates them.
#
# The trick: `can_inline_reads` is a conjunction whose first term is
# `conn.acl_skip_allowed()` (handler_monoio/mod.rs). A connection authenticated
# as a NON-unrestricted ACL user therefore runs *the same GET handler* through
# the full generic preamble. Same command, same data, same host, same session --
# only the path differs.
#
# CONFOUND, stated up front: the restricted leg also pays the ACL check that the
# inline leg skips. That is one-directional, so the result is an UPPER BOUND on
# what removing the path overhead could recover. Do not quote it as an exact
# figure.
#
# THE INSTRUMENT MUST DISCRIMINATE. `moon_dispatch_path_total{path="local_inline"}`
# has exactly one production call site. This script fails closed unless the
# counter advances on every inline leg and stays EXACTLY frozen on every generic
# leg -- otherwise a leg that silently ran zero commands, or an ACL user that was
# still unrestricted, would look like a result. The first run of this experiment
# did exactly that: `redis-benchmark` takes `-a`, not `--pass`, so the generic
# legs produced empty output while the frozen counter "confirmed" the path.
#
# Usage:
#   ./scripts/bench-path-tax.sh --moon-bin ./target/release/moon --reps 3
###############################################################################

MOON_BIN="./target/release/moon"
PORT=7561
ADMIN_PORT=7562
REPS=3
KEYSPACE=100000
COMMAND="get key:__rand_int__"
MODE=tax

while [[ $# -gt 0 ]]; do
  case "$1" in
    --moon-bin) MOON_BIN="$2"; shift 2 ;;
    --reps)     REPS="$2"; shift 2 ;;
    --port)     PORT="$2"; shift 2 ;;
    --admin-port) ADMIN_PORT="$2"; shift 2 ;;
    --command)  COMMAND="$2"; shift 2 ;;
    # tax      = inline vs generic for an inline-ELIGIBLE command (GET, plain SET).
    #            Measures the generic preamble, but the generic leg also pays the
    #            ACL check -- hence an upper bound.
    # aclcost  = unrestricted vs restricted for a command that is NEVER
    #            inline-eligible (HSET). BOTH legs take the generic path, so the
    #            only difference is the ACL check itself. This turns the `tax`
    #            mode's confound from an unbounded caveat into a number you can
    #            subtract. `acl/table.rs:428` does `to_ascii_lowercase()` -- a
    #            heap allocation per command -- and is reached only when the user
    #            is NOT unrestricted, i.e. only on the `tax` generic leg.
    --mode)     MODE="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

case "$MODE" in
  tax|aclcost) ;;
  *) echo "FATAL: --mode must be 'tax' or 'aclcost', got '$MODE'" >&2; exit 1 ;;
esac

for _tool in timeout redis-cli redis-benchmark curl; do
  command -v "$_tool" >/dev/null 2>&1 || {
    echo "FATAL: '$_tool' not found. Linux-only harness." >&2; exit 1; }
done

SERVER_PID=""; SERVER_DIR=""
stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  [[ -n "$SERVER_DIR" ]] && rm -rf "$SERVER_DIR"
}
trap stop_server EXIT

if [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]]; then
  echo "FATAL: something already answers PING on $PORT -- refusing to start." >&2
  exit 1
fi

SERVER_DIR="$(mktemp -d)"
MOON_DISK_FREE_MIN_PCT=0 "$MOON_BIN" --port "$PORT" --admin-port "$ADMIN_PORT" \
  --shards 1 --dir "$SERVER_DIR" --protected-mode no \
  --appendonly no --disk-offload disable >"$SERVER_DIR/log" 2>&1 &
SERVER_PID=$!
_up=0
for _ in $(seq 1 100); do
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "FATAL: exited at startup" >&2; cat "$SERVER_DIR/log" >&2; exit 1; }
  [[ "$(timeout 2 redis-cli -p "$PORT" ping 2>/dev/null)" == "PONG" ]] && { _up=1; break; }
  sleep 0.1
done
(( _up )) || { echo "FATAL: never answered PING" >&2; cat "$SERVER_DIR/log" >&2; exit 1; }

inline_count() {
  curl -s --max-time 5 "http://127.0.0.1:$ADMIN_PORT/metrics" 2>/dev/null \
    | awk '/moon_dispatch_path_total.*local_inline/ {print $NF}' | head -1
}

# A user is `unrestricted` only when allowed_commands is AllAllowed (acl/table.rs
# recompute_unrestricted). `-debug` after `+@all` breaks that while leaving GET
# fully permitted, so the ONLY thing that changes is the dispatch path.
timeout 5 redis-cli -p "$PORT" acl setuser bench on '>benchpw' '~*' '&*' +@all -debug >/dev/null
if ! timeout 5 redis-cli -p "$PORT" -a benchpw --user bench --no-auth-warning get nosuchkey >/dev/null 2>&1; then
  echo "FATAL: restricted user cannot run GET -- the ACL rule is wrong." >&2; exit 1
fi

redis-benchmark -p "$PORT" -n 200000 -c 50 -P 16 -r "$KEYSPACE" -q \
  set key:__rand_int__ xxxxxxxx >/dev/null 2>&1
dbsize="$(timeout 5 redis-cli -p "$PORT" dbsize 2>/dev/null | tr -d '\r')"
(( dbsize > KEYSPACE / 4 )) || { echo "FATAL: dbsize=$dbsize -- keyspace never materialised" >&2; exit 1; }

cat <<EOF
# moon: sha256:$(sha256sum "$MOON_BIN" | cut -c1-16) ($MOON_BIN)
# redis-benchmark: $(redis-benchmark --version 2>/dev/null | head -1)
# cpu: $(awk -F': ' '/model name|Model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || lscpu | awk -F': +' '/Model name/{print $2; exit}') ($(nproc) cores)
# kernel: $(uname -sr)
# shards: 1 keyspace: $KEYSPACE dbsize: $dbsize reps: $REPS
# mode: $MODE command: $COMMAND
# date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')
leg,depth,rps,inline_before,inline_after,inline_delta
EOF

run() { # run <leg> <depth>
  local leg="$1" depth="$2" n before after rps delta
  case "$depth" in 8) n=400000 ;; 64) n=1500000 ;; esac
  before="$(inline_count)"
  # `unauth` legs run as the default (unrestricted) user; `auth` legs as `bench`.
  # --user REQUIRES -a on redis-benchmark. `--pass` is redis-cli only and
  # silently yields an empty leg -- which is how the first run of this
  # experiment "confirmed" a frozen counter while nothing had run at all.
  case "$leg" in
    inline|unrestricted) auth=() ;;
    generic|restricted)  auth=(-a benchpw --user bench) ;;
  esac
  # shellcheck disable=SC2086
  rps="$(redis-benchmark -p "$PORT" "${auth[@]}" -n "$n" -c 50 -P "$depth" -r "$KEYSPACE" -q \
         $COMMAND 2>/dev/null | tr '\r' '\n' \
         | awk '{for(i=1;i<=NF;i++) if($i=="requests"&&$(i+1)=="per"){v=$(i-1);gsub(/,/,"",v);print v;exit}}')"
  after="$(inline_count)"
  [[ -n "$rps" ]] || { echo "FATAL: $leg p=$depth produced NO rps -- refusing to record an empty leg." >&2; exit 1; }
  delta=$(( ${after:-0} - ${before:-0} ))
  case "$leg" in
    inline)
      (( delta > 0 )) || { echo "FATAL: inline leg did not advance local_inline -- it was not inline." >&2; exit 1; } ;;
    generic)
      (( delta == 0 )) || { echo "FATAL: generic leg advanced local_inline by $delta -- user still unrestricted." >&2; exit 1; } ;;
    unrestricted|restricted)
      # aclcost mode: the command must be inline-INELIGIBLE, so NEITHER leg may
      # advance the counter. If it does, the command was inline-eligible after
      # all and this measures a path difference, not the ACL check.
      (( delta == 0 )) || { echo "FATAL: $leg advanced local_inline by $delta -- '$COMMAND' is inline-eligible, so aclcost mode is measuring the wrong thing." >&2; exit 1; } ;;
  esac
  echo "$leg,$depth,$rps,$before,$after,$delta"
}

if [[ "$MODE" == tax ]]; then LEG_A=inline; LEG_B=generic
else                            LEG_A=unrestricted; LEG_B=restricted; fi

for rep in $(seq 1 "$REPS"); do
  if (( rep % 2 == 1 )); then legs=("$LEG_A" "$LEG_B"); else legs=("$LEG_B" "$LEG_A"); fi
  for leg in "${legs[@]}"; do for d in 8 64; do run "$leg" "$d"; done; done
done
