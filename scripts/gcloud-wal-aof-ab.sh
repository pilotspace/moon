#!/usr/bin/env bash
# gcloud-wal-aof-ab.sh — same-instance A/B proof of the write-path durability fixes
# (--wal-kv-log gate + everysec bounded backpressure) on GCloud real SSD.
#
# Proves, per config {--wal-kv-log on (old behavior) | auto (fix)} at shards={1,4},
# appendonly=yes everysec (defaults):
#   1. VOLUME     — on-disk bytes attributed AOF vs WAL (+ /proc/<pid>/io write_bytes).
#                   Expect: auto ==> WAL header-only at shards=4 (~44% total cut); shards=1 unchanged.
#   2. THROUGHPUT — SET RPS, P1 & P64, c50, median of 3 reps (fresh server per rep).
#                   Expect: no regression (OrbStack showed the tax is flat; real SSD may show a win).
#   3. RECOVERY   — auto @ shards=4: write N keys, kill -9, restart, DBSIZE must equal N
#                   (AOF alone recovers everything with the WAL gate active).
#   4. BACKPRESSURE — INFO aof_backpressure_dropped must stay 0 in every run
#                   (bounded-backpressure path never engages on a healthy disk).
#
# Measure-only: never edits src. Patterned on gcloud-kv-scale-bench.sh (provision/
# scp/measure/teardown). The instance builds the LOCAL source snapshot (scp'd
# tarball via `git archive`), NOT the GitHub main — the fix is unpushed.
#
# Subcommands:
#   --measure   INNER: runs ON the instance (expects ~/moon-src + toolchain)
#   --one       OUTER (default): provision $GCE_MACHINE, run, teardown
set -uo pipefail

GCE_MACHINE="${GCE_MACHINE:-c3-standard-4}"
GCE_NAME="${GCE_NAME:-moon-wal-ab}"
GCE_ZONE="${GCE_ZONE:-us-central1-a}"
REQUESTS="${REQUESTS:-500000}"
KEYSPACE="${KEYSPACE:-1000000}"
REPS="${REPS:-3}"
PORT="${PORT:-6399}"

log(){ printf '%s\n' "$*" >&2; }
die(){ log "FATAL: $*"; exit 1; }

# ============================================================================ INNER
wait_port(){ # host port tries — every probe timeout-bounded (a half-dead server
  # that accepts but never replies must not hang the harness)
  local t=0; until timeout 2 redis-cli -p "$2" ping 2>/dev/null | grep -q PONG; do
    t=$((t+1)); [[ $t -ge 100 ]] && return 1; sleep 0.2
  done
}

stop_moon(){ # $1=pid — SIGKILL + pattern backstop + wait-for-port-free.
  # Moon handles SIGTERM gracefully and can linger; SO_REUSEPORT then lets the
  # NEXT server bind the same port alongside the old one (two servers, split
  # traffic, hung pings). kill -9 is the house convention (see CLAUDE.md).
  kill -9 "$1" 2>/dev/null; wait "$1" 2>/dev/null
  pkill -9 -f "release/moon --port $PORT" 2>/dev/null
  local t=0
  while pgrep -f "release/moon --port $PORT" >/dev/null 2>&1; do
    t=$((t+1)); [[ $t -ge 50 ]] && { log "WARN: moon refused to die"; return 1; }; sleep 0.2
  done
  return 0
}

rps_of(){ # extract last "requests per second" (redis-benchmark 8.x \r progress trap)
  tr '\r' '\n' | grep -i "requests per second" | tail -1 | grep -oE '[0-9.]+' | head -1
}

dir_bytes(){ # $1=dir  $2=glob-ere  -> total bytes of matching files
  find "$1" -type f 2>/dev/null | grep -E "$2" | xargs -r stat -c '%s' | awk '{s+=$1} END{print s+0}'
}

start_moon(){ # $1=shards $2=walkv $3=dir -> pid
  ./moon-src/target/release/moon --port "$PORT" --shards "$1" \
    --appendonly yes --appendfsync everysec --wal-kv-log "$2" \
    --dir "$3" >"$3/server.log" 2>&1 &
  echo $!
}

measure_one(){ # $1=shards $2=walkv  -> prints a report block
  local shards="$1" walkv="$2" dir pid rps_p1=() rps_p64=() r
  echo "### shards=$shards wal-kv-log=$walkv"

  # -- volume run (single, fixed workload) --
  dir=$(mktemp -d /ssd-bench/walab.XXXX)
  pid=$(start_moon "$shards" "$walkv" "$dir")
  wait_port localhost "$PORT" || { cat "$dir/server.log"; die "moon never accepted (volume run)"; }
  redis-benchmark -p "$PORT" -t set -n "$REQUESTS" -r "$KEYSPACE" -d 64 -c 50 -P 1 -q >/dev/null 2>&1
  timeout 5 redis-cli -p "$PORT" set MARKERKEY_ZZZ9 MARKERVALUE_QQQ8 >/dev/null
  sleep 3   # let 1ms-tick flush + everysec fsync settle
  local aof_b wal_b pio drops
  aof_b=$(dir_bytes "$dir" 'appendonly.*\.aof$|appendonlydir/.*\.aof$')
  wal_b=$(dir_bytes "$dir" '\.wal$')
  pio=$(awk '/^write_bytes/{print $2}' "/proc/$pid/io" 2>/dev/null || echo 0)
  drops=$(timeout 5 redis-cli -p "$PORT" info persistence 2>/dev/null | tr -d '\r' | awk -F: '/aof_backpressure_dropped/{print $2}')
  local marker_wal=0
  if find "$dir" -name '*.wal' | head -5 | xargs -r strings 2>/dev/null | grep -q MARKERVALUE_QQQ8; then marker_wal=1; fi
  echo "volume: aof_bytes=$aof_b wal_bytes=$wal_b proc_write_bytes=$pio backpressure_dropped=${drops:-NA} marker_in_wal=$marker_wal"

  # -- recovery check (auto only, shards=4: prove AOF-alone recovery) --
  if [[ "$walkv" == "auto" && "$shards" == "4" ]]; then
    local nkeys; nkeys=$(timeout 5 redis-cli -p "$PORT" dbsize | grep -oE '[0-9]+')
    stop_moon "$pid"
    pid=$(start_moon "$shards" "$walkv" "$dir")
    wait_port localhost "$PORT" || die "moon never accepted (recovery run)"
    local nkeys2 marker2
    nkeys2=$(timeout 5 redis-cli -p "$PORT" dbsize | grep -oE '[0-9]+')
    marker2=$(timeout 5 redis-cli -p "$PORT" get MARKERKEY_ZZZ9)
    echo "recovery: keys_before_kill9=$nkeys keys_after_restart=$nkeys2 marker=$marker2"
  fi
  stop_moon "$pid"
  rm -rf "$dir"

  # -- throughput reps (fresh server per rep; phantom-regression rule) --
  for r in $(seq 1 "$REPS"); do
    dir=$(mktemp -d /ssd-bench/walab.XXXX)
    pid=$(start_moon "$shards" "$walkv" "$dir")
    wait_port localhost "$PORT" || die "moon never accepted (rep $r)"
    rps_p1+=("$(redis-benchmark -p "$PORT" -t set -n "$REQUESTS" -r "$KEYSPACE" -d 64 -c 50 -P 1 2>/dev/null | rps_of)")
    rps_p64+=("$(redis-benchmark -p "$PORT" -t set -n "$REQUESTS" -r "$KEYSPACE" -d 64 -c 50 -P 64 2>/dev/null | rps_of)")
    drops=$(timeout 5 redis-cli -p "$PORT" info persistence 2>/dev/null | tr -d '\r' | awk -F: '/aof_backpressure_dropped/{print $2}')
    stop_moon "$pid"
    rm -rf "$dir"
    echo "rep$r: P1=${rps_p1[-1]:-NA} P64=${rps_p64[-1]:-NA} drops=${drops:-NA}"
  done
  echo "throughput: P1=[${rps_p1[*]}] P64=[${rps_p64[*]}]"
  echo
}

measure(){
  set -e
  sudo mkdir -p /ssd-bench && sudo chmod 777 /ssd-bench
  pkill -9 -f "release/moon --port $PORT" 2>/dev/null; rm -rf /ssd-bench/walab.* 2>/dev/null
  command -v redis-benchmark >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq redis-tools; }
  # steal gate (dedicated vCPU expected ~0)
  awk '/^cpu /{print "cpu-steal-jiffies:", $9}' /proc/stat
  echo "# ===== WAL/AOF A/B ($(uname -m), $(date -u +%FT%TZ)) ====="
  echo "# moon: $(./moon-src/target/release/moon --version 2>/dev/null || echo unknown)"
  set +e
  for shards in 1 4; do
    for walkv in on auto; do
      measure_one "$shards" "$walkv"
    done
  done
  echo "# ===== END ====="
}

# ============================================================================ OUTER
run_one(){
  command -v gcloud >/dev/null || die "gcloud CLI not found"
  local imgfam
  case "$GCE_MACHINE" in c4a-*|t2a-*) imgfam=ubuntu-2404-lts-arm64;; *) imgfam=ubuntu-2404-lts-amd64;; esac
  local GSSH=(gcloud compute ssh "$GCE_NAME" --zone="$GCE_ZONE" --quiet
        --ssh-flag=-oStrictHostKeyChecking=no --ssh-flag=-oConnectTimeout=15)

  log "=== packing LOCAL source (HEAD commit) ==="
  git -C "$(dirname "$0")/.." archive --format=tar.gz -o /tmp/moon-src.tar.gz --prefix=moon-src/ HEAD \
    || die "git archive failed (commit your changes first — the A/B ships HEAD)"

  log "=== provisioning $GCE_NAME ($GCE_MACHINE, $GCE_ZONE, $imgfam, pd-ssd) ==="
  gcloud compute instances create "$GCE_NAME" \
    --machine-type="$GCE_MACHINE" --zone="$GCE_ZONE" \
    --image-family="$imgfam" --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB --boot-disk-type=pd-ssd --quiet || die "instance create failed"
  local _inst="$GCE_NAME" _zone="$GCE_ZONE"
  trap "log '=== tearing down $_inst ==='; gcloud compute instances delete '$_inst' --zone='$_zone' -q 2>/dev/null || true" EXIT INT TERM

  log "=== waiting for SSH ==="
  local tries=0
  until "${GSSH[@]}" --command='echo up' >/dev/null 2>&1; do
    tries=$((tries+1)); [[ $tries -ge 40 ]] && die "SSH never came up"; sleep 5
  done

  log "=== toolchain ==="
  "${GSSH[@]}" --command='
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config libssl-dev git curl ca-certificates redis-tools
    command -v cargo >/dev/null || curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1
  ' || die "toolchain provisioning failed"

  log "=== pushing source + harness ==="
  gcloud compute scp /tmp/moon-src.tar.gz "$(dirname "$0")/gcloud-wal-aof-ab.sh" \
    "$GCE_NAME":~/ --zone="$GCE_ZONE" --quiet --scp-flag=-oStrictHostKeyChecking=no || die "scp failed"

  log "=== building release on instance (the long part) ==="
  "${GSSH[@]}" --command='
    set -e; source $HOME/.cargo/env
    tar xzf moon-src.tar.gz
    cd moon-src && cargo build --release 2>&1 | tail -3
    file target/release/moon | grep -q ELF || { echo "NOT AN ELF"; exit 1; }
  ' || die "build failed"

  local rawfile="tmp/wal-ab-${GCE_MACHINE}.txt"; mkdir -p tmp
  log "=== running --measure -> $rawfile ==="
  "${GSSH[@]}" --command="
    source \$HOME/.cargo/env
    REQUESTS='$REQUESTS' KEYSPACE='$KEYSPACE' REPS='$REPS' PORT='$PORT' \
      bash ~/gcloud-wal-aof-ab.sh --measure
  " | tee "$rawfile"
  log "=== results in $rawfile; teardown follows (trap) ==="
}

measure_single(){ # $1=shards $2=walkv — one config only (chunked outer driving)
  set +e
  sudo mkdir -p /ssd-bench && sudo chmod 777 /ssd-bench
  pkill -9 -f "release/moon --port $PORT" 2>/dev/null; rm -rf /ssd-bench/walab.* 2>/dev/null
  command -v redis-benchmark >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq redis-tools; }
  awk '/^cpu /{print "cpu-steal-jiffies:", $9}' /proc/stat
  measure_one "$1" "$2"
}

case "${1:---one}" in
  --measure) measure ;;
  --measure-one) measure_single "$2" "$3" ;;
  --one)     run_one ;;
  *) die "unknown subcommand: $1 (use --measure | --one)" ;;
esac
