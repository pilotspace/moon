#!/usr/bin/env bash
# diag-write-attribution.sh — attribute Moon's disk WRITE bytes across persistence
# subsystems under a SET-heavy benchmark, and measure the write throughput tax.
#
# Answers the user's diagnosis: "moon takes heavy disk writes under a write benchmark,
# it makes a bottleneck — which subsystem is the dominant writer, and what does it cost?"
#
# Method (measure-only; nothing is changed):
#   RUN 1  STOCK moon (appendonly=yes, disk-offload=enable — the write-heavy default)
#          -> per-file `du` classified by subsystem (attribution)
#          -> /proc/diskstats device sectors_written delta (GROUND-TRUTH total bytes to device)
#          -> /proc/<pid>/io write_bytes (cross-check; may undercount io_uring io-wq writes)
#          -> write-amplification = device bytes / logical payload
#   RUN 2  FAIR moon (--appendonly no --disk-offload disable)
#          -> SET throughput, to size the persistence TAX vs RUN 1.
#
# MUST run on a Linux host with a REAL block-backed --dir (NOT tmpfs — RAM writes don't
# register as disk writes). OrbStack caveat: virtio fsync is near-free, so the throughput
# TAX here is a LOWER BOUND; the real-SSD stall needs bare-metal/GCloud.
set -uo pipefail

MOON_BIN="${MOON_BIN:?set MOON_BIN to the moon binary (ELF)}"
PORT="${PORT:-7601}"
SHARDS="${SHARDS:-1}"
REQUESTS="${REQUESTS:-500000}"
KEYSPACE="${KEYSPACE:-1000000}"
VALUE="${VALUE:-64}"              # redis-benchmark -d (value bytes)
CLIENTS="${CLIENTS:-50}"         # concurrency to push write volume
PIPELINE="${PIPELINE:-1}"
DIR_BASE="${DIR_BASE:-$HOME/moon-diag}"   # MUST be real block fs (btrfs/ext4), not tmpfs
SETTLE="${SETTLE:-3}"            # s to let async WAL/AOF flush drain post-bench
BENCH="${BENCH:-redis-benchmark}"
CLI="${CLI:-redis-cli}"
DEV="${DEV:-}"                   # block device basename for diskstats (auto from DIR_BASE)

log(){ printf '%s\n' "$*" >&2; }
die(){ log "FATAL: $*"; exit 1; }

command -v "$BENCH" >/dev/null || die "$BENCH not on PATH"
magic=$(od -An -tx1 -N4 "$MOON_BIN" | tr -d ' '); [[ "$magic" == "7f454c46" ]] || die "MOON_BIN not ELF (magic=$magic) — stale Mach-O?"
mkdir -p "$DIR_BASE"
fstype=$(stat -f -c %T "$DIR_BASE" 2>/dev/null || echo unknown)
[[ "$fstype" == "tmpfs" ]] && die "DIR_BASE $DIR_BASE is tmpfs (RAM) — disk writes won't register. Use a real block fs."
if [[ -z "$DEV" ]]; then
  src=$(df --output=source "$DIR_BASE" 2>/dev/null | tail -1); DEV=$(basename "$src")
fi
awk -v d="$DEV" '$3==d{f=1} END{exit !f}' /proc/diskstats || die "device '$DEV' not in /proc/diskstats"

diskstats_wbytes(){ awk -v d="$DEV" '$3==d{print $10*512; exit}' /proc/diskstats; }   # sectors_written*512
proc_wbytes(){ awk -F': ' '/^write_bytes/{print $2}' "/proc/$1/io" 2>/dev/null; }
proc_syscw(){  awk -F': ' '/^syscw/{print $2}'       "/proc/$1/io" 2>/dev/null; }

MOON_PID=""
cleanup(){ [[ -n "$MOON_PID" ]] && { kill "$MOON_PID" 2>/dev/null; wait "$MOON_PID" 2>/dev/null; MOON_PID=""; }; return 0; }
trap cleanup EXIT INT TERM

start_moon(){  # start_moon <dir> [extra moon args...]
  cleanup; local dir="$1"; shift
  rm -rf "$dir"; mkdir -p "$dir"
  "$MOON_BIN" --port "$PORT" --shards "$SHARDS" --dir "$dir" --admin-port 0 "$@" >/dev/null 2>&1 &
  MOON_PID=$!
  local deadline=$((SECONDS+15))
  until "$CLI" -p "$PORT" ping >/dev/null 2>&1; do
    kill -0 "$MOON_PID" 2>/dev/null || die "moon died on start (args: $*)"
    [[ $SECONDS -ge $deadline ]] && die "moon did not start (args: $*)"
    sleep 0.1
  done
}

set_bench_rps(){  # -> SET rps (writes real keys across KEYSPACE)
  "$BENCH" -p "$PORT" -c "$CLIENTS" -P "$PIPELINE" -n "$REQUESTS" -r "$KEYSPACE" -d "$VALUE" -t set --csv 2>/dev/null \
    | tr '\r' '\n' | grep '"SET"' | awk -F',' '{gsub(/"/,"",$2); printf "%.0f\n",$2}' | tail -1
}

classify(){  # classify <path> -> subsystem label (by filename)
  local p="${1,,}"
  case "$p" in
    *appendonly*|*/aof/*|*aof.*|*.aof) echo AOF ;;
    *wal*)                             echo WAL ;;
    *offload*|*cold*|*spill*|*warm*|*kv_page*|*page_cache*|*tiered*|*segment*) echo OFFLOAD ;;
    *snapshot*|*rdb*|*.dump|*dump.*|*checkpoint*|*clog*)                        echo SNAPSHOT ;;
    *manifest*)                        echo MANIFEST ;;
    *)                                 echo OTHER ;;
  esac
}

# ============================================================ RUN 1: STOCK + attribution
DIR_STOCK="$DIR_BASE/stock"
log "=== RUN 1: STOCK moon (appendonly=yes, disk-offload=enable) — attribution ==="
# MAXMEMORY (optional): set e.g. 128mb to force the disk-offload spill path (threshold 0.85)
# when the SET workload exceeds it — the Moon-specific "heavy write" scenario.
start_moon "$DIR_STOCK" ${MAXMEMORY:+--maxmemory "$MAXMEMORY"}
sync; ds0=$(diskstats_wbytes); io0=$(proc_wbytes "$MOON_PID"); sc0=$(proc_syscw "$MOON_PID")
stock_rps=$(set_bench_rps)
sleep "$SETTLE"; sync
ds1=$(diskstats_wbytes); io1=$(proc_wbytes "$MOON_PID"); sc1=$(proc_syscw "$MOON_PID")

log "--- files under $DIR_STOCK (subsystem  bytes  path) ---"
declare -A LBL
tot_files=0
while IFS= read -r -d '' f; do
  sz=$(du -b --apparent-size "$f" 2>/dev/null | awk '{print $1}'); sz="${sz:-0}"
  label=$(classify "$f")
  LBL[$label]=$(( ${LBL[$label]:-0} + sz ))
  tot_files=$(( tot_files + sz ))
  log "$(printf '    %-9s %12d  %s' "$label" "$sz" "${f#"$DIR_STOCK"/}")"
done < <(find "$DIR_STOCK" -type f -print0 2>/dev/null)
cleanup

dev_bytes=$(( ${ds1:-0} - ${ds0:-0} ))
pio_bytes=$(( ${io1:-0} - ${io0:-0} ))
syscw=$(( ${sc1:-0} - ${sc0:-0} ))
payload=$(( REQUESTS * (VALUE + 20) ))   # ~20B/key overhead estimate

# ============================================================ RUN 2: FAIR throughput
DIR_FAIR="$DIR_BASE/fair"
log "=== RUN 2: FAIR moon (--appendonly no --disk-offload disable) — throughput only ==="
start_moon "$DIR_FAIR" --appendonly no --disk-offload disable
fair_rps=$(set_bench_rps)
cleanup

# ============================================================ REPORT
amp=$(awk -v d="$dev_bytes" -v p="$payload" 'BEGIN{ if(p>0) printf "%.1fx", d/p; else print "n/a" }')
bpo=$(awk -v d="$dev_bytes" -v n="$REQUESTS" 'BEGIN{ if(n>0) printf "%.0f", d/n; else print "n/a" }')
tax=$(awk -v s="${stock_rps:-0}" -v f="${fair_rps:-0}" 'BEGIN{ if(f>0 && s>0) printf "%.1f%% slower (fair is %.2fx stock)", 100*(1-s/f), f/s; else print "n/a" }')

echo   "# ===== MOON WRITE-ATTRIBUTION DIAGNOSIS ====="
echo   "host:      $(uname -srm)   device: $DEV   dir-fs: $fstype"
echo   "moon:      $MOON_BIN  (shards=$SHARDS)"
echo   "workload:  SET -n $REQUESTS -r $KEYSPACE -d ${VALUE}B -c $CLIENTS -P $PIPELINE"
printf 'payload:   ~%d MiB logical (%d ops x ~%dB)\n' "$(( payload/1048576 ))" "$REQUESTS" "$(( VALUE+20 ))"
echo
printf '## bytes to device (diskstats %s): %d B  (%d MiB)\n' "$DEV" "$dev_bytes" "$(( dev_bytes/1048576 ))"
printf '## /proc/pid/io write_bytes:        %d B  (%d MiB)  [io_uring io-wq may undercount]\n' "$pio_bytes" "$(( pio_bytes/1048576 ))"
printf '## write syscalls (syscw):          %d  [io_uring submits via ring, not write(); undercounts under monoio]\n' "$syscw"
printf '## WRITE AMPLIFICATION:             %s  (device bytes / logical payload)\n' "$amp"
printf '## bytes per SET op:                %s\n' "$bpo"
echo
echo   "## per-subsystem file bytes (attribution by filename):"
for label in "${!LBL[@]}"; do printf '%s %d\n' "$label" "${LBL[$label]}"; done \
  | sort -k2 -rn \
  | awk -v T="$tot_files" '{ pct=(T>0?100*$2/T:0); printf "   %-9s %12d B  (%5.1f%%)\n", $1, $2, pct }'
printf '   %-9s %12d B  (total on-disk files)\n' "TOTAL" "$tot_files"
echo
echo   "## throughput tax (OrbStack fsync near-free -> LOWER BOUND):"
printf '   stock (AOF+offload on):  %s SET/s\n' "${stock_rps:-n/a}"
printf '   fair  (both off):        %s SET/s\n' "${fair_rps:-n/a}"
printf '   tax:                     %s\n' "$tax"
