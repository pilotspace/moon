#!/usr/bin/env bash
# Offline self-test for the memory gate's COMPARISON logic (moon#764).
#
# scripts/bench-memory-steady-state.sh already has a --self-test phase, but it
# costs a server boot + a 1M-key populate + a 60s settle, so it only ever runs
# inside the Memory steady-state job and only ever injects ONE mutation
# (dashtable +6%). This wrapper sources the script -- which is why the script
# has a lib-only `BASH_SOURCE` guard -- and drives compare_snapshot(),
# kind_threshold() and check_workload_ran() against synthetic snapshots. No
# server, no build, no network, ~1s.
#
# It exists because #764 was a gate that could not fail. Every case below is
# an assertion about the gate's ability to go RED; if this file passes while
# the gate is vacuous, this file is wrong.
#
# Cases:
#   1  identical snapshot vs baseline                 -> PASS
#   2  dashtable +6%                                  -> RED, GREW
#   3  rss -14.02% (the real #764 numbers)            -> RED, SHRANK
#   3a rss +6%   -- growth threshold NOT relaxed      -> RED, GREW
#   3b rss -8%   -- inside measured hosted noise      -> PASS
#   3c rss -11%  -- past the shrink floor             -> RED, SHRANK
#   4  #764's exact measured-vs-committed pair        -> RED (never a pass)
#   5  hnsw +15% (inside the measured noise floor)    -> PASS
#   6  hnsw +73% (>3x the floor)                      -> RED, GREW
#   7  allocator_overhead +30% (past its 25% floor)   -> RED, GREW
#   7a allocator_overhead -31% (rss-derived floor)    -> PASS
#   7b allocator_overhead -60%                        -> RED, SHRANK
#   8  empty-server snapshot                          -> check_workload_ran RC 2
#   9  populated snapshot                             -> check_workload_ran RC 0
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GATE="$ROOT/scripts/bench-memory-steady-state.sh"

[[ -f "$GATE" ]] || { echo "RED: gate script not found: $GATE" >&2; exit 1; }

for cmd in jq python3; do
    command -v "$cmd" &>/dev/null || { echo "SKIP: $cmd not on PATH" >&2; exit 0; }
done

# Sourcing must define functions and run NOTHING. If the lib-only guard ever
# regresses, this source boots a server and this test hangs -- which is a
# louder failure than a silent one.
# shellcheck disable=SC1090
source "$GATE"
set +e   # the gate sets -e; we deliberately call functions that return 1

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

PASSES=0
FAILS=0

# A snapshot shaped exactly like a real hosted-runner capture.
BASE_SNAPSHOT='{
  "platform": {"os":"Linux","arch":"x86_64","profile":"debug",
               "cpu_model":"AMD EPYC 7763 64-Core Processor","cpu_count":4,
               "mem_total_kb":16373444,"runner_environment":"github-hosted",
               "runner_name":"selftest"},
  "rss": 124432384,
  "kinds": {
    "dashtable":           {"doctor":96594821,"prom":96598476},
    "hnsw":                {"doctor":1205862,"prom":1203052},
    "csr":                 {"doctor":50298,"prom":50304},
    "wal":                 {"doctor":0,"prom":0},
    "sealed":              {"doctor":0,"prom":0},
    "replication_backlog": {"doctor":0,"prom":0},
    "allocator_overhead":  {"doctor":0,"prom":26408931}
  }
}'

# The baseline that was actually committed when #764's first real comparison
# ran: captured one commit BEFORE start_server() gained --memory-arenas-cap 2.
STALE_BASELINE='{
  "platform": {"os":"Linux","arch":"x86_64","profile":"debug",
               "cpu_model":"AMD EPYC 9V74 80-Core Processor","cpu_count":4,
               "mem_total_kb":16373452,"runner_environment":"github-hosted",
               "runner_name":"selftest"},
  "rss": 144723968,
  "kinds": {
    "dashtable":           {"doctor":96657735,"prom":96653601},
    "hnsw":                {"doctor":1205862,"prom":1203052},
    "csr":                 {"doctor":50298,"prom":50304},
    "wal":                 {"doctor":0,"prom":0},
    "sealed":              {"doctor":0,"prom":0},
    "replication_backlog": {"doctor":0,"prom":0},
    "allocator_overhead":  {"doctor":0,"prom":46612622}
  }
}'

# expect_case <name> <expected: pass|red> <expected-substring|-> <snapshot> <baseline-json>
expect_case() {
    local name="$1" expect="$2" needle="$3" snap="$4" base="$5"
    local bfile="$WORK/baseline.json" out rc
    echo "$base" > "$bfile"
    out="$(compare_snapshot "$snap" "$bfile" 5 2>&1)"
    rc=$?

    local ok=true
    if [[ "$expect" == "pass" && "$rc" -ne 0 ]]; then ok=false; fi
    if [[ "$expect" == "red"  && "$rc" -eq 0 ]]; then ok=false; fi
    if [[ "$needle" != "-" ]] && ! grep -qF -- "$needle" <<<"$out"; then ok=false; fi

    if [[ "$ok" == true ]]; then
        echo "  ok   $name (rc=$rc)"
        PASSES=$((PASSES + 1))
    else
        echo "  FAIL $name: expected=$expect rc=$rc needle='$needle'"
        echo "$out" | sed 's/^/       | /'
        FAILS=$((FAILS + 1))
    fi
}

mutate() { echo "$BASE_SNAPSHOT" | jq "$1"; }

echo "== compare_snapshot() =="
expect_case "1 identical -> PASS" pass \
    "ALL KINDS WITHIN TOLERANCE" "$BASE_SNAPSHOT" "$BASE_SNAPSHOT"

expect_case "2 dashtable +6% -> RED (GREW)" red \
    "FAIL (GREW):   dashtable" "$(mutate '.kinds.dashtable.prom = (.kinds.dashtable.prom * 1.06 | floor)')" "$BASE_SNAPSHOT"

expect_case "3 rss -14% (a STEP) -> RED (SHRANK)" red \
    "FAIL (SHRANK): rss" "$(mutate '.rss = (.rss * 0.8598 | floor)')" "$BASE_SNAPSHOT"

# The asymmetry, both halves. Growth detection must NOT have been relaxed to
# buy the shrink slack: a +6% RSS regression stays red at the 5% growth
# threshold while -8% (inside the measured hosted-runner noise) stays green.
expect_case "3a rss +6% -> RED (GREW), growth threshold NOT relaxed" red \
    "FAIL (GREW):   rss" "$(mutate '.rss = (.rss * 1.06 | floor)')" "$BASE_SNAPSHOT"

expect_case "3b rss -8% (inside measured noise) -> PASS" pass \
    "ALL KINDS WITHIN TOLERANCE" "$(mutate '.rss = (.rss * 0.92 | floor)')" "$BASE_SNAPSHOT"

expect_case "3c rss -11% (past the shrink floor) -> RED (SHRANK)" red \
    "FAIL (SHRANK): rss" "$(mutate '.rss = (.rss * 0.89 | floor)')" "$BASE_SNAPSHOT"

expect_case "4 #764 measured vs stale baseline -> RED, never a pass" red \
    "BASELINE NO LONGER DESCRIBES THIS BUILD" "$BASE_SNAPSHOT" "$STALE_BASELINE"

expect_case "5 hnsw +15% (inside 20% floor) -> PASS" pass \
    "ALL KINDS WITHIN TOLERANCE" "$(mutate '.kinds.hnsw.prom = (.kinds.hnsw.prom * 1.15 | floor)')" "$BASE_SNAPSHOT"

expect_case "6 hnsw +73% -> RED (GREW)" red \
    "FAIL (GREW):   hnsw" "$(mutate '.kinds.hnsw.prom = (.kinds.hnsw.prom * 1.73 | floor)')" "$BASE_SNAPSHOT"

expect_case "7 allocator_overhead +30% -> RED (GREW)" red \
    "FAIL (GREW):   allocator_overhead" "$(mutate '.kinds.allocator_overhead.prom = (.kinds.allocator_overhead.prom * 1.30 | floor)')" "$BASE_SNAPSHOT"

# allocator_overhead is rss - tracked_sum, so it carries all of rss's
# absolute jitter at ~23% of its magnitude. Its shrink floor is derived from
# rss's own allowance (~47% for this snapshot), NOT from the 25% growth
# floor: a real run with rss at -7.26% -- inside the shrink limit -- carried
# allocator_overhead at -30.83% and would otherwise turn that green
# whole-process measurement red.
expect_case "7a allocator_overhead -31% (rss-derived floor) -> PASS" pass \
    "shrink limit" "$(mutate '.kinds.allocator_overhead.prom = (.kinds.allocator_overhead.prom * 0.69 | floor)')" "$BASE_SNAPSHOT"

expect_case "7b allocator_overhead -60% (past the derived floor) -> RED (SHRANK)" red \
    "FAIL (SHRANK): allocator_overhead" "$(mutate '.kinds.allocator_overhead.prom = (.kinds.allocator_overhead.prom * 0.40 | floor)')" "$BASE_SNAPSHOT"

# The derivation itself: never tighter than rss's absolute allowance, never
# looser than the kind's own measured floor.
d=$(ao_shrink_threshold 124432384 26408931 25)
if python3 -c "import sys; sys.exit(0 if abs($d - 47.12) < 0.1 else 1)"; then
    echo "  ok   ao_shrink_threshold(rss=124432384, ao=26408931) = $d (rss allowance in ao's units)"
    PASSES=$((PASSES + 1))
else
    echo "  FAIL ao_shrink_threshold = $d, want ~47.12"
    FAILS=$((FAILS + 1))
fi
d=$(ao_shrink_threshold 124432384 900000000 25)
if [[ "$d" == "25" ]]; then
    echo "  ok   ao_shrink_threshold floors at the kind's own 25% when rss's allowance is smaller"
    PASSES=$((PASSES + 1))
else
    echo "  FAIL ao_shrink_threshold did not floor at 25: $d"
    FAILS=$((FAILS + 1))
fi

# Fail closed: a snapshot that cannot be evaluated is not a pass.
expect_case "10 unparseable snapshot -> RED (UNEVALUATED), never OK" red \
    "FAIL (UNEVALUATED)" "$(mutate '.rss = null | .kinds.dashtable.prom = null')" "$BASE_SNAPSHOT"

echo "== kind_threshold() =="
for spec in "dashtable 5" "rss_unknown_kind 5" "hnsw 20" "allocator_overhead 25"; do
    set -- $spec
    got="$(kind_threshold "$1" 5)"
    if [[ "$got" == "$2" ]]; then
        echo "  ok   kind_threshold($1)=$got"
        PASSES=$((PASSES + 1))
    else
        echo "  FAIL kind_threshold($1)=$got, want $2"
        FAILS=$((FAILS + 1))
    fi
done

echo "== check_workload_ran() =="
EMPTY_SNAPSHOT="$(mutate '.kinds.dashtable.prom = 131072 | .kinds.hnsw.prom = 0 | .kinds.csr.prom = 0 | .rss = 12000000')"
out="$(check_workload_ran "$EMPTY_SNAPSHOT" 2>&1)"; rc=$?
if [[ "$rc" -eq 2 ]] && grep -qF "MEASUREMENT VOID" <<<"$out"; then
    echo "  ok   8 empty server -> rc=2 MEASUREMENT VOID"
    PASSES=$((PASSES + 1))
else
    echo "  FAIL 8 empty server: rc=$rc"; echo "$out" | sed 's/^/       | /'
    FAILS=$((FAILS + 1))
fi

out="$(check_workload_ran "$BASE_SNAPSHOT" 2>&1)"; rc=$?
if [[ "$rc" -eq 0 ]]; then
    echo "  ok   9 populated server -> rc=0"
    PASSES=$((PASSES + 1))
else
    echo "  FAIL 9 populated server: rc=$rc"; echo "$out" | sed 's/^/       | /'
    FAILS=$((FAILS + 1))
fi

echo ""
if [[ "$FAILS" -gt 0 ]]; then
    echo "=== memory-gate comparison self-test: $FAILS FAILED, $PASSES passed ==="
    exit 1
fi
echo "=== memory-gate comparison self-test: all $PASSES checks passed ==="
