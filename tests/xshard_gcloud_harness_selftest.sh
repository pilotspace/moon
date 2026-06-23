#!/usr/bin/env bash
# Red wrapper for task `xshard-read-gcloud-validation` (§4 HARNESS GATE-LOGIC).
#
# Asserts the GCloud-xshard absolute harness exists and its built-in `--self-test`
# passes. The self-test runs the harness validity GATE against synthetic inputs and
# proves it VOIDs correctly (no real GCloud instance, no cost):
#   - high loadavg (>= 0.7)          -> VOID contended_instrument   (xrgv-r1)
#   - planted moon busy-poller       -> refuse  dirty_instrument    (xrgv-r2)
#   - non-flat s1-LOCAL control      -> VOID contended_instrument   (xrgv-r1)
#   - single-rep / no best-of-N floor-> reject unstable_absolute    (xrgv-r3)
#   - harness present + executable, declares the 4 cells + 4 gates  (xrgv1)
#
# RED until §5 BUILD creates `scripts/gcloud-xshard-absolute.sh` with a passing `--self-test`.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS="$ROOT/scripts/gcloud-xshard-absolute.sh"

if [[ ! -x "$HARNESS" ]]; then
  echo "RED: harness not found or not executable: $HARNESS" >&2
  echo "     (expected — §5 BUILD creates it; this wrapper goes green then)" >&2
  exit 1
fi

# Delegate to the harness's own gate self-test (no GCloud provisioning).
exec "$HARNESS" --self-test
