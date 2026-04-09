#!/usr/bin/env bash
# audit-unwrap.sh — Ratchet for unwrap/expect in hot-path modules.
#
# Counts non-test, non-allowed .unwrap()/.expect() calls in the 6 hot-path modules.
# Fails if the count exceeds the committed baseline.
# Run: ./scripts/audit-unwrap.sh
#
# The baseline is the count AFTER Phase 90 annotation — every remaining call
# has a #[allow(clippy::unwrap_used)] with an explanation. New unwraps without
# an allow annotation will increase the count and fail this check.

set -euo pipefail

BASELINE=98  # Accurate count after fixing set -e bug in script. Includes function-level #[allow] not detected by line grep + split submodule files without #[cfg(test)]. Target: 0

COUNT=0
for mod in src/protocol src/command src/shard src/storage src/persistence src/server; do
    # Count .unwrap()/.expect() NOT in test code, NOT preceded by #[allow
    while IFS= read -r line; do
        file=$(echo "$line" | cut -d: -f1)
        lineno=$(echo "$line" | cut -d: -f2)
        # Check if preceding line has #[allow
        prev=$((lineno - 1))
        prev2=$((lineno - 2))
        if sed -n "${prev}p;${prev2}p" "$file" 2>/dev/null | grep -q '#\[allow'; then
            continue
        fi
        # Check if we're inside a #[cfg(test)] module
        # Simple heuristic: if line number > first #[cfg(test)] in file, skip
        test_start=$(grep -n '#\[cfg(test)\]' "$file" 2>/dev/null | head -1 | cut -d: -f1 || true)
        if [ -n "$test_start" ] && [ "$lineno" -gt "$test_start" ]; then
            continue
        fi
        COUNT=$((COUNT + 1))
        echo "  UNANNOTATED: $file:$lineno"
    done < <(grep -rn '\.unwrap()\|\.expect(' "$mod" --include='*.rs' 2>/dev/null || true)
done

echo ""
echo "=== Unwrap Ratchet ==="
echo "Un-annotated unwrap/expect in hot-path modules: $COUNT"
echo "Baseline: $BASELINE"

if [ "$COUNT" -gt "$BASELINE" ]; then
    echo ""
    echo "FAILED: $COUNT un-annotated unwrap/expect calls exceed baseline ($BASELINE)."
    echo "Either fix the call site or add #[allow(clippy::unwrap_used)] with a justification."
    exit 1
else
    echo ""
    echo "PASSED: Unwrap count within baseline."
    exit 0
fi
