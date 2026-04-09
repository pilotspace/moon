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

BASELINE=0  # Target: zero un-annotated unwrap/expect in hot-path modules

COUNT=0
for mod in src/protocol src/command src/shard src/storage src/persistence src/server; do
    # Count .unwrap()/.expect() NOT in test code, NOT preceded by #[allow
    while IFS= read -r line; do
        file=$(echo "$line" | cut -d: -f1)
        lineno=$(echo "$line" | cut -d: -f2)

        # Skip files that are test-only modules (e.g., tests.rs included via #[cfg(test)] mod tests;)
        basename=$(basename "$file")
        if [ "$basename" = "tests.rs" ]; then
            # Check if the parent mod.rs includes this via #[cfg(test)]
            dir=$(dirname "$file")
            parent_mod="$dir/mod.rs"
            if [ -f "$parent_mod" ] && grep -q '#\[cfg.*test.*\]' "$parent_mod" 2>/dev/null && grep -q 'mod tests' "$parent_mod" 2>/dev/null; then
                continue
            fi
        fi

        # Check if we're inside a #[cfg(test)] module
        # Simple heuristic: if line number > first #[cfg(test)] in file, skip
        test_start=$(grep -n '#\[cfg(test)\]' "$file" 2>/dev/null | head -1 | cut -d: -f1 || true)
        if [ -n "$test_start" ] && [ "$lineno" -gt "$test_start" ]; then
            continue
        fi

        # Skip comment-only lines (// or ///)
        actual_line=$(sed -n "${lineno}p" "$file" 2>/dev/null)
        stripped=$(echo "$actual_line" | sed 's/^[[:space:]]*//')
        if echo "$stripped" | grep -q '^//'; then
            continue
        fi

        # Check preceding 30 lines for #[allow — covers function-level annotations
        start=$((lineno - 30))
        if [ "$start" -lt 1 ]; then start=1; fi
        if sed -n "${start},${lineno}p" "$file" 2>/dev/null | grep -q '#\[allow.*clippy::unwrap_used\|#\[allow.*clippy::expect_used'; then
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
