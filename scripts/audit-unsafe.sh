#!/usr/bin/env bash
# audit-unsafe.sh — Enforce SAFETY comment on every unsafe block.
#
# Exit code 0 if all unsafe blocks have a preceding // SAFETY: comment.
# Exit code 1 if any are missing, with a report of violations.
#
# Run: ./scripts/audit-unsafe.sh
# CI:  added to .github/workflows/ci.yml

set -euo pipefail

VIOLATIONS=0
TOTAL=0

while IFS= read -r line; do
    file=$(echo "$line" | cut -d: -f1)
    lineno=$(echo "$line" | cut -d: -f2)
    TOTAL=$((TOTAL + 1))

    # Check 3 preceding lines for // SAFETY:
    found=0
    for offset in 1 2 3; do
        prev=$((lineno - offset))
        if [ "$prev" -gt 0 ]; then
            if sed -n "${prev}p" "$file" | grep -q "SAFETY:"; then
                found=1
                break
            fi
        fi
    done

    if [ "$found" -eq 0 ]; then
        echo "MISSING SAFETY: $file:$lineno"
        VIOLATIONS=$((VIOLATIONS + 1))
    fi
done < <(grep -rn "unsafe {" src --include='*.rs')

echo ""
echo "=== Unsafe Audit ==="
echo "Total unsafe blocks: $TOTAL"
echo "With SAFETY comment: $((TOTAL - VIOLATIONS))"
echo "Missing SAFETY:      $VIOLATIONS"

if [ "$VIOLATIONS" -gt 0 ]; then
    echo ""
    echo "FAILED: $VIOLATIONS unsafe blocks are missing // SAFETY: comments."
    echo "Add a // SAFETY: comment on the line(s) immediately above each unsafe block"
    echo "explaining which invariant makes the unsafe operation sound."
    exit 1
else
    echo ""
    echo "PASSED: All unsafe blocks have SAFETY comments."
    exit 0
fi
