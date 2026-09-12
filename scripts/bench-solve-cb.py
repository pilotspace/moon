#!/usr/bin/env python3
"""Solve the per-command term C and per-batch term B from a bench-ab-matrix CSV.

Throughput is modelled as one batch costing `B + C*p` at pipeline depth p, so
per-op cost is `C + B/p`.  With the p=8 and p=64 points that is two equations in
two unknowns:

    C = us64 - (us8 - us64)/7
    B = 64 * (us8 - us64) / 7

It is EXACTLY DETERMINED -- no residual, no goodness of fit.  This is a
decomposition under an assumed model, not a fit, and a two-point solve inherits
both points' noise.  Read every C as a band, not a figure.

Why this exists: BENCHMARK.md's per-family "required cut" numbers are C_moon -
C_redis, and on 2026-09-12 a published cut table turned out to have been solved
from a dataset twelve commits stale (`ae6cd003`, pre-#861), which inverted a
conclusion.  Re-deriving from a named CSV is now one command.
"""
import csv
import statistics
import sys
from collections import defaultdict


def load(path):
    rows = defaultdict(list)
    with open(path) as fh:
        lines = [ln for ln in fh if not ln.startswith("#")]
    for r in csv.DictReader(lines):
        if r.get("rps") in ("NA", "", None):
            continue
        rows[(r["family"], int(r["depth"]), r["engine"])].append(float(r["rps"]))
    return rows


def solve(rows, fam, eng):
    a, b = rows.get((fam, 8, eng), []), rows.get((fam, 64, eng), [])
    if len(a) < 2 or len(b) < 2:
        return None
    r8, r64 = statistics.median(a), statistics.median(b)
    if not r8 or not r64:
        return None
    us8, us64 = 1e6 / r8, 1e6 / r64
    cv = max(statistics.stdev(a) / statistics.mean(a),
             statistics.stdev(b) / statistics.mean(b))
    return us64 - (us8 - us64) / 7, 64 * (us8 - us64) / 7, cv


def main(path):
    rows = load(path)
    print("".join(ln for ln in open(path) if ln.startswith("#")), end="")
    fams = []
    for (f, _d, _e) in rows:
        if f not in fams:
            fams.append(f)
    print(f"\n| family | C moon | C redis | cut | B moon | worst CV |")
    print("|---|---:|---:|---:|---:|:---:|")
    for f in fams:
        m, r = solve(rows, f, "moon"), solve(rows, f, "redis")
        if not m or not r:
            print(f"| {f} | INCOMPLETE | | | | |")
            continue
        print(f"| {f} | {m[0]:.3f} | {r[0]:.3f} | **{m[0]-r[0]:+.3f}** | "
              f"{m[1]:.2f} | {max(m[2], r[2])*100:.1f}% |")
    print("\n`cut` = how much moon must shed per command to reach Redis parity. "
          "Negative means moon is already ahead.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin")
