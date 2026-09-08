#!/usr/bin/env python3
"""Reduce bench-ab-matrix.sh CSV to a ratio table with an explicit noise floor.

A ratio is only signal when it clears the floor. The floor for a row is the
worst within-leg coefficient of variation across the two series being compared
(moon and redis) -- if the control wobbles more than the difference, there is
no difference to report (BENCHMARK.md §2.11).
"""
import csv
import statistics
import sys
from collections import defaultdict


def cv(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return float("nan")
    m = statistics.mean(xs)
    return statistics.stdev(xs) / m if m else float("nan")


def main(path):
    rows = defaultdict(list)  # (family, depth, engine) -> [rps]
    header = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                header.append(line.rstrip())
                continue
            break
        rdr = csv.DictReader([line] + fh.readlines())
        for r in rdr:
            if r["rps"] in ("NA", "", None):
                continue
            rows[(r["family"], int(r["depth"]), r["engine"])].append(float(r["rps"]))

    print("\n".join(header))
    families, depths = [], []
    for (fam, depth, _eng) in rows:
        if fam not in families:
            families.append(fam)
        if depth not in depths:
            depths.append(depth)
    depths.sort()

    print()
    print("| command | " + " | ".join(f"p={d}" for d in depths) + " |")
    print("|---------|" + "|".join([":---:"] * len(depths)) + "|")
    detail = []
    for fam in families:
        cells = []
        for d in depths:
            moon = rows.get((fam, d, "moon"), [])
            redis = rows.get((fam, d, "redis"), [])
            if not moon or not redis:
                cells.append("—")
                continue
            mm, mr = statistics.median(moon), statistics.median(redis)
            ratio = mm / mr if mr else float("nan")
            floor = max(cv(moon), cv(redis))
            # A ratio within one floor-width of 1.0 is a tie, not a result.
            marker = "*" if abs(ratio - 1.0) <= floor else ""
            cells.append(f"{ratio:.2f}x{marker}")
            detail.append(
                (fam, d, mm, mr, ratio, floor, cv(moon), cv(redis), len(moon), len(redis))
            )
        print(f"| {fam} | " + " | ".join(cells) + " |")
    print()
    print("`*` = within the noise floor for that row (a tie, not a win or a loss).")
    print()
    print("| command | p | moon rps | redis rps | ratio | floor | CV moon | CV redis | n |")
    print("|---|---:|---:|---:|:---:|:---:|:---:|:---:|:---:|")
    for fam, d, mm, mr, ratio, floor, cm, cr, nm, nr in detail:
        print(
            f"| {fam} | {d} | {mm:,.0f} | {mr:,.0f} | {ratio:.3f}x | "
            f"{floor*100:.1f}% | {cm*100:.1f}% | {cr*100:.1f}% | {min(nm,nr)} |"
        )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin")
