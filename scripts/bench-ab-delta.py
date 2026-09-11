#!/usr/bin/env python3
"""Compare moon against moon across two bench-ab-matrix.sh runs.

bench-ab-report.py answers "is moon faster than Redis in this run". It cannot
answer "did this commit regress moon", because that is a comparison BETWEEN
runs -- and between runs the host is free to drift. Redis is unchanged code
benchmarked in both runs, so it is the control: if Redis moved as far as moon
did, the moon delta is drift and the ratio column is the only readable one.

Usage:
  bench-ab-delta.py --head A1.csv[,A2.csv] --base B1.csv[,B2.csv] [--label-head X --label-base Y]

Emits, per (family, depth):
  raw    -- median moon rps each side, delta%, and the noise floor those two
            series support (worst within-side CV).
  ratio  -- moon/redis each side, and the delta of the ratios. Immune to a
            control that moved.
  control-- Redis median each side and its own drift, so the reader can see
            WHY raw and ratio disagree when they do.

A delta inside its floor is printed as `tie`, never as a direction.
"""
import argparse
import csv
import statistics
import sys
from collections import defaultdict


# Above this, Redis's own series is too scattered to divide by. Set from the
# observed split: stable control rows in this harness sit at 0.3-3%, while the
# two bimodal rows that manufactured a false +13% sat at 7%+.
CONTROL_CV_LIMIT = 0.05


def cv(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return float("nan")
    m = statistics.mean(xs)
    return statistics.stdev(xs) / m if m else float("nan")


def load(paths):
    """Pool every pass for one binary into (family, depth, engine) -> [rps]."""
    rows = defaultdict(list)
    headers = []
    for path in paths:
        with open(path) as fh:
            lines = fh.readlines()
        body = []
        for line in lines:
            if line.startswith("#"):
                headers.append(line.rstrip())
                continue
            body.append(line)
        for r in csv.DictReader(body):
            if r["rps"] in ("NA", "", None):
                # A lost sample shortens one series silently. Count it so an
                # incomplete cell can be suppressed rather than averaged.
                rows[(r["family"], int(r["depth"]), r["engine"], "NA")].append(1)
                continue
            rows[(r["family"], int(r["depth"]), r["engine"])].append(float(r["rps"]))
    return rows, headers


def pct(x):
    return f"{x*100:+.1f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--head", required=True)
    ap.add_argument("--base", required=True)
    ap.add_argument("--label-head", default="head")
    ap.add_argument("--label-base", default="base")
    a = ap.parse_args()

    head, hh = load(a.head.split(","))
    base, bh = load(a.base.split(","))

    print(f"# head: {a.label_head}  <- {a.head}")
    for h in hh:
        print(f"#   {h}")
    print(f"# base: {a.label_base}  <- {a.base}")
    for h in bh:
        print(f"#   {h}")
    print()

    fams, depths = [], set()
    for (f, d, e, *rest) in list(head) + list(base):
        if rest:
            continue
        if f not in fams:
            fams.append(f)
        depths.add(d)
    depths = sorted(depths)

    print(f"| family | p | {a.label_head} rps | {a.label_base} rps | raw Δ | floor | "
          f"redis Δ (control) | ctl CV | {a.label_head} ratio | {a.label_base} ratio | ratio Δ | read |")
    print("|---|---:|---:|---:|---:|:---:|---:|:---:|---:|---:|---:|---|")

    for f in fams:
        for d in depths:
            hm = head.get((f, d, "moon"), [])
            bm = base.get((f, d, "moon"), [])
            hr = head.get((f, d, "redis"), [])
            br = base.get((f, d, "redis"), [])
            if min(len(hm), len(bm), len(hr), len(br)) < 2:
                print(f"| {f} | {d} | INCOMPLETE | | | | | | | | suppressed |")
                continue
            hmm, bmm = statistics.median(hm), statistics.median(bm)
            hrm, brm = statistics.median(hr), statistics.median(br)
            raw_d = hmm / bmm - 1.0
            floor = max(cv(hm), cv(bm))
            ctl_d = hrm / brm - 1.0
            ctl_floor = max(cv(hr), cv(br))
            h_ratio, b_ratio = hmm / hrm, bmm / brm
            ratio_d = h_ratio / b_ratio - 1.0

            # Which column is readable is decided by the CONTROL, per row:
            # a control that itself moved further than its own noise means raw
            # ops/s carries that movement and only the ratio is comparable.
            #
            # But a control can also be too unstable to normalise BY. Redis is
            # unchanged code; when its own series is bimodal, dividing by it
            # manufactures a moon "result" out of Redis's variance -- this run
            # saw Redis's ZADD p=8 cluster at ~275k and ~320k within one
            # session, which turns a flat moon row into a +13% ratio. So when
            # the control is that noisy, neither column is published: the row
            # reports moon's own raw movement and says the control failed.
            control_moved = abs(ctl_d) > ctl_floor
            if ctl_floor > CONTROL_CV_LIMIT:
                which = "raw"
                if abs(raw_d) <= floor:
                    read = f"**control unstable** (ctl CV {ctl_floor*100:.1f}%); moon raw = tie"
                else:
                    read = f"**control unstable** (ctl CV {ctl_floor*100:.1f}%); moon raw {pct(raw_d)}"
            else:
                verdict_val = ratio_d if control_moved else raw_d
                which = "ratio" if control_moved else "raw"
                if abs(verdict_val) <= floor:
                    read = f"tie ({which}, |Δ| ≤ floor)"
                else:
                    read = f"{pct(verdict_val)} ({which})"

            print(f"| {f} | {d} | {hmm:,.0f} | {bmm:,.0f} | {pct(raw_d)} | {floor*100:.1f}% | "
                  f"{pct(ctl_d)} | {ctl_floor*100:.1f}% | {h_ratio:.3f}x | {b_ratio:.3f}x | "
                  f"{pct(ratio_d)} | {read} |")

    print()
    print("`floor` = worst within-side CV of the two moon series being compared.")
    print("`ctl CV` = worst within-run CV of Redis, the unchanged control.")
    print("`read` = the column the control licenses: **raw** when Redis held still "
          "(|control Δ| ≤ its own CV), **ratio** when Redis itself moved, and "
          f"**neither** when the control's own CV exceeds {CONTROL_CV_LIMIT*100:.0f}% "
          "-- a control that noisy cannot normalise anything.")
    print("A Δ inside the floor is reported as `tie`, never as a direction.")


if __name__ == "__main__":
    main()
