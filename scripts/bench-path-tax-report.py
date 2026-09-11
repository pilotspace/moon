#!/usr/bin/env python3
"""Reduce bench-path-tax.sh CSV to the per-command path tax.

Throughput at pipeline depth P is modelled as `us_per_op = C + B/P`, where C is
the per-command term and B the per-batch term. Solving from the P=8 and P=64
points gives C for each leg; the difference between the two legs' C is the cost
of the generic dispatch path for IDENTICAL handler work.

The solve is exactly determined from two points -- a decomposition under an
assumed model, not a fit, with no residual to check. Treat the result as a band,
not a figure, and remember the ACL confound documented in the harness makes it an
UPPER bound.
"""
import csv
import statistics as st
import sys
from collections import defaultdict


def main(path):
    rows = defaultdict(list)
    header = []
    with open(path) as fh:
        lines = fh.readlines()
    body = []
    for line in lines:
        if line.startswith("#"):
            header.append(line.rstrip())
            continue
        body.append(line)
    for r in csv.DictReader(body):
        # A leg the harness refused to record never reaches here; guard anyway.
        if not r.get("rps"):
            continue
        rows[(r["leg"], int(r["depth"]))].append(float(r["rps"]))

    print("\n".join(header))
    print()

    need = [("inline", 8), ("inline", 64), ("generic", 8), ("generic", 64)]
    missing = [k for k in need if len(rows.get(k, [])) < 2]
    if missing:
        print(f"> **INCOMPLETE**: fewer than 2 samples for {missing}. No tax published.")
        return 1

    out = {}
    print("| leg | p=8 rps | p=64 rps | µs@8 | µs@64 | C (µs/op) | B (µs/batch) | CV8 | CV64 |")
    print("|---|---:|---:|---:|---:|---:|---:|:---:|:---:|")
    for leg in ("inline", "generic"):
        r8, r64 = rows[(leg, 8)], rows[(leg, 64)]
        u8, u64 = 1e6 / st.median(r8), 1e6 / st.median(r64)
        C = u64 - (u8 - u64) / 7.0
        B = 64.0 * (u8 - u64) / 7.0
        cv8 = st.stdev(r8) / st.mean(r8) * 100 if len(r8) > 1 else float("nan")
        cv64 = st.stdev(r64) / st.mean(r64) * 100 if len(r64) > 1 else float("nan")
        out[leg] = (C, B, max(cv8, cv64))
        print(f"| {leg} | {st.median(r8):,.0f} | {st.median(r64):,.0f} | {u8:.3f} | "
              f"{u64:.3f} | **{C:.3f}** | {B:.2f} | {cv8:.1f}% | {cv64:.1f}% |")

    Ci, _, cvi = out["inline"]
    Cg, _, cvg = out["generic"]
    tax = Cg - Ci
    floor = max(cvi, cvg) / 100.0
    print()
    print(f"**Path tax = {tax:.3f} µs/op** ({Cg / Ci:.2f}x the inline per-command cost), "
          f"same handler, same session.")
    print()
    # The tax must clear the noise the two legs themselves support, or it is not
    # a result. Expressed against the larger leg so it is the conservative test.
    if tax <= floor * Cg:
        print(f"> **Inside the noise floor** (worst within-leg CV {floor*100:.1f}% "
              f"= {floor*Cg:.3f} µs on the generic leg). Not a result.")
    else:
        print(f"Worst within-leg CV {floor*100:.1f}% ({floor*Cg:.3f} µs on the generic "
              f"leg) — the tax clears it by {tax/(floor*Cg):.1f}x.")
    print()
    print("Upper bound: the generic leg also pays the ACL check the inline leg skips.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin"))
