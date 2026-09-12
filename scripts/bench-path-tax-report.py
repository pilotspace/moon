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

    # Leg names differ by mode: tax mode is inline-vs-generic, aclcost mode is
    # unrestricted-vs-restricted (both generic, so the delta IS the ACL check).
    legs = ("inline", "generic") if any(k[0] == "inline" for k in rows) \
           else ("unrestricted", "restricted")
    need = [(legs[0], 8), (legs[0], 64), (legs[1], 8), (legs[1], 64)]
    missing = [k for k in need if len(rows.get(k, [])) < 2]
    if missing:
        print(f"> **INCOMPLETE**: fewer than 2 samples for {missing}. No tax published.")
        return 1

    out = {}
    print("| leg | p=8 rps | p=64 rps | µs@8 | µs@64 | C (µs/op) | B (µs/batch) | CV8 | CV64 |")
    print("|---|---:|---:|---:|---:|---:|---:|:---:|:---:|")
    for leg in legs:
        r8, r64 = rows[(leg, 8)], rows[(leg, 64)]
        u8, u64 = 1e6 / st.median(r8), 1e6 / st.median(r64)
        C = u64 - (u8 - u64) / 7.0
        B = 64.0 * (u8 - u64) / 7.0
        cv8 = st.stdev(r8) / st.mean(r8) * 100 if len(r8) > 1 else float("nan")
        cv64 = st.stdev(r64) / st.mean(r64) * 100 if len(r64) > 1 else float("nan")
        out[leg] = (C, B, max(cv8, cv64))
        print(f"| {leg} | {st.median(r8):,.0f} | {st.median(r64):,.0f} | {u8:.3f} | "
              f"{u64:.3f} | **{C:.3f}** | {B:.2f} | {cv8:.1f}% | {cv64:.1f}% |")

    Ci, _, cvi = out[legs[0]]
    Cg, _, cvg = out[legs[1]]
    tax = Cg - Ci
    label = "Path tax" if legs[0] == "inline" else "ACL check cost"
    floor = max(cvi, cvg) / 100.0
    print()
    print(f"**{label} = {tax:.3f} µs/op** ({Cg / Ci:.2f}x the baseline leg's per-command cost), "
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
    if legs[0] == "inline":
        print("Upper bound: the generic leg also pays the ACL check the inline leg")
        print("skips, AND -- if the command is GET -- the GET-only cold-tier peek at")
        print("handler_monoio/mod.rs:3674, which takes the db's exclusive guard and a")
        print("second full DashTable probe. Neither transfers to the write families.")
        print("Run `--mode aclcost` to measure the ACL component and subtract it;")
        print("run `--command 'set key:__rand_int__ xxxxxxxx'` to drop the GET peek.")
    else:
        print("BOTH legs take the generic path (the command is inline-ineligible), so")
        print("this difference is the ACL check alone -- subtract it from a `tax` run.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/dev/stdin"))
