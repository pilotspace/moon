---
name: five-family-pipeline-consult-2026-09-12
description: 2026-09-12 strategy consult on HSET/INCR/SADD/ZADD/LPUSH losing to Redis at p>=8 — recommended "plain lane" (finish skip_name_gates, one metadata lookup, same dispatch) over new byte-level inline paths; first experiment = GET forced generic via non-unrestricted ACL user
metadata:
  type: project
---

Recommendation given (advisory, main @ d3b8a206): do NOT add per-family byte-level
inline paths (option a); build a metadata-driven "plain lane" in the monoio frame
loop that skips the ~45 per-command state/name gates when
`plain_conn && meta.flags.contains(NO_INTERCEPT)` and still calls the ONE
`command::dispatch`. Per-family micro-opts (option c) cannot reach parity on their
own: the required removal is 0.57-0.80 us/op per family vs 0.15-0.30 handler share.

**Why:** measured ARM decomposition — inline GET/SET per-command cost is 0.56-0.58x
Redis, generic families 1.32-1.65x, delta ~1.0 us constant across four container
types. `try_inline_dispatch`'s correctness surface is a ledger of 17 incident refs
in the fn body + 7 in its gate; every one would need re-discovery per new family.
`skip_name_gates` (handler_monoio/mod.rs ~2280) is already a half-built plain lane
covering ~20 gates; ~25 remain ungated.

**How to apply:** if asked again about these families, check first whether the
zero-code experiment was run: GET C via a permitted-but-not-`unrestricted` ACL
user (forces `acl_skip_allowed()` false → generic read path) vs inline GET C. If
the delta is < 0.3 us the preamble hypothesis is falsified and the answer moves to
Frame/Bytes-refcount/handler work. Bundle Phase-1 refactors before measuring
(#938 lesson: single items are under the floor). Structural safety kit: one
predicate fn, table-enumerated differential test (DEBUG DIGEST oracle), a
`local_plain` dispatch-path metric.
