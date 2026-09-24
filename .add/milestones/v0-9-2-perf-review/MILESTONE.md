# MILESTONE: 2026-09 performance & code-quality review — fix wave

goal: land fixes for the 40 findings of the 2026-09 whole-codebase review (moon#1159–#1198, index moon#1199) — each measured or test-proven, each committed individually, without regressing wire parity, durability, or the shared-nothing invariants
rationale: the review verified every finding against HEAD `935c555` and measured 13 of them against redis-server 7.0.15 (up to 3,215× gaps, 553 MB of hidden RSS, 15 s shard stalls, a data-integrity bug). Fixes are grouped into workstreams by FILE OWNERSHIP so parallel agents do not collide.
stage: production · status: active · created: 2026-09-24 · base: `935c555`

## Scope
In: every issue in moon#1199's tiers 1–5, executed as the 11 workstreams below.
Out: the RESET/ACL privilege-retention finding (security — handled privately per SECURITY.md, never in this branch) · any wire-visible behaviour change that makes moon diverge further from redis · on-disk format changes without a version bump + backward-compat read path.

## Shared decisions (every workstream must honor)
- **One issue ⇒ at least one commit**, message `perf(<area>): <what> (moon#NNNN)` or `fix(<area>): … (moon#NNNN)`; a commit may close at most one issue; partial progress says `refs moon#NNNN` not `fixes`.
- **Behaviour parity is frozen**: replies stay byte-identical to redis (and to HEAD where HEAD already matched redis). Every change ships a test that would have caught the old cost or bug (wall-time / op-count / allocation-count / RSS assertion, per CONVENTIONS "behavioral wall-time red test").
- **Hot-path rules** (CLAUDE.md): no new alloc/lock on dispatch/parse/event loop/io; parking_lot only; no new `unsafe` (a fix that needs one is DESCRIBED in SUMMARY.md, not written); dual-runtime compiles.
- **Shared orchestrator artifacts are read-only for workstream agents**: `CHANGELOG.md`, `.add/state.json`, this `MILESTONE.md`, `TEAM-RULES.md`, `.add/PROJECT.md`, `.add/CONVENTIONS.md`, `CLAUDE.md`, `README.md`, other workstreams' plan directories. The orchestrator writes CHANGELOG + milestone status after each wave from the SUMMARY.md files.
- Build / test / measurement rules for this box: `TEAM-RULES.md`.

## Workstreams (file ownership decides the wave)
| wave | ws | persona(s) | issues |
|---|---|---|---|
| 1 | WS1-storage-core | performance-engineer · storage-durability-engineer | #1159 #1161 #1190 #1189(expiry-index part) |
| 1 | WS2-datatype-commands | routing-dispatch-engineer · performance-engineer | #1168 #1169 #1170 #1171 #1172 #1189(B+tree part) #1174(§4 listpack zset) |
| 1 | WS3-lists-listpack | routing-dispatch-engineer · performance-engineer | #1173 #1174(§1–3) |
| 1 | WS4-protocol-wire | performance-engineer · acl-security-gatekeeper (untrusted input) | #1164 #1179 |
| 1 | WS5a-vector-engine | performance-engineer | #1192 #1193 #1194(vector parts) #1196 |
| 1 | WS5b-text-graph | performance-engineer | #1191 #1195 #1194(text part) #1197 |
| 1 | WS6-persistence | storage-durability-engineer · performance-engineer | #1185 #1186 #1187 #1188 #1181 |
| 2 | WS7-conn-hotpath | performance-engineer · acl-security-gatekeeper | #1175 #1165 #1176 #1178 #1166 #1187(staging buffer) |
| 2 | WS8-shard-coordination | routing-dispatch-engineer · performance-engineer | #1162 #1177 #1182 #1183 #1184 #1214(1) #1229 #1228(MSET leg) |
| 2 | WS9-scripting-pubsub | acl-security-gatekeeper · performance-engineer | #1167 #1180 #1214(2) |
| 2 | WS10-memory-ownership | storage-durability-engineer · performance-engineer | #1225 #1160 #1163 #1198 #1206 #1212 #1214(3) |
| 2 | WS11-vector-followups | performance-engineer · storage-durability-engineer | #1213 #1194(remainder) |
| 2 | WS12-snapshot-integrity | storage-durability-engineer · performance-engineer | #1216(P0) #1217 #1185(incremental fold) |
| 2 | WS13-text-graph-followups | performance-engineer · ci-test-integrity-engineer | #1219 #1220 |
| 2 | WS15-durability-followups | storage-durability-engineer · ci-test-integrity-engineer | #1215(P0) #1223(P1) #1230 |
| 2 | WS16-snapshot-capture (queued after WS7+WS8+WS10 land) | storage-durability-engineer · ci-test-integrity-engineer | #1228(capture gaps, epoch liveness) #1185(incremental fold) #1231 #1232 |
| 2 | WS17-vector-text-residuals | performance-engineer · ci-test-integrity-engineer | #1226(vector/text/graph items, file splits, root-test skip) #1228(vector items) #1220(item 3) #1222 |
| 3 | WS14-ci-verification | ci-test-integrity-engineer | merged-tree gates, test-consistency.sh / test-commands.sh vs redis, re-measure the review's 📏 numbers |

## Found during the fix wave (filed 2026-09-24)
- Bugs: moon#1205 (B+tree corruption — fixed in WS2), #1206 (listpack backlen order — WS10), #1207 (TQ4A2 FT.SEARCH panic — fixed in WS5a), #1208 (EXACT QJL misalignment — fixed in WS5a), #1209 (LPOS/LMOVE parity — fixed in WS3), #1211 (inert LFU params / NOTOUCH introspection — fixed in WS1).
- More bugs (from WS6): #1216 **P0** BGSAVE loses keys on a mid-epoch DashTable split (WS12), #1217 COW captures only the first key of multi-key writes (WS12).
- From WS5b: #1218 FT prefix/fuzzy expansion nondeterministic across processes (fixed in WS5b), #1219 ignored cross-shard FT consistency suites broken at their seed (WS13).
- From the PR #1221/#1227 reviews: #1223 (P1 spill withdraw after fold — WS15), #1225 (list cold-fault element loss — WS10), #1228 (snapshot capture gaps — WS16/WS8), #1229 (SCRIPT FLUSH one shard — WS8), #1230 (bgsave status sticky — WS15); #1215 P0 cold DEL resurrects after rewrite (WS15); #1222 flaky test (WS17), #1226 review nits (vector/text/graph part — WS17). From WS15: #1231 (promote-then-sweep loss — WS16), #1232 (sharded --save never fires — WS16).
- Part 4 queue (after part 3): residual perf partials #1190 (O(1) entry_overhead, lazy free), #1189 (structural expiry index), #1171 (O(1) HRANDFIELD), #1220 item 3, #1194 key_hash merge (after WS8), #1226 nits. Hardware-blocked items (aarch64 A/B, Linux perf host, ≥8-core load generator, MiniLM recall) are recorded, not faked.
- Follow-ups: #1220 (text/graph residuals — WS13), #1212 (listpack residuals — WS10), #1213 (vector follow-ups — WS11), #1214 (needs-profile items — WS8/WS9/WS10).

## Merge protocol (every fix-wave PR)
- PR body lists `Fixes #N` for every issue the PR FULLY fixes (per the SUMMARY verdict table), so
  the merge auto-closes them; PARTIAL issues are listed as `Refs #N` with what remains.
- After each merge the orchestrator closes any fully-fixed issue that did not auto-close (comment
  naming the merge commit and evidence, `state_reason: completed`) and leaves a status comment on
  every PARTIAL issue (what landed, what remains, where it is routed). Resolved tasks never stay open.
- Part 1 (PR #1221, merged `37774e1`): closed #1159 #1161 #1211 #1173 #1209 #1193 #1192 #1196
  #1207 #1208 #1191 #1195 #1197 #1218 #1188 #1186 #1181; status comments on PARTIAL #1190 #1189
  #1174 #1194 #1185 #1187.

## Exit criteria
- [ ] every workstream has a SUMMARY.md with a per-issue verdict (FIXED / PARTIAL / DEFERRED + reason) and evidence
- [ ] `cargo fmt --check`, `cargo clippy --lib -- -D warnings` on both feature sets, `cargo test --lib` on both runtimes green on the merged branch
- [ ] the measured findings re-measured on the merged binary against redis-server (same harness as the review)
- [ ] CHANGELOG entries written by the orchestrator
