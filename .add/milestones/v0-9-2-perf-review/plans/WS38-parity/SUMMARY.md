# WS38-parity SUMMARY

Wave 2, lane C, phase 1: branch `w2/ws38-parity`, base `2e99254`. Linux container, not the merge bar.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1286 `expired_keys` always 0 | FIXED | 96c63ac | `tests/info_expired_keys_1286.rs`, 7 tests: 50 × `SET PX 20` through the active, lazy and DEL paths at s1 and s4, plus a hash-field negative control.<br>• Base, both runtimes: 6 of 7 fail (0 instead of 50); the control passes.<br>• Fix, both runtimes: 7/7 pass.<br>• Oracle: redis 7.0.15 and 7.2.7 agree, a master counts 100, a replica 0, and a DEL of an expired key 150.<br>• 5 unit tests plus a replica assertion.<br>• Rows added to test-consistency.sh and test-commands.sh; they fail on base and pass on the fix. | A replica's own cold TTL sweep and on-read cold reclaim still count (they affect only spilled TTL keys on replicas). A DEL of a cold-only expired key is not counted, although redis would count it. |
| moon#1296 ACL rule order | FIXED for command rules; category tokens PARTIAL | cf8d6ca | An oracle matrix of 39 rule sequences against redis 7.2.7: base differs on 22, the fix on 7.<br>• 6 of the 7 remaining differences are category cases: moon expands `+@read` into commands, redis keeps the token.<br>• The last is `+@all +get`, which grants the same permissions either way.<br>• `tests/acl_rule_order_1296.rs` checks 14 sequences through GETUSER, ACL LIST, the SAVE file and GETUSER after LOAD, at s1 and s4. It fails on base on both runtimes and passes on the fix.<br>• 7 unit tests. | Rendering category tokens needs an origin-tagged rule model, and is filed separately. |

## Measurements
**Consistency suite** (7.2.7 oracle, s1):
- monoio: base has 36 failures; ws38 has 8, all present on base. The 28 new rows fail on base and pass on the fix.
- tokio: base 42 failures, ws39 14, none new.

**test-commands.sh:**
- `--category acl`: base 8 failures, ws38 0.
- `--category connection`: base 6 failures, ws38 4, all present on base.

**GET/SET p1 A/B:** not valid evidence, because load was 3–5 while the other lanes were building. **The orchestrator reruns it at R1.**

## Cross-ownership edits
- `src/replication/apply.rs`: about +8 lines in one existing test.
- `src/storage/db/cold_promote.rs`, `src/storage/db/accessors.rs`, `src/command/key.rs`: one `record_expired_key` call each.
- `src/admin/metrics_setup`: a test-only probe field.

## Risks
- Any test that asserts an absolute `expired_keys` value will now see a non-zero count.
- ACL files written by the old binary reload fine, but the first ACL SAVE rewrites them in a different order.
- `CommandPermissions::Specific` changed shape from `{allowed, denied}` to `{rules}`, which conflicts with any other ACL work.

## CHANGELOG bullets
- **fix(info):** `INFO stats` `expired_keys` counted nothing (moon#1286). It now counts every expiry-driven whole-key removal:
  - the active cycle, including #1288's fast slices;
  - the lazy-reap drain;
  - DEL / UNLINK, or a write, that reaps an expired key;
  - the cold TTL sweep and on-read cold reclaim.

  Hash-field expiry and a replica applying its master's DEL are not counted. Both match redis 7.0.15 and 7.2.7.
- **fix(acl):** `ACL GETUSER`, `ACL LIST` and `ACL SAVE` now render command rules in the order they were applied, as redis 7.2+ does (moon#1296). They were sorted alphabetically before.
  - Existing ACL files re-save with a different rule order. Permissions are unchanged, but diff-based config management will see a one-time change.
  - Category tokens (`+@read`) are still expanded into their commands, unlike redis.

## Self-evaluation (0–1)
Completeness 0.9 · Clarity 0.92 · Practicality 0.92 · Optimization 0.85 (bench pending a quiet window) · Edge cases 0.9 · Self-evaluation 0.9
