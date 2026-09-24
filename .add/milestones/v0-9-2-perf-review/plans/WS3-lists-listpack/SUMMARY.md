# WS3-lists-listpack SUMMARY

> Committed by the orchestrator from the agent's final report (the harness refuses
> subagent writes of SUMMARY.md). Branch `perf/ws3-lists-listpack`, 7 commits over `a925e64`.

## Per-issue verdict
| issue | verdict | commits | evidence | follow-ups |
|---|---|---|---|---|
| moon#1173 | FIXED | e611112, 974a13f | LREM 200K/100K-matches 13.23 s → 2.5 ms (redis 0.30 s); 0.45/2.5/11.5 ms at 20K/200K/1M. LPOS MAXLEN 10 on 1M 39.1 ms → 71 µs (redis 63), flat 10K..1M. Tests: lrem_on_a_200k_list_is_one_pass, lpos_with_maxlen_does_not_copy_the_list, lrem_deque_agrees_with_the_naive_oracle, lrem_handler_agrees_with_the_oracle_on_both_encodings, lpos_agrees_with_the_oracle_on_both_encodings, list_ops::remove_matches_agrees_with_the_naive_oracle, lpos_option_errors_match_redis | — |
| moon#1174 §1 | FIXED (LTRIM/LREM/LINSERT/LMOVE/RPOPLPUSH + LMPOP); shrink-back DEFERRED | 0b921b5 (LREM in e611112) | 10K × 110 × (LPUSH+LTRIM 0 99): used_memory 81.9 → 43.1 MB, RSS 201 → 62 MB (redis 27.4 / 80 MB). Tests: a_capped_list_stays_a_listpack, each_secondary_write_keeps_a_small_listpack, the_arms_still_promote_past_the_threshold, ltrim_windows_match_the_linkedlist_path, a_rotation_keeps_the_key_and_its_ttl, the_arms_are_byte_transparent, refusals_create_nothing_and_pop_nothing | Shrink-back: owner decision (redis 7.0.15 reports quicklist for every list). LPUSHX/RPUSHX still flatten (moon#832 pin). The blocking wake path (`Database::list_pop_*/push_*` in WS1's accessors.rs) still flattens. Remaining gap: Vec doubling vs redis's exact realloc |
| moon#1174 §2 | FIXED | a2de9db, 4f4dacf | LRANGE 0 -1 median 22.5K → 52.8K rps (every paired rep ≥ 1.66×; redis 64.6K). Tests: range_refs_seeks_once_and_agrees_with_slicing, lrange_and_lindex_on_a_listpack_seek_once, srandmember_count_on_a_listpack_walks_once, for_each_at_walks_once_and_keeps_draw_order | — |
| moon#1174 §3 | FIXED | 3aaaa3a | HKEYS median 58.8K → 68.2K (every paired rep > 1); HGETALL 40.6K → 53.3K (mixed per rep — noise). Tests: into_bytes_takes_the_vec_over_without_copying, owned_read_paths_do_not_decode_into_a_vec_first, hash_helpers_agree_with_get_field_on_every_encoding, hkeys_hvals_hexists_hstrlen_read_in_place | `key_extra.rs` SORT (not owned) → `into_bytes()` |
| found: listpack backlen byte order | worked around | 974a13f | Encoder writes `[low\|0x80, high]` (goldens pin it); `decode_backlen` reads the redis order → backward walks break for entries ≥ 128 B (latent under today's 64 B policy). New walks are forward-only. Test: every_operation_is_correct_on_wide_entries | File an issue: flipping the encoder is a format decision (also fixes redis interop) |

Red on HEAD (git-archive of a925e64 + the test files): 11/12 handler-level tests fail (the passing one is the equivalence guard) — e.g. LREM 13.4 s; 500 × LPOS 5.0 s; TTL read 0 after `LMOVE k k`; a WRONGTYPE-refused LMOVE flattened its source.
Parity vs redis 7.0.15: fixed probes baseline 5 diffs → 0; differential fuzz 28,000 ops at shards 1 and 4, both encodings, 0 diffs. Cross-shard LMOVE is still refused (unchanged).
Dispatch: reads reach `dispatch` (via `_readonly` twins / shared helpers) and `dispatch_read`; none are inline-dispatched; writes are dispatch-only.

## Measurements
Method: shared 4-vCPU box, release-fast builds, `--shards 1`; each rep runs baseline → new → redis in turn.
- LREM (s): baseline 20K 0.122/0.125/0.121, 200K 13.20/13.41/13.23; new 20K 0.00058/0.00045/0.00042, 200K 0.0023/0.0025/0.0037, 1M 0.0107/0.0117/0.0122; redis 20K 0.030/0.029/0.033, 200K 0.302/0.298/0.293, 1M 1.50/1.52/1.47.
- LPOS MAXLEN 10 (µs/op): baseline 10K 385/349/351, 1M 39392/39265/38670; new 10K 83/81/101, 1M 73/67/75; redis 10K 84/71/73, 1M 65/59/66.
- rps, 6 reps (10K lists × 100 × 16 B; 10K hashes × 50 fields): LRANGE 0 -1 baseline 22379/23460/22676/22912/17934/21084 · new 60606/38994/71301/68564/43516/45025 · redis 65445/74991/59506/63694/81766/42580. HKEYS baseline 55602/65147/62035/64350/29308/40667 · new 65424/71994/77012/71048/47562/51586. HGETALL baseline 44063/36846/60846/52645/35002/37058 · new 54900/54975/56850/51653/46200/35568. Noise floor: the redis control ranged 42.6K–81.8K.
- Memory (fresh servers): baseline 81,914,165 B / RSS 201,244,672; new 43,090,000 B / RSS 62,353,408; redis 27,390,800 B / RSS 80,166,912.

## Cross-ownership edits
- 4f4dacf `src/command/set/set_read.rs` (WS2's file): only `srandmember_readonly`'s two count arms, a private helper and an appended test module; RNG draws unchanged.
- 3aaaa3a `src/storage/db_read.rs`: four one-token `to_bytes` → `into_bytes` edits inside `SetRef`/`SortedSetRef`.
- No edit under `src/storage/db/**`.

## Risks / things the orchestrator must re-check at integration
1. Wire-visible changes, all toward redis parity (CHANGELOG): LPOS decides the option before parsing its value; COUNT/MAXLEN non-integer and RANK 0 error texts; `LMOVE k k` keeps the key's TTL; OBJECT ENCODING stays `listpack` after these writes (redis 7.2+ semantics); a refused command no longer flattens either key.
2. No shrink-back; `every_list_writer_that_empties_a_list_removes_the_key` relies on that.
3. Shared target dir: every worktree's lib-test binary has the same name — re-run tests on the merged tree.
4. One lib test fails as root (chmod-000 test) — environmental.
5. `list_write.rs` is at 1492 lines; new code went to child modules (`listpack/list_ops.rs`, `read_tests.rs`) and separate test files.
6. Suggested consistency-script rows (scripts/ not owned): the LPOS error cases; `RPUSH k a; EXPIRE k 100; LMOVE k k LEFT RIGHT; TTL k`.
7. No new unsafe; both audits pass. Full lib suite on the branch: 6053 passed, 1 failed (item 4).

## Self-evaluation
Completeness 0.92 · Clarity 0.9 · Practicality 0.93 · Optimization 0.9 · Edge cases 0.93 · Self-evaluation 0.9
