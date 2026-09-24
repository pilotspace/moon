# WS3-lists-listpack — PLAN (wave 1)
personas: `.add/personas/routing-dispatch-engineer.md` (lead) · `.add/personas/performance-engineer.md`

Review measurements: LREM l 0 a on a 200K list (100K matches) 13.1 s vs redis 0.30 s; LPOS l2 a MAXLEN 10 on a 1M list 38.9 ms vs 78 µs.

## Issues
1. **moon#1173** LREM O(N·K); LPOS copies the whole list.
   - Must: LREM single compaction pass bounded by `max_remove` (count ≥ 0 forward, count < 0 reverse), listpack arm via `iter_refs` + one rebuild, existence/WRONGTYPE gate through `get_list_ref_if_alive` (no flattening, fewer probes).
   - Must: LPOS iterates the `ListRef` in place (forward, or backward for negative RANK), stops at MAXLEN / COUNT, no `element.clone()`.
   - Tests: randomized equivalence vs a naive oracle for all count signs/RANK/COUNT/MAXLEN combos; size sweep proving O(N) LREM and O(MAXLEN) LPOS.
2. **moon#1174 §1** LTRIM/LREM/LINSERT/LMOVE/RPOPLPUSH permanently flatten listpack lists.
   - Must: listpack arms (LTRIM = one range drain on the byte buffer; LREM/LINSERT via iter_refs + remove_at/insert; LMOVE via listpack pop/push) so `LPUSH k x; LTRIM k 0 99` stays `listpack` (`OBJECT ENCODING`) exactly when redis would.
   - Should: convert back to listpack when a list shrinks below half the threshold IF redis 7.x does so for the same sequence (check against redis-server 7.0.15 on PATH; match it, don't exceed it).
3. **moon#1174 §2** `ListRef::range` seeks from head per index → one seek then walk (`iter_refs().skip().take()` / `iter_rev` for tail ranges); SRANDMEMBER-with-count on listpack sets: sort sampled indices, walk once. Pin with the test-only `HEAD_SEEKS` counter (one seek per LRANGE).
4. **moon#1174 §3** listpack owned reads allocate twice per element: `ListpackEntry::to_bytes` consuming `self` (zero-copy `Bytes::from(vec)`) or callers moved to `iter_refs()` + one copy/itoa; field-only / length-only helpers for HKEYS, HVALS, HEXISTS, HSTRLEN.

## Owned files
`src/command/list/**`, `src/storage/listpack.rs`, `src/storage/db_kind.rs`, `src/storage/db_read.rs` (`ListRef` / `HashRef` impl blocks + `ListpackEntry` call sites only), `src/command/hash/hash_read.rs` (HKEYS/HVALS/HEXISTS/HSTRLEN functions only), `benches/listpack_*.rs`, new tests `tests/perf_ws3_*.rs`.
Cross-ownership (own commit + note): a new list accessor in `src/storage/db/accessors.rs` if an existing one cannot serve the listpack mutation arms.

## Not yours
`src/storage/entry.rs`, `src/storage/db/**` (except the note above), `src/storage/bptree*`, `src/command/{set,sorted_set,string,geo}/**`, HRANDFIELD in `hash_read.rs` (WS2).
