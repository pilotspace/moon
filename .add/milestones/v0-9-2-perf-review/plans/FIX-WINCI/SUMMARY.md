# FIX-WINCI — SUMMARY

(Committed by the orchestrator from the agent's final report; the harness refused the agent's own write. NOTES.md, committed by the agent as `9fd6006e`, has the mechanisms, the stall tooling, the red/green evidence and the gates.)

- **Branch:** `fix/winci-harness`, base `d49a2a98`. Test-only: no `src/` change.
- **Persona:** ci-test-integrity-engineer.
- **Scope:** six integration tests that treated fixed sleeps, fixed round counts or 50–300 ms windows as facts. Main's post-merge run for `4a96cd5f` went red on Windows 3/3 during a ~3-minute runner stall.
- **Stall injection:** a script freezes the server at a fixed point, patched identically into the old and new file. It either SIGSTOPs the whole server or ptrace-freezes named threads (`aof-writer`, `spill-0`, `manifest-sync-0`) while the rest run. This is evidence tooling only and was never committed.
- **Binaries:** all runs used debug binaries pinned with `MOON_BIN`, built from unchanged `src`.

## Verdicts
All six are **FIXED**: red under an induced stall before the change, green under the same stall after it.

| test | issue | old failure mode | old, under induced stall | fix | new, same stall | runtime (no stall) old → new |
|---|---|---|---|---|---|---|
| `cold_cut_single_shard_914` respill | moon#1065 | fixed 1 s sleep | `spill-0` frozen 5 s: "nothing spilled before the rewrite" | poll for cold files and for the respill's file; wait for the AOF to hold the last acked write before each SIGKILL; filler retries refusals | green, 6.1 s | 2.66 → 0.94 s |
| `cold_index_duplicate_resolution_983` 1 shard | moon#1065 | 1000-round cap; a backpressure refusal asserted as an error | spill frozen 8 s: "after 1000 filler rounds"; writer frozen 8 s: "unexpected error: AOF fsync failed" | deadline in place of rounds; a "copy" must be listed Active in the manifest (unregistered orphans no longer count); refusals re-sent and counted | green, 10.7 s and 13.0 s (3 refusals re-sent) | 4.84 → 2.99 s |
| `cold_file_id_seed_997_893` torn manifest | moon#1065 | same refusal assert and round cap | writer + spill + manifest-sync frozen 8 s: the refusal | retrying filler; deadline loop | green, 22.2 s | 12.89 → 15.24 s |
| `cold_file_id_reuse_1067` prune kill9 | moon#1065 | flat 20 s read per 50-command pipeline; fixed 1 / 4 / 1.5 s sleeps | whole server stopped 25 s (tokio): "timed out waiting for 50 replies; got 0 bytes" | read budget n × 2 s + 60 s (from the server's measured 2 s-per-write bound; one pipeline took 18.7 s); poll for spill idle, reclaim and tombstone pruning; AOF check before kill | green, 29.9 s | 10.66 → 4.36 s (tokio) |
| `acl_user_revocation` kill-by-user | moon#1273 | fixed 300 ms read window | whole server stopped 2 s: `ACL SETUSER alice failed: ""` | read exactly one complete reply with a 20 s deadline; a silent skip is now a panic | green, 2.2 s | 1.96 → 0.06 s |
| `perf_ws16_bgsave_capture` FLUSHDB | moon#1273 | absolute 50 ms budget | whole server stopped 3 s: "FLUSHDB took 4.02s" | compare with an in-process `DEL` of an equal hash; per sample, check the hash was still queued and billed; retry in rounds | green on monoio (10.2 s) and tokio (8.8 s) | 5.43 → 5.03 s |

**Guards still fail on their defects:**
- ws16, with the WS16 Charge-redirect fix reverted locally, is red with and without the stall: "4 FLUSHDBs … freed the UNLINKed hash inline".
- 1067, with moon#1113 reverted, is red: ids re-issued.

**Two design pivots, caught by the evidence:**
- A timing-only ws16 check passed the reverted build under the stall, because after SIGCONT the missed ticks run back to back and drain the other queued hashes.
- The first 1067 version raced the delete wave against in-flight spills.

**Whole-file runs:**
- monoio: acl 6/6, ws16 8/8, 914 5/5, 983 4/4, 997 10/10. 1067 is 0/3, which is pre-existing: see finding 1.
- tokio: all six pass; 1067 3/3, and ws16 passed 4 of 4 full-file runs.

## Findings outside scope (product side)
1. `cold_file_id_reuse_1067` is red on monoio at main `4a96cd5f` too, in debug and release, with and without io_uring. Dead cold files stay pending unlink (`cold_files_pending_unlink:7`). Per-PR CI runs tokio only, so only the self-hosted monoio main-push leg can see it.
2. Tick catch-up bursts. After a stall, tokio's intervals fire the missed ticks back to back (default `MissedTickBehavior::Burst`), and monoio catches up too. Each tick frees a slice of the lazy-free queue, so a queued value can be freed in one piece ahead of the next command. The old ws16 test failed 3 of 3 on tokio in whole-file runs (224–241 ms). This is the likely real cause of the Windows "FLUSHDB took 85ms".
3. A DEL wave racing in-flight spills can leave dead cold files that are never reclaimed (tokio). This is the likely cause of the reclaim-timeout flake in moon#1065's second comment.
4. moon#1272 still applies: the backpressure refusal shares its text with a real fsync failure.

## Risks
- A genuine fsync failure now surfaces at the 120 s deadline, with INFO counters and `server.err`, instead of at once. Red runs are slower: monoio 1067 takes 120 s per test.
- The ws16 and 1067 checks rely on server behaviour: `MEMORY STATS` reports the selected db's ledger synchronously; `current_cow_size` carries the frozen table's bill; tombstone GC keeps the highest file id. If any of these changes, the tests fail loudly ("unjudgeable" or a timeout), never silently.
- ws16 now uses about 384 MB of `used_memory` (1M fields; it was about 256 MB at 2M).
- The shared helper `tests/common/slow_host.rs` is additive, but it compiles into every suite that includes `common`.
- Windows, MSRV and the monoio self-hosted leg were not run locally.

## Gates
- `cargo fmt --check`, `audit-test-tempdirs`, `cargo clippy --all-targets -D warnings` and the tokio `cargo check --all-targets` are clean.
- Every cargo call ran with `CARGO_INCREMENTAL=0`.

## CHANGELOG bullet
- **Tests (moon#1065, moon#1273):** six integration tests no longer fail on a slow or stalled host (hosted Windows runners). They poll for the condition they need instead of fixed sleeps, round counts or 50–300 ms windows. Cold-tier fillers retry an AOF backpressure refusal. The ACL test reads one complete reply. The FLUSHDB-during-a-save check compares against an in-process inline free instead of an absolute 50 ms budget.

## Commits (`git log --oneline d49a2a98..9fd6006e`)
```
9fd6006e docs(add): FIX-WINCI NOTES — mechanisms, stall tooling, red/green evidence, findings, gates
b86bbefd test(acl,snapshot): read one framed reply; judge FLUSHDB against an in-process inline free (moon#1273)
d490920f test(cold-tier): wait for the condition, not a sleep or a round count; retry AOF backpressure refusals (moon#1065)
```
