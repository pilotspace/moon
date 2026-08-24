# MILESTONE: Wire parity: reply types and command surface match Redis byte-for-byte

goal: every reply Moon puts on the wire decodes in a statically-typed Redis client exactly as the same reply from redis-server does — verified by diffing Moon against a live redis-server, command for command, not by reading the docs
rationale: sub-milestone. `v0-9-client-compat` closed having proved the big shapes (RESP3 types, push frames, cluster bootstrap, INFO). What it did NOT do is diff Moon against a live Redis reply-by-reply — and the moment that diff was run (2026-08-17, `redis-server 8.6.1` vs Moon `--shards 1`) it returned 16 wrong reply TYPES plus 2 missing commands, none of which any existing suite catches. These are decode errors in typed clients, not cosmetic differences, so they belong together under one oracle-driven milestone rather than scattered as one-off fixes.
stage: production · status: active · created: 2026-08-17

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:  RESP2/RESP3 reply-TYPE correctness for null and empty aggregates (#482); command-surface
     gaps a client can reach and get an unknown-command/arity error from (#520 RPOPLPUSH,
     #521 ZRANK/ZREVRANK WITHSCORE); reply-shape parity on introspection commands
     (#469 COMMAND COUNT, #480 PUBSUB NUMPAT, #491 arity-error command casing).
Out: reply *content* correctness where the type is already right (that is per-command work, not
     wire parity); RESP3-only shapes already closed by `resp3-type-fidelity`; cluster/replication
     wire formats (#451); performance of the reply path. Adding commands Redis itself has
     removed. `SINTERCARD`/`XRANGE`/`SORT_RO`-class empties that the oracle confirmed are
     ALREADY correct — those are regression fences, not work.

## Shared decisions & glossary deltas   (living — every task must honor these)
- **The oracle is the contract.** No parity claim in this milestone is written from the Redis docs
  or from recall. Each is measured against a live `redis-server` with a probe that (a) gives every
  case its own never-written key and (b) asserts `EXISTS == 0` before probing. The first version of
  that probe let one case create `nokey` as a list and returned `WRONGTYPE` for 30 rows that read
  as real findings — verify the instrument before the result.
- **Empty aggregate ≠ null aggregate.** `*0` and `*-1` are different replies with different client
  decodings. The oracle confirmed ~12 sites that correctly return `*0` today and must STAY `*0`.
  Any change in this milestone must name which of the two it means, per site.
- **Fix every dispatch path.** This codebase has more than one command-dispatch path
  (dispatch / dispatch_read / inline), and a missing arm in one of them is invisible to CI. A task
  here is not done when one path works.
- **Both runtimes.** `runtime-monoio` is the shipped runtime and `runtime-tokio` is the CI leg;
  a reply-path change must be proven on both, not on whichever one `cargo test` defaults to.

## Shared / risky contracts (freeze these first)
- `Frame::NullArray` — the null-aggregate variant, serialising `*-1` in RESP2 and `_\r\n` in
  RESP3, composable INSIDE `Frame::Array` (GEOPOS nests one). Every other task in this milestone
  either consumes it or must not disturb it. -> owning task `resp2-null-array`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] resp2-null-array      depends-on: none                — #482: add `Frame::NullArray`; audit every `Frame::Null` site per the measured table; fix the 16 divergences incl. EXEC-abort and nested GEOPOS
- [ ] rpoplpush-command     depends-on: none                — #520: `RPOPLPUSH` is unknown-command; delegate to LMOVE semantics, add metadata, wire all dispatch paths
- [ ] zrank-withscore       depends-on: resp2-null-array    — #521: `ZRANK/ZREVRANK ... WITHSCORE`; hit path returns `[rank, score]`, miss path returns the null array
- [ ] introspection-shapes  depends-on: none                — #469 COMMAND COUNT returns an array not an integer; #480 PUBSUB NUMPAT counts subscribers not patterns; #491 arity errors upper-case the command name

## Exit criteria (observable; map each to the task that delivers it)
- [x] A client that decodes `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP` as an array gets a null array on timeout, not a null bulk        (← resp2-null-array)  (verify: `cargo test --test resp2_null_array rna1_blocking_timeouts_are_null_array` — spawns a real server and reads the raw bytes off the socket)
- [x] `EXEC` aborted by a broken `WATCH` replies `*-1`, so optimistic-locking client code decodes the abort path instead of erroring        (← resp2-null-array)  (verify: `cargo test --test resp2_null_array rna3_exec_aborted_by_watch_is_null_array`)
- [x] `LPOP key <count>` / `RPOP key <count>` on an absent key reply `*-1`, while `ZPOPMIN`/`SPOP n`/`SMEMBERS`/`HGETALL`/`XRANGE` on an absent key still reply `*0`        (← resp2-null-array)  (verify: `cargo test --test resp2_null_array rna4_pop_with_count_on_an_absent_key_is_null_array` for the `*-1` half and `rna7_regression_fence_the_already_correct_sites_do_not_move` for the `*0` half — the fence is what stops a fix here breaking the ~12 sites that were already right)
- [x] `GEOPOS` of an absent member nests a null array inside the outer array        (← resp2-null-array)  (verify: `cargo test --test resp2_null_array rna5_geopos_nests_a_null_array_but_geohash_does_not`)
- [ ] `RPOPLPUSH source destination` moves the element and appears in `COMMAND INFO`        (← rpoplpush-command)  (verify: manifest rows `parity_rpoplpush_moves_the_tail` / `_leaves_the_source_short` / `_pushes_to_the_destination_head` / `_absent_source_is_null_bulk` in `scripts/client-compat/manifest.yaml`, diffed against live redis-server — unwaived)
- [ ] `ZRANK key member WITHSCORE` returns `[rank, score]` for a present member and a null array for an absent one        (← zrank-withscore)  (verify: manifest rows `parity_zrank_withscore_hit`, `parity_zrevrank_withscore_hit`, `parity_zrank_withscore_absent_key_is_null_array`, `parity_zrank_withscore_absent_member_is_null_array`, `parity_zrank_without_withscore_miss_is_null_bulk` — the last one is the discriminator: without WITHSCORE a miss must stay a null BULK)
- [ ] `COMMAND COUNT` replies an integer; `PUBSUB NUMPAT` counts unique patterns; an arity error names the command in lower case        (← introspection-shapes)  (verify: manifest rows `identity_command_count`, `pubsub_numpat_counts_distinct_patterns`, `multi_aborts_on_wrong_arity`)
- [ ] Re-running the Moon-vs-Redis oracle diff over this milestone's command set returns zero divergences, and the diff runs in the compat harness so it stays that way        (← resp2-null-array, rpoplpush-command, zrank-withscore, introspection-shapes)  (verify: `scripts/client-compat/` full run against a live redis-server reports FAIL=0 with none of the rows above on the WAIVED list — a waived row is an unproven criterion, not a met one)

## Close — ship review   (AI fills when every task is done — the evidence behind the engine gate, read before the boxes are checked)
> Whole-milestone, cross-task review the AI fills in. It is the evidence behind the EXISTING engine
> gate (milestone-done / checking the Exit-criteria boxes) — NOT a new approval. Tool-agnostic.

### Ship by domain   (what changed, per bounded context)
- tooling : <add.py / state.json / templates — what shipped, or "untouched">
- skill   : <SKILL.md / phases/* / guides — what shipped, or "untouched">
- book    : <docs/* — what shipped, or "untouched">

### Cross-task evidence   (one row per task)
- <slug> : gate=<PASS|RISK-ACCEPTED> · tests=<n green> · residue=<none|note>

### Goal met?   (map the evidence back to this milestone's Exit criteria — read before the Exit-criteria boxes are checked)
- [ ] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cite which)
- goal: <restate the milestone goal — and the one evidence line that proves the ship meets it>

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
> The AI writes the release steps for THIS milestone here (hints, not engine commands). MERGE is one
> small step among them. These feed the release scope (release.md) when the cut is bundled.
- [ ] one PR per task, each merged before the next starts, each with a full `workflow_dispatch` matrix run on its branch (PR CI path-filters skip Windows/macOS/console)
- [ ] land the oracle diff in the compat harness so the parity claim is re-checked on every PR, not just at fix time
- [ ] re-run the oracle against the merged `main` binary and paste the zero-divergence table into the Close section
- [ ] cut the release per release.md (human-run) — bundling this milestone with the already-closed `v0-9-client-compat`
