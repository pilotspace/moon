# MILESTONE: KV Write Correctness & Data-Integrity Parity

goal: Moon's KV write commands honor Redis data-integrity contracts: no wrong-type command destroys data, integer commands reject overflow instead of panicking or wrapping, expire-in-the-past deletes the key, and MSETNX is atomic (cross-shard spans rejected).
rationale: new-major (split 3/3) — the KV-correctness sibling of the v3 "secondary-engine correctness & parity" theme. v3-1 (FTS) and v3-2 (graph) closed the search/graph defects; the 2026-07-02 KV deep review (`tmp/KV-DEEP-REVIEW.md`) surfaced a cluster of P0 data-integrity bugs in the *primary* KV engine (silent data loss on wrong-type GETDEL/GETSET/SET..GET, i64 overflow panics/wraps in DECRBY + the expire family, and a missing MSETNX). Correctness only — the p=1 throughput question from the same review is platform-driven (confound closed) and is NOT in scope here.
stage: production · status: active · created: 2026-07-02

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- **Wrong-type data loss (P0):** GETDEL, GETSET, and `SET key val GET` must return WRONGTYPE and
  leave the key intact when it holds a non-string — never delete/overwrite it. (`check-before-mutate`
  parity with Redis.) `src/command/string/string_read.rs` (GETDEL), `string_write.rs` (SET..GET, GETSET).
- **Integer overflow (P0):** `DECRBY key i64::MIN` must not panic on `checked_neg`; the expire family
  (`SET EX/EXAT`, `SETEX`, `EXPIRE`/`EXPIREAT`/`PEXPIRE`) must reject seconds/ms that overflow
  `i64 * 1000 + now` with an "invalid expire time" error instead of wrapping to a bogus TTL.
- **Expire-in-the-past semantics (P0):** `EXPIRE`/`PEXPIRE`/`EXPIREAT` with a non-positive / already-past
  time DELETES the key and returns 1 (Redis parity), and that deletion PROPAGATES through command-based
  WAL replay to replicas (`DispatchReplayEngine::replay_command` re-dispatches the raw command).
- **MSETNX (P0, missing command):** add MSETNX as an atomic multi-key write (set all iff none exist).
  Single-shard: two-phase check-then-set with no await (atomic). Multi-shard: the coordinator REJECTS a
  cross-shard span with CROSSSLOT (by design — no 2PC), runs co-located keys atomically on the owner.

Out:
- **P1 semantics polish (future milestone):** SET-option mutual-exclusion validation (EX+PX, NX+XX),
  SCAN delete-stability under rehash, TOUCH access-time bump, stricter integer parse (leading `+`,
  whitespace), TTL rounding parity. Advisor-endorsed P0/P1 split — these are correctness *refinements*,
  not data-loss/crash bugs.
- **p=1 throughput vs Redis:** the KV deep review CLOSED this as platform/environment-driven (same
  build wins p=1 on OrbStack, loses on GCloud shared-vCPU); not a KV-code defect, not in scope.
- **Cross-shard MSETNX atomicity via 2PC:** explicitly rejected (CROSSSLOT) rather than half-built.

## Shared decisions & glossary deltas   (living — every task must honor these)
- **check-before-mutate:** any command that could destroy data on a type mismatch must peek the entry
  type and return WRONGTYPE BEFORE the mutation — never remove/overwrite first.
- **TDD red/green (CLAUDE.md Rule 3):** each defect lands a FAILING test first, then the fix. The
  cross-shard MSETNX reject was red/green-proven by neutralizing only the CROSSSLOT branch.
- **Overflow is an error, not a panic/wrap:** integer/time arithmetic on the command path uses
  `checked_*` and returns a `Frame::Error`, never `unwrap()`/silent wrap (CLAUDE.md error-handling).
- **Replay consistency:** semantic changes to write commands must be verified to propagate through
  command-based WAL replay (a replay test), so master and replica converge.
- **New commands carry script coverage:** MSETNX added to `scripts/test-consistency.sh` (hash-tagged
  for 1/4/12-shard parity with Redis) + `scripts/test-commands.sh` (CLAUDE.md New Commands rule).

## Shared / risky contracts (freeze these first)
- **MSETNX cross-shard disposition** — reject (CROSSSLOT) vs best-effort scatter vs 2PC. Chosen: REJECT
  (user decision). A wrong choice here re-does the coordinator + the command's whole test surface.
  -> owning task `kv-msetnx-atomic`
- **Expire-past delete + replay** — deleting on past-time must replay identically or replicas diverge.
  -> owning task `kv-expire-past-deletes`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [x] kv-wrongtype-guard        depends-on: none              — GETDEL / GETSET / SET..GET check type
      before mutate; wrong-type returns WRONGTYPE and preserves the key (no silent data loss).
- [x] kv-integer-overflow-guard depends-on: none              — DECRBY i64::MIN + the expire family
      (SET EX/EXAT, SETEX, EXPIRE/EXPIREAT/PEXPIRE) reject overflow with an error instead of panic/wrap.
- [x] kv-expire-past-deletes    depends-on: none              — EXPIRE/PEXPIRE/EXPIREAT with a past/
      non-positive time deletes the key (returns 1); verified to propagate through WAL replay.
- [x] kv-msetnx-atomic          depends-on: none              — add MSETNX; atomic on one shard,
      CROSSSLOT-reject across shards, co-located keys atomic on the owner.

## Exit criteria (observable; map each to the task that delivers it)
- [x] `GETDEL`/`GETSET`/`SET k v GET` on a key holding a list/hash returns WRONGTYPE and the key still
      exists afterward (no data loss) — unit tests assert key preserved.        (← kv-wrongtype-guard)
- [x] `DECRBY k -9223372036854775808` and `SETEX`/`SET … EX <huge>` return an error, not a panic or a
      wrapped TTL — unit tests for min-overflow + expire overflow.              (← kv-integer-overflow-guard)
- [x] `EXPIRE k -1` (and past `EXPIREAT`) deletes k and returns 1; a WAL-replay test proves the delete
      re-applies on replay (master/replica consistent).                          (← kv-expire-past-deletes)
- [x] Co-located `MSETNX` is atomic (all-new→1, any-exists→0 with no partial write); a cross-shard
      `MSETNX` returns CROSSSLOT and writes nothing — unit + `--shards 4` integration test.  (← kv-msetnx-atomic)

## Close — ship review   (AI fills when every task is done — the evidence behind the engine gate, read before the boxes are checked)
> Whole-milestone, cross-task review the AI fills in. It is the evidence behind the EXISTING engine
> gate (milestone-done / checking the Exit-criteria boxes) — NOT a new approval. Tool-agnostic.

### Ship by domain   (what changed, per bounded context)
- command : `src/command/string/{string_read,string_write,mod}.rs` (GETDEL/GETSET/SET..GET/DECRBY/
  SETEX/SET-EX guards + `msetnx` handler + unit tests), `src/command/key.rs` (expire-past delete +
  overflow guards + tests), `src/command/metadata.rs` (MSETNX phf entry), `src/command/mod.rs`
  ((6,'m') MSETNX dispatch).
- shard   : `src/shard/coordinator.rs` (`coordinate_msetnx` — CROSSSLOT reject / owner-atomic),
  `src/server/conn/shared.rs` (`is_multi_key_command` MSETNX arm).
- persistence : `src/persistence/replay.rs` (expire-past-delete replay propagation tests).
- scripts : `scripts/test-consistency.sh` + `scripts/test-commands.sh` (MSETNX entries).
- tests   : `tests/msetnx_cross_shard_reject.rs` (new `--shards 4` regression suite).

### Cross-task evidence   (one row per task)
- kv-wrongtype-guard        : gate=PASS · commits 4a2245b + 5044487 · tests=3 unit (key-preserved) green
- kv-integer-overflow-guard : gate=PASS · commits 56008df + 9a915d3 + d6b4136 (i64-domain bound — review Finding 2/3) ·
      tests=3 unit (min-overflow, SETEX, SET EX) + 9 unit (i64-bound reject + extreme-negative preserve) green
- kv-expire-past-deletes    : gate=PASS · commit 56008df · tests=unit + 2 replay-propagation green
- kv-msetnx-atomic          : gate=PASS · commit 3b2c7dc · tests=3 unit + 2 integration (--shards 4, red/green-proven) green

### Adversarial code review   (whole-diff, senior-rust-engineer agent — **SHIP** verdict)
The five milestone commits deliver their stated fixes with no new regressions or reachable
panics. Three findings surfaced (all independently re-verified against source before acting):
- **Finding 2/3 (MEDIUM/LOW) — FIXED in `d6b4136`.** The overflow guards bounded against
  `u64::MAX`, not Redis's effective `i64::MAX`; a huge-but-sub-`u64` TTL (e.g. `EXPIRE k
  15000000000000000`) was accepted and then read back as a **negative** `PTTL`/`PEXPIRETIME`
  on a live key, and an extreme-negative `EXPIRE` deleted instead of erroring. Now bounded to
  the i64 domain at all eight expiry setters via a shared `expiry_ms_in_range` helper (red/green,
  9 tests). This makes the "not a wrapped TTL" exit criterion hold on the READ path too.
- **Finding 1 (HIGH) — PRE-EXISTING, DEFERRED (out of v3-4 scope).** The cross-shard coordinator's
  LOCAL leg (`run_local`, `coordinate_mset` fast-path) executes co-located multi-key writes in
  memory but never appends them to the local shard's WAL — while the REMOTE leg (`MultiExecute` in
  `spsc_handler`) does (`wal_append_and_fanout`). So a co-located `MSET`/`MSETNX` is durable when the
  owner is a *remote* shard but silently **non-durable** when the owner is the connection's *own*
  shard. Verified end-to-end: `run_local`→`cmd_dispatch`→`db.set_string` are all no-WAL, and both
  handler call sites (`handler_monoio/dispatch.rs`, `handler_sharded/mod.rs`) `push(response)` then
  `continue`/`return` with no `try_send_append_durable`. **Blast radius:** multi-shard (`--shards >1`)
  **+ appendonly + keys hashing to the connection's own shard**; single-shard (the recommended default)
  is unaffected — the coordinator is bypassed for `num_shards<=1` and normal dispatch DOES persist.
  **Not introduced by MSETNX** (it copied the existing MSET coordinator pattern; MSETNX's exit
  criterion is *atomicity*, delivered — not durability). Fixing it means plumbing the persistence
  context (WAL writer / repl backlog / AOF pool) into the coordinator local path + a crash-recovery
  test — a dedicated P0 follow-up, tracked separately (disposition confirmed with the user).

### Goal met?   (map the evidence back to this milestone's Exit criteria — read before the Exit-criteria boxes are checked)
- [x] each Exit criterion above is satisfied by a Cross-task evidence row or a Ship-by-domain change (cited inline)
- goal: KV writes now honor Redis data-integrity contracts — full default lib suite 3633 green + tokio
  feature set green + both clippy gates + fmt clean; no wrong-type deletes, no overflow panics/wraps,
  past-expire deletes (and replays), MSETNX atomic with cross-shard reject.

## Release steps   (AI-DEFINED — fill the ordered steps to ship this milestone; engine records, human gate)
> The AI writes the release steps for THIS milestone here (hints, not engine commands). MERGE is one
> small step among them. These feed the release scope (release.md) when the cut is bundled.
- [ ] Add a CHANGELOG `[Unreleased]` entry (### Fixed — KV data-integrity; ### Added — MSETNX) — CI Lint gate.
- [ ] Open a PR from `fix/v3-4-kv-correctness` using the Close ship-review above; human reviews + merges.
- [ ] `add.py milestone-done v3-4-kv-correctness` once the Exit-criteria boxes are verified.
- [ ] Fold milestone deltas into the foundation on merge (per project convention); bundle into the next release cut.
