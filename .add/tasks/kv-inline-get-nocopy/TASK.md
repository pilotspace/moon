# TASK: Inline GET writes the value once — drop the `val.to_vec()` double-copy

slug: kv-inline-get-nocopy · created: 2026-07-03 · stage: production
phase: tests   <!-- specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
milestone: v3-3-vector-kv-polish · depends-on: none  (ships as a standalone follow-up PR)

> **STATUS: SCOPED, not built.** §1 SPECIFY → §3 CONTRACT and the §4 test-plan are authored here
> for a standalone follow-up PR (this task is `depends-on: none`, so it does not need the rest of
> v3-3 activated). Resume at §4: write the RED alloc-probe first, then §5 BUILD. Source of the
> finding: `tmp/KV-DEEP-REVIEW.md` §4 Fix #1 (found independently by two review agents).

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: the inline (shards=1 / local-key fast-path) `GET` hit copies the value **twice** —
`val.to_vec()` allocates a fresh heap `Vec` to escape the thread-local `RefCell` borrow
(`src/server/conn/blocking.rs:1275`), then `write_buf.extend_from_slice(&val)` copies it again into
the codec buffer (`:1290`). Frame the `$<len>\r\n<bytes>\r\n` reply **inside** the `with_shard_db`
closure, writing straight from the borrowed `&[u8]` into `write_buf`, and drop the intermediate `Vec`.
One copy instead of two, and **zero** hot-path heap allocation on the GET-hit path.

Why it's real (evidence): `with_shard_db<R>(idx, f: impl FnOnce(&mut Database) -> R) -> R` is
**synchronous** (a thread-local slice borrow, no `.await`), and `write_buf` is disjoint from the
thread-local `ShardSlice` — so the closure can capture `&mut write_buf` and write the response while
the value borrow (`entry.value.as_bytes() -> Option<&[u8]>`) is still alive. The `to_vec()` exists
only to move bytes past the closure boundary; framing inside removes that need.

Framings weighed: **frame-inside-closure, single copy** (chosen — removes the alloc *and* a copy,
matches how the cold-miss and WRONGTYPE arms already sit relative to the borrow) · return a
`Cow`/`Bytes` from the closure (rejected — `Bytes::copy_from_slice` still copies once to own; no
better than one `extend_from_slice`, and adds a type) · leave it (rejected — violates the CLAUDE.md
"no `to_vec` on the command/io hot path" invariant and the wasted copy scales with value size).

Must:
<must>
  - M1 One copy, zero alloc: the inline GET **hit** frames the reply from the borrowed `&[u8]` inside
    the `with_shard_db` closure; `val.to_vec()` is gone from `blocking.rs` (grep-auditable). The only
    value copy is `write_buf.extend_from_slice(val)`.
  - M2 RESP byte-parity: every GET outcome is byte-identical to today — hit (value sizes
    {0, 1, 12, 13, 65536}), WRONGTYPE, miss→`$-1`, miss→cold-hit, miss→cold-wrongtype. The cold-miss
    arm (`blocking.rs:1298-1317`) is **unchanged** (its `v` is already owned — a different concern).
  - M3 Allocation proven gone: a unit test with a counting allocator asserts the inline GET-hit path
    performs **0** value-sized heap allocations — RED before the fix (`to_vec` allocs), GREEN after.
    (This IS the milestone exit criterion, not an extra.)
  - M4 Both runtimes compile: the change is in `blocking.rs` (defined unconditionally) and MUST build
    under default (monoio) **and** `--no-default-features --features runtime-tokio,jemalloc`.
    Production reachability of the inline path is **monoio-only** (see Assumptions) — documented, not
    a tokio regression.
</must>
Reject:
<reject>
  - any RESP byte-change on any GET path (hit/wrongtype/miss/cold) -> "behavior_regression" (perf-only; parity frozen)
  - moving buffer bookkeeping (`read_buf.split_to(consumed)`, the `return 1`/return codes) or the cold
    read INTO the closure, OR any code that re-enters `with_shard`/`with_shard_db` while the closure
    borrow is held -> "double_borrow_or_scope_creep" (thread-local `RefCell` reentrancy = panic)
  - a new hot-path allocation, a new `unsafe` block, or widening scope to fixes #2–5 -> "violates_conventions"
</reject>
After:
<after>
  - `val.to_vec()` no longer appears on the inline GET hit path (grep); exactly one `extend_from_slice` copy remains.
  - The alloc-probe unit test is green; the 9 existing `try_inline_dispatch` unit tests + `scripts/test-consistency.sh` GET rows stay green; both-runtime CI green.
  - A 64KB-value GET bench (monoio, moon-dev or GCloud) is recorded before/after; small-value runs are stated as expected-null (below the ±5–10% bench-noise floor), NOT a regression.
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **No counting-allocator test harness exists yet** (grep-confirmed: only `.planning/` design
    sketches of a `TrackingAllocator` + main.rs's mimalloc/jemalloc `#[global_allocator]`, which is in
    the *binary*, not the lib). Building a `#[cfg(test)]` global counting allocator (atomic/thread-local
    wrapper delegating to `System`, in the LIB test build since `try_inline_dispatch` is `pub(crate)`)
    is IN SCOPE as a dependency. Lowest confidence because it is shared: `kv-incr-itoa` and
    `vector-search-keyhash-noclone` need the same probe — author it to be reused, and it belongs in
    whichever of the three lands first. If wrong / too heavy: fall back to a behavioral proxy, weaker,
    not recommended (the exit criterion explicitly wants an allocation probe).
  ⚠ Capturing `&mut write_buf` in the closure alongside the `&mut Database` borrow: expected fine
    (disjoint), but confirm at build the outcome enum shape carries `cold_loc` for the miss arm without
    a borrow-checker conflict.
  - [ ] The **general Frame-dispatch GET** path (tokio, and non-inline monoio) is OUT of scope and has
    NOT been assessed for an analogous copy — a sibling follow-up may be warranted; do not assert it is clean.
  - [ ] Confirm the inline unit tests + the new alloc-probe compile/run under both `cargo test` (monoio
    default) and the tokio feature build; if `try_inline_dispatch` is only reachable under monoio in the
    test tree, gate the probe to that runtime.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first. -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: inline GET hit is byte-identical across the SSO boundary (M1, M2)
  Given a shards=1 server with keys holding values of size 0, 1, 12, 13, and 65536 bytes
  When each key is fetched via the inline dispatch path
  Then each reply is exactly "$<len>\r\n<bytes>\r\n" — byte-identical to the pre-change output
  And the 12B (CompactValue inline max) and 13B (heap) cases both round-trip correctly

Scenario: inline GET hit performs no intermediate value allocation (M3 — the exit criterion)
  Given the counting allocator is active in the test build
  And a key holding a 64KB string value
  When a single inline GET hit is dispatched
  Then the number of heap allocations sized to the value is 0 (RED before fix: >= 1 from to_vec)
  And the returned bytes equal the stored value

Scenario: non-hit GET arms are untouched (M2)
  Given keys that are absent, hold a wrong type, or live only in the cold tier
  When each is fetched via inline dispatch
  Then miss returns "$-1\r\n", wrong-type returns the WRONGTYPE error, and a cold hit returns the value
  And all three are byte-identical to today (the cold-read arm is not moved into the closure)

Scenario: both runtimes build (M4)
  Given the fix in blocking.rs
  When the crate is built with default features (monoio) and with runtime-tokio,jemalloc
  Then both compile; the inline path remains reached only by the monoio handler in production

Scenario: no reentrancy panic (Reject: double_borrow_or_scope_creep)
  Given the response framing now runs inside the with_shard_db closure
  When a GET hit is served
  Then the closure only writes response bytes + itoa and returns the outcome enum
  And it never calls with_shard/with_shard_db again (no thread-local RefCell double-borrow)

Scenario: parity frozen (Reject: behavior_regression)
  Given the change applied
  When scripts/test-consistency.sh runs GET at 1/4/12 shards and CI runs both runtimes
  Then all GET rows pass and CI is green
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
No new wire surface. The contract is invariants over the existing GET surface.

WIRE (frozen, byte-parity): "$<len>\r\n<bytes>\r\n" for a hit · "$-1\r\n" for a miss ·
  "-WRONGTYPE ...\r\n" · cold-tier hit/wrongtype — all byte-identical to pre-change.

INTERNAL SHAPE (the change), src/server/conn/blocking.rs, inside try_inline_dispatch GET arm:
  BEFORE: enum GetResult { Found(Vec<u8>), WrongType, Miss }
          closure returns Found(val.to_vec())         // COPY 1 (heap alloc)
          outside: write_buf.extend_from_slice(&val)   // COPY 2
  AFTER:  enum GetOutcome { Handled, Miss(Option<ColdLoc>) }
          closure, on hit: write_buf.extend_from_slice(b"$") + itoa(len) + CRLF
                           + write_buf.extend_from_slice(val)   // the ONE copy, straight from &[u8]
                           + CRLF ; returns Handled
          closure, on wrong-type: writes the WRONGTYPE error ; returns Handled
          closure, on absent: returns Miss(db.cold_lookup_location(key))
          OUTSIDE the closure (unchanged): the cold read (read_cold_entry_at), the nil/cold framing,
                           read_buf.split_to(consumed), and the return codes.

INVARIANTS: the closure does buffer writes + itoa ONLY — it must not re-enter with_shard* (RefCell
  reentrancy panic). write_buf (&mut Vec<u8>) is disjoint from the thread-local ShardSlice.

ERROR CODES (test-visible): behavior_regression · double_borrow_or_scope_creep · violates_conventions

Schema: no storage/WAL/persistence change. Touched:
  src/server/conn/blocking.rs (GET arm) · src/server/conn/tests.rs (size-matrix + alloc-probe) ·
  a shared #[cfg(test)] counting-allocator harness module (NEW, if it lands here first).
```

Status: FROZEN @ v1 — build directive "a→b: freeze then build red→green" (Tin Dang), 2026-07-03.

Least-sure flag surfaced at freeze:
- ⚠ [test-infra] the counting-allocator harness is a shared dependency (M3 assumption #1). Freeze
  decision: **this task OWNS and builds it** — a `#[cfg(test)] #[global_allocator]` counting allocator
  in the lib, reusable by kv-incr-itoa + vector-search-keyhash-noclone. Rust std exposes no stable
  allocation-count API, so this requires a **test-only `unsafe impl GlobalAlloc`** (standard System
  delegation) → gated on the CLAUDE.md unsafe approval before §5 build lands it. Without approval, M3
  falls back to the weaker bench + `to_vec`-gone grep audit (exit criterion partially met).

<!-- Changing this frozen contract = change request back to SPECIFY. -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: M1/M2 by byte-parity pins across the size matrix; M3 by the counting-allocator
RED→GREEN probe (the exit criterion); M4 by the both-runtime CI matrix at verify.
Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - src/server/conn/tests.rs (unit — alongside the existing 9 try_inline_dispatch tests):
    - get_hit_byte_parity_sizes — PIN green: for value sizes {0, 1, 12, 13, 65536}, assert the exact
      "$<len>\r\n<bytes>\r\n" bytes land in write_buf via try_inline_dispatch.
    - get_hit_zero_value_alloc — **RED before / GREEN after** (M3): with the counting allocator active,
      one inline GET hit on a 64KB value performs 0 value-sized heap allocations. RED today (to_vec).
    - get_miss_nil / get_wrongtype / cold arms — regression PINs (byte-parity, closure not entered for cold).
  - Shared harness (NEW, if this task lands first): a #[cfg(test)] global counting allocator
    (AtomicUsize alloc/dealloc counters + size filter, delegating to System) reusable by kv-incr-itoa
    and vector-search-keyhash-noclone. If a sibling lands first, import theirs.
  - Parity backstop: scripts/test-consistency.sh GET rows at 1/4/12 shards + both-runtime CI at verify.
</test_plan>

Tests live in: `src/server/conn/tests.rs` (+ shared alloc harness). Red must be confirmed BEFORE §5 build.

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Safety rule (feature-specific): the closure must remain reentrancy-free (no nested with_shard*) and
`.await`-free — buffer writes + itoa only. Do NOT touch the cold-miss arm.
Code lives in: `src/server/conn/blocking.rs` (+ test harness in `tests.rs`).
Constraints: no contract/test change during build; no new unsafe; no new hot-path alloc; both runtimes green; ask if unclear.

Expected impact (state honestly in the PR — do NOT lead with a latency claim):
- Removes one hot-path **heap allocation** + one O(N) memcpy per inline GET hit. Primary justification
  is **CLAUDE.md no-alloc-on-hot-path rule compliance** (and the milestone's stated "honor the no-alloc
  rules" rationale). The throughput/tail benefit **scales with value size** and is measurable only at
  large values (64KB). Per `tmp/KV-DEEP-REVIEW.md` it will **NOT** move the p=1 small-value ratio
  (the copy is <6% of the ~1.8µs GCloud residual; below bench noise at 8–64B). This is a **P2** polish.
Rollback: one behavior-preserving commit — revert restores the `to_vec` path exactly. Design-for-failure:
  no IO / durability / failure semantics touched (pure in-memory read framing).

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [ ] all tests pass (both runtimes) — inline suite + alloc-probe green; lib + tokio+jemalloc matrix green
- [ ] `val.to_vec()` gone from the GET hit path (grep); exactly one value copy remains
- [ ] no RESP byte-change on any GET path (byte-parity pins + consistency GET rows)
- [ ] no reentrancy: the closure never calls with_shard* (code audit)
- [ ] 64KB-value GET bench recorded before/after on monoio (moon-dev / GCloud); small-value expected-null noted
- [ ] a person reviewed and approved the change

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch: 64KB-value GET throughput/tail delta · allocator pressure on large-value read workloads.
Open follow-ups this task surfaces:
- The shared counting-allocator harness, once built, unblocks `kv-incr-itoa` and
  `vector-search-keyhash-noclone` (same exit-criterion probe).
- The general Frame-dispatch GET path (tokio / non-inline) was scoped OUT and NOT assessed — decide
  whether a sibling task should check it for an analogous copy.
