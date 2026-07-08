---
title: "Isolation semantics"
description: "What Moon's multi-tenancy mechanisms guarantee, and what they don't."
---

# Isolation semantics

Moon has three overlapping mechanisms that look like "isolation" from a
distance but each guarantee something narrower than the word implies:
**logical databases** (`SELECT 0..N`), **workspaces** (`WS AUTH`), and
**per-db resource quotas** (`db-maxmemory`). This page states plainly what
each one promises, what it does not, and every limit found during the
WS5b hardening sweep (2026-07). None of these are marketing claims —
if a guarantee isn't listed here, don't assume it holds.

## Logical databases (`SELECT`)

- **Guarantee**: keys in db N are never visible to a client selected into
  db M ≠ N via normal KV commands (GET/SET/KEYS/SCAN/etc.).
- **Not a security boundary**: any authenticated connection can `SELECT`
  to any db (0..`--databases`). There is no ACL concept of "this user may
  only touch db 3." Use workspaces (below) or ACL key patterns for real
  tenant separation.
- **`FLUSHDB` is whole-db, not workspace-scoped** (see Workspaces below) —
  it clears every key in the selected db, including keys belonging to
  every workspace that happens to be co-resident in that db. This is
  pinned by `test_workspace_flushdb_is_whole_db_not_workspace_scoped` in
  `tests/workspace_integration.rs` as intentional, current behavior, not
  a bug — workspaces layer a key-prefix on top of a shared db, they do
  not carve out a separate db per workspace.
- **`FLUSHALL`/`FLUSHDB` and `FT.*` indexes**: index *contents* are
  cleared keyspace-globally (every index, every db) on FLUSHALL/FLUSHDB,
  while the index *definition* (`FT.CREATE`) survives — this matches
  moon's restart-reload semantics. See "FT.* / vector indexes" below.

## Workspaces (`WS AUTH`)

- **Guarantee**: once a connection runs `WS AUTH <id>`, every key argument
  it sends is transparently rewritten with a `{ws_hex}:` hash-tag prefix
  before dispatch, and stripped back off in KEYS/SCAN/RANDOMKEY/FT.SEARCH
  responses. Two workspaces with colliding logical key names
  (`user:1` in workspace A and workspace B) do not collide in storage or
  in KEYS output — verified by
  `test_workspace_keys_no_cross_workspace_leakage`.
- **Composes orthogonally with `SELECT`**: `WS AUTH` and `SELECT` are
  independent connection-state. A workspace-bound connection can still
  `SELECT` to any db; its keys land in whichever db was selected at
  write time, still under its `{ws_hex}:` prefix. This is intentional —
  workspaces are a keyspace partition, not a db partition — but it means
  **`WS DROP`'s cleanup must sweep every db, not just db 0**, which was a
  real bug (see below).
- **`WS DROP` cascade-delete gap (fixed in this branch)**: `WS DROP`'s
  best-effort key cleanup (`handler_monoio/write.rs`,
  `handler_sharded/write.rs`, `spsc_handler.rs`'s `WsDropCleanup`) only
  swept db 0 via a hardcoded `with_shard_db(0, ...)` call. A workspace
  connection that ever `SELECT`ed to a non-zero db before writing would
  leak its keys **forever** after `WS DROP` — they were orphaned, prefixed
  with a workspace id nothing could ever `WS AUTH` into again (workspace
  ids aren't reusable). Found via `git apply -R` RED/GREEN TDD with
  `test_workspace_drop_cleans_keys_across_all_dbs`; fixed by sweeping
  `s.databases.iter_mut()` at all three call sites. **This affects every
  moon release before this fix** — operators running workspaces with
  connections that `SELECT` non-zero dbs should treat prior `WS DROP`
  calls as having potentially leaked keys, recoverable only via manual
  `SCAN` for the `{ws_hex}:*` prefix pattern.
- **`WS DROP`'s all-dbs sweep is synchronous and O(total keys × --databases)
  on the owning shard's event-loop thread.** The fix above trades a
  permanent leak for a full linear scan of every key in every logical db on
  that shard, run inline (no yield points) during `WS DROP`'s handling —
  it blocks that shard thread for the duration, i.e. it stalls every other
  connection pinned to the same shard while it runs. At the default
  `--databases 16` and typical workspace-sized keyspaces this is
  sub-millisecond and not worth optimizing; it becomes a real latency spike
  on a shard holding a very large keyspace (millions of keys) combined with
  a large `--databases` count. `WS DROP` is an admin-rare operation (create
  a tenant once, drop it once), so this is an accepted trade-off, not
  scheduled for a fix — flagging it here so a large-`--databases`,
  large-keyspace deployment doesn't discover it as a surprise production
  latency blip.
- **`FLUSHDB` does not respect workspace boundaries** — see above.
- **Not a security boundary on its own**: `WS AUTH` requires knowing the
  workspace's UUID v7. There is no password/ACL gate on `WS AUTH` itself
  in this release — anyone who can open a connection and knows (or
  guesses) a workspace id can bind to it. Pair with ACL `requirepass` /
  TLS client-cert auth for real tenant boundaries; workspaces solve
  **keyspace collision**, not **authentication**.

## Per-db resource quotas (`db-maxmemory`, new in this branch)

- **Config surface**: `--db-maxmemory <db>:<bytes>` (repeatable CLI flag)
  and `CONFIG SET db-maxmemory <db> <bytes>` (0 = unlimited, the
  default for every db). `CONFIG GET db-maxmemory` lists only the
  nonzero entries.
- **Guarantee**: when db N's estimated memory is at or above its quota
  and the effective eviction policy is `noeviction`, writes that would
  grow db N's memory are rejected with a `MOONERR db maxmemory exceeded`
  error, without touching any other db's memory or quota. Sibling dbs
  are unaffected — `neighbor_db_is_unaffected_by_sibling_quota` covers
  this. Under an eviction policy (`allkeys-lru`, etc.) db N sheds its
  own keys to get back under quota instead of rejecting.
- **Zero cost when unset**: the enforcement path is gated by a single
  process-wide `AtomicBool` (`DB_MAXMEMORY_ANY_SET`) published whenever
  `--db-maxmemory`/`CONFIG SET db-maxmemory` change the config; if no db
  quota is ever configured, every hot-path check is a single relaxed
  atomic load, mirroring the existing global-`maxmemory` pre-gate
  pattern in `src/storage/eviction.rs`.
- **`SELECT`/`SWAPDB` are exempted from the on-write quota check.**
  Moon's command-metadata table flags `SELECT` and `SWAPDB` as
  "write-fast" (`WF`) for ACL/dispatch-classification reasons unrelated
  to memory growth, which means they flow through the *same*
  write-path eviction gate as a real `SET`. Without an exemption, a
  connection that fills a `noeviction` db to its quota could not even
  `SELECT` away from that db on the same connection afterward — the
  gate ran using the pre-switch db index before the SELECT itself
  updated connection state, so a full db effectively **trapped the
  connection**. This was caught by a real-server repro (raw socket
  script driving `SELECT 1` → writes to exhaustion → `SELECT 0` →
  observed the db-1 quota error instead of `+OK`) and fixed via
  `db_quota::command_exempt_from_db_quota()` +
  `check_db_maxmemory_for_command()`. A fresh connection starting at
  db 0 was never affected — this was a same-connection state artifact,
  not a global lock.
  - **Known, deliberately unfixed twin**: the pre-existing *global*
    `--maxmemory` gate has the identical quirk (SELECT is also
    `is_write`-flagged there) and was NOT touched by this branch — fixing
    it would mean changing widely-used, shared eviction-gate code well
    beyond per-db quotas' scope. Filed as a follow-up, not fixed here.
- **`MOVE` is not covered by the immediate on-write quota check** — a
  `MOVE` into a quota'd db does not re-check that db's quota synchronously
  (the write-path gate only covers the *originating* db of the command
  being dispatched). The periodic background sweep in
  `src/shard/timers.rs::run_eviction()` (runs every eviction tick,
  gated by the same zero-cost atomic) catches this lazily — a db that
  drifts over quota via `MOVE`/`SWAPDB` gets reconciled on the next
  tick, not instantaneously.
- **No disk-offload spill integration**: db-quota eviction always
  deletes the victim key outright (mirrors
  `eviction::evict_one_with_spill` called with `None` for the spill
  sender). Global `--maxmemory` eviction can spill to disk-offload
  storage when configured; per-db quota eviction cannot. A db under
  quota pressure with `allkeys-lru` will lose data it could otherwise
  have kept cold-tiered under the global gate. Document this before
  recommending db-quotas as a substitute for disk offload.
- **Eviction candidate sampling is a pre-existing, unrelated
  limitation**: `eviction::sample_random_keys`/`find_victim_random` uses
  a fixed 1-sample/8-attempt retry budget regardless of the configured
  `maxmemory-samples`. At very low live-key counts under `allkeys-random`
  this can return an OOM error even though technically-evictable keys
  remain, because the bounded retry gives up first. Not introduced or
  fixed by db-quota work; noted here because it is easy to mistake for a
  db-quota bug when writing tests against a small dataset.
- **Non-inline write commands were bypassing the quota gate entirely**
  (fixed): `--maxmemory 0` combined with no disk-offload spill sender
  (e.g. `--disk-offload disable`) meant `handler_monoio`'s
  `batch_eviction_active` (and the cross-shard-leg twin `spsc_handler`'s
  `evict_active`) never ran the write-eviction-gate call at all for any
  command other than the byte-level inline GET/SET fast path — so HSET,
  LPUSH, SADD, ZADD, INCR, APPEND, MSET, SET-with-options, RESTORE, and
  every other non-inline write silently ignored a configured db quota.
  Found via adversarial review, fixed by adding
  `db_quota::db_maxmemory_any_set()` to both conditions (mirroring the Lua
  bridge's gate, which already had this term). Covered by
  `test_quota_rejects_non_inline_writes_without_spill_sender` in
  `tests/db_maxmemory_quota.rs`.
- **Container-growth memory accounting gap (WS6, fixed)**: `used_memory`
  used to be charged once, at key-creation time, for an empty container's
  fixed overhead (`entry_overhead()` in `src/storage/db.rs`, called
  immediately after `Entry::new_hash()`/equivalent, before any
  fields/elements were inserted). Every subsequent mutation of that SAME
  key — `HSET`'s `map.insert(field, value)`, and the equivalent
  direct-collection-mutation pattern in List/Set/ZSet write commands —
  never updated `used_memory` again, so growing one hash key's fields
  never grew its accounted memory, defeating both the global
  `--maxmemory` gate and the per-db quota above identically. Fixed via
  O(1) per-mutation delta accounting (`Database::charge_memory` /
  `credit_memory`, plus per-container byte-cost helpers
  `hash_field_cost`/`list_elem_cost`/`set_member_cost`/`zset_member_cost`
  next to `entry_overhead` in `src/storage/db.rs`) rather than a full
  recompute per command — recomputing `RedisValue::estimate_memory()` on
  every mutation would turn an O(1) HSET into O(n) on a large hash. The
  listpack/intset compact encodings use before/after `estimate_memory()`
  snapshots instead (already O(1) there — capacity-based). Covered by
  `tests/container_growth_memory_accounting.rs` (HSET/LPUSH growth trips
  `--maxmemory`; HDEL correctly credits deleted fields back) plus unit
  tests in each container command family's `mod.rs`. The eviction loop's
  before/after `estimated_memory()` delta arithmetic needed no changes —
  it was already reading the (now-correct) live accumulator.
  - **Self-inflicted write lockout (WS6, fixed — adversarial review
    2026-07-08, HIGH)**: making container growth visible to `used_memory`
    (above) made a second, previously-unreachable bug reachable:
    `run_write_eviction_gate` / `check_db_maxmemory_for_command` applied
    the noeviction reject to *every* write command uniformly, so once a
    key's growth tripped `--maxmemory` or a db's `--db-maxmemory` quota,
    a pure-shrink command on that SAME key or db — `HDEL`, `SREM`,
    `LPOP`, `ZREM`, ... — was *also* rejected. A tenant that grew a key
    past the boundary had no self-recovery path short of `FLUSHALL` or a
    restart, undermining the WS5b per-db-quota guarantee. Fixed with a
    static, provably shrink-only command classification
    (`db_quota::is_shrink_only_command` in `src/storage/db_quota.rs`),
    mirroring Redis's `CMD_DENYOOM` semantics:
    `DEL`/`UNLINK`, `HDEL`/`HGETDEL`, `SREM`/`SPOP`, `LPOP`/`RPOP`/`LREM`/
    `LTRIM`/`LMPOP`/`BLMPOP`, `ZREM`/`ZPOPMIN`/`ZPOPMAX`/`ZMPOP`/
    `BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`ZREMRANGEBYSCORE`/`ZREMRANGEBYRANK`/
    `ZREMRANGEBYLEX`, `GETDEL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/
    `PEXPIREAT`/`PERSIST`, `FLUSHDB`/`FLUSHALL` bypass the *reject* from
    both the global maxmemory gate and the per-db quota gate (eviction is
    still attempted first — an evicting policy may as well reclaim while
    the write lock is held; only the reject is skipped). Deliberately
    conservative allow-list — commands that can grow a destination key
    (`LMOVE`/`SMOVE`/`COPY`/`RESTORE`/any `*STORE` variant) or aren't
    statically classifiable (`SET`, even with a shorter value) are
    excluded. Applied at all three connection-handler call sites
    (`handler_monoio::run_write_eviction_gate`, the inline block in
    `handler_sharded`, and both inline blocks in `handler_single`).
    Covered by `test_hdel_self_recovery_past_maxmemory_boundary` and
    `test_hdel_self_recovery_past_db_maxmemory_boundary` in
    `tests/container_growth_memory_accounting.rs` — each grows a key past
    its cap (asserting the growing write IS rejected), then asserts an
    `HDEL` on that same over-cap key succeeds, then asserts a follow-up
    write succeeds once back under budget.

## FT.* / vector indexes and workspaces (handoff note — WS5a scope)

**This branch (WS5b) does not modify `src/vector/`,
`src/command/vector_search/`, or any FTS code** — that surface is owned
by the concurrent WS5a workstream (db-scoped FT indexes). What follows
is an observational finding for WS5a to fold in, not a WS5b fix:

- As of this writing, `FT.*` indexes are **keyspace-global**, not
  workspace-scoped or (pre-WS5a) db-scoped: `FLUSHALL`/`FLUSHDB` clear
  every index's contents regardless of which db or workspace triggered
  the flush (see the `Vector Search` section of the project root
  `CLAUDE.md`, "FLUSHALL/FLUSHDB/HDEL keyspace parity"). A workspace's
  `WS AUTH`-injected key prefix does reach `FT.SEARCH`/auto-indexing (the
  prefix is applied before dispatch like any other command), so two
  workspaces indexing hashes under logically-identical field names do
  get distinct, non-colliding *entries* keyed by their distinct prefixed
  keys — but they share the *same index definition and segment set*.
  There is currently no notion of "workspace A's FT index" vs
  "workspace B's FT index" as separate objects; a workspace-scoped
  `FT.DROPINDEX` or `FT.SEARCH` cannot avoid touching sibling workspaces'
  documents in the same index, and `FT.INFO num_docs` reports the
  combined total across all workspaces.
- Recommendation for WS5a: if db-scoped FT indexes land, the natural
  follow-up is workspace-scoped indexes gated the same way per-db quotas
  are — an explicit index-creation-time association plus a cheap
  zero-cost-when-unused check, not a blanket prefix-filter over search
  results (which would break HNSW/TQ recall accounting per
  `CLAUDE.md`'s vector search notes on segment-level `num_docs`).
- No code changes were made on the WS5b side to accommodate or preempt
  this; it is purely an observation for the other workstream.

## Summary table

| Mechanism | Guarantees | Does NOT guarantee |
|---|---|---|
| `SELECT` (logical db) | Keys in db N invisible to db M via normal KV ops | Auth/ACL boundary; `FLUSHDB` still whole-db |
| Workspaces (`WS AUTH`) | No keyspace collision between workspaces, even same db | Auth boundary (no password on `WS AUTH`); not `FLUSHDB`-safe; `FT.*` indexes not workspace-scoped |
| `db-maxmemory` quota | Per-db memory ceiling, independent of sibling dbs, zero-cost when unset, covers ALL write commands (inline and non-inline alike, including RESTORE) | Not spill-integrated; `MOVE`/`SWAPDB` reconciled lazily not synchronously; shares the SELECT-exemption quirk with global maxmemory (both now fixed for db-quota, global left as-is); does not see memory growth from mutating an EXISTING Hash/List/Set/ZSet key (pre-existing, systemic, also affects global `--maxmemory`) |
