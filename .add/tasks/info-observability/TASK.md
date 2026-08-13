# TASK: INFO sections/fields, keyspace notifications, protocol-error replies

slug: info-observability · created: 2026-08-09 · stage: production
autonomy: auto   <!-- inherited from the project default (PROJECT.md); explicit level: manual < conservative < auto (visible · overridable) — lower below if a high-risk task needs it, or run `add.py autonomy set`. -->
phase: done   <!-- ground -> specify -> scenarios -> contract -> tests -> build -> verify -> observe -> done -->
<!-- high-risk/method-defining scope? declare `risk: high` on the slug line above and lower the
     autonomy level to `manual` or `conservative` — the engine refuses an unguarded completion
     (`unguarded_high_risk_auto`, run.md guard). A comment is never a declaration. -->

> One file = one task. Fill sections top-to-bottom; the `add` skill drives each phase.
> When a phase is unclear, read its book chapter in `.add/docs/` (linked per section).
> The phase marker above is the single source of truth — keep it in sync via `add.py phase`.

---

## 0 · GROUND — the real codebase ▸ docs/02-the-flow.md

Touches (files · symbols · signatures):
  - `src/command/connection.rs:172` — `pub fn info(db: &Database, _args: &[Frame]) -> Frame`
    The single INFO body builder. **`_args` is discarded**: the section argument is
    parsed by nobody, so `INFO replication` returns the entire payload.
  - `src/command/connection.rs:477` — `info_with_keyspace(db, args, keyspace)`
    Calls `info(db, args)`, then string-searches `"# Keyspace\r\n"` and rebuilds the
    tail with accurate cross-shard per-db counts. Any section filter must keep this
    cut working (or the filter must run before the cut, not after).
  - `src/command/connection.rs:506` — `info_readonly(db, args)` — delegates to `info`.
  - Sections Moon emits, in order: Server · Clients · Memory · Persistence · Vector ·
    MoonStore · Stats · CPU · Replication · Commandstats · Keyspace.

Measured (2026-08-13, moon @ 292269ac vs redis-server 8.6.1, INFO all):
  - field count: **Moon 61 · Redis 213**
  - sections Redis has that Moon does not: **Errorstats · Latencystats · Cluster · Modules**
    (also 8.x-only: Threads · Hotkeys · Keysizes — out of scope, milestone targets 7.4)
  - sections Moon has that Redis does not: Vector · MoonStore (additive, no client breaks)
  - `notify-keyspace-events`, `__keyspace@`, `__keyevent@`: **zero occurrences in src/ or
    tests/** — keyspace notifications are entirely unimplemented, not partially.
  - `INFO <section>` filtering: unimplemented (see `_args` above). `INFO all` /
    `INFO everything` likewise unrecognized as distinct from bare `INFO`.

Missing fields that clients and monitoring agents actually read (measured subset):
  - identity/mode: `run_id` · `redis_mode` · `cluster_enabled` · `process_id` · `os` ·
    `arch_bits` · `executable` · `config_file` · `multiplexing_api` · `redis_git_sha1`
  - hit-rate + eviction: `keyspace_hits` · `keyspace_misses` · `expired_keys` ·
    `evicted_keys` · `rejected_connections`
  - memory: `maxmemory` · `maxmemory_policy` · `mem_fragmentation_ratio`
  - rate: `instantaneous_ops_per_sec` · `instantaneous_input_kbps` / `output_kbps`
  - persistence health (what alerts fire on): `rdb_changes_since_last_save` ·
    `rdb_saves` · `aof_last_write_status` · `aof_last_bgrewrite_status` ·
    `aof_rewrites` · `latest_fork_usec`
  - replication/sentinel: `master_replid2` · `master_failover_state` ·
    `repl_backlog_active` / `_size` / `_histlen` / `_first_byte_offset`
  - pubsub: `pubsub_channels` · `pubsub_patterns` · `blocked_clients`
  - `role` is already present.

Context (working folder): `.add/tasks/info-observability/` · compat harness at
  `scripts/client-compat/` (`differ.py`, `manifest.yaml`) is the parity verifier.

Honors (patterns / conventions): INFO is built with `write!` into one pre-sized
  `String` (no per-field allocation); `REDIS_COMPAT_VERSION` is the advertised
  version. New counters must not add a per-command atomic on the hot path.

Anchors the contract cites: `connection.rs:info` · `connection.rs:info_with_keyspace` ·
  `scripts/client-compat/manifest.yaml`.

Note: the third clause of this task's title — "protocol-error replies" — SHIPPED in
  #472 (`protocol-error-lifetime`). It is struck from this task's scope.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: INFO section/field parity + keyspace notifications

Framings weighed:
  - **Two halves, one contract, one PR** (chosen) — INFO parity and keyspace
    notifications ship together. They share the observability surface a
    monitoring agent consumes, and the compat harness gates both.
  - Split into two tasks — rejected by scope decision: the title bundles them.
  - Notifications first — rejected: INFO is the cheaper, lower-risk half and
    de-risks the harness wiring both need.

Must:
<must>
  - INFO with no argument returns every section EXCEPT Commandstats and
    Latencystats (measured against redis-server 8.6.1).
  - INFO <section> returns ONLY that section, header included. Matching is
    case-insensitive (`INFO REPLICATION` == `INFO replication`).
  - INFO accepts MULTIPLE section names (`INFO server clients`) and returns
    exactly those, in the server's canonical order — not the caller's order.
  - INFO all and INFO everything return every section, Commandstats and
    Latencystats included.
  - INFO <unknown-section> returns an EMPTY bulk string, not an error.
  - Every section header appears AT MOST ONCE in one reply. (Moon currently
    emits `# Replication` twice.)
  - INFO reports these fields with values derived from real server state, never
    a placeholder constant: run_id · redis_mode · cluster_enabled · process_id ·
    os · arch_bits · keyspace_hits · keyspace_misses · expired_keys ·
    evicted_keys · rejected_connections · maxmemory · maxmemory_policy ·
    instantaneous_ops_per_sec · blocked_clients · pubsub_channels ·
    pubsub_patterns · total_net_input_bytes · total_net_output_bytes.
  - run_id is a 40-char hex string, stable for a process lifetime and DIFFERENT
    across restarts (clients use it to detect a restart).
  - A field Moon cannot answer truthfully is OMITTED, never emitted as a lie.
  - CONFIG GET notify-keyspace-events returns the active flag string;
    CONFIG SET notify-keyspace-events <flags> updates it at runtime.
  - The flag readback is CANONICALIZED, not echoed: class chars in the order
    `g$lshzxetdmn`, then `K`, then `E`. Measured: `KEA` -> `AKE`, `Kg$` -> `g$K`.
  - With `K` set, a mutating command publishes to `__keyspace@<db>__:<key>`
    with the EVENT NAME as payload.
  - With `E` set, it publishes to `__keyevent@<db>__:<event>` with the KEY as
    payload. The pair is inverted; both fire when `KE` is set.
  - Event names match Redis exactly, including where they differ from the
    command name. Measured: INCR -> `incrby` · SET -> `set` · APPEND -> `append` ·
    DEL -> `del` · LPUSH -> `lpush` · HSET -> `hset` · SADD -> `sadd` ·
    ZADD -> `zadd` · EXPIRE -> `expire` · TTL elapse -> `expired`.
  - RENAME emits TWO events: `rename_from` on the source key and `rename_to` on
    the destination.
  - Notifications reach a subscriber on ANY shard, not only the shard that owns
    the mutated key.
  - The `<db>` in both channel names is the db the write happened in.
</must>

Reject:
<reject>
  - CONFIG SET notify-keyspace-events with a char outside `Ag$lshzxeKEtmdn`
    -> "ERR CONFIG SET failed (possibly related to argument
       'notify-keyspace-events') - Invalid event class character. Use
       'Ag$lshzxeKEtmdn'." (verbatim, measured)
  - A key MISS with only `A` set -> NO event. `m` (keymiss) is deliberately
    NOT a member of the `A` class; emitting on miss would flood every GET.
  - Notifications enabled but neither K nor E set -> NO event. Class flags
    alone select WHICH events, K/E select WHETHER they are delivered.
</reject>

After:
<after>
  - A stock monitoring agent scraping INFO gets hit-rate, eviction, memory and
    persistence-health fields without special-casing Moon.
  - `INFO replication` costs one section's worth of formatting, not thirteen.
  - A cache-invalidation framework subscribed to `__keyevent@0__:del` receives
    events regardless of which shard owns the key.
  - With notifications disabled (the default), the write path does no channel
    formatting and no allocation.
</after>

Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **The event hook can be placed where it sees both the key and the db without
    re-plumbing command signatures.** Lowest confidence because Moon's write
    commands take `&Database` and a key, but the db INDEX and the per-shard
    pubsub registry live on the connection/shard context, not in `Database`.
    If wrong: the hook has to be threaded through every write command's
    signature — a much larger, more invasive diff than this contract assumes,
    and the §5 scope would need to grow to `src/command/**`.
  - [x] Cross-shard notification fan-out can reuse PUBLISH's existing
    `all_pubsub_registries` path rather than needing a new SPSC message class.
    **CONFIRMED** (`handler_monoio/pubsub.rs:60`): PUBLISH resolves targets via
    `ctx.remote_subscriber_map.read().target_shards(&ch)` and dispatches in
    per-shard batches. The outbox drain reuses exactly this — no new message
    class. Filed #474 along the way: the MQ-trigger path
    (`shard/timers.rs:249`) does the LOCAL publish only and so never reaches a
    subscriber on another shard; the same mistake is the one `kn9` guards
    against here.
  - [ ] `instantaneous_ops_per_sec` can be derived from an existing counter
    sampled on the 1s chore, with no new per-command atomic on the hot path.
    Confirm against the existing Stats counters; if it needs a new atomic,
    drop the field rather than pay hot-path cost (Must says omit, never lie).
  - [x] Moon's `# Reclamation` and duplicate `# Replication` come from a path
    outside `connection.rs:info`; the section filter must cover that path too
    or filtering will silently leak sections. **CONFIRMED, and located.**
    `connection.rs:426` writes a STUB `# Replication`, then all three handlers
    append the REAL one from `replication::handshake::build_info_replication`
    (`handler_monoio/dispatch.rs:677`, `handler_sharded/dispatch.rs:369`,
    `handler_single.rs:1078`). That is the duplicate. `# Reclamation` is
    written from inside `info()` (`connection.rs:391`) and is fine.
    Consequence: INFO has THREE assembly points, so a filter applied only in
    `connection.rs` would leak the replication section on every request. The
    fix is to give `info()` the real section and delete the append — which
    requires touching the handlers, hence the §3 scope amendment below.
</assumptions>

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: INFO selects a single section
  Given a running server
  When the client sends "INFO replication"
  Then the reply contains exactly one section header, "# Replication"
  And no other "# " header appears in the reply

Scenario: INFO section matching is case-insensitive
  Given a running server
  When the client sends "INFO REPLICATION"
  Then the reply is byte-identical to "INFO replication"

Scenario: INFO accepts several sections
  Given a running server
  When the client sends "INFO server clients"
  Then the reply contains exactly "# Server" and "# Clients"
  And they appear in the server's canonical order

Scenario: unknown section is empty, not an error
  Given a running server
  When the client sends "INFO nosuchsection"
  Then the reply is an empty bulk string
  And the connection stays open

Scenario: default INFO omits Commandstats
  Given a running server
  When the client sends "INFO"
  Then "# Commandstats" does not appear
  And "# Commandstats" DOES appear for "INFO all"

Scenario: no section header is emitted twice
  Given a running server
  When the client sends "INFO"
  Then every "# " header in the reply is unique

Scenario: run_id changes across a restart
  Given a server whose INFO reports run_id R1
  When the server is restarted on the same dir
  Then INFO reports a run_id different from R1
  And it is 40 hex characters

Scenario: keyspace hit and miss counters move
  Given notifications are irrelevant and the counters are read from INFO
  When the client GETs an existing key and then a missing key
  Then keyspace_hits increases by 1
  And keyspace_misses increases by 1

Scenario: invalid notify flag is rejected verbatim
  Given a running server
  When the client sends "CONFIG SET notify-keyspace-events KEQ"
  Then the error is exactly Redis's Invalid-event-class-character message
  And CONFIG GET still returns the PREVIOUS flag string

Scenario: notify flags are canonicalized on readback
  Given a running server
  When the client sends "CONFIG SET notify-keyspace-events KEA"
  Then CONFIG GET notify-keyspace-events returns "AKE"

Scenario: keyspace and keyevent channels carry inverted payloads
  Given notify-keyspace-events is "KEA"
  And a client subscribed to "__key*@0__:*"
  When another client sends "SET foo bar"
  Then a message arrives on "__keyspace@0__:foo" with payload "set"
  And a message arrives on "__keyevent@0__:set" with payload "foo"

Scenario: INCR reports incrby, not incr
  Given notify-keyspace-events is "KEA"
  And a client subscribed to "__keyevent@0__:*"
  When another client sends "INCR ctr"
  Then the event channel is "__keyevent@0__:incrby"

Scenario: RENAME emits both halves
  Given notify-keyspace-events is "KEA"
  And a client subscribed to "__keyevent@0__:*"
  When another client renames "a" to "b"
  Then an event "rename_from" arrives carrying "a"
  And an event "rename_to" arrives carrying "b"

Scenario: expiry emits expired
  Given notify-keyspace-events is "KEA"
  And a client subscribed to "__keyevent@0__:expired"
  When a key set with PX 60 elapses
  Then an "expired" event arrives naming that key

Scenario: a key miss emits nothing under A
  Given notify-keyspace-events is "KEA"
  And a client subscribed to "__key*@0__:*"
  When another client GETs a key that does not exist
  Then no message arrives
  And the same GET under flags "KEm" DOES produce a keymiss event

Scenario: subscriber on a different shard still receives
  Given a server with 4 shards and notify-keyspace-events "KEA"
  And a client subscribed to "__keyevent@0__:set" on one connection
  When keys owned by every shard are SET from other connections
  Then an event arrives for each key

Scenario: disabled notifications cost nothing on the write path
  Given notify-keyspace-events is "" (the default)
  And a client subscribed to "__key*@0__:*"
  When another client sends "SET foo bar"
  Then no message arrives
```

</scenarios>

---

## 3 · CONTRACT — the frozen interface ▸ docs/05-step-3-contract.md

Status: DRAFT

### Least-sure flag surfaced at freeze:

⚠ **"The event hook can be placed where it sees both the key and the db without
re-plumbing command signatures."** — **PARTIALLY WRONG, resolved before freeze.**
Checked `ShardSlice` (`src/shard/slice.rs:63`): it carries `shard_id` and
`databases: Box<[Database]>` but **no pubsub registry and no current-db index**
(db selection is per-connection). Hooking each write command directly would
therefore have required threading both through every command signature — the
invasive diff the flag warned about.

Resolved by inverting the design: commands do not publish. They append to a
per-shard **notification outbox** on `ShardSlice`, which the event loop drains
after the command completes — the loop already holds `all_pubsub_registries`.
This keeps `src/command/**` out of the scope, matches the established shard-slice
side-table idiom (`kv_write_intents`), and puts the fan-out on the loop where
awaiting is legal. The db index is stamped into the outbox entry by the caller
that already knows it, not looked up later.

Cost of being wrong a second time: if the outbox cannot be drained without
crossing an await while holding the slice borrow, fan-out moves to a dedicated
chore tick and events gain up-to-one-tick latency (still at-most-once, still
ordered per key). That is a latency change, not a correctness change.

### Interface

```rust
// src/pubsub/keyspace.rs (new)

/// Event classes, one bit each. Parsed from the `notify-keyspace-events` flag
/// string; `A` expands to every class EXCEPT keymiss (`m`) and new-key (`n`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NotifyFlags(u16);

impl NotifyFlags {
    /// Parse a flag string. Returns Err(offending_char) on any char outside
    /// `Ag$lshzxeKEtmdn`, so the caller can build Redis's verbatim error.
    pub fn parse(spec: &[u8]) -> Result<Self, u8>;
    /// Canonical readback: classes in `g$lshzxetdmn` order, then K, then E.
    /// `A` is re-collapsed when every A-member class is present.
    pub fn to_spec(&self) -> String;
    /// True when neither K nor E is set — the whole feature is off.
    #[inline] pub fn disabled(&self) -> bool;
    #[inline] pub fn wants(&self, class: EventClass) -> bool;
}

/// One pending notification. `key` is a cheap Bytes clone, never a copy.
pub struct PendingNotify { pub db: u32, pub class: EventClass, pub event: &'static str, pub key: Bytes }

/// Appended by write paths, drained by the shard event loop.
/// Push is a no-op (single flag load, no formatting, no alloc) when disabled.
impl ShardSlice { pub fn notify(&mut self, db: u32, class: EventClass, event: &'static str, key: &Bytes); }
```

```rust
// src/command/connection.rs — INFO gains real section selection
/// `sections` is the caller's argument list, already lowercased.
/// Empty => the default set (everything except Commandstats/Latencystats).
pub fn info_sections(db: &Database, sections: &[&str], keyspace: &[(u64,u64)]) -> Frame;
```

### Guarantees
  - `NotifyFlags::parse` never allocates and never panics on arbitrary bytes.
  - `to_spec(parse(x))` is idempotent: canonical form round-trips.
  - `ShardSlice::notify` when `disabled()` performs no allocation and no
    string formatting — verified by a test asserting the outbox stays empty
    and by keeping channel construction inside the drain, not the push.
  - A section name is emitted at most once per INFO reply even if the caller
    repeats it (`INFO server server` yields one `# Server`).
  - Event delivery is at-most-once and may be dropped for a slow subscriber,
    exactly as PUBLISH already behaves — notifications are NOT a durability
    mechanism and this contract does not promise delivery.

### Scope (files this task may touch)
  - `src/pubsub/keyspace.rs` (new) · `src/pubsub/mod.rs` (re-export)
  - `src/shard/slice.rs` (outbox field + `notify`) · `src/shard/event_loop.rs` (drain)
  - `src/command/connection.rs` (INFO) · `src/config.rs` (+ `config/conf_file.rs`)
  - `src/command/introspect.rs` (CONFIG GET/SET wiring)
  - `tests/info_observability.rs` (new) · `tests/keyspace_notifications.rs` (new)
  - `scripts/client-compat/manifest.yaml` · `CHANGELOG.md`
  - `src/server/conn/handler_monoio/dispatch.rs` · `handler_sharded/dispatch.rs` ·
    `handler_single.rs` — **added by amendment (contract still DRAFT)**. INFO
    has three assembly points, not one: each handler appends the real
    `# Replication` section after `info()` has already written a stub. A filter
    applied only in `connection.rs` would leak that section on every request,
    and the duplicate-header Must (`io6`) cannot be satisfied without deleting
    the append. Scope is limited to passing the real section into `info()` and
    removing the append — no other handler behaviour changes.
  - **Out of scope:** `src/command/**` other than `connection.rs`/`introspect.rs`.
    If the outbox turns out to need per-command call sites, that is a contract
    change, not a build decision.

### Target
  - `INFO replication` returns one section; default `INFO` omits Commandstats.
  - The 19 Must-listed fields present with real values; `run_id` differs across restart.
  - All 17 §2 scenarios green, including the 4-shard cross-shard delivery case.
  - Disabled-path: no allocation on the write path (outbox-empty assertion).

---

## 4 · TESTS & SCENARIOS — the red suite ▸ docs/06-step-4-tests.md

Run red BEFORE any build code. Every Must and every Reject has a test below.

`tests/info_observability.rs`
  - `io1_single_section_only` — covers: INFO <section> returns only that section
  - `io2_section_case_insensitive` — covers: case-insensitive matching
  - `io3_multiple_sections` — covers: `INFO server clients`
  - `io4_unknown_section_is_empty` — covers: unknown section => empty bulk, conn open
  - `io5_default_omits_commandstats` — covers: default vs `INFO all`
  - `io6_no_duplicate_headers` — covers: `# Replication` emitted twice (current bug)
  - `io7_repeated_section_emitted_once` — covers: `INFO server server`
  - `io8_required_fields_present` — covers: the 19 Must-listed fields
  - `io9_run_id_shape_and_restart` — covers: 40 hex, differs across restart
  - `io10_keyspace_hit_miss_counters` — covers: hits/misses move by exactly 1

`tests/keyspace_notifications.rs`
  - `kn1_invalid_flag_char_verbatim` — covers: Reject #1, exact error text,
    AND that CONFIG GET still returns the previous value (no partial apply)
  - `kn2_flags_canonicalized` — covers: `KEA`->`AKE`, `Kg$`->`g$K`
  - `kn3_keyspace_and_keyevent_inverted` — covers: both channels, inverted payloads
  - `kn4_incr_reports_incrby` — covers: event name != command name
  - `kn5_rename_emits_both_halves` — covers: rename_from + rename_to
  - `kn6_expired_event` — covers: TTL elapse emits `expired`
  - `kn7_keymiss_silent_under_A` — covers: Reject #2, and that `m` DOES emit
  - `kn8_k_or_e_required` — covers: Reject #3, classes without K/E deliver nothing
  - `kn9_cross_shard_delivery` — covers: 4 shards, subscriber on one connection
  - `kn10_disabled_emits_nothing` — covers: default config is silent

Unit tests (in-module, `src/pubsub/keyspace.rs`):
  - `parse_rejects_out_of_class_char` · `to_spec_round_trips` ·
    `a_expands_without_m_or_n` · `disabled_when_no_k_or_e`

Harness: `scripts/client-compat/manifest.yaml` gains live parity assertions for
INFO section selection and the notify-flag canonicalization, replacing nothing
(no existing waiver covers these).

Note: `kn9` MUST run under the monoio default runtime, not only tokio — the
cross-shard path differs between handlers and a tokio-only test is CI-blind
to the shipped runtime.

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/` `tests/` `scripts/client-compat/`
Strategy (ordered batches): 1. INFO section selection + dedup (`src/command/connection.rs`)
  2. the real values the new fields need, published where a connection can read them
  (`src/storage/eviction.rs` policy, `src/blocking/` waiter gauge, pub/sub counts)
  3. the notify flag model + parser as pure logic (`src/notify.rs`)
  4. emission call sites in command code
  5. delivery — the two drains (`src/notify_fanout.rs`, `src/shard/`, `src/server/conn/`)
Safety rule (feature-specific): a notification must never block or fail a write. Delivery is
  fire-and-forget end to end — a full SPSC ring DROPS the batch rather than back-pressuring
  the command path, which is the trade Redis makes for a slow subscriber. Correspondingly,
  no INFO field may be invented: a value Moon cannot answer truthfully is omitted, never
  filled with a plausible-looking placeholder that a client would act on.
Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

<!-- Scope tokens, backticked, FIRST declaring line: `./…` = this task dir · a token
     with "/" = project root · a bare name = sibling of the previous token's dir ·
     outside-root resolutions are dropped fail-closed · a DIRECTORY token covers its
     whole subtree (containment — diverges from §4's non-recursive counting) ·
     absent line = UNDECLARED (pre-existing tasks grandfathered, never retro-red) ·
     engine enforcement (touched ⊆ declared) lands in scope-gate-enforce.
     EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [ ] all tests pass
- [ ] coverage did not decrease
- [ ] no test or contract was altered during build
- [ ] the green was EARNED, not gamed — no overfit to fixtures, vacuous asserts, or stubbed-away logic (score with an adversarial refute-read — a subagent recommended under `autonomy: auto`; a confirmed cheat is HARD-STOP)
- [ ] concurrency / timing of the risky operation is safe
- [ ] no exposed secrets, injection openings, or unexpected dependencies
- [ ] layering & dependencies follow CONVENTIONS.md
- [ ] a person reviewed and approved the change

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] A client that asks for one section gets ONE section — no second `# Replication`
      header, no `# Commandstats` unless asked — confirmed by io1/io5/io6/io7, and against
      a live redis-server by the compat harness's INFO manifest.
- [x] Every field the new INFO answers carries a value that MOVES when the thing it names
      moves — `maxmemory_policy` follows a `CONFIG SET`, `blocked_clients` rises while a
      BLPOP waits and falls on timeout, `pubsub_channels` tracks SUBSCRIBE/UNSUBSCRIBE,
      `keyspace_hits/misses` step by exactly 1 per GET — confirmed by io8/io10/io11.
      A field Moon cannot answer truthfully is ABSENT, not zero: the harness proves the
      absence is visible (`info:atomicvar_api` reported missing by name).
- [x] A subscriber on ANY connection sees events for keys owned by ANY shard — confirmed
      by kn9 at `--shards 4`, which is the test that caught the single-drain build seeing
      1 of 8 events.
- [x] Default config is silent — no channel traffic at all until the operator opts in,
      and `m`/`n` are NOT swept in by `A` — confirmed by kn10 and kn7.
- [x] The canonical flag string Moon echoes back matches what a live redis-server 8.6.1
      echoes for the same input — confirmed by 29 measured pairs pinned in
      `canonical_form_matches_measured_redis` (measured, not recalled from docs).

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — this task's central risk, and it bit twice. `notify_keyspace_event`
      is called from the command paths; `flush_from_connection` from the sharded/monoio
      handlers; `flush_from_shard` from THREE event-loop sites (two `runtime-tokio`, one
      `runtime-monoio`). Confirmed by running kn1–kn10 and io1–io13 under BOTH runtimes,
      not by reading the call graph: the first build wired the shard drain into the
      monoio arm only and kn6/kn9 stayed red under tokio.
- [x] DEAD-CODE (code) — `NotifyFlags::NEW_KEY` is parsed and canonicalized but emits
      nothing, because no `n` event is produced yet. Deliberate and NOT orphaned: the
      flag is part of the wire contract (a client may set it and read it back), and
      accepting-then-silently-dropping is what Redis does for a class with no emitters.
      Recorded in §7 rather than hidden.
- [x] SEMANTIC — the `notify-keyspace-events` canonicalization rule was MEASURED against
      a running redis-server 8.6.1 (15 pairs, then 6 more to settle `n`/`m`), because the
      documented "one ordered list" model is wrong: `n` sorts with the classes, `m` trails
      `K`/`E`, and `A` suppresses `n` but not `m`. All 21 measurements are pinned as test
      data, so a future reading of the docs cannot quietly "correct" the code back.

### GATE RECORD
Outcome: PASS
Evidence: PR #481 (squash `73c6597d`). 10/10 `keyspace_notifications`, 13/13
`info_observability`, run under BOTH `runtime-monoio` (shipped) and `runtime-tokio,jemalloc`;
lib suites 4605 and 3768 green. Full CI matrix dispatched on the branch —
run `31725319305`, 9/9 including the three jobs PR path-filters skip (Windows, macOS,
console) and `Check (monoio — the shipped runtime)`.
Reviewed by: Tin Dang · date: 2026-08-13

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): dropped notification batches (a full SPSC ring is
silent by design — if it is ever non-rare, the fire-and-forget trade needs revisiting);
`blocked_clients` failing to return to 0 after a blocking command drains (a leaked gauge
increment reads as a hung client that isn't there).

### Spec delta
- [SPEC · open] The `n` (new-key) class is accepted and canonicalized but emits nothing —
  no call site produces a `new` event yet (evidence: `NotifyFlags::NEW_KEY` has no emitter;
  §6 DEAD-CODE).
- [SPEC · open] Only `set`, `incrby`, `rename_from`/`rename_to`, `expired` and `keymiss`
  are emitted. The list, hash, set, zset and stream classes parse and gate correctly but
  have no call sites (evidence: §4 covers the classes' plumbing, not their events).
- [SPEC · open] `PUBSUB NUMPAT` counts subscribers, not unique patterns (evidence: #480,
  found while wiring `pubsub_patterns` into INFO).
- [SPEC · open] `metrics_setup.rs` is 1822 lines against the repo's 1500 ceiling even after
  `src/admin/footprint.rs` was split out (evidence: #479).

### Competency deltas
- [TDD · open] A test suite that runs under only ONE runtime is CI-blind to the shipped one.
  `keyspace_hits`/`keyspace_misses` had been reporting zero for plain GETs under monoio the
  whole time and PR CI never saw it, because tokio reads correctly (evidence: io10 passed
  under tokio, failed under monoio; #477 closed as a consequence).
- [TDD · open] A fixture that pins a CAPABILITY GAP fails as a reward for closing the gap.
  The compat harness's `run_id`-missing fixture broke the moment INFO grew `run_id` — the
  fourth such retirement in that file. Pin gaps on things the subject cannot plausibly grow
  (evidence: repinned on `atomicvar_api`, a field meaningless for a Rust server).
- [ADD · open] Verify a platform claim by MEASUREMENT before building on it. The macOS
  footprint read used `task_vm_info` offset 16 (`resident_size`) where it needed 144
  (`phys_footprint`) — a header-shaped assumption that would have made the whole fix inert
  on the live instance (evidence: discriminated with a clean file-backed mmap, which moves
  one field and not the other).
