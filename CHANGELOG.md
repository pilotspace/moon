# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security
- **A blocking command with an oversized timeout killed the whole server.** `BLPOP k 1e300` built
  its deadline with `Duration::from_secs_f64`, which **panics** on a value it cannot represent, and
  moon turns a panic on a shard thread into a process-wide abort — so one unauthenticated line
  took the server down, every shard and every other client with it:

  ```text
  $ redis-cli -p 7833 BLPOP nokey 1e300
  Error: Server closed the connection
  cannot convert float seconds to Duration: value is either too big or NaN
  FATAL: thread 'shard-0' panicked; aborting the whole process rather than serving with a
  dead shard or a dead cluster control plane
  ```

  Found while adding `XREAD BLOCK` (#595), which routes into the same deadline code — the defect
  itself predates it on the `BLPOP` float-seconds path. Both parsers now port the tail of Redis's
  `getTimeoutFromObjectOrReply` exactly, including the ORDER of its checks, which is observable:
  it converts to milliseconds first and only then judges the sign, so a negative timeout is
  `-ERR timeout is negative` and not the "not a float" text moon used to send. Measured against
  redis-server 8.6.1 with one fresh connection per case (a case that BLOCKS leaves its reply in
  the socket and silently desynchronises every later read on a shared one — the first run of this
  table was fiction):

  ```text
                                    before              after / redis 8.6.1
  BLPOP nokey 1e300                 process abort       -ERR timeout is out of range
  BLPOP nokey 1e16                  process abort       -ERR timeout is out of range
  BLPOP nokey 1e15                  parks               parks
  BLPOP nokey -0.5                  not a float …       -ERR timeout is negative
  XREAD BLOCK 9223372036854775807   parked forever      -ERR timeout is out of range
  XREAD BLOCK 9223372036854775      parks               parks
  ```

  Deadline construction itself is now total as well (`deadline_after` returns `None` instead of
  panicking), so nothing that slips past a parser can reach the abort. Both sides of the boundary
  are asserted, in-process and end-to-end: the unit tests pin every error text, and
  `bsr12_oversized_timeout_errors_and_the_server_survives` `PING`s a live server after each case,
  because a panicking build passes every in-process assertion right up to the moment it aborts.

- **`h2` DoS advisory RUSTSEC-2026-0258 and `event-listener` unsoundness RUSTSEC-2026-0221**
  were both live on `main`. `h2` 0.4.14 accepted and queued **empty DATA frames without limit** —
  a remote memory-exhaustion vector in a networked server. `event-listener` 5.4.1 allowed `!Send`
  tags to cross thread boundaries via `StackSlot`, which is uncomfortably adjacent to this
  codebase's core concurrency assumption: monoio tasks are `!Send` and moon's cross-shard reply
  path is built entirely on the premise that they never cross threads. Bumped to `h2` 0.4.18 and
  `event-listener` 5.4.2 (lockfile only — no source, API, or MSRV change; `cargo check` and
  `cargo audit` both clean, 401 crates scanned).

  These went unnoticed because `cargo audit` / `cargo deny` do NOT run on ordinary pull requests —
  they run weekly, or when a PR happens to touch `Cargo.toml`. moon#598 touched `Cargo.toml` to
  register a bench, which is the only reason the advisories surfaced at all.

- **`redis.call()` inside `EVAL`/`FCALL` bypassed ACL key patterns entirely** (#569). The
  dispatcher gates `EVAL script numkeys k1 ...` on the keys an invocation DECLARES — but a script
  need not declare anything, and `redis.call`/`redis.pcall` went straight to
  `Database::execute_command` with no permission check at all. `numkeys 0` therefore made the outer
  key check a no-op and everything the script touched was unchecked: a `~app:* +@all` user read and
  wrote any key in the keyspace with `EVAL "return redis.call('GET','secret:x')" 0`, and could run
  commands their `-del`/`-flushall` rules denied. Every inner command now runs under the calling
  user's identity, resolved once per script (`acl::ScriptAcl`) and re-checked per `redis.call`
  against the same `check_command_permission` + `check_key_permission` the dispatcher uses, with
  the same fail-closed `acl::keyspec` walker — so movable-key layouts (`LMPOP`, `ZMPOP`,
  `SINTERCARD`, `ZUNIONSTORE`, `SORT ... STORE`, `GEORADIUS ... STORE`, `COPY`, `SMOVE`, `BITOP`,
  `XREAD STREAMS`) are covered and an argv the walker cannot enumerate — including `SORT k BY w_*`,
  whose weight keys are computed at runtime — is DENIED. The check sits at the single choke point
  every script-issued command passes through, before the SELECT/read-only/replica gates, before
  the eviction gate and COW pre-image capture and before MONITOR is fed, so a denied command
  produces no side effect and no laundering shape helps: literal keys, `..` concatenation,
  `string.format`, `ARGV`, table lookups, loops, closures, `redis.pcall` and nested Lua `pcall` all
  refuse identically, on EVAL, EVALSHA, FCALL and FCALL_RO. The caller's identity also rides the
  SPSC message to the shard a script routes to (#508), so the cross-shard copy is not a hole; the
  fail-closed default (`ScriptAcl::deny`) means a future script runner that forgets to supply an
  identity refuses every command instead of inheriting `~*`. Denials answer `-NOPERM ACL failure in
  script: ...` on both surfaces. Unrestricted users pay one enum test plus one `Acquire` load of the
  ACL version counter per `redis.call` — no lock, no allocation, no key extraction: **+1.9%** per
  call in an interleaved same-machine A/B against the pre-fix binary (20 000 `redis.call('GET')`
  per sample, 9 reps, min-of-reps, empty-loop cost subtracted; **dev profile**, so the absolute
  +86 ns is inflated several-fold and only the ratio transfers). A key-restricted user pays the
  full check — one uncontended ACL read-lock plus the same key extraction and glob match the
  dispatcher already runs for every non-script command — measured at +33% per call on the same
  harness. That version check is what makes the cached verdict safe: an `ACL SETUSER` landing
  mid-script is honoured by the very next `redis.call` rather than a script outliving its own
  revocation.
- **Fuzz the shared key-position walker, and stop a bare `.gitignore` entry from hiding new
  fuzzers** (#576). `acl::keyspec::command_key_positions` parses attacker-controlled argv on behalf
  of three consumers — ACL key-pattern enforcement, client-side cache invalidation, and command
  introspection — so one bounds bug is a remote panic in three places at once. PR #571's review had
  already found exactly that: a `numkeys` `usize` overflow that wrapped `first + nk` in release
  builds and sliced `&args[1..0]`, reachable by any key-restricted authenticated user. The new
  `acl_keyspec` target asserts the properties those callers rely on — every reported position
  indexes `args`; `At` is never empty; and, the security-relevant one, `Unknown` and
  `AtPlusComputed` must reach ACL as `Indeterminate`, so a `~pattern` user can never be granted a
  key whose name is computed at runtime (`SORT k BY w_*`). Proven non-vacuous by reverting the
  `checked_add` guard: the target reproduces the #571 crash from the seed corpus alone, minimizing
  to the original attack string `LMPOP 18446744073709551615 a LEFT`. Clean over 3.27M executions
  after restoring it. Two coverage gaps closed alongside: `.gitignore` matched a bare `fuzz`, which
  ignores only NEW files (already-tracked ones are unaffected), so the 17 existing targets stayed
  visible while the 18th would have committed clean locally and failed CI as "no such fuzz target";
  and `term_fst_sidecar` had been present in the tree but listed in neither CI matrix, so it had
  never actually run.

- **ACL `~pattern` restrictions were silently unenforced for most multi-key commands** (#566).
  `AclTable::check_key_permission` read a command's keys from `extract_command_keys`, a
  hand-maintained match on the command name whose fallthrough returned an EMPTY key list — and an
  empty key list is not "checked less precisely", it makes the permission loop a no-op, so every
  `~pattern` was ignored outright for any command the list forgot. Measured against a live server,
  a user restricted to `~app:*` reached arbitrary keys through 21 distinct commands, in both
  `--shards 1` and `--shards 4`: `COPY`, `ZRANGESTORE` (both positions), `SMOVE`'s DESTINATION (it
  was listed, but as a single-key command), `LMPOP`/`ZMPOP`/`BLMPOP`/`BZMPOP`, `SINTERCARD`,
  `ZDIFF`/`ZINTER`/`ZUNION`/`ZINTERCARD`, `SORT ... STORE`, `SORT ... BY <pattern>`,
  `GEORADIUS ... STORE`, `EVAL`'s declared keys and `MEMORY USAGE`.
  Key extraction is now **derived from the command registry's key specs**
  (`COMMAND_META` `first_key`/`last_key`/`step`), so a command that declares its keys is enforced
  automatically; hand-written arms remain only for layouts a fixed spec cannot express (`numkeys`
  vectors, positional `STORE` clauses, the `STREAMS` token, subcommand-shaped key positions).
  Extraction **fails closed**: a command that names keys but whose argv cannot be enumerated — or a
  command missing from the registry entirely — is DENIED with the standard `NOPERM` error and
  logged once per command name, so the next command that ships without a key spec fails safe
  instead of falling open. `SORT`'s `BY`/`GET` patterns read key names computed at runtime and are
  therefore refused for key-restricted users (`BY nosort` / `GET #` are unaffected). Commands that
  genuinely name no key (`PING`, `CONFIG`, `SUBSCRIBE`, `KEYS`, the `FT.*`/`GRAPH.*` families, ...)
  are unaffected, and a registry sweep test now fails if a NEW command declares no keys without
  being reviewed. Unrestricted and `~*` users still short-circuit before any extraction; the new
  path borrows key slices into a `SmallVec` and no longer heap-allocates per command.

- **A remote client could hang a shard thread and exhaust memory with a 4-byte inline command
  (#487).** `split_args_quoted` — the path any inline command containing a quote takes — skipped
  only `' '` and `'\t'` between arguments, while its token loop *terminated* on the wider
  `' ' \t \n \r \x0b \x0c`. A byte in the difference (`\n`, `\r`, vertical tab, form feed) sitting at a
  token boundary therefore made no progress at all: the token loop returned without advancing, an
  empty argument was appended, and the outer loop restarted at the same offset — forever, with the
  argument vector growing until the process died. A lone `\r` reaches the line because the CRLF
  scanner only terminates on the `\r\n` *pair*. Parsing happens before dispatch, so this was
  reachable pre-authentication: `\r"` followed by CRLF was enough. Both loops now read one
  `is_inline_space` definition, so they cannot drift apart again — the same six bytes C's
  `isspace()` accepts, which is what Redis's own `sdssplitargs` skips with. Found by the
  `resp_parse`, `resp_parse_differential` and `inline_parse` fuzz targets; guarded by an exhaustive
  test over every short line built from the bytes the splitter branches on (271k cases, ~0.03s),
  which without the fix allocates until the OS kills the process.
- **ACL bypass on the inline GET fast path (monoio runtime).** An
  authenticated but restricted user could read any key with plain `GET`:
  `try_inline_dispatch` answered the `*2 $3 GET` shape straight from the shard
  map, running neither the ACL command check nor the ACL key-pattern check.
  A `-@all` user read arbitrary keys by name, and a `~app:*` user read outside
  its pattern; no ACL LOG entry was produced. Writes were already gated on
  `can_inline_writes` (which folds in `conn.acl_skip_allowed()`) — reads were
  gated on nothing. Not single-shard-only: at `--shards 4` every key hashing
  to the connection's own shard leaked (measured 24/160 and 44/160 in the new
  suite); `--shards 1` — the config recommended for non-pipelined workloads —
  leaked 160/160. Reads are now gated on the same `acl_skip_allowed()` latch,
  so a restricted connection falls through to generic dispatch where both ACL
  checks run. The tokio handlers were never affected (they gate correctly at
  `handler_single.rs` / `handler_sharded/mod.rs`), which is why no CI job
  caught this: every CI test job builds tokio. New suite:
  `tests/acl_inline_read_enforcement.rs` (deny-all and key-pattern users, at
  `--shards 1` and `--shards 4`).

### Fixed
- **`XREAD BLOCK` / `XREADGROUP BLOCK` were a silent no-op: never blocked, never woken** (#595).
  Both readers parsed `BLOCK`, stepped over its value with an `// ignored` comment, and answered
  the null array immediately. That is worse than an unimplemented command, because the reply is
  indistinguishable from a legitimate "nothing new": every consumer loop in the wild — `redis-py`,
  `go-redis`, `lettuce` — degrades from a parked connection into a **hot spin**, re-issuing at full
  speed and generating unbounded request volume where Redis would have slept. Measured raw-socket
  against redis-server 8.6.1, both runtimes, `--shards 1` and `--shards 4`:

  ```text
  XADD bt 1-1 f v ; XREAD BLOCK 2000 STREAMS bt $
    redis 8.6.1 :  *-1  after 2.044s     moon (before) :  *-1  after 0.000s
  parked XREAD BLOCK 5000 STREAMS blk $ , XADD blk 2-1 g w at t=600ms
    redis 8.6.1 :  the entry, at 0.605s  moon (before) :  *-1  at 0.000s
  ```

  `crate::blocking::WaitFamily::Stream` and a stream waker already existed; what was missing was
  everything that would have put a waiter in front of them. Four defects, each independently
  sufficient to keep the command broken:

  1. **Nothing ever registered.** `is_blocking_command` answers from the command NAME, which is
     enough for the eight blocking pops but not for `XREAD` — the same name is a plain read without
     `BLOCK`. Added `is_blocking_command_args`, used at every site that decides whether a command
     parks. The two `MULTI` queue gates deliberately keep the name-only predicate: inside a
     transaction a stream read is queued as its ordinary self and executed by the dispatch table at
     `EXEC`, which already ignores `BLOCK` (measured: `MULTI; XREAD BLOCK 3000 …; EXEC` → `*1 *-1`).
  2. **The local write path could not wake anything.** The wakeup gate was open-coded at ten
     dispatch sites; eight said `is_list_producer || ZADD || XADD` while the two **connection
     handlers** said only `is_list_producer || ZADD`. A reader blocked on a key its own shard owned
     was therefore unwakeable, while the same `XADD` arriving over SPSC woke it — a
     routing-dependent hang that reads as a flake. All ten now share `wakeup::is_producer`.
  3. **The waker was built for a destructive pop.** It stopped at the first served waiter and ran
     `remove_wait` + `send(None)` on every waiter it popped, servable or not. Both halves are wrong
     for streams: an `XADD` wakes **every** parked reader (measured — two clients on
     `XREAD BLOCK 5000 STREAMS k $` both receive the entry), and a reader this `XADD` cannot serve
     must stay parked. The `send(None)` also fired on the re-check that runs immediately after a
     remote registration, so a cross-shard `XREAD BLOCK` would have answered null the instant it
     registered. It now decides each waiter while it is still queued and removes only the ones it
     can answer.
  4. **`$` had nowhere correct to be resolved.** The parsing connection may not own the stream, so
     `$` now travels as `StreamSince::Latest` and is bound by the shard that owns the key, with no
     `.await` between reading `last_id` and registering — which is the lost-wakeup argument, not a
     stylistic preference. Redis binds it the same way, and it is observable: block on `$`, `DEL`
     the stream, re-`XADD` at a LOWER id, and the waiter still times out.

  Errors are answered at once rather than parked: `-WRONGTYPE` for a stream read on a string key,
  and `XREADGROUP`'s missing-key / missing-group errors — the latter checked on the shard that owns
  the key, because otherwise `XREADGROUP GROUP nope c BLOCK 800 STREAMS k >` answered `-NOGROUP`
  when `k` hashed to the client's own shard and parked for the full budget when it did not.

  **The gate is a whitelist, and it covers ONE stream.** Everything it does not positively
  recognise keeps reaching the dispatch table and answers exactly as it did before this change, so
  each clause is a narrowing and never a regression:

  * **One stream.** Moon cannot decide a multi-key command on one shard, so a multi-stream read
    would be deferred to one `BlockRegister` **per key**, on a different thread each — and every
    owner shard's post-registration re-check MUTATES: `read_group_new` moves entries into the
    consumer's PEL. The client's coordinator keeps the first reply and drops the rest, stranding
    the sibling's entries as delivered-and-unacked to a consumer that never received them
    (`XREADGROUP >` never returns them again; only `XPENDING` / `XAUTOCLAIM` does). Measured on
    `--shards 4`, 10 key pairs each holding one undelivered entry: **5 lost an entry that way**,
    against 10/10 correct on redis 8.6.1 and on `--shards 1`. Gating on placement instead would
    make one command behave two ways depending on a hash, so the gate reads the argument the client
    wrote. Multi-stream `BLOCK` therefore still answers immediately, as it did before — no worse,
    and never lossy. Lifting it needs multi-key stream reads decided by a single owner, the shape
    #602 gave two-key writes; filed as a follow-up.
  * **Wakeable ids only.** `$`, or an explicit id, for `XREAD`; `>`, and only `>`, for
    `XREADGROUP` (a history read answers immediately even with `BLOCK` — measured). `+` is
    excluded because `StreamId::parse` maps it to `StreamId::MAX`, so a waiter parked on it could
    never be served by any `XADD` and would sit there until its client disconnected — a permanent
    park where moon used to answer instantly. `>` on a plain `XREAD` is excluded because
    registering it would bind the waiter at `0-0` and replay the whole stream on the first write.
  * **Valid arguments only.** A malformed `COUNT` must be an error, not a 300 ms wait ending in the
    null array. `BLOCK` is validated with Redis's own texts (`-ERR timeout is not an integer or out
    of range`, `-ERR timeout is negative`) and with Redis's own `string2ll` accept-set, which takes
    neither a leading `+` nor leading zeros — moon parked the full budget on `BLOCK +300` and
    `BLOCK 0300`, both of which Redis rejects outright. `BLOCK 0` still blocks forever.

  The waker walks each queue **once**. Deciding waiter-by-waiter through an id-then-lookup loop was
  quadratic in the number of clients tailing one stream — the canonical fan-out workload — and
  `XADD` p50 against parked, never-servable readers crossed over Redis at about 2000 tailers:

  ```text
  waiters                0     500    1000    2000    3000
  moon (ids-then-lookup) 48.9  314.7  1005.2  3511.9  7601.3 us
  redis 8.6.1           209.8  878.1   717.9  3032.9  5259.0 us
  moon (one pass)        46.0  167.8   335.9   685.9   530.2 us
  ```

  At 3000 tailers a single `XADD` had been holding the shard thread for 7.6 ms p50, which on a
  thread-per-core runtime stalls every other client on that shard.

  Non-vacuity proven by mutation: each component was reverted in turn and the corresponding tests
  confirmed to fail (predicate off → 7 of 11; `XADD` dropped from the producer gate → 4;
  premature-null restored → 4; `$` binding disabled → 3; the whitelist widened → the two gate
  tests). `handler_single` (the embedded server) is unchanged — it implements no blocking commands
  at all, `BLPOP` included.

  **Not fixed here, and pre-existing:** a multi-key stream read whose keys span shards is answered
  by the routing key's shard alone, which cannot see the others. `XREADGROUP` claims the local
  stream's entries and then under-reports or errors, stranding them in the PEL — measured
  identically on a binary built from the merge-base and on this branch (**7 of 24 both ways**;
  redis 8.6.1: 0 of 24). It needs the same single-owner design as the multi-stream `BLOCK` case and
  is filed with it.

- **`XREAD` emitted an unserved stream with an empty entry list; Redis omits it** (#594). Measured
  against redis-server 8.6.1 after `XADD pelA 1-1 f v` and `XADD pelB 1-1 f v`:

  ```text
  XREAD COUNT 10 STREAMS pelA pelB 0 99999
    redis 8.6.1 : *1 …pelA…
    moon        : *2 …pelA… *2 $4 pelB *0        <- the extra
  ```

  A client iterating the reply saw a stream it had to special-case as present-but-empty. Under
  RESP3 it is sharper still: since #593 the container is a Map keyed by stream name, where
  membership is the natural "was this stream served?" test, so an entry with an empty list asserts
  the stream is quiet — a claim the serving shard is not entitled to make. Fixed in
  both `xread` and `xread_readonly`, so the reply
  does not depend on which dispatch path served it. The all-unserved miss is unchanged (still the
  null array, moon#482), and this is **not** the `XREADGROUP` history rule: `XREADGROUP … 0` on an
  empty PEL genuinely does answer `{name: []}` in Redis (verified), so the omission belongs to
  plain `XREAD`'s "did anything arrive after this id" question alone. The parity suites never
  called `XREAD` with a mix of served and unserved streams, which is why it survived them.

  One consequence worth naming: a stream on ANOTHER shard is invisible to the routed shard, which
  reads it as "does not exist", so a cross-shard multi-stream `XREAD` now omits it instead of
  reporting `[name, []]`. Both are wrong against Redis, which serves the entries — but the empty
  list was an active lie (it asserts the stream is quiet), where the omission merely says nothing.
  The underlying routing gap is pre-existing and filed with the multi-key work above.

- **Scripting state never left the shard that created it, so `EVALSHA` and `FCALL` failed
  depending on where the key landed** (#515, #514). Moon keeps the `EVAL` script cache and the
  Functions library registry per shard. `SCRIPT LOAD` fanned out; nothing else did — so the state a
  command needs was, like moon#592's second key, simply not where the routing key sent the command.
  To an application author that is indistinguishable from corruption. Measured at `--shards 4` on
  fresh connections, before the fix:

  * **`EVAL` did not publish its body** (#515). Redis caches an `EVAL`'d script server-wide, which
    makes "`EVAL` once, then `EVALSHA` by sha" a supported and very common idiom — it is what
    `redis-py`'s `Script` wrapper does. One bare `EVAL` followed by `EVALSHA` of that sha on twelve
    other keys: **ok=2, NOSCRIPT=10**. The same body via `SCRIPT LOAD` first: 12/12.

  * **`FCALL` was three defects stacked**, each hiding the next (#514). One `FUNCTION LOAD` then
    eight single-key `FCALL`s: **5/8 CROSSSLOT, 3/8 `ERR Function not found`, 0/8 succeeded**, and
    `FUNCTION LIST` on a fresh connection returned `*0`.
    1. `FCALL` still used `validate_keys_same_shard`, which demands every key hash to the
       *connection's* shard — the exact defect #508 fixed for `EVAL`. A single key cannot cross a
       slot; it just lives elsewhere.
    2. The `FunctionRegistry` was built **per connection**, so a loaded library was invisible to the
       very next connection, and `FUNCTION LOAD` had no fan-out either. This half reproduced at
       `--shards 1` too.
    3. Callbacks were invoked with **no arguments**, so the documented `function(keys, args)`
       signature received `nil` and any idiomatic library died with
       `attempt to index a nil value (local 'keys')`. Only visible once (1) and (2) stopped erroring
       first.

  The fix. `EVAL` publishes its body to every other shard once per distinct script, and
  `FUNCTION LOAD`/`DELETE`/`FLUSH` replay on every other shard (`DELETE`/`FLUSH` for the same reason
  as `LOAD` in reverse: a delete that reaches one shard leaves the library callable elsewhere, which
  is a worse lie than not deleting it). The registry is now per **shard** — a thread-local shared by
  every connection on that shard and by the shard's SPSC drain loop. `FCALL`/`FCALL_RO` route to the
  shard owning their key through the same helper `EVAL` uses, so there is one routing policy rather
  than two that can drift. `call_function` passes `(keys, args)`; the `KEYS`/`ARGV` globals are still
  set, so `EVAL`-style bodies keep working. A genuinely cross-shard key set is still refused
  `CROSSSLOT` before anything is touched — a function runs against one shard's database and cannot
  reach another's, so deleting that check instead of routing around it would have converted #514
  straight into #592.

  **The replay is acknowledged, and a partial replay is reported, not swallowed.** Pushing to a ring
  only enqueues; the target applies it in its own drain loop. Returning `+OK` at push time makes
  `FUNCTION LOAD` a lie for as long as that queue takes to drain — a client that loads and
  immediately calls can have its `FCALL` reach a shard that has not installed the library yet. So
  each replay carries an ack that reports whether the shard **applied** the op (delivery is not
  application: a shard can receive a library and still reject it), and the whole ack loop runs under
  a single 2s budget rather than a fresh 30s per shard. When some shard did not apply it, a
  `FUNCTION` mutation answers `MOONERR partialfanout FUNCTION <verb> applied on N of M shards` — its
  ops are idempotent under retry (`DELETE`/`FLUSH` outright, `LOAD` when re-sent with `REPLACE`), so
  the truth plus a retry beats `+OK` over a registry that answers differently per shard. A partial
  `EVAL` publish does **not** fail the `EVAL`: the script still runs, only a later `EVALSHA` routed
  to the shard that missed it is affected, and `NOSCRIPT` is exactly what client libraries already
  handle by re-sending the full `EVAL` — which republishes, because the cache tracks "published"
  separately from "cached". Every failure is also loud (`warn!` +
  `moon_xshard_fanout_drop_total{kind}`).

  A **repair leg** was designed first and rejected under adversarial review: a later routed call that
  hit `NOSCRIPT` / `ERR Function not found` would re-push the state from a shard that had it. Both
  errors are also what a shard says when it has ALREADY applied a `FUNCTION DELETE` the origin has
  not seen yet, so the repair could not tell "you never got it" from "you already dropped it" and
  would resurrect deleted libraries — spreading the divergence, since a diverged shard keeps
  re-seeding whichever shard owns the next key. The same mechanism un-did `SCRIPT FLUSH`, which is
  local-only. Divergence is now prevented at publish time and reported, never patched up after.

  Known limitations, stated rather than implied. Registry mutations have **no total order**: two
  `FUNCTION` mutations issued concurrently from connections on different shards can apply in
  different orders on different shards, and nothing reconciles them — issue them one at a time until
  an ordering authority exists. `SCRIPT FLUSH` still does not fan out (pre-existing, genuinely
  unchanged here). And a client that interpolates values into script bodies now replicates every
  distinct body to all N shards instead of one, so that anti-pattern costs N× the cache it used to.

  Covered in `handler_monoio` (the shipped runtime), `handler_sharded` (tokio) and the shard-side
  SPSC drain; `handler_single` is excluded deliberately — it only runs at `--shards 1`, where there
  is no other shard to publish to. Each half is proven load-bearing by mutation: deleting the `EVAL`
  publish, reverting the registry to per-connection, deleting the `FCALL` routing, restoring the
  zero-argument callback, swallowing a partial fan-out, and claiming the publish regardless of the
  outcome each fail a distinct subset of `tests/script_function_fanout.rs`; restored, it passes
  13/13. Two vacuous tests were found and fixed that way — the first draft took the sha from
  `SCRIPT LOAD` on the server under test, the one command that already fanned out (it now uses a
  hard-coded SHA1 moon cannot influence), and the republish assertion counted log lines where one
  round already emits one per peer.
- **`volatile-ttl` eviction could spin the shard thread forever on an index-only victim**
  (#600). `find_victim_volatile_ttl` returned the head of the deadline index verbatim, with no
  cross-check against the keyspace — the only sampler that does, because it is the only one that
  reads a *maintained index* instead of iterating occupied DashTable slots. Every caller's sole
  way to make progress is `db.remove`, so a `(deadline, key)` pair whose hot entry is gone is not
  a victim, it is a non-terminating loop: `evict_one_with_spill` returned `true` while removing
  nothing, `before == after`, `current_total` never moved, and the next iteration peeked the very
  same head pair. The failure mode is silent and total — no `OOM` reply, no log line, no metric:
  the shard thread spins at 100%, the instance stays over `maxmemory`, and every client on that
  shard simply stops being served. Reproduced with a unit test that runs `evict_to_budget` on a
  worker thread behind a 10 s watchdog: before the fix the watchdog fires (the call never
  returns); after it, the same call completes in 0.1 s having evicted every live victim.

  Two layered guarantees, because "the sampler is now correct" and "the loop cannot hang" are
  different claims and the second must not depend on the first:

  - **The sampler verifies and self-heals.** `find_victim_volatile_ttl` now walks the index head
    and reaps any pair that is *provably* stale, using byte-for-byte the predicate the active
    expiry sweep already uses (`src/server/expiration.rs`): the entry is gone, or it carries a
    different deadline than the pair claims. Anything else is left untouched — in particular a
    valid pair that merely is not due yet, which is why no clock is read here at all. Reaping,
    not skipping, is what `Database::drop_expiry_index_pair` exists for and what its doc comment
    already warned about: without it the next peek walks the same head again. Termination is
    structural — each non-returning iteration removes exactly the pair it peeked, so the walk is
    bounded by the index length — and a further per-tick cap of 64 keeps a large leaked backlog
    off the 100 ms sweep; the reaping is permanent, so successive ticks converge, and exhausting
    the cap logs a `warn!` naming the writer-coverage bug rather than failing quietly.

  - **The loop terminates regardless of the sampler.** `evict_to_budget` now judges progress on
    an OBSERVATION rather than on the sink's claim: `(estimated_memory, pending_spill_bytes,
    hot key count)` sampled between victims. Sixteen consecutive victims that move none of the
    three answers `OOM` with a `warn!` instead of looping. A bounded, visible, retryable error
    is strictly better than a spun shard. The probe is a triple and not a byte count on purpose —
    async spill (moon#466) moves a value from the hot plane to the pending plane and leaves
    `estimated_memory` flat, so a bytes-only test would call a working reclaim stalled and
    convert it into a rejection; the counter also resets on the first real progress, so slow
    reclaim can never accumulate into a false `OOM`. Verified by mutation: with the sampler fix
    removed the loop stops spinning and answers `OOM` in 0.14 s, and with both removed it hangs.

  moon#599's accounting distinction is preserved exactly, and neither change touches it: reaping
  a stale pair is a repair, not an eviction — the key it names is already absent from the
  keyspace, so `DBSIZE` cannot move and no counter is recorded. Confirmed end to end on a live
  server in both destinations: with `--disk-offload disable` 2,415 victims were DROPPED
  (`evicted_keys` 2,415, `spilled_keys` 0, `DBSIZE` 3,585), and with disk offload on the same
  2,415 were TIERED (`spilled_keys` 2,415, `evicted_keys` 0, `DBSIZE` unchanged at 6,000 and the
  tiered keys still readable) — `evicted_keys + DBSIZE == keys written` in both. `noeviction` is
  unaffected: its gate runs before any victim is selected, so it still answers `OOM` immediately
  and, deliberately, does not even reap the stale pair — it promises not to mutate the keyspace.

  No producer of a stale pair is known today: `self.data.remove` appears exactly once in the
  codebase (inside `Database::remove_hot`, which un-indexes the entry's own pair), and
  `debug_expiry_index_consistent` is an oracle over every TTL writer. This is the defence being
  present on a path where the equivalent path already had one, because the cost of being wrong is
  an unreachable shard rather than a wrong number.
- **The two-key write family acknowledged success while destroying the data at `--shards > 1`**
  (#592). Moon routes a command to ONE shard — the owner of the key `extract_primary_key` picks —
  and that shard then executes the whole command against its own slice. `first_key` names the
  **routing** key, not every key the command writes, so for this family the other key was read from
  and written to the routing key's table, under the right name but on the wrong shard, where every
  normally-routed access is blind to it. `RENAME alpha omega` answered `+OK` with `alpha` destroyed
  and `omega` never created — the value simply gone, with no error, no log line and no metric a
  client could detect. Measured at `--shards 4` against 12 pairs *constructed* to straddle a shard
  boundary: **12 of 12 lost the data, for all twelve commands** — `RENAME`, `RENAMENX`, `SMOVE`,
  `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `ZRANGESTORE`, `ZUNIONSTORE`, `ZINTERSTORE`,
  `PFMERGE`, `GEOSEARCHSTORE` and `SORT ... STORE`. At `--shards 1` the loss rate is 0. A
  thirteenth shape had the same defect through Lua: `EVAL "redis.call('RENAME', KEYS[1], ARGV[1])"
  1 src dst` — routing never sees a key that arrives via `ARGV`, so the script ran on the source's
  shard and returned `+OK` with the value gone. Such a command is now **refused before anything is
  read, written or deleted**, from the two key names alone:
  `CROSSSLOT Keys in request don't hash to the same shard; co-locate every key of the command with
  a {hash} tag`. Refusing is the only answer that cannot lose data — there is no window in which
  the value exists in neither place, no undo to get wrong under a race or a timeout, and no partial
  state to recover after a crash; a shard hop would trade the loss for a non-atomic `RENAME` plus
  two independent AOF records with no shared commit point. It is also what Moon already answers for
  every other operation it cannot perform atomically across shards (a MULTI/EXEC body spanning
  shards, a script spanning shards, `MSETNX`). Guarded on all three dispatch paths that can reach
  the keyspace at `shards > 1` — `handler_monoio` (the shipped runtime), `handler_sharded` (tokio),
  and `redis.call` from Lua — each proven independently load-bearing by mutation; `handler_single`
  has no multi-shard mode. `MULTI ... EXEC` was already safe (`analyze_txn_locality` walks every key
  of every queued command) and is now covered by a regression test. Commands NOT affected and
  deliberately untouched: `MGET`/`MSET`/`MSETNX`/`DEL`/`UNLINK`/`EXISTS`/`BITOP`/`COPY`, which
  `shard::coordinator` already dispatches a leg per owning shard; and `LMOVE`/`RPOPLPUSH`/`BLMOVE`/
  `BRPOPLPUSH`, the same defect owned by #570.

  > **⚠ Breaking change.** At `--shards > 1`, these commands now return an error where they
  > previously returned success. Co-locate the keys with a `{hash}` tag — `RENAME {user:42}:staging
  > {user:42}:live` — and the command works exactly as it does at `--shards 1`. Every affected call
  > was silently destroying data before, so no working deployment loses behaviour; a `--shards 1`
  > deployment is unaffected entirely.
- **Lua numbers passed to `redis.call` produced an argv no command could parse** (#569). Argument
  conversion reused the RETURN-value converter, so `redis.call('SET', 'k', 5)` handed the dispatcher
  a `Frame::Integer` — something no wire client can send — and the command answered `wrong number of
  arguments`. `redis.call('SET', 'app:k'..i, i)` in a loop, and `redis.call('LMPOP', 1, 'k',
  'LEFT')`, both failed on that. Upstream Redis stringifies Lua numbers for exactly this reason;
  Moon now does too (float truncation unchanged: `3.7` still becomes `3`). This is also why the
  #569 ACL gate had to be paired with it — `acl::keyspec` reads keys and `numkeys` out of the argv
  as bytes, so an integer frame in either slot was un-enumerable and a LEGITIMATE in-pattern
  `LMPOP` would have failed closed.
- **The hash-field TTL sweep was unbudgeted, expiry ticks ran with nothing due, and eviction
  spilled keys about to expire** (#543, #552, #553) — three CPU/IO wastes on the 100ms shard-loop
  timer, the latency-critical path.
  - **#543 — sweep 2 is now deadline-indexed and bounded.** The hash-field reap collected EVERY
    `HashWithTtl` key in the database with a full table scan and reaped all of them, with no time
    budget, no sampling and no batch cap, every tick. It now pops due `(min_expiry_ms, key)` pairs
    off a `hash_expiry_index` — the sibling of #541's whole-key index — and is bounded three ways:
    the same 1ms wall-clock budget as sweep 1, 256 hashes visited per tick, and 256 fields drained
    per visit. A partly-reaped hash is re-armed at its still-due minimum and resumed. The index
    holds a LOWER BOUND on each hash's minimum, which makes it self-healing: only
    `hash_set_field_ttl` can lower a minimum (so only it must index), while a raised TTL or a
    deleted hash leaves a stale-EARLY pair that the sweep pops, finds nothing due for, and re-arms
    or drops. Deferring a reap is invisible to clients — the read path already filters `ttls`
    against the shard clock, so only the memory reclaim is deferred. The reap now also takes its
    clock from the caller: sweeping against a clock ahead of `Database::cached_now_ms` (which the
    hash read path filters with) could physically drop a field reads still considered live.
  - **#552 — a tick with nothing due costs two head-peeks.** The `maybe_has_expiring_keys` latch
    only ever saved a database with ZERO TTL'd keys; a TTL-heavy one entered the cycle every 100ms
    just to re-derive "nothing due". Both indexes are deadline-ordered, so one `O(log n)` head-peek
    each answers that definitively, and the gate asks each sweep against the clock IT reaps with.
  - **#553 — eviction stops spilling keys that are about to expire.** A victim under
    `SPILL_TTL_FLOOR_MS` (1s) from expiry cost a datafile write, an fsync, a manifest commit and a
    cold-index entry for a value reclaimed moments later — and cold TTLs are lazy-only, so nothing
    sweeps it afterwards. `volatile-ttl` made this pathological by construction: it selects the
    NEAREST deadline in the database, i.e. the victim least worth persisting. Such victims are now
    plain-dropped instead (same RAM reclaimed, zero IO) on both the sync-batch and async-spill
    paths, reported through the same `on_plain_drop` sink a plain eviction uses so the dual-plane
    `DEL` still reaches replicas and the AOF, and counted as progress so an all-drops batch is not
    mistaken for "cannot evict". Eviction already DELETES victims under every non-`noeviction`
    policy in redis, so this gives up at most one second of extra visibility on a key that is about
    to become invisible anyway; `noeviction` never reaches the path. New metric
    `moon_eviction_expiring_drop_total` counts the spills avoided.
  - Found and fixed alongside: a `HashWithTtl` arriving as a WHOLE value (RESTORE, replication
    apply, cold promotion) never raised `maybe_has_expiring_keys`, so unless some unrelated key in
    the same database happened to hold a TTL, its expired fields were never actively reaped at all.
  - New `benches/expiry_sweep.rs` measures one tick in all three shapes; the bounded-sweep and
    spill-guard tests were each verified to FAIL against the pre-fix behaviour.
- **Recovery re-walked and re-warned about dead warm-segment manifest entries on every boot, forever**
  (#546). A warm vector segment is retired by deleting its directory, but the manifest entry was left
  `Active` on the stated grounds that recovery "already tolerates" a manifest entry whose directory is
  missing. It does tolerate it — by logging `manifest references warm segment N but directory missing`
  and moving on, every single boot, because recovery opened the manifest read-only and never wrote
  back. Nothing ever retired the entry, so the dead entries could only accumulate: a live instance
  logged **13,214 of these warnings against 55 real segment directories**, and separately 5,096
  `failed to read mvcc.mpf for ownership attribution` for the same vanished segments. Recovery is the
  only component positioned to observe the discrepancy, so it now heals it: entries whose directory is
  gone are tombstoned via the existing `remove_file` path and committed in a **single** manifest
  generation (one dual-root swap for the whole batch, not one per entry — a store with thousands of
  stale entries would otherwise pay thousands of swaps on the boot path). The existing two-axis
  retention in `gc_tombstones` still governs when entries are physically pruned. Tombstoning is safe in
  exactly the way deleting the directory already was: the data is gone, so no reader can be served from
  the entry either way. A commit failure is non-fatal and logged — recovery proceeds identically, since
  the entries are skipped regardless. Live segments are untouched, which the new test asserts
  explicitly in both directions (dead entries must be retired AND the live entry must stay `Active`),
  re-opening the manifest from its path so an in-memory-only retirement cannot pass.
- **GraphUnion segment merges were rejected forever on any index carrying duplicate vectors, so
  segments accumulated without bound** (#546). `verify_merge_recall` gates every merge on a recall
  measurement that scored an **ID-set overlap**: it took the brute-force top-k ids and intersected
  them with the ids HNSW returned. That measure is undefined when distances TIE. Real corpora tie
  constantly — repeated text chunks, boilerplate, and hash fields whose vector was never written all
  produce exact-duplicate embeddings — and when a point has 99 duplicates the ground truth takes an
  ARBITRARY 10 of the 100 tied candidates while the graph takes a DIFFERENT arbitrary 10. Both
  answers are exactly correct; the overlap scores them 0.0, the merge aborts, and it aborts again on
  every retry because the input never changes. The live store recorded **5,596 merge attempts with
  zero successes**, 1,367 of them at recall exactly 0.0000, with the retry backoff saturated at 480s
  — every affected index grew its segment count monotonically, which is upstream of both query cost
  and startup recovery time (recovery rebuilds per segment). Recall is now scored by **distance
  equivalence**: the k-th ground-truth distance is the acceptance threshold, so any neighbour at
  least that close counts, whichever of the tied ids it happens to be — which is what recall@k means
  when distances tie. The gate is not weakened: measured against a deliberately corrupted merge
  (layer-0 codes decoupled from their graph nodes) on an all-distinct corpus, the old id-overlap
  metric scores 0.9667 and the new distance metric scores 0.9673 — a 0.0006 difference — so the two
  are equally sensitive to real breakage and differ only on ties. The new test merges 8 segments
  drawn from 8 distinct vectors (100x tie sets), is red on the old metric at recall 0.0000, and
  asserts the merged segment is FUNCTIONALLY correct (every one of the k hits is an exact duplicate
  at ~0 distance) so a merely-laxer gate cannot pass it.
- **`COMMAND GETKEYS` answered "the command has no key arguments" for every movablekeys command**
  (#537). `getkeys()` decided from `meta.first_key <= 0`, which is true of `LMPOP`, `ZMPOP`,
  `SINTERCARD`, `ZINTERCARD`, `ZDIFF`/`ZINTER`/`ZUNION`, `BLMPOP`/`BZMPOP`, `EVAL`/`EVALSHA`/
  `FCALL`/`FCALL_RO`, `XREAD`/`XREADGROUP`, `ZUNIONSTORE`/`ZINTERSTORE` and the `SORT`/`GEORADIUS`
  `STORE` family. In redis's table — which `COMMAND_META` mirrors — that value means "the keys are
  not at a FIXED argument position", **not** "there are no keys", so Moon replied with an error
  whose text was itself false. `COMMAND GETKEYS` is precisely how a cluster-aware client routes a
  command it cannot parse itself, so such a client either refused to route these commands or
  guessed. `getkeys()` now delegates to the shared walker in `acl::keyspec` — the same one ACL
  `~pattern` enforcement and client-side cache invalidation consume, so the three answers cannot
  drift apart again (the drift #537 called "the deeper problem"). All four of redis's error strings
  are reproduced, in redis's order: the no-keys check runs BEFORE arity, which is observable
  (`COMMAND GETKEYS SELECT` reports no-keys even though its arity is also wrong), and an argv whose
  keys cannot be enumerated now says `Invalid arguments specified for command` rather than lying
  about the command having none. `EVAL`/`EVALSHA`/`FCALL`/`FCALL_RO` carry redis's
  `no-mandatory-keys` flag, so `EVAL <script> 0` — and an unparsable count — reply with an empty
  array instead. Verified on the wire against `redis-server 8.6.1`: 87/87 probes byte-identical on
  both runtimes (monoio and tokio) at 1 and 4 shards. Two deliberate divergences remain, both on
  argv that cannot execute anyway: a repeated `STORE` reports every destination where redis keeps
  only the last (a superset, so ACL still checks both), and a container command missing its key
  (`OBJECT ENCODING`) reports the no-keys error where redis reports its subcommand's arity error.
- **`CLIENT TRACKING` over-invalidated: the read-only SOURCES of `*STORE` commands were pushed as
  changed** (#584). The invalidation hook used every key a write command NAMED; redis pushes only
  what it MODIFIES (`signalModifiedKey`). A client caching `a` was told `a` had changed by
  `ZUNIONSTORE d 2 a b`, `SINTERSTORE d a b`, `BITOP AND d a b`, `PFMERGE d a b`, `COPY a d`,
  `ZRANGESTORE d a ...`, `GEOSEARCHSTORE d a ...` or `SORT a STORE d`; worse, a plain `SORT src`
  (a write-flagged command that writes nothing without `STORE`) invalidated `src` unconditionally.
  Each spurious push costs the client a dropped cache entry and a refetch. The shared walker now
  reports a per-position `KeyRole` — the `RO`/`OW` distinction redis carries in its key specs and
  `first_key`/`last_key`/`step` cannot express — and `invalidate_after_write` uses only the
  writable positions. ACL is unchanged by construction: it ignores the role, because a `~pattern`
  gates reads and writes alike. Roles are deliberately over-inclusive where the argv genuinely
  cannot say: `LMPOP 2 a b LEFT` pops from whichever key is non-empty at execution time and `EVAL`
  may write any key it was handed, so those stay writable — over-invalidating costs a refetch,
  under-invalidating leaves a cache stale forever (#582). Verified against `redis-server 8.6.1`
  with destination CONTROLS on every case (a fix that simply stopped pushing would fail all of
  them): 25/27 identical on both runtimes at 1 and 4 shards, the two residuals being the known
  `LMPOP` limit above and `GEORADIUS ... STORE`, which Moon rejects as a syntax error today
  (a pre-existing gap, unrelated to this change).
- **`evicted_keys` counted keys that had NOT left the keyspace, so it contradicted `DBSIZE`**
  (#585). On a live instance `evicted_keys` climbed by 456,018 while `DBSIZE` never moved. The
  two numbers were counting different things: with `--disk-offload enable` (the default) the
  durable batch spiller *tiers* a victim — it frees the hot copy, registers the key in the cold
  index, and the key stays readable and stays in `DBSIZE` (#355) — yet it recorded that as an
  eviction. The one metric an operator instinctively checks therefore stayed flat while the
  counter that is supposed to explain it ran away, which reads exactly like "DBSIZE never
  decrements". `evicted_keys` now counts only keys REMOVED FROM THE KEYSPACE, so it and `DBSIZE`
  move together; tiered keys are reported in a new `INFO stats` field, **`spilled_keys`**
  (Prometheus `moon_spilled_keys_total`), which is the counter to watch for memory-pressure
  activity on a disk-offload instance. Two further inconsistencies closed on the way: the *async*
  spill path recorded nothing at all (two spill paths, two different answers), and the plain-drop
  path incremented `evicted_keys` even when its sampled victim turned out not to be there to
  remove. Invariant now locked by test — `DBSIZE` falls by exactly the `evicted_keys` delta, and
  does not move for a `spilled_keys` delta.
- **`CONFIG SET maxmemory 4gb` was rejected** (#586). Memory-unit suffixes are the spelling redis
  documents and every runbook, Helm chart and tuning guide uses; an operator applying a cap under
  memory pressure got `ERR Invalid argument` and had to convert to bytes by hand — which is
  exactly when they are least able to. `maxmemory`, `db-maxmemory`'s byte half, the `--maxmemory`
  CLI flag and therefore `moon.conf` now share one parser reproducing redis's `memtoull()`
  exactly, including its two-scale rule (`1k` = 1000 but `1kb` = 1024, same for `m`/`mb` and
  `g`/`gb`), its rejections (`1.5gb`, `4 gb`, `-1`, `+4gb`, unknown units) and — verified against
  a live `redis-server` 8.6.1 rather than read off its source — its refusal of *surrounding*
  whitespace: redis rejects `" 4gb"`, `"4gb "` and even `"1024 "`, so Moon does too. This also
  fixes startup: `moon.conf` synthesises CLI arguments, so the `maxmemory 1gb` line in Moon's own
  conf-file documentation used to fail the parse (the conf-file parser trims each value itself, so
  padded conf lines keep working). One deliberate deviation, in the safe direction: redis does not
  check `strtoull` for `ERANGE`, so it saturates on a bare integer and *wraps* with a suffix —
  `CONFIG SET maxmemory 17179869184gb` returns `OK` and reads back **0**, silently disarming the
  memory limit. Moon rejects overflow instead, surfacing the typo.
- **`used_memory` did not count spilled-but-not-yet-written values** (#466). A victim queued for
  async spill has its whole entry cost credited back to the ledger while a full copy of the value
  is still pinned in RAM — by the queued `SpillRequest` and, since #459, by the in-flight plane
  until the completion is applied. `used_memory` now carries that term (`pending_spill_bytes`,
  maintained O(1) at mark and at every retire), paired with the rule that makes it safe: once the
  pending bytes cover the overshoot, `evict_to_budget` treats the tick as done rather than
  selecting more victims to chase memory that cannot drop yet. Without the pairing the honest
  figure would have turned the eviction loop into a runaway that drains the whole database; that
  is why the visibility fix was deferred out of #459, and both halves now ship together under
  test.
- **A list MOVE to a destination on another shard acked the element and then discarded it**
  (#570). `BLMOVE`/`BRPOPLPUSH` — and, unreported until now, their non-blocking twins `LMOVE`
  and `RPOPLPUSH` — are routed by their PRIMARY key, the source, and were then executed whole on
  that one shard: the pop came off the real source list and the push went into the SOURCE owner's
  slice under the DESTINATION's name. Every normally-routed read of the destination goes to the
  destination's OWNER and is blind to that entry, so the client received the moved element as its
  reply while the element left the keyspace. Measured against the pre-fix binary at `--shards 4`:
  `BLMOVE` lost 10 of 12 key placements, `RPOPLPUSH` 11 of 12, `LMOVE` 6 of 6 — the survivors are
  the placements where source and destination happened to be co-located. Silent, acked data loss
  on a two-key write, the failure mode a durable queue exists to prevent.
  Moon is shared-nothing across shards: a shard can pop only from keys it owns and push only to
  keys it owns, and there is no cross-shard commit to split a move across. Such a move is now
  **refused** with `CROSSSLOT Keys in request don't hash to the same shard; co-locate source and
  destination with a {hash} tag`, decided from the two key NAMES before anything is popped — so
  there is no window in which the element exists in neither list, no undo to get wrong under a
  race or a timeout, and no partial state to recover after a crash. This is the answer Moon
  already gives for every other operation it cannot perform atomically across shards (a
  MULTI/EXEC body spanning shards, a script spanning shards, `MSETNX`), and cross-shard `COPY`
  already degrades to an error naming `{hash}` tags for the types it cannot carry. Splitting the
  move across a shard hop was rejected as the alternative: it would trade the loss for a
  non-atomic `LMOVE` — an intermediate state Redis never exposes — plus two independent AOF
  records with no shared commit point.
  For the blocking pair the refusal is delivered **immediately**, before a waiter is registered,
  rather than after the source is served: shard ownership is a static property of the key names,
  so a client that blocked to its timeout only to learn Moon cannot route its move would have
  waited for nothing. Three independent guards cover the three execution sites (pre-registration
  scan, the wake path in `blocking::wakeup`, and pre-routing for the non-blocking pair); each was
  proven load-bearing by disabling it alone and watching exactly the expected assertions go red.
  Narrowness is enforced by the same suite: `{hash}`-tagged pairs still move at `--shards 4`,
  every move still works at `--shards 1` for arbitrary key names, and the rotate form
  (`LMOVE k k`) is never refused. **Users running `--shards > 1` who move between untagged lists
  must co-locate the pair with a `{hash}` tag** — previously those moves reported success and
  destroyed the element.
- **`XREAD`/`XREADGROUP` answered a RESP2 Array to RESP3 clients instead of a Map** (#577).
  Redis 7+ types a non-empty stream read as a RESP3 **Map** keyed by stream name
  (`%N <name> => <entries>`); Moon built `Frame::Array` unconditionally, in every protocol, so a
  client that dispatches on the reply CONTAINER was handed the wrong type. Now classified as
  `Resp3Shape::StreamMap` in `src/protocol/resp3.rs`, which means the conversion happens at the
  single choke point every dispatch path already shares — including the cross-shard path, where the
  1-byte shape tag rides along on the batch. Only the container re-types: entry ids and their
  field/value pairs stay BulkStrings, the empty-PEL history stays `%1{name: *0}`, and the miss stays
  a null array (`_` on RESP3, `*-1` on RESP2) in both protocols. **RESP2 output is unchanged.**
  The client-compat probe `parity_xreadgroup_history_resp3_reply_is_map` is un-waived in
  `scripts/client-compat/manifest.yaml` and now compares exact bytes.
- **`GEOSEARCH`/`GEORADIUS ... WITHCOORD` coordinates were BulkStrings and rounded to 4 decimals**
  (#568). Two independent defects in one reply. Redis emits the coordinate pair through
  `addReplyHumanLongDouble`, so on RESP3 it is a pair of **Doubles** — now classified as
  `Resp3Shape::GeoCoords` for `GEOSEARCH`, `GEORADIUS(_RO)` and `GEORADIUSBYMEMBER(_RO)` whenever
  `WITHCOORD` is present, converting the trailing coordinate element (Redis emits the extras in a
  fixed dist/hash/coord order, so coords are always last). Separately, the coordinates were
  formatted `{:.4}` — about 11 m of resolution — where Redis sends the full shortest
  round-tripping decimal; they now use the same `fmt_geo_coord` that `GEOPOS` already used.
  **That half is protocol-independent, so RESP2 `WITHCOORD` bytes change too** — from
  `$7\r\n13.3614` to `$18\r\n13.361389338970184`, matching `redis-server 8.6.1` exactly.
  `WITHDIST` is deliberately untouched: Redis builds it with `addReplyDoubleDistance`, a `%.4f`
  BulkString in both protocols.
- **`CLIENT NO-EVICT` and `CLIENT NO-TOUCH` accepted a missing argument and answered `+OK`**
  (#580). Redis registers both subcommands with arity 3 (exact), so `CLIENT NO-EVICT` with no
  `ON`/`OFF` is `-ERR wrong number of arguments for 'client|no-evict' command` — and so is a fourth
  argument, while a present-but-unrecognised value is `-ERR syntax error`. Moon answered `+OK` to
  every one of those, telling a client the setting had been applied when nothing was ever parsed.
  All three dispatch paths now share one parser (`command::client::no_evict_or_no_touch`); this
  also fixes a third divergence found while fixing it — the single-shard tokio path
  (`handler_single`) did not implement either subcommand at all and answered
  `-ERR unknown subcommand`.
- **`CLIENT TRACKING` never invalidated for movablekeys commands, so client-side caches went
  permanently stale** (#582). Commands whose keys are not at a fixed argument position carry
  `first_key: 0` in `COMMAND_META`, mirroring redis's own table — that means "the keys are not at a
  FIXED position", not "there are no keys". The tracking hook's key extractor read it as the latter
  and returned an empty key list, which disabled BOTH halves of the protocol: a movablekeys **read**
  (`SINTERCARD`, `ZINTERCARD`, `ZDIFF`/`ZINTER`/`ZUNION`, `XREAD`) never registered the client, so
  it cached a value it would never be told about; and a movablekeys **write** (`LMPOP`, `ZMPOP`,
  `BLMPOP`, `BZMPOP`, `XREADGROUP`) never pushed an `invalidate`. `SORT src ... STORE dst` was a
  third shape: `first_key = 1` names the SOURCE, so Moon invalidated a key it had not written and
  missed the one it had — likewise `GEORADIUS`/`GEORADIUSBYMEMBER ... STORE`/`STOREDIST`. Because
  client-side caching is a correctness contract (the client may serve its cached copy until told
  otherwise), a missed invalidation is unbounded stale data with no signal to the client, rather
  than a slow path. Measured against `redis-server 8.0.5`, which invalidates in all of these cases;
  now verified on the wire at `--shards 1` and `--shards 4`, each case run beside a fixed-position
  control so a silent harness cannot pass for a fix. Key extraction here is now **shared with**
  `acl::keyspec`, which already understood every one of these layouts (`numkeys` vectors, the
  `STREAMS` token, positional `STORE` clauses, subcommand-shaped positions). The shared walker
  reports key POSITIONS and each consumer applies its own policy, because the consumers
  legitimately disagree: `SORT ... BY <pattern>` reads runtime-computed key names, which ACL must
  refuse outright while invalidation must still act on the keys that ARE named. ACL behaviour is
  unchanged — the fail-closed contract, its error text and every existing key-permission test are
  byte-identical.

- **Inline commands terminated by bare LF were never dispatched, and an interior LF silently merged
  two commands into one** (#381). Moon's inline parser searched for `\r\n` and, on finding a lone
  `\r`, kept scanning; Redis's `processInlineBuffer` searches for `\n` and then strips at most one
  preceding `\r`. Two consequences, both measured against redis-server 8.0.5 over raw sockets:
  a bare-LF stream (shell loops, `awk` output, anything piped into `redis-cli --pipe`) never
  terminated a line, so the command was never dispatched and the client simply **hung** — and,
  worse, `SET k v1\nSET k v2\r\n` skipped the interior `\n` to reach the trailing `\r\n` and
  produced ONE command with the second command's arguments appended, a silent write loss rather
  than a rejection. The terminator is now `\n` with an optional preceding `\r`.
  The token-separator sets were corrected in the same pass, in both directions: `\r` now ends an
  unquoted token (Redis's `sdssplitargs` breaks on `{space, \n, \r, \t}`, so `RPUSH k a\rb` is two
  elements there and was one here), while `\x0b`/`\x0c` no longer do — they are `isspace()` but are
  NOT Redis token separators, so a line that merely *contained* a quote used to split `a\x0bb` while
  the same line without one did not. Redis reads two different sets here (`isspace()` to skip
  between tokens and to validate the byte after a closing quote; an explicit 5-byte list to end a
  token) and Moon now does too; the skip set remains a strict superset of the terminator set, which
  is the invariant that keeps the #487 livelock fixed. `\0` is deliberately unchanged — redis-server
  returns nothing at all for an inline line containing a NUL, so there is no oracle to match.
  The inline fuzz target now drives the pipelined drain loop with a forward-progress assertion and
  replays every corpus entry a second time with CRLF rewritten to bare LF.
- **A blank inline line stalled the command queued behind it** (#578). `parse_inline` correctly
  consumes an empty line and returns `Ok(None)`, but `Ok(None)` also means "need more bytes", and
  every read loop acts on that second meaning — so `\r\n\r\nPING\r\n` arriving in ONE read left the
  `PING` unparsed until unrelated later traffic happened to wake the loop, where redis-server
  answers immediately. This needed no bare LF and predates #381. Fixed in `protocol::parse`, the
  single funnel the codec and all three connection handlers share, so the fix cannot be
  CI-invisible in whichever handler was missed; the re-dispatch goes back through the RESP/inline
  decision, because what follows a blank line is very often a RESP array.
- **Arity errors named the command in upper case, and spelled container subcommands with a space**
  (#491). Redis formats this message with the command table's `->fullname`, which is stored lower
  case, so `echo`, `ECHO` and `EcHo` all produce `'echo'` — Moon normalised UP instead, and any
  client that string-matches error text saw a mismatch. Measured against redis-server 8.0.5, the
  container form differs too: `'client|setname'`, `'acl|getuser'`, `'memory|usage'`,
  `'xgroup|create'`, `'object|encoding'` — a pipe, not a space. Underscores are not separators and
  stay put (`sort_ro`, `bitfield_ro`). Normalisation now happens inside `err_wrong_args`, which 703
  call sites already share, plus 33 static literals and two `HGETDEL`/`HPERSIST` sites rerouted
  through it.
  Scoped to commands with a real redis-server oracle. Moon-native families (`WS`, `MQ`, `GRAPH.*`,
  `TEMPORAL.*`, `FT.*`, `TXN`) keep their current names — there is nothing to be compatible with,
  and the redis-server used as the oracle has no search module to answer for `FT.*`.
  The sibling rule is deliberately NOT swept: `unknown command '<x>'` echoes back what the client
  sent, case intact, because there is no registered name to normalise to. A guard test pins that,
  since a blanket lower-casing would have broken it.
- **`MEMORY USAGE key SAMPLES 0` was rejected by a dead second parser** (#519). `key_extra::memory_usage`
  had no callers at all — the live path is `server_admin.rs` — but it was stricter than Redis
  (`SAMPLES 0` means "sample every nested value" and is valid) and sat next to `copy`/`sort`, which
  ARE wired up. A plausible-looking duplicate is how the next person wiring a new dispatch path
  silently narrows accepted syntax, so it is deleted rather than corrected. The live behaviour stays
  pinned by `memory_usage_samples_zero_is_accepted`.
- **A blocking pop's immediate path consulted the LOCAL shard slice for every key** (#557),
  including keys owned by another shard. The pre-registration scan runs against the connection's
  own `ShardSlice`, where a remote key does not live, so at `--shards N` the fast path missed on
  (N-1)/N of keys and every one of them fell through to registration. Post-#560 that was harmless
  to the keyspace, but the scan can only ever produce a WRONG answer for a key it does not own —
  a stale local look-alike would be popped from the wrong shard, and since #556 it could even
  invent a `-WRONGTYPE` for a key whose real owner holds the right type. The scan now skips keys
  this shard does not own (`key_to_shard`, the same routing the slotted dispatch uses) and lets
  the owning shard answer them at `BlockRegister` time, which is how a remote blocking pop has
  always been served — a populated remote key still comes back in milliseconds, with no second
  client pushing.
- **A blocking pop on a key of the WRONG TYPE blocked instead of erroring** (#556). `SET s v` then
  `BLPOP s 0` sat there forever where Redis answers `-WRONGTYPE` immediately: the pop helpers reach
  the store through `get_mut_if_present(..).ok()??`, which collapses `Err(WRONGTYPE)` into `None` —
  indistinguishable from "empty" — so the connection registered a waiter on a key that can never
  serve it and the client eventually read a null as "queue empty". All eight blocking pops
  (`BLPOP`/`BRPOP`/`BZPOPMIN`/`BZPOPMAX`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZMPOP`) now run a
  read-only type gate over their keys, in argument order, BEFORE the pop attempt and before any
  registration — the same gate the in-MULTI path has had since #524, so the live and transactional
  replies agree by construction. The value itself is untouched (the #560 guarantee holds). Keys the
  connection's own shard does not own are answered by their OWNING shard at `BlockRegister` time,
  so the fix covers `--shards >= 2` and not just co-located keys; a multi-key waiter's remote keys
  keep the old behaviour on purpose (an error raised there would race a sibling key's real wake-up).
  `BLMOVE`/`BRPOPLPUSH` additionally check the DESTINATION's type once the move is about to happen,
  matching `lmoveGenericCommand` and moon's own non-blocking `LMOVE`: previously the element was
  popped from the source and then silently swallowed by `list_push_*`'s `if let Ok(list)`, so the
  client received a value that had left the keyspace entirely — on the immediate path and on the
  wake path alike.
- **RESP3: keyed sorted-set pops sent their score as a BulkString** (#559). `BZPOPMIN`, `BZPOPMAX`,
  `ZMPOP` and `BZMPOP` answered `$3\r\n1.5` where redis-server 8.x answers the RESP3 Double
  `,1.5` — every one of these replies is built by Redis's `genericZpopCommand`, which emits the
  score through `addReplyDouble`, exactly like the `ZPOPMIN` Moon already got right. Two causes,
  both fixed at the single seam rather than per command: (1) the shape classifier
  (`protocol::resp3::resp3_shape_of`) lumped `BZPOPMIN`/`BZPOPMAX` in with `ZPOPMIN`, whose rule is
  "a second argument is a COUNT" — but a blocking pop's second argument is the TIMEOUT, so every
  call classified as pair-wrapped, and since the reply is `[key, member, score]` (odd) the
  pair-wrapper passed it through untouched; `ZMPOP`/`BZMPOP` had no rule at all. They now have
  their own shapes, `KeyedScoredFlat` and `KeyedScoredPairs`. (2) The monoio handler's blocking
  branch is an **intercept**: it short-circuits the dispatch exit where every other reply meets the
  RESP3 policy, and it never applied that policy itself (#462's class), so on the shipped runtime
  the whole blocking family answered RESP2 shapes to RESP3 clients while the tokio handler, which
  does convert there, was right. It now routes through the same choke point. The blocking-in-MULTI
  executor converts too, so standalone, pipelined, in-`MULTI` and cross-shard all answer the one
  shape. **RESP2 is byte-identical** — the conversion is gated on the negotiated protocol version,
  and the Double's text is the same shortest-repr formatting the bulk carried, asserted directly.
  Also pre-registered, inert until the commands land: `ZRANK`/`ZREVRANK WITHSCORE` (Double score)
  and `ZADD ... INCR` (Double reply), both classified positionally so a member literally named
  `WITHSCORE` or `INCR` is not mistaken for the modifier. Verified unchanged against the oracle:
  `GEODIST` and `ZSCAN` scores stay BulkStrings in RESP3.
- **`ZREVRANGE key start stop WITHSCORES` was rejected inside `MULTI`.** Found by the #559 sweep:
  the command table gave `ZREVRANGE` arity 4 where Redis gives -4, and the MULTI queue gate is the
  only consumer of that number — so the optional `WITHSCORES` turned a legal command into a
  wrong-arity error at QUEUE time, aborting the whole transaction. Standalone it always worked,
  which is why no suite saw it.
- **Ordinary LOCAL writes skipped snapshot copy-on-write, double-applying on recovery** (#558).
  `spsc_handler::cow_intercept` — the only pre-image capture ordinary commands had — is reachable
  exclusively from the routed/queued arms that run on the shard event loop's own stack, where
  `&mut Option<SnapshotState>` is in scope. Every LOCAL write reaches the database from a
  connection task instead: the monoio inline `SET` fast path frames the write straight from the
  read buffer, and the monoio/tokio local dispatch arms, both MULTI/EXEC executors and the
  coordinator scatter arms all call `command::dispatch` directly. None of them could capture
  anything. At `--shards 1` that is *every* write; at `--shards N` it is the same-shard fraction.
  Consequence: an `INCR` issued while a BGSAVE was in flight, on a key whose segment had not been
  serialized yet, was written into the snapshot at its POST-increment value while the WAL still
  held the `INCR` — recovery loaded the snapshot and replayed the `INCR` on top of it, so a key
  that was 11 came back as 13. Silent, and worse the longer the snapshot runs. Capture is now
  wired at the choke point every non-routed write funnels through (`command::dispatch`) plus the
  inline `SET` path, reusing the per-shard thread-local queue #517 added for Lua writes (drained
  into the live `SnapshotState` by the persistence tick, before it advances another segment).
  Costs one thread-local `bool` load per command when no snapshot is in flight; the `is_write`
  lookup, key extraction and entry clone are all behind that gate. Double capture on the routed
  arms is harmless — `SnapshotState::capture_cow` is first-wins deduped.
- **Blocking pops queued inside `MULTI` answer the wrong reply SHAPE** (#524). `BLPOP`/`BRPOP`/
  `BZPOPMIN`/`BZPOPMAX` were rewritten at queue time into `LPOP`/`RPOP`/`ZPOPMIN`/`ZPOPMAX`, whose
  replies drop the key: `MULTI; BLPOP q 0; EXEC` answered `["v1"]` where Redis 8.6.1 answers
  `[["q","v1"]]`, and a miss came back as a null bulk (`$-1`) instead of a null array (`*-1` /
  `_` on RESP3). The key is not decoration — `BLPOP` takes many keys precisely so the caller can
  tell which one served, and a client unpacking the documented 2-element reply either errors or
  silently reads the value as the key. The rewrite was also wrong beyond the shape: `BLPOP q1 q2 0`
  became `LPOP q1 q2`, where `q2` is LPOP's optional COUNT — Moon popped up to two elements from
  `q1` and never looked at `q2`. Those four commands are now queued **unrewritten** and executed at
  EXEC in immediate-only mode (Redis's `CLIENT_DENY_BLOCKING` path) through the same helper the
  live blocking fast path uses, so the in-MULTI reply is shape-identical to the standalone one by
  construction. Both transaction executors and both runtimes. The AOF/replication record is the
  synthesised single-key sibling scoped to the key that actually served — never the blocking
  command itself, which a replica would block its apply loop on. `BLMOVE`/`BLMPOP`/`BZMPOP`/
  `BRPOPLPUSH` keep the rewrite: their siblings are shape- and arity-correct. Three side effects
  fall out: a queued blocking pop now appears in the `MONITOR` feed as the command the client
  actually sent; a miss no longer conjures the key it failed to find (the pop helpers are
  `get_or_create`, so an absent key was inserted as an empty collection and left behind, making
  `EXISTS` answer 1); and a multi-key blocking pop whose keys straddle shards is now refused
  `CROSSSLOT` like every other cross-shard `MULTI` body, because the queued frame finally declares
  the full key set — a loud refusal replacing a silent mis-execution against the first key alone.
- **RESP3: the inline `GET` fast path replied `$-1` for a miss** (#522), the RESP2 null bulk, where
  Redis replies `_`. The path frames its own reply bytes and so never reached the
  protocol-version-aware serializer. At `--shards >= 2` this made the null spelling depend on the
  KEY: keys owned by the connection's own shard took the inline path and got `$-1`, keys that
  routed elsewhere came back through the serializer and got `_` — on the same connection, so a
  client could not even cache "this server speaks RESP3". The inline path now takes the negotiated
  protocol version and selects between two static byte strings (`io::static_responses::null_bulk`,
  which also gives the previously caller-less `NULL_BULK` constant a home). `+OK` and `-WRONGTYPE`,
  the only other bytes this path frames, are spelled identically in both protocols.
- **`TXN.COMMIT` no longer answers `+OK` for a transaction that was applied in part** (#499).
  A TXN body whose ops are rejected by a TXN guard — a cross-shard write at `--shards > 1`,
  `MOVE`, `COPY ... DB`, `SWAPDB`, a cross-shard Cypher write — used to commit the *accepted*
  subset and reply `+OK`. The per-op errors did go back to the client, but a driver inspects the
  COMMIT reply, not the replies of the body commands (exactly as it inspects `EXEC` and not the
  `QUEUED`s), so a routing mistake became silent partial application. A rejection now poisons the
  transaction the way a queue-time error poisons `MULTI`: `TXN.COMMIT` rolls the whole transaction
  back through the `TXN.ABORT` path and answers
  `EXECABORT TXN.COMMIT discarded because of previous errors: N operation(s) rejected inside the
  transaction (first: <CMD>) -- rolled back and NOT committed`, leaving the connection out of the
  transaction. The wording stops at what the code can guarantee — the commit did not happen —
  because rollback is the same best-effort `TXN.ABORT` path whose undo capture still has gaps
  (#500). Nothing changes for a transaction whose every op was accepted. Both handlers (monoio
  and sharded/tokio) carry the check, and both are covered end-to-end.
- **`INFO memory` now reports `used_memory_lua`, and the Lua memory gauges stop reading ~48 bytes**
  (#506). The reported defect was "monoio's shard event loop holds a different Lua VM than the one
  executing scripts". Instrumenting both sites disproved that: on monoio the tick's `lua_rc` and
  `handle_eval`'s VM are the *same* `Rc` pointer, reporting the same 24165 → 25403 → 4219542 bytes
  the tokio leg reported. The real defect was the SOURCE of the published number.
  `ShardStoreMemory::lua` carried `ScriptCache::resident_bytes()` — the cached script *text*, which
  for `EVAL "return 1" 0` is exactly 48 bytes (a 40-char SHA1 key plus an 8-byte body), the very
  figure the issue read as monoio's "wrong VM". Nothing anywhere sampled the interpreter heap, so
  `moon_memory_bytes{kind="lua_scripts"}`, `moon_used_memory_bytes`, `INFO used_memory` and MEMORY
  DOCTOR all under-reported Lua by three orders of magnitude, and a script that anchors megabytes
  of tables in `_G` was invisible to every one of them. The VM is now sampled into its own
  `ShardStoreMemory::lua_vm` atomic and summed into all four, with `used_memory_lua` emitted under
  Redis's field name (its client-compat waiver is deleted). The sample deliberately lives in
  `persistence_tick::run_eviction_tick` — the one tick body both event loops share — because the
  shard periodic tick has TWO arms in `event_loop.rs` (a tokio `select!` arm and a monoio counter
  arm) and a publish written into either one alone is invisible on the other runtime; that is the
  trap the original investigation fell into, and a guard test now fails if the sample migrates back
  into a runtime-specific arm.

- **ACL key patterns are now enforced for `RPOPLPUSH` and `BRPOPLPUSH`** (#520). The ACL layer
  extracts a command's key arguments from a name table, and a command missing from that table
  falls through to an empty key list — which makes the permission loop a no-op, so every `~pattern`
  is ignored rather than merely approximated. `LMOVE`/`BLMOVE` were listed; their `RPOPLPUSH`
  siblings, with the identical two-key layout, were not. A `~cache:*` user could therefore move an
  element out of any key in the keyspace into any other. Found while wiring #520; `BRPOPLPUSH` was
  already reachable, so this closes a live hole as well as pre-empting one in the new command.
- **`XREADGROUP` in history mode answers the stream, not a null** (#526). `XREADGROUP ... STREAMS
  s 0` asks for the consumer's PENDING entries; Redis serves the stream before it knows whether
  the PEL slice has anything in it, so an empty PEL replies `*1\r\n*2\r\n$1\r\ns\r\n*0\r\n` —
  `[["s", []]]`. Moon replied `$-1`, so a client that iterates the returned stream list got a
  decode error (or a `None` branch) where Redis gives it zero iterations. This is a reply-SHAPE
  divergence, not a null-type one. The two request modes are now served on Redis's own
  conditions: history (an explicit ID) always contributes its stream, `>` contributes only when
  there ARE new entries — a `>` stream with nothing new is dropped from the reply rather than
  rendered as an empty entry list, which also fixes the mixed `STREAMS a b > 0` case. The reply
  is the null array only when nothing at all was served, so the `>`-with-no-new-entries answer
  stays `*-1` (#482).
- **`LPOP`/`RPOP` validate the optional count BEFORE the key lookup, with Redis's error text**
  (#527). Both commands looked the key up first and returned the miss (`*-1`) without ever
  reaching the count parser, so `LPOP nokey abc` answered "no such list" and the identical
  command answered `-ERR ...` the moment somebody created the key — the same malformed request
  getting opposite replies depending on unrelated keyspace state. Redis parses `argv[2]` before
  `lookupKeyWrite`, so the argument error never depends on the key. The message now matches too:
  a non-integer and a negative count both answer `ERR value is out of range, must be positive`
  (Moon said `ERR value is not an integer or out of range`, which clients that classify retries
  by error string read as a different failure). A well-formed count on an absent key still
  answers the null array (#482), and validating first does not create the key. `LPOS`'s
  `RANK`/`COUNT`/`MAXLEN` were already ordered and worded correctly and are untouched.
- **Blocking pops no longer create the key they miss on** (#523, #539). `BLPOP`, `BRPOP`,
  `BLMOVE` (source), `BRPOPLPUSH` (source), `BZPOPMIN`, `BZPOPMAX` and `BZMPOP` reached their
  value through `get_or_create_list` / `get_or_create_sorted_set`, so every miss materialised an
  empty list/zset before discovering there was nothing to pop — a phantom key that `EXISTS`,
  `TYPE`, `DBSIZE`, `KEYS` and `SCAN` all reported, that the ordinary "block on a key another
  client is about to create" idiom then hit with `WRONGTYPE` on the producer's `RPUSH`, and that
  nothing ever removed (a worker polling rotating ids leaked one empty collection per timed-out
  poll). An empty list/zset is not a representable Redis value, so those phantoms were also
  leaking into RDB, AOF rewrite and replication. The four pop helpers now take a new non-creating
  accessor (`Database::get_mut_if_present::<K>`) — `get_or_create` minus the fabrication step, so
  expiry drop, cold-tier promotion, compact-encoding upgrade, `WRONGTYPE` classification and the
  become-empty ⇒ remove-the-key behaviour are all unchanged, but a missing key leaves the
  keyspace and `used_memory` byte-identical. `BLMPOP` was already correct (it length-checks
  through the read-only `get_list`) and is untouched. At `--shards >= 2` the phantom appeared only
  for client-local keys (~1/N of them), which is why it read as a flake.
- **Lua script writes no longer skip the AOF plane under `runtime-tokio`, nor snapshot
  copy-on-write on either runtime** (#517). Two independent holes, both pre-existing:
  (1) `LuaEvictionCtx::emit_effect` was `#[cfg(feature = "runtime-monoio")]`-gated in full, with a
  `let _ = (db_index, cmd_and_args)` arm that DISCARDED the effect off monoio — a `runtime-tokio`
  build ran `EVAL "redis.call('SET',KEYS[1],'v')" 1 k`, mutated the keyspace, answered `OK`, and
  wrote nothing to the AOF, so the write was gone after a restart while every ordinary write around
  it survived. The gate now lives per plane inside `reason_del::record_bytes_conn`: the AOF leg (a
  channel send to the writer pool, exactly what the tokio connection handler already does for
  ordinary writes) runs on every runtime; the replication leg stays monoio-only because it pushes
  through `shard::self_msg` and master-side PSYNC does not exist under tokio at all.
  (2) All three `handle_eval` call sites (local monoio, local tokio, routed `ShardMessage::Execute`)
  run inside a bare `with_shard(...)`, and `spsc_handler::cow_intercept` is only reachable from the
  shard event loop's own stack — so a script writing while a `BGSAVE` was in flight left no
  pre-image, and the snapshot captured the POST-write value that WAL replay then applies a second
  time (`INCR` double-counts). Capture now happens in the scripting bridge at the `redis.call`
  level, which is also the only level where a real key exists — `EVAL <script> <numkeys> k`'s own
  `command[1]` is the script body, so wrapping the call sites in `cow_intercept` would have stashed
  the script text under a bogus key. Pre-images go to a per-shard thread-local queue that the
  persistence tick folds into the live `SnapshotState` before it advances another segment
  (`persistence::snapshot_cow`); the whole path costs one thread-local `bool` load when no snapshot
  is in flight. `SnapshotState::capture_cow` is now first-capture-wins, which also fixes a
  pre-existing hole on the ordinary-command path: a key written twice in one epoch appended two
  overflow records and load took the LATER one — a value that already contained the first write.
- **`EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` now accept the Redis 7.0 `NX | XX | GT | LT`
  conditions** (#544) — previously any option token was rejected with a wrong-arity error, which
  breaks typed clients that call them directly (redis-py `expire(k, t, nx=True)`). Semantics match
  Redis exactly, oracle-diffed against a live redis-server: `NX` sets only when the key has no
  expiry; `XX` only when it has one; `GT`/`LT` only move the expiry later/earlier, with a
  no-TTL key counting as infinite (so `GT` never sets it and `LT` always does); `XX` composes with
  `GT`/`LT`; `NX` composes with nothing (`ERR NX and XX, GT or LT options at the same time are not
  compatible`), and `GT`+`LT` is its own error — Redis's exact strings. The gate runs after the
  invalid-expire-time range check and **before** the past-time delete, so `EXPIRE k -1 GT` on a
  TTL'd key refuses (`:0`, key intact) while `EXPIRE k -1 LT` deletes — Redis's evaluation order.
  `COMMAND INFO` arity for the four commands corrected from `3` to `-3`. In the same file: **`TTL`
  now rounds to the nearest second** like Redis (`(ms+500)/1000`) — the old floor answered `99` for
  a fresh `EXPIRE k 100`, a divergence the oracle probe surfaced and a pipelined `/dev/tcp` probe
  (spawn-latency-free) confirmed fixed byte-for-byte.
- **Lazy expiry now emits — reads no longer delete expired keys silently** (#542). The removal
  `Database::get`/`get_mut`/`exists` performed when a read touched an expired key fired neither the
  `expired` keyspace notification nor the dual-plane (AOF + replication) `DEL` — only the active
  sweep did both. Two consequences: subscribers to `__keyevent@<db>__:expired` never heard about
  any key that was *read* after expiring (the common case for cache/session patterns, where the
  read is what discovers the expiry), and once the master lazily removed a key its sweep could
  never emit the authoritative `DEL`, so an attached replica kept the entry resident forever
  (logically hidden, but RAM/`DBSIZE`/`used_memory` diverging unboundedly). The lazy paths now
  HIDE the key (answer absent, leave it in place) and record it in a bounded per-db queue
  (`PENDING_EXPIRED_CAP` = 8192, adjacent-dedup); the shard's next active-expiry tick drains the
  queue, re-verifies expiry (a key overwritten in between is a new incarnation and is skipped),
  and deletes through the same emitting sink as sweep victims — identical notification + DEL
  treatment. This also fixes the replica-side divergence for free: a replica's reads now hide
  without deleting (Redis semantics — the replica waits for the master's `DEL`), its queue sits
  bounded at ≤ cap since replicas never drain, and promotion resumes draining with re-verification.
  Past the cap the lazy path still hides but stops recording; the probabilistic sweep remains the
  backstop. Write-path expired-entry replacement (`get_or_create_*`) is intentionally unchanged —
  the streamed write itself supersedes the key.
- **Commands whose key is not their first argument were routed by hashing a literal** (#533, #534).
  `extract_primary_key` special-cases the commands whose key is not `args[0]` and falls through to
  `args[0]` for everything else. `LMPOP`, `ZMPOP` and `SINTERCARD` take `numkeys` first, and
  `XREADGROUP` takes the literal token `GROUP`, so each hashed a **constant**: every invocation
  landed on one fixed shard regardless of the key, and a key any other shard owned read as absent.

  The failure is quiet, which is why it survived. A mis-routed `LMPOP`/`ZMPOP` answers `*-1` and a
  mis-routed `SINTERCARD` answers `:0` — byte-identical to the honest answer for a key that really
  is empty. `XREADGROUP` at least errors. `BLMPOP`/`BZMPOP` were fixed in the same arm for their
  other consumers (cluster slot resolution asks the same function, and hashing the `timeout`
  yields a wrong `MOVED` target); their blocking path extracts keys separately and was correct.
  Also corrected: the `XREAD` arm's `len == 5` guard could never match `XREADGROUP` despite the
  comment above it naming both.

  Two rules, both learned by having the previous instrument miss this class entirely, govern the
  regression tests in `tests/shard_routing_parity.rs` and the `route_probe` rows in
  `scripts/test-consistency.sh`: probe **populated** keys, because on an absent key a mis-routed
  command and a correct one return the same bytes; and probe **many** keys, because a constant
  route is still right for ~1/N of them, so a single-key test at `--shards 4` passes a quarter of
  the time and reads as a flake. Against the pre-fix binary the four parity rows fail with 9, 11,
  10 and 7 of 12 keys wrong; the fence rows (`LPOP`, `ZDIFF`, `XREAD`) pass on both.
- **A blocking waker destroyed waiters it could not serve** (#535). `try_wake_list_waiter`,
  `try_wake_zset_waiter` and `try_wake_stream_waiter` each popped whatever waiter sat at the
  front of a key's queue. A command outside the waker's family fell through a `_ => (None, None)`
  arm into the cleanup that runs for every waiter it pops — `remove_wait(wait_id)` plus
  `reply_tx.send(None)` — so the registration was dropped across all its keys and the client was
  answered a null it never earned.

  Two independent paths reached it, which is why the fix is in the wakers rather than at one call
  site. `ShardMessage::BlockRegister` registers a remote waiter and then, **gated on the key
  existing**, runs all three wakers in sequence, list first — so a `BZPOPMIN` on a populated zset
  owned by another shard was eaten by the list waker during its own registration and answered
  `*-1` in microseconds instead of returning the member sitting right there. Separately, an
  ordinary `RPUSH` on a key name that also carried a zset waiter destroyed it the same way.

  Measured at `--shards 4` before the fix: 9, 8 and 10 of 12 populated zsets answered `*-1`
  through `BZPOPMIN`, `BZPOPMAX` and `BZMPOP` — the ~3-of-4 signature of a key the client's own
  shard does not own — each in 130µs–1.5ms despite a 3-second timeout, because the null is sent
  during registration and the command never blocks.

  `BlockedCommand::family()` now maps every variant to the one waker entitled to serve it, and
  `pop_front_of_family` takes only that family's waiters, leaving the rest queued in order. The
  mapping is exhaustive with no `_` arm, so a new blocking command will not compile until it
  declares its family instead of silently becoming edible. The wakers' loop condition moved from
  `has_waiters` to the pop itself — a queue holding only foreign waiters is not empty, and the
  old condition would have spun forever.

- **RESP2 replies whose missing value is an *array* sent the null *string* instead** (#482).
  RESP2 has two nulls — `$-1` says "the string you asked for is missing", `*-1` says "the array
  you asked for is missing" — and `Frame` had a single `Null` variant that always serialised
  `$-1`. A statically-typed client decodes the two differently, so this was a decode error
  client-side, not a cosmetic difference. `Frame::NullArray` now carries the second one: `*-1`
  under RESP2, `_` under RESP3 (which has only one null), and composable inside an array.

  Seventeen sites were wrong, each measured against a live `redis-server 8.6.1` rather than read
  from the docs: the eight blocking commands on timeout (`BLPOP`, `BRPOP`, `BLMOVE`,
  `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`), `LMPOP`/`ZMPOP` finding nothing,
  `XREAD` and `XREADGROUP` with no data, `GEOPOS` of an absent member (nested inside the outer
  array), `LPOP`/`RPOP` with a count on an absent key, `EXEC` aborted by a broken `WATCH`, and the
  parser, which collapsed an inbound `*-1` so a reply relayed from a peer went back out as `$-1`.

  Two of those are worth calling out. **`EXEC`-abort was the highest-impact one and was not in
  the original report**: every client library decodes `EXEC` as an array, so the abort path — the
  one optimistic-locking code is written to handle — was the one that mis-decoded. And
  **`LPOP key <count>` was never a `Frame::Null` at all**; it returned an *empty* array (`*0`),
  so an audit that only rewrote null sites would have missed it. That cuts both ways: roughly
  thirty sites already answered correctly (nineteen `$-1`, twelve `*0`) and had to stay put —
  including `GEOHASH`, which answers `$-1` for the same absent member on which `GEOPOS` answers
  `*-1`, in the same source file. `tests/resp2_null_array.rs` pins both directions, and
  `scripts/test-consistency.sh` now diffs the reply TYPE against a live Redis over a raw socket,
  because `redis-cli` renders both nulls as `(nil)` and so cannot see this class of defect at all.

  One case in the original report is deliberately **not** fixed here. `BLPOP` inside `MULTI` still
  answers a null bulk, because that path rewrites the queued command to `LPOP`, whose own null
  correctly is `$-1`. Measuring the hit path showed the rewrite is wrong in shape rather than in
  null type — Redis answers `[["q","v1"]]`, Moon answers `["v1"]`, dropping the key that `BLPOP`
  exists to report — so patching the null alone would have turned its test green over a command
  that still answers wrongly. Filed as #524; the test stays ignored with its reason moved there.

  `XREADGROUP` was added to the list in review rather than by the original sweep: `XREAD` lives in
  `stream_read.rs`, which the audit walked, while `XREADGROUP` lives in `stream_write.rs`, which it
  did not. Splitting a command family across a read and a write file is exactly what makes a
  file-scoped audit miss a case.

- **`MEMORY USAGE <key>` reported existing keys as absent at `--shards >= 2`.** It routed by
  hashing the literal subcommand `"USAGE"` rather than the key (#511), so every invocation went to
  one fixed shard and read that shard's slice for a key it does not own. Measured at `--shards 4`:
  22 of 24 existing keys reported absent. The rate is `1 - 1/shards` — the signature of a constant
  route, not a race — and at `--shards 1` it was always correct, which is why it survived.

  `extract_primary_key` treats `args[0]` as the key for anything not in its keyless list, and
  `MEMORY` was in neither that list nor the set of commands with an explicit subcommand arm
  (`OBJECT`, `XGROUP`, `XINFO`, `BITOP`, `ZDIFF`/`ZINTER`/`ZUNION`/`ZINTERCARD`, `XREAD` — each
  added for exactly this reason). `MEMORY USAGE` now routes by `args[1]`, matched on the
  **subcommand** rather than blindly: `USAGE` is the only `MEMORY` subcommand that takes a key, so
  a bare `args[1]` would work today and silently hash a future subcommand's first *option* as a key
  tomorrow. `DOCTOR`, `STATS`, `PURGE`, `MALLOC-STATS` and `HELP` are keyless and now execute
  locally instead of routing by `"DOCTOR"`/`"STATS"`; they aggregate across shards internally, so
  the answer is unchanged.

  Neither shell suite covered it — `test-commands.sh` tested only `MEMORY DOCTOR`, and
  `test-consistency.sh` explicitly noted `MEMORY` as out of scope — so a 75%-failure-rate bug was
  invisible to both. `test-consistency.sh` now sizes 20 keys at 1/4/12 shards (a single key passes
  by luck at 12 shards), and `test-commands.sh` asserts the single-shard shape via a new
  `assert_moon_matches` helper, since `assert_moon_contains` greps `-F` and cannot express
  "some positive integer".

- **A script whose keys all lived on one shard was refused instead of routed (`CROSSSLOT`).**
  Reported as "EVALSHA of a single-key script fails with `CROSSSLOT`" (#508), with the guess that a
  1-element key list was being mis-folded. It was not. `validate_keys_same_shard` required every key
  to hash to the shard the CONNECTION happened to occupy — so `CROSSSLOT` was standing in for "I
  cannot run this *here*", and nothing ever asked where it *could* run. One key cannot cross slots;
  it just lives somewhere else. Measured at `--shards 4`: **7 of 8** single-key `EVAL`s rejected,
  only the key that happened to land on the connection's own shard running. `numkeys=0` always
  worked, which is why the defect read as intermittent rather than total.

  This broke `redis.lock.Lock.release()` — a single-key `EVALSHA`, and one of the most-used
  constructs in `redis-py`. Through the `Script.__call__` wrapper the caller saw a `NoScriptError`
  followed by a cross-slot error, neither of which names the cause.

  A script now ROUTES to the shard owning its keys (`scripting::route_script_keys` →
  `coordinator::coordinate_script` → a shard-side `EVAL`/`EVALSHA` arm on the SPSC `Execute` path),
  because a script executes against one shard's database and the correct place to run it is where
  that data is. Keys that GENUINELY span shards are still refused — there is no single target for
  them — and that refusal is now doubly held: the routing decision rejects it before the hop, and
  `validate_keys_same_shard` remains as the shard-side backstop, so the two must agree before a
  script can read another shard's (empty) view of a key. Shards build their Lua VM lazily on first
  use and share the one slot `conn_accept` fills, so a shard reached only by routing still gets
  exactly one VM.

  One thing this changes rather than fixes: `EVAL` caches its script only on the shard that ran it
  and, unlike `SCRIPT LOAD`, never fans out — so a bare `EVAL` followed by a direct `EVALSHA` on a
  key owned by another shard now answers `NOSCRIPT` where it previously answered `CROSSSLOT`.
  Measured after the fix at `--shards 4`: one bare `EVAL`, then `EVALSHA` of that sha across 12
  other keys — 4 ok, 8 `NOSCRIPT`. Neither ever worked, and
  `NOSCRIPT` is the better failure because every client library retries it — `redis-py`'s
  `Script.__call__` re-issues `EVAL` and self-heals, whereas `CROSSSLOT` was unrecoverable. Anything
  built on `register_script`/`SCRIPT LOAD`, including `redis.lock.Lock`, is unaffected (12/12).
  Filed as #515.

  Applied to both handlers through one shared helper rather than two implementations that can drift.
  `FCALL` has the same defect *plus* a second one — `FUNCTION LOAD` never fans out to other shards,
  so routing alone would trade `CROSSSLOT` for `ERR Function not found` at the same rate. Filed as
  #514 rather than half-fixed here.

  A routed script that gets no reply no longer claims it did not run. `recv_reply_bounded` returns
  the same error for a closed channel and for a 30s reply timeout, and the first cut of this path
  reported both as "cross-shard reply channel closed during script execution". Those have opposite
  retry semantics: a timeout means the target may still be executing, or may have already applied
  its writes, so a client told the script never ran will re-send a non-idempotent script that did.
  The two are now distinguished (`ReplyFailure::{Closed, TimedOut}`), both say execution status is
  **unknown**, and the timeout records `moon_xshard_reply_timeout_total{kind="script"}` so a wedged
  owner shard is visible in metrics like every other cross-shard reply path.

  Not fixed here, and pre-existing rather than introduced: script writes skip `cow_intercept` on
  every path, and under `runtime-tokio` `emit_effect` is compiled out entirely, so script writes
  emit no AOF or replication record. Routed scripts persist exactly as well as local ones do —
  which is the bug. Filed as #517, since a fix needs replication parity, kill-9 durability and a
  VM A/B bench of its own.

- **A pipeline did not execute in order at `--shards >= 2`, and writes were silently lost.**
  Reported as "MGET in the same pipeline as its SETs returns nulls" (#507). The cause is wider than
  the symptom: the sharded pipeline handlers DEFER a single-key command whose key lives on another
  shard into `remote_groups`, dispatching the whole group as one `PipelineBatchSlotted` at the end
  of the batch — while multi-key and keyless commands execute INLINE, mid-loop. An inline command
  therefore ran against a shard whose earlier writes in the same batch had not been sent yet.

  Measured at `--shards 2`, 20 trials each, before the fix:

  | in one pipeline batch | wrong | consequence |
  |---|---|---|
  | `SET a`, `MSET a` | 7/20 | **the MSET's value is lost** — the earlier SET lands on top of it |
  | `SET`, `FLUSHALL` | 10/20 | the key survives a flush that returned `+OK` |
  | `SET`,`SET`, `DEL` | 6/20 | the keys survive a `DEL` that returned success |
  | `SET`,`SET`, `MGET` | 10/20 | the reported symptom |
  | `SET`, `DBSIZE` | 12/20 | also `KEYS`, `RANDOMKEY`, `EXISTS`, `UNLINK`, `COPY`, `BITOP`, `INFO keyspace` |
  | `SET`, `TOUCH k` | 0/20 | single-key: always correct, and the shape of the fix |

  So this was not only the stale read it was filed as — same-key write ordering inverted, which is
  silent data loss. The rate rises with shard count (a key is remote with probability
  `1 - 1/shards`).

  The fix defers such a command and the unconsumed batch tail to the next iteration, reusing the
  mechanism #438 already built for early-flush commands: phase 2 resolves the pending remote replies
  first, and the tail re-parses with `remote_groups` empty, so it cannot loop. Applied to both
  sharded handlers (`handler_monoio` and `handler_sharded`); `handler_single` has no deferral and
  was never affected.

  The predicate is keyed on ROUTABILITY, not on a list of command names: a command routed by its own
  single key needs no wait, because a key maps to exactly one shard — if that shard is local the key
  cannot be pending, and if it is remote the command is appended behind the pending ones and the
  slotted batch preserves order. Everything else waits. A name list written for an MGET bug would
  not have contained `INFO keyspace` or `RANDOMKEY`, both of which were wrong. The one case
  routability cannot see — commands intercepted inline BEFORE routing, which still have a key-shaped
  first argument (`EVAL`, `SWAPDB`, …) — is named explicitly and carries a test that fails if an
  entry is dropped.

  Deferring is the conservative direction: a command sent down this path unnecessarily is merely
  executed at the start of the next batch, which is always correct.

  **This costs throughput, and the cost is per interleaving rather than per pipeline** — each
  deferral is one extra shard dispatch/await boundary (~50µs). Measured at `--shards 2` on one
  connection, 9 reps, alternating leg order, median; "floor" is the worst within-leg spread, so a
  delta smaller than its floor resolved nothing:

  | pipeline shape | guard fires | before | after | delta | floor |
  |---|---|---|---|---|---|
  | `MGET` after every 2 `SET`s | 64×/flush | 125,885 | 59,889 | **−52.4%** | 5.8% |
  | 128 `SET`s, then one `MGET` | 1×/flush | 528,764 | 512,686 | −3.0% | 22.2% |
  | `SET`,`SET`,`GET` (guard never fires) | never | 873,526 | 867,715 | −0.7% | 33.9% |

  A multi-key or keyless command at the END of a pipeline — the shape #507 was filed from, and the
  shape `redis-py`'s `pipeline()` produces — costs nothing measurable. One interleaved after every
  pair of writes halves throughput. `redis-benchmark` cannot express any of these shapes (it sends a
  single command type, so the guard never fires), so this came from a purpose-built harness that
  refuses to report unless the pre-fix binary actually reproduces the bug first.

  Recovering that cost means letting multi-key commands participate in the slotted batch instead of
  executing inline — a cross-shard-coordinator change well outside a correctness fix, filed
  separately.

- **Writes were paying for a memory measurement on every SET.** The `maxmemory` real-footprint
  correction (#478) was computed inside `evict_to_budget`, which runs on the write path, so every
  write performed `open`/`read`/`close` on `/proc/self/statm` *and* an instance-wide accounting sum
  that takes a read lock on the global replication state. The early-out above it never fired,
  because Moon auto-sets `maxmemory` to 75% of RAM, and the correction's own noise-floor guard sat
  *inside* the callee with the expensive reader passed as its argument — Rust evaluates arguments
  eagerly, so the syscalls ran to completion and only then were judged unnecessary.

  Measured on the moon-dev VM against v0.8.5, interleaved legs and median-of-5: `SET` at c=8 P=16
  fell **1.36M -> 0.58M ops/s (-57%)** and at c=1 P=1 **159K -> 134K (-16%)**, while `GET` was
  neutral in both — the shape of a cost paid only by writers. `git bisect` named the commit.

  The measurement now happens once a second, in the shard-0 chore that was already reading
  `/proc/self/statm` for the RSS gauge (and already carried a comment about not doing it more
  often); the write path reads one relaxed atomic. The correction is unchanged in value — a
  budget divisor in the GB range does not care about a second of staleness — and stays neutral
  (1.00) until the first sample lands, so an unmeasured process is never over-evicted.

  Two new INFO fields make the mechanism observable: `maxmemory_footprint_correction` is the
  divisor actually applied, so an operator can tell a cap being honoured from one being silently
  tightened, and `maxmemory_footprint_samples` is the sampler's liveness counter. The counter
  exists because the ratio alone cannot distinguish a healthy small instance from a wedged
  sampler — both read 1.00 — and because the chore lives in a shard loop with separate monoio and
  tokio arms, where a correction wired on only one runtime would restore the swap-death bug #478
  fixed, invisibly. `io20` asserts the counter advances on whichever runtime the test binary was
  built for.
- **Five commands were advertised by `COMMAND`/ACL but dispatched nowhere; they are deregistered.**
  `LATENCY`, `MODULE`, `DUMP`, `RESTORE` and `RECLAMATION` sat in `metadata.rs` while answering
  `unknown command` on every dispatch path, so `COMMAND`, `COMMAND COUNT` (267 -> 262) and ACL all
  described a surface Moon cannot serve — a client that introspects before calling was told it
  could. Three were never top-level commands at all: `RECLAMATION` is reachable only as
  `DEBUG RECLAMATION`, and `DUMP`/`RESTORE` only as `FUNCTION DUMP` / `FUNCTION RESTORE`, both of
  which are unaffected. With these gone the registry sweep
  (`cdg1_registry_sweep_no_unknowns`) runs with **no waiver list at all** — every entry in the
  registry is now proven reachable on both feature legs, which is what the v0.9 exit criterion
  asked for.
- **CI now proves reply shapes match across MULTI and pipeline, not just standalone.** The compat
  harness has always supported `--contexts standalone,multi,pipeline` and CI only ever ran
  `--strict`, so the "a command must not change shape by context" rule was asserted by hand and
  never gated. Wired in as its own step (PASS=201 FAIL=0 across all three contexts).
- **A test waiver went stale and hid five working commands from the registry sweep.**
  `cdg1_registry_sweep_no_unknowns` enumerates `COMMAND_META` and asserts nothing answers
  `unknown command`, skipping a list of 10 backlogged-unimplemented names. Five of them had since
  shipped — `WATCH`/`UNWATCH` (`+OK`), `RESET` (`+RESET`), `SSUBSCRIBE`/`SUNSUBSCRIBE` (a 3-element
  confirmation), all delivered by the v0.9 client-compat tasks — and none was removed from the
  list, so for the whole milestone the sweep reported green over a surface it had quietly stopped
  covering. The list is now the five that genuinely do not dispatch (`LATENCY`, `MODULE`, `DUMP`,
  `RESTORE`, `RECLAMATION`), and the sweep passes with the other five included on both feature legs.
  The waiver now also expires by itself: each backlogged command is asserted to STILL answer
  `unknown command`, so implementing one fails the test and names it rather than letting the
  exemption outlive its reason. A skip nobody re-checks is indistinguishable from coverage.
- **Five Rust SDK helpers sent wire forms Moon rejects on every call; two server-side gaps that
  hid them are closed.** `moondb` 0.2.1 → **0.3.0** (breaking: five `pub` methods removed).

  Each removed method failed on its first round trip, always, for the whole published lifetime of
  the crate — so no caller can have depended on its behaviour, only on it compiling. Two named
  commands Moon does not have, and three named real commands with the wrong arguments, which is
  why a name-level audit had already cleared them:

  | Removed | Sent | Server answered | Use instead |
  | --- | --- | --- | --- |
  | `MqClient::push_partitioned` | `MQ.PUSH` (a command name) | `unknown command` | `MqClient::push` |
  | `MqClient::pop_partitioned` | `MQ.POP` (a command name) | `unknown command` | `MqClient::pop` |
  | `VectorClient::upsert` | `FT.UPSERT` | `unknown FT.* command` | `FT.CREATE`, then `HSET` the index's vector field |
  | `TemporalClient::snapshot_at_packed` | `TEMPORAL.SNAPSHOT_AT <hlc>` | `wrong number of arguments` | `snapshot_at` (the server captures the timestamp) |
  | `TemporalClient::release_snapshot` | `TEMPORAL.INVALIDATE` (no args) | `wrong number of arguments` | nothing — delete the call |

  `upsert` was not reimplemented over the real wire form because there is no faithful one: Moon
  indexes a vector by `HSET`-ing a hash whose vector FIELD NAME comes from the index definition,
  and the signature never carried it. Guessing would trade a loud error for a silent wrong write.
  `release_snapshot` has no replacement because its premise was false: `TEMPORAL.SNAPSHOT_AT` never
  pinned the connection to a snapshot view — it records a shard-global `wall_ms → LSN` binding that
  `AS_OF` resolves later — so no pin is taken and none can be dropped. Callers can simply delete
  the call; their reads were already live. `snapshot_at`'s documentation, which described the
  imaginary pin, is corrected.

  **`TXN` was NOT removed** — `txn_begin` / `txn_commit` / `txn_abort` are correct and stay. An
  earlier shell probe appeared to show `TXN` dead; the probe was wrong (zsh does not word-split an
  unquoted parameter expansion, so the server received one argument literally named `TXN BEGIN`).

  Server-side, two commands were unreachable through introspection because they are served by
  intercepts that run *before* the metadata table: `TXN` and `FT.AGGREGATE` are now registered, so
  `COMMAND INFO` and `COMMAND COUNT` (265 → 267) report them. Registration is metadata only and
  does not reroute either command. Separately, a bare `TXN` or an unrecognised subcommand answered
  `unknown command 'TXN'`, which is false — the command exists — and misleads a driver into
  concluding Moon has no cross-store transactions. It now answers an arity/subcommand error, the
  shape Redis uses for container commands and the one driver error handling keys on.

  The SDK tree had no CI of any kind, which is how all five shipped. Three guards now run:
  a command-NAME sweep over the SDK sources (`tests/sdk_wire_forms.rs`), a live round trip through
  every one of the 52 public Rust helpers (`sdk/rust/tests/round_trip.rs`), and a Python
  `__version__` derivation check. The round trip is what found `release_snapshot`, after review had
  already passed the file — and mutating a helper's argument order fails it while the name sweep
  stays green, which is the point of having both.
- **`moondb.__version__` reported a release the package had not been for two versions.** The Python
  SDK published as `0.1.1` while `__version__` was a hand-maintained literal still answering
  `"0.1.0"`, and the test covering it asserted the same stale literal — so the suite stayed green
  while every caller reading `__version__`, every bug report quoting it, and anything gating on
  "SDK >= x" got the wrong number. It is now derived from installed distribution metadata (falling
  back to `pyproject.toml` for an uninstalled source checkout), so it cannot drift again, and the
  test asserts the derivation rather than restating the value.
- **Remote panic on the cluster bus: a truncated v3 gossip header killed the process.** The gossip
  wire v3 (#493) appended a 40-byte `sender_master_id`, but the deserializer's length guard still
  admitted any frame of at least the v2 header size so that a genuine v2 peer would still parse —
  and the v3 branch then read `data[2130..2170]` unconditionally. Any frame carrying version 3 with
  a length in `2130..2170` indexed past the end. The cluster bus listener hands this function
  peer-supplied bytes, so a single unauthenticated 2130-byte frame to the bus port panicked the
  `cluster-ctl` thread, which by policy aborts the whole server. A short v3 header is now rejected
  as malformed. Seeded into `fuzz/corpus/gossip_deser` — the target was correct but had not
  synthesised the 4-byte magic plus that 40-byte length window within its PR budget.
- **A pipelined `HELLO` no longer re-encodes the replies that came before it.** Moon accumulates a
  read batch's replies and serialized them all at flush time under whichever protocol version was in
  effect at the END of the batch, so a `HELLO 2` sent in the same write as an earlier command
  retro-downgraded that earlier reply: `CONFIG GET maxmemory` produced under RESP3 went out as `*2`
  instead of `%1`. Every reply is now encoded in the protocol that was in effect when that reply was
  produced, with a switch taking effect from its own index onward — inclusive, so a `HELLO`'s own
  reply is rendered in the protocol it establishes, which is what redis-server 8.6.1 does. Only the
  downgrade direction was ever visible: the frame *shape* is already fixed correctly at dispatch, and
  a RESP2-flattened array re-serialized as RESP3 still emits `*`, which is why the upgrade direction
  looked fine by accident. It is now pinned in both directions. `RESET` is covered too: it is
  contracted to return the connection to its default state, RESP2 included, so it moves the protocol
  exactly as a pipelined `HELLO 2` does — and a fix that covered only the two `HELLO` sites left
  `HELLO 3` + `RESET` in one write still retro-downgrading. `redis-cli` cannot express two commands
  in one `write()`, which is how this survived — the new suite drives a raw socket, and runs on both
  runtimes at 1 and 4 shards. Batches without a protocol switch — essentially all of them — keep the
  previous single-version loop, one branch and no allocation.
- **`CONFIG GET` answers every parameter, not just the first.** `CONFIG GET maxmemory appendonly`
  reported only `maxmemory`; the rest were silently dropped, which is what `redis-py`'s
  `config_get(*params)` and monitoring agents that read several settings per call send. The reply is
  now the union over all patterns, deduplicated (`maxmemory` plus `maxmemory*` reports it once), in
  the server's own table order rather than the caller's argument order, with unknown patterns skipped
  rather than erroring — all four properties measured against redis-server 8.6.1.
- **`CLUSTER INFO` no longer claims `cluster_enabled`, and a slotless node no longer claims health.**
  Two integration assertions encoded the pre-fix behaviour and contradicted the measured oracle:
  redis-server 8.6.1 reports `cluster_enabled` in `INFO` only — `CLUSTER INFO` never carries it —
  and a node with zero slots assigned answers `cluster_state:fail`, because state is derived from
  slot coverage rather than from the `--cluster-enabled` flag.
- **Replica routing tests no longer race gossip.** A freshly-`MEET`-ed node holds an incomplete slot
  map until gossip hands it the rest, and an incomplete map answers `CLUSTERDOWN The cluster is down`
  in front of any redirect — matching Redis, where the down-state check precedes `MOVED` once the
  slot resolves. The replica tests now wait for the joining node's OWN view to reach
  `cluster_state:ok` before asserting on redirects.
- **Gossip never said WHICH master a replica follows, so replicas were reported as their own
  shards.** The flags word carried a role bit but no master id, and a sender's own role was not
  propagated at all — a replica was known as one only on the node its `CLUSTER REPLICATE` ran
  against, and every other node saw a slotless master. The sender's `master_id` now rides in the
  gossip header (wire v3). The header LAYOUT, not just the flags encoding, now depends on the
  version, so a v1/v2 peer's sections begin 40 bytes earlier and are still parsed.
- **`CLUSTER REPLICATE` neither validated its argument nor replicated anything.** It answered `+OK`
  for a node id it had never heard of, and `NodeRole::Replica` was read nowhere outside
  `src/cluster/` — so a cluster "replica" held no data and could serve no read. It now rejects with
  the measured `ERR Unknown node <id>` / `ERR Can't replicate myself`, and starts the same
  replication the `REPLICAOF` path starts. The validation is not cosmetic: a caller retrying until
  `OK` — the only way to wait out gossip convergence — succeeded instantly against an empty node
  table, leaving the node relabelled but permanently empty.
- **An inline closing quote followed by `\r`, vertical tab or form feed was rejected as unbalanced.**
  Redis's `sdssplitargs` tests `isspace(p[1])` after a closing quote; Moon tested only `' '` and
  `'\t'`, so `ECHO "hi"\rx` answered `-ERR Protocol error: unbalanced quotes in request` where Redis
  splits the line into three arguments. Measured over a raw socket against redis-server 8.6.1, both
  quote types, one fresh connection per probe: space, TAB, `\r`, `\x0b` and `\x0c` are all accepted as
  separators, and only a non-whitespace byte (`ECHO "hi"y`) is unbalanced. The check now reads the
  same `is_inline_space` set as the rest of the splitter — the narrow/wide disagreement here was the
  milder form of the bug that made the splitter unable to advance at all.
- **`cluster_state` was a constant, so a cluster that had lost a master still reported itself
  healthy.** The `status` field was assigned once in `ClusterState::new` and never written again;
  `cluster_slots_pfail` and `cluster_slots_fail` were the literals `0`, and `cluster_slots_ok` was
  reported as "however many slots are assigned". A client had no way to learn the cluster was
  degraded. `cluster_state` is now derived on read from real coverage — `ok` only when every one of
  the 16384 slots is claimed by a node that is not confirmed `FAIL` — and the three slot counters
  are counted per slot through their owners' bitmaps rather than by summing per-node counts, so a
  slot two nodes both claim cannot mask a real coverage hole.

  Fixing the reporting exposed that the transition it reports could never happen either:
  `try_mark_fail_with_consensus` counted only reports received FROM peers, but Redis's
  `markNodeAsFailingIfNeeded` also counts the local node's own suspicion when it is a master
  (`if (nodeIsMaster(myself)) failures++`). Without that vote a 3-master cluster is arithmetically
  unable to promote PFAIL to FAIL — quorum is 2, and each survivor hears about the dead node from
  exactly one other survivor, so the count tops out at 1 forever. A replica still casts no vote.
- **A degraded cluster kept serving keys, handing clients a partial view they could not detect.**
  While `cluster_state` is `fail`, every keyspace command now answers `CLUSTERDOWN The cluster is
  down` — including keys in slots the receiving node owns and could serve. That over-refusal is
  deliberate and measured: a partially-visible keyspace is worse than a refusal, because a client
  cannot tell which half of the data it is seeing. Redis's two CLUSTERDOWN messages stay distinct
  and the order between them is load-bearing — an unclaimed slot answers the per-slot `CLUSTERDOWN
  Hash slot not served` even while the cluster is fail, matching a real server (a lone
  `--cluster-enabled` node reports `cluster_state:fail` and yet answers `Hash slot not served`). A
  single-node cluster never trips the gate, so bootstrap stays usable. The gate is one bool read on
  state already under the caller's lock; the coverage scan behind it runs on topology change, never
  per command.
- **`CLUSTER INFO` reported `cluster_enabled`, which Redis reports only in `INFO`.** Removed from
  `CLUSTER INFO`; measured against redis-server 8.6.1, which emits it in INFO's Cluster section and
  never here.
- **A multi-node cluster silently served keys it did not own (#485).** `CLUSTER ADDSLOTS` does not
  bump the config epoch, so a hand-built cluster sits at epoch 0 forever. The gossip merge accepted
  a peer's slot bitmap only at a *strictly higher* epoch — `0 > 0` is false — so peer ownership was
  never merged and every node believed the only slots in existence were its own. `route_slot` then
  found no owner for a peer's slot and fell through to a bootstrap fallback that serves everything
  unclaimed locally. The result was the worst available failure mode: a write went to the wrong
  node, returned `+OK`, and was invisible to the node that actually owned the slot — no `MOVED`, no
  error, nothing a client could detect. The merge now accepts at equal epoch (a node is
  authoritative for its own bitmap; a strictly *lower* epoch is still ignored, preserving Redis's
  highest-epoch-wins tie-break), and the unclaimed-slot fallback is split: a lone node still serves
  locally so single-node bootstrap keeps working, while a formed cluster answers
  `CLUSTERDOWN Hash slot not served` rather than inventing an answer. That is the per-slot message,
  measured — Redis's other CLUSTERDOWN, `The cluster is down`, means `cluster_state` is fail, and
  sending it for one unowned slot tells an operator the whole cluster is gone. Direct contact with a
  node also
  now clears any suspicion about it — previously nothing ever reset health, so a node that recovered
  stayed suspected forever and its slots never counted as covered again.
- **A node's role and its health were the same field, so learning one erased the other.**
  `NodeFlags` had mutually exclusive `Master` / `Replica` / `Pfail` / `Fail` variants: marking a node
  PFAIL destroyed the fact that it was a replica, and with it the `master_id` recording which shard
  it belonged to. Redis treats these as orthogonal — a dead master is reported `role: master,
  health: fail`, a shape the old type could not represent at all. Split into `NodeRole` + `NodeHealth`,
  both carried together in the gossip flags word (bit 0 role, bits 1-2 health) so the wire header
  keeps its fixed 2130-byte size; unknown health bits decode as online, so a forward-compatible peer
  is never treated as failed by accident. `CLUSTER NODES` and `nodes.conf` now render both axes in
  Redis's own spelling: a comma-separated list where PFAIL is **`fail?`** and confirmed FAIL is
  `fail`, appended to the role (`master,fail?` → `master,fail`). Moon previously emitted `pfail`, a
  token Redis never produces and no client parses.
- **The nightly fuzz job never reported, and threw away its corpus every night.** The nightly
  budget was `-max_total_time=21600` — exactly GitHub's 6h hosted-job ceiling — so the fuzzer was
  still running when the platform killed the job. Measured on the 2026-08-14 run: `inline_parse`,
  `resp_parse` and `resp_parse_differential` each ended at 6h 00m 17s with conclusion `cancelled`.
  A cancelled job reports no findings, and because the fuzz step never returned, the `Archive
  corpus` step never ran either — so every night's accumulated corpus was discarded and the next
  night restarted from scratch. Only targets that fail fast enough to finish inside the ceiling
  (`mq_registry_blob`, ~7 min) were ever visible, which is why the workflow looked merely "red"
  rather than broken. The budget is now 5h under an explicit 350-minute job timeout, leaving
  headroom for setup, build and archiving; an overrun is now a clean job timeout instead of a
  silent platform cancellation. This is the gap that let a pre-auth inline-parser hang reach
  `main` undetected.
- **RESP3 pub/sub confirmations arrived as Arrays where Redis sends Push frames.** Moon's
  *deliveries* were already correct — `message` and `pmessage` lead with `>` — but `subscribe`,
  `unsubscribe`, `psubscribe` and `punsubscribe` were built by four functions that hardcoded
  `Frame::Array` and took no protocol argument. That half-correctness is worse than no RESP3
  support at all: a client that separates out-of-band pushes from command replies by the leading
  byte reads the Array confirmation as the reply to whatever it sends *next*, and every later
  reply on that connection is off by one. Confirmations are now built as what they mean — Push —
  and each protocol's serializer renders it in that protocol's form, so RESP2 clients see
  byte-for-byte what they saw before. The tokio handler additionally serialized every pub/sub
  frame through the RESP2 serializer regardless of the connection's protocol, which downgraded
  Push back to Array; it now picks the serializer the way the codec already does.
- **RESP3 no longer keeps a subscribed connection in the RESP2 jail.** Redis lets a subscribed
  RESP3 connection run any command — that is a large part of why RESP3 exists — while Moon
  diverted every subscribed connection into a loop that answers only pub/sub verbs. RESP3
  connections now stay in the normal command loop and take a delivery branch alongside it. A
  reply and a delivery cannot tear: both are whole frames written by the same task. Two further
  divergences closed themselves as a consequence: a subscribed RESP3 `PING` now answers `+PONG`
  instead of the RESP2 `*2 pong ""` shape (the shape follows the protocol, not the mode), and a
  subscribed RESP3 connection can now run `GET`/`SET` at all, where Moon used to refuse both.
- **The subscriber-mode allow-list was stated in three handlers with two different texts and two
  different behaviours.** Only the sharded handler accepted `RESET`; `handler_single` advertised
  `HELLO` as allowed in its error message while refusing it; and none matched Redis, which names
  `(P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET`. The rule now lives in one module.
  `RESET` works everywhere, the sharded verbs are admitted, and `HELLO` stays refused (measured —
  a RESP2 subscriber genuinely cannot upgrade mid-subscription).
- **`PUBSUB NUMPAT` counted subscribers instead of distinct patterns**, and double-counted a
  pattern whose subscribers landed on different shard threads. Two clients on `p.*` answered `2`
  where Redis answers `1`. It now reuses the same gather `INFO` uses, so `pubsub_patterns` and
  `NUMPAT` cannot disagree. The bug survived because it is only visible while both subscribers
  are live — after one leaves, the buggy and correct answers coincide. (#480)
- **`UNSUBSCRIBE` with no arguments on a connection subscribed to nothing** named an empty
  channel (`$0`) where Redis sends a Null channel (`$-1`); a statically-typed client decodes the
  two differently.
- **`keyspace_hits` and `keyspace_misses` had been reporting zero for plain `GET`s on the shipped
  runtime.** `try_inline_dispatch` — the route a plain `GET key` actually takes under monoio —
  frames its reply straight into the write buffer and returns, reaching neither `string::get` nor
  `string::get_readonly` where the recorders live. Every hit-rate dashboard built on those two
  fields has been dividing by zero-over-zero. The reason it survived CI is worth stating: the
  counters read CORRECTLY under `runtime-tokio`, which is what the test matrix ran, so a test that
  proved the fix under tokio passed while the shipped runtime stayed broken. The inline path now
  records hit, miss, and the `keymiss` notification. (#477)
- **`maxmemory` now bounds what the process actually costs, not what the allocator says is live.**
  A live instance ran for an hour with `used_memory:4.21G` against a **10 GB** real footprint —
  a 2.3x gap — so a 19.2 GB cap never engaged, eviction never fired (`spill_batches_flushed:0`),
  and the OS reclaimed by swapping 9.9 GB instead: 76M page faults and 53 GB read back off disk.
  Activity Monitor showing "Real Memory 66 MB" was the *symptom*, not the reassurance it looks
  like — that is the residue left after everything else was paged out. The eviction budget is now
  scaled by how far real footprint exceeds accounted memory, so the cap holds against the number
  the machine is charged for. Two things this fix needed to get right, both caught before merge:
  the macOS reader was pulling `resident_size` (offset 16 of `task_vm_info_data_t`) rather than
  `phys_footprint` (offset 144), which on a swapped-out process reports 91 MB for a 10 GB
  instance and would have made the whole correction inert; and the ratio must ignore fixed
  process overhead — binary, thread stacks, arena metadata cost tens of MB before a single key
  exists — so it prices only the marginal cost of the data over a startup baseline and stays
  inert below 64 MiB of accounted memory. `INFO memory` gained `mem_fragmentation_ratio` and
  `maxmemory`, and `used_memory_rss` now reports footprint rather than a resident figure that
  understates the truth by two orders of magnitude under swap.
- **A malformed frame no longer closes the connection silently, and no longer eats the valid
  commands that arrived with it.** `Err(_) => break` in the read loops discarded two things: the
  parse error's reason, so a client got a bare FIN and could not tell a bad encoder from a dropped
  network, and the already-parsed batch — so `PING\r\n*-9\r\n` in a single write answered
  *nothing at all*. All three handlers now execute and flush the valid prefix, then send
  `-ERR Protocol error: <reason>` using redis-server 8.6.1's verbatim wording, then close. Also
  measured and fixed alongside it: `*-9` (any negative multibulk count) killed the connection where
  Redis ignores it; an oversized inline request closed mute even though Moon already *built* the
  correct "too big inline request" message; and inline quoting did not exist at all, so
  `SET k "a b"` became three arguments containing literal quote bytes and `GET "unclosed` was
  silently accepted as a key — the inline parser is now a port of Redis's `sdssplitargs`.
- **`MULTI` is atomic with respect to queue-time faults.** Moon had no queue-time validation, so
  `EXEC` ran whichever half of a transaction happened to parse: `MULTI / NOSUCHCMD / SET k v / EXEC`
  left `k` **set** where Redis discards everything — data corruption, not a compatibility nit. A
  command that could never run (unknown name, impossible arity, `SUBSCRIBE`, `WATCH`) is now refused
  as it is queued and poisons the transaction, and `EXEC` answers
  `-EXECABORT Transaction discarded because of previous errors.` Fixing this exposed a wider defect:
  Moon decided "am I in a transaction?" hundreds of lines *below* the `INFO` / `CLIENT` / `WS` /
  `MQ` / `PUBLISH` / `SUBSCRIBE` intercepts, so every one of those executed for real inside a
  transaction — `SUBSCRIBE ch` put the connection into subscriber mode mid-`MULTI`, and
  `INFO server` returned a 3 KB dump where Redis returns `+QUEUED`. The queue decision now sits
  directly below each handler's ACL gate, which fixes the whole class at once.

- **`HELLO` no longer contradicts `INFO replication` about what the server is.** `hello_acl` built
  its reply with `mode` and `standalone` and `role` and `master` as compile-time literals, so a
  replica told `HELLO` it was a master while telling `INFO` it was a slave — on the same connection.
  Both fields now read `ReplicationState`/`ClusterState`, the same source `INFO` uses. Note that
  Redis deliberately uses three vocabularies for this one fact (`HELLO` -> `replica`, `INFO` ->
  `slave`, `ROLE` -> `slave`), verified against a live replica pair on redis-server 8.6.1, and Moon
  now matches each.
- **The client-compat harness no longer needs a live Moon defect to test itself.** Its
  divergence-classification test borrowed a real bug as its fixture, so it FAILED whenever
  someone fixed that bug — three fixtures were burned this way (GET-inside-MULTI #457,
  SISMEMBER RESP3 #463, and `COMMAND COUNT` here). It now fabricates the divergence through a
  test-only `inject_moon_reply` hook, with a guard test asserting the shipped manifest never
  uses it. The `identity_command_count` and `identity_role` waivers were retired as fixed.
- **`CLIENT INFO` / `CLIENT LIST` report the real `laddr`.** The field was the literal
  `laddr=127.0.0.1:0` inside the format string, so every client on every listener reported port 0 —
  a monitoring agent using `laddr` to tell listeners apart saw nothing.
- **CI now tests the runtime Moon actually ships (`check-monoio`).** Every CI job that *executed*
  tests did so under `--no-default-features --features runtime-tokio,…`, while Moon's default
  feature set — and what ships on Linux — is `runtime-monoio`. 26 monoio integration test files and
  30 monoio-gated `src/` files were unreachable by CI. That is how the v0.8.6 inline-GET ACL bypass
  shipped green: it was wrong only on the monoio dispatch path, which no CI job could see. The new
  job runs on the self-hosted Linux runner (the only place monoio's io_uring driver executes at
  all), uses the default feature set, and invokes `cargo nextest run --profile ci` so the repo's
  existing flake policy applies — a bare `cargo test` has no retries, and an intermittently-red
  required job gets disabled, which is worse than no job because it still looks like coverage.
  Measured before landing: **5145 passed, 1 flaky, 244 skipped, exit 0, 80.3s**.

  Proven by negative control rather than asserted: a deliberate defect on the monoio-only
  `try_inline_dispatch` path **passes** the tokio suite 6/6 and **fails** the new job 3/6.
  `tests/ci_covers_monoio.rs` guards the job against being silently weakened — wrong feature set,
  `continue-on-error`, a bare `cargo test`, or a shared `CARGO_TARGET_DIR` all fail the suite.

  The job's first cut proved the premise but not the driver: `MOON_NO_URING: "1"` lived in the
  workflow-level `env:`, which merges into every job and cannot be unset by one, so the job whose
  whole point is io_uring ran with io_uring force-disabled. `monoio_yield_overhead_is_microscopic`
  caught it at 1.45ms/yield (the `sleep(ZERO)` timer-park signature). That variable is now per-job,
  and `ci_covers_monoio.rs` asserts **both** scopes. The guard suite itself then failed on
  Windows — it matched `"\n  <job>:\n"` against a CRLF checkout — so it now normalizes line
  endings and carries a platform-independent CRLF regression test (Windows is skipped on every
  PR, so that class is invisible until a main push).

- **Client-compat harness: raw-RESP diff against a real `redis-server`
  (`scripts/test-client-compat.sh`).** Moon's existing Redis comparison
  (`scripts/test-commands.sh`) goes through `redis-cli`, which renders replies
  to text before any assertion can see them — it even strips `(integer) ` — and
  never sends `-3`, so the entire RESP3 surface was uncompared. That blindness
  is why ~22 type-level compatibility defects survived into v0.8.5. The new
  harness speaks RESP on a raw socket and compares in a fixed order — TYPE, then
  SHAPE, then VALUE — across the full {RESP2, RESP3} × {standalone, MULTI/EXEC,
  pipeline} matrix, so a finding says which of the three diverged and whether a
  reply changes shape by context. A missing `redis-server` FAILS
  (`ERR_NO_ORACLE`) rather than skipping. New CI job `client-compat` on the
  self-hosted runner; the manifest carries a recorded baseline of reasoned
  waivers so the job is a ratchet — a new divergence fails it, and `--strict`
  fails the moment a waived divergence is fixed and its waiver goes stale.
  First full run vs Redis 8.6.1: 152 comparisons, 94 pass, 58 waived, plus 34
  named missing `INFO` fields via `--info-manifest`.

- **`WATCH` / `UNWATCH` now actually guard a transaction on the production
  dispatch paths.** Both commands parsed and answered `+OK`, and the tokio and
  embedded handlers re-checked the recorded versions at `EXEC` — but the two
  paths clients really reach (`handler_monoio`, `handler_sharded`) never
  consulted the watch set at all. A conflicting write from another client
  committed anyway, so every check-and-set built on `WATCH` (inventory
  decrement, balance transfer, leader election) silently degraded to
  last-writer-wins. Four defects, all fixed together because a partial fix is
  indistinguishable from none:

  1. **The watch set was not consulted at `EXEC`** on the monoio and sharded
     handlers. `EXEC` now aborts with a RESP null array on any version
     mismatch, and clears the watch set on *both* outcomes by construction
     (`mem::take`) rather than by a `clear()` each exit path has to remember.
  2. **A watched key on another shard was read from the local slice** — a
     different database entirely, so the version compared was some unrelated
     key's or zero. `WATCH` now snapshots versions where the keys live, via a
     new `ShardMessage::ReadVersions` (one hop per owning shard, not per key),
     and a watch set spanning shards is classified and refused `CROSSSLOT`
     like a cross-shard `MULTI` body already was.
  3. **`WATCH` inside `MULTI` was queued** as an ordinary command instead of
     being refused, and `WATCH` with no arguments answered "unknown command"
     instead of an arity error.
  4. **Delete + recreate was invisible (ABA).** Versions are per-entry and die
     with the entry, so every incarnation of a key started at
     `INITIAL_VERSION`: `DEL k` + `SET k` handed the watcher back the exact
     token it had recorded and `EXEC` committed on a key that had been
     destroyed and rebuilt underneath it — where Redis aborts. Entries are now
     stamped from a per-database creation ticket, so a recreated key is
     observably a different incarnation. Restored keys draw tickets too:
     versions are not persisted, so otherwise the first key created after a
     restart would collide with the whole restored population.

  Residual risk, stated rather than buried: the ticket shares the entry's
  24-bit version field and so wraps at 16,777,216 creations (~18s of saturated
  single-database insert at the measured 914K/s). A miss now needs that wrap to
  land inside one client's open `WATCH`..`EXEC` window *and* hit the one
  watched key — ~1 in 16.7M, against the pre-fix certainty. Only a wider
  incarnation field removes it entirely; `WatchToken` stays a named struct so
  adding one later does not churn the call sites.

  New suite `tests/watch_cas_transactions.rs` (10 wire-level tests, raw-byte
  assertions, two connections, run at both `--shards 1` and `--shards 4`),
  plus WATCH/CAS entries in `scripts/test-consistency.sh` and
  `scripts/test-commands.sh`.

- **RESP3 reply types now match Redis, and no longer change with the calling
  context.** A live sweep against `redis-server` 8.6.1 found the conversion
  table wrong in both directions and, structurally, unable to be right: it keyed
  only on the command NAME, while `WITHSCORES`, `WITHVALUES` and a `<count>`
  argument are what actually decide the reply shape. `ZRANGE … WITHSCORES`
  arrived as a flat array of bulk strings instead of pair-wrapped `[member,
  Double]` — enough to make an unmodified redis-py raise
  `ValueError: not enough values to unpack`, so every RESP3 application using
  sorted-set scores was broken outright. `HRANDFIELD WITHVALUES` and
  `ZRANDMEMBER WITHSCORES` answered a Map where Redis answers an array of pairs.
  In the other direction the server over-converted: all seven of `SISMEMBER`,
  `HEXISTS`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `SETNX` and `MSETNX` returned
  Boolean where Redis returns Integer, and `INCRBYFLOAT`/`HINCRBYFLOAT` returned
  a lossy Double (`,10.6`) where Redis returns the exact Bulk
  `"10.59999999999999964"`.

  The conversion is now decided by `(command, args)` at one policy choke point
  (`Resp3Shape` in `src/protocol/resp3.rs`) instead of by 11 call sites across
  three handlers. Because the cross-shard reply loop no longer has the command's
  args by the time its batch returns, the shape is classified at ENQUEUE time
  and the 1-byte `Copy` tag travels in `RemoteMeta` — no per-command allocation
  on the shard hot path. `execute_transaction_sharded` now takes the connection's
  protocol version and converts each inner reply with its own command, so a
  command answers the same shape standalone, inside `MULTI/EXEC` and inside a
  pipeline; previously `SMEMBERS` was a Set outside a transaction and a flat
  Array inside one. `CONFIG GET` (Map) and `CLIENT INFO` (Verbatim) are fixed at
  their intercepts, which short-circuit the dispatch exit entirely — the reason
  `CONFIG` could never be fixed before. Also added: `ZMSCORE` and `GEOPOS`
  Doubles with Null preserved, `SPOP <count>` as a Set, `ZPOPMIN`/`ZPOPMAX`
  Double scores (flat with no count, wrapped with one), and `XINFO STREAM` as a
  Map.

  Emptiness no longer changes the reply type either: `HGETALL` and `CONFIG GET`
  on a miss answered `*0` where Redis answers `%0`, so a client dispatching on
  the type byte broke on exactly the path it hits most. Every case in the
  harness populated its key first, which is how that survived a full green run —
  five miss-path cases were added so it stays diffed against the live oracle.

  RESP2 is byte-identical — pinned by a test written before the fix. Harness
  result vs Redis 8.6.1: **157 pass / 0 fail / 25 waived**, up from 98 / 0 / 54,
  with all 13 now-stale waivers deleted and `--strict` green. New suite
  `tests/resp3_type_fidelity.rs` (13 tests) asserts the wire type byte directly.
  Known remainder, waived and reasoned: `XINFO STREAM` is now the right TYPE but
  still reports 7 fields to Redis's 16 — the missing ones need real stream
  bookkeeping and are tracked separately rather than fabricated.
- **A key whose disk-offload spill was in flight was invisible to the whole
  server — `DEL` on it was silently undone (#459).** `evict_one_async_spill`
  freed the hot entry the moment the `SpillRequest` was queued, and the key
  was only registered in `cold_index` when the completion landed. In between
  it existed in no plane the database consults: `spill_inflight` held only a
  request id and was read solely by the completion path. The eviction code
  documented the window as an acceptable "brief read-miss" backstopped by the
  AOF — but the AOF backstops *durability*, not *visibility*, and nothing
  considered a write landing inside the window. Measured on 400 × 4 KiB keys
  against a 512 KiB cap: `DBSIZE` answered 124 for 400 acked keys and then
  climbed to 400 on its own; `GET`/`EXISTS` denied live keys that returned
  unaided 250 ms later; and 277 of 400 `DEL`s answered `:0` and were then
  reversed by the completion — which publishes into `cold_index`
  unconditionally, so those resurrections reached the manifest and survived
  restart. A client that deleted data got it back.

  `spill_inflight` is now a real third storage plane carrying the payload
  (the same refcounted `Bytes` the queued request already pins — a refcount,
  not a copy, and no extra peak memory). Reads promote from it with no disk
  read at all, across all three dispatch paths (`promote_cold_if_present` for
  collections and Lua/MULTI, the monoio async GET pre-warm, and the inline
  `GET` fast path). `EXISTS`/`DEL` count it, `DBSIZE`/`logical_len` count it,
  and `KEYS`/`RANDOMKEY` enumerate it. `DEL`, an overwriting `SET`, and a
  promoting read each retire the record, which withdraws the completion's
  authorization to publish — that is what makes a delete inside the window
  final. Unpublished completions are counted as `spill_completion_superseded`
  in INFO. Non-spilling servers pay one `is_empty()` load on the affected
  paths. Known remaining gap, documented at the call site: `SCAN`'s ordered
  cursor does not merge the unordered in-flight plane, so it may skip a key
  for the milliseconds its spill is queued — within SCAN's contract, unlike
  `KEYS`.
- **`GET` inside `MULTI` was executed instead of queued (monoio).** Third
  defect from the same ungated inline read path, and the one most visible to a
  working client: `MULTI; GET k; EXEC` answered `+OK`, `$1 v`, `*0` where Redis
  answers `+OK`, `+QUEUED`, `*1[$1 v]`. The client receives a value where it
  expects `+QUEUED`, and then an `EXEC` that silently omits the read — a
  redis-py/go-redis transaction returns an empty result set for an exchange it
  believes succeeded. `can_inline_writes` already carried `!conn.in_multi`
  (so `SET` queued correctly); `can_inline_reads` now carries it too. `MGET`,
  not being inline-eligible, queued correctly throughout, which is what
  isolated the path. Found by the new `scripts/test-client-compat.sh` raw-RESP
  harness on its first run against a real redis-server. New suite:
  `tests/multi_queues_inline_get.rs`, including controls pinning that `MGET`
  and `SET` still queue and that a plain `GET` outside a transaction still
  takes the fast path (measured: 2000/2000 GETs still `local_inline`, before
  and after a completed transaction).
- **`CLIENT TRACKING` answered `+OK` and then never invalidated (monoio).**
  Same root cause: the inline GET path also skips
  `tracking::invalidation::track_read_keys`, so a client-side-caching client's
  own `GET`s were never registered and nothing ever invalidated them — the
  cache served stale data indefinitely. At `--shards 1` this was total; at
  `--shards 4` it depended on whether the key hashed to the reader's own
  shard, which also made the existing `mset_invalidates_every_second_arg_key`
  test flaky (observed failing 2 of 3 runs on 0.8.5). Reads from a connection
  with tracking enabled now take the generic path. Deliberately gated on this
  connection's own `tracking_state.enabled`, not the process-global
  `tracking_active()`: only a connection's own reads populate its invalidation
  set, so one caching client must not push every other connection off the fast
  path.
- **`CLIENT TRACKING ON BCAST` with no `PREFIX` never invalidated anything.**
  Redis treats prefix-less BCAST as "invalidate me for every key", but the
  handlers only registered broadcast interest inside
  `for prefix in &config_parsed.prefixes`, so a prefix-less BCAST client
  registered nothing — at any shard count, and regardless of what it read
  (BCAST does not depend on reads). `parse_tracking_args` now normalises
  prefix-less BCAST to the empty prefix, which `TrackingTable`'s
  `key.starts_with(prefix)` match treats as "all keys"; one change fixes all
  three handlers.

  The default (unrestricted, non-tracking) connection keeps the inline fast
  path byte-for-byte, verified against
  `moon_dispatch_path_total{path="local_inline"}`: 2000/2000 GETs still
  inlined with and without `--requirepass`, 0/2000 for restricted and
  tracking connections.

### Added
- **`INFO Server` now reports `num_shards`** (#497), the resolved shard count rather than the
  configured one, so `--shards 0` auto-detection reports the number actually in effect. A client
  that cannot operate against a multi-shard instance — one committing a cross-key transaction per
  request — previously had no way to refuse at connect time except a two-key co-location canary,
  which is slow and still a guess. `redis_mode`/`cluster_enabled` do not answer this: a
  single-process Moon with several shards is `standalone` and still routes keys across threads. The
  value is never 0; an embedded harness that never records a count reports the single-shard truth,
  because a zero reads as a valid answer and is not one.

- **An acceptance suite driven by unmodified redis-py** (`scripts/client-compat/redis_py/`), wired
  into the `client-compat` CI job. The raw-RESP differ compares bytes against a real redis-server;
  it is precise and blind to everything a client library does *around* the reply — the handshake it
  opens with, the connection it reuses, the Python type it decodes into, the second command it
  issues on your behalf. A server can answer every byte correctly and still be unusable from
  redis-py. The suite therefore drives redis-py's own idioms — connection pools, `pipeline()`,
  `pubsub()`, `scan_iter()`/`hscan_iter()`, `redis.lock.Lock`, `from_url`, RESP2 and RESP3
  handshakes, `WATCH` optimistic locking — rather than hand-rolled sockets. Stdlib `unittest` and
  the distro `python3-redis` package, because the runner has no pytest and PEP 668 blocks pip.

  It found three defects on its first run, each pinned inside the suite so that fixing one breaks
  the run with an actionable message rather than leaving a stale skip: at `--shards >= 2`, an
  `MGET` in the same pipeline batch as the `SET`s that wrote its keys returns nulls despite those
  `SET`s acking `+OK` earlier in the batch (a silent read-your-own-writes violation), `EVALSHA` of
  a **single-key** script is rejected with `CROSSSLOT` (which breaks `redis.lock.Lock.release()`),
  and `CLIENT INFO` reports a literal `cmd=NULL` for every connection.

  The two multi-shard defects fire for roughly **half** of keys — decided by which shard owns the
  key relative to the connection's own shard — so each pin runs twenty distinct keys rather than
  one. A single-trial pin was tried first and made CI flaky, which is how the ~50% rate was found;
  the amplified form is 0/10 flaky runs on both macOS and Linux, and still fails loudly when the
  underlying bug is fixed.

- **Nine INFO fields a standard monitoring stack reads.** `tcp_port`, `uptime_in_seconds`,
  `uptime_in_days`, `aof_last_write_status`, `aof_last_bgrewrite_status`,
  `rdb_changes_since_last_save`, `sync_full`, `sync_partial_ok` and `sync_partial_err` are now
  emitted, each from a real source rather than a constant: uptime from a start instant captured
  before the listener binds, `rdb_changes_since_last_save` from a sharded
  keyspace-mutation counter reset at save completion, and the three `sync_*` counters recorded at
  the one point in the PSYNC handshake where full-vs-partial is still distinguishable (`PSYNC ? -1`
  counts as a full resync the replica ASKED for, not a partial that failed). `tcp_port` reports the
  configured listener port, not the port the INFO connection arrived on — behind a container port
  map the two differ, and the field exists so a client can hand a peer a reachable address.
  The four fields Moon cannot answer truthfully are recorded as waivers with reasons instead of
  being emitted: `latest_fork_usec` (Moon never calls `fork(2)`; BGSAVE snapshots in-process), the
  two `client_recent_max_*_buffer` high-water marks (untracked, and tracking them means a counter on
  every connection read and write), and `used_memory_lua` — which was implemented and then withdrawn
  when measurement showed the value the shard can publish is ~2 orders of magnitude below the real
  VM footprint on the shipped monoio runtime (80 bytes against tokio's 26183, same `setup_lua_vm`,
  ruled out as Cargo feature unification). This keeps the INFO emitter's standing rule intact: a
  wrong number on a dashboard is worse than an absent one.

- **`ZRANK`/`ZREVRANK ... WITHSCORE`** (#521), the Redis 7.2 option — previously an arity error, so
  a 7.2-aware client asking for the rank and the score in one round trip got a hard failure. A hit
  is the two-element array `[rank, score]`; a miss (absent key or absent member) is the null ARRAY
  `*-1`, not the null bulk the option-less form answers, so `WITHSCORE` changes the null type as
  well as the hit type — measured against redis-server 8.6.1, and a statically-typed client decodes
  the two differently. The token is SINGULAR, unlike the `WITHSCORES` that `ZRANGE` takes; the
  plural is a syntax error here, and only a FOURTH argument is an arity error (Redis distinguishes
  the two and so does Moon). The score is emitted as a RESP double, so RESP3 gets `,1` while RESP2
  keeps the identical bulk-string bytes. Both entry points were fixed — `zrank` and
  `zrank_readonly` each carried their own arity check, and which one answers is decided by shard
  routing, so fixing one would have left the option working on some keys and failing on others.
  `ZRANK`/`ZREVRANK` arity in `COMMAND INFO` is now `-3`, matching Redis 7.2+.
- **`RPOPLPUSH source destination`** (#520) — previously "unknown command" even though `LMOVE` and
  `BRPOPLPUSH` both worked. It is deprecated in Redis but never removed, and it is the form baked
  into a decade of client code: `redis-py`, `jedis`, `go-redis` and `node-redis` all expose it as a
  first-class method, so `r.rpoplpush(...)` failed outright. The name was already in the workspace
  routing table, in the metrics labels, and was the internal rewrite target of `BRPOPLPUSH` —
  missing only from the two places a client reaches, the dispatch table and `metadata.rs`
  (`COMMAND INFO rpoplpush` answered an empty array, so driver feature-detection concluded the
  command did not exist). Implemented by delegating to `LMOVE`'s body with `RIGHT LEFT` rather than
  by duplicating the list logic, and registered with LMOVE's two-key spec (arity 3, write,
  first_key 1, last_key 2, step 1, ACL `@list @slow`). Along the way the blocking-wakeup guard —
  open-coded at eight dispatch sites — moved behind one shared predicate, which fixed a drift the
  duplication had hidden: the two connection handlers woke `LMOVE`'s SOURCE key, which no blocked
  reader waits on, while the SPSC handler correctly woke its DESTINATION. A `BLPOP dst` was
  therefore woken or left to time out depending on which shard owned the key.

- **Keyspace notifications (`notify-keyspace-events`).** Moon had none: cache-invalidation
  frameworks and change-data-capture consumers subscribe to `__keyspace@<db>__:<key>` and
  `__keyevent@<db>__:<event>` and got silence. Both channel families are now published, gated by
  the full Redis flag model — including the parts that are not what the letters suggest, all
  measured against redis-server 8.6.1 rather than recalled: `CONFIG SET KEA` reads back as `AKE`,
  `mn` as `nm`, `Km` as `Km`, and `A` deliberately excludes `m` (keymiss) and `n` (newkey) so it
  stays safe to enable in production. Events wired: `set`, `incrby` (INCR publishes `incrby`, not
  `incr`), `rename_from`/`rename_to` (RENAME emits BOTH halves, carrying different keys),
  `expired`, `keymiss`. Off by default and genuinely zero-cost when off — one relaxed atomic load.
  Delivery reaches subscribers on **every** shard, not just the one that owns the mutated key: a
  local-only publish would pass at `--shards 1` and silently drop roughly (N-1)/N of events at
  `--shards N`.
- **`ROLE`, `RESET`, and a real `COMMAND` introspection surface.** `COMMAND` and `COMMAND COUNT`
  each returned the OTHER'S RESP TYPE — bare `COMMAND` replied `:0` (an Integer where an Array
  belongs) and `COMMAND COUNT` replied `*0` (an Array where an Integer belongs); `COMMAND
  INFO`/`DOCS`/`LIST`/`GETKEYS` all replied an empty array. A driver that builds its command map at
  connect time does not read that as "unsupported", it reads it as a protocol violation, and
  `redis-cli` renders `:0` and `*0` identically as "0" so it never showed up by eye. All six now
  derive from `COMMAND_META`, so registering a command is what makes it introspectable and there is
  no second table to drift. `ROLE` and `RESET` were unknown commands: `RESET` was registered in the
  metadata table with full flags while dispatch rejected it (the same advertise-then-reject class as
  WATCH/UNWATCH before v0.8.6), and a partial `RESET` existed only inside `handler_sharded`'s
  subscribe-mode loop, so it worked if you happened to be subscribed on one runtime and nowhere
  else. `RESET` is wired on every production path and on `handler_single` (the in-process
  single-shard handler that only tests reach) so the third copy of this surface cannot drift
  unnoticed the way it just did. `ROLE` is answered from the shared dispatch table instead,
  reading the process-global replication handle that `INFO` already uses — which is what lets a
  QUEUED `ROLE` work: `EXEC` replays the queue through `dispatch()`, so a connection-layer
  intercept would have executed `ROLE` at queue time and dropped it from the `EXEC` array,
  shifting every later result index for the client. Caught by the client-compat harness.

### Changed
- **Active expiry runs on a deadline-ordered index instead of probabilistic sampling** (#541).
  Every hot entry with a TTL now has an `(expires_at_ms, key)` pair in a per-database ordered
  index, maintained O(log n) by the storage-layer writers; the 100ms sweep pops exactly the DUE
  keys off the front — no 20-key random sample (which went blind when due keys were a small
  fraction of the volatile population, leaving them resident for many ticks), and no O(N)
  full-map scans (three per tick on any database with even one TTL'd key). Measured on a
  100K-volatile-key database: 1.36ms → 47ns per tick, and the old cost exceeded the sweep's own
  1ms budget before it had expired anything. The hash-field-TTL sweep is now latch-gated too, so
  databases that never touch HEXPIRE skip its scan entirely (the scan itself when the latch is up
  remains O(N) — #543). `INFO`'s `expires` count reads the index, O(1). Along the way, two
  writers that bypassed the expiry machinery were rerouted: GETEX's TTL options now go through
  `set_expiry` (previously a GETEX-only TTL never armed the sweep latch, so the key was invisible
  to active expiry forever), and EXPIRE-family commands hitting an already-expired key now hide
  and queue it for the emitting drain (#542 semantics) instead of deleting it silently with no
  `expired` notification and no dual-plane DEL. Review follow-ups: GETEX `EX`/`EXAT` seconds
  values that overflow the millisecond conversion now answer the range error instead of
  wrapping (debug builds panicked); the sweep only drops an index pair that is provably stale
  (entry gone or TTL retargeted) so a backwards wall-clock step can't discard live pairs; the
  per-key budget clock read is batched to every 64 pops; and sweep 2 lowers the hash latch from
  its own reap outcomes instead of a second O(N) rescan.
- **`volatile-ttl` eviction picks the exact nearest-expiry victim** (#551). The policy now reads
  the head of the #541 deadline-ordered expiry index — O(log n), always the globally soonest
  deadline — instead of sampling `maxmemory-samples` random volatile keys and taking the sample's
  minimum, which could evict a key hours from expiring while the one expiring in seconds
  survived. `maxmemory-samples` still governs the LRU/LFU/random policies, which remain
  sampling-based.

- **The `INFO field coverage` CI step is now a hard gate**, no longer `continue-on-error`. The
  pinned-field harness gained a waiver syntax (`field  # WAIVED: <reason>`) that refuses an
  unreasoned waiver at load time (exit 2) and reports a waiver on a field Moon has since started
  emitting as a failure — so the list cannot go stale unnoticed the way the registry sweep's did.

- **`CLUSTER SHARDS`, `CLUSTER MYSHARDID`, `READONLY` and `READWRITE`** — the four verbs a
  cluster-aware client needs to bootstrap. `CLUSTER SHARDS` reports every shard cluster-wide, one
  entry per master with its replicas, master first; a shard that has lost every live node reports an
  empty `slots` array while still listing the dead node as `role: master, health: fail`. The top
  level is an Array under both protocols and only the shard and node entries change shape — a Map
  under RESP3, a flat array under RESP2 — which falls out of building them as `Frame::Map` and
  letting each serializer render it, the same approach the RESP3 pub/sub work used. A node entry
  carries exactly `id`, `port`, `ip`, `endpoint`, `role`, `replication-offset`, `health`, in that
  order, because under RESP2 a client may read it positionally. `READONLY` lets a replica serve
  reads for slots its own master owns; a WRITE still answers `MOVED`, the asymmetry a
  "just return +OK" implementation gets wrong.
- **`INFO` reports cluster identity honestly.** `redis_mode` and `cluster_enabled` were the hardcoded
  literals `standalone` and `0`, under a comment promising the cluster subsystem would say otherwise
  — nothing ever did. An SDK branches on `redis_mode` before it ever calls `CLUSTER SHARDS`, so a
  server answering SHARDS correctly while reporting `standalone` stayed undiscoverable as a cluster.
- **`MONITOR` — the command feed.** `redis-cli monitor` now works against Moon, in Redis's exact
  line format: `+<unix>.<micros> [<db> <addr>] "CMD" "arg" …`, arguments quoted and escaped per
  byte (`sdscatrepr` semantics — `"` `\`, `\n` `\r` `\t`, `\a` `\b`, and `\xHH` for everything
  outside printable ASCII, so UTF-8 escapes per byte rather than per character). The line is a
  SimpleString under **both** RESP2 and RESP3 — measured; Redis does not use a Push frame here,
  and the reflex to make it one after the RESP3 pub/sub work would have been a new divergence.
  `AUTH`'s arguments and the credentials in `HELLO … AUTH` render as `(redacted)`, decided before
  any argument is written rather than filtered afterwards.

  Two behaviours are worth knowing because they are not the obvious implementation. First,
  administrative commands are hidden at **subcommand** granularity: `CONFIG *`, `SLOWLOG *`,
  `LATENCY *`, `ACL LIST/SETUSER` and `CLIENT LIST` never reach a monitor, while `INFO`, `DBSIZE`,
  `LASTSAVE`, `CLIENT GETNAME/ID`, `ACL WHOAMI/CAT` and `CLUSTER INFO/MYID` do. Moon's own
  `CommandFlags::ADMIN` is container-granular and could not express that split — using it would
  have hidden six commands Redis shows — and Redis feeds the entire `EVAL` family despite flagging
  it `skip_monitor`, so neither flag is consulted; the rule is stated explicitly and pinned
  row-by-row against the measured oracle. `MONITOR` is absent from its own feed as a consequence of
  that general rule rather than a self-suppression special case. Second, a monitor that stops
  reading has its **connection dropped**: silently skipping lines would leave an operator unable to
  tell a quiet server from a lossy feed, and blocking would let one slow TCP reader stall every
  shard.

  A monitor connection may not touch the keyspace, matching Redis
  (`-ERR Replica can't interact with the keyspace`). The refusal set is measured, not derived from
  a flag: `DBSIZE`, `KEYS`, `SCAN`, `RANDOMKEY`, `FLUSHALL`, `FLUSHDB`, `SWAPDB`, `EVAL`,
  `PUBLISH` and `MEMORY USAGE` name no key yet are all refused, while `PING`, `INFO`, `TIME`,
  `ECHO`, `COMMAND`, `LASTSAVE`, `WAIT`, `SELECT`, `CLIENT`, `ACL`, `SUBSCRIBE` and `RESET` are
  served. Neither `first_key` nor Moon's `WRITE`/`READONLY` flags reproduce that split — Moon
  flags `PING` and `INFO` readonly and Redis does not — so the rule is stated explicitly and
  pinned row by row against the oracle.

  Commands issued by a Lua script are fed too, carrying the literal `lua` in place of a peer
  address and appearing in execution order after the `EVAL` line — matching Redis. A script command
  never passes a connection handler, so it needs its own hook; without it an operator watching a
  script-driven workload would see every `EVAL` and none of its effects.

  `MONITOR` requires the `admin` ACL category, and costs one relaxed atomic load per command when
  nobody is attached — every other step lives behind that load. While a monitor IS attached the
  inline fast path stands down, because it answers straight from the read buffer and never sees a
  peer address; the feed is therefore correct by construction on that path rather than by a hook
  that must be kept in sync. Fast-path retention when unattached is confirmed by
  `moon_dispatch_path_total{path="local_inline"}`, not inferred from latency.
- **Sharded pub/sub: `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`, and `PUBSUB SHARDCHANNELS` /
  `SHARDNUMSUB`.** Deliveries carry the `smessage` event name. The sharded namespace is a
  genuinely separate map from the plain one in both the per-shard registry and the
  remote-subscriber map, so `SPUBLISH ch` cannot reach a `SUBSCRIBE ch` and vice versa — the two
  may share a channel *name* while being different destinations, and that separation is
  structural rather than a filter each call site has to remember. Cross-shard delivery reuses the
  existing batched fan-out with a sharded marker on each entry, and is proven at `--shards 4`:
  a local-only registry would have passed every single-shard test while dropping (N-1)/N of
  deliveries in production. `SPUBLISH` was absent from `COMMAND_META` entirely, while
  `SSUBSCRIBE` and `SUNSUBSCRIBE` were declared there but answered "unknown command" by the
  dispatcher — so `COMMAND COUNT` was advertising verbs Moon could not run.

- **CI migration: hosted-only PR gate + local merge bar** — the three self-hosted jobs
  (`Check`, `Check (monoio)`, `Client compat`) serialized on the single moon-dev runner on every
  PR (17–24m wall, with manual dispatches queueing behind them). The PR gate is now entirely
  GitHub-hosted and parallel (Lint, Check on ubuntu-latest with sccache + rust-cache, MSRV,
  Memory gate, ~5–8m); the monoio and client-compat legs moved to main-push + `workflow_dispatch`,
  and — before every push — to the new **`scripts/ci-local.sh`** (host lint gates + both full
  suites in the moon-dev VM; `--full` adds the client-compat harness and the macOS host suite).
  The script captures exit codes directly (no piped gates), keeps VM builds on VM-local target
  dirs, pins `MOON_BIN` for the compat harness, and fingerprints the working tree at start/end —
  a branch switch or edit mid-run marks the run INVALID (exit 3) rather than reporting a false
  green (both the fail path and the tripwire were attack-tested before landing).
  `tests/ci_covers_monoio.rs` still guards the monoio job's integrity; its trigger scope is the
  documented tradeoff. Console Integration was already path-gated and Integration Tests
  label-gated; both unchanged.

### Removed
- **The dead `connection::command` stub** (#469). `COMMAND COUNT` replying an empty array (and
  bare `COMMAND` replying `Integer(0)` — each returning the other's RESP type) was fixed in #471,
  which routed both dispatch sites to `introspect::command`; verified on the wire, `COMMAND COUNT`
  now answers `:262` and `COMMAND INFO`/`DOCS WATCH` answer real specs. The superseded stub was
  left behind unreferenced, together with three unit tests that still asserted its wrong replies —
  a green test pinned to code nothing could reach. Both are deleted, and the issue's three probes
  are pinned as one test beside the live handler.

## [0.8.5] — 2026-08-08

### Added
- **Cluster mode now works on the default (monoio) runtime — v0.9 W0/C-1
  (#405).** The cluster control plane (bus listener, gossip ticker, failover
  election) runs on a dedicated `cluster-ctl` std thread hosting a
  current-thread tokio runtime, on BOTH server runtimes. Before this the
  monoio startup path never spawned the bus or the ticker: `--cluster-enabled`
  accepted `CLUSTER MEET` but no peer ever learned anything. Under tokio the
  control plane previously shared the listener runtime with the accept loop
  and every connection, so gossip starved under load (observed as PFAIL
  detection stalling); the dedicated thread fixes that too. The unused,
  untested monoio duplicates of the bus/gossip/election code were deleted —
  one control-plane implementation on both runtimes. New e2e suite
  `tests/cluster_formation.rs` proves a real 3-node cluster forms via
  MEET + gossip and flags a killed node, per runtime.

### Fixed
- **Deep-review wave (2026-08): long-uptime and large-scale correctness.**
  Eight fix groups from a six-dimension architecture review (durability
  ordering, long-uptime resource growth, concurrency, cluster correctness,
  long-horizon arithmetic, silent degradation):
  - *Eviction/metadata*: `CompactEntry` now stores a full-width u32
    `last_access` (repurposed padding — zero size cost) and a 24-bit entry
    version starting at 1. Fixes LFU decay that was effectively random
    (u16/u32 domain mix truncated `as u8`), LRU inversion for idle gaps
    beyond ~9.1h, `OBJECT IDLETIME` wrapping at 18.2h, and WATCH/EXEC's
    8-bit version ABA (wrap now needs 16.7M writes; version 0 reliably
    means "absent" so WATCH detects creation).
  - *AOF everysec*: tokio deadline-fsync paths no longer swallow errors and
    record success; all four sites (both runtimes/layouts) latch
    `aof_last_fsync_status:ok|err` + `aof_fsync_failures` into INFO.
  - *Spill*: a failed background spill pwrite re-inserts the already-evicted
    key into the hot table (payload carried back in the completion) instead
    of silently serving nil until restart; counted as
    `spill_failed_reinserted` in INFO.
  - *AOF rewrite prune*: the old generation is deleted only after a new
    manifest-sync flush barrier (pending deferred ShardManifest commits
    made durable — they relied on the old incr as their crash backstop) and
    an explicit durable dir-fsync of the manifest flip.
  - *CLIENT TRACKING*: the documented `max_keys` bound is enforced (evict +
    invalidate instead of unbounded growth).
  - *Cluster*: inline GET/SET fast path is disabled in cluster mode (was
    bypassing MOVED/ASK entirely); election acks are now actually received
    (read back on the request stream), epoch-bound and voter-deduped; the
    election winner no longer claims an unvoted epoch; graceful `CLUSTER
    FAILOVER` errors instead of permanently wedging automatic failover;
    `ClusterState` migrated to `parking_lot::RwLock` (no poisoning, ~25
    `.unwrap()` sites removed from the per-command path).
  - *Fail-loud + bounds*: vector background-compaction failures now log at
    every layer; CDC reaps disconnected subscribers on write-idle shards;
    `TemporalRegistry` is bounded (262K bindings/shard, oldest evicted).
- **Durability wave 1 (#452, #54): AOF rewrite-window append drops, degraded-state
  visibility, escalated reason-DEL backpressure, and a fail-loud WAL mid-chain
  tear policy.** (1) While a rewrite fold ran, the writer thread was out of its
  recv loop, so under sustained pipelined writes the bounded (10k) append
  channel saturated and acked records were dropped — lost even on a clean
  restart, and default-on since #433 made rewrites automatic. A per-writer
  `RewriteOverflow` spill buffer (aof_rewrite_buf equivalent; 256 MiB cap,
  strict ordering, all six fold arms on both runtimes) now buffers the
  overflow and drains it into the committed incr right after the fold;
  `INFO persistence` gains `aof_rewrite_overflow_spilled`. Merge-base A/B
  (tests/recovery_matrix_w1.rs): main lost/error-failed ~5.6k acked writes per
  hit; fixed is exact across SIGKILL + recovery. (2) Any dropped acked append
  now latches sticky `aof_last_append_status:err` (INFO) via a single
  accounting helper. (3) Eviction/expiry reason-DELs get a 100× escalated
  backpressure bound (500ms) plus a dedicated `aof_reason_del_dropped`
  counter — a dropped reason-DEL means restart replay resurrects data clients
  were told was gone. (4) WAL v3 replay now REFUSES to continue past a
  corrupt record when later segments exist (mid-chain tear = on-disk
  corruption; applying later segments silently replays operations from after
  a hole) — `MOON_WAL_SALVAGE=1` is the explicit operator override; a torn
  FINAL segment stays the benign crash-tail it always was.

  Adversarial-review hardening round (pre-merge): the rewrite fold's
  exactly-once contract now extends to the overflow buffer — a snapshot
  **cut** (`mark_cut`, recorded at the fold's atomic snapshot instant)
  splits spilled entries so a COMMITTED fold discards pre-snapshot spills
  (their effects are in the new base; replaying them would double-apply
  INCR/APPEND/LPUSH) while an aborted fold still writes everything.
  Producer paths are fully gated (`spill_first` on every enqueue leg,
  including backpressure/AppendSync parks) so mid-fold channel drains can
  never invert same-key replay order, and the finish drain is two-phase
  (channel before buffer, disarm under the buffer lock). Arming is
  unwind-safe: a panicking fold disarms with drop accounting instead of
  leaving producers spilling into a dead buffer forever. Boot paths now
  actually ABORT (exit 70) on a fatal mid-chain tear instead of falling
  back to legacy recovery, reason-DEL backpressure shares ONE bound per
  eviction/expiry sweep (was one 500ms bound per victim key), and every
  cap/shutdown drop path routes through the loss-accounting latch.

- **Cluster formation actually converges (pre-existing, both runtimes).**
  A 3-node cluster could never complete its mesh: (1) `CLUSTER MEET`'s
  random-id placeholder was never retired when the peer's handshake arrived
  under its real id, double-counting every met peer (`known_nodes` 5 in a
  3-node cluster); (2) gossip sections were only consumed for PFAIL/FAIL
  reports, so nodes MEET-ed into a common peer never learned about EACH
  OTHER; (3) nodes adopted from rumors started with `pong_recv_ms = 0`,
  which `check_failure_states` skips — a rumored node that died before
  first direct contact could NEVER be marked PFAIL. Placeholders are now
  retired on handshake (same-address, different-id), healthy rumors are
  adopted (with self/known-address guards), and adoption stamps a freshness
  baseline so the staleness clock always runs. Review round: `CLUSTER MEET`
  is idempotent by ADDRESS (repeats no longer stack one placeholder per
  call) and refuses the node's own address; a cluster-bus bind failure now
  aborts startup loudly instead of leaving a node serving clients while
  invisible to every peer; `--cluster-enabled` refuses ports > 55535 (bus
  port would wrap past 65535); a `cluster-ctl` thread panic aborts the
  process via the same hook that guards shard threads.
- **The AOF now compacts itself (#433): Redis-parity automatic rewrite.**
  `--auto-aof-rewrite-percentage` (default 100, `0` disables) and
  `--auto-aof-rewrite-min-size` (default `64mb`, size strings accepted)
  trigger a background rewrite once the AOF has grown the given percentage
  over its size after the last rewrite. Before this, the AOF grew with write
  volume rather than dataset size — observed 4.8 GB on disk for a 2.43 GB
  dataset, ~1 GB/day — until the diskfull guard paused writes. A monitor
  thread samples the on-disk size once a second and dispatches the same
  entry point as `BGREWRITEAOF`; a failed dispatch backs off 60 s instead of
  hot-retrying. Both knobs appear in `CONFIG GET`.
- **Multi-shard `BGREWRITEAOF` is un-gated.** The per-shard fan-out rewrite
  (cooperative snapshot + synchronized manifest commit) is now the default —
  the historical gate dated from a pre-C4 design that lost ~38% of keys, and
  the current path holds exact INCR recovery across a rewrite straddling a
  live write stream plus SIGKILL (crash matrix, 5/5 repeat runs).
  `--experimental-per-shard-rewrite` is deprecated (warns, no-op).

### Fixed
- **Test-hygiene sweep: pid-only temp dirs + timing-race asserts.** Eight
  spawn sites across six integration suites named their data dirs by pid
  only, so a crashed run's leftover dir was silently resurrected once the
  pid was reused — the server then reloaded stale persistence state and the
  suite failed on ghosts (the documented stale-reload trap). All eight now
  use `tempfile` (RAII where a single owner exists; unique `keep()` dirs
  where the restart flow shares one dir between two server handles).
  `parked_idle_parity` additionally gets deadline-polled asserts: CLIENT
  KILL's registry removal is polled instead of read once (the CI
  assert-too-soon race that fired 3/3 on a starved 2-vCPU runner),
  connects retry against a listening-but-backlogged server, and read
  deadlines widened 10s→30s (deadline-bound — green runs are unaffected).
  The `client_tracking_invalidation` multikey second-key push flake is
  product-side, not harness-side, and is now tracked as #448.
- **Central accept loop no longer head-of-line blocks on one wedged shard
  (#438 F3).** Every central-listener delivery (tokio plain/TLS, monoio
  plain/TLS — the monoio sends were *synchronous*, stalling the whole
  listener thread) previously blocked on the routed shard's bounded conn
  channel; one shard wedged at its 4096-conn cap froze accepts for every
  other shard. Deliveries now `try_send` and rotate to the next shard with
  room; only when every shard's channel is full does the loop fall back to
  the blocking send (server-wide saturation, where back-pressure is
  correct).
- **Connection-migration fds can no longer leak (#438 F4).**
  `MigrateConnectionPayload` carried the socket as a raw `i32` with no drop
  semantics: a migration message still queued when a shard shut down — or
  drained but never spawned — leaked the fd and stranded the client on a
  connection no task would ever serve. The payload now owns the socket as an
  `OwnedFd` end to end (producer → SPSC ring → pending-migrations queue →
  target-shard spawn), so every undelivered path closes the socket and the
  client sees a FIN. Three `unsafe from_raw_fd` blocks became safe
  ownership conversions in the process. (Resumed-parked connections remain
  deliberately `can_migrate:false`; rationale documented at the spawn site.)
- **Migrated connections now carry the real `requirepass` (#438 F5, sec
  L3).** The target-shard `ConnectionContext` was built with
  `requirepass: None`; the session's auth state was unaffected (it travels
  in `MigratedConnectionState`), but a later `AUTH` on a migrated
  connection wrongly answered "no password is set", and any future code
  deriving auth from the context would have failed open.
- **Unauthenticated connections never task-park (#438 F6, sec L2).** On an
  auth-enabled server, a client could open sockets and never authenticate
  nor speak; each one downshifted and task-parked into the ~3.3 KB watcher
  state, letting an attacker hold a maxclients-worth of silent connections
  indefinitely at near-zero cost. Pre-AUTH connections now stay un-parked
  (full handler task — visible in monitoring, still reaped by `timeout N`);
  no-auth servers are unaffected.
- **Idle-park cancel provenance + registry counter hygiene (#438
  conn-secondary).** A bare `ECANCELED` (errno 125) from any non-sweep
  source was indistinguishable from the idle sweep's cancel and re-parked
  the connection — for a dead fd that is a permanent park→wake→park spin at
  100% CPU. Park arms now require the sweep/drain to have marked the slot
  (`was_swept_cancel`, consume-once) before treating 125 as a park signal.
  Separately, a re-register over a still-present client id double-counted
  `TOTAL_CLIENTS`/shard gauges permanently (skewing `shard_overloaded`
  routing); `register` now balances against the replaced entry and the
  `kept_registration` miss-arm logs the invariant violation loudly. (The
  third secondary finding — `timeout` read once at connection setup — was
  already fixed by the D1 chore-sweep rework, which re-reads the config
  every second.)
- **Graceful shutdown now drains connections (#438 F1, conn#8).** On
  SIGTERM/SIGINT each shard previously tore its runtime down the moment its
  event loop observed the cancellation, dropping every pending connection
  task mid-poll: in-flight replies were truncated and the blocking/subscriber
  shutdown arms (`-ERR server shutting down`) never ran — measured 19/50
  BLPOP-blocked clients losing their shutdown reply on Linux io_uring
  (macOS passed only by scheduler luck). Shards now run a bounded drain
  before persistence teardown: parked stage-1/2 reads are woken through
  their cancellers (re-fired per tick to close the re-park race), token arms
  cover the tracking/subscriber/blocking parks, and the loop waits for the
  shard's live connection tasks to exit through the normal flush+FIN
  epilogue, up to a 5 s ceiling so a wedged peer cannot hold up shutdown.
  Writing the drain's red test surfaced a second, tokio-only leak in the
  same class: connections accepted by the CENTRAL listener were io-bound to
  the MAIN runtime's driver (tokio io resources bind at creation), so their
  reads/writes died with main's runtime no matter what the shard drained —
  with SO_REUSEPORT splitting accepts roughly evenly, about half of all
  tokio connections failed their final writes with "A Tokio 1.x context was
  found, but it is being shutdown". Forwarded streams are now re-registered
  with the owning shard's runtime driver at spawn (the monoio path already
  did this by forwarding std streams).
- **Parked-connection teardown can no longer close a reused fd out from
  under a kill scan (#438 F2, conn#9 / sec L1).** `kill_clients`'
  fd-liveness invariant (registry deregister strictly before fd close) held
  in handler tasks by local-before-parameter drop order, but a task-parked
  connection's watcher co-owned guard and stream as future upvars, whose
  drop order is merely capture order — and the F1 drain makes dropping that
  future a routine path. Both are now wrapped in a `ParkedSession` whose
  hand-written `Drop` deregisters before closing, with the invariant
  restated at the kill site.
- **Remotely-triggerable shard-thread crash on pipelined batch tails
  (#438 follow-on).** On any `--shards >= 2` deployment, one pipelined write
  shaped `[<remote-key cmd>…, SUBSCRIBE|BLPOP|…]` aborted the whole server:
  the early-flush arm flushed the remote commands' placeholder replies,
  cleared the response vec, and the batch-end remote-reply drain then
  indexed into it out of bounds — shard-thread panic, process abort (both
  runtimes). Early-flush commands (blocking / SUBSCRIBE / PSUBSCRIBE /
  PSYNC) now defer themselves and the unconsumed batch tail to the next
  iteration when remote-slotted work is pending, so every prior reply
  resolves and flushes first.
- **Migration no longer discards MULTI / subscriptions latched mid-batch
  (#438 D4).** The affinity sampler latched `migration_target` mid-batch,
  but migration executed at batch end with no re-check — a tail like
  `[…GETs, MULTI, SET]` migrated with the transaction queued and
  `MigratedConnectionState` carries none of that state (queued txn
  discarded, EXEC answered `-ERR EXEC without MULTI`; a tail SUBSCRIBE was
  orphaned). `ConnectionState::migration_eligible()` (not in MULTI, no
  cross-store txn, no subscriptions, no CLIENT TRACKING, not a replica) is
  now evaluated at BOTH the latch and the batch-end execution point; an
  ineligible batch end keeps the latch and migrates at the first clean one.
- **Migrated connections no longer stall on their carried remainder.** A
  resumed migrated handler received the source's unparsed bytes in its read
  buffer but awaited a fresh socket read before parsing them — a pipelined
  tail crossing a migration sat unanswered until the client happened to
  send more. The resumed handler now parses the carried remainder
  immediately.
- **The parsed batch tail after SUBSCRIBE/blocking is no longer swallowed.**
  `[SUBSCRIBE ch, PING]` in one pipelined write silently dropped the PING
  (the frame iterator discarded the remainder on break); the tail is now
  re-encoded and carried into the next iteration (subscriber mode answers
  it in order).
- **`INFO persistence` reports real AOF state (#432).** `aof_enabled` and
  `aof_rewrite_in_progress` were hardcoded `0` even with `--appendonly yes`
  (the default) and a rewrite running; they now reflect reality, and new
  `aof_base_size` / `aof_current_size` fields expose the growth the
  auto-rewrite trigger acts on — an operator can finally see the
  AOF-vs-dataset ratio the diskfull incident hid.
- **The per-shard BGREWRITEAOF crash matrix no longer reports phantom data
  loss on nearly-full hosts.** The harness lacked `--disk-free-min-pct 0`
  and parsed `MOONERR diskfull` INCR rejections as silently-dropped writes
  (the host root volume hovers at ~4% free, making it intermittent). The
  suite now disables the guard, panics on any non-numeric INCR reply, and is
  green 5/5 consecutive runs.
- **Replicas now apply streamed `SWAPDB` (#386), and the record reaches the
  wire exactly once per client call.** Two stacked defects: (1) the replica's
  apply path had no SWAPDB intercept — generic dispatch hard-errors ("must be
  issued at the connection handler level") and the error was only logged, so
  every streamed SWAPDB silently no-op'd and the replica served pre-swap data
  for both databases until a full resync; (2) a multi-shard master emitted the
  record once per REMOTE shard leg and never for the coordinator's own leg —
  against today's single merged replica stream that means N−1 swaps, a net
  no-op whenever N−1 is even (e.g. `--shards 3`). The coordinator now emits
  the replication record exactly once, after the durability gate and the
  local swap (an aborted SWAPDB can never ship to replicas); remote SPSC legs
  keep their per-shard AOF/WAL writes but stay off the replication plane; the
  tokio single-shard handler emits it too. Replicas apply it with the same
  slice-split swap as WAL replay, skipping (with a warning) indexes outside
  their own `--databases` range instead of poisoning the stream.
  (c10k E1/E3), and cross-shard reply awaits are bounded (E4).** PUBLISH
  fan-out (immediate, batched, and EXEC-queued) and SCRIPT LOAD propagation
  used a single `try_push` — a transiently-full ring lost the message with no
  log or metric: subscribers on that shard silently missed the publish, or its
  script cache diverged (NOSCRIPT for a sha the server had just returned).
  All five sites now retry with the same bounded, shutdown-aware backpressure
  as the command dispatch path, and a final give-up is loud
  (`moon_xshard_fanout_drop_total`). Reply awaits on the slotted dispatch and
  publish paths — previously unbounded, so one wedged shard could hang a
  client task forever — now share the coordinator's 30 s bound: publish counts
  degrade (under-report, counted by `moon_xshard_reply_timeout_total`), while
  a slotted-dispatch timeout errors the batch and closes the connection,
  because the per-connection reply slot cannot be safely reused after an
  abandoned await. Rehydrated connection handlers (park wake / migration)
  also start with 512 B I/O buffers instead of 3×8 KiB, shrinking the memory
  spike of fleet-synchronized wakes; buffers grow back on first real traffic
  (c10k D3).

### Added
- **`INFO memory` can now explain a `used_memory`-vs-RSS gap.** Build with
  `--features jemalloc-stats` and `INFO memory` reports Redis's `allocator_*`
  fields: `allocator_allocated`, `allocator_active`, `allocator_resident`,
  `allocator_retained`, `allocator_frag_bytes`, `allocator_frag_ratio`, and
  `allocator_unreturned_bytes`. `active - allocated` is fragmentation,
  `resident - active` is dirty pages jemalloc holds but has not returned, and
  anything the OS charges beyond `resident` belongs to something other than the
  allocator. Motivated by a real instance reporting `used_memory` 2.43 GB while
  the OS charged it 7.3 GB (7.2 GB of that swapped) with no way to tell which.
  OFF by default — jemalloc's stats add bookkeeping to every allocation. Fields
  are absent rather than zero-filled when not built in, because a zero would
  read as "no fragmentation", which is worse than "not measured".
- **`INFO clients` reports `parked_clients`.** How many connections are
  currently held only by a task-exit park watcher — real operational
  visibility into how much of the fleet is actually parked, and a direct
  signal for tests instead of inferring parking from process RSS.

### Known issues
- **An idle moon may not return freed memory to the OS on Apple platforms, and
  there is no in-process fix.** jemalloc's `background_thread` is compiled out
  when `abi == macho` (`JEMALLOC_BACKGROUND_THREAD` is only defined otherwise),
  so the `background_thread:true` moon bakes into its malloc conf is a silent
  no-op there and decay runs only as a side effect of allocator activity. The
  same 384 MiB churn-and-free reclaimed to a 3.7 MiB physical footprint on one
  Apple Silicon machine and retained all 386 MiB indefinitely on a GitHub macOS
  runner, so the behaviour varies by machine. A `--memory-decay-interval-ms`
  timer calling `mallctl("arena.4096.decay")` — the same call jemalloc's own
  background thread makes — was implemented and then **removed after it failed
  to reclaim anything on the runner that reproduces the retention**, across 30
  seconds of driving it and three independent retries, while the ctl itself
  returned success. Production targets Linux, where the background thread is
  compiled in and the retention has not been observed; on macOS, build with
  `--features jemalloc-stats` and watch `allocator_unreturned_bytes` to tell
  whether a given host is affected.

### Security
- **One byte made a connection invisible to the idle sweep (c10k D2).** The
  stage-2 park predicate required an EMPTY `read_buf`, so a single `*` — the
  first character of every RESP array — kept it non-empty permanently, made the
  connection unparkable, and dropped it into the handler's UNREGISTERED plain
  read. Nothing on that path carries a sweep handle, so the connection held its
  full stage-2 working set for as long as the attacker left the socket open,
  with no authentication and no further traffic. One byte and one socket per
  connection; at 1M connections roughly 10-15 GB that no amount of idle time
  reclaims. Unparsed input no longer blocks a park: the remainder is carried in
  `read_buf_remainder` and re-parsed on resume, exactly as a migrating
  connection already did. Deliberately NOT capped — `read_buf` is already
  bounded by `client_query_buffer_limit`, and a second threshold would only
  move the attack past it (send 513 bytes instead of 1). A pending `write_buf`
  still blocks the park, because a reply the client is owed is carried nowhere
  and would be silently dropped.

- **Reply writes were unbounded — a client that stops reading held the whole
  reply forever (c10k C1).** `write_all` on a socket whose receive window is
  closed never returns. A client could pipeline a large response and then
  simply stop reading, parking the handler inside that write while it held the
  ENTIRE serialized reply — the monoio handler coalesces a whole batch into one
  `Bytes` before its single write syscall, so a deep pipeline is hundreds of MB
  — plus its `maxclients` slot, for as long as the attacker cared to wait. N
  such clients is an OOM that costs the attacker nothing: no reads, no CPU,
  just a TCP window it refuses to open. New `--client-write-timeout-ms`
  (default `60000`, `0` = the previous wait-forever behaviour) bounds every
  reply-carrying write in all three handlers — monoio top-level, sharded, and
  the tokio single-shard `Framed` path — closing the connection and dropping
  the reply when a write makes no progress for that long. 60s is far beyond any
  healthy client's stall and replication does not use this path (PSYNC hijacks
  the connection before it), but operators streaming very large replies over
  very slow links should raise it: the budget covers the whole write call, not
  each byte. Verified at 1 and 4 shards on both runtimes, with the `0` case as
  a differential control proving the mechanism rather than the harness
  (`tests/write_timeout.rs`). **Unverified on Windows**: the tests need the
  server's write to actually block, and two attempts to force that under
  Winsock failed (a 25 MB reply was absorbed by send/receive autotuning, and
  clamping the victim's `SO_RCVBUF` to 8 KiB did not change it), so they are
  skipped there rather than weakened until they pass. The code path is shared
  and compiles on Windows, but nothing proves the timeout fires; Windows is not
  a target platform (Linux and macOS are).
- **`CLIENT LIST` reported `obl=0 oll=0 omem=0` unconditionally (c10k C1).**
  The held-output counters were hardcoded, so an operator watching a
  slow-client output-buffer OOM in progress saw every client reporting zero
  bytes held — the attack above left no trace anywhere. `obl`/`omem` now report
  the reply bytes a connection has in an in-flight write, and `tot-net-out`
  counts reply bytes that actually reached the peer. `oll` stays 0: moon has
  one contiguous output buffer, not Redis's static-buffer + reply-list pair, so
  there is no list whose length it could report. Wired in the two handlers that
  own a client-registry entry (monoio top-level and sharded); the legacy tokio
  `handler_single` path bounds its writes but does not register, so it has
  nothing to report through.
- **No size ceiling on a reply (c10k C1).** Adds
  `--client-output-buffer-limit-normal`, Redis's
  `client-output-buffer-limit normal <hard>`, and ships it at **256 MiB rather
  than Redis's unlimited default** — that default is exactly why an unread
  socket can OOM Redis too. A reply exceeding the cap is refused and the
  connection closed instead of buffering it. Consequence, intended and worth
  knowing: the cap covers the whole serialized reply, so a single value larger
  than the cap is undeliverable, not merely a long pipeline. No pub/sub-class
  knob ships: moon's only subscriber write is already hard-capped at 64 KiB by
  `MAX_COALESCE_BYTES`, so such a knob could never fire, and the real pub/sub
  backpressure is the 4096-slot bounded channel (`CONN_CHANNEL_CAPACITY`) —
  which bounds message COUNT, not bytes, and is a genuine follow-up.
- **The write watchdog no longer arms on the hot path.** A timer per batch
  flush would land once per command at pipeline depth 1, the path this project
  spent a milestone winning against Redis. A reply that fits in the socket
  buffer cannot block, so only writes ≥ 256 KiB arm the watchdog now
  (`util::arm_write_timeout`). Residual, stated rather than hidden: a small
  write to a genuinely wedged socket is still unbounded — it holds at most
  256 KiB instead of hundreds of MB, but keeps its `maxclients` slot. It is now
  visible (`omem` > 0) and killable (`CLIENT KILL` shuts the fd down), which it
  was not before.
- **Privileged commands ran BEFORE the ACL permission check (c10k B1).**
  Both connection handlers intercepted `EVAL`/`EVALSHA`/`SCRIPT`, `ACL`, and
  `CLUSTER` above the ACL gate, and each intercept `continue`s on a match, so
  the gate below it never ran — even though the comment attached to that gate
  claimed it "must run before any command-specific handlers ... so that
  low-privilege users cannot reach admin commands". Any authenticated account
  could escalate to full admin: a user holding nothing but `~app:* +get` was
  correctly refused a plain `SET` yet could run
  `ACL SETUSER evil on nopass ~* +@all` (persisted), `CONFIG SET` (persisted),
  arbitrary Lua via `EVAL`, `SCRIPT LOAD`, `REPLICAOF` and `BGSAVE`. The gate
  now sits above every privileged intercept in both handlers, with `AUTH` and
  `HELLO` lifted above it as Redis's `NO_AUTH` commands. Verified end-to-end at
  1 and 4 shards (`tests/acl_privileged_intercepts.rs`).
- **Unknown ACL users failed OPEN (c10k B2).** All three permission checks
  started with `users.get(username)?`, and `None` is their "allowed" answer, so
  a username missing from the table was granted everything. Nothing closes a
  live session when its account disappears, so `ACL DELUSER alice` / `ACL LOAD`
  silently PROMOTED every connection alice still had open to `~* +@all`.
  Unknown users are now denied; `default` is guaranteed present after any load
  (`ensure_default_user`) so a server whose ACL file omits it is not bricked.
- **`CLIENT KILL USER` was inert and `CLIENT LIST` reported `user=default` for
  everyone (c10k B3).** The client registry captured the username once, at
  accept time; AUTH/HELLO updated only the connection-local copy. The primary
  incident-response lever for a compromised credential matched nothing. AUTH
  and HELLO now publish the adopted identity to the registry, and — matching
  Redis — `ACL DELUSER` disconnects the sessions the deleted user still holds.
- **The experimental io_uring bridge served commands with no auth (c10k B4).**
  `MOON_URING=1` (tokio, Linux) binds a second `SO_REUSEPORT` listener on the
  server's own port whose accept path has no auth gate, no ACL check and no
  client registry — so on a server with `requirepass`/`aclfile` configured the
  kernel load-balanced roughly half of all new connections onto a listener that
  skipped authentication entirely. The documented limitation named maxclients
  and `CLIENT LIST`/`KILL`, not this. The bridge now refuses to arm (loudly)
  when authentication is configured and the shard stays on the tokio path.

### Fixed
- **`FLUSHALL`/`FLUSHDB` inside `MULTI`/`EXEC` cleared one shard and reported
  success (c10k E2).** The transactional executor runs the queued body against
  the LOCAL slice with no fan-out, so a queued flush emptied a single shard of
  N while `EXEC` still answered `+OK`. Measured at `--shards 4`: 45 of 64 keys
  survived a transaction that reported the database emptied — a silent wrong
  answer to a destructive command, typically noticed much later via a non-zero
  `DBSIZE`. The live (non-`MULTI`) path has broadcast since D-2; the
  transactional path now does the same. The executor records each flush and the
  ORIGINATOR fans it out — it cannot fan out itself, being synchronous while
  the broadcast awaits, and for a routed transaction it runs on the owner
  shard, where broadcasting from inside that shard's own message loop risks a
  shard-to-shard wait cycle. A failed leg replaces that entry of the `EXEC`
  result array with an explicit partial-flush error, so a `+OK` for a flush
  inside a transaction can be trusted exactly as on the live path. Both
  handlers and both transaction shapes are covered, and a queued `SELECT`
  before the flush is honoured. Cross-shard atomicity is unchanged and
  unchangeable: a concurrent reader can still see shard A flushed before shard
  B — `MULTI` bounds the report, not the visibility, in a shared-nothing
  engine.

- **TLS park veto samples `wants_write()` after processing, not before (c10k
  B5).** `Stream::task_park_safe` read `wants_write()` before
  `process_new_packets()`, which can queue outbound bytes and still return
  `Ok` — reachable in rustls 0.23 via the TLS 1.2 renegotiation rebuff
  (`NoRenegotiation` warning alert) — so the connection could park owing the
  peer a reply. (The TLS 1.3 KeyUpdate variant is *not* reachable: rustls
  defers that reply until the next outbound record; `tests/tls_park_keyupdate.rs`
  pins the behaviour so a future rustls change is caught.)
- **A client that disconnects while blocked is now reaped (c10k hardening
  A1).** The infinite-wait `select!` behind `BLPOP`/`BRPOP`/`BLMOVE`/
  `BZPOPMIN`/`BZPOPMAX`/`BLMPOP`/`BZMPOP`/`BRPOPLPUSH` had exactly two arms,
  `reply_rx` and `shutdown` — nothing watched the socket. `BLPOP key 0`
  followed by a disconnect therefore leaked the handler task, the
  `WaitEntry`, the client-registry entry and the maxclients slot *forever*:
  infinite waiters carry `deadline: None` so the deadline sweep skips them,
  and `timeout` exempts blocked clients by design (Redis parity). A few
  thousand throwaway connections wedged the server until restart, using one
  unauthenticated command. The wait now also watches the peer, and a
  vanished client tears down every registration it held (local and remote)
  before closing. `CLIENT KILL` of a blocked client works through the same
  path, since it closes the fd with `shutdown(2)`. Bytes a client legally
  pipelines behind its blocking command are carried into the parse stream
  rather than consumed and dropped — they used to wait in the kernel, so
  the handler now skips exactly one read to drain them. **Known gap:** TLS
  connections keep the old behaviour; the vendored monoio-rustls read loops
  internally until rustls yields plaintext, so a post-readiness read can
  park inside a partial record, which the cancel-free design must never do.
- **A timed-out cross-shard `BLPOP` no longer eats the next push to that
  key (c10k hardening A2/A3).** Two defects compounded into silent data
  loss at `--shards > 1`. First, the tokio single-key cleanup condition
  `is_remote && !matches!(result, Frame::Null) || matches!(result, Frame::Error(_))`
  parses as `(is_remote && !Null) || Error`, so on a **timeout** — where
  the result *is* `Frame::Null` — the `BlockCancel` was never sent and the
  owning shard kept the `WaitEntry` forever (remote entries carry
  `deadline: None`, so the deadline sweep never reaps them). Second, when
  a later push found that ghost waiter, every wake path popped the element
  and then did `let _ = reply_tx.send(...)`: the receiver was long gone, so
  the value was neither delivered, nor requeued, nor offered to the next
  FIFO waiter — it simply vanished. Wakes now skip waiters whose client has
  disconnected *before* touching the datastore, and restore anything popped
  if the receiver drops in the remaining race window (including BLMOVE's
  destination push and BLMPOP's multi-element pops, in order). The same
  boolean bug also sent a local waiter's shutdown cleanup through
  `target_index(shard, shard)`, which underflows to a panic on shard 0.
- **Blocking registrations are no longer silently dropped or retried
  forever (c10k hardening A4/A5/A6).** Cross-shard `BlockRegister` /
  `BlockCancel` messages were pushed with a bare `try_push` (tokio) or an
  uncapped `loop { try_push; sleep(10µs) }` with no shutdown arm (monoio).
  A dropped `BlockRegister` drops the reply sender with it, so `BLPOP key 0`
  returned nil at t=0 — a protocol violation; a dropped `BlockCancel` leaked
  the owner-side waiter permanently; and the uncapped retry turned a
  saturated owner shard into a zombie connection that also blocked graceful
  shutdown. All four call sites now use the plane-wide bounded,
  shutdown-aware push, notify the owning shard on success, and fail the
  command loudly (`MOONERR blocking registration failed`) rather than
  blocking on a key nobody is watching.
- **`timeout N` no longer silently disables the c1M connection park.** The
  idle timeout was enforced by a per-connection `select!` arm racing the read
  against `sleep(timeout)`, and that arm sat FIRST in the read loop's if/else
  chain — so setting `timeout` made the stage-1 buffer downshift, the stage-2
  park and task-exit parking all structurally unreachable. Every connection
  reverted from the parked footprint to its full working set, silently, in
  exactly the deployments that use the only slowloris knob moon ships. The
  same arm also read its config once at connection setup (so `CONFIG SET
  timeout` never reached a live connection) and had no exemption for
  replication links. Enforcement now runs from each shard's existing 1 s
  chore (`client_registry::kill_idle_clients`), which needs no per-connection
  timer and additionally reaches connections whose handler task has exited —
  matching how Redis enforces `timeout` from `serverCron`. The registry is
  striped by client id rather than by shard, so each shard sweeps a disjoint
  subset of stripes: total cost stays at one full pass per second regardless
  of shard count, instead of `O(num_shards × total_clients)`. A parked
  connection closed by the sweep (or by `CLIENT KILL`) is now torn down by
  its watcher directly rather than rehydrating a full handler task purely to
  observe the kill and exit. Idle clients are closed within one sweep
  interval of the deadline rather than exactly at it. Blocked clients,
  subscribers and replication links are exempt, as in Redis.
- **`CLIENT LIST`'s blocked flag was dead code.** `ClientFlags::blocked` was
  hardcoded `false` at every call site, so a client parked in `BLPOP key 0`
  never showed `flags=b` — and would have been treated as idle by the new
  timeout sweep. The flag is now set for the duration of a blocking command
  on both runtimes.
- **Warm-tier transitions leaked their staging directory on every failure
  (#435).** `transition_to_warm` has ~10 fallible steps between creating
  `.segment-{id}.staging` and renaming it to its final name, and every early
  return in that stretch abandoned the whole directory. Found on a live
  instance: a 27 GB data directory against 2.53 GB of `used_memory`, 20 GB of
  which was 14,499 orphaned staging directories versus 174 MB in the 94 real
  segments — 99% of the vector store was abandoned scratch, written in a single
  three-minute window and already survived a restart. The dot prefix kept them
  out of `ls` and out of every `vectors/*` glob, so nothing surfaced them. A
  `Drop` guard now covers every early return, and `sweep_orphan_staging` runs
  at recovery for orphans no in-process guard can catch (a `kill -9` between
  the manifest commit and the rename, or anything left by an older build). The
  sweep is safe by construction: staging paths are produced in exactly one
  place and read by nothing — every reader opens the final `segment-{id}` name.
  `transition_to_warm` also removes a stale staging directory for the same id
  before creating it, so a retry cannot inherit a previous attempt's partial
  files.

## [0.8.4] — 2026-07-29

### Added
- **TLS connections task-park too (c1M P1-TLS).** Idle TLS connections now
  exit their handler task after `--conn-park-secs`, like plain TCP: the
  parked watcher awaits readability on the raw fd via a vendored
  `io_ref()` passthrough, and a vendored `task_park_safe()` check vetoes
  any park while the TLS stack holds state the fd cannot signal (wrapper
  buffer bytes, pending EOF/error status, decrypted plaintext, a received
  close_notify, or unsent output). Parked footprint keeps the rustls
  session; the task future and working set are freed.

### Fixed
- **Read errors on a parked-capable connection terminate instead of
  re-parking (park→wake→park spin).** The idle-park arms treated every
  read error as the sweep's cancel; with task-exit parking that spun a
  dead connection through park→wake→park at 100 % CPU forever (a TLS
  client vanishing with a raw FIN — no close_notify — surfaces as a read
  ERROR, and the dead fd stays readable; found live in E11 with 3 000
  CLOSE_WAIT connections pinning a shard; an RST'd plain-TCP conn hits
  the same loop). The arms now match monoio's exact ECANCELED (raw os
  error 125, both drivers): only the sweep cancel downshifts/parks; real
  errors tear down promptly. Regression tests: FIN-while-parked (TLS) and
  RST-while-parked (plain).
- **Resumed parked connections keep their registry identity (c1M P1
  follow-up).** Waking from task-exit parking previously deregistered and
  re-registered the client across a task-scheduling boundary: a racing
  `CLIENT LIST` could briefly miss the connection, `CLIENT KILL ID` could
  return 0, and — worse — the fresh registration silently reset the
  connection's `CLIENT SETNAME` and `age` in `CLIENT LIST`. The wake now
  hands the held registration through to the resumed handler
  (registration handoff), so the entry — name, connected-at, kill state,
  client counters — persists unbroken across any number of park/wake
  cycles.

### Changed
- **Migrated connections task-park too (c1M P1 follow-up).** The
  migrated-connection spawn path (Linux connection migration) now routes
  `ParkIdle` like the primary accept path, so a connection that migrated
  shards and then went idle parks out of the task model instead of holding
  its full handler task forever.

## [0.8.3] — 2026-07-29

### Added
- **Task-exit parking for idle connections (c1M P1, `--conn-park-secs`,
  default 60 s).** A plain-TCP monoio connection that stays idle past the
  W11 downshift now has its handler task exit entirely: only a tiny
  readiness watcher (boxed future holding the stream, ~100 B of session
  state, and the client-registry guard) remains, reclaiming the ~6 KB task
  state machine plus the remaining per-task buffers. The watcher wakes on
  read-readiness (`readable(false)`, race-free on io_uring and
  epoll/kqueue) or server shutdown and rehydrates a fresh handler through
  the migration-restore path — wire-invisible across repeated park/wake
  cycles. Parked connections stay in CLIENT LIST, keep their maxclients
  slot, and CLIENT KILL still works (its `shutdown(2)` wakes the watcher;
  the resumed handler sees EOF). Exclusions: subscriber/tracking/timeout
  connections (structurally never reach the park arm), MULTI/EXEC or
  cross-store-txn sessions, partial frames, TLS (keeps W11+P4b buffer
  downshift), and the tokio runtime. `--conn-park-secs 0` disables.

### Fixed
- **c10k connection-plane wave (PR #421)** — from the 2026-07-29 empirical
  review (`tmp/C10K-REVIEW.md`: 10k/25k live-connection ramps, idle-CPU
  measurement, symbolized flames, pipeline-ratchet A/B on the Linux VM):
  - **Pipeline memory ratchet fixed (W1).** One 1024-deep pipeline
    permanently ratcheted a connection from 56.5 KB to ~217 KB RSS until
    disconnect (measured: 5000 such conns pinned 1.09 GB). Batch scratch
    vecs now shrink after oversized batches, I/O buffer shrink floor
    lowered 64 KiB → 16 KiB, tokio bump arena capped, subscriber-mode
    per-message 8 KiB alloc+zero removed.
  - **maxclients rejection is loud (W3).** Over-cap connections now receive
    `-ERR max number of clients reached` (Redis parity) instead of a silent
    EOF on the monoio + TLS paths; the gate runs before per-connection
    context construction; startup now checks `RLIMIT_NOFILE` against
    maxclients, raises the soft limit when possible, and warns with the
    real client ceiling otherwise.
  - **Blocked-client sweep is deadline-heap driven (W6)** — O(due) instead
    of walking every blocked waiter at 100 Hz (~2M entry visits/s/shard at
    10k BLPOP clients); block-forever waiters are never visited.
  - **SPSC drain starvation fixed (W7).** The cross-shard drain rotates its
    start consumer, so the 256-message budget no longer lets a hot
    low-index peer starve high-index peers indefinitely.
  - **IP-affinity funnel load-gated (W8).** A shard above 2× the mean
    connection count no longer receives affinity-funneled connections
    (falls back to round-robin) — previously one migrated/subscribed client
    pinned all same-IP connections to one shard with no load feedback.

### Added
- **`--uring-entries N` (c10k P4)** — per-shard io_uring submission-queue
  size (monoio default 1024; also the legacy driver's event-batch
  capacity). Values below monoio's 256 floor clamp up with a warning;
  monoio runtime only.

### Changed
- **Idle connections downshift to a ~0.5 KB working set (c10k W11).**
  A connection parked in `read()` ≥1s has its read cancelled by the shard's
  1s chore (loss-free cancel-and-await on both io_uring and epoll/kqueue),
  sheds its 8 KiB rent buffer and empty scratch buffers, and re-parks on a
  512 B probe buffer. Idle RSS/conn 43.5 → 19.9 KB (10k-conn VM A/B);
  GCE-pinned p=1/p=16 A/B perf-neutral. TLS conns and the tokio runtime
  keep the previous behavior.
- **Idle TLS connections shed their 32 KB wrapper buffers (c10k P4b).**
  `monoio-rustls` + `monoio-io-wrapper` are now vendored (moon-patch style,
  like `vendor/monoio`): the wrapper's two eagerly-allocated, never-freed
  16 KiB per-stream buffers are lazy + releasable, and the TLS stream
  implements a loss-free cancelable read, so W11's idle downshift now
  covers TLS connections too — a parked TLS conn drops moon's working set
  AND both wrapper buffers (they reallocate lazily on the next I/O).
  Cancel errors are deliberately not stashed for replay in the wrapper
  (a stashed ECANCELED would resurface on the next read and tear down a
  healthy connection). Wire parity across downshift/wake cycles is
  regression-tested on one continuous TLS session
  (`tests/tls_idle_downshift_parity.rs`).
- **Cold command futures boxed out of the connection state machine
  (c10k P3).** TXN.ABORT rollback, leaked-txn disconnect teardown, and
  the FT.* body now `Box::pin` behind their name-check fast paths; the
  per-connection monoio task allocation drops 9.3 KB → 5.9 KB plain TCP
  (13.2 → 9.9 KB TLS) with zero allocation on the KV dispatch path.
- **Client registry striped 16 ways (W5).** Accepts/closes no longer
  serialize on one global lock; `CLIENT LIST` locks one stripe at a time
  (output is no longer insertion-ordered — Redis makes no ordering
  guarantee); `CLIENT KILL ID` is an O(1) lookup instead of a full scan.
- **`active_cross_txn` boxed (W2)** — ~2.2 KB of inline transaction
  SmallVecs no longer ride in every connection's task future;
  `ConnectionState` is guarded ≤768 B by a regression test.
- **Dead plumbing deleted (W4):** the zero-registrant `pending_wakers`
  waker relay (per-conn Rc clone + twice-per-iteration sweep) and the
  never-adopted duplicate `server/conn_state.rs` module (209 lines).
- The experimental `MOON_URING=1` tokio bridge now logs a loud startup
  warning that its connections bypass maxclients and CLIENT LIST/KILL
  (T3; full accounting integration tracked in
  `.planning/rfcs/c1m-connection-plane.md`).

## [0.8.2] — 2026-07-20

### Fixed
- **Millisecond-TTL keys no longer expire up to 999 ms early (W3).**
  `CompactEntry` stored expiry as *seconds* (`ms / 1000` floor) even though
  the field was a full `u64`: a `PEXPIRE key 1500` died at the 1000 ms
  boundary, and `PTTL` reported the truncated value. Expiry is now stored
  as absolute Unix **milliseconds** end-to-end (same 32-byte entry layout),
  `PTTL` reads back the exact deadline, and RDB/snapshot round-trips
  preserve millisecond fidelity (the round-trip test's old 5-second
  tolerance is now an exact equality).

### Changed
- **`storage::db` is now a directory module.** `db.rs` had grown to 3113
  lines against the repo's 1500-line ceiling; the W5 accessor unification
  made a clean split possible. The file is now `db/mod.rs` (types, cost
  constants, ctor/clock, memory accounting, tests) plus `db/hash_ttl.rs`
  (HEXPIRE-family per-field TTL primitives), `db/kv_ops.rs` (core keyspace
  ops: get/set/remove, lazy expiry, cold-tier promotion, scan) and
  `db/accessors.rs` (ValueKind/OwnedKind generics, per-type delegators,
  read-only refs, blocking-hook helpers, streams). Pure code motion — every
  method body moved verbatim, public paths unchanged; four private helpers
  became `pub(super)` for cross-file visibility within the module.

- **`aof_manifest::ShardManifest` renamed `AofShardManifest` (W6).** Two
  unrelated structs shared the name `ShardManifest` — the page manifest's
  (`persistence::manifest`, the durable spill/offload root) and the AOF
  manifest's per-shard entry — making cross-plane persistence code search
  and discussion ambiguous. The AOF one (private to its module tree, no
  external importers) now carries the plane in its name. `storage::tier`'s
  `ResidencyTier` gained a doc note disambiguating it from the on-disk
  `manifest::StorageTier` (they intersect in concept but not in role and
  must not be merged); the unified-manifest RFC gained the task-#48
  poison-record policy tie-in for its future section decoders.

- **One typed-accessor skeleton in `Database` (W5).** The ~20-method
  accessor matrix (`get_X` / `get_or_create_X` / `get_X_ref_if_alive` per
  container type) copy-pasted the same skeleton — expiry check → cold
  promote/read-through → compact-encoding upgrade → variant projection —
  once per type. The skeleton now exists once per access shape
  (`get_ref_if_alive::<K>` / `get_or_create::<K>` / `get_promoted::<K>`);
  everything genuinely per-type lives in the new `storage::db_kind` module
  as `ValueKind`/`OwnedKind` marker impls (static dispatch — generated code
  identical to the hand-written originals). The public per-type methods
  remain as thin delegators, so command-layer call sites are unchanged.
  The four caller-less `get_X_if_alive` variants (compact encodings
  returned `None`; superseded by the `_ref_if_alive` family) are deleted.
  Pure refactor — behavior pinned by two new tests (hot-WRONGTYPE through
  the ref accessors; `HashWithTtl` → `HashRef::WithTtl` wiring incl.
  caller-`now_ms` propagation) plus the existing cold-visibility pins.

- **One eviction entry point (W4).** The 13-name `try_evict_if_needed*`
  family (every combination of spill-sink / explicit-total / elastic-budget /
  plain-drop-reporting encoded as a function-name suffix) is replaced by a
  single `evict_to_budget(db, config, EvictionRun)` — the victim destination
  (`Plain` / `SyncSpill` / `AsyncSpill`) and the optional knobs are data on
  the `EvictionRun` options struct. The two former core loops (sync + async,
  five near-identical reclaim-loop copies in total) are now one loop with a
  per-iteration sink dispatch; the `--appendonly` durability-ordering
  branches are unchanged and documented on `EvictionSink::AsyncSpill`.
  Pure refactor — no behavior change; behavior pinned by three new
  entry-point tests plus the full existing eviction suite.

- **Fossil `base_ts` plumbing removed (W3).** `is_expired_at` /
  `expires_at_ms` / `set_expires_at_ms` / `new_string_with_expiry` carried a
  `base_ts: u32` parameter that has been ignored since expiry became
  absolute; the parameter, ~60 dead call-site arguments, dead
  `let base_ts = …` bindings, the never-read
  `SnapshotState.base_timestamps` field, the dead `u32` element in the
  AOF-rewrite/BGSAVE snapshot tuple (`AofFoldSnapshot.dbs` and the
  `save_from_snapshot`/`save_snapshot_to_bytes` signatures), and the
  caller-less `merge_shard_snapshots` are gone.

### Performance
- **Sync eviction spill now batches like the async path (W2).** The
  tick-driven sync spill paths (`run_eviction`, the memory-pressure cascade's
  sync fallback) previously wrote **one single-entry `.mpf` file and did one
  shard-thread-blocking durable manifest commit per victim key**; they now
  route through the same `flush_buffer` batch writer the background
  `SpillThread` and the no-AOF durable path use — shared multi-entry files
  (up to 256 keys), one manifest commit per file. Evicting N keys under
  memory pressure costs ~N/256 manifest fsync round-trips instead of N.

### Changed
- **One spill pipeline (W2).** Victim policy dispatch (`select_victim`) and
  victim serialization (`build_spill_payload`) now exist once instead of
  being copy-pasted at all three spill entry points; the sync
  `SpillContext` path delegates to the one durable batch spiller
  (`evict_batch_durable`, formerly `evict_batch_durable_no_aof` — no longer
  no-AOF-only). The sync/async divergence class behind the task #34
  plain-drop misclassification, the task #45 `is_string` gate, and the #139
  test blindness is structurally gone. `kv_spill::spill_to_datafile` is no
  longer a production eviction path (kept as the single-entry primitive test
  fixtures use).

### Fixed
- **Unserializable eviction victims are retained, fail-closed.** All three
  spill sites previously did `serialize_collection(..).unwrap_or_default()`:
  a victim whose value failed to serialize (in-memory corruption) was
  "spilled" as an EMPTY value body and evicted — silent durable data loss on
  reload. Such victims are now retained hot and skipped, loudly logged.
- **Batch-durable evictions now count in the eviction metric.** The durable
  batch path (`--appendonly no` cascade) removed keys without calling
  `record_eviction()`; single-victim paths did. Both now record.
- **One value codec for RDB snapshots and KV disk-offload spill (W1).** The
  ~150-line value-body encoder/decoder that existed twice — `rdb::write_entry`'s
  value section and `kv_serde::serialize_collection` — kept bit-compatible only
  by discipline, is now a single spec-documented module
  (`src/storage/value_codec.rs`) that both call sites delegate to; the
  `RedisValueRef → ValueType` mapping likewise exists once (the former inline
  copies in `eviction.rs` and `kv_spill.rs` delegate). Wire format is
  byte-identical (pinned by hand-built golden byte-spec tests + legacy
  pre-trailer decode tests); existing RDB files and spill pages load unchanged.

### Fixed
- **Spill decode allocation-DoS hardening.** The spill value decoder missed the
  RDB count-validation fix and fed corrupt length fields straight into
  `Vec::with_capacity`; the unified codec validates every count against
  remaining input before allocating, on both planes.
- **Corrupt sorted-set listpack scores are fail-closed.** Both former encoder
  copies silently wrote `0.0` for a listpack zset score that failed to parse;
  the codec now refuses to encode (`ValueCodecError::CorruptScore`) — an RDB
  save fails loudly and a spill aborts instead of persisting a corrupted score.

### Documentation
- **New "The Moon Journey" page (`docs/journey.md`) — the development story
  toward a production-efficient database, grounded in real evidence.** Traces
  the milestone arc (v0.4 → v0.8.1) and the measured efficiency achievements —
  throughput (GET 5.11M/s 1.72×, SET 3.50M/s 1.92×, ARM64 2.20×; 1.71× at p=64
  on 23× less CPU), the single-connection p=1 conquest, memory (27% less at
  1KB values), latency (p50 8–10× lower), the durability write-path campaign
  (`always` p16 0.12×→0.91×, `everysec` p16 1.32×), AI-native vector/graph
  (12.7K QPS; beats Qdrant 1.6–2.3×, FalkorDB 23×), and the storage-kernel
  production hardening (46-cell kill-9 matrix, 10× RAM datasets, 24 h
  replication soak with zero acked-write loss). Includes a "what we measured
  and threw away" section (prefetch, lock-free `Notify`, THP-by-default,
  `ef/√G` beam split — all rejected on the evidence). Linked from the README
  (refreshed roadmap table + journey callout) and the mkdocs nav.
- **Visual performance timeline** — a step-line chart
  (`docs/assets/journey-efficiency.png`) tracing the three efficiency curves
  that each crossed Redis parity (p=1 latency x86 1.06→1.66×, ARM 0.95→1.21×,
  `fsync=always` durable throughput 0.12→0.91×), plus a mermaid milestone
  timeline (v0.4.x → v0.8.1). Embedded in the journey page and the README.

## [0.8.1] — 2026-07-19

### Added
- **`conf/moon-standalone.conf` + a broadened `--profile standalone`: the
  best single-shard tuning as one flag or one file, now safe on any host.**
  `--profile standalone` still fills `--shards 1`, `--io-busy-poll-us 40`, and
  `--io-driver epoll`, and now additionally drops the jemalloc arena cap to `2`
  on `--features jemalloc` builds — a single-shard instance has one hot
  allocator thread, so the baked-in 8 arenas are oversized and 2 lowers the RSS
  baseline with no contention cost. The arena cap is resolved in the pre-clap
  allocator re-spawn scan (the only path that reaches jemalloc before init), so
  it is CLI-only — `--profile standalone` on the command line folds it in; a
  conf-file `profile standalone` still sets the other three. The new annotated
  [`conf/moon-standalone.conf`](https://github.com/pilotspace/moon/blob/main/conf/moon-standalone.conf) ships the same tuning
  (durability on) as an editable config file. Paired with the O3 contention
  governor below, the preset no longer requires pinned cores — the tuning and
  configuration guides drop the pinned-cores-only caveat throughout and the
  README/architecture docs are updated to match.

### Performance
- **`--io-busy-poll-us` is now deploy-safe: a per-shard contention governor
  gates the spin on shared cores (O3).** The p=1 busy-poll win (GCE pinned:
  ARM 0.95→1.21×, x86 1.06→1.66× vs Redis) previously inverted into a
  regression whenever the shard's core was shared (OrbStack/laptop), making
  the flag pinned-cores-only operator judgment. Each shard thread's 1s
  chore now samples its own `nonvoluntary_ctxt_switches` from
  `/proc/thread-self/status` — the kernel's direct "someone else needed
  this core" signal — and flips a per-thread gate in the vendored monoio
  legacy driver: one window over 25 preempts/s stops the spin immediately;
  five consecutive quiet windows re-enable it (asymmetric hysteresis, no
  flapping). Startup state is ungated, so pinned-core deployments see the
  win from the first request and a shared-core host pays at most ~one
  window of spin. Idle cost is unchanged (the existing 10ms idle-disengage
  already covers quiet threads). `MOON_SPIN_ADAPTIVE=0` restores
  unconditional spinning (same-binary A/B knob);
  `MOON_SPIN_MAX_PREEMPTS_PER_SEC` tunes the threshold. Non-Linux has no
  preemption signal — the governor is inert there (pre-O3 behavior). This
  also makes `--profile standalone` (which presets busy-poll 40) safe on
  non-dedicated hosts.

### Documentation
- **SPSC-wake `Notify` stays on flume — the lock-free replacement was
  rejected by measurement (O2).** A fully-validated AtomicBool token +
  `AtomicWaker` implementation (loom lost-wake model, 209/209 consistency
  at 1/4/12 shards, monoio busy-poll smoke; branch
  `perf/o2-notify-atomic-waker`) measured NEUTRAL on GCE t2a shards=4
  (SET p1 +0.08%, GET p1 +1.6% across 5 leg-order-alternating rounds):
  the cachegrind-attributed flume misses are dwarfed by the eventfd+epoll
  wake delivery, and busy-poll deployments elide flume via the skip-wake
  gate anyway. A NOTE at the `Notify` definition records the verdict and
  the re-attempt bar.
- **`--memory-thp` is permanently opt-in — the RSS-drift soak disqualified
  a default flip.** 45min mixed-size-churn + idle-decay soak (moon-dev VM,
  THP leg vs control, AnonHugePages-verified): RSS is flat while hot, but
  once the workload quiets khugepaged collapses the heap into 2M pages and
  re-materializes every 4K hole jemalloc had purged — RSS climbed
  699→890MB at constant used_memory 539MB, plateaued, and never returned
  (control: 680MB flat). Same-data RSS settles ~+31% over non-THP,
  eroding maxmemory eviction headroom. The classic Redis fork-CoW
  objection does not apply to moon (BGSAVE is an in-process per-shard
  epoch snapshot; no `fork()` in the tree). Flag/CLAUDE.md docs updated
  with the verdict and the enable-when guidance (uniform value sizes +
  RSS headroom); full data in `tmp/THP-SOAK-RESULTS.md`.

### Fixed
- **Multi-shard SWAPDB is now durable before `+OK` (#133).** Two defects
  in `coordinate_swapdb`: (1) no fsync rendezvous — under
  `--appendfsync always` the client saw `+OK` before any shard had
  fsynced its SWAPDB record; (2) the coordinator shard's own record was
  written to WAL v3 only, never the per-shard AOF — and for
  `--shards ≥ 2 --appendonly yes` the per-shard AOF manifest is the sole
  recovery authority, so a kill-9 after `+OK` permanently lost the LOCAL
  shard's half of the swap while remote shards' halves survived
  (cross-shard keyspace divergence; reproduced empirically). The local
  leg now performs a durable AOF group-commit append + fsync barrier
  BEFORE swapping AND before any remote dispatch (every abort point fires
  while the whole cluster is still unmutated — the pre-fix order left
  N−1 shards swapped on a local abort); the coordinator confirms each
  remote shard's durability with one post-ack `fsync_barrier`
  (H1-BARRIER ordering). A remote barrier failure after that shard's
  swap is reported truthfully as durability-unconfirmed rather than a
  false `+OK`. Replica-side SWAPDB application (streamed SWAPDB records
  currently no-op on replicas — a pre-existing gap on the remote legs
  too) is deliberately out of scope and tracked separately.

### Performance
- **New `benches/dashtable_probe.rs`: DashTable point-lookup benchmark at
  cache-exceeding sizes (O1), and a recorded dead end for value-line
  prefetch.** The existing 10K-key bench is cache-resident and cannot see
  probe miss latency (18.65%/11.09% of cycles on ARM/x86 per real-PMU GCE
  measurement); the new bench builds 100K/1M-key tables and looks up in
  fixed-seed shuffled order, with the hit path forcing a real load
  through the returned entry — `black_box` alone does not read through a
  reference (disassembly-verified), a harness trap that initially
  produced a phantom "3-6% x86 win" for prefetching `values[slot]` at the
  first H2 tag match. With the forced load, that prefetch measured ~1-2%
  SLOWER or noise on pinned x86 (c3) at 1M keys and a consistent
  regression on aarch64 (t2a `prfm` variant: get_hit/100K +14%) — so no
  prefetch ships; the negative result and methodology requirement are
  documented in `segment/mod.rs` next to the earlier aarch64 key-prefetch
  verdict. The probe path keeps its existing one-cache-line ctrl
  verdicts, directory-level segment prefetch, and x86 key prefetch.
- **`--memory-thp` opts jemalloc's value heap into transparent huge pages
  (O4).** New opt-in flag layers `thp:always` onto the baked-in
  `_rjem_malloc_conf` (on top of the always-on `metadata_thp:auto`, which
  only huge-pages jemalloc's own metadata, not application allocations).
  Real-PMU measured on GCE vPMU (c4a Axion / c4 Emerald Rapids,
  `tmp/GCE-PMU-RESULTS.md`): GET RPS +24.4% (ARM) / +12.1% (x86), dTLB
  MPKI 7.2→4.7 (ARM, −35%) / 1.10→0.017 (x86, −98.4%), IPC +0.15/+0.09 —
  ARM pays roughly 10× the x86 dTLB tax at baseline (smaller dTLB, 4K
  pages) so THP is a disproportionately larger ARM win. Costs RSS +4.2%
  on both architectures. Implemented via the same `_RJEM_MALLOC_CONF`
  execve re-spawn as `--memory-arenas-cap` (PERF-10) — passing both flags
  together composes into exactly one re-spawn carrying one conf string
  (`narenas:N,...,thp:always`); an operator-set `_RJEM_MALLOC_CONF` still
  wins over either flag (warns, no-op). No-op with a warning on
  non-jemalloc builds, AND on non-Linux jemalloc builds (macOS) — THP is a
  Linux kernel feature, and jemalloc does not silently ignore an
  unsupported `thp:always` under the baked-in `abort_conf:true`; it aborts
  the process at init instead (verified experimentally on macOS
  2026-07-18). `--memory-arenas-cap` still composes normally when paired
  with a downgraded `--memory-thp` on non-Linux hosts. VM-verified on
  OrbStack Linux (madvise THP policy): `AnonHugePages` jumps from ~5% of
  RSS (baked-in `metadata_thp:auto` only) to ~96% of RSS with the flag on
  a 500MB SET load, confirming the opt actually engages. Shipped opt-in
  only — an RSS-drift soak is still pending before any default flip.
  Both flags are CLI-only: a `moon.conf` value cannot reach jemalloc
  (its config is read at process start, before the conf file is parsed)
  and now triggers a loud startup warning instead of a silent no-op.
- **Auxiliary threads no longer contend with pinned shard cores (O5).**
  `manifest-sync-N`, `spill-N`, and `moon-wal-sync-N` are spawned from
  inside the shard's own (pinned) event-loop thread and, on Linux,
  inherit that thread's exact single-core affinity mask at
  `pthread_create` time — they weren't merely sharing the shard's core,
  they were born wearing its mask and could never leave it (confirmed via
  `ps -eLo psr`: `manifest-sync-N`/`spill-N` co-located on shard N's
  core; the main accept/dispatch thread also floated onto a shard core,
  ~12% CPU steal observed under load). When `--shards` leaves a
  remainder of un-pinned cores, every auxiliary thread (main
  accept/dispatch, `manifest-sync-N`, `spill-N`, `moon-wal-sync-N`,
  the AOF writer(s), auto-save) now re-pins itself to that non-shard
  core set (round-robin) as the first act of its closure — except the
  main thread itself, which re-pins at the LAST moment before entering
  the accept loop, because children spawned from main inherit its mask
  and the vector pools must keep the full-machine mask. Adversarial
  review widened coverage to every OTHER pinned-parent spawn site in
  the tree: `moon-heap-orphan-sweep-N` (a ~40s crash-recovery I/O burst
  previously confined to its shard's own core), the lazily-spawned
  `moon-cold-read-N` pool (previously born on whichever ONE shard core
  served the first cold read and stuck there for the process lifetime,
  serving all shards), `moon-vec-snapshot-N`, `moon-vec-idx-gc`, and
  the `moon-vec-compact-N` pool (its old last-core-downward heuristic
  landed workers on shard cores whenever shards ≈ cores; it now prefers
  the non-shard set via `aux_core_at`, keeping the heuristic as
  fallback). cpuset caveat: core IDs come from the machine-wide online
  list, so under a restrictive cgroup cpuset the pin can fail — the
  thread then keeps its inherited mask (debug-logged), never worse than
  before. No-op when
  `--shards >= cores` (no remainder to place them on) or on non-Linux.
  `MOON_NO_AUX_PIN=1` disables it. The vector segment-reload pool
  (`moon-vec-reload-N`) is deliberately left scheduler-placed — reloads
  are latency-critical and the cold-start/crash-recovery reload storm
  wants every core while shard threads are idle; confining it to the
  small non-shard remainder risks 3x worse cold-reload latency, the
  regression PR #237 fixed. jemalloc's background-reclaim threads are
  unaffected — jemalloc spawns them internally with no Rust-side
  affinity hook. A/B on GCE t2a-standard-8 (8 dedicated ARM vCPUs,
  shards=4, AOF on, 4 interleaved rounds vs `MOON_NO_AUX_PIN=1`):
  SET median +1.5%, GET median −1.9% (inside noise; GET run-to-run
  spread tightened 23%→7% under pinning), with `ps -eLo psr` placement
  proof — pinned legs show all 12 aux threads on the non-shard cores
  4-7, unpinned legs show `manifest-sync-N`/`spill-N` sitting exactly
  on shard cores 0-3.
- **Shard offload paths are precomputed at shard init — the recurring
  tick paths no longer allocate (#45).** The 100ms eviction tick, the
  memory-pressure cascade, the 10s warm-transition check, and the
  cold-orphan sweep each rebuilt `<offload>/shard-{id}` via
  `format!` + `PathBuf::join` (plus a `PathBuf` clone inside
  `effective_disk_offload_dir()`) on every firing, per shard — a
  CLAUDE.md hot-path-allocation violation. The event loop's existing
  per-shard `disk_offload_dir` (built once at init, `Some` iff
  disk-offload is enabled) is now threaded into `run_eviction_tick`
  and `handle_memory_pressure` as `Option<&Path>` and used directly by
  the warm/orphan tick arms. Cold event-gated builders (save trigger,
  reclamation-schedule persist, startup/recovery) are unchanged.
- **SCAN cold-plane pages now range-resume from the cursor — no full
  cold-index filter per page (#368).** The in-RAM cold index (spilled
  keys under disk-offload) is now ordered by the same `(hash48, key)`
  SCAN order as the hot plane (`BTreeMap` keyed by the 48-bit cursor
  hash), so each SCAN page seeks to the cursor in O(log n) and takes
  the first COUNT live candidates instead of filtering every spilled
  key on every page. Completes the two-plane O(COUNT)-per-page walk on
  the hot-plane change below. Cold `lookup`/`remove` pay an O(log n)
  tree descent instead of an O(1) hash probe — those sit on
  disk-read-through, promotion, and sweep paths where the descent is
  noise next to the I/O they front, and the ordered map replaces (not
  duplicates) the hash map, so per-entry RAM stays comparable. Public
  `ColdIndex` API and recovery/rebuild behavior are unchanged.
- **SCAN hot-plane pages are now true O(COUNT) — no full-table walk per
  page (#368).** The SCAN cursor hash is now the DashTable's own
  fixed-seed key hash truncated to its top 48 bits, which makes cursor
  ranges line up exactly with the extendible-hashing directory (indexed
  by top hash bits): segments are range-partitioned in hash space, so a
  page visits only the segments covering hashes at or after the cursor
  and stops as soon as COUNT entries are collected
  (`DashTable::hash_page` → `Database::scan_hot_page`). Per-page hot
  cost is now independent of keyspace size (previously every page walked
  and hashed all n live entries). Splits, merges, and directory doubling
  between pages remain safe by construction — the cursor is a position
  in hash space, and structural churn only changes which segment covers
  that position, never the set of keys at or above it. The cold plane
  keeps its filtered in-RAM index walk (bounded by spilled-key count;
  ordered cold-side paging remains a follow-up in #368). SCAN cursors
  from before this change are invalidated (cursors are documented as
  ephemeral; restart scans at 0).

### Fixed
- **Cold recovery re-attaches spilled keys to the logical database they
  were evicted from (#139).** Under disk-offload with `SELECT N` (N>0),
  the recovery rebuild used to merge every spilled key into db 0's cold
  index: SELECT>0 keys answered nil after restart (their only durable
  copy unreachable), and a same-named key could even serve the WRONG
  database's value from db 0. Spill files are now attributed wholesale
  via a `db_index` field in the manifest `FileEntry` (a byte-compatible
  repurpose of the always-written-as-zero `min_key_hash` slot — old
  manifests read as db 0, which matches their actual provenance), spill
  flush chunks never cross a db boundary so each file is single-db, and
  recovery attaches one rebuilt cold index per database before WAL/AOF
  tail replay (so replayed DEL/FLUSH still tombstone the right plane). A
  manifest referencing a db beyond the configured `--databases` count is
  skipped with a loud warning instead of silently resurrecting keys into
  a wrong database.
- **SCAN now honors the Redis stable-key guarantee under churn (#368).**
  The cursor was a positional index into a keyspace snapshot re-collected
  and re-sorted on every page, so any insert/delete (or cold-plane
  spill/promotion/TTL churn) between pages shifted positions and could
  skip a key that existed for the entire scan — directly affecting the
  backup/migration-via-SCAN use case. SCAN pages now iterate in stable
  48-bit key-hash order and the cursor is a position in hash space: a
  key's hash never changes, so churn cannot displace it, and a key present
  throughout the scan is returned exactly once. Cursors stay numeric
  (48-bit, fitting the multi-shard composite cursor's per-shard slot
  unchanged) and remain client-compatible. Page cost drops from
  `collect + sort O(n log n)` plus a second full-table lookup pass (and,
  on the write path, a full-keyspace lazy-expiry probe per page) to one
  walk with a bounded COUNT-min selection heap. COUNT stays a hint (Redis
  parity): a full page may defer a trailing equal-hash group to the next
  page. Follow-up tracked in #368: O(COUNT)-per-page via a DashTable
  bucket-order cursor.

### Testing
- **`crash_matrix_cross_plane` flake diagnostics for the rare mid-scenario
  `ConnectionReset` (#365).** The harness's RESP connection now records the
  last command (or pipeline summary) sent, and includes it — plus the peer
  address — in the panic when a read/write fails or the connection closes
  mid-frame, answering "WHICH phase reset". `ServerGuard`'s `pkill -9 -f
  <dir>` backstop now logs `pgrep -fl` survivors before firing (the child
  is already reaped at that point, so any hit is a leaked respawn or a
  marker-substring collision about to be collateral-killed — the issue's
  unproven alternate hypothesis). No behavior change on green runs.

### Performance
- **Adaptive idle park (#373 phase 2, monoio runtime).** After 64
  consecutive provably-no-op 1ms ticks, a shard's event loop stretches its
  periodic park to 10ms, cutting idle timer wakeups ~10×. Zero chore-cadence
  changes: every counter-based sub-timer (block-timeout 10ms, expiry/eviction
  100ms, WAL-sync 1s, monitors 5s, warm/autovacuum/orphan-sweep) divides by
  the stretched period and entry is gated on an aligned counter, so each
  chore fires exactly when it did before — BLPOP timeout precision is
  unchanged. Eligibility is conservative (no commands counted on the shard
  thread since the last tick — local dispatch AND replica-applied commands
  both bump the counter; no cross-shard SPSC message pending at the drain,
  probed by queue occupancy; WAL buffer empty; no snapshot/checkpoint
  active; `appendfsync != always`; no CDC subscribers); any SPSC notify
  wake exits idle immediately, and the cached clock is refreshed before
  draining pending cross-shard messages on BOTH race outcomes, so
  cross-shard commands never observe stretched staleness. Documented
  trade-off: a command arriving mid-park on an existing LOCAL connection
  can read a cached clock up to 10ms stale (vs 1ms before) for lazy-expiry
  checks, only after ≥64ms of complete shard quiet; active expiry keeps its
  100ms cadence. Replica-applied commands additionally now count toward
  `total_commands_processed` (Redis parity; previously uncounted). Escape hatch: `MOON_IDLE_PARK=0` pins the fixed 1ms period
  (same-binary A/B knob). Tokio runtime unchanged.
- **Idle-tick CPU trimmed (#373 phase 1).** Three per-tick costs on the
  shard event loop's 1ms/100ms cadences were removed without touching any
  cadence or park timing: (1) `PageCache::resident_buffer_bytes` — the #1
  consumer in an idle-server perf profile (~17% of samples; it walked every
  frame buffer taking a `parking_lot` read lock each, every 100ms per
  shard) — is now an O(1) relaxed atomic load, maintained at the single
  buffer-grow site (buffers never shrink, so a monotonic counter is exactly
  equivalent); (2) `CheckpointTrigger::should_checkpoint` no longer calls
  `Instant::now()` every 1ms tick for its whole-seconds timeout — it reads
  the shard's cached clock; (3) `CachedClock::update` (the one designated
  clock read per tick) now makes one `SystemTime::now()` call instead of
  two, deriving seconds from milliseconds.

### Documentation
- Persistence guide and Valkey comparison no longer describe the removed
  WAL v2 (`src/persistence/wal.rs`); both now document WAL v3
  (`src/persistence/wal_v3/`: segmented files, per-record LSN, lz4 FPI,
  off-loop fsync agent).

### Fixed
- **Test-harness hygiene sweep: hardcoded `target/release/moon` paths and
  unguarded child spawns across `tests/`.** 33 integration suites resolved
  the moon server binary via a bare `./target/release/moon` default, a
  `CARGO_MANIFEST_DIR`-relative guess, or a local `find_moon_binary()` copy
  that never checked `CARGO_BIN_EXE_moon` — a stale binary of unknown
  provenance on a shared checkout could silently run instead of the one
  cargo actually built for the test; migrated to `common::find_moon_binary()`
  (5 suites with a documented "skip gracefully when unbuilt" contract keep
  their own `Option<PathBuf>` resolver, with the same `CARGO_BIN_EXE_moon`
  tier added ahead of the stale-path fallback). Separately, 6 suites
  (`console_gateway_test`, `scan_fanout_multishard`,
  `allocator_mimalloc_smoke`, `perf_v0112_arenas_cap`,
  `replication_readonly_eval`, `replication_readonly_ws_mq`) held a bare
  `Child` across many `assert!`/`.expect()` calls with cleanup only at the
  end of the function — a mid-test panic orphaned the server, the same
  shape behind issue #366's 667%-CPU incident — and now use the
  kill-on-drop `MoonGuard` pattern from `tests/bgsave_startup_race.rs`.
  Complex multi-restart harnesses (crash-recovery kill-9 cycles, Jepsen,
  instance-lock, SIGTERM) were left untouched by design.
- **`tests/bgsave_startup_race.rs` can no longer orphan its server on a
  mid-test panic** — the spawned child is now held by a kill-on-drop guard
  (the pattern from `tests/dir_deleted_degraded.rs`). An orphan from this
  exact suite, its tmpdir later swept, was the 667%-CPU incident behind
  issue #366.
- **Data-dir deleted under a running server now latches a degraded state
  instead of error-looping the persistence tick (#366).** When `--dir`
  vanishes (operator `rm -rf`, tmp-cleaner sweep, mount loss), the per-shard
  1ms WAL/checkpoint tick used to retry its file operations on `ENOENT`
  forever — observed in the wild at ~667% CPU on an idle 4-shard server,
  with a wedged `PING`. The disk monitor's 5s poll now distinguishes
  `statvfs` `ENOENT`/`ENOTDIR` on the monitored path and latches `dir_lost`
  (one loud `ERROR` log): write commands are refused with `MOONERR
  dirmissing` (reads and `PING` keep serving, mirroring the diskfull guard),
  and the persistence tick skips WAL flush / checkpoint / WAL-overflow scan
  / auto-save entirely — zero per-tick syscalls and zero per-tick log lines
  while latched. The latch self-heals: the next poll that sees the directory
  again resumes writes (with an explicit warning that data accepted since
  the deletion is not guaranteed durable until restart). Only engages when
  persistence or disk-offload is configured, and works with
  `--disk-free-min-pct 0`. Two hardening layers close the shallow-heal hole
  (adversarial review): WAL segment rotation re-creates a missing parent
  directory (`mkdir -p <dir>` after an incident no longer leaves the nested
  `wal-v3/` dir dead), and any tick-flush failure arms a 1s retry backoff so
  no flush error class can ever loop at the 1ms tick cadence again. The
  latch is unix-only by design (the non-unix statvfs stub never fails); the
  latch-behavior unit test is `cfg(unix)`-gated accordingly, and the e2e
  suite's latched-CPU check is relative to a pre-deletion baseline window so
  shared-runner load cannot flake it.

### Performance
- **Disk-offload: shard event loop no longer pays manifest-commit fsyncs
  (task #59 levers 1+2).** Under spill load, `apply_completion_vec` ran one
  DURABLE manifest commit (up to 2 fsyncs) per spilled file **on the shard
  event-loop thread** — strace-measured at 1.0–2.1s cumulative block time per
  8s flood window per shard (single fsyncs up to 1.0s), stalling every
  connection on the shard. `ShardManifest` now splits into loop-owned RAM
  state + a per-shard `manifest-sync-{id}` thread owning the file handle:
  spill-completion commits are deferred, coalescing snapshot sends (correct
  because that path only runs under `--appendonly yes`, where AOF replay +
  the orphan sweep reconstruct anything a lost manifest commit recorded);
  `commit()` keeps its exact blocking durability for the checkpoint protocol
  and the no-AOF durable batch spill path. The crash-safety write order
  (overflow pages sync'd before the root that references them; dual-slot
  atomic root flip) is preserved verbatim. Additionally the spill writer now
  yields a 4ms quantum after each durable batch flush while a cold read is
  in flight (`COLD_READS_INFLIGHT` RAII signal from both the async pool and
  the synchronous MGET/MULTI/Lua read paths), never pacing when the request
  backlog is deep (pacing must not push eviction into `-OOM`) or at
  shutdown.

### Added
- **Data-dir instance lock: a second moon on the same `--dir` is refused at
  startup.** Two servers writing one directory means two writers on the same
  per-shard WAL/AOF/spill manifests — silent corruption with no error at
  either end (observed in dev as dozens of leaked instances accumulating
  against shared folders). Moon now takes an exclusive non-blocking
  `flock(2)` on `<dir>/moon.lock` before any listener binds or data file
  opens, held for the process lifetime; a competing start exits fast with
  the holder's pid. `kill -9` releases the lock instantly (kernel advisory
  lock — no stale-pidfile failure mode; crash-restart is unaffected). A
  custom `--disk-offload-dir` is locked too when it differs from `--dir`.
  Unix only (Linux + macOS); Windows warns and proceeds. Operational note:
  concurrent instances on one host must now use distinct `--dir` values —
  which was already the only safe configuration.

### Fixed
- **BGSAVE issued during shard startup could hang forever (pre-existing,
  reachable in v0.8.0 and earlier).** The listener answers clients as soon as
  the fastest shard's event loop is up, but each shard seeded its snapshot
  epoch cursor from the trigger watch channel's *current* value at loop
  start — a BGSAVE (or auto-save) broadcast while a slower shard was still
  initializing was silently swallowed by that shard: its snapshot never ran,
  `BGSAVE_SHARDS_REMAINING` never reached zero, and `rdb_bgsave_in_progress`
  stuck at `1` with every later BGSAVE refused as already-in-progress until
  restart. Found as a ~15%/run crash-matrix flake under full-suite CPU
  contention; reproduced deterministically with a delayed shard start (new
  test-only `MOON_TEST_SLOW_SHARD_START_MS` fault injection) and pinned by
  `tests/bgsave_startup_race.rs`. The cursor now starts at 0 (epochs are
  per-process), so a trigger that arrives during startup is honored on the
  shard's first tick. The same test exposed a second pre-existing gap: the
  **tokio** shard loop never called `bgsave_shard_done` after finalizing its
  snapshot (only the monoio arm did), so under `runtime-tokio` every sharded
  BGSAVE left `rdb_bgsave_in_progress` stuck at 1 — now decremented in both
  arms.
- **DBSIZE / INFO `# Keyspace` count logical keys under disk-offload
  (issue #355).** Both previously reported the resident set only — the
  2026-07-16 G2 re-run wrote ~164K distinct keys and DBSIZE answered 24,275
  (~86% under-report), breaking operator capacity math. `Database::
  logical_len()` now counts hot + cold keys with hot∩cold overlap (a fresh
  SET over a cold-only key legitimately leaves its cold shadow until the
  next touch) counted once via an O(cold) probe pass — same order as the
  `expires_count` scan INFO already pays, zero hot-path cost, no
  counter-drift risk. Wired through all six sites: `dbsize`,
  `dbsize_readonly`, the INFO fallback keyspace section, the
  `KeyspaceStats` scatter handler, the embedded/non-sharded
  `handler_single` INFO keyspace vector, and `coordinate_dbsize`'s local leg
  (which inlined a resident-only `db.len()` while its remote legs
  dispatched real DBSIZE commands — the two definitions disagreed inside a
  single reply). Known remaining parity gap, tracked separately: SCAN /
  KEYS / RANDOMKEY still enumerate the hot plane only.
- **Keyspace enumeration under disk-offload: SCAN/KEYS/RANDOMKEY now see
  spilled keys (#364).** With disk-offload enabled, cold-only keys (spilled by
  eviction, no in-RAM entry) were readable via GET/EXISTS but invisible to
  enumeration — a 4-shard instance holding 400 logical keys returned only 116
  from `redis-cli --scan`, silently losing spilled keys for any
  migration/backup consumer. SCAN, KEYS, and RANDOMKEY (both dispatch tracks)
  now enumerate the union of the hot plane and the in-RAM cold index,
  partitioned so a key present in both planes is returned exactly once and
  TTL-expired cold entries are skipped — pure in-RAM, no disk reads and no
  promotion. SCAN's `TYPE` filter judges cold keys from a new
  `ColdLocation::value_type` cache (same cached-copy contract as `ttl_ms`:
  populated at spill time, re-derived from the on-disk pages by
  `ColdIndex::rebuild_from_manifest` after restart; fits existing struct
  padding, so the cold index does not grow; no on-disk format change).
- **Storage: recovery panic `double NeedsSplit after split_segment` in
  `DashTable::insert_or_update`.** The insert-or-update path split an overflowing
  segment exactly once and declared a second `NeedsSplit` unreachable — false under
  hash skew: when the overflowing segment's keys share the next directory bit, the
  split routes all of them into the same child (still over `LOAD_THRESHOLD`) and the
  retry panicked. Deterministically reproduced in production loading a 219k-key shard
  checkpoint (`shard-0.rrdshard`) on 0.7.1 recovery — the server crash-looped until
  the checkpoint was quarantined (code identical back to v0.6.0). Now split-retries
  in a loop, mirroring `insert`'s recursion; regression test brute-forces 56 keys
  sharing the top 12 xxh64 bits to force the double split.
- **Test harness: five latently broken integration suites unmasked by the
  task #59 gate battery.** `cmd_flush_dbsize_debug_memory` still passed the
  `--persistence-dir` flag removed in June (server exited 2 before accepting;
  CI stayed green only because the suite self-skips without a release
  binary); it and `memory_doctor_response` ignored `MOON_BIN` via hardcoded
  `target/release/moon` finders (stale host Mach-O via OrbStack proxy → 30s
  spawn timeouts). `mem_watchdog`, `oom_bypass_closure`, and `spsc_two_db`
  now pass `--disk-free-min-pct 0` so a near-full dev volume's diskfull
  write pause can't shadow the memory-guard/routing behavior they assert.

## [0.8.0] — 2026-07-16

### Documentation
- **G2 10×-RAM re-run report (v0.8 close-out evidence).** New
  `docs/perf/2026-07-16-g2-10x-ram-rerun.md`: re-ran the G2 acceptance
  benchmark on main @ `4dcfd533` with the full v0.8 storage batch merged.
  Headlines vs the 2026-07-13 baseline: spill files 236K → **840** (~280×,
  PR #350 confirmed at scale); `used_memory` truthful at **1.00× cap**
  steady-state with a ≤5 s post-restart drain to under-cap (task #56,
  PR #349; demote pass logged 83,788 shadows); cold-GET-during-spill worst
  tail **1,910 ms → 205 ms** (task #59 still open for sub-10 ms); restart
  now AOF-replay-bound (16.9 s at 3.3 GB unrewritten incr AOF — spill-file
  count is out of the boot path entirely); 500/500 kill-9 integrity.
  PRODUCTION-CONTRACT rows `CRASH-02` (37→46 cells + scheduled CI) and
  `MEM-10X-01` refreshed with this evidence + audit-trail row. New finding
  filed as #355: `DBSIZE` counts only resident keys under disk-offload
  (~24K reported vs ~164K logical).

### Fixed
- **Vector auto-merge CPU livelock: rejected GraphUnion merges now back off
  exponentially instead of retrying every autovacuum tick.** A background
  merge rejected by the recall gate (or the 512 MiB memory ceiling) keeps its
  source segments, so every `needs_merge` trigger condition — segment count
  > 16, dead fraction > 20% — remains true, and the next tick resubmitted the
  *identical* doomed merge. Each recall-gate rejection discards a fully built
  union HNSW graph (plus the AE-1 self-probe), so an index stuck below
  tolerance pinned the `moon-vec-compact-*` worker pool at ~100%/core
  indefinitely — observed in production at ~300% CPU sustained on an
  embeddings workload, with no log signal (the rejection was `debug!`-level).
  Fixes, all in `src/vector/store.rs`:
  - `poll_install_merge`'s rejection path now records a `MergeBackoff`
    (xxh64 fingerprint of the source-segment `Arc` identities, consecutive
    failure count, exponential window 60s→1h) and logs at `warn!` with the
    failure count and next-retry delay — naturally rate-limited to once per
    window.
  - `begin_background_merge_due` skips dispatch while the *same* segment set
    is inside its window; any set change (new compaction, warm-tier
    transition, installed merge) or a successful install clears the backoff
    immediately. Manual `FT.COMPACT` (force-merge) is unaffected, and
    `FT.CONFIG SET <idx> MERGE_RECALL_TOLERANCE` clears the backoff so
    operator intervention retries at once.
  - `needs_merge`'s segment-count trigger now honors the merge memory
    ceiling (previously only the dead-fraction trigger did): an index whose
    live vectors exceed 512 MiB was re-dispatching a merge that
    `merge_immutable` deterministically refuses — a guaranteed livelock for
    indexes past the ceiling. The store-level `needs_merge` now delegates to
    the index-level check instead of duplicating (and diverging from) it.
  New unit tests: `bg_merge_gate_failure_backs_off` (RED against the old
  behavior), `bg_merge_backoff_clears_on_segment_set_change`,
  `bg_merge_backoff_clears_on_tolerance_change`, `bg_merge_backoff_schedule`.

### Changed
- **CI: heavy vector recall benchmarks moved out of the per-PR gate
  (~6-24 min tail cut).** Duration data from a green main run (2026-07-16)
  showed the recall family was ~half the whole suite's CPU, with the top
  case (`test_f32_recall_10k_128d`) at 182-480s per attempt at the
  unoptimized test profile — and the two biggest cases were the only tests
  that flaked (480s nextest timeout × 3 retries = 24 wasted minutes) when
  the runner VM shared a loaded host. Five recall-quality canaries
  (`test_f32_recall_10k_128d`, `test_f32_recall_1k_768d`,
  `recall_10k_128d_ef128`, `recall_1k_768d_ef128`,
  `test_parallel_recall_parity_with_sequential`) are now `#[ignore]`d,
  matching the pre-existing convention for the 768d/10K cases; a fast smoke
  variant (1k/128d, both unit and integration) still runs per-PR, and 27
  recall-adjacent tests remain in the PR gate. A new nightly
  `recall-canaries` job (`.github/workflows/crash-matrix.yml`) runs the
  FULL family — ignored and not — at release profile via
  `--run-ignored all -E 'test(/recall/)'`, limited to the two binaries that
  contain them. nextest `profile.ci` override: `test(/recall/)` gets
  `retries = 0` (CPU-bound + seed-deterministic — retrying a timeout or a
  threshold failure only multiplies the waste) and a 120s×5 slow-timeout.
  Dependabot: new `cargo-minor-patch` catch-all group collapses the Monday
  minor/patch flood into one PR — six near-simultaneous dependabot CI runs
  starved the single self-hosted runner for >20 min on 2026-07-15.

### Performance
- **Disk-offload spill files now batch effectively — file count scales as
  ~keys/batch, not ~keys (v0.8 item 3, task #57 follow-up).** The G2
  acceptance run (260K × 10KB keys, 256MB cap) produced ~236K `heap-*.mpf`
  files despite the format nominally supporting ≤256 keys/file batching.
  Root cause: `SpillThread::flush_buffer` (`src/storage/tiered/spill_thread.rs`)
  already coalesced up to 256 requests per flush, but partitioned them by
  value size before writing — every entry over `INLINE_MAX_VALUE_BYTES`
  (3500B, e.g. all of G2's 10KB values) was routed to its own dedicated
  single-entry file via `spill_single_entry`, defeating the batch entirely.
  `build_kv_spill_batch` / `write_kv_spill_batch`
  (`src/storage/tiered/kv_spill.rs`) are rewritten to pack inline AND
  oversized entries into ONE shared batch file per flush: each oversized
  entry gets a dedicated leaf (holding an overflow pointer) plus its
  overflow-page chain placed inline in the same page stream, with the
  atomic temp-file+`sync_all`+rename+`fsync_directory` write sequence
  unchanged (payload-before-reference durability ordering preserved — a
  key is only considered cold after its containing segment is durable).
  Empirical measurement (macOS, matching G2's shape): RED (pre-fix)
  3742 files / ~34,908 evicted 10KB keys; GREEN (post-fix) 29 files for
  the same eviction load — a ~129× reduction, `spill_batches_flushed`
  now matches file count 1:1 as designed.
  Fixing this also uncovered and repaired a latent bug in
  `build_overflow_chain` (`src/persistence/kv_page.rs`): its prev/next
  link computation silently assumed every chain's `start_page_id == 1`
  (true of every caller before this change, since a single-entry file
  always put its one leaf at page 0) — a chain-local `i+1`/`i+2` formula
  that produced dangling/wrong links the moment a caller (this new
  batching code) passed a variable `start_page_id > 1` for the second,
  third, etc. oversized entry sharing a file. Links are now computed as
  file-absolute (`start_page_id + i`), which is also what
  `read_overflow_chain` already expected. New unit tests cover
  oversized-entries-share-one-file, mixed inline/oversized batches, and
  the recovery-scan/builder agreement for oversized batches
  (`kv_spill.rs::tests`); a new end-to-end integration test,
  `tests/crash_recovery_spill_batch_kill9.rs`, drives a real server
  through a sustained eviction+spill burst of 8000-byte values and
  SIGKILLs it with no settle window, asserting zero acknowledged-write
  loss and a batched (not ~1-file-per-key) heap-file count survive the
  crash and restart under both the monoio and tokio runtimes.
- **Adversarial-review follow-ups on the above (same task).** Three gaps
  closed on `perf/spill-segment-batching` before merge:
  - **Genuine post-restart cold-read coverage.** The kill-9 test above only
    proves AOF-replay-derived recovery — every acked key ends up hot via
    `DispatchReplayEngine`, never touching `cold_read_through`, so it cannot
    tell "the leaf-offset + overflow-chain-with-`start_page_id>1` read path
    works" apart from "AOF replay papered over a broken cold layout". A new
    `spill_batch_shared_file_survives_cold_read_after_kill9` test
    pre-seeds a 3-entry batch file (1 inline + 2 oversized, ordered so the
    second oversized entry's chain starts at a file-absolute `page_idx > 1`)
    directly via `build_kv_spill_batch`/`write_kv_spill_batch` +
    `ShardManifest`, boots a real `--appendonly no` server on top of it,
    kills it without ever touching the keys, restarts, and GETs all three —
    the only way any of these bytes can reach the client is a genuine
    `ColdIndex::rebuild_from_manifest` + `cold_read_through` read. Getting
    this test to actually exercise that path (rather than a hot-RAM hit)
    required also pulling in `src/persistence/recovery.rs`'s fix from the
    concurrent task #56 effort (`fix/t56-used-memory-offload`, commit
    `689c52e0`): v3 recovery Phase 3 used to re-insert every previously-
    spilled `String` entry directly into the hot DashTable in ADDITION to
    rebuilding the `ColdIndex` stub for the same key — for an
    `entry_flags::OVERFLOW`-stubbed entry this hot-preload loop ignored the
    flag and inserted the raw 4-byte page-pointer as if it were the literal
    value, silently corrupting every oversized cold key on every restart
    (independent of this batching fix — it affected the pre-existing
    single-entry-per-file layout too). That recovery.rs hunk is a clean,
    self-contained cherry-pick (verified: identical parent-state diff,
    no dependency on task #56's other, out-of-scope-here, `used_memory`
    ledger/RSS/AOF-replay-demotion changes) and is a hard prerequisite for
    this test's premise, not something this task's rewrite could route
    around. A companion library-level unit test,
    `test_rebuild_from_manifest_mixed_inline_and_oversized_roundtrip`
    (`kv_spill.rs`), independently proves the exact same read path correct
    via direct calls to the production functions, without depending on
    server boot sequencing.
  - **Bounded batch-build memory (`BATCH_BYTES_CAP`, `spill_thread.rs`).**
    `build_kv_spill_batch` materialized an entire flush's pages in RAM
    before a single byte reached disk — fine for the 256-small-entries case
    the old comment described (~50 KB), but the whole point of this fix is
    that oversized entries now share that same unconditional batch, so 256
    consistently-large (e.g. near-`maxmemory`-sized) values could resident
    hundreds of MB at once, exactly when spills fire under memory pressure.
    `flush_buffer` now splits each `FLUSH_ENTRY_CAP`-sized buffer into
    sub-batches capped at 4 MiB of cumulative `value_bytes` (chosen against
    the OLD per-entry path's peak — one value's pages at a time, no
    cross-entry amplification — while leaving the G2 acceptance shape,
    256 × ~10 KB ≈ 2.5 MiB, as a single file, unchanged), each becoming its
    own file via that sub-batch's own already-pre-assigned `file_id` (no new
    allocation needed — see `SpillRequest::file_id`'s doc on harmless id
    gaps). New unit test: `oversized_flush_splits_into_byte_capped_sub_batches`.
  - **Hygiene.** Fixed two stale comments caught in review: `FLUSH_ENTRY_CAP`
    no longer claims to bound in-RAM size by itself (superseded by
    `BATCH_BYTES_CAP` above), and `entry_flags::OVERFLOW`'s doc corrected
    from a fictitious 12-byte `file_id:u64 + page_id:u32` layout to the
    actual 4-byte file-absolute `start_page_idx` (no `file_id` is stored —
    the chain always lives in the same physical file as its leaf stub).
    Added a one-line comment at `write_kv_spill_batch` explaining why it
    hand-rolls temp+fsync+rename instead of calling
    `persistence::atomic::atomic_write_durable`: that helper takes one
    pre-materialized `&[u8]`, and concatenating `batch.pages` into a single
    buffer first would undo `BATCH_BYTES_CAP`'s whole point — streaming
    each already-built page into the temp file keeps this write's own
    working set at one page at a time.

### Added
- **Crash-matrix CI (v0.8 exit-criterion item 4).** New `.github/workflows/crash-matrix.yml`
  wires `tests/crash_matrix_cross_plane.rs` (the 46-cell kernel M3/G1 kill-9 durability suite —
  KV/graph/vector/WS/MQ/cross-store-TXN across appendonly x disk-offload x shards) into scheduled
  CI on the self-hosted `moon-dev` runner: a nightly job (03:17 UTC) runs the full matrix at the
  default single iteration, and a weekly soak job (Saturday 04:41 UTC) runs
  `MOON_CRASH_MATRIX_ITERS=20` on the 10 cells with a probabilistic kill point
  (`mid_checkpoint`, `graph_drop_survives_repeated_checkpoints`, `txn_isolated_*`) — the cells
  kernel M3 stage 2 (task #53) proved need repeated sampling, not a single pass, to catch a
  timing-window regression. Both jobs support `workflow_dispatch` for on-demand runs. Not
  wired into the per-PR gate: the self-hosted runner is a single machine and PR-time is already
  tight (same rationale as `integration-tests.yml`'s `ci-full` label gate).

  Confirmation run on the moon-dev Linux VM (fresh ELF release binary, main @ `ec084556`,
  `MOON_BIN` pinned, `--test-threads=1`): the full 46-cell matrix passed **2/2 consecutive runs**
  (`46 passed; 0 failed`, ~79s each), including all former RED cells — the 6 legacy-mode graph
  cells (task #60 / PR #322) and the MQ generic-DEL resurrection cell (task #46 / PR #301) all
  now run ungated (no `harness::red_guard` call sites remain in the suite). A
  `MOON_CRASH_MATRIX_ITERS=5` soak of the 10 probabilistic-kill-point cells also passed 5/5
  iterations clean (`10 passed; 0 failed`, 40.3s) — these are the same cells that, pre-fix,
  failed at iteration 7/20 and 11/20 in the kernel M3 stage 2 investigation.

### Fixed
- **`used_memory` truthfulness under disk-offload (task #56).** The G2
  acceptance run (4 shards, `--maxmemory 256MB`, 2.6GB dataset,
  disk-offload enabled) reported `INFO` `used_memory` at 406-762MB against
  the 256MB cap, and worse after a kill-9 restart. Three independent bugs,
  found by instrumenting a 2-shard/8MB-cap/40k-key macOS repro
  (`tests/used_memory_offload_truthful.rs`) at load, steady state, and
  post-restart:
  - **`used_memory` was literally process RSS** (`src/command/connection.rs`),
    not the logical ledger `--maxmemory` eviction actually gates on. RSS
    always carries allocator arena overhead, mmap'd cold-read page-cache
    frames, thread stacks, the binary image, the (intentionally unbounded)
    Lua script cache, and the replication backlog ring — none of which the
    eviction system charges against the cap, so `used_memory` was
    guaranteed to read high under any disk-offload workload even when
    eviction was correctly holding the real ledger under budget. `INFO`'s
    `used_memory` now reports the same KV(+ColdIndex)+vector+text+graph
    "used-term" `ShardDatabases::recompute_elastic_budget` already gates on
    (`admin::metrics_setup::logical_used_memory_bytes`); `used_memory_rss`
    / `used_memory_peak` still report the true OS footprint alongside it, a
    new `moon_used_memory_bytes` Prometheus gauge mirrors the ledger figure,
    and `MEMORY DOCTOR` gained an explicit "gated vs RSS vs outside-the-cap"
    header so the two numbers are never conflated again.
  - **Restart double-loaded every spilled `String` key into hot RAM**
    (`src/persistence/recovery.rs`): v3 recovery Phase 3 re-inserted each
    previously-spilled `ValueType::String` entry directly into the hot
    DashTable (`#79-04`, predating cold read-through) AND — two days later,
    unrelated to the first change — rebuilt a `ColdIndex` stub for the same
    key pointing at the same on-disk location (`#80-02`); nobody removed
    the first loop when the second, correct mechanism landed. Every value
    type now recovers as a cold-index-only stub, exactly like
    Hash/List/Set/ZSet/Stream already did; the first `GET` lazily promotes
    a key into hot RAM (and only then charges `used_memory`) via the same
    `Database::promote_cold_if_present` path ordinary (non-restart) cold
    read-through uses.
  - **AOF replay re-hydrated already-cold keys back into hot RAM on every
    restart** (`src/main.rs`, `src/storage/db.rs`): fixing the bug above
    exposed a second, independent restart-time re-charge path that only
    surfaces when `--appendonly yes` *and* disk-offload are both active.
    An evicted-and-spilled key gets no AOF `DEL` record — a spilled entry
    stays cold-readable, not deleted, so `record_reason_del` never fires
    for it — so the AOF's incr log still holds that key's original `SET`.
    AOF replay (`main.rs`'s per-shard and single-shard multi-part branches)
    has no cold-tier awareness and blindly reapplies every historical `SET`
    into the hot DashTable, so a restart transiently re-inflated
    `used_memory` to ~6x the steady-state figure (self-healing over
    several eviction ticks, but proportionally slower to catch up the
    larger the disk-offloaded dataset — the "got WORSE after restart"
    symptom on the real 2.6GB G2 dataset). `Database::demote_replayed_cold_shadows`
    reconciles the two right after AOF replay finishes and before the
    server accepts connections: any key still present in the (already
    crash-consistent) `ColdIndex` at that point is provably redundant with
    whatever AOF replay just wrote hot for it (replay cannot re-cold a key,
    so the last AOF command touching a crash-time-cold key must be exactly
    the SET the eviction path spilled), so the redundant hot copy is
    dropped and `used_memory` no longer spikes at all post-restart.
  `tests/used_memory_offload_truthful.rs` drives all three mechanisms
  end-to-end (spill past `--maxmemory`, confirm real disk spill, SIGKILL,
  restart) and asserts `used_memory` stays within 1.75x `--maxmemory` at
  steady state AND immediately post-restart. See `docs/guides/monitoring.md`
  for the updated operator story on what counts against `--maxmemory` and
  what doesn't.
- **Adversarial-review follow-up on task #56 (three findings, all fixed on
  the same branch):**
  - **CRITICAL — stale cold-shadow resurrection on live overwrite.** The
    `demote_replayed_cold_shadows` reconciliation above assumed any key
    still present in the recovered `ColdIndex` after AOF replay must be
    crash-time-cold and untouched. False whenever a key is spilled at v1
    and then *live-overwritten* to v2 with no further eviction before a
    crash: the manifest still shows it spilled at v1, so a restart
    rebuilds `ColdIndex[key] = v1`, AOF replay reconstructs `hot[key] =
    v2`, and the (pre-fix) demote pass wrongly dropped the hot v2,
    resurrecting stale v1 on the next `GET`. `Database::set`
    (`src/storage/db.rs`) now clears the key's `ColdIndex` shadow the
    instant a SECOND write to that key is observed
    (`InsertOrUpdate::Updated`, not `Inserted`) — provably safe because AOF
    replay always starts from an empty DashTable (`db.clear()` runs before
    every replay branch), so the first replayed write to a key is always
    `Inserted` (left alone; it may be exactly the write that later got
    spilled) and any subsequent write to the same key during that same
    replay can only happen if the AOF recorded a write *after* the one
    that got spilled — proving the cold copy stale. No new record type or
    spill-file format change needed. Covered by the new unit test
    `storage::db::tests::test_second_write_invalidates_cold_shadow` and
    the new end-to-end kill-9 test
    `tests/cold_shadow_overwrite_resurrection.rs`.
  - **HIGH — demotion never wired into the no-manifest (tokio +
    `--shards 1`) recovery branch.** `demote_replayed_cold_shadows` was
    only invoked from the manifest-based AOF replay branches in
    `src/main.rs`; under `runtime-tokio` + `--shards 1` no manifest is
    ever created (`AofManifest::initialize` is monoio-only there), so the
    only KV replay for that runtime/shard combination — the Phase 4b
    `appendonly.aof` fallback inside `recover_shard_v3_pitr`
    (`src/persistence/recovery.rs`) — never demoted anything, leaving that
    config just as exposed to the AOF-replay-rehydration bug as the
    manifest-based paths were before task #56. The demote call is now
    also invoked at the end of Phase 4b. Covered by the new
    tokio+jemalloc-feature integration test
    `tests/cold_shadow_single_shard_tokio.rs`
    (`single_shard_overwritten_cold_key_returns_new_value_after_crash`),
    using `SETEX` rather than a bare `SET` per the documented
    monoio-write-gate/inline-SET-path gotcha.
  - **MEDIUM — `used_memory` parity gap: Lua script cache and replication
    backlog.** Real Redis's `used_memory` is "total allocator-attributed
    memory", not "memory eviction can reclaim" — it counts the Lua script
    cache and replication backlog even though neither is evictable.
    Decision: include both in the reported figure rather than document an
    exclusion list, since both were already tracked as separate
    `moon_memory_bytes{kind="lua_scripts"}` /
    `{kind="replication_backlog"}` gauges — folding them into
    `logical_used_memory_bytes()` (`src/admin/metrics_setup.rs`) and the
    `moon_used_memory_bytes` gauge cost no new instrumentation. The actual
    `--maxmemory` eviction gate (`ShardDatabases::recompute_elastic_budget`)
    is deliberately left unchanged and narrower — eviction has no
    mechanism to reclaim Lua bytecode or backlog bytes, so gating on them
    would free nothing. `MEMORY DOCTOR` (`src/command/server_admin.rs`)
    now prints three distinct figures (elastic budget / used_memory
    reported / RSS) instead of conflating the first two; see the expanded
    `docs/guides/monitoring.md` "`used_memory` vs RSS under disk-offload"
    section.

### Documentation
- **Roadmap Rev 2 (task #68, doc half).** `docs/roadmap/ROADMAP.md` updated to
  post-v0.7.1 actuals: v0.6.1/v0.7.0 marked shipped (with the R5/R6 slips and the
  single-shard-replica disclosure recorded), v0.8 re-slotted to **One Storage
  Kernel GA** (close-out of tasks #49/#56, spill-file batching, crash-matrix CI,
  10×-RAM benchmark publication), cluster hardening + multi-shard replicas moved
  to v0.9, enterprise foundation to v0.10; debt register refreshed.
- **Task #49 v0.8 close-out audit: no code change needed, roadmap corrected
  instead.** Re-verified every bare-write persistence site named in the kernel
  review (ACL SAVE, cluster `nodes.conf`, CONFIG REWRITE, replication state,
  native BGSAVE/RDB, `clog`, `kv_page`) against current `HEAD`: all 7 already
  route through `atomic_write_durable` (temp file → `sync_all` → `rename` →
  dir-fsync), shipped in PR #304 (merged 2026-07-13) and released as part of
  v0.7.0 — `git log 4e0688e6..HEAD` on every touched file confirms none of the
  9 converted call sites (ACL SAVE has two: `acl::io::acl_save` + the command
  handler routing through it; native BGSAVE has three: `rdb::save`,
  `rdb::save_from_snapshot`, `redis_rdb::save`) were reverted or bypassed
  since. The Rev 2 roadmap pass (task #68, above) had re-listed task #49 as an
  open v0.8 gap without checking it against the already-shipped v0.7.0
  CHANGELOG entry; `docs/roadmap/ROADMAP.md` §1 gap table, §4 v0.8.0 item 1,
  and §5 debt register are corrected to reflect this (struck through /
  removed, not deleted from history). Also audited adjacent hand-rolled
  writers for regression risk: `storage/tiered/warm_tier.rs`'s staging-dir →
  final-dir rename (own documented atomicity protocol, per-file `.mpf` writes
  inside the staging dir are covered by the directory-level rename, not a
  bare-write gap) and `storage/tiered/kv_spill.rs::write_kv_spill_batch`
  (already hand-rolled tmp+fsync+rename+dir-fsync correctly; not one of the 7
  named sites) both remain correctly out of scope — no change made.

### Changed
- **TLS: migrated off the unmaintained `rustls-pemfile` onto `rustls-pki-types`'s
  `PemObject` trait (task #66).** `build_tls_config` (`src/tls.rs`) now parses
  certificate chains via `CertificateDer::pem_reader_iter` and private keys via
  `PrivateKeyDer::from_pem_reader` — both provided directly by `rustls-pki-types`
  (already in the dependency graph as rustls's own `pki_types` re-export), so no
  new supply-chain surface is added. Behavior is unchanged: same fail-loud
  `io::Error` wrapping per stage (`TLS cert file` / `TLS cert parse` / `TLS key
  file` / `TLS key parse` / `CA cert parse`), same SIGHUP hot-reload path. Two
  new unit tests (`test_build_tls_config_garbage_cert_parse_error`,
  `test_build_tls_config_garbage_key_parse_error`) exercise the parser seam
  directly with corrupt-but-well-formed PEM bodies — the existing suite only
  covered missing-file paths, not actual DER decode failures. `rustls-pemfile`
  is fully removed from `Cargo.toml`/`Cargo.lock`; the RUSTSEC-2025-0134
  ignore entries in `deny.toml` and `.cargo/audit.toml` are removed since the
  advisory no longer applies. `cargo deny check advisories licenses bans
  sources` and `cargo audit` both pass clean with no ignore needed.
- **`cargo clippy --tests -- -D warnings` is now clean on both feature
  configurations (task #39).** CI previously only gated non-test code;
  ~170 test-target warnings (mostly `collapsible_if`, `doc_lazy_continuation`,
  `field_reassign_with_default`, `needless_range_loop`) are fixed
  mechanically, plus a handful of real bugs surfaced along the way: a
  `#[deny]`-level `approx_constant` false positive that was silently
  blocking `--tests` compilation entirely; three test-only helpers whose
  `#[cfg]` was looser than their actual (feature-gated) callers, making
  them dead code under `--no-default-features --features
  runtime-tokio,jemalloc` (`src/runtime/mod.rs`, `src/text/store.rs`,
  `tests/vector_db_isolation.rs`); and a `cargo clippy --fix` autofix that
  would have deleted a `key_to_shard` import still required under the
  default `graph` feature (`tests/sharded_multi_exec_locality.rs`) — caught
  by cross-checking against a default-features build before committing.
  `cargo clippy --all-targets -- -D warnings` (default features) is also
  clean — no residue in benches. No test assertions were altered.

## [0.7.1] — 2026-07-15

Patch release closing the two follow-ups disclosed in the v0.7.0 tag notes: the
SQ8 vector CPU error-storm and replica TTL determinism.

### Fixed
- **Vector: SQ8/TQ code-size mis-dispatch CPU error-storm.** SQ8's `bits()` returns 8,
  which falls outside TurboQuant's supported `1..=4` range; the free
  `code_bytes_per_vector(padded, bits)` helper hit its `_ =>` arm and returned `0` while
  logging a `tracing::error!` on **every** call. On the hot memory-accounting path
  (`store.rs` via `bytes_per_code_per_vector()`) this produced a CPU-pegging error storm
  and wrong resident-byte accounting for SQ8 indexes. Dispatch now computes the true SQ8
  layout (`dim` unpadded u8 codes + 8-byte affine `(min,scale)` trailer) directly, and the
  free-fn logs the unsupported-bit-width error at most once via an `AtomicBool` latch.
- **Replica TTL semantics — deterministic cross-node expiry (#71).** Two changes
  close the replica-TTL caveat disclosed in the v0.7.0 tag:
  - **#71a — master-side absolute rewrite.** Relative-expiry commands are now
    rewritten to absolute deadlines before they enter the durable log and the
    replication stream: `EXPIRE`/`PEXPIRE` → `PEXPIREAT`, `SETEX`/`PSETEX` →
    `SET … PXAT`, `SET … EX/PX` → `SET … PXAT`, `GETEX … EX/PX` → `PEXPIREAT`.
    The absolute deadline is computed from the master's per-tick cached clock —
    the exact value the command handler stored — so a replica (or an AOF replay
    after a restart) reproduces the master's expiry *instant* instead of
    restarting the countdown at apply/replay time. Already-absolute forms
    (`PEXPIREAT`, `EXPIREAT`, `EXAT`, `PXAT`, `PERSIST`) and past-time deletes
    propagate verbatim.
  - **#71b — role-gated active expiry.** A replica no longer runs its own
    active-expiry deletion sweep (both the monoio shard tick and the tokio
    background task); it keeps a logically-expired key resident (reads still see
    it as gone) until the master streams the authoritative removal, so both
    nodes delete a key at the same point in the stream instead of racing
    independent TTL sweeps.

## [0.7.0] — 2026-07-15

**Replication GA for multi-shard masters.** Moon now supports real Redis-compatible
asynchronous replication with `WAIT`/`ACK` acknowledgement semantics: a multi-shard
master (`--shards N`) streams a merged, exactly-once command feed to a single-shard
streaming replica across all data planes — KV, vector/text index, graph, workspace (WS),
message-queue (MQ), and temporal. Every write plane is crash-durable and survives
`kill -9` on either side, validated by a 24h continuous-load kill-9 soak (alternating
master/replica restarts every 12 min) that asserts zero loss of any `WAIT`-acknowledged
write. Also folds in the full v0.6.1 hardening scope (WAL v3 storage-kernel M1–M4:
cross-plane crash matrix, unified per-shard WAL-recycle floor, atomic durable writes,
FTS term-dict durability) and a supply-chain CI gate (`cargo audit` + `cargo deny`).

Soak evidence (release gate REPL-SOAK-01): `SOAK-PASS duration=86400s cycles=114
acked=82044 inflight=7 master_kills=57 replica_kills=57` — 82,044 WAIT-acked writes
preserved across 114 alternating kill-9 cycles, zero acked-write loss (2026-07-15,
run dir `moon-soak/runs/20260714-141946`, RC `e2d87893`).

Known limitation: the streaming replica is single-shard only (`--shards 1`) — the
multi-shard work in this release is master-side (merged N-shard PSYNC feed). Multi-shard
replicas are roadmapped for v0.8/v0.9.

Replica TTL semantics (disclosure): relative-expire commands (`EXPIRE`, `SETEX`, `PEXPIRE`,
`GETEX` with a relative TTL) currently replicate verbatim rather than being rewritten to
absolute `PEXPIREAT` on the master, and replicas run their own active-expiry cycle
regardless of role. In practice keys expire correctly on both sides under normal clock
sync, but a master/replica clock skew can shift a relative-TTL key's expiry moment between
the two by up to that skew. Absolute-expiry rewrite + role-gated passive expiry land in
v0.7.1 (task #71b). Applications needing exact cross-node expiry parity should set
absolute deadlines with `PEXPIREAT` until then.

### Documentation — replication configuration & tuning

Documented the v0.7 replication surface: corrected the stale `guides/clustering.md`
replica walkthrough (it still showed the pre-GA topology — single-shard leader,
multi-shard replica — the reverse of the shipped shape) and its outdated
"WS/MQ not replicated yet" note (all six planes replicate as of Wave B). Added the
`WAIT`-durability ladder, replica promotion (`REPLICAOF NO ONE`), `INFO replication`
monitoring, a **Read/write splitting** subsection (Moon has no built-in R/W-split
proxy — client-side vs command-aware external-proxy patterns), and the replica-TTL
caveat. New **Replication** section in `configuration.md` and a **Replication
durability** section in `guides/tuning.md` (RPO/latency trade-offs, read-scaling,
lag alerting).

### Fixed — `segment_plane_scan` missed v0.6.0's nested-Command plane framing, risking WS/MQ/temporal data loss on upgrade (task #69)

`WalWriterV3::recycle_aggressive`/`recycle_segments_before` gate deletion of
sealed WAL v3 segments on `segment_plane_scan`'s `blocks_recycle` verdict,
which — until this fix — classified a segment purely from its OUTER
record-type byte. v0.6.0's shard event-loop `wal_append` drain hardcoded
`WalRecordType::Command` as the outer wrapper for EVERY plane record
(`WorkspaceCreate`/`Drop`, `MqCreate`/`Push`/`Pop`/`Ack`/`Trigger`/`Drop`,
`TemporalUpsert`, `GraphTemporal`) — verified against the v0.6.0 tag's
`event_loop.rs` (lines 1452/2070). So on a v0.6.0→v0.7.0 upgrade, every
plane record in a pre-upgrade segment was invisible to the scan: it saw only
`Command` at the outer level and classified the whole segment as
recyclable pure-KV history. WS/MQ/temporal-upsert have no snapshot or
checkpoint format in ANY mode — their replay is WAL-only (task #43) — so
recycling one of these segments permanently destroyed that history, with no
way to recover it.

`segment_plane_scan` now peeks a `Command` record's payload for a nested v3
record frame (length-prefix + CRC32C validated, matching
`read_wal_v3_record`'s own framing) and classifies the INNER type against
the same blocking set, mirroring the unwrap
`shared_databases.rs::replay_workspace_wal`/`replay_mq_wal`/
`replay_temporal_wal` already perform via `read_wal_v3_record(&record.payload)`
at replay time — scan and replay now agree on what counts as a nested
record. Fails closed: a structurally valid nested frame with an
unrecognized inner type byte (a future plane type this build predates)
still blocks recycling. An ordinary (non-nested) `Command` payload — the
overwhelmingly common case — is unaffected and stays recyclable.

Byte-slice peek only, no allocation and no LZ4 decompression (`Command`
payloads are never LZ4-compressed by `write_wal_v3_record`) — off the hot
path (recycle passes only) but kept allocation-free per repo convention.
### Fixed — Replication fanout gate perf debt: `parking_lot` migration, FANOUT_HINT mis-activation, single-guard write path (task #70); deleted dead master-side PSYNC path (task #72)

Two pre-soak perf/hygiene defects blocking the v0.7.0 tag, fixed together
since both touch `ReplicationState` locking on the per-write hot path.

**Task #70 (4 sub-fixes):**

1. `ReplicationState`'s outer lock (`ConnectionContext::repl_state` and every
   `Arc<RwLock<ReplicationState>>` holder — 27 call sites across
   `admin/metrics_setup.rs`, `cluster/gossip.rs`, `command/server_admin.rs`,
   `main.rs`, `persistence/aof/pool.rs`, `replication/{reason_del,replica,
   master,state}.rs`, `scripting/bridge.rs`, `server/conn/*`, `shard/*`) was
   `std::sync::RwLock` while the inner `per_shard_backlogs` was already
   `parking_lot::Mutex` — an inconsistent locking policy and a source of
   `.read().unwrap()` / `if let Ok(g) = rs.read()` poisoning dances on the
   write path. Migrated the outer lock to `parking_lot::RwLock` throughout;
   removed all poisoning-related `Result` unwrapping (parking_lot's
   `read()`/`write()` return the guard directly, `try_read()`/`try_write()`
   return `Option`, not `Result`).
2. `ensure_backlogs_allocated()` (called from `try_handle_replconf` on ANY
   bare `REPLCONF`, including monitoring probes and failed handshakes) used
   to call `mark_fanout_active()` unconditionally. Since `FANOUT_HINT` is a
   sticky, never-cleared process-global flag, a single stray `REPLCONF`
   permanently taxed every subsequent write with the fanout-active gate
   check — even on a server that never completes a PSYNC. Split allocation
   from hint-activation: `ensure_backlogs_allocated()` still allocates
   backlogs (load-bearing for the handshake) but no longer touches
   `FANOUT_HINT`; `mark_fanout_active()` now fires only from
   `try_handle_psync`, on an actual PSYNC/replica registration.
3. `record_local_write` / `record_local_write_db` took up to 3-4 separate
   `repl_state.read()` acquisitions per write (SELECT-needed check, backlog
   append, offset advance, ...). Collapsed the internal chain to ONE guard
   acquired at the top of `record_local_write_db` and threaded through to
   `record_local_write` (now `fn record_local_write(g: &ReplicationState,
   shard_id, bytes)`, no longer self-locking). `replication_fanout_active`
   stays a separate, short-lived gate lock — deliberately NOT folded into the
   same guard, because some external call sites (`handler_monoio/mod.rs`'s
   MOVE/COPY handling) reach an `.await` between the gate check and the next
   repl-state touch, and Rust drops lock guards at end of lexical scope, not
   at last use — holding one guard across that span would violate the
   never-lock-across-`.await` rule. Net effect: at most 2 lock acquisitions
   per replicated write (1 gate + 1 write guard), down from up to 4-5.
4. Added `#[inline]` to `record_local_write`, `record_local_write_db`, and
   `replication_fanout_active`, matching the sibling `try_handle_*`
   convention in `dispatch.rs`.
5. **Correctness follow-up (review-caught regression on #2):** splitting
   backlog allocation from hint activation opened a REPLCONF→PSYNC window
   where a bare `REPLCONF` could allocate + seed a shard's
   `ReplicationBacklog` at its offset at that instant, then — with
   `FANOUT_HINT` still false — every subsequent local write advanced the
   shard's real offset counter via the bare-`issue_lsn` branch with NO
   matching backlog append. The backlog's `end_offset` silently skewed stale
   relative to the real counter for as long as the window stayed open (an
   AOF-enabled master under continuous write load with periodic replica
   kill-9/reconnect — exactly the v0.7.0 24h soak's shape). Any
   snapshot/cut/push-offset captured from that backlog after activation
   would then be range-inconsistent with it, corrupting partial-resync
   catch-up reads (wrong bytes or a silently-skipped catch-up) and
   acked-write accounting. Fixed by adding
   `ReplicationBacklog::realign_to(offset)` (resets the buffer to empty,
   reseeded at `offset` — a skewed buffer's contents are already useless, so
   dropping them is correct; an offset request that falls below the new
   `start_offset` fails safe into a full resync) and
   `ReplicationState::realign_backlog(shard_id)`, called at every activation
   site — `try_handle_psync` (dispatch.rs, right after `mark_fanout_active`)
   and the `RegisterReplica`/`PrepareReplicaSync` arms
   (`shard/spsc_handler.rs`) — BEFORE any snapshot/cut/push offset is
   captured. All three sites run on the shard thread that owns that shard's
   own offset advances, so the offset read + realign is race-free with the
   shard's own append/advance sequence.

**Task #72:** `handle_psync_on_master` (both `runtime-tokio` and
`runtime-monoio` variants) and `register_replica_with_shards` in
`src/replication/master.rs` were unreachable — every PSYNC handler actually
wired from `shard/conn_accept.rs` goes through
`handle_psync_inline_single_shard` / `handle_psync_inline_multi_shard`
instead. The dead pair also carried a latent broken WAIT implementation: it
initialized `ack_offsets` but never spawned the `ack_read_loop` that drains
replica ACKs into them, so `wait_for_replicas` against a registration made
through that path would have blocked forever. Deleted both variants plus
`evaluate_psync_shared` (only reachable from the deleted code); kept
`backlog_bytes_from`, still used by the live `send_backlog_range`.

Two new unit tests (`replication::state::tests`) cover the FANOUT_HINT fix
with delta-based assertions (robust against shared-binary test-order
contamination): `test_ensure_backlogs_allocated_does_not_activate_fanout_hint`
(RED on the pre-fix code — verified by temporarily reintroducing the bare
`mark_fanout_active()` call and observing the assertion fail) and
`test_mark_fanout_active_sets_hint`. Four more cover the backlog-realign
correctness fix (#70.5): `test_realign_to_resets_skewed_backlog` and
`test_realign_to_noop_when_already_aligned` (`replication::backlog::tests`,
the low-level buffer reset behavior) plus
`test_realign_backlog_fixes_skew_after_hint_false_window` and
`test_realign_backlog_then_append_reads_back_exact_record`
(`replication::state::tests`, reproducing the exact skew scenario — allocate,
advance the offset via `issue_lsn` with no append, then activate — and
proving the post-fix backlog is byte-position-consistent with the real
offset again). RED verified by temporarily neutering `realign_backlog`'s
body and observing both `state::tests` cases fail (`end_offset` stuck at 0
instead of matching the real offset 300; the post-append record unreadable
at its own pre-append offset), then reverting.

No wire-protocol or observable behavior change beyond the hint gating: the
REPLCONF → PSYNC handshake sequence is unchanged and covered end-to-end by
the `replication_hardening` / `replication_multishard` / `replication_planes`
integration suites (all green, monoio — master-side PSYNC is monoio-only by
pre-existing design, `try_handle_psync_unsupported` under tokio, untouched
by this change) plus the full `replication::` unit-test module under both
`runtime-monoio` (default) and `runtime-tokio,jemalloc`.

### Fixed — `crash_recovery_disk_offload_no_aof` harness assumed eviction-throughput durability the write path no longer provides (task #44)

`cold_keys_recover_after_crash_without_aof` forced LRU eviction with a
16,000-key pipelined `SET` burst and asserted `post >= 65% of PROBE_COUNT`,
assuming most evicted probes land durably in a `heap-*.mpf` file. That
predates `disk_offload_spill_inert()` / PR #273 (policy-aware eviction
fail-close): under `--appendonly no`, the per-connection write-path eviction
gate (`run_write_eviction_gate`, `src/server/conn/handler_monoio/mod.rs`) —
the path every ordinary `SET`/`HSET` past `maxmemory` actually takes — has no
`ShardManifest` handle and PLAIN-DROPS victims (documented Redis "pure cache,
no durability" semantics; see `docs/PRODUCTION-CONTRACT.md`'s CRASH-02 note
on task #57, which made this drop-not-OOM behavior the intended fix). Only
the periodic memory-pressure tick (`shard/persistence_tick.rs`) durably
spills under `appendonly no`, and a busy connection's synchronous per-write
eviction always wins the race before that tick observes a sustained
over-budget shard — so the pipelined-`SET` burst durably spilled 0-1/200
probes (ground-truth-verified via direct manifest/`heap-*.mpf` inspection),
making the 65% floor permanently unreachable. Not a data-loss regression:
the dropped keys were never claimed durable (the boot-time WARN says so
explicitly for this exact config).

Two changes, no `src/` change: (1) `write_filler` now sends the filler as
ONE `MSET` instead of 16,000 pipelined `SET`s — `MSET`'s handler applies all
pairs with no per-key eviction check, and the write-path gate that does run
checks memory once, pre-`MSET`, so a shard can end up far over budget with no
further write pending to re-trigger the plain-drop path; the periodic tick is
then the only thing left to reclaim it, durably spilling the LRU-oldest keys
(the probes) via the manifest. (2) The fixed `RECOVERY_FLOOR` is replaced by
`count_durable_probe_entries`, which reads each shard's manifest + `Active`
`KvLeaf` DataFiles directly (the same way `recover_shard_v3_pitr` does at
boot) right before the kill to establish ground truth, and asserts recovery
returns AT LEAST that many probes — the actual #22 regression signal,
decoupled from eviction throughput. Verified the rewritten test still
red-flags a reintroduced #22 regression (manually reverted the
`persistence_dir.is_some() || disk_offload_base.is_some()` gate in
`main.rs`, confirmed RED, reverted back). Green 10×+ consecutive on macOS
(monoio + tokio runtimes); `crash_recovery_cold_del_resurrection` and the
`crash_matrix_cross_plane` no-durability-contract spot check show no
regression.

### Fixed — `crash_recovery_graph_durability` g1/g2/g3 harness polled the deleted WAL v2 flat file (task #31)

The G1–G3 legacy-mode graph crash tests never actually ran their kill -9
scenario: `wait_for_wal_bytes` (the "my last write reached the WAL fd"
probe that makes the crash point deterministic) and G3's replay-idempotency
length check still read the flat `<dir>/shard-0.wal` WAL v2 file, which
was removed in PR #236 — one day before this test file was added (#237).
The probe waited 20s for a file that is never created and panicked, on
every platform (verified identical on the Linux dev VM — not a macOS
durability gap). Masked because the suite is `#[ignore]`d. Both helpers
now scan the real `<dir>/shard-0/wal-v3/*.wal` segment directory; the G3
length check excludes each segment's fixed 64-byte header so normal
per-boot segment rotation isn't misread as replay double-append. With the
task #60 replay fix already on main, g1–g5 are green 3× consecutively on
macOS. The underlying `replay_graph_wal` RESP-parse bug these tests expose
was fixed separately in PR #322.
### Changed — PRODUCTION-CONTRACT.md reconciled to post-replication-GA main (v0.7.0 prep)

2026-07-14 owner reconciliation of the GA Exit Ledger against the tree: re-ticked
`FUZZ-01` (12th fuzz target restored), `ACL-REG-01` (PR #258), `COLD-TTL-01`
(`ColdIndex::sweep_expired` + reclaim counters), `SEC-07` (release-agnostic
supported-versions policy); `FT-PARITY-01` updated (FT.AGGREGATE shipped,
FT.ALTER open). `CRASH-01` gained honesty caveats (g1–g3 harness never ran
before PRs #322/#324; `crash_recovery_disk_offload_no_aof` residual red,
task #44). New rows for shipped-but-untracked guarantees: `CRASH-02`
(37-cell cross-plane kill-9 matrix), `MEM-10X-01` (10× RAM G2 acceptance),
`REPL-PLANES-01` (all-plane replication); new `REPL-SOAK-01` row gates the
v0.7.0 tag on the 24h replication soak. GA-blocking gap now 17 rows.
### Added — supply-chain security CI gate: `cargo audit` + `cargo deny check` (task #63, SUPPLY-01)

`deny.toml` existed in the tree but was never wired into CI (its own header
comment said so). Added `.github/workflows/supply-chain.yml`: two
`ubuntu-latest` jobs, `audit` (`cargo audit`, blocking on RUSTSEC
vulnerability-class advisories) and `deny` (`cargo deny check advisories
licenses bans sources`, blocking on any deny.toml violation), triggered on
PRs touching `Cargo.toml`/`Cargo.lock`/`deny.toml`/`.cargo/audit.toml`,
push to `main`, and a weekly schedule (advisories publish independent of
code changes). Runs on the hosted runner, not the self-hosted `moon-dev`
box — no build is required, just dependency-graph inspection.

Fixed three real advisories to get to green: `memmap2` 0.9.10 → 0.9.11
(RUSTSEC-2026-0186, unsound pointer-offset validation),
`crossbeam-epoch` 0.9.18 → 0.9.20 (RUSTSEC-2026-0204, invalid pointer
dereference in `Display`), `spin` 0.9.8 → 0.9.9 (yanked), and dropped the
`core2`/`proc-macro-error2` unmaintained transitives by bumping
`rust-embed` 8.11.0 → 8.12.0 (console feature). Three unmaintained
transitive advisories with no available safe upgrade
(`fxhash` via monoio, `paste` via tikv-jemalloc-ctl, `rustls-pemfile`
pending a `rustls-pki-types::PemObject` migration) are explicitly
ignore-listed with reasons in `deny.toml` / `.cargo/audit.toml` rather
than left to silently pass — an always-red gate is worse than none, but a
silently-permissive one is worse still.
### Added — 24h replication kill-9 soak harness gating the v0.7.0 release tag (task #61)

`scripts/soak-replication-24h.sh` + `scripts/soak_replication_driver.py`: a
machine-verifiable zero-data-loss soak for the "Replication GA for
multi-shard masters" headline. A master (`--shards 4 --appendonly yes
--appendfsync always`) and replica (`--shards 2`) run under continuous
`SET soak:{seq} {seq}:{ts}` + `WAIT 1 <timeout>` load; only a `WAIT>=1`
reply appends the seq to an fsync'd acked-write ledger (a `WAIT` timeout is
recorded separately as "in-flight" — allowed to be lost or present, never a
failure). Every ~12 minutes the harness alternates `kill -9` on
master/replica, restarts the killed side, waits for a *data-driven* resync
gate (polls the last few acked seqs back from the replica — `INFO
replication`'s `master_link_status:up` only proves the TCP link is back, not
that the backlog/RDB replay has landed), then samples ≥1000 random + the
last 200 acked seqs and asserts exact value parity on **both** master and
replica. Any mismatch prints `SOAK-FAIL seq=<n> side=<m|r> cycle=<k>` and
exits 1 immediately; a full-ledger sweep runs at soak end. Hourly progress:
`SOAK-OK hour=<h> acked=<n> cycles=<k> master_kills=<a> replica_kills=<b>`.
`--smoke` runs a 30-minute validation (3 chaos cycles) instead of the full
24h. Builds from a VM-local clone (`~/moon-soak/repo`) per the OrbStack
diskfull-guard/tmpfs rules; kill/restart is strictly PID-targeted `kill -9`
(never a broad `pkill` pattern) per the SO_REUSEPORT hang trap.
### Fixed — `WAIT` wedged forever on a restarted multi-shard master after kill-9 (task #67)

After a multi-shard master (`--shards >= 2`) was `kill -9`'d and restarted
with prior write history, the surviving replica kept streaming and applying
writes correctly, but `WAIT 1 <timeout>` on the restarted master timed out
indefinitely — the replica's periodic `REPLCONF ACK` never registered as
"caught up" even though it was arriving every second. Root cause: AOF
recovery's `ReplicationState::seed_master_offset` seeded ONLY the
process-wide `master_repl_offset` (`total_offset()`) from the recovered
max LSN, leaving every per-shard `shard_offsets[i]` at the fresh-boot 0.
`handle_psync_inline_multi_shard`'s full-resync handshake advertises
`Σ shard_offset(i)` — not `total_offset()` — as a reconnecting replica's new
baseline (each shard captures its own offset atomically with its RDB body,
which the exactly-once live-fanout `cut` gate depends on), so a replica
reconnecting after the restart adopted a near-zero baseline while
`wait_for_replicas` kept comparing ACKs against the correctly-seeded (large)
`total_offset()` — a gap the replica could never close. `seed_master_offset`
now also seeds shard 0 to the same recovered value, restoring the
`Σ shard_offsets == total_offset()` invariant every write already
maintains going forward. Reproduced 2x by the v0.7.0 replication soak
(`scripts/soak-replication-24h.sh`); regression test:
`tests/replication_hardening.rs::master_kill_restart_wait_acks`.

### Fixed — legacy-mode (`--disk-offload disable`) graph WAL replay silently dropped the entire graph plane on kill-9 restart (task #60)

`replay_graph_wal` (`src/shard/shared_databases.rs`, the legacy-mode-only
replay path taken when `persistence_dir` is set but disk-offload is
disabled) passed the raw RESP-encoded `WalRecord::payload` directly as the
`cmd` argument to `CommandReplayEngine::replay_command`, instead of parsing
it into frames and passing the bare command name + `&[Frame]` args as the
sibling `replay_graph_wal_v3` (disk-offload-enabled path) and
`recovery.rs`'s KV `Command` replay both already did.
`GraphReplayCollector::is_graph_command` compares against literal names
like `b"GRAPH.CREATE"`, so it never matched the multi-line RESP blob —
`graph_command_count()` stayed 0, the `replay_graph_commands` guard never
fired, and every graph mutation was silently lost on restart. Regression
from PR #236 (WAL v2 → v3 port). Un-gates 6 previously-RED crash-matrix
cells (`tests/crash_matrix_cross_plane/{tests_legacy.rs,tests_spot.rs}`):
legacy s1 `graph_isolated`, `txn_isolated_committed`,
`txn_isolated_atomicity`, `mixed_all_planes_synced`,
`mixed_all_planes_mid_pass_c`, and spot-check
`cross_plane_spot_legacy_s4_graph_isolated`.
### Fixed — async-park cold GET off the shard event loop, no timeout (task #59)

`Database::get()`'s cold-tier fallback (`promote_cold_if_present` ->
`cold_read::read_cold_entry`) does a blocking `pread` inline on the
single-threaded shard event loop; under spill/AOF write backlog on the same
disk that read can block for up to ~1.9s, stalling every connection on the
shard for the duration. The monoio connection handler's GET fast path
(`src/server/conn/handler_monoio/mod.rs`) now peeks whether the key needs a
disk read and, if so, `.await`s the real result via a new off-thread worker
pool (`storage::tiered::cold_read_pool::read_cold_entry_async`, 2 threads by
default, `MOON_COLD_READ_POOL_THREADS` override) — **no timeout**: the
awaiting connection task yields, letting sibling connections on the same
shard thread keep running while the real disk I/O completes, and the
resolved outcome (never a placeholder) is promoted into hot RAM
(`Database::promote_cold_outcome`) before the normal synchronous dispatch
path answers. An earlier revision of this fix used a bounded/timeout pool
that could answer `Miss` for an existing spilled key under backlog — a
silent correctness regression — and has been fully removed; there is no
timeout anywhere in the new design. A second review round caught a
DEL-during-await TOCTOU: a `DEL`/`FLUSHDB` on the same key running on the
same shard thread while the GET's `.await` was suspended could resurrect
the deleted key once the stale `Hit` outcome resolved. Fixed by
revalidating, synchronously and without any intervening `.await`, that the
cold index still maps the key to the SAME `ColdLocation` (now
`PartialEq`/`Eq`) the outcome was read from before promoting anything —
otherwise the outcome is discarded and the caller's normal dispatch answers
from current state. MULTI/EXEC bodies, Lua `redis.call`, and every non-GET
command (MGET/HGET/etc.) are architecturally unable to `.await` from their
current synchronous call sites and continue to use the original,
unmodified synchronous blocking read — slower under backlog, but never
wrong. A third review round caught that the round-1/round-2 fix, though
correct in isolation, was landed on a branch plain `GET key` never actually
reaches: `src/server/conn/blocking.rs`'s synchronous inline fast path
(`try_inline_dispatch`, which handles plain GET UNCONDITIONALLY — unlike
SET, GET inlining isn't gated by disk-offload) still called the blocking
`read_cold_entry_at` directly on a cold miss, before the async pre-warm
hook was ever reached — so `redis-benchmark GET` and the task's own repro
were completely unaffected by rounds 1-2. Fixed: the inline GET path now
distinguishes "genuinely absent" (still answered inline, fast `$-1`) from
"a real cold entry exists" (declines to inline — returns the pre-existing
"not inlined" sentinel with the command bytes unconsumed, exactly like
every other early-return in that function) and lets the already-correct
fall-through hand it to the async pre-warm hook instead. See
`tmp/task59-design.md` for the full call-site audit and the scope this pass
covers vs. defers.

### Added — allocator-overhead and PageCache observability in INFO memory / Prometheus (task #58)

`INFO memory` and `/metrics` previously had no way to see the gap between
process RSS and what Moon could account for (DashTable+entries, vector/text/
graph/lua planes, replication backlog) — the only place that gap was ever
computed was `MEMORY DOCTOR`'s on-demand snapshot. Similarly, the disk-offload
`PageCache`'s resident 4KB/64KB frame buffers were tracked internally but
never surfaced anywhere.

Added `allocator_overhead_bytes` (= `RSS - tracked_sum`, now including
PageCache and the replication backlog) as a continuously-updated metric,
sampled once per 100ms tick by shard 0 (`persistence_tick::run_eviction_tick`)
rather than recomputed per-request, and published through a single atomic
(`admin::metrics_setup::{update,get}_allocator_overhead_bytes`) that both
`INFO memory` and the Prometheus `moon_memory_bytes{kind="allocator_overhead"}`
gauge now read — the 15s Prometheus updater no longer independently
recomputes it against a separately-timed RSS read, closing a drift source.
Added `pagecache_bytes` (`PageCache::resident_buffer_bytes()`, published into
a new `ShardStoreMemory.pagecache` atomic alongside vector/text/graph/lua on
the same tick) as its own INFO/Prometheus line
(`moon_memory_bytes{kind="pagecache"}`, `memory_prometheus_kinds` test now
expects 10 kinds). Both figures are observability-only — neither feeds
eviction or elastic-budget gating. See `src/shard/shared_databases.rs`
(`ShardStoreMemory.pagecache`), `src/shard/persistence_tick.rs`
(`compute_allocator_overhead`, publish sites), `src/admin/metrics_setup.rs`,
`src/command/connection.rs` (`INFO memory`). New test:
`tests/info_memory_allocator_pagecache.rs`.

### Fixed — startup blocked ~153s on the crash-orphan heap-file sweep at scale (task #55)

`recover_shard_v3_pitr` called `kv_spill::sweep_orphan_heap_files` synchronously
— classify AND delete — for every shard on the main thread, before any
listener/event loop started. At production scale (G2 crash-matrix bench:
~236K spilled files) this stalled restart-to-first-served-command by ~153s
(~40s/shard × 4 shards), even though the manifests themselves recovered in
~4s: the deletion I/O, not the manifest rebuild, was the bottleneck.

Split the sweep into a cheap synchronous classify (`read_dir` + `HashSet`
membership, no `remove_file` syscalls — safe to run during recovery because
it observes the exact manifest state recovery just rebuilt, before the shard
serves a single command) and a deferred delete
(`kv_spill::remove_orphan_heap_file`) that now runs on a plain `std::thread`
spawned once the shard's own event loop starts, fully decoupled from the
accept path. Orphan files are by definition unreferenced by the manifest or
any in-memory index, so reclaiming them needs no shard-state synchronization,
and the file-id namespace is monotonic — nothing a shard writes after this
snapshot can retroactively register one of the already-classified paths, so
no epoch fence beyond "classify before serving" is required. Restart-to-ready
is now seconds-scale regardless of cold-plane size, and the orphans are still
fully reclaimed shortly after. See `src/persistence/recovery.rs`,
`src/storage/tiered/kv_spill.rs` (`classify_orphan_heap_files` /
`remove_orphan_heap_file`), `src/shard/mod.rs` (`pending_heap_orphans`
staging, mirroring the existing `recovered_warm_segments` pattern), and
`src/shard/event_loop.rs`. New test:
`tests/crash_recovery_orphan_sweep_readiness.rs`.

### Fixed — read-only replicas silently executed writing EVAL/EVALSHA (task #38)

A client-issued `EVAL`/`EVALSHA` on a read-only replica ran unconditionally:
`EVAL`/`EVALSHA` deliberately carry no `WRITE` command-metadata flag (a
script's writes replicate as individually-emitted effect records, not as the
literal `EVAL`), so the connection-layer `try_enforce_readonly` gate that
blocks every other write command on a replica never saw them — a script's
`redis.call('SET', ...)` mutated the replica's local state with no
rejection at all, silently diverging it from the master.

Fixed at the actual write boundary — `scripting::bridge::make_redis_call_fn`
(the `redis.call`/`redis.pcall` bridge) now rejects a `WRITE`-flagged inner
command with `-READONLY You can't write against a read only replica.` the
moment the shard's `ReplicationState` reports the replica role, mirroring
upstream Redis semantics: a script that never writes still runs to
completion on a replica (`EVAL_RO`/`FCALL_RO` and plain read scripts are
unaffected), while one that does write aborts at the first offending call.
Reuses the same `WS`/`MQ`/`GRAPH.QUERY` read-only-subcommand carve-outs as
the connection-level gate, and reads a lock-free `AtomicBool` mirror of
`ReplicationState::is_replica_mirror` (same pattern as
`ConnectionContext::is_replica_mirror`) so a tight script write-loop never
takes the `ReplicationState` lock.

Verified this does not regress master→replica Lua effect replication (Wave
A part 2, task #34): replayed effects apply via
`replication::apply::apply_local`, which dispatches the inner write command
directly against storage and never touches the Lua bridge, so the new gate
cannot see — and cannot block — replica apply of a master's script effects
(`eval_effects_parity_shards1`/`_shards4`, `eval_incr_no_double_apply`,
`eval_writes_survive_restart` all still green).

Multi-db note: `redis.call('SELECT', ...)` was already rejected inside
scripts (pre-existing fail-loud guard — the `CURRENT_DB` thread-local is
pinned to one `Database` for the whole script execution), so there is no
per-script db-switching path that could race or diverge from this new
readonly check.
### Fixed — MqPop WAL/replication payload v2 carries real PEL idle metadata (task #47)

`apply_mq_pop` (boot-time WAL replay AND live replica apply — same function,
`src/shard/shared_databases.rs`) hardcoded a claimed PEL entry's
`delivery_time`/`seen_time` to `0` regardless of when the original `MQ.POP`
actually claimed it. Since `XPENDING`'s idle time is `now - delivery_time`,
a kill-9 restart or a promoted replica reported "idle since the Unix epoch"
(an astronomically large idle time) instead of the real elapsed duration —
`XPENDING`/`XPENDING ... IDLE` consumers on a recovered node saw wrong data
for every pre-crash claim.

Fixed by bumping the `MqPop` WAL record to its own v2 payload
(`MQ_POP_WAL_V1`/`MQ_POP_WAL_V2` in `src/mq/wal.rs` — `MqPop` now versions
independently of the shared `MQ_WAL_VERSION` used by every other MQ record
kind) that appends `[delivery_time_ms:u64][seen_time_ms:u64]` captured once
per POP batch (every entry one `MQ.POP` claims shares a single `now`, per
`Stream::read_group_new`). `src/shard/mq_exec.rs`'s POP handler now reads
these back out of the PEL/consumer state it just wrote instead of
discarding them. Decode is version-led and fail-closed: v1 payloads (no
trailing timing fields) still decode, filling an explicit `0` sentinel that
`apply_mq_pop` resolves to `current_time_ms()` at apply time — a strict
improvement over the old hardcoded-`0` behavior, not a behavior match to it.
Any version byte other than 1 or 2 is rejected (`None`), same fail-closed
posture as every other MQ WAL decoder.

Producer (`src/shard/mq_exec.rs`), boot-time replay
(`src/shard/shared_databases.rs::apply_mq_pop`), and live replica apply
(`src/replication/apply.rs::apply_mq`) all updated; the MQ WAL fuzz target
(`fuzz/fuzz_targets/mq_wal_record.rs`) needed no changes since it fuzzes
`decode_mq_pop` generically. New coverage: codec round-trip tests for v1
(sentinel-filled) and v2 (full round-trip) in `src/mq/wal.rs`, a kill-9
integration test asserting `XPENDING` idle survives restart
(`tests/crash_recovery_mq_effects.rs::xpending_idle_metadata_survives_kill9`),
and a replica-promotion test asserting the promoted node's `XPENDING` idle
reflects the master's original claim time, not the replica's own apply time
(`tests/replication_mq.rs::promoted_replica_reports_real_pending_idle_time`).
### Fixed — unified replica poison-record policy across graph/MQ/WS/temporal planes (task #48)

Each replica-apply plane (graph WAL replay, MQ effect records, workspace
create/drop, temporal invalidate, RESP framing) previously handled a
malformed/undecodable record ad hoc — mostly `tracing::warn!` + silently
skip-and-continue, one path (`WS.CREATE.APPLY`/`WS.DROP.APPLY`) even
swallowed the failure entirely (`try_with_shard(...).is_some()` returned
`true` regardless of the inner decode outcome). A silent skip means every
subsequent record in the stream keeps applying against state that has
already diverged from the master — invisible until an operator notices
missing data.

Unified policy (`replication::apply`, see its "Unified poison-record policy"
module docs): any decode/parse failure on a live-stream record is now a
`ApplyOutcome::Poisoned` — logged loud (rate-limited to 1/sec), counted in
a new INFO counter, and propagated up so the replica connection task drops
the link and lets the existing reconnect/resync loop renegotiate PSYNC
(never silently skip, never panic). PSYNC snapshot install already
fail-closed correctly (a malformed aux blob fails the whole install); it now
also increments the same counter. Semantic apply errors on well-formed
records (e.g. `WRONGTYPE`, "entity not found") keep the existing
warn-and-continue posture — that is data-level divergence from a command
that legitimately executed differently, not stream corruption.

- New `INFO replication` field: `replication_poison_records_total`.
- `src/replication/apply.rs`: `ApplyOutcome` enum, `poison()` helper,
  `apply_graph`/`apply_mq`/`apply_ws_create`/`apply_ws_drop`/
  `apply_temporal_invalidate` now return `bool` (poisoned vs. applied)
  instead of logging-and-swallowing.
- `src/replication/replica.rs`: both tokio and monoio stream-apply loops
  match on `ApplyOutcome` and drop the connection on `Poisoned`, same as the
  existing `NoShardSlice` / `DrainResult::fatal` paths.
### Added — real SHUTDOWN [NOSAVE|SAVE] (task #27)

`SHUTDOWN` was previously a stub that always replied `ERR Errors trying to
SHUTDOWN. Check logs.` and never terminated the process. It is now handled
at the connection-handler level (like BGSAVE/ACL, alongside all three
dispatch paths: `handler_single`, `handler_sharded`, `handler_monoio`):

- Parses the optional `NOSAVE` / `SAVE` modifier (Redis parity: bare
  `SHUTDOWN` forces a save iff RDB save points are configured; `NOSAVE`
  always skips it; `SAVE` always forces it). `ABORT` is rejected (Moon's
  SHUTDOWN runs synchronously to completion, so there is never an
  in-progress shutdown); `FORCE`/`NOW` are accepted no-ops.
- A forced save that fails replies an error and the server stays up
  (single-shard: synchronous `SAVE`; sharded/monoio: the cooperative
  per-shard BGSAVE snapshot, polled with a bounded timeout).
- On success, triggers the exact same `CancellationToken`-driven graceful
  shutdown sequence already used for SIGTERM — per-shard WAL/AOF flush +
  fsync across every plane, accept-loop stop, and clean connection
  teardown — rather than reimplementing it. No reply is sent (Redis
  parity: the client observes the connection close).
- ACL category was already `admin/dangerous` (`DNG`) in the command
  registry; unchanged.

Coverage: `tests/shutdown_integration.rs` (NOSAVE prompt exit + waitpid
status, AOF durability across a SHUTDOWN→restart round trip, a failed
forced SAVE keeps the server up, syntax errors keep the server up) plus a
cross-shard durability smoke section in `scripts/test-consistency.sh`.

### Fixed — AOF writer manifest-wait/cancellation data-loss race (found via task #27)

Writing the SHUTDOWN durability test surfaced a real, pre-existing bug: on
first boot, main.rs's recovery creates the AOF manifest on a separate
thread/task from the one that starts accepting connections, so a client can
already be writing (queuing `Append` messages) while the AOF writer thread
is still polling for that manifest to appear (`src/persistence/aof/writer_task.rs`,
three writer-loop variants). That polling loop checked
`cancel.is_cancelled()` *before* attempting `AofManifest::load` each
iteration — a graceful shutdown landing in the same ~50ms poll window made
the writer return immediately without ever loading the (by-then-current)
manifest, silently discarding every already-queued `Append`. Reproduced
reliably (>80% of runs) by sending `SHUTDOWN` within tens of milliseconds of
boot: `AOF writer: cancelled while waiting for manifest` in the log, and a
0-byte incr AOF file. Fixed by trying the load first in all three affected
loops (TopLevel-monoio, PerShard-tokio, PerShard-monoio); a manifest that
exists by the time the writer gets there is never missed just because a
shutdown signal happened to land in the same instant. In practice this
window was rarely hit via SIGTERM (real signal delivery has enough latency
to clear it), which is presumably why it went unnoticed until a command
that can call `cancel()` synchronously, milliseconds after the connection
that issued the write, existed.
### Changed — CI pipeline optimization (round 2: cargo-nextest)

- CI test steps (Linux/macOS/Windows) now run under `cargo nextest run
  --profile ci`: per-test-binary parallelism and two automatic retries for
  the known flaky classes (fixed-port listeners, kill-9 timing under
  full-suite load) — a pass-on-retry is reported as FLAKY, keeping the
  signal while ending manual reroll round-trips. `fail-fast = false`
  surfaces every failure in one run. Doctests keep a dedicated
  `cargo test --doc` step (nextest does not run them). Config in
  `.config/nextest.toml`; local `cargo test` is unaffected.

### Changed — CI pipeline optimization (round 1)

- CodeQL no longer runs on every PR (main-push + weekly schedule only) — it
  was a full instrumented 20-40 min build and the slowest job on every PR,
  while serving as audit tooling rather than a per-PR review gate.
- `Swatinem/rust-cache` `shared-key`s no longer embed the `Cargo.lock` hash:
  the action already invalidates changed dependencies via its own
  lockfile-aware restore, and hashing the lock into the key cold-started
  every job's cache on each dependency bump.
- CI builds/tests run with `debug = 0` (no debuginfo) via
  `CARGO_PROFILE_*_DEBUG` env — smaller artifacts (faster cache
  save/restore) and faster linking; local builds unaffected.
### Fixed — FTS term-dict + FST sidecar durability, ends restart-rescan-only recovery (kernel M4, task #50)

The full-text-search inverted index was the last plane not kill-9-lossless:
every restart rebuilt every text index by rescanning the keyspace and
reassigning term ids by `DashTable` hash-iteration first-encounter order —
not reproducible across restarts, which is why the pre-existing `.fst`
sidecar write path had its load path (`load_fst_sidecars`) deliberately left
uncalled in production (wiring it once corrupted FUZZY/PREFIX results: the
sidecar's baked-in term ids silently collided with a freshly-rescanned
dictionary's differently-assigned ids).

Fixed by persisting the term dictionary itself alongside the FST, in one
atomic sidecar (`{shard_dir}/{index}.tfst`, magic `TFS2`, version-stamped,
`atomic_write_durable`): per TEXT field, `next_id`, `fst_high_water_mark`,
every `(term, id)` pair, and the optional FST bytes. `TermDictionary::from_pairs`
reconstructs a dictionary whose ids are taken verbatim from the sidecar
(never reassigned) and whose `next_id` continues the persisted high-water
mark. Wired into shard boot (`src/shard/event_loop.rs`) so
`TextStore::load_term_fst_sidecars` runs AFTER text index schemas are
restored but BEFORE the keyspace auto-reindex rescan — seeding the term
dicts first makes the rescan's `get_or_insert` calls resolve known terms to
their persisted ids and assign fresh, non-colliding ids only to genuinely
new terms, which is what makes loading the FST alongside it safe (FST ids
and live dict ids are the same id-space by construction). Fails closed per
index on any missing/truncated/corrupt/version-mismatched/field-count-
mismatched sidecar — falls back to today's full rescan, never partially
applies a sidecar. `FT.COMPACT` now calls the combined saver
(`save_term_fst_sidecar_for_index`) instead of the old FST-only one. New
`FT.INFO` counters `sidecar_recovered_indexes` / `text_indexes_total`
(additive across shards) surface fast-boot coverage. New default-GREEN
crash-matrix cells `cross_plane_prod_{s1,s4}_text_fts_sidecar_isolated`
verify a FUZZY query survives kill-9 identically to a from-scratch rebuild.
New fuzz target `term_fst_sidecar` covers the sidecar decoder.
### Security — clear dependency vulnerability backlog (task #51)

`sdk/python` (uv.lock, 36 open Dependabot alerts incl. 1 CRITICAL) and
`console` (pnpm-lock.yaml, 17 open alerts incl. 2 HIGH react-router):

- **sdk/python**: bumped `requires-python` floor `>=3.9` → `>=3.10` (Python
  3.9 reached EOL 2025-10). This was required, not cosmetic — `nltk` (pulled
  transitively via the `llama-index` extra) has no python-3.9-compatible
  release past 3.9.2, which carries the CRITICAL zip-slip advisory
  (GHSA, `nltk.corpus.util.LazyCorpusLoader` zip extraction) plus 4 more
  high/medium path-traversal and XSS CVEs. Collapsing the py3.9 resolution
  branch lets `uv lock --upgrade` land on `nltk` 3.10.0 everywhere. Also
  picked up `pillow` 12.3.0, `aiohttp` 3.14.1, `urllib3` 2.7.0, `requests`
  2.34.2, `orjson` 3.11.9, `langsmith` 0.10.2, `langchain-core` 1.4.9,
  `pytest` 9.1.1 (all natural resolutions within existing `>=` floors — no
  manifest ceiling changes needed beyond the python floor). 263/263 SDK
  tests green post-bump.
- **console**: targeted `pnpm update` (no `package.json` range changes
  needed — all fixes were already inside existing `^`/transitive ranges)
  for `react-router-dom` 7.14.0→7.18.1 (clears the react-router vendored
  turbo-stream RCE + 3 more), `dompurify` 3.4.0→3.4.12 (transitive via
  `@cosmos.gl/graph`, clears 8 XSS/pollution advisories), `vite`
  7.3.5→7.3.6 (needed first — 7.3.5 hard-pins `esbuild@^0.27.0`, below the
  fixed 0.28.1), `esbuild` 0.28.1, `form-data` 4.0.6, `ws` 8.21.0,
  `js-yaml` 4.3.0, `@babel/core` 7.29.7. `pnpm audit --audit-level high`
  clean. `pnpm build` and `pnpm install --frozen-lockfile` verified; the pre-existing
  `console.test.ts` failures (7/56 tests — zustand persist middleware vs.
  mocked storage) are unrelated to this change, reproduced identically on
  the pre-bump lockfile, left untouched.

### Fixed — cross-shard MULTI/EXEC graph-leg misrouting (kernel M3 stage 3 / task #52, review round 3, P1)

CodeRabbit finding on PR #300: `analyze_txn_locality` (the pre-EXEC classifier
that decides whether a queued body runs locally, hops to a remote owner
shard, or is rejected `CROSSSLOT`) only scans KV keys via `command_keys`.
`GRAPH.*` commands declare NO keys in command metadata
(`first_key==last_key==0`), so a graph-bearing body was misclassified —
`Keyless` for a graph-only body, or by its KV keys alone for a mixed body —
completely ignoring the graph name's REAL owner shard (the standalone
`GRAPH.*` path routes by `graph_to_shard(name, num_shards)`, identical
hash-tag-aware xxh64 to `key_to_shard`). At `num_shards > 1` a transaction
whose true graph owner differed from the classified owner would durably
apply the `GRAPH.*` write to the WRONG shard's `graph_store` — invisible to
subsequent normally-routed single-command reads. The task #52 crash cells
never caught this because their KV key and graph name deliberately share one
`{txniso}` hash tag (co-location by construction), so they always agree on
owner regardless of the bug.

Fixed by folding the graph name into `analyze_txn_locality`'s existing
`visit` closure (the same one KV keys and the SORT/GEORADIUS `STORE`-dest
already use): a body whose KV keys and graph name disagree on owner becomes
`CrossShard` (rejected `CROSSSLOT`, same posture as any other genuine
cross-shard body); a graph-only body becomes `SingleShard(graph_owner)`,
which routes through the SAME whole-body-atomic owner hop
(`execute_txn_on_owner` / `ShardMessage::TxnExecute`) a KV-only remote-owner
body already uses — no new hop mechanism, so the "body runs on ONE local
shard slice" invariant is preserved.

New tests in `tests/sharded_multi_exec_locality.rs`:
`graph_leg_cross_shard_rejected` (a KV key and graph name deterministically
computed, not guessed, to hash to different shards at shards=4 → `CROSSSLOT`,
neither leg applied) and `graph_only_txn_routes_to_true_owner` (a graph-only
body commits and is visible via a fresh, normally-routed connection —
proving owner placement, not "whatever shard the connection landed on").
Confirmed RED without the `analyze_txn_locality` fix (both new tests fail:
the rejected-body test instead applies the KV leg and errors "graph not
found" on the misrouted `GRAPH.ADDNODE`; the owner-routing test reads back 0
rows), GREEN with it, 3× consecutive. `no_silent_divergence_across_shards` /
`multi_shard_span_rejected` / `single_shard_unaffected` (pre-existing KV-only
locality tests) and the task #52 crash-matrix txn cells are unaffected.

### Fixed — cross-store MULTI/EXEC graph leg replication (kernel M3 stage 3 / task #52, review round 2, P1)

Follow-up to the durability fix below: `execute_transaction_sharded`'s new
`GRAPH.*` branch originally `wal_append`ed the graph-leg records itself,
*inside* the function — but that function has no `ConnectionContext` /
replication access, so the graph leg applied and persisted on the master
but never reached a replica, a NEW master/replica divergence the durability
fix introduced (pre-fix the in-txn `GRAPH.*` applied nowhere, so there was
no divergence to have). `execute_transaction_sharded` now returns the
collected graph records (bound to the db each command executed in, same
per-entry-db rule as the AOF entries — a queued `SELECT` redirects later
commands) instead of appending them itself. The monoio EXEC caller
(`handler_monoio/write.rs`) now replicates each record via
`record_local_write_db` — gated `ctx.num_shards == 1 &&
replication_fanout_active(ctx)`, matching the live single-command graph
path's scope exactly (graph replication is single-shard only; multi-shard
graph replication rides the R2 broadcast redesign) — in the same
synchronous stretch as the mutation, THEN `wal_append`s, same
replicate-then-append order as the live path. The tokio/sharded caller and
the cross-shard `TxnExecute` shard-message arm (`src/shard/spsc_handler.rs`)
only `wal_append` (no replication plane in scope there; the `TxnExecute` hop
only fires at `num_shards > 1`, where graph replication is out of scope by
design regardless). New regression test
`replica_syncs_multi_exec_graph_leg` (`tests/replication_graph.rs`):
shards=1 master+replica, `MULTI`/`SET`+`GRAPH.ADDNODE`/`EXEC` on the master,
asserts both legs land on the replica over one live stream (no
reconnect/full-resync masking) — confirmed RED (graph leg missing) with
just the new replication call disabled, GREEN 3× consecutive with the fix.

### Fixed — cross-store MULTI/EXEC graph leg durability (kernel M3 stage 3 / task #52)

A committed cross-store transaction (`MULTI`; `SET k v`; `GRAPH.ADDNODE g
Label id 1`; `EXEC`) survived kill-9 on the KV leg (PR #247's
`persist_txn_aof`) but silently lost the graph leg — worse than a missed
durability leg, the `GRAPH.ADDNODE` never even applied in memory. Root
cause: `execute_transaction_sharded` (`src/server/conn/shared.rs`), the
executor both the sharded and monoio connection handlers use for `EXEC`,
had no `GRAPH.*` branch at all — a queued `GRAPH.ADDNODE` fell through to
the generic KV `dispatch()` table (which only knows keyspace commands) and
errored `ERR unknown command`, an error MULTI/EXEC's per-command tolerance
swallowed without surfacing it to the client. Fixed by giving the txn
executor a `GRAPH.*` branch that mirrors the single-command path
(`try_handle_graph_command`): dispatches writes/reads against
`ShardSlice::graph_store`, and flushes every command's drained wal-v3
records via `wal_append` after the whole transaction body runs (same
per-shard append-order contract as the live path). Verified 3× consecutive
GREEN at shards=1 and shards=4; the sibling atomicity claim (a transaction
queued but killed before `EXEC` applies nothing) was unaffected and stays
GREEN. Crash-matrix cells `cross_plane_prod_s1_txn_isolated_committed` /
`cross_plane_prod_s4_txn_isolated_committed` (`tests/crash_matrix_cross_plane/`)
flip from `red_guard`-gated RED to default-GREEN tripwires (42 cells: 33→35
GREEN by default, 9→7 RED).
### Fixed — MQ durable-stream tombstone: DEL/UNLINK/FLUSHALL/FLUSHDB no longer resurrect on kill-9 (kernel M3 stage 3 / task #46)

Root cause: MQ durable streams live as ordinary keys in the shard keyspace,
but `replay_mq_wal` had no way to represent "this queue was deleted after
these pushes" — a generic `DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL` removed the
stream from the live db and the `DurableQueueRegistry`, but a kill-9
afterward re-materialized the full pre-delete content on the next restart
(replay reapplies every `MqCreate`/`MqPush`/... record regardless). Same
bug class as the already-fixed KV/vector cold-plane resurrection (PR #257),
now closed for MQ. Proven by the former RED crash-matrix cell
`cross_plane_seeded_red_mq_generic_del_resurrection`
(`tests/crash_matrix_cross_plane/tests_seeded_red.rs`), now un-gated (no
more `harness::red_guard`) and green 3/3 consecutive runs.

Fix: a new `MqDrop` WAL v3 record (discriminant `0x75`,
`src/persistence/wal_v3/record.rs`; versioned encode/decode in
`src/mq/wal.rs` — `[version:u8][db_index:u32][key_len:u32][key:N]`, fails
closed on any malformed/future-version payload like every other MQ record)
is emitted whenever a durable MQ stream is removed via generic
`DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL` — new hooks
`mq_exec::auto_drop_mq_streams` / `auto_drop_mq_streams_on_flush`, wired
into every connection-layer write path that already runs the equivalent
vector/text index-parity hooks (`handler_monoio/mod.rs`,
`handler_sharded/mod.rs`, the sharded MULTI/EXEC helper in
`server/conn/shared.rs`, and the replica-side `apply_index_parity_hooks` in
`replication/apply.rs` — a replica must tombstone its OWN WAL too, or it
resurrects the stream on its own restart even though its live copy stayed
correctly deleted). Boot-time replay applies `apply_mq_drop`
(`src/shard/shared_databases.rs`) strictly in WAL order alongside every
other MQ record, so a Drop only kills records that PRECEDE it for that
key — a later `MqCreate`/`MqPush` for the same key survives intact
(create -> drop -> create round-trips a kill-9 with the second incarnation
whole; new regression tests
`test_replay_mq_wal_drop_only_kills_prior_records` and the crash-matrix
`cross_plane_mq_create_drop_create_survives`). `segment_plane_scan`'s
plane-history block set (`src/persistence/wal_v3/segment.rs`) gained
`MqDrop` alongside the other MQ discriminants so autovacuum/recycle never
deletes a sealed segment still holding an unfloored tombstone. The MQ WAL
fuzz target (`fuzz/fuzz_targets/mq_wal_record.rs`) now also fuzzes
`decode_mq_drop`.

Replication decision: MQ effect records replicate live only at
`num_shards == 1` (the pre-existing gate, matching `MqCreate`/etc — see
`mq_exec::replicate_mq_record`'s docs). `MqDrop` follows the same posture:
`MQ._REPL.DROP` is emitted and applied by `replication::apply::apply_mq`
exactly like the other MQ replay commands. Separately — and regardless of
that gate — a REPLICATED generic `DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL`
already removes the stream from the replica's live keyspace via normal
command replication; what it did NOT do before this fix is tombstone the
replica's OWN wal-v3 MQ plane, so the replica's live state was correct but
its restart-durability was not. `apply_index_parity_hooks` now closes that
gap identically on the replica side.

Scope decision (documented in code, not re-litigated per-callsite):
`DurableQueueRegistry` entries are NOT db-indexed (pre-existing limitation,
same as `MqCreate`'s registry) — a key match tombstones regardless of
which db the deleting command ran in, and `FLUSHDB` drops every registered
durable queue exactly like `FLUSHALL` (the registry has no way to scope to
one db). Two different dbs sharing an MQ queue NAME is not a supported
configuration.

Test-gotcha fixed along the way: the crash-matrix DEL/FLUSHALL scenarios'
original sync-marker strategy waited on an AOF-family write to prove the
delete was durable — correct pre-fix (DEL only touched the AOF), but the
new `MqDrop` record lands on the wal-v3 MQ plane via a separate
fire-and-forget channel drained on its own 1ms tick, so an AOF-only marker
no longer proves anything about the tombstone's own durability. Both tests
now also sync a throwaway durable queue's `MQ.PUSH` (wal-v3-family) after
the delete/flush before crashing.
### Fixed — adopt shared `atomic_write_durable` at remaining bare-write persistence sites (kernel M4 prep / task #49)

Six durable-state writers still hand-rolled their own (often incomplete)
tmp-write/rename sequence instead of the shared K3 primitive
(`src/persistence/atomic.rs`, PR #296): `acl::io::acl_save` and the ACL
SAVE command handler (`src/command/acl.rs`) had **zero** atomicity at all
(bare `fs::write` + `rename`, no fsync); CONFIG REWRITE
(`src/command/config.rs`), `cluster::migration::save_nodes_conf`
(nodes.conf), and `replication::save_replication_state` all did
write+rename with no fsync, so a kill-9 immediately after `rename()`
returns could still revert the file to stale/empty contents on
`data=ordered` ext4/xfs; `persistence::clog::write_clog_page` and
`persistence::kv_page::write_datafile`/`write_datafile_mixed` wrote
directly to the FINAL path with no temp file or rename at all (only a
trailing `sync_all`/`fsync_file`), so a crash mid-write could leave a torn
CLOG page or KV data file. The native RDB save paths
(`persistence::rdb::save`/`save_from_snapshot`,
`persistence::redis_rdb::save`, used by BGSAVE) also lacked the
parent-directory fsync step. All eight sites now route through
`atomic_write_durable` (temp file, `sync_all`, `rename`, dir-fsync); each
conversion is paired with a "no leftover temp file after a successful
write" regression test. AOF appends, WAL segments, and the legacy
`#[allow(dead_code)]` `rewrite_aof_sync` RDB-preamble path are out of
scope (different framing/fsync mechanisms, per task brief).
### Fixed — task #45: tick-path eviction now spills collections instead of plain-dropping them

Root cause of the intermittent `tests/cold_collection_visibility.rs` CI flake
(stable GHOST: `EXISTS == 1`, `LRANGE`/`ZRANGE`/etc. permanently empty — only
observed on shared/slow Linux runners, never test-induced). Two compounding
defects in `src/storage/eviction.rs`'s synchronous spill path
(`evict_one_with_spill`):

1. Collection victims (Hash/List/Set/ZSet/Stream) were gated behind a
   stale `is_string` check that predates `kv_spill::spill_to_datafile`
   gaining full collection support (it already serializes any
   `RedisValueRef` via `kv_serde`) — so a collection picked as a victim
   while a `SpillContext` WAS present still fell through to a silent
   plain-drop, indistinguishable from `spill: None`.
2. Even for strings, a successful sync spill never registered a
   `ColdIndex` entry (`spill_to_datafile` was always called with
   `cold_index: None`) — the durable `.mpf` file existed and was
   manifest-registered, but nothing could ever read it back via
   `promote_cold_if_present`.

Separately, `src/shard/timers::run_eviction` (the periodic 100ms tick,
independent of the memory-pressure cascade) *never* received a
`SpillContext` at all — every victim it picked was plain-dropped even with
`--disk-offload enable` and a durability backstop (`--appendonly yes`)
configured. `src/shard/persistence_tick.rs`'s `run_eviction_tick` now
builds a real `SpillContext` (from the shard's `ShardManifest` + shard
data dir + `next_file_id`) whenever disk-offload is enabled and a manifest
is present, and threads it through — falling back to the pre-existing
fail-close plain-drop (PR #273 policy-aware discipline: `noeviction` still
OOMs, an evicting policy still frees RAM) when no durability backstop
exists, matching the already-documented "spill is inert without one" rule.

New deterministic regression tests (no server process, no timing race):
`storage::eviction::tests::sync_spill_non_string_victim_is_durably_spilled_not_plain_dropped`
and `shard::timers::tests::test_run_eviction_spills_collection_victim_when_spill_context_given`.
`tests/cold_collection_visibility.rs` (10x local reruns, both runtimes)
remains green and unmodified — its GHOST assertions still fail the suite on
any regression.

### Added — unified per-shard floor register + min-across-planes WAL recycle (kernel M3 stage 2 / K2)

`ShardControlFile` (`src/persistence/control.rs`) gains `graph_floor_lsn`,
`ws_floor_lsn`, `mq_floor_lsn: u64` fields (control payload 57→81 bytes,
backward-compatible: fields default to `0` when reading a control file
written by an older binary, keyed off the on-disk `payload_bytes` header).
Both WAL-recycle call sites — checkpoint `Finalize`
(`src/shard/persistence_tick.rs`) and autovacuum Pass C
(`src/shard/autovacuum.rs`, wired via a new `control_file` parameter on
`run_tick`) — now recycle up to `min(kv_floor, graph_floor)` instead of the
KV-only floor, so a WAL segment is never recycled while it still holds the
only copy of an unflushed graph record. WS/MQ floors are tracked but
deliberately **excluded** from the `min()` (brief's Risk #2): both planes
have no snapshot format in any mode, so folding their sentinel `0` floor
into the minimum would permanently freeze recycling on any shard that ever
saw a WS/MQ record. Their content stays protected by the existing,
orthogonal `segment_plane_scan` content-scan (renamed from
`segment_holds_plane_history`; `GraphTemporal` removed from its blocking
match arm now that the LSN floor covers that record type, with a new
`RECL_WAL_RECYCLE_GRAPH_TEMPORAL_FREED_TOTAL` counter in the `# Reclamation`
INFO section marking segments it stops blocking).

**Task #53 root-caused and fixed in the same stage** (required by this
stage's mandate: fix in-scope if the root cause is the Finalize
floor/durability-ordering invariant K2 formalizes). The
checkpoint-`Finalize`-window graph-total-loss finding
(`tests/crash_matrix_cross_plane.rs`'s former RED cell 4) was exactly that
invariant: `save_graph_store` (`src/graph/recovery.rs`) wrote the
reference/floor (`graph_metadata.json`, via `GraphStore::save_metadata`)
BEFORE the payload it claims durable (CSR segments + `manifest.json`) — an
ARIES-inverted write order. A kill-9 between the two writes left a
fully-advanced floor pointing at a payload that never reached disk, so
recovery trusted the floor and skipped WAL replay for records it claimed
were already covered, total-losing the graph batch. Fixed by reordering
`save_graph_store` to write CSR segments + `manifest.json` first,
`store.save_metadata` last, and making `save_metadata` itself atomic
(temp+fsync+rename+dir-fsync via the shared
`persistence::atomic::atomic_write_durable` helper) so the floor write can
never itself be torn. Safe against double-replay:
`graph::replay::node_present` checks both `write_buf` and loaded CSR
segments before re-inserting a WAL-logged node/edge, so replaying a record
whose payload actually made it to disk before the kill is a no-op, not a
duplicate. Confirmed via `MOON_CRASH_MATRIX_RED=1
MOON_CRASH_MATRIX_ITERS=20` soak: 20/20 clean at both `prod_s1` and
`prod_s4` (pre-fix baseline: hit at iteration 11/20 and 7/20
respectively). `cross_plane_prod_s1_mixed_all_planes_mid_checkpoint` /
`cross_plane_prod_s4_mixed_all_planes_mid_checkpoint` now run ungated —
the crash-matrix suite's default-GREEN count moves from 29/40 to 31/40 (9
RED cells remain: task #52's cross-store TXN graph leg and the legacy-mode
graph-reconstruction/MQ-resurrection findings above, all still tracked
separately and explicitly out of scope for this stage). A subsequent
adversarial review round found a second graph-durability P0 plus a VACUUM
floor bypass on top of this fix — see the next section — which add 2 more
GREEN cells, moving the suite to 42 cells total (33 GREEN by default).

`src/persistence/recovery.rs`'s PITR path threads the three new floors
through unchanged on replay. New unit tests: control-file round-trip +
backward-compat (old-format read defaults new floors to 0), an inverted
`GraphTemporal` recycle test (segment recycles once the graph floor covers
it even though the plane-scan no longer blocks it), and a Risk #2
regression test (a pure-KV write after a WS/MQ record on the same shard
still recycles — the WS/MQ sentinel floor must never collapse recycling to
zero).

No per-write hot path touched — `ShardControlFile` is only written at
checkpoint `Finalize` and read at Pass C/recovery, both off the command
dispatch path; a VM hot-path A/B bench was judged unnecessary for this
change and not run.

### Fixed — K2 adversarial review round: graph drop-resurrection + VACUUM floor bypass (kernel M3 stage 2)

An adversarial review of the floor-register work above (SHIP-WITH-FIXES
verdict) found two P0s and a P1 in the same area before the stage could
close:

**P0 — `GRAPH.DELETE`'d graphs could resurrect across repeated
checkpoints.** `persist_graph_at_checkpoint`'s short-circuit
(`!store.is_dirty() || store.graph_count() == 0 -> return true`) skipped
`save_graph_store` whenever a delete emptied the graph map — even though
the delete itself marks the store dirty via the WAL drain. `graph_count()
== 0` and "nothing to persist" are independent conditions: an
empty-but-dirty store still needs `graph_metadata.json` rewritten to
reflect zero graphs at the new `snapshot_lsn`. With the skip in place,
metadata stayed stale forever (dirty never clears without a real save)
while every later checkpoint kept advancing `control.graph_floor_lsn` past
the WAL record holding the `DELETE` — once that segment was recycled, a
crash+restart loaded the stale metadata with nothing left in the WAL to
replay the deletion. Fixed by dropping `graph_count() == 0` from the
short-circuit; dirty alone gates it now, and `save_graph_store`'s per-graph
loop correctly no-ops on zero graphs while `store.save_metadata` still
durably rewrites the (now-empty) graph list. New regression cells
`cross_plane_prod_s1_graph_drop_survives_repeated_checkpoints` /
`_s4_...` (`tests/crash_matrix_cross_plane/scenarios.rs`), RED-first
verified against the pre-fix binary at both shard counts (two earlier
padding designs — a live second graph, then MQ pushes — were each proven
NOT to reproduce the bug before a third, using throwaway create+delete
graph cycles as padding, did).

**P0 — manual `VACUUM` bypassed the unified floor register.**
`run_vacuum_passes` (`src/command/server_admin.rs`) recycled WAL to
`w.current_lsn()` unconditionally — a client-reachable path around the
`min(kv_floor, graph_floor)` invariant K2 established for the checkpoint
and autovacuum recycle sites. Fixed by threading the control-file floors
into `VACUUM`: in checkpoint-backed (disk-offload) mode it now recycles to
`min(last_checkpoint_lsn, graph_floor_lsn)` read from the shard's
`ShardControlFile`, mirroring autovacuum Pass C; in legacy mode (no
disk-offload dir — no snapshot format to bound WS/MQ recycling against) it
now REFUSES to recycle WAL at all and increments the shared
`RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL` counter (surfaced in
`INFO`/`DEBUG RECLAMATION`'s `# Reclamation` section), the same
skip-and-warn shape Pass C already uses for the identical precondition.

**P1 — the WAL-overflow emergency recycle path** (`src/shard/
persistence_tick.rs`) used `last_checkpoint_lsn` alone; now
`.min(control.graph_floor_lsn)`, closing the same gap in the emergency
code path.

Plus two P2s: `GraphManifest::save` (`src/graph/manifest.rs`) now routes
through the shared `atomic_write_durable` helper instead of a hand-rolled
temp+fsync+rename+dir-fsync sequence (behavior-equivalent); and a stale
doc comment on `scenarios::mixed_mid_checkpoint` that still described the
Finalize-ordering bug as unfixed was corrected.

Re-gated after these fixes: the crash-matrix suite (42 cells, 33 GREEN by
default, including the 2 new regression cells), `mixed_mid_checkpoint`
`MOON_CRASH_MATRIX_RED=1 MOON_CRASH_MATRIX_ITERS=20` soak at both shard
counts (re-verified since the P0 fix touches the same `Finalize` path),
`g4`/`g5` graph durability cells, `cargo fmt --check`, and both clippy
matrices (default features; `runtime-tokio,jemalloc`).

### Added — cross-plane kill-9 crash-matrix suite (kernel M3 stage 1 / G1)

New `tests/crash_matrix_cross_plane.rs` + `tests/crash_matrix_cross_plane/`
harness: 40 kill-9 crash-recovery cells spanning every persistence plane
(KV/AOF, KV disk-offload, graph/vector/WS/MQ WAL v3, cross-store MULTI/EXEC,
mixed-plane concurrent workloads, and a checkpoint-`Finalize`-window kill)
across the `{shards=1, shards=4} x {appendonly, disk-offload}` matrix. This
is a RED-first tripwire suite — 29 cells run GREEN by default; 11 known-RED
cells are gated behind a runtime guard (`harness::red_guard`, checked via
`MOON_CRASH_MATRIX_RED=1`) rather than `#[ignore = "RED: ..."]`, because the
suite's own `--ignored` invocation convention (every cell needs a release
binary) makes an ignore-reason string alone non-functional as a gate. A
`MOON_CRASH_MATRIX_ITERS` env var (default 1) enables ad-hoc soak runs for
probabilistic findings.

A second review round caught a vacuous-precondition bug and a gating
granularity bug before this suite could be trusted as a tripwire:
`kv_spilled_isolated`'s filler (3000×512B against a 4 MiB cap) never
actually forced a cold-tier spill at either shard count, silently
degenerating to the same coverage as `kv_isolated` — fixed by mirroring
`crash_recovery_cold_del_resurrection.rs`'s proven filler ratios
(16,000×600B against 8 MiB) plus a hard `heap-*.mpf` precondition assert so
a future load-parameter regression fails loud instead of vacuously passing.
`txn_isolated` was split into `txn_isolated_committed` (still RED, task #52
below) and `txn_isolated_atomicity` (a transaction queued but killed before
`EXEC` must apply nothing) — the two halves have unrelated root causes and
sharing one `red_guard` hid a working, GREEN regression tripwire on
`prod_s1`/`prod_s4` by default; the atomicity half now runs ungated on
those two configs (still RED on `legacy_yes_s1`, same pre-existing
legacy-graph gap as everything else there).

Two real, unresolved findings surfaced and are intentionally left RED (not
fixed — that is out of scope for this stage) for the kernel M3 backlog:

- **Cross-store TXN graph leg not durable (task #52):** a committed
  `MULTI`/`EXEC` mixing `SET` + `GRAPH.ADDNODE` survives kill-9 on the KV
  leg (PR #247's `persist_txn_aof`) but the graph leg is lost. Deterministic
  repro (3x consecutive at shards=1 and shards=4): kill immediately after
  the post-`EXEC` WAL-v3 sync wait, with `--checkpoint-timeout 3600` so no
  periodic checkpoint can incidentally rescue the graph leg. See
  `scenarios::txn_isolated_committed`'s doc comment for the minimal repro.
- **Checkpoint-`Finalize` window can total-loss the graph plane (new):**
  some kill offsets in the 0-150ms window after `BGSAVE` lose the *entire*
  already-wal-v3-synced graph batch (not just an unsynced tail) while
  KV/vector/WS/MQ all survive in the same run. Probabilistic
  (~1-in-7 to 1-in-12 per iteration) — a single default run can report
  false-green even with `MOON_CRASH_MATRIX_RED=1`; reproduce reliably with
  `MOON_CRASH_MATRIX_RED=1 MOON_CRASH_MATRIX_ITERS=20`. Strong P0 candidate
  for kernel M3 stage 2 (K2). See `scenarios::mixed_mid_checkpoint`'s doc
  comment.

Also confirmed GREEN (not RED, contrary to the brief's grouping at the
design-doc level): `WS DROP` durability under kill-9 — `WorkspaceDrop` WAL
records replay correctly in order. The sibling MQ generic-`DEL`
resurrection gap (same bug class as the already-fixed KV/vector cold-plane
resurrection, PR #257) was RED at the time of this entry; **fixed in
kernel M3 stage 3 (task #46)** — see the `MqDrop` entry below.

`tests/common/mod.rs` gains `find_moon_binary()`/`sigkill()`/
`wait_for_port_down()` shared helpers this suite depends on (the latter now
panics on loop exhaustion instead of silently returning — a tripwire
codebase should not have silent-pass verification helpers). Shared helpers
added; migration of the 5 existing `crash_recovery_*.rs` suites' own local
copies to the shared versions is deferred, not done in this stage.

Test-only; no production code changed in this stage.

### Added — memory + tier accounting spine (kernel M2 stage 2 / K4)

`resident_bytes()` is now implemented by every storage plane, and the
elastic memory budget's used-term (`ShardDatabases::recompute_elastic_budget`,
GAP-1/PR #170) folds in all of them — previously kv+vector only.

- `TextStore`/`TextIndex::resident_bytes()`: posting lists, term
  dictionaries, FST fuzzy/prefix sidecars, per-document bookkeeping maps,
  and TAG/NUMERIC secondary indexes. FTS memory was hard-coded 0
  everywhere it was published (elastic budget, MEMORY DOCTOR, Prometheus)
  until this change. **Data-size-independent incremental accumulator**
  (the publish-site read sums cached per-structure totals — O(schema
  field count), bounded by `FT.CREATE` definitions, never corpus size;
  same contract as `ColdIndex`/graph below) — an initial version was an
  O(doc-count +
  vocabulary) full-recompute walk invoked unconditionally every 100ms from
  the shard eviction tick regardless of `maxmemory`, measured 6.4–21.3ms/call
  at 50K–200K docs (>20% of the tick budget, recurring P99 spikes for every
  command on that shard). Fixed before merge: `PostingStore`/
  `TermDictionary`/`TextIndex` each carry a cached total maintained
  incrementally at every mutation site (index/delete/upsert/TAG/NUMERIC
  update/FST rebuild), verified against a `#[cfg(test)]` ground-truth
  full-walk after a mixed mutation sequence.
- `ColdIndex::resident_bytes()` (KV disk-offload bookkeeping): an O(1)
  incremental accumulator (not a per-tick walk — sized for G2's "10x RAM"
  scale target), charged into the shard's published KV memory.
- Graph resident bytes (already computed) now also feed the elastic
  budget's used-term, not just the observability atomic.
- `moon_memory_bytes{kind="text"}` Prometheus gauge + `Text (FTS):` line
  in `MEMORY DOCTOR`. Also fixes a pre-existing gap where
  `moon_memory_bytes{kind="lua_scripts"}` was emitted but never primed.
- New `src/storage/tier.rs`: `ResidencyTier` (Hot/WarmReloadable/ColdStub)
  + `TierPolicy` trait skeleton — types only, no plane adoption in this
  milestone (that is M4).

No eviction policy semantics changed: this widens what the existing
donor/hot formula sees, not how it decides. Verified against
`eviction_parity`/`eviction_parity_hash_disk_offload` (shards 1 and 4,
including the disk-offload `ColdIndex` path) with no behavior change.

### Fixed — Windows CI: `replication_planes` used un-gated `libc::kill`

`tests/replication_planes.rs`'s `sigkill` helper called `libc::kill`
unconditionally — `libc` is not linked on Windows, so the `Check (Windows)`
job failed to compile the suite on every `main` push since the Wave A merge.
Now cfg-gated exactly like `tests/aof_multidb_kill9.rs` (`Child::kill` on
non-unix). Test-only.

### Fixed — graph CSR segments and text/vector sidecars were not crash-durable (K3, storage-kernel M2 stage 1)

Extracted the vector engine's Stack-B temp+fsync+rename+dir-fsync
sequence into one shared `atomic_write_durable(path, bytes)`
(`src/persistence/atomic.rs`) and adopted it at every durable-artifact
write site the K3 audit flagged as missing part of the sequence
(`storage-audit-2026-07-12-graph-fts.md`):

- **Graph CSR segments** (`CsrSegment::write_to_file`) used a bare
  `std::fs::write` — no fsync at all, and the final path was
  overwritten in place instead of replaced via rename. A crash
  mid-write left a torn segment directly at the path `GraphManifest`
  already references. Proven with a concurrent-writer regression test
  that reliably caught a torn read (`InvalidData("data shorter than
  header")`) against the pre-fix code.
- **Text sidecars** (`save_text_index_metadata`, `save_fst_sidecar`)
  and the **vector index metadata sidecar**
  (`save_index_metadata_v3`) already did temp-write + fsync + rename,
  but never fsync'd the parent directory — on `data=ordered`-journaled
  filesystems a crash between rename and dir-fsync can revert the
  directory entry to the old name even though `rename()` returned
  success.
- **FST term-dict sidecar deliberately left unwired** (and now
  documented as such on `TextStore::load_fst_sidecars`): wiring the
  load was attempted and REVERTED after adversarial review proved a
  loaded FST's baked-in term-ids are stale id-space garbage against
  the term dictionary the restart rescan rebuilds (first-encounter-
  order ids over non-reproducible hash iteration) — merging them
  silently corrupts `FT.SEARCH` FUZZY/PREFIX results. Restart fuzzy/
  prefix queries keep the slow-but-correct brute-force path; making
  the sidecar loadable requires persisting the term dictionary itself
  (task #50, kernel M4 FTS persistence).

Ref: `.planning/reviews/kernel-m2-brief-2026-07-12.md` (K3, stage 1).

### Fixed — read-only replicas accepted client-issued `WS`/`MQ`/`TEMPORAL.*` writes (Wave B readonly-enforcement, task #34 follow-up)

`WS` and `MQ` (dispatched as `WS <SUB> ...` / `MQ <SUB> ...`) and the dotted
`TEMPORAL.SNAPSHOT_AT` / `TEMPORAL.INVALIDATE` commands were entirely absent
from `command::metadata::COMMAND_META`, so `metadata::is_write` silently
returned `false` for all of them. `try_enforce_readonly`
(`handler_monoio::dispatch` / `handler_sharded::dispatch`) is driven purely
by `is_write`, so a client could issue `WS CREATE`/`WS DROP`,
`MQ CREATE`/`PUSH`/`POP`/`ACK`/`TRIGGER`/`PUBLISH`, and
`TEMPORAL.SNAPSHOT_AT`/`TEMPORAL.INVALIDATE` directly against a read-only
replica — mutating its workspace registry, message queues, or graph
temporal state (`TEMPORAL.INVALIDATE` sets `valid_to`) with no path back to
the master. A silent divergence source, same class of bug fixed for ACL in
PR #258.

`WS` and `MQ` now carry the blanket `WRITE` flag (their read-only
subcommands — `WS LIST`/`INFO`/`AUTH`, `MQ DLQLEN` — are carved out at the
`try_enforce_readonly` call site via the new
`command::workspace::is_ws_readonly_subcommand` /
`command::mq::is_mq_readonly_subcommand` classifiers, mirroring the existing
`SELECT`/`GRAPH.QUERY` blanket-write-with-carve-out pattern). Both
`TEMPORAL.*` commands are unconditionally `WRITE` (no subcommand token, so no
carve-out is needed). The replica **apply** path is unaffected — it never
calls `try_enforce_readonly` by construction (see the module doc on
`replication::apply`), so replaying a master's own WS/MQ/TEMPORAL records on
a replica still works.

Covered by unit tests in `command::metadata`, `command::workspace`, and
`command::mq`, plus a new black-box integration test,
`tests/replication_readonly_ws_mq.rs` (`--ignored`, spawns a real `moon`
binary — WS/MQ are only wired in the sharded handlers, so the synthetic
single-shard harness used by `tests/replication_test.rs` can't exercise this
fix). Verified against both a monoio (default-feature) build and a
`runtime-tokio,jemalloc` build.
### Added — WS.CREATE/DROP replication (Wave B ws-plane)

`WS.CREATE`/`WS.DROP` mutated the process-global `WorkspaceRegistry` from
whichever connection thread received the command and pinned their WAL
records to shard 0 without actually running on shard 0's thread — the
replication offset advance (which must happen synchronously with the
mutation on shard 0, matching the snapshot-capture atomicity argument used
elsewhere) had no such guarantee, and the plane was not replicated at all
(round-2 finding A / task #34).

Routed the whole mutation + WAL-append + replication-record sequence through
a dedicated shard-0 hop (`ShardMessage::WsRegistryCreate` /
`WsRegistryDrop`, mirroring the existing `WsDropCleanup` hop): a connection
already on shard 0 runs the sequence inline, every other connection hops
over via `spsc_send` + a bounded oneshot reply. After the shard-0 execution,
the WorkspaceCreate/WorkspaceDrop record (same payload as the WAL record —
`ws_id` + `name` + `created_at_ms`) is pushed into the replication stream via
the graph-plane's `record_local_write_db` pattern (offset advances IFF the
backlog append happens).

Replica apply arms (`WS.CREATE.APPLY` / `WS.DROP.APPLY` internal
pseudo-commands, `replication::apply::apply_local`) install the MASTER's
`ws_id` + `created_at` VERBATIM — UUIDv7 is nondeterministic, so a replica
must never mint its own id. `WS.DROP.APPLY` reruns the master's best-effort
`{ws_hex}:`-prefix key sweep on the replica's single shard (R0 replication is
single-shard-only). Full-resync backfill: a new versioned
`MOON_AUX_WORKSPACE_REGISTRY` RDB aux blob (`replication::ws_sync`,
shard-0-authoritative — captured only in shard 0's `PrepareReplicaSync` leg
on the multi-shard master path) is installed authoritatively at
`load_snapshot`, same convention as the graph/vector/text aux blobs. New
`ws_registry_record` cargo-fuzz target covers the blob decoder.

The former combined `warn_unreplicated_plane` fail-loud marker
(`handler_monoio/ft.rs`) is fully retired: its WS half by this WS-plane
work, and its MQ half by the MQ-plane replication entry below.

New `tests/replication_ws.rs`: live-stream parity (id + `created_at`
round-trip through `WS.INFO`/`WS.AUTH` on the replica), snapshot-leg
backfill, `WS.DROP` propagation, and a `--shards 4` leg exercising the
shard-0 hop from connections that may land on any shard.

### Added — MQ-plane replication (Wave B stage 2b)

Durable-queue MQ.* mutations now replicate to attached replicas, closing the
plane gap `warn_unreplicated_plane` previously fail-loud-warned about for
MQ.*. Builds on the stage-2a MQ WAL effect records (PR #291):

- **Live stream**: `shard::mq_exec::replicate_mq_record` emits the SAME
  encoded `MqCreate`/`MqPush`/`MqPop`/`MqAck`/`MqTrigger` payload bytes
  already built for the WAL as one of five synthetic `MQ._REPL.*`
  pseudo-commands (never dispatched to real clients), gated single-shard-only
  (`num_shards == 1`) — the same posture graph's own live path takes for its
  multi-shard gap. `MQ.PUBLISH`'s TXN-materialization leg
  (`server::conn::handler_monoio::txn.rs`) replicates the same way at
  commit. `replication::apply::apply_mq` decodes and applies through the
  SAME `apply_mq_*` engine boot-time WAL replay uses
  (`shard::shared_databases`, generalized over a new `MqApplyTarget` trait so
  `ShardSliceInit` boot replay and `ShardSlice` live apply share one codec
  instead of two).
- **FULLRESYNC**: a per-shard `MOON_AUX_MQ_REGISTRY` aux blob
  (`replication::mq_sync`) carries the durable-queue registry and trigger
  registry — the shard-level bookkeeping that lives outside the keyspace.
  Installed additively into every replica shard
  (`mq_sync::install_mq_registry_many`), mirroring
  `graph_sync::install_graph_store_many`.
- **Fixed a pre-existing FULLRESYNC data-loss bug found along the way**: the
  PSYNC RDB codec (`persistence::redis_rdb`, distinct from the
  `persistence::rdb` SAVE/BGSAVE codec) serialized `Stream` values as a
  placeholder `"__stream__:<len>"` string, discarding all entries/PEL/
  consumer-group state. A full `RDB_TYPE_STREAM_MOON` (0xC8) codec — ported
  from `persistence::rdb`'s battle-tested Stream serializer — now carries
  entries, `last_id`, consumer groups (PEL + per-consumer pending), the
  `durable` flag, and `max_delivery_count`. Without this fix, MQ FULLRESYNC
  backfill would silently lose every durable queue's message content.
  **Operator note — upgrade masters and replicas together** if any durable
  Streams exist: a pre-fix replica syncing from a fixed master hard-fails
  FULLRESYNC on the new `RDB_TYPE_STREAM_MOON` tag (and retries on its
  reconnect loop until upgraded), while a fixed replica syncing from a
  pre-fix master absorbs the old placeholder as a corrupted string value —
  both inherent to fixing a wire-format bug, neither loses data already
  durable in the master's WAL.
- Two new `cargo-fuzz` targets: `mq_registry_blob` (the new
  `install_mq_registry_many` decoder) and `redis_rdb_load` (the FULLRESYNC
  RDB loader as a whole, including the new Stream type tag) — wired into
  both PR and nightly fuzz matrices.
- `tests/replication_mq.rs`: live-stream, snapshot-backfill, and a pinned
  multi-shard-live-write-not-streamed limitation test (shards=1 scope,
  matching graph's own precedent).

A multi-shard master still does not live-stream MQ writes (durability is
unaffected — WAL + FULLRESYNC still cover it); tracked as a known follow-up
alongside graph's own multi-shard live-stream gap.

### Fixed — docs site strict build red since 2026-07-08 (root-relative links)

`docs/guides/tuning.md` (PR #245) and `docs/PRODUCTION-CONTRACT.md` (PR #263)
linked repo-root files (`BENCHMARK.md`, `RELEASES.md`,
`.github/workflows/release.yml`) via relative paths that escape the MkDocs
`docs/` tree — `mkdocs build --strict` aborts on the 5 resulting warnings, so
the GitHub Pages deploy had failed on every main push since 2026-07-08.
Converted the 5 links to absolute GitHub blob URLs; strict build verified
clean locally. Docs-only.
### Fixed — MQ effect records now survive kill-9 (Wave B stage 2a, task #34)

`MQ.CREATE/PUSH/POP/ACK/TRIGGER` intercept before the generic AOF-logging
dispatch path (they route via `execute_mq_on_owner` in
`src/shard/mq_exec.rs`), so under `--appendonly yes` the AOF-authority
recovery (`db.clear()` on every shard, rebuild solely from the AOF manifest)
silently discarded every durable `Stream`, consumer-group PEL, DLQ routing
decision, and trigger registration on every restart — regardless of whether
`replay_mq_wal` itself was correct. Two additional pre-existing defects in
`replay_mq_wal` made it dangerous even when reached: `MqAck` records were
applied by COUNT (rolling the whole PEL back to a snapshot cursor) instead
of by ID, and every MQ payload hardcoded db index 0.

Fixed by giving each MQ mutation its own versioned WAL v3 effect record,
emitted at the owner-shard execution site: `MqPush` (0x72), `MqPop` (0x73),
and `MqTrigger` (0x74) are new discriminants; `MqCreate` (0x70) and `MqAck`
(0x71) keep their existing discriminants but move to a versioned,
db-index-carrying payload (precedent: the `XactCommit` 0x51→0x53 format
freeze — WAL v3 segments are short-lived with no cross-version contract).
Every decoder in the new `src/mq/wal.rs` module returns `None` on a
malformed OR unsupported-version payload; `replay_mq_wal` skip-and-warns
(`tracing::warn!`) rather than aborting the scan. `MqPop` now carries the
full claim set (id + delivery_count per claimed message) plus any DLQ
routing decisions (source id → assigned DLQ id), so replay reconstructs the
consumer group's PEL and `last_delivered_id` exactly instead of guessing.
`MqAck` now applies via `Stream::xack` by id — idempotent, and immune to
the old count-based rollback bug. `MQ.PUBLISH`'s TXN materialization hop
also emits `MqPush` records, at both the self-fold and foreign-shard legs,
on both the monoio and tokio connection handlers.

Trigger registrations are durable/replayed as opaque data — replay never
*fires* a trigger; only a live `MQ.PUSH`'s debounce arming does
(`src/shard/timers.rs::fire_pending_mq_triggers`).

New RED/GREEN kill-9 crash test: `tests/crash_recovery_mq_effects.rs`
(`--ignored`, needs a built binary) exercises the full lifecycle —
CREATE → PUSH×5 → POP(3) → ACK(2 of 3) → a second CREATE/PUSH/POP pair that
forces immediate DLQ routing → a TRIGGER registration — kill -9, restart on
the same `--dir`, and asserts stream content, delivery cursor, PEL-by-id,
DLQ routing, and trigger re-arming all survive. New fuzz target
`mq_wal_record` covers the five new op-blob decoders (`fuzz/fuzz_targets/`,
registered in both `fuzz-pr` and `fuzz-nightly` CI matrices).

Measured WAL footprint (release, single shard): CREATE + 5×PUSH + POP(3)
= 7 records, 576 bytes on disk including the 64-byte segment header —
~68 bytes per small PUSH (48-byte payload + 20-byte framing/CRC), ~132
bytes for a 3-claim POP.

**WAL recycle plane guard (adversarial-review fix):** `recycle_aggressive`
and `recycle_segments_before` now refuse to delete any sealed segment
holding workspace/MQ/temporal records — those planes have no snapshot
format in ANY mode, so the WAL is their sole durable copy and no caller's
LSN floor (autovacuum Pass C in disk-offload mode, the checkpoint
protocol, admin `VACUUM`) makes such a segment safe. Previously,
disk-offload deployments under `--max-wal-size` pressure would silently
and permanently lose MQ/WS/temporal history during normal operation, no
crash required. Kept segments are counted in
`reclamation_wal_recycle_blocked_no_checkpoint_total` and warned
(rate-limited); WAL size may exceed `--max-wal-size` for plane-heavy
workloads until plane checkpointing lands (storage-kernel M3). The
guard fails closed: unreadable/torn/unknown-type segments are kept.

Known limitation (pre-existing, tracked as task #46): durable MQ streams
deleted via generic `DEL`/`UNLINK`/`FLUSHALL`/`FLUSHDB` are resurrected —
now with full content — by MQ WAL replay after a restart; there is no MQ
tombstone record yet (same bug class as the fixed vector/KV cold-plane
resurrection). **Fixed in kernel M3 stage 3** — see the "MQ durable-stream
tombstone" entry below.

Out of scope (stage 2b+): replication emission/apply for the MQ plane.

### Changed — WAL v3 `wal_append` channel now preserves the caller's REAL record type end-to-end (K1a, storage-kernel M1 stage 1)

`ShardDatabases::wal_append` / `try_wal_append_required` and
`mq_exec::wal_append_on_slice` took a plain `Bytes` payload; the shard
event-loop's 1ms-tick drain (`event_loop.rs`, two sites) unconditionally
re-wrapped every message as an outer `WalRecordType::Command` record. Every
non-Command producer (`XactCommit`, `WorkspaceCreate`/`WorkspaceDrop`,
`MqCreate`/`MqAck`, `GraphTemporal`) worked around this by pre-framing its
own record with `write_wal_v3_record` and sending the ALREADY-FRAMED bytes
through — a second, nested WAL frame inside the outer `Command` frame,
recovered on replay via a bespoke `read_wal_v3_record`-on-payload unwrap per
record type. One nested-framing call (`handler_monoio/txn.rs` /
`handler_sharded/txn.rs` XactCommit) additionally passed the transaction's
`txn_id` into the inner frame's `lsn` field, mislabeling it.

Fixed structurally: the channel item is now `(WalRecordType, Bytes)` — the
producer's real type plus the UNFRAMED payload — threaded through
`ShardSlice::wal_append_tx` / `ShardDatabases::wal_append_txs` end-to-end.
The drain calls `wal.append(record_type, &payload)` directly, so the WAL
writer assigns the real LSN and does the single framing. Every producer's
pre-framing (`write_wal_v3_record` call) was deleted:
`handler_monoio`/`handler_sharded` `write.rs` (WS.CREATE/DROP) and `txn.rs`
(XactCommit — the `txn_id`-as-`lsn` mislabel is gone with it),
`shard/uring_handler.rs`'s WS batch path, `shard/mq_exec.rs` (MqCreate/MqAck),
and `command::temporal::apply_invalidate` (GraphTemporal — PR #286 had just
added this record's pre-framing; it is deleted in favor of the typed
channel, and the function now RETURNS the raw payload instead of pushing it
into `GraphStore::wal_pending`, which stays exclusively `Command`-typed RESP
bytes). The one caller that cannot change its `Frame`-only return signature
without a ~90-call-site test ripple (`command::graph::dispatch_graph_command`,
the cross-shard `GraphCommand` entry point) stashes the returned payload in
a new `GraphStore::temporal_wal_pending` side-channel instead, which its two
real callers (`shard/spsc_handler.rs`) `.take()` right after dispatch.

Replay is **fully backward compatible, no format bump**: every existing
direct-type match arm (`replay_workspace_wal`, `replay_mq_wal`,
`replay_temporal_wal` in `shared_databases.rs`) already had a nested-Command
unwrap fallback (added when the records were still nested) plus, for
`GraphTemporal`, a legacy-raw fallback for even older un-nested records —
both are UNTOUCHED, so segments written before this fix keep replaying
exactly as before. New records simply hit the direct-type arm for the first
time instead of falling through the unwrap.

`persistence::recovery.rs` Phase 4's `on_command` closure had no
`XactCommit` arm at all (silent `_ => {}`) — now reachable for the first
time because the outer type used to always be `Command`. Decision: an
EXPLICIT documented no-op (counts toward `commands_replayed`, never
dispatches). The forward-image KV payload (`encode_xact_commit_payload`) is
redundant in every reachable config — the transaction's individual SET/DEL
ops already ride either this same Phase-4 WAL replay as ordinary `Command`
records (`--wal-kv-log` on) or the AOF, the KV recovery authority in every
config (Phase 4b falls back to it whenever `kv_commands_replayed == 0`).
Decoding it would be a no-op at best and risks double-applying a
non-idempotent op at worst — the same overlap risk the adjacent Phase 4b
comment already flags. `wal_v3::replay::replay_wal_v3_dir_commands` (the
legacy last-resort-fallback path, used only when no AOF exists) already had
a correct `replay_xact_commit` call for the real outer type — untouched,
and now reachable for the first time too since it stops receiving records
nested inside `Command`.

### Fixed — WS.CREATE's `created_at` was silently dropped by the WAL, restored as 0 after every restart (K1b, storage-kernel M1 stage 1)

`encode_workspace_create`/`decode_workspace_create` (`src/workspace/wal.rs`)
only ever serialized `[ws_id][name_len][name]` — the `created_at` computed
at WS.CREATE time never reached the payload, so `replay_workspace_wal`
(`shared_databases.rs`) had no choice but to hardcode `created_at: 0` on
every restart, silently losing the real creation time. Fixed with a
versioned, backward-compatible layout: new records append a trailing
`created_at_ms: i64 LE`; the decoder accepts BOTH the old
(`20 + name_len`-byte) and new (`20 + name_len + 8`-byte) lengths, returning
`created_at = 0` for old records (matching prior restart behavior exactly —
no format bump, mixed-segment compatible). All three producers
(`handler_monoio`/`handler_sharded` `write.rs`, `shard/uring_handler.rs`'s
batch path) now pass the already-computed `created_at` through; the replay
decoder threads the real value into `WorkspaceMetadata` instead of the
hardcoded `0`.

### Fixed — autovacuum Pass C recycled the sole durable copy of graph/WS/MQ/temporal WAL history in legacy mode (task #43, P1)

`AutovacuumDaemon::run_tick`'s Pass C (`src/shard/autovacuum.rs`) called
`WalWriterV3::recycle_aggressive(redo_lsn = wal.current_lsn())` — the LSN
*about to be assigned*, i.e. "everything sealed is durable elsewhere" —
whenever total on-disk WAL bytes exceeded `--max-wal-size` (default
256 MiB), **regardless of whether disk-offload was enabled**. That floor
only holds in disk-offload mode, where the checkpoint protocol maintains a
real `redo_lsn` and `persist_graph_at_checkpoint` snapshots the graph
write-buffer before the floor advances. In legacy (non-disk-offload) mode
there is no periodic plane snapshot at all: `save_graph_store` runs only at
graceful shutdown, and the workspace/MQ/temporal registries have **no
snapshot format whatsoever** — `replay_workspace_wal` / `replay_mq_wal` /
`replay_temporal_wal` rebuild them purely by re-scanning the WAL. Once the
ceiling was breached, Pass C deleted every sealed segment unconditionally,
including segments holding the sole durable copy of that history; a kill-9
afterwards lost it permanently, even though replay itself (task #42,
PR #286) works correctly for whatever records survive.

Fixed per the "never trade data loss for disk space" principle: `run_tick`
now takes a `disk_offload_enabled` flag. When `true`, behavior is byte-for-
byte unchanged (`recycle_aggressive` against `current_lsn()`, as before).
When `false`, Pass C no longer recycles: it skips the delete, emits a
rate-limited (5 min) `tracing::warn!`, and increments the new
`reclamation_wal_recycle_blocked_no_checkpoint_total` INFO counter
(`RECL_WAL_RECYCLE_BLOCKED_NO_CHECKPOINT_TOTAL`) so operators can see WAL
growth is unbounded in this mode. A snapshot-then-floor design (option A)
was considered and rejected for the workspace/MQ/temporal planes — they
have no snapshot format to floor against, and inventing one was explicitly
out of scope for this fix; only the graph plane has a callable snapshot
(`persist_graph_at_checkpoint`), so a partial-A fix would still have left
WS/MQ/temporal unprotected. Operators needing bounded WAL growth without
this trade-off should enable `--disk-offload enable`.

New integration test `tests/crash_recovery_wal_recycle_legacy.rs`
(`t43_legacy_mode_pass_c_must_not_lose_ws_and_graph_history`, live kill -9 +
restart against tiny `--wal-segment-size`/`--max-wal-size` to force several
Pass C ticks within seconds) proves two things: (1) byte-level survival —
after Pass C has had several chances to run, the earliest workspace AND
graph WAL records are still present on disk; and (2) round-trip survival —
after a real kill -9 + restart, `WS LIST` still returns the workspace
(replay-only plane, no snapshot format, the exact risk called out above).
Full `GRAPH.QUERY` reconstruction after restart is deliberately not
asserted in legacy mode — independent probing found graph command replay
from WAL v3 does not reconstruct the graph in that mode even with zero
Pass C interference (default `--max-wal-size`), a separate, pre-existing
gap unrelated to WAL recycling; assertion (1) above still proves task #43
is fixed at the layer Pass C actually operates on. Disk-offload mode is
unchanged, verified by the existing `crash_recovery_graph_durability.rs`
G4/G5 suite (checkpoint-driven recycle path) passing unmodified.

### Fixed — cold-collection visibility: silent data loss on evicted Hash/List/Set/ZSet/Stream (task #41, P0)

With disk-offload enabled (the default), the production eviction paths
(`evict_one_async_spill`, `evict_batch_durable_no_aof` in
`storage/eviction.rs`) spill Hash/List/Set/ZSet/Stream values to the cold
tier via `kv_serde::serialize_collection` — but the type-specific accessors
never consulted the cold index before this fix. Consequences: `HGET`/
`HGETALL`/`LRANGE`/`SMEMBERS`/`ZRANGE`/`EXISTS` reported the key absent
after eviction even though it was durably spilled, and `HSET`/`LPUSH`/
`SADD`/`ZADD` silently fabricated a **new empty container**, permanently
shadowing (destroying) the cold copy on the next flush — a genuine,
silent user-data-loss bug for any collection key subject to eviction.

Fixed with a single promote-if-cold hook rather than rewriting every
accessor:

- `Database::promote_cold_if_present` — on a hot miss, does one
  `ColdIndex` lookup; on a hit it decodes the cold value, installs it hot,
  and removes the cold-index entry (single owning copy, no dual
  reference). All 8 `get_or_create_*`/`get_*` mutable accessors
  (`get_or_create_hash[_listpack]`, `get_or_create_list[_listpack]`,
  `get_or_create_set`/`get_or_create_intset`, `get_or_create_sorted_set`,
  `get_or_create_stream`, `get_hash`, `get_list`, `get_set`,
  `get_sorted_set`, `get_stream[_mut]`) now call this hook before falling
  through to their existing "fabricate new" path — so HSET/LPUSH/SADD/ZADD/
  XADD merge into the promoted collection instead of shadowing it.
- The four `&self`-typed "`*_ref_if_alive`" shared-read accessors
  (`get_hash_ref_if_alive`, `get_list_ref_if_alive`, `get_set_ref_if_alive`,
  `get_sorted_set_ref_if_alive`, `get_stream_if_alive`) cannot promote
  (they're also called from the `_readonly` dispatch path holding only a
  shared `&Database`), so they instead do a **non-promoting** cold
  read-through: new `Owned`/`Borrowed` variants on `HashRef`/`ListRef`/
  `SetRef`/`SortedSetRef` (and a new `StreamRef<'a>` `Deref` wrapper
  replacing the old `&StreamData` return type) let a cold hit return a
  freshly-decoded owned value without a backing hot entry, at the cost of
  exactly one extra branch + one `ColdIndex` lookup on a hot miss. Hot-hit
  cost is unchanged (single probe, same as before).
- `exists`/`exists_if_alive` now fall back to a cheap
  `cold_contains_alive` check (ColdIndex presence + TTL only, no disk I/O,
  no promotion) instead of unconditionally returning `false` on a hot
  miss.
- `zrank_readonly`/`zrevrank_readonly` (`command/sorted_set/sorted_set_read.rs`)
  updated to route the new `SortedSetRef::Owned` variant through the same
  O(n) fallback as the existing `Listpack` variant (previously any
  unmatched variant silently fell into a `Frame::Null` wildcard arm —
  would have reported a promoted member as not-ranked).

New tests: 13 unit tests in `storage::db::tests` (spill-then-promote for
all 5 collection types + `EXISTS`-without-promoting) plus a new black-box
suite `tests/cold_collection_visibility.rs` (real server,
`--disk-offload enable`, sampled-LRU-driven eviction, asserts `EXISTS`,
read-after-evict, and write-merges-with-promoted-data for Hash/List/Set/
ZSet). Explicitly out of scope for this fix (unchanged):
`recovery.rs` rehydration, replication, and the eviction gates' Wave A
reporting sinks.

### Fixed — TEMPORAL/MQ WAL v3 replay data loss on kill-9 (task #42)

`TEMPORAL.INVALIDATE` and `MQ.CREATE`/`MQ.ACK` durable state was silently
lost on every crash restart. `replay_mq_wal` / `replay_temporal_wal`
(`src/shard/shared_databases.rs`) had two bugs: (a) they scanned
`persistence_dir/shard-{id}/` instead of `.../shard-{id}/wal-v3/` —
`std::fs::read_dir` is non-recursive, so every boot replayed zero records
from an empty directory; (b) even with the directory fixed, they matched
`record.record_type` directly, but every cross-thread `wal_append` blob is
re-wrapped as `WalRecordType::Command` by the shard event-loop drain — the
typed inner record (`MqCreate`/`MqAck`/`GraphTemporal`) is nested in the
Command's payload and was never unwrapped (`replay_workspace_wal` was the
only replay function that already did this). `GraphTemporal` records also
turned out to be pushed as raw, un-framed bytes (not nested-record-framed)
by `command::temporal::apply_invalidate`, requiring a direct-decode branch
alongside the nested-unwrap for `replay_temporal_wal`.
Also fixed a related dead-channel bug found while verifying the MQ path:
`ShardSlice::wal_append_tx` (used by `mq_exec.rs`'s owner-shard MQ.CREATE /
MQ.ACK path) was never assigned anywhere — `ShardDatabases::wal_append_txs`
(a separate field, used by TEMPORAL/WS) was wired in `event_loop.rs`, but
the per-slice field was not, so MQ.CREATE/MQ.ACK never reached the WAL v3
writer at all regardless of the replay-side fix. Wired it in `event_loop.rs`
alongside the existing `set_wal_append_tx` call.
New crash-recovery integration test `tests/crash_recovery_temporal_mq.rs`
(`temporal_invalidate_survives_kill9`, live kill -9 + restart against
`GRAPH.QUERY ... VALID_AT`) and two white-box unit tests in
`shared_databases.rs` (`test_replay_mq_wal_restores_registry_and_rolls_back_cursor`,
`test_replay_mq_wal_missing_wal_v3_subdir_is_a_noop`) proving the MQ replay
fix directly against real WAL v3 bytes — a live MQ.CREATE round trip is
architecturally blocked by a separate, deeper, out-of-scope gap (MQ has
zero AOF durability, so `--appendonly yes`'s unconditional AOF-authority
recovery wipes the Stream on every restart independent of this fix); that
gap is left for follow-up.

**CodeRabbit review follow-ups on the above (same PR):**
- **Dead-channel sender wiring under `appendonly=no` + `disk-offload=on`.**
  `event_loop.rs` wired `ShardSlice::wal_append_tx` /
  `ShardDatabases::set_wal_append_tx` whenever
  `appendonly_enabled || disk_offload_enabled()`, but `wal_writer` itself is
  only ever created when `appendonly_enabled` is true (`wal_shard_dir`
  requires it regardless of disk-offload) — so with disk-offload on and
  AOF off, a live sender was wired to a channel with no writer behind it,
  and the 1ms-tick drain silently discarded every record instead of the
  `None`-sender no-op `wal_append`/`try_wal_append_required` are documented
  to degrade to. Gated the wiring on `wal_writer.is_some()` instead.
- **`replay_temporal_wal`'s legacy raw-`GraphTemporal` discriminator could
  drop a real invalidation.** The un-framed-record fallback rejected a
  payload whenever its first byte was `b'*'`, to distinguish it from a RESP
  command payload — but that byte is the low byte of `entity_id`, so any
  invalidation on an entity whose id happened to end in `0x2A` was silently
  skipped on replay. Fixed both ends: `command::temporal::apply_invalidate`
  now pre-frames its `GraphTemporal` WAL payload with `write_wal_v3_record`
  (matching how MQ's `MqCreate`/`MqAck` are already pre-framed), so NEW
  records replay unambiguously via the existing nested-Command unwrap
  (CRC-validated, no byte-pattern guessing); the raw-legacy fallback (still
  needed for WAL segments written before this fix) now runs only when the
  nested unwrap finds nothing, gated by a new
  `decode_graph_temporal_legacy_raw` sanity check (`is_node` byte must be
  literally 0/1, both timestamps plausible) instead of the single
  leading-byte guess. Added a red/green regression test reproducing the
  exact `0x2A`-collision entity id, a round-trip test for the new
  pre-framed producer format, and an adversarial test proving a RESP-shaped
  25-byte payload is not misdecoded into a spurious invalidation.
- **`temporal_invalidate_survives_kill9` only asserted absence.** The test
  checked that the node was invisible at a far-future `VALID_AT` post-restart,
  which would pass vacuously if `GRAPH.ADDNODE` replay had failed entirely
  (no node at all is also "not visible"). Added a positive assertion that
  the node is visible at a `VALID_AT` preceding its own invalidation,
  proving the node itself actually survived the restart.
- MQ.ACK's PEL/queue-key drop on replay is deliberately NOT addressed here —
  deferred to the same follow-up as MQ's zero-AOF-durability gap above,
  since PEL replay is only meaningful once MQ stream content itself survives
  a restart.

### Fixed — Wave A adversarial-review fixes (task #34)

Three defects found reviewing Wave A (plane replication) before merge, all
fixed on top of the branch's part 1/2/3 work below.

- **Non-string eviction victims under a `SpillContext` were dropped with no
  cold copy AND no DEL record.** `storage::eviction::evict_one_with_spill`
  snapshotted `is_plain_drop = spill.is_none()` BEFORE its spill body ran,
  but that body only ever spills `RedisValueRef::String` values — a
  Hash/List/Set/ZSet victim picked while a `SpillContext` was live took
  neither branch of the `is_string` check (no bytes written anywhere) yet
  still fell through to the unconditional `db.remove`, and because
  `is_plain_drop` was already `false`, `on_plain_drop` never fired: a
  genuine, silent, unreported data loss. Fixed by tracking whether THIS
  victim was actually spilled (`spilled`, set only inside the branch that
  performs the write) instead of snapshotting the precondition. Also
  threaded the real `record_reason_del` sink into
  `persistence_tick::handle_memory_pressure`'s "durable spill, manifest
  reachable" branch, which previously passed a hardcoded no-op sink on the
  (incorrect, for non-strings) assumption that this branch could never
  plain-drop. New unit test:
  `eviction::tests::sync_spill_non_string_victim_reports_plain_drop`.
- **`record_reason_del`/`record_reason_del_conn`/`record_effect_write`
  allocated a fully serialized RESP record BEFORE checking whether any
  replica or AOF pool was even wired.** The single most common deployment
  shape (standalone server, no replica ever attached, `--appendonly no`)
  paid a `Bytes`/`Frame` allocation for every background-tick DEL or script
  write effect, only to discard it a few instructions later inside
  `wal_append_and_fanout`'s own no-op fast path. Fixed by hoisting the exact
  same has-work predicate (`wal_fanout_has_work` for the shard-loop
  context; a new `conn_has_work` — `fanout_hint_active() ||
  aof_pool.is_some()` — for the connection context) to the top of all three
  entry points, before any serialization. Behavior when there IS work is
  byte-for-byte unchanged (same predicate, same downstream call). This is a
  perf-only fix with no allocation-counting harness in this repo to red/
  green against; verified instead by regression tests confirming both the
  no-op path (`record_reason_del_noop_when_nothing_wired`) and the
  has-work path still emits correctly
  (`record_reason_del_still_emits_to_aof_when_wired`,
  `conn_has_work_true_when_aof_pool_wired`).
- **Lua's bystander-eviction gate was unwired from both durability planes.**
  `scripting::bridge::LuaEvictionCtx::gate` — the OOM check run before every
  `WRITE`-flagged `redis.call`/`redis.pcall` inside a script — called the
  non-reporting `try_evict_if_needed_budget`/
  `try_evict_if_needed_async_spill_budget`, which hardcode a no-op sink.
  When a script's write pushed the shard over `maxmemory`, whatever
  BYSTANDER key the policy sampled (never anything the script itself
  touched) was plain-dropped with no AOF/replication record — invisible
  eviction, same class of bug as every other Wave-A plain-drop site this
  branch fixed elsewhere. New `eviction::try_evict_if_needed_async_spill_budget_reporting`
  wrapper (mirroring the existing `try_evict_if_needed_budget_reporting`);
  `gate` now threads a real sink through to `record_reason_del_conn` with
  the gate's own `db_index`. Unit-level regression test (not a full
  black-box EVAL+replica repro, to avoid a flaky multi-process harness for
  a pure wiring check):
  `scripting::bridge::tests::gate_reports_bystander_eviction_to_aof`.
- **Same unwired-reporting bug at the two call sites ordinary writes
  actually reach.** Black-box testing defect 1 (disk-offload enabled,
  Hash victims, real replica) surfaced that the narrowly-described
  `evict_one_with_spill`/`SpillContext` path above is effectively dead code
  via any CLI configuration today — `spill_thread` and `shard_manifest` are
  co-gated on the identical `disk_offload_enabled()` check, so the sync
  `SpillContext::Some` branch that `persistence_tick` wires is never
  actually constructed. The real reachable paths for an ordinary write
  under `--disk-offload enable` are `server::conn::handler_monoio::
  run_write_eviction_gate` (monoio) and `shard::spsc_handler::
  spsc_eviction_gate` (cross-shard dispatch), and both had the identical
  defect-3-class bug: their spill-sender branch called the non-reporting
  `try_evict_if_needed_async_spill_budget` with a hardcoded no-op sink
  (`spsc_eviction_gate` additionally had an already-plumbed
  `on_plain_drop` parameter that this one branch silently discarded).
  Fixed both to call `try_evict_if_needed_async_spill_budget_reporting`
  with the real `record_reason_del_conn` sink, mirroring the sibling
  no-spill-sender branch each function already had correct. New black-box
  regression: `tests/replication_planes.rs`
  `eviction_parity_hash_disk_offload_shards1`/`_shards4` (disk-offload
  enabled, HSET-driven eviction, real replica attached). Note: full
  dbsize equality is not asserted — the tick-driven memory-pressure
  cascade's `evict_batch_durable_no_aof` batch spill is a real, correctly
  *unreported* spill (durable cold copy + `cold_index` registration) for
  non-string values too, so a residual share of evictions legitimately
  never reach the replica. That cold copy is unreadable today for Hash/
  List/Set/ZSet (`get_hash_ref_if_alive` and siblings never consult
  `cold_index`, unlike the generic `Database::get()` path GET uses) — a
  separate, pre-existing, out-of-scope gap — which is why the test gates
  on a large-majority convergence ratio (0.70, matching this repo's own
  `MERGE_RECALL_TOLERANCE`-style "good enough" convention) rather than
  exact equality; before this fix the ratio was 0.0 (the replica retained
  every hash key, full stop).

**Known limitations (documented, not fixed here):**
- `redis.pcall('SELECT', ...)` inside a script swallows the new
  `SELECT`-rejection error into a Lua table instead of raising — the script
  may continue running against the ORIGINAL db, silently, exactly as before
  Wave A's `SELECT` lockout. Only `redis.call` fails loud. A real
  multi-db-scripts feature (tracked as a follow-up) is the actual fix; until
  then, avoid `redis.pcall('SELECT', ...)` in scripts.
- Nondeterministic write commands issued inside a script (`SPOP`, `SRANDMEMBER`-
  derived writes, etc.) replicate **verbatim** via `record_effect_write` (the
  exact `cmd + args` the script invoked) and can diverge from the master on
  a replica, exactly like any other verbatim-replicated nondeterministic
  command in this codebase (pre-existing class, first exposed for Lua by
  Wave A). A real fix requires replicating the script's *effects* in a
  deterministic form, not the nondeterministic call itself — out of scope
  for task #34.
- `replication::state::fanout_hint_active()` is **sticky**: once ANY replica
  has attached, it stays `true` for the rest of the process's lifetime (see
  `FANOUT_HINT`, a one-way latch, not a live "is a replica currently
  attached" flag). Consequence for operators: after the first-ever replica
  attaches and later detaches, the monoio inline-SET fast path
  (`server::conn::blocking::try_inline_dispatch`) stays permanently
  disabled for that process — every subsequent `SET` falls back to the
  generic dispatch path for the rest of the process's life, even with zero
  replicas currently attached. A process restart is the only way back to
  the fast path.

### Added — Wave A part 1: `record_reason_del` dual-plane DEL emission (task #34)

Master-side key removals for a reason OTHER than a client write command
(active TTL expiry, `--maxmemory` eviction plain-drops) previously reached
NEITHER the AOF plane nor the replication plane: the key vanished from the
master's own keyspace, but an attached replica kept serving it forever, and
a `kill -9` + restart against `--appendonly yes` resurrected it from the AOF
replay (the AOF never recorded the DEL, only the original SET/write).

- New `replication::reason_del::record_reason_del` (shard-event-loop
  context, reuses `wal_append_and_fanout` — the same mechanism the
  `ShardMessage::SwapDb` synthetic-command record already uses) and
  `record_reason_del_conn` (connection-handler context, mirrors
  `handler_monoio::ft::record_local_write_db`'s backlog/offset/fan-out
  mechanics and adds the AOF leg that helper omits). Both emit a real
  `DEL <key>` RESP record, respecting the fused-`SELECT` multi-db framing
  every ordinary write already uses.
- Wired into: active expiry's whole-key sweep (`expire_cycle`), background
  eviction's plain-drop path (`timers::run_eviction`,
  `persistence_tick::handle_memory_pressure`'s no-manifest fallback), the
  inline fast-path SET eviction gate, the generic per-command write-eviction
  gate, and the SPSC cross-shard write-eviction gate. Every call site is
  restricted to `spill.is_none()` plain drops — a spilled/cold-tiered key is
  NOT a delete and must never be reported here.
- Fixed a related pre-existing gap found while wiring this: with
  `--disk-offload disable`, `server::conn::blocking::try_inline_dispatch`
  (the monoio inline SET fast path) fed the AOF but never the replication
  backlog/fan-out at all — a plain `SET` on such a master silently never
  reached an attached replica. `can_inline_writes` now also gates on
  `!replication::state::fanout_hint_active()`, falling back to the generic
  dispatch path (which replicates correctly) once any replica has ever
  attached.
- Known Wave-A-scoped gaps (documented, not silent): hash-field TTL reaps,
  Lua `redis.call` write effects, per-db quota (`--db-maxmemory`) eviction,
  and cross-db `COPY ... DB n` destination eviction do not yet emit —
  tracked as follow-ups.

### Added — Wave A part 2: Lua script write-effect replication + SELECT lockout (task #34)

Continues part 1: a Lua script's `redis.call`/`redis.pcall` write effects
previously reached NEITHER durability plane — `EVAL`/`EVALSHA` carry no
`WRITE` command-metadata flag (by design, see below), so the generic
per-command AOF/replication gate never saw them. A script's writes vanished
on `kill -9` + restart against `--appendonly yes`, and an attached replica
never observed them at all. `redis.call('SELECT', ...)` inside a script also
silently corrupted state: dispatch's SELECT handler mutated only a local
variable, so every subsequent write in the script kept landing in the
ORIGINAL db while looking like it had switched.

- `scripting::bridge::make_redis_call_fn` now records every successfully-
  executed, `WRITE`-flagged inner command to both the AOF and replication
  planes itself, immediately after each `redis.call`/`redis.pcall` returns
  (not batched to script end) — a script that writes two keys and then
  errors on a third still durably records the first two. New
  `replication::reason_del::record_effect_write` (+ shared
  `record_bytes_conn` core, refactored out of part 1's
  `record_reason_del_conn`) does the emission: same fused-`SELECT`/backlog/
  offset/fan-out mechanics as every other write path, recording the
  verbatim `cmd + args` the script invoked.
- `LuaEvictionCtx` (built once per shard at Lua-VM setup, already caching
  the OOM eviction handles) now also carries `num_shards`/`repl_state`/
  `aof_pool` for this emission — one addition at each of the 5 production
  construction sites (4 in `shard::conn_accept`, 1 in
  `server::conn::core::ConnectionContext::build_lua_eviction_ctx`, shared by
  EVAL/EVALSHA and FCALL/FCALL_RO, which route through the identical
  bridge closure).
- `redis.call('SELECT', ...)` inside any script now fails loud with
  `ERR SELECT inside scripts is not supported by moon yet` instead of
  silently corrupting state — intercepted before dispatch, so no partial
  write from the same call ever lands. A real multi-db-scripts feature is a
  follow-up.
- Deliberately did NOT flip `EVAL`/`EVALSHA` to `WRITE` in the command
  metadata table — that would make the generic per-command AOF/replication
  gate ALSO record the literal `EVAL <script> ...` invocation on top of the
  effect records above, double-applying every write the script made (e.g.
  an `INCR` landing as 2 instead of 1). Guarded by a new unit test
  (`command::metadata::eval_evalsha_never_write_flagged`) and a black-box
  regression test (`eval_incr_no_double_apply`). `FCALL` (unlike EVAL) IS
  `WRITE`-flagged — mirrors upstream Redis Functions and only feeds ACL /
  `READONLY`-replica gating, since `try_handle_functions` always consumes
  FCALL before the generic AOF/replication block runs; it rides the same
  single-emission bridge path.
- Turns the 4 remaining RED tests in `tests/replication_planes.rs` green:
  `eval_effects_parity_shards1`, `eval_effects_parity_shards4`,
  `eval_writes_survive_restart`, `select_in_script_errors`. All 9 tests in
  the suite (the 5 from part 1 plus the new `eval_incr_no_double_apply`)
  pass.
- Known gap, unchanged from today: a write-`EVAL` issued directly against a
  read-only replica is not rejected (`try_enforce_readonly` only gates on
  the `WRITE` metadata flag, which EVAL intentionally lacks) — tracked as a
  follow-up; replicas never receive `EVAL` itself via replication (only the
  effect records), so this cannot cause replication divergence, only local
  replica-side corruption if a client is misdirected to write against a
  replica directly.

### Fixed — test-harness port-flake sweep (task #18)

33 integration suites that spawn a real `moon` process shared a copy-pasted
`free_port()` that binds `:0`, reads the port, and drops the listener before
the server spawns. Two CI-observed failure modes shipped with that pattern:
a **port TOCTOU** (between probe drop and moon's bind, a concurrent test's
probe — or an outbound connection's ephemeral source port — takes the port,
so moon exits with EADDRINUSE) and a **dead-server blind poll** (harnesses
polled `connect()` for up to 30s without checking child liveness, reporting
"server never accepted: Connection refused" while the real bind error sat
unread in the server's stderr log).

- New shared `tests/common/mod.rs`: `reserve_port()` (process-wide dedup set
  over kernel-chosen probe ports) and `spawn_listening()` (spawns via a
  caller closure, polls TCP accept **while watching `child.try_wait()`**,
  and respawns on a fresh port the moment a child dies — external
  ephemeral-port steals can't be prevented, only recovered from).
- All 33 suites converted; protocol-level readiness (PING/AUTH) stays with
  each suite. Kill-9/SIGTERM/restart tests keep their deliberate
  same-port+same-dir restart legs untouched — only the first spawn of each
  server lifecycle goes through `spawn_listening`. Expected-startup-failure
  tests (CLI validation) keep direct spawns on `reserve_port()` ports.
- Verified: all suites green plus a 10-rep stress running 14 port-hungry
  suites concurrently (the CI contention pattern that produced the original
  flakes).

### Added — R2: multi-shard master PSYNC (task #20, RFC 1B)

A master running `--shards N` now serves full replication to a single-shard
replica — previously PSYNC was rejected with `-ERR PSYNC across multiple
shards is not yet supported`.

- **Per-shard atomic snapshot legs.** A new `ShardMessage::PrepareReplicaSync`
  fans out to every shard (own shard via the self queue, the rest over the
  SPSC mesh). Each shard serializes its keyspace slice to an RDB *body*,
  captures its replication offset, and registers the replica's live channel
  in ONE synchronous stretch on its own thread — per shard, nothing can land
  between "inside the snapshot" and "streamed live", so there is no backlog
  catch-up leg and non-idempotent commands (INCR) can never double-apply.
- **One merged Redis-format RDB.** The PSYNC task stitches the per-shard
  bodies into a single valid RDB (`redis_rdb::write_rdb_merged`: header +
  bodies + EOF/CRC64) and answers `+FULLRESYNC <replid> <Σ shard offsets>`
  with one `$<len>` bulk — the replica's existing R0 loader needs no changes.
  Index definitions ride once; graph content is sharded, so the snapshot
  carries one `moon-graph-store` aux entry per shard and the replica imports
  all of them (`install_graph_store_many`, `read_moon_aux_all`).
- **Per-record SELECT framing on the merged wire.** N shard threads feed one
  replica socket, so a shared "current db" context cannot exist: on
  multi-shard masters every db-scoped record is fused with its own
  `SELECT <db>` prefix (single channel send / backlog append pair / offset
  advance) — no cross-shard interleave can split a SELECT from the write it
  frames. Gated on the replica-attach hint; single-shard masters keep the
  cheaper emit-on-change tracking.
- **Partial resync degrades to full.** A replica's single scalar offset
  cannot be mapped back onto N per-shard backlogs, so a multi-shard master
  answers every PSYNC (any replid/offset) with `+FULLRESYNC`.
- Overflow-kick (task #35), `REPLCONF ACK`, and `WAIT` all carry over: the
  summed snapshot offset keeps `total_offset - base == bytes on wire`, so
  WAIT/ACK math stays exact on multi-shard masters.
- New e2e suite `tests/replication_multishard.rs`: 2/4/8-shard full resync +
  live-stream convergence with INCR exactness, interleaved multi-db writers
  with db-leak asserts, per-shard graph snapshot import, and the
  partial→full degradation handshake.
- Known limitation (unchanged from R1): master-side PSYNC requires
  `runtime-monoio` (the default). A `runtime-tokio` master now answers PSYNC
  with a clear `-ERR PSYNC requires runtime-monoio on the master` instead of
  an unknown-command reply (RFC R3/2A). Multi-shard *replicas* remain
  unsupported (`--shards 1`).
- Known limitation: during snapshot preparation + transfer the live stream
  buffers in the replica's 16,384-record channel. A very large keyspace
  under sustained heavy write load can overflow it mid-attach — the replica
  is then KICKED (loud) and retries the sync; it never diverges silently.
  Attach such deployments during a write lull, or raise the buffer if this
  becomes a practical constraint.
- Docs refreshed for the new topology: `docs/guides/clustering.md` deployment
  shape, `README.md` replication bullets, and `docs/PRODUCTION-CONTRACT.md`
  rows REPL-MULTISHARD-01 + WAIT-01 flipped to ✅ with evidence.

### Fixed — live-fanout exactly-once redesign + replica-task leak (task #20 follow-up)

Attach-under-write stress testing of R2 surfaced three defects; all fixed
before release (none shipped):

- **`REPLICAOF` leaked the previous replica task — every re-attach stacked
  one more live applier.** `REPLICAOF host port` spawned a fresh
  `run_replica_task` without stopping the old one, and `REPLICAOF NO ONE`
  only flipped the role state: the old task kept its master link open and
  kept APPLYING the stream. After NO-ONE → re-attach cycles the replica ran
  INCR counters ~25-35% ABOVE the master (reproduced at shards=1 AND
  shards=4 — pre-existing, not an R2 defect). Replica tasks now carry a
  process-global epoch ticket; a new `REPLICAOF` target or `NO ONE` bumps
  the generation and superseded tasks exit before their next connect,
  before loading a snapshot, and before applying any parsed chunk.
- **Snapshot-vs-live exactly-once is now offset-cut based, not
  FIFO-placement based.** Two adversarial-review rounds found opposite
  failure modes for placement schemes (a queued self-shard snapshot leg
  double-delivered local writes; an inline-captured one lost same-cycle
  cross-shard writes — neither in the body nor live-sent, with the offset
  advanced: permanent replica lag). Every fan-out entry now records
  `cut = <shard offset at body capture>` and every live record carries its
  per-shard `end_offset`; delivery requires `end_offset > cut`, making
  correctness independent of where the registration lands in the drain
  FIFO. All shards (including the PSYNC connection's own) use the same
  `PrepareReplicaSync` arm.
- **Same-key wire ordering.** Cross-shard (SPSC-dispatched) writes used to
  send to replicas directly from the execute arm while local handler writes
  deferred through the self queue — a later-offset write could reach the
  wire before an earlier-offset write to the same key, replaying same-key
  writes out of the master's order on the replica (found by analysis; not
  reproduced in ~10 black-box runs). ALL live replica sends now flow
  through the self-queue `ReplicaLiveFanout` arm, so per-shard wire order
  equals offset order by construction.
- New e2e regressions: `attach_under_write_no_double_apply` (4-shard +
  single-shard control, 5× detach/re-attach under pipelined INCR load,
  exact per-counter parity) and `same_key_write_order_parity` (12
  connections APPEND-race the same 32 keys through both write paths;
  replica strings must byte-equal the master).

### Fixed — consistency/durability defects caught by the R1 gates (task #35)

Three pre-existing data-integrity bugs surfaced by the new load/kill-9 gates
run for the R1 WAIT/ACK work (all reproduced on `main` before the fix):

- **Replication: silent record drop for lagging replicas.** The per-replica
  fan-out used `try_send` and *skipped* the record when the bounded channel
  was full — under one pipelined burst a replica received ~2k of 40k keys,
  stayed `master_link_status:up`, and diverged forever. A replica whose
  channel overflows is now KICKED (shared `kicked` flag → drain loop closes
  the socket) so it reconnects and resyncs from the backlog — Redis's
  output-buffer-limit policy. Channel capacity raised 1024 → 16384 records
  so bursts kick rarely. New e2e
  `replica_converges_under_interleaved_multidb_load` locks in exact
  post-load parity.
- **Client `SELECT` commands leaked into the AOF and replication stream.**
  SELECT is W-flagged (routing), so every handler persisted the literal
  client SELECT while bare writes carried no db context — two interleaved
  connections on different dbs corrupted BOTH planes' db attribution
  (observed: all 40k keys recovered into db2 after kill-9). SELECT is now
  connection-state only (`metadata::is_persisted_write`), never persisted or
  replicated.
- **AOF had no per-record db attribution.** The AOF writer now threads the
  executing db through `AofMessage` and tracks the stream's current db,
  prepending a `SELECT <db>` record exactly when it changes (Redis's
  `aof_selected_db`), including through BGREWRITEAOF fold drains (context
  resets per fresh incr segment; ambiguous drains use a force-emit
  sentinel). New e2e `aof_multidb_kill9` (TopLevel + PerShard) proves
  interleaved multi-db writes recover into the correct databases after
  kill-9 on both runtimes.

### Added — R1: real WAIT/ACK plumbing (replica acknowledgements)

- **`WAIT <numreplicas> <timeout>` now works on the production (monoio)
  runtime.** It previously answered `:0` unconditionally from the synchronous
  dispatch table; a new connection-layer intercept awaits
  `wait_for_replicas` (10ms poll, early exit; `timeout 0` = block until
  satisfied, capped at one year). Wired on the tokio sharded path too.
- **Replicas acknowledge their applied offset**: a dedicated 1s ticker task
  owns the write half of the (split) replication socket and sends
  `REPLCONF ACK <offset>` — Redis's replicationCron cadence, doubling as an
  idle keepalive for master-side lag detection. A timeout-wrapped read was
  rejected: cancelling an in-flight io_uring read whose completion already
  landed DISCARDS those bytes (silent stream corruption).
- **The master reads ACKs off the hijacked PSYNC socket**: the inline drain
  loop splits the stream; a same-thread reader task parses
  `REPLCONF ACK <offset>` frames (dedicated parser — the shared replication
  drainer deliberately drops REPLCONF as chatter) and records them into the
  replica's `ack_offsets`/`last_ack_time` via `fetch_max` (reordered or
  duplicate ACKs can never regress the recorded offset).
- New e2e `wait_returns_acked_replica_count`: WAIT with no replicas → 0
  fast; WAIT 1 after a write → 1 within the ACK cadence; WAIT 2 with one
  replica → times out reporting 1 (RED before this change).

### Fixed — multi-db replication: master streams `SELECT`, replicas serve it (HIGH-2)

- **Writes outside db 0 landed in db 0 on the replica**: the replica-side
  drain already understood in-stream `SELECT` context, but the master never
  emitted it — every replicated command applied to the replica's db 0
  regardless of the db it executed in on the master. `record_local_write_db`
  now prepends a `SELECT <db>` record whenever the writing connection's db
  differs from the stream's per-shard db context (`ReplicationState::
  stream_db`); the context is reset in the same synchronous stretch as every
  FULLRESYNC snapshot capture (Redis's `slaveseldb = -1` idiom), so a fresh
  replica always sees an explicit context before its first non-0-db write.
- **`+CONTINUE` keeps the db context across reconnects**: resumed backlog
  bytes only carry `SELECT` at db CHANGES, so the replica now preserves its
  drain-side db in `ReplicaTaskConfig::stream_db` across link drops (reset to
  0 on FULLRESYNC). In-memory only — a replica process restart starts at
  offset 0 and always full-resyncs.
- **`SELECT` was rejected on read-only replicas** (task #23): flagged W in
  the metadata table, so the READONLY guard blocked it — a client could
  never read a replica's non-zero dbs. All three dispatch paths now serve
  SELECT on replicas (connection-state only, Redis parity).

### Fixed — replication round-2 hardening: TEMPORAL.INVALIDATE, replay liveness, blob endpoint checks

- **TEMPORAL.INVALIDATE never replicated** (round-2 finding B): the handler
  drained the graph WAL — the same record mechanism GRAPH.\* replication
  uses — but only fed the local WAL, never the replication plane; a replica
  silently kept `valid_to = ∞` for entities the master had invalidated. The
  master now streams a deterministic, wall-clock-pinned internal form
  (`TEMPORAL.INVALIDATE-AT <graph> <N|E> <entity_id> <wall_ms>`, single-shard
  scope like every replication leg) so master and replica agree on the exact
  `valid_to`; the replica applies it through the same `apply_invalidate` the
  master ran. New e2e REPL-GRAPH-03 proves temporal visibility converges
  (red without the master leg, green with it).
- **Streamed replay could resurrect a tombstoned node** (round-2 finding F,
  regression from the P1-4 lazy-resolver rewrite): the lazy `node_exists`
  accepted DEAD write-buffer entries (`get_node` does not filter
  `deleted_lsn`), so a stray SETPROP for a node removed in an earlier
  streamed replay call re-registered it into the live property index. Split
  into `node_present` (AddNode dedup — any record of the id, matching the
  never-reuse slotmap id contract) and `node_alive` (edge endpoints /
  SETPROP / SETLABEL / REMOVENODE — write-buffer entry is authoritative,
  live-only, matching the old pre-seeded map's `iter_nodes()` semantics).
- **Graph snapshot install now rejects delta edges with unknown endpoints**
  (round-2 finding E, defense-in-depth): `add_edge_across_tiers_with_id`'s
  aliveness check only fires for resident endpoints — a corrupted blob
  referencing a nonexistent node installed silently. The install loop now
  verifies both endpoints against the just-installed segments and drops the
  edge LOUD (`tracing::warn!`) otherwise.
- **WS.\*/MQ.\* writes are NOT replicated in v0.7 — now fail-loud** (round-2
  finding A, known limitation): WS.CREATE/WS.DROP and MQ mutations persist
  durably on the master (WAL) but have no deterministic replication record
  form yet (WS.CREATE mints a fresh UUIDv7 per execution — verbatim
  streaming would diverge). A one-time `tracing::warn!` now fires when such
  a write executes while a replica is attached, instead of silent divergence
  discovered at failover. Full support (id-pinned record forms + replica
  apply arms + snapshot coverage) is tracked as follow-up work, alongside
  the pre-existing Lua-EVAL and expiry/eviction propagation gaps.
### Fixed — listing parity: `INFO # Keyspace` all dbs × all shards, `GRAPH.LIST` all shards

- `INFO`'s `# Keyspace` section always printed a single `db0:` line holding
  the SELECTED db's LOCAL-shard key count — `SELECT 2; SET k v; INFO`
  reported the db-2 count as db0, every other db was invisible, and at
  `--shards N` the other shards' keys were uncounted. It now lists every
  NON-EMPTY logical db (Redis semantics) with `(keys, expires)` summed
  across all shards via a new `KeyspaceStats` scatter (O(#dbs) counter
  reads per shard, no key iteration). All three dispatch paths
  (monoio / tokio sharded / single).
- `GRAPH.LIST` at `--shards N` listed only the connection shard's graphs
  (~1/N of them, since a graph lives on the shard that owns its name). It
  now scatters to every shard and returns the sorted, deduplicated union.

### Fixed — CLIENT TRACKING dead on the monoio runtime (H-3 reorder regression)

- Since the H-3 ACL reorder (#258), `CLIENT TRACKING ON|OFF` answered
  `ERR unknown subcommand 'TRACKING'` on the monoio runtime (the production
  default) at every shard count: `try_handle_client_admin` runs before
  `try_handle_client_tracking` in the frame loop and its unknown-subcommand
  fallback consumed TRACKING before the dedicated handler could see it.
  RESP3 invalidation push was therefore entirely unavailable. Invisible to CI
  because the test matrix exercises the tokio handler (`handler_sharded`),
  whose intercept ordering differs.
- `try_handle_client_admin` now falls through for TRACKING (both handlers are
  post-ACL, so the H-3 deniability guarantee is unchanged). Re-greens all 5
  `client_tracking_invalidation` black-box tests on monoio.

### Fixed — replication exactly-once + txn/graph fidelity (adversarial-review P0/P1)

- **MULTI/EXEC bodies never replicated at `--shards 1`** (P0-1): EXEC persisted
  its body through `persist_txn_aof`'s AOF-only leg — the one local write path
  that skipped the replication plane. A `MULTI/SET/EXEC` committed durably on
  the master and never reached the replica: silent deterministic divergence
  for every application using transactions. The txn body now records each
  entry through the same `record_local_write` leg as single-command writes
  (AOF `lsn = 0`, no double-advance). New e2e
  `replica_applies_multi_exec_bodies` (INCR doubles as a double-apply canary).
- **FULLRESYNC snapshot capture raced undrained local writes** (P0-2): the
  original design queued backlog append + offset advance + live fan-out as ONE
  deferred event-loop message, so a mutation could sit inside the RDB while
  still below the advertised snapshot offset — re-delivered via backlog
  catch-up and double-applied (INCR/LPUSH divergence). `record_local_write`
  now appends the backlog bytes and advances the shard offset SYNCHRONOUSLY
  at write time (atomic with the mutation w.r.t. the inline PSYNC capture);
  only the live replica `try_send` is deferred (`ReplicaLiveFanout`).
  `RegisterReplica` correspondingly carries a push-time offset so catch-up
  and live delivery stay disjoint for every write/attach interleave.
- **Graph snapshot lost soft state that lives outside CSR segments** (P1-5 +
  two adjacent gaps): the CSR byte format has no validity section, `freeze()`
  RETAINS cross-tier delta edges in the write buffer, and copy-up node
  tombstones never freeze — the master recovers all three from its WAL on
  restart, but a replica has no WAL, so it resurrected deleted edges/nodes
  and silently lost every cross-tier edge. Blob format v2 ships a per-segment
  deleted-edge sidecar, the retained delta edges (original edge ids), and the
  dead-shadow list; install re-applies all three.
- **Streamed graph replay was O(N²)** (P1-4): each replicated GRAPH.* record
  re-scanned every write-buffer node and every segment row to pre-seed the
  replay id map — unbounded replication lag on bulk graph loads. Node
  existence is now resolved lazily (O(1) write-buf probe + MPH segment
  lookup), and the id-allocation floor is raised per segment header max
  instead of per row.
- Also: FULLRESYNC graph export skips freezing untouched write buffers
  (P1-6, repeated-resync latency), and the shard self-queue is drained
  unbounded per cycle (P2-7 — entries are cheap try_sends; a cycle cap could
  strand a replica's live bytes by a full tick).

### Fixed — single-shard live replication stream was DEAD (self-SPSC gap)

- **The R0 live stream never actually flowed at `--shards 1`.** The SPSC mesh
  is N·(N−1) with skip-self mapping, so a task on a shard's own thread had NO
  producer to that shard: the inline PSYNC task's `RegisterReplica` failed
  every attach ("shard 0 producer missing") and the replica fell into a 0.5s
  reconnect/full-resync loop. Tests stayed green because each resync's RDB
  carried the latest keyspace + FT defs — data crawled across via snapshot
  polling, masking the dead stream. Fixed with a thread-local self-message
  queue (`shard::self_msg`) drained by the event loop alongside its SPSC
  consumers; PSYNC registration, FT.*/graph fan-out, and local-write fan-out
  all route through it.
- **Local (same-shard) writes now feed the replication plane.** Successful
  local writes push their wire bytes as `ReplicateVerbatim` before any await
  (mutation + record are one synchronous stretch, atomic w.r.t. snapshot
  capture); the drained message does backlog + offset + replica fan-out
  together, and the AOF leg no longer double-advances the offset (lsn = 0 when
  fan-out owns the advance).
- **Replication backlog now seeds at the current shard offset** on lazy
  allocation (`ReplicationBacklog::new_at`) — an unseeded backlog made every
  catch-up range read on a pre-written master fail as "evicted".
- New process-global `fanout_hint_active()` (one Relaxed load, set on first
  replica attach, never cleared) gates all fan-out serialization so
  non-replicating servers pay nothing on the hot path.

### Added — v0.7 graph-plane replication (live stream + snapshot backfill)

- **Live leg:** graph mutations (GRAPH.* + Cypher writes) stream to replicas
  as their deterministic, id-pinned WAL records (`GRAPH.ADDNODE <g> <id> …`;
  label/prop ids are a stateless FNV hash, identical on both sides). The
  replica applies them through the same `GraphReplayCollector` restart
  recovery uses — no id re-allocation, no divergence. Replay's edge/SET
  resolution now also seeds from write-buffer-resident nodes so one-record-at-
  a-time streaming replay resolves endpoints applied by earlier records.
- **Snapshot leg:** the FULLRESYNC RDB carries a `moon-graph-store` aux blob —
  every graph's write buffer is frozen to CSR segments (the checkpoint's own
  "freeze is the only serialization path" contract) and shipped as
  `to_bytes()` encodings + id cursors; the replica installs them exactly like
  restart recovery (`replication::graph_sync`). Mmap (restart-loaded) segments
  export their mapped bytes verbatim.
- **READONLY guard is now Cypher-aware:** a read-only `GRAPH.QUERY`
  (MATCH/RETURN) is served by replicas; only write queries (CREATE/DELETE/
  SET/MERGE tokens) are rejected. Previously the blanket `W` flag rejected all
  GRAPH.QUERY on replicas.
- New e2e `tests/replication_graph.rs`: live-stream parity (nodes, properties,
  GRAPH.LIST) with a zero-reconnect stream-health assertion that would have
  caught the masked dead stream, plus snapshot backfill + post-snapshot live
  growth.

### Fixed — PSYNC attach races closed (adversarial-review findings on R0/R0.5)

- **Registration-bounded catch-up:** the master now registers the replica with
  the event loop BEFORE reading backlog catch-up bytes, and the event loop
  replies with the exact offset where live fan-out begins
  (`RegisterReplica.registered` reply channel). Catch-up sends exactly
  `[snapshot_offset, registration_offset)` — previously a write drained
  between the catch-up read and registration reached neither the RDB, the
  catch-up, nor the live stream: a silent, unlogged replica gap (worst for
  FT.* def mutations, whose fanout always crosses the SPSC queue).
- **Atomic snapshot capture:** the FULLRESYNC snapshot offset is read in the
  same synchronous stretch as the RDB capture (no `.await` between), closing
  the inverse race where a write landed inside the RDB *and* above the
  advertised offset — double-applying non-idempotent commands (INCR) on the
  replica.
- **Backlog eviction during catch-up now aborts the sync loudly** (replica
  retries a fresh full resync) instead of silently skipping the missing bytes.
- A replica whose full resync carries NO index definitions (pre-R0.5 master
  or def-serialization failure) now logs a warning when the authoritative
  replace drops local indexes with no replacement.
- New race-guard e2e `replica_attach_races_live_ft_create` (30 FT.CREATEs
  racing a mid-stream attach, exact index-list parity required).
- Hygiene: `read_moon_aux` validates RDB version bytes like `load_rdb`; the
  FT.* fanout hook skips the serialize+SPSC round trip until a replica or
  backlog exists.

### Added — vector/text index-plane replication sync (v0.7 R0.5)

- **Before this change a replica synchronized only the KV keyspace** — FT index
  definitions never left the master (FT.CREATE/FT.DROPINDEX/FT.CONFIG are
  handled at the connection layer and never reached the replication fanout),
  and the replica's `apply` path ran generic dispatch only, skipping the
  master's auto-index parity hooks. A replica answered `FT._LIST` with nothing
  and `FT.SEARCH` with errors even while its hashes matched the master.
- **Snapshot leg:** the FULLRESYNC RDB now carries the master's vector + text
  index *definitions* as moon-private RDB AUX fields (opcode `0xFA`, keys
  `moon-vector-defs` / `moon-text-defs`), reusing the sidecar codecs
  (`serialize_index_metas_v5`, `serialize_text_index_metas`). Standard RDB
  loaders skip AUX fields, so the snapshot stays Redis-tool-compatible. On
  load the replica drops all local indexes (full resync = authoritative
  replace), installs the master's definitions, and backfills them by
  rescanning matching HASH keys — the same "restart semantics" rescan restart
  recovery performs.
- **Live leg:** successful FT.CREATE / FT.DROPINDEX / FT.CONFIG SET on the
  master now fan out verbatim to the backlog + connected replicas via a new
  `ShardMessage::ReplicateVerbatim` (offset-accounted like any replicated
  write; durability remains the sidecar's job — no WAL/AOF leg). The replica
  applies them through the same `ft_create`/`ft_dropindex`/`ft_config`
  handlers, and runs the master's index-parity hooks after every applied KV
  write (HSET auto-index, DEL/UNLINK tombstone, HDEL vector-field tombstone,
  FLUSHDB/FLUSHALL content clear) so replica indexes track replica keyspace.
- New black-box acceptance test (`replica_syncs_vector_index_defs_and_contents`)
  covering snapshot defs + backfill, live HSET indexing, live DEL tombstoning,
  live FT.CREATE streaming, and FLUSHALL clear-contents-keep-defs semantics.
- **Scope:** single-shard master (matches R0); multi-shard FT.* replication
  rides the R2 broadcast redesign. Graph-plane replication is a separate
  v0.7 workstream.

### Added — replica now applies the replication stream end-to-end (v0.7 R0)

- **Foundational fix**: before this change a `REPLICAOF` replica completed the
  PSYNC2 handshake but applied **nothing** — `run_handshake_and_stream`
  discarded the FULLRESYNC RDB (logged "received … bytes" only) and
  `stream_commands` did `buf.clear()` after advancing the offset. A freshly
  attached replica reported `DBSIZE 0`. This went unnoticed because every
  `replication_hardening.rs` test is `#[ignore]`d and never runs in CI.
- The replica now (1) loads the full-resync RDB snapshot into its local shard
  and (2) parses the live RESP command stream and applies each write, tracking
  `SELECT`. Both runtime variants (monoio, tokio). Apply runs synchronously on
  the shard thread via the thread-local `ShardSlice` (single-shard: every
  command is local, no SPSC self-hop), bypassing the connection-layer read-only
  guard as a replica must.
- Also fixes a wire bug: the replica read the diskless full-resync RDB bulk as
  `len + 2` bytes (assuming a trailing `\r\n` that diskless replication does not
  send), stealing the first two bytes of the command stream and desyncing it.
  Now reads exactly `len`. The replication offset advances by bytes *consumed*
  by complete frames, not the raw socket read count.
- `MOVE` and cross-db `COPY ... DB n` are applied through the same two-db core
  helpers the master's handler-level intercept uses (generic dispatch cannot
  apply them), so they replicate correctly. A replicated command that still
  fails to apply is logged loudly rather than dropped silently.
- New pure, unit-tested stream router (`replication::apply`) and a black-box
  streaming acceptance test (`tests/replication_streaming.rs`, covering
  snapshot + live SET/DEL/MOVE/COPY).
- **Scope:** single-shard (`--shards 1`), logical **db 0**. A multi-shard
  replica refuses to start replication loudly (rather than diverging silently).
  Non-default-db writes are a known follow-up — the master's live fanout does
  not yet stream `SELECT`, so `SELECT n; SET` replicates into db 0. Per-shard
  WAIT/ACK and multi-shard PSYNC build on this in R1/R2.

### Added — `--repl-backlog-size` (Redis `repl-backlog-size` parity)

- New flag: per-shard replication backlog capacity in raw bytes (default
  1 MiB, clamped to the 16 KiB Redis floor). Bounds how far a disconnected
  replica may fall behind and still partial-resync. Previously the capacity
  was hardcoded at three sites (handshake allocation ×2, SPSC lazy fallback)
  and `replication_hardening::full_resync_outside_backlog` failed at spawn
  with `unexpected argument` — the master process never started.
  `ReplicationState.backlog_capacity` is now the single source of truth,
  carried into `ShardMessage::RegisterReplica` for the fallback-init and
  reported truthfully by `INFO replication` `repl_backlog_size`.

### Fixed — `replication_hardening` harness could never complete

- Test teardown used `SHUTDOWN NOSAVE` + `Child::wait()`, but SHUTDOWN is not
  implemented on the production path (the dispatch arm is an error stub and no
  connection handler intercepts it) — every test hung forever at cleanup and
  a mid-test assert failure leaked live servers that wedged later tests'
  ports. Teardown is now a kill-on-drop guard (panic-safe). Implementing a
  real `SHUTDOWN [NOSAVE|SAVE]` is tracked separately.

### Fixed — `txn_kv_wiring` integration test port-collision flake

- `start_txn_server` picked a port from a throwaway `bind(:0)` probe, dropped
  it, then let `run_sharded` rebind it — a TOCTOU window where a parallel test
  (this binary or another) could steal the freed ephemeral port, after which a
  blind 200ms sleep handed the caller a dead/wrong port. Now: a process-global
  reservation set (`reserve_unique_port`) guarantees no two tests in the same
  binary are handed the same recycled port, and an active connect+PING
  readiness probe (`await_server_ready`) replaces the fixed sleep and lets the
  start retry on a fresh port when a bind is lost to another process. Verified
  flake-free across repeated fully-parallel runs (no `--test-threads=1`).

### Fixed — `--disk-offload` without a durability backstop broke the default LRU cache recipe

- **GCP benchmark finding (2026-07-10)**: with `--disk-offload enable` (the
  default) but `--appendonly no` and no `--save`, the durable-spill eviction
  path added to fix the crash window above (`evict_batch_durable_no_aof`)
  never runs, because it needs a `ShardManifest` that is only threaded
  through the tick-driven memory-pressure cascade
  (`shard::persistence_tick::handle_memory_pressure`), itself gated on
  `persistence_dir`, which `main.rs` only constructs when
  `appendonly == "yes" || save.is_some()`. The inline write-path eviction
  gate has no manifest access, so durable spill is impossible from that call
  site — but the pre-fix `manifest is None` branch OOM'd **unconditionally**
  regardless of `--maxmemory-policy`, silently breaking the documented
  "Pure cache (no durability)" recipe
  (`--appendonly no --maxmemory <bytes> --maxmemory-policy allkeys-lru`): a
  classic LRU cache rejected writes at the cap instead of evicting.
- **Behavior fix** (`src/storage/eviction.rs`,
  `try_evict_if_needed_async_spill_with_total_budget`'s `manifest is None`
  branch): made the fail-close policy-aware, matching Redis semantics. When
  `total_memory <= budget` nothing happens; when `policy == NoEviction` it
  still OOMs (correct — noeviction always rejects); otherwise (any evicting
  policy — `allkeys-*`/`volatile-*`) it now reclaims the budget by
  **dropping** victims via `evict_one_with_spill(.., spill: None)` (the same
  plain-drop helper the disk-offload-disabled write path already uses — no
  new victim-selection logic), looping until the budget is satisfied or no
  eligible victim remains (then OOM). No durable spill is attempted here —
  that still only happens via the tick's `evict_batch_durable_no_aof` when a
  manifest is reachable. Three new tests in `src/storage/eviction.rs`:
  `async_spill_no_aof_backstop_no_manifest_allkeys_lru_evicts_pure_cache`
  (the regression test — fails with OOM on pre-fix code, passes after),
  `async_spill_no_aof_backstop_no_manifest_noeviction_still_rejects`
  (unchanged noeviction OOM behavior), and
  `async_spill_no_aof_backstop_no_manifest_volatile_lru_scoped_to_ttl_keys`
  (`volatile-*` only evicts keys with a TTL, never a persistent key). The
  `--appendonly yes` async-spill path and the manifest-reachable durable
  batch-spill path are untouched.
- **Startup warning**: `ServerConfig::disk_offload_spill_inert` (new
  predicate, `src/config.rs`) plus
  `ServerConfig::warn_disk_offload_without_durability` (mirrors
  `warn_deprecated_cold_tier_flags`) emit a single `tracing::warn!` at
  startup when `--disk-offload enable` + no durability backstop is
  detected, called from `main.rs` right after the existing cold-tier-flags
  warning: disk-offload's cold-spill tiering is still inert in this
  combination (evicting policies drop eligible victims instead of tiering;
  `noeviction` — and any evicting policy with no eligible victim left, e.g.
  `volatile-*` once its TTL-bearing keys are gone — returns OOM). Documented
  in `docs/guides/tuning.md`'s "Tiered
  memory offload" section.

### Fixed — test flakiness: oom_bypass_closure readiness on loaded CI runners

- `tests/oom_bypass_closure.rs`: the readiness path used a fixed 30s connect
  deadline with no CI allowance and no dead-child detection —
  `test_case_e_cross_db_copy_oom` lost that race on a loaded macOS CI runner
  (2026-07-10). Same remedy as the sigterm/bgsave harnesses (#264):
  `wait_ready` now fails fast via `try_wait()` when the server process has
  exited (surfacing `moon.stderr.log`/`moon.stdout.log` tails instead of
  burning the deadline), retries in bounded 1s connect windows, and extends
  the deadline 30s → 120s under `CI`.

### Fixed — CI: fts_query_parse missing from fuzz matrix

- `.github/workflows/fuzz.yml`: the `fts_query_parse` fuzz target was
  registered in `fuzz/Cargo.toml` but absent from both the PR and nightly
  CI matrices, so it never ran. Added to both. Also corrected CLAUDE.md's
  stale fuzz-target count (7 → 12) and target list.
### Fixed — RDB stream consumer-group allocation-DoS gap (`src/persistence/rdb.rs`)

- **Four more untrusted-length counts in the native RDB `TYPE_STREAM` decoder
  were unvalidated**, the same vulnerability class fixed for
  `snapshot.rs`/`redis_rdb.rs` above: `read_entry` and its zero-copy twin
  `read_entry_zero_copy` (`src/persistence/rdb.rs`) read a stream's
  `group_count`, `pel_count`, `consumer_count`, and `pending_count` straight
  off the wire without bounding them against the bytes actually remaining in
  the cursor, unlike every sibling count in the same functions
  (`entry_count`, `field_count`, hash/list/set/zset counts), which already
  went through `rdb::validate_count`. None of the four drives an eager
  `with_capacity`/zero-fill today (they grow a `BTreeMap`/`HashMap`
  incrementally), so this wasn't a single-allocation DoS, but it left a
  structural inconsistency that a future refactor could easily turn into
  one and skipped the fail-fast rejection every other count gets. Fixed by
  adding `validate_count` calls with the true structural minimum bytes per
  item (28 for a group: 4-byte empty name + 16-byte last-delivered-id +
  4-byte pel_count + 4-byte consumer_count; 36 for a PEL entry: 16-byte
  StreamId + 4-byte empty consumer name + 16-byte delivery time/count;
  16 for a consumer: 4-byte empty name + 8-byte seen_time + 4-byte
  pending_count; 16 for a pending id: StreamId) — chosen so no legitimate
  file is ever rejected. Red/green TDD: 4 crafted-blob tests (one per
  count) confirmed failing before the fix and passing after, plus a control
  test asserting a fully-populated 1-group/1-pel/1-consumer/1-pending
  stream is still accepted by both `read_entry` and `read_entry_zero_copy`.
### Fixed — KV disk-offload: make eviction spill durable before dropping the hot value

- **Crash window** (`src/storage/eviction.rs`): under memory pressure with
  `--disk-offload enable`, the async-spill eviction path (`evict_one_async_spill`,
  driven from the shard event loop's memory-pressure tick) queued the
  `SpillRequest` to the background `SpillThread` and immediately dropped the
  hot value from RAM — before the pwrite, fsync, or manifest commit had
  happened. Under `--appendonly yes` this is safe (a crash in that window is
  covered by AOF replay of the original write), but under `--appendonly no`
  there is no AOF to fall back on: the value existed nowhere (not in RAM, not
  yet on disk, no WAL/AOF record) for the entire `SpillThread` batching
  window (up to 256 entries or its 100ms tick, longer under backpressure). A
  kill -9 in that window was unrecoverable, silent data loss.
- **Fix**: `try_evict_if_needed_async_spill_with_total_budget` now branches on
  `--appendonly`. With an AOF backstop the fast fire-and-forget path
  (`evict_one_async_spill`, queue-then-drop over the `SpillThread` channel) is
  unchanged (documented, intentional trade-off). Without one, it drives a new
  `evict_batch_durable_no_aof`: collects up to 256 victims (mirroring
  `SpillThread`'s own `FLUSH_ENTRY_CAP`) *without* removing them from RAM,
  writes the whole batch as one durable, synchronous call to
  `spill_thread::flush_buffer` (pwrite + fsync + manifest commit — the same
  inline/oversized page routing the background thread itself uses, now made
  `pub(crate)` for reuse), and only *then* removes from RAM the keys whose
  file both pwrite-succeeded and manifest-committed, immediately populating
  `ColdIndex` so each stays read-through-able (previously only a later
  `SpillCompletion` — which never arrives on this path — did that). Batching
  is required, not optional: an earlier draft did one fsync per evicted key
  and stalled the single-threaded shard event loop under sustained pressure.
  `try_evict_if_needed_async_spill_with_total_budget` gained an
  `Option<&mut ShardManifest>` parameter to carry the manifest down; the
  memory-pressure cascade (`handle_memory_pressure` in
  `src/shard/persistence_tick.rs`) is the one call site that has a manifest
  and passes it through. The four other callers (inline per-connection
  write-path gate, sharded/monoio handlers, `spsc_handler`, the Lua scripting
  bridge) have no manifest reachable; under `--appendonly no` they now bail
  (retain the hot value, surface OOM) instead of risking the same crash
  window — the next 100ms memory-pressure tick reclaims the memory via the
  durable path instead. **Trade-off**: those inline call sites no longer
  evict opportunistically under `--appendonly no` (that unconditional
  fast-path eviction, regardless of durability, was the bug being fixed), so
  a write burst that crosses `maxmemory` now converges to budget only as fast
  as the 100ms tick reclaims it, evicting the minimum needed each pass rather
  than eagerly on every write — verified live (per-shard `estimated_memory()`
  accounting, not RSS) to converge correctly and stay converged.
- **Second bug, same file** (`evict_one_with_spill`, the legacy fully-sync
  spill path used when no `SpillThread` exists): on a spill I/O error it
  logged a warning and *still* evicted the key from RAM — unconditional data
  loss on every spill failure, independent of any crash. Now returns without
  evicting on failure, matching the async path's fail-closed contract.
- **Regression coverage**: `sync_spill_failure_retains_hot_value_no_silent_drop`,
  `async_spill_no_aof_backstop_durably_spills_before_drop` (locks the durable
  pwrite+fsync+manifest-commit-before-drop ordering and immediate
  `ColdIndex` availability), `async_spill_no_aof_backstop_no_manifest_retains_hot_value`,
  `async_spill_no_aof_backstop_spill_failure_retains_hot_value` — plus a
  `db.remove()` (DEL) check after the new durable-spill path to guard against
  reintroducing the DEL/cold-tombstone resurrection class (#257): an earlier
  draft of this fix inserted into `ColdIndex` *before* `db.remove()`, and
  `Database::remove`'s own cold-tier cleanup silently wiped the insert back
  out — caught by this test suite, not by inspection.

### Fixed — test flakiness: sigterm-shutdown readiness + BGSAVE file polling

- `tests/sigterm_shutdown.rs`: `wait_for_ready` now distinguishes a crashed
  child (fails fast via `try_wait()` instead of burning the full deadline)
  from a genuinely slow-to-start one, and every readiness panic surfaces the
  captured `moon.stdout.log` / `moon.stderr.log` so a real failure stays
  diagnosable. The readiness deadline also doubles under `CI` (60s → 120s) —
  the observed flake was macOS CI runner startup latency under load, not a
  hang in the SIGTERM path under test.
- `tests/integration.rs`: `test_bgsave_creates_rdb_file` and its two siblings
  that shut down and restart a server right after `bgsave_with_retry`
  (`test_rdb_restore_on_startup`, `test_aof_priority_over_rdb`) asserted
  `dump.rdb` existence off a fixed 200ms guess-sleep — BGSAVE queues the
  write on a `spawn_blocking` thread and replies before it lands on disk.
  Added a `wait_for_file` bounded-poll helper (50ms interval, 10s deadline)
  and used it at all three call sites in place of the point-in-time
  assumption. The MOON-magic-bytes assertion is unchanged.
### Fixed — CI: nightly fuzz shard (`graph_props_record`) + stale `deny.toml` comment

- **Missing fuzz harness**: `fuzz/Cargo.toml` and `.github/workflows/fuzz.yml`
  both declared a `graph_props_record` fuzz target, but
  `fuzz/fuzz_targets/graph_props_record.rs` didn't exist — that matrix shard
  failed every nightly run (audit finding, `docs/PRODUCTION-CONTRACT.md`
  FUZZ-01). The target function is real: `src/graph/csr/props.rs`'s module
  doc even names it (`fuzz target: graph_props_record`) — the v5 node/edge
  property-record codec (`decode_node_props`, `decode_node_prop`,
  `decode_node_embedding`, `node_record_len`, `decode_edge_weight`,
  `decode_edge_props`, `decode_edge_prop`, `edge_record_len`) decodes bytes
  read straight off a `.csr` segment file and is documented to return
  `None`/empty on malformed input rather than panic. Added the missing
  harness so the shard actually fuzzes it, following the exact pattern of
  the sibling targets (`wal_v3_record.rs`, `gossip_deser.rs`).
- **Stale comment**: `deny.toml`'s header claimed a `ci.yml` "safety-audit"
  job runs `cargo audit`/`cargo deny`; no such job exists. The actual "Lint"
  job runs `scripts/audit-unsafe.sh` + `scripts/audit-unwrap.sh` (SAFETY-
  comment coverage / unwrap ratchet) — a different, unrelated check.
  Corrected the comment to describe reality and left a `TODO(ci):` marking
  the gap (wiring `cargo deny check` into CI is a separate decision, out of
  scope here).
### Changed — Tiering: drop unused meta/undo placeholder files

- `transition_to_warm` (HOT->WARM segment seal) no longer writes `meta.mpf`
  (VecMeta) or `undo.mpf` (VecUndo) — both were always-empty placeholders with
  zero readers anywhere in the codebase (the WARM load path
  `WarmSearchSegment::from_files` only ever opens `codes.mpf`/`graph.mpf`/
  `mvcc.mpf`/optional `vectors.mpf` by name). Removes 2 extra
  `File::create` + fsync round-trips per WARM segment seal. `write_meta_mpf`
  and `write_undo_mpf` (`src/vector/persistence/warm_segment.rs`) are removed
  as they had no other callers.
- `src/vector/persistence/segment_io.rs`: removed the dead
  `sq_vectors.bin`/`f32_vectors.bin` comment-only branch (never emitted since
  SQ8/f32 storage was dropped from `ImmutableSegment` in favor of TQ-ADC) and
  corrected the module doc header, which still listed both files as if
  written.
- Back-compat: old WARM segments written by a prior build that still contain
  `meta.mpf`/`undo.mpf` continue to load — the loader opens files by name and
  never scans the segment directory, so the extra files are silently ignored
  (verified by a new regression test with stray legacy files on disk).
### Fixed — RDB DoS hardening

- **Two untrusted-input allocation-DoS vectors in RDB/snapshot loading are
  now bounds-checked.** `shard_snapshot_load`'s `SEGMENT_BLOCK_MARKER`
  branch (`src/persistence/snapshot.rs`) read an untrusted `entry_count:
  u32` and passed it straight to `Vec::with_capacity` with no gate; the
  Redis-RDB reader (`src/persistence/redis_rdb.rs`) did the same for
  `read_redis_string`'s length-prefixed byte buffer and for the
  list/set/hash/zset collection decoders, where `read_length`'s 8-byte
  (`0x81`) form can return up to `u64::MAX`. Both are on the server-startup
  / replica-full-sync path, so a single crafted or corrupt file could drive
  a multi-gigabyte allocation before a byte of the claimed data was read —
  aborting the process (release builds use `panic = "abort"`) or OOM-killing
  it. Fixed by promoting `rdb::validate_count` to `pub(crate)` and calling it
  before the snapshot segment's `Vec::with_capacity`, and by adding a
  matching `check_alloc_bound` helper in `redis_rdb.rs` that bounds every
  untrusted length/count against the bytes actually remaining in the input
  before allocating. No wire-format or valid-file behavior changes — a
  legitimate length/count always fits within the remaining input.
### Removed — Experimental DiskANN COLD vector tier

- **Deleted the experimental "COLD-ann" vector tier** (`src/storage/tiered/cold_tier.rs`,
  the whole `src/vector/diskann/` module — Vamana graph, product quantizer,
  co-located page format, io_uring beam search). It was gated off by default
  (`MOON_VEC_COLD_TIER=1`), incomplete by its own docs (no cold-segment
  deletion, ADC-only recall, no restart-time PQ-codebook reload), and had no
  restart recovery story. The M3-exit review decided delete-over-finish. The
  default COLD valve remains `SegmentList.unloaded` (`UnloadedSegment`,
  exact, near-zero RAM, reload-on-touch) plus WARM byte-budget LRU eviction
  (`--vec-warm-mmap-budget`) — neither is affected by this removal.
- Removed the dead call chain: `VectorIndex::try_cold_transitions[_all]`,
  `VectorStore::register_cold_segments`, the COLD 60s event-loop timer arm
  (tokio + monoio), `cold_tier_experimental_enabled()`, cold-segment manifest
  discovery in `src/persistence/recovery.rs` (`RecoveryResult::cold_segments`
  / `cold_segments_loaded`), and the `snap.cold` fan-out in `FT.SEARCH` /
  `FT.INFO` / `SegmentHolder::resident_bytes`/`total_vectors`.
- `--segment-cold-after`, `--segment-cold-min-qps`, `--vec-diskann-beam-width`,
  `--vec-diskann-cache-levels` are kept as **parseable, inert no-ops** (rather
  than a hard CLI break) so existing `moon.conf` files/launch scripts do not
  fail to start; a startup `tracing::warn!` fires once if any is set away
  from its historical default (`ServerConfig::warn_deprecated_cold_tier_flags`,
  called from `main.rs`).
- `StorageTier::Cold`/`Archive` (the shared on-disk tier-byte enum in
  `src/persistence/manifest.rs`, format §4.3) are left in place — they are
  part of the stable `FileEntry.tier` wire format and shared with the
  unrelated KV disk-offload subsystem's `ColdIndex`/`sweep_orphans` (which
  this change does not touch).

### Fixed — Vector: WARM segments now survive a restart, and stop leaking superseded segment directories

- **Restart-recovery dead wiring fixed.** `VectorStore::register_warm_segments`
  had zero non-test callers: `Shard::restore_from_persistence`'s v3 recovery
  pass discovered WARM segments from the manifest (`RecoveryResult.warm_segments`)
  into a throwaway `Shard`-owned `vector_store` that `event_loop.rs` discards
  wholesale in favor of the live `ShardSlice.vector_store` — so the discovery
  result was thrown away with it, and WARM's RSS win evaporated on every
  restart. Fixed by staging the discovered segments on a new
  `Shard::recovered_warm_segments` field and draining them into the live
  store via `register_warm_segments` right after B3 recovery
  (`RecoveryState::finish`) completes in `event_loop.rs`.
- **Superseded segment-directory leak fixed.** `VectorIndex::try_warm_transitions_idle`
  never told Stack B's durability manifest (`vector/persistence/manifest.rs`)
  that a segment had left the immutable tier, so the old
  `idx-<hex>/segment-<old_id>/` directory it left behind was never garbage
  collected — permanent disk growth on every HOT→WARM/COLD transition, and
  (independently) the reason a restart used to silently reload the segment
  as a fully-materialized HOT copy of stale data instead of respecting the
  WARM tier it had just been moved to. Fixed by calling the existing
  `persist_hook_after_install` durability hook after every transition; its
  manifest diff (`run_snapshot_job`) now GCs the superseded directory the
  same way a normal compact/merge already does.
- `VectorIndex::persist_hook_after_install` changed from `&mut self` to
  `&self` so `try_warm_transitions_idle` (which only holds `&self`) can call
  it; `next_snapshot_seq` changed from `u64` to `AtomicU64` to keep
  `VectorIndex`/`VectorStore` `Sync` (`fetch_add`, `Relaxed` — every actual
  writer is still the single owning shard thread, so this only needs to be a
  valid value, not a cross-thread synchronization point).
- COLD (`SegmentList.unloaded`, the reload-on-touch stub tier) is
  unaffected in the case that already worked (WARM→COLD idle transitions —
  those segments were never in Stack B's `segment_ids` to begin with) and
  loses only an accidental, undocumented side effect in the other case
  (HOT→COLD-direct transitions used to reload as stale HOT on restart due to
  the same leak this fixes) — it still has no dedicated restart-recovery
  path of its own; a restart now correctly falls through to the existing
  dedup-rescan/AOF-replay self-heal instead (same fallback as any index with
  no durable manifest state), never silent data loss.
- New TDD regression test (`vector::persistence::recover_v2::tests::
  warm_transition_leak_fix_and_restart_recovery`) proves, on real
  force-compacted on-disk state: (a) the superseded segment directory is
  GC'd within a bounded wait, (b) a simulated restart does NOT reload it as
  a phantom HOT segment, (c) `register_warm_segments` reattaches it as WARM,
  and (d) post-restart search returns the identical top-1 `global_id` as the
  pre-transition baseline — no recall regression. Verified red (fails with
  the leak-fix call removed) before green.

### Fixed — Vector: hand-review found `register_warm_segments` could permanently duplicate documents or attach a segment to the wrong index

- **CRITICAL — crash-before-GC restart could leave the same vectors live
  twice, forever.** `try_warm_transitions_idle` commits Stack A's shard
  manifest durably (synchronous, fsync+commit) *before*
  `persist_hook_after_install` merely schedules the async Stack B snapshot
  job. A `kill -9` landing in that window left the old segment still
  tracked by Stack B (reloaded as an ordinary HOT/immutable segment by
  `recover_v2`) *and* the warm copy discovered by Stack A (reattached WARM
  by the restart-recovery wiring above) — the same key_hashes live in two
  segments. `search_mvcc`'s merge (`src/vector/segment/holder.rs`,
  `all.sort_unstable(); all.truncate(k);`) has no key_hash dedup, so this
  never self-healed: duplicate keys in `FT.SEARCH` results, inflated
  `num_docs`, and the next snapshot re-adopted the reloaded HOT copy into
  `segment_ids` forever. Fixed: `register_warm_segments` now checks a warm
  segment's own key_hash set (`warm_search::peek_key_hashes`, a cheap
  `mvcc.mpf`-only read — no codes/graph mmap, no `CollectionMetadata`
  dependency) against its owning index's *current in-memory*
  `key_hash_to_key`. If those keys are already covered by a live HOT
  segment, Stack B wins: the warm copy is left unattached and its on-disk
  directory is deleted (`std::fs::remove_dir_all`) instead of being
  registered, so Stack B stays the single source of truth.
- **HIGH — a warm segment could attach to the wrong index.** The
  restart-recovery wiring above made a previously-dead code path live:
  `register_warm_segments` attached each segment to the *first* index for
  which `WarmSearchSegment::from_files` happened to succeed —
  `from_files` accepts any caller-supplied `CollectionMetadata` and never
  validates it against the on-disk codes/graph, so two indexes of the same
  dimension/quantization made the outcome a coin flip (wrong collection,
  wrong search results, no error). Fixed: ownership is now decided from
  key/keymap evidence, never from `from_files` success. Each candidate
  index's *persisted* Stack-B keymap (read straight off disk via a new
  `read_manifest_and_keymap_consistent` helper — bounded retry with
  backoff against the benign TOCTOU where a concurrent `run_snapshot_job`
  advances the epoch and GCs the old keymap file between the manifest read
  and the keymap read) is checked for key_hash overlap with the segment;
  the index with the most matches wins. No match, or a tie between two
  indexes, leaves the segment unregistered (files intact, logged via
  `tracing::warn!`) rather than guessing.
- Two new TDD regression tests in `vector::persistence::recover_v2::tests`:
  `warm_reattach_dedups_against_crash_before_gc_race` stages the exact
  crash-before-GC state (rolls the on-disk manifest/segment/keymap back to
  pre-transition *after* waiting for the real background GC to land, so the
  rollback is deterministically the last write) and asserts zero duplicate
  `global_id`s in search results, `warm.len() == 0`, and the superseded
  warm directory is deleted; `warm_reattach_picks_correct_index_among_same_dim_indexes`
  builds two same-dimension indexes with disjoint keysets — where the old
  first-match behavior would have funneled both segments into one index
  regardless of `HashMap` iteration order — and proves each segment lands
  on its true owner via an end-to-end recall check (a cross-attached
  segment would still pass a bare `warm.len() == 1` count but return the
  wrong top-1 result). Both verified red (fail at the exact assertion the
  fix satisfies) against the prior "first successful `from_files`"
  implementation before green.

### Fixed — Vector: WARM restart recovery was still defeated on every NORMAL restart (not just a crash) by call-site ordering

- **CRITICAL — round 2 of hand-review found the previous fix's own wiring re-triggered its own duplication check.** The full production B3 sequence is `create_index` -> keyspace dedup rescan (`reconcile_key`, once per matching HASH key) -> `finish` -> `register_warm_segments`. `load_segments_and_keymap` never populates `key_hash_to_key`/`key_hash_to_global_id` for WARM keys (see its `segment_resident` gate — nothing has attached the WARM segment to `idx.segments` yet at that point in `create_index`), so with `register_warm_segments` running LAST, the rescan saw every WARM key as unknown and re-encoded it into the mutable segment (full HNSW/TQ rebuild, not just metadata) on every boot. The duplication check added in the previous fix then saw those just-re-indexed keys as "already covered by a live HOT copy" and retired (permanently deleted) the WARM segment — the exact failure this feature exists to prevent, triggered by the fix's own ordering, on every normal restart. Fixed: `VectorStore::register_warm_segments` now runs immediately after the sidecar `create_index` loop, BEFORE the keyspace rescan (`event_loop.rs`) — ownership decisions only need every sidecar index to already exist, which they do by then.
- **On a clean attach, `register_warm_segments` now populates `key_hash_to_key`/`key_hash_to_global_id`/`key_hash_to_vec_checksum`** for the segment's keys, sourced from the same owner-evidence persisted-keymap entries already read for the ownership decision (no extra disk read). This is what lets the rescan (which runs right after) see the keys as known/checksum-unchanged instead of re-encoding them, and lets `FT.SEARCH` resolve a WARM doc's real key bytes via `key_hash_to_key` (the exact map `resolve_hybrid_doc_key` consults) instead of falling through to a synthetic `vec:<id>`. A segment key_hash absent from the owner's persisted keymap (an earlier async-snapshot job never committed for it — the same class of race as the crash-before-GC finding, at per-key rather than per-segment granularity) is left out of the maps, so the rescan self-heals it into mutable, and is tombstoned in the warm copy for that one key_hash (`WarmSearchSegment::seed_tombstones`) so the stale warm copy and the freshly re-indexed mutable copy never coexist as live duplicates.
- **`RecoveryState`'s deletion-probe baseline moved to a new explicit phase, `snapshot_recovered_baseline`.** Previously `create_index` snapshotted the baseline immediately (before any WARM segment existed in-memory), which — now that `register_warm_segments` populates WARM keys into `key_hash_to_key` — would have permanently excluded every WARM key from `finish()`'s deletion probe: a WARM key whose underlying HASH was deleted while the server was down would never be recognized as "used to exist, now doesn't," and would silently survive as a stale search hit forever. `event_loop.rs` now calls `snapshot_recovered_baseline` once, after BOTH `create_index` (all indexes) AND `register_warm_segments` have settled, and before the keyspace rescan begins — the baseline is then complete for the deletion probe to catch a delete-while-down WARM key too (`VectorIndex::mark_deleted_for_key_in_index` already tombstones across every tier including WARM, so once the probe fires the fix is mechanical).
- New TDD integration-level regression test,
  `vector::persistence::recover_v2::tests::warm_tests::warm_recovery_full_production_sequence_matches_real_boot_order`,
  drives the FULL real sequence in order (`create_index` -> `register_warm_segments`
  -> keyspace rescan, replaying every key except one to simulate a delete-while-down
  -> `finish`) and asserts: (a) the WARM segment attaches and is not retired, (b) every
  still-live WARM key is counted `verified_unchanged` (0 `re_indexed`, nothing lands in
  the mutable segment), (c) search resolves the ORIGINAL key bytes for a WARM doc via
  `key_hash_to_key` (not `vec:<id>`), (d) no duplicate `key_hash` across results, (e) the
  delete-while-down key is tombstoned by the deletion probe and absent from search.
  Verified red by hand (temporarily moving `register_warm_segments` back to run after
  the rescan/`finish`, matching the prior ordering): (a) fails first — the WARM segment
  is retired as a false-positive duplicate (`warm.len() == 0`) instead of attaching,
  which also makes (b)/(c) unreachable. The three existing WARM tests from the previous
  fix (`warm_transition_leak_fix_and_restart_recovery`,
  `warm_reattach_dedups_against_crash_before_gc_race`,
  `warm_reattach_picks_correct_index_among_same_dim_indexes`) and one pre-existing
  deletion-probe test (`recover_finish_tombstones_keys_missing_from_rescan`) were
  updated to call `register_warm_segments`/`snapshot_recovered_baseline` in the new
  production order so they keep testing real behavior instead of a now-stale sequence;
  two of them needed a full keyspace-rescan replay added (previously implicit/absent)
  to avoid the deletion probe wrongly tombstoning their now-baseline-visible WARM keys.
  `crash_recovery_vector_durability.rs`'s six real-server-spawning scenarios (`s1`-`s6`)
  were re-run against a release-fast build as an additional non-WARM regression check
  on the reordered `event_loop.rs` sequence — all pass unchanged.
- `vector::persistence::recover_v2`'s test module, having grown past the 1500-line file
  size guideline with these additions, was split: `recover_v2_tests.rs` keeps the
  non-WARM B3 recovery tests + shared helpers, and a new nested
  `recover_v2_warm_tests.rs` (`#[path]`-included as `mod warm_tests` from within
  `recover_v2_tests.rs`) holds all WARM-specific tests.

### Docs — Roadmap: native `moon://` / `moons://` connection URI scheme

- Define Moon's native connection URI scheme in `docs/roadmap/ROADMAP.md` as a
  superset of Redis's `redis://` / `rediss://` (`moons://` = TLS 1.3 variant,
  parity with `rediss://`). Adds the full cross-cutting spec (§8.5), the v0.6.1
  spec-doc task (H-7), the v0.7.0 implementation workstream (R6), and a Protocol
  node in the features mindmap. `redis://` / `rediss://` remain fully supported;
  the native scheme adds a `?workspace=<tenant>` selector and a fail-fast,
  no-opportunistic-downgrade TLS contract.

### Docs — `moon://` / `moons://` connection URI scheme spec (H-7, PR #261)

- New `docs/protocol/moon-uri.md`: the authoritative, doc-only spec for Moon's native
  `moon://` / `moons://` connection URI scheme — ABNF grammar, a field-by-field
  `redis(s)://` parity table, a query-parameter reference (parity + Moon-native
  `?workspace=`), the design-for-failure section (no opportunistic downgrade, no
  auto-upgrade, fail-fast diagnostic text, unknown-scheme parse errors, bounded
  connect), server-participation semantics (`--announce-url`, `INFO replication`,
  cluster redirects, `REPLICAOF`), worked examples, and the conformance checklist the
  v0.7.0 Workstream R6 implementation + `tests/uri_scheme.rs` must satisfy. No Rust
  code changes — implementation is tracked separately as v0.7.0 R6.
- Verified every cited flag/module against the current tree (`--tls-port` et al. in
  `src/config.rs`, TLS 1.3/rustls/aws-lc-rs + SIGHUP hot-reload in `src/tls.rs`, `WS
  AUTH` UUID-only argument in `src/command/workspace.rs`) rather than restating the
  roadmap's prose uncritically; flags the `WS AUTH` UUID-vs-name gap for the R6
  implementer as an open decision instead of assuming it away.
- `mkdocs.yml`: adds a "Protocol" nav section for the new page so `mkdocs build
  --strict` doesn't fail on an orphan doc.

### Fixed — Cold-tier proactive TTL reclaim (H-2, R1: tmp/OFFLOAD-COMPRESSION-REVIEW.md)

- **TTL-expired disk-offload cold entries that are never re-read no longer
  leak forever.** The on-read reclaim path (`cold_read.rs`) only reclaimed an
  expired `ColdIndex` entry when a caller issued a `GET` against it; a key
  that expired and was never touched again (the flagship offload use case —
  TTL'd sessions, caches) permanently leaked its index entry (RAM, full key
  bytes) and pinned its backing DataFile's refcount (disk), because nothing
  else in the system ever inspected a cold entry's TTL.
- `ColdLocation` now carries `ttl_ms: Option<u64>`, a cached copy of the
  on-disk `KvEntry::ttl_ms` populated at spill time and re-derived fresh by
  `rebuild_from_manifest` on every restart — so the sweep can judge expiry
  from the in-RAM index alone, without a pread of the cold file. No on-disk
  format changed and `ColdIndex` has no serialized form of its own, so there
  is no index format to version and no old file that could fail to load.
- New `ColdIndex::sweep_expired` runs in the same tick as the existing
  cold-tier orphan sweep (`--cold-orphan-sweep-interval-secs`, default 5
  minutes), bounded to `MAX_EXPIRED_SWEEP_BATCH` (4096) entries per call so
  an expiry storm against a large cold index cannot stall the shard event
  loop; any remainder is picked up by the next tick. Batch-file colocation is
  preserved exactly like the orphan sweep (an expired key never drags down a
  co-located live key's file).
- New `INFO` fields `reclamation_cold_expired_reclaimed_total` /
  `reclamation_cold_expired_bytes_reclaimed_total` (distinct from the
  existing orphan counters, which count any zero-ref file unlink regardless
  of cause).

### Docs — H-4 doc reconciliation (ROADMAP §8.1)

- **`docs/redis-compat.md`**: `FT.AGGREGATE` was listed as "Not implemented"; it is
  actually implemented and dispatched (`src/command/vector_search/ft_aggregate.rs` /
  `src/text/aggregate.rs`, wired through `src/command/mod.rs` and the
  single/sharded/monoio `FT.*` handlers on both read and write paths). Corrected to
  "Implemented", noting the `APPLY`-stage v1 stub, the FILTER-clause no-op, and that
  it has no `phf` `COMMAND_META` entry by design (so `COMMAND INFO`/`COMMAND DOCS`
  return nothing for it even though it works).
- **`docs/comparison-valkey.md`**: "Hash field expiration — `HEXPIRE`/`HTTL` family
  Moon does not implement" was stale — Moon has shipped the full family (`HEXPIRE`,
  `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`,
  `HPEXPIRETIME`, `HPERSIST`, `HGETDEL`, `HGETEX`) with full read+write dispatch and
  WAL replay since before this document's last refresh. Moved from "Valkey wins
  decisively" to "Effective parity" (Moon trails Valkey by 4-10% on these ops per
  `docs/perf/2026-05-27-hash-ttl-3way-bench.md` — a tracked perf gap, not a missing
  feature); also dropped it from the cluster-features risk bullet in §4, since it is
  unrelated to cluster mode.
- **`SECURITY.md`**: supported-versions table said "0.1.x" (stale since well before
  v0.6.0). Updated to the current v0.6.x line, with an explicit pre-1.0 support
  policy (latest-minor-only; no LTS/backport promise until the v1.0.0 GA tag, per
  `docs/roadmap/ROADMAP.md`'s stated 18-month LTS policy).

### Docs — Production contract refresh + CI gate (ROADMAP H-5)

- **`docs/PRODUCTION-CONTRACT.md` refreshed from its stale v0.1.3-era draft to
  v0.6.0 reality** and converted into a checked, evidence-linked ledger (56
  GA-blocking rows across toolchain/CI, correctness hardening, durability,
  replication & HA, cluster mode, security, observability, compatibility, and
  release engineering). Every ✅ row cites a real file/CI job/script that was
  read and confirmed during this pass — no aspirational ticks. 33/56
  GA-blocking rows are shipped today; 22 are honestly unticked (multi-shard
  replication, `WAIT`/keyspace-notifications/`MONITOR`, monoio cluster
  wiring, encryption at rest, and others already tracked in
  `docs/roadmap/ROADMAP.md` §8). The verification pass also surfaced two
  previously undocumented defects, left unticked rather than assumed fixed:
  `deny.toml`'s header comment claims a `ci.yml` "safety-audit" job that does
  not exist (`cargo audit`/`cargo deny` are not CI-blocking), and
  `fuzz/Cargo.toml` + `.github/workflows/fuzz.yml` both reference a 12th fuzz
  target (`graph_props_record`) whose source file is missing from the tree,
  so that nightly fuzz matrix shard fails every run.
- **New `scripts/check-production-contract.sh`** (grep-based, mirrors
  `scripts/audit-unsafe.sh`'s style): reports unticked GA-blocking ledger rows
  on every tag, and hard-fails only on a `v1.0*` tag if any remain unticked.
  Wired into `.github/workflows/release.yml`'s `setup` job alongside the
  existing RELEASES.md release-ledger gate, using the same env-var
  interpolation pattern (`VERSION: ${{ steps.ver.outputs.version }}`, never
  inline `${{ }}` in a `run:` body).

### Changed — Memory: vector tiering accounting spine (M1, tiering-v2 D3/D9)

- **`--maxmemory` now counts vector segment memory.** The background eviction
  check compared only KV bytes against the per-shard budget, so a pure-vector
  workload could drive RSS to OOM while eviction reported "under budget".
  `timers::run_eviction` now gates on the shard AGGREGATE — Σ all dbs' KV +
  the shard's published vector bytes, computed once per 100ms tick — and
  evicts across dbs only until the aggregate is back under budget (adversarial
  review caught the initial per-db formulation, which both under-detected with
  KV spread across dbs and over-evicted sibling dbs). When the un-evictable
  vector term alone exceeds the budget, KV drains then errors OOM
  (shared-budget semantics; per-db quotas remain the tenant-isolation
  mechanism); the pressure cascade (which shrinks vectors via offload) fires
  earlier at `--disk-offload-threshold`, so with disk-offload enabled vectors
  shed first. **Known limitation:** the on-write eviction gate still checks
  KV-only, so under `noeviction` a vector-heavy shard over the vector-aware
  budget does not yet reject client writes — RSS is bounded by the pressure
  cascade and the RSS watchdog instead (write-gate consistency is a tracked
  M1 follow-up).
- **Elastic budget classification is vector-aware.** A vector-heavy/KV-light
  shard was misclassified as an idle donor, lending headroom to siblings while
  its true footprint was over base — and the pressure cascade compared a
  vector-inclusive used-term against a budget inflated by that donation. The
  donor/hot snapshot now sums KV + vector per shard; a vector-heavy shard is
  classified hot and borrows instead of donating.
- **`--vec-warm-mmap-budget` is now an instance-total cap divided across
  shards** (matching `--maxmemory` semantics). Each shard previously applied
  the full value — an N-shard instance silently allowed N× the configured WARM
  memory. **Behavior change:** multi-shard deployments relying on the old
  per-shard meaning should multiply their flag value by the shard count. A
  nonzero total floors at 1 byte/shard ("0" still disables enforcement).
- **IVF and DiskANN-cold segments report real `resident_bytes()`.** Both tiers
  contributed a hardcoded 0 to the roll-up (untracked RAM: IVF centroids +
  posting lists; DiskANN PQ codes + codebook). They now feed the pressure
  trigger, MEMORY DOCTOR, and Prometheus like every other tier.
- **`INFO reclamation_mmap_warm_bytes` reports the live WARM counter.** The
  field read a never-incremented local static (permanent 0) while the real
  `MmapBudget` counter was write-only. It now reads the live counter, and a
  new `reclamation_mmap_budget_evictions_total` field exposes cumulative
  byte-cap evictions.
- **D9 (DiskANN retention) quarantine:** config docs now disambiguate
  COLD-stub (`unloaded`, exact reload-on-touch default valve) vs COLD-ann
  (`cold`, DiskANN serve-from-disk, inert behind `MOON_VEC_COLD_TIER`); the
  dead knobs are marked `[reserved: M3/M5]`. Keep-vs-delete is decided at the
  M3 exit-review on real per-index query-frequency telemetry.

### Security — ACL now covers the early-intercepted command families (H-3) (PR #258)

- **CDC.READ bypassed ACL entirely on the monoio runtime (the default build).**
  It was dispatched ~117 lines before the ACL gate, so any authenticated user
  — even a `+get`-only one — could run `CDC.READ <any-wal-dir> <lsn>` and read
  arbitrary WAL directories off the server disk. Moved the CDC.READ intercept
  to after the ACL gate (matching the tokio/sharded handler, which already
  ordered it correctly).
- **Category carve-outs silently didn't cover TXN/WS/MQ/TEMPORAL/CDC.READ (all
  runtimes).** These connection-handler intercepts appeared in no
  `get_category_commands()` arm, so the common deny-list idiom
  (`+@all -@dangerous`, `+@all -@transaction`, `+@all -@write`) left them
  allowed. Added them to the relevant categories: `ws`/`cdc.read` →
  `@admin` + `@dangerous`; `txn`/`temporal` → `@transaction`; `mq`/`txn` →
  `@write`; all five → `@all`.
- **`-@pubsub` didn't block PUBLISH/SUBSCRIBE at the command level.** The
  pub/sub intercepts consulted only the `&pattern` channel rule, so a
  `-@pubsub` carve-out was ineffective for a user with `&*`. Added a
  command-level ACL check to the PUBLISH and SUBSCRIBE/PSUBSCRIBE paths in the
  monoio and tokio-single handlers (the sharded handler already gated them
  post-ACL-gate).
- **`CLIENT TRACKING` ran before the ACL gate on monoio.** Moved it to a
  post-ACL `try_handle_client_tracking` (it registers server-side invalidation
  state); `CLIENT ID`/`SETNAME`/`GETNAME` stay pre-ACL as connection-local
  metadata.
- Tests: new unit test pins the category expansion for all five families;
  new integration tests prove end-to-end NOPERM for `CDC.READ` under
  `-@dangerous` and `PUBLISH` under `-@pubsub`.

### Fixed — Vector: memory-aware WARM offload with a real, reloadable ceiling (PR #252)

- **Reloadable byte-cap eviction (A).** `MmapBudget::enforce_budget` (the
  per-shard `--vec-warm-mmap-budget` cap, default 2gb) evicted a WARM segment by
  dropping its `Arc` outright with no COLD stub — so a byte-cap eviction silently
  removed the segment from search until process restart (recall loss), despite
  the doc claiming reload-on-touch. It now demotes each evicted segment to a
  reloadable `UnloadedSegment` stub in `unloaded` (every WARM segment is durably
  disk-backed via `transition_to_warm`, so the stub reloads byte-identically).
  This makes `--vec-warm-mmap-budget` a real, reloadable memory ceiling. The
  eviction tick now also takes the holder `reload_lock` so it serializes with
  the reload/install path instead of relying solely on shard-thread affinity.
- **WARM memory accounting fix.** `SegmentHolder::resident_bytes()` hardcoded the
  WARM/COLD tiers to 0, so a shard whose HOT segments had aged into WARM reported
  ~0 vector memory — blinding both INFO/Prometheus and the memory-pressure
  trigger below. It now sums the WARM tier (the dominant term for a long-lived
  shard) plus COLD stubs. (IVF/DiskANN-cold still lack a resident accessor.)
- **Memory-triggered early offload (C).** Vector segment memory was invisible to
  every pressure mechanism, so a vector-heavy shard (the primary disk-offload
  case) only ever offloaded on the wall-clock idle timer
  (`--engine-offload-idle-secs`, default 3600s), never on RAM pressure; and the
  pressure cascade's step 2 demoted HOT→WARM (no RSS win). Now the shard's vector
  resident bytes (HOT immutable + WARM) count toward the pressure trigger, and
  under pressure step 2 offloads idle vector segments straight to COLD (real RAM
  reclaim, reloadable) at an aggressive 60s idle floor. Known follow-up:
  HOT-immutable memory still has no standalone byte budget (only idle/pressure
  demotion bounds it).

### Fixed — Disk-offload: crash recovery no longer resurrects deleted cold keys, nor drops the AOF after a spill (PR #257)

- **Deleted/flushed cold keys stayed deleted only until the next crash.** A
  DEL/UNLINK/FLUSHALL/FLUSHDB of a spilled key tombstones the in-memory
  ColdIndex, but the manifest entry stays `Active` until the orphan sweep. A
  kill-9 inside that window lost the tombstone, and boot-time replay could not
  re-apply it: the replayed DEL ran against databases whose `cold_index` was
  `None` — a silent cold-plane no-op — so the key resurrected via cold
  read-through (measured 85–97/200 probes under `--appendonly yes` +
  `--disk-offload`). Two detach points fixed: (a) `recover_shard_v3_pitr` now
  attaches the Phase-3-rebuilt ColdIndex to the databases BEFORE Phase 4 WAL
  replay (was: stashed on `RecoveryResult`, merged only after recovery
  returned); (b) the per-shard/multi-part AOF replay now keeps cold wiring
  live during incr replay — main.rs re-attaches it after the pre-replay hot
  wipe, and `replay_per_shard`/`replay_multi_part` bridge it across
  `rdb::load`'s wholesale database swap (which silently dropped it). New
  crash suite `tests/crash_recovery_cold_del_resurrection.rs` (DEL + FLUSHALL
  scenarios, kill-9 inside the pre-sweep window).
- **Any disk-offload spill silently discarded the AOF on the next restart.**
  The "WAL replayed 0 commands → fall back to the AOF" gate counted vector +
  file-lifecycle records: after a spill wrote `FileCreate` records into the
  shard WAL, the gate saw a "non-empty" WAL, skipped `appendonly.aof` (the
  only complete KV history — `--wal-kv-log` is auto-off when the AOF is the
  authority), and every KV write since the last snapshot was lost. The gate
  now keys on KV `Command` records only.

## [0.6.0] — 2026-07-08

**Release highlights** (full detail in the sections below; "PR #TBD" entries
below all shipped together in this release's PR):

- **Multi-db isolation is now a first-class, enforced boundary**: FT.*/graph/
  full-text indexes are scoped to the db that created them (`SELECT 1`'s
  indexes are invisible to db 0, verified across shards, restarts, and the
  recovery path); per-db memory quotas (`--db-maxmemory <db>:<bytes>`,
  `CONFIG SET db-maxmemory`) enforce noeviction/evicting policies per db slot
  with Redis-style deny-OOM semantics (shrink commands always pass, so a
  tenant can never wedge itself); named workspaces harden the prefix layer.
- **Memory accounting is now truthful under container growth**: HSET/LPUSH/
  SADD/ZADD growth into existing keys is charged to `used_memory` in O(1)
  (previously invisible to `--maxmemory` — an unbounded single-key hash could
  never trigger eviction).
- **Engines offload when idle**: vector segments demote HOT→WARM (mmap)→COLD
  (unloaded stub, reload-on-search) on configurable idle/age thresholds
  (`--engine-offload-idle-secs`, `--segment-warm-after`), with DEL/HDEL
  tombstone correctness across all tiers — measured −26% process RSS on a
  40K×768d corpus with search results identical after reload.
- **Single-node tuning preset**: `--profile standalone` fills in the measured
  best flags for a shard-1 deployment (the p=1 busy-poll configuration that
  beats Redis on both GCE arches).
- **Command parity widened**: SORT_RO / BITFIELD_RO / GEORADIUS_RO /
  GEORADIUSBYMEMBER_RO plus subcommand gaps; the compat matrix is regenerated.
  PFDEBUG/PFSELFTEST/FAILOVER/MODULE/SENTINEL are documented non-goals.

> **Operator callout — historical `WS DROP` key leak.** Before this release,
> `WS DROP`'s cleanup sweep was hardcoded to logical db 0: any workspace whose
> connection ever `SELECT`ed a non-zero db before writing leaked those keys
> permanently on drop. v0.6.0 fixes the sweep (all dbs on the owning shard),
> but keys leaked by PAST drops are still resident. To detect/clean on an
> upgraded instance: `SELECT <n>` each non-zero db and `SCAN 0 MATCH <ws-uuid>:*`
> for workspace prefixes that no longer appear in `WS LIST`, then `DEL` the
> matches (or `FLUSHDB` if the db held nothing else).

> **Absorbed post-release-PR merges.** The following sections merged after the v0.6.0 release PR but before the tag was cut (2026-07-10) and are part of the tagged v0.6.0 binary.

### Performance — Vector: COLD-segment reload moved off the shard event loop (PR #251)

- Under `--disk-offload`, an idle vector segment is demoted to the COLD tier
  (`UnloadedSegment` stub, everything in-memory dropped). The first FT.SEARCH to
  touch it must reload it — `WarmSearchSegment::from_files`: mmap + page-in (+
  optional mlock) of a whole segment. That reload ran **inline on the
  single-threaded shard event loop** (`promote_unloaded` at the top of the
  yielding search capture), so it stalled EVERY other connection on the shard for
  the full reload (hundreds of ms–seconds on real NVMe/EBS with a cold page
  cache) — a multi-tenancy violation (audit finding 18).
- A new process-global `SegmentReloadPool` (`src/vector/reload_pool.rs`, auto-on;
  `MOON_VEC_RELOAD_WORKERS=0` disables) now performs the reload I/O on dedicated
  worker threads. The yielding capture path calls `submit_unloaded_reloads`
  (non-blocking): it submits each COLD stub off-loop and stashes the `flume`
  receivers in the `SearchSnapshot`; the handler `await`s them
  (`await_pending_reloads`) before scanning, which parks only the triggering
  query's **task** (not the OS thread) and then splices the reloaded segments
  into that query's own snapshot. Result: the dense-KNN query that triggered the
  reload still sees **full recall**, while sibling connections on the shard keep
  running. Reloads are **single-flight** (concurrent first-touches of one segment
  share a job) and cached until the next capture installs them into WARM
  (reclaiming the stub's memory).
- Design-for-failure: a reload that errors or whose worker panics/dies replies
  `Err`/disconnect to every waiter — the query answers with degraded recall
  (segment stays COLD, retried next touch) rather than hanging or crashing. When
  the pool is disabled, `submit_unloaded_reloads` falls back to the blocking
  `promote_unloaded`, preserving exact pre-#18 behavior. Non-yielding sync search
  paths are unchanged (still block); HYBRID/SPARSE/RANGE/non-default-field
  queries do not take the off-loop path (accepted follow-up).

### Ops — v0.6.0 release-ledger closure + hygiene (PR #256)

- `RELEASES.md` gains the missing v0.6.0 entry (shipped 2026-07-08, ledger
  lagged); `release.yml` now fails a version-tag push whose entry is absent
  from `RELEASES.md` — the ledger can never lag a tag again.
- `replication.state` (root artifact from replication test runs) gitignored.

### Docs — engineering roadmap suite under `docs/roadmap/` (PR #254)

- New planning docs (excluded from the published docs site via `mkdocs.yml`
  `exclude_docs`): product roadmap v0.7→v1.0 with features mindmap and
  task-level execution plans (`ROADMAP.md`), scale/HA architecture incl.
  multi-shard PSYNC wire-format draft and failover timeline
  (`scale-ha-architecture.md`), and the standalone horizontal-scale deep
  dive (`standalone-horizontal-scale.md`). Commercial/EE planning is
  maintained privately.
- Docs only — no runtime code changes.

### Changed — CI: give the macOS `Test (tokio)` step 30m (was 15m)

- The macOS `Test (tokio)` step budget includes compilation. On a cold or
  rebased cache, ~11m compile + ~8m test suite exceeds 15m and trips a
  wall-clock timeout with zero actual test failures. Bumped to 30m to match
  the Windows Test step's headroom; removes recurring false-negative reds on
  rebased branches.

### Fixed — Conn-plane: monoio central listener joins the SO_REUSEPORT group (PR #250)

- Under the monoio runtime, each shard binds `bind:port` via `SO_REUSEPORT`, but
  the central listener bound the same port with a plain
  `monoio::net::TcpListener::bind` — unlike the tokio sibling, which uses
  `create_reuseport_socket`. Linux requires every socket sharing a REUSEPORT port
  to set the option, so depending on startup scheduling the central bind either
  lost the race (EADDRINUSE propagated out of `run_sharded`, silently killing the
  TLS listener + `conn_rx` fallback) or won it (forcing every shard's REUSEPORT
  bind to fail and collapsing all traffic onto the single central accept loop) —
  both silent and nondeterministic run-to-run. The monoio central listener now
  binds via `create_reuseport_socket` + `from_std` like the tokio path (plain-bind
  fallback on non-unix / unparseable addr). (audit finding 16)

### Fixed — Durability: checkpoint Finalize backs off on repeated failure instead of flooding the WAL (PR #250)

- When a checkpoint's Finalize step failed (`wal.wait_durable`, `manifest.commit`,
  `graph_save`, or the control-file write), the state machine stayed `Finalizing`
  and the next 1ms persistence tick re-ran finalize from step 1 — re-appending a
  WAL Checkpoint record every single millisecond with no backoff. Under a
  sustained failure (slow/degraded disk) this floods the WAL, and since
  `last_checkpoint_lsn` never advances, `recycle_segments_before` never fires —
  WAL disk usage grows fastest exactly during the disk-pressure incident it
  should be backing off from. `CheckpointManager` now arms an exponential backoff
  (50ms→5s cap) on each failed finalize; the tick checks `finalize_ready(now)`
  before doing any finalize I/O, so retries are bounded instead of per-tick, and
  a successful `complete()` clears it. (audit finding 13)

### Fixed — Durability: legacy AOF replay stops at mid-stream corruption instead of resyncing (PR #250)

- The default startup path `replay_aof` (single-file `appendonly.aof`, unframed
  RESP with no per-record length/CRC) treated ANY mid-stream parse error by
  discarding one byte, scanning forward to the next `*`, and resuming dispatch —
  but a `*` appears freely inside binary value blobs, so the resync could land
  mid-value and execute misaligned garbage as live SET/DEL/etc. against the
  recovering keyspace (silent data corruption, WARN-only). The per-shard
  `shard_replay` sibling already rejects exactly this. `replay_aof` now STOPS at
  the first genuine mid-stream corruption (clean tail-truncation is still handled
  gracefully), keeping only the valid prefix already applied and never
  dispatching past the corruption point; it logs a loud error with the byte
  offset and points at `redis-check-aof`. Operators who want the old best-effort
  behavior can opt in with `MOON_AOF_BEST_EFFORT_RESYNC=1`. (audit finding 12)

### Fixed — Xshard: bound cross-shard reply wait so a wedged shard can't hang a client (PR #250)

- Every cross-shard leg (MGET/MSET/DEL/UNLINK/EXISTS/BITOP/COPY/MSETNX remote
  legs, MULTI-on-owner, and the flush/scatter-gather loops) pushed a message via
  the backoff-retried `spsc_send` and then did `reply_rx.recv().await` with **no
  timeout**. `spsc_send`'s retry budget only bounds getting the message INTO the
  target ring buffer; if the push succeeds but the target shard then stalls while
  executing (wedged-disk fsync during snapshot/AOF, uninterruptible D-state I/O,
  or a dead shard), the awaiting connection parked forever with no response and
  no way to cancel. Added a `recv_reply_bounded` helper (races the receiver
  against a runtime-agnostic 30s `XSHARD_REPLY_TIMEOUT` via `race2`) and applied
  it to all 11 reply-await sites in the coordinator — a genuinely wedged shard
  now surfaces the existing cross-shard-reply error instead of hanging. (audit finding 11)

### Fixed — Hardening: bound TLS handshake + cluster-bus body reads against slow-loris (PR #250)

- **TLS handshake has no timeout (#17):** after `try_accept_connection` consumed
  a `maxclients` slot, `acceptor.accept(tcp_stream).await` ran with no deadline
  on both the monoio and tokio TLS paths. A peer that completed the TCP handshake
  on the TLS port and then sent no (or a partial) ClientHello pinned the task,
  its fd, and its `maxclients` slot indefinitely at zero CPU cost — a classic
  slow-loris that could exhaust the connection limit. Wrapped both accepts in a
  10s `TLS_HANDSHAKE_TIMEOUT`, releasing the slot via `record_connection_closed()`
  on expiry.
- **Cluster-bus gossip body read has no timeout (#10):** `handle_cluster_peer`
  guarded only the 4-byte length-prefix read with a shutdown-cancel select; once
  a valid length was read, the body `read_exact` had no timeout and no
  shutdown arm. A client could send a valid length header and never send the
  body, blocking the spawned task forever (immune to graceful shutdown, socket
  never reclaimed). Both runtimes now read the body under a
  `GOSSIP_BODY_READ_TIMEOUT` (10s) + shutdown-cancel select.

### Fixed — Txn: MULTI locality analysis honors SORT/GEORADIUS STORE destination (PR #250)

- In a multi-shard deployment, `MULTI; SORT src STORE dst; EXEC` (or
  `GEORADIUS ... STORE/STOREDIST dst`) where `src` and `dst` hash to different
  shards was misclassified as single-shard and routed entirely to `src`'s
  shard — writing `dst` into the WRONG shard's dataset, invisible to a later
  normally-routed `GET dst`. The STORE/STOREDIST destination is positional
  (the arg after the token) and so was not covered by the fixed command-metadata
  key specs `analyze_txn_locality` consulted. It now detects the STORE clause
  and forces a CrossShard classification, which the caller rejects with
  CROSSSLOT instead of silently misrouting. (audit finding 15)

### Fixed — Vector: DEL tombstones secondary fields + FT.INFO num_docs accuracy (PR #250)

- **Multi-vector-field deletion resurrection (#20):** `DEL`/`UNLINK`/`HDEL` on a
  document only tombstoned the *default* vector field. Secondary VECTOR fields
  live in a separate `field_segments` map that `tombstone_key_in_index` never
  iterated, so a subsequent `FT.SEARCH idx '@field2:[...]'` still returned the
  deleted document (under a synthetic `vec:<id>` key, since the shared
  key-hash→key map was cleared). Now every field's segments are tombstoned.
- **FT.INFO `num_docs` over/under-count (#28):** the mutable segment's `len()`
  counts tombstoned entries in place until compaction, inflating `num_docs` by
  up to `compact_threshold` under DEL/HSET churn — switched to a new `live_len()`
  that excludes tombstones. Separately, DiskANN cold-tier segments (experimental,
  gated by `MOON_VEC_COLD_TIER`) were never summed at all even though cold docs
  ARE returned by FT.SEARCH; added a `snap.cold` counting loop. (audit findings 20, 28)

### Fixed — Vector: GraphUnion merge recall gate no longer lets a total collapse through (PR #250)

- The background/manual GraphUnion merge recall gate read
  `recall < tolerance && recall > 0.0`, so a merge whose verified recall was
  **exactly 0.0** (total collapse) slipped past the guard and committed. Since
  `verify_merge_recall` returns 1.0 (not 0.0) for the too-few-vectors cases,
  0.0 only ever means a real measured collapse — the `&& recall > 0.0` clause
  was removed so such a merge now aborts. (audit finding 21)

### Fixed — Hardening: APPEND 512MB cap + reject FT.* inside MULTI on prod handlers (PR #250)

- **APPEND** now enforces the 512MB max-string-size limit (matching
  SETRANGE/SETBIT); repeated APPENDs previously grew a value past the documented
  limit unbounded. (audit finding 22)
- **FT.\*** commands are now explicitly rejected inside MULTI/EXEC on the
  production sharded + monoio handlers (they aren't wired through the txn
  execution path), matching the existing handler_single guard — a clear
  "not supported inside MULTI/EXEC" error instead of an incidental one.
  (audit finding 25)

### Changed — Vector storage: fence experimental DiskANN cold tier + drop dead vector WAL (PR #250)

- The DiskANN cold tier (WARM→COLD segment demotion + on-disk Vamana beam
  search) was running by default (`--disk-offload` defaults on, `--segment-cold-after`
  defaults 86400) despite being incomplete — **no cold-segment deletion**
  (DEL/HDEL never removes cold docs), blocking per-hop uring I/O holding the
  segment mutex, `FT.INFO num_docs` under-counting cold segments, unfinished
  restart reload of PQ codebooks, and a library-code `panic!` in
  `DiskAnnSegment::new`. It is now **fenced behind the experimental
  `MOON_VEC_COLD_TIER=1` env gate** (default off); the WARM→COLD transition is
  a no-op otherwise. Warm-tier mmap + LRU eviction (`--vec-warm-mmap-budget`)
  continues to handle out-of-RAM indexes. (audit findings 19, 24, 28)
- `DiskAnnSegment::new` now returns `io::Result` instead of panicking on a
  file-open error, so a cold transition can never abort the shard.
- Removed the dead vector WAL module (`vector/persistence/wal_record.rs`): it
  was never on the live recovery path (superseded by manifest + segment +
  keymap + dedup rescan) and carried a documented double-apply footgun.

### Fixed — Cluster: gossip PING task leak + non-blocking accept send (PR #250)

- The gossip PING ticker (both tokio + monoio) spawned an unbounded
  connect+write+read task every 100ms with **no timeout**; against a dead or
  partitioned peer the tasks piled up forever (task/FD/memory leak), and the
  "random peer" it documented was always the first non-self node. Now a single
  in-flight guard bounds outstanding probes to one, a probe timeout
  (`node_timeout/2`, ≥100ms) cancels a hung connect/read, and targets rotate
  across peers. (audit finding 7)
- The monoio per-shard accept task used the **blocking** `flume::Sender::send()`
  on a bounded channel; because the accept task shares the shard's single
  monoio thread with the event-loop consumer, a full channel deadlocked the
  loop that drains it under connection-storm churn. Fixed by `send_async().await`
  (cooperative yield, keeps backpressure). (audit finding 8, Batch B)

### Fixed — Security: bound client-controlled allocation counts (DoS class, PR #250)

- Six wire-reachable command paths fed a client-supplied count straight into
  `Vec::with_capacity` / `Vec::resize` with no upper bound. On a real host the
  oversized request fails allocation and Rust's `handle_alloc_error` calls
  `abort()` — uncatchable, crashing every shard and connection. A single
  unauthenticated request was a full-process DoS. Fixed by clamping each count
  before allocating: **FT.SEARCH HIGHLIGHT/SUMMARIZE FIELDS** (bound by
  remaining tokens), **HSCAN COUNT** (`count.min(total)`),
  **SRANDMEMBER / HRANDFIELD / ZRANDMEMBER** negative COUNT (absolute 2²⁰ cap
  with a loud `ERR COUNT is out of range` beyond it — the first-cut relative
  `len()*10` cap silently truncated legitimate requests on small collections,
  breaking Redis's exact-|COUNT|-with-duplicates contract; caught in review and
  fixed at all six sites including the pre-existing HRANDFIELD/ZRANDMEMBER
  ones), **BITFIELD SET/INCRBY** offset (reject past the 512MB limit, matching
  SETBIT; GET exempt), and **ZMPOP COUNT** (`pop_count.min(card)` — this last
  site was missed by the audit finders and caught by the allocation sweep).
  Each fix has a red/green test. From the production-hardening audit (Batch A).

### Fixed — Review follow-ups on the hardening sweep (PR #250)

- **Gossip PONG fragmentation (monoio):** the failure-detector's PING probe read
  the 4-byte length and the PONG body with single `read()` calls, so a valid
  frame arriving fragmented was silently dropped — leaving `pong_recv_ms` stale
  and pushing a healthy peer toward PFAIL. Both reads now use the bus listener's
  exact-read loop, and both runtimes' probes cap the peer-supplied PONG length
  at the bus's 64KB frame bound before sizing the buffer (the tokio probe
  previously allocated an unchecked `vec![0u8; pong_len]`).
- **AOF corruption offsets are file-absolute:** replay's truncation/corruption
  logs reported offsets relative to the RESP section; with an RDB preamble the
  operator-facing repair guidance (redis-check-aof, truncation point) pointed at
  the wrong file location. Offsets now include the preamble length.
- **Cross-shard MSET no longer OKs an unconfirmed leg:** a timed-out, closed, or
  errored remote `MultiExecute` ack was discarded and the client still got `OK`.
  All legs are drained (each was already dispatched, and the local leg still
  persists), but any failed leg now surfaces as an error instead of a false ack.
- **`recv_reply_bounded` extended to the handler plane:** the coordinator's 30s
  bounded reply-await now also covers the 11 remaining direct cross-shard acks
  (WS.DROP cleanup, routed MQ commands, routed graph commands, TXN.COMMIT MQ
  materialization acks, and the txn-abort remote rollback ack, on both
  handler_monoio and handler_sharded) — a wedged owner shard can no longer park
  those connections forever. MQ/graph/TXN owner execution is synchronous on the
  shard thread (no blocking-wait commands route through these paths), so the
  bound cannot cut off a legitimate long wait.
- **Checkpoint finalize backoff arms from the failure time:** the backoff was
  armed with a timestamp captured before the finalize I/O; when
  `wait_durable`/`manifest.commit`/`graph_save`/control-write took longer than
  the backoff window, the retry deadline was already in the past and the next
  1ms tick retried immediately — exactly the flood the backoff exists to stop.

### Added — CLI/moon.conf defaults for vector + graph tuning knobs (PR #248)

- **`--vector-ef-runtime` / `--vector-rerank-mult` / `--vector-exact-beam`**
  set the server-wide starting values every NEW vector index is created with
  (FT.CREATE), so recall-sensitive fleets configure once instead of issuing
  FT.CONFIG per index. Per-index `FT.CONFIG SET` always overrides. Same
  ranges as FT.CONFIG (ef 10-4096 or 0=auto, mult 1-64), validated loudly at
  startup. All three work as `moon.conf` keys (`vector-exact-beam yes|no`).
- **`--graph-result-cache-entries` / `--graph-result-cache-bytes`** size the
  per-graph Cypher result cache (previously hardcoded 256 entries / 4 MiB).
- Startup wiring is first-write-wins process state, installed by both the
  binary entry and `run_embedded`; unit tests and library embedders that
  never install defaults keep the exact pre-flag behavior.

### Added — Recall knobs: FT.CONFIG RERANK_MULT + EXACT_BEAM (PR #248)

- **`FT.CONFIG SET <idx> RERANK_MULT <n>`** (1-64, default 4) deepens the HQ-1
  exact-rerank stage: the top `n·k` beam candidates are re-scored with true f16
  sidecar distances before top-k truncation, recovering true neighbors the
  quantized ADC ranking dropped below the default 4·k cut. Cost ~`n·k·dim` f16
  decodes per segment.
- **`FT.CONFIG SET <idx> EXACT_BEAM ON|OFF`** (default OFF) navigates the HNSW
  beam itself with exact f16 distances instead of quantized ADC estimates —
  beam candidate *selection* becomes exact, so recall is graph-limited
  (Qdrant-parity at equal ef) rather than quantization-limited. QPS cost grows
  with dimension. Segments without an exact-rerank sidecar (pre-HQ-1 disk
  reloads) silently keep the quantized beam.
- Both knobs are per-index, apply to the next FT.SEARCH (all search paths:
  sync, yielding, worker-pool fan-out, FT.RECOMMEND, hybrid), broadcast to all
  shards, and persist across restarts via a new v4 index-meta sidecar format
  (v1-v3 sidecars load with defaults).

### Added — Runtime-tunable EF_RUNTIME via FT.CONFIG (PR #248)

- **`FT.CONFIG SET <idx> EF_RUNTIME <n>`** adjusts the HNSW search beam width at
  runtime without rebuilding the index (RediSearch parity). Range matches
  FT.CREATE (10-4096); `0` restores the auto heuristic. Applies to the next
  FT.SEARCH immediately and persists via the index meta sidecar. `FT.CONFIG GET
  EF_RUNTIME` added alongside.
- **Fixed — FT.CONFIG SET was local-shard only under monoio**: at shards>1 the
  setting silently applied to 1/N index partitions (AUTOCOMPACT,
  COMPACTION_WEIGHT, MERGE_RECALL_TOLERANCE were equally affected). SET now
  broadcasts to all shards like FT.CREATE; GET stays local.

### Fixed — TQ ADC estimator is now metric-faithful on raw L2 (PR #248)

- **TQ quantization ranked L2 queries by `sphere_dist·‖a‖²`** — the unit-sphere
  distance between normalized directions scaled by the document norm. On
  unnormalized L2 data this makes every small-norm vector look near regardless
  of direction (gist-960: recall 0.002). All TQ scoring paths (HNSW beam +
  budgeted variant, mutable brute-force + FastScan pre-filter bound, flat
  scan, multibit brute-force, A2 decoded-L2) now reconstruct the true metric:
  `‖a−q‖² = (‖a‖−‖q‖)² + ‖a‖·‖q‖·d̂²_sphere`. COSINE/IP scoring is unchanged.
  Unit tests pin recall on varying-norm data at both the mutable-scan and
  HNSW-beam levels (7+/10 and 6+/10 vs exact f32 ground truth; 4/10 and worse
  before the fix). GCE re-verification on gist-960 with explicit TQ4:
  recall@10 0.784/0.942/0.986 at ef 16/64/256 vs 0.002-0.003 pre-fix (~350x),
  within ~0.03 of SQ8.

### Changed — FT.CREATE L2 indexes default to SQ8 quantization (PR #248)

- **`FT.CREATE ... DISTANCE_METRIC L2` without an explicit `QUANTIZATION` now
  defaults to SQ8** instead of TQ4. TQ's norm-scaled ADC estimator assumes
  unit-sphere metrics (COSINE/IP) and collapses on unnormalized L2 data —
  recall 0.002 measured on gist-960-euclidean (1M × 960d). SQ8 is
  metric-faithful on raw L2. COSINE/IP defaults are unchanged (TQ4). An
  explicit `QUANTIZATION TQ*` + L2 is still honored but logs a warning.
- Benchmarks: `BENCHMARK.md` §10.10 — ANN-benchmarks 4-way campaign (Moon vs
  RediSearch vs Qdrant vs turbovec on glove-200-angular 1.18M / gist-960 1M /
  glove-100K). Tuning guide gains bulk-load (`--max-unflushed-immutable-segments
  0`), settle (`VACUUM VECTOR`), and runtime-`EF_RUNTIME` recipes.

### Added — FastScan SIMD vector scan: NEON TBL kernel + live-path integration (PR #248)

- **NEON TBL FastScan kernel** (`src/vector/distance/fastscan.rs`): register-resident
  4-bit LUT accumulation via `vqtbl1q_u8` for aarch64 — the primary platform previously
  fell back to the scalar kernel. 32 candidate distances per block at ~2 ns/candidate
  (128d), 17-21× faster than the per-candidate scalar ADC path.
- **AVX2 kernel overflow fix**: nibble-pair distances were added as u8 before widening,
  silently wrapping for LUT pairs summing past 255 and corrupting rankings. All kernels
  now widen to u16 before adding and accumulate with saturating adds (scalar/NEON/AVX2
  bit-identical across the full u8 LUT range; parity tests no longer mask LUTs to 0x7F).
- **Adaptive quantized LUT builder** (`build_quantized_lut`): FAISS-style per-coordinate
  bias + global scale chosen so entries fit u8 AND the worst-case accumulated sum fits
  u16 (no kernel saturation for in-range data), with f32 reconstruction (`acc/scale +
  bias`). Replaces the legacy `precompute_lut`, which hardcoded the v1 1/sqrt(768)
  `CENTROIDS` table (encode/search codebook asymmetry — the same recall bug class fixed
  by codebook v2) and a fixed `LUT_SCALE` that could overflow both u8 entries and the
  u16 accumulator.
- **Mutable-segment FastScan pre-filter** (`src/vector/segment/mutable.rs`): the MVCC
  brute-force scan now maintains a FAISS-interleaved shadow of the TQ4 codes (32-vector
  blocks, +padded_dim/2 B/vector) and screens candidates with the SIMD kernel; only
  candidates whose sound lower bound (`approx − 0.5·padded_dim/scale`) beats the current
  heap worst get the exact f32 ADC rescore. Results are bit-identical to the plain scan
  (the bound is a true lower bound; saturation only under-estimates). Measured: top-10
  over 20K×128d 1.25 ms → 96 µs (~13×), 5K×768d 1.66 ms → 423 µs (3.9×) on Apple
  Silicon. Engages above 64 entries (per-query LUT build costs ~2-8 µs); SQ8/A2
  collections and TQ-prod scoring keep the existing paths.
- New criterion bench `benches/fastscan_bench.rs` (block kernels, LUT build, end-to-end
  mutable scan A/B); shadow-layout + FastScan-vs-plain equality tests (full scan,
  chunked scan, MVCC visibility + bitmap-filter paths).

### Fixed — WS3 COLD/WARM tier adversarial-review fixes (round 2 follow-up, PR #TBD)

An adversarial review of the round-2 COLD tier (below) found one CRITICAL
correctness bug and several hardening gaps. All fixed on the same branch:

1. **CRITICAL: DEL/HDEL resurrection through WARM/COLD.**
   `tombstone_key_in_index` (`src/vector/store.rs`) only ever walked
   `mutable` + `immutable` — a HDEL against a key living in a WARM or COLD
   (unloaded) segment was silently dropped. Sequence: index idles overnight
   -> HDEL an old doc -> next-day query resurrects it as a phantom hit, and
   `num_docs` over-counts. Fixed by giving both tiers a live tombstone set:
   - `WarmSearchSegment` gained the same interior-mutable
     `tombstoned_keys`/`has_tombstones` pattern `ImmutableSegment` already
     has (`mark_deleted_by_key_hash`, `seed_tombstones`,
     `tombstoned_key_hashes`, `live_count`) — `search_filtered` now filters
     tombstoned entries out before rerank/truncate, same ordering as the HOT
     path. Takes effect immediately, no reload required.
   - `UnloadedSegment` gained a `pending_tombstones` set — a HDEL against a
     COLD key is queued in the stub (no reload forced just to record a
     delete) and replayed onto the freshly-reloaded `WarmSearchSegment` in
     `reload()` via `seed_tombstones`.
   - `tombstone_key_in_index` now also calls `mark_deleted_by_key_hash` on
     every `warm` and `unloaded` entry.
   - Bonus fix found while wiring this up: `mvcc_raw_bytes()` (the HOT->WARM
     transition's serialization of MVCC headers) only captures INSTALL-time
     deletes (`mvcc[i].delete_lsn`), never the STEADY-STATE
     `tombstoned_keys` interior set — so a HDEL landing shortly before a
     transition was ALSO silently lost, independent of the round-2 COLD
     work (this bug predates it, in the pre-existing age-based WARM path).
     `try_warm_transitions_idle` now seeds the new `WarmSearchSegment` with
     `imm.tombstoned_key_hashes()` right after opening it, closing this gap
     for both WARM and COLD destinations for free.
   - `FT.INFO`'s `num_docs` now sums `warm.live_count()` /
     `stub.live_count()` (total minus live tombstones) instead of the raw
     `total_count()`, at all 4 call sites (top-level + per-field, default +
     non-default field).
   - New tests: `tests/vector_idle_unload.rs`'s
     `hdel_during_cold_does_not_resurrect` and
     `hdel_during_warm_does_not_resurrect` — HDEL a doc, wait for the
     COLD/WARM transition (or vice versa), assert `num_docs` drops
     immediately and the doc never resurfaces in a post-transition
     `FT.SEARCH`, in both tier orderings.
2. **Reload thread placement — verified empirically, documented honestly.**
   Traced all 3 `promote_unloaded` call sites
   (`SegmentHolder::search_filtered`, `SegmentHolder::search_mvcc`, and the
   FT.SEARCH yielding-path snapshot capture in
   `command/vector_search/ft_search/dispatch.rs`): all three run inside
   `crate::shard::slice::with_shard(...)`, i.e. on the shard's own OS
   thread, before any `.await` boundary — PR #179's off-loop worker pool
   only carries the HNSW beam search itself off-loop, not the capture phase
   a COLD reload runs in. This means a first-touch reload is a SHARD-WIDE
   stall (every connection sharing that shard, not just the querying one),
   for the reload's duration. A real off-loop fix needs `SegmentHolder`
   reachable from the async continuation without an open `VectorIndex`
   borrow (e.g. `Arc`-wrapping it, a ~100-call-site change) — judged
   out-of-scope / too risky for this pass and tracked as a follow-up rather
   than attempted half-finished. `docs/guides/tuning.md` now documents this
   precisely instead of the earlier (incorrect) "one-query latency hit"
   framing, plus a same-instance measurement of the stall's actual duration:
   the first-touch `FT.SEARCH` reload took 79.59 ms round-trip, during which
   a concurrent `PING` on a second connection to the same shard peaked at
   76.70 ms (vs sub-millisecond baseline) — confirming the block is
   shard-wide and roughly the full reload duration, not a per-connection cost.
3. **WARM segments could never reach COLD.** `try_warm_transitions_idle`
   only ever scanned `snapshot.immutable` for idle/age eligibility — a
   segment that had already demoted to WARM (age-based) had no further path
   to COLD even if it then went idle, defeating the whole point of the COLD
   tier for that segment permanently. Fixed: the same function now also
   scans `snapshot.warm` (`WarmSearchSegment::idle_secs`, a new method
   mirroring `ImmutableSegment::idle_secs`) and demotes idle WARM segments
   straight to COLD stubs via the same `UnloadedSegment::from_warm` used for
   the immutable path — mechanical, since a WARM segment's `.mpf` files
   already exist on disk.
4. **Repo-rule compliance: no per-query `SystemTime::now()` syscalls.**
   `WarmSearchSegment::touch_last_access`/`from_files` and
   `ImmutableSegment`'s module-level `now_micros()` called
   `SystemTime::now()` directly on a path that runs once per query per
   segment. Both now read `crate::storage::entry::current_time_ms()` — the
   existing shard-tick-populated thread-local cached clock (`TL_NOW_MS`,
   updated once per ~1ms shard tick) that already backs the KV hot path,
   with its documented automatic syscall fallback on threads that never
   ticked a shard clock (tests, or an off-loop FT.SEARCH worker thread).
   Millisecond resolution (×1000 for unit parity) is sufficient since every
   caller only compares `age_secs`/`idle_secs` at second granularity.
5. **Tightened the RSS regression test.** `cold_unload_reduces_process_rss`
   asserted a plain `rss_after < rss_before`, which would still pass for a
   regression that only frees (say) the HNSW graph while leaving TQ codes +
   sidecar resident. Now asserts `rss_after < rss_before * 0.85` (a 15%
   floor — the real 40K×768d measurement showed −26.2%, so this has margin
   for the smaller CI-scale fixture and jemalloc arena noise without being
   toothless).
6. **`SegmentList::with_unloaded` / `with_warm_and_unloaded` clone-and-patch
   helpers.** `SegmentList` now derives `Clone` (all fields are
   `Arc`/`Vec<Arc<_>>` — refcount bumps, never a data copy) so reconstruction
   sites can use Rust's functional-update syntax
   (`SegmentList { changed_field, ..old.clone() }`) instead of enumerating
   all 6 fields by hand. Applied at `try_warm_transitions_idle`'s final
   `swap` (the site touched by this review's item 3 fix). This does NOT
   convert all ~17 `SegmentList { ... }` construction sites in the codebase
   — only the ones this pass's changes already touch, verified to compile.
   The remaining sites are left as full literals rather than converted
   speculatively without individual verification; they will fail to compile
   the moment a new field (e.g. WS5a's `db_index`) is added, which is itself
   the forcing function that gets them individually attended to. The
   pattern (derive Clone + functional update, plus the two named helpers)
   is now established for that future pass to reuse.
7. **Freshly compacted segments were unloaded to COLD before they were ever
   searchable.** Two stacked bugs, found via `cold_unload_reduces_process_rss`
   timing out 600s waiting for `graph_segments >= 1`:
   - `ImmutableSegment` seeds `last_access_micros` at *construction* (build
     start), but a multi-second HNSW build consumes the whole
     `--engine-offload-idle-secs` budget before the segment is published —
     the idle sweep then unloaded the brand-new segment in its very next
     tick (server log: "Cold (unload) transition: segment 1 (15000 vectors,
     idle 3s)" 4.4s after server start). Fixed with
     `ImmutableSegment::mark_installed()`, called at every point a built
     segment is swapped into the live `SegmentList` (inline + background
     compaction installs, both merge installs): idle time is now measured
     from *visibility*, not from the start of the build.
   - `mark_installed` deliberately bypasses the cached clock (item 4 above)
     with a real `SystemTime::now()` syscall: the inline FT.COMPACT path
     builds ON the shard thread, so `TL_NOW_MS` hasn't advanced for the
     entire build and stamping the cached value would be build-duration
     stale — the first iteration of this fix did exactly that and still
     produced "idle 9s" on a just-installed segment. Install is a cold path;
     one syscall is fine (does not violate the per-query no-syscall rule).
### Added — true COLD tier: idle segments now actually free memory (WS3 round 2, PR #TBD)

Round 1 (below) shipped an idle *trigger* but routed it into the pre-existing
WARM tier, which copies `.mpf` payloads into owned buffers of the same size
as the HOT segment it replaces -- measured flat-to-+1.1% RSS, not a
reduction. Round 2 fixes the actual memory problem:

- **New tier, `SegmentList.unloaded: Vec<Arc<UnloadedSegment>>`**
  (`src/vector/persistence/unloaded_segment.rs`, new file): a COLD segment
  stub holding only a handful of scalars (segment id, doc count, whether it
  had an exact-rerank sidecar, an `mlock_codes` flag) plus a cloned
  `SegmentHandle` keeping the on-disk directory alive. Nothing else --
  no TQ/SQ8 codes, no HNSW graph, no f16 sidecar buffer.
- **Idle now routes straight to COLD, not WARM**
  (`VectorIndex::try_warm_transitions_idle`, `src/vector/store.rs`): the two
  triggers now have different destinations. `age_eligible`
  (`--segment-warm-after`) still demotes to WARM exactly as before (an
  old-but-still-hot segment structurally simplifies to disk-backed storage,
  no memory-reduction promise). `idle_eligible`
  (`--engine-offload-idle-secs`, genuinely cold) now demotes straight to
  COLD -- this is the one that actually frees memory. If a segment
  satisfies both simultaneously, COLD wins. Both destinations write the
  identical `.mpf` files via the existing `warm_tier::transition_to_warm`
  (sidecar included); COLD additionally drops the materialized
  `WarmSearchSegment` immediately after capturing the stub, instead of
  keeping it resident.
- **Transparent, synchronous, single-flight reload on touch**
  (`SegmentHolder::promote_unloaded`, `src/vector/segment/holder/promote.rs`
  -- split out as a child module to keep `holder.rs` under the 1500-line
  cap): every search path (`search_filtered`, `search_mvcc`, and the
  yielding worker-pool capture in
  `command/vector_search/ft_search/dispatch.rs`) calls this before scanning
  -- a KNN query must consider every segment for correctness, so a COLD
  segment cannot stay unloaded and still participate. Fast path (nothing
  unloaded) is a single `is_empty()` check, no lock, no allocation. When
  there IS something to reload, a blocking (never `try_lock`, never held
  across `.await`) `reload_lock` mutex makes it single-flight: N concurrent
  callers touching the same COLD segment all wait for the one reload
  (double-checked re-load after acquiring the lock) rather than racing
  ahead with a stale, missing-segment snapshot. Reload reuses
  `WarmSearchSegment::from_files` -- the same function the WARM tier and
  server-boot recovery already use -- so recall/exactness (sidecar
  included) is unaffected; a segment that fails to reload (corrupt/missing
  files) stays in `unloaded` and is retried on the next touch without
  poisoning the rest of the batch.
- **`FT.INFO`**: new additive counters `unloaded_segments` /
  `unloaded_segments_with_exact_rerank` (the latter answered from the stub,
  no reload needed), and `num_docs` (both top-level and per-field) now also
  sums `unloaded.iter().map(UnloadedSegment::total_count)` -- the same
  regression class fixed for WARM in round 1, this time for COLD.
- **Lifecycle**: `VectorStore::drop_index` / `clear_all_contents` (FLUSHALL/
  FLUSHDB) now also tombstone every `unloaded` stub's `SegmentHandle`, or
  its on-disk directory would leak once the stub itself is dropped (WARM
  segments already got this treatment; COLD needed the same). GraphUnion
  merge scheduling (`VectorIndex::needs_merge` / `begin_background_merge`)
  only ever reads/filters `snapshot.immutable` (HOT segments) -- it never
  touches `warm`, `cold` (DiskAnn), or `unloaded`, so COLD segments are
  skipped by construction, not by added special-casing. Server restart:
  COLD segments have no restart-time restoration path, same as WARM already
  didn't -- the segment directories they point at are simply reloaded as
  fresh HOT/immutable segments by the existing boot-recovery scan
  (`persistence/recover_v2.rs`); "should be free" per the original ask.
- **RSS re-measurement** (same methodology as round 1: real server,
  `--shards 1`, 40,000 x 768-dim vectors, SQ8, `ps -o rss=`) -- this time a
  real drop, not the round-1 flat-to-increase result:

  | Phase | RSS (KB) | Delta |
  |---|---|---|
  | Before unload (HOT) | 400,544 | -- |
  | After unload (COLD) | 295,760 | -104,784 KB (-26.2%) |
  | After reload (touched, back to WARM) | 395,376 | +99,616 KB vs COLD |

  The reload number lands just under the original HOT figure because the
  segment reloads into the WARM (owned-buffer) representation, which is
  itself ~1.3% smaller than the original HOT `ImmutableSegment` layout in
  this run -- consistent with round 1's WARM-vs-HOT measurement, not a new
  effect.
- Red/green: `tests/vector_idle_unload.rs`'s
  `hot_segment_idle_unloads_to_cold_and_preserves_recall` (supersedes round
  1's `hot_segment_idle_unloads_and_preserves_recall`, which polled
  `warm_segments` -- now stays 0 for a purely-idle transition) walks HOT ->
  COLD -> reload -> WARM, asserting `FT.INFO` counters at every stage
  (`graph_segments`, `warm_segments`, `unloaded_segments`,
  `*_with_exact_rerank`, `num_docs`) plus recall/exactness before and after.
  `cold_unload_reduces_process_rss` is the process-level RSS proxy (40,000 →
  scaled down to a CI-reasonable 3,000 × 256-dim fixture -- see the full
  40K×768d numbers below for the production-scale claim) asserting a real
  `ps` RSS drop, not just a counter flip. `unloaded_segment.rs`'s own unit
  tests cover stub-capture + reload round-trip and tombstone-triggers-
  directory-removal.
- **Correctness edges considered and their disposition**: concurrent search
  during unload -- covered by single-flight `reload_lock` above; writes/
  tombstones arriving for a COLD segment's key -- `WarmSearchSegment` (and
  therefore also the COLD stub, which reloads into one) has **no per-key
  tombstone mechanism at all**, a pre-existing gap discovered during this
  work (`tombstone_key_in_index` only ever touches `mutable` and
  `immutable`) -- COLD inherits exactly the same behavior as WARM already
  had, so this is a documented pre-existing limitation, not a regression;
  see "Known limitation" below. FLUSHALL/FLUSHDB/DROPINDEX -- handled (see
  Lifecycle above). GraphUnion merge -- skipped by construction (see
  Lifecycle above). Single-flight reload -- handled (see above).

- **⚠ Known limitation (pre-existing, not introduced by WS3): no per-key
  tombstoning for WARM or COLD segments.** `tombstone_key_in_index`
  (`src/vector/store.rs`) only marks deletions in the `mutable` and
  `immutable` (HOT) segments; `WarmSearchSegment` has no delete-bitmap or
  MVCC-visibility mechanism of its own. A key deleted (DEL/HDEL/UNLINK)
  after its vector's HOT segment has demoted to WARM or COLD stays
  returned by that segment's own local search until the segment is later
  merged away or the whole index is dropped/flushed. This predates WS3
  (round 1's WARM tier already had it) and reloading a COLD stub does not
  make it worse or better -- it inherits WARM's exact behavior. Flagged
  here because implementing the new COLD tier required reading this code
  path closely enough to notice it; fixing it is out of scope for this
  workstream (adding per-segment tombstone tracking to `WarmSearchSegment`
  is its own, separately-scoped change) and is recorded as a follow-up.

### Added — vector idle-unload with sidecar-preserving WARM transition (WS3, PR #TBD)

- **`src/vector/segment/immutable.rs`**: `ImmutableSegment` gains a
  `last_access_micros` atomic (mirrors `WarmSearchSegment`'s existing field),
  touched on every `search`/`search_filtered`, exposed via `idle_secs()`.
- **`src/config.rs`**: new `--engine-offload-idle-secs <secs>` (default
  `3600`, `0` disables). A HOT segment now demotes to the mmap-backed WARM
  tier when EITHER `--segment-warm-after` (age) OR
  `--engine-offload-idle-secs` (idleness since last search) is reached —
  whichever fires first — so a segment that is old but still busy stays HOT
  instead of unloading mid-traffic. `src/vector/store.rs` adds
  `try_warm_transitions_idle` / `try_warm_transitions_all_idle` (the
  pre-existing age-only methods now delegate to these with idle disabled, so
  every existing caller is unaffected). `src/shard/{persistence_tick,
  event_loop}.rs` thread the new threshold through; the warm-check poll
  interval also adapts to the lower of the two thresholds for fast local
  testing.
- **Recall fix (was silently dropped, not new WS3 behavior)**:
  `VectorIndex::try_warm_transitions` always passed `None` for the HOT
  segment's f16 exact-rerank sidecar when writing `vectors.mpf`, and
  `WarmSearchSegment` had no sidecar field or rerank logic at all — every
  HOT->WARM transition silently downgraded to quantized ADC-only distances,
  contradicting the sidecar's whole purpose (HQ-1, PR #232). Fixed:
  `src/vector/persistence/warm_search.rs`'s `WarmSearchSegment` now loads
  `vectors.mpf` (when present) into a `RawF16Store`, exposes `raw_f16()`, and
  runs the same `rerank_exact` HQ-1 logic `ImmutableSegment` does before
  truncating to `k`. `VectorIndex::try_warm_transitions` now passes
  `imm.raw_f16()`'s bytes through instead of `None`.
- **Correctness fix (also pre-existing, caught by the new integration
  test)**: `warm_search.rs`'s `parse_global_ids` assumed a 24-byte MVCC entry
  with no `key_hash` field; the actual writer
  (`ImmutableSegment::mvcc_raw_bytes`) emits 32-byte entries
  (`internal_id + global_id + key_hash + insert_lsn + delete_lsn`). Every
  entry past the first was misaligned, and `key_hash` was dropped entirely —
  every WARM-tier FT.SEARCH hit resolved to a synthetic `vec:<id>` key
  instead of the real Redis key. Renamed to `parse_mvcc_ids`, fixed to the
  correct 32-byte stride, and `key_hash` now propagates onto
  `SearchResult.key_hash` via `remap_to_global_ids`.
- **`FT.INFO`**: new additive (scatter-gathered across shards) counters
  `warm_segments` / `warm_segments_with_exact_rerank`, mirroring the existing
  `graph_segments` / `segments_with_exact_rerank` for the HOT tier.
- **Correctness fix (regression this WS3 work would otherwise have shipped,
  caught by RSS verification below)**: `command/vector_search/ft_info.rs`'s
  `num_docs` computation (both the top-level and per-`vector_fields` entry)
  only summed `mutable.len()` + `imm.live_count()` over HOT segments — it
  never counted WARM segments. Before WS3 this was latent (age-based
  HOT->WARM demotion existed but nothing exercised it in a live num_docs
  check); with idle-unload now demoting segments routinely, a fully-idle
  single-segment index would report `num_docs 0` even though every document
  was still present and searchable. Fixed by adding
  `warm.iter().map(WarmSearchSegment::total_count)` to both sums.
- **⚠ Known limitation — RSS does NOT drop after HOT->WARM idle-unload.**
  Verified with a real server (`--shards 1`, SQ8, 40,000 × 768-dim vectors,
  `ps -o rss=` sampled every 2s for 16s after the transition): RSS went from
  395,168 KB to a flat 399,504–399,520 KB post-unload — a **1.1% increase**,
  not a decrease, with zero decay over the observation window. Root cause is
  pre-existing (predates this WS3 change, confirmed by the standing doc
  comment on `WarmSearchSegment::from_files` in
  `src/vector/persistence/warm_search.rs`): the "mmap-backed" WARM tier opens
  each `.mpf` file's mmap only for the duration of `from_files()` and
  immediately copies every payload out into owned `Vec<u8>` /
  parsed-`HnswGraph` buffers of essentially the same size as the HOT
  segment's — the mmap is then dropped. So the two structures that dominate
  memory at scale (TQ codes + HNSW graph) are fully duplicated in heap memory
  on the WARM side, not lazily paged in from disk. The idle-based *trigger*
  added by this change is correct and verified end-to-end (`FT.INFO` counters
  flip, recall/exactness preserved, `num_docs` now stays correct — see
  `tests/vector_idle_unload.rs`), but it does not, by itself, achieve the
  memory-reduction goal this workstream set out to prove. A genuine RSS win
  requires reworking `WarmSearchSegment` to search directly over borrowed
  mmap'd bytes (HNSW traversal + TQ-ADC distance kernels operating on
  `&[u8]` instead of owned `Vec`), which is a substantially larger, higher-risk
  change (touches SIMD distance kernels, needs careful lifetime/alignment
  handling, and was explicitly out of scope for this pass per the "no new
  `unsafe` without approval" constraint). Tracked as an open follow-up; see
  `docs/guides/tuning.md`.
- **`src/config.rs`**: corrected the `--disk-offload-threshold` doc comment,
  which claimed "parsed but not acted upon" — the memory-pressure cascade
  (`persistence_tick::{should_run_pressure_cascade,handle_memory_pressure}`)
  has actually acted on it since an earlier phase; the comment was stale.
- **`src/storage/tiered/cold_read.rs`**: new `_cached` variants
  (`cold_read_through_outcome_cached`, `read_cold_entry_at_cached`) read the
  cold KV leaf page through `PageCache` when given one, instead of always
  `pread`ing — the existing (uncached) functions are now thin wrappers
  calling these with `page_cache: None`, so their behavior and every existing
  call site are unchanged. **Not yet wired into `Database::get()`'s live
  cold-read path** — that needs a `PageCache` handle threaded from the
  per-shard event loop into `Database`, a broader plumbing change deferred as
  a follow-up; the cache-capable read path itself is implemented and tested.
- Docs: `docs/guides/tuning.md` gains "Tiered memory offload" and
  "Vector/FTS/graph idle-unload" sections documenting the new flags,
  `FT.INFO` counters, and the FTS/graph limitation (no equivalent idle-unload
  yet — FTS has no aggregate memory-accounting API, and the graph engine's
  mmap'd `MmapCsrSegment` has no LRU-eviction-driven unload comparable to
  vector's `MmapBudget`).
- Red/green: `tests/vector_idle_unload.rs` (real server process, `MOON_BIN` /
  `CARGO_BIN_EXE_moon`) — idle-out a HOT segment with `--segment-warm-after`
  set far above the test's timeout so only `--engine-offload-idle-secs` can
  trigger the transition; asserts `FT.INFO` counters flip HOT->WARM and a
  post-transition `FT.SEARCH` returns the same key with the same near-zero
  exact-rerank distance (this is what caught both fixes above — it failed on
  the pre-fix code, first on distance, then on key identity).
  `src/storage/tiered/cold_read.rs` unit test proves a second cold read is
  served from `PageCache` even after the backing file is renamed away.
  `src/vector/persistence/warm_search.rs` unit tests cover the sidecar
  round-trip and the corrected MVCC entry parsing.

### Fixed — WS6: self-inflicted write lockout on an over-quota key (HIGH, adversarial review 2026-07-08)

- **HIGH, release-blocking**: making container growth visible to
  `used_memory` (see the accounting-gap fix below) made a second,
  previously-unreachable bug reachable: `run_write_eviction_gate`
  (`src/server/conn/handler_monoio/mod.rs`) and
  `check_db_maxmemory_for_command` (`src/storage/db_quota.rs`) applied
  the `noeviction` reject to **every** write command uniformly — so once
  a key's growth tripped the global `--maxmemory` gate or a db's
  `--db-maxmemory` quota, a pure-shrink command on that SAME key —
  `HDEL`, `SREM`, `LPOP`, `ZREM`, ... — was *also* rejected. A tenant
  that grew a key past the boundary had **no self-recovery path** short
  of `FLUSHALL`/restart, undermining the WS5b per-db-quota guarantee
  this same release ships.
- Fixed with a static, provably shrink-only command classification,
  `db_quota::is_shrink_only_command` (`src/storage/db_quota.rs`),
  mirroring Redis's `CMD_DENYOOM` semantics: `DEL`/`UNLINK`,
  `HDEL`/`HGETDEL`, `SREM`/`SPOP`, `LPOP`/`RPOP`/`LREM`/`LTRIM`/`LMPOP`/
  `BLMPOP`, `ZREM`/`ZPOPMIN`/`ZPOPMAX`/`ZMPOP`/`BZPOPMIN`/`BZPOPMAX`/
  `BZMPOP`/`ZREMRANGEBYSCORE`/`ZREMRANGEBYRANK`/`ZREMRANGEBYLEX`,
  `GETDEL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`/`PERSIST`,
  `FLUSHDB`/`FLUSHALL` now bypass the *reject* from both the global
  maxmemory gate and the per-db quota gate — eviction is still attempted
  first (an evicting policy may as well reclaim while the write lock is
  already held); only the reject is skipped. Deliberately conservative
  — commands that can grow a destination key (`LMOVE`/`SMOVE`/`COPY`/
  `RESTORE`/any `*STORE` variant) or aren't statically classifiable
  (`SET`, even with a shorter value) are excluded ("when in doubt,
  exclude").
- Applied at all three connection-handler write-path chokepoints:
  `handler_monoio::run_write_eviction_gate`, the inline eviction block in
  `handler_sharded`'s sharded write path, and both inline blocks in
  `handler_single` (the pipelined-dispatch loop and the single-command
  write path).
- RED/GREEN: `test_hdel_self_recovery_past_maxmemory_boundary` and
  `test_hdel_self_recovery_past_db_maxmemory_boundary`
  (`tests/container_growth_memory_accounting.rs`) — confirmed RED against
  the pre-fix handler/db_quota code (`git stash`): both failed with the
  exact OOM-on-HDEL symptom described above. Each test now: grows a hash
  past its cap (asserting the growing HSET IS rejected with the correct
  OOM flavor), asserts `HDEL` on that same over-cap key succeeds, drains
  the rest, then asserts a follow-up `HSET` succeeds once back under
  budget.

### Fixed — WS6: container-growth memory accounting gap (HSET/LPUSH/SADD/ZADD... growth was invisible to `--maxmemory`)

- **Critical**: `used_memory` was charged once at key-creation time for an
  EMPTY Hash/List/Set/ZSet container (`entry_overhead()` in
  `src/storage/db.rs`, called from `get_or_create_hash`/`_list`/`_set`/
  `_sorted_set`), then a raw `&mut` reference into the map/deque/set/tree
  was handed to the command layer. Every subsequent mutation through that
  reference — `HSET` adding fields, `LPUSH` growing the deque, `SADD`/
  `ZADD` growing the set/sorted-set, and their listpack/intset compact-
  encoding equivalents — never updated `used_memory` again, so growing a
  SINGLE key's contents was invisible to both the global `--maxmemory`
  gate and the per-db quotas (WS5b). Confirmed by adversarial review
  2026-07-08 (flagged as a known limitation in `docs/guides/isolation.md`
  at the time).
- Fixed via O(1) per-mutation delta accounting rather than a full
  recompute: `RedisValue::estimate_memory()` sums over every element for
  the expanded Hash/List/Set/SortedSetBPTree encodings, so recomputing it
  on every single-field HSET on a large hash would turn an O(1) command
  into O(n). New `Database::charge_memory`/`credit_memory`/`adjust_memory`
  (O(1), saturating) plus per-container byte-cost helpers next to
  `entry_overhead` (`hash_field_cost`, `list_elem_cost`, `set_member_cost`,
  `zset_member_cost`) that mirror `RedisValue::estimate_memory`'s
  per-variant formulas exactly. The listpack/intset COMPACT encodings are
  the exception — their `estimate_memory()` is already O(1)
  (capacity-based), so those call sites snapshot before/after instead.
- Every hash/list/set/zset mutation site instrumented: `HSET`/`HMSET`/
  `HINCRBY`/`HINCRBYFLOAT`/`HSETNX`/`HDEL`/`HGETDEL`/`HEXPIRE`-family
  past-expiry delete (`src/command/hash/hash_write.rs`,
  `src/storage/db.rs`); `LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LSET`/`LINSERT`/
  `LREM`/`LTRIM`/`LMOVE`/`LPUSHX`/`RPUSHX`/`LMPOP`
  (`src/command/list/list_write.rs`, `src/storage/db.rs`'s
  `list_push_front`/`_back`/`list_pop_front`/`_back`); `SADD` (intset +
  HashSet paths)/`SREM`/`SPOP`/`SMOVE` (`*STORE` variants were already
  correct — they replace the whole entry via `Database::set`, which
  already accounted correctly); `ZADD`/`ZREM`/`ZINCRBY`/`ZPOPMIN`/
  `ZPOPMAX`/`ZMPOP`/`ZUNIONSTORE`/`ZINTERSTORE`/`ZRANGESTORE`
  (`src/command/sorted_set/sorted_set_write.rs`). Whole-key removal paths
  (`Database::remove`/`Database::set`) needed no changes — they already
  recompute `entry_overhead` on the final (already-mutated) entry, an O(n)
  cost paid once per key lifetime, not per mutation, so they correctly
  credit a grown container's true size when it's fully removed or
  replaced.
- The eviction loop's before/after `estimated_memory()` delta arithmetic
  (`src/storage/eviction.rs`) required no code changes — it already reads
  the (now-correct) live `used_memory` accumulator.
- RED/GREEN: `tests/container_growth_memory_accounting.rs` (growing one
  HSET/LPUSH key past `--maxmemory` under `noeviction` now rejects further
  writes; draining a grown hash via `HDEL` correctly credits the freed
  bytes back). Unit tests per container family (hash/list/set/zset
  `mod.rs`) assert `estimated_memory()` rises/falls with insert/remove/
  overwrite, including a same-length overwrite netting a zero delta.
- Making this growth visible surfaced a second, previously-unreachable
  bug — a self-inflicted write lockout where even shrink commands like
  `HDEL` were rejected on an over-quota key. Now fixed; see the dedicated
  entry above.

### Added — sharded MULTI/EXEC routes a single-owner-shard body to its owner (Phase B, PR #TBD)

- **`src/shard/{dispatch,spsc_handler,coordinator}.rs`,
  `src/server/conn/{handler_sharded,handler_monoio}/write.rs`**: with keys routed
  per-key to their owning shard but MULTI/EXEC executing on the connection's
  (random, SO_REUSEPORT) accept shard, a hash-tagged `{tag}` transaction only
  worked from the ~1/N connections that happened to land on the owning shard —
  Phase A rejected the rest with `CROSSSLOT`. Phase B adds a boxed
  `ShardMessage::TxnExecute` (kept within the 64-byte cache-line cap like
  `MqCommand`) and a `coordinator::execute_txn_on_owner` hop: when
  `analyze_txn_locality` proves every key is owned by ONE shard that isn't the
  accept shard, the whole body is routed there, executed atomically on the
  owner's slice, and each write persisted to the OWNER's AOF/WAL via the same
  `wal_append_and_fanout` path as normal cross-shard writes. PUBLISH fan-out is
  deferred back to the originating connection (returned in the reply) so it keeps
  the normal scatter path; under `appendfsync=always` the originator issues one
  `fsync_barrier` to the owner before acking (H1-BARRIER parity), and owner-side
  append backpressure surfaces `AOF_APPEND_LOST_ERR`. Genuinely multi-shard
  bodies (`CrossShard`) stay rejected — a shared-nothing engine can't commit
  across shards atomically. Red/green: `tests/sharded_multi_exec_routing.rs`
  (hash-tagged txn succeeds from every connection + writes visible; routed writes
  survive kill-9 + restart via the owner's AOF; multi-shard span still CROSSSLOT).
  WATCH in sharded mode remains an unimplemented follow-up.

### Fixed — sharded MULTI/EXEC writes are now persisted (were lost on restart) (PR #TBD)

- **`src/server/conn/{shared,handler_sharded/write,handler_monoio/write}.rs`**:
  `execute_transaction_sharded` — the MULTI/EXEC executor used by the monoio
  handler at **every** shard count (including `--shards 1`) and by the tokio
  sharded handler at `--shards ≥ 2` — appended **nothing** to the AOF. Every
  transactional write was silently lost on restart under `appendonly=yes`, while
  an identical write issued outside MULTI survived. The executor now returns the
  serialized AOF bytes for each successful write (mirroring the single-shard
  tokio `execute_transaction`), and a shared `persist_txn_aof` helper appends
  them to the owning shard's writer via the normal group-commit path, issuing one
  `fsync_barrier` under `appendfsync=always` before EXEC is acked. On a barrier
  failure EXEC returns `AOF_FSYNC_ERR` and suppresses any queued PUBLISH fan-out,
  rather than acking a durability it can't guarantee. `try_handle_multi_exec` is
  now `async` in both sharded handlers. Red/green:
  `tests/sharded_multi_exec_durability.rs` pins *EXEC-committed write survives
  kill-9 + restart, exactly like a non-MULTI write* (fails on the pre-fix path:
  the txn key returns nil after restart while the plain control key survives).

### Security — sharded MULTI/EXEC no longer silently misplaces cross-shard writes (Phase A, PR #TBD)

- **`src/server/conn/{shared,handler_sharded/write,handler_monoio/write}.rs`**:
  `execute_transaction_sharded` runs the whole queued body on the connection's
  OWN shard with no per-key routing, so at `--shards ≥ 2` a key owned by another
  shard was silently written to / read from the wrong shard's table — EXEC
  reported success while the data diverged (silent lost updates; other
  connections routing to the true owner saw nothing). Hash tags did **not** help:
  they co-locate keys with each other but not with the (random, SO_REUSEPORT)
  execution shard, and migration is disabled during MULTI. A new
  `analyze_txn_locality` classifies the queued body via the command-metadata key
  specs + `key_to_shard`; EXEC now **rejects** with `CROSSSLOT` any transaction
  whose keys aren't all owned by the executing shard, instead of corrupting.
  Invariant restored: *EXEC-success ⇒ every write is visible to other
  connections; CROSSSLOT ⇒ nothing was written.* No effect at `--shards 1`.
  Red/green: `tests/sharded_multi_exec_locality.rs` (silent-divergence invariant,
  multi-shard span rejection, single-shard unaffected) + `analyze_txn_locality`
  unit tests. Phase B (above) now routes single-owner-shard bodies to their owner
  instead of rejecting them, so only genuinely cross-shard bodies still get
  CROSSSLOT. Note found in passing (unrelated, out of scope): a *solo* GET sent
  mid-MULTI on the monoio single-shard handler executes immediately instead of
  queueing.

### Added — `--profile standalone` tuning preset (v0.6.0 WS4)

- New `--profile <name>` flag (`src/config.rs`). `standalone` fills the proven
  single-instance p=1 recipe — `--shards 1`, `--io-busy-poll-us 40` (implying
  `--io-driver epoll`) — for any of those flags the operator left unset.
  Fill-only precedence: an explicitly-passed flag (CLI or `moon.conf`) always
  wins over the preset. Startup logs exactly which flags the profile set;
  an unknown profile name is a startup error (exit 2), never a silent no-op.
- `ServerConfig::parse_from_with_matches` + `ServerConfig::apply_profile` use
  `clap::ArgMatches::value_source` to distinguish explicit flags from
  defaults; `FromArgMatches::from_arg_matches` (non-consuming, clones
  internally) is used instead of `from_arg_matches_mut`, which removes
  consumed entries from `ArgMatches` and would erase this provenance.
- **Safety:** `--io-busy-poll-us` busy-polls the shard thread and REGRESSES
  throughput on shared/unpinned cores (OrbStack default, laptops,
  noisy-neighbor cloud VMs) — `standalone` prints a prominent startup warning
  requiring pinned/dedicated cores, and `docs/guides/tuning.md#profiles` +
  `docs/configuration.md` document the same caveat. No raw flag default
  changed.
- Tests: profile expansion, explicit-flag-wins precedence, no-profile no-op,
  and unknown-profile error (`src/config.rs` `tests::test_profile_*`).
### Added — WS1 command parity: `*_RO` variants + ACL LOG/CLIENT LIST/OBJECT HELP audit (PR #TBD)

- **`BITFIELD_RO`, `SORT_RO`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER_RO`**: new
  read-only twins registered in the `phf` command registry with all three
  dispatch paths wired (mutable `dispatch()`, immutable `dispatch_read()`,
  and the `is_dispatch_read_supported()` fast-reject bucket list — a command
  missing that last one is unreachable over the wire despite compiling and
  passing unit tests). Each rejects its write-capable subcommand/option
  (`SET`/`INCRBY`/`OVERFLOW` for BITFIELD_RO; `STORE` for SORT_RO;
  `STORE`/`STOREDIST` for the GEORADIUS twins) via positional parsing (not a
  blind token scan, so a `GET`/`BY` pattern value that happens to read
  "STORE" is never misclassified).
- **CLIENT LIST/INFO**: added the missing Redis fields (`laddr`, `multi-mem`,
  `tot-net-in`/`tot-net-out`, `rbs`/`rbp`/`obl`/`oll`/`omem`, `events`,
  `cmd`, `redir`, `resp`, `lib-name`, `lib-ver`) so key=value parsers no
  longer choke on absent keys; `redir` and the tracking flag stay at their
  "off" defaults; wiring them to the CLIENT TRACKING state shipped in PR #234
  is a known follow-up.
- **Audit findings**: `ACL LOG` (real entries + RESET) and `OBJECT HELP`
  were already fully implemented — added regression tests to lock in that
  coverage rather than re-implementing.
- `docs/redis-compat.md` regenerated against `src/command/metadata.rs`
  (258 commands): fixed stale claims that `WAIT` and `FUNCTION *` are
  unimplemented (both are live), documented the four new `_RO` commands,
  and added an explicit non-goals section for `PFDEBUG`, `PFSELFTEST`,
  `FAILOVER`, `MODULE *`, `SENTINEL *` with one-line rationale each.
- `scripts/test-consistency.sh` (+section 9b) and `scripts/test-commands.sh`
  (key-commands category) gained coverage for all four new commands.

### Fixed — WS5b fix-first adversarial review: db-quota bypass on non-inline writes

- **Critical**: `--maxmemory 0` combined with `--disk-offload disable` (no
  disk-offload spill sender) silently bypassed the per-db quota gate for
  every non-inline write command (HSET/LPUSH/SADD/ZADD/INCR/APPEND/MSET/
  SET-with-options/RESTORE/...) — plain `SET`/`GET` were unaffected because
  they take a separate, always-correctly-gated inline fast path. Root cause:
  `handler_monoio`'s `batch_eviction_active` and its cross-shard-leg twin
  `spsc_handler`'s `evict_active` only checked
  `spill_sender.is_some() || maxmemory != 0`, entirely skipping the
  write-eviction-gate call (and the db-quota check nested inside it) when
  neither was true. Fixed by adding `db_quota::db_maxmemory_any_set()` to
  both conditions, mirroring the Lua bridge's gate (which already had this
  term). RED/GREEN TDD via `git apply -R`; new integration test
  `test_quota_rejects_non_inline_writes_without_spill_sender` in
  `tests/db_maxmemory_quota.rs` (HSET + SET-with-EX). Manually re-verified
  `RESTORE` also rides the fixed gate (rejected before payload
  deserialization).
- Along the way, found a second, independent, pre-existing bug: `used_memory`
  accounting for Hash (and, by the same code pattern, List/Set/ZSet) is only
  charged once at key-creation time for the empty container's overhead —
  subsequent field/element inserts into an EXISTING key never update
  `used_memory`. This defeats the *global* `--maxmemory` gate identically
  (verified: growing one hash key's fields never trips a small
  `--maxmemory` cap regardless of value size), so it predates and is
  independent of db-quota. At the time this was out of scope to fix here
  (touches every container-type mutation site, a much larger body of work)
  and was documented as a known limitation; **now fixed** — see "WS6:
  container-growth memory accounting gap" above.
- `--db-maxmemory` CLI parsing now fails fast at startup (`REFUSING TO
  START:` + nonzero exit, matching this file's existing trusted-config
  validation convention) instead of silently warning and dropping a
  malformed/out-of-range entry — this is trusted operator config supplied
  at launch, not untrusted wire input, so a typo should refuse to start
  rather than silently leave a db unprotected. `CONFIG SET db-maxmemory`
  was already this strict; the CLI form now matches. New
  `config::validate_db_maxmemory_cli()`; new tests
  `test_malformed_db_maxmemory_cli_refuses_to_start` /
  `test_out_of_range_db_maxmemory_cli_refuses_to_start`.
- Documented (not changed): `WS DROP`'s all-dbs cascade-delete sweep is a
  synchronous, in-place O(total keys × `--databases`) scan on the owning
  shard's event-loop thread — it blocks every other connection pinned to
  that shard for the duration. Accepted trade-off for an admin-rare
  operation at the default `--databases 16`; flagged in
  `docs/guides/isolation.md` and inline code comments for large-keyspace,
  large-`--databases` deployments.

### Added — per-db resource quotas + workspace hardening sweep (WS5b, PR #TBD)

- New per-db memory quota: `--db-maxmemory <db>:<bytes>` (repeatable CLI
  flag) and `CONFIG SET/GET db-maxmemory <db> <bytes>` (0 = unlimited,
  default). Enforcement mirrors global `--maxmemory`: `noeviction`
  rejects writes at quota with a `MOONERR db maxmemory exceeded` error;
  eviction policies shed keys from the offending db only. Zero-cost
  when unconfigured via a single relaxed-atomic pre-gate
  (`DB_MAXMEMORY_ANY_SET`), same pattern as the existing global-maxmemory
  gate. New `src/storage/db_quota.rs`.
- Fixed a real bug found via this work: `SELECT`/`SWAPDB` are flagged
  `is_write` in moon's command-metadata table (for unrelated ACL/dispatch
  reasons), which meant a connection that filled a `noeviction`-quota'd db
  could not even `SELECT` away from it afterward — the eviction/quota gate
  ran against the pre-switch db index before `SELECT` updated connection
  state. Fixed for the new db-quota gate via
  `check_db_maxmemory_for_command()` (SELECT/SWAPDB exempt); the
  pre-existing identical quirk in the global `--maxmemory` gate is
  documented but deliberately left unfixed (out of scope — shared,
  widely-used eviction code).
- Fixed a real, pre-existing bug: `WS DROP`'s best-effort key cleanup
  only swept logical db 0 (hardcoded), silently leaking a workspace's
  keys forever if its connection ever `SELECT`ed to a non-zero db before
  writing (`WS AUTH` and `SELECT` are orthogonal connection state). Now
  sweeps every db on the owning shard. Proven via RED/GREEN TDD
  (`test_workspace_drop_cleans_keys_across_all_dbs`).
- New `docs/guides/isolation.md` — honest "isolation semantics" reference
  covering logical dbs, workspaces, and per-db quotas: what each
  guarantees, and every discovered limit (FLUSHDB is whole-db not
  workspace-scoped; MOVE/SWAPDB reconciled lazily by a background sweep,
  not synchronously; no disk-offload spill integration for db-quota
  eviction; FT.* indexes remain keyspace-global, not workspace-scoped —
  handoff note for the concurrent WS5a db-scoped-FT-index work).
- New tests: 9 unit tests in `src/config.rs` (CLI/CONFIG parsing), 7 in
  `src/storage/db_quota.rs` (accounting + enforcement), 6 in
  `src/command/config.rs` (CONFIG GET/SET), 3 new workspace hardening
  tests in `tests/workspace_integration.rs` (WS DROP all-dbs regression,
  cross-workspace KEYS non-leakage, FLUSHDB-is-whole-db pin), and a new
  `tests/db_maxmemory_quota.rs` real-server integration test.

### Fixed — WS5a round 4: write-path db-isolation leak in auto-index/auto-delete hooks (adversarial review)

A follow-up adversarial delta-review of round 3 (below) confirmed all of
round 3's fixes, then found one more **CRITICAL, data-integrity-severity**
gap: the auto-index/auto-unindex WRITE-path hooks were never db-scoped,
despite the round-2 CHANGELOG entry (further below) explicitly claiming
they were. Unlike the read-path leak fixed in round 3 (a foreign db could
merely *see* another db's search results), this gap let a foreign db
*corrupt or erase* another db's index contents — and it triggers at
`--shards 1` (no multi-shard fan-out required):

- **`auto_index_hset`** (`src/shard/spsc_handler.rs`, private; reached via
  `auto_index_hset_public`/`_public_txn` from `handler_single.rs`,
  `handler_monoio/mod.rs`, `handler_sharded/mod.rs`, `server/conn/shared.rs`,
  and the sharded MULTI/EXEC replay path) called UNSCOPED
  `vector_store.find_matching_index_names(key)` /
  `text_store.find_matching_index_names(key)`. An `HSET` issued from ANY db
  whose key happened to match another db's index PREFIX silently fed that
  foreign index. Fixed via `find_matching_index_names_for_db`, with
  `db_index: u8` threaded through both public wrappers and every call site
  (`conn.selected_db as u8` / `sel_db as u8` / `db_idx as u8`, matching the
  idiom already established for FLUSHDB scoping at each of those sites).
- **`auto_delete_vectors`** (same file) called UNSCOPED
  `vector_store.mark_deleted_for_key`, so a `DEL`/`UNLINK` issued from ANY
  db could tombstone another db's vector entry for a same-named key — the
  already-existing, already-unit-tested `mark_deleted_for_key_for_db` had
  **zero production callers** before this fix. Now threaded the same way.
- **`auto_hdel_vectors`** (bonus fix, same bug class, not explicitly named
  by the review but caught while auditing the surrounding code): also
  called unscoped `find_matching_index_names`; now uses
  `find_matching_index_names_for_db`.
- **The inline `DEL`/`UNLINK` unindex path** inside `ShardMessage::Execute`
  (`spsc_handler.rs` ~line 747) called unscoped `mark_deleted_for_key`
  directly (not through `auto_delete_vectors`) — fixed to
  `mark_deleted_for_key_for_db(key, db_idx)`.
- **Restart-time reconciliation** (`RecoveryState::reconcile_key` in
  `src/vector/persistence/recover_v2.rs`, called from `event_loop.rs`'s
  per-db rescan loop) had the SAME unscoped `find_matching_index_names`
  call and delegated to the unscoped `auto_index_hset_public`. Left
  unfixed, this would have silently RE-INTRODUCED the exact write-path leak
  on every server restart even after the live-write fix above — closed by
  threading `db_index: u8` through `reconcile_key`, sourced from
  `event_loop.rs`'s existing `for db_idx in 0..db_count` loop (the caller
  already had the right value; it just wasn't being passed in).
- **Checked and cleared**: active expiry (`expire_cycle`/`expire_cycle_direct`
  in `src/server/expiration.rs`, ticked per-db by `run_active_expiry` in
  `src/shard/timers.rs`) and hash-field lazy/active expiry do not call any
  vector/text unindex hook at all — `Database::remove` is a pure KV-layer
  operation with no vector_store/text_store hook. There is therefore no
  unscoped call to fix on the expiry path; this is a separate, pre-existing
  "TTL'd keys never get unindexed at all" gap (vectors/text entries for an
  expired key linger until an explicit DEL or HDEL), out of scope for a
  db-isolation fix and not new in this pass.
- New integration tests in `tests/vector_db_isolation.rs`:
  `write_path_auto_index_does_not_leak_across_db` (FT.CREATE in db 0, HSET
  of a prefix-matching key from db 5, assert db 0's `FT.INFO num_docs`
  unchanged and FT.SEARCH does not return the foreign doc) and
  `write_path_auto_delete_does_not_leak_across_db` (HSET in db 0, DEL of
  the same key name from db 5, assert the db-0 document remains
  searchable). Both verified RED against the unfixed hooks (real
  cross-db corruption reproduced with the exact documented symptoms),
  GREEN after the fix. Note: `FT.INFO num_docs` is `mutable_segment.len()`
  (append-only; a tombstone doesn't decrement it), so the DEL-side test
  asserts on `FT.SEARCH` hit count instead — an early draft of that
  assertion only checked "not Nil", which a foreign-db tombstone's
  empty-but-OK `[0]` result also satisfies, and silently passed against
  the unfixed code; corrected to check for a real (`> 0`) hit count.

### Fixed — WS5a round 3: multi-shard text-search db-isolation leak + hardening (adversarial review)

A fix-first adversarial review of round 2 (below) found the headline
"index created in db N is invisible from every other db" promise still
had a **critical multi-shard gap**: `scatter_text_search` (the plain-text/
BM25 FT.SEARCH scatter path), plus the remote legs of `scatter_hybrid_search`
and `scatter_text_aggregate`, took no `db_index` at all — on `--shards >= 2`
a connection on any db got real results from another db's TEXT index. Round
2's own suite only spawned single-shard servers, so the bug was invisible
to CI (single-shard scatter degenerates to the already-scoped local path).

- **`scatter_text_search`** (`src/shard/coordinator.rs`) now threads
  `db_index` through its `num_shards == 1` fast path AND its true
  multi-shard DFS fan-out (both the local DocFreq/TextSearch legs and the
  remote `DocFreq`/`TextSearch` SPSC legs), via new `db_index: u8` fields
  on `TextSearchPayload`, `InvertedSearchPayload`, and a newly-boxed
  `DocFreqPayload` (boxing was required to keep `ShardMessage` within its
  64-byte cache-line cap after adding the field to the previously-inline
  `DocFreq` variant). The dead `scatter_text_search_filter` was scoped the
  same way rather than left as a latent unscoped copy.
- **`scatter_hybrid_search`'s remote leg** and **`scatter_text_aggregate`'s
  remote leg** — previously documented below as an open residual gap — are
  now also `db_index`-scoped (`FtHybridPayload`/`TextAggregatePayload` gain
  the field; `execute_hybrid_search_local_raw_streams` and
  `execute_local_partial` take it as a parameter). **The "known gap" note
  in the round-2 entry below is superseded: both legs are closed.**
- **`VACUUM VECTOR`** (`src/command/server_admin.rs::vacuum_vector`) was an
  unscoped existence oracle — any db could merge/compact/probe another
  db's vector index by name. Now takes `db_index` and resolves ownership
  via `get_index_for_db`/`get_index_mut_for_db` before any mutation or
  existence check; the subsequent unscoped `needs_merge`/
  `immutable_segment_count`/`force_merge_index` calls are safe unchanged
  (index names are globally unique per shard, so an ownership check by
  name is sufficient once performed).
- **`--databases` is now bounded to 256** (`ServerConfig::MAX_DATABASES`,
  `validate_databases_bound()`, checked unconditionally at both normal
  boot and `--check-config`). Vector/text index db-scoping uses a `u8`
  tag; an unbounded `--databases` (e.g. 300) silently aliased `SELECT 256`
  onto db 0's indexes. Deliberately NOT widened to `u16` — 256 dbs is
  already far beyond Redis's default of 16.
- **New multi-shard coverage** in `tests/vector_db_isolation.rs`
  (`--shards 4`): plain-text FT.SEARCH cross-db invisibility (the exact
  finding-1 scenario), KNN cross-db invisibility, and FT._LIST scoping,
  all under real multi-shard fan-out. Verified RED (real cross-db hits
  leaked) against the unfixed `scatter_text_search`, GREEN after the fix.
- Stale comments in `handler_monoio/ft.rs` / `handler_sharded/ft.rs` /
  `coordinator.rs`'s test module claiming `execute_text_search_local` is
  the multi-shard path were corrected — that function is dead code outside
  its own unit tests; the real multi-shard path is `scatter_text_search`
  → `run_text_query_on_index`.

### Fixed — WS5a: db-scoped vector/text index isolation (headline bug closed)

Round 1 shipped only the data model (see below) — FT.CREATE still tagged
every index `db_index: 0`, so no read-path scoping had any observable
effect outside db 0. Round 2 closes the headline promise: **an index
created in db N is invisible from every other db, across all three
dispatch paths (`handler_single`, `handler_sharded`/tokio,
`handler_monoio`), including the multi-shard scatter/broadcast legs.**

- **`IndexMeta`/`TextIndex`** gain a `db_index: u8` tag (vector sidecar
  format bumped to v4, text sidecar to v2; legacy sidecars default to db 0,
  fully backward compatible).
- **FT.CREATE now tags the connection's actual SELECTed db** instead of a
  hardcoded 0. Naming decision: index **names stay globally unique per
  shard** (not a composite `(db, name)` key) — each index is bound to
  exactly one db; `FT.CREATE` of a name that already exists in ANY db
  (including the same db) errors `"Index already exists"`, unchanged from
  pre-WS5a behavior. This avoids migrating the `HashMap<Bytes, _>` key
  type across ~150+ call sites for a benefit (same name reusable per db)
  nobody asked for; an operator wanting per-db name reuse renames (e.g.
  `idx_db0`, `idx_db1`).
- **Full read-path scoping**: FT.SEARCH, FT.INFO, FT._LIST, FT.DROPINDEX,
  FT.COMPACT, FT.CONFIG (GET/SET), FT.AGGREGATE, FT.CACHESEARCH,
  FT.RECOMMEND, FT.NAVIGATE, FT.INVALIDATE_RANGE, and hybrid search all
  resolve indexes scoped to the caller's current db across every dispatch
  path — ~45 call sites migrated from `get_index`/`get_index_mut` to
  `get_index_for_db`/`get_index_mut_for_db` (or the equivalent scoped
  helper), no new hot-path allocations (same `O(n)` filter shape as the
  unscoped originals).
- **Write-path scoping (claim corrected by round 4 below): this entry
  originally claimed `auto_index_hset` and the DEL/HDEL auto-unindex hooks
  only touched indexes bound to the write's db via
  `mark_deleted_for_key_for_db`. That claim was FALSE — the round-4
  adversarial review found `auto_index_hset`, `auto_delete_vectors`, and
  `auto_hdel_vectors` all called their UNSCOPED counterparts
  (`find_matching_index_names`/`mark_deleted_for_key`), and
  `mark_deleted_for_key_for_db` had zero production callers at the time
  this was written. See the "WS5a round 4" entry above for the actual fix.**
- **Cross-shard scatter/broadcast fully threaded**: `ShardMessage::VectorCommand`
  and `VectorSearchPayload` now carry `db_index` across the SPSC boundary;
  `broadcast_vector_command`, `scatter_ft_info`, `scatter_invalidate_range`,
  `scatter_vector_search`/`_remote` all honor it. `scatter_hybrid_search`
  and `scatter_text_aggregate`'s multi-shard DFS legs honor it on the
  `num_shards == 1` fast path; the true multi-shard remote leg of hybrid
  search and FT.AGGREGATE's `execute_local_partial`, **and the
  plain-text/BM25 `scatter_text_search` scatter path itself, were left
  fully unscoped** — a critical gap an adversarial review caught (single-
  shard-only test coverage hid it). **Closed in the WS5a round 3 entry
  above** — do not treat this note as still-open.
- **Bonus fix found in the same code path**: `scatter_text_aggregate`'s
  single-shard fast path always hardcoded `s.databases.first()` (db 0)
  for `@field` value materialization regardless of the caller's SELECTed
  db — fixed alongside the db_index threading (was a pre-existing bug,
  not a WS5a regression).
- **Known gaps, explicitly NOT implemented this pass** (see
  `.planning/v0.6.0-release/WS5A-NOTES.md` for the full design rationale):
  - **SWAPDB** does not retag index ownership. `SWAPDB a b` swaps the KV
    keyspace but leaves every index's `db_index` tag untouched — an index
    created in db `a` stays visible only from db `a` even after its data
    moves to db `b`. Target design (swap `db_index` on every index owned
    by either swapped db) is documented but not coded.
  - **MOVE/COPY** of an indexed hash does not re-index the key into the
    target db's matching index — operator must re-HSET in the target db.
  - **Graph engine** (`src/graph/store.rs`) is a single per-shard
    `GraphStore`, structurally global across all 16 logical dbs — same
    shape vector/text stores had before this fix, but NOT scoped in this
    pass (see "Known Limitations" below). Timeboxed analysis confirmed no
    per-db concept exists anywhere in the graph engine (no `db_index`, no
    per-db FLUSHDB differentiation); scoping it would require the same
    class of work done here for vector/text, sized as its own follow-up.
- Legacy (pre-v0.6.0) persisted sidecars continue to load with
  `db_index` defaulting to 0 (unit-tested, unchanged from round 1).
- New integration suite `tests/vector_db_isolation.rs` (real spawned
  server, real TCP client): cross-db invisibility both directions,
  FT._LIST db scoping, FT.CREATE cross-db name-collision error, and a
  true process-restart round-trip proving a db-1-scoped index reloads
  still bound to db 1.

### Fixed — WS5a: FLUSHDB no longer clears vector/text index contents across dbs (foundation)

- **`IndexMeta`/`TextIndex`** gain a `db_index: u8` tag (vector sidecar
  format bumped to v4, text sidecar to v2; legacy sidecars default to db 0,
  fully backward compatible).
- **FLUSHDB now scopes** to the connection's selected db —
  `VectorStore::clear_all_contents_for_db` /
  `TextStore::clear_all_contents_for_db`, wired through all 3 dispatch
  paths (`handler_single`, `handler_sharded`, `handler_monoio`) plus the
  cross-shard MULTI/EXEC replay path. FLUSHALL is unchanged (still clears
  every db). Previously FLUSHDB in ANY db cleared ALL index contents
  keyspace-wide — direct fix, covered by a new integration test
  (`tests/vector_flush_hdel_tombstone.rs`).
- Db-scoped lookup/listing/delete primitives (`get_index_for_db`,
  `index_names_for_db`, `find_matching_index_names_for_db`,
  `mark_deleted_for_key_for_db`, `drop_index_for_db`) added alongside the
  existing unscoped methods on both `VectorStore` and `TextStore`, unit
  tested.

### Docs — tuning guide: vector bulk load & compaction (PR #TBD)

- `docs/guides/tuning.md`: new "Vector bulk load and compaction" section — documents
  immediate-search-then-background-HNSW-build behavior, the `COMPACT_THRESHOLD`
  time-to-serve vs recall lever, the `MOON_VEC_COMPACT_WORKERS` pool knob, the
  ~10K-vector parallel-build threshold, per-shard bulk-load parallelism, and the
  ingest-rate/recall trade-offs. Operator guidance for the parallel-build +
  insert-path-compaction work shipped in #237.
- Removes a stray `<<<<<<< HEAD` conflict marker accidentally left in the
  `[Unreleased]` heading by #237's squash merge.

### Added — parallel HNSW build + insert-path compaction trigger: time-to-index-green 11× (PR #237)

- **`src/vector/hnsw/parallel_build.rs`** (new): concurrent HNSW
  construction into one shared graph — per-node `parking_lot::Mutex`
  adjacency (copy-under-lock, distance math unlocked; single lock held at a
  time, deadlock-free by construction), entry point under a `RwLock`,
  levels pre-generated from the same seeded LCG as the sequential builder,
  sequential 1K warmup, then dynamic fan-out over an atomic cursor.
  Finalize adds a connectivity repair pass (concurrent back-link pruning
  orphaned ~0.2% of nodes = permanent recall loss; BFS + force-link makes
  every node reachable, unit-asserted) and the exact same BFS-reorder →
  `HnswGraph` path as the sequential builder — drop-in for search,
  persistence, and GraphUnion merge. `compact()` routes builds ≥ 10K
  vectors here; smaller segments keep the bitwise-deterministic
  single-threaded builder. Measured (50K × 384d, 6-core Linux VM):
  `FT.COMPACT` wall 30.2s → **2.71s**; a 24K segment build 14.1s →
  **2.65s** (~88% scaling efficiency).
- **Affinity-mask trap fixed (3 sites)**: shard threads are core-pinned and
  every thread spawned from one inherits the SINGLE-core mask on Linux, so
  `std::thread::available_parallelism()` returned 1 — the "parallel" build
  ran at exactly sequential speed and the background-compactor pool sized
  itself to one worker. New `shard::numa::system_parallelism()` (sysfs
  online-CPU count, affinity-independent, cached) now sizes both; parallel
  build workers and pool workers explicitly re-pin round-robin across the
  machine (`pin_worker_to_core`), pool workers from the last core downward
  so small builds stop time-slicing shard 0's core.
- **`src/shard/spsc_handler.rs`**: HSET auto-index hook now calls
  `try_compact()` after appending a vector — a pure bulk load (no
  FT.SEARCH traffic) previously left everything in the brute-force mutable
  tier until the autovacuum backstop's 30s tick, so the whole HNSW build
  landed on the first explicit `FT.COMPACT`. Builds now start and install
  DURING ingest (in-flight guard unchanged; respects
  `FT.CONFIG AUTOCOMPACT OFF`). Red/green: new
  `test_insert_path_triggers_background_compact_without_search`.
- Net effect on the §10.8 losing metric (bulk load 50K × 384d → HNSW-tier
  serving, VM): ~30s of post-ingest compaction becomes ~0 (already
  compacted by measure time) — Moon's time-to-green now beats Qdrant's
  optimizer on the same box (GCE re-validation pending). Multi-segment
  recall@10 0.9986 vs 0.9992 single-segment (−0.0006, within the ≥0.99
  gate); multi-shard verified (`--shards 4`: per-shard triggers, 9
  segments, FT.COMPACT 1.57s).

### Docs — tuning guide: durability + pub/sub enhancements (PR #TBD)

- `docs/guides/tuning.md`: rewrote the "Persistence: what durability costs"
  section to match the write-path campaign — `everysec` is now a win at
  pipeline depth (~1.32× Redis), `always` is disk-fsync-bound (parity floor)
  and safe to pipeline via group commit (P16 0.12×→0.91×), with a
  "which policy?" decision line. All framed as automatic behavior (no knobs).
- New "Pub/sub fan-out" section: coalesced delivery (5.09M msg/s, zero drops)
  and the intentional slow-subscriber drop policy (256-msg queue cap).
- Quick-recipe table: refreshed the durable-store row and added a pub/sub row.
  Cross-links to `BENCHMARK.md` §7.3.

### Docs — durability write-path benchmark results (PR #TBD)

- `BENCHMARK.md` §7.3: new section recording the 2026-07-08 durability
  write-path campaign (PRs #238–#242) — the vs-Redis matrix (`always` P16
  0.12×→0.91×, `everysec` P16 1.32× win, `everysec` P1 0.80×→0.99× parity,
  pub/sub fan-out 438→5.09M msg/s), per-PR root-cause table (strace
  diagnostics), durability-invariant note, and A/B reproduction steps.
  Executive summary + header updated.
- `docs/production-guide.md`, `docs/benchmarks.md`, `docs/configuration.md`:
  AOF `appendfsync` tuning guidance updated with the measured ratios and a
  plain-language explanation of group commit / coalesced writes / park-free
  writer poll (all automatic; durability unchanged).

### Fixed — Windows build: `accept_backoff` used unix-only `libc` errnos (PR #TBD)

- `is_resource_exhaustion` (accept-loop backoff, PR #230) referenced
  `libc::EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM` unguarded; `libc` is not linked
  on Windows, breaking the main-push Windows check (PRs never caught it —
  Windows CI is skipped on PRs). Now cfg-split: unix keeps the errno match,
  Windows matches WSAEMFILE (10024) / WSAENOBUFS (10055) /
  `ErrorKind::OutOfMemory`. Follow-up: the module's unit test
  (`resource_exhaustion_classification`) also referenced `libc` errnos —
  now cfg-split the same way (unix errnos vs WSA codes).

### Changed — AOF writer coalesces each group-commit batch into one write (PR #TBD)

- The two monoio AOF writer paths (TopLevel via `commit_group_commit_batch`,
  PerShard framed loop) wrote each record with its own `write(2)` on the raw
  unbuffered `File` (the PerShard path even used a header+body pair). strace
  during always-P16 SET: **127,812 write calls / 1.25 s** of an 8 s window vs
  2,144 fdatasyncs / 0.2 s — the writer thread was write-syscall-bound, not
  fsync-bound, while Redis batches ~120 records per write via `aof_buf`. Each
  batch's records now coalesce into one contiguous buffer (reusable in the
  PerShard loop, capped at 1 MB high-water) and are written with ONE
  `write_all` before the single per-batch fsync. Single-message batches keep
  the zero-copy direct write. Failure semantics unchanged: a failed batch
  write acks every waiter `WriteFailed` and engages the torn-stream latch.
- tokio writer loops already amortize via `BufWriter` — untouched.
- `wal_group_commit` sink tests updated to the coalesced contract (one write,
  channel-order bytes, fsync after it).

### Changed — AOF writer polls park-free under everysec/no (PR #TBD)

- The two std-thread AOF writer loops (monoio TopLevel + PerShard) no longer
  park in `recv_timeout` under `appendfsync everysec`/`no`. A parked receiver
  makes every producer `try_send` pay a futex WAKE **on the shard thread** —
  measured at ~150k futex calls (63% of shard-thread syscall time) during an
  8s non-pipelined SET run, the mechanism behind Moon's everysec P1 SET
  deficit vs Redis (whose AOF append is a plain memcpy). The writer now polls
  `try_recv` with adaptive sleeps (wait/16, clamped 500µs–50ms) so producer
  sends stay pure userspace atomics; post-fix the same run shows 96 futex
  calls. `appendfsync always` keeps the parked recv (its callers await the
  per-batch fsync ack, so receive latency is client-visible RTT).
- EverySec durability bound unchanged: at the 50ms fast floor the deadline is
  re-checked within ~3ms slack; idle cost is ≤20 writer wakes/s at the 1s
  escalated wait (vs 1/s parked — still far below the pre-wave-5 fixed 50ms
  cadence).

### Changed — Pub/sub subscriber delivery coalesces message bursts (PR #TBD)

- All three connection handlers' subscriber delivery arms (`handler_monoio`,
  `handler_sharded`'s `run_subscriber_step`, `handler_single`) now drain the
  subscriber queue (`try_recv`, capped at 64 KB) and deliver the burst with
  ONE `write_all` instead of one write syscall per message — the same batched
  write pattern the command path already uses. Raises the fan-out delivery
  ceiling and shortens the window in which a slow subscriber's bounded queue
  (256) overflows and drops messages. The single-message case keeps the
  zero-copy path (`is_empty` fast path, no allocation; `handler_sharded`
  reuses the connection's `write_buf`).
- New wire-level integration test `tests/pubsub_burst_delivery.rs`: a
  pipelined publish burst must arrive complete, frame-boundary-intact, and in
  order at 1 and 4 shards (both runtimes).

### Changed — `appendfsync always`: local writes now group-commit per pipeline batch (PR #TBD)

- **All three connection handlers** (`handler_monoio`, `handler_sharded`,
  `handler_single`): plain local writes (plus MOVE/COPY and single-shard
  GRAPH.* WAL records) no longer await one fsync ack **per command** under
  `appendfsync always`. Appends are enqueued fire-and-forget
  (`send_append_group`) and the whole pipelined batch is confirmed by ONE
  `fsync_barrier` before response serialization — the same contract
  cross-shard writes and coordinator local legs already used (PR #213).
  The writer processes its channel in order, so an acked barrier proves
  every prior append durable; the fsync-before-ack H1 guarantee is
  unchanged (a failed barrier converts every joined response to an error,
  never a silent `+OK`).
- **Why**: a 16-deep pipeline paid 16 serialized fsync round-trips per
  connection while Redis fsyncs once per event-loop iteration — measured
  8× SET-throughput deficit at P16 (Moon 5.2k vs Redis 44k ops/s, GCE
  c3-standard-8, tmp/MOON-VS-REDIS-DURABILITY.md). Writer-side group
  commit existed but could only batch *across* connections; the per-command
  await defeated it *within* a connection.
- everysec/no policies are unchanged (`send_append_group` degrades to the
  same bounded-backpressure enqueue).
- `flush_with_aof_ack`'s discriminating H1 ordering test updated to the
  batch protocol (one `Append` + one `AppendSync` barrier; ack still gates
  the first response).

### Changed — WAL v3 fsync moved off the shard event loop (PR #TBD)

- **`src/persistence/wal_v3/sync_agent.rs` (new)**: per-shard `WalSyncAgent`
  thread receives fd-dup'd sync requests over a bounded flume channel and
  publishes a monotonic durable-LSN watermark after each `fdatasync`.
  `WalWriterV3` gains `request_sync()` (non-blocking initiation; queue-full
  falls back to inline fsync — a durability request is never dropped) and
  `wait_durable(lsn, timeout)` (bounded blocking wait, used only by the two
  checkpoint ordering invariants and shutdown). An fsync error poisons the
  agent permanently and fails subsequent syncs loudly; the checkpoint then
  refuses to advance `redo_lsn`.
- **Why**: `flush_sync()` ran `fdatasync` on the shard event-loop thread.
  Measured on GCE pd (tmp/WALV3-OFFLOOP-FSYNC.md): the everysec 1s timer
  froze every connection on the shard for **10–16 ms once per second** when
  the WAL held real bytes, and `appendfsync always` paid −20% RPS / ~2× tail
  on top of the AOF cost.
- **Call sites**: everysec timer (`timers::sync_wal_v3`) and the always-mode
  per-drain-batch sync (both runtimes) now use `request_sync()`; the
  checkpoint log-before-data page gate and WAL-before-manifest finalize use
  `wait_durable` (5s bound); shutdown paths keep the inline `flush_sync()`.
  everysec semantics are now "sync initiated every 1s, durable typically ms
  later" — the same window the AOF everysec writers provide.
- Loom model for the watermark/poison state machine in
  `tests/loom_wal_sync_agent.rs`.

### Changed — consolidated dependency bumps wave 2 (PR #TBD, supersedes dependabot #223–227)

- Patch-level bumps rolled into one `Cargo.lock` update (no `Cargo.toml`
  edits — all within existing ranges): `bumpalo` 3.20.2→3.20.3,
  `uuid` 1.23.1→1.23.4, `smallvec` 1.15.1→1.15.2,
  `unicode-segmentation` 1.13.2→1.13.3, and the crypto-tls group
  `rustls` 0.23.40→0.23.41 + `aws-lc-rs` 1.17.0→1.17.1
  (pulls `aws-lc-sys` 0.41.0→0.42.0).
- **Perf side-effect verified** by same-instance p=1 KV A/B (deps vs main)
  on GCE `c3-standard-4` (x86) + `c4a-standard-4` (ARM), shard=1, spin={0,40},
  clients={1,8}, best-of-5, steal=0%. All single-op cells land 0.974–1.008
  (`new_vs_base`) on both arches — no regression from `smallvec` on the
  dispatch/protocol hot path, nor from the `aws-lc-sys` 0.42 codegen. See
  `tmp/DEPS-WAVE2-AB.md`.
- **CI-only GitHub Actions bumps** folded in (no runtime effect):
  `actions/setup-node` 4→6 (release.yml), `actions/setup-python` 5→6
  (docs.yml), `taiki-e/install-action` 2.81.10→2.82.9 (fuzz.yml) —
  supersedes dependabot #220/#221/#222.

### Changed — BREAKING: WAL v3 is now the only WAL; XactCommit unified to the db-aware record (pre-1.0 format freeze)

Moon is pre-release: this drops on-disk format compatibility that no shipped
release depended on. There is no migration path — operators upgrading a node
that still has WAL v2 files (`shard-N.wal`) or pre-freeze XactCommit(0x51)
records on disk must let those age out via the AOF-authoritative recovery
path (below), or replay them on a pre-freeze build first and re-persist.

- **`src/persistence/wal_v3/record.rs`**: deleted the pre-1.0 `XactCommit`
  (v1, tag `0x51`) record type, its encoder, and its replay arm. Renamed
  `XactCommitV2` -> `XactCommit`, **keeping discriminant `0x53`** (`0x51` is
  retired, not reused — a stray `0x51` record in a v3 stream now decodes as
  an unrecognized tag rather than being silently reinterpreted). All
  producers (the TXN.COMMIT WAL record in `handler_sharded`/`handler_monoio`)
  already wrote only the v2 (now unified) record, so this is a pure
  simplification of the read side.
- **`src/persistence/wal.rs` deleted** — WAL v2 (`WalWriter`, `replay_wal`,
  `wal_path`) is removed in its entirety. WAL v3 (`WalWriterV3`,
  `src/persistence/wal_v3/`) is the only WAL, and is now also created in the
  old v2 niche (`--appendonly yes` with `--disk-offload disable`), rooted at
  `<persistence_dir>/shard-N/wal-v3/` instead of the old flat
  `<persistence_dir>/shard-N.wal` file — matching the disk-offload layout.
  This closes a latent gap: `--appendfsync always` previously only fsynced
  WAL v3 (a no-op in non-offload mode, since only v2 existed there); it now
  fsyncs the WAL unconditionally.
- **`replay_wal_auto`** rejects a v2-format WAL file (`RRDWAL` magic +
  version byte `2`) with a loud `WalError::UnsupportedVersion` instead of
  delegating to the deleted v2 replay path.
- **Recovery**: the `shard-N.wal` (v2) fallback rung is removed from both
  `persistence::recovery::recover_shard_v3_pitr` (disk-offload path) and
  `Shard::restore_from_persistence_v2` (legacy path) — `appendonly.aof`
  remains the recovery authority and is unaffected. `shared_databases::
  replay_graph_wal` (graph-feature boot-time WAL scan) is ported from the v2
  flat file to the v3 segment directory (`shard-N/wal-v3/*.wal`), matching
  `replay_workspace_wal`/`replay_temporal_wal`. Additionally, both legacy-dir
  recovery paths gain a **last-resort WAL v3 fallback**: when NO
  `appendonly.aof` exists, the legacy-mode `shard-N/wal-v3/` directory is
  replayed (with a loud partial-coverage warning — WAL v3's KV coverage is
  intentionally partial post-#211, so it never shadows a present AOF), and a
  leftover legacy `shard-N.wal` (v2) file on disk now triggers a
  `tracing::error!` naming the file instead of being silently ignored.
- **`wal_append_and_fanout` / `wal_fanout_has_work`** (`src/shard/
  spsc_handler.rs`) drop the v2 `wal_writer` parameter and the "v3
  supersedes v2" branch — a single `Option<WalWriterV3>` parameter, renamed
  `wal_writer`. `finalize_snapshot_success` no longer takes a WAL writer:
  v2's per-snapshot `truncate_after_snapshot(epoch)` had no v3 analogue — v3
  retention is LSN-driven (`WalWriterV3::recycle_aggressive` /
  `recycle_segments_before`, run from autovacuum Pass C and the checkpoint
  protocol) and already ran independently of legacy snapshot epochs, so
  nothing was lost by not porting it over.
- Deferred (unrelated to this change, found in passing): `shared_databases::
  replay_temporal_wal` scans `shard-N/` directly for `*.wal` files instead of
  `shard-N/wal-v3/` like its siblings — looks like a pre-existing directory
  mismatch, left as-is pending its own investigation.
### Fixed — CLIENT TRACKING invalidation actually works on sharded servers (PR #TBD)

- **`src/tracking/`, all three handler stacks**: RESP3 client-side-caching
  invalidation was non-functional outside the single-shard handler via five
  stacked defects: (1) sharded handlers registered the per-connection
  invalidation channel but never drained it — pushes went into a channel
  nobody read; (2) invalidation was gated on the WRITER's own tracking state
  (backwards — a non-tracking writer never invalidated anyone); (3) only the
  first key of a multi-key write was invalidated (`DEL k1 k2` left `k2`
  stale); (4) the tracking table was per-shard, so a reader and writer
  accepted on different shards never saw each other; (5) FLUSHALL/FLUSHDB
  never invalidated. The table is now process-global (`OnceLock`, gated by a
  relaxed `ACTIVE_TRACKERS` atomic so the hot path pays one load when
  tracking is unused), keys are extracted via the command-metadata key
  specs, and all write paths (local, cross-shard remote leg, multi-key
  coordinator, monoio inline fast-path via a tracking-aware gate, FLUSH) hook
  invalidation. Red/green: `tests/client_tracking_invalidation.rs` (4 tests,
  raw RESP3 client) fails 0/4 on the previous binary.

### Changed — pub/sub publish fan-out moved outside the registry lock (P1, PR #TBD)

- **`src/pubsub/mod.rs`** (`publish_shared`): PUBLISH previously held the
  per-shard registry **write** lock for the whole O(N) subscriber fan-out —
  and the SPSC drain held it across the entire drain cycle. High fan-out
  (10K cache clients on one invalidation channel) stalled every concurrent
  (UN)SUBSCRIBE and the shard's cross-shard message drain. The new 3-phase
  path snapshots matching subscribers under a brief read lock, serializes +
  `try_send`s lock-free, and takes the write lock only to remove slow
  subscribers actually hit. Handler PUBLISH sites (both runtimes), the SPSC
  `PubSubPublish`/`PubSubPublishBatch` arms, and the MQ trigger-notification
  loop all switched. Documented at-most-once trade (matches Redis).

### Fixed — PUBLISH inside MULTI executed immediately instead of queueing (C2, PR #TBD)

- **All three handler stacks**: the PUBLISH arm ran before the `in_multi`
  queue check, so `MULTI; SET k v; PUBLISH ch m; EXEC` fanned the message out
  at queue time — subscribers received it before the transaction's writes
  applied (even on DISCARD). PUBLISH now queues like any other command; both
  transaction executors intercept it and the handler fans it out after the
  transaction body, patching the reply placeholder with the real receiver
  count (remote shards awaited via targeted `PubSubPublish`). Discovered but
  NOT fixed (pre-existing, tracked as follow-up): sharded MULTI/EXEC executes
  the queued body on the connection's local shard regardless of key
  ownership — at shards=4 most txn-written keys are silently invisible to
  other connections; the new test documents this and runs on `--shards 1`.

### Added — pub/sub ↔ KV ordering guarantee documented + locked in (C1/C2, PR #TBD)

- **`src/pubsub/mod.rs`** module docs now state the contract: same-connection
  write→publish ordering IS guaranteed (a received message implies every
  preceding write on the publisher's connection is visible), cross-connection
  ordering is NOT, delivery is at-most-once. `tests/pubsub_kv_ordering.rs`
  pins both the pipelined `SET;PUBLISH` case and `MULTI{SET,PUBLISH}EXEC`
  across a 4-shard server. (P2 of the review — event-loop/scheduler coupling
  benchmark — deferred to a Linux box; macOS numbers are not decision-grade.)

### Security — channel ACL enforced on the transactional PUBLISH path (C2 follow-up, PR #TBD)

- **`src/server/conn/{shared,handler_single,handler_sharded/mod,handler_monoio/mod}.rs`**:
  the C2 ordering fix queues `PUBLISH` inside `MULTI` and fans it out after the
  transaction body, but the fan-out skipped the per-channel ACL check that the
  immediate path enforces — so a client denied a channel could wrap `PUBLISH`
  in `MULTI/EXEC` to reach it. A shared `publish_channel_acl_deny` helper now
  gates every fan-out site (all three handlers); a denied channel is patched
  into the EXEC reply slot as `NOPERM` and never delivered. The check runs at
  fan-out time because Moon has no queue-time EXECABORT machinery. Also closes a
  pre-existing gap where the single-handler *immediate* PUBLISH never checked
  channel ACLs at all (the sharded/monoio immediate paths already did).
  Red/green: `tests/pubsub_multi_channel_acl.rs` (2 tests, 1- and 4-shard) —
  a restricted user's `MULTI; PUBLISH denied m; EXEC` returned `:0` on the old
  binary, `NOPERM` on the fixed one.

### Fixed — CLIENT TRACKING invalidation + cleanup edge cases (review follow-up, PR #TBD)

- **EXEC now invalidates tracked keys**: `SET`/`DEL`/`MSET` applied inside
  `MULTI/EXEC` bypassed the post-write invalidation hook, leaving RESP3
  client-side caches stale until the next non-txn write. All three handlers now
  run `invalidate_after_write` for each successful queued write (self-gated on
  `tracking_active()`, so no cost when tracking is unused).
- **monoio disconnect releases tracking registration**: a client that
  disconnected without `CLIENT TRACKING OFF` left `ACTIVE_TRACKERS` nonzero and
  kept `tracking_active()` hot for the rest of the process (the single/sharded
  handlers already cleaned up on close). The monoio disconnect path now calls
  `untrack_all`, gated on `tracking_active()` to keep the common no-tracking
  close lock-free.

### Fixed — TopLevel-monoio AOF writer: EverySec fsync deferred indefinitely when idle (PR #TBD)

- **`src/persistence/aof/writer_task.rs`**: the TopLevel monoio AOF writer
  blocked on an **untimed** `rx.recv()`, with its EverySec deadline check
  living only inside the batch-commit path. A batch written under
  `appendfsync everysec` gets no per-batch fsync, so if the client stopped
  writing right after a burst, the buffered bytes only became durable when
  the NEXT message happened to arrive — the 1s fsync bound was deferred
  indefinitely while idle. Exposure is host-crash-only (the per-batch
  `flush()` already reaches the kernel page cache, so a plain process kill
  loses nothing), which is exactly the window EverySec exists to bound.
  The loop now mirrors the audited PerShard writers: bounded
  `recv_timeout` on the wave-5 `IdleWait` ladder (50ms → 250ms → 1s,
  pinned at the floor while a batch awaits its fsync) plus an end-of-loop
  proactive fsync that runs on message AND timeout iterations. This
  supersedes wave 5's "TopLevel monoio needs none of this" note — it was
  the one writer loop left without the ≤ ~1s idle durability bound.
- **`tests/crash_matrix_per_shard_aof.rs` harness hardening** (found while
  validating the above): (1) `redis_set` asserted only redis-cli's exit
  status, which is 0 even for server ERROR replies — a tripped diskfull
  guard (host <5% free) turned every SET into a silent no-op and surfaced
  as a bogus "200 keys missing after recovery"; the helper now pins the
  reply to `+OK`. (2) The three server-spawning tests split-brain when run
  in parallel: `unique_port()` hands out OS-sequential ephemeral ports, the
  other tests offset +1/+2, and SO_REUSEPORT lets two tests bind the SAME
  port without an error — one test's redis-cli traffic lands on another
  test's server (rotating total-loss false alarms). A shared mutex now
  serializes them.

### Fixed — RSS/CPU remediation wave 5 (PR #TBD)

- **Item A — mmap the exact-rerank f16 sidecar on segment reload**
  (`src/vector/segment/raw_f16_store.rs`, new `RawF16Store` enum): a segment
  reloaded from disk used to `fs::read` the entire `raw_f16.bin` sidecar into
  a second heap `Vec<u16>`, doubling resident vector memory for
  reload-heavy deployments (warm starts, segment promotion). Reload now
  memory-maps the file (`memmap2`, already a workspace dependency) and hands
  out a zero-copy `&[u16]` view backed by the kernel page cache — RSS only
  grows for pages the rerank path actually touches. Freshly-built segments
  (compaction/merge) are unaffected — they keep their owned buffer. Rerank
  parity (Owned vs Mapped, byte-identical sidecar + identical `search()`
  output) is pinned by
  `test_reload_raw_f16_sidecar_uses_mmap_and_matches_owned_rerank`.
- **Item A follow-up — text posting-list capacity reclaim**
  (`src/text/posting.rs`): `PostingList::term_freqs`/`positions` grow to the
  peak document count ever seen for a term and, per the existing
  `remove_doc` contract, the `postings` HashMap entry is kept forever even
  once a term has zero live documents. The buffers now `shrink_to_fit()`
  once the last document leaves a posting, releasing peak capacity for
  terms that go idle without changing the "entry survives" contract.
- **Item B — AOF writer idle wake made adaptive** (`src/persistence/aof/writer_task.rs`,
  new `IdleWait` state machine): the 3 steady-state writer loops that need a
  bounded channel poll to service the EverySec proactive-fsync deadline
  (TopLevel tokio, PerShard tokio, PerShard monoio — TopLevel monoio blocks
  on an untimed `rx.recv()` and needed no change) used to poll at a FIXED
  cadence forever (50ms monoio / 200ms tokio), waking an idle server's AOF
  writer thread 5-20 times a second doing nothing. The wait now escalates
  50ms → 250ms → 1s once a poll times out with nothing queued, and resets to
  the floor the instant any message arrives — a real write always wakes the
  loop immediately regardless of the current timeout, since the poll races
  a message against the deadline. Escalation is refused (pinned at the
  floor) whenever a write is buffered under `FsyncPolicy::EverySec` without
  an immediate fsync, or `last_fsync` was manually back-dated (the F6
  post-fold drain trick) — the ~1.2s EverySec bound is provably unchanged.
  `FsyncPolicy::Always`/`No` have no such deadline and escalate freely once
  idle.
- **Item C1 — WAL v3 write buffer shrinks after an oversized flush**
  (`src/persistence/wal_v3/segment.rs`): a single large record (e.g. a
  FullPageImage) grew the 8KB write buffer to fit it, and `clear()` alone
  never released that capacity — the peak allocation was pinned for the
  writer's lifetime. `flush_write`/`rotate_segment` now `shrink_to` the
  8KB default once capacity exceeds 4x that, a no-op for the common
  small-record case.
- **Item C2 — SearchScratch visited-set: already bitset-based (SKIP)**
  (`src/vector/hnsw/search.rs`): the per-query search hot path already uses
  a word-based `BitVec` (u64 words, `test_and_set`/`clear_all` memset),
  thread-cached and reused across queries — no change needed. The other
  `Vec<bool>` visited sets found in the vector module are all build-time/
  compaction/merge-oracle code, not the per-query path; `search_sq.rs` in
  particular carries an explicit comment warning that a prior BitVec
  conversion there caused correctness issues, so it was left untouched.
- **Item C3 — SmallVec the per-tick elastic-budget shard snapshot**
  (`src/shard/shared_databases.rs`): `recompute_elastic_budget` (called
  from every shard's 100ms eviction tick) `collect()`ed a fresh
  `Vec<usize>` snapshot of all shards' published memory on every call.
  Switched to `SmallVec<[usize; 16]>` — stack-only for the common <=16
  shard case, unchanged single heap allocation beyond that.
- **Item C4 — Lua script-cache byte estimate exposed via INFO/MEMORY
  DOCTOR** (`src/scripting/cache.rs`, `ScriptCache::resident_bytes()`): the
  per-shard Lua cache was invisible to observability — its growth folded
  silently into "allocator overhead." Added a byte-estimate accounting
  method, published per-shard via the existing C5/M4 `ShardStoreMemory`
  tick pattern (new `lua` atomic), and surfaced in both the Prometheus
  `moon_memory_bytes{kind="lua_scripts"}` gauge and `MEMORY DOCTOR`'s text
  report. The cache itself remains intentionally unbounded (Redis parity —
  `SCRIPT FLUSH` is the only eviction path); this is observability only.
- **Item C5 — removed dead `parse_single_frame_zc` RESP parser**
  (`src/protocol/parse.rs`): a full ~150-line RESP2/RESP3 parser
  superseded by the current `validate_frame` + `parse_frame_zerocopy`
  pipeline, with zero external callers (only self-recursion) — silently
  masked by the file's `#![allow(dead_code)]`. Removed along with its
  exclusively-private helper `read_decimal_zc`.
- **Item C6 — jemalloc decay policy audited, docs added (SKIP code
  change)** (`CLAUDE.md`): the baked-in `_rjem_malloc_conf` static and the
  `--memory-arenas-cap` re-spawn override already carry byte-identical
  `dirty_decay_ms:1000,muzzy_decay_ms:5000,background_thread:true` tuning
  — no drift to reconcile. Added the missing operator-facing
  `_RJEM_MALLOC_CONF` documentation (docs-only, no code changed).
- **Item C7 — tokio 1ms shard tick idle cost audited (SKIP)**
  (`src/shard/event_loop.rs`, `src/shard/spsc_handler.rs`): the 1ms
  `periodic_interval` tick's SPSC drain is already a non-blocking,
  zero-allocation `try_pop()` loop, and every downstream side effect is
  already gated behind a cheap conditional. The one unconditional cost
  (`cached_clock.update()`, a single `clock_gettime`) is the documented
  "Timestamp caching" design. Unlike item B's AOF writer poll, this 1ms
  cadence IS the low-latency WAL-flush contract (CLAUDE.md), not
  incidental idle waste — escalating it would widen that bound. No code
  change; a real fix would be event-driven WAL triggering, an
  architectural change out of scope here.
- Item C8 (sigterm readiness deadline) landed early via the Windows-CI PR
  (#229) — see the CI section below.

### CI — fix Windows main-push test failures (PR #TBD)

- `test_poll_real_process_smoke` is now gated to Linux/macOS: `get_rss_bytes()`
  returns the documented `0` fallback on every other platform, so asserting
  `rss_bytes() > 0` can never pass on Windows.
- `test_scatter_text_aggregate_single_shard_skips_spsc` now runs on a fresh
  OS thread and calls `init_shard` itself (via a new shared
  `slice::test_support::make_init` fixture). As a plain `#[tokio::test]` it
  only passed when libtest scheduled it onto a harness thread where an
  earlier test had left a `ShardSlice` behind — a latent order-dependent
  flake on all platforms that failed deterministically on Windows CI.
- All 8 `find_moon_binary` test helpers now fall back to
  `env!("CARGO_BIN_EXE_moon")` (after the `MOON_BIN` override) instead of
  probing `target/{release,debug}/moon`: the old probing found nothing on
  Windows (missing `.exe`), ignored `CARGO_TARGET_DIR`, and preferred a
  possibly-stale release binary over the one cargo just built for the run.
- New `MOON_DISK_FREE_MIN_PCT` env override for `--disk-free-min-pct`
  (clap `env` feature; CLI flag still wins). Windows CI exports it as `0`:
  windows-latest runners sit below the 5% free-disk default, so every
  server-spawning suite failed with `MOONERR diskfull` instead of `OK`.
- `shardslice_shape.rs::split_off_test_module` computed line offsets with a
  1-byte `\n` assumption; on CRLF checkouts (Windows runners set
  `core.autocrlf=true`) the split point drifted one byte per line and
  panicked slicing inside a multibyte comment char. Now uses
  `split_inclusive('\n')` for byte-exact offsets on both line endings.
- mem_watchdog integration cases A/B (guard-engages assertions) are gated
  to Linux/macOS for the same `get_rss_bytes()` 0-fallback reason as the
  lib smoke test: with RSS reported as 0 the memfull guard is structurally
  inert on Windows. Cases C/D (guard-disabled directions) still run there.
- `sigterm_shutdown.rs` readiness deadline widened 15s → 60s behind a
  shared `READY_TIMEOUT` constant (poll-based loop notices readiness within
  100ms either way; the fixed 15s was a documented host-load flake,
  observed on the macOS CI runner).
### Fixed — connection-plane durability & lifecycle wave (PR #230)

- **D-2 — FLUSHDB/FLUSHALL now clear every shard, not just the local one**
  (`src/shard/coordinator.rs` `coordinate_flush_broadcast`, both sharded
  handlers): FLUSHDB/FLUSHALL are keyless, so key-based routing executed them
  only on the issuing connection's shard — on `--shards 4` a FLUSHALL left
  ~3/4 of the keyspace intact (red/green: 49/64 keys survived, now 0). The
  originating shard now broadcasts the flush to every peer shard as a
  `MultiExecute` leg, which reuses the existing remote dispatch + per-shard
  AOF/WAL persistence + index-flush path. Any failed leg returns a loud
  `MOONERR FLUSH partial` error (local shard already flushed; client should
  retry). Known limits: the flush is not atomic across shards (same relaxed
  semantics as SWAPDB), and a FLUSH issued inside MULTI/EXEC still executes
  local-only (follow-up); FLUSHALL still clears only the selected db
  (documented v0.1.5 single-active-DB behavior — all-dbs semantics is a
  separate follow-up needing replay parity).
- **D-1 — cross-store TXN crash replay restores into the correct db**
  (`src/transaction/mod.rs`, `wal_v3/record.rs`, `wal_v3/replay.rs`, txn
  handlers): `CrossStoreTxn` had no db field and WAL replay hardcoded
  `databases[0]`, so a `TXN.BEGIN…COMMIT` issued under `SELECT n≠0` replayed
  its KV ops into db 0 after a crash — silent recovery corruption. Commits now
  write a new `XactCommitV2` record (`0x53`) whose header carries the
  BEGIN-time db index; replay targets that db (out-of-range falls back to
  db 0 with a loud warning). v1 records still replay into db 0 (faithful to
  the builds that wrote them).
- **R-6 — connection migration fails open** (`conn_accept.rs`,
  `handler_sharded/mod.rs`): the source shard dropped its socket before the
  SPSC hand-off to the target was confirmed, so a full ring lost the client —
  precisely under the overload that triggers migration. Both runtimes now
  keep the original stream alive until the push succeeds (bounded retry,
  8 × 100µs) and on give-up resume serving the connection on the source shard
  with migration disabled, using the state recovered from the undelivered
  message. Also fixes the tokio path's `connected_clients` leak on the old
  loss path.
- **Observability** (`admin/metrics_setup.rs`): new
  `moon_xshard_backpressure_drops_total{target_shard}` counter + warn on every
  R-1 give-up, and `moon_shard_connected_clients{shard}` gauge (maintained at
  registry register/deregister) to surface SO_REUSEPORT imbalance and
  affinity funnels without parsing `CLIENT LIST`.
- **P-1 — lazy per-connection `FunctionRegistry`** (both handlers,
  `conn/core.rs`): the Functions API registry + `LuaEvictionCtx` (6 Arc/Rc
  clones) were built eagerly for every connection; now built on first
  FUNCTION/FCALL/FCALL_RO, cutting per-connection setup cost at high
  connection counts.
- **S-5 — `--tcp-backlog`** (`config.rs`, `conn_accept.rs`, `main.rs`): the
  per-socket listen backlog was hardcoded 1024; now configurable (default
  unchanged) for connection-storm tuning alongside `ulimit -n`.

### Fixed — connection-plane robustness hardening (Track B) (PR #230)

- **R-3 — `CLIENT KILL` force-closes idle connections** (`src/client_registry.rs`,
  handlers, `conn_accept.rs`): `CLIENT KILL` only set a cooperative `kill_flag`
  checked once per batch, so a connection parked in `read().await` (idle, the
  default `timeout 0`) was never torn down until it next sent bytes. Now the
  registry stores each connection's socket fd and `kill_clients` also
  `shutdown(2)`s it, so the parked read returns `Ok(0)`/`Err` immediately (the
  existing disconnect path). The raw-fd close is race-free: `kill_clients` holds
  the registry read lock, and a connection's `RegistryGuard` deregisters (needs
  the write lock) strictly before its stream drops — so a visible entry always
  has a live fd. No-op on non-unix (`CLIENT KILL` stays cooperative there).
  Self-kill (the filter matching the executing connection) stays cooperative —
  flag only, no fd shutdown — so the `+count` reply is still delivered before
  the handler closes, matching Redis's reply-first self-kill semantics.
- **R-2 — inline-command length cap** (`src/protocol/inline.rs`,
  `frame.rs`): the RESP-less inline path had no maximum line length, so a
  client that never sends `\r\n` (raw non-RESP bytes) grew the connection read
  buffer without bound — a per-connection memory-exhaustion vector. Added
  `ParseConfig::max_inline_size` (default 64 KB, mirroring Redis's
  `PROTO_INLINE_MAX_SIZE`); `parse_inline` now rejects an over-cap line
  (complete or incomplete) with `Protocol error: too big inline request`
  instead of buffering forever.
- **R-1 — bounded cross-shard `spsc_send`** (`src/shard/coordinator.rs`): the
  single dispatch primitive behind every scatter-gather command (MGET/MSET/
  DEL/EXISTS/SCAN/KEYS/DBSIZE, vector scatter, FT.INFO, graph traverse) retried
  a full SPSC ring in an **unbounded** `loop { try_push; yield/sleep }` — on
  tokio `yield_now()` reschedules with no backoff, busy-spinning a full core,
  and a wedged target could block graceful shutdown forever. Replaced with a
  bounded retry (`CROSS_SHARD_PUSH_MAX_RETRIES` × `CROSS_SHARD_PUSH_BACKOFF`,
  the same budget as `push_with_backpressure`) returning `PushOutcome`. On
  give-up the message is dropped; reply-carrying callers observe the closed
  reply channel and synthesize a per-shard error via the path they already
  handle. `PushOutcome` is `#[must_use]` so no call site can drop a message
  silently by accident, and the two side-effect-bearing senders fail loud:
  a dropped `MqTxnMaterialize` leg turns `TXN.COMMIT`'s reply into an explicit
  `MOONERR TXN.COMMIT partial` error (the commit is already WAL-durable; only
  foreign MQ materialization was lost) and a dropped `GraphRollback` logs at
  `error!`. A brief bounded spin (≤64 iterations) before the first timed
  backoff preserves the old ~10µs-class latency when the target ring is only
  transiently full (the 100µs timer sleep may round up to ~1ms).
- **R-4 — accept-loop backoff on fd exhaustion** (`src/server/accept_backoff.rs`,
  `listener.rs`, `shard/event_loop.rs`): all six socket-accept loops (tokio
  and monoio; sharded, non-sharded, and TLS) retried `accept()` errors with
  zero backoff, so `EMFILE`/`ENFILE` under a connection storm pinned a shard
  core at 100% and flooded the log. Added `AcceptBackoff`: capped exponential
  backoff (1 ms → 100 ms) on resource-exhaustion errors only, plus rate-limited
  logging; benign per-connection errors (e.g. `ECONNABORTED`) log without
  sleeping. The cap is 100 ms (not 1 s) because the sleep runs inside `select!`
  arms where the shutdown branch cannot preempt it — this bounds shutdown
  latency during a storm. The io_uring multishot-accept resubmit (opt-in `MOON_URING=1`
  bridge) is documented as a scoped follow-up (synchronous CQE handler, no
  async context to sleep in).

### Changed — graph engine deep-review fixes + optimization waves (PR #TBD)

- **Freeze-boundary correctness (P0):** CSR v5 segments now persist node
  properties, embeddings, and edge weights/properties across freeze;
  Cypher reads see the frozen tier via `MergedNodeView` (NodeScan/eval/
  IndexScan); cross-tier delta edges; GRAPH.NEIGHBORS existence +
  direction fixes; GRAPH.HYBRID/VSEARCH operate across both tiers. New
  red/green `graph_freeze_boundary` suite (11 tests) pins the lifecycle.
- **Point-query indexes (P1):** lazy per-segment property indexes (built
  once at first use from freeze-time data) + `PhysicalOp::IndexScan` in
  the planner/executor — `MATCH (n:L {p: v})` narrows instead of
  label-scanning.
- **Query-path cost (P2):** plan-cache auto-parameterization (literal
  variants share one cached plan; cache hits skip parse+compile
  entirely) with LRU eviction; slot-indexed executor rows (no per-row
  HashMap allocation, no per-insert String clone); write-path/WAL
  allocation cleanups (ADDNODE double WAL-encode deleted, itoa/ryu
  statement temporaries, FxHash for slotmap-keyed sets, Dijkstra
  three-maps-to-one merge, allocation-free neighbor iteration in Cypher
  Expand).
- **Traversal engine (P3):** CSR row-space BFS fast path on fully-frozen
  graphs — dense-bitmap visited set, TRUE parallel frontier expansion
  (thread::scope over Send+Sync CsrStorage), direction-optimizing
  Beamer push/pull over the incoming index; `TraversalGuard` wall-clock
  budget (30s default) now enforced per hop in Cypher variable-length
  Expand and ShortestPath (`ExecErrorKind::Timeout`); GRAPH.HYBRID's
  `HnswPreFilter` strategy is real — a lazy per-segment HNSW bridge
  (`src/graph/hnsw_bridge.rs`, raw-f32 cosine over v5 embeddings, >= 4096
  vectors) replaces the silent brute-force stub, with exact-scoring
  fallback whenever the approximate beam under-fills.
- **Pre-merge review fixes:** (1) restart NodeKey aliasing (P0) — a fresh
  post-recovery `MemGraph`'s deterministic SlotMap could mint keys
  bit-identical to loaded CSR segments' `external_id`s, silently
  shadowing frozen nodes; recovery now seeds the mutable tier via
  `MemGraph::with_id_offset` (watermark past the largest persisted id)
  and WAL replay dedup-skips AddNodes already resident in a loaded
  segment (red/green `graph_restart_id_aliasing` suite). (2) Plan-cache
  raw-hash fast path — exact-repeat queries hit the cache without the
  `parameterize()` lexer pass, and the `SlotTable` is cached alongside
  the plan instead of being rebuilt per execution; cache backing moved
  to `FxHashMap`. (3) `CsrStorage::resident_bytes` now counts the v5
  property blobs, lazily-built per-segment property indexes, and the
  HNSW bridge (heap-owned; mmap-backed sections count 0 per the
  `RawF16Store` precedent), keeping the elastic memory budget honest.

### Changed — graph engine wave 2: durability, Cypher coverage, hardening (PR #TBD)

- **Durability (P0, found by GCP production-hardening soak):** graph data now
  survives kill -9 under the DEFAULT disk-offload (WAL v3) configuration.
  Two stacked pre-existing bugs erased graphs on crash: (A) v3 recovery
  collected graph WAL commands into a throwaway replay engine — graph replay
  was a complete no-op in v3 mode (a dedicated `replay_graph_wal_v3` boot
  pass now applies them); (B) the checkpoint advanced the WAL replay floor
  and recycled segments without ever snapshotting the graph store —
  `save_graph_store` ran only on graceful shutdown and never persisted the
  mutable tier (checkpoint finalize now freezes each graph's write buffer,
  persists segments + a `snapshot_lsn` replay floor, and ABORTS the
  checkpoint if the graph snapshot fails, keeping the old floor). The prior
  crash suite ran exclusively with `--disk-offload disable`; new G4/G5
  scenarios cover the default config with and without a checkpoint crossing.
- **Durability:** stable external node/edge ids across WAL replay (handles
  handed to clients before a crash resolve to the same rows after recovery);
  Cypher `SET` property/label writes now emit WAL records
  (`GRAPH.SETPROP`/`GRAPH.SETLABEL` — previously a documented durability gap:
  SET mutations silently vanished on kill -9); new kill-9 crash-recovery
  suite (`tests/crash_recovery_graph_durability.rs`: mutable tier, frozen
  64K-edge tier rebuilt through replay-time freeze, double-crash idempotence).
- **Cypher coverage:** aggregations `count`/`sum`/`avg`/`min`/`max`/`collect`
  with implicit grouping, `DISTINCT`, and count-over-zero-rows semantics;
  `OPTIONAL MATCH` null-pads unmatched expansions (previously compiled
  silently to inner MATCH; unsupported shapes now reject loudly);
  `WITH` rebinds the pipeline mid-query (aggregate + `WHERE`-as-HAVING,
  `ORDER BY`/`SKIP`/`LIMIT` between WITH and RETURN; previously every clause
  after WITH ran on an empty row stream). `WITH *` rejects loudly.
- **Copy-up writes:** `SET`/`DELETE`/`MERGE` on frozen rows copy the row up
  into the write buffer instead of silently missing the frozen tier.
- **Query performance:** IndexScan range predicates (`WHERE n.p > x` prunes
  via per-segment B-tree-ish numeric index, superset semantics + residual
  filter); write-side plan cache (repeated write shapes skip parse+compile);
  row-BFS fast path engages on multi-segment fully-frozen graphs (~4× vs
  reader fallback, criterion-pinned); `Value::String` holds `Bytes` for a
  zero-copy reply path; HYB-02/HYB-04 use the HNSW bridge with off-thread
  bridge builds.
- **Query performance — mutable-tier property index (task #31):** `MATCH
  (a:N {id: X})` on the write buffer used to degrade to a full
  `O(live_node_count)` linear scan even though `PhysicalOp::IndexScan` was
  already the plan (profiling on the 5K-node/15K-edge Cypher point-query
  bench put this scan at 82.5% of shard CPU — the mutable tier never
  freezes below `edge_threshold`, default 64,000). A new incrementally
  maintained `MutablePropertyIndex` (`src/graph/index.rs`, mirrors the
  frozen tier's `SegmentPropertyIndexes` numeric-BTree/string-hash split,
  keyed by `NodeKey`) turns the mutable-tail probe into an O(log N +
  |result|) index seed; `index_scan_keys`'s residual label/MVCC/property
  checks are unchanged (SUPERSET contract preserved, no planner changes).
  `MemGraph::set_node_property`/`remove_node_property`/`undelete_node` are
  now the single source of truth for node-property mutation, replacing 5
  previously hand-rolled call sites (Cypher `SET`, MERGE ON CREATE/MATCH
  SET, TXN.ABORT undo, WAL replay) that could silently let the index drift
  from live state. Closes a TXN.ABORT `UndeleteNode` gap found during
  design review: undoing a `DELETE` inside a transaction now re-indexes the
  restored node instead of leaving it live but permanently unreachable by
  index probes.
- **Query performance — Cypher result cache (task #32):** read-only
  `GRAPH.QUERY`/`GRAPH.RO_QUERY` now cache the fully-encoded RESP reply
  bytes for repeated identical queries (same raw Cypher text + args),
  keyed by `(query_hash, args_hash)` and served via the existing
  `Frame::PreSerialized` passthrough — no new protocol variant needed, and
  both RESP2 and RESP3 encodings are cached in separate slots so a
  protocol-version switch is a clean miss rather than a stale re-encode.
  Invalidation is a per-graph monotonic `write_gen: u64` bumped by
  `NamedGraph::touch()` at every real mutation site (plain `GRAPH.ADDNODE`/
  `GRAPH.ADDEDGE`, the Cypher write-plan mutation loop, `TS.*` temporal
  invalidation, and TXN.ABORT rollback of graph undo-ops) — deliberately
  NOT at `freeze_and_compact`, which reshapes storage without changing
  query-visible content. The write_gen is captured before executing a
  read and the encoded reply is only cached if it is still unchanged
  after — a miss is always safe (SUPERSET semantics), so a benign race
  just skips caching rather than serving stale data. Decayed queries and
  errored/timed-out results are never cached. `ResultCache` is a bounded
  per-graph LRU (256 entries / 4MiB default, `src/graph/cypher/
  result_cache.rs`) whose resident bytes are folded into
  `GraphStore::resident_bytes()`; dropping a graph (`GRAPH.DELETE`) drops
  its cache with it. The cache is only consulted on the connection-local
  dispatch path where the negotiated protocol version is reliably known
  (`dispatch_graph_read`, threaded as `Option<u8>`); the cross-shard
  `GraphCommand` hop and `graph_query_or_write`'s internal read-execute
  calls pass `None` and bypass the cache entirely rather than risk an
  unverified protocol version.
- **Query performance — FTS reuse for Cypher text predicates (task #33):**
  first-class `CONTAINS`/`STARTS WITH`/`ENDS WITH` Cypher operators (pure
  syntax sugar over the existing `=~` byte-level checks — `src/graph/
  cypher/lexer.rs`, `ast.rs`, `parser/expr.rs`, `executor/eval.rs`), plus a
  new per-frozen-segment `SegmentTextIndex` (`src/graph/text_index.rs`,
  lazy `OnceLock` + `resident_bytes` accounting, same pattern as
  `SegmentPropertyIndexes`/`hnsw_bridge`) that accelerates `CONTAINS`/
  `STARTS WITH`/`ENDS WITH`/`=~` conjuncts in `WHERE` (`planner.rs`'s new
  `extract_text_conjuncts`, mirroring W2-3's `extract_range_conjuncts`).
  Reuses `crate::text::posting::PostingStore`/`crate::text::bm25::
  {FieldStats, bm25_score}`/`crate::text::term_dict::TermDictionary`
  verbatim (already ID-space-agnostic, zero-fork). **Correctness note:**
  the index prunes on PROPERTY PRESENCE only, not token identity — a
  tokenized/stemmed/case-folded posting lookup cannot safely decide
  substring/prefix/suffix containment (e.g. `CONTAINS 'rust'` must also
  match `"trusted"`, which tokenizes to a different term entirely), so
  `candidate_rows` returns every row whose target property is a
  String/Bytes at all, and the existing residual `Filter` remains the sole
  decider (SUPERSET contract, same as `prop_eq`/`prop_range`). The
  MUTABLE tier gets no acceleration (falls back to an exact scan of the
  write buffer, pre-approved scope decision — mirrors the pre-task-#31
  numeric story); a `GraphUnion`-merged segment does not remap posting row
  ids and simply rebuilds its text index lazily on first use post-merge.
  `SegmentTextIndex::bm25_score_for` is a tested-but-unwired internal BM25
  relevance-scoring hook (no Cypher `ORDER BY` grammar was specified for
  it) — proves the `bm25_score` reuse end-to-end; surface syntax is a
  documented follow-up.
- **Operability:** traversal timeout is configurable — `--graph-timeout-ms`
  server default plus per-query `GRAPH.QUERY ... TIMEOUT <ms>` (RedisGraph
  parity, 0 = unlimited).
- **Dead code:** cross-shard traverse scaffolding deleted (532 lines, no
  sender existed); the single-shard-per-graph sharding model is now
  documented in `src/graph/mod.rs`.

- Cargo: `ringbuf` 0.4.8 → 0.5.0 and `metrics-exporter-prometheus` 0.16.2 →
  0.18.3 (semver-major; both compile and test green with no code changes —
  verified by dependabot's per-bump CI runs and locally), `bytes` 1.11.1 →
  1.12.0, `rand` 0.10.1 → 0.10.2, `lz4_flex` 0.13.0 → 0.13.1, `cmov` 0.5.3 →
  0.5.4, `cudarc` 0.19.4 → 0.19.8 (gpu feature, not covered by CI — patch
  bump), and `openssl` 0.10.78 → 0.10.81 in `sdk/rust` (security bump).
- GitHub Actions: `actions/checkout` v4 → v7 (including one straggler in
  `docker-publish.yml` dependabot missed), `actions/deploy-pages` v4 → v5.
- Console: `vite` 7.3.2 → 7.3.5 (dev dependency).
- Dependabot: `dtolnay/rust-toolchain` is now ignored — its pin IS the MSRV
  (1.94) gate, so auto-bumping it to latest stable (rejected PR #195)
  silently defeats the MSRV check.
- Supersedes dependabot PRs #191, #194, #196–#201, #208–#210 (their Lint
  failures were the CHANGELOG-entry gate, which this consolidated PR
  satisfies once).

### Fixed — proactive RSS watchdog pauses writes before kernel OOM (PR #TBD)

- **New `mem_monitor` guard** (`src/shard/mem_monitor.rs`), the memory
  analogue of the existing diskfull guard (MA12): pauses writes once process
  RSS crosses `--mem-full-pct` (default 95, 0 = disabled) of the DETECTED
  system/cgroup memory limit, not the configured `--maxmemory` (which can be
  an unconfigured `0`/unlimited). Mirrors `disk_monitor`'s hysteresis state
  machine, inverted for direction (high RSS is bad): pauses at
  `rss% >= mem_full_pct`, resumes only at `rss% <= mem_full_pct - 5`.
  `MOONERR memfull: writes paused until memory pressure recovers` joins the
  existing `MOONERR diskfull` / `MOONERR busy` message-selection chain in
  both dispatch paths (order: diskfull, memfull, segment-backlog busy); the
  hot-path cost is one additional `AtomicBool::load(Relaxed)` inside the
  already write-gated branch. Polled on shard 0's existing 5s disk-monitor
  timer tick (both event-loop poll sites), plus one immediate poll at
  startup (`init_global`) so the AOF-replay/segment-load recovery peak is
  visible before the first tick. New INFO fields
  `reclamation_mem_rss_bytes` / `reclamation_mem_watchdog_active`
  (OR'd into the existing `reclamation_write_stall_active`). Same accepted
  trade-off as the diskfull guard: `DEL`/`UNLINK`/`EXPIRE`/`FLUSHALL` are
  write-flagged and blocked too while paused — no allowlist.

### Fixed — vector keymap CoW amplification + bounded snapshot queue (PR #TBD)

- `VectorIndex`'s three `key_hash -> V` maps (`key_hash_to_key`, `key_hash_to_global_id`,
  `key_hash_to_vec_checksum`) were each a single `Arc<HashMap<u64, V>>`. Under
  `ft-search-off-eventloop`, a live FT.SEARCH snapshot holds an extra `Arc` clone of the whole
  map while the event loop keeps processing writes; the next write's `Arc::make_mut` then sees
  refcount > 1 and full-clones the entire map synchronously on the shard event loop (~47MB
  @1M vectors). Replaced with `BucketedKeyMap<V>` (`src/vector/keymap.rs`): 256 fixed buckets,
  each independently `Arc<HashMap<u64, V>>`, bucket selected by `(key_hash >> 56)`. A concurrent
  snapshot now pins at most 1/256th of the map per write instead of the whole thing. Same
  treatment applied to `SnapshotJob`'s mirrored fields and `SearchSnapshot.key_hash_to_key`.
  On-disk `keymap.bin` format is unchanged (read-compatible).
- `SnapshotPool` (`src/vector/persistence/manifest.rs`) used an unbounded `flume` queue with a
  single worker; if compact/merge cadence outpaced the worker, queued jobs pinned every
  submitter's `Arc`s indefinitely. Replaced with a coalescing slot keyed by index directory: a
  new submit for an index with a job still pending (not yet dequeued) replaces it in place,
  dropping the stale job's `Arc`s immediately; an in-flight (already-dequeued) job is unaffected.
  Submit never blocks the caller.

### Fixed — CI: de-flake OOM case E + Windows test-compile gate (PR #TBD)

- **`test_case_e_cross_db_copy_oom` (was red on main's post-merge CI for PR #217/#218/#219,
  0/300 OOM on both platforms while passing locally):** the single-shot statistical assert sat
  inside the slack GAP-1's elastic budget + its 100ms-stale usage snapshots can grant. Rewritten
  as bounded escalation: up to 8 rounds of COPYs into fresh destination keys until the
  destination db's per-shard usage (~2.4MB) provably exceeds the ABSOLUTE budget ceiling
  (`compute_elastic_budget ≤ maxmemory` = 2MB) — timing-independent on any runner speed, and the
  RED floor stays exact (gate stashed = 0 OOM through all rounds; re-verified red/green).
- **`tests/quickwins_red_api.rs` broke the Windows CI leg at COMPILE time** (also red on main):
  `qw1_accepted_socket_has_nodelay` calls `apply_client_socket_opts`, which is `#[cfg(unix)]`
  (takes `AsFd`). The test (and its imports) are now unix-gated to match.
- **`write_stall_segment_backlog`: two tests raced on the process-global
  `RECL_SEGMENT_STALL_ACTIVE` atomic** (test harness runs same-binary tests on parallel
  threads; observed on CI macOS as `store(1)` reading back `0`). All mutation of the global now
  lives in one merged test.

### Fixed — FT.COMPACT left mutable residue behind when draining a background build (PR #TBD)

- **Pre-existing** (`main`, not introduced by this branch): when an explicit
  `FT.COMPACT`/`force_compact` found a background auto-compaction already in flight, it drained
  that build, installed it, persisted, and returned — WITHOUT compacting the documents inserted
  while the background build was running (the `clone_suffix(frozen_len)` mutable tail). Those
  docs stayed mutable-only (no durable segment) until some future compact fired, breaking
  force_compact's documented full-drain contract (frozen == mutable on reply) and — combined
  with the B3 phantom-keymap hole below — silently losing them on a kill -9 (observed stuck
  durable state: segments live=1961, keymap=2000, converging never). Load-dependent: only
  reproduces when insert cadence keeps a background build in flight at FT.COMPACT time (this is
  what made the crash suite's S1/S2 flake by machine load, masquerading as a branch regression).
  `force_compact` now falls through to the inline drain loop after installing the in-flight
  build (a no-op when the tail is empty).

### Fixed — B3 vector recovery: phantom keymap entries silently dropped docs (PR #TBD)

- **Pre-existing silent data loss on kill -9** (found while validating this branch against the
  `crash_recovery_vector_durability` suite; reproduces on `main`): the durable
  `keymap-<epoch>.bin` covers EVERY indexed key (mutable + immutable) at snapshot-submit time,
  while the paired `manifest.json` lists only installed immutable segments. A crash landing after
  one snapshot commits but before the next (covering a just-frozen segment) leaves the on-disk
  keymap a strict superset of the on-disk segments. The B3 dedup rescan then "verified" those
  uncovered keys as unchanged (keymap checksum matches the AOF blob — exactly the crash-window
  signature) and never re-indexed them: their documents vanished from search with
  `num_docs` under-reporting (observed: 1950/2000) while recovery logged
  `verified unchanged` for all keys. Fix: `recover_v2::load_segments_and_keymap` now drops any
  keymap entry whose `key_hash` is not LIVE in a loaded segment
  (`ImmutableSegment::live_key_hashes`), so the rescan re-indexes those keys from the AOF; a
  loud log line reports the dropped-phantom count. New deterministic unit regression
  (`recover_reindexes_keymap_entries_not_backed_by_any_segment`, red/green verified) plus
  integration scenario S6 (kill inside the async-snapshot window; asserts only the crash
  contract: `num_docs == N`, every key answers its own exact-match query). S1/S2's
  `re_indexed == 0` fast-path determinism is restored by a new full-durability pre-kill gate
  (`wait_for_durable_docs`: manifest'd segment live-counts AND keymap entries both equal N).

### Fixed — zombie/CPU hardening wave 1 (PR #TBD)

- **Busy-poll spin idle-disengages** (`--io-busy-poll-us` / vendored monoio
  legacy driver): an idle server no longer burns the spin budget on every
  park forever (measured ~2.4s CPU per 3s wall on an idle 4-shard server at
  200µs). The driver tracks the last real event per thread and skips the
  spin window after `MOON_SPIN_IDLE_DISENGAGE_US` (default 10ms; 0 = old
  always-spin) of quiet; the next event re-arms it via the normal wake path,
  so steady traffic — the GCE p=1 win path — never disengages.
- **Shard-thread panics abort the whole process** instead of leaving an
  N-1-shard server silently answering with a dead shard's keys gone; the
  shutdown join loop logs instead of swallowing panics. New `DEBUG PANIC`
  subcommand (Redis parity) as the crash-handling test aid.
- **SIGTERM shutdown regression matrix**: shards × held-connections ×
  busy-poll × 16-writer AOF write-storm (7 cases, both runtimes, 3
  platforms). The documented SIGTERM+SO_REUSEPORT bench hang did NOT
  reproduce at HEAD in any composition; the matrix stays as the guard and
  the detached monoio accept-task change is deferred until a reproducer
  exists.
- **`wait_ready` test harness hardening**: readiness probes retry with a
  fresh connection on mid-startup connection resets (CI flake: per-shard
  SO_REUSEPORT listeners accept-then-reset during init).

### Fixed — OOM eviction bypass closure (PR #TBD)

- **Cross-shard SPSC write legs now enforce `--maxmemory`.** `Execute`,
  `MultiExecute`, `PipelineBatch` and their `*Slotted` variants
  (`src/shard/spsc_handler.rs`) executed writes against the TARGET shard's
  `Database` directly, bypassing the connection handlers' eviction gate
  entirely — a scatter-gather write (e.g. a pipeline of individually-routed
  `SET`s hashing to a remote shard) could grow that shard's memory past
  `maxmemory` without limit. A new `spsc_eviction_gate` helper mirrors
  `run_write_eviction_gate` (`handler_monoio/mod.rs`) exactly — same elastic
  per-shard budget (GAP-1), same spill-vs-plain branching — threaded through
  `drain_spsc_shared`/`handle_shard_message_shared` and gated by a
  once-per-drain-cycle `evict_active` snapshot (perf parity with
  `batch_eviction_active`: zero extra lock acquires when `maxmemory` is
  unset). **Perf follow-up (same PR):** that snapshot, and the Lua bridge's
  gate below, each used to answer "is `maxmemory` nonzero?" with either a
  `runtime_config.read()` per drain cycle or a generation/`Cell` snapshot.
  Both now read a single process-global `AtomicU64`
  (`storage::eviction::{publish_maxmemory, maxmemory_is_set}`), published at
  every production write site of `RuntimeConfig.maxmemory` (`CONFIG SET
  maxmemory`, server startup) — one Relaxed load, no lock, no per-script
  Cell bookkeeping.
- **Lua `redis.call`/`redis.pcall` writes now enforce `--maxmemory`.**
  EVAL/EVALSHA carry no WRITE command flag, so the dispatch-level OOM check
  never saw them, and the bridge (`src/scripting/bridge.rs`) never ran the
  check before executing a write inside a script — a tight `redis.call('SET',
  ...)` loop could write arbitrarily far past the cap. A new
  `LuaEvictionCtx`, captured by value into the `redis.call`/`redis.pcall`
  closures at VM-setup time (`setup_lua_vm`, once per shard — zero new
  `unsafe`, zero per-call allocation), runs the same gate before a WRITE
  `redis.call` executes; `redis.call` raises the OOM error as a Lua runtime
  error (script aborts, matching Redis semantics), `redis.pcall` returns it
  as an `{err = ...}` table. Read-only scripts are unaffected.
- **FCALL-internal `redis.call`/`redis.pcall` writes now enforce
  `--maxmemory` too.** `FUNCTION LOAD` (`src/scripting/functions.rs`)
  creates its own per-library sandboxed Lua VM, separate from the shard's
  shared EVAL/EVALSHA VM — that per-library VM registered `redis.call`/
  `redis.pcall` with `LuaEvictionCtx::disabled()` unconditionally, so a
  `FUNCTION` whose body wrote in a loop could grow memory past the cap
  independent of the EVAL/EVALSHA fix above. `FunctionRegistry::new` now
  takes a `LuaEvictionCtx`, stored on the registry and cloned into every
  library's VM at `create_library` time; both production construction sites
  (`handler_monoio/mod.rs`, `handler_sharded/mod.rs`) build a real ctx from
  the same shard handles `setup_lua_vm` already uses for the shared VM
  (`ConnectionContext::{shard_databases, runtime_config, shard_id,
  spill_sender, spill_file_id, disk_offload_dir}`); only test-only callers
  pass `LuaEvictionCtx::disabled()`.
- **Cross-shard `MOVE`/`COPY ... DB n` two-database intercept now runs on
  EVERY `ShardMessage` SPSC arm, not just the plain `Execute` arm.** The
  intercept (special-cased ahead of the generic single-db write path because
  both commands need two `&mut Database` borrows at once) previously existed
  only in `Execute`. Every other arm — including `PipelineBatchSlotted`, the
  one real client traffic (`handler_monoio`/`handler_sharded`'s pipelined
  remote-write path) actually uses — fell through to the generic single-db
  `key_extra::copy`, which parses and silently ignores the `DB` clause: a
  remote `COPY src dst DB n` performed a same-db copy instead (data
  corruption, confirmed pre-fix via manual probe: 4/20 remote `COPY`s landed
  in the wrong db), and a remote `MOVE` returned a loud-but-wrong "MOVE
  requires handler-level dispatch (cross-db operation)" error instead of
  moving the key. The intercept is now extracted into a shared helper
  (`src/shard/spsc_two_db::try_two_db_intercept`) that `Execute`,
  `MultiExecute`, `PipelineBatch`, `ExecuteSlotted`, `MultiExecuteSlotted`
  and `PipelineBatchSlotted` all call verbatim; cross-db `COPY` still runs
  `spsc_eviction_gate` against the destination db before `copy_core` (`COPY`
  duplicates the value, so it needs the gate; same-shard `MOVE` is net-zero
  and stays ungated, same rationale as `DEL`).
- **Found, NOT fixed (out of scope): cross-shard `COPY`'s destination key is
  only reliably retrievable when it hash-tag-colocates with the source key.**
  Moon shards purely by key hash, with no db-awareness. A `COPY src dst DB n`
  executes on whichever shard owns `src` (that's the routing key for the
  whole command) and writes `dst` into that SAME shard's db space — it does
  not (and cannot, without a real cross-shard two-phase protocol) relocate
  the write to whichever shard `dst`'s OWN hash would naturally pick. A later
  plain `GET dst` routes by `hash(dst)`, so it only reliably finds the value
  when `src` and `dst` share a hash tag (or `--shards 1`). `MOVE` is exempt
  (the key name is unchanged, so `hash(key)` is identical before and after).
  This is a pre-existing, inherent property of the per-key-hash sharding
  model applied to `COPY` with a renamed destination — the plain `Execute`
  arm this fix mirrors has the identical property — not a regression from
  extending the intercept to every arm. Fixing it is a materially larger
  change (routing the destination write to `dst`'s own natural shard) than
  "apply the existing intercept to more arms" and is out of scope here.
- **Found, NOT fixed (out of scope): Moon's eviction gate is per-DATABASE,
  not per-shard-aggregate-across-databases.** `try_evict_if_needed_budget`
  (and every SPSC write arm, including the new cross-db `COPY` gate above)
  measures `db.estimated_memory()` for the ONE database being written to,
  not the sum across all 16 logical dbs a shard holds — consistent
  throughout Moon's write path, but a deviation from real Redis's
  instance-wide `maxmemory`. A cross-db `COPY` into a mostly-empty
  destination db can undercount total shard pressure. Uniform, pre-existing
  behavior; fixing it is a cross-cutting change to the eviction model, not a
  `COPY`/`MOVE`-specific one — out of scope here.
- New wire-level regression suite `tests/spsc_two_db.rs` (3 cases, both
  runtimes) exercises the one arm real client traffic uses
  (`PipelineBatchSlotted`): pipelined cross-shard `COPY ... DB n` (RED
  before the fix — `MOVE`'s loud wrong error and `COPY`'s silent same-db
  copy — both confirmed via a git-stash re-run against the pre-fix source),
  pipelined cross-shard `MOVE`, and a same-db `COPY` control (routes through
  the coordinator's multi-key scatter path, unaffected by this fix, kept as
  a negative control). The `COPY` case uses `{i}`-hash-tagged src/dst key
  pairs so the destination key is independently `GET`-able post-fix — see
  the "destination hash-tag" caveat above; an untagged version of the same
  test only succeeds for the ~1-in-4 keys where `hash(dst)` collides with
  `hash(src)` by chance.
- New wire-level regression suite `tests/oom_bypass_closure.rs` (8 cases,
  both runtimes): direct-SET control (A), cross-shard pipeline (B), Lua EVAL
  loop (C), read-only EVAL under pressure not blocked (D), cross-shard COPY
  under pressure (E), `CONFIG SET maxmemory` atomic publish/un-publish (F),
  FCALL-internal write loop (G), FCALL control with no maxmemory (H).
  B, C, E and G RED before their respective fixes. D locks the write-only
  scope of the Lua gate. Case F (added with the atomic-snapshot perf
  follow-up) starts the server with `--maxmemory 0 --disk-offload disable`
  (both required so the atomic starts genuinely unset — omitting
  `--maxmemory` trips the auto-guardrail's nonzero default, and
  disk-offload's spill-sender presence ORs into the same gate) and drives a
  cross-shard pipeline before and after `CONFIG SET maxmemory`/`CONFIG SET
  maxmemory 0`; RED before the CONFIG SET call site published the atomic
  (775/3000 OOM — the local-only share) GREEN after (3000/3000). Case G
  (FCALL) RED before the FunctionRegistry fix — a 2000-iteration
  `redis.call('SET', ...)` loop inside an FCALL'd function completed with
  `'DONE'` instead of an OOM error against a 2MB-capped `noeviction` server;
  GREEN after. Case H locks the write-only/OOM-only scope of the FCALL gate,
  mirroring case D for EVAL. Case E was revised for the cross-shard SPSC
  intercept fix above: it now pre-loads db 1 (the `COPY` destination) with
  the same ballast as db 0 before the OOM phase, since Moon's per-database
  eviction gate (see the "found, not fixed" note above) needs the
  destination db under its OWN pressure to trip — the original version
  passed only as a side effect of the pre-fix same-db-copy bug (0/300 OOM
  with the destination-db gate disabled and routing intact, confirmed via a
  targeted stash of just that gate block; 76/300 fully fixed).

### Added — int8 symmetric ADC for SQ8 vector search (task #13)

- New per-candidate integer dot-product path for SQ8 asymmetric distance
  computation (ADC), replacing the f32-widening `sq8_stats` kernel on the
  hot per-candidate pass with exact-integer int8 dot products: NEON
  widen-multiply (`vmull_s8` + the `c^0x80` offset trick, since baseline
  ARMv8-A has no mixed u8×i8 widening multiply), AVX2 (`cvtepu8/cvtepi8_epi16`
  → `mullo_epi16` → `madd_epi16`, saturation-free), and AVX512 VNNI
  (`_mm512_dpbusd_epi32`, feature-gated behind `simd-avx512`). Query is
  quantized to symmetric int8 once per search
  (`turbo_quant::sq8::sq8_quantize_query_scalar`); `sum_c`/`sumsq_c` are
  exact integers straight from the u8 codes. Combines through the unchanged
  `sq8_l2_from_stats` — the on-disk SQ8 format and the exact-rerank sidecar
  (HQ-1) are untouched. Wired into both `hnsw/search.rs` beam-search closures
  and all three SQ8 call sites in `segment/mutable.rs` brute-force search.
  Local (dev-Mac, NEON) directional speedup vs the existing f32 SIMD path:
  ~1.5x at dim 128, ~2.0x at dim 384, ~2.15x at dim 768 (`benches/sq8_adc_bench.rs`,
  `int8_dispatch` variant) — absolute/cross-arch numbers are GCE-validation
  scope (task #14). Recall A/B-gated (`turbo_quant::sq8` test module):
  R@10 delta ≤ 0.01 vs the f32 ADC path and ≥ 0.98 top-10 overlap, both L2
  and Cosine, at dim 128/384. `MOON_SQ8_INT8_ADC=0` forces the f32 kernels
  (bench/diagnostic escape hatch, not yet added to CLAUDE.md's env list —
  flagged for a follow-up doc pass).

### Fixed — merge recall gate self-exclusion bias (manual merges could never pass)

- `verify_merge_recall` compared HNSW results **including the query point**
  (every sampled query is a database point, distance 0 → always rank 1)
  against a ground truth that **excluded** it, structurally capping measurable
  recall at (k−1)/k = 0.90 — exactly the manual `FT.COMPACT`/`VACUUM VECTOR`
  merge gate, so any manual merge of an index with ≥ 50 vectors was rejected
  with `merge recall 0.9000 < tolerance 0.9000` regardless of actual graph
  quality (the 0.70 background gate masked this). The merged-graph search now
  requests k+1 and drops the self-point before comparison.

### Added — vector-index durability: kill-9 crash-recovery test suite (B4)

- New `tests/crash_recovery_vector_durability.rs`: five end-to-end scenarios
  against real server processes (SIGKILL + restart): unchanged-key dedup
  fast path, update/delete reconcile, orphan sweep on boot, collection_id
  pin surviving a post-recovery compact + GraphUnion merge, and a
  no-persistence regression guard. All waits are bounded condition polls;
  servers run with `--disk-free-min-pct 0` so the suite is immune to the
  dev volume hovering at the 5% diskfull guard.

### Added — vector-index durability: startup recovery from disk (B1-B3)

- **Vector indexes now persist their segments across restarts** instead of
  paying a full re-index (TQ encode + HNSW build) of every matching hash key
  on every restart. Each index gets an `idx-<hex(name)>/` directory holding
  an atomically-written `manifest.json` (collection_id / segment ids /
  id-allocator floors), a checksummed `keymap-<epoch>.bin` (key_hash →
  global_id + vector checksum + original key), and its immutable HNSW
  segments (staged write: `staging-<id>` → fsync → atomic rename), written
  in the background after each compact/merge install (B1/B2).
- **Startup now loads that state instead of discarding it**: segments are
  read back and reattached (pinning the recreated index's `collection_id` to
  the persisted value — required for the HNSW QJL rotation seed to match),
  and the keyspace rescan is now a **dedup rescan**: a key whose vector
  bytes checksum-match the last snapshot is rebuilt as metadata-only (no
  HNSW/TQ re-encode); only genuinely changed or unknown keys are fully
  re-indexed; keys removed from the keyspace are tombstoned (B3).
- **Crash-safety contract**: any corrupt/missing artifact (segment,
  checksum mismatch, unreadable headers) degrades to a rescan-rebuild of
  exactly the affected keys, never to wrong search results; a manifest may
  understate what's durable (costs a rescan) but never overstates. Startup
  also sweeps orphaned segment/staging/keymap files and stale `idx-*`
  directories left by an interrupted drop.
- Multi-field indexes: only the default vector field's segments/checksum
  are persisted; additional named vector fields always re-encode on
  restart (documented gap, safe/conservative).
- Removed the vestigial WAL-replay vector recovery path
  (`src/vector/persistence/recovery.rs`, `VectorStore::attach_recovered`/
  `pending_segments`): it only ever populated a `VectorStore` that was
  discarded before the shard event loop started (recovery authority is now
  exclusively the manifest/segment/keymap layout above).

### Fixed — FLUSHALL/FLUSHDB ghost vectors + HDEL stale-vector gap

- **FLUSHALL/FLUSHDB never touched the FT indexes** (persistence-review R3):
  flushed hashes stayed searchable as ghost vectors/documents until restart.
  Both commands now clear every vector + text index's CONTENTS (segments,
  key-hash maps, postings, TAG/NUMERIC indexes) while KEEPING the FT.CREATE
  definitions — matching restart semantics, where definitions come from the
  sidecar and contents are re-derived from the (now empty) keyspace.
  Recovered-but-unclaimed `pending_segments` are discarded too, so a
  post-flush FT.CREATE cannot resurrect pre-flush contents.
- **`HDEL key <vector-field>` left the vector searchable** (R4): only
  whole-key DEL/UNLINK tombstoned. A successful HDEL that removes an
  index's vector field now tombstones that key in exactly the affected
  indexes (per-index `mark_deleted_for_key_in_index`; sibling indexes keyed
  on other fields keep their entries). Known follow-ups documented in
  `auto_hdel_vectors`: multi-vector-field indexes tombstone the whole doc,
  and TEXT/TAG/NUMERIC field removal is not yet re-indexed.
- Both hooks wired with wire-parity on ALL dispatch paths (single-shard,
  monoio conn-local, tokio sharded, SPSC execute, shared batch, MULTI/EXEC).
  New integration suite `tests/vector_flush_hdel_tombstone.rs` (red→green).

### Observability — exact-rerank sidecar coverage is visible and loud (R5)

- A GraphUnion merge with even one sidecar-less source segment silently
  dropped the exact-rerank f16 sidecar for the ENTIRE merged segment
  (all-or-nothing propagation — a partial sidecar would mix exact and ADC
  distances within one segment). The drop now logs a `tracing::warn` with
  source counts, and FT.INFO exposes additive `graph_segments` /
  `segments_with_exact_rerank` counters (merged across shards) so ADC-only
  segments are observable in the steady state.

### Performance — AE-1: saturation-gated per-segment adaptive ef

- With G graph segments, every segment searched at the FULL resolved ef —
  ~G× the CPU of one ef-wide beam (the root cause of the pooled regression
  on SMT-constrained boxes in PR #214's GCE run). Compact/GraphUnion builds
  now run a one-time self-probe against the exact f16-sidecar ground truth
  (leave-self-out R@10 across an ef ladder, `estimate_suggested_ef`): a
  segment whose curve is FULLY SATURATED from the minimum rung (flat at
  ≈1.0) is certified trivially easy and searches at min-ef (24); every
  other segment keeps the full resolved ef. Applied identically to the
  sync, serial-yielding, and worker-pool paths (pooled == serial identity
  holds); never active when the user pins `EF_RUNTIME`; in-memory only
  (reloaded segments fall back to the full beam).
- Two weaker designs were tried and REJECTED by recall A/B (20k×384d SQ8,
  5 segments): a blanket `ef/√G` split (EF-SPLIT) silently cost gaussian
  R@10 0.9915 → 0.9295 (its original "recall preserved" validation had
  exercised a stale binary), and letting the self-probe pick a mid-ladder
  knee cost 0.9915 → 0.939 — self-sampled queries are optimistically
  biased on unstructured data, so the probe's ONLY trustworthy verdict is
  "saturated at min-ef".
- Recall/latency gate (same seeds/harness end-to-end): clustered 5-seg
  R@10 1.0000 at p50 0.79 → 0.26 ms (~3×), clustered 1-seg 1.0000 → 0.9960
  (within the probe's ε=0.005 contract) at 1.78 → 1.46 ms; gaussian 5-seg
  0.9915 at ~0.79 ms and 1-seg 0.7710 — byte-identical to the pre-split
  baseline (probe self-rejects, full beam).

### Performance — FT.SEARCH per-query fixed-cost cleanup (QP-2/QP-3)

- **Thread-cached `SearchScratch`** (QP-3): the wire path built a fresh
  scratch per query — heaps, visited bitmap, and the 32–65KB ADC LUT
  reallocated every FT.SEARCH. Captures now take a thread-local recycled
  scratch (exact padded-dim match, mirroring the worker pool's cache) and
  the yielding search returns it on completion.
- **Amortized committed-treemap capture** (QP-2): every search cloned the
  MVCC committed `RoaringTreemap`; `TransactionManager::committed_snapshot()`
  now hands out a cached `Arc` refreshed only on the first capture after a
  commit/prune — read-heavy captures are one refcount bump, write-heavy is
  never worse than before. All 7 capture sites switched.
- **ryu score formatting**: RESP `__vec_score` replies formatted f32 via
  `Display` (grisu, 1.4% of a matched-recall query); now ryu.
- VM A/B (same box/config as the f16 kernel entry): ef64 8,468 → 8,810 QPS
  (+4.0%; +13% cumulative with the f16 kernels).

### Performance — SIMD f16 exact-rerank kernels (NEON + F16C)

- The HQ-1 exact-rerank pass decoded its f16 sidecar with scalar software
  `f16_to_f32` — 17% of a matched-recall (ef 64) query. New fused
  decode+distance kernels in the distance dispatch table (`f16_l2`,
  `f16_dot_normsq`): aarch64 uses a baseline-NEON integer-rescale decode
  (no ARMv8.2 FP16 intrinsics needed; exact for all finite f16, Inf/NaN
  bit-selected), x86_64 uses F16C `vcvtph2ps` + FMA (runtime-detected with
  AVX2, scalar fallback). Subnormal/Inf/NaN semantics match the scalar
  reference exactly (pinned by tests). VM A/B (20k×384d clustered SQ8,
  single conn, ef 64): 7,795 → 8,468 QPS (+8.6%).
- **Negative result, kept for the record**: skipping the rerank pass
  entirely for SQ8 was A/B'd and REFUTED — it costs real recall
  (R@10 −0.010 clustered, −0.017 gaussian 5-segment). The pass stays
  unconditional; the SIMD kernels make it cheap instead.

### Performance — FT.SEARCH intra-query worker pool + bounded bulk compaction

- **`--ft-search-workers N` worker pool** (`src/vector/search_pool.rs`): the
  per-segment HNSW searches of ONE KNN query fan out across a pool of searcher
  threads while the shard task scans the mutable segment; replies are awaited
  with `flume::recv_async`, so the shard event loop is never blocked. Default
  **0 (off, opt-in)** — each segment pays the full resolved ef, so a pooled
  N-segment query does ~N× the CPU work for its latency win: a 4.9× QPS win on
  physical-core-rich boxes but a measured regression on SMT-constrained ones
  (same posture as `--io-busy-poll-us`). Good opt-in size: physical cores −
  shards, cap 8. Results are identical pooled or serial (enforced by
  pooled-vs-serial identity + 8-thread concurrency stress tests); worker
  panics are contained per-job (empty segment result + warn, never a hang).
  Runtime-agnostic (monoio + tokio).
- **Bounded bulk compaction**: with a pool active and `COMPACT_THRESHOLD > 0`,
  one compaction build freezes at most `max(threshold, len/8)` entries
  (`MutableSegment::freeze_prefix`), so FT.COMPACT after a bulk load yields
  several independently searchable segments instead of one giant graph —
  bounded build memory and pool-parallel search. Pool-less deployments keep
  single-segment builds (multi-segment serial search would be strictly
  slower). Red/green: `test_force_compact_bulk_bounded_segments`,
  `test_bg_compact_bulk_bounded_segments`.
- Measured (20k×384d clustered SQ8, single connection, post-FT.COMPACT,
  R@10 = 1.0 in all rows): macOS 10-core p50 2.06 ms → **0.46 ms**, 473 →
  **2,321 QPS (4.9×)**; GCE c3-standard-8 (4 physical cores) regresses at
  default ef (1,732 → 1,165 QPS) — hence the opt-in default. The GCE probe
  matrix also showed the per-index `EF_RUNTIME` knob alone closes most of the
  vs-RediSearch clustered gap (ef 64: 4,493 QPS serial at R@10 = 1.0). Full
  GCE vs-RediSearch tables in PR #214.

### Fixed — update-churn no longer mass-deletes vectors at compact/merge install (soak-diagnostic find)

- **Compact install: dead window entries no longer kill their own update.**
  `snap_and_reconcile` treated ANY tombstoned entry in the frozen window as
  "key deleted" and applied a key_hash-wide tombstone to the new immutable —
  but since the VEC-1 update path tombstones the old copy in place, a key
  updated before the freeze had a dead old copy AND a live new copy in the
  same window, and the install deleted the new copy out of the segment.
  Every key updated-then-compacted silently vanished from FT.SEARCH (32% of
  live keys lost / live-set recall 0.985 → 0.685 in a 1-minute churn soak;
  regression vs v0.5.1). A dead window entry now only proves deletion when
  the key has no live window sibling.
- **Merge install: source tombstones are origin-gated.** The merge replay
  applied each source segment's lifetime interior tombstone set key_hash-wide
  to the merged output, so a key whose old copy was tombstoned-by-update in
  one source killed its current copy merged in from a sibling segment. The
  replay now only tombstones entries whose `global_id` originated in the
  tombstone's own source; DEL/UNLINK tombstones land in every source's set
  and still apply everywhere.
- **`VectorStore::insert_vector` allocates monotonic LSNs** (same allocator
  as the wire path) instead of `mutable.len()+1`, which restarted after every
  compaction and made merge dedup keep a stale copy over the current one.
- Red/green: `test_bg_compact_update_before_freeze_survives_install`,
  `test_bg_merge_update_across_segments_survives`; end-to-end churn repro
  (24k mixed ops): LOST 1054 → 0, live-set recall 0.685 → 0.98.

### Fixed — DEL/UNLINK now unindexes vectors on every dispatch path (soak-diagnostic find)

- **Deleted keys no longer resurface in FT.SEARCH** — the vector auto-delete
  hook (`mark_deleted_for_key`) existed only on the cross-shard SPSC `Execute`
  arm and the tokio sharded handler. The monoio conn-local path (the default
  runtime's only path at `--shards 1`), `handler_single`, the MULTI/EXEC batch
  paths, and the SPSC pipeline arms never tombstoned vectors on DEL/UNLINK, so
  deleted keys kept matching KNN searches forever (20% of results after one
  minute of mixed churn; live-set recall collapsed 0.985 → 0.735 in the
  Bundle-5 soak diagnostic). All paths now share one `auto_delete_vectors`
  parity helper; wire-level red/green coverage in `tests/vector_del_unindex.rs`.

### Added — long-run vector reliability harness

- `scripts/vector-validate.py` — on-target validation driver: recall/QPS
  comparison between two moon binaries (SQ8 + TQ4, ground-truth brute force),
  churn soak with live-set recall / RSS / resurrection sampling, and kill -9
  durability (settled-write survival + double-crash restart) under
  `appendonly yes`.
- `scripts/gcloud-vector-soak.sh` — GCE orchestration (c4a ARM + c3 x86):
  ships the working branch via `git bundle`, builds baseline + branch binaries,
  runs the validation driver, fetches JSON results, tears down on exit.
- `scripts/bench-vector-vs-redisearch.py` — Moon vs RediSearch head-to-head
  driver (same FT.* wire dialect for both engines): insert throughput, KNN-10
  QPS/p50/p99, and R@10 vs numpy ground truth on random-Gaussian and
  clustered-mixture 384d datasets, with an `EF_RUNTIME` sweep so QPS is
  compared at matched recall.

### Fixed — vector search correctness bundle (deep-review VEC-1/XC-SHARD-1/XC-3/VEC-4/VEC-7)

- **HSET update no longer duplicates a vector** — re-indexing an existing key
  tombstones the old copy first (O(1) fast path in the mutable segment via the
  key→global-id map, scan fallback across mutable + immutable segments), so KNN
  totals and results no longer count both the stale and the fresh vector.
- **FT.INFO is now cluster-wide at `--shards N`** — previously answered from the
  local shard only (~1/N of `num_docs`). Now scatter-gathers to every shard and
  merges additively (top-level counters + per-field stats), on both the sharded
  and monoio handlers.
- **Filtered FT.SEARCH on the cooperative-yield path honors `FilterStrategy`** —
  the yielding search always graph-filtered; the post-filter strategy (selective
  filters) now searches unfiltered with 3×k oversampling and bitmap post-filter,
  matching the non-yielding path's recall behavior.
- **`FT.CREATE ... MERGE_MODE KEEP_RAW` is rejected fail-loud** — it silently
  behaved as graph-union; now errors until the raw-vector sidecar is implemented
  (the separate `KEEP_RAW ON` flag is unchanged).
- **MEMORY DOCTOR / Prometheus KV memory no longer report 0 under unlimited
  `maxmemory`** — the per-shard KV memory publish on the 100ms eviction tick
  had been gated on `maxmemory > 0` since the GAP-1 elastic-budget work,
  permanently zeroing the `DashTable + entries` line for the default config.
  The publish (an O(1) accumulator read per DB) now runs unconditionally;
  elastic-budget recompute stays gated on a finite cap.

### Added

- **`FT.CONFIG SET/GET <index> MERGE_RECALL_TOLERANCE <0.0..=1.0>`** — per-index
  recall gate for unattended (background/vacuum) GraphUnion merges; default 0.70
  unchanged.

### Added — exact rerank stage (deep-review HQ-1)

- **FT.SEARCH distances on compacted segments are now (near-)exact.** Immutable
  segments carry an f16 sidecar of the original vectors (built at compaction,
  BFS-ordered); the top `4·k` beam candidates are re-scored with true metric
  distances (L2: squared L2; Cosine/InnerProduct: normalized-pair squared L2)
  before top-k truncation, replacing pure quantized ADC estimates — the
  recall lever vs engines that keep full-precision vectors. SQ8, previously
  ZERO-refinement, benefits most; TQ4 estimates improve from percent-level
  error to f16 tolerance (~1e-3).
- The sidecar persists (`raw_f16.bin` per segment dir, missing file = no
  sidecar, fully backward/forward compatible), survives GraphUnion merges
  (all-or-nothing propagation), and is MEMORY DOCTOR-accounted. Memory cost:
  +2·dim bytes per vector in both the mutable segment and compacted segments
  (384d ≈ +768 B/vector); an opt-out knob is a follow-up.

### Performance — SIMD SQ8 ADC kernels (deep-review HQ-2)

- **SQ8 asymmetric distance is now SIMD-dispatched** (NEON / AVX2+FMA /
  AVX-512F, scalar fallback) via an algebraic decomposition: per-query
  constants `(Σq, Σq²)` computed once, per-candidate work reduced to three
  fused widen-u8→f32 FMA sums `(Σq·c, Σc, Σc²)` combined in O(1). Wired into
  HNSW beam search and both mutable-segment brute-force paths. Criterion
  (aarch64 NEON): 2.7×/1.8×/1.6× faster per candidate at 128/384/768d.
  Note: the pre-AVX2 x86 scalar fallback is ~1.65× slower than the old naive
  loop (the decomposition only pays with SIMD); all supported targets
  (aarch64 NEON, x86-64 AVX2+) are wins.

### Performance — vector search hot-path quick wins

- Copy-on-write `Arc` key map (no full `HashMap` clone per snapshot), hoisted
  brute-force query prep, striped search metrics counters, stable aarch64
  `prfm` node prefetch, dead quantize removed from the insert path, SQ8
  `code_len` fix in compaction, and raw-buffer work skipped for SQ8 segments.
### Performance — coordinator local legs ride group commit under appendfsync=always (PR #TBD)

- The cross-shard coordinator's LOCAL-leg persist (co-located MSET/MSETNX and
  scattered-MSET local slices) awaited one fsync ack **per command**, each
  bounded by `--aof-fsync-timeout-ms` (default 2000ms) — a pipeline of
  coordinated writes stacked these serially into the 2000–3000ms `always`
  far-tail measured on Linux/GCE (c2d-standard-16, pd-ssd; see the v3-4
  bench notes — OrbStack/macOS fsync is near-free and does not reproduce it). Local legs now enqueue fire-and-forget (bounded
  backpressure, same contract as the remote SPSC legs) and the connection
  handler confirms them with **one** `fsync_barrier` on the local shard per
  pipeline batch, before responses are serialized — so `+OK` still implies
  confirmed durability, but a batch of N coordinated writes costs 1 awaited
  fsync instead of N. `everysec`/`no` behavior is unchanged. On barrier
  failure every affected response is replaced with `MOONERR AOF fsync`
  (never a false `+OK`).

### Fixed — BITOP/COPY/DEL/UNLINK coordinator local legs now persist (PR #TBD)

- The cross-shard coordinator's in-process legs for BITOP (dest write),
  COPY (dst write + TTL restore), and multi-key DEL/UNLINK (co-located
  fast path AND the scattered local slice) executed in memory but never
  reached the owning shard's AOF — deleted keys **resurrected** from
  their seed writes on restart, and BITOP/COPY results on the
  connection's own shard silently vanished (carried v3-4 follow-up;
  remote legs were always durable via MultiExecute). All four now
  persist through the same `persist_local_leg` group-commit path as
  MSET/MSETNX: synthesized over only locally-owned keys, skipped when
  nothing was written (DEL of missing keys), and confirmed by the
  batch-end fsync barrier under `appendfsync=always`.

### Fixed — cold-tier (disk offload) correctness & reliability (PR #TBD)

- **DEL/UNLINK/FLUSHALL now reach the cold tier** — deleting a key whose value
  had been offloaded to disk left its `ColdIndex` entry alive, so the next GET
  resurrected the deleted value from the `.mpf` heap file (`DEL` even returned
  0 for cold-only keys). `Database::remove`/`clear` now drop the cold-index
  entry (and queue file unlinks) alongside the hot entry, and `DEL`/`UNLINK`
  count cold-only keys correctly.
- **Expired cold reads reclaim their index entry** — a cold read that found an
  expired entry returned nothing but left the index entry + file refcount
  behind forever (nothing else ever reclaims them). The read path now
  distinguishes `Expired` from `Miss` and removes the index entry on expiry;
  transient I/O errors still leave the entry alone.
- **Spill files survive a crash at the directory level** — spill writes fsynced
  the file but never the directory, so after a power loss the (dir-fsynced)
  manifest could reference a heap file whose directory entry vanished. Both the
  batch (tmp+rename) and single-file spill paths now fsync `data/` after
  publishing the file.
- **Crash-orphaned heap files are swept at startup** — a crash between the
  spill write and the manifest commit left `heap-*.mpf`/`.tmp` files on disk
  that no manifest references (invisible to the cold index, leaked disk
  forever). Recovery now unlinks unregistered heap files once the manifest has
  opened successfully.

### Added

- **Spill-thread liveness metrics in `INFO persistence`** —
  `spill_batches_flushed`, `spill_completions_dropped`, and
  `spill_last_heartbeat_ms` (0 = never ran) expose a silently-dead spill
  thread, whose only prior symptom was an unbounded eviction backlog.

## [0.5.1] — 2026-07-04

### Fixed

- **AOF+WAL double-write eliminated for the common config (PR #211)** — new
  `--wal-kv-log auto|on|off` (default `auto`). At `--appendonly yes` every
  SPSC-executed KV write was logged to BOTH the per-shard AOF and the per-shard WAL
  (measured 2.7× file-byte / 4.1× device write amplification at `--shards 4`), yet
  startup recovery wipes WAL-replayed state and replays the AOF — the WAL copy was
  pure disk wear. `auto` skips WAL KV records while the AOF is the recovery
  authority and no CDC subscriber is attached (logging re-engages dynamically when
  one attaches); `on` restores the pre-0.6 always-log behavior (needed for PITR /
  full CDC history alongside AOF); `off` never logs KV records. FPI/checkpoint/
  feature records are unaffected.
- **everysec/no AOF appends are no longer silently dropped under backpressure
  (PR #211)** — a full writer channel used to `warn!` + drop the record while the
  client still received `+OK` (client-acked write loss + AOF/memory divergence on
  replay). Durable handler paths now await enqueue under `--aof-fsync-timeout-ms`
  and surface `ChannelFull`/`WriteFailed` as an error frame; the synchronous
  SPSC-drain and monoio inline-SET paths apply a bounded blocking send (ONE 5ms
  budget shared across a whole pipeline/MULTI batch) and, on loss, replace the
  success frame with `MOONERR AOF backpressure` instead of acking — every drop is
  `error!`-logged and counted. `--wal-kv-log` also rejects unknown values at parse
  time. Watch `aof_backpressure_dropped` in `INFO`.

- **Docker image build** — the multi-stage `Dockerfile` now copies the vendored
  `vendor/monoio` source into the cargo-chef planner and cook stages. The v0.5.0
  release introduced a `[patch.crates-io] monoio = { path = "vendor/monoio" }`
  path dependency, but the chef stages only copied `Cargo.toml`/`Cargo.lock`/`src`,
  so `cargo chef cook` panicked (`failed to load source for dependency monoio …
  vendor/monoio/Cargo.toml: No such file or directory`) and the `Docker Image`
  release job failed. Adds a standalone `docker-publish.yml` workflow to
  (re)publish only the container image for a tag whose other assets are already live.

## [0.5.0] — 2026-07-04

Two milestones since v0.4.1:

- **v3-4 KV Write Correctness & Data-Integrity Parity** — the primary KV engine now honors
  Redis's data-integrity contracts (no wrong-type data loss, no integer-overflow panics/wraps,
  past-expire deletes, atomic MSETNX).
- **p=1 & multi-shard throughput** — Moon now beats Redis on non-pipelined (p=1) GET/SET on both
  GCE arches (ARM 1.19–1.21×, x86 1.65–1.66×), and multi-shard wins from 8 concurrent connections
  up (s4 c8 1.57–2.0×, c64 2.50×), via a poll-mode park (`--io-busy-poll-us`, backed by a vendored
  monoio fork) plus a cross-shard reply-path convoy fix. All perf gains are bench-only knobs /
  opt-in flags — the default runtime behavior is unchanged unless you set `--io-busy-poll-us`.

### Added

- **MSETNX** — atomic multi-key string write (set all pairs iff none of the keys exist; returns 1/0).
  Single-shard atomic (two-phase check-then-set, no await between phases). The cross-shard coordinator
  rejects a key span with `CROSSSLOT` (by design — no two-phase commit) and runs co-located `{hash-tag}`
  keys atomically on the owning shard. New `--shards 4` regression suite `tests/msetnx_cross_shard_reject.rs`.
- **`--io-busy-poll-us <µs>`** — poll-mode park for the monoio legacy (epoll/kqueue) driver: the shard
  thread busy-polls readiness for the given budget before blocking, deleting the per-op scheduler
  sleep+wake. This is the lever that flips non-pipelined (p=1) GET/SET to a win vs Redis on both GCE
  arches. Defaults to `0` (off); implies `--io-driver epoll`. Costs up to budget-µs CPU per idle park —
  only a win on pinned, disjoint cores (a regression on shared-core hosts). Env equivalent
  `MOON_EPOLL_SPIN_US`.
- **`--io-driver <auto|epoll>`** for the monoio runtime — force the epoll/kqueue LegacyDriver instead of
  io_uring (some platforms, e.g. GCE ARM Axion, run KV faster on epoll).
- **Per-use-case tuning guide** at `docs/guides/tuning.md` (quick recipes, shard-count guidance,
  busy-poll trade-offs, persistence cost, platform notes), linked from the docs-site Operations nav.

### Performance

- **Multi-shard now wins from 8 concurrent connections up, even without pipelining.** Fixed the C2
  reply-spin *convoy*: the cross-shard reply busy-poll was synchronous on the shard thread and, at
  ≥2 connections per shard, a spinning connection starved its sibling and the shard's SPSC drain — the
  root cause of the `--shards 4` c8-P1 0.45× collapse. A solo-conn gate now spins only when a connection
  is alone on its shard. Result (s4, P1, `--io-busy-poll-us 40`, vs Redis): c8 GET/SET ARM 1.57/1.71×,
  x86 1.9/2.0×; c64 2.50× both arches. (c1-P1 remains the structural single-hop ceiling, 0.91–0.96×.)
- **monoio cross-shard replies unified on the zero-allocation `ResponseSlotPool`** (L3b), removing the
  per-op flume-oneshot allocation and chunked park from the hot reply path (+11–14% at c8).
- **Non-pipelined GET reply framed from a borrow** — dropped a per-GET `Vec` copy.
- Stripped four per-op lock/atomic costs from the p=1 dispatch hot path.
- Tuned the default monoio io_uring driver (`COOP_TASKRUN | SINGLE_ISSUER | DEFER_TASKRUN`) on Linux.

### Changed

- Corrected default-config documentation: `--shards` default is **1** (not "0/auto") and `--appendonly`
  default is **yes** (not "no") — both config pages were wrong.

### Fixed

- **Cross-shard reply use-after-free on panic-unwind (memory safety).** The cross-shard reply path
  sent a raw `*const ResponseSlot` into a pool living on the connection task's stack; a panic unwinding
  through the reply dispatch/drain would drop that pool while a target shard still held the pointer,
  making the target's later `slot.fill()` a use-after-free (heap corruption). `ResponseSlotPtr` now
  carries an `Arc<ResponseSlot>`, so the slot outlives whichever of {connection pool, in-flight message}
  drops last — structurally closing the window on both dispatch and drain. This also **retroactively
  fixes the same latent hazard on the tokio runtime** (shipped since v0.4.0) and removes four `unsafe`
  blocks (net unsafe reduction). Follow-up: a shutdown-aware bound on the reply await (now safe to add)
  to close a partial-shutdown liveness hang.
- **`MOON_NO_URING=1` now actually switches the monoio driver.** It was a silent no-op for the monoio
  FusionDriver (io_uring was selected regardless); it now forces the epoll/kqueue LegacyDriver, matching
  its documented contract and the new `--io-driver epoll`.

- **KV data-integrity (P0):** wrong-type `GETDEL` / `GETSET` / `SET … GET` no longer silently delete or
  overwrite a key holding a non-string — they return `WRONGTYPE` and preserve the value (check-before-mutate).
- **Integer overflow (P0):** `DECRBY key -9223372036854775808` no longer panics; every expiry-setting
  command (`SET EX/PX/EXAT`, `SETEX`, `PSETEX`, `EXPIRE`/`EXPIREAT`/`PEXPIRE`) now rejects a time whose
  absolute expiry falls outside the `i64` millisecond domain with an "invalid expire time" error —
  matching Redis (`when > LLONG_MAX/1000`). This closes a latent wrap where an accepted-but-huge TTL
  (e.g. `EXPIRE k 15000000000000000`) surfaced as a **negative** `PTTL`/`PEXPIRETIME` on a live key, and
  makes an extreme-negative `EXPIRE`/`EXPIREAT` (`< i64::MIN/1000`) error instead of deleting.
- **Expire-in-the-past semantics (P0):** `EXPIRE`/`PEXPIRE`/`EXPIREAT` with a non-positive or already-past
  time now deletes the key and returns 1 (Redis parity); verified to propagate through command-based WAL
  replay so replicas stay consistent.
- **Cross-shard coordinator local-leg durability (P0, pre-existing):** a co-located `MSET`/`MSETNX` whose
  keys hash to the **connection's own shard** now appends to that shard's AOF. The coordinator's local
  leg previously executed the write in memory but never persisted it (the remote `MultiExecute` leg
  always did), so with `--shards >1 --appendonly yes` an own-shard co-located `MSET`/`MSETNX` could be
  lost on crash. It now persists via the same append path every local single-key write uses — the whole
  command for a co-located owner, and a synthesized `MSET` over **only the local keys** for a scattered
  `MSET`'s local slice — returning an AOF error instead of a false `+OK` on append failure.
  Crash-recovery verified on both monoio and tokio (`tests/coordinator_local_leg_durability.rs`).
  Single-shard (the default) was never affected. The same local-leg gap remains for coordinator
  `BITOP` / `COPY` / `DEL` / `UNLINK` (tracked follow-up).

### Dependencies

- **Vendored `monoio` 0.2.4** (`vendor/monoio`, wired via `[patch.crates-io]`; upstream git sha
  `f7827ddd`, recorded in `vendor/monoio/.cargo_vcs_info.json`). A fork of upstream monoio 0.2.4 with a
  bounded "moon patch" (marked `// moon patch`, confined to 4 files — `lib.rs`, `driver/mod.rs`,
  `driver/uring/mod.rs`, `driver/legacy/mod.rs` — verified against pristine upstream) that adds the
  env/CLI-gated poll-mode park behind `--io-busy-poll-us` and a per-thread spin-hook handshake. Adds
  **zero new `unsafe`**; the dependency list is byte-identical to upstream (no added transitive deps);
  behavior is byte-identical to upstream when the `MOON_*` spin knobs are unset. Introduced to enable
  the p=1 poll-mode park.

## [0.4.1] — 2026-06-23

Measure-only validation release — **no server behavior change**. Bundles the closed
milestone **Cross-Shard Read Absolute Validation** (v2-2): bare-metal GCloud proof that
the v0.4.0 cross-shard read C2 fast-path performs as claimed, plus a reusable
absolute-latency benchmark harness and a data-backed decision to not pursue further
cross-shard-read optimization.

### Added

- `scripts/gcloud-xshard-absolute.sh` — reusable dual-vendor (Intel c3 + AMD c2d),
  dual-runtime (monoio + tokio) absolute cross-shard latency harness: best-of-5 RPS floor,
  fail-closed validity gates (load / steal / clean / s1-LOCAL-control), per-run
  provision + teardown, and an `--self-test` that exercises the gates with no cloud cost.

### Validated

- The shipped C2 cross-shard read fast-path, measured **absolute on bare-metal** across
  four instruments (Intel + AMD × monoio + tokio): C2 recovers **38–49%** of the
  SPSC-migration regression, leaving a vendor/runtime-converging **~10µs (±1µs) structural
  residual** — one irreducible cross-thread reply hop. monoio is the high-confidence set
  (tokio is rep-bimodal; best-of-5 floor clean). Disposition: **close-the-line** —
  coalescing cannot help the singleton path (the concurrent guard is already flat) and
  RCU's per-shard RSS cost is unjustified by a 10µs gap. Mechanism asserted byte-identical
  (`tests/xshard_mechanism_unchanged.rs`); `git diff --stat src/` empty.

### Risk-accepted (shipped, disclosed)

- The shardslice cross-shard-read fast-path removal waiver (PR #175, owner Tin Dang,
  expires 2026-08-01) rides into this release. **This release validates it**: the regression
  is quantified and the C2 recovery confirmed on bare-metal, so the waiver is now retirable
  and its follow-up task (`cross-shard-read-acceleration`) is closed as "no further work
  warranted."

## [0.4.0] — 2026-06-22

Bundles 5 closed milestones (first ADD-recorded cut; see `RELEASES.md` for
attribution): **Shared-Nothing Integrity & Cross-Shard Latency** (3 carried) ·
**Multi-Core Throughput Hardening** (12 carried) · **Throughput Polish**
(3 carried) · **FTS Hardening** (8 carried) · **Graph Correctness & Cypher
Filtering**. Headlines: working FTS query combinators (OR / TEXT+TAG / NUMERIC)
with true total-matched counts and the O(M²) high-DF cliff gone; correct Cypher
inline-property narrowing, incoming-edge traversal after compaction, and >32
graph labels; WAL group-commit, cross-shard read fast-path, and off-event-loop
FT.SEARCH throughput wins. Per-change detail below.

### Risk-accepted (shipped, disclosed) — cross-shard read fast-path removed (PR #175)

The shared-nothing migration (PR #175) deleted the direct-`RwLock` cross-shard
read fast-path; cross-shard reads now route through the owner shard's SPSC ring,
so cross-shard-read cells regress versus the pre-migration lock-read. This is a
**signed RISK-ACCEPTED waiver** (owner: Tin Dang) carried into this release, with
a follow-up task **cross-shard-read-acceleration** (lock-free cross-shard read
acceleration) tracked to land before **2026-08-01**. No correctness impact —
consistency is 197/197 across 1/4/12 shards on both runtimes; the cost is latency
on the cross-shard read path only.

### Docs — Verified cross-arch GCloud benchmark confirms v3-1 FTS + v3-2 graph fixes (PR #194)

Re-ran the 4-feature concurrent-vs-competitor benchmark (KV / Vector / Graph / FTS
vs Redis / RediSearch / FalkorDB) on GCloud x86 `c3-standard-8` (Sapphire Rapids) +
ARM `t2a-standard-8` (Neoverse-N1) against build `8238515`, with the graph harness
corrected to filter on the node `id` **property value** (not the internal
`GRAPH.ADDNODE` handle that produced a false `cypher_match_rows=0` in the prior
re-baseline). Proves **in-harness** that v3-2's inline filter narrows Cypher
point-queries (`cypher_match_rows` 14991 → **4**, identical to FalkorDB; cypher_1hop
~28–30× faster than its old full-scan self) and that v3-1's FTS hardening makes every
query combinator return RediSearch-exact total counts (OR 2072, TAG 5064, TEXT+TAG
253, NUMERIC 10119) while indexing ~40× faster (376 → 15052 docs/s) with the O(M²)
high-DF cliff gone (419 ms → 33 ms). KV and Vector unchanged (no regression). Folded
as layered BENCHMARK.md §2.9 / §10.6 / §11.5 / §12.3 over the 2026-06-16 record;
full report + corrected harness + raw logs in `docs/reviews/2026-06-17/`.

### Docs — Wider cross-arch benchmark: shard scaling (1/4/12) × structures × pipeline/datasize × production (PR #194)

Ran the repo-canonical `scripts/bench-compare.sh` (all-commands × pipeline 1–128 ×
datasize 8B–64KB × connections) at `--shards 1/4/12` plus `scripts/bench-production.sh`
(10 scenarios at shards 1 and 12) on GCloud x86 `c3-standard-8` + ARM `t2a-standard-8`,
Moon `8238515` vs Redis 7.0.15. Quantifies the core scaling claim honestly: Moon's
advantage is **pipeline depth, not shard count** (break-even p=16; GET p=128 ~2.8×, SET
p=64 ~1.85×), every core data structure tracks GET/SET at p=1 (~0.79×, TCP-RTT bound),
and **adding shards hurts uniform single-key non-pipelined throughput** (12 shards →
0.46–0.51× Redis vs ~0.79× at 1 shard). Folded as BENCHMARK.md §4.5 (structure coverage)
+ §6.4 (cross-arch shard scaling); full detail + logs in
`docs/reviews/2026-06-17/WIDER-BENCH.md`. The run also patched a latent unfairness in the
bench scripts (they start Redis AOF-off but Moon AOF-on, flooding ~1M WARN lines).

Follow-up root cause for the `shards=12, pipeline=1` worst case: a controlled single-VM
A/B (`docs/reviews/2026-06-22/`) isolates it as a three-factor interaction — cross-shard
dispatch (2 cross-thread wakes/op) × `p=1` (no batch amortization) × random keys (defeat
the ≥62.5% `AffinityTracker` migration, so the connection never goes local). The collapse
is confined to the `s12 × random` cell (GET 0.47× / SET 0.41×); `s1 random` is 0.95×/1.15×
and `s12 single` self-heals to 0.74×/0.73×. WIDER-BENCH.md §4a + `XSHARD-P1-ROOTCAUSE.md`.

### Fixed — bench-compare.sh / bench-production.sh start moon AOF-off to match Redis (fair, no flood) (PR #194)

Both benchmark scripts started Redis in-memory (`--save "" --appendonly no`) but moon with
persistence defaults (AOF on). Under SET load moon's AOF channel saturated, flooding
`AOF append dropped … channel full` WARN logs (~1M lines, which also pinned the port so
later shard configs failed to bind) and unfairly handicapping moon against a no-AOF Redis.
Both scripts now start moon with `--appendonly no --disk-offload disable` (and redirect its
stdout/stderr), so the comparison is AOF-off on both sides and the report stays clean.

### Fixed — Graph node labels with id ≥ 32 are stored, matched, and persisted (no silent truncation) (PR #193)

CSR node labels were packed into a 32-bit `NodeMeta::label_bitmap`, so any label
id ≥ 32 was silently dropped at `from_frozen` (`if label < 32`) — `MATCH (a:Label40)`
matched nothing and a graph using more than 32 distinct labels lost the rest. Labels
0–31 keep the u32 bitmap fast path; ids ≥ 32 now live in a sparse, per-row
`label_overflow` store, and `LabelIndex` is built from both sources, so every `u16`
label id is matchable. The on-disk CSR format is bumped to **version 4** with a new
version-gated trailing section (`[entry_count][ (row, label_count, label_id…) ]`,
ascending by row, covered by the existing CRC); the `GraphSegmentHeader` gains a
`label_overflow_offset` field reusing existing padding (on-disk header size
unchanged). Both the heap (`from_bytes`) and mmap (`from_mmap_file`) loaders parse
the section into an owned map via one shared bounds-checked parser (returns
`CsrError` on truncation, never panics; transitively fuzzed by `csr_from_bytes`),
and `compact_segments` re-keys overflow labels to each surviving node's new merged
row. Backward-compatible: version ≤ 3 segments load with an empty overflow store and
behave exactly as before; `NodeMeta`'s 48-byte layout is unchanged.

### Fixed — Graph Incoming / Both traversals return incoming edges after compaction (PR #193)

A compacted graph stores edges only in immutable CSR segments, which keep just
OUTGOING adjacency. `SegmentMergeReader` therefore `continue`d past the CSR for
`Direction::Incoming` (returning nothing) and silently dropped the incoming half
for `Direction::Both` — so `MATCH (a)<-[]-(b)` / undirected traversals over
compacted data missed every predecessor. The reader now serves predecessors from
a derived reverse index (`IncomingIndex`) built once per segment from the existing
forward arrays (`row_offsets` + `col_indices`) and cached in a non-persisted
`OnceLock` — no segment format/version change, so on-disk heap and mmap segments
answer incoming queries unchanged. Incoming edges reuse the same `edge_meta` /
`edge_created_ms` / validity / node-visibility by edge index, so the edge-type
filter, tombstones, decay stamp, deleted-node exclusion, snapshot rules, and
NodeKey dedup are identical to the outgoing path. `Direction::Outgoing` is
unchanged. MemGraph already handled all three directions; this fixes the
immutable-CSR path only.

### Fixed — Cypher MATCH narrows on inline node-property predicates instead of full-scanning the label (PR #193)

`compile_match` built each pattern node's `NodeScan`/`Expand` but silently
dropped the node's inline properties `(v {k:e, …})`, so
`MATCH (a:Person {id:1})-[]->(b)` ignored `{id:1}` and returned every edge of the
`Person` label (≈|E| rows) instead of node 1's out-edges. The planner now emits a
`Filter` — the equality conjunction `v.k = e AND …` built by the existing
`properties_to_filter` — immediately after the op that binds each
inline-propertied node: after its `NodeScan` for the first node, after the
`Expand` that produces any subsequent node. It reuses the existing `Filter`/`Expr`
evaluation, so parameters (`{id:$p}`), cross-type compares, and missing-property
⇒ excluded all follow the same semantics as `WHERE`; an empty `{}` pattern emits
no Filter, leaving plain label/all scans byte-identical. No new `PhysicalOp`, no
index, no executor change.

### Fixed — FT.SEARCH routing: prose "knn" searches as text, standalone SPARSE reaches the vector engine, no panic on the BM25 AND path (PR #192)

`is_text_query` uppercased the query and matched the bare substring `KNN `, so a
text search whose terms merely contained the word *knn* (e.g.
`FT.SEARCH idx "knn tutorial"`) was misclassified as a vector query and fell to
the KNN parser, returning `ERR invalid KNN query syntax` instead of text
results. It now keys on the canonical `[KNN` vector-query bracket — prose
containing "knn" searches as text while `*=>[KNN …]` still routes to the vector
engine. A standalone `SPARSE @field $param` clause paired with a text-looking
query string was silently dropped onto the text path (`is_text_query` only sees
`args[1]`); a `has_sparse_clause` guard now defers any SPARSE-carrying query to
the vector engine at every text-route gate. The three `.expect("posting exists")`
on `TextStore::search_field`'s BM25 AND + scoring path are replaced with
defensive control flow, so a vanished posting yields empty results instead of
panicking the server. Output is byte-identical for all existing queries.

### Fixed — FT.SEARCH integer reply is the true total-matched, not the page size (PR #192)

The first element of an `FT.SEARCH` reply (the match count) was capped at the
`LIMIT`/`top_k` page size: `FT.SEARCH idx "term" LIMIT 0 5` over 100 matches
reported `5`, not `100`, because the count was read from the already-truncated
result page. On multi-shard indexes the coordinator compounded it by counting the
merged-and-truncated returned docs rather than each shard's true matched count.
The reply now reports the true number of matched, key-resolvable documents
(RediSearch semantics): the evaluator surfaces the count before truncation, and
the multi-shard merge sums each shard's local matched count (keys partition to
exactly one shard, so the sum is exact; errored shards contribute 0). Verified
identical on 1- and 4-shard servers. Returned document pages (count, order,
scores, keys) are unchanged.

### Performance — FT.SEARCH upsert / bulk re-index no longer O(V) per document (PR #192)

Re-indexing a document (an HSET upsert, or a bulk re-index pass) called
`PostingStore::remove_doc`, which scanned EVERY term's posting list to clear one
document — O(total-vocabulary) per doc — so per-upsert cost grew with the corpus
(the indexing-rate cliff: ~376 vs ~18,052 docs/s as vocabulary V grew).
`PostingStore` now keeps a reverse `doc_id → term_ids` index, populated once per
(doc, term) edge as terms are added, so `remove_doc` visits only the terms the
document actually contributed — O(terms-in-doc), independent of V. Search output
is byte-identical (the rank-aligned posting contract is unchanged): matched doc
sets, BM25 scores, `doc_freq`, `num_docs`, and avgdl are unaffected. A missing or
already-cleared reverse entry is skipped defensively — never an unwrap/panic.

### Fixed — FT.SEARCH `OR` and multi-clause queries return correct result sets (PR #190)

`FT.SEARCH` query combinators were silently broken: `OR` (`alpha | beta`)
collapsed to an intersection (returning only docs matching *both* terms), and a
multi-clause query such as `@body:foo @tag:{bar}` returned **zero** results
instead of the intersection. A code trace showed the root cause was a missing
parser layer — four runtime handlers each re-parsed the query inline with a
flat, AND-only shape that could not represent an `OR`/grouped AST, and the
cross-shard scatter carried that same flat shape so `OR` was unfixable at the
leaf. This lands a recursive-descent query parser producing a frozen
`QueryNode` AST and one centralized evaluator (`eval_set` folds the AST to a
`RoaringBitmap` — `AND` = intersection, `OR` = union — over the shared doc-id
space; `eval_query` adds best-effort BM25 with deterministic score-desc /
doc-id-asc ordering). All four FT.SEARCH text branches plus the cross-shard
Phase-2 scatter now route raw query bytes through `parse_query → eval_query →
build_text_response`; the cross-shard payload carries the opaque query bytes and
each shard re-parses, so `OR`/grouping/`TEXT+TAG`/`TEXT+NUMERIC` work in every
shard configuration. Malformed queries return a coded `Frame::Error` (one of
five frozen codes) and never panic the server. Verified end-to-end over the wire
on 1- and 4-shard servers (`alpha | beta` → union, `@body:foo @tag:{bar}` →
intersection, 1-shard vs 4-shard result sets identical). `HYBRID`/vector-KNN/
`SPARSE`/`SESSION`/`RANGE` dispatch is untouched.

### Performance — high-DF FT.SEARCH term queries no longer O(M²) (PR #190)

The per-document term-frequency lookup on the BM25 path was an O(N)
`.position(|id| id == doc_id)` linear scan over a posting list, so a query on a
high-document-frequency term degraded to O(M²) (a ~5%-of-corpus term took
~419 ms on a 100K-doc index). `PostingList` now keeps `term_freqs`/`positions`
in sorted-doc-id (rank) order aligned with the doc-id `RoaringBitmap`, and
`tf()`/`positions_for()` resolve via `RoaringBitmap::rank` (sub-linear). The
rank alignment also fixed a latent BM25 corruption where re-indexing an updated
low-id document pushed its term frequency out of position, silently misaligning
scores.

### Performance — monoio FT.SEARCH yield is now cost-free; brute-force knee raised to 1024 (PR #189)

PR #179's monoio cooperative yield reaped the io_uring completion queue by
parking on `sleep(ZERO)` — correct, but ~1746 µs per yield, which is what forced
#179 to keep the brute-force chunk small and defer ~22% of transient throughput.
The yield now reaps the CQ by parking on a pre-armed `UnixStream::pair` read
(~0.317 µs — effectively free), so the brute-force chunk knee walks back up from
256 to **1024** elements per yield without reintroducing the co-located latency
#179 fixed. The knee was A/B confirmed per-architecture on GCloud: K=512 breached
the 5% latency budget on x86 (+6–8%) but held on aarch64, so 1024 was chosen as
the value safe on both. #179's co-located p99 relief is fully preserved — this
reclaims only the throughput #179 had to trade away. tokio is unaffected (it
already used `yield_now()`). Companion benchmark/review docs land alongside:
`BENCHMARK.md` §2.8 (GCloud cross-arch re-measurement) and a 4-feature deep
review + concurrent-vs-competitor benchmark under `docs/reviews/2026-06-16/`.

### Performance — FT.SEARCH no longer stalls the shard event loop (PR #179)

A heavy `FT.SEARCH` (large brute-force mutable segment or deep HNSW traversal)
used to run fully synchronously on its shard's event loop, blocking the 1ms
tick and every co-located command on that shard until the search returned.
The per-shard local search slice now captures an owned, point-in-time snapshot
of the index at entry and walks the same logical steps cooperatively, yielding
to the event loop between bounded chunks so the tick fires and co-located
`PING`/`GET` are serviced while the search is in flight. Results are
byte-identical to the synchronous path (the mutable segment is append-only, so
a chunked scan over the captured length matches an atomic scan), MVCC snapshot
isolation is preserved, and there is no new cross-thread lock and no
steady-state RSS growth. The cooperative yield is runtime-specific: tokio uses
`yield_now()`; monoio uses a zero-duration timer park, because a bare self-wake
never lets monoio's io_uring loop reap the completion queue (it would be a
silent no-op). Measured co-located `PING` p99 during a heavy search drops from
~48 ms (1 client) / ~300 ms (3 clients) to **6.6 ms / 27 ms** — roughly 7–11×
relief — on both runtimes. The yield costs throughput only on a large
*uncompacted* brute-force scan (operator-tunable via `MOON_FT_YIELD_CHUNK`);
light and HNSW searches whose work is under one chunk never yield and pay
nothing. The cross-shard scatter-gather, merge/rerank, and result semantics are
unchanged.

### Performance — WAL group commit under `appendfsync=always` (PR #178)

Concurrent writes pending at the same shard now coalesce into a single
`fsync` instead of one sync per write, collapsing a large share of the
~11× `appendfsync=always` throughput penalty that appears when multiple
clients write in parallel. The batching is opportunistic — a writer drains
every `AppendSync` already queued on its shard channel and issues one
barrier `fsync` for the whole group — and is wired into all four AOF writer
loops (`{TopLevel, PerShard}` × `{monoio sync, tokio async}`). Durability is
unchanged: control records route to a separate deferred queue so a batch can
never straddle a non-data message, and the exactly-once-under-crash
invariant (crash-matrix + SIGKILL integration tests) holds on both runtimes.
The absolute throughput gain is disk-dependent — structurally unmeasurable on
the near-free virtio `fsync` of the dev VM, so the coalescing mechanism (K
`AppendSync` → 1 `fsync`) is pinned by a deterministic batching seam test and
the wall-clock magnitude is deferred to real-disk hardware.

### Removed (BREAKING) — `--cross-shard-fast-path` flag and its dead telemetry

The orphaned `--cross-shard-fast-path` CLI flag (with its `CrossShardFastPath`
config enum and `cross_shard_fast_path_enabled()`) is deleted, along with the
dead `moon_cross_shard_lock_contention_total` metric, the
`moon_dispatch_cross_read_fastpath_latency_us` histogram, the
`record_dispatch_cross_read_fastpath_*` recorders, and the hardcoded-`0`
`cross_read_fast_dispatches` stat. These were Phase-0 scaffolding for the
pre-shared-nothing RwLock read path (gone since PR #175) and had zero
production callers. **Breaking:** `moon --cross-shard-fast-path …` now exits
non-zero with a clap unknown-argument error — remove it from any launch
script. The internal `scripts/bench-cross-shard-fastpath.sh` is also deleted.

### Performance — Lock-free cross-shard read recovery (idle-gated reply spin) (PR #177)

Cross-shard single-key reads recover a meaningful share of the latency the
shared-nothing migration (PR #175) gave up, **without** re-introducing a
cross-thread lock or growing per-key memory. When a shard is near-idle the
requesting connection briefly busy-polls its cross-shard reply instead of
parking — skipping one of the round-trip's two cross-thread wakes. The poll is
gated on a thread-local in-flight counter (`Cell<u32>`, no atomic/lock), so a
busy shard never spins and high-concurrency throughput is unaffected. Same-run
quiesced-VM measurement (monoio, 4 shards): single-client cross-shard GET
**+18.0%** (and cross-shard SET +21%, sharing the reply path); c100 GET +8.4%
with no starvation. Consistency 197/197 across 1/4/12 shards; dual-runtime
(monoio + tokio). Cross-connection read *coalescing* — the second half of the
original design — is deferred to a dedicated follow-up.

### Changed — Shared-nothing thread-local shard storage (PR #175)

Shard storage moved from `RwLock`/`Mutex`-wrapped shared databases to
thread-local **ShardSlices** owned exclusively by each shard's event
loop, on both runtimes (monoio and tokio). The lock wrappers are
deleted; all cross-shard access routes through the owner shard's SPSC
ring. Routed multi-shard workloads are parity or better (up to +12% on
pipelined GET at 4 shards); the cross-shard *read fast-path* (direct
RwLock read of another shard's data) is definitionally gone and those
cells regress — accepted with a follow-up task for lock-free cross-shard
read acceleration. BGREWRITEAOF on every layout now uses the C4
cooperative-fold protocol: the AOF writer requests an atomic snapshot
from the owning shard and drains pre-snapshot appends with an exact
bound, so concurrent writes are neither lost nor double-applied. This
also fixes BGREWRITEAOF being silently inoperative on the top-level AOF
layout (`--shards 1`), where the rewrite previously errored internally
while the client was told it had started.

### Added — HYBRID FT.SEARCH `FILTER` push-down on both branches (PR #174)

`FT.SEARCH ... HYBRID` now accepts an optional `FILTER` clause that is
applied to **both** the BM25 and the dense-KNN streams *before* RRF
fusion, closing a bypass where a filtered hybrid query leaked
foreign-field hits through the unfiltered dense branch. The filter is a
recursive, arity-counted grammar — `TAG @field value` (exact, or prefix
when `value` ends in `*`), `NUMERIC @field min max` (inclusive range),
and `AND`/`OR` combinators — bounded at depth 4 / 16 leaves and parsed
without panicking on malformed input. Filtering is per-shard on the
scatter/gather path, so multi-shard results stay correct. Absent a
`FILTER` clause the wire format and behavior are byte-identical to
before (fully backward compatible). Exposed through the Rust SDK as
`TextClient::hybrid_search(..., filter: Option<&HybridFilter>)`.

### Fixed — Cross-shard commands now route to the owning shard (PR #173)

On multi-shard servers, SO_REUSEPORT spreads client connections across
shard accept loops — and several command families operated on the
*connection's* shard instead of the shard that owns the data. Whether
these commands worked depended on which shard the kernel happened to
accept the connection on. All four groups are fixed; the consistency
suite now passes 197/197 at 1, 4, and 12 shards:

- **BITOP** routed by its sub-operation literal (`AND`/`OR`/…): sources
  were read and the destination written on an arbitrary shard. A new
  coordinator gathers sources per owning shard and writes the result on
  the destination's owner (Redis semantics: NOT arity, zero-padding,
  all-missing sources delete the destination and return 0).
- **COPY** wrote the destination into the *source's* shard, making it
  unreadable at its own. Cross-shard string copies preserve value + TTL
  and honor REPLACE/NX; cross-shard non-string COPY returns an explicit
  error instead of corrupting state.
- **GRAPH.\*, TEMPORAL.INVALIDATE, and TXN.ABORT graph rollback** ran on
  the connection's per-shard graph store — graphs randomly "did not
  exist" from ~3/4 of connections at 4 shards. Graph commands now hop to
  the graph-name-owning shard; transactional rollback partitions undo
  work by owner and awaits acknowledgements.
- **WS.\* and MQ.\***: the workspace registry was per-shard (WS AUTH
  from another connection answered "workspace not found"; WS LIST
  diverged per connection) and durable queues lived on the creating
  connection's shard (MQ PUSH/POP/ACK/DLQLEN from elsewhere answered
  "queue is not durable"). The workspace registry is now global with a
  single WAL stream, and every MQ operation targets the queue key's
  owning shard — including dead-letter routing, trigger registration,
  and MQ.PUBLISH materialization at TXN.COMMIT.

### Changed — Event-driven cross-shard wake (the ~1ms monoio floor is gone)

- **The monoio shard event loop is now event-driven.** Its single await point
  was the 1ms periodic tick, so every cross-shard hop queued up to 1ms on the
  target shard plus up to 1ms on the origin's reply sweep. The loop now races
  the tick against the shard's SPSC `Notify` (hand-rolled allocation-free
  `race2` future — `monoio::select!` remains banned), and connection tasks
  await cross-shard replies directly. Cadence work (WAL flush, cached clock,
  snapshot/auto-save sub-timers) stays pinned to the timer arm. Measured on
  a 4-shard Linux server: single-client cross-shard SET p99 4.07 ms → 0.071 ms
  (57×), pipelined SET +368%, non-pipelined SET +404%; single-shard
  throughput +63–80%. (PR #172)
- **Drain-cap self-re-notify:** a >256-message cross-shard burst no longer
  strands its tail until the next tick — a capped drain re-arms the wake
  immediately (both runtimes).
- **New INFO Stats counters:** `spsc_notify_wakes` and `spsc_drain_renotify`
  make the event-driven path observable from a black-box client.

### Changed — Hot-path lock quick wins

- Per-command global lock acquisitions removed from the command dispatch path
  (replication offset reads, shared-databases lookups, socket-option setup,
  accept-path state), with the replication backlog append switched to a
  bulk-copy. (PR #172)

### Fixed

- `uring_handler` in-flight send queue used `Vec` API on a `VecDeque`
  (`.push` → `.push_back`) — the Linux+tokio io_uring bridge path failed to
  compile on its own. (PR #172)

## [0.3.0] — 2026-06-11

Hot-shard elasticity + vector engine maturity: background HNSW compaction
(no more shard freezes), working SQ8 quantization, elastic per-shard memory
budgets, `HOTKEYS` detection, and a +33%/+25% pipelined SET/GET perf pass.

### Added — Docs: Design Advantages page with architecture diagrams

- New docs page (`design-advantages.md`, MkDocs nav → Concepts) telling the
  full architecture story with 6 whiteboard diagrams: platform hero, memory
  engine, persistence, vector engine, graph engine, converged GraphRAG
  workflow, plus an "AI agent era" section mapping agent memory types to
  Moon surfaces. Originally authored for the retired Mintlify site (PR
  #145), converted to MkDocs Material and refreshed: quantization guidance
  now says SQ8 (TQ8 never existed), and the segment-lifecycle and disk
  offload sections reflect background compaction (PR #165) and elastic
  memory budgets (PR #170). README "Why Moon" embeds the hero diagram.

### Changed — Background FT compaction + segment merge (no more shard freezes)

- **HNSW index builds no longer block the shard event loop.** `try_compact`
  (auto-compaction on search) now begins the build on a background worker
  pool and installs the finished immutable segment on a later poll; the
  triggering search continues against the still-present brute-force mutable
  segment. Inline builds previously froze the shard for the full build
  (0.42s @2k → 24.9s @50k vectors, measured); the event-loop stall is now
  ~4.4ms and PING latency stays flat during compaction.
- **Background immutable-segment merge**: many small immutable segments are
  merged into one on the same worker pool (graph-union, no lossy
  decode/re-encode), bounding per-search segment fan-out. Real-MiniLM
  recall preserved across the merge (0.9447 → 0.9440 R@10, 18 segments → 1).
- Deletes that race a background build are reconciled at install time
  (delete-window replay) — no resurrection of removed vectors.
- `FT.COMPACT` (explicit user intent) stays synchronous: it drains any
  in-flight background build, then compacts inline. Tradeoff: searches run
  brute-force on the mutable segment until the background build installs.

### Added — Elastic per-shard memory budgets (hot-shard headroom borrowing)

- **Hot shards now borrow idle siblings' unused `maxmemory` headroom**
  instead of being capped at a static `maxmemory / num_shards`. Each shard
  publishes its used memory on its existing 100ms eviction tick and
  recomputes an elastic budget from the published snapshot:
  `base + Σ(under-budget surplus) / hot_shard_count`, clamped to total
  `maxmemory`. No new locks or channels — two relaxed atomics per shard,
  read once per write-path eviction check.
- Snapshot invariant: hot shards' budgets plus under-budget shards' usage
  never exceed `N × base` at recompute time; transient overshoot is bounded
  by one tick of donor write growth and converges via write-path eviction.
- Live 4-shard validation (32MB cap, allkeys-lru, all writes to one shard
  via a `{tag}`): hot shard retained ~28k × 1KB keys (~30MB) vs the ~7.2k
  (8MB) static ceiling — 3.9× more of the configured memory actually
  usable under skewed load. Uniform load is unchanged (same global
  ceiling, budgets stay at base).
- Disk-offload pressure cascade and timer-based eviction enforce the same
  elastic budget; budget `0` (not yet published, single shard, or
  `maxmemory=0`) falls back to the static per-shard cap everywhere.

### Added — Hot-key detection: `HOTKEYS` command + per-shard SpaceSaving sketch

- **New `HOTKEYS [COUNT n]` command** (Moon extension, `@server` ACL,
  read-only): returns the top sampled keys as `[key, sampled_count]` pairs,
  count descending. Counts are 1-in-64 samples of keyed commands — multiply
  by 64 for an approximate command rate. In multi-shard mode the connection
  handler merges per-shard sketches (DBSIZE-style scatter-gather), so clients
  always see the global view.
- **Per-shard SpaceSaving top-K sketch** (K=128, ~5 KB per database,
  L1-resident, single-pass scan): fed by all three execution paths — write dispatch, the shared
  read path, and the inline GET/SET fast path. The tick counter is one relaxed
  `fetch_add` per command; the O(K) sketch update runs only on sampled ticks
  and `try_lock`s (a contended sample is dropped — the hot path never blocks).
  Kill switch: `MOON_NO_HOTKEYS=1`.

### Fixed — `OBJECT` was unreachable over the wire (monoio handler)

- **`OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT` returned `ERR unknown command`
  on a live server** even though dispatch implemented them: the monoio
  handler routes every non-write command through the read dispatcher, which
  had no `OBJECT` arm. Added a read-only `OBJECT` handler (`get_if_alive`,
  never mutates expiry state) and enabled it on the cross-shard fast path.
- **`OBJECT <subcmd> <key>` routed by the subcommand name** in multi-shard
  mode (`extract_primary_key` hashes `args[0]`, which is `FREQ`/`ENCODING`,
  not the key) — the command landed on an arbitrary shard and answered
  `ERR no such key`. Key extraction now uses the real key (arg 2).

### Changed — Hot-path performance: deep-review optimization pass (PR #168)

- **+33% pipelined SET, +25% pipelined GET (single shard)** from a deep
  performance review covering protocol → dispatch → shard → storage →
  persistence → vector. 11 verified findings implemented:
  - `Database::get`: 3 → 2 DashTable probes on a hit, 2 → 1 on a miss
    (single-probe live/expired/absent classification).
  - WAL v2 flush: 3 → 1 `write(2)` syscalls per 1ms tick via a reusable
    staging buffer; on-disk format unchanged.
  - WAL v3 record append no longer heap-allocates per non-FPI record
    (`Cow::Borrowed`); checkpoint state machine no longer clones per tick.
  - SPSC drain (`drain_spsc_shared`) reuses thread-local batch buffers
    instead of allocating two `Vec`s per call.
  - `Frame::Double` serialization is zero-alloc (stack `f64` Display
    formatter, byte-identical wire output) in RESP2 and RESP3.
  - SQ8 vector search no longer heap-allocates per query (HNSW + both
    brute-force paths); IVF rotation/LUT buffers hoisted out of the
    per-segment loop in `search_filtered` (parity with `search_mvcc`).
  - io_uring `drain_completions` reuses persistent CQE/event buffers
    (allocation-free drain loop at steady state).
- Multi-shard (2/4/8/16/auto): no regressions; +9–32% pipelined SET at
  2 and 8 shards. The unpipelined cross-shard latency floor (~1.7ms p50,
  waker-relay + dispatch tick) is unchanged and tracked for follow-up.

### Fixed — SQ8 vector quantization now works across the full FT lifecycle

- **`FT.CREATE ... QUANTIZATION SQ8` was a declared-but-unimplemented
  quantizer** — vectors fell through to the TQ4 encoder against an empty
  codebook, producing degenerate codes and random search (recall 0.014,
  exact-match returned the wrong key) on real embeddings. Implemented a real
  per-vector affine scalar-8-bit quantizer (`dim` u8 codes + `(min, scale)`
  trailer) and wired it through the entire segment lifecycle: append,
  brute-force search, compact, immutable HNSW search, multi-segment search
  fan-out, segment **merge**, and persistence reload. Recall on real
  all-MiniLM-L6-v2 384d embeddings: **0.014 → 0.897**, exact-match correct.
- **`append_transactional()` corrupted SQ8 vectors** — it wrote the TQ slot
  layout (`padded/2 + 4`) into `dim + 8` SQ8 slots, corrupting the code stride
  for every transactionally-inserted or WAL-recovered SQ8 vector. Both append
  paths now share one `encode_sq8_slot()` helper.
- **SQ8 ignored the index metric** — an `InnerProduct` index ranked
  non-normalized inputs by magnitude. SQ8 now normalizes for both unit-sphere
  metrics (Cosine + InnerProduct), matching the rest of the engine; `L2` keeps
  raw vectors for true Euclidean ranking.
- **Docs:** corrected the ≤384d quantization guidance to recommend **SQ8**
  (the real 8-bit option) instead of the nonexistent "TQ8".

### Changed — Rust SDK released as moondb 0.2.0 on crates.io

- **`moondb` crate `0.1.1` → `0.2.0`** — publishes the v0.2-era client API
  that has lived in-tree since the `hybrid_search` sparse upgrade
  (`f4fcd5a`). Breaking: `text().hybrid_search()` now takes
  `sparse_field: Option<&str>` and `weights: [f64; 3]` (was two-way
  `[f64; 2]`) and speaks the PARAMS-based wire format with `@`-prefixed
  field refs. Added: `Client::connect_with_timeout` for bulk-write
  workloads. Released as 0.2.0 — not 0.1.2 — because cargo treats 0.1.x
  versions as compatible, and the signature change would break published
  0.1.x consumers (lunaris-retrieve / lunaris-storage-moon 0.2.1 pin
  `"0.1.1"` with the two-way call). Aligns the SDK version with the Moon
  v0.2.0 server release.

## [0.2.0] — 2026-06-06

The v0.2 enterprise beachhead. Built additively on per-shard WAL v3 + the
dual-root manifest; no changes to the KV hot path, MVCC, page format, or
transaction layer.

### Changed — default persistence directory is the platform user-data dir (was: current directory)

- **`--dir` now defaults to the platform user-data directory**, created
  on first run: Linux `$XDG_DATA_HOME/moon` (or `~/.local/share/moon`),
  macOS `~/Library/Application Support/moon`, Windows
  `%LOCALAPPDATA%\moon`. Installed binaries no longer litter whatever
  directory they happen to be started from.
- **Back-compat guard:** if the startup directory already contains moon
  persistence data (`appendonlydir`, `shard-0`, `dump.rdb`,
  `replication.state` — the pre-v0.2.0 default layout), moon keeps using
  it and logs a warning, so upgrades never silently boot with an empty
  keyspace away from their data.
- Explicit `--dir <path>` / conf `dir` (including `--dir .`) opt out of
  auto-resolution entirely; environments with no `HOME`/`LOCALAPPDATA`
  fall back to the current directory with a warning. Docker
  (`--dir /data`) and the systemd package (`dir /var/lib/moon`) already
  pass explicit paths and are unaffected.

### Changed — default shard count is now 1 (was: auto-detect)

- **`--shards` defaults to 1** instead of auto-detecting the CPU count.
  Single-shard gives the best throughput for non-pipelined workloads
  (cross-shard SPSC dispatch dominates local lookups) and a deterministic
  persistence layout across hosts. `--shards 0` remains the explicit
  auto-detect opt-in; nothing changes for deployments that pass `--shards`
  explicitly.
- **Upgrade note:** deployments that previously relied on the auto-detect
  default with `appendonly yes` will refuse to start after upgrading
  (`ERR shard count changed (manifest=N, config=1)`) — this is the
  intended data-loss guard. Start with `--shards <N>` matching the
  manifest, or see the new
  [shard-count-change runbook](docs/runbooks/shard-count-change.md)
  (also linked from the error message itself, now as a full GitHub URL so
  installed binaries point somewhere reachable).

### Fixed — release pipeline verification + prerelease safety

- `release.yml` sign job now publishes Fulcio certificates (`*.crt`)
  alongside signatures: keyless `cosign verify-blob` requires the
  certificate — the previous `.sig`-only output was unverifiable by
  users.
- Docker job no longer moves `ghcr.io/pilotspace/moon:latest` on
  prerelease tags (e.g. `v0.2.0-rc.1`); only stable releases repoint
  `:latest`. The versioned tag is always pushed.

### Changed — v0.2.0 release prep

- Crate version bumped 0.1.12 → 0.2.0; `INFO server` `moon_version` now
  reports 0.2.0 (previously released binaries would have self-reported
  the stale crate version regardless of the git tag).
- `release.yml`: the `homebrew-tap` bump job is gated behind the
  `HOMEBREW_TAP_ENABLED` repository variable. Homebrew distribution is
  deferred — v0.2.0 ships via curl `install.sh`/`install.ps1`, .deb/.rpm,
  and Docker. Without the gate the job would hard-fail on every stable
  tag (missing `HOMEBREW_TAP_TOKEN` secret). `packaging/bump-homebrew.sh`
  and the formula template stay in-tree for later enablement.

### Added — Temporal-decay traversal scoring (agent-memory recency)

User-facing recency bias for graph traversal: paths through recently
created edges win over stale ones. The decay engine (scorers, Dijkstra
composite cost) existed but was unreachable — `shortestPath()` hardcoded
`lambda = 0`. This wires it end to end.

- **`GRAPH.QUERY ... --decay <λ> [--time-weight <w>]`** — per-edge cost
  becomes `|weight| + λ·w·age_seconds` for `shortestPath()`; λ is 1/seconds,
  strictly validated. Decay off (no flag) keeps exact distance-only
  behavior — the age term contributes zero to every edge cost, so path
  choice is identical to pre-decay Moon. Applies to the read-only and
  `GRAPH.PROFILE` paths via `ExecutionContext` (same pattern as
  `VALID_AT`); write queries (CREATE/SET/DELETE/MERGE) reject the flag
  instead of silently ignoring it.
- **`FT.NAVIGATE ... DECAY <λ>`** — graph-expanded hits pay
  `λ × age_seconds` of their discovery edge on top of the hop penalty
  (a re-rank of the already-explored expansion, not a steer of the
  expansion itself); KNN direct hits unaffected.
- **Edges stamp `created_ms` at insert** from the shard-cached clock
  (zero syscall on the insert path). `0 = unknown` is decay-neutral —
  pre-upgrade edges never look maximally old. Distinct from the
  user-owned bi-temporal `valid_from`/`valid_to`.
- **CSR segment format v3** — per-edge `created_ms` array (parallel to
  `col_indices`) survives freeze → disk → mmap → compaction, so decay
  sees true edge age after segments rotate. v1/v2 files keep loading
  (empty stamps = neutral); both parsers (heap + mmap zero-copy) are
  version-gated, plus a new `csr_from_bytes` fuzz target.
- **Fixed (latent durability bug):** `compact_segments` stamped merged
  segments `version: 1` while the serializer always writes 48-byte v2+
  NodeMeta records — a vacuumed segment written to disk misparsed on
  reload (panic in debug, silent node_meta corruption in release).
  Merged segments are now v3 and carry per-edge stamps through dedup.
- Docs: `guides/temporal.mdx` decay section + `commands.mdx`; script
  coverage in `test-commands.sh` (6 DECAY cases) and `test-consistency.sh`
  (1/4/12-shard path-flip parity).
- Known gap: WAL-replayed not-yet-frozen edges re-stamp to replay time
  on restart (newest edges look new — bias direction preserved);
  CSR-resident edges keep exact age.

### Added — Ship moon as an installable application on macOS, Linux, and Windows

Five-PR milestone making moon installable on all three platforms
(consolidated entry; the sibling PRs carry the `skip-changelog` label).

- **Windows native port** — cross-platform positioned-I/O helper
  (`src/util/file_ext.rs`; pwrite/pread on unix, `seek_write`/`seek_read`
  loops on Windows), portable `RawSocketFd` alias with `#[cfg(unix)]`-gated
  connection-migration fd ops, `compile_error!` guard for `jemalloc` on
  Windows MSVC (mimalloc fallback), and Windows-only startup warnings
  (VirtualLock working-set, no memory guardrail). Build:
  `--no-default-features --features runtime-tokio,graph,text-index`.
- **Graceful shutdown on SIGTERM** — `ctrlc` `termination` feature with a
  single centralized handler in `main.rs`; `systemctl stop` / `launchctl
  stop` now flush AOF before exit (previously only SIGINT was caught).
- **Version strings** — `INFO server` reports `redis_version:7.4.0`
  (client feature-gating) plus a new `moon_version:` field from
  `CARGO_PKG_VERSION`; HELLO/LOLWUT report the real moon version
  (previously hardcoded `0.1.0`).
- **moon.conf config file** — redis-style `key value` config
  (`moon /etc/moon/moon.conf` or `--config`), CLI flags override the file;
  commented default shipped as `packaging/moon.conf.example` and installed
  to `/etc/moon/moon.conf` (deb/rpm, `config|noreplace`).
- **Install channels** — `install.sh` (Linux/macOS) and `install.ps1`
  (Windows) one-liner installers with SHA256 verification; Homebrew tap
  automation (`packaging/homebrew/moon.rb.tmpl` + `bump-homebrew.sh`
  publishing to `pilotspace/homebrew-moon` on release).
- **Release pipeline overhaul** — 8-target matrix (linux gnu/musl
  x86_64/aarch64, macOS arm64/Intel, Windows x86_64 MSVC), all release
  binaries now include `graph,text-index,console` (previously missing from
  releases), `prepare-console` pnpm build job, checksums + cosign over all
  artifacts including .deb/.rpm, `--prerelease` for `-rc` tags, and a
  `workflow_dispatch` dry-run mode.
- **Packaging fixes** — nfpm license corrected to Apache-2.0,
  `moon.service` ExecStart path fixed (`/usr/bin/moon`), CI gains
  main-push-only `check-windows` and `check-console` jobs plus a macOS
  Intel cross-build check.
- **Console build fix (PR #152)** — graph components aligned with the
  pinned `@cosmos.gl/graph` 2.6.4 API (`setConfigPartial` → `setConfig`,
  no async `ready` hook); `pnpm run build` type-checks again, unblocking
  the Console Integration workflow and the release `prepare-console` job.
- **RESP3 negotiation fix (PR #153)** — the monoio (default) handler now
  syncs the wire codec when `HELLO 3` negotiates RESP3; previously the
  codec stayed on RESP2 and silently flattened map/set/push frames,
  breaking RESP3 clients on connect (redis-py 8 defaults to RESP3).
- **Windows runtime fixes (PR #154)** — first fully green `check-windows`
  run: fsync helpers reworked for Windows semantics (no directory
  handles, writable handle for `FlushFileBuffers`); unsigned-`Instant`
  underflows fixed (autovacuum scheduling, rate-limiter cutoffs, mvcc
  test anchors); the tokio sharded handler no longer initiates connection
  migration on non-unix platforms (previously aborted clients mid-session
  in multishard workloads); `GraphManifest::save` now propagates
  parent-dir fsync errors.
- **Known limitations (v0.2.0)** — Windows binaries are not
  Authenticode-signed (SmartScreen warning); connection migration is
  unix-only (connections stay on the originating shard on Windows);
  Windows service wrapper + MSI deferred.

### Fixed — CodeRabbit PR #136 durability follow-ups + decomposition + test isolation (PR #144)

Closes the 8 CodeRabbit findings left open after PR #136, plus two PR #144-review
Majors, oversized-file decomposition, and a parallel-test flake. No production
hot-path behaviour change.

- **Disk-offload spill (data-loss fixes):** block instead of dropping spill
  completions; salvage inline-batch spill failures per-entry rather than
  wholesale; preserve spill context across tokio connection migration; recover
  the cold tier under `appendonly=no`.
- **Per-shard AOF rewrite robustness:** clear the rewrite flag when fan-out
  fails partway; roll per-shard rewrite writers back to the committed generation
  on abort (barrier-before-resume + panic-safe `ShardDoneGuard`); ack drained
  `AppendSync` only after the boundary fsync (issue #140 ordering).
- **Async-spill eviction:** corrected a stale doc comment, removed the dead
  remove-first eviction path, and added a regression test locking the fail-safe
  send-before-remove ordering (a full spill channel keeps the victim resident —
  no data loss).
- **Platform hygiene:** gate the migrated-connection spawn fns behind
  `cfg(all(..., unix))` to match their `RawFd` usage.
- **File decomposition (1500-line cap):** split `aof_manifest.rs` (3058 →
  mod/shard_replay/shard_rewrite) and `aof.rs` (4379 → mod/pool/writer_task/
  rewrite); pure code relocation, verified line-exact on both runtimes.
- **Test isolation:** fixed the `VECTOR_INDEXES` counter flake — the process-
  global metrics counter is now guarded by an `RwLock` (delta-reader tests take
  `write()`, mutator tests take `read()`), making the index-count delta
  assertions deterministic under the parallel test harness.

### Persistence — Per-shard AOF migration complete (PR #129)

Closes the P0 multi-shard AOF data-loss bug (~50% loss on SIGKILL with
`--shards >= 2 + --appendonly yes`) by shipping the full per-shard AOF
architecture (Option B of `tmp/rfc-per-shard-aof-v02.md`).

- **H2 closed** — `src/main.rs` no longer skips multi-shard AOF replay.
  Each shard owns its own writer task; recovery walks every shard's segment
  manifest independently. Shard replay is parallel (recovery time does
  not grow linearly with shard count).
- **H1 closed** — new `AppendSync { bytes, ack }` rendezvous variant
  ensures `+OK` is on the wire only after fsync ack under
  `appendfsync=always`. `try_send` (`everysec`/`no`) paths unchanged.
- **`--unsafe-multishard-aof` deprecated** — was the v0.1.13 escape hatch
  acknowledging the ~50% loss risk. The flag is now a no-op that prints
  `[DEPRECATED]` at startup; will be removed in v0.2.0-rc.
- **CRASH-01-LITE matrix** — 200/200 SIGKILL recoveries across
  `--shards 1/2/4/8` × `appendfsync always/everysec`. Gated in
  `.github/workflows/integration-tests.yml`; run locally with
  `cargo test --release crash_01_lite` on the moon-dev OrbStack VM.
- **TopLevel-manifest safety guard** — Moon refuses to start (exit 2) when
  it finds an existing v0.1.13-style TopLevel manifest with `--shards >= 2`,
  to prevent silent data loss from replaying a non-routed log. Migration
  via `docs/runbooks/multi-shard-aof-rewrite.md` Option A.
- **`INFO persistence`** — new field `aof_backpressure_dropped:<N>` exposes
  per-shard writer drop counts; non-zero indicates the AOF writer is
  falling behind write throughput.
- **Per-shard layout on disk:** `appendonlydir/shard-{N}/moon.aof.{seq}.base.rdb`
  + `moon.aof.{seq}.incr.aof` mirrors the per-shard WAL v3 design.

Architectural follow-ups parked for v0.2.0:

- Rule 3 (LSN ordering invariant) under per-shard topology — issue #131
- Always-fsync handler-layer integration audit — issue #132
- `OrderedAcrossShards` merge-replay correctness on large transcripts — issue #133

Per-shard `BGREWRITEAOF` (step 6 of the migration RFC) is **not yet in
this PR**; rewriting a per-shard AOF returns `ERR BGREWRITEAOF is not yet
supported under per-shard AOF layout`. Tracked for v0.2.0.

**Headline capabilities landed in alpha:**

- **Point-in-Time Recovery (PITR)** — `--recovery-target-lsn` /
  `--recovery-target-time` restore to any LSN or wall-clock boundary
  inside the WAL retention window.
- **Change Data Capture (CDC)** — `CDC.READ` polling command with
  Debezium-compatible JSON envelopes, resumable cursors, segment-rotation
  safety.
- **Hash-field TTL** — full Valkey 9.0 / 9.1 surface (`HEXPIRE` /
  `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HEXPIRETIME` / `HPEXPIRETIME`
  / `HTTL` / `HPTTL` / `HPERSIST` / `HGETDEL` / `HGETEX`) with O(1) HGET +
  HLEN fast path. Three-way benchmark vs Redis 8.0.2 / Valkey 9.1.0
  ships in `docs/perf/2026-05-27-hash-ttl-3way-bench.md`.
- **Tier 2 Lane A** — `SWAPDB`, `MOVE`, `COPY ... DB n`,
  `CLUSTER REPLICAS` / `SLAVES`, `CLUSTER COUNT-FAILURE-REPORTS`. All
  WAL-durable with cross-shard atomic semantics.
- **Storage format v1 commitment** — RDB v2 + WAL v3 + multi-part AOF
  manifest grouped under a single `--storage-format v1` umbrella with
  ≥18-month LTS forward-read guarantees.
- **Embedded sharded server** — `server::embedded::run_embedded(config,
  cancel)` exposes the full sharded handler (with `TXN.*`) to in-process
  embedders.

**What is not yet in alpha:** PITR live-snapshot LSN wiring (P3c),
`CDC.SUBSCRIBE` push channel (C3b), and the multi-shard master PSYNC
deferred from v0.1.10. Tracked in `.planning/rfcs/v02-enterprise-architecture.md`.

### Fixed — Multishard idle-RAM blowup + tokio-Linux serving hang + graph parser DoS (PR #136)

Closes the "multishard RAM zombie": a fresh multishard instance with no
`maxmemory` could commit multiple GB of RSS while idle, and the tokio
runtime could hang its accept loop on Linux. Three independent root
causes, all fixed and verified in the OrbStack VM under a cgroup memory
cap (idle RSS **3791 MB → 29 MB** for a 4-shard no-`maxmemory` instance;
~45 MB under load):

- **PageCache eager pre-allocation.** Each shard committed
  `num_frames × PAGE` zeroed bytes at construction, sized to 25% of the
  **whole-instance** `maxmemory` (or a large default when unset). Now the
  page buffers allocate lazily and the frame budget is divided by shard
  count (`per_shard_pagecache_budget`); a startup `WARN` reports the
  resolved per-shard budget.
- **tokio listener bind-race.** The central accept listener bound the port
  *without* `SO_REUSEPORT` while per-shard listeners bound *with* it,
  producing a bind-order race that could leave the port served by a shard
  that never received connections. The central listener now also binds
  `SO_REUSEPORT`; `--per-shard-accept` defaults to `false` under tokio.
- **io_uring-under-tokio default-off.** The tokio→io_uring bridge floods
  errors under load. It is now **opt-in via `MOON_URING=1`**; tokio shards
  run plain epoll/kqueue by default. `MOON_NO_URING=1` still force-disables
  io_uring everywhere. The monoio runtime is unaffected (always io_uring
  unless `MOON_NO_URING`).

Two durability defects found during review were fixed in-PR with
red/green TDD:

- **Manifest compact-reopen failure no longer silently loses commits.**
  `manifest.rs compact()` reopened `self.file` *after* `rename(tmp, path)`;
  if that reopen failed, later commits silently wrote to an orphaned inode.
  A `needs_reopen` flag now reattaches to `self.path` before any subsequent
  commit (or fails loudly).
- **tokio per-shard AOF writer now latches after a torn write.** The tokio
  per-shard writer lacked the `write_error` latch present on the single-file
  and monoio writers; a torn write (header OK, data fails) corrupted the
  frame stream. It now latches on any write failure and acks `WriteFailed`.

- **Cypher parser stack-overflow DoS bounded.** The precedence-climbing
  expression grammar pushes ~9 stack frames per source nesting level but
  `check_depth()` counts only one level per recursion, so the previous
  limit of 64 overflowed a 2 MiB async-worker stack (SIGABRT) **before**
  the guard could fire. `DEFAULT_MAX_NESTING_DEPTH` is now `32`
  (~288 worst-case debug frames, ~50% margin); deep nesting returns
  `NestingDepthExceeded` gracefully.

Low-severity durability edge cases filed as follow-ups: #137
(`apply_spill_completions` failed-commit cold-key window), #138
(`do_rewrite_per_shard` panic wedges `--experimental-per-shard-rewrite`),
#139 (multi-DB `SELECT >0` cold recovery restores db0 only).

### Fixed — `maxmemory` is now a whole-instance cap across shards (G2)

**Behavior change for multishard deployments.** Previously each shard
enforced eviction against the *full* `maxmemory`, so an N-shard server
tolerated ~N× the configured cap before evicting (a 4-shard server at
`--maxmemory 100mb` retained ~307 MB / 768K keys vs a 1-shard server's
124 MB / 322K — the "RAM keeps growing in multishard mode" report).

`maxmemory` is now a true **whole-instance** cap. Each shard enforces
eviction against `maxmemory / num_shards`, so aggregate RSS converges on
the configured value regardless of shard count.

- **`CONFIG GET maxmemory` / INFO are unchanged** — they report the
  whole-instance value verbatim (Redis-compatible). Division happens only
  at enforcement.
- **Operators running explicit `--maxmemory N --shards M (M>1)`** now get
  an effective ceiling of `N` (not `N×M`). A startup log line states the
  resolved per-shard budget so the change is visible:
  `maxmemory <N> bytes is a whole-instance cap; each of <M> shards enforces
  eviction against a per-shard budget of <N/M> bytes`.
- Single-shard servers are byte-for-byte unaffected (`num_shards == 1` ⇒
  no division).

### Docs — Hash-field TTL three-way benchmark suite (PR #127)

- `scripts/bench-hash-ttl.sh` (2-way harness) + `scripts/bench-hash-ttl-3way.sh`
  (3-way harness with Valkey 9.1.0). Both use `redis-benchmark` against
  concurrent Moon / Redis / Valkey servers on distinct ports with trap-based
  cleanup, FLUSHALL + re-seed between scenarios, and median-of-3 RPS reporting.
- `docs/perf/2026-05-26-hash-ttl-bench.md` — pre-fix 2-way baseline that
  surfaced the two HashWithTtl perf issues fixed in PR #126.
- `docs/perf/2026-05-27-hash-ttl-3way-bench.md` — headline Moon vs Redis 8.0.2
  vs Valkey 9.1.0 comparison across 26 scenarios. Plain HGET p=16 ties both
  competitors (1.00–1.01×). HEXPIRE-family Moon vs Valkey: 0.90–0.99× across
  the surface; HGETEX hits parity at 0.99×. Redis 8.x has no HEXPIRE-family —
  Moon is the only Redis-compatible alternative aside from Valkey.

### Performance — HashWithTtl HGET + HLEN O(1) fast path (PR #126)

Resolves the two HashWithTtl perf issues surfaced by the 2026-05-26 bench:

- `HGET` on `HashWithTtl` was 39.9% slower than plain `Hash` at p=16; now
  **+4% faster** (within VM measurement noise — effective parity).
- `HLEN` on `HashWithTtl` was 80.5× slower than plain `Hash` at 1000 fields;
  now **1.00–1.03× parity** (the O(N) live-count scan is fully eliminated when
  no field has expired).

Two changes shipped together (variant layout forces both at once):

- **`HashWithTtl.ttls` BTreeMap → HashMap.** Per-field TTL probe becomes O(1)
  HashMap lookup instead of O(log N) BTreeMap descent. Active-expire iteration
  doesn't require ordered keys.
- **`HashWithTtl.min_expiry_ms: u64` cached minimum.** Tracks the smallest
  expiry across all per-field TTLs. Invariant: `min_expiry_ms = min(ttls.values())`.
  When `cached_now_ms < min_expiry_ms`, no field can be expired, so `HGET`
  skips the `ttls` probe and `HLEN` returns `fields.len()` directly. The hot
  path is a single `u64 < u64` compare. Invariant maintenance is amortized
  O(1) (one `min(min, ts_ms)` per `HEXPIRE`; conditional recompute on
  `HPERSIST` / overwrite / active reap when the removed TTL equalled the min).

No on-disk format change. `min_expiry_ms` is recomputed at load time from the
existing v2 RDB per-field TTL trailer. 6 new invariant tests cover HEXPIRE /
HPERSIST / HSET-overwrite / active-reap / persistence-decode paths.

### Docs — Hash-field TTL audit follow-up (PR #123)

- `docs/commands.mdx` — Hashes section bumped from "(14)" → "(25)" and now
  lists all 11 new Valkey 9.0/9.1 commands plus a paragraph on the per-field
  return convention and the active-expiry downgrade behaviour.
- `docs/STORAGE-FORMAT-V1.md` §3.2 — added the per-field hash TTL trailer
  format: every `TYPE_HASH` body is followed by
  `[ttl_count u32][field_len varint | field_bytes | ttl_ms u64]*` in v2 RDB
  files; v1 readers stop after the hash body.
- `src/command/metadata.rs` — `HEXPIRETIME` / `HPEXPIRETIME` / `HTTL` /
  `HPTTL` PHF flags flipped from `R` (READONLY) to `RF` (READONLY|FAST) —
  per-field TTL lookup is O(1) thanks to the PR #126 fast path. Cosmetic;
  no behavioural impact.

### Added — HGETDEL / HGETEX atomic compound hash commands (phase 199, issue #110)

Two new Valkey 9.1 atomic compound hash commands:

- `HGETDEL key FIELDS numfields field [field ...]` — returns the values of
  the specified fields and deletes them from the hash atomically. Returns a
  RESP Array with one `BulkString(value)` per found field and `Null` for
  missing fields. If the hash becomes empty after all deletes the key is
  removed entirely (auto-cleanup).

- `HGETEX key [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] FIELDS numfields field [field ...]`
  — returns the values of the specified fields and optionally updates (or
  removes) their per-field TTLs atomically. TTL modes:
  - `EX s`         — set relative expiry in **seconds** from now.
  - `PX ms`        — set relative expiry in **milliseconds** from now.
  - `EXAT unix-s`  — set absolute expiry as unix **seconds**.
  - `PXAT unix-ms` — set absolute expiry as unix **milliseconds**.
  - `PERSIST`      — remove any existing per-field TTL.
  - (no mode)      — pure read; no TTL change (fast path, zero DB mutation).
  TTL changes apply only to live (non-expired) fields. Missing / expired
  fields return `Null` and leave TTLs untouched.

Atomicity: per-shard single-threaded execution gives atomicity for free
across the entire field list — no client can observe a partial state between
reads and deletes/TTL-updates within a single call.

Implementation:
- Two new `Database` primitives in `src/storage/db.rs`:
  - `hash_get_and_delete_field` — atomically reads and removes a single
    field; handles Hash, HashListpack, and HashWithTtl (also removes TTL
    sidecar). Downgrades HashWithTtl → Hash when the last TTL is removed.
  - `cleanup_empty_hash` — removes the key when its hash has become empty;
    called once after a HGETDEL/HGETEX field loop.
- `HGETDEL` handler uses `parse_key_and_fields` (phase-198 shared parser)
  plus a single `cleanup_empty_hash` call after the field loop.
- `HGETEX` parser (`parse_hgetex_args`) scans the optional mode token(s)
  before `FIELDS` with mutual-exclusion enforcement; uses saturating i128
  arithmetic + u64 clamp for safe overflow handling (mirrors phase 196).
- Both commands dispatch as writes (WF flags, `&mut Database`); neither is
  added to `is_dispatch_read_supported`.
- 17 new unit tests: 8 for HGETDEL, 9 for HGETEX.

### Added — Hash-field TTL read + persist (phase 198, issue #109)

Five new Valkey 9.0 hash-field TTL commands:

- `HEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **seconds**.
- `HPEXPIRETIME key FIELDS numfields field [field ...]` — absolute expiry
  per field as a unix timestamp in **milliseconds**.
- `HTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **seconds**; already-expired-but-not-reaped fields return `0`.
- `HPTTL key FIELDS numfields field [field ...]` — remaining TTL per field
  in **milliseconds**; same `0` edge-case for expired-but-not-reaped.
- `HPERSIST key FIELDS numfields field [field ...]` — removes the per-field
  TTL; downgrades `HashWithTtl` back to plain `Hash` when the last TTL is
  removed (handled by the phase-195 `hash_persist_field` primitive).

Per-field return codes (Valkey 9.0):
- `-2` — field does not exist (or key is missing — **not** a WRONGTYPE error)
- `-1` — field exists but has no TTL
- `≥0` — absolute unix time or remaining duration (HEXPIRETIME/HPEXPIRETIME)
- `1` — TTL successfully removed (HPERSIST only)

WRONGTYPE is returned immediately (before field iteration) when the key
holds a non-hash value.

Implementation:
- `FieldState` tri-state enum and `hash_field_state` helper (pre-landed in
  phase 197) provide the zero-allocation field-state read used by all five
  commands.
- `parse_key_and_fields` shared parser (`hash_write.rs`) extracts
  `key FIELDS numfields field [field ...]`; reuses `SmallVec<[&[u8]; 4]>`
  to avoid heap allocation for the common ≤4-field case.
- Four read handlers (`HEXPIRETIME`, `HPEXPIRETIME`, `HTTL`, `HPTTL`) take
  `&Database`; `HPERSIST` takes `&mut Database`.
- All five commands routed in both `dispatch()` (mutable path) and
  `dispatch_read_inner()` / `is_dispatch_read_supported()` (shared-read
  path) for the four read commands.
- 14 unit tests cover all return-code variants, the already-expired edge
  case, WRONGTYPE, missing key, numfields=0, and encoding downgrade.

### Added — Hash-field active expiration (phase 197, issue #108)

All 9 hash read commands (`HGET`, `HMGET`, `HGETALL`, `HEXISTS`, `HLEN`,
`HKEYS`, `HVALS`, `HSCAN`, `HRANDFIELD`) now respect per-field TTLs set by
the `HEXPIRE` family. Expired fields are invisible to callers without
requiring the active-expiry tick to have run first (lazy expiry).

**Lazy expiry** (read path, `&Database`): `HashRef` gains a third variant
`WithTtl { fields, ttls, now_ms }` that filters expired fields on every
field-level operation. The `get_hash_ref_if_alive` accessor now returns this
variant for `HashWithTtl` entries instead of falling through to `WRONGTYPE`.
All 9 mutable read commands now call `get_hash_ref_if_alive` instead of the
unfiltered `get_hash`, so expired fields are never returned.

**Active expiry** (tick path, `&mut Database`): the per-shard `expire_cycle`
gains a second sweep via `Database::hashes_with_field_expiry()` and
`Database::reap_expired_fields_one_hash()`. The reaper removes expired
fields from the `fields` and `ttls` maps, downgrades the hash back to plain
`Hash` when the last TTL sidecar entry is drained, and signals `KeyDeleted`
when all fields expire (the key is then removed by the caller).

The `maybe_has_expiring_keys` fast-path flag is now cleared only when
**both** the whole-key sweep and the hash-field sweep return empty, preventing
premature flag-clearing that would have silenced future field reaping.

**Complexity change**: `HLEN` is now O(N) for `HashWithTtl` hashes (counts
only live fields). Plain `Hash` and `HashListpack` remain O(1) / O(N)
respectively — no regression.

### Added — HEXPIRE-family write commands (phase 196, issue #107)

- `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]`
- `HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HEXPIREAT key unix-seconds [NX|XX|GT|LT] FIELDS numfields field [...]`
- `HPEXPIREAT key unix-ms [NX|XX|GT|LT] FIELDS numfields field [...]`

Per-field return codes match Valkey 9.0: `0` (no such field), `1` (TTL set or
updated), `2` (field expired during this call and was deleted), `-2` (NX / XX /
GT / LT condition not met). Wrong-type key returns `WRONGTYPE`; missing key
returns one `0` per requested field. PHF + dispatch routes the four commands
to `hash::{hexpire,hpexpire,hexpireat,hpexpireat}`; AOF replay already handled
by the phase-200 intercepts.

Side fix — HashWithTtl-aware hash-write path: `HSET`, `HMSET`, `HSETNX`,
`HDEL`, `HINCRBY`, and `HINCRBYFLOAT` previously returned `WRONGTYPE` once
`HEXPIRE` had promoted a hash to the `HashWithTtl` encoding. Extended
`Database::get_or_create_hash` / `get_or_create_hash_listpack` to handle
`HashWithTtl` (returning the inner `fields` map / `Ok(None)` respectively).
Added `Database::hash_clear_field_ttls` (called by HSET/HMSET on overwrite,
no-op on plain `Hash`) and `Database::hash_delete_field` (used by HDEL —
removes from both the fields map and the per-field TTL sidecar, downgrades
back to plain `Hash` when the last TTL is dropped). HSETNX correctly leaves
existing TTLs intact; HINCRBY / HINCRBYFLOAT preserve TTL on the incremented
field.

### Fixed — Test infra: txn_kv_wiring flake diagnosis

- `test_txn_commit_wal_crash_recovery` previously masked moon-server crashes
  as "Connection refused (os error 61) after 60s" because the spawned moon
  binary's stdout / stderr were piped to `Stdio::null()`. Hardened the
  child-process harness:
  - `ChildGuard::spawn` now redirects child stdout/stderr to
    `moon-phase-{1,2}.log` inside the per-test temp dir.
  - `ChildGuard::poll_exit` checks `try_wait()` inside the connection retry
    loop — if the child exited, the test fails immediately with the exit
    status instead of timing out for a useless 60 s.
  - `ChildGuard::dump_log` is called on every failure path
    (`wait_for_server` timeout, `connect_redis_with_retry` timeout,
    child crash), so the CI log records the actual server output.
  - `wait_for_server` deadline widened from 5 s → 15 s, matching the
    realistic CI-runner spawn + WAL boot envelope.
- No semantic change for the happy path. Future flakes become diagnosable
  instead of silently retrying for 60 s.

### Docs — Storage Format v1 commitment (phase 192, PR #115)

- `docs/STORAGE-FORMAT-V1.md` — public on-disk format contract for v0.2.x.
  Documents the WAL v3 / RDB v2 / AOF multi-part sub-formats as a single
  "storage format v1" umbrella with explicit forward-read, reverse-read,
  crash-recovery, and migration guarantees through ≥18 months of LTS.
  Adds cross-reference doc-comments to `src/persistence/aof_manifest.rs`,
  `snapshot.rs`, and `wal_v3/segment.rs` pointing readers at the canonical
  on-disk markers. Reserves a `--storage-format <v1>` CLI flag for the
  follow-up code PR closing issue #103's second checkbox.

### Added — Hash-Field TTL primitive (phase 195, PR #116)

- `RedisValue::HashWithTtl { fields, ttls }` storage variant + borrowed
  `RedisValueRef::HashWithTtl` view. Per-field TTL sidecar (`BTreeMap<Bytes, u64>`)
  carries absolute unix-ms expiry alongside an unchanged `HashMap<Bytes, Bytes>`
  field map. `OBJECT ENCODING` reports `hashtable`.
- `Database::hash_set_field_ttl(key, field, abs_ms, cond)` — Valkey 9.0 parity
  result codes (0 missing / 1 set / 2 deleted-on-set / -2 cond not met).
  NX / XX / GT / LT semantics matching Valkey (non-volatile is +∞ for GT/LT).
  Auto-promotes `Hash` / `HashListpack` → `HashWithTtl` on first per-field TTL;
  past-expiry short-circuits to in-place delete with code 2.
- `Database::hash_persist_field(key, field)` — clears one field's TTL;
  downgrades back to plain `Hash` when the last TTL is removed.
- `Database::hash_get_field_ttl_ms` + `Database::hash_field_state` — read
  helpers returning the tri-state `FieldState::{Missing, NoTtl, Ttl(u64)}`
  consumed by phase-198 HTTL / HEXPIRETIME / HPTTL / HPEXPIRETIME read commands.
- Additive `HashWithTtl` match arms in `command::key::should_async_drop`,
  `server_admin::estimate_serialized_length`, `eviction::evict_one_async_spill`,
  `tiered::kv_spill`, `tiered::kv_serde`, `persistence::rdb::write_entry`,
  `persistence::redis_rdb`, and `persistence::aof::generate_rewrite_commands`.
  Persistence-side TTL payload lands in PR #117 (phase 200); arms here are
  TTL-stripping placeholders so HEXPIRE handlers (phase 196) cannot be
  merged ahead of the persistence wiring.

### Added — Hash-Field TTL persistence wiring (phase 200, PR #117)

- **RDB v2** — bumped `RDB_VERSION 1 → 2`. New per-hash trailer
  `[ttl_count u32][field, ttl_ms u64]*` follows every `TYPE_HASH` body
  (count=0 for non-TTL hashes). Reader accepts both v1 and v2 files;
  `count_entries_per_db` / `skip_entry` / `read_entry_zero_copy` /
  `read_entry` all plumb a `has_hash_ttl_trailer` flag so the format
  is parsed correctly on both code paths (file load + in-memory bytes).
- **Shard RDB V3** — `SHARD_RDB_VERSION 2 → 3` with the same trailer
  plumbed through `rdb::read_entry`. V1 (legacy) / V2 (PITR LSN+ts) /
  V3 (TTL trailer) all load via per-version preamble + min-file-size
  branching.
- **Tiered KV serde** — per-hash trailer on disk-offload blobs. Plain
  Hash + HashListpack serializers now append `ttl_count = 0`;
  HashWithTtl serializer writes the real trailer. Deserializer treats
  a truncated trailer as zero TTLs for graceful migration from
  pre-trailer in-process spill blobs.
- **BGREWRITEAOF** — `RedisValueRef::HashWithTtl` arm emits
  `HSET key f1 v1 f2 v2 ...` followed by per-field
  `HPEXPIREAT key abs_ms FIELDS 1 field`, one per TTL'd field. Per-
  field framing keeps the replay shim simple (single-field parse).
- **Replay shim** — `CommandReplayEngine::replay_command` intercepts
  `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HPERSIST`
  (case-insensitive) **before** `command::dispatch` and routes
  directly to `Database::hash_set_field_ttl` / `hash_persist_field`.
  This bypasses the phase-196 command handlers (which do not yet
  exist) so crash-restart restores per-field TTLs from any AOF stream
  emitted by either user-typed HEXPIRE or BGREWRITEAOF.
- **Redis-compat RDB** — emits `tracing::warn!` when dropping per-field
  TTLs on Redis-compat export. Redis 7.4 hash-field-TTL opcode
  emission deferred to a future cross-vendor compat phase.

### Added — Tier 2 Lane A (PR #100)

- **T2.1** `c381b31` — `SWAPDB` cross-shard atomic swap via `ShardMessage::SwapDb`;
  WAL-durable; BGREWRITEAOF concurrency guard; restart-replay test.
- **T2.2** `4958dc9` — `MOVE key db` with `with_two_dbs_locked` (lower-index-first
  lock ordering); WAL-durable; intercept in all four handler paths.
- **T2.3** `bbc6117` — `COPY ... DB n` cross-database; reuses `with_two_dbs_locked`;
  WAL-durable.
- **T2.4** `f538589` — `CLUSTER REPLICAS` / `CLUSTER SLAVES`; shared
  `format_node_line(node, self_node_id)` helper extracted from `CLUSTER NODES`.
- **T2.5** `ebd240a` — `CLUSTER COUNT-FAILURE-REPORTS`; counts non-stale
  `pfail_reports`; exposes `DEFAULT_NODE_TIMEOUT_MS` as `pub(crate)`.

### Fixed

- **PERF** `608e2d1` — collapse duplicate `is_write` PHF gate on MOVE/COPY hot
  path; restores s=1 SET p=1 throughput (−9.5 % → +0.9 % vs. merge base).
- **CR** _(this PR)_ — SWAPDB now runs **after** the ACL gate in `handler_monoio`,
  closing a runtime-specific authorization bypass.
- **CR** _(this PR)_ — `with_two_dbs_locked` and `ShardDatabases::swap_dbs` now
  hard-assert non-equal indices in release builds, preventing same-index
  self-deadlock.
- **CR** _(this PR)_ — `SWAPDB` strict arity (exactly two args) across all three
  handlers; rejects `SWAPDB 0 1 extra` with the canonical wrong-arity error.
- **CR** _(this PR)_ — `DashTable::Segment::insert_or_update_at` now sets
  `has_non_home_keys = true` whenever the chosen free slot is in a non-home
  group; fixes a latent miss where `find()` could not locate a fallback-placed
  key on subsequent lookups.
- **CR** _(this PR)_ — Local `MOVE` / `COPY` AOF append is gated on
  `Frame::Integer(1)` (success) rather than `!Error`, matching the
  `handler_single` behavior and suppressing no-op `:0` log entries.
- **CR** _(this PR)_ — `MOVE`, `COPY ... DB n`, and `SWAPDB` are rejected with
  `ERR_TXN_CROSS_SHARD` while an `active_cross_txn` is in flight; previously the
  intercepts bypassed undo/intents bookkeeping and escaped `TXN.ABORT` rollback.
- **CR** _(this PR)_ — `spsc_handler` `MOVE`/`COPY` arms call
  `refresh_now_from_cache` on both source and destination DBs before
  `move_core`/`copy_core`; fixes expired-key visibility skew on the local-write
  path.

### Refactor

- `e429b2b` — `src/storage/dashtable/segment.rs` (1587 LOC) split into
  `segment/{mod,find,insert,ops}.rs`; mechanical refactor, zero semantic change,
  brings all files under the 1500-LOC limit ahead of future hot-path additions.

### Added — Point-in-Time Recovery (PITR)

- **P0** `ac3aa92` — `WalWriterV3::new()` now scans existing `.wal` segments on
  open and resumes `next_lsn` from `max_observed_lsn + 1`. Fixes a latent
  durability bug where LSN reset to 1 on every restart and blocked both PITR
  and CDC.
- **P1** `e1e9bda` — `FileEntry` extended with `last_modified_lsn` (offset
  48..56, struct size 48 → 56). Manifest `format_version` bumped to v2;
  backward-compat reader synthesizes `last_modified_lsn = created_lsn` for v1
  entries. New CLI flags `--recovery-target-lsn` and `--recovery-target-time`.
- **P2** `25ece4b` — Snapshot header bumped to `SHARD_RDB_VERSION = 2`,
  embedding `last_lsn` and `created_at_unix_ms`. v1 snapshots load with
  `last_lsn = 0` and are conservatively skipped by PITR. Adds
  `read_snapshot_metadata` peek API.
- **P3a** `048a883` — `replay_wal_v3_dir_until(stop_at_lsn)` and
  `resolve_target_time_to_lsn()` (scans `TemporalUpsert` / `GraphTemporal`
  records for `system_from` anchors).
- **P3b** `a496413` — `recover_shard_v3_pitr()` honors `target_lsn`: skips
  snapshots whose `last_lsn` is unknown or past the target, then stops replay
  at the cutoff without advancing `wal_flush_lsn`.

See `docs/guides/pitr.md` for operator usage.

### Added — Change Data Capture (CDC)

- **C1** `b271e21` — `WalTailReader` with resumable `TailCursor { segment_seq,
  byte_offset, last_lsn }`. Re-stats segment metadata on each call for
  torn-write safety, auto-advances on segment rotation.
- **C2** `e97b80d` — New `src/cdc/` module with typed `CdcEvent` enum,
  `decode_wal_record()` translator, and hand-rolled
  Debezium-compatible JSON envelope serializer
  (`encode_debezium`). Non-UTF-8 keys fall back to `{"_b64":"..."}`.
- **C3 v1** `bf4230b` — `CDC.READ <wal_dir> <from_lsn> [LIMIT N]` polling
  command. Returns RESP array `[next_lsn, env1, env2, ...]`; default LIMIT
  256, hard ceiling 10 000. Idle response is `[from_lsn]` (length 1) — stable
  no-new-data signal.

See `docs/guides/cdc.md` for consumer integration.

### Added — Embedded sharded server

- **PR #95** — `server::embedded::run_embedded(config, cancel)` exposes the
  full sharded handler (with TXN.* cross-store transactions) to in-process
  embedders such as `helios moon-daemon`. The existing `run_with_shutdown`
  drives `handler_single`, which deliberately does not implement TXN.
  Embedded mode skips TLS, console, cluster bus, admin port, and multi-part
  AOF manifest replay; it does include per-shard RDB + WAL recovery,
  graph/temporal/workspace/MQ WAL replay, SO_REUSEPORT, NUMA pinning, and
  cancel-driven graceful shutdown.

### Fixed — PR #96 test deflake + tokio AOF replay

- `main.rs` AOF recovery: gated the multi-part AOF manifest replay block to
  `#[cfg(feature = "runtime-monoio")]`. Under tokio, the legacy single-file
  `appendonly.aof` is loaded via the v2 recovery chain; the multi-part loader
  no longer creates an empty manifest at first boot that wiped v2-loaded
  state on the next restart (every tokio SET was lost on restart).
- `tests/txn_kv_wiring.rs`: made `test_txn_commit_wal_crash_recovery`
  runtime-agnostic by polling for either the monoio multi-part `.base.rdb`
  artifact or the tokio single-file `appendonly.aof`. Added
  `connect_redis_with_retry` helper to bound and retry the post-bind RESP
  handshake (was racing the shard accept loop, surfacing as EAGAIN on Linux
  and ECONNRESET on macOS CI).

### Fixed — PR #95 review hardening

- `main.rs` `malloc_conf` symbol: replaced the union-based unsafe pun with
  a `#[repr(transparent)]` `Sync` wrapper around a `c"..."` literal.
- `command/server_admin.rs` `get_vsz_bytes`: replaced four `unsafe`
  libc::{open,read,close,sysconf} blocks with safe `/proc/self/status`
  parsing on the cold MEMORY DOCTOR path.
- `main.rs` arena scan now uses `env::args_os()` so non-UTF-8 argv no
  longer panics before clap reports the error.
- `server/embedded.rs` shutdown sequence: cancel → join shard threads →
  drop the outer `aof_tx` → join the AOF thread, so the writer never
  exits while shards are still queuing appends. Thread join panics are
  now propagated through the function result.
- `storage/db.rs` annotated two `.expect()` calls in `Database::set`'s
  `insert_or_update` closures for the hot-path unwrap ratchet.

### Deferred to v0.2 follow-ups

- **P3c** — wire `SnapshotState::set_last_lsn(wal_flush_lsn)` into the live
  persistence tick so freshly-written snapshots embed their LSN (currently
  PITR falls back to full WAL replay when only v1-shaped snapshots exist).
- **C3b** — push-based `CDC.SUBSCRIBE` over RESP3 Push frames, per-shard
  subscriber registry hooked into `wal_append_and_fanout`, slow-consumer
  disconnect policy. Envelope format unchanged.
- Integration suites (`tests/pitr_integration.rs`,
  `tests/cdc_integration.rs`), `scripts/test-pitr-cdc.sh` end-to-end smoke,
  and the benchmark gates (PITR restart ±10%, CDC ≥100K events/s/shard,
  write p99 ±5%).

## [0.1.12] — 2026-05-12

Performance & memory observability release. 50 commits since v0.1.11, no
public API breaks, no on-disk format change. Validated on OrbStack `moon-dev`
(2026-05-12) and locally green for both `runtime-monoio` and
`runtime-tokio,jemalloc`.

### Performance — DashTable hot-path (Phase 189, PERF-07 + PERF-09)

- **Pre-sized DashTable.** `DashTable::with_capacity()` plus the new
  `--initial-keyspace-hint <N>` flag size the segment array up front so
  steady-state operation hits zero `split_segment` calls. Pre-size
  invariant test confirms zero splits at 1 M keys. The 27 % CPU spent
  in `split_segment` during SET p=16 (PERF-07) is fully eliminated.
- **`Database::set` rewrite.** New `DashTable::insert_or_update` /
  `Segment::insert_or_update_at` single-probe helpers replace the
  previous `find + remove + insert` triple-probe pattern.
- **`Segment::find` fallback elimination + force-inlined SIMD.** The
  cold "key spilled to non-home group" fallback path is removed once
  `has_non_home_keys` is invariant-tracked on insert (the
  `insert_or_update_at` change above already maintains the flag);
  the SIMD probe helpers are `#[inline(always)]`. PERF-09
  attributed 12.65 % of `Segment::find` self-time to the fallback;
  remaining cost is the irreducible per-hit `memcmp` confirm
  (threshold amended to <3 %). 1 M-key correctness gate validates
  zero false positives/negatives.

### Performance — Memory observability (Phase 190)

- **`moon_memory_bytes{kind=…}` Prometheus gauge.** Seven subsystem
  labels — `dashtable`, `hnsw`, `csr`, `wal`, `sealed_replication_backlog`,
  `allocator_overhead`, and the rolled-up `total`. Updated every
  scrape via a single hook so the sum reconciles to `RSS` within the
  CI tolerance window.
- **`MEMORY DOCTOR` full schema.** Multi-line RESP response covering
  every subsystem, the rolled-up total, and a derived `allocator_overhead`
  pseudo-kind (RSS − Σ subsystems). Adds operator triage signal beyond
  the legacy single-line summary.
- **`resident_bytes()` trait** implemented across `Database`,
  `DashTable`, `VectorStore` (HNSW + IVF), `GraphStore` (CSR + SlotMap),
  `WalWriter`, `ReplicationBacklog` (sealed-segment side), and
  `AllocatorOverhead`. Zero-allocation, on-demand poll.
- **Memory steady-state CI job.** `scripts/bench-memory-steady-state.sh`
  + baseline fixture; gate widened to `±10 %` on RSS / Σ ratio after a
  Linux-CI tolerance pass.

### Changed — Allocator UX (Phase 191)

- **jemalloc `narenas:8` cap** with `--memory-arenas-cap <N>` CLI
  override. Caps the per-CPU arena explosion that inflates VSZ on
  high-core hosts; mostly a cosmetic fix on Linux containers but
  produces a meaningfully tighter `top`/`ps` reading for operators.
- **Tri-state allocator selection.** New `mimalloc-alt` cargo
  feature alongside the existing `jemalloc` / `mimalloc` (fallback)
  paths; mutually exclusive at compile time. A/B benchmark script
  `scripts/bench-allocator-ab.sh` ships with the release.
- **`docs/OPERATOR-GUIDE.md` — Memory Accounting section.** Documents
  the VSZ-vs-RSS distinction, MEMORY DOCTOR field-by-field, and the
  `--memory-arenas-cap` / `mimalloc-alt` tuning knobs.

### Added — Dispatch Observability (Phase 177)

- **`moon_dispatch_path_total{path=...}` Prometheus counter**: four-way classification of every command by shard-routing decision — `local_inline` (SIMD fast path), `local` (standard local branch), `cross_read_fast` (RwLock shared-read bypass of SPSC), `cross_spsc` (deferred cross-shard write via `PipelineBatchSlotted`). Ratio `cross_spsc / Σ` is the ground-truth signal for dispatch-layer optimization work. Zero-allocation hot-path overhead (`&'static str` labels, `#[inline]` with early-return on `!METRICS_INITIALIZED`). Verified on macOS + Linux: counter sums close exactly to driven traffic, no overcount.

### Changed

- **`text-index` is now a default feature.** BM25 full-text search (`FT.SEARCH` BM25 mode), `FT.AGGREGATE`, and three-way RRF hybrid fusion are included in all standard builds. No longer requires `--features text-index`. To exclude it (e.g. minimal embedded builds): `--no-default-features --features runtime-monoio,jemalloc,graph`.

### Added — SDK Validation

- **Python SDK `sdk/python/examples/validate.py`**: End-to-end live validator for all SDK sub-clients: ping, strings, counter, hash, list, set, zset, vector index lifecycle, graph engine, session search, semantic cache, text search (BM25 + aggregate + hybrid), and server info. Result against Moon with `text-index`: **114 PASS / 0 FAIL / 0 SKIP**. Gracefully skips text sections when server built without `text-index`.
- **Rust SDK `sdk/rust/examples/validate.rs`**: Re-validated against `text-index` build — **85 PASS / 0 FAIL**.

### Fixed — Python SDK

- **`moondb.graph._parse_neighbors`**: server returns alternating `[edge_map, node_map, ...]` as flat key-value arrays (`b'id'`, `int`, `b'src'`, `int`, `b'dst'`, `int`, …). Previous parser expected positional `[node_id, label, props]` — caused `int() on b'id'` crash. Now correctly identifies node entries by `labels` key and parses them from the flat kv format.

### Fixed — CI Hygiene

- **`tests/pipeline_auto_index.rs`**: tighten outer cfg from `runtime-tokio` to `all(runtime-tokio, text-index)` so the file compiles to zero tests when text-index is disabled. Previously the file compiled but the FT.SEARCH text fast path was `#[cfg]`-ed out, causing `@name:corpus` queries to fall through to the KNN-only parser and panic with "invalid KNN query syntax".
- **4 FT unwraps**: add inline `#[allow(clippy::unwrap_used)]` with invariant justifications in `vector_search/ft_text_search.rs` (3 sites inside `apply_post_processing` where `do_summarize` / `do_highlight` implies the Option is Some) and `handler_monoio/ft.rs:165` (`is_text` was derived from `query_bytes.as_ref().map_or(false, _)`). Restores the audit-unwrap baseline to 0.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.11.
- **Persistence on-disk format**: unchanged.
- **Default feature set**: `text-index` is now on by default. Minimal
  embedded builds need an explicit `--no-default-features --features
  runtime-monoio,jemalloc,graph`.

## [0.1.11] — 2026-04-27

Hot-path perf release — eliminates two atomic-CAS hot paths in the write
dispatch loop discovered via ARM `perf annotate` on c4a-16 (GCloud Axion).
Empirically validated on the same hardware: **8-shard SET p=64 c=200
throughput 1.84M → 3.87M RPS (+110%)** when run with `--disk-offload disable`,
or **+15% under default flags**. No public API change.

Sprint 3.5a and 3.5b from `.planning/rfcs/v02-enterprise-architecture.md`.

### Performance — Sprint 3.5a: Lock-free `is_replica` mirror

`try_enforce_readonly` was taking `RwLock::try_read()` on
`Arc<RwLock<ReplicationState>>` for every command before dispatch — an
atomic CAS on the per-command hot path. ARM annotate showed `mov w8, #0xfffd;
cmp w11, w9` consuming **84% of self-time inside the function** (10% of
total CPU on 8-shard SET p=64).

Fix: replace the per-command lock probe with a single
`AtomicBool::load(Acquire)`.

- **`ReplicationState::is_replica_mirror: Arc<AtomicBool>`** — lock-free mirror
  of `role == Replica { .. }`, kept in sync via the new
  `ReplicationState::set_role(&mut self, role)` method (single owner of the
  invariant).
- **`ConnectionContext::is_replica_mirror: Option<Arc<AtomicBool>>`** —
  snapshotted from `ReplicationState` once at connection setup; per-command
  `try_enforce_readonly` is now just an atomic load with no lock
  acquisition.
- **All 6 production `rs_guard.role = ...` sites** in `handler_single`,
  `handler_monoio/dispatch`, and `handler_sharded/dispatch` migrated to
  `set_role()`. Test fixtures in `replication/handshake.rs` migrated too so
  the invariant holds in test code.
- Round-5 verification on commit `32f48c4` (c4a-16, 8-shard SET p=64 c=200):
  `try_enforce_readonly` is now **0%** of profile (down from 10%). Sprint
  3 acceptance criterion `<1%` met.

### Performance — Sprint 3.5b: WAL no-op bypass

`wal_append_and_fanout` was acquiring a `parking_lot::Mutex` (replication
backlog) and a `std::sync::RwLock` (replication state) on every write,
even when no replica was connected and no WAL writer existed. ARM annotate
showed `caslb`/`casab` ARM CAS-byte atomics dominating self-time (~21% of
total CPU on 8-shard SET p=64 with `--appendonly no` and zero replicas).

Fix: hoist a single early-return at the top of the function:

```rust
if wal_writer.is_none() && wal_v3_writer.is_none() && replica_txs.is_empty() {
    return;
}
```

The criterion is fully derivable from existing inputs — no new shared
state. Skips both the backlog `Mutex::lock` and the `repl_state`
`RwLock::read` on the cold path.

- **Round-5 verification with `--disk-offload disable`** (commit `32f48c4`):
  `wal_append_and_fanout` is now **0.05%** of profile (down from 21%).
  Sprint 3 acceptance criterion `<2%` met.
- **Operator note**: the bypass only fires when (a) `--appendonly no`,
  (b) `--disk-offload disable`, AND (c) no replicas are connected. Default
  builds with disk-offload on (the production default) keep
  `wal_v3_writer = Some(_)` and the function continues to do real WAL v3
  work — that path is unaffected.

### Throughput Impact

| Workload (8-shard SET p=64 c=200, c4a-16, frame pointers ON) | v0.1.10 | v0.1.11 | Δ |
|---|---|---|---|
| Default flags | 1.84M RPS | **2.11M RPS** | **+15%** |
| `--disk-offload disable` | ~2.95M RPS (projected) | **3.87M RPS sustained** | **+31%** |
| `try_enforce_readonly` self-time | 10.0% | **0%** | -10pp |
| `wal_append_and_fanout` self-time | 21.2% | **0.05%** (with disk-offload disable) | -21.1pp |

Production builds without frame pointers should clear 5.5–6.5M RPS at the
same flag set. Round-4 baseline data: `memory/benchmark_perf_round4_2026_04_27.md`.
Round-5 verification data: `memory/benchmark_perf_round5_2026_04_27.md`.

### Tests Added

- `replication::state::tests::test_set_role_updates_is_replica_mirror`
- `replication::state::tests::test_is_replica_mirror_default_false`
- `shard::spsc_handler::wal_append_tests::test_wal_append_bypass_when_no_writers_no_replicas`
- `shard::spsc_handler::wal_append_tests::test_wal_append_writes_backlog_when_replicas_present`

All 2450 lib tests passing locally (`cargo test --no-default-features
--features runtime-tokio,jemalloc --lib`); clippy clean (`cargo clippy
--no-default-features --features runtime-tokio,jemalloc -- -D warnings`).

### Drive-by Fixes

- **`src/shard/mod.rs`**: pre-existing test compile failure where two
  `drain_spsc_shared` call sites passed `&mut None` for `repl_backlog`
  instead of a `SharedBacklog` (`Arc<Mutex<Option<...>>>`). Fixed by
  constructing `Arc::new(parking_lot::Mutex::new(None))` in the test fixture.
  This was blocking `cargo test --lib` on `main` independent of this
  release.
- **`src/shard/dispatch.rs`**: pre-existing clippy `doc_lazy_continuation`
  error on the `TextSearchPayload` doc comment, blocking
  `cargo clippy -- -D warnings` on `main`.

### Compatibility

- **Wire protocol**: unchanged. Drop-in replacement for v0.1.10.
- **Public API**: only additive (`ReplicationState::set_role`,
  `ReplicationState::is_replica_mirror` field). No breaking changes.
- **Persistence on-disk format**: unchanged.
- **Replication wire format**: unchanged.

## [0.1.10] — 2026-04-23

Stable replication marker. **Single-shard PSYNC2 wired end-to-end and
production-ready** for `--shards 1` master with any `--shards N` replica
topology. Multi-shard master PSYNC is scheduled for v0.2 (see
`.planning/rfcs/multi-shard-replication-design.md`).

- **Replication** (`081c43b`): single-shard master PSYNC2 end-to-end wired,
  REPLCONF validated, `master_link_status` reports the actual handshake
  state instead of the legacy `up` stub.
- **Performance**: batch-level eviction gate; `try_handle_*` paths
  `#[inline]`-ed; DashTable carries through the v0.1.10 pre-size
  groundwork (capacity hint + headroom).
- **Docs**: BENCHMARK.md §2.7 updated with the 2026-04-22 GCloud
  re-measurement; v0.1.x replication scope documented under
  `docs/guides/clustering.mdx#replication`.

## [0.1.9] — 2026-04-19

**Lunaris Retriever Gap Closure.** Every v0.1.8 client-side fallback in
the Lunaris SDK is now closed so `HybridRRFRetriever` (dense path),
`GraphFirstRetriever`, and `PathReasoningRetriever` run Moon-native.

- **Phase 167 CYP-01/02**: Cypher `CREATE` / `MERGE` writes participate
  in `CrossStoreTxn` via `record_graph()`; `TXN.ABORT` rolls them back.
- **Phase 168 CYP-03/06**: `coalesce()` built-in + single-hop edge-var
  binding in variable-length `EXPAND`.
- **Phase 169 CYP-04/05**: `shortestPath()` parser + Dijkstra executor
  bridge with path-variable binding.
- **Phase 170 HYB-01/02/04**: `FT.SEARCH HYBRID` dense stream honours
  `as_of_lsn`.
- **Phase 171 SCAT-01/02/03**: `ShardMessage::VectorSearch` +
  `FtHybridPayload` carry `as_of_lsn` for multi-shard `AS_OF` correctness.
- **Phase 172 PIPE-01/02/03**: pipeline-aware HSET auto-indexing
  regression guard (3-test suite).

Audit status: **PASSED_WITH_DOCUMENTED_DEFERRALS**. 15 / 20 requirements
fully satisfied; HYB-03 BM25 MVCC deferred and closed in v0.1.10
follow-up (G-1); Phase 173 hygiene HYG-02 handler split RFC'd.

Stats: 6 phases shipped, 17 plans, 27 files changed, +2924 / −376 LOC.

## [0.1.8] — 2026-04-18

### Added — Cross-Store ACID Transactions (Phases 157, 161-163)

- **TXN.BEGIN**: Start a cross-store transaction — buffers KV, vector, and graph writes as intents.
- **TXN.COMMIT**: Commit all changes atomically with WAL record (`XactCommit` 0x34) for crash recovery.
- **TXN.ABORT**: Roll back all changes via undo-log replay with before-images.
- **KvWriteIntents**: Sparse MVCC side-table for uncommitted KV writes during transactions.
- **DeferredHnswInserts**: Vector index inserts deferred until commit, avoiding partial graph states.
- **UndoLog**: SmallVec-based KV rollback with before-image recording for all mutation types.
- **WAL transaction records**: `XactBegin` (0x33), `XactCommit` (0x34), `XactAbort` (0x37) with crash recovery replay.
- **Mutual exclusion**: `TXN` and `MULTI/EXEC` cannot be mixed (enforced at handler level).

### Added — Bi-Temporal MVCC (Phase 158)

- **TEMPORAL.SNAPSHOT_AT**: Record wall-clock → WAL LSN binding for point-in-time queries.
- **TEMPORAL.INVALIDATE**: Set `valid_to` on a graph entity (NODE or EDGE) for temporal visibility control.
- **FT.SEARCH AS_OF**: Query vector indexes at a historical timestamp via LSN resolution.
- **GRAPH.QUERY VALID_AT**: Execute Cypher queries against graph state valid at a specific timestamp.
- **TemporalRegistry**: BTreeMap-backed wall-clock → LSN mappings with O(log n) range lookups.
- **TemporalKvIndex**: Sparse versioned KV index with lazy initialization.
- **CSR segment format v2**: Bi-temporal `NodeMeta` with `valid_from`/`valid_to` fields.
- **WAL temporal records**: `TemporalUpsert` (0x35) and `GraphTemporal` (0x36) for crash recovery.

### Added — Workspace Partitioning (Phase 159)

- **WS CREATE**: Create a workspace with UUID v7 (time-ordered, 74-bit random). Name max 64 bytes.
- **WS DROP**: Delete a workspace and its registry entry.
- **WS AUTH**: Bind a connection to a workspace — all subsequent commands transparently prefixed.
- **WS INFO**: Return workspace metadata (name, creation timestamp).
- **WS LIST**: Enumerate all registered workspaces.
- **Transparent key rewriting**: `workspace_rewrite_args()` injects `{ws_hex}:` hash tag prefix on key arguments and strips it from responses.
- **WorkspaceRegistry**: Per-shard metadata with creation timestamps and WAL persistence.

### Added — Durable Message Queues (Phase 160)

- **MQ CREATE**: Create a durable queue with `MAXDELIVERY` (default 3) and `DEBOUNCE` options.
- **MQ PUSH**: Enqueue messages with field/value pairs (returns stream ID).
- **MQ POP**: Claim messages with optional `COUNT` (defaults to 1). Increments delivery counter.
- **MQ ACK**: Acknowledge messages by stream ID.
- **MQ DLQLEN**: Return dead-letter queue depth.
- **MQ TRIGGER**: Register debounced trigger callbacks with configurable debounce interval.
- **MQ PUBLISH**: Transactional enqueue within a `TXN` block — applied on `TXN COMMIT`.
- **Dead-letter queue**: Automatic DLQ at `{queue_key}::mq:dlq` after exceeding `MAXDELIVERY` attempts.
- **TriggerRegistry**: Debounced callback execution via pub/sub publish.
- **WAL recovery**: `replay_mq_wal()` with cursor rollback for durable queue state restoration.

### Added — Handler Parity

- All TXN/TEMPORAL/MQ/WS commands wired into `handler_monoio.rs`, `handler_sharded.rs`, `uring_handler.rs`, and `handler_single.rs`.
- 12 KV transaction integration tests, 14 MQ integration tests, workspace cross-shard dispatch tests.
- TEMPORAL/MQ/WS/TXN entries added to `test-commands.sh` and `test-consistency.sh`.

## [0.1.7] — 2026-04-17

### Added — BM25 Full-Text Search Engine (Phases 149-156)

- **BM25 inverted index**: Full-text search with multi-field boosting and per-field term frequency tracking.
- **TEXT field type**: Unicode tokenization with stemming and normalization in `FT.CREATE`.
- **TAG field type**: Categorical tag filtering with multi-value support (`@field:{val1|val2}`).
- **NUMERIC field type**: Range filtering (`@field:[min max]`) in `FT.CREATE` and `FT.SEARCH`.
- **FT.AGGREGATE**: Aggregation pipeline with `GROUPBY`/`REDUCE`, scatter-gather across shards, HLL `COUNT_DISTINCT`.
- **Three-way RRF hybrid fusion**: Combines BM25 + dense vector + sparse vector results via Reciprocal Rank Fusion.
- **Typo tolerance**: FST Levenshtein fuzzy matching (`%%term%%`) and prefix search (`term*`).
- **HIGHLIGHT/SUMMARIZE**: Post-processors for formatting search results with matched term highlighting.
- **Multi-shard DFS global IDF**: Distributed frequency statistics for accurate BM25 scoring regardless of shard count.
- **FT.DROPINDEX DD**: Atomic index + document deletion flag — deletes all hash keys matching index prefixes.
- **Python SDK text module**: `client.text.text_search()`, `client.text.aggregate()`, `client.text.hybrid_search()` with typed pipeline DSL.
- **LangChain/LlamaIndex hybrid adapters**: Framework integrations updated for three-way hybrid search.

### Statistics

- 8 phases (149-156), 27 plans, 26 requirements, 122 commits.

## [0.1.6] — 2026-04-15

### Added — AI-Native Data Primitives

- **Multi-field vector indexes** (`FT.CREATE`): multiple VECTOR fields per index, per-field segment storage, field-targeted `@field_name` syntax in `FT.SEARCH` KNN clause.
- **Sparse vector module** (`src/vector/sparse/`): inverted index with `SparseStore`, enabling BM25-style sparse retrieval alongside dense HNSW.
- **Hybrid dense+sparse search**: `SPARSE` clause in `FT.SEARCH` with Reciprocal Rank Fusion (RRF) for combining dense and sparse results.
- **Text index** (`src/vector/text_index.rs`): Unicode tokenization pipeline with stemming and normalization, feature-gated under `text-index`.
- **Boolean and geo filter expressions**: `BoolEq` and `GeoRadius` filter variants with evaluation logic in `FilterExpr`.
- **FT.RECOMMEND**: centroid-based recommendation over vector indexes — computes centroid of seed vectors and returns nearest neighbors.
- **FT.NAVIGATE**: multi-hop knowledge graph navigation from vector search results, bridging vector and graph queries.
- **FT.EXPAND**: GraphRAG expansion command — traverses graph edges from vector search results to discover related entities, with configurable depth.
- **FT.CACHESEARCH**: semantic cache-or-search command — returns cached results on similarity hit, falls back to full search on miss.
- **FT.CONFIG SET/GET**: runtime configuration for per-index knobs (e.g., `AUTOCOMPACT` toggle).
- **SESSION clause**: session-scoped filtering in `FT.SEARCH` for multi-tenant and agent memory isolation.
- **RANGE threshold post-filter**: distance threshold filtering in `FT.SEARCH` results.
- **LIMIT pagination**: `LIMIT offset count` support in `FT.SEARCH` with multi-segment merge.

### Added — Production Infrastructure

- **moondb Python SDK** (`python/moondb/`): high-level client with vector, graph, session, cache, and framework integrations (LangChain, LlamaIndex).
- **5 quickstart examples**: RAG, semantic cache, GraphRAG, AI agent tools, memory engine — all using real MiniLM embeddings.
- **Production deployment guide** (`docs/production-guide.md`): configuration reference, TLS setup, monitoring, ACL, tuning.
- **Dockerfile improvements**: OCI labels, admin port exposure, production defaults.
- **docker-compose.yml**: production configuration with resource limits, health checks, ulimits.
- **Prometheus metrics**: counters and histograms for v0.1.6 commands (FT.RECOMMEND, FT.NAVIGATE, FT.EXPAND, FT.CACHESEARCH, FT.CONFIG).
- **OpenTelemetry stubs**: `otel` feature flag with tracing infrastructure for future OTLP export.
- **Benchmark script** (`scripts/bench-v0.1.6.sh`): automated benchmarks for new vector search features.

### Added — Graph-Vector Integration

- **EXPAND GRAPH clause** in `FT.SEARCH`: inline graph expansion during vector search with configurable depth.
- **`graph_expand.rs` bridge module**: connects vector search results to graph traversal engine.
- **`key_to_node` mapping** on `NamedGraph`: enables graph expansion from vector search key hashes.

### Fixed — Critical Production Bugs

- **Deadlock in cross-shard HSET auto-index**: `parking_lot::Mutex` re-entry in `PipelineBatch` and `PipelineBatchSlotted` handlers — `shard_databases.vector_store(shard_id)` attempted to re-lock a non-reentrant mutex already held by the caller. Fixed by using the passed-in reference.
- **Auto-index HSET inside MULTI/EXEC**: vector auto-indexing now works correctly within transactions and pipeline batch paths.
- **FT.RECOMMEND filter bug**: filter expressions were not applied correctly to recommendation results.
- **FT.CONFIG/RECOMMEND/NAVIGATE/EXPAND routing**: commands now dispatched correctly in all connection handlers (monoio, tokio, single-threaded).
- **Session deduplication**: fixed duplicate session entries in search results.
- **Inline filter parsing**: corrected parsing of filter expressions in inline command mode.
- **FT.COMPACT, FT.CONFIG, FT.CACHESEARCH metadata**: registered in phf command metadata table for ACL and COMMAND DOCS.

### Fixed — CI/Release Pipeline

- **nfpm download URL**: pinned v2.46.1 with correct `Linux_x86_64` asset name (old `nfpm_linux_amd64.tar.gz` returns 404).
- **SBOM filenames**: `cargo-cyclonedx` v0.5+ writes `.json` not `.cdx.json`.
- **Cosign keyless signing**: added `id-token: write` permission for Sigstore OIDC (was falling back to interactive device flow).
- **Package job race condition**: deb/rpm upload now waits for GitHub release to be created first.
- **upload-artifact**: upgraded v4 → v7 (Node.js 20 deprecation).
- **Test compilation**: added tokio dev-dependency with `process` feature for `blocking_list_timeout.rs` under default (monoio) features.

### Changed

- **Graph enabled by default**: `graph` feature now included in default feature set.
- **Vector index persistence**: v2 format with backward-compatible v1 migration.
- **Memory engine example**: rewritten as 142-line script with real MiniLM embeddings (was 487-line complex agent loop).
- **Clippy 1.94 compliance**: all warnings resolved for Rust 1.94 MSRV.

### Validation

- 2,613+ unit tests pass (release mode, default features).
- 2,139 library tests pass under `runtime-tokio` feature set.
- 184 unsafe blocks, all with SAFETY comments (audit pass).
- 0 unannotated unwraps on hot paths (ratchet pass).
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.
- Full release pipeline validated: 6 binary targets, Docker image, deb/rpm packages, SBOMs, cosign signatures.

## [0.1.5] — 2026-04-12

### Added — Moon Console (Interactive Data Client)

- **HTTP/WebSocket gateway** (`src/admin/`): REST endpoints (`/api/v1/info`, `/api/v1/command`, `/api/v1/keys`, `/api/v1/key/*`, `/api/v1/memory/treemap`, `/api/v1/hnsw/trace`), WebSocket-to-RESP3 bridge at `/ws/console`, SSE metrics stream at `/sse/metrics` (1 Hz), CORS allowlist, per-IP token-bucket rate limit, HMAC-SHA256 Bearer auth, HTTP/2 support, static file serving via `rust-embed`.
- **React 19 console** (`console/`): 7-view SPA (Dashboard, Browser, Console, Vector Explorer, Graph Explorer, Memory, Help) served at `/ui/`. 50.9 KB gzipped initial bundle (6× under 300 KB target) via Vite 8 + manual chunk splitting (Three.js/Monaco/Recharts lazy).
- **Real-time Dashboard**: 7 widgets (QPS, latency P50/P99, memory, clients, ops by type, keyspace) driven by SSE stream.
- **KV Data Browser**: namespace tree, virtual-scrolled key list (TanStack Virtual), type-specific editors for Strings/Hashes/Lists/Sets/Sorted Sets/Streams, TTL display + edit, bulk delete with toasts.
- **Query Console**: Monaco editor with RESP + Cypher Monarch syntax, 233-command auto-complete, multi-tab, history, Cmd+Enter (current line) / Cmd+Shift+Enter (whole buffer), line-by-line execution for paste safety.
- **Vector 3D Explorer**: UMAP projection in a web worker, HNSW layer overlay, KNN search with distance rings, lasso selection, Three.js r183 + React Three Fiber.
- **Graph 3D Explorer**: force-directed layout (d3-force-3d worker), Cypher editor, node/edge property inspector, hybrid query integration.
- **Memory view**: keyspace treemap (server-aggregated `/api/v1/memory/treemap`), slowlog table, command stats.
- **Built-in Help guide**: 427-line Getting Started tutorial with seed examples.
- **Core admin commands** (`src/command/server_admin.rs`): FLUSHALL, FLUSHDB, DBSIZE, DEBUG OBJECT/SLEEP/JMAP, MEMORY USAGE — closing pre-existing dispatch gaps.
- **Multi-shard SCAN fan-out** (`src/admin/scan_fanout.rs`): composite cursor `{shard_id}:{cursor}` so Browser sees unified keyspace.
- **Frontend test infrastructure**: 56 Vitest unit tests + 9 Playwright E2E specs. New `scripts/test-integration.sh` harness and `.github/workflows/console-integration.yml`.
- **Admin-port hardening** (`src/admin/{auth,cors,rate_limit,middleware}.rs`): Bearer auth, CORS allowlist, per-IP rate limit.

### Fixed

- **WebSocket request ID echo**: errors now echo back the client's `id`, preventing client-side promise timeouts on malformed input.
- **Console type badges**: `execCommand` response was returning the full `{result, type}` envelope; now unwraps `.result` correctly.
- **Multi-line paste in Console**: Cmd+Enter now executes the current line only (redis-cli paste behavior). Cmd+Shift+Enter executes the whole buffer line-by-line.

### Validation

- 101+ Rust unit/integration tests pass on both `runtime-tokio` and `runtime-monoio`.
- 56 Vitest tests + 9 Playwright specs pass.
- Zero clippy warnings (default + `runtime-tokio,jemalloc` feature sets).
- `cargo fmt --check` clean.
- 8 fuzz targets in CI.

## [0.1.4] — 2026-04-11

### Added — Graph Engine Integration (v0.1.4, 2026-04-11)

- **Property graph engine** (`src/graph/`, feature-gated under `graph`): segment-aligned CSR storage with SlotMap generational indices, ArcSwap lock-free reads, Roaring validity bitmaps, and Rabbit Order compaction for cache locality. 8,500+ LOC, 319 tests.
- **12 GRAPH.\* commands**: CREATE, ADDNODE, ADDEDGE, NEIGHBORS, QUERY, RO_QUERY, EXPLAIN, VSEARCH, HYBRID, INFO, LIST, DELETE — all with RESP3 Map responses and ACL annotations.
- **Cypher subset parser**: hand-rolled recursive descent with logos lexer, 12 clauses (MATCH/WHERE/RETURN/CREATE/DELETE/SET/MERGE/WITH/UNWIND/CALL/ORDER/LIMIT), parameterized queries ($param), nesting depth limit (64), plan caching.
- **Hybrid graph+vector queries**: graph-filtered vector search, vector-to-graph expansion, vector-guided walk with automatic strategy selection.
- **Traversal engine**: BFS/DFS/Dijkstra with bounded frontiers (100K cap), temporal decay + distance scoring, segment merge reader across mutable + immutable segments.
- **Graph indexes**: per-label/type Roaring bitmaps, boomphf minimal perfect hash (~3 bits/key), property B-tree for range queries.
- **Cross-shard traversal**: scatter-gather via SPSC mesh, graph hash tags for shard co-location, snapshot-LSN forwarding, configurable depth limit.
- **Graph MVCC**: extends existing TransactionManager with graph write intents, snapshot-isolated multi-hop traversal, bounded epoch hold (30s).
- **Graph WAL durability**: RESP-encoded graph commands in per-shard WAL, two-pass replay (nodes before edges), CRC32-validated CSR segment persistence.
- **Cost-based planner**: GraphStats with incremental degree tracking, graph-first vs vector-first strategy selection, P99 hub detection.
- **Criterion benchmarks**: CSR 1-hop 1.02ns, edge insert 64.8ns, 2-hop BFS 4.99µs, CSR freeze 5.12ms, SIMD cosine 384d 33.9ns.
- **Fair comparison benchmark** (`tests/graph_bench_compare.rs`): Moon 2.4x FalkorDB on Cypher MATCH, 19x on native 1-hop, 23x on population.
- **New dependencies**: `slotmap` 1.x (generational indices), `boomphf` 0.6 (MPH), `logos` 0.14 (Cypher lexer, optional).

### Added — High-Impact Redis Command Parity (2026-04-10)

- **COPY command** — atomic key duplication with DESTINATION, REPLACE options (Redis 6.2+).
- **Bit operations** — GETBIT, SETBIT, BITCOUNT (byte/bit range modes), BITOP (AND/OR/XOR/NOT), BITPOS (byte/bit range modes) with read-only dispatch variants.
- **SORT command** — full BY/GET/LIMIT/ALPHA/ASC/DESC/STORE support for lists, sets, and sorted sets.
- **Geospatial commands** — GEOADD (NX/XX/CH), GEOPOS, GEODIST (M/KM/FT/MI), GEOHASH (11-char base32), GEOSEARCH (FROMLONLAT/FROMMEMBER, BYRADIUS/BYBOX, WITHCOORD/WITHDIST/WITHHASH), GEOSEARCHSTORE.
- **CONFIG REWRITE** — atomic write of runtime config to `<dir>/moon.conf` (tmpfile + rename). CONFIG RESETSTAT stub.
- **CLIENT PAUSE/UNPAUSE** — delays command processing with WRITE-only mode support. CLIENT INFO, CLIENT LIST (stub), CLIENT NO-EVICT/NO-TOUCH accepted.
- **MEMORY USAGE/DOCTOR/HELP** — key memory estimation via `estimate_memory()`.
- **Lazyfree threshold** — configurable via `CONFIG SET lazyfree-threshold N` (default 64).
- **GETBIT/SETBIT metadata** — added to PHF command registry.
- **GEOADD/GEOSEARCHSTORE** — added to AOF write commands test list.
- **EXPIREAT/PEXPIREAT** — absolute Unix timestamp expiry (seconds/milliseconds).
- **EXPIRETIME/PEXPIRETIME** — read back absolute expiry timestamp.
- **FLUSHDB/FLUSHALL** — clear all keys in current database.
- **TIME** — server clock as `[seconds, microseconds]`.
- **RANDOMKEY** — return a random key from the database.
- **TOUCH** — refresh LRU/LFU access time without reading value.
- **SHUTDOWN** — dispatch entry (graceful stop via signal handler).
- **BITFIELD** — GET/SET/INCRBY with type specifiers (u8/i16/u32/...), OVERFLOW WRAP/SAT/FAIL.
- **LCS** — Longest Common Substring with LEN option.
- **XSETID** — set stream last-delivered ID without adding entries.
- **GEORADIUS/GEORADIUSBYMEMBER** — deprecated wrappers translating to GEOSEARCH.
- **OBJECT FREQ/IDLETIME/REFCOUNT** — LFU counter, idle seconds, reference count introspection.
- **LOLWUT** — Easter egg returning Moon version.

### Added — Client Connection Security Hardening (2026-04-10)

- **`--maxclients` (P0):** Connection limit with atomic CAS rejection (default 10000, 0=unlimited). Returns `-ERR max number of clients reached` when exceeded.
- **`--timeout` (P0):** Client idle timeout in seconds (default 0=disabled). Disconnects idle clients via `tokio::time::timeout` / `monoio::select!`.
- **`--tcp-keepalive` (P0):** TCP keepalive interval (default 300s, 0=disabled). Sets `SO_KEEPALIVE` + `TCP_KEEPIDLE` on accepted sockets via `socket2`.
- **AUTH rate limiting (P0):** Per-IP exponential backoff on AUTH failures (100ms base, 10s cap, 60s auto-reset). New module `src/auth_ratelimit.rs`.
- **CLIENT LIST / INFO / KILL (P1):** Global client registry with Drop-guard deregister. Redis-compatible output format. Kill by ID/ADDR/USER. New module `src/client_registry.rs`.
- **CLIENT PAUSE / UNPAUSE (P1):** Server-wide pause with ALL/WRITE modes and auto-expiry. New module `src/client_pause.rs`.
- **CLIENT NO-EVICT / NO-TOUCH (P1):** Accepted stubs for Redis compatibility.
- **ACL GENPASS (P1):** Cryptographically secure random password generation (1-4096 bits, hex output).
- **CONFIG GET/SET** support for `maxclients`, `timeout`, `tcp-keepalive` (runtime-mutable).
- **Monoio connection tracking:** Added missing `record_connection_opened` / `record_connection_closed` for accurate `connected_clients` metric.

### Fixed — Deep Review Findings (2026-04-11)

- **DoS protection**: `execute_profile` and `execute_mut` Cypher paths now enforce MAX_HOPS_LIMIT=20 and MAX_RESULT_ROWS=100K (were unbounded).
- **WAL correctness**: Cypher DELETE passes actual LSN to `remove_node`/`remove_edge` (was hardcoded to 0).
- **GRAPH.DROP metadata**: added missing phf dispatch table entry.
- **SAFETY comments**: added to all 7 unsafe SIMD/mmap functions.
- **BFS 30% faster**: scratch buffer reuse in SegmentMergeReader, zero-alloc CsrStorage callback, MergedNeighbor derives Copy.
- **ParallelBfs**: uses plain HashSet on sequential path (was DashSet with 64 shards overhead).
- **Recovery hardening**: CSR manifest path traversal validation, WAL embedding dimension cap (65536), LSN saturating_add.
- **CI optimized**: consolidated 26 jobs → 4 per PR, concurrency groups cancel superseded runs, fixed org runner group for public repos.

### Fixed — Wave 0-4 Gap Closure (2026-04-09)

- **ZREVRANGEBYSCORE/ZREVRANGEBYLEX correctness bug:** Fixed double-swap of min/max bounds in `zrange_by_score` and `zrange_by_lex` that caused empty results for finite score ranges (e.g., `ZREVRANGEBYSCORE key 3 1`). Added finite-range test to `test-commands.sh`.
- **INFO command enriched:** Clients section now reports `connected_clients`, Memory section reports `used_memory`/`used_memory_human`/`used_memory_rss` (from `/proc/self/status`), Replication section wired to actual `ReplicationState` (role, connected_slaves, master_replid, master_repl_offset).
- **Tracing spans:** Added `#[instrument]` to connection handlers (single, monoio), replication master (tokio, monoio), HNSW compaction, and AOF rewrite — 6 new spans.
- **Replication lag metric wired:** `moon_replication_lag_bytes` Prometheus gauge now updated from `get_replication_info()`.
- **CI supply chain security:** `cargo deny check` + `cargo audit` added to CI pipeline (deny.toml was previously unenforced).
- **Release pipeline:** aarch64-unknown-linux-gnu build added via `cross` for primary production target.
- **Crash matrix expanded:** BGSAVE and BGREWRITEAOF crash cells added (6/7 coverage).
- **Compatibility tests expanded:** Stream (XADD/XLEN/XRANGE/XTRIM), Lua scripting (EVAL/EVALSHA/SCRIPT), and ACL (WHOAMI/LIST) tests added to `redis_compat.rs`.

### Added — Production Contract (Phase 87, 2026-04-08)

- **`docs/PRODUCTION-CONTRACT.md`** — Moon's v1.0 promises: per-command-class SLOs (provisional until Phase 97), supported platform matrix (Linux aarch64 primary, Linux x86_64 secondary contingent on `PERF-04`, macOS dev-only via OrbStack), durability mode semantics per `appendfsync` × failure-class, availability & replication guarantees, security guarantees, explicit out-of-scope list, and a machine-checkable GA Exit Criteria checklist that every v0.1.3 phase ticks off. This is the contract every downstream hardening phase (88–100) tests against.
- **`docs/runbooks/`** — stub directory for operator runbooks authored in Phase 99 (`REL-05`).

### Changed — Toolchain Upgrade (Phase 88, 2026-04-08)

- **MSRV bumped from Rust 1.85 to 1.94.0.** `rust-toolchain.toml` committed so fresh clones auto-install the pinned version; CI workflows (`ci.yml`, `codeql.yml`, `release.yml`) and OrbStack `moon-dev` VM provisioning in `CLAUDE.md` updated. No language/runtime behavior change; downstream phases benefit from new clippy lints and std/compiler improvements. Contributors must run `rustup update` on next pull.

### Added — Production Readiness Phases 92-105 (2026-04-09)

- **Observability:** Prometheus `/metrics` on `--admin-port`, SLOWLOG GET/LEN/RESET/HELP, HEALTHZ + READYZ commands, `/healthz` + `/readyz` HTTP endpoints, INFO extended with Server/Clients/Memory/Stats/CPU sections, `--check-config` flag, per-command latency histograms + connection metrics wired into dispatch
- **Durability proof:** Crash-injection test matrix, torn-write WAL v3 tests (CRC32C validated), Jepsen-lite linearizability harness, backup/restore workflow test
- **Replication hardening:** PSYNC partial resync, full resync, network partition, kill-restart, replica promotion tests
- **Client compatibility:** CI matrix (redis-py, go-redis, jedis, ioredis, node-redis, redis-rs, hiredis), 24 Redis compat tests, vector client smoke script, `docs/redis-compat.md`
- **Performance gates:** Criterion regression CI with baseline caching, RSS-per-key memory gate script
- **Security hardening:** `deny.toml` (cargo-deny), `SECURITY.md`, `docs/THREAT-MODEL.md`, `docs/security/lua-sandbox.md`, TLS cipher suite freeze
- **Release engineering:** `docs/versioning.md`, 6 operator runbooks, CHANGELOG CI gate, user docs (getting-started, configuration, monitoring), release pipeline SHA256 checksums + SBOM + cosign

## [0.1.3] — 2026-04-10

Production-readiness foundation: dispatch hot-path recovery, vector-search
4× QPS + correctness fixes, and the tiered disk-offload landing with 100 %
crash recovery across 7 persistence configurations. Bundles three work
streams originally tracked as separate Unreleased blocks (Apr 7–8).

### Dispatch Hot-Path Recovery (2026-04-08)

**Pipelined SET +37%, pipelined GET +68% at p=16 after PR #43 regression recovery.**

Three targeted perf fixes landed after flamegraph-driven analysis of pipelined
SET on aarch64 (OrbStack moon-dev, 1 shard, default config, redis-benchmark
-c 50 -n 3M -P 16 -r 100000 -d 64):

| Metric                  | Broken baseline | After T0a+T0b+T0c | Δ      |
|-------------------------|----------------:|------------------:|-------:|
| SET p=1   (ratio Redis) | 0.99x           | **1.12x**         | +13pp  |
| SET p=16                | 1.42M/s         | **1.94M/s**       | +37%   |
| SET p=32                | 2.06M/s         | **2.26M/s**       | +10%   |
| GET p=16                | 2.40M/s         | **4.04M/s**       | +68%   |
| GET p=128 vs Redis      | 1.87x           | **1.91x**         | +4pp   |

### Perf fixes

- **T0a — Thread-local cached clock** (4041b0d). `Entry::new_*` constructors
  were calling `SystemTime::now()` / `clock_gettime` on every write, showing up
  at **10.14% of CPU** in the perf profile. Added a thread-local `Cell<u32>` /
  `Cell<u64>` refreshed once per shard tick (~1 ms) from `CachedClock::update()`.
  `current_secs` / `current_time_ms` now read the Cell and fall back to the
  syscall only on tests / cold init. `__kernel_clock_gettime` dropped from
  10.14% → **0%** of CPU.

- **T0b — Hot command dispatch bypasses phf SipHasher** (4b0eec3). The command
  metadata registry is a `phf::Map` keyed by `&'static str` using `SipHasher` —
  cryptographic overkill for a 173-entry ASCII table. Combined `phf::Map::get`
  + `SipHasher::write` + `hash_one` was **~6% of CPU**. Added a direct match
  path in `command::metadata::lookup`: pack the first ≤8 bytes of the command
  name as a `u64` with ASCII letters uppercased, match against 24 hand-picked
  hot commands (GET/SET/DEL/TTL/MGET/MSET/INCR/DECR/HSET/HGET/HDEL/HLEN/LPOP/
  RPOP/LLEN/PING/LPUSH/RPUSH/EXPIRE/EXISTS/INCRBY/DECRBY/SELECT/HGETALL).
  Hot-path resolves through a pre-resolved `LazyLock<[&'static CommandMeta; 24]>`
  — single array index, no hashing. Cold commands fall through to phf unchanged.
  Correctness asserted by `hot_path_matches_phf_map` test: every hot entry must
  return the same `&'static` pointer as a direct phf probe, in both upper and
  lowercase.

- **T0c — ACL unrestricted-user short-circuit** (4603511). Every command
  executed `check_command_permission` + `check_key_permission` even for the
  default `on nopass ~* &* +@all` user, burning **2.11% of CPU** on
  lowercasing, `extract_command_keys`, and glob matching. Added a cached
  `unrestricted: bool` field to `AclUser`, true iff the user is enabled, has
  `AllAllowed` commands, only `~*` read/write key patterns, and only `*`
  channel patterns. The three `check_*_permission` methods early-return `None`
  on `unrestricted` before any allocation or iteration. The cache is
  recomputed once at the end of `apply_rule` (the single mutation entry point
  used by ACL SETUSER / LOAD / reset). Correctness covered by three new tests
  (`default_user_is_unrestricted`, `restrictions_clear_unrestricted_flag`,
  `unrestricted_user_passes_all_checks`).

### Correctness fix (PR #43 review)

- **Inline monoio fast-path restricted to GET** (613c164). The previous inline
  dispatch in `try_inline_dispatch` handled both GET and SET directly against
  the DashTable, bypassing replica READONLY enforcement, ACL checks, maxmemory
  eviction, client-side tracking invalidation, keyspace notifications,
  replication propagation, and blocking-waiter wakeups. Under any of those
  configurations the inlined SET would silently diverge from the normal path —
  accepted writes on replicas, ACL-denied clients writing, maxmemory overshoot,
  stale client-side caches. Fix: inline only handles `*2\r\n$3\r\nGET` now;
  SET and everything else fall through to the full dispatcher where all
  side-effects run.

### Cold-tier lock hygiene (PR #43 review)

- **Release shard read guard before cold-tier disk read** (ff51135). The
  cold-tier fallback in `server::conn::blocking` previously called
  `get_cold_value()` — which does a synchronous `std::fs::read()` — while still
  holding the per-shard read guard, blocking all concurrent operations on that
  shard during disk I/O. Split the path: `Database::cold_lookup_location`
  returns the `(ColdLocation, PathBuf)` under the lock, the guard is dropped,
  and `cold_read::read_cold_entry_at` performs the disk read unlocked.

### Additional PR #43 fixes

- `read_overflow_chain` now bounded at 1000 iterations (cycle guard against
  corrupted `next_page` links)
- `recovery.rs` FPI replay replaces `.unwrap()` on `try_into()` with explicit
  byte-array construction (coding-guidelines compliance)
- `bench-production.sh`: fixed unsupported `-t zrangebyscore` (→ `zpopmin`),
  MSET rps parser for `"MSET (10 keys):"` output, heredoc `$(date)` expansion,
  and Redis RSS probe (`pgrep`/`/proc` instead of missing `lsof`)
- `bench-cold-tier.sh`: removed stray `&` backgrounding `FT.CREATE`
- `test-recovery-all-cases.sh`: `NoPersistence` case now PASSes at 0 keys
- `benches/resp_parsing.rs`, `benches/get_hotpath.rs`: wrap `Vec<Frame>` in
  `FrameVec` via `.into()` after frame.rs type change

All 1872 unit tests pass under `--no-default-features --features
runtime-tokio,jemalloc`. Follow-up work (T1 `dispatch_raw` zero-alloc entry
point, Tier 2 storage/DashTable optimization, residual ACL SipHash elimination)
captured as todo in `.planning/todos/pending/`.

---

### Vector Search 4× QPS + Correctness (2026-04-07)

**4x search QPS, 4.1x lower latency, 2.56x faster than Qdrant on real MiniLM data.**

#### Performance (perf-profiled on GCloud c3-standard-8, Intel Xeon 8481C)
- 8-wide ILP unrolled `dist_bfs_budgeted` subcent path (the real hot loop, 90% of
  search time per perf profile). Loads 4 code bytes + 1 sign byte per iteration,
  8 independent f32 accumulators. Confirmed via objdump: parallel `vaddss` into
  xmm3-xmm8 (vs serial single-xmm0 chain before).
- 4-way unrolled `dist_bfs` non-subcent path with `unsafe` pointer arithmetic
- Pre-allocated ADC LUT in `SearchScratch` (eliminates 32-65KB heap alloc per query)
- Hoisted IVF `q_rotated` and `lut_buf` allocation out of per-segment loop

#### Correctness fixes
- **`FT.COMPACT` silent no-op**: split `try_compact` (threshold-gated) from
  `force_compact` (unconditional). Previously `FT.COMPACT` returned OK without
  compacting when `compact_threshold >= mutable_len`, leaving all vectors in
  brute-force O(n) mutable segment.
- **`key_hash_to_key` mapping restored** (lost in earlier refactor). `FT.SEARCH`
  now returns original Redis keys (`doc:N`) instead of `vec:<internal_id>`.
  Carried through `SearchResult.key_hash` and populated by `remap_to_global_ids`.
- **`FT.INFO num_docs`** now sums mutable + immutable segments (was 0 after compact)
- **Vector index recovery** metadata loads without `--disk-offload` flag
  (was gated behind `server_config.disk_offload_enabled()`)

#### Real MiniLM benchmarks (10K vectors, 384d, x86 Xeon 8481C)

| Metric | Mar 31 (M4 Pro) | Apr 7 (Xeon 8481C) | Δ |
|--------|---:|---:|---:|
| Recall@10 | 0.9250 | **0.9670** | +4.5% |
| QPS | 1,126 | **1,296** | +15% |
| p50 | 0.878 ms | **0.783 ms** | -11% |

| | Moon | Qdrant 1.12 FP32 | Ratio |
|---|---:|---:|---:|
| QPS (10K MiniLM) | 1,296 | 507 | **2.56x** |
| p50 | 0.783 ms | 1.79 ms | **2.29x lower** |
| Recall@10 | 0.967 | ~0.95 | **+1.7%** |

#### Infrastructure (for future segment merge work)
- `ImmutableSegment::decode_vector` / `iter_live_decoded`
- `MutableSegment::iter_live`

#### Attempted and reverted
Segment merge on `FT.COMPACT` via TQ4 decode → re-encode. Dropped recall from
0.73 → 0.0005 due to accumulated quantization error across 14 segments. Proper
fix requires retaining f32/f16 vectors alongside TQ codes in immutable segments.

#### Known limitation
TQ4 quantization at 384d with random Gaussian inputs hits ~0.73 recall floor
(curse of dimensionality — all points nearly equidistant). Real semantic
embeddings (clustered) achieve 0.92-0.97 recall with the same code.

---

### Disk Offload & x86_64 Performance (2026-04-06)

Tiered storage, crash recovery, and 2× Redis on x86_64 (Intel Xeon, io_uring).

### Added

#### Disk Offload (Tiered Storage)
- `--disk-offload enable` — evicted keys under maxmemory are spilled to NVMe instead of being deleted
- Async SpillThread: background pwrite via dedicated `std::thread` per shard (no event loop blocking)
- Cold read-through: GET transparently reads spilled keys from NVMe DataFiles
- ColdIndex: in-memory key→file mapping, updated immediately on eviction for consistent reads
- SpillThread channel capacity: 4096 bounded flume channel for burst absorption
- `--disk-offload-dir`, `--disk-offload-threshold` configuration flags

#### Crash Recovery
- V3 recovery falls back to appendonly.aof when WAL v3 has 0 commands
- V2 recovery falls back to appendonly.aof when shard WAL has 0 commands
- Automatic `--dir` creation before AOF writer starts (fixes silent write failure)
- Cold index rebuilt from manifest during v3 recovery
- Verified: 100% recovery (5000/5000 keys) across 7 persistence configurations after SIGKILL

#### Inline GET Optimization
- `read_db` + `get_if_alive` replaces `write_db` + triple-lookup `get()` — single DashTable probe
- Removed unnecessary write lock for timestamp refresh before inline dispatch
- Multi-shard inline dispatch: local keys bypass Frame construction via `key_to_shard()` check
- Cold storage fallback in `get_readonly` and inline GET dispatch paths

### Changed

- Connection handler eviction uses `try_evict_if_needed_async_spill` when disk offload enabled
- `spawn_monoio_connection` passes spill sender, file ID counter, and offload dir to handlers
- Event loop syncs `next_file_id` between `Rc<Cell<u64>>` (handlers) and local variable (timer tick)
- Inline dispatch `try_inline_dispatch` takes `now_ms` and `num_shards` parameters

### Fixed

- **Data loss under maxmemory**: evicted keys were silently deleted instead of spilled to disk (6 bugs)
- **Crash recovery = 0 keys**: appendonly.aof never tried as fallback source
- **AOF writer silent failure**: `--dir` directory not created before AOF writer task started
- **Cold read miss**: `get_if_alive` (read path) didn't check cold storage; `get_readonly` returned NULL for spilled keys
- **ColdIndex never initialized**: `cold_index` and `cold_shard_dir` were None on all databases at startup

### Performance (GCP c3-standard-8, Intel Xeon 8481C, CPU-pinned)

| Metric | Before | After |
|--------|--------|-------|
| c=1 p=1 GET vs Redis | 0.35x (47K) | **1.0x (47K)** — parity |
| c=10 p=64 GET | 2.29M | **4.71M** (2.06x Redis) |
| c=50 p=64 GET | 2.36M | **4.81M** (2.04x Redis) |
| Disk offload GET overhead | N/A | **<1%** vs no-persist |
| Recovery (SIGKILL) | 0/5000 | **5000/5000** (100%) |

---

## [0.1.2] - 2026-03-29

Multi-shard scaling milestone. Eliminated negative scaling, achieving 5M GET/s and 2.5M SET/s at 4 shards — both exceeding Redis 8.6.1.

### Added

#### Shared-Read Direct Access (Phase 49)
- `Arc<ShardDatabases>` with `parking_lot::RwLock<Database>` replaces `Rc<RefCell<Vec<Database>>>`
- Cross-shard read commands (GET, HGET, SCARD, ZRANGE, etc.) bypass SPSC channels entirely via `read_db()` + `dispatch_read()` — reduces cross-shard read latency from ~88μs to ~56ns
- Local read path uses shared `read_db()` lock instead of exclusive `write_db()` — eliminates RwLock contention between shards

#### Connection Affinity (Phase 50)
- `AffinityTracker` samples first 16 commands per connection to detect dominant shard
- Lazy FD migration: if ≥60% of keys target a non-local shard, migrates the TCP connection's file descriptor to the target shard via `ShardMessage::MigrateConnection`
- `MigratedConnectionState` preserves selected_db, client_name, protocol_version across migration
- Graceful fallback: if migration fails, connection stays on current shard with shared-read

#### Pre-Allocated Response Slots (Phase 51)
- `ResponseSlotPool` with lock-free `AtomicU8` state machine for zero-allocation cross-shard write dispatch (Tokio path)
- Eliminates per-dispatch `channel::oneshot()` heap allocation (~80-120ns savings per cross-shard write)

#### SO_REUSEPORT Per-Shard Accept (Phase 52)
- Each shard opens its own TCP listener with `SO_REUSEPORT` on Linux via `socket2` crate
- Kernel distributes connections across shard listeners using consistent 4-tuple hashing
- macOS/non-Linux: falls back to single-listener + MPSC round-robin (no behavior change)

#### jemalloc Production Tuning (Phase 53)
- `malloc_conf` static: `percpu_arena:percpu`, `background_thread:true`, `metadata_thp:auto`, `dirty_decay_ms:5000`, `muzzy_decay_ms:30000`, `abort_conf:true`
- Closes ~50% of allocation speed gap with mimalloc while retaining jemalloc's superior fragmentation behavior

#### New Commands (Phase 55)
- GETRANGE — return substring of stored string value
- SETRANGE — overwrite part of stored string at offset with zero-fill
- SUBSTR — alias for GETRANGE (Redis 1.x compatibility)

### Changed

- Custom `AtomicU8` oneshot channel replaced with `flume::bounded(1)` for cross-thread safety on monoio's `!Send` executor
- `pending_wakers` relay pattern: event loop locally wakes connection tasks after SPSC processing, bridging monoio's cross-thread waker limitation
- `write_db()` uses `try_write()` spin loop instead of blocking `write()` — prevents OS thread freeze on monoio when cross-shard readers hold locks
- Benchmark scripts: `scripts/bench-scaling.sh` for multi-shard test matrix, `scripts/bench-production.sh` updated

### Fixed

- ResponseSlot `UnsafeCell<Option<Waker>>` data race on ARM64 — replaced with `AtomicWaker`
- Local read path took exclusive write lock (`write_db()`) even for GET — split into `read_db()` + `dispatch_read()`
- Monoio local write path silently dropped responses (`responses.push(response)` missing after read/write split) — all write commands (SET, INCR, LPUSH, etc.) hung on monoio
- Pipeline ordering guard: `!remote_groups.contains_key(&target)` prevents stale reads when batch has pending writes for same shard

### Performance

| Metric | Before (v0.1.0) | After (v0.1.2) | Change |
|--------|:---------------:|:--------------:|:------:|
| Multi-shard GET p=16 | 688K (0.38x Redis) | **1,923K (1.17x Redis)** | **2.8x** |
| Multi-shard GET p=64 | N/A | **5,002K (1.60x Redis)** | New |
| Multi-shard SET p=16 | N/A | **1,515K (1.32x Redis)** | New |
| Multi-shard SET p=64 | N/A | **2,500K (1.55x Redis)** | New |
| Monoio 1s p=128 GET | 5,407K | **5,005K (1.25x Redis)** | Maintained |
| Negative scaling | -25% at 12 shards | **Zero at 1-8 shards** | Eliminated |
| Command coverage p=1 | Parity | **Monoio beats Redis 8/10** | Improved |

## [0.1.1] - 2026-03-28

Structural stability milestone. Codebase refactoring for maintainability — no feature changes, no performance changes.

### Changed

#### Error and State Foundations (Phase 44)
- Unified `MoonError` type hierarchy with structured `#[source]` on I/O variants carrying `PathBuf` context
- `ConnectionContext` struct for connection state (selected_db, authenticated, client_name, protocol_version)
- Criterion benchmark baseline (GET dispatch 69.1ns) to guard against regressions

#### Command Metadata Registry (Phase 45)
- `phf` static perfect hash map for O(1) command lookup (112 commands)
- `CommandMeta` struct: name, arity, flags (read/write/fast/admin), key positions, ACL categories
- `is_write()` classification via const bitflags — replaces duplicated match arms across codebase

#### Persistence Hardening (Phase 46)
- Eliminated server-crashing `unwrap()` calls in WAL, AOF, and RDB persistence code
- Corruption recovery: WAL uses per-block CRC32 log+skip, AOF seeks to next RESP `*` marker, RDB breaks on mid-stream corruption
- `WalWriter` methods remain `std::io::Result` (must-panic on flush = data loss prevention)

#### AOF Replay Decoupling (Phase 47)
- `CommandReplayEngine` trait breaks circular dependency between persistence and command dispatch
- `StorageEngine` trait boundary for persistence replay and Lua scripting
- `execute_command()` at command level (not individual get/set methods)

#### God-File Decomposition (Phase 48)
- `connection.rs` (5,102 lines) → 6 sub-modules in `conn/`: `handler_sharded.rs`, `handler_monoio.rs`, `handler_single.rs`, `shared.rs`, `blocking.rs`, `conn_state.rs`
- `shard/mod.rs` (2,004 lines) → 6 sub-modules: `event_loop.rs`, `spsc_handler.rs`, `persistence_tick.rs`, `conn_accept.rs`, `timers.rs`, `uring_handler.rs`
- Module facade pattern with `pub(crate)` re-exports preserving all external import paths
- No single file exceeds 800 lines

### Added
- Docker: optimized multi-stage build (113MB → 41MB)
- Mintlify documentation site
- Claude Code GitHub workflow for PR reviews
- `scripts/bench-resources.sh` for memory/CPU efficiency benchmarking

## [0.1.0] - 2026-03-27

Initial release. A Redis-compatible in-memory data store written in Rust, achieving 1.84-1.99x Redis throughput at 8 shards and 27-35% less memory for 1KB+ values.

### Added

#### Core Data Types (Phases 1-5)
- RESP2 protocol parser and serializer with inline command support
- TCP server with concurrent connections, graceful shutdown, and `redis-cli` compatibility
- String commands: GET, SET, MGET, MSET, INCR/DECR, APPEND, GETEX, GETDEL (17 commands)
- Hash commands: HSET, HGET, HGETALL, HINCRBY, HSCAN (14 commands)
- List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LPOS (12 commands)
- Set commands: SADD, SREM, SINTER, SUNION, SDIFF, SPOP (15 commands)
- Sorted Set commands: ZADD, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN (18 commands)
- Key management: DEL, EXISTS, EXPIRE, TTL, SCAN, KEYS, RENAME (13 commands)
- Lazy + active key expiration with probabilistic sampling
- RDB persistence with point-in-time snapshots
- AOF persistence with configurable fsync (always/everysec/no)
- Pub/Sub messaging: SUBSCRIBE, PUBLISH, PSUBSCRIBE (4 commands)
- Transactions: MULTI/EXEC/DISCARD with WATCH optimistic locking
- LRU/LFU/random eviction policies with configurable maxmemory

#### Performance Architecture (Phases 6-15)
- SIMD-accelerated RESP parsing via memchr CRLF scanning and atoi
- CompactValue 16-byte SSO struct with embedded TTL delta
- DashTable segmented hash table with Swiss Table SIMD probing
- Thread-per-core shared-nothing architecture with per-shard event loops
- io_uring networking layer with multishot accept/recv and registered buffers
- Per-shard memory management with jemalloc and bumpalo arenas
- Forkless compartmentalized persistence (no COW memory spike)
- B+ tree sorted sets replacing BTreeMap for cache-friendly access
- Per-connection arena allocation with bumpalo

#### Protocol & Data Types (Phases 16-18)
- RESP3 protocol: Map, Set, Double, Boolean, VerbatimString, Push frames
- HELLO command for protocol negotiation
- Client-side caching invalidation via Push frames
- Blocking commands: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX
- Streams data type: XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM

#### Clustering & Replication (Phases 19-20, 26)
- PSYNC2-compatible replication with per-shard WAL streaming
- Partial resync support with replication backlog
- Cluster mode with 16,384 hash slots and gossip protocol
- MOVED/ASK redirections and live slot migration
- Majority consensus failover election with automatic promotion

#### Scripting & Security (Phases 21-22, 43)
- Lua 5.4 scripting via mlua: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH
- Sandboxed Lua VM with Redis API bindings (redis.call, redis.pcall)
- ACL system: per-user command/key/channel permissions
- ACL SETUSER, GETUSER, DELUSER, LIST, WHOAMI, LOG, SAVE, LOAD
- TLS 1.3 via rustls + aws-lc-rs with dual-port support
- mTLS client authentication
- Protected mode (reject non-loopback when no password set)

#### Optimization (Phases 24-42)
- WAL v2 format: checksums, header, block framing, corruption isolation
- CompactKey SSO: 23-byte inline keys, eliminating heap allocation
- Response buffer pooling and adaptive pipeline batching
- Dual runtime: Tokio (all platforms) + Monoio (Linux io_uring / macOS kqueue)
- Full Monoio migration: channels, TCP, codec, spawn, persistence, replication
- Direct GET serialization bypassing Frame allocation
- Zero-copy argument slicing from parse buffer
- Lock-free oneshot channels (12% CPU reduction vs tokio::oneshot)
- CachedClock timestamp caching (4% throughput gain)
- HeapString for values (eliminates Arc overhead)
- Inline dispatch for single-shard commands

### Performance

| Benchmark | Result |
|-----------|--------|
| Peak GET throughput | 3.79M ops/sec (4 shards, p=64) |
| Peak SET with AOF | 2.78M ops/sec (AOF everysec, p=64) |
| vs Redis (pipeline=64) | 3.17x SET, 2.50x GET |
| vs Redis (8 shards, p=16) | 1.84-1.99x |
| vs Redis with AOF | 2.75x (per-shard WAL vs global) |
| Memory (1KB+ values) | 27-35% less than Redis |
| Memory (empty server) | Identical 7.0 MB baseline |
| p50 latency (8 shards) | 0.031ms (Redis: 0.26ms) |
| Data consistency | 132/132 tests pass |

### Technical Details

- **Language:** Rust (stable, edition 2024)
- **Lines of code:** ~54,000 across 96 files
- **Dependencies:** tokio, monoio, jemalloc, rustls, mlua, bumpalo, bytes, clap
- **Supported platforms:** Linux (io_uring via Monoio), macOS (kqueue via Monoio or Tokio)
- **Build time:** ~50s release build

[0.1.0]: https://github.com/pilotspace/moon/releases/tag/v0.1.0
