# TASK: HELLO/COMMAND/ROLE/RESET identity + registry-dispatch reconciliation

slug: client-identity-introspection · created: 2026-08-09 · stage: production
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
- `src/command/connection.rs:113` — `command(args: &[Frame]) -> Frame`. Sixteen lines, and its own
  doc comment states the stub: "COMMAND (bare): return integer 0. COMMAND DOCS: return empty array.
  Any other subcommand: return empty array." The two shapes are INVERTED versus Redis.
- `src/command/connection.rs:630` — `hello_acl(...) -> (Frame, u8, Option<Bytes>, Option<String>)`.
  Builds the HELLO map with `mode` and `role` as `Bytes::from_static` literals — `standalone` and
  `master` — derived from nothing.
- `src/command/metadata.rs:87` — `CommandMeta { name, arity, flags, first_key, last_key, step,
  acl_categories }`; `COMMAND_META` (phf, 271 entries); `command_count()` at :702.
- `src/command/metadata.rs:353` — `"RESET" => CommandMeta { arity: 1, flags: FAST|NO_AUTH|LOADING|
  STALE, acl_categories: CON }`. Registered, with no dispatch arm anywhere.
- `src/command/mod.rs:640` and `:1369` — the `(7, b'c')` arms in `dispatch` and `dispatch_read`,
  both `return resp(connection::command(args))`.
- `src/server/conn/core.rs:61,67,68` — `ConnectionContext.repl_state:
  Option<Arc<RwLock<ReplicationState>>>`, `is_replica_mirror: Option<Arc<AtomicBool>>` (lock-free),
  `cluster_state: Option<Arc<RwLock<ClusterState>>>`. The truth ROLE and HELLO need is already
  reachable from the connection.
- `src/replication/handshake.rs:80` — `build_info_replication(repl_state)` switches on
  `ReplicationState.role`, emitting `role:master` / `role:slave` + `master_host`/`master_port`/
  `master_link_status`. This is the SECOND source of truth HELLO contradicts.
- `src/client_registry.rs:709` — the CLIENT INFO/LIST line format string, with
  `laddr=127.0.0.1:0` hardcoded inside it.
- `src/server/conn/core.rs:215-284` — the RESET surface: `protocol_version`, `selected_db`,
  `authenticated`, `current_user`, `client_name`, `subscription_count`, `in_multi`,
  `command_queue`, `tracking_state`, `tracking_rx`, `watched_keys`.

Context (working folder): `.add/tasks/client-identity-introspection/` · probes in the session
scratchpad (`identity_probe.sh`, raw-RESP python).

Honors (patterns / conventions): the three-dispatch-paths rule (CLAUDE.md — `dispatch` +
`dispatch_read` + inline wiring, or the command is CI-invisible; `watch-cas-transactions` is the
worked example of what skipping it costs); `Frame::Error` in dispatch, never `Result`; no
allocation on the hot path in `src/command/`.

Anchors the contract cites: `COMMAND_META`, `CommandMeta`, `command_count`, `connection::command`,
`hello_acl`, `ReplicationState.role`, `ConnectionContext.{repl_state,cluster_state}`,
`client_registry` line format.

### Measured on `main` @ec0c4650 vs `redis-server` 8.6.1 (not inferred)
Raw RESP, both servers, same inputs. `redis-cli` output is not trusted here — it renders `:0` and
`*0` identically as "0", which is exactly the defect being measured.

| input | moon | redis 8.6.1 |
|---|---|---|
| `COMMAND COUNT` | `*0\r\n` | `:274\r\n` |
| `COMMAND` (bare) | `:0\r\n` | `*274\r\n` + 10-field specs |
| `COMMAND INFO GET` | `*0\r\n` | `*1` + 10-field spec |
| `COMMAND DOCS GET` | `*0\r\n` | `*2` name + doc map |
| `COMMAND GETKEYS SET k v` | `*0\r\n` | `*1 $1 k` |
| `COMMAND LIST` | `*0\r\n` | `*274` names |
| `ROLE` | `-ERR unknown command 'ROLE'` | `*3 $6 master :0 *0` |
| `RESET` | `-ERR unknown command 'RESET'` | `+RESET\r\n` |
| `MONITOR` | `-ERR unknown command 'MONITOR'` | `+OK` then the command feed |
| `CLIENT INFO` | `... laddr=127.0.0.1:0 ...` | `... laddr=127.0.0.1:7412 ...` |

Reply-shape inversion is the headline: bare `COMMAND` answers with an INTEGER and `COMMAND COUNT`
answers with an ARRAY — each returning the other's type. A client that type-switches on the reply
(any RESP3-typed driver) does not see "unsupported"; it sees a protocol violation.

Target semantics pinned against 8.6.1 rather than recalled:
- `RESET` on a RESP3 connection REVERTS THE PROTOCOL TO RESP2 — bare `HELLO` after it goes `%7`
  (map) -> `*14` (flat array). Also discards MULTI (`EXEC` -> `ERR EXEC without MULTI`), clears the
  client name (`CLIENT GETNAME` -> `$-1`), and returns to `db=0`.
- `COMMAND INFO nosuchcmd` -> `*1` wrapping `$-1`: a null ELEMENT inside an array, not an empty
  array and not a top-level null.
- `COMMAND COUNT extra` -> `ERR wrong number of arguments for 'command|count' command` — note the
  `parent|sub` naming convention in the message.
- `COMMAND GETKEYS PING` -> `ERR The command has no key arguments`.

### The registry and the dispatch table disagree in BOTH directions
- `RESET` is in `COMMAND_META` with full flags and is rejected by dispatch — the same
  advertise-then-reject class as WATCH/UNWATCH in `watch-cas-transactions`.
- `ROLE` and `MONITOR` are in neither.
- `COMMAND` is in both, and its handler ignores the registry it exists to publish:
  `command_count()` has exactly one caller in the tree, its own unit test at `metadata.rs:1118`.

Registry holds **263** commands (262 before this task registers `ROLE`); Redis 8.6.1 reports 274.
The count difference is not itself a defect — Moon is not obliged to implement every Redis command
— but `COMMAND COUNT` must report Moon's real number rather than zero.

> CORRECTION: this section first said 271, from `grep -cE '^\s*"[A-Z]'`, which also counts
> non-registry lines such as the `b"COMMAND"` byte-string literals elsewhere in the file. The real
> figure, `grep -cE '"[A-Z0-9._-]+" => CommandMeta'`, is 263 — and the running server confirms it:
> `COMMAND COUNT` -> `:263`. Recorded rather than quietly overwritten, because the sloppy grep is
> the kind of measurement error that would otherwise be inherited by the next task.

Out of scope, found while running the red suite, do not fix here: an unknown command inside MULTI
replies `+QUEUED` on Moon. Redis rejects it at QUEUE time (`-ERR unknown command`) and then fails
the transaction with `-EXECABORT Transaction discarded because of previous errors.` — measured on
8.6.1. That is `multi-exec-queue-semantics`, already filed in this milestone; noted here so the
next task starts from a measurement rather than a re-probe.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: the identity + introspection surface (`COMMAND`, `ROLE`, `RESET`, `HELLO`, `CLIENT INFO`)
answers from real server state instead of constants. `MONITOR` was measured in §0 alongside these
and is split to `monitor-command-feed` — see the resolved assumption below.

Framings weighed:
- **Registry-derived (chosen).** `COMMAND_META` becomes the single source for every `COMMAND`
  reply, and `ReplicationState`/`ClusterState` the single source for role and mode. Adding a
  command to the registry then automatically corrects `COMMAND COUNT`/`INFO`/`LIST`, so the two
  cannot drift apart again. This is the same fix-shape as extracting `watch.rs`: remove the second
  place the truth could live.
- *Hand-maintained reply tables.* A literal list of specs beside the registry. Rejected: it
  recreates the exact drift this task exists to close, one release later.
- *Report only what is implemented, computed at startup by probing dispatch.* Rejected: dispatch is
  three separate match trees; there is no enumerable list to probe, which is itself the disease.

Must:
<must>
  - `COMMAND COUNT` replies an Integer equal to the registry size, and that integer equals the
    element count of the bare `COMMAND` reply.
  - `COMMAND` (bare) replies an Array of per-command specs, each a 10-element array
    [name, arity, flags[], first_key, last_key, step, acl_categories[], tips[], key_specs[],
    subcommands[]], with name lower-cased as Redis emits it.
  - `COMMAND INFO <name> [name …]` replies one element per requested name, in request order; an
    unknown name yields a Null ELEMENT inside the array.
  - `COMMAND LIST` replies an Array of every registered command name.
  - `COMMAND GETKEYS <cmd> [args …]` replies the extracted key arguments, derived from the
    registry's first_key/last_key/step.
  - `COMMAND DOCS [name …]` replies a shape a client can parse (name followed by a doc map);
    summary text may be thin, the SHAPE may not be wrong.
  - `ROLE` replies `[master, <offset>, [replicas…]]` on a master and
    `[slave, <host>, <port>, <state>, <offset>]` on a replica, read from `ReplicationState`.
  - `RESET` replies `+RESET` and returns the connection to default state: discards MULTI, clears
    the watch set, disables tracking, exits subscribe mode, sets db 0, clears the client name, and
    reverts the protocol to RESP2.
  - `RESET` is EXECUTED IMMEDIATELY inside MULTI, never queued — measured against 8.6.1: with a
    transaction open, `RESET` replies `+RESET` and the following `EXEC` errors `without MULTI`.
    It must therefore be intercepted BEFORE the queueing step, exactly like `WATCH`. (Surfaced by
    the red run: ci10 got `+QUEUED`, which would still have been "red" while hiding the ordering
    requirement.)
  - `HELLO`'s `role` and `mode` fields are read from `ReplicationState` and `ClusterState` — a
    replica answers `role: replica` [AMENDED v1 -> v2, see below], a cluster-enabled node answers
    `mode: cluster` — so HELLO and `INFO replication` can no longer contradict each other.
  - `CLIENT INFO`/`CLIENT LIST` report the connection's real local address in `laddr`.
  - Every one of the above behaves identically on `handler_monoio`, `handler_sharded`, and the
    inline fast path.
</must>

Reject:
<reject>
  - `COMMAND COUNT <extra>` -> "ERR wrong number of arguments for 'command|count' command"
  - `COMMAND GETKEYS <cmd-with-no-keys>` -> "ERR The command has no key arguments"
  - `COMMAND GETKEYS <unregistered>` -> "ERR Invalid command specified"
  - `COMMAND GETKEYS` with fewer args than the command's arity -> "ERR Invalid number of arguments
    specified for command"
  - `RESET` with any argument -> "ERR wrong number of arguments for 'reset' command"
</reject>

After:
<after>
  - `COMMAND_META` is the ONLY place a command's introspectable shape is stated.
  - No command in scope is registered-but-undispatchable, and none dispatched is unregistered:
    `RESET` gains its arm; `ROLE` gains both. (`MONITOR` is split to its own task and is the one
    remaining known hole in this milestone's identity surface.)
  - A driver's startup handshake — which calls `COMMAND DOCS`/`COMMAND COUNT` to build its command
    map — works unmodified, as does `redis-cli --stat`.
  - HELLO and INFO agree about role and mode on the same connection.
</after>

Assumptions — lowest-confidence first:
<assumptions>
  ✓ **[spec] MONITOR — RESOLVED AT FREEZE: split into its own task** (`monitor-command-feed`).
    Raised as the bundle's lowest-confidence point on new §0 information: MONITOR has zero existing
    infrastructure (no feed, no fan-out, no subscriber set), it is the only item here that is a
    STREAM rather than a reply, the only one touching the per-command hot path on every shard, and
    the only one exposing other clients' traffic including credential-bearing arguments. Sized
    honestly it equalled the rest of the bundle. Decision (Tin Dang, at freeze): split — six cheap
    certain fixes should not wait behind the one risky item. Consequence accepted: `redis-cli
    monitor` still fails against Moon until that task lands, and the milestone's "a monitoring
    agent works unmodified" goal is not met by this task alone.
  ⚠ **[contract] The 10-field COMMAND spec shape is what clients actually parse.** Now the bundle's
    lowest-confidence point. Redis 7+ emits 10 fields (tips, key_specs, subcommands added in 7.0);
    pre-7 clients expect 6. Emitting 10 with the last three empty is what redis-server 8.6.1 itself
    does for commands with no tips or key specs — measured, not assumed — so it should satisfy
    both. Confirmed as the choice at freeze (Tin Dang). Residual risk: a client that hard-indexes
    field 7 misreads it, and a subtly-wrong command map is worse than the current empty array
    because a client can detect empty but cannot detect wrong. Mitigation: the compat harness
    diffs Moon's reply against redis-server's field by field for the same command, so a shape
    divergence fails a test rather than reaching a driver.
  - [spec] `RESET` reverting RESP3 -> RESP2 is intended, not a redis-server quirk — verified
    empirically against 8.6.1 above rather than read from docs. Cost if wrong: a RESP3 client that
    issues RESET silently starts receiving RESP2 frames; but this is measured behavior, so
    matching it is the compat-preserving choice regardless of intent.
  - [spec] Reporting 271 for `COMMAND COUNT` (vs Redis's 274) is correct behavior, not a gap to
    close in this task. Confirm: a driver that requires a specific command to exist checks
    `COMMAND INFO <name>`, not the count.
</assumptions>

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: COMMAND COUNT reports the registry size as an integer
  Given a running server
  When the client sends COMMAND COUNT
  Then the reply is a RESP Integer equal to the registry size
  And it is not an Array

Scenario: bare COMMAND returns one spec per registered command
  Given a running server
  When the client sends COMMAND
  Then the reply is an Array whose length equals the COMMAND COUNT integer
  And each element is a 10-element Array whose first field is the lower-cased name

Scenario: COMMAND INFO answers per requested name, in order
  Given a running server
  When the client sends COMMAND INFO GET nosuchcmd SET
  Then the reply is a 3-element Array
  And element 0 names "get", element 1 is Null, element 2 names "set"
  And the registry is unchanged

Scenario: COMMAND GETKEYS extracts keys from the registry key spec
  Given a running server
  When the client sends COMMAND GETKEYS MSET k1 v1 k2 v2
  Then the reply is the Array [k1, k2]

Scenario: COMMAND GETKEYS refuses a keyless command
  Given a running server
  When the client sends COMMAND GETKEYS PING
  Then the reply is the error "ERR The command has no key arguments"
  And no other reply is emitted

Scenario: COMMAND COUNT rejects an extra argument
  Given a running server
  When the client sends COMMAND COUNT extra
  Then the reply is "ERR wrong number of arguments for 'command|count' command"
  And the connection stays usable

Scenario: ROLE reports master with its replication offset
  Given a server with no master configured
  When the client sends ROLE
  Then the reply is a 3-element Array starting with "master"
  And element 2 is an Array of connected replicas

Scenario: ROLE reports slave on a replica
  Given a server that has been made a replica of another
  When the client sends ROLE
  Then the reply starts with "slave" and carries the master host and port
  And it agrees with the role reported by INFO replication

Scenario: RESET returns the connection to default state
  Given a connection that has sent HELLO 3, SELECT 5, CLIENT SETNAME bob, WATCH k, and MULTI
  When the client sends RESET
  Then the reply is the Simple String "RESET"
  And a subsequent EXEC replies "ERR EXEC without MULTI"
  And CLIENT GETNAME replies Null, the selected db is 0, and the protocol is RESP2

Scenario: RESET rejects arguments
  Given a running server
  When the client sends RESET now
  Then the reply is "ERR wrong number of arguments for 'reset' command"
  And the connection state is unchanged

Scenario: HELLO agrees with INFO about role and mode
  Given a server that has been made a replica of another
  When the client sends HELLO 3
  Then the reply map's "role" field is "slave"
  And it matches the role: line of INFO replication on the same connection

Scenario: CLIENT INFO reports the real local port
  Given a client connected to the server's listening port
  When the client sends CLIENT INFO
  Then the laddr field carries that listening port
  And it is not port 0

```

</scenarios>

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

```
COMMAND                       -> Array[N] of spec, N == command_count()
COMMAND COUNT                 -> Integer(command_count())
COMMAND LIST                  -> Array[N] of BulkString(name)
COMMAND INFO <name>...        -> Array[len(names)] of (spec | Null)
COMMAND DOCS [<name>...]      -> Array of (BulkString(name), Map(doc))
COMMAND GETKEYS <cmd> <args>  -> Array of BulkString(key)
  errors -> "ERR wrong number of arguments for 'command|count' command"
          | "ERR The command has no key arguments"
          | "ERR Invalid command specified"
          | "ERR Invalid number of arguments specified for command"

spec := Array[10] [ BulkString(name.to_lowercase()), Integer(arity),
                    Array(flag SimpleStrings), Integer(first_key),
                    Integer(last_key), Integer(step),
                    Array(acl-category SimpleStrings), Array(tips)=[],
                    Array(key_specs)=[], Array(subcommands)=[] ]
  source: COMMAND_META — no second table

ROLE  -> master:  Array[3] [ "master", Integer(offset), Array(replica triples) ]
      -> replica: Array[5] [ "slave", host, Integer(port), state, Integer(offset) ]
  source: ReplicationState.role — the same value build_info_replication reads

RESET -> SimpleString("RESET")
  after: in_multi=false, command_queue cleared, watched_keys cleared,
         tracking disabled + untracked, subscriptions exited, selected_db=0,
         client_name=None, protocol_version=2
  errors -> "ERR wrong number of arguments for 'reset' command"

HELLO -> unchanged shape; "role" from ReplicationState.role,
         "mode" = "cluster" when cluster_state is enabled else "standalone"

CLIENT INFO/LIST -> unchanged shape; laddr carries the real local SocketAddr

Schema: no persistent state, no server-global state. COMMAND_META (static phf)
        is read-only. Per-connection: the RESET surface listed above.

Out of contract (split to `monitor-command-feed`): MONITOR.
```

Status: FROZEN @ v2

### Amendments v1 -> v2 (recorded, not silently edited)

**v1 said a replica's HELLO answers `role: slave`. It does not — it answers `role: replica`.**

Found at build time when ci12 failed against a CORRECT implementation. Rather than edit the test
to agree with my code, I measured a real replica pair on redis-server 8.6.1:

| surface | word Redis uses for the SAME fact |
|---|---|
| `HELLO` | `role: replica` |
| `INFO replication` | `role:slave` |
| `ROLE` | `slave` |

Three vocabularies, one fact, deliberately. v1's §1 Must and the §3 contract both stated `slave`
for HELLO, so the frozen contract was factually wrong about the parity target this whole task
exists to hit. Corrected here; `identity::hello_role_and_mode` returns `replica` and
`identity::role` returns `slave`, which is what the measurement shows.

ci12 was corrected in the same direction and made STRONGER rather than weaker: it now asserts all
three surfaces agree the node is a replica, each in its own vocabulary, plus a negative assertion
that HELLO no longer claims `master`. A test that had been changed to match the implementation
would be the cardinal sin here — the distinction is that independent measurement, not the code,
decided which side was wrong.

Least-sure flag surfaced at freeze: [contract] the 10-field `COMMAND` spec shape. Redis 7+ emits
10 fields; pre-7 clients expect 6. Emitting 10 with tips/key_specs/subcommands empty is what
redis-server 8.6.1 was measured doing for commands that have none, so it should satisfy both — but
a client that hard-indexes field 7 misreads it, and a subtly-wrong command map is worse than the
current empty array, because a client can detect empty and cannot detect wrong. Confirmed as the
choice at freeze (Tin Dang); the compat harness diffs Moon's reply against redis-server's field by
field so a shape divergence fails a test rather than reaching a driver.

Second flag, RESOLVED at this freeze rather than carried: [spec] MONITOR was scoped into this task
before §0 showed it has no existing infrastructure, is a stream rather than a reply, is the only
item touching the per-command hot path, and is the only one exposing other clients' credentials.
Split to `monitor-command-feed` (Tin Dang, at freeze). Consequence accepted and recorded in §1.

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: one wire-level test per Must and per Reject in §1 (12 scenarios), each asserting
RAW RESP BYTES rather than a client library's rendering — `redis-cli` prints `:0` and `*0`
identically as "0", which is precisely how this defect survived to now. Parity legs on the monoio
and sharded handlers plus the inline fast path.

Plan (one test per scenario, asserting behavior not internals):
<test_plan>
  - ci1_command_count_is_an_integer: send COMMAND COUNT / assert the reply byte 0 is b':' and the
    value parses > 0 / assert byte 0 is not b'*'
  - ci2_bare_command_array_len_matches_count: send COMMAND then COMMAND COUNT / assert the array
    header count equals the integer / assert each element is a 10-field array
  - ci3_command_info_order_and_null_element: send COMMAND INFO GET nosuchcmd SET / assert *3,
    element 1 is exactly b"$-1" or b"_" per protocol / assert elements 0 and 2 name get and set
  - ci4_command_getkeys_extracts: send COMMAND GETKEYS MSET k1 v1 k2 v2 / assert [k1, k2]
  - ci5_command_getkeys_keyless_rejects: send COMMAND GETKEYS PING / assert the exact error text /
    assert a following PING still replies +PONG
  - ci6_command_count_arity_rejects: send COMMAND COUNT extra / assert the 'command|count' error
  - ci7_command_list_names: send COMMAND LIST / assert the array contains "get" and "reset"
  - ci8_role_master_shape: send ROLE / assert *3, "master", integer offset, array of replicas
  - ci9_role_replica_agrees_with_info: REPLICAOF a second server / assert ROLE starts "slave" and
    its host/port match INFO replication's master_host/master_port
  - ci10_reset_returns_default_state: HELLO 3, SELECT 5, CLIENT SETNAME bob, WATCH k, MULTI, RESET
    / assert +RESET / assert EXEC errors "without MULTI", CLIENT GETNAME is null, db is 0, and a
    bare HELLO now returns a RESP2 array header not a RESP3 map header
  - ci11_reset_arity_rejects: send RESET now / assert the arity error / assert MULTI still open
  - ci12_hello_role_matches_info: on a replica, HELLO 3 / assert role field is "slave" / assert it
    equals INFO replication's role on the same connection
  - ci13_client_info_laddr_real_port: connect on a known port / assert laddr ends with that port /
    assert it is not ":0"
</test_plan>

Tests live in: `tests/client_identity_introspection.rs` · MUST run red (missing implementation)
before Build.

### Red run recorded — `cargo test --release --test client_identity_introspection` @ec0c4650
**0 passed; 11 failed; 2 ignored.** Every failure is the shape defect, not a missing server:

| test | red reason |
|---|---|
| ci1 | `*0\r\n` where an Integer belongs |
| ci2 | bare `COMMAND` is `:0` — no specs to count |
| ci3 | `*0` instead of one element per requested name |
| ci4 | `*0` — extracts no keys |
| ci5 | `*0` instead of "The command has no key arguments" |
| ci6 | arity unenforced |
| ci7 | `*0` — names nothing |
| ci8 | `-ERR unknown command 'ROLE'` |
| ci10 | `+QUEUED` — see below |
| ci11 | arity never reached (unknown command) |
| ci13 | `laddr=127.0.0.1:0` |

Two required scrutiny rather than acceptance:
- **ci3 first failed with "server never answered PING"** — a spawn-contention artifact from 11
  servers starting at once, NOT the defect. Re-run alone it fails correctly with `*0` vs `*3`.
  A red suite is only evidence if each test is red for ITS OWN reason, so this was checked rather
  than counted.
- **ci10 failed with `+QUEUED`, not "unknown command"** — because the test opens MULTI first. Red
  either way, but the reason mattered: it exposed that `RESET` must be intercepted BEFORE queueing
  (added to §1 Must) and, separately, that Moon queues unknown commands where Redis rejects them
  (recorded in §0 as out of scope, owned by `multi-exec-queue-semantics`).

The RESP reader is deliberately length-aware rather than a single bounded read: the bare `COMMAND`
reply is one element per registered command and spans many reads, so a chunk-reader would assert
against a truncated reply and could go green or red for reasons unrelated to the server.

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/command/connection.rs` `src/command/metadata.rs` `src/command/mod.rs`
`src/command/introspect.rs` `src/server/conn/core.rs` `src/server/conn/shared.rs`
`src/server/conn/handler_monoio/` `src/server/conn/handler_sharded/`
`src/server/conn/handler_single.rs` `src/client_registry.rs` `src/shard/dispatch.rs`
`src/shard/spsc_handler.rs` `tests/client_identity_introspection.rs`
`scripts/test-consistency.sh` `scripts/test-commands.sh` `CHANGELOG.md`

Strategy (ordered batches):
1. Red suite first — `tests/client_identity_introspection.rs`, every scenario, raw-RESP
   assertions, failing for the right reason (shape, not absence-of-server).
2. `src/command/introspect.rs` (new): build a spec Frame from a `CommandMeta`, and the
   `COMMAND` subcommand router. Keeps `connection.rs` from growing a second job and gives the
   registry-derived logic one home.
3. Wire `COMMAND` in `dispatch` AND `dispatch_read` AND the inline path — all three, per the
   three-dispatch-paths rule.
4. `RESET`: register the arm on all three paths; the state reset itself is one method on
   `ConnectionState` so no path can implement a partial reset.
5. `ROLE` + HELLO role/mode from `ConnectionContext.repl_state` / `cluster_state`; delete the
   hardcoded literals so there is no second source left to drift.
6. `CLIENT INFO` laddr from the real local address.
7. Consistency/command script entries; CHANGELOG.

Safety rule (feature-specific): `RESET` must reset EVERY field named in §3, from ONE method on
`ConnectionState` that all three paths call. A per-path reset arm is how this surface drifted in the
first place — a partial reset that leaves tracking registered or the watch set populated is a
correctness bug that presents as a phantom invalidation on the NEXT connection to reuse the id.

Code lives in: `src/`
Constraints: do NOT change any test or the contract; no new crate; `COMMAND` replies are built on
the cold introspection path — `Vec::with_capacity` for result building is fine there, but nothing
in this task may add work to the per-command hot path; `Frame::Error` in dispatch, never `Result`.

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — 14 scenarios (ci1–ci14). Default suite 32 consecutive green runs at
      `--test-threads=13`; the replica pair green under `--ignored`; ci14 green under the tokio
      feature set. Full `cargo test --release` and full tokio CI-parity suite re-run after the
      `shared.rs` refactor.
- [x] coverage did not decrease — one new test file, no test deleted or weakened. ci12 was
      STRENGTHENED mid-build (see the v1→v2 amendment): it now asserts all three role surfaces
      agree, plus a negative assertion.
- [x] no test or contract was altered during build — with one recorded exception that went the
      OTHER way: §1/§3 were amended at v2 because independent measurement against redis-server
      8.6.1 proved the FROZEN CONTRACT wrong about HELLO's vocabulary, not the code. The test was
      made stronger, never matched to the implementation.
- [x] the green was EARNED — proven by one-variable A/Bs, not by reading the diff:
      · stub `COMMAND COUNT` → ci2 red.
      · remove the `handler_single` ROLE/RESET intercepts → ci14 red with
        `-ERR unknown command 'ROLE'`; restore → green.
      · revert the `handler_single` HELLO role to the hard-coded `("master","standalone")` →
        ci12 STAYED GREEN. That negative result is what exposed the coverage hole below, and is
        recorded because it disproved a claim I had already made.
- [x] concurrency / timing of the risky operation is safe — no lock is held across an `.await`
      in the new code; `try_handle_reset` takes the tracking and pubsub locks in separate,
      non-overlapping scopes. The suite's own flake was root-caused rather than muted (below).
- [x] no exposed secrets, injection openings, or unexpected dependencies — no new dependency.
      `COMMAND` replies are derived from a compile-time `phf` map, so no user input reaches them.
      RESET's ACL identity is restored through `restore_migrated_state`, the same function
      connection setup uses, so it cannot restore MORE privilege than a fresh connection has.
- [x] layering & dependencies follow CONVENTIONS.md — reply construction lives in
      `src/command/`, handler wiring in `src/server/conn/`; the shared RESET body is in
      `shared.rs` so no handler owns a private copy.
- [ ] a person reviewed and approved the change — PR pending.

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] `COMMAND COUNT` is an Integer whose value equals the registry size — confirmed on the wire:
      `:263`, matching `COMMAND_META`. (§0's claimed 271 was a bad `grep`; corrected on the record.)
- [x] The same four verbs answer identically on the INLINE fast path and the RESP-array path —
      confirmed by raw `/dev/tcp` probes sending each form to the same server: `ROLE`, `COMMAND
      COUNT`, `RESET` and the arity error are byte-identical either way. This mattered: the inline
      path is where the #457 ACL bypass hid.
- [x] A real replica reports itself as a replica in all three of Redis's vocabularies — confirmed
      against a live master/replica pair: HELLO `role: replica`, `ROLE` → `slave 127.0.0.1 7401
      connect 0`, `INFO replication` → `role:slave`.
- [x] `RESET` executes immediately inside MULTI instead of queueing — confirmed on the wire:
      `MULTI` `+OK`, `RESET` `+RESET`, `EXEC` `-ERR EXEC without MULTI`.
- [x] `RESET` moves the WIRE codec, not just the connection struct — confirmed by `HELLO 3`
      then `RESET` then `PING` on one connection: the `PING` reply comes back in RESP2.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — this was the deep check that actually earned its keep, and it found a real
      gap AFTER I had already claimed the task was wired. Moon has THREE connection handlers plus
      an inline fast path. `COMMAND` routes through `dispatch`/`dispatch_read` (shared by all),
      but `ROLE` and `RESET` cannot: their answers live on `ConnectionContext`/`ReplicationState`,
      not in the `Database` that `dispatch()` receives, so each handler needs its own intercept.
      `handler_monoio` and `handler_sharded` were wired; `handler_single` was NOT — it had
      neither, and my first patch there hard-coded HELLO's role to `("master","standalone")` with
      a comment asserting the path "has no replication state by construction". That comment was
      false: `listener.rs:315` passes `Some(rs)`. Confirmed by grepping every call site rather
      than trusting the claim.
- [x] REACHABILITY (code) — and the correction to the above: `handler_single` is not reachable
      from the shipped binary at all. `main.rs` and `embedded.rs` BOTH call `run_sharded` →
      `handler_sharded`; the only caller of `listener::run_with_shutdown` (the sole path to
      `handler_single`) is a handful of in-process test suites. So my `--shards 1` tokio probes,
      which I had described as exercising `handler_single`, were exercising `handler_sharded`.
      The A/B above is what caught this: ci12 stayed green with the bug reinstated, which is only
      possible if no spawned-process test reaches that handler. Kept the fix rather than reverting
      it — a third copy of this surface silently drifting is the exact disease this task treats —
      and closed the blind spot with ci14, which drives `run_with_shutdown` in-process.
- [x] DEAD-CODE (code) — no orphaned symbol. `introspect.rs` and `identity.rs` are both reached
      from all wired paths; `PubSubTeardown` has exactly two impls and both are used
      (`RwLock` for the two context-based handlers, `Mutex` for `handler_single`).
- [x] SEMANTIC (prose) — `scripts/test-consistency.sh` and `scripts/test-commands.sh` read in
      full where edited; the new sections use `$PORT_RUST`, this repo's actual variable, after a
      first draft referenced a `$MOON_PORT` that does not exist and would have silently asserted
      against an empty string.

### GATE RECORD
Outcome: PASS (pending human review on the PR)
Reviewed by: <pending> · date: 2026-08-12

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): `COMMAND COUNT` vs registry size (they can only diverge if
someone reintroduces a hand-maintained table); the `role` field across HELLO / `INFO replication`
/ `ROLE` on any node that changes role.

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.
<!-- e.g.  - [DDD · open] the model missed multi-tenancy (evidence: scenario_x failed) -->

- [SPEC · seeded] response batches are encoded in the protocol in effect at FLUSH time, so a
  protocol-changing command retro-encodes earlier replies in the same batch. `HELLO 3` alone
  returns `%7`; `HELLO 3` + `HELLO 2` in ONE write returns `*14`, the RESP2 flattening. Filed as
  task `batch-protocol-version-fidelity` with the full measurement table. Pre-existing — the
  `HELLO`+`HELLO` reproducer touches none of this task's code — but RESET, which reverts the
  protocol by contract, adds a second trigger, which is how it surfaced here.
- [TDD · open] The client-compat harness caught a regression that FOURTEEN raw-RESP tests and
  two full suites missed: `ROLE` intercepted at the connection layer ran ahead of the MULTI
  queueing step, so `MULTI; ROLE; EXEC` executed ROLE at queue time and returned `*0` — the
  command vanished from the EXEC array and shifted every later result index. Nothing in this
  task's own suite exercised a connection-layer command INSIDE a transaction. Lesson: a new
  intercept must be tested in MULTI as well as standalone, because the intercept's POSITION
  relative to queueing is the thing that can be wrong. Fixed by moving ROLE into the shared
  dispatch table (answered from the process-global replication handle `INFO` already uses),
  which also deleted all three handler intercepts.
- [ADD · open] A harness test that borrows a live defect as its fixture punishes the fix. This
  task retired the third such fixture (`COMMAND COUNT`); the durable hook was already designed
  and filed as #461, so it was implemented here rather than rotating to a fourth defect.
- [SPEC · open] `COMMAND INFO`'s 10-field SHAPE now matches Redis, but three inner divergences
  remain and are now WAIVED with owners rather than unmeasured: acl_categories is thin
  (`@string` where Redis says `@read @string @fast`), key_specs is empty, and under RESP3 Redis
  types flags/acl_categories as Sets and key_specs entries as Maps where Moon emits Arrays.
- [SPEC · open] `handler_single` is dead weight in the shipped binary (no non-test caller) yet
  carries a full command loop that must be kept in parity with two live handlers. Either wire it
  to something real or delete it; a third copy nobody executes is where drift hides. This task
  paid that tax directly.
- [SPEC · open] `ServerConfig::default()` yields `databases: 0`, because the sane values live in
  clap `default_value_t` attributes that apply only to CLI parsing. The handler then indexes
  `db[0]` of an empty slice and PANICS on the first command. In-process harnesses hit this;
  whether `--databases 0` is rejected at the CLI was not checked.
- [TDD · open] A test can pass for a path it never reaches. ci12 stayed green with the
  `handler_single` HELLO bug reinstated, because every spawned-process test in this file reaches
  only `handler_sharded`/`handler_monoio`. The A/B, not the green badge, is what proved coverage.
  Lesson: for a fix on handler N, revert it and demand a specific RED before believing the green.
- [TDD · open] The suite's own flake was a thundering herd, not a slow server: 13 servers
  initialising data dirs at once on an external volume left one unable to answer PING inside 30s
  (~1 run in 8). Rejected a timeout bump (slower AND still flaky under heavier load) and a shared
  `OnceLock` server (statics are never dropped, so the guard would leak a live moon past exit).
  Serialising STARTUP only — test bodies still parallel — took the suite to 32/32 green in ~0.9s.
- [ADD · open] The scope gate is a whole-tree byte snapshot with no git awareness, so unrelated
  merges and `target/` rebuilds read as "this task touched it" (7020 files). The task's git file
  list is the real evidence; a re-taken baseline proves nothing and was recorded as such.
