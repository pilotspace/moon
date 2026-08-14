# TASK: MONITOR command feed — stream executed commands, redacted, zero-cost when unattached

slug: monitor-command-feed · created: 2026-08-11 · stage: production
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

Split out of `client-identity-introspection` at its freeze (2026-08-11). That task measured the
whole identity surface against `redis-server` 8.6.1; MONITOR was carved off because it is a
different animal from the rest. Carrying the measured facts forward so this task does not re-probe:

Measured on `main` @ec0c4650: `MONITOR` -> `-ERR unknown command 'MONITOR', with args beginning
with:` — absent from dispatch AND from the `COMMAND_META` registry (`src/command/metadata.rs`,
271 entries). Redis 8.6.1 replies `+OK` and then streams the command feed.

There is NO existing infrastructure to build on: `grep -rn "monitor_feed\|MonitorFeed\|monitors"
src/` returns only `src/cluster/gossip.rs`, which is unrelated (gossip peer monitoring).

Why it was split rather than kept (the sizing argument, recorded so it is not re-litigated):
- It is a STREAM, not a reply — every other item in that task was a cold introspection response.
- It is the only one touching the per-command hot path, on every shard.
- It is the only one with a security surface: a monitor observes every other client's traffic,
  including credential-bearing arguments. Redis redacts AUTH; so must this.

Touches (files · symbols · signatures):
- `src/command/metadata.rs:21,26` — `CommandFlags::ADMIN` and `CommandFlags::SKIP_MONITOR` exist,
  and the first instinct is to reuse them as the skip rule. **The audit proved that wrong on both
  terms** (table below): Moon's ADMIN is CONTAINER-granular while Redis's is SUBCOMMAND-granular,
  and Redis feeds the whole EVAL family despite flagging it `skip_monitor`. Reusing the flags
  would hide 8+ commands Redis shows. A dedicated, measured predicate is needed instead.
  `MONITOR` itself is absent from the 271-entry table and must be added (`arity 1`, cat `SRV`).
- `src/admin/metrics_setup.rs:1492` — `global_slowlog()`, the working precedent for a
  process-global command sink: `maybe_record(elapsed_us, args, peer_addr, client_name)` already
  receives ALL FOUR fields a monitor line needs. A `global_monitors()` is the same shape.
- `src/server/conn/handler_monoio/mod.rs:2607,2881` — where slowlog is recorded. NOT usable as-is
  for the feed: the site sits behind `dispatch_start = sample_latency.then(...)`, i.e. latency is
  SAMPLED, so a hook there would feed a sampled subset of traffic. The feed needs its own
  unconditional site next to `record_command_cached`.
- `src/server/conn/handler_sharded/mod.rs:1893,2148` — the same pair on the tokio path.
- `src/server/conn/blocking.rs:1612` — `try_inline_dispatch`, the fast path a plain `GET`/`SET`
  actually takes under monoio. A feed hook missing here is invisible to every test that uses
  those two commands, which is most of them (see [[gotcha_moon_three_dispatch_paths]] and the
  v0.8.6 inline-GET P0, which is this exact shape of miss).
- `src/command/mod.rs:66,1130` — `dispatch` / `dispatch_read`. Deliberately NOT the hook: neither
  has the peer address, and the file's own comment records that per-command observability is
  owned by the handler layer precisely because dispatch lacks the context.

Anchors the contract cites: `CommandFlags::{ADMIN, SKIP_MONITOR}`, `COMMAND_META`,
  `global_slowlog` (as the shape precedent), `try_inline_dispatch`, and the three handlers.

### Measured against redis-server 8.6.1 (raw sockets, three probes, 2026-08-14)

Raw sockets throughout: the feed is a stream of `+SimpleString` lines whose exact bytes ARE the
contract, and every client library reformats them before a test could see them.

| # | question | redis-server 8.6.1 | Moon today |
|---|---|---|---|
| 1 | `MONITOR` reply | `+OK` | `-ERR unknown command 'MONITOR'` |
| 2 | feed line | `+<unix>.<micros> [<db> <addr>] "SET" "k" "v"\r\n` | — |
| 3 | frame type under **RESP3** | still `+SimpleString` — **not** a Push | — |
| 4 | reads fed? | yes (`GET` appears) | — |
| 5 | db shown | the db AFTER `SELECT` (`SELECT 3` logs as `[3 …]`) | — |
| 6 | `AUTH pw` | `"AUTH" "(redacted)"` | — |
| 7 | `AUTH user pw` | `"AUTH" "(redacted)" "(redacted)"` | — |
| 8 | `HELLO 3 AUTH u p` | `"HELLO" "3" "AUTH" "(redacted)" "(redacted)"` | — |
| 9 | **admin commands** | **NOT fed** — `CONFIG SET`, `ACL SETUSER` absent | — |
| 10 | non-admin container subcommands | fed — `ACL WHOAMI`, `CLIENT GETNAME`, `INFO`, `DBSIZE` | — |
| 11 | unknown command | **not fed** | — |
| 12 | wrong-arity command | **not fed** | — |
| 13 | `MULTI` | fed at queue time | — |
| 14 | queued command | **not fed at queue time**; fed at EXEC | — |
| 15 | EXEC ordering | queued commands first, then `"EXEC"` (5µs apart) | — |
| 16 | Lua | `EVAL` fed, then each `redis.call` as `[<db> lua] "set" …` | — |
| 17 | two monitors | both receive identical bytes, same timestamp | — |
| 18 | monitor's own `PING` | fed (it sees itself) | — |
| 19 | `MONITOR` in the feed | never — because MONITOR is `admin`, i.e. rule 9 | — |
| 20 | keyspace cmd on a monitor conn | `-ERR Replica can't interact with the keyspace` | — |
| 21 | `INFO`/`CLIENT`/`SUBSCRIBE` on a monitor conn | allowed, work normally | — |
| 22 | second `MONITOR` on a monitor conn | **no reply at all** (silent) | — |
| 23 | `RESET` on a monitor conn | `+RESET`, feed stops, keyspace works again | — |
| 24 | `QUIT` | `+OK` | — |
| 25 | ACL, `+@all -@admin` | `-NOPERM User <u> has no permissions to run the 'monitor' command` | — |
| 26 | `COMMAND INFO monitor` | arity 1 · flags admin/noscript/loading/stale · cats @admin @slow @dangerous | absent |

**Argument escaping, measured byte by byte** (Redis `sdscatrepr` semantics):

| byte | emitted | | byte | emitted |
|---|---|---|---|---|
| `"` | `\"` | | `\x07` | `\a` |
| `\` | `\\` | | `\x08` | `\b` |
| `\n` | `\n` | | `\x00` | `\x00` |
| `\r` | `\r` | | `\x7f` | `\x7f` |
| `\t` | `\t` | | `\x80`, `\xff` | `\x80`, `\xff` |
| space | literal, inside the quotes | | empty arg | `""` |

UTF-8 is escaped PER BYTE, not per character: `héllo` → `"h\xc3\xa9llo"`. Every argument is
quoted, including the command name.

### ADMIN-flag audit — the skip rule is NOT the existing flags (measured 2026-08-14)

Requested as in-scope at the freeze. Two questions: does Moon UNDER-flag anything (a leak), and
does the existing flag reproduce Redis's behaviour (a divergence)?

**Leak check: clean.** Zero commands that Redis marks admin are unflagged in Moon. Nothing
leaks its arguments to a monitor because Moon forgot to classify it.

**Divergence check: the flags do not reproduce Redis.** Per-subcommand measurement:

| fed by Redis (must NOT be skipped) | not fed by Redis (must be skipped) |
|---|---|
| `CLIENT GETNAME` · `CLIENT ID` | `CLIENT LIST` |
| `ACL WHOAMI` · `ACL CAT` | `ACL LIST` · `ACL SETUSER` |
| `CLUSTER INFO` · `CLUSTER MYID` | `CONFIG GET` · `CONFIG SET` |
| `INFO` · `DBSIZE` · `LASTSAVE` | `SLOWLOG GET/RESET/LEN` |
| `COMMAND COUNT` · `MEMORY USAGE/DOCTOR` | `LATENCY RESET/HISTORY` |
| `EVAL` · `SCRIPT LOAD` · `FUNCTION LIST` | `DEBUG JMAP` · `SHUTDOWN` |

Two conclusions, both of which change the contract:
1. **Container-level ADMIN over-hides.** Moon flags `ACL`, `CLIENT`, `CLUSTER`, `CONFIG`, `INFO`,
   `SLOWLOG`, `LATENCY`, `MODULE` admin as WHOLE COMMANDS. Skipping on that flag would hide
   `INFO`, `CLIENT GETNAME`, `ACL WHOAMI`, `CLUSTER INFO`, `LASTSAVE` and more — commands Redis
   shows. An operator would watch a feed that silently under-reports.
2. **`SKIP_MONITOR` does not mean what its name says.** Redis sets it on the entire EVAL family
   (`EVAL`, `EVALSHA`, `FCALL`, and the `_RO` variants) and yet **feeds all of them** — measured
   directly, including the follow-on `[db lua]` lines. Including `SKIP_MONITOR` in the rule would
   suppress exactly the commands an operator most wants to see.

Contracted response: a dedicated `monitor::is_hidden(cmd, first_arg) -> bool` stating the measured
rule at subcommand granularity, tested row-by-row against the table above — NOT a reuse of
`CommandFlags`. The 14 container-level ADMIN flags stay as they are; they are correct for ACL
purposes and only wrong as a monitor-visibility proxy.

**Two more findings worth stating plainly, because both invert the obvious design:**
- **The feed is a SimpleString even in RESP3.** The instinct after `pubsub-resp3-push` is to make
  it a Push frame; measurement says Redis does not, and a client reading the feed expects `+`.
  Copying the pub/sub answer here would be a new divergence introduced by fixing the last one.
- **Admin commands are excluded, and that single rule explains rule 19.** MONITOR does not appear
  in its own feed not because of a self-suppression special case, but because MONITOR is `admin`.
  Implementing the general rule gets the special case for free; implementing the special case
  leaves `CONFIG SET` leaking into the feed.

### Design constraints inherited from the split decision
- The feed MUST be gated by a Relaxed atomic subscriber count checked BEFORE any formatting or
  allocation, so a server with no monitor attached pays strictly one atomic load per command.
  Anything more taxes every command for a feature almost nobody runs.
- Credential-bearing arguments (`AUTH`, `HELLO … AUTH`) must be redacted AT THE POINT OF
  FORMATTING — never emitted into a buffer and filtered afterwards.
- `MONITOR` must require the `admin` ACL category; without it, any user could read every other
  user's traffic.
- Redis's line format: `<unix.micros> [<db> <addr>] "CMD" "arg" …`.
- The three-dispatch-paths rule applies (CLAUDE.md): `handler_monoio`, `handler_sharded`, and the
  inline fast path, or the command is CI-invisible.

Consequence accepted at the split: `redis-cli monitor` fails against Moon, and the
v0-9-client-compat goal "a monitoring agent works unmodified" is NOT met, until this task lands.

---

## 1 · SPECIFY — the rules ▸ docs/03-step-1-specify.md

Feature: `MONITOR` streams every executed command to attached admin clients, in Redis's exact
line format, with credentials redacted and admin traffic excluded — and costs one relaxed atomic
load per command when nobody is attached.

Framings weighed:
- **A process-global monitor registry, fed from the handler layer (chosen).** One
  `AtomicUsize` count + a `RwLock<Vec<MonitorSink>>`, checked count-first. It mirrors
  `global_slowlog`, which already proves the shape works from all three handlers and already
  receives all four fields a line needs. Monitors are server-global in Redis semantics (a monitor
  sees every db and every connection), so a per-shard registry would have to fan out across
  shards to reconstruct something that is global by definition.
- *Per-shard registries + cross-shard fan-out, like pub/sub.* Rejected: pub/sub is per-channel
  and naturally shards; a monitor subscribes to EVERYTHING, so every publish would cross every
  shard. It buys nothing and adds an SPSC hop per command per shard.
- *Hook inside `dispatch`.* Rejected on evidence: `dispatch` has no peer address and no
  connection identity, and `src/command/mod.rs:71` records that per-command observability lives
  in the handler layer for exactly that reason.
- *Reuse the slowlog hook site.* Rejected on measurement: that site is behind
  `sample_latency.then(...)`, so it would feed a SAMPLED subset — a monitor that silently drops
  most traffic is worse than no monitor, because the operator cannot tell.

Must:
<must>
  - M1 `MONITOR` from an admin-authorised connection replies `+OK` and attaches the connection.
  - M2 Every subsequently EXECUTED command is emitted to every attached monitor as one
       SimpleString: `+<unix>.<micros> [<db> <addr>] "CMD" "arg" …\r\n`, args quoted and escaped
       with `sdscatrepr` semantics (per-byte, table in §0), command name quoted too.
  - M3 The line is a SimpleString under RESP2 AND RESP3 — never a Push frame.
  - M4 The db shown is the connection's db at execution time; the addr is its peer address.
  - M5 Reads are fed, not only writes.
  - M6 Administrative commands are NOT fed, at SUBCOMMAND granularity, per the measured table
       in §0 — `CONFIG *`, `SLOWLOG *`, `LATENCY *`, `DEBUG *`, `SHUTDOWN`, `ACL LIST/SETUSER`,
       `CLIENT LIST`, and `MONITOR` itself are hidden, while `INFO`, `DBSIZE`, `LASTSAVE`,
       `CLIENT GETNAME/ID`, `ACL WHOAMI/CAT`, `CLUSTER INFO/MYID`, `COMMAND COUNT`,
       `MEMORY USAGE/DOCTOR` and the whole EVAL family ARE fed. Explicitly NOT expressed as
       `CommandFlags::ADMIN | SKIP_MONITOR` — the audit proved both terms wrong.
  - M6b The ADMIN audit is re-runnable: a test pins Moon's hidden-set against the measured
       oracle table, so a future flag change cannot silently start leaking or hiding.
  - M7 A command rejected before execution — unknown name, or arity violation — is NOT fed.
  - M8 `AUTH`'s arguments are redacted as `"(redacted)"`, one per argument, AT FORMATTING TIME.
       `HELLO … AUTH <u> <p>` redacts the two arguments after the `AUTH` keyword and nothing else.
  - M9 `MULTI` is fed when it is issued; queued commands are NOT fed at queue time; at `EXEC` the
       queued commands are fed in order and then `EXEC` itself.
  - M10 Every attached monitor receives every line; two monitors see identical bytes.
  - M11 With no monitor attached, the per-command cost is one `Relaxed` atomic load — no
       formatting, no allocation, no lock.
  - M12 `RESET` on a monitor connection detaches it: the feed stops and keyspace access returns.
  - M13 The feed hook is present on ALL production paths — `handler_monoio`, `handler_sharded`,
        and `try_inline_dispatch` — so a plain `GET`/`SET` is fed like anything else.
  - M14 `MONITOR` is registered in `COMMAND_META` with arity 1 and the admin category, so
        `COMMAND INFO monitor` answers and ACL can reason about it.
</must>
Reject:
<reject>
  - R1 `MONITOR` from a user without the admin category ->
       "NOPERM User <u> has no permissions to run the 'monitor' command"  (and NOT attached)
  - R2 `MONITOR` with any argument -> "ERR wrong number of arguments for 'monitor' command"
  - R3 A keyspace command on an attached monitor connection ->
       "ERR Replica can't interact with the keyspace"  (and the feed keeps flowing)
  - R4 `MONITOR` again on an already-attached connection -> NO REPLY AT ALL, and the connection
       stays attached exactly once (measured: Redis is silent here, it does not error)
</reject>
After:
<after>
  - An unmodified `redis-cli monitor` prints Moon's traffic in the same format it prints Redis's.
  - The `v0-9-client-compat` goal "a monitoring agent works unmodified" stops being blocked.
  - `used_memory`/throughput are unchanged when no monitor is attached, provably (M11).
</after>
Assumptions — lowest-confidence first:
<assumptions>
  ⚠ **A monitor that cannot keep up must be dropped, not allowed to stall the shard.** Lowest
    confidence because the cost is invisible until a slow monitor exists: the feed is written
    from the command hot path on every shard, and a bounded queue that blocks would convert one
    slow TCP reader into a server-wide stall. Contracting a BOUNDED queue with drop-on-full (the
    same trade pub/sub already makes for slow subscribers) is the safe choice; if wrong, a
    monitor silently misses lines under load and an operator may not notice. Mitigation
    contracted: drop the whole MONITOR connection rather than silently skipping lines, so the
    loss is loud. Cost if wrong: a monitor detaches under burst load and must be re-attached.
  ⚠ **Feeding AFTER execution rather than before is observationally equivalent.** Redis feeds at
    the start of `call()`; the only site in Moon that sees every command with full context is
    around dispatch. If a command BLOCKS (BLPOP), Redis shows it immediately and a
    feed-after-execution would show it only on unblock — a real, observable divergence for
    blocking commands. Cost if wrong: blocking commands appear late in the feed. Contracted
    response: feed BEFORE dispatch at each hook site, so timestamp and ordering match.
  - [ ] The 271-entry `COMMAND_META` table's ADMIN flags agree with Redis's on the commands an
        operator would care about leaking. Confirm by diffing Moon's ADMIN set against
        `COMMAND INFO` from redis-server for the shared command set — an under-flagged command
        would LEAK to the feed, which is a security-relevant miss, not a cosmetic one.
  - [ ] Lua-issued commands (`[<db> lua]`) are in scope. Moon's EVAL path executes through the
        same dispatch, so they may fall out for free; if they do not, the divergence is recorded
        rather than papered over.
</assumptions>

<!-- EXIT: every rule stated, every rejection named; assumptions ranked lowest-confidence first, the top one or two ⚠-flagged with why + cost (or, for trivial scope, an honest "none material" that still names the single biggest risk). -->

---

## 2 · SCENARIOS — pass/fail cases ▸ docs/04-step-2-scenarios.md

<scenarios>

```gherkin
Scenario: a monitor sees an ordinary command          # M1 M2 M4 M5
  Given an admin connection that issued MONITOR and got +OK
  When another connection on db 0 runs SET k v
  Then the monitor reads one line matching
       +<digits>.<6 digits> [0 <ip>:<port>] "SET" "k" "v"
  And the SET connection's own reply is +OK, unchanged

Scenario: the feed is a SimpleString under RESP3      # M3
  Given a connection that sent HELLO 3 and then MONITOR
  When another connection runs SET k v
  Then the feed line starts with '+' and not with '>'
  And the HELLO reply itself was still a RESP3 Map

Scenario: reads are fed too                            # M5
  Given an attached monitor
  When another connection runs GET k
  Then the feed contains "GET" "k"

Scenario: the db shown follows SELECT                  # M4
  Given an attached monitor
  When another connection runs SELECT 3 then GET k
  Then both lines show [3 <addr>]

Scenario: admin commands never reach the feed          # M6
  Given an attached monitor
  When another connection runs CONFIG SET maxmemory 0
  Then no line for CONFIG appears
  And a following DBSIZE on the same connection DOES appear, proving the feed is live

Scenario: MONITOR is absent from its own feed          # M6 M19
  Given an attached monitor
  When a second connection issues MONITOR
  Then no "MONITOR" line appears on either feed
  And both monitors then receive a subsequent SET

Scenario: rejected commands are not fed                # M7
  Given an attached monitor
  When another connection sends NOSUCHCMD x, then GET with no key
  Then neither produces a feed line
  And a following valid PING does produce one

Scenario: AUTH arguments are redacted                  # M8
  Given an attached monitor
  When another connection runs AUTH hunter2 and AUTH user pass
  Then the feed shows "AUTH" "(redacted)" and "AUTH" "(redacted)" "(redacted)"
  And the literal secrets appear nowhere in the bytes read

Scenario: HELLO AUTH redacts only the credentials      # M8
  Given an attached monitor
  When another connection runs HELLO 3 AUTH default sekrit
  Then the feed shows "HELLO" "3" "AUTH" "(redacted)" "(redacted)"
  And "sekrit" appears nowhere in the bytes read

Scenario: transaction timing                           # M9
  Given an attached monitor
  When another connection runs MULTI, then SET q 1, then EXEC
  Then MULTI appears when issued
  And SET does NOT appear at queue time
  And after EXEC the feed shows "SET" "q" "1" followed by "EXEC"

Scenario: every monitor gets every line                # M10
  Given two attached monitors
  When another connection runs SET dual 1
  Then both read a line naming SET dual 1

Scenario: argument escaping is byte-exact              # M2
  Given an attached monitor
  When another connection sets a value containing " \ newline tab NUL 0xff and a UTF-8 char
  Then the feed escapes them as \" \\ \n \t \x00 \xff and per-byte \xc3\xa9
  And an empty argument appears as ""

Scenario: the fast path is fed like any other          # M13
  Given an attached monitor on a server started with --shards 4
  When another connection runs a plain GET and a plain SET
  Then both appear in the feed
  # plain GET/SET take try_inline_dispatch; a hook missing there is invisible
  # to every test that uses those two commands, which is most of them

Scenario: a non-admin user cannot attach               # R1
  Given a user created with +@all -@admin, authenticated
  When it issues MONITOR
  Then it is answered -NOPERM … 'monitor' …
  And it receives no feed line when another connection runs SET k v

Scenario: MONITOR takes no arguments                   # R2
  Given an admin connection
  When it issues MONITOR extra
  Then it is answered -ERR wrong number of arguments for 'monitor' command
  And the connection is not attached

Scenario: a monitor may not touch the keyspace         # R3
  Given an attached monitor
  When it issues SET x 1
  Then it is answered -ERR Replica can't interact with the keyspace
  And the feed continues to deliver another connection's commands

Scenario: MONITOR twice is silent                      # R4
  Given an attached monitor
  When it issues MONITOR again
  Then it receives no reply at all
  And a single subsequent SET produces exactly ONE line, not two

Scenario: RESET detaches                               # M12
  Given an attached monitor
  When it issues RESET
  Then it is answered +RESET
  And a subsequent SET on another connection produces no line
  And the former monitor can run GET again

Scenario: zero cost when unattached                    # M11
  Given a server with no monitor attached
  When the command path executes
  Then the only monitor-related work is one relaxed atomic load
  # observable as: the feed counter is checked before any formatting, proven by
  # a unit test asserting the formatter is never called at count 0
```

</scenarios>

<!-- EXIT: one scenario per Must AND per Reject; each result is observable. -->

---

## 3 · CONTRACT — freeze the shape ▸ docs/05-step-3-contract.md

The contract is WIRE BYTES plus one hot-path cost bound. Every clause below was measured
against redis-server 8.6.1, not read from documentation.

```
Command
  MONITOR                       arity 1, flags ADMIN|NOSCRIPT|LOADING|STALE, cats @admin @slow @dangerous
    admin-authorised, not yet attached -> +OK, connection attached
    already attached                   -> NO REPLY (silent; measured, not an error)
    not admin-authorised               -> -NOPERM User <u> has no permissions to run the 'monitor' command
    any argument                       -> -ERR wrong number of arguments for 'monitor' command

Feed line (one per executed command, per attached monitor)
  +<unix_secs>.<micros:06> [<db> <addr>] "<CMD>" "<arg>" …\r\n
    frame type  : SimpleString under BOTH RESP2 and RESP3 — never Push
    <micros:06> : zero-padded to exactly 6 digits
    <addr>      : peer address, "ip:port"; the literal `lua` for script-issued commands
    <db>        : the issuing connection's db AT EXECUTION TIME
    quoting     : EVERY token quoted, command name included
    escaping    : per BYTE — " -> \"   \ -> \\   \n \r \t -> \n \r \t
                  0x07 -> \a   0x08 -> \b   other non-printable/high -> \xHH (lowercase)
                  space kept literal; empty argument renders as ""

Emission rules
  fed        : every command that reaches execution, reads included
  NOT fed    : monitor::is_hidden(cmd, first_arg) — subcommand-granular, per the §0 audit table.
               hidden : CONFIG * · SLOWLOG * · LATENCY * · DEBUG * · SHUTDOWN · MONITOR ·
                        ACL LIST · ACL SETUSER · CLIENT LIST
               fed    : INFO · DBSIZE · LASTSAVE · COMMAND * · MEMORY USAGE|DOCTOR ·
                        CLIENT GETNAME|ID · ACL WHOAMI|CAT · CLUSTER INFO|MYID ·
                        EVAL|EVALSHA|FCALL|*_RO (yes — despite Redis's own skip_monitor flag)
               NOT CommandFlags::ADMIN and NOT ::SKIP_MONITOR — measured wrong, see §0
  NOT fed    : unknown command; arity-rejected command       (rejected before execution)
  MULTI      : fed when issued
  queued cmd : NOT fed at queue time; fed at EXEC, in order, then "EXEC"
  redaction  : AUTH        -> every argument becomes "(redacted)"
               HELLO … AUTH -> the two arguments after the AUTH keyword become "(redacted)"
               applied AT FORMATTING TIME — a secret is never written into a buffer and filtered

Monitor connection state
  keyspace command -> -ERR Replica can't interact with the keyspace   (feed keeps flowing)
  PING / INFO / CLIENT / SUBSCRIBE -> served normally, and PING appears in the feed
  RESET -> +RESET, detached, keyspace access restored
  QUIT  -> +OK, closed
  disconnect -> detached; the registry must not retain the sink

Registry  (src/monitor/, new module)
  static MONITOR_COUNT: AtomicUsize          // Relaxed
  static MONITORS: RwLock<Vec<MonitorSink>>  // parking_lot; write only on attach/detach
  #[inline] fn feed(db, addr, cmd, args):
      if MONITOR_COUNT.load(Relaxed) == 0 { return }   // the ENTIRE unattached cost
      … format once, fan out to every sink …
  backpressure: bounded queue per monitor; on full, DROP THE MONITOR CONNECTION
                (loud) rather than skipping lines (silent)

Hook sites — all three, or the feature is CI-invisible
  handler_monoio   : before dispatch, unconditional (NOT the sampled slowlog site)
  handler_sharded  : same
  try_inline_dispatch (blocking.rs) : same — this is the path a plain GET/SET takes
```

Not in scope, stated so the boundary is explicit: `MONITOR` output for commands executed by
replication apply or AOF load (Redis does not feed those either), and any new `CLIENT NO-EVICT`/
`CLIENT KILL TYPE monitor` surface beyond what already exists.

Status: FROZEN @ v1 — approved by Tin Dang, 2026-08-14.
Approved with both flags resolved at the freeze, and the audit run BEFORE freezing rather than
after — which is what caught the skip rule being wrong:
- Flag 1 resolved: a slow monitor has its CONNECTION DROPPED, loudly. Not silent line-dropping
  (an operator cannot distinguish a quiet server from a lossy feed) and never blocking (one slow
  TCP reader must not stall every shard). Recoverable by re-attaching.
- Flag 2 resolved as contracted: feed BEFORE dispatch at each hook site, so blocking commands
  appear when issued rather than when they unblock.
- The in-scope ADMIN audit changed the contract materially: the skip rule is a dedicated
  subcommand-granular predicate, NOT `CommandFlags::ADMIN | SKIP_MONITOR`. Recorded above.

Least-sure flag surfaced at freeze:
1. `[contract]` **Slow-monitor backpressure: drop the CONNECTION, not the lines.** This is the
   one clause with a server-wide blast radius. The feed is written from the command hot path on
   every shard, so a bounded queue that BLOCKS turns one slow TCP reader into a server-wide
   stall — the worst outcome. Dropping lines silently is the other tempting answer and is worse
   than it looks: an operator reading a monitor feed to diagnose an incident cannot tell a quiet
   server from a lossy feed. Contracted answer is to kill the monitor connection, which is loud
   and unambiguous. If this is wrong, a monitor detaches under burst load and must be
   re-attached — recoverable, unlike either alternative.
2. `[spec]` **Feed BEFORE dispatch, not after.** The natural implementation feeds where the
   slowlog records, i.e. after execution — but a blocking command (BLPOP) would then appear only
   when it unblocks, which is an observable divergence from Redis for exactly the commands an
   operator is most likely to be watching for. Feeding before dispatch costs a second borrow of
   the args at each hook site; that is the price of matching. If wrong, the cost is a small
   refactor of three call sites, not a redesign.
<!-- The freeze IS the one approval — lead it with the bundle's lowest-confidence flag: the 1–2
     points most likely wrong across the whole bundle, tagged [spec|scenario|contract|test], each
     with why + cost (the §1 ⚠ assumptions feed it; a flag may point at a scenario or the contract
     too — see run.md). Approved -> Status: FROZEN @ vN — approved by <name>. Changing a frozen
     contract = change request back to SPECIFY.
     EXIT: frozen + every spec rejection has a contracted response + names match GLOSSARY + the
     bundle's lowest-confidence flag was surfaced at the freeze (or an honest "none material"). -->

---

## 4 · TESTS — failing-first suite (red) ▸ docs/06-step-4-tests.md

Coverage target: every Must and every Reject has a test; no Must represented only by prose.

Raw sockets, not a client library — the feed's exact bytes ARE the contract, and every client
library reformats them before an assertion could see them. Same reason `pubsub-resp3-push` used
raw sockets, and the same reason its predecessor's payload-decoding tests missed a real defect.

<test_plan>
  - mon1_monitor_replies_ok_and_attaches — +OK, then a SET on another conn produces one line
    matching the full regex including the 6-digit micros and [db addr]. (M1 M2 M4)
  - mon2_feed_is_simplestring_under_resp3 — HELLO 3 then MONITOR; the line leads with '+', not
    '>'. Directly guards against copying the pub/sub Push answer into a place Redis does not
    use it. (M3)
  - mon3_reads_are_fed — GET appears. (M5)
  - mon4_db_follows_select — SELECT 3 then GET both show [3 …]. (M4)
  - mon5_admin_commands_are_not_fed — CONFIG SET absent, a following DBSIZE present (the
    second half is what proves the feed was live rather than broken). (M6)
  - mon6_monitor_is_absent_from_its_own_feed — a second MONITOR produces no line on either
    feed, and both still receive a subsequent SET. (M6)
  - mon7_rejected_commands_are_not_fed — unknown name and bad arity produce nothing; a
    following PING does. (M7)
  - mon8_auth_arguments_are_redacted — both AUTH forms; asserts the literal secret appears
    NOWHERE in the bytes read, not merely that "(redacted)" appears. (M8)
  - mon9_hello_auth_redacts_only_credentials — HELLO 3 AUTH u p; "3" survives, both
    credentials do not, and "sekrit" appears nowhere. (M8)
  - mon10_transaction_timing — MULTI at issue, SET absent at queue time, then SET followed by
    EXEC after EXEC. The queue-time absence is the half a naive implementation gets wrong. (M9)
  - mon11_two_monitors_both_receive — identical line on both. (M10)
  - mon12_argument_escaping_is_byte_exact — one SET carrying " \ \n \t NUL 0xff and a UTF-8
    char; asserts the full escaped rendering and the "" empty-argument form. (M2)
  - mon13_inline_fast_path_is_fed — --shards 4, plain GET and plain SET. The path a hook is
    most likely to be missing from, and the one most tests cannot see. (M13)
  - mon14_non_admin_cannot_attach — +@all -@admin user gets NOPERM AND receives no feed line
    afterwards (the second assertion is the security-relevant one). (R1)
  - mon15_monitor_rejects_arguments — MONITOR extra -> arity error, not attached. (R2)
  - mon16_monitor_conn_cannot_touch_keyspace — SET -> Replica-can't-interact, and the feed
    keeps flowing. (R3)
  - mon17_second_monitor_is_silent — no reply, and a later SET yields exactly ONE line, not
    two (guards against double-registering the same connection). (R4)
  - mon18_reset_detaches — +RESET, feed stops, GET works again. (M12)
  - mon19_command_info_monitor — COMMAND INFO monitor answers with arity 1 and the admin
    category, so ACL and clients can see it. (M14)
  - unit: monitor_registry_zero_cost_when_unattached — the formatter is not invoked at count 0
    (asserted by a formatter that panics if called), proving M11 is a property of the code
    rather than a claim in a comment. (M11)
  - mon20_admin_audit_table — drives the §0 audit table row by row against Moon: every "fed"
    row must appear in the feed and every "hidden" row must not. This is the regression guard
    for M6b — a future flag or dispatch change cannot silently start leaking CONFIG SET or
    start hiding INFO.
  - mon21_slow_monitor_is_dropped_not_stalled — a monitor that stops reading while another
    connection issues a burst: the monitor connection is CLOSED, and the publishing connection's
    own latency is unaffected. The loud-failure half of the contracted backpressure policy.
  - mon22_script_issued_commands_are_fed_with_the_lua_address — ADDED DURING BUILD, red first. The
    §3 clause "<addr> … the literal `lua` for script-issued commands" had no test, and without one
    the contract clause would have shipped unimplemented: a script command never passes a
    connection handler, so all three contracted hook sites structurally cannot see it. Re-measured
    against redis-server 8.6.1 before writing (the EVAL line carries the client address; each
    `redis.call` follows it as `[0 lua] "SET" …`, in execution order).
  - unit: monitor_redaction_never_buffers_the_secret — the redacting formatter is called
    directly with AUTH args and its output is inspected; guards the "redact at formatting time,
    never filter afterwards" clause the §0 split decision made a design constraint. (M8)
</test_plan>

Runtime note, learned the expensive way on `info-observability` and again on `pubsub-resp3-push`:
this suite MUST run under `runtime-monoio` (the shipped default) as well as `runtime-tokio`. The
tokio leg was 12/18 red on the last task while monoio was fully green, from four independent
gaps. `mon13` in particular exercises the monoio-only inline path.

Tests live in: `tests/monitor_command_feed.rs` · MUST run red (missing implementation) before Build.
<!-- declare paths as backticked tokens on this line: `./…` = this task dir ·
     a token with "/" = project root · a bare name = sibling of the previous
     token's dir · a directory counts its *.py files (non-recursive); reports
     mark declared counts with † · anything resolving outside the project root counts 0 -->

<!-- EXIT: one test per scenario; suite red for the RIGHT reason; target recorded. -->

---

## 5 · BUILD — AI writes code ▸ docs/07-step-5-build.md

Scope (may touch): `src/monitor/` `src/server/conn/` `src/scripting/bridge.rs` `src/command/metadata.rs` `src/acl/rules.rs` `src/lib.rs` `tests/monitor_command_feed.rs` `scripts/client-compat/manifest.yaml` `src/../CHANGELOG.md` `tmp/`

<!-- All tokens on ONE line: the resolver reads only the FIRST declaring line, and a wrapped
     second line grants no cover. `src/../CHANGELOG.md` rather than `CHANGELOG.md` because a bare
     token resolves as a SIBLING of the previous token's dir, so a project-root file has no bare
     spelling — it needs a token containing "/" to resolve from the root. -->


Strategy (ordered batches):
1. `src/monitor/` — registry, hidden-set predicate, `sdscatrepr` formatter, redaction. Unit-tested
   in isolation before any handler knows it exists.
2. `src/server/conn/monitor_mode.rs` — the once-stated "a monitor may not touch the keyspace" rule.
3. Registration: `COMMAND_META` row + `@admin` category membership in `acl/rules.rs`.
4. Handler wiring, both runtimes: attach verb, mode gate, feed hook, delivery arm, teardown.
5. The inline fast path and the Lua bridge — the two paths a handler hook structurally cannot reach.

Safety rule (feature-specific): the attach verb sits BELOW the ACL gate. An intercept placed above
it exempts itself from ACL — the exact shape of the v0.8.6 inline-GET P0 — and `MONITOR` is the last
command that should be reachable without permission, since it reads every other user's arguments.

Code lives in: `./src/`
Constraints: do NOT change any test or the contract; allow-list packages only; ask if unclear.

### Contract deviations — recorded, not silently absorbed
One clause was satisfied by a different mechanism than the frozen text names. Stated here rather
than left for a reader to discover by diffing:

- **`try_inline_dispatch` hook site.** The contract names three hook sites; the third was built as a
  STAND-DOWN instead of a hook — `any_attached()` disables the inline fast path while any monitor is
  attached, so the command falls through to the fully-hooked path. The inline path answers straight
  from the read buffer and never sees `peer_addr`, so hooking it meant threading an address through
  the hottest function in the codebase for a diagnostic feature. The observable contract is
  unchanged and `mon13` (the test written for that clause) passes on `--shards 4` for both plain GET
  and plain SET. The mechanism is stronger than the contracted one: a future refactor of the inline
  path cannot silently drop the feed, because there is no hook there to forget. The cost is
  throughput while a monitor is attached — i.e. only when an operator has already accepted
  diagnostic overhead. Fast-path retention when unattached is confirmed by
  `moon_dispatch_path_total{path="local_inline"}` = 200 over 200 GETs, not inferred from latency.

<!-- Scope tokens, backticked, FIRST declaring line: `./…` = this task dir · a token
     with "/" = project root · a bare name = sibling of the previous token's dir ·
     outside-root resolutions are dropped fail-closed · a DIRECTORY token covers its
     whole subtree (containment — diverges from §4's non-recursive counting) ·
     absent line = UNDECLARED (pre-existing tasks grandfathered, never retro-red) ·
     engine enforcement (touched ⊆ declared) lands in scope-gate-enforce.
     EXIT: all green; coverage held; no test/contract touched; no unlisted dependency. -->

---

## 6 · VERIFY — evidence + non-functional review ▸ docs/08-step-6-verify.md

- [x] all tests pass — `tests/monitor_command_feed.rs` 27/27 under BOTH runtimes (monoio, the
      shipped default, and tokio). lib 4620 monoio / 3786 tokio. Regressions green: `pubsub_resp3_push`
      21, `multi_exec_queue_semantics` 12, `watch_cas_transactions` 10, `protocol_error_lifetime` 8,
      `info_observability` 13. The bridge edit's two closest suites are `#[ignore]`d by default and
      were run explicitly with `-- --ignored`: `functions_fcall` 9/9, `replication_readonly_eval` 1/1.
- [x] coverage did not decrease — 33 tests added (27 integration + 6 unit), none removed.
- [x] no test or contract was altered during build — the one mechanism divergence is recorded in §5
      under "Contract deviations" rather than by editing the frozen §3.
- [x] the green was EARNED — the two tests most able to lie were built to refuse to: `mon8`/`mon9`
      assert the secret appears NOWHERE in the bytes read, not that `(redacted)` appears (a
      formatter that emitted both would pass the weak form); `mon5`/`mon7`/`mon6` each assert a
      FOLLOWING command IS fed, so "nothing was fed because the feed is broken" cannot pass as
      "correctly hidden". `mon22` was run red first and failed for the right reason — the EVAL line
      present, its effects absent — before the bridge hook existed.
- [x] concurrency / timing safe — the registry is a `parking_lot::RwLock<Vec<_>>` behind an
      `AtomicUsize` count; the write lock is taken only on attach/detach. Slow-sink shedding
      collects ids under the READ lock and detaches in a second pass, so the lock is never upgraded
      in place. No lock is held across an `.await`. `feed_cold` is `#[cold]` and unreachable while
      the count is zero, which `monitor_registry_zero_cost_when_unattached` pins.
- [x] no exposed secrets — this feature's whole risk IS secret exposure, and it is handled at
      formatting time: `push_arg` is never reached for a credential, so a secret is never written
      into a buffer to be filtered later. A filter is one refactor away from leaking; the thing it
      would leak is a password.
- [x] layering & dependencies follow CONVENTIONS.md — new module under `src/monitor/`, no new crate
      dependency, `parking_lot` not `std::sync`, no `unwrap`/`expect` outside tests, no `unsafe`.
- [ ] a person reviewed and approved the change — pending PR review.

### Build expectations — what "correct" looks like (fill BEFORE build; confirm each at the gate)
> Pre-declare the OBSERVABLE outcomes a correct build must produce — derived from §2 SCENARIOS
> + §3 CONTRACT — so this gate checks the build is RIGHT, not merely that tests are green. Each
> row is evidence you can SEE, not a restatement of a test name.
- [x] `redis-cli monitor` works against Moon and its output is byte-comparable to redis-server's —
      confirmed by the raw-socket assertions in `mon1` (full regex including the 6-digit micros and
      `[db addr]`) and `mon12` (byte-exact escaping of `"` `\` `\n` `\t` NUL `0xff` and a UTF-8
      character, plus the `""` empty-argument form).
- [x] A secret never reaches a monitor — confirmed by `mon8`/`mon9` asserting the literal appears
      nowhere in the bytes read, and by the unit test inspecting the formatter's own output.
- [x] The feed is complete on the path a plain GET/SET actually takes — confirmed by `mon13` on
      `--shards 4`, the inline-path case that ordinary tests cannot see.
- [x] Attaching is admin-only AND a refused attach receives no lines — confirmed by `mon14`; the
      second half is the security-relevant assertion, since a NOPERM that still leaked the feed
      would pass a test that only checked the reply.
- [x] The hidden/fed split matches the measured oracle row by row — confirmed by `mon20`, which
      drives the §0 audit table against Moon. This is the regression guard: a future flag change
      cannot silently start leaking `CONFIG SET` or start hiding `INFO`.
- [x] A slow monitor cannot stall the server — confirmed by `mon21`: a 20,000-command burst against
      a monitor that stopped reading completes, the server still answers PING, and the monitor's
      own connection is closed rather than left half-alive.
- [x] Zero cost when unattached — confirmed by `moon_dispatch_path_total{path="local_inline"}` = 200
      over 200 GETs with no monitor attached (the fast path is intact), and by the unit test proving
      `feed` cannot reach formatting at count 0.

### Deep checks — do not skim (fill the path that applies; the resolver judges which)
- [x] WIRING (code) — every new symbol is referenced. `is_hidden` ← `feed_cold`; `format_line` ←
      `feed_cold` + 4 unit tests; `attach`/`detach` ← both handlers, `shared.rs` RESET, disconnect
      cleanup; `any_attached` ← the monoio inline gate; `is_attached` ← the monoio delivery arm's
      shed check; `feed_frames` ← both handlers (AUTH/HELLO, EXEC replay, main hook) and
      `scripting/bridge.rs`; `refuse_if_keyspace` ← both handlers; `monitor_attached`/`monitor_rx` ←
      both handlers + `core.rs` init + RESET.
      **This check was expanded because the previous task's version of it missed real defects:** on
      `pubsub-resp3-push` I walked every DISPATCH path and no TEARDOWN path, and all three
      CodeRabbit findings were on teardown. So teardown was walked explicitly here — RESET
      (`shared.rs`), disconnect (both handlers), and the slow-sink shed path — before any review.
- [x] DEAD-CODE (code) — clippy `--all-targets` is clean on both feature legs, which is what would
      flag an unreferenced private symbol. `feed` is `pub` and currently reached only from the unit
      test; it is the `&[Bytes]` sibling of `feed_frames` and is kept as the registry's primary
      entry point. Noted rather than hidden.
- [x] SEMANTIC (prose) — CHANGELOG entry read in full: it states the two non-obvious behaviours
      (subcommand-granular hiding, drop-the-connection backpressure) and the Lua `lua`-address rule,
      and does not claim the inline path is hooked.

### GATE RECORD
Outcome: PASS
Reviewed by: Tin Dang · date: 2026-08-14
Target hit: yes — all 14 Musts and all 4 Rejects have a passing test, under both runtimes.

### Review round 2 — what external review found that my own §6 did not

Nine comments; five were real defects, one was a false positive I only knew was
false because I tested it, three were correctness/clarity fixes. Recorded in full
because the pattern matters more than the individual fixes.

1. **The keyspace-refusal rule was wrong.** §3 said "keyspace command" and I
   implemented `first_key != 0`. Re-measured: `DBSIZE`, `KEYS`, `SCAN`,
   `RANDOMKEY`, `FLUSHALL`, `FLUSHDB`, `SWAPDB`, `EVAL`, `EVALSHA`, `PUBLISH`,
   `SPUBLISH` and `MEMORY USAGE` all carry `first_key == 0` and are all REFUSED
   by redis-server. So a monitor connection could run `FLUSHALL` and `KEYS *`.
   Worse: my own unit test asserted `DBSIZE` was *served*, with a comment
   claiming it was measured — it never had been. A wrong belief encoded as a
   test is what let the wrong rule ship green.
   The obvious repair — Redis's own `WRITE | READONLY` pair — is ALSO wrong,
   because Moon flags `PING`, `ECHO`, `TIME`, `INFO`, `COMMAND`, `LASTSAVE` and
   `WAIT` as `READONLY` and Redis flags none of them so. **That is the same trap
   as `CommandFlags::ADMIN` in the hidden-set, hit a second time in the same
   task.** Final rule is explicit and measured, matching `is_hidden`'s shape, and
   pinned by `mon23`.
2. **The first AUTH of a session was never fed** — on BOTH runtimes. Both
   handlers gate on `!conn.authenticated` above the ACL-exempt intercepts and
   `continue`, so the feed hook below never saw it. `mon8`/`mon9` passed because
   they run against a server with NO PASSWORD, where that gate is already
   satisfied — a redaction test that never exercises an authenticating
   connection tests the wrong path. Fixed in the gate itself; `mon24`/`mon25`
   run against `--requirepass`.
3. **Connection migration bypassed every detach path** — `migration_eligible()`
   excluded MULTI, cross-txn, subscribers, tracking and replconf, but not
   monitors. A migrated monitor leaves a dead sink registered forever, which
   also pins `any_attached()` and holds the inline fast path down for the life
   of the process. Guarded.
4. **A rejected attach left the connection half-attached** — marked attached,
   receiver dropped: keyspace commands refused, no reply, no feed, no way to
   notice. Now evicts the stale registration and re-attaches, or fails loudly.
5. **`mon21`'s closed-connection assertion was vacuous** — `after.is_empty() || …`
   is satisfied by an empty drain, i.e. by exactly the starved-but-open
   connection the policy exists to prevent. Now asserts end-of-stream.
6. **The sharded MONITOR block was a second copy of the attach rule** and had
   already drifted — the failure mode `monitor_mode`'s own doc comment warns
   about. Both handlers now call one helper.
7. **A global `RwLock` read on every fed command** violated "per-shard locks
   only, no global lock on the write path". The read path is now an `ArcSwap`
   load (no lock); attach/detach publish copy-on-write under a `Mutex` never
   touched by the command path.
8. False positive: `+@all` DOES grant MONITOR — Moon's `@all` is a wildcard, not
   the category name list. I only knew because `mon26` was written to check it,
   and it passed on the first run. **`mon14` alone could not have told me**: a
   `-@admin` refusal test passes just as well when no grant reaches MONITOR at
   all, which would have made the command ungrantable by anyone.

What this says about my §6: the WIRING check I had *just* widened to cover
teardown found nothing here, and every one of these lived somewhere else — in a
rule I derived instead of measured (1), in a gate ABOVE the code I was reading
(2, 3), in an error branch I wrote and never exercised (4), and in an assertion I
had described as strong (5). Checking that each new symbol is referenced does not
test whether the rule it implements is the right rule.

Residue carried forward (none blocking, each logged in §7):
- `COMMAND INFO monitor` reports `@admin`-equivalent membership via the ACL name list, but Moon's
  registry has no `@admin` category BIT, so the `acl_categories` field of the `COMMAND INFO` reply
  is thinner than redis-server's. Waived in the compat manifest with a reason, owned by
  `sdk-wire-form-fixes`. Not a permission hole: `+@all -@admin` does refuse MONITOR, pinned by
  `mon14`.

<!-- A security finding is ALWAYS HARD-STOP. Record exactly one outcome — no silent pass. -->

---

## 7 · OBSERVE — feed the next loop ▸ docs/09-the-loop.md

Watch (reuse scenarios as monitors): `moon_dispatch_path_total{path="local_inline"}` staying at its
pre-MONITOR share while no monitor is attached (the stand-down must not become permanent — a leaked
sink would hold the fast path down for the life of the process); monitor connections closed for
backpressure, which should be rare and is the signal that the bounded queue is undersized for a real
workload rather than for a test burst.

### Spec delta
Forward changes for the next loop — each re-enters at Specify as the next task. One line
each, tagged `[SPEC · open|seeded|dropped]`, with evidence (e.g. `[SPEC · open] rate-limit
the retry path (evidence: prod herd spikes)`). See the `add` skill's `deltas.md`.

- [SPEC · seeded] Moon's ACL registry has no `@admin` category bit, so `COMMAND INFO` reports a
  thinner `acl_categories` array than redis-server for every admin command, not just MONITOR
  (evidence: compat differ, `monitor_command_is_registered` and the pre-existing
  `identity_command_info_known_and_unknown` waiver — the same gap, found twice by two tasks).
  Owned by `sdk-wire-form-fixes`.
- [SPEC · open] `CLIENT LIST` does not mark monitor connections with the `flags=O` that redis-server
  uses, and `CLIENT KILL TYPE monitor` is not a supported selector (evidence: explicitly placed out
  of scope at the §3 freeze). An operator who cannot see or kill a monitor from `CLIENT LIST` has to
  find the connection by hand.
- [SPEC · open] The per-monitor queue depth (4096) is a fixed constant with no operator control. The
  contracted policy is to drop the connection when it fills, so the constant decides how large a
  burst a healthy monitor survives (evidence: `mon21` needed a 20,000-command burst to trip it).
  Worth a `--monitor-queue-depth` knob if a real deployment sheds monitors.

### Competency deltas
What did this loop teach the foundation? One line each, tagged by competency
(`DDD · SDD · UDD · TDD · ADD`), status `open`, with evidence. See the `add` skill's `deltas.md`.

- [SDD · open] Running the audit that a contract clause depends on BEFORE the freeze, rather than
  during build, changed the contract materially. The draft §3 said "skip on `CommandFlags::ADMIN |
  SKIP_MONITOR`"; measuring redis-server proved BOTH terms wrong (Moon's ADMIN is container-granular
  and would have hidden six commands Redis shows; Redis feeds the whole EVAL family despite its own
  `skip_monitor` flag). Had the audit run after the freeze it would have been a change request
  against a frozen contract instead of a better contract. Evidence: §3's recorded amendment and
  `mon20`, which now pins the table row by row.
- [TDD · open] A "hidden" assertion is vacuous unless the same test proves the feed was LIVE. Every
  negative test here asserts a FOLLOWING command IS fed, because a broken feed and a correctly
  hidden command are indistinguishable otherwise. Evidence: `mon5`, `mon6`, `mon7` each carry the
  positive half deliberately.
- [ADD · open] The §6 WIRING check must walk TEARDOWN paths, not only dispatch paths. On the
  previous task it walked every dispatch path, passed, and all three defects review found were on
  teardown (RESET, disconnect). Applying the corrected version here found the RESET and disconnect
  detach requirements before review rather than after. Evidence: the §6 WIRING entry above and the
  three `pubsub-resp3-push` CodeRabbit findings that motivated it.
- [SDD · open] Deriving a rule from a flag Moon already has, instead of measuring
  it, produced the SAME defect twice in one task: once for the MONITOR hidden-set
  (`CommandFlags::ADMIN`, caught before freeze by the in-scope audit) and once for
  the keyspace-refusal rule (`first_key`, then `WRITE|READONLY` — caught only by
  external review). Moon's flags are named after Redis's and do not mean the same
  thing. Evidence: `PING`/`ECHO`/`TIME`/`INFO` are `READONLY` in Moon and are not
  in Redis; `DBSIZE`/`KEYS`/`FLUSHALL` have `first_key == 0` and are refused.
  The rule: when a behaviour must match Redis, the predicate is measured and
  table-pinned, never derived from a same-named local flag.
- [TDD · open] A test written against a fixture that does not reach the code path
  it names is worse than no test, because it reports the path as covered.
  `mon8`/`mon9` claimed to cover AUTH redaction and ran against a server with no
  password, so they never touched the pre-auth gate where the only
  credential-bearing AUTH actually goes. Evidence: `mon24` fails on the same
  binary those two pass on. Ask what fixture makes the path REACHABLE, not just
  what call makes the assertion true.
- [TDD · open] A negative-permission test needs its positive twin. `mon14`
  (`-@admin` refuses MONITOR) passes identically whether the grant works or
  MONITOR is ungrantable by anyone; only `mon26` (`+@all` grants it) tells the
  two apart. Evidence: external review flagged the `@all` expansion as a defect,
  and `mon26` is what proved it was not one.
- [ADD · open] A frozen contract clause naming an IMPLEMENTATION SHAPE (three hook sites) rather
  than an observable can be satisfied better by a different mechanism. The right move was to build
  the better mechanism and record the deviation in §5, not to edit §3 and not to build the
  contracted shape against judgment. Evidence: the inline-path stand-down, with `mon13` — the test
  written for that clause — passing unchanged.
