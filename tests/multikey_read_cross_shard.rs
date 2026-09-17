//! A multi-key command must never answer — or mutate — from ONE shard's slice
//! when its keys live on several (moon#962).
//!
//! ## The defect
//!
//! moon routes a command to ONE shard: the owner of the key
//! `extract_primary_key` picks, which is the FIRST key. That shard then
//! executes the WHOLE command against its own slice, with `db: &mut Database`
//! naming a single keyspace slice. Every other key of the argv is looked up in
//! the wrong table, finds nothing, and reads as ABSENT rather than erroring.
//!
//! For twelve read-multi-key commands that produced a confidently wrong ANSWER
//! — `SDIFF`/`ZDIFF` returned members that should have been subtracted,
//! `SINTER`/`ZINTER`/`*CARD` returned empty or `0`, `SUNION`/`ZUNION` returned
//! a short set, `LCS` an empty string, `PFCOUNT` an undercount.
//!
//! For `LMPOP` and `ZMPOP` it is worse, and is why this file exists rather than
//! an issue comment. They are `flags: W`. `list_write::lmpop` walks the keys in
//! argv order and `continue`s past any whose length reads `0`; a REMOTE key
//! reads `0`, so the priority scan proceeds to a key the command is defined
//! never to reach, pops from it, and acks:
//!
//! ```text
//! LMPOP 3 {t1}a {t2}b {t7}c LEFT   ({t1}a absent + the routing key,
//!                                   {t2}b REMOTE non-empty, {t7}c LOCAL non-empty)
//! redis 8.6.1     -> {t2}b B1    {t2}b=[B2]     {t7}c=[C1 C2] untouched
//! moon --shards 4 -> {t7}c C1    {t2}b=[B1 B2]  {t7}c=[C2]      <-- WRONG KEY POPPED
//! ```
//!
//! That shape needs THREE keys — with two, the routing key is always local, so
//! the scan degrades to a benign nil. It is why nothing caught this: every
//! existing routing probe in `scripts/test-consistency.sh` substitutes a single
//! `%K` (`LMPOP 1 %K LEFT`), and one key cannot span shards.
//!
//! ## What these tests assert
//!
//! NOT "moon returns CROSSSLOT" — that would freeze today's remedy into the
//! suite and pass vacuously for any future implementation. They assert the
//! **answer contract**:
//!
//! ```text
//! reply is success  =>  it is the RIGHT answer, and the keyspace moved exactly
//!                       as that answer says it did
//! reply is an error =>  the keyspace did not move at all
//! ```
//!
//! A future per-command fan-out that merges the operand sets properly satisfies
//! this identically; the defect violates it, and so does a fix that refuses
//! co-located keys.
//!
//! ## Why this is not vacuous
//!
//! Placements are constructed, not sampled: `spanning_keys` calls the server's
//! OWN routing function (`moon::shard::dispatch::key_to_shard`) to pick a
//! second key on a DIFFERENT shard than the first and a third on the SAME
//! shard as the first — the exact arrangement above — so every trial exercises
//! the split, and `mkr4` re-checks that arrangement key by key.
//!
//! Two controls keep it honest in the other direction:
//!
//! * `mkr2` runs the SAME key names at `--shards 1`, where every command must
//!   still produce its correct answer — so a fix that simply broke these
//!   commands fails here;
//! * `mkr3` runs `{hash}`-tagged key sets at `--shards 4`, where every command
//!   must still answer — so a blanket refusal fails here.
//!
//! And `TOUCH` carries `must_answer: true` everywhere: it is per-key
//! decomposable and fans out, so a `CROSSSLOT` for it is a regression, not a
//! remedy.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

/// Four shards: the count the issue measured at, and enough that a three-key
/// set can be placed with one key remote and one local.
const SHARDS: usize = 4;
/// Distinct key placements per probe. Each is *constructed* to straddle a shard
/// boundary, so this is breadth (different names, different owners), not a
/// lottery.
const TRIALS: usize = 12;

// ---------------------------------------------------------------------------
// Probe table — one row per command that reads keys it did not route on
// ---------------------------------------------------------------------------

/// One multi-key command, with the reads that decide whether it told the truth.
///
/// `{k1}`..`{k3}` in any argv (seed, command, state probe, expected reply) are
/// substituted with this trial's key names before the command is sent.
struct Probe {
    label: &'static str,
    /// How many keys this row needs. `spanning_keys` places exactly this many.
    nkeys: usize,
    /// Single-key writes that build the operands. Each routes normally, so none
    /// of them is affected by the defect under test.
    seed: &'static [&'static [&'static str]],
    /// The multi-key command itself.
    argv: &'static [&'static str],
    /// The canonical reply the command owes whenever it answers at all —
    /// i.e. the answer `--shards 1` gives for the same seeds.
    expect: &'static str,
    /// Sort the elements of a flat array reply before comparing. Set
    /// combinators return an unordered collection; ordering parity is a
    /// different test's job and would make this one flaky.
    sorted: bool,
    /// Normally-routed reads of every key, and their canonical values AFTER a
    /// successful answer. For a read-only row this is the seeded state; for
    /// `LMPOP`/`ZMPOP` it is the state the answered pop implies.
    state_after: &'static [(&'static [&'static str], &'static str)],
    /// The same reads, and their values when the command did NOT run — the
    /// seeded state. Identical to `state_after` for every read-only row, which
    /// is exactly why those rows could hide a wrong mutation.
    state_untouched: &'static [(&'static [&'static str], &'static str)],
    /// `true` when this command is per-key decomposable and moon fans it out,
    /// so an error reply is a REGRESSION rather than the remedy. `TOUCH` only.
    must_answer: bool,
}

/// `SCARD` of all three operands, unchanged — the read-only set rows.
const SET_STATE: &[(&[&str], &str)] = &[
    (&["SCARD", "{k1}"], "2"),
    (&["SCARD", "{k2}"], "2"),
    (&["SCARD", "{k3}"], "2"),
];

/// `ZCARD` of all three operands, unchanged — the read-only zset rows.
const ZSET_STATE: &[(&[&str], &str)] = &[
    (&["ZCARD", "{k1}"], "2"),
    (&["ZCARD", "{k2}"], "2"),
    (&["ZCARD", "{k3}"], "2"),
];

/// Three sets sharing one member, each with one member of its own: every
/// combinator below has a distinct, single-valued correct answer over them.
const SET_SEED: &[&[&str]] = &[
    &["SADD", "{k1}", "common", "m1"],
    &["SADD", "{k2}", "common", "m2"],
    &["SADD", "{k3}", "common", "m3"],
];

/// The sorted-set twin of [`SET_SEED`].
const ZSET_SEED: &[&[&str]] = &[
    &["ZADD", "{k1}", "1", "common", "2", "m1"],
    &["ZADD", "{k2}", "1", "common", "2", "m2"],
    &["ZADD", "{k3}", "1", "common", "2", "m3"],
];

/// `SDIFF`/`ZDIFF` need a DIFFERENT fixture, and the reason is the whole
/// moon#962 lesson in miniature.
///
/// `spanning_keys` puts `{k3}` on the same shard as `{k1}`, so `{k3}` is read
/// correctly even on the defective build. Under [`SET_SEED`] every operand
/// carries `common`, so `{k3}` subtracted it whether or not `{k2}` was visible
/// — and the DIFF rows came back with the RIGHT answer for the WRONG reason,
/// 12 of 12 (measured on the pre-fix binary before this fixture existed).
///
/// Here only `{k2}` — the REMOTE operand — can subtract `common`. If its
/// contribution is lost, the answer is visibly wrong.
const SDIFF_SEED: &[&[&str]] = &[
    &["SADD", "{k1}", "common", "m1"],
    &["SADD", "{k2}", "common"],
    &["SADD", "{k3}", "m3"],
];

/// The sorted-set twin of [`SDIFF_SEED`].
const ZDIFF_SEED: &[&[&str]] = &[
    &["ZADD", "{k1}", "1", "common", "2", "m1"],
    &["ZADD", "{k2}", "1", "common"],
    &["ZADD", "{k3}", "2", "m3"],
];

/// Operand sizes under [`SDIFF_SEED`].
const SDIFF_STATE: &[(&[&str], &str)] = &[
    (&["SCARD", "{k1}"], "2"),
    (&["SCARD", "{k2}"], "1"),
    (&["SCARD", "{k3}"], "1"),
];

/// Operand sizes under [`ZDIFF_SEED`].
const ZDIFF_STATE: &[(&[&str], &str)] = &[
    (&["ZCARD", "{k1}"], "2"),
    (&["ZCARD", "{k2}"], "1"),
    (&["ZCARD", "{k3}"], "1"),
];

const PROBES: &[Probe] = &[
    Probe {
        label: "SINTER",
        nkeys: 3,
        seed: SET_SEED,
        argv: &["SINTER", "{k1}", "{k2}", "{k3}"],
        // Defect: {k2},{k3} read as absent, so the intersection is empty.
        expect: "[common]",
        sorted: true,
        state_after: SET_STATE,
        state_untouched: SET_STATE,
        must_answer: false,
    },
    Probe {
        label: "SUNION",
        nkeys: 3,
        seed: SET_SEED,
        argv: &["SUNION", "{k1}", "{k2}", "{k3}"],
        // Defect: a SHORT set — only {k1}'s members.
        expect: "[common,m1,m2,m3]",
        sorted: true,
        state_after: SET_STATE,
        state_untouched: SET_STATE,
        must_answer: false,
    },
    Probe {
        label: "SDIFF",
        nkeys: 3,
        seed: SDIFF_SEED,
        argv: &["SDIFF", "{k1}", "{k2}", "{k3}"],
        // Defect: the remote operand is not subtracted, so `common` comes back
        // as well — EXTRA members, the direction a client cannot detect.
        expect: "[m1]",
        sorted: true,
        state_after: SDIFF_STATE,
        state_untouched: SDIFF_STATE,
        must_answer: false,
    },
    Probe {
        label: "SINTERCARD",
        nkeys: 3,
        seed: SET_SEED,
        argv: &["SINTERCARD", "3", "{k1}", "{k2}", "{k3}"],
        // Defect: `0`. One member is shared by all three sets.
        expect: "1",
        sorted: false,
        state_after: SET_STATE,
        state_untouched: SET_STATE,
        must_answer: false,
    },
    Probe {
        label: "ZDIFF",
        nkeys: 3,
        seed: ZDIFF_SEED,
        argv: &["ZDIFF", "3", "{k1}", "{k2}", "{k3}"],
        // Defect: as `SDIFF` — the remote operand subtracts nothing.
        expect: "[m1]",
        sorted: true,
        state_after: ZDIFF_STATE,
        state_untouched: ZDIFF_STATE,
        must_answer: false,
    },
    Probe {
        label: "ZINTER",
        nkeys: 3,
        seed: ZSET_SEED,
        argv: &["ZINTER", "3", "{k1}", "{k2}", "{k3}"],
        expect: "[common]",
        sorted: true,
        state_after: ZSET_STATE,
        state_untouched: ZSET_STATE,
        must_answer: false,
    },
    Probe {
        label: "ZUNION",
        nkeys: 3,
        seed: ZSET_SEED,
        argv: &["ZUNION", "3", "{k1}", "{k2}", "{k3}"],
        // Defect: short AND wrong-scored — `common` comes back with score 1
        // instead of the summed 3.
        expect: "[common,m1,m2,m3]",
        sorted: true,
        state_after: ZSET_STATE,
        state_untouched: ZSET_STATE,
        must_answer: false,
    },
    Probe {
        label: "ZINTERCARD",
        nkeys: 3,
        seed: ZSET_SEED,
        argv: &["ZINTERCARD", "3", "{k1}", "{k2}", "{k3}"],
        expect: "1",
        sorted: false,
        state_after: ZSET_STATE,
        state_untouched: ZSET_STATE,
        must_answer: false,
    },
    Probe {
        label: "LCS",
        nkeys: 2,
        seed: &[&["SET", "{k1}", "ohmytext"], &["SET", "{k2}", "mynewtext"]],
        argv: &["LCS", "{k1}", "{k2}"],
        // Defect: {k2} absent, so the longest common subsequence is "".
        expect: "mytext",
        sorted: false,
        state_after: &[
            (&["GET", "{k1}"], "ohmytext"),
            (&["GET", "{k2}"], "mynewtext"),
        ],
        state_untouched: &[
            (&["GET", "{k1}"], "ohmytext"),
            (&["GET", "{k2}"], "mynewtext"),
        ],
        must_answer: false,
    },
    Probe {
        label: "PFCOUNT",
        nkeys: 3,
        seed: &[
            &["PFADD", "{k1}", "a", "b", "c"],
            &["PFADD", "{k2}", "d", "e", "f"],
            &["PFADD", "{k3}", "g", "h", "i"],
        ],
        argv: &["PFCOUNT", "{k1}", "{k2}", "{k3}"],
        // Defect: an UNDERCOUNT of 3 — indistinguishable from a real answer.
        expect: "9",
        sorted: false,
        state_after: &[
            (&["PFCOUNT", "{k1}"], "3"),
            (&["PFCOUNT", "{k2}"], "3"),
            (&["PFCOUNT", "{k3}"], "3"),
        ],
        state_untouched: &[
            (&["PFCOUNT", "{k1}"], "3"),
            (&["PFCOUNT", "{k2}"], "3"),
            (&["PFCOUNT", "{k3}"], "3"),
        ],
        must_answer: false,
    },
    Probe {
        label: "TOUCH",
        nkeys: 3,
        seed: &[
            &["SET", "{k1}", "v"],
            &["SET", "{k2}", "v"],
            &["SET", "{k3}", "v"],
        ],
        argv: &["TOUCH", "{k1}", "{k2}", "{k3}"],
        // The one genuinely decomposable member: it fans out per key and sums,
        // exactly like EXISTS. Refusing it would be gratuitous, so
        // `must_answer` makes a CROSSSLOT here a test failure.
        expect: "3",
        sorted: false,
        state_after: &[
            (&["GET", "{k1}"], "v"),
            (&["GET", "{k2}"], "v"),
            (&["GET", "{k3}"], "v"),
        ],
        state_untouched: &[
            (&["GET", "{k1}"], "v"),
            (&["GET", "{k2}"], "v"),
            (&["GET", "{k3}"], "v"),
        ],
        must_answer: true,
    },
    // ---- the two that MUTATE -------------------------------------------
    //
    // `{k1}` is deliberately NOT seeded: it is the routing key, and it must be
    // empty for the priority scan to walk past it into keys this shard does not
    // own. `{k2}` is remote and non-empty; `{k3}` is local and non-empty and
    // must never be reached.
    Probe {
        label: "LMPOP",
        nkeys: 3,
        seed: &[
            &["RPUSH", "{k2}", "B1", "B2"],
            &["RPUSH", "{k3}", "C1", "C2"],
        ],
        argv: &["LMPOP", "3", "{k1}", "{k2}", "{k3}", "LEFT"],
        expect: "[{k2},[B1]]",
        sorted: false,
        state_after: &[
            (&["LRANGE", "{k1}", "0", "-1"], "[]"),
            (&["LRANGE", "{k2}", "0", "-1"], "[B2]"),
            (&["LRANGE", "{k3}", "0", "-1"], "[C1,C2]"),
        ],
        state_untouched: &[
            (&["LRANGE", "{k1}", "0", "-1"], "[]"),
            (&["LRANGE", "{k2}", "0", "-1"], "[B1,B2]"),
            (&["LRANGE", "{k3}", "0", "-1"], "[C1,C2]"),
        ],
        must_answer: false,
    },
    Probe {
        label: "ZMPOP",
        nkeys: 3,
        seed: &[
            &["ZADD", "{k2}", "1", "B1", "2", "B2"],
            &["ZADD", "{k3}", "1", "C1", "2", "C2"],
        ],
        argv: &["ZMPOP", "3", "{k1}", "{k2}", "{k3}", "MIN"],
        expect: "[{k2},[[B1,1]]]",
        sorted: false,
        state_after: &[
            (&["ZRANGE", "{k1}", "0", "-1"], "[]"),
            (&["ZRANGE", "{k2}", "0", "-1"], "[B2]"),
            (&["ZRANGE", "{k3}", "0", "-1"], "[C1,C2]"),
        ],
        state_untouched: &[
            (&["ZRANGE", "{k1}", "0", "-1"], "[]"),
            (&["ZRANGE", "{k2}", "0", "-1"], "[B1,B2]"),
            (&["ZRANGE", "{k3}", "0", "-1"], "[C1,C2]"),
        ],
        must_answer: false,
    },
];

// ---------------------------------------------------------------------------
// RESP -> canonical text
// ---------------------------------------------------------------------------

/// Render a RESP reply as stable text: `+OK` -> `OK`, `:3` -> `3`,
/// `$6\r\nmytext` -> `mytext`, an array -> `[a,b,c]`, any error -> `!<text>`.
///
/// Comparing rendered text rather than raw RESP is what lets an expectation
/// like `[{k2},[B1]]` name a key whose byte length changes every trial.
fn canon(s: &str) -> String {
    let b = s.as_bytes();
    let mut i = 0usize;
    canon_one(b, &mut i)
}

fn take_line(b: &[u8], i: &mut usize) -> String {
    let start = *i;
    while *i + 1 < b.len() && !(b[*i] == b'\r' && b[*i + 1] == b'\n') {
        *i += 1;
    }
    let s = String::from_utf8_lossy(&b[start..*i]).into_owned();
    *i = (*i + 2).min(b.len());
    s
}

fn canon_one(b: &[u8], i: &mut usize) -> String {
    if *i >= b.len() {
        return "<truncated>".to_string();
    }
    let tag = b[*i];
    *i += 1;
    let head = take_line(b, i);
    match tag {
        b'+' | b':' | b',' | b'#' => head,
        b'-' => format!("!{head}"),
        b'_' => "nil".to_string(),
        b'$' | b'=' => {
            if head.starts_with('-') {
                return "nil".to_string();
            }
            let n: usize = head.parse().unwrap_or(0);
            let end = (*i + n).min(b.len());
            let s = String::from_utf8_lossy(&b[*i..end]).into_owned();
            *i = (end + 2).min(b.len());
            s
        }
        b'*' | b'~' | b'>' => {
            if head.starts_with('-') {
                return "nil".to_string();
            }
            let n: usize = head.parse().unwrap_or(0);
            let mut parts = Vec::with_capacity(n);
            for _ in 0..n {
                parts.push(canon_one(b, i));
            }
            format!("[{}]", parts.join(","))
        }
        other => format!("<unparsed {}: {}>", other as char, head),
    }
}

/// Sort a FLAT array's elements. A nested reply is returned untouched — the
/// set combinators are the only unordered rows, and none of them nests.
fn maybe_sort(s: &str, sorted: bool) -> String {
    if !sorted {
        return s.to_string();
    }
    let Some(inner) = s.strip_prefix('[').and_then(|x| x.strip_suffix(']')) else {
        return s.to_string();
    };
    if inner.is_empty() || inner.contains('[') {
        return s.to_string();
    }
    let mut parts: Vec<&str> = inner.split(',').collect();
    parts.sort_unstable();
    format!("[{}]", parts.join(","))
}

// ---------------------------------------------------------------------------
// Key placement
// ---------------------------------------------------------------------------

/// `n` key names placed, using the server's own routing hash, in the ONE
/// arrangement that exposes the `LMPOP`/`ZMPOP` wrong-key pop at `--shards 4`:
///
/// * `k1` — whatever shard it hashes to; it is the ROUTING key;
/// * `k2` — a DIFFERENT shard, so it reads as absent on the routed slice;
/// * `k3` — the SAME shard as `k1`, so the priority scan can reach it.
///
/// Every read-only row is straddled by `k2` alone, so the same constructor
/// serves the whole table.
fn spanning_keys(tag: &str, i: usize, n: usize) -> Vec<String> {
    let k1 = format!("mkr:{tag}:{i}:a");
    let owner = key_to_shard(k1.as_bytes(), SHARDS);
    let mut out = vec![k1];
    if n >= 2 {
        out.push(
            (0..1000)
                .map(|j| format!("mkr:{tag}:{i}:b{j}"))
                .find(|k| key_to_shard(k.as_bytes(), SHARDS) != owner)
                .expect("a remote second key must exist among 1000 candidates"),
        );
    }
    if n >= 3 {
        out.push(
            (0..1000)
                .map(|j| format!("mkr:{tag}:{i}:c{j}"))
                .find(|k| key_to_shard(k.as_bytes(), SHARDS) == owner)
                .expect("a co-located third key must exist among 1000 candidates"),
        );
    }
    out
}

/// The documented remedy: one `{hash}` tag collapses the whole set onto one
/// shard, where every command must keep working exactly as at `--shards 1`.
fn colocated_keys(tag: &str, i: usize, n: usize) -> Vec<String> {
    (1..=n).map(|j| format!("{{mkr:{tag}:{i}}}:{j}")).collect()
}

// ---------------------------------------------------------------------------
// Server harness
// ---------------------------------------------------------------------------

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: usize) -> Moon {
    // `CARGO_BIN_EXE_moon` is the binary cargo built for THIS test run;
    // `common::find_moon_binary()` would fall back to `target/release/moon`,
    // whose provenance is unknown — a stale one turns a real failure green.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-mkr-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                // The repo's data volume hovers near the diskfull guard's 5%
                // threshold; without this the server refuses writes and every
                // assertion below fails for an unrelated reason.
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-mkr-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

// ---------------------------------------------------------------------------
// The invariant
// ---------------------------------------------------------------------------

fn subst(argv: &[&str], keys: &[String]) -> Vec<String> {
    argv.iter()
        .map(|p| {
            let mut s = (*p).to_string();
            for (n, k) in keys.iter().enumerate() {
                s = s.replace(&format!("{{k{}}}", n + 1), k);
            }
            s
        })
        .collect()
}

fn send(c: &mut Conn, argv: &[&str], keys: &[String]) -> String {
    let owned = subst(argv, keys);
    let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
    c.send(&refs)
}

/// Run one probe against one key placement.
///
/// Returns `Some(diagnosis)` when the answer contract was broken.
#[must_use]
fn answer_honoured(c: &mut Conn, p: &Probe, keys: &[String], must_land: bool) -> Option<String> {
    for cmd in p.seed {
        let reply = canon(&send(c, cmd, keys));
        if reply.starts_with('!') {
            return Some(format!("seeding {cmd:?} failed: {reply}"));
        }
    }

    let raw = send(c, p.argv, keys);
    let reply = canon(&raw);
    let refused = reply.starts_with('!');

    let read_state = |c: &mut Conn, want: &[(&[&str], &str)]| -> Vec<(String, String, String)> {
        want.iter()
            .map(|(probe, expected)| {
                let got = canon(&send(c, probe, keys));
                let shown = subst(probe, keys).join(" ");
                ((*expected).to_string(), got, shown)
            })
            .collect()
    };

    if refused {
        if must_land {
            return Some(format!(
                "refused on a placement that MUST work: reply={reply}"
            ));
        }
        if p.must_answer {
            return Some(format!(
                "refused {reply}, but this command is per-key decomposable and \
                 must be fanned out, not rejected"
            ));
        }
        // A refusal is only honest if it changed nothing at all.
        for (want, got, shown) in read_state(c, p.state_untouched) {
            if want != got {
                return Some(format!(
                    "refused ({reply}) but the keyspace moved: `{shown}` answered \
                     {got}, expected the untouched {want}"
                ));
            }
        }
        return None;
    }

    // Answered. The answer must be the RIGHT one, and the keyspace must have
    // moved exactly as much as that answer claims.
    let expected = subst(&[p.expect], keys).remove(0);
    if maybe_sort(&reply, p.sorted) != maybe_sort(&expected, p.sorted) {
        return Some(format!(
            "ANSWERED {reply} but the correct answer is {expected} — the keys this \
             command did not route on were read from the routed shard's slice and \
             came back absent"
        ));
    }
    for (want, got, shown) in read_state(c, p.state_after) {
        if want != got {
            return Some(format!(
                "answered {reply} (correct) but the keyspace disagrees: `{shown}` \
                 answered {got}, expected {want}"
            ));
        }
    }
    None
}

/// Run every probe over `TRIALS` placements and report every broken trial at
/// once, rather than stopping at the first.
#[track_caller]
fn run_all(
    port: u16,
    why: &str,
    place: impl Fn(&str, usize, usize) -> Vec<String>,
    must_land: bool,
) {
    let mut wrong: Vec<String> = Vec::new();
    let mut trials = 0usize;
    for p in PROBES {
        for i in 0..TRIALS {
            // A fresh connection per trial: which shard a connection is pinned
            // to is part of the routing state under test, so reusing one would
            // sample a single arrangement over and over.
            let mut c = Conn::open(port);
            let keys = place(p.label, i, p.nkeys);
            trials += 1;
            if let Some(detail) = answer_honoured(&mut c, p, &keys, must_land) {
                wrong.push(format!("  {} {:?}: {detail}", p.label, keys));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{trials} placements broke the answer contract ({why}):\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// moon#962: the whole read-multi-key family, every key set split across
/// shards. An answer must be the correct answer; an error must have changed
/// nothing.
#[test]
fn mkr1_cross_shard_multi_key_reads_never_answer_from_one_shard() {
    let m = spawn_moon(SHARDS);
    run_all(
        m.port,
        "moon#962 — a multi-key command whose keys span shards answered from \
         the routed shard's slice alone, reading every other key as absent",
        spanning_keys,
        false,
    );
}

/// The shard-count control. Identical key names, `--shards 1`: there is no
/// boundary to cross, so every one of these commands must still produce its
/// correct answer. This is what proves `mkr1` measures routing rather than a
/// broken command.
#[test]
fn mkr2_single_shard_control_every_multi_key_read_still_answers() {
    let m = spawn_moon(1);
    run_all(
        m.port,
        "single shard: every multi-key command must still answer correctly for \
         arbitrary key names",
        spanning_keys,
        true,
    );
}

/// The narrowness control. `--shards 4`, but the key set is collapsed onto one
/// shard with a `{hash}` tag — the documented remedy. A fix that refuses these
/// too has over-reached.
#[test]
fn mkr3_hash_tagged_key_sets_still_answer_at_four_shards() {
    let m = spawn_moon(SHARDS);
    run_all(
        m.port,
        "{hash}-tagged key sets are co-located and must never be refused",
        colocated_keys,
        true,
    );
}

/// The wrong MUTATION, on its own, with the placement spelled out.
///
/// `mkr1` covers this through `state_after`/`state_untouched`, but only this
/// test says in one place what the defect actually was: a key the command is
/// defined never to touch had an element removed, and the client was told
/// which OTHER key it came from. Three keys is the minimum — with two, `{k1}`
/// is the routing key and therefore always local, and the scan degrades to a
/// benign nil.
#[test]
fn mkr4_lmpop_and_zmpop_never_pop_a_key_they_did_not_route_on() {
    let m = spawn_moon(SHARDS);
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        // A distinct key NAMESPACE per command. Sharing one would seed a list
        // and then ZADD over it, and every row would come back WRONGTYPE — a
        // green-looking refusal that proves nothing about routing.
        for family in ["l", "z"] {
            let keys = spanning_keys(&format!("mpop{family}"), i, 3);
            let (k1, k2, k3) = (&keys[0], &keys[1], &keys[2]);
            // The placement is re-derived here rather than trusted: a
            // constructor that silently stopped straddling would make this
            // test vacuous.
            let o1 = key_to_shard(k1.as_bytes(), SHARDS);
            assert_ne!(
                o1,
                key_to_shard(k2.as_bytes(), SHARDS),
                "k2 must be REMOTE from the routing key"
            );
            assert_eq!(
                o1,
                key_to_shard(k3.as_bytes(), SHARDS),
                "k3 must be LOCAL to the routing key"
            );

            let (label, seed, argv, pop_probe, untouched, drained) = if family == "l" {
                (
                    "LMPOP",
                    vec![vec!["RPUSH", k2, "B1", "B2"], vec!["RPUSH", k3, "C1", "C2"]],
                    vec!["LMPOP", "3", k1, k2, k3, "LEFT"],
                    vec!["LRANGE", k3, "0", "-1"],
                    "[C1,C2]",
                    vec!["LRANGE", k2, "0", "-1"],
                )
            } else {
                (
                    "ZMPOP",
                    vec![
                        vec!["ZADD", k2, "1", "B1", "2", "B2"],
                        vec!["ZADD", k3, "1", "C1", "2", "C2"],
                    ],
                    vec!["ZMPOP", "3", k1, k2, k3, "MIN"],
                    vec!["ZRANGE", k3, "0", "-1"],
                    "[C1,C2]",
                    vec!["ZRANGE", k2, "0", "-1"],
                )
            };
            let mut c = Conn::open(m.port);
            for s in &seed {
                let reply = canon(&c.send(s));
                assert!(
                    !reply.starts_with('!'),
                    "seeding {s:?} failed: {reply} — the probe would prove nothing"
                );
            }
            let reply = canon(&c.send(&argv));
            let k3_now = canon(&c.send(&pop_probe));
            let k2_now = canon(&c.send(&drained));
            let refused = reply.starts_with('!');
            let ok = if refused {
                // Nothing ran: both operands intact.
                k3_now == untouched && (k2_now == "[B1,B2]")
            } else {
                // Answered: it must have come from k2, and k3 must be intact.
                reply.contains(k2.as_str()) && k3_now == untouched && k2_now == "[B2]"
            };
            if !ok {
                wrong.push(format!(
                    "  {label} [{k1} (absent, routes) | {k2} (remote) | {k3} (local)]: \
                     reply={reply} {k2}={k2_now} {k3}={k3_now}{}",
                    if !refused && k3_now != untouched {
                        " <-- POPPED THE WRONG KEY: an element left a key this command may never touch"
                    } else {
                        ""
                    }
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} LMPOP/ZMPOP placements popped a key the command did not route \
         on, or acked a pop that did not happen (moon#962):\n{}",
        wrong.len(),
        TRIALS * 2,
        wrong.join("\n")
    );
}

/// The two paths that reach the keyspace WITHOUT passing the connection
/// handlers' pre-routing guard: a queued MULTI/EXEC body, and a `redis.call`
/// from Lua. Precedent: `two_key_write_cross_shard.rs::t2k5`.
///
/// * `MULTI ... SINTER k1 k2 k3 ... EXEC` — `analyze_txn_locality` walks every
///   key of every queued command, so a straddling body is refused at `EXEC`.
/// * `EVAL` with every key DECLARED — `route_script_keys` refuses a straddling
///   key set before the script runs.
/// * `EVAL` with the extra keys arriving through `ARGV` — routing never sees
///   them, so the script runs on `KEYS[1]`'s shard and `redis.call` must be
///   refused by the bridge's own copy of the guard.
///
/// `LMPOP` is the body, because it is the member that MUTATES: a hole here is
/// not a wrong answer, it is an element leaving a key nobody named.
#[test]
fn mkr5_transactions_and_scripts_cannot_answer_from_one_shard_either() {
    let m = spawn_moon(SHARDS);
    const LMPOP_DECLARED: &str = "return redis.call('LMPOP', 3, KEYS[1], KEYS[2], KEYS[3], 'LEFT')";
    const LMPOP_VIA_ARGV: &str = "return redis.call('LMPOP', 3, KEYS[1], ARGV[1], ARGV[2], 'LEFT')";

    let mut wrong: Vec<String> = Vec::new();
    for shape in ["multi", "eval-keys", "eval-argv"] {
        for i in 0..TRIALS {
            let keys = spanning_keys(shape, i, 3);
            let (k1, k2, k3) = (&keys[0], &keys[1], &keys[2]);
            let mut c = Conn::open(m.port);
            let _ = c.send(&["RPUSH", k2, "B1", "B2"]);
            let _ = c.send(&["RPUSH", k3, "C1", "C2"]);

            let reply = match shape {
                "multi" => {
                    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
                    let q = c.send(&["LMPOP", "3", k1, k2, k3, "LEFT"]);
                    if q.starts_with('-') {
                        // Refused at QUEUE time is a legitimate refusal too;
                        // EXEC then has nothing to run.
                        let _ = c.send(&["DISCARD"]);
                        q
                    } else {
                        c.send(&["EXEC"])
                    }
                }
                "eval-keys" => c.send(&["EVAL", LMPOP_DECLARED, "3", k1, k2, k3]),
                _ => c.send(&["EVAL", LMPOP_VIA_ARGV, "1", k1, k2, k3]),
            };

            let k2_now = canon(&c.send(&["LRANGE", k2, "0", "-1"]));
            let k3_now = canon(&c.send(&["LRANGE", k3, "0", "-1"]));
            let refused = reply.starts_with('-') || canon(&reply).contains('!');
            let ok = if refused {
                k2_now == "[B1,B2]" && k3_now == "[C1,C2]"
            } else {
                // If it ran at all, it must have popped B1 from k2 and left k3.
                k2_now == "[B2]" && k3_now == "[C1,C2]"
            };
            if !ok {
                wrong.push(format!(
                    "  {shape} [{k1} | {k2} | {k3}]: reply={reply:?} {k2}={k2_now} \
                     {k3}={k3_now}"
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} transaction/script placements answered or mutated from one \
         shard (moon#962):\n{}",
        wrong.len(),
        TRIALS * 3,
        wrong.join("\n")
    );
}
