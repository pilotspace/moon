//! A write that lands data on a key wakes the clients blocked on that key —
//! whatever command wrote it, not only a plain push (moon#1059, moon#1069).
//!
//! # The two bugs
//!
//! * moon#1059: a parked `BLMOVE`/`BRPOPLPUSH` served by a wake pushed its
//!   element onto the DESTINATION, and a `BLPOP` parked on that destination
//!   stayed asleep beside it. Chains (the destination is itself the source of
//!   another parked `BLMOVE`) stalled at the first hop.
//! * moon#1069: `RENAME`, `COPY`, `MOVE`, `COPY ... DB n` — and, it turned out,
//!   every other writer that is not a plain push (`SORT ... STORE`,
//!   `ZUNIONSTORE`, `ZRANGESTORE`, `ZINCRBY`, `GEOADD`, `RESTORE`, `EVAL`, a
//!   `RENAME` inside `MULTI`) — never woke a client blocked on the key it
//!   created.
//!
//! Redis signals a key as ready from the keyspace write itself (`dbAdd` ->
//! `signalKeyAsReady`) and serves the ready set with a loop that keeps going
//! while serving makes more keys ready (`handleClientsBlockedOnKeys`), so both
//! cases are served at once. Every expectation below was recorded against
//! redis-server 8.6.1 first.
//!
//! # What is asserted
//!
//! The VALUE and the LATENCY of every reply: a value alone passes against a
//! server that answered only at its own timeout, and a latency alone passes
//! against a premature null (the moon#606 lesson). Each blocking client runs
//! on a FRESH connection — a blocking reply desynchronises a shared one.
//!
//! # Why both shard counts
//!
//! At `--shards 1` every key is owned by the writer's own shard, so every case
//! takes the connection's local write tail. At `--shards 4` the hash tags
//! spread the cases over the shards and most of them route over the SPSC mesh
//! instead, exercising `spsc_handler`'s wake sites and remote registrations.

mod common;

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// Every waiter's own timeout. Long enough that a woken reply and a timed-out
/// one cannot be confused.
const BLOCK_SECS: &str = "5";
/// A reply later than this did not come from the write.
const WOKEN_WITHIN: Duration = Duration::from_secs(2);
/// How long a client may take to register as blocked after it was sent.
const PARK_WITHIN: Duration = Duration::from_secs(5);
/// Hash tags: distinct tags land on distinct shards at `--shards 4`, so each
/// case runs on both the local and the remote route.
const TAGS: [&str; 3] = ["a", "b", "c"];

fn spawn(shards: &str) -> (ServerGuard, u16) {
    let dir = common::unique_test_dir(&format!("ready-key-wake-s{shards}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bin = find_moon_binary();
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stderr(server_stderr(&dir))
            .spawn()
            .expect("moon spawns")
    });
    // The port accepting is not the server answering; a blocking read parked
    // against a half-started server measures start-up, not wakeups.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return (guard, port);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon never answered PING on port {port} (--shards {shards})");
}

type Argv = Vec<String>;

fn argv(parts: &[&str]) -> Argv {
    parts.iter().map(|s| (*s).to_string()).collect()
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// One blocked client: the db it selects, what it runs, and a substring its
/// reply must contain (`None` = it must NOT be served: the null reply after
/// its own timeout).
struct Waiter {
    db: u32,
    cmd: Argv,
    expect: Option<String>,
}

fn waiter(cmd: &[&str], expect: &str) -> Waiter {
    Waiter {
        db: 0,
        cmd: argv(cmd),
        expect: Some(expect.to_string()),
    }
}

/// Park `w` on its own connection, SELECTing its db first.
fn park(port: u16, w: &Waiter) -> std::thread::JoinHandle<(Duration, String)> {
    let db = w.db;
    let cmd = w.cmd.clone();
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        c.sock
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        if db != 0 {
            let r = c.send(&["SELECT", &db.to_string()]);
            assert!(r.starts_with("+OK"), "SELECT {db}: {r:?}");
        }
        let start = Instant::now();
        let reply = c.send(&strs(&cmd));
        (start.elapsed(), reply)
    })
}

/// A write step: plain commands on one fresh connection (so `MULTI` ...
/// `EXEC` stays on one connection), or a `DUMP`/`DEL`/`RESTORE` round trip,
/// which needs the binary payload intact.
enum WriteStep {
    Cmds(Vec<Argv>),
    Restore { src: String, dst: String },
}

fn cmds(list: &[&[&str]]) -> WriteStep {
    WriteStep::Cmds(list.iter().map(|c| argv(c)).collect())
}

/// Send one command and return its reply's RAW bytes (a `DUMP` payload is not
/// UTF-8, and `Conn` decodes lossily).
fn raw_bulk(sock: &mut TcpStream, parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    sock.write_all(&out).expect("write");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = sock.read(&mut chunk).expect("read");
        assert!(n > 0, "server closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(eol) = buf.windows(2).position(|w| w == b"\r\n") {
            if buf[0] != b'$' {
                return buf[..eol].to_vec();
            }
            let len: usize = std::str::from_utf8(&buf[1..eol])
                .expect("len")
                .parse()
                .expect("bulk length");
            if buf.len() >= eol + 2 + len + 2 {
                return buf[eol + 2..eol + 2 + len].to_vec();
            }
        }
    }
}

fn run_write(port: u16, w: &WriteStep) -> Vec<String> {
    match w {
        WriteStep::Cmds(list) => {
            let mut c = Conn::open(port);
            list.iter().map(|a| c.send(&strs(a))).collect()
        }
        WriteStep::Restore { src, dst } => {
            let mut sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(5)))
                .expect("timeout");
            let payload = raw_bulk(&mut sock, &[b"DUMP", src.as_bytes()]);
            let del = raw_bulk(&mut sock, &[b"DEL", src.as_bytes()]);
            let restored = raw_bulk(&mut sock, &[b"RESTORE", dst.as_bytes(), b"0", &payload]);
            vec![
                String::from_utf8_lossy(&del).into_owned(),
                String::from_utf8_lossy(&restored).into_owned(),
            ]
        }
    }
}

/// `blocked_clients` from `INFO clients`: one per waiter registered on its
/// key's owning shard (each waiter here blocks on exactly one key).
fn blocked_clients(admin: &mut Conn) -> usize {
    let info = admin.send(&["INFO", "clients"]);
    info.lines()
        .find_map(|l| l.strip_prefix("blocked_clients:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Poll until exactly `want` clients are registered as blocked. Parking the
/// next waiter only after the previous one is REGISTERED makes their FIFO
/// order the order they were parked in, and writing only after the last one
/// is registered means the write cannot race a waiter that is still on its
/// way in — without a sleep long enough for a slow CI runner.
fn await_blocked(admin: &mut Conn, want: usize, case: &str) {
    let deadline = Instant::now() + PARK_WITHIN;
    loop {
        let n = blocked_clients(admin);
        if n == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{case}: blocked_clients stuck at {n}, want {want}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One scenario: seed, park every waiter in order, write, then check every
/// reply and every post-condition. Failures are appended, not panicked, so
/// one run reports the whole table.
struct Case {
    name: String,
    setup: Vec<Argv>,
    waiters: Vec<Waiter>,
    write: WriteStep,
    /// `(command, expected reply prefix)` checked after every waiter answered.
    after: Vec<(Argv, String)>,
}

fn run_case(port: u16, case: Case, failures: &mut Vec<String>) {
    let mut seed = Conn::open(port);
    for s in &case.setup {
        let r = seed.send(&strs(s));
        assert!(
            !r.starts_with('-'),
            "{}: setup {s:?} failed: {r:?}",
            case.name
        );
    }
    // The previous case's waiters have all answered, but a server may drop
    // the registration a moment after the reply is on the wire.
    await_blocked(&mut seed, 0, &case.name);
    let mut handles = Vec::new();
    for (i, w) in case.waiters.iter().enumerate() {
        handles.push(park(port, w));
        await_blocked(&mut seed, i + 1, &case.name);
    }
    for r in run_write(port, &case.write) {
        if r.starts_with('-') {
            failures.push(format!("  {}: the write itself failed: {r:?}", case.name));
        }
    }
    for (w, h) in case.waiters.iter().zip(handles) {
        let (elapsed, reply) = h.join().expect("waiter thread");
        match &w.expect {
            Some(want) if !reply.contains(want.as_str()) => failures.push(format!(
                "  {}: {:?} answered {reply:?} after {elapsed:?}; expected {want:?}",
                case.name, w.cmd
            )),
            Some(_) if elapsed > WOKEN_WITHIN => failures.push(format!(
                "  {}: {:?} answered correctly but {elapsed:?} late — its own timeout, \
                 not the write, released it",
                case.name, w.cmd
            )),
            None if !(reply.starts_with("*-1") || reply.starts_with('_')) => {
                failures.push(format!(
                    "  {}: {:?} must stay parked, but was answered {reply:?} after {elapsed:?}",
                    case.name, w.cmd
                ));
            }
            _ => {}
        }
    }
    for (cmd, want) in &case.after {
        let got = seed.send(&strs(cmd));
        if !got.starts_with(want.as_str()) {
            failures.push(format!(
                "  {}: after the wake, {cmd:?} = {got:?}; expected {want:?}",
                case.name
            ));
        }
    }
}

/// moon#1059: moves served by a wake hand their element on to the
/// destination's own waiters, through chains and cycles.
fn wake_served_moves(tag: &str) -> Vec<Case> {
    // Own keys per case, so an element a broken build leaves behind cannot
    // answer the next case's waiter.
    let k = |n: &str, i: u32| format!("{{m{tag}}}{n}{i}");
    let (src0, dst0, src1, dst1, src4, dst4) = (
        k("src", 0),
        k("dst", 0),
        k("src", 1),
        k("dst", 1),
        k("src", 4),
        k("dst", 4),
    );
    let (a2, b2, c2, a3, b3) = (k("a", 2), k("b", 2), k("c", 2), k("a", 3), k("b", 3));
    let (src5, dst5) = (k("src", 5), k("dst", 5));
    let (a6, b6, c6, a7, b7, c7) = (
        k("a", 6),
        k("b", 6),
        k("c", 6),
        k("a", 7),
        k("b", 7),
        k("c", 7),
    );
    // Redis serves ALL the keys one command made ready as one batch before
    // the keys the moves it served push onto: with `BLMOVE a c`, `BLMOVE b c`
    // and `BRPOP c` parked, one EXEC (or one script) pushing onto `a` then `b`
    // runs both moves first, so `BRPOP` takes `y` and `c` keeps `x`
    // (redis-server 8.6.1). Serving `c` between the two moves hands it `x`.
    let order = |name: &str, a: &str, b: &str, c: &str, write: WriteStep| Case {
        name: format!("{name} [{tag}]"),
        setup: vec![],
        waiters: vec![
            waiter(&["BLMOVE", a, c, "LEFT", "RIGHT", BLOCK_SECS], "x"),
            waiter(&["BLMOVE", b, c, "LEFT", "RIGHT", BLOCK_SECS], "y"),
            waiter(&["BRPOP", c, BLOCK_SECS], "y"),
        ],
        write,
        after: vec![(argv(&["LRANGE", c, "0", "-1"]), "*1\r\n$1\r\nx\r\n".into())],
    };
    vec![
        Case {
            name: format!("BLMOVE-wake -> BLPOP dst [{tag}]"),
            setup: vec![],
            waiters: vec![
                waiter(&["BLMOVE", &src0, &dst0, "LEFT", "RIGHT", BLOCK_SECS], "x"),
                waiter(&["BLPOP", &dst0, BLOCK_SECS], "x"),
            ],
            write: cmds(&[&["RPUSH", &src0, "x"]]),
            after: vec![(argv(&["LLEN", &dst0]), ":0".into())],
        },
        Case {
            name: format!("BRPOPLPUSH-wake -> BLPOP dst [{tag}]"),
            setup: vec![],
            waiters: vec![
                waiter(&["BRPOPLPUSH", &src1, &dst1, BLOCK_SECS], "y"),
                waiter(&["BLPOP", &dst1, BLOCK_SECS], "y"),
            ],
            write: cmds(&[&["RPUSH", &src1, "y"]]),
            after: vec![(argv(&["LLEN", &dst1]), ":0".into())],
        },
        Case {
            name: format!("chain a->b, b->c, BLPOP c [{tag}]"),
            setup: vec![],
            waiters: vec![
                waiter(&["BLMOVE", &a2, &b2, "LEFT", "RIGHT", BLOCK_SECS], "z"),
                waiter(&["BLMOVE", &b2, &c2, "LEFT", "RIGHT", BLOCK_SECS], "z"),
                waiter(&["BLPOP", &c2, BLOCK_SECS], "z"),
            ],
            write: cmds(&[&["RPUSH", &a2, "z"]]),
            after: vec![
                (argv(&["EXISTS", &a2]), ":0".into()),
                (argv(&["EXISTS", &b2]), ":0".into()),
                (argv(&["EXISTS", &c2]), ":0".into()),
            ],
        },
        // A key served earlier becomes ready AGAIN when a later hop pushes
        // onto it; redis serves its next waiter then.
        Case {
            name: format!("cycle a->b, b->a, BLPOP a [{tag}]"),
            setup: vec![],
            waiters: vec![
                waiter(&["BLMOVE", &a3, &b3, "LEFT", "RIGHT", BLOCK_SECS], "w"),
                waiter(&["BLMOVE", &b3, &a3, "LEFT", "RIGHT", BLOCK_SECS], "w"),
                waiter(&["BLPOP", &a3, BLOCK_SECS], "w"),
            ],
            write: cmds(&[&["RPUSH", &a3, "w"]]),
            after: vec![
                (argv(&["EXISTS", &a3]), ":0".into()),
                (argv(&["EXISTS", &b3]), ":0".into()),
            ],
        },
        Case {
            name: format!("MULTI RPUSH src EXEC -> BLMOVE -> BLPOP dst [{tag}]"),
            setup: vec![],
            waiters: vec![
                waiter(&["BLMOVE", &src4, &dst4, "LEFT", "RIGHT", BLOCK_SECS], "v"),
                waiter(&["BLPOP", &dst4, BLOCK_SECS], "v"),
            ],
            write: cmds(&[&["MULTI"], &["RPUSH", &src4, "v"], &["EXEC"]]),
            after: vec![(argv(&["LLEN", &dst4]), ":0".into())],
        },
        // A BLMOVE served IMMEDIATELY (its source already had data) pushes
        // onto its destination just the same.
        Case {
            name: format!("immediate BLMOVE -> BLPOP dst [{tag}]"),
            setup: vec![argv(&["RPUSH", &src5, "u"])],
            waiters: vec![waiter(&["BLPOP", &dst5, BLOCK_SECS], "u")],
            write: cmds(&[&["BLMOVE", &src5, &dst5, "LEFT", "RIGHT", "1"]]),
            after: vec![(argv(&["LLEN", &dst5]), ":0".into())],
        },
        order(
            "order: MULTI RPUSH a x; RPUSH b y; EXEC",
            &a6,
            &b6,
            &c6,
            cmds(&[
                &["MULTI"],
                &["RPUSH", &a6, "x"],
                &["RPUSH", &b6, "y"],
                &["EXEC"],
            ]),
        ),
        order(
            "order: EVAL RPUSH a x; RPUSH b y",
            &a7,
            &b7,
            &c7,
            cmds(&[&[
                "EVAL",
                "redis.call('RPUSH', KEYS[1], 'x'); return redis.call('RPUSH', KEYS[2], 'y')",
                "2",
                &a7,
                &b7,
            ]]),
        ),
    ]
}

/// moon#1069 and the rest of its class: every writer that creates a key.
///
/// Each case gets its own keys (`s<i>`, `d<i>`): a `COPY` leaves its source
/// behind and `SORT ... STORE` leaves the rest of the list, so shared names
/// would make one case's leftovers the next case's setup.
fn keyspace_writers(tag: &str) -> Vec<Case> {
    let key = |n: &str, i: usize| format!("{{w{tag}}}{n}{i}");
    type WriteFn = fn(&str, &str) -> WriteStep;
    // (name, waiter pop command, seed the source as a zset?, the write)
    let table: Vec<(&str, &str, bool, WriteFn)> = vec![
        ("RENAME list", "BLPOP", false, |s, d| {
            cmds(&[&["RENAME", s, d]])
        }),
        ("RENAMENX list", "BLPOP", false, |s, d| {
            cmds(&[&["RENAMENX", s, d]])
        }),
        ("COPY list", "BLPOP", false, |s, d| cmds(&[&["COPY", s, d]])),
        ("LMOVE list", "BLPOP", false, |s, d| {
            cmds(&[&["LMOVE", s, d, "LEFT", "RIGHT"]])
        }),
        ("SORT STORE", "BLPOP", false, |s, d| {
            cmds(&[&["SORT", s, "ALPHA", "STORE", d]])
        }),
        ("RESTORE", "BLPOP", false, |s, d| WriteStep::Restore {
            src: s.to_string(),
            dst: d.to_string(),
        }),
        ("MULTI RENAME EXEC", "BLPOP", false, |s, d| {
            cmds(&[&["MULTI"], &["RENAME", s, d], &["EXEC"]])
        }),
        ("RENAME zset", "BZPOPMIN", true, |s, d| {
            cmds(&[&["RENAME", s, d]])
        }),
        ("COPY zset", "BZPOPMAX", true, |s, d| {
            cmds(&[&["COPY", s, d]])
        }),
        ("ZUNIONSTORE", "BZPOPMIN", true, |s, d| {
            cmds(&[&["ZUNIONSTORE", d, "1", s]])
        }),
        ("ZRANGESTORE", "BZPOPMAX", true, |s, d| {
            cmds(&[&["ZRANGESTORE", d, s, "0", "-1"]])
        }),
    ];
    let mut out = Vec::new();
    for (i, (name, pop, zset, write)) in table.into_iter().enumerate() {
        let (s, d) = (key("s", i), key("d", i));
        let (seed, want) = if zset {
            (argv(&["ZADD", &s, "1", "m"]), "m")
        } else {
            (argv(&["RPUSH", &s, "v"]), "v")
        };
        out.push(Case {
            name: format!("{name} [{tag}]"),
            setup: vec![seed],
            waiters: vec![waiter(&[pop, &d, BLOCK_SECS], want)],
            write: write(&s, &d),
            after: vec![(argv(&["EXISTS", &d]), ":0".into())],
        });
    }
    let (l1, l2, z1, z2, x, e) = (
        key("l", 100),
        key("l", 101),
        key("z", 102),
        key("z", 103),
        key("x", 104),
        key("e", 105),
    );
    let xs = key("xs", 104);
    let (l3, l4) = (key("l", 106), key("l", 107));
    out.extend([
        Case {
            name: format!("MOVE into db 3 [{tag}]"),
            setup: vec![argv(&["RPUSH", &l1, "v"])],
            waiters: vec![Waiter {
                db: 3,
                cmd: argv(&["BLPOP", &l1, BLOCK_SECS]),
                expect: Some("v".into()),
            }],
            write: cmds(&[&["MOVE", &l1, "3"]]),
            after: vec![(argv(&["EXISTS", &l1]), ":0".into())],
        },
        Case {
            name: format!("COPY ... DB 3 [{tag}]"),
            setup: vec![argv(&["RPUSH", &l2, "v"])],
            waiters: vec![Waiter {
                db: 3,
                cmd: argv(&["BLPOP", &l2, BLOCK_SECS]),
                expect: Some("v".into()),
            }],
            write: cmds(&[&["COPY", &l2, &l2, "DB", "3"]]),
            // The source keeps its copy.
            after: vec![(argv(&["LLEN", &l2]), ":1".into())],
        },
        // moon#1062 made both run inside MULTI into the db they name; the
        // waiter there is served after EXEC.
        Case {
            name: format!("MULTI MOVE into db 3 EXEC [{tag}]"),
            setup: vec![argv(&["RPUSH", &l3, "v"])],
            waiters: vec![Waiter {
                db: 3,
                cmd: argv(&["BLPOP", &l3, BLOCK_SECS]),
                expect: Some("v".into()),
            }],
            write: cmds(&[&["MULTI"], &["MOVE", &l3, "3"], &["EXEC"]]),
            after: vec![(argv(&["EXISTS", &l3]), ":0".into())],
        },
        Case {
            name: format!("MULTI COPY ... DB 3 EXEC [{tag}]"),
            setup: vec![argv(&["RPUSH", &l4, "v"])],
            waiters: vec![Waiter {
                db: 3,
                cmd: argv(&["BLPOP", &l4, BLOCK_SECS]),
                expect: Some("v".into()),
            }],
            write: cmds(&[&["MULTI"], &["COPY", &l4, &l4, "DB", "3"], &["EXEC"]]),
            after: vec![(argv(&["LLEN", &l4]), ":1".into())],
        },
        Case {
            name: format!("ZINCRBY creates the zset [{tag}]"),
            setup: vec![],
            waiters: vec![waiter(&["BZPOPMIN", &z1, BLOCK_SECS], "m")],
            write: cmds(&[&["ZINCRBY", &z1, "1", "m"]]),
            after: vec![(argv(&["EXISTS", &z1]), ":0".into())],
        },
        Case {
            name: format!("GEOADD creates the zset [{tag}]"),
            setup: vec![],
            waiters: vec![waiter(&["BZPOPMIN", &z2, BLOCK_SECS], "p")],
            write: cmds(&[&["GEOADD", &z2, "13.36", "38.11", "p"]]),
            after: vec![(argv(&["EXISTS", &z2]), ":0".into())],
        },
        Case {
            name: format!("RENAME a stream under XREAD $ [{tag}]"),
            setup: vec![argv(&["XADD", &xs, "1-1", "f", "v"])],
            waiters: vec![waiter(
                &["XREAD", "BLOCK", "5000", "STREAMS", &x, "$"],
                "1-1",
            )],
            write: cmds(&[&["RENAME", &xs, &x]]),
            after: vec![(argv(&["EXISTS", &x]), ":1".into())],
        },
        Case {
            name: format!("EVAL RPUSH KEYS[1] [{tag}]"),
            setup: vec![],
            waiters: vec![waiter(&["BLPOP", &e, BLOCK_SECS], "x")],
            write: cmds(&[&["EVAL", "return redis.call('RPUSH', KEYS[1], 'x')", "1", &e]]),
            after: vec![(argv(&["EXISTS", &e]), ":0".into())],
        },
    ]);
    out
}

/// `SWAPDB` makes every key of both databases ready at once. Its own server
/// per shard count: a swap moves every other case's keys too.
fn swapped_databases(tag: &str) -> Vec<Case> {
    let k = format!("{{s{tag}}}k");
    vec![Case {
        name: format!("SWAPDB 0 7 [{tag}]"),
        setup: vec![argv(&["RPUSH", &k, "v"])],
        waiters: vec![Waiter {
            db: 7,
            cmd: argv(&["BLPOP", &k, BLOCK_SECS]),
            expect: Some("v".into()),
        }],
        write: cmds(&[&["SWAPDB", "0", "7"]]),
        after: vec![],
    }]
}

/// The controls: a key that becomes the WRONG type for a waiter leaves the
/// waiter parked (redis: `serveClientsBlockedOnKey` skips a type mismatch),
/// and a wake-served move onto a wrong-typed destination answers
/// `-WRONGTYPE` and consumes nothing. These are the halves that price a fix
/// which serves too eagerly.
fn type_mismatch_controls(tag: &str) -> Vec<Case> {
    let k = |n: &str| format!("{{x{tag}}}{n}");
    let (s, d, src, dst) = (k("s"), k("d"), k("src"), k("dst"));
    vec![
        Case {
            name: format!("RENAME a zset onto a BLPOP key [{tag}]"),
            setup: vec![argv(&["ZADD", &s, "1", "m"])],
            waiters: vec![Waiter {
                db: 0,
                cmd: argv(&["BLPOP", &d, "2"]),
                expect: None,
            }],
            write: cmds(&[&["RENAME", &s, &d]]),
            after: vec![(argv(&["TYPE", &d]), "+zset".into())],
        },
        Case {
            name: format!("BLMOVE wake onto a string destination [{tag}]"),
            setup: vec![argv(&["SET", &dst, "str"])],
            waiters: vec![waiter(
                &["BLMOVE", &src, &dst, "LEFT", "RIGHT", BLOCK_SECS],
                "WRONGTYPE",
            )],
            write: cmds(&[&["RPUSH", &src, "x"]]),
            after: vec![
                (argv(&["LLEN", &src]), ":1".into()),
                (argv(&["TYPE", &dst]), "+string".into()),
            ],
        },
    ]
}

fn run_all(shards: &str, cases: impl Fn(&str) -> Vec<Case>) {
    let (_guard, port) = spawn(shards);
    let mut failures = Vec::new();
    for tag in TAGS {
        for case in cases(tag) {
            run_case(port, case, &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "{} ready-key wake assertion(s) failed at --shards {shards}:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn wake_served_moves_feed_destination_waiters_at_one_shard() {
    run_all("1", wake_served_moves);
}

#[test]
fn wake_served_moves_feed_destination_waiters_at_four_shards() {
    run_all("4", wake_served_moves);
}

#[test]
fn keyspace_writers_wake_the_destination_at_one_shard() {
    run_all("1", keyspace_writers);
}

#[test]
fn keyspace_writers_wake_the_destination_at_four_shards() {
    run_all("4", keyspace_writers);
}

#[test]
fn type_mismatch_leaves_waiters_alone_at_one_shard() {
    run_all("1", type_mismatch_controls);
}

#[test]
fn type_mismatch_leaves_waiters_alone_at_four_shards() {
    run_all("4", type_mismatch_controls);
}

#[test]
fn swapdb_wakes_keys_parked_in_either_database_at_one_shard() {
    run_all("1", swapped_databases);
}

#[test]
fn swapdb_wakes_keys_parked_in_either_database_at_four_shards() {
    run_all("4", swapped_databases);
}
