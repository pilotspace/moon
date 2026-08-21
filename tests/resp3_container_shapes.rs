//! moon#631 red/green: the introspection replies Redis types as **Maps** and
//! **Sets** under RESP3.
//!
//! Unlike moon#462, no shared conversion can fix these: each reply is BUILT by
//! its own handler, and the structures are not flat-array-to-map rewrites —
//! `ACL GETUSER` is a map whose values have per-key types, `XINFO GROUPS` is an
//! array OF maps, `COMMAND DOCS` is a map OF maps. So the handler has to emit
//! the right `Frame` when the connection negotiated RESP3, and these tests read
//! the wire directly rather than trusting a `Frame` the code built itself.
//!
//! Every expectation here was transcribed from redis-server 8.6.1 on the wire,
//! never from documentation — `COMMAND DOCS` has no `arity` field there (that
//! lives in `COMMAND INFO`), and `ACL GETUSER` has no `username` field since
//! Redis 7, both of which Moon had invented.
//!
//! Run with:
//!   cargo test --release --test resp3_container_shapes

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// A RESP reader that keeps the TYPE of every node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// `+`, `-`, `:`, `,`, `#`, `(` — one line, sigil kept.
    Line(char, String),
    /// `$` or `=`; `None` payload is the null bulk.
    Blob(char, Option<String>),
    /// `_` — the RESP3 null.
    Null,
    /// `*`, `~`, `>` — sigil kept so Array and Set stay distinguishable.
    Agg(char, Vec<Node>),
    /// A NEGATIVE aggregate length (`*-1`), which RESP2 uses as its null.
    /// Deliberately NOT folded into an empty `Agg`: several assertions below
    /// turn on a field being an EMPTY array (`selectors`, `passwords`), and a
    /// regression that answered the null array instead would satisfy an
    /// `items().is_empty()` check while being a different wire value.
    NullAgg(char),
    /// `%` — pairs, so key order is preserved.
    Map(Vec<(Node, Node)>),
}

impl Node {
    fn sigil(&self) -> char {
        match self {
            Node::Line(c, _) | Node::Blob(c, _) | Node::Agg(c, _) | Node::NullAgg(c) => *c,
            Node::Null => '_',
            Node::Map(_) => '%',
        }
    }

    /// The map's keys in wire order. Panics for anything else — a test that
    /// asks for keys has already asserted the node is a map.
    fn keys(&self) -> Vec<String> {
        match self {
            Node::Map(pairs) => pairs.iter().map(|(k, _)| k.text()).collect(),
            other => panic!("expected a map, got {other:?}"),
        }
    }

    fn get(&self, key: &str) -> &Node {
        match self {
            Node::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| k.text() == key)
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("map has no key {key:?}; keys: {:?}", self.keys())),
            other => panic!("expected a map, got {other:?}"),
        }
    }

    fn items(&self) -> &[Node] {
        match self {
            Node::Agg(_, v) => v,
            // Loud on purpose — see `NullAgg`. An empty slice here would let a
            // null-array regression pass an emptiness assertion.
            Node::NullAgg(c) => panic!("expected an aggregate, got the null aggregate {c}-1"),
            other => panic!("expected an aggregate, got {other:?}"),
        }
    }

    fn text(&self) -> String {
        match self {
            Node::Blob(_, Some(s)) => s.clone(),
            Node::Line(_, s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Types only, no values — the whole point of these tests.
    fn sketch(&self) -> String {
        match self {
            Node::Map(pairs) => format!(
                "%{{{}}}",
                pairs
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k.text(), v.sketch()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Node::Agg(c, items) => format!(
                "{c}[{}]",
                items
                    .iter()
                    .map(Node::sketch)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Node::NullAgg(c) => format!("{c}-1"),
            other => other.sigil().to_string(),
        }
    }
}

struct Conn {
    sock: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn open(port: u16) -> Self {
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Conn {
            sock,
            buf: Vec::new(),
            pos: 0,
        }
    }

    fn send(&mut self, parts: &[&str]) -> Node {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
        }
        self.sock.write_all(&out).expect("write");
        self.read_node()
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 65536];
        match self.sock.read(&mut chunk) {
            Ok(0) => panic!("server closed mid-reply"),
            Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            Err(e) => panic!("read failed: {e}"),
        }
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(i) = self.buf[self.pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .map(|i| self.pos + i)
            {
                let s = String::from_utf8_lossy(&self.buf[self.pos..i]).into_owned();
                self.pos = i + 2;
                return s;
            }
            self.fill();
        }
    }

    /// Read exactly `n` payload bytes and the CRLF that must follow them.
    ///
    /// The terminator is VERIFIED, not skipped: a server that under- or
    /// over-declared a bulk length would otherwise leave this reader silently
    /// mis-framed and every later assertion would be read off the wrong bytes.
    fn exact(&mut self, n: usize) -> String {
        while self.buf.len() < self.pos + n + 2 {
            self.fill();
        }
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + n]).into_owned();
        let term = &self.buf[self.pos + n..self.pos + n + 2];
        assert_eq!(
            term,
            b"\r\n",
            "bulk payload of {n} bytes was not CRLF-terminated (got {:?}); \
             the declared length disagrees with the wire",
            String::from_utf8_lossy(term)
        );
        self.pos += n + 2;
        s
    }

    fn read_node(&mut self) -> Node {
        let line = self.line();
        let mut chars = line.chars();
        let sigil = chars.next().expect("empty reply line");
        let body: String = chars.collect();
        match sigil {
            '+' | '-' | ':' | ',' | '#' | '(' => Node::Line(sigil, body),
            '_' => Node::Null,
            '$' | '=' => {
                let n: i64 = body.parse().expect("bulk length");
                if n < 0 {
                    Node::Blob(sigil, None)
                } else {
                    Node::Blob(sigil, Some(self.exact(n as usize)))
                }
            }
            '*' | '~' | '>' => {
                let n: i64 = body.parse().expect("aggregate length");
                if n < 0 {
                    return Node::NullAgg(sigil);
                }
                Node::Agg(sigil, (0..n).map(|_| self.read_node()).collect())
            }
            '%' => {
                let n: i64 = body.parse().expect("map length");
                Node::Map(
                    (0..n.max(0))
                        .map(|_| (self.read_node(), self.read_node()))
                        .collect(),
                )
            }
            other => panic!("unknown RESP sigil {other:?} in line {line:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// server
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

fn spawn_moon() -> Option<Moon> {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!("skipping: {} not built", bin.display());
        return None;
    }
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-shapes-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir: std::env::temp_dir().join(format!("moon-shapes-{port}")),
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            matches!(Conn::open(moon.port).send(&["PING"]), Node::Line('+', ref s) if s == "PONG")
        }))
        .unwrap_or(false);
        if ok {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not answer PING on port {port}");
    None
}

/// A connection with a stream + consumer group ready for the XINFO cases.
fn seeded(port: u16, resp3: bool) -> Conn {
    let mut c = Conn::open(port);
    if resp3 {
        c.send(&["HELLO", "3"]);
    }
    c.send(&["DEL", "st631"]);
    c.send(&["XADD", "st631", "1-1", "f", "v"]);
    c.send(&["XGROUP", "CREATE", "st631", "g631", "0"]);
    c
}

// ---------------------------------------------------------------------------
// ACL GETUSER
// ---------------------------------------------------------------------------

/// Redis 8.6.1, RESP3:
/// `%{flags:~[…], passwords:*[], commands:$, keys:$, channels:$, selectors:*[]}`
///
/// Moon sent a flat Array with an invented `username` field, no `selectors`,
/// and `keys`/`channels` as arrays instead of the single space-joined string
/// Redis sends.
#[test]
fn cs1_acl_getuser_is_a_map_with_redis_field_set() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, true);

    let r = c.send(&["ACL", "GETUSER", "default"]);
    assert_eq!(r.sigil(), '%', "ACL GETUSER must be a Map on RESP3: {r:?}");
    assert_eq!(
        r.keys(),
        vec![
            "flags".to_string(),
            "passwords".to_string(),
            "commands".to_string(),
            "keys".to_string(),
            "channels".to_string(),
            "selectors".to_string(),
        ],
        "field set and order must match redis 8.6.1 exactly — a map with the \
         wrong keys is worse than a flat array, because a client reading it by \
         name silently gets nothing"
    );
    assert_eq!(r.get("flags").sigil(), '~', "flags is a Set");
    assert_eq!(r.get("passwords").sigil(), '*');
    assert_eq!(r.get("commands").sigil(), '$');
    assert_eq!(r.get("keys").sigil(), '$', "keys is ONE string, not a list");
    assert_eq!(r.get("channels").sigil(), '$');
    // `sketch()`, not `sigil()`: both an empty array and the RESP2 null array
    // report '*', and Moon must send the EMPTY one. `*-1` would mean "this
    // user has no selector support" to a client that distinguishes them.
    assert_eq!(
        r.get("selectors").sketch(),
        "*[]",
        "selectors is an EMPTY Array, never the null array"
    );
    assert_eq!(
        r.get("passwords").sketch(),
        "*[]",
        "default user has no passwords"
    );
}

/// RESP2 keeps the flat array — same names, same order, same value types.
#[test]
fn cs2_acl_getuser_resp2_is_the_same_field_set_flat() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, false);

    let r = c.send(&["ACL", "GETUSER", "default"]);
    assert_eq!(r.sigil(), '*', "RESP2 must stay a flat Array");
    let items = r.items();
    let names: Vec<String> = items.iter().step_by(2).map(Node::text).collect();
    assert_eq!(
        names,
        vec![
            "flags",
            "passwords",
            "commands",
            "keys",
            "channels",
            "selectors"
        ]
    );
    // `2 * i + 1` is the VALUE slot of pair `i` in the flattened array.
    let value_of = |pair: usize| items[2 * pair + 1].sigil();
    assert_eq!(value_of(0), '*', "flags is an Array on RESP2");
    assert_eq!(value_of(2), '$', "commands");
    assert_eq!(value_of(3), '$', "keys is ONE string here too");
    assert_eq!(value_of(4), '$', "channels");
    // Same empty-vs-null distinction as cs1, on the downgraded wire.
    assert_eq!(items[11].sketch(), "*[]", "selectors is an EMPTY Array");
    assert_eq!(items[3].sketch(), "*[]", "passwords is an EMPTY Array");
}

// ---------------------------------------------------------------------------
// XINFO GROUPS
// ---------------------------------------------------------------------------

/// Redis 8.6.1, RESP3:
/// `*[%{name:$, consumers::, pending::, last-delivered-id:$, entries-read:_, lag::}]`
#[test]
fn cs3_xinfo_groups_entries_are_maps() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, true);

    let r = c.send(&["XINFO", "GROUPS", "st631"]);
    assert_eq!(r.sigil(), '*', "the outer reply stays an Array");
    assert_eq!(r.items().len(), 1, "one group was created");
    let g = &r.items()[0];
    assert_eq!(g.sigil(), '%', "each group is a Map on RESP3: {g:?}");
    assert_eq!(
        g.keys(),
        vec![
            "name".to_string(),
            "consumers".to_string(),
            "pending".to_string(),
            "last-delivered-id".to_string(),
            "entries-read".to_string(),
            "lag".to_string(),
        ]
    );
    assert_eq!(g.get("name").sigil(), '$');
    assert_eq!(g.get("consumers").sigil(), ':');
    assert_eq!(g.get("pending").sigil(), ':');
    assert_eq!(g.get("last-delivered-id").sigil(), '$');
}

#[test]
fn cs4_xinfo_groups_resp2_stays_flat() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, false);

    let r = c.send(&["XINFO", "GROUPS", "st631"]);
    let g = &r.items()[0];
    assert_eq!(g.sigil(), '*', "RESP2 keeps each group a flat Array");
    let names: Vec<String> = g.items().iter().step_by(2).map(Node::text).collect();
    assert_eq!(
        names,
        vec![
            "name",
            "consumers",
            "pending",
            "last-delivered-id",
            "entries-read",
            "lag"
        ]
    );
}

// ---------------------------------------------------------------------------
// COMMAND DOCS
// ---------------------------------------------------------------------------

/// Redis 8.6.1, RESP3: `%{get: %{summary:$, since:$, group:$, …}}`.
///
/// Moon answered `*[$, %{…}]` — an array whose first element is the name, and
/// whose map carried an `arity` field Redis does not put in `DOCS` at all.
#[test]
fn cs5_command_docs_is_a_map_of_maps() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, true);

    let r = c.send(&["COMMAND", "DOCS", "GET"]);
    assert_eq!(
        r.sigil(),
        '%',
        "COMMAND DOCS is a Map keyed by command name on RESP3: {}",
        r.sketch()
    );
    assert_eq!(r.keys(), vec!["get".to_string()], "keys are lower-cased");
    let doc = r.get("get");
    assert_eq!(doc.sigil(), '%', "each command's doc is a Map too");
    assert!(
        doc.keys().contains(&"summary".to_string()) && doc.keys().contains(&"since".to_string()),
        "doc keys: {:?}",
        doc.keys()
    );
    assert!(
        !doc.keys().contains(&"arity".to_string()),
        "redis puts arity in COMMAND INFO, never in COMMAND DOCS: {:?}",
        doc.keys()
    );
}

#[test]
fn cs6_command_docs_resp2_stays_flat() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, false);

    let r = c.send(&["COMMAND", "DOCS", "GET"]);
    assert_eq!(r.sigil(), '*', "RESP2 is a flat Array");
    assert_eq!(r.items()[0].text(), "get");
    assert_eq!(r.items()[1].sigil(), '*', "the doc itself is flat too");
}

// ---------------------------------------------------------------------------
// COMMAND INFO
// ---------------------------------------------------------------------------

/// Redis types the four flag-ish members of a `COMMAND INFO` row as Sets:
/// flags, acl-categories, tips, key-specs, subcommands. Moon sent Arrays,
/// which is item (3) of the `identity_command_info_known_and_unknown` waiver.
#[test]
fn cs7_command_info_uses_sets_for_its_flag_lists() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, true);

    let r = c.send(&["COMMAND", "INFO", "GET"]);
    let row = &r.items()[0];
    assert_eq!(row.sigil(), '*', "the row itself stays an Array");
    let f = row.items();
    assert_eq!(
        f.len(),
        10,
        "redis 8.6.1 rows have 10 members: {}",
        row.sketch()
    );
    for (idx, what) in [
        (2usize, "flags"),
        (6, "acl-categories"),
        (7, "tips"),
        (8, "key-specs"),
        (9, "subcommands"),
    ] {
        assert_eq!(
            f[idx].sigil(),
            '~',
            "member {idx} ({what}) is a Set on RESP3: {}",
            row.sketch()
        );
    }
}

#[test]
fn cs8_command_info_resp2_stays_arrays() {
    let Some(m) = spawn_moon() else { return };
    let mut c = seeded(m.port, false);

    let r = c.send(&["COMMAND", "INFO", "GET"]);
    let f = r.items()[0].items();
    for idx in [2usize, 6, 7, 8, 9] {
        assert_eq!(f[idx].sigil(), '*', "member {idx} is an Array on RESP2");
    }
}
