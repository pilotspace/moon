//! `FT.SEARCH <index> "*"` must enumerate a VECTOR-only index (moon#695).
//!
//! moon#693 made `*` the match-all query and answered it from the **inverted**
//! index, where the document registry lives. An index built from VECTOR fields
//! alone has no inverted index at all, so `*` fell through to the text engine and
//! answered `ERR no such index` — for an index `FT._LIST` lists.
//!
//! ## Why this runs at BOTH shard counts
//!
//! The issue calls this out by name: a fix that only covers the local handler
//! path passes at `--shards 1` and silently returns nothing at `--shards 4`,
//! which is worse than the honest error it replaced. It is not a hypothetical —
//! the first cut of this fix wired the multi-shard branch and missed the
//! single-shard one, and only running both caught it. So every assertion below
//! runs against 1 shard and 4 shards.
//!
//! ## What this proves that the unit tests cannot
//!
//! The lib tests seed `key_hash_to_key` directly to exercise the enumerator. Only
//! a real server proves the thing that actually matters: that the ordinary HSET
//! indexing path fills that map, that DEL empties it, that the answer survives a
//! restart, and that a scattered match-all reassembles into the same answer the
//! single-shard path gives.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DIM: usize = 4;
const DOCS: usize = 8;

/// Documents in the TEXT and mixed control indexes.
///
/// Those two exist only to prove the fix does not annex queries the text engine
/// already answers, and a build without `text-index` refuses to create them at
/// all (`ERR TEXT fields require the text-index feature`). The VECTOR-only
/// assertions are deliberately NOT gated: a vector-only schema is exactly the one
/// that needs no text engine, so it must keep working in a build compiled without
/// one — which is the CI tokio leg.
#[cfg(feature = "text-index")]
const CONTROL_DOCS: usize = 5;
#[cfg(not(feature = "text-index"))]
const CONTROL_DOCS: usize = 0;

// ─── Minimal RESP client ─────────────────────────────────────────────────────
// Vectors are FLOAT32 blobs full of NUL bytes, so every probe has to be
// length-aware over a raw socket. A `redis-cli`-shaped probe cannot carry them.

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<Resp>),
    Err(String),
    Simple(String),
    Nil,
}

impl Resp {
    /// `reply[0]` — the RediSearch total-matched count.
    fn total(&self) -> i64 {
        match self {
            Resp::Arr(items) => match items.first() {
                Some(Resp::Int(n)) => *n,
                other => panic!("reply[0] is not an Integer: {other:?}"),
            },
            other => panic!("expected an Array reply, got {other:?}"),
        }
    }

    /// The document keys, i.e. every other element after the total.
    fn keys(&self) -> Vec<Vec<u8>> {
        match self {
            Resp::Arr(items) => items[1..]
                .iter()
                .step_by(2)
                .map(|f| match f {
                    Resp::Bulk(b) => b.clone(),
                    other => panic!("expected a key BulkString, got {other:?}"),
                })
                .collect(),
            other => panic!("expected an Array reply, got {other:?}"),
        }
    }

    /// The score field name of the first document, e.g. `__bm25_score`.
    fn first_score_field(&self) -> Option<Vec<u8>> {
        match self {
            Resp::Arr(items) => match items.get(2) {
                Some(Resp::Arr(fields)) => match fields.first() {
                    Some(Resp::Bulk(b)) => Some(b.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    fn as_err(&self) -> Option<&str> {
        match self {
            Resp::Err(e) => Some(e),
            _ => None,
        }
    }
}

/// Parse one RESP value. `None` means "need more bytes", never "malformed".
fn parse(buf: &[u8], pos: &mut usize) -> Option<Resp> {
    let line_end = buf[*pos..].windows(2).position(|w| w == b"\r\n")? + *pos;
    let tag = buf[*pos];
    let body = &buf[*pos + 1..line_end];
    let after = line_end + 2;
    match tag {
        b'+' => {
            *pos = after;
            Some(Resp::Simple(String::from_utf8_lossy(body).into_owned()))
        }
        b'-' => {
            *pos = after;
            Some(Resp::Err(String::from_utf8_lossy(body).into_owned()))
        }
        b':' => {
            *pos = after;
            Some(Resp::Int(
                String::from_utf8_lossy(body).parse().unwrap_or(0),
            ))
        }
        b'$' => {
            let n: i64 = String::from_utf8_lossy(body).parse().unwrap_or(-1);
            if n < 0 {
                *pos = after;
                return Some(Resp::Nil);
            }
            let n = n as usize;
            if buf.len() < after + n + 2 {
                return None;
            }
            let v = buf[after..after + n].to_vec();
            *pos = after + n + 2;
            Some(Resp::Bulk(v))
        }
        b'*' => {
            let n: i64 = String::from_utf8_lossy(body).parse().unwrap_or(-1);
            if n < 0 {
                *pos = after;
                return Some(Resp::Nil);
            }
            let mut cur = after;
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(parse(buf, &mut cur)?);
            }
            *pos = cur;
            Some(Resp::Arr(items))
        }
        other => panic!("unknown RESP tag {:?}", other as char),
    }
}

/// One connection for the whole test: a fresh connection per probe would let a
/// shard-local answer look global.
struct Conn {
    sock: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn open(port: u16) -> Self {
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        sock.set_read_timeout(Some(Duration::from_secs(20))).ok();
        Self {
            sock,
            buf: Vec::new(),
        }
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        self.sock.write_all(&out).expect("write");

        let mut chunk = [0u8; 1 << 16];
        loop {
            let mut pos = 0usize;
            if let Some(v) = parse(&self.buf, &mut pos) {
                self.buf.drain(..pos);
                return v;
            }
            let n = self.sock.read(&mut chunk).expect("read");
            assert!(n > 0, "server closed the connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

fn vec_blob(seed: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(DIM * 4);
    for i in 0..DIM {
        v.extend_from_slice(&(1.0f32 + seed as f32 + i as f32).to_le_bytes());
    }
    v
}

fn spawn_on(port: u16, shards: usize, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            // Durable, because one leg restarts the server and asserts the
            // enumeration survives. With appendonly=no the documents themselves
            // do not survive, and the test would "pass" against an empty index.
            //
            // `always`, not the default `everysec`: the restart is done with
            // SIGKILL, so under everysec the last second of writes is legitimately
            // lost and the restart leg fails on its own precondition rather than
            // on anything to do with moon#695.
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)")
}

/// Wait until the server on `port` answers `PING`.
///
/// The probe is deliberately fallible rather than routed through [`Conn`]: on a
/// same-port restart the kernel can still hand out a connection on the dying
/// process's listener, and that connection is then RESET. `Conn::cmd` panics on
/// a read error, so a readiness wait built on it turns a transient reset into a
/// bare `ConnectionReset` that names neither the port nor the phase. Retry until
/// the deadline instead, and give each probe its own read timeout so a server
/// that accepts but never answers cannot park the wait past that deadline.
fn await_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 64];
            if s.write_all(b"PING\r\n").is_ok()
                && let Ok(n) = s.read(&mut buf)
                && buf[..n].starts_with(b"+PONG")
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on port {port} never became ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Build the three index shapes and load them.
fn seed(c: &mut Conn) {
    let dim = DIM.to_string();
    let vec_schema: Vec<&[u8]> = vec![
        b"e",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        dim.as_bytes(),
        b"DISTANCE_METRIC",
        b"L2",
    ];

    let mut create_vec: Vec<&[u8]> = vec![
        b"FT.CREATE",
        b"vidx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"v:",
        b"SCHEMA",
    ];
    create_vec.extend_from_slice(&vec_schema);
    assert_eq!(
        c.cmd(&create_vec),
        Resp::Simple("OK".into()),
        "FT.CREATE vector-only"
    );

    #[cfg(feature = "text-index")]
    {
        assert_eq!(
            c.cmd(&[
                b"FT.CREATE",
                b"tidx",
                b"ON",
                b"HASH",
                b"PREFIX",
                b"1",
                b"t:",
                b"SCHEMA",
                b"title",
                b"TEXT"
            ]),
            Resp::Simple("OK".into()),
            "FT.CREATE text-only"
        );

        let mut create_mix: Vec<&[u8]> = vec![
            b"FT.CREATE",
            b"midx",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"m:",
            b"SCHEMA",
            b"title",
            b"TEXT",
        ];
        create_mix.extend_from_slice(&vec_schema);
        assert_eq!(
            c.cmd(&create_mix),
            Resp::Simple("OK".into()),
            "FT.CREATE mixed"
        );
    }

    for i in 0..DOCS {
        let key = format!("v:{i}");
        c.cmd(&[b"HSET", key.as_bytes(), b"e", &vec_blob(i)]);
    }
    #[cfg(feature = "text-index")]
    {
        for i in 0..3 {
            let (k, t) = (format!("t:{i}"), format!("hello world {i}"));
            c.cmd(&[b"HSET", k.as_bytes(), b"title", t.as_bytes()]);
        }
        for i in 0..2 {
            let (k, t) = (format!("m:{i}"), format!("mixed doc {i}"));
            c.cmd(&[
                b"HSET",
                k.as_bytes(),
                b"title",
                t.as_bytes(),
                b"e",
                &vec_blob(i),
            ]);
        }
    }
}

fn run_all(shards: usize) {
    let dir = tempfile::Builder::new()
        .prefix("moon-695-")
        .tempdir()
        .expect("tempdir");
    let port = common::reserve_port();
    let mut guard = common::ServerGuard::new(spawn_on(port, shards, dir.path()));
    await_ready(port);

    let mut c = Conn::open(port);
    seed(&mut c);

    // ── the bug ──────────────────────────────────────────────────────────────
    let star = c.cmd(&[b"FT.SEARCH", b"vidx", b"*"]);
    assert!(
        star.as_err().is_none(),
        "shards={shards}: `*` on a VECTOR-only index was refused — that is moon#695. \
         FT._LIST lists it, so answering `no such index` is a lie: {star:?}"
    );
    assert_eq!(
        star.total(),
        DOCS as i64,
        "shards={shards}: every indexed document must be enumerated"
    );
    let mut got = star.keys();
    got.sort();
    let want: Vec<Vec<u8>> = (0..DOCS).map(|i| format!("v:{i}").into_bytes()).collect();
    assert_eq!(
        got, want,
        "shards={shards}: the enumeration must be the real keys, not synthetic vec:<id>"
    );
    assert_eq!(
        star.first_score_field().as_deref(),
        Some(&b"__bm25_score"[..]),
        "shards={shards}: `*` answers in ONE shape whatever the schema — a mixed \
         index already replies like this"
    );

    // ── LIMIT: page the docs, but reply[0] stays the full total ──────────────
    let page1 = c.cmd(&[b"FT.SEARCH", b"vidx", b"*", b"LIMIT", b"0", b"3"]);
    let page2 = c.cmd(&[b"FT.SEARCH", b"vidx", b"*", b"LIMIT", b"3", b"3"]);
    for (n, p) in [(1, &page1), (2, &page2)] {
        assert_eq!(
            p.total(),
            DOCS as i64,
            "shards={shards}: page {n} reply[0] is the total MATCHED, not the page length"
        );
        assert_eq!(p.keys().len(), 3, "shards={shards}: page {n} holds 3 docs");
    }
    let (k1, k2) = (page1.keys(), page2.keys());
    assert!(
        k1.iter().all(|k| !k2.contains(k)),
        "shards={shards}: LIMIT must page, not re-serve the same documents. \
         A per-shard cap applied without adjusting for the caller's offset looks \
         exactly like this: {k1:?} vs {k2:?}"
    );

    // ── the fix must not annex anything else ─────────────────────────────────
    #[cfg(feature = "text-index")]
    {
        assert_eq!(
            c.cmd(&[b"FT.SEARCH", b"tidx", b"*"]).total(),
            3,
            "shards={shards}: a TEXT-only index still answers `*` from the text engine"
        );
        let mixed = c.cmd(&[b"FT.SEARCH", b"midx", b"*"]);
        assert_eq!(
            mixed.total(),
            2,
            "shards={shards}: a mixed index still answers `*` from the text engine"
        );
    }

    // An index that really is missing must still be refused, never reported as a
    // successful empty listing.
    //
    // Asserted only with `text-index`, the shipped default. WITHOUT it there is a
    // pre-existing divergence this fix neither causes nor cures — proven by A/B
    // against a build with this gate neutered to `return None`: at shards=1 the
    // query falls through to the KNN parser (`ERR invalid KNN query syntax`), and
    // at shards>1 `scatter_text_search` merges empty per-shard replies into a
    // successful `[0]`, so a missing index reads as an empty one. Filed as
    // moon#728; this assertion widens to both settings when that lands.
    #[cfg(feature = "text-index")]
    {
        let missing = c.cmd(&[b"FT.SEARCH", b"nosuch", b"*"]);
        assert!(
            missing
                .as_err()
                .is_some_and(|e| e.contains("no such index")),
            "shards={shards}: an index that really is missing must still say so, \
             rather than reading as an empty-but-successful enumeration: {missing:?}"
        );
    }

    // KNN on the same vector-only index keeps its own engine and its own score
    // field — the match-all gate must not swallow it.
    let knn = c.cmd(&[
        b"FT.SEARCH",
        b"vidx",
        b"*=>[KNN 3 @e $q]",
        b"PARAMS",
        b"2",
        b"q",
        &vec_blob(0),
    ]);
    assert_eq!(knn.total(), 3, "shards={shards}: KNN still returns k hits");
    assert_eq!(
        knn.first_score_field().as_deref(),
        Some(&b"__vec_score"[..]),
        "shards={shards}: KNN must still be answered by the vector engine"
    );

    // ── the registry is LIVE, not a log of historical inserts ────────────────
    c.cmd(&[b"DEL", b"v:0"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let t = c
            .cmd(&[b"FT.SEARCH", b"vidx", b"*", b"LIMIT", b"0", b"0"])
            .total();
        if t == DOCS as i64 - 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "shards={shards}: a deleted key stayed enumerable (total {t}); the key \
             map is supposed to track live keys, not historical inserts"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !c.cmd(&[b"FT.SEARCH", b"vidx", b"*"])
            .keys()
            .contains(&b"v:0".to_vec()),
        "shards={shards}: the deleted key must be gone from the listing too, not \
         just from the count"
    );

    // ── survives a restart ───────────────────────────────────────────────────
    drop(c);
    guard.kill_now();
    // Required before rebinding the same port, not optional hygiene: moon's
    // per-shard listeners are `SO_REUSEPORT`, so the replacement server binds
    // successfully while the SIGKILLed one is still tearing its sockets down,
    // and a client that connects in that window is RESET. Omitting this made
    // both legs fail 6/6 on Linux while passing on macOS, where the teardown
    // happens to win the race (moon#489 is the same signature).
    common::wait_for_port_down(port);
    let mut guard = common::ServerGuard::new(spawn_on(port, shards, dir.path()));
    await_ready(port);
    let mut c = Conn::open(port);

    // Recovery re-indexes asynchronously; wait for the documents themselves
    // before judging the enumeration, so a slow reload cannot read as data loss.
    let deadline = Instant::now() + Duration::from_secs(30);
    while c.cmd(&[b"DBSIZE"]) != Resp::Int((DOCS - 1 + CONTROL_DOCS) as i64) {
        assert!(
            Instant::now() < deadline,
            "shards={shards}: the keyspace never came back after restart, so the \
             enumeration assertion below would prove nothing"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let after = c.cmd(&[b"FT.SEARCH", b"vidx", b"*"]);
    assert_eq!(
        after.total(),
        DOCS as i64 - 1,
        "shards={shards}: `*` must still enumerate after a restart. The key map it \
         reads is the same one KNN resolves hits through, so if this is empty, KNN \
         is answering with synthetic vec:<id> keys too: {after:?}"
    );

    drop(c);
    guard.kill_now();
}

#[test]
fn moon695_star_enumerates_a_vector_only_index_single_shard() {
    run_all(1);
}

#[test]
fn moon695_star_enumerates_a_vector_only_index_multi_shard() {
    // The half the issue says a local-only fix would silently fail.
    run_all(4);
}
