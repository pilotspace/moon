//! `FT.CACHESEARCH` must report `cache_hit` consistently across metrics (moon#748).
//!
//! The bug: `is_within_threshold` used `distance >= threshold` for `COSINE` and
//! `INNER_PRODUCT`, on the belief that those metrics return a *similarity* where
//! higher is closer. They do not. Every scoring path in the engine goes through
//! `l2_f32`, and the unit-sphere metrics normalize first, so `‖a−b‖² = 2−2·cos`
//! — a monotonic function of cosine *distance*, where LOWER is closer, exactly
//! like L2. The predicate was therefore reversed: a query identical to a cached
//! entry reported a MISS, and an unrelated query reported a HIT.
//!
//! The same inversion appeared a second time, in the best-candidate comparison
//! that picks between entries already inside the threshold. That one is not
//! visible from the hit/miss flag alone — it silently returns the FARTHEST
//! cached answer instead of the nearest — so this test asserts on the returned
//! KEY, not just on `cache_hit`.
//!
//! ## Why this has to be end-to-end
//!
//! The pre-existing unit tests (`src/command/vector_search/tests.rs`) called the
//! predicate with hand-written numbers chosen to match the wrong assumption, so
//! they passed while the product was broken. Nothing fed a score produced by the
//! real engine into the predicate. Only a live server closes that gap: it proves
//! the distance the engine actually emits and the predicate that consumes it
//! agree about which direction means "closer".
//!
//! ## Scope
//!
//! Single shard. `FT.CACHESEARCH` is local-only in all three dispatch paths — it
//! probes the store of whichever shard serves the connection and never scatters —
//! so at `--shards 4` a cache entry on another shard is simply invisible. That is
//! a real gap, but it is a different one from #748 and folding it in here would
//! make this test fail for a reason it is not about.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DIM: usize = 8;

/// Threshold used by every probe below.
///
/// Sits far above the quantization floor (an exact self-match scores ~0.006, not
/// 0.0, because vectors are stored quantized) and far below the distance of the
/// deliberately-unrelated vector, so neither the HIT nor the MISS assertion is
/// riding on a rounding edge.
const THRESHOLD: &str = "0.5";

// ─── Minimal RESP client ─────────────────────────────────────────────────────
// FLOAT32 blobs are full of NUL bytes, so probes must be length-aware over a raw
// socket; a redis-cli-shaped probe cannot carry them.

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<Resp>),
    Err(String),
    Simple(String),
    Nil,
}

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

// ─── Fixtures ────────────────────────────────────────────────────────────────

fn blob(v: &[f32; DIM]) -> Vec<u8> {
    let mut out = Vec::with_capacity(DIM * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// The cached entry the query is identical to. Unit length.
const EXACT: [f32; DIM] = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

/// Unit length, cosine distance 0.1 from `EXACT` (cos = 0.9, sin ≈ 0.43589).
/// Inside `THRESHOLD`, but clearly farther than `EXACT` — this is the entry a
/// reversed best-pick comparison wrongly prefers.
const NEAR: [f32; DIM] = [0.9, 0.435_889_9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

/// Orthogonal to `EXACT`: cosine distance 1.0, squared-L2 2.0. Outside
/// `THRESHOLD` under every metric, so a probe with this vector must MISS.
const FAR: [f32; DIM] = [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

/// Owns a spawned server and kills it on drop.
///
/// The kill MUST NOT be a plain statement at the end of a test: every fixture
/// helper below asserts, and a panic in one skips straight past such a line and
/// orphans the process. That is not hypothetical — a failing run during
/// development left a server alive for 1h29m holding a port whose temp dir had
/// already been deleted. `Drop` runs during unwind, so it covers every path.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_on(port: u16, dir: &std::path::Path) -> Server {
    let child = Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)");
    Server(child)
}

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

/// Create an index over `c:` and `d:` with `metric`, then load the fixtures.
///
/// Two prefixes, on purpose. `c:` holds the cache entries a `FT.CACHESEARCH`
/// probe is allowed to match; `d:` holds an ordinary document that the cache
/// probe must never consider. Putting the deliberately-far vector under `d:`
/// gives the RANGE filter something it is required to exclude without making it
/// a legitimate cache hit for the far-query probe.
fn seed(c: &mut Conn, index: &str, metric: &str) {
    let dim = DIM.to_string();
    let created = c.cmd(&[
        b"FT.CREATE",
        index.as_bytes(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"2",
        b"c:",
        b"d:",
        b"SCHEMA",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        dim.as_bytes(),
        b"DISTANCE_METRIC",
        metric.as_bytes(),
    ]);
    assert_eq!(
        created,
        Resp::Simple("OK".into()),
        "FT.CREATE {index} with metric {metric} failed: {created:?}"
    );

    for (key, v) in [("c:exact", &EXACT), ("c:near", &NEAR), ("d:far", &FAR)] {
        let r = c.cmd(&[b"HSET", key.as_bytes(), b"vec", &blob(v)]);
        assert_eq!(r, Resp::Int(1), "HSET {key} failed: {r:?}");
    }
}

/// Run `FT.SEARCH ... RANGE <threshold>` and return the matched keys, sorted.
fn range_search(c: &mut Conn, index: &str, query: &[f32; DIM], threshold: &str) -> Vec<Vec<u8>> {
    let reply = c.cmd(&[
        b"FT.SEARCH",
        index.as_bytes(),
        b"*=>[KNN 10 @vec $q]",
        b"PARAMS",
        b"2",
        b"q",
        &blob(query),
        b"RANGE",
        threshold.as_bytes(),
        b"DIALECT",
        b"2",
    ]);
    let items = match &reply {
        Resp::Arr(items) => items,
        other => panic!("FT.SEARCH RANGE did not return an array: {other:?}"),
    };
    let mut keys: Vec<Vec<u8>> = items[1..]
        .iter()
        .step_by(2)
        .filter_map(|f| match f {
            Resp::Bulk(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    keys.sort();
    keys
}

/// Run one FT.CACHESEARCH and return `(cache_hit, key)`.
///
/// `key` is `None` on a miss — the fallback reply is an ordinary KNN result set,
/// not a single cached answer, so there is no "the cached key" to report.
fn probe(c: &mut Conn, index: &str, query: &[f32; DIM]) -> (bool, Option<Vec<u8>>) {
    let reply = c.cmd(&[
        b"FT.CACHESEARCH",
        index.as_bytes(),
        b"c:",
        b"*=>[KNN 10 @vec $q]",
        b"PARAMS",
        b"2",
        b"q",
        &blob(query),
        b"THRESHOLD",
        THRESHOLD.as_bytes(),
        b"FALLBACK",
        b"KNN",
        b"10",
    ]);

    let items = match &reply {
        Resp::Arr(items) => items,
        other => panic!("FT.CACHESEARCH did not return an array: {other:?}"),
    };

    // Scan every field array for the cache_hit marker rather than assuming a
    // position: a hit and a miss have different reply shapes.
    let mut hit = None;
    for item in items {
        if let Resp::Arr(fields) = item {
            let mut i = 0;
            while i + 1 < fields.len() {
                if let (Resp::Bulk(name), Resp::Bulk(val)) = (&fields[i], &fields[i + 1])
                    && name.as_slice() == b"cache_hit"
                {
                    hit = Some(val.as_slice() == b"true");
                }
                i += 2;
            }
        }
    }
    let hit = hit.unwrap_or_else(|| panic!("no cache_hit field in reply: {reply:?}"));

    let key = if hit {
        match items.get(1) {
            Some(Resp::Bulk(k)) => Some(k.clone()),
            other => panic!("hit reply has no key at [1]: {other:?}"),
        }
    } else {
        None
    };
    (hit, key)
}

/// The whole point of #748: identical vector ⇒ HIT, unrelated vector ⇒ MISS, and
/// the hit names the NEAREST cached entry. Asserted identically for all three
/// metrics, because all three rank by a lower-is-closer distance.
#[test]
fn cache_hit_direction_is_the_same_for_every_metric() {
    let mut failures: Vec<String> = Vec::new();

    for (metric, index) in [("L2", "idx_l2"), ("COSINE", "idx_cos"), ("IP", "idx_ip")] {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = common::reserve_port();
        let _server = spawn_on(port, dir.path());
        await_ready(port);

        let mut c = Conn::open(port);
        seed(&mut c, index, metric);

        // 1. A query identical to a cached entry must HIT.
        let (hit, key) = probe(&mut c, index, &EXACT);
        if !hit {
            failures.push(format!(
                "{metric}: exact-match query reported MISS (threshold {THRESHOLD}); \
                 the threshold predicate is reversed for this metric"
            ));
        } else if key.as_deref() != Some(b"c:exact".as_slice()) {
            failures.push(format!(
                "{metric}: hit returned {:?}, expected c:exact — among entries inside \
                 the threshold the best-candidate comparison picked the FARTHER one",
                key.as_deref().map(String::from_utf8_lossy)
            ));
        }

        // 2. An unrelated query must MISS.
        let (far_hit, _) = probe(&mut c, index, &FAR);
        if far_hit {
            failures.push(format!(
                "{metric}: unrelated query reported HIT (threshold {THRESHOLD}); \
                 the threshold predicate is reversed for this metric"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "FT.CACHESEARCH cache_hit is metric-dependent (moon#748):\n  {}",
        failures.join("\n  ")
    );
}

/// The same inversion, third occurrence: `FT.SEARCH ... RANGE <threshold>`.
///
/// `apply_range_filter` branched on the metric exactly the way
/// `is_within_threshold` did, so on a COSINE or IP index a RANGE query kept the
/// results FARTHEST from the query and discarded the nearest — the precise
/// inverse of what RANGE means. Found by sweeping for the comparison pattern
/// rather than by trusting that moon#748 had listed every site.
#[test]
fn range_filter_keeps_the_near_results_for_every_metric() {
    let mut failures: Vec<String> = Vec::new();

    for (metric, index) in [("L2", "idx_l2"), ("COSINE", "idx_cos"), ("IP", "idx_ip")] {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = common::reserve_port();
        let _server = spawn_on(port, dir.path());
        await_ready(port);

        let mut c = Conn::open(port);
        seed(&mut c, index, metric);

        // Query == EXACT. c:exact (~0.006) and c:near (0.1 cosine / 0.2 sq-L2)
        // are inside 0.5; d:far (1.0 cosine / 2.0 sq-L2) is outside it.
        let got = range_search(&mut c, index, &EXACT, THRESHOLD);
        let want: Vec<Vec<u8>> = vec![b"c:exact".to_vec(), b"c:near".to_vec()];
        if got != want {
            let show = |v: &Vec<Vec<u8>>| {
                v.iter()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            failures.push(format!(
                "{metric}: RANGE {THRESHOLD} returned [{}], expected [{}]",
                show(&got),
                show(&want)
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "FT.SEARCH RANGE keeps the wrong side of the threshold (moon#748):\n  {}",
        failures.join("\n  ")
    );
}

/// Guard for the OTHER direction, which the #748 fix could easily have broken.
///
/// `apply_range_filter` is applied to three different score conventions, and
/// before this change it chose its comparison from the index's *dense* metric in
/// all of them. Dense KNN results are distances (lower is closer), but sparse
/// results are raw dot products and RRF fusion scores are accumulated
/// `1/(k+rank)` terms — both higher-is-better. So "just make it `<=`
/// everywhere", which is right for the dense bug in #748, silently inverts the
/// hybrid path instead. This pins that direction.
///
/// RRF scores sit near `1/(60+1) ≈ 0.0164` for any small rank, so a `RANGE 0.01`
/// keeps every fused result under the correct (higher-is-better) comparison and
/// drops every one of them under the reversed one. Two documents in, two out.
///
/// Only the hybrid site is covered. The sparse-ONLY site cannot be reached from
/// a live server at all: `SparseStore::insert` has no production caller — HSET
/// never populates a sparse store — so `sparse_stores` is always empty and that
/// branch always filters an empty list. Worth knowing, but a separate problem
/// from #748.
#[test]
fn hybrid_range_keeps_the_high_scoring_documents() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = common::reserve_port();
    let _server = spawn_on(port, dir.path());
    await_ready(port);

    let mut c = Conn::open(port);
    seed(&mut c, "hidx", "L2");

    // Sparse blob: alternating u32 LE dim id + f32 LE weight. Present only to
    // select the hybrid branch — the sparse side contributes no results.
    let sparse_q: Vec<u8> = {
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&1.0f32.to_le_bytes());
        out
    };

    let reply = c.cmd(&[
        b"FT.SEARCH",
        b"hidx",
        b"*=>[KNN 2 @vec $q]",
        b"SPARSE",
        b"@vec",
        b"$sq",
        b"PARAMS",
        b"4",
        b"q",
        &blob(&EXACT),
        b"sq",
        &sparse_q,
        b"RANGE",
        b"0.01",
        b"DIALECT",
        b"2",
    ]);

    let items = match &reply {
        Resp::Arr(items) => items,
        other => panic!("hybrid FT.SEARCH did not return an array: {other:?}"),
    };
    // The hybrid reply appends `dense_hits`/`sparse_hits` metadata after the
    // documents, so walk pairs and keep only those whose value is a field array.
    let mut keys: Vec<String> = Vec::new();
    let mut i = 1;
    while i + 1 < items.len() {
        if let (Resp::Bulk(k), Resp::Arr(_)) = (&items[i], &items[i + 1]) {
            keys.push(String::from_utf8_lossy(k).into_owned());
        }
        i += 2;
    }
    keys.sort();

    assert_eq!(
        keys,
        vec!["c:exact".to_string(), "c:near".to_string()],
        "hybrid RANGE 0.01 kept {keys:?}; expected both documents. An empty result \
         means the RRF fusion scores are being compared as if they were distances"
    );
}
