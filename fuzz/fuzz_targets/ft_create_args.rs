#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::Bytes;
use moon::command::vector_search::ft_create;
use moon::protocol::Frame;
use moon::text::store::TextStore;
use moon::vector::store::VectorStore;

/// Fuzz `FT.CREATE` argument parsing (moon#681).
///
/// This parser had no fuzz target for its whole life, and that is exactly how
/// a one-line remote crash survived in it: `FT.CREATE idx ON HASH PREFIX 1 d:
/// SCHEMA v VECTOR HNSW` -- truncated right after the algorithm keyword --
/// indexed one past the end of argv and panicked. The panic ran on a shard
/// thread, and moon escalates a shard panic to a whole-process abort, so a
/// single short line from any unauthenticated client took the server down with
/// every database and every other connection on it.
///
/// The property is simply "never panics". `FT.CREATE` walks attacker-supplied
/// argv with a hand-rolled cursor (`*pos += 1` then read), and every such
/// cursor is a bounds bug waiting to happen -- the fix for #681 added the one
/// missing guard, and this target exists so the next one is found here rather
/// than in production.
///
/// Both `VectorStore` and `TextStore` are rebuilt per input so a create that
/// succeeds cannot make a later input take a different path; each run sees an
/// empty registry, which is the state a fresh server is in.
const MAX_ARGS: usize = 128;

/// The vocabulary `FT.CREATE` actually branches on.
///
/// **This table is why the target works.** Two earlier drafts failed, and each
/// failure was measured against a deliberately un-fixed parser rather than
/// assumed:
///
///   1. Fully-arbitrary argv: 1.2M execs, nothing. `ft_create` demands the
///      preamble `idx ON HASH PREFIX 1 d: SCHEMA v VECTOR` before the vector
///      parser is reached, and random mutation does not synthesize a
///      nine-keyword sequence. It fuzzed the preamble and never got past it.
///   2. Valid skeleton + byte-level tail: 871K execs, still nothing. Reaching
///      the parser is not enough -- the crash needs the literal ASCII `HNSW`
///      in argv, and inventing a specific four-byte string by mutation is a
///      2^32 search.
///
/// So the fuzzer picks *keywords*, not bytes: one input byte selects one argv
/// element from this table. Now `HNSW` is one byte away and the search is over
/// keyword sequences -- which is the actual state space the parser walks.
const VOCAB: &[&[u8]] = &[
    b"HNSW",
    b"FLAT",
    b"TYPE",
    b"FLOAT32",
    b"DIM",
    b"DISTANCE_METRIC",
    b"L2",
    b"COSINE",
    b"IP",
    b"M",
    b"EF_CONSTRUCTION",
    b"EF_RUNTIME",
    b"COMPACT_THRESHOLD",
    b"QUANTIZATION",
    b"TQ4",
    b"SQ8",
    b"FP32",
    b"BUILD_MODE",
    b"MERGE_MODE",
    b"GRAPH_UNION",
    b"KEEP_RAW",
    b"WEIGHTED",
    b"0",
    b"1",
    b"2",
    b"4",
    b"6",
    b"8",
    b"16",
    b"768",
    b"-1",
    b"99999999999999999999",
    b"",
    b"NOT_A_KEYWORD",
];

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

/// A well-formed `FT.CREATE` up to and including the `VECTOR` keyword, so the
/// fuzzed tail lands exactly where `parse_vector_field_params` starts reading.
fn skeleton() -> Vec<Frame> {
    [
        b"idx".as_slice(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"d:",
        b"SCHEMA",
        b"v",
        b"VECTOR",
    ]
    .iter()
    .map(|s| bulk(s))
    .collect()
}

/// Decode `data` into an argv: one byte per argv element.
///
/// A byte selects a `VOCAB` entry; two reserved residues emit an `Integer` and
/// a `Null` instead, because a non-string where a keyword belongs is a
/// malformed invocation the parser still has to survive, and `extract_bulk`
/// returning `None` drives cursor arithmetic the all-strings shape never
/// reaches.
///
/// One input in four is argv with no skeleton at all, so the preamble parser
/// -- everything before `VECTOR` -- is not left uncovered by the specialisation.
fn decode(data: &[u8]) -> Vec<Frame> {
    if data.is_empty() {
        return Vec::new();
    }
    let tag = data[0];
    let tail: Vec<Frame> = data[1..]
        .iter()
        .take(MAX_ARGS)
        .enumerate()
        .map(|(i, &b)| {
            let slot = b as usize % (VOCAB.len() + 2);
            match slot.checked_sub(VOCAB.len()) {
                Some(0) => Frame::Integer(i as i64),
                Some(_) => Frame::Null,
                None => bulk(VOCAB[slot]),
            }
        })
        .collect();

    if tag % 4 == 0 {
        return tail;
    }
    let mut args = skeleton();
    args.extend(tail);
    args
}

fuzz_target!(|data: &[u8]| {
    let args = decode(data);
    // No length guard here on purpose: `take(MAX_ARGS)` already bounds the
    // tail, and an early `return` would be a silent skip -- inputs the target
    // reports as covered while never running them.
    let mut store = VectorStore::new();
    let mut text = TextStore::new();
    // The reply is not asserted on: FT.CREATE legitimately answers +OK or any
    // of a dozen errors depending on argv. Surviving the call IS the property.
    let _ = ft_create(&mut store, &mut text, &args, 0);
});
