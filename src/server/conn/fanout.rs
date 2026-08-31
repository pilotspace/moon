//! moon#513 (A2a): splitting a SPANNING multi-key READ across its owner shards
//! so it can join the slotted pipeline batch, and folding the parts back into
//! one client reply.
//!
//! # Why this exists
//!
//! A pipelined batch buffers a foreign-shard command into `remote_groups[s]`
//! and pushes one `PipelineBatchSlotted` per shard AFTER the batch pass ends. A
//! multi-key command that spans shards had no single destination for that 1:1
//! `resp_idx -> reply` slot, so it stayed on `coordinate_multi_key`, which
//! pushes its OWN message into the same SPSC ring mid-loop — ahead of the
//! batch's earlier, still-buffered writes. That inversion is moon#507 (silent
//! write loss), which is why the ordering guard has to cut the batch for any
//! command taking that route.
//!
//! Splitting removes the second push. Each part is an ordinary entry in its
//! owner shard's slotted batch, appended at the command's own position in the
//! batch, so ordering is per-key and comes from the batch itself:
//!
//! 1. a key `k` has exactly one owner `s = key_to_shard(k, num_shards)`, a pure
//!    function of the key bytes that does not move mid-batch;
//! 2. every operation on `k` in this batch is therefore appended to
//!    `remote_groups[s]` in loop order, or executed inline against the local
//!    slice when `s` is this shard;
//! 3. the owner executes its vector with `for cmd_frame in &commands` on one
//!    thread, so program order is preserved for `k`.
//!
//! # Scope: READS ONLY
//!
//! `MGET` and `EXISTS` only. The splittable WRITES (`MSET`, `DEL`, `UNLINK`)
//! add per-part AOF serialization, per-part replication, a group-commit barrier
//! for the local leg and per-part tracking invalidation — the double-write
//! blast zone — and land separately (A2b). `MSETNX`, `BITOP` and `COPY` are not
//! per-key decomposable at all and never leave the coordinator.
//!
//! # Failure design
//!
//! There is no new I/O here and no new timeout: a part rides the SAME
//! `PipelineBatchSlotted` message, reply slot, bounded await and backpressure
//! retry as any other cross-shard command. Every failure a part can suffer
//! (backpressure give-up, reply timeout, a shard's own error) arrives as a
//! `Frame::Error` in its slot, and [`fold`] propagates the FIRST error to the
//! client rather than assembling a partial answer. A part that is somehow never
//! written keeps the [`MISSING_PART`] sentinel, which is also an error — the
//! fail-closed direction, because the alternative is a silently short `MGET`.
//!
//! Partial failure across parts is possible (one shard errors while another
//! answered), and `coordinate_multi_key` has the identical exposure today for
//! the same commands — reads, so nothing is applied either way.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::protocol::resp3::Resp3Shape;

/// Sentinel a part slot holds until a reply lands in it.
///
/// An error, not `Frame::Null`: a fold that ran over an unfilled slot would
/// otherwise hand the client a short or null-padded `MGET` that looks like a
/// legitimate miss. This is the only value that cannot be mistaken for data.
const MISSING_PART: &[u8] = b"ERR cross-shard fan-out part missing";

/// A part slot whose reply was not an aggregate the fold could read.
const BAD_PART_SHAPE: &[u8] = b"ERR malformed cross-shard fan-out reply";

/// The most shards a fan-out can address. Same bound as `command_shard_mask`'s
/// `u64` bitmask, which is the only thing that can produce a plan.
const MAX_FANOUT_PARTS: usize = 64;

/// How the parts of one fanned-out command combine into the client's reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FanoutKind {
    /// One reply element per client key, re-interleaved into CLIENT key order
    /// (`MGET`). The parts each answer an array covering their own keys in
    /// client-relative order, so the fold is a k-way merge driven by the
    /// per-key part map — never a concatenation, which would silently reorder.
    Gather,
    /// One integer per part, summed (`EXISTS`).
    SumInteger,
}

/// Where a slotted reply must be written when it comes back.
///
/// An enum rather than a bare index because the two destinations are different
/// arrays with different post-processing: a `Direct` reply is the client's and
/// gets its RESP3 shape applied, a `Part` is an intermediate that must NOT be
/// shaped — the shape was classified on the WHOLE command and belongs to the
/// folded reply (moon#460; tagging each part is the "batch encoder only walks
/// some replies" class).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplySink {
    /// Index into the batch's `responses`.
    Direct(usize),
    /// Index into [`FanoutState::parts`].
    Part(usize),
}

/// One fanned-out command, recorded at plan time and resolved at fold time.
#[derive(Clone, Copy, Debug)]
struct Plan {
    /// Slot in the batch's `responses` this fold writes.
    resp_idx: usize,
    kind: FanoutKind,
    /// Classified at ENQUEUE on the whole command, applied to the FOLDED reply.
    shape: Resp3Shape,
    /// `parts[first_part .. first_part + n_parts]`.
    first_part: usize,
    n_parts: usize,
    /// `key_part[first_key .. first_key + n_keys]` — for client key `i`, the
    /// part-local index (0-based within this command) that answers it.
    first_key: usize,
    n_keys: usize,
}

/// Batch-scoped buffers for every fan-out in one pipeline batch.
///
/// Allocated once per connection and `clear()`ed beside `remote_groups.clear()`
/// so the batch loop never allocates a plan.
#[derive(Default)]
pub(crate) struct FanoutState {
    parts: Vec<Frame>,
    key_part: Vec<u8>,
    plans: Vec<Plan>,
}

impl FanoutState {
    /// Reset for a new batch. Called beside `remote_groups.clear()`.
    pub(crate) fn clear(&mut self) {
        self.parts.clear();
        self.key_part.clear();
        self.plans.clear();
    }

    /// True when this batch has planned at least one fan-out whose parts have
    /// not been folded yet.
    ///
    /// Read by the #438 early-flush guard: a path that flushes `responses` and
    /// clears it mid-batch would strand the placeholder for a fanned-out
    /// command exactly as it would strand a `remote_groups` placeholder.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    /// Write a reply into a part slot. Out-of-range is ignored rather than
    /// panicking: the index comes from a [`ReplySink::Part`] this module
    /// issued, so a mismatch is a bug in the caller, not client input, and the
    /// fold will report it as a missing part.
    pub(crate) fn set_part(&mut self, idx: usize, reply: Frame) {
        if let Some(slot) = self.parts.get_mut(idx) {
            *slot = reply;
        }
    }
}

/// Write a failure into whichever destination a slotted entry was bound for.
///
/// One helper rather than a `match` open-coded at each of the four failure
/// sites (backpressure give-up and reply timeout, in each of the two handlers):
/// a site that forgot the `Part` arm would leave the slot holding
/// [`MISSING_PART`] — still an error, so still fail-closed, but reported as the
/// wrong cause.
pub(crate) fn fail_sink(
    sink: ReplySink,
    responses: &mut [Frame],
    st: &mut FanoutState,
    message: &'static [u8],
) {
    let err = Frame::Error(Bytes::from_static(message));
    match sink {
        ReplySink::Direct(idx) => {
            if let Some(slot) = responses.get_mut(idx) {
                *slot = err;
            }
        }
        ReplySink::Part(idx) => st.set_part(idx, err),
    }
}

/// Split `cmd args` per owner shard and record the fold.
///
/// On success `out` holds one `(shard, sub_command_frame, part_index)` per
/// touched shard, in ASCENDING shard order, and the caller must route every one
/// of them — a local shard inline against its own slice, a foreign shard into
/// `remote_groups` with [`ReplySink::Part`]. Returns `false` and leaves `st`
/// exactly as it found it when the command cannot be split, in which case the
/// caller must fall through to the coordinator unchanged.
///
/// `out` is cleared on entry; both it and `st` are batch-scoped buffers, so a
/// planned command costs no allocation beyond the sub-command frames themselves
/// — which replace, rather than add to, the ones the coordinator built.
pub(crate) fn plan(
    st: &mut FanoutState,
    out: &mut Vec<(usize, Frame, usize)>,
    cmd: &[u8],
    args: &[Frame],
    num_shards: usize,
    resp_idx: usize,
    kind: FanoutKind,
    shape: Resp3Shape,
) -> bool {
    out.clear();
    if args.is_empty() || num_shards == 0 || num_shards > MAX_FANOUT_PARTS {
        return false;
    }

    // Owner per argument, hashed ONCE per key. `key_part` holds the raw shard
    // id for now and is remapped to the part-local index below, rather than
    // walking the argv a second time — a spanning `MGET` of 1000 keys should
    // not pay 2000 hashes.
    //
    // Every argument of MGET/EXISTS is a key; anything else is a malformed argv
    // that must reach the command and earn its own error rather than be routed
    // on. `truncate` on the reject path leaves `st` byte-identical to how this
    // call found it, so a fall-through to the coordinator cannot see a
    // half-written plan.
    let first_key = st.key_part.len();
    let mut mask: u64 = 0;
    for a in args {
        let key: &[u8] = match a {
            Frame::BulkString(b) | Frame::SimpleString(b) => b.as_ref(),
            _ => {
                st.key_part.truncate(first_key);
                return false;
            }
        };
        let s = crate::shard::dispatch::key_to_shard(key, num_shards);
        mask |= 1u64 << s;
        // `num_shards <= MAX_FANOUT_PARTS` (checked above), so the shard id
        // fits a `u8` with room to spare.
        st.key_part.push(s as u8);
    }
    let n_parts = mask.count_ones() as usize;
    if n_parts < 2 {
        // A single owner is A1's case (`MultiKeyPlacement::Slotted`) and must
        // not come here — routing it as one command is strictly cheaper.
        st.key_part.truncate(first_key);
        return false;
    }

    // Shard id -> part-local index, in ascending shard order so the plan is a
    // deterministic function of the argv (a HashMap iteration order here would
    // make the same batch produce different sub-commands run to run).
    let mut shard_to_part = [u8::MAX; MAX_FANOUT_PARTS];
    {
        let mut next: u8 = 0;
        let mut rest = mask;
        while rest != 0 {
            let s = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            shard_to_part[s] = next;
            next += 1;
        }
    }
    for slot in &mut st.key_part[first_key..] {
        *slot = shard_to_part[usize::from(*slot)];
    }

    let first_part = st.parts.len();
    st.parts.resize(
        first_part + n_parts,
        Frame::Error(Bytes::from_static(MISSING_PART)),
    );

    // One sub-command per part, keys in CLIENT order within the part. One pass
    // over `args` per part rather than N scratch vectors: `n_parts` is at most
    // the key count and typically 2-4, and this allocates nothing but the
    // sub-command frames the caller needs anyway. No hashing here — the owner
    // of every key is already recorded in `key_part`.
    let mut rest = mask;
    while rest != 0 {
        let shard = rest.trailing_zeros() as usize;
        rest &= rest - 1;
        let part_local = shard_to_part[shard];
        let mut sub: Vec<Frame> = Vec::with_capacity(1 + args.len());
        sub.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
        for (i, a) in args.iter().enumerate() {
            if st.key_part[first_key + i] == part_local {
                sub.push(a.clone());
            }
        }
        out.push((
            shard,
            Frame::Array(sub.into()),
            first_part + usize::from(part_local),
        ));
    }

    st.plans.push(Plan {
        resp_idx,
        kind,
        shape,
        first_part,
        n_parts,
        first_key,
        n_keys: args.len(),
    });
    true
}

/// Fold every planned fan-out of this batch into `responses`.
///
/// Runs AFTER phase 2 has drained every remote reply slot and BEFORE the
/// responses are encoded. Idempotent per batch because [`FanoutState::clear`]
/// runs at the top of the next one.
pub(crate) fn fold(st: &FanoutState, responses: &mut [Frame], protocol_version: u8) {
    for plan in &st.plans {
        // Indexed through `get`, not `[..]`. The range is correct by
        // construction — `plan` and `parts` are written together and cleared
        // together — but this runs on the shard's event-loop thread, where a
        // panic takes the whole process down with it (the #438 batch-tail crash
        // class). An impossible state is worth one error reply, never an abort.
        let Some(parts) = st
            .parts
            .get(plan.first_part..plan.first_part + plan.n_parts)
        else {
            if let Some(slot) = responses.get_mut(plan.resp_idx) {
                *slot = Frame::Error(Bytes::from_static(MISSING_PART));
            }
            continue;
        };
        // A part that failed for ANY reason — backpressure give-up, reply
        // timeout, the shard's own error, or the never-written sentinel —
        // becomes the client's reply. Assembling the answer from the parts that
        // did arrive would report a confident, wrong result.
        let folded = match parts.iter().find_map(|p| match p {
            Frame::Error(e) => Some(Frame::Error(e.clone())),
            _ => None,
        }) {
            Some(err) => err,
            None => match plan.kind {
                FanoutKind::Gather => gather(st, plan, parts),
                FanoutKind::SumInteger => sum_integer(parts),
            },
        };
        let shaped = crate::protocol::resp3::apply_shape(plan.shape, folded, protocol_version);
        if let Some(slot) = responses.get_mut(plan.resp_idx) {
            *slot = shaped;
        }
    }
}

/// Re-interleave the parts into CLIENT key order.
fn gather(st: &FanoutState, plan: &Plan, parts: &[Frame]) -> Frame {
    // One cursor per part; `key_part` says which cursor answers each key. Parts
    // hold their keys in client-relative order, so advancing the named cursor
    // reconstructs the original order exactly.
    let mut cursor = [0usize; MAX_FANOUT_PARTS];
    let mut out: Vec<Frame> = Vec::with_capacity(plan.n_keys);
    for i in 0..plan.n_keys {
        // `get`, not `[..]` — same reason as in `fold`: this runs on the shard
        // thread and an out-of-range index there is a process abort.
        let Some(&part_local) = st.key_part.get(plan.first_key + i) else {
            return Frame::Error(Bytes::from_static(BAD_PART_SHAPE));
        };
        let p = usize::from(part_local);
        let Some(Frame::Array(elems)) = parts.get(p) else {
            return Frame::Error(Bytes::from_static(BAD_PART_SHAPE));
        };
        let Some(v) = elems.get(cursor[p]) else {
            return Frame::Error(Bytes::from_static(BAD_PART_SHAPE));
        };
        cursor[p] += 1;
        out.push(v.clone());
    }
    Frame::Array(out.into())
}

/// Sum the parts' integer replies.
fn sum_integer(parts: &[Frame]) -> Frame {
    let mut total: i64 = 0;
    for p in parts {
        let Frame::Integer(n) = p else {
            return Frame::Error(Bytes::from_static(BAD_PART_SHAPE));
        };
        total = total.saturating_add(*n);
    }
    Frame::Integer(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// `n` distinct keys whose owner is `s` at `num_shards`. Probed, never
    /// assumed — a helper that silently returned keys on the wrong shard would
    /// make every assertion below vacuous.
    fn on(prefix: &str, s: usize, num_shards: usize, n: usize) -> Vec<String> {
        let keys: Vec<String> = (0..)
            .map(|i| format!("{prefix}{s}:{i}"))
            .filter(|k| crate::shard::dispatch::key_to_shard(k.as_bytes(), num_shards) == s)
            .take(n)
            .collect();
        assert_eq!(keys.len(), n);
        keys
    }

    #[test]
    fn plan_refuses_a_single_owner_set() {
        let num_shards = 4;
        let keys = on("fo_single", 2, num_shards, 3);
        let args: Vec<Frame> = keys.iter().map(|k| bulk(k)).collect();
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(
            !plan(
                &mut st,
                &mut out,
                b"MGET",
                &args,
                num_shards,
                0,
                FanoutKind::Gather,
                Resp3Shape::None
            ),
            "a single-owner set is A1's Slotted case and must not fan out"
        );
        assert!(st.is_empty(), "a refused plan must leave no state behind");
    }

    /// A rejected plan must leave the batch state byte-identical, INCLUDING
    /// after a successful plan has already written to it — the reject path runs
    /// after the argv walk has begun, so it has real state to undo.
    #[test]
    fn a_rejected_plan_leaves_the_batch_state_untouched() {
        let num_shards = 4;
        let mut st = FanoutState::default();
        let mut out = Vec::new();

        // One good plan first, so the reject below is not undoing an empty state.
        let a = on("fo_rej", 0, num_shards, 1);
        let b = on("fo_rej", 1, num_shards, 1);
        let good = [bulk(&a[0]), bulk(&b[0])];
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &good,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        let (parts, keys, plans) = (st.parts.len(), st.key_part.len(), st.plans.len());

        // A non-string at a key position: malformed argv, must reach the
        // command and earn its own error.
        for bad in [
            vec![bulk(&a[0]), Frame::Integer(7)],
            // …and rejected LATE, after several keys have been walked.
            vec![bulk(&a[0]), bulk(&b[0]), bulk(&a[0]), Frame::Null],
        ] {
            assert!(!plan(
                &mut st,
                &mut out,
                b"MGET",
                &bad,
                num_shards,
                1,
                FanoutKind::Gather,
                Resp3Shape::None
            ));
            assert_eq!(
                (st.parts.len(), st.key_part.len(), st.plans.len()),
                (parts, keys, plans),
                "a rejected plan must not leave a half-written entry behind — \
                 the caller falls through to the coordinator and the fold must \
                 see exactly what it saw before"
            );
        }

        // A single-owner set is rejected too (it is A1's case), and also cleanly.
        let solo = [bulk(&a[0]), bulk(&a[0])];
        assert!(!plan(
            &mut st,
            &mut out,
            b"MGET",
            &solo,
            num_shards,
            1,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        assert_eq!(
            (st.parts.len(), st.key_part.len(), st.plans.len()),
            (parts, keys, plans)
        );
    }

    /// The fold must restore CLIENT order, not part order. Built with the keys
    /// deliberately interleaved so a concatenation would produce a different
    /// answer and be caught.
    #[test]
    fn gather_restores_client_key_order() {
        let num_shards = 4;
        let a = on("fo_g", 0, num_shards, 2);
        let b = on("fo_g", 3, num_shards, 2);
        // client order: a0, b0, a1, b1 — two shards, strictly interleaved.
        let args = [bulk(&a[0]), bulk(&b[0]), bulk(&a[1]), bulk(&b[1])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        assert_eq!(out.len(), 2, "two owner shards, two parts");
        assert_eq!(out[0].0, 0, "parts are emitted in ascending shard order");
        assert_eq!(out[1].0, 3);
        // Each part carries its own two keys, plus the command name.
        for (_, frame, _) in &out {
            let Frame::Array(elems) = frame else {
                panic!("a part must be a command array")
            };
            assert_eq!(elems.len(), 3);
        }

        // Answer each part with values naming their key, then fold.
        for (shard, _, part_idx) in &out {
            let vals: Vec<Frame> = if *shard == 0 {
                vec![bulk("a0"), bulk("a1")]
            } else {
                vec![bulk("b0"), bulk("b1")]
            };
            st.set_part(*part_idx, Frame::Array(vals.into()));
        }
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        let Frame::Array(got) = &responses[0] else {
            panic!("fold must produce one array: {:?}", responses[0])
        };
        let got: Vec<&[u8]> = got
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.as_ref(),
                other => panic!("unexpected element {other:?}"),
            })
            .collect();
        assert_eq!(
            got,
            vec![&b"a0"[..], &b"b0"[..], &b"a1"[..], &b"b1"[..]],
            "the fold must re-interleave into CLIENT order — concatenating the \
             parts would answer a0,a1,b0,b1 and silently mis-pair every value \
             with its key"
        );
    }

    #[test]
    fn a_repeated_key_gets_its_own_element() {
        let num_shards = 4;
        let a = on("fo_rep", 0, num_shards, 1);
        let b = on("fo_rep", 1, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0]), bulk(&a[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        for (shard, frame, part_idx) in &out {
            let Frame::Array(elems) = frame else { panic!() };
            // shard 0 was named twice, so its sub-command carries both copies.
            let want = if *shard == 0 { 3 } else { 2 };
            assert_eq!(elems.len(), want, "shard {shard} sub-command arity");
            let n = elems.len() - 1;
            let vals: Vec<Frame> = (0..n).map(|_| bulk("v")).collect();
            st.set_part(*part_idx, Frame::Array(vals.into()));
        }
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        let Frame::Array(got) = &responses[0] else {
            panic!()
        };
        assert_eq!(
            got.len(),
            3,
            "one element per NAMED key, duplicates included"
        );
    }

    #[test]
    fn sum_integer_adds_the_parts() {
        let num_shards = 4;
        let a = on("fo_sum", 0, num_shards, 1);
        let b = on("fo_sum", 2, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"EXISTS",
            &args,
            num_shards,
            0,
            FanoutKind::SumInteger,
            Resp3Shape::None
        ));
        st.set_part(out[0].2, Frame::Integer(1));
        st.set_part(out[1].2, Frame::Integer(1));
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        assert!(matches!(responses[0], Frame::Integer(2)));
    }

    /// One failed part must fail the whole reply. Assembling the answer from
    /// the parts that arrived would report a confident, wrong `MGET`.
    #[test]
    fn one_failed_part_fails_the_reply() {
        let num_shards = 4;
        let a = on("fo_err", 0, num_shards, 1);
        let b = on("fo_err", 1, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        st.set_part(out[0].2, Frame::Array(vec![bulk("v")].into()));
        st.set_part(
            out[1].2,
            Frame::Error(Bytes::from_static(b"ERR cross-shard reply timeout")),
        );
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        assert!(
            matches!(&responses[0], Frame::Error(e) if e.as_ref() == b"ERR cross-shard reply timeout"),
            "got {:?}",
            responses[0]
        );
    }

    /// A part nobody ever wrote must surface as an error, never as data. The
    /// sentinel is the whole reason the slots are pre-filled with an error.
    #[test]
    fn an_unfilled_part_is_an_error_not_a_null() {
        let num_shards = 4;
        let a = on("fo_miss", 0, num_shards, 1);
        let b = on("fo_miss", 1, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        st.set_part(out[0].2, Frame::Array(vec![bulk("v")].into()));
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        assert!(
            matches!(&responses[0], Frame::Error(e) if e.as_ref() == MISSING_PART),
            "got {:?}",
            responses[0]
        );
    }

    /// A part that answered the wrong SHAPE must not be read as data either.
    #[test]
    fn a_part_with_the_wrong_shape_is_an_error() {
        let num_shards = 4;
        let a = on("fo_shape", 0, num_shards, 1);
        let b = on("fo_shape", 1, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        st.set_part(out[0].2, Frame::Array(vec![bulk("v")].into()));
        st.set_part(out[1].2, Frame::Integer(9));
        let mut responses = vec![Frame::Null];
        fold(&st, &mut responses, 2);
        assert!(
            matches!(&responses[0], Frame::Error(e) if e.as_ref() == BAD_PART_SHAPE),
            "got {:?}",
            responses[0]
        );
    }

    /// Two fan-outs in ONE batch must not read each other's parts.
    #[test]
    fn two_fanouts_in_one_batch_stay_separate() {
        let num_shards = 4;
        let a = on("fo_two", 0, num_shards, 1);
        let b = on("fo_two", 1, num_shards, 1);
        let c = on("fo_two", 2, num_shards, 1);
        let d = on("fo_two", 3, num_shards, 1);
        let mut st = FanoutState::default();

        let args1 = [bulk(&a[0]), bulk(&b[0])];
        let mut out1 = Vec::new();
        assert!(plan(
            &mut st,
            &mut out1,
            b"MGET",
            &args1,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        let out1: Vec<(usize, Frame, usize)> = out1;

        let args2 = [bulk(&c[0]), bulk(&d[0])];
        let mut out2 = Vec::new();
        assert!(plan(
            &mut st,
            &mut out2,
            b"EXISTS",
            &args2,
            num_shards,
            1,
            FanoutKind::SumInteger,
            Resp3Shape::None
        ));

        st.set_part(out1[0].2, Frame::Array(vec![bulk("x")].into()));
        st.set_part(out1[1].2, Frame::Array(vec![bulk("y")].into()));
        st.set_part(out2[0].2, Frame::Integer(1));
        st.set_part(out2[1].2, Frame::Integer(0));

        let mut responses = vec![Frame::Null, Frame::Null];
        fold(&st, &mut responses, 2);
        let Frame::Array(got) = &responses[0] else {
            panic!("{:?}", responses[0])
        };
        assert_eq!(got.len(), 2);
        assert!(matches!(responses[1], Frame::Integer(1)));
    }

    #[test]
    fn clear_resets_the_batch() {
        let num_shards = 4;
        let a = on("fo_clr", 0, num_shards, 1);
        let b = on("fo_clr", 1, num_shards, 1);
        let args = [bulk(&a[0]), bulk(&b[0])];
        let mut st = FanoutState::default();
        let mut out = Vec::new();
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        assert!(!st.is_empty());
        st.clear();
        assert!(st.is_empty());
        assert!(st.parts.is_empty() && st.key_part.is_empty());
        // A second batch reuses the buffers and starts its indexes at 0 again.
        assert!(plan(
            &mut st,
            &mut out,
            b"MGET",
            &args,
            num_shards,
            0,
            FanoutKind::Gather,
            Resp3Shape::None
        ));
        assert_eq!(out[0].2, 0);
    }
}
