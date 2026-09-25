/// Posting list storage for the BM25 inverted index.
///
/// Each term maps to a `PostingList` containing:
/// - `doc_ids`: RoaringBitmap of document IDs containing this term
/// - rank-aligned term frequencies (parallel to `doc_ids` iteration order)
/// - optional rank-aligned per-document position lists (for phrase queries)
///
/// Positions are optional per D-04: saves memory when positions are not
/// needed, but stores them from day one for future phrase queries and
/// HIGHLIGHT support.
///
/// # Column layout (moon#1195, moon#1220)
///
/// The rank-aligned `(tf, positions)` columns are ONE flat run while a posting
/// is small, and a sequence of runs (with their starting rank indexes) beyond
/// that. A flat `Vec` made every insert/remove at rank `i` memmove the whole
/// tail, so re-indexing an OLD document of a large corpus moved O(Σ posting
/// length) bytes — ~110 MB for 20 terms with 100K-long postings. Runs bound
/// each memmove; locating a run is a binary search over the run starts.
/// Document ids never change, so upserts keep their identity (and their
/// `score DESC, doc_id ASC` tie position) and `.tpost` is byte-identical.
///
/// Positions are stored contiguously per run ([`PosColumn`]: one `u32` end
/// offset per entry plus the concatenated positions), not as one `Vec<u32>`
/// per (term, document) — that was a 24-byte header plus a heap chunk for
/// what is usually a single `u32` (moon#1220). Because a run's position data
/// now moves with every insert/remove inside it, runs (and the flat layout)
/// are bounded by position count as well as by entry count, so documents with
/// very high term frequencies cannot turn an upsert into a large memmove.
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use std::collections::HashMap;

/// Columns stay one flat run up to this many entries (and `FLAT_POS_MAX`
/// positions).
///
/// Sizing (measured on release-fast, moon#1195): re-indexing a document costs
/// one memmove of its run per touched term (O(RUN_MAX), position-dependent)
/// plus one `u32` shift per later run (O(len / RUN_MAX), vectorised). HSET of
/// a 30-token body, 300 interleaved reps: baseline oldest/newest doc
/// 3.58 ms/0.26 ms at 50K docs and 15.5 ms/0.30 ms at 200K; with 256-entry
/// runs 0.34/0.26 ms and 0.42/0.25 ms (1024-entry runs: ~2x at 200K). The
/// remaining gap was then the per-run memmove, dominated by the 24-byte
/// `Vec<u32>` header of each position list — gone with [`PosColumn`].
const FLAT_MAX: usize = 256;
/// A chunked posting converts back to one flat run below this many entries
/// (hysteresis against `FLAT_MAX`, so a posting hovering at the boundary does
/// not convert back and forth) — and only while its positions fit in half of
/// `FLAT_POS_MAX`.
const FLAT_MIN: usize = 64;
/// A run is split once it exceeds this many entries.
const RUN_MAX: usize = 256;
/// A run shrinking below this many entries merges into a neighbour when the
/// result fits in `RUN_MAX` (and `RUN_POS_MAX`), keeping the run count
/// O(len / RUN_MIN) for ordinary term frequencies.
const RUN_MIN: usize = 32;
/// A run holding more than this many positions is split (at the entry nearest
/// the middle of its position data) — the memmove bound for postings whose
/// documents repeat the term many times. A single entry above it is a run of
/// its own.
const RUN_POS_MAX: usize = 2048;
/// The flat layout converts to runs above this many positions.
const FLAT_POS_MAX: usize = RUN_POS_MAX;

/// Rank-aligned position lists stored contiguously (moon#1220): entry `i`'s
/// positions are `data[ends[i - 1]..ends[i]]` (with `ends[-1] = 0`), so an
/// entry costs one `u32` offset plus its positions.
///
/// Offsets are `u32`: a run or flat posting holds at most `RUN_POS_MAX`
/// positions plus one entry's own list, and one document's list is bounded by
/// its token count.
#[derive(Debug, Default)]
struct PosColumn {
    ends: Vec<u32>,
    data: Vec<u32>,
}

impl PosColumn {
    /// `n` empty lists (the untracked → tracked upgrade).
    fn with_empty_lists(n: usize) -> Self {
        Self {
            ends: vec![0; n],
            data: Vec::new(),
        }
    }

    /// Pack owned lists (the `.tpost` load path).
    fn from_lists(lists: Vec<Vec<u32>>) -> Self {
        let mut col = Self {
            ends: Vec::with_capacity(lists.len()),
            data: Vec::with_capacity(lists.iter().map(Vec::len).sum()),
        };
        for list in lists {
            col.data.extend_from_slice(&list);
            col.ends.push(col.data.len() as u32);
        }
        col
    }

    #[inline]
    fn len(&self) -> usize {
        self.ends.len()
    }

    /// Positions held (all entries).
    #[inline]
    fn positions(&self) -> usize {
        self.data.len()
    }

    /// Offset of entry `i`'s first position (`i <= len()`; `len()` → the end).
    #[inline]
    fn start(&self, i: usize) -> usize {
        match i.checked_sub(1) {
            Some(prev) => self.ends.get(prev).map_or(self.data.len(), |&e| e as usize),
            None => 0,
        }
    }

    #[inline]
    fn get(&self, i: usize) -> Option<&[u32]> {
        let end = *self.ends.get(i)? as usize;
        self.data.get(self.start(i)..end)
    }

    /// Every entry's list in rank order.
    fn iter(&self) -> impl Iterator<Item = &[u32]> + '_ {
        let mut start = 0usize;
        self.ends.iter().map(move |&end| {
            let end = end as usize;
            let list = self.data.get(start..end).unwrap_or(&[]);
            start = end;
            list
        })
    }

    /// Insert a new entry at `i` (`<= len()`) holding `pos`.
    fn insert(&mut self, i: usize, pos: &[u32]) {
        let at = self.start(i);
        self.data.splice(at..at, pos.iter().copied());
        let n = pos.len() as u32;
        self.ends.insert(i, at as u32 + n);
        for e in self.ends.iter_mut().skip(i + 1) {
            *e += n;
        }
    }

    /// Append `pos` to entry `i`'s list.
    fn extend(&mut self, i: usize, pos: &[u32]) {
        let Some(&end) = self.ends.get(i) else {
            return;
        };
        let end = end as usize;
        self.data.splice(end..end, pos.iter().copied());
        let n = pos.len() as u32;
        for e in self.ends.iter_mut().skip(i) {
            *e += n;
        }
    }

    /// Remove entry `i`, returning how many positions it held.
    fn remove(&mut self, i: usize) -> usize {
        let Some(&end) = self.ends.get(i) else {
            return 0;
        };
        let (start, end) = (self.start(i), end as usize);
        self.data.drain(start..end);
        self.ends.remove(i);
        let n = (end - start) as u32;
        for e in self.ends.iter_mut().skip(i) {
            *e -= n;
        }
        end - start
    }

    /// Split off entries `at..` (`at <= len()`), rebased to start at 0.
    fn split_off(&mut self, at: usize) -> Self {
        let cut = self.start(at);
        let mut ends = self.ends.split_off(at);
        for e in &mut ends {
            *e -= cut as u32;
        }
        Self {
            ends,
            data: self.data.split_off(cut),
        }
    }

    /// Append `other`'s entries after this column's.
    fn append(&mut self, other: &Self) {
        let base = self.data.len() as u32;
        self.ends.extend(other.ends.iter().map(|&e| e + base));
        self.data.extend_from_slice(&other.data);
    }

    fn shrink_to_fit(&mut self) {
        self.ends.shrink_to_fit();
        self.data.shrink_to_fit();
    }
}

/// One contiguous slice of the rank-aligned columns of a chunked posting.
#[derive(Debug, Default)]
struct Run {
    tf: Vec<u32>,
    /// Parallel to `tf` when positions are tracked, else empty.
    pos: PosColumn,
}

impl Run {
    /// Entry at which to split an oversized run: the entry-count midpoint, or
    /// — when only the position bound is exceeded — the entry nearest the
    /// middle of the position data. `None` when the run is within bounds or is
    /// a single entry.
    fn split_point(&self) -> Option<usize> {
        let n = self.tf.len();
        if n > RUN_MAX {
            return Some(n / 2);
        }
        if n < 2 || self.pos.positions() <= RUN_POS_MAX {
            return None;
        }
        let half = (self.pos.positions() / 2) as u32;
        Some(
            self.pos
                .ends
                .partition_point(|&e| e <= half)
                .clamp(1, n - 1),
        )
    }

    fn split_off(&mut self, at: usize) -> Self {
        let tail = Self {
            tf: self.tf.split_off(at),
            pos: if self.pos.len() == 0 {
                PosColumn::default()
            } else {
                self.pos.split_off(at)
            },
        };
        // moon#1226: `Vec::split_off` gives the tail an exact-size buffer but
        // leaves this (left) half the whole pre-split capacity — about twice
        // its entries. Under ascending (append) inserts the left run is never
        // written again, so every settled run kept ~2x its tf / position
        // memory for good. One small realloc per split (every ~RUN_MAX/2
        // inserts) makes it exact-size like the tail.
        self.tf.shrink_to_fit();
        self.pos.shrink_to_fit();
        tail
    }
}

/// Columns of a long posting: consecutive runs; `starts[i]` is the rank index
/// of `runs[i]`'s first entry (`starts[0] == 0`).
#[derive(Debug, Default)]
struct Chunks {
    runs: Vec<Run>,
    starts: Vec<u32>,
}

impl Chunks {
    fn len(&self) -> usize {
        match (self.starts.last(), self.runs.last()) {
            (Some(&s), Some(r)) => s as usize + r.tf.len(),
            _ => 0,
        }
    }

    /// Positions held across every run.
    fn positions(&self) -> usize {
        self.runs.iter().map(|r| r.pos.positions()).sum()
    }

    /// Split flat columns into runs of at most `RUN_MAX / 2` entries and
    /// `RUN_POS_MAX / 2` positions (at least one entry each).
    fn from_flat(tf: Vec<u32>, pos: Option<PosColumn>) -> Self {
        let mut runs = Vec::with_capacity(tf.len() / (RUN_MAX / 2) + 1);
        let mut starts = Vec::with_capacity(runs.capacity());
        let mut i = 0;
        while i < tf.len() {
            let mut j = (i + RUN_MAX / 2).min(tf.len());
            if let Some(p) = &pos {
                // Shrink the run until its positions fit (keeping one entry).
                let base = p.start(i);
                let limit = (base + RUN_POS_MAX / 2) as u32;
                let fit = i + p.ends[i..j].partition_point(|&e| e <= limit);
                j = fit.max(i + 1);
            }
            let run_pos = match &pos {
                Some(p) => {
                    let base = p.start(i) as u32;
                    PosColumn {
                        ends: p.ends[i..j].iter().map(|&e| e - base).collect(),
                        data: p.data[base as usize..p.start(j)].to_vec(),
                    }
                }
                None => PosColumn::default(),
            };
            starts.push(i as u32);
            runs.push(Run {
                tf: tf[i..j].to_vec(),
                pos: run_pos,
            });
            i = j;
        }
        Self { runs, starts }
    }

    /// Concatenate the runs back into flat columns.
    fn into_flat(self, tracked: bool) -> (Vec<u32>, Option<PosColumn>) {
        let len = self.len();
        let mut tf = Vec::with_capacity(len);
        let mut pos = tracked.then(|| PosColumn {
            ends: Vec::with_capacity(len),
            data: Vec::with_capacity(self.positions()),
        });
        for run in &self.runs {
            tf.extend_from_slice(&run.tf);
            if let Some(p) = &mut pos {
                p.append(&run.pos);
            }
        }
        (tf, pos)
    }

    /// `(run, offset)` of rank index `idx`. `idx == len()` resolves to one past
    /// the end of the last run (the append position).
    #[inline]
    fn locate(&self, idx: usize) -> (usize, usize) {
        let r = self
            .starts
            .partition_point(|&s| s as usize <= idx)
            .saturating_sub(1);
        (r, idx - self.starts.get(r).map_or(0, |&s| s as usize))
    }

    #[inline]
    fn get(&self, idx: usize) -> Option<(&Run, usize)> {
        let (r, off) = self.locate(idx);
        let run = self.runs.get(r)?;
        (off < run.tf.len()).then_some((run, off))
    }

    /// Insert `(tf, pos)` at rank `idx`; `pos` is `Some` exactly when
    /// positions are tracked.
    fn insert(&mut self, idx: usize, tf: u32, pos: Option<&[u32]>) {
        if self.runs.is_empty() {
            self.runs.push(Run::default());
            self.starts.push(0);
        }
        let (r, off) = self.locate(idx);
        let run = &mut self.runs[r];
        run.tf.insert(off, tf);
        if let Some(p) = pos {
            run.pos.insert(off, p);
        }
        for s in &mut self.starts[r + 1..] {
            *s += 1;
        }
        self.split_if_oversized(r);
    }

    /// Add `count` to the tf of rank `idx` and append `pos` to its positions.
    fn bump(&mut self, idx: usize, count: u32, pos: Option<&[u32]>) {
        let (r, off) = self.locate(idx);
        let Some(run) = self.runs.get_mut(r) else {
            return;
        };
        let Some(tf) = run.tf.get_mut(off) else {
            return;
        };
        *tf += count;
        if let Some(p) = pos {
            run.pos.extend(off, p);
            self.split_if_oversized(r);
        }
    }

    /// Split run `r` until every piece it produces is within bounds.
    fn split_if_oversized(&mut self, r: usize) {
        let (mut r, mut end) = (r, r + 1);
        while r < end {
            match self.runs.get(r).and_then(Run::split_point) {
                Some(at) => {
                    let tail = self.runs[r].split_off(at);
                    let start = self.starts[r] + at as u32;
                    self.runs.insert(r + 1, tail);
                    self.starts.insert(r + 1, start);
                    end += 1; // the tail is one more piece to check
                }
                None => r += 1,
            }
        }
    }

    /// Remove rank index `idx` (`< len()`), returning its tf and how many
    /// positions it held.
    fn remove(&mut self, idx: usize) -> Option<(u32, usize)> {
        let (r, off) = self.locate(idx);
        let run = self.runs.get_mut(r)?;
        if off >= run.tf.len() {
            return None;
        }
        let tf = run.tf.remove(off);
        let positions = if off < run.pos.len() {
            run.pos.remove(off)
        } else {
            0
        };
        for s in &mut self.starts[r + 1..] {
            *s -= 1;
        }
        let run_len = self.runs[r].tf.len();
        if run_len == 0 {
            self.runs.remove(r);
            self.starts.remove(r);
        } else if run_len < RUN_MIN {
            let run_pos = self.runs[r].pos.positions();
            let fits = |n: &Run| {
                n.tf.len() + run_len <= RUN_MAX && n.pos.positions() + run_pos <= RUN_POS_MAX
            };
            if self.runs.get(r + 1).is_some_and(fits) {
                self.merge_into_left(r);
            } else if r > 0 && fits(&self.runs[r - 1]) {
                self.merge_into_left(r - 1);
            }
        }
        Some((tf, positions))
    }

    /// Append `runs[left + 1]` onto `runs[left]`.
    fn merge_into_left(&mut self, left: usize) {
        let right = self.runs.remove(left + 1);
        self.starts.remove(left + 1);
        let run = &mut self.runs[left];
        run.tf.extend_from_slice(&right.tf);
        run.pos.append(&right.pos);
    }
}

/// A single term's posting data across all documents.
///
/// INVARIANT (fts-posting-rank-tf, frozen): the i-th tf / position list
/// belongs to the i-th document of `doc_ids` in ascending order —
/// `idx(d) = doc_ids.rank(d) - 1`. Maintained only by
/// `PostingStore::add_term_positions` / `remove_doc` (and `from_parts`).
#[derive(Debug)]
pub struct PostingList {
    /// Bitmap of document IDs containing this term.
    pub doc_ids: RoaringBitmap,
    /// Flat rank-aligned term frequencies; empty while `chunks` is `Some`.
    term_freqs: Vec<u32>,
    /// `None` = positions not tracked. `Some` = tracked: the flat rank-aligned
    /// position lists, or empty while `chunks` is `Some`.
    positions: Option<PosColumn>,
    /// Chunked columns of a posting past the flat bounds (moon#1195).
    chunks: Option<Box<Chunks>>,
}

impl PostingList {
    /// Create a new empty posting list with position tracking.
    fn new_with_positions() -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            term_freqs: Vec::new(),
            positions: Some(PosColumn::default()),
            chunks: None,
        }
    }

    /// Create a new empty posting list without position tracking.
    fn new_without_positions() -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            term_freqs: Vec::new(),
            positions: None,
            chunks: None,
        }
    }

    /// 0-based index of `doc_id` within the rank-aligned parallel arrays.
    ///
    /// `RoaringBitmap::rank(d)` is the count of stored ids `<= d`, so for a present
    /// `doc_id` it is the 1-based sorted position; subtract one for the array index.
    /// Sub-linear (container-stride + popcount), unlike `iter().position()` (O(N)).
    #[inline]
    fn rank_index(&self, doc_id: u32) -> usize {
        (self.doc_ids.rank(doc_id) as usize).saturating_sub(1)
    }

    /// Term frequency of `doc_id` in this posting list.
    ///
    /// Returns the rank-aligned tf entry when the doc is present, else `0`
    /// (the `tf_absent` default — BM25 treats the term as not occurring). Never panics:
    /// the rank-alignment invariant guarantees the index is valid, and a defensive
    /// lookup degrades to `0` rather than indexing out of bounds.
    #[inline]
    pub fn tf(&self, doc_id: u32) -> u32 {
        if !self.doc_ids.contains(doc_id) {
            return 0;
        }
        self.tf_at(self.rank_index(doc_id))
    }

    /// Rebuild a posting list from its persisted parts (`.tpost` load).
    ///
    /// `doc_ids` must be strictly increasing, `term_freqs` (and `positions`,
    /// when present) must have exactly one entry per doc, and every `tf`
    /// must be `>= 1`. Returns `None` on any violation — the caller falls
    /// back to a rebuild; nothing partial is ever constructed.
    #[must_use]
    pub fn from_parts(
        doc_ids: &[u32],
        term_freqs: Vec<u32>,
        positions: Option<Vec<Vec<u32>>>,
    ) -> Option<Self> {
        if doc_ids.is_empty() || term_freqs.len() != doc_ids.len() {
            return None;
        }
        if positions.as_ref().is_some_and(|p| p.len() != doc_ids.len()) {
            return None;
        }
        if doc_ids.windows(2).any(|w| w[0] >= w[1]) || term_freqs.contains(&0) {
            return None;
        }
        // Offsets are u32 (see `PosColumn`); a list set this large is not a
        // real posting — refuse it (rebuild) rather than wrap.
        if positions
            .as_ref()
            .is_some_and(|p| p.iter().map(Vec::len).sum::<usize>() > u32::MAX as usize)
        {
            return None;
        }
        // `from_sorted_iter` is O(n) and rejects non-increasing input — the
        // check above already guarantees it, so `ok()?` is a belt-and-braces
        // failure path, never a panic.
        let doc_ids = RoaringBitmap::from_sorted_iter(doc_ids.iter().copied()).ok()?;
        let mut list = Self {
            doc_ids,
            term_freqs,
            positions: positions.map(PosColumn::from_lists),
            chunks: None,
        };
        list.rebalance_layout();
        Some(list)
    }

    /// Position list for `doc_id` (rank-aligned), or `None` when positions are not
    /// tracked or the doc is absent.
    #[inline]
    pub fn positions_for(&self, doc_id: u32) -> Option<&[u32]> {
        if !self.doc_ids.contains(doc_id) {
            return None;
        }
        self.positions_at(self.rank_index(doc_id))
    }

    /// Whether per-document positions are tracked for this term.
    #[inline]
    #[must_use]
    pub fn has_positions(&self) -> bool {
        self.positions.is_some()
    }

    /// Term frequencies in rank (ascending doc id) order.
    pub fn tf_values(&self) -> impl Iterator<Item = u32> + '_ {
        let runs: &[Run] = self.chunks.as_ref().map_or(&[], |c| &c.runs);
        self.term_freqs
            .iter()
            .chain(runs.iter().flat_map(|r| r.tf.iter()))
            .copied()
    }

    /// Call `f(doc, tf)` for every entry in ascending doc-id order, stopping
    /// early when `f` returns `false` — the term-at-a-time scan (moon#1220):
    /// the tf column is walked slice by slice alongside the bitmap.
    #[inline]
    pub fn for_each_tf(&self, mut f: impl FnMut(u32, u32) -> bool) {
        let mut ids = self.doc_ids.iter();
        let runs: &[Run] = self.chunks.as_ref().map_or(&[], |c| &c.runs);
        let slices =
            std::iter::once(self.term_freqs.as_slice()).chain(runs.iter().map(|r| r.tf.as_slice()));
        for slice in slices {
            for &tf in slice {
                let Some(doc) = ids.next() else {
                    return;
                };
                if !f(doc, tf) {
                    return;
                }
            }
        }
    }

    /// Position lists in rank order — empty when positions are not tracked.
    pub fn position_lists(&self) -> impl Iterator<Item = &[u32]> + '_ {
        let tracked = self.positions.is_some();
        let flat = self.positions.as_ref().map(PosColumn::iter);
        let runs: &[Run] = match &self.chunks {
            Some(c) if tracked => &c.runs,
            _ => &[],
        };
        flat.into_iter()
            .flatten()
            .chain(runs.iter().flat_map(|r| r.pos.iter()))
    }

    /// Term frequency at a rank index (`0` when out of range — never panics).
    #[inline]
    fn tf_at(&self, idx: usize) -> u32 {
        match &self.chunks {
            Some(c) => c
                .get(idx)
                .and_then(|(run, off)| run.tf.get(off).copied())
                .unwrap_or(0),
            None => self.term_freqs.get(idx).copied().unwrap_or(0),
        }
    }

    #[inline]
    fn positions_at(&self, idx: usize) -> Option<&[u32]> {
        let flat = self.positions.as_ref()?;
        match &self.chunks {
            Some(c) => c.get(idx).and_then(|(run, off)| run.pos.get(off)),
            None => flat.get(idx),
        }
    }

    /// Add `count` to the tf at rank index `idx`, appending `pos` to its
    /// positions when positions are tracked.
    fn bump_entry(&mut self, idx: usize, count: u32, pos: &[u32]) {
        let tracked = self.positions.is_some();
        match &mut self.chunks {
            Some(c) => c.bump(idx, count, tracked.then_some(pos)),
            None => {
                if let Some(tf) = self.term_freqs.get_mut(idx) {
                    *tf += count;
                    if let Some(p) = &mut self.positions {
                        p.extend(idx, pos);
                    }
                }
            }
        }
        self.rebalance_layout();
    }

    /// Insert a new entry at rank index `idx`. `pos` is stored when positions
    /// are tracked and dropped otherwise.
    fn insert_entry(&mut self, idx: usize, tf: u32, pos: &[u32]) {
        let tracked = self.positions.is_some();
        match &mut self.chunks {
            Some(c) => c.insert(idx, tf, tracked.then_some(pos)),
            None => {
                self.term_freqs.insert(idx, tf);
                if let Some(p) = &mut self.positions {
                    p.insert(idx, pos);
                }
            }
        }
        self.rebalance_layout();
    }

    /// Remove the entry at rank index `idx`, returning `(tf, positions held)`.
    fn remove_entry(&mut self, idx: usize) -> Option<(u32, usize)> {
        let removed = match &mut self.chunks {
            Some(c) => c.remove(idx)?,
            None => {
                if idx >= self.term_freqs.len() {
                    return None;
                }
                let tf = self.term_freqs.remove(idx);
                let positions = match &mut self.positions {
                    Some(p) if idx < p.len() => p.remove(idx),
                    _ => 0,
                };
                (tf, positions)
            }
        };
        self.rebalance_layout();
        Some(removed)
    }

    /// Start tracking positions: every existing entry gets an empty list.
    fn track_positions(&mut self) {
        if self.positions.is_some() {
            return;
        }
        match &mut self.chunks {
            Some(c) => {
                for run in &mut c.runs {
                    run.pos = PosColumn::with_empty_lists(run.tf.len());
                }
                self.positions = Some(PosColumn::default());
            }
            None => self.positions = Some(PosColumn::with_empty_lists(self.term_freqs.len())),
        }
    }

    /// Flat while small; chunked above `FLAT_MAX` entries or `FLAT_POS_MAX`
    /// positions; back to flat below `FLAT_MIN` entries once the positions
    /// fit in half of `FLAT_POS_MAX` (hysteresis on both bounds).
    fn rebalance_layout(&mut self) {
        match &self.chunks {
            None => {
                let positions = self.positions.as_ref().map_or(0, PosColumn::positions);
                if self.term_freqs.len() > FLAT_MAX
                    || (positions > FLAT_POS_MAX && self.term_freqs.len() > 1)
                {
                    let tf = std::mem::take(&mut self.term_freqs);
                    let pos = self.positions.as_mut().map(std::mem::take);
                    self.chunks = Some(Box::new(Chunks::from_flat(tf, pos)));
                }
            }
            Some(c) if c.len() < FLAT_MIN && c.positions() <= FLAT_POS_MAX / 2 => {
                let tracked = self.positions.is_some();
                if let Some(c) = self.chunks.take() {
                    let (tf, pos) = c.into_flat(tracked);
                    self.term_freqs = tf;
                    self.positions = pos;
                }
            }
            Some(_) => {}
        }
    }

    /// Release column capacity once the last document has left (the entry
    /// itself survives — see `PostingStore::remove_doc`).
    fn release_if_empty(&mut self) {
        if self.doc_ids.is_empty() {
            self.chunks = None;
            self.term_freqs.shrink_to_fit();
            if let Some(pos_list) = &mut self.positions {
                pos_list.shrink_to_fit();
            }
        }
    }

    /// A doc-ordered TF cursor over this posting (moon#1191).
    ///
    /// Scoring walks candidates in ascending doc-id order; the cursor walks the
    /// posting alongside them, so the rank index of the current posting entry is
    /// known without a per-document `rank()` (a bitmap-container `rank` popcounts
    /// up to 1024 words). Long gaps between consecutive candidates are crossed
    /// with a container-aware `advance_to` plus ONE `rank`, so sparse candidate
    /// sets do not pay a linear walk of a long posting either.
    #[must_use]
    pub fn cursor(&self) -> PostingCursor<'_> {
        let mut iter = self.doc_ids.iter();
        let cur = iter.next();
        let len = self.doc_ids.len();
        // Expected posting entries per doc-id of gap: decides linear step vs jump.
        let span = self.doc_ids.max().map_or(1u64, |m| u64::from(m) + 1);
        PostingCursor {
            list: self,
            iter,
            cur,
            idx: 0,
            run: 0,
            off: 0,
            density: len as f64 / span as f64,
        }
    }
}

/// Crossing a gap that is expected to hold more than this many posting entries
/// uses `advance_to` + `rank` instead of stepping entry by entry. Stepping costs
/// a few ns per entry; a bitmap-container `rank` costs up to ~1024 popcounts.
const CURSOR_JUMP_ENTRIES: f64 = 64.0;

/// Forward-only TF cursor produced by [`PostingList::cursor`].
///
/// `seek(doc)` must be called with non-decreasing `doc` values; it returns the
/// document's term frequency when `doc` is in the posting and `None` otherwise.
/// Results are identical to [`PostingList::tf`] (`None` ⇔ `tf == 0`).
pub struct PostingCursor<'a> {
    list: &'a PostingList,
    iter: roaring::bitmap::Iter<'a>,
    /// Posting entry the cursor rests on (`None` once exhausted).
    cur: Option<u32>,
    /// Rank index of `cur` in the rank-aligned columns.
    idx: usize,
    /// `(run, off)` of `idx` when the posting is chunked (unused when flat).
    run: usize,
    off: usize,
    density: f64,
}

impl PostingCursor<'_> {
    /// Advance to `doc` (non-decreasing across calls) and return its tf, or
    /// `None` when the posting does not contain it.
    #[inline]
    pub fn seek(&mut self, doc: u32) -> Option<u32> {
        let c = self.cur?;
        if c < doc {
            let expected_entries = f64::from(doc - c) * self.density;
            if expected_entries > CURSOR_JUMP_ENTRIES {
                self.iter.advance_to(doc);
                self.cur = self.iter.next();
                if let Some(n) = self.cur {
                    self.idx = self.list.rank_index(n);
                    if let Some(chunks) = &self.list.chunks {
                        (self.run, self.off) = chunks.locate(self.idx);
                    }
                }
            } else {
                while let Some(c) = self.cur {
                    if c >= doc {
                        break;
                    }
                    self.cur = self.iter.next();
                    self.step();
                }
            }
        }
        if self.cur == Some(doc) {
            Some(self.tf_here())
        } else {
            None
        }
    }

    /// Call `f(doc, tf)` for every entry from the cursor's position through
    /// `last` (inclusive), in order, leaving the cursor on the first entry
    /// after `last` — the per-term resume point of the blocked
    /// term-at-a-time fold (moon#1226). Pair with [`Self::seek`] to skip what
    /// lies before a block.
    #[inline]
    pub fn drain_through(&mut self, last: u32, mut f: impl FnMut(u32, u32)) {
        while let Some(c) = self.cur {
            if c > last {
                break;
            }
            f(c, self.tf_here());
            self.cur = self.iter.next();
            self.step();
        }
    }

    /// Move the rank position forward by one entry.
    #[inline]
    fn step(&mut self) {
        self.idx += 1;
        if let Some(chunks) = &self.list.chunks {
            self.off += 1;
            if chunks
                .runs
                .get(self.run)
                .is_some_and(|r| self.off >= r.tf.len())
            {
                self.run += 1;
                self.off = 0;
            }
        }
    }

    #[inline]
    fn tf_here(&self) -> u32 {
        match &self.list.chunks {
            Some(chunks) => chunks
                .runs
                .get(self.run)
                .and_then(|r| r.tf.get(self.off))
                .copied()
                .unwrap_or(0),
            None => self.list.term_freqs.get(self.idx).copied().unwrap_or(0),
        }
    }
}

/// Fixed per-term overhead charged exactly once, when a term's `PostingList`
/// entry is first created in `postings` (K4 P0 fix: this entry is kept
/// forever even after its last document is removed -- see `remove_doc`'s doc
/// comment -- so the cost is charged once and never refunded, matching that
/// contract). Approximates the `HashMap<u32, PostingList>` bucket overhead
/// plus the `PostingList` struct shell (its growable contents are charged
/// separately via `POSTING_OCCURRENCE_COST`/`POSITION_COST`).
const POSTING_ENTRY_OVERHEAD: usize = 48 + std::mem::size_of::<PostingList>();

/// Fixed approximate cost of one (term, doc) occurrence: one `term_freqs`
/// `u32` slot plus an amortized per-id `RoaringBitmap` cost. A flat constant
/// -- not `RoaringBitmap::serialized_size()` -- because compressed bitmap
/// size is non-linear/non-additive across arbitrary insert/remove patterns
/// and cannot be delta-tracked in O(1); this is the same "monotonic signal,
/// not exact RSS" approximation style used by `ColdIndex`/`Database::
/// entry_overhead` elsewhere in the accounting spine.
const POSTING_OCCURRENCE_COST: usize = 4 + 4;

/// Fixed approximate cost of one tracked token position (`u32`).
const POSITION_COST: usize = std::mem::size_of::<u32>();

/// Per-field inverted index storing term_id -> PostingList.
pub struct PostingStore {
    postings: HashMap<u32, PostingList>,
    /// Reverse index: doc_id -> the term_ids that document contributed (a set, no duplicates).
    /// Lets `remove_doc` visit only a document's own terms instead of scanning every posting,
    /// making per-doc removal O(terms-in-doc) instead of O(total vocabulary) — the upsert/bulk
    /// re-index cliff (fts-upsert-incremental). Kept in sync with `postings`: `add_term_occurrence`
    /// records the edge on the new-doc branch; `remove_doc` erases the doc's entry.
    doc_terms: HashMap<u32, SmallVec<[u32; 8]>>,
    /// K4 (P0 fix): O(1) cached total mirroring `estimated_bytes()`.
    /// Maintained incrementally at every mutation site (`add_term_occurrence`,
    /// `remove_doc`) instead of being recomputed by a full walk on every read
    /// -- `estimated_bytes()` used to be an O(vocabulary) walk called
    /// unconditionally every 100ms from the shard eviction tick, which does
    /// not scale with corpus size. `estimated_bytes_ground_truth`
    /// (`#[cfg(test)]`) is the walk this field must always match.
    resident_bytes: usize,
}

impl PostingStore {
    /// Create an empty posting store.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_terms: HashMap::new(),
            resident_bytes: 0,
        }
    }

    /// Add a term occurrence for a document.
    ///
    /// If the document already exists in the posting list, its term frequency
    /// is incremented and positions are appended (if provided).
    ///
    /// # Position handling
    /// - `positions: Some(pos)` -- store positions; upgrades a no-position list to have positions
    /// - `positions: None` -- don't track positions for this occurrence; keeps existing positions if any
    pub fn add_term_occurrence(&mut self, term_id: u32, doc_id: u32, positions: Option<Vec<u32>>) {
        self.add_occurrences(term_id, doc_id, 1, positions.as_deref());
    }

    /// Add one occurrence of `term_id` in `doc_id` per entry of `positions`
    /// (the token positions, ascending) — the indexing path's batched form of
    /// calling [`Self::add_term_occurrence`] once per token with
    /// `Some(vec![position])`, leaving exactly the same state: the (term,
    /// doc) entry is inserted once with all its positions instead of being
    /// inserted and then extended token by token, and no `Vec` is built per
    /// token (moon#884, moon#1220). A no-op for an empty slice.
    pub fn add_term_positions(&mut self, term_id: u32, doc_id: u32, positions: &[u32]) {
        let Ok(count) = u32::try_from(positions.len()) else {
            return;
        };
        self.add_occurrences(term_id, doc_id, count, Some(positions));
    }

    /// `count >= 1` occurrences of `term_id` in `doc_id`, with `positions`
    /// appended when given (upgrading an untracked posting).
    fn add_occurrences(
        &mut self,
        term_id: u32,
        doc_id: u32,
        count: u32,
        positions: Option<&[u32]>,
    ) {
        if count == 0 {
            return;
        }
        let is_new_term = !self.postings.contains_key(&term_id);
        let posting = self.postings.entry(term_id).or_insert_with(|| {
            if positions.is_some() {
                PostingList::new_with_positions()
            } else {
                PostingList::new_without_positions()
            }
        });
        if is_new_term {
            self.resident_bytes += POSTING_ENTRY_OVERHEAD;
        }
        let added_positions = positions.map_or(0, <[u32]>::len);
        if positions.is_some() {
            // Upgrade (first positioned occurrence of an untracked term):
            // every existing doc gets an empty list.
            posting.track_positions();
        }

        if posting.doc_ids.contains(doc_id) {
            // Existing doc: bump the tf at the rank-aligned index; append positions.
            let idx = posting.rank_index(doc_id);
            posting.bump_entry(idx, count, positions.unwrap_or_default());
            self.resident_bytes += added_positions * POSITION_COST;
        } else {
            // New document: insert into the bitmap, then insert tf/positions AT THE RANK
            // INDEX (not push) so the columns stay rank-aligned with doc_ids — correct
            // even when doc_id is not the current maximum (the document-update re-add path).
            posting.doc_ids.insert(doc_id);
            let idx = posting.rank_index(doc_id);
            posting.insert_entry(idx, count, positions.unwrap_or_default());
            // Record the (doc -> term) reverse edge exactly once: this branch fires only the first
            // time `doc_id` joins `term_id`'s posting, so no de-dup is needed. `posting`'s borrow of
            // `self.postings` has ended (last use above), so this disjoint-field access is sound.
            self.doc_terms.entry(doc_id).or_default().push(term_id);
            self.resident_bytes += POSTING_OCCURRENCE_COST + added_positions * POSITION_COST;
        }
    }

    /// Get a reference to a posting list for the given term.
    pub fn get_posting(&self, term_id: u32) -> Option<&PostingList> {
        self.postings.get(&term_id)
    }

    /// Every `(term_id, posting)` pair, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &PostingList)> {
        self.postings.iter().map(|(&t, p)| (t, p))
    }

    /// Rebuild a store from persisted lists (`.tpost` load): recomputes the
    /// `doc -> terms` map and the resident-bytes accounting with the same
    /// constants `add_term_occurrence` charges, so a loaded store reports
    /// exactly what an incrementally built one would. `None` on a duplicate
    /// `term_id` — the caller falls back to a rebuild.
    #[must_use]
    pub fn from_lists(lists: Vec<(u32, PostingList)>) -> Option<Self> {
        let mut postings: HashMap<u32, PostingList> = HashMap::with_capacity(lists.len());
        let mut doc_terms: HashMap<u32, SmallVec<[u32; 8]>> = HashMap::new();
        let mut resident_bytes = 0usize;
        for (term_id, list) in lists {
            resident_bytes += POSTING_ENTRY_OVERHEAD;
            resident_bytes += list.doc_ids.len() as usize * POSTING_OCCURRENCE_COST;
            resident_bytes += list
                .position_lists()
                .map(|p| p.len() * POSITION_COST)
                .sum::<usize>();
            for doc_id in &list.doc_ids {
                doc_terms.entry(doc_id).or_default().push(term_id);
            }
            if postings.insert(term_id, list).is_some() {
                return None;
            }
        }
        Some(Self {
            postings,
            doc_terms,
            resident_bytes,
        })
    }

    /// Number of documents containing the given term.
    pub fn doc_freq(&self, term_id: u32) -> u32 {
        self.postings
            .get(&term_id)
            .map(|p| p.doc_ids.len() as u32)
            .unwrap_or(0)
    }

    /// Number of unique terms in this store.
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }

    /// Clear all postings for a specific document (used during upsert).
    ///
    /// Returns the old term frequencies `(term_id, old_tf)` for stats adjustment (order
    /// unspecified — callers only sum it). Visits ONLY the terms this document contributed via the
    /// `doc_terms` reverse map — O(terms-in-doc), not O(total vocabulary) — eliminating the upsert /
    /// bulk re-index cliff. The empty-posting entries are intentionally left in `postings` (a fully
    /// removed term keeps an empty `PostingList`), matching the prior O(V) implementation so
    /// `doc_freq`/`tf`/search output stay byte-identical.
    pub fn remove_doc(&mut self, doc_id: u32) -> Vec<(u32, u32)> {
        // absent_doc_noop: a doc never indexed has no reverse entry -> nothing to remove.
        let Some(term_ids) = self.doc_terms.remove(&doc_id) else {
            return Vec::new();
        };
        let mut removed = Vec::with_capacity(term_ids.len());
        for term_id in term_ids {
            // stale_reverse_entry_skip: defend against a reverse edge whose posting is gone or no
            // longer holds the doc — skip, never unwrap/expect/panic.
            let Some(posting) = self.postings.get_mut(&term_id) else {
                continue;
            };
            if !posting.doc_ids.contains(doc_id) {
                continue;
            }
            // Rank-aligned index — compute BEFORE removing from the bitmap.
            let idx = posting.rank_index(doc_id);
            if let Some((old_tf, old_positions)) = posting.remove_entry(idx) {
                posting.doc_ids.remove(doc_id);
                removed.push((term_id, old_tf));
                // K4 (P0 fix): symmetric uncharge for the occurrence + its positions
                // added by `add_term_occurrence`. The entry's `POSTING_ENTRY_OVERHEAD`
                // is deliberately NOT refunded here -- the `postings` map entry itself
                // survives (see below), matching the never-refunded charge on creation.
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(POSTING_OCCURRENCE_COST + old_positions * POSITION_COST);
                // The `postings` HashMap entry itself is kept even when empty
                // (see doc comment on `remove_doc` — callers rely on
                // `tf`/`doc_freq` for a "term with zero live docs" staying
                // answerable without a fresh insert). But once the LAST doc
                // leaves, the entry's column buffers have no reason to keep
                // capacity sized for a document count of zero — release it.
                // Reallocation on the next occurrence of this term is a
                // one-time, bounded cost; the alternative is holding peak
                // capacity forever for a term that may never recur.
                posting.release_if_empty();
            }
        }
        removed
    }

    /// Reverse-map term_ids a document contributed (rank-unordered). `#[cfg(test)]` accessor.
    #[cfg(test)]
    pub(crate) fn doc_terms_for(&self, doc_id: u32) -> Option<&[u32]> {
        self.doc_terms.get(&doc_id).map(SmallVec::as_slice)
    }

    /// Number of distinct documents tracked in the reverse map. `#[cfg(test)]` accessor.
    #[cfg(test)]
    pub(crate) fn doc_terms_count(&self) -> usize {
        self.doc_terms.len()
    }

    /// Test-only: forcibly clear `doc_id` from `term_id`'s posting WITHOUT touching the reverse map,
    /// to synthesize the stale-reverse-entry state that `remove_doc` must tolerate.
    #[cfg(test)]
    pub(crate) fn test_force_clear_doc_from_posting(&mut self, term_id: u32, doc_id: u32) {
        if let Some(posting) = self.postings.get_mut(&term_id) {
            if posting.doc_ids.contains(doc_id) {
                let idx = posting.rank_index(doc_id);
                posting.remove_entry(idx);
                posting.doc_ids.remove(doc_id);
            }
        }
    }

    /// Estimated memory usage in bytes.
    ///
    /// K4 (P0 fix): O(1) cached read. This used to be an O(vocabulary) walk
    /// calling `RoaringBitmap::serialized_size()` per term -- fine as an
    /// occasional diagnostic, but this is invoked unconditionally every
    /// 100ms from the shard eviction tick (`persistence_tick.rs`), where an
    /// O(n) walk does not scale with corpus size. See
    /// `estimated_bytes_ground_truth` (`#[cfg(test)]`) for the equivalent
    /// full-walk formula this cached value must always match.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Ground-truth full recompute of `estimated_bytes()`, using the exact
    /// same fixed-cost formula as the incremental accumulator. Test-only:
    /// exists solely to assert the incremental accumulator never drifts from
    /// a from-scratch recount after a mixed mutation sequence.
    #[cfg(test)]
    pub(crate) fn estimated_bytes_ground_truth(&self) -> usize {
        let mut total = 0usize;
        for posting in self.postings.values() {
            total += POSTING_ENTRY_OVERHEAD;
            total += posting.doc_ids.len() as usize * POSTING_OCCURRENCE_COST;
            for positions in posting.position_lists() {
                total += positions.len() * POSITION_COST;
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RSS/CPU wave 5 (item A hygiene follow-up): a posting's `term_freqs`
    /// (and `positions`, when tracked) grow to the peak document count ever
    /// seen for that term. The `postings` HashMap entry is intentionally
    /// kept forever once created (existing contract — see `remove_doc` doc
    /// comment), but the per-entry `Vec` buffers must not hold onto peak
    /// capacity once every document has been removed.
    #[test]
    fn remove_doc_shrinks_now_empty_posting_capacity() {
        let mut store = PostingStore::new();
        // Below FLAT_MAX: the flat-column capacity is what this test watches.
        for doc_id in 0..200u32 {
            store.add_term_occurrence(7, doc_id, None);
        }
        let peak_cap = store.get_posting(7).unwrap().term_freqs.capacity();
        assert!(peak_cap >= 200, "expected growth to >=200, got {peak_cap}");

        for doc_id in 0..200u32 {
            store.remove_doc(doc_id);
        }

        // Entry survives (existing contract) ...
        let posting = store.get_posting(7).expect("entry must survive removal");
        assert_eq!(posting.doc_ids.len(), 0);
        assert_eq!(posting.tf(0), 0);
        // ... but its buffer no longer holds peak capacity.
        assert!(
            posting.term_freqs.capacity() < peak_cap,
            "expected shrink after last doc removed: peak={peak_cap} still={}",
            posting.term_freqs.capacity()
        );
    }

    /// Same shrink must apply to the `positions` buffer when position
    /// tracking is enabled for the term.
    #[test]
    fn remove_doc_shrinks_now_empty_posting_positions_capacity() {
        let mut store = PostingStore::new();
        for doc_id in 0..200u32 {
            store.add_term_occurrence(3, doc_id, Some(vec![doc_id]));
        }
        let cap = |p: &PostingList| {
            let col = p.positions.as_ref().unwrap();
            col.ends.capacity() + col.data.capacity()
        };
        let peak_cap = cap(store.get_posting(3).unwrap());
        assert!(peak_cap >= 400);

        for doc_id in 0..200u32 {
            store.remove_doc(doc_id);
        }

        let posting = store.get_posting(3).unwrap();
        let pos_cap = cap(posting);
        assert!(
            pos_cap < peak_cap,
            "expected positions shrink: peak={peak_cap} still={pos_cap}"
        );
    }

    /// A term that still has live documents after a removal must not be
    /// touched by the shrink (only a fully-emptied posting shrinks).
    #[test]
    fn remove_doc_does_not_shrink_still_live_posting() {
        let mut store = PostingStore::new();
        for doc_id in 0..50u32 {
            store.add_term_occurrence(1, doc_id, None);
        }
        let cap_before = store.get_posting(1).unwrap().term_freqs.capacity();

        store.remove_doc(0); // one doc gone, 49 remain live

        let posting = store.get_posting(1).unwrap();
        assert_eq!(posting.doc_ids.len(), 49);
        assert_eq!(
            posting.term_freqs.capacity(),
            cap_before,
            "must not shrink while the posting still has live docs"
        );
    }

    /// K4 (P0 fix): RED-first — the O(1) incremental `resident_bytes`
    /// accumulator maintained by `add_term_occurrence`/`remove_doc` must
    /// never drift from a from-scratch ground-truth recompute, across a
    /// mixed sequence of new terms, repeat occurrences (tf bump + position
    /// append), a position-tracking upgrade, and both full and partial doc
    /// removal (including the term_id-shared-across-docs case that leaves a
    /// posting with live docs after another doc is removed).
    #[test]
    fn estimated_bytes_matches_ground_truth_after_mixed_mutations() {
        let mut store = PostingStore::new();
        assert_eq!(store.estimated_bytes(), 0);
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // New terms, some with positions, some without.
        store.add_term_occurrence(1, 100, Some(vec![0, 3]));
        store.add_term_occurrence(2, 100, None);
        store.add_term_occurrence(3, 100, Some(vec![7]));
        store.add_term_occurrence(1, 101, Some(vec![1]));
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Repeat occurrence: tf bump + position append on an existing doc.
        store.add_term_occurrence(1, 100, Some(vec![5, 6]));
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Upgrade: term 2 had no position tracking, now gets one.
        store.add_term_occurrence(2, 101, Some(vec![2]));
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Shared term across many docs.
        for doc_id in 200..210u32 {
            store.add_term_occurrence(3, doc_id, Some(vec![doc_id]));
        }
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Partial removal: term 3 keeps live docs after doc 205 is removed.
        store.remove_doc(205);
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Full removal of a document touching multiple terms.
        store.remove_doc(100);
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );

        // Drain every remaining document -- resident_bytes must settle back
        // to the entry-overhead-only floor (never below it: entries survive
        // empty per the documented contract), matching ground truth exactly.
        for doc_id in [101, 200, 201, 202, 203, 204, 206, 207, 208, 209] {
            store.remove_doc(doc_id);
        }
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );
        assert_eq!(
            store.estimated_bytes(),
            3 * POSTING_ENTRY_OVERHEAD,
            "3 terms ever created, all doc occurrences drained -- only entry overhead remains"
        );
    }

    /// K4 (P0 fix): `estimated_bytes()` must be a pure O(1) load with no
    /// iteration in the accessor -- enforced by construction here: the
    /// accessor is called on a store sized large enough that an O(n) walk
    /// would be trivially detectable by any reasonable wall-clock budget,
    /// paired with the source-level guarantee that the method body is a
    /// single field read (see the implementation above).
    #[test]
    fn estimated_bytes_is_o1_not_a_walk() {
        let mut store = PostingStore::new();
        for term_id in 0..5_000u32 {
            for doc_id in 0..20u32 {
                store.add_term_occurrence(term_id, doc_id, Some(vec![doc_id]));
            }
        }
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            std::hint::black_box(store.estimated_bytes());
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "100k reads of estimated_bytes() took {elapsed:?} -- looks like a walk, not O(1)"
        );
    }

    /// moon#1221 review: `PostingCursor::seek` agrees with `tf()` over roaring bitmap AND array
    /// containers and across chunked columns, under forward seeks of every stride. Reused doc ids
    /// (freed by FT.INVALIDATE_RANGE) make mid-posting inserts routine, so the cursor must hold on
    /// any id layout, not only an append-only one.
    #[test]
    fn cursor_matches_tf_over_bitmap_and_array_containers() {
        let mut rng = Rng(0x5eed);
        let dense: Vec<u32> = (0..200_000u32)
            .filter(|d| d % 3 != 1 || d % 7 == 0)
            .collect();
        let sparse: Vec<u32> = (0..4_000u32).map(|i| i * 997).collect();
        let mut mixed: Vec<u32> = (0..70_000u32).filter(|d| d % 5 != 2).collect();
        mixed.extend((0..3_000u32).map(|i| 70_000 + i * 613));
        let mut checked = 0usize;
        for ids in [dense, sparse, mixed] {
            let tfs: Vec<u32> = ids.iter().map(|d| 1 + (d * 7 + d / 3) % 11).collect();
            let p = PostingList::from_parts(&ids, tfs, None).expect("parts");
            assert!(p.chunks.is_some(), "fixture must be chunked");
            let max = *ids.last().expect("non-empty") + 5_000;
            for trial in 0..60 {
                let mut c = p.cursor();
                let mut d = rng.below(64) as u32;
                while d < max {
                    let tf = p.tf(d);
                    assert_eq!(
                        c.seek(d),
                        (tf != 0).then_some(tf),
                        "trial {trial} seek({d})"
                    );
                    checked += 1;
                    d += match rng.below(10) {
                        0..=4 => 1,
                        5 | 6 => 1 + rng.below(100) as u32,
                        7 | 8 => 1 + rng.below(3_000) as u32,
                        _ => 1 + rng.below(70_000) as u32,
                    };
                }
            }
        }
        assert!(checked > 10_000, "only {checked} seeks");
    }

    // ── moon#1195: chunked rank-aligned columns ─────────────────────────────

    /// SplitMix64 — deterministic, dependency-free.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    type Model = std::collections::BTreeMap<u32, (u32, Vec<u32>)>;

    /// Layout invariants of a chunked posting (flat postings trivially hold).
    fn assert_layout(p: &PostingList) {
        if let Some(c) = &p.chunks {
            assert!(!c.runs.is_empty());
            assert_eq!(c.runs.len(), c.starts.len());
            assert_eq!(c.starts[0], 0);
            let mut expect = 0u32;
            for (run, &start) in c.runs.iter().zip(&c.starts) {
                assert_eq!(start, expect, "run starts are the running entry count");
                assert!(!run.tf.is_empty() && run.tf.len() <= RUN_MAX);
                // moon#1220: the memmove bound on position data (one entry may exceed it alone).
                assert!(
                    run.pos.positions() <= RUN_POS_MAX || run.tf.len() == 1,
                    "run of {} entries holds {} positions",
                    run.tf.len(),
                    run.pos.positions()
                );
                if p.has_positions() {
                    assert_eq!(run.pos.len(), run.tf.len());
                    assert_pos_column(&run.pos);
                } else {
                    assert!(run.pos.len() == 0 && run.pos.data.is_empty());
                }
                expect += run.tf.len() as u32;
            }
            assert!(p.term_freqs.is_empty());
            assert!(
                p.positions
                    .as_ref()
                    .is_none_or(|col| col.len() == 0 && col.data.is_empty())
            );
            assert!(c.len() >= FLAT_MIN || c.positions() > FLAT_POS_MAX / 2);
        } else {
            assert!(p.term_freqs.len() <= FLAT_MAX);
            if let Some(col) = &p.positions {
                assert_eq!(col.len(), p.term_freqs.len());
                assert!(col.positions() <= FLAT_POS_MAX || col.len() <= 1);
                assert_pos_column(col);
            }
        }
    }

    /// `ends` is non-decreasing and ends exactly at the data length.
    fn assert_pos_column(col: &PosColumn) {
        assert!(
            col.ends.windows(2).all(|w| w[0] <= w[1]),
            "ends non-decreasing"
        );
        assert_eq!(col.ends.last().map_or(0, |&e| e as usize), col.data.len());
    }

    /// Every read path of the posting agrees with the model.
    fn assert_matches(
        store: &PostingStore,
        term: u32,
        model: &Model,
        universe: u32,
        rng: &mut Rng,
    ) {
        let p = store.get_posting(term).expect("posting");
        assert_layout(p);
        assert_eq!(p.doc_ids.len() as usize, model.len());
        assert_eq!(store.doc_freq(term) as usize, model.len());
        assert_eq!(
            p.tf_values().collect::<Vec<_>>(),
            model.values().map(|(tf, _)| *tf).collect::<Vec<_>>(),
            "tf column in rank order"
        );
        if p.has_positions() {
            assert_eq!(
                p.position_lists().map(<[u32]>::to_vec).collect::<Vec<_>>(),
                model
                    .values()
                    .map(|(_, pos)| pos.clone())
                    .collect::<Vec<_>>(),
                "position column in rank order"
            );
        }
        for d in 0..universe {
            let want = model.get(&d);
            assert_eq!(p.tf(d), want.map_or(0, |(tf, _)| *tf), "tf({d})");
            if p.has_positions() {
                assert_eq!(p.positions_for(d), want.map(|(_, pos)| pos.as_slice()));
            }
        }
        // Cursor over a random ascending probe set (sparse and dense stretches).
        let mut cursor = p.cursor();
        let mut d = 0u32;
        while d < universe {
            let want = model.get(&d).map(|(tf, _)| *tf);
            assert_eq!(cursor.seek(d), want, "cursor seek({d})");
            d += 1 + if rng.below(4) == 0 {
                rng.below(600) as u32
            } else {
                0
            };
        }
        assert_eq!(
            store.estimated_bytes(),
            store.estimated_bytes_ground_truth()
        );
    }

    fn add(store: &mut PostingStore, model: &mut Model, term: u32, doc: u32, pos: u32) {
        store.add_term_occurrence(term, doc, Some(vec![pos]));
        let e = model.entry(doc).or_insert((0, Vec::new()));
        e.0 += 1;
        e.1.push(pos);
    }

    /// moon#1195: random churn across every layout transition — flat → chunked
    /// (> FLAT_MAX), run splits (> RUN_MAX), run merges / removals (< RUN_MIN),
    /// chunked → flat (< FLAT_MIN), a position-tracking upgrade while chunked —
    /// with DISTINCT per-doc tfs (CONVENTIONS: equal tfs hide misalignment).
    #[test]
    fn chunked_columns_match_the_model_under_churn() {
        const T: u32 = 1; // dense, tracked
        const U: u32 = 2; // untracked until upgraded while chunked
        const N: u32 = 4_000;
        let mut rng = Rng(1195);
        let mut store = PostingStore::new();
        let mut model = Model::new();
        let mut order: Vec<u32> = (0..N).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i as u64 + 1) as usize);
        }
        // Random-order inserts: crosses FLAT_MAX, then splits runs repeatedly.
        for (n, &d) in order.iter().enumerate() {
            for k in 0..=(d % 5) {
                add(&mut store, &mut model, T, d, d * 8 + k);
            }
            store.add_term_occurrence(U, d, None);
            if n % 997 == 0 {
                assert_matches(&store, T, &model, N, &mut rng);
            }
        }
        assert!(store.get_posting(T).is_some_and(|p| p.chunks.is_some()));
        assert_matches(&store, T, &model, N, &mut rng);

        // Upgrade the untracked, chunked posting U: every doc gets [] except one.
        assert!(!store.get_posting(U).is_some_and(PostingList::has_positions));
        store.add_term_occurrence(U, 17, Some(vec![99, 100]));
        let u = store.get_posting(U).expect("U");
        assert!(u.chunks.is_some() && u.has_positions());
        assert_layout(u);
        assert_eq!(u.positions_for(17), Some(&[99u32, 100][..]));
        assert_eq!(u.tf(17), 2);
        assert_eq!(u.positions_for(18), Some(&[][..]));

        // Remove a contiguous block (run merges/removals) and random docs.
        for d in 1_000..2_200 {
            store.remove_doc(d);
            model.remove(&d);
        }
        for _ in 0..800 {
            let d = rng.below(u64::from(N)) as u32;
            store.remove_doc(d);
            model.remove(&d);
        }
        assert_matches(&store, T, &model, N, &mut rng);
        // Re-add (upsert) old docs mid-posting with new, distinct tfs.
        for _ in 0..600 {
            let d = rng.below(u64::from(N)) as u32;
            store.remove_doc(d);
            model.remove(&d);
            for k in 0..(1 + rng.below(4) as u32) {
                add(&mut store, &mut model, T, d, 7 * k + d % 3);
            }
        }
        assert_matches(&store, T, &model, N, &mut rng);
        // Drain below FLAT_MIN: converts back to one flat run.
        let live: Vec<u32> = model.keys().copied().collect();
        for &d in live.iter().skip(FLAT_MIN - 14) {
            store.remove_doc(d);
            model.remove(&d);
        }
        assert!(store.get_posting(T).is_some_and(|p| p.chunks.is_none()));
        assert_matches(&store, T, &model, N, &mut rng);
    }

    /// moon#1220: postings whose documents repeat the term many times. Runs (and
    /// the flat layout) are bounded by POSITION count too, so an insert, extend
    /// or remove never memmoves more than about `RUN_POS_MAX` positions — a
    /// single entry above the bound is a run of its own. Skewed, distinct tfs
    /// (most 1–3, some hundreds, a few past `RUN_POS_MAX`), inserted batched
    /// (`add_term_positions`, the indexing path) and extended per token
    /// (`add_term_occurrence`), then removals, upserts and a drain; every read
    /// path agrees with the model after each phase.
    #[test]
    fn high_tf_postings_respect_the_position_bound_under_churn() {
        const H: u32 = 9;
        const N: u32 = 3_000;
        let mut rng = Rng(1220);
        let mut store = PostingStore::new();
        let mut model = Model::new();
        let tf_of = |rng: &mut Rng, d: u32| -> u32 {
            match rng.below(100) {
                0 => RUN_POS_MAX as u32 + 1 + d % 700,
                1..=6 => 100 + (d * 7) % 300,
                _ => 1 + d % 3,
            }
        };
        let put = |store: &mut PostingStore, model: &mut Model, d: u32, tf: u32| {
            let positions: Vec<u32> = (0..tf).map(|k| d * 3 + k * 2).collect();
            store.add_term_positions(H, d, &positions);
            let e = model.entry(d).or_insert((0, Vec::new()));
            e.0 += tf;
            e.1.extend_from_slice(&positions);
        };
        let mut order: Vec<u32> = (0..N).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i as u64 + 1) as usize);
        }
        for (n, &d) in order.iter().enumerate() {
            let tf = tf_of(&mut rng, d);
            put(&mut store, &mut model, d, tf);
            if n % 499 == 0 {
                assert_matches(&store, H, &model, N, &mut rng);
            }
        }
        let p = store.get_posting(H).expect("H");
        let c = p.chunks.as_ref().expect("chunked");
        assert!(
            c.runs
                .iter()
                .any(|r| r.tf.len() == 1 && r.pos.positions() > RUN_POS_MAX),
            "a single high-tf entry must form a run of its own"
        );
        assert!(
            c.runs.iter().any(|r| r.tf.len() < RUN_MAX / 2),
            "the position bound must have split runs below the entry bound"
        );
        assert_matches(&store, H, &model, N, &mut rng);
        // Per-token extends of existing entries (the pre-batching call shape).
        for _ in 0..400 {
            let d = order[rng.below(u64::from(N)) as usize];
            add(&mut store, &mut model, H, d, 1_000_000 + d);
        }
        assert_matches(&store, H, &model, N, &mut rng);
        // Removals, then upserts of old documents with new tfs.
        for _ in 0..1_200 {
            let d = rng.below(u64::from(N)) as u32;
            store.remove_doc(d);
            model.remove(&d);
        }
        assert_matches(&store, H, &model, N, &mut rng);
        for _ in 0..500 {
            let d = rng.below(u64::from(N)) as u32;
            store.remove_doc(d);
            model.remove(&d);
            let tf = tf_of(&mut rng, d);
            put(&mut store, &mut model, d, tf);
        }
        assert_matches(&store, H, &model, N, &mut rng);
        // Drain to a handful of LOW-tf docs: back to one flat run.
        let live: Vec<u32> = model.keys().copied().collect();
        for &d in &live {
            if model.len() <= 20 {
                break;
            }
            store.remove_doc(d);
            model.remove(&d);
        }
        let high: Vec<u32> = model
            .iter()
            .filter(|(_, (tf, _))| *tf > 3)
            .map(|(&d, _)| d)
            .collect();
        for d in high {
            store.remove_doc(d);
            model.remove(&d);
        }
        assert!(store.get_posting(H).is_some_and(|p| p.chunks.is_none()));
        assert_matches(&store, H, &model, N, &mut rng);
        // Batched and per-token indexing leave identical state.
        let mut batched = PostingStore::new();
        let mut per_token = PostingStore::new();
        for d in 0..600u32 {
            let tf = 1 + (d * 13) % 9 + if d % 97 == 0 { 3_000 } else { 0 };
            let positions: Vec<u32> = (0..tf).map(|k| k * 5 + d % 4).collect();
            batched.add_term_positions(4, d, &positions);
            for &p in &positions {
                per_token.add_term_occurrence(4, d, Some(vec![p]));
            }
        }
        let (a, b) = (
            batched.get_posting(4).expect("a"),
            per_token.get_posting(4).expect("b"),
        );
        assert_eq!(a.doc_ids, b.doc_ids);
        assert!(a.tf_values().eq(b.tf_values()));
        assert!(a.position_lists().eq(b.position_lists()));
        assert_eq!(batched.estimated_bytes(), per_token.estimated_bytes());
        assert_layout(a);
        assert_layout(b);
    }

    /// `from_parts` (the `.tpost` load path) builds the same columns as
    /// incremental indexing, for a posting long enough to be chunked.
    #[test]
    fn from_parts_chunks_long_postings_identically() {
        let n = 5_000u32;
        let doc_ids: Vec<u32> = (0..n).map(|i| i * 3 + (i % 2)).collect();
        let tfs: Vec<u32> = (0..n).map(|i| 1 + i % 9).collect();
        let pos: Vec<Vec<u32>> = (0..n).map(|i| vec![i; (1 + i % 9) as usize]).collect();
        let p = PostingList::from_parts(&doc_ids, tfs.clone(), Some(pos.clone())).expect("parts");
        assert!(p.chunks.is_some());
        assert_layout(&p);
        assert_eq!(p.tf_values().collect::<Vec<_>>(), tfs);
        assert_eq!(
            p.position_lists().map(<[u32]>::to_vec).collect::<Vec<_>>(),
            pos
        );
        for (i, &d) in doc_ids.iter().enumerate() {
            assert_eq!(p.tf(d), tfs[i]);
            assert_eq!(p.positions_for(d), Some(pos[i].as_slice()));
        }
    }

    /// moon#1226 red test: runs left behind by a split hold what they store,
    /// not the pre-split buffer. An ascending (append) load — the common
    /// indexing order — writes only the last run, so HEAD's left halves kept
    /// ~2x their tf / position capacity forever.
    #[test]
    fn split_runs_do_not_keep_the_pre_split_capacity() {
        let mut store = PostingStore::new();
        for d in 0..20_000u32 {
            let tf = 1 + d % 3;
            let positions: Vec<u32> = (0..tf).map(|k| k * 7 + d % 5).collect();
            store.add_term_positions(1, d, &positions);
        }
        let p = store.get_posting(1).expect("posting");
        assert_layout(p);
        let chunks = p.chunks.as_ref().expect("chunked");
        let settled = &chunks.runs[..chunks.runs.len() - 1]; // the last run is still growing
        assert!(settled.len() > 50, "fixture must split many times");
        let (mut len, mut cap) = (0usize, 0usize);
        for r in settled {
            len += r.tf.len() + r.pos.ends.len() + r.pos.data.len();
            cap += r.tf.capacity() + r.pos.ends.capacity() + r.pos.data.capacity();
        }
        assert!(
            cap * 10 <= len * 11,
            "settled runs hold {cap} u32 slots for {len} values ({:.2}x)",
            cap as f64 / len as f64
        );
    }

    /// moon#1195 red test: re-indexing the OLDEST document of a large corpus
    /// must cost the same as re-indexing the NEWEST. HEAD's flat rank-aligned
    /// columns memmoved `(len - rank) × 28` bytes per term on both the remove
    /// and the re-insert, so doc 0 paid O(Σ posting length) (~70 MB here) while
    /// the newest doc paid ~0. Best-of-N, alternating, generous 4x bound.
    #[test]
    fn upsert_cost_is_flat_across_doc_position() {
        const N: u32 = 200_000;
        const TERMS: u32 = 6;
        // Bulk-load through the `.tpost` path so the setup is O(N) even unoptimised.
        let doc_ids: Vec<u32> = (0..N).collect();
        let lists = (0..TERMS)
            .map(|t| {
                let tfs = (0..N).map(|d| 1 + (d + t) % 3).collect();
                let pos = (0..N)
                    .map(|d| vec![t; (1 + (d + t) % 3) as usize])
                    .collect();
                (
                    t,
                    PostingList::from_parts(&doc_ids, tfs, Some(pos)).expect("parts"),
                )
            })
            .collect();
        let mut store = PostingStore::from_lists(lists).expect("store");
        let upsert = |store: &mut PostingStore, d: u32| {
            let t0 = std::time::Instant::now();
            store.remove_doc(d);
            for t in 0..TERMS {
                store.add_term_occurrence(t, d, Some(vec![t]));
            }
            t0.elapsed()
        };
        let (mut first, mut last) = (std::time::Duration::MAX, std::time::Duration::MAX);
        for _ in 0..9 {
            first = first.min(upsert(&mut store, 0));
            last = last.min(upsert(&mut store, N - 1));
        }
        let floor = std::time::Duration::from_micros(50);
        assert!(
            first <= last.max(floor) * 4,
            "upserting doc 0 took {first:?} vs doc N-1 {last:?}: cost grows with posting length"
        );
        assert_eq!(store.doc_freq(0), N);
    }
}
