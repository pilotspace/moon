//! Safe 16-level TQ-ADC distance kernels for the HNSW beam (moon#1193).
//!
//! The budgeted 16-level arm of `hnsw_search_filtered`'s `dist_bfs_budgeted`
//! used to be a single serial accumulator with bounds-checked LUT indexing —
//! a `2·code_len`-long FP-add dependency chain (1024 adds at 768d) on the
//! kernel that scores most candidates of a search once the result heap holds
//! `ef` entries. It is the ONLY arm WARM segments take, and the arm every HOT
//! segment reloaded without sub-centroid signs takes.
//!
//! [`adc16_sum_budgeted`] gives it the same structure as its unbudgeted twin:
//! eight independent accumulators, four code bytes (eight nibbles) per step,
//! and a budget check every [`CHECK_BYTES`] code bytes. It needs no `unsafe`:
//! the LUT is viewed as `&[[f32; 16]]` rows via `as_chunks::<16>()` and the
//! code as `&[[u8; 16]]` blocks, so every index is either a constant into a
//! fixed-size array or a nibble (`b & 0x0F`, `b >> 4` of a `u8`, both < 16)
//! into a `[f32; 16]` row — the compiler proves all of them in bounds.
//!
//! **LUT shape.** The code reads exactly `2·code.len()` LUT rows (one per
//! nibble), but the search hands over its whole `padded`-row LUT: for scalar
//! TQ4 that is the same count (`code_len = padded/2`), for TQ4A2 twice as
//! many (`code_len = padded/4` — one nibble per coordinate PAIR). Both
//! kernels therefore cut the row view to the code's extent ([`code_rows`])
//! BEFORE chunking, so the block/tail pairing of code bytes and rows holds
//! for any LUT that is at least that long (moon#1221 review: chunking the
//! whole LUT panicked on A2 padded 8 and scored every A2 padded-32 candidate
//! 0.0).
//!
//! **Numeric contract.** The accumulator assignment (nibble `j` of each
//! 4-byte step feeds `s[j]`) and the final reduction
//! `(s0+s1)+(s2+s3)+(s4+s5)+(s6+s7)` are identical to the unbudgeted
//! 8-accumulator loop, so whenever the budget does not trigger the returned
//! sum is BIT-IDENTICAL to what the unbudgeted path computes for the same
//! candidate (see [`adc16_sum`]). The old serial loop only agreed with it to
//! within f32 reassociation error — the same candidate could carry two
//! slightly different distances depending on which closure scored it.

/// Code bytes between two budget checks (32 nibbles = 32 LUT rows).
pub(crate) const CHECK_BYTES: usize = 16;

/// Eight accumulators — nibble `j` of a 4-byte step always feeds `s[j]`.
type Acc = [f32; 8];

/// One 4-byte step: 8 nibbles against 8 consecutive 16-entry LUT rows.
///
/// Every index is a constant into a fixed-size array or a nibble into a
/// `[f32; 16]` row, so this compiles without bounds checks.
#[inline(always)]
fn step4(s: &mut Acc, b: &[u8; 4], r: &[[f32; 16]; 8]) {
    s[0] += r[0][(b[0] & 0x0F) as usize];
    s[1] += r[1][(b[0] >> 4) as usize];
    s[2] += r[2][(b[1] & 0x0F) as usize];
    s[3] += r[3][(b[1] >> 4) as usize];
    s[4] += r[4][(b[2] & 0x0F) as usize];
    s[5] += r[5][(b[2] >> 4) as usize];
    s[6] += r[6][(b[3] & 0x0F) as usize];
    s[7] += r[7][(b[3] >> 4) as usize];
}

/// The unbudgeted twin's final reduction, in the same association order.
#[inline(always)]
fn reduce(s: &Acc) -> f32 {
    (s[0] + s[1]) + (s[2] + s[3]) + (s[4] + s[5]) + (s[6] + s[7])
}

/// One 16-byte block (4 steps) against its 32 LUT rows.
#[inline(always)]
fn block16(s: &mut Acc, code: &[u8; CHECK_BYTES], rows: &[[f32; 16]; 2 * CHECK_BYTES]) {
    let (b4, _) = code.as_chunks::<4>();
    let (r8, _) = rows.as_chunks::<8>();
    for (b, r) in b4.iter().zip(r8.iter()) {
        step4(s, b, r);
    }
}

/// Trailing `< CHECK_BYTES` code bytes: whole 4-byte steps first, then the
/// last `< 4` bytes into `s[0]`/`s[1]` — the unbudgeted twin's tail order.
/// `rows` must be exactly `2·code.len()` rows ([`code_rows`]); every pairing
/// is then a zip of equal-length chunk views — no index can go out of bounds.
#[inline(always)]
fn tail(s: &mut Acc, code: &[u8], rows: &[[f32; 16]]) {
    let (b4, b_rem) = code.as_chunks::<4>();
    let (r8, r_rem) = rows.as_chunks::<8>();
    for (b, r) in b4.iter().zip(r8.iter()) {
        step4(s, b, r);
    }
    let (r2, _) = r_rem.as_chunks::<2>();
    for (&b, r) in b_rem.iter().zip(r2.iter()) {
        s[0] += r[0][(b & 0x0F) as usize];
        s[1] += r[1][(b >> 4) as usize];
    }
}

/// The LUT rows `code` reads: the first `2·code.len()` rows of the flat
/// `rows × 16` LUT, or `None` when the LUT is shorter (a caller bug —
/// `hnsw_search_filtered` checks the length once per search).
///
/// Cutting to the code's extent is what keeps the chunked loops correct for
/// a LUT with MORE rows than the code reads (TQ4A2, see the module docs):
/// blocks and tail of the code and of the rows then line up one to one.
#[inline(always)]
fn code_rows<'a>(code: &[u8], lut: &'a [f32]) -> Option<&'a [[f32; 16]]> {
    let (rows, rest) = lut.as_chunks::<16>();
    debug_assert!(
        rest.is_empty(),
        "16-level ADC LUT length must be a multiple of 16"
    );
    rows.get(..code.len() * 2)
}

/// Unbudgeted 16-level TQ-ADC sphere sum over nibble-packed `code` — the
/// safe reference form of `dist_bfs`'s unbudgeted 16-level arm (same
/// accumulator assignment, same reduction order ⇒ same bits). Test-only:
/// the production unbudgeted arm keeps its measured-faster pointer loop.
#[cfg(test)]
#[inline]
pub(crate) fn adc16_sum(code: &[u8], lut: &[f32]) -> f32 {
    let Some(rows) = code_rows(code, lut) else {
        debug_assert!(false, "ADC LUT shorter than the code");
        return f32::MAX;
    };
    let mut s: Acc = [0.0; 8];
    let (blocks, code_tail) = code.as_chunks::<CHECK_BYTES>();
    let (row_blocks, row_tail) = rows.as_chunks::<{ 2 * CHECK_BYTES }>();
    for (cb, rb) in blocks.iter().zip(row_blocks.iter()) {
        block16(&mut s, cb, rb);
    }
    tail(&mut s, code_tail, row_tail);
    reduce(&s)
}

/// Budgeted 16-level TQ-ADC sphere sum (moon#1193).
///
/// Returns `None` as soon as the running sum exceeds `scaled_budget` at a
/// [`CHECK_BYTES`] boundary (the caller maps that to `f32::MAX`), otherwise
/// the full sum — bit-identical to [`adc16_sum`]. LUT entries are squared
/// differences (≥ 0), so partial sums are monotone and the early exit never
/// rejects a candidate whose full sum would have qualified.
///
/// `lut` may hold more rows than the code reads (TQ4A2); only the first
/// `2·code.len()` are used. A LUT shorter than that is a caller bug: it
/// asserts in debug builds and returns `None` (candidate rejected) in
/// release — never an out-of-bounds panic on the shard thread.
#[inline]
pub(crate) fn adc16_sum_budgeted(code: &[u8], lut: &[f32], scaled_budget: f32) -> Option<f32> {
    let Some(rows) = code_rows(code, lut) else {
        debug_assert!(false, "ADC LUT shorter than the code");
        return None;
    };
    let mut s: Acc = [0.0; 8];
    let (blocks, code_tail) = code.as_chunks::<CHECK_BYTES>();
    let (row_blocks, row_tail) = rows.as_chunks::<{ 2 * CHECK_BYTES }>();
    for (cb, rb) in blocks.iter().zip(row_blocks.iter()) {
        block16(&mut s, cb, rb);
        if reduce(&s) > scaled_budget {
            return None;
        }
    }
    tail(&mut s, code_tail, row_tail);
    Some(reduce(&s))
}

/// The pre-moon#1193 budgeted loop, verbatim: one serial accumulator,
/// bounds-checked indexing, budget check every 16 code bytes. Kept only as
/// the reference the equivalence tests compare against.
#[cfg(test)]
pub(crate) fn adc16_sum_budgeted_legacy(
    code_only: &[u8],
    adc_lut: &[f32],
    scaled_budget: f32,
) -> Option<f32> {
    let mut sum = 0.0f32;
    let check_interval = 16;
    let chunks = code_only.len() / check_interval;
    let remainder = code_only.len() % check_interval;
    for chunk in 0..chunks {
        let base = chunk * check_interval;
        for j in 0..check_interval {
            let i = base + j;
            let byte = code_only[i];
            let qi = i * 2;
            sum += adc_lut[qi * 16 + (byte & 0x0F) as usize];
            sum += adc_lut[(qi + 1) * 16 + (byte >> 4) as usize];
        }
        if sum > scaled_budget {
            return None;
        }
    }
    let tail = chunks * check_interval;
    for j in 0..remainder {
        let i = tail + j;
        let byte = code_only[i];
        let qi = i * 2;
        sum += adc_lut[qi * 16 + (byte & 0x0F) as usize];
        sum += adc_lut[(qi + 1) * 16 + (byte >> 4) as usize];
    }
    Some(sum)
}

/// The unbudgeted 16-level arm of `dist_bfs` (8 accumulators, 4 bytes per
/// step), re-expressed with checked indexing — the bit-level reference the
/// budgeted kernel must match.
#[cfg(test)]
pub(crate) fn adc16_sum_unbudgeted_reference(code_only: &[u8], adc_lut: &[f32]) -> f32 {
    let n = code_only.len();
    let chunks = n / 4;
    let rem = n % 4;
    let mut s = [0.0f32; 8];
    for c in 0..chunks {
        let i = c * 4;
        let (b0, b1, b2, b3) = (
            code_only[i] as usize,
            code_only[i + 1] as usize,
            code_only[i + 2] as usize,
            code_only[i + 3] as usize,
        );
        let qi0 = i * 2;
        s[0] += adc_lut[qi0 * 16 + (b0 & 0x0F)];
        s[1] += adc_lut[(qi0 + 1) * 16 + (b0 >> 4)];
        s[2] += adc_lut[(qi0 + 2) * 16 + (b1 & 0x0F)];
        s[3] += adc_lut[(qi0 + 3) * 16 + (b1 >> 4)];
        s[4] += adc_lut[(qi0 + 4) * 16 + (b2 & 0x0F)];
        s[5] += adc_lut[(qi0 + 5) * 16 + (b2 >> 4)];
        s[6] += adc_lut[(qi0 + 6) * 16 + (b3 & 0x0F)];
        s[7] += adc_lut[(qi0 + 7) * 16 + (b3 >> 4)];
    }
    let tail_start = chunks * 4;
    for j in 0..rem {
        let i = tail_start + j;
        let byte = code_only[i] as usize;
        let qi = i * 2;
        s[0] += adc_lut[qi * 16 + (byte & 0x0F)];
        s[1] += adc_lut[(qi + 1) * 16 + (byte >> 4)];
    }
    (s[0] + s[1]) + (s[2] + s[3]) + (s[4] + s[5]) + (s[6] + s[7])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// xorshift64* — deterministic, dependency-free.
    struct Rng(u64);
    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn f32_unit(&mut self) -> f32 {
            (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
        }
    }

    /// Scalar-TQ4 shape: `2 · code_len` LUT rows (see [`make_case_rows`]).
    fn make_case(rng: &mut Rng, code_len: usize) -> (Vec<u8>, Vec<f32>) {
        make_case_rows(rng, code_len, 2)
    }

    /// A realistic LUT: `(q_j - c)^2` for a unit-ish rotated query and a
    /// dimension-scaled 16-centroid codebook, `rows_per_byte · code_len`
    /// rows. The search always builds `padded` rows: scalar TQ4 has
    /// `code_len = padded/2` (2 rows per code byte — exactly the rows the
    /// code reads); TQ4A2 packs a coordinate PAIR per nibble, so `code_len =
    /// padded/4` (4 rows per code byte — the code reads only the first half).
    fn make_case_rows(rng: &mut Rng, code_len: usize, rows_per_byte: usize) -> (Vec<u8>, Vec<f32>) {
        let padded = code_len * rows_per_byte;
        let scale = 1.0 / (padded.max(1) as f32).sqrt();
        let centroids: Vec<f32> = (0..16)
            .map(|c| (c as f32 - 7.5) / 4.0 * scale * 1.5)
            .collect();
        let mut lut = Vec::with_capacity(padded * 16);
        for _ in 0..padded {
            let q = (rng.f32_unit() * 2.0 - 1.0) * 2.0 * scale;
            for &c in &centroids {
                let d = q - c;
                lut.push(d * d);
            }
        }
        let code: Vec<u8> = (0..code_len).map(|_| rng.next_u64() as u8).collect();
        (code, lut)
    }

    /// Every code length the kernel can see: padded dims are powers of two
    /// (code_len = padded/2 for scalar TQ4, padded/4 for TQ4A2), plus
    /// odd/non-multiple-of-4/16 lengths to pin the tail handling for any
    /// future layout.
    const CODE_LENS: &[usize] = &[
        1, 2, 3, 4, 5, 7, 8, 12, 15, 16, 17, 31, 32, 33, 48, 64, 100, 128, 192, 256, 384, 512,
    ];

    /// LUT rows per code byte the search hands the kernel: 2 (scalar TQ4 —
    /// the LUT has exactly one row per code nibble) and 4 (TQ4A2 — the LUT
    /// has `padded` rows, twice what the `padded/4`-byte code reads).
    const ROWS_PER_BYTE: &[usize] = &[2, 4];

    /// Every `(code_len, rows_per_byte)` shape the equivalence tests sweep.
    fn shapes() -> impl Iterator<Item = (usize, usize)> {
        ROWS_PER_BYTE
            .iter()
            .flat_map(|&r| CODE_LENS.iter().map(move |&c| (c, r)))
    }

    #[test]
    fn budgeted_matches_unbudgeted_twin_bit_for_bit() {
        // moon#1193 red test: the legacy serial loop only agrees with the
        // unbudgeted 8-accumulator path to within reassociation error, so the
        // same candidate could score differently depending on which closure
        // ran. The new kernel must reproduce the unbudgeted bits exactly —
        // for both LUT shapes (2 and 4 rows per code byte).
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut legacy_mismatch = 0usize;
        let mut total = 0usize;
        for (code_len, rows_per_byte) in shapes() {
            for _ in 0..64 {
                let (code, lut) = make_case_rows(&mut rng, code_len, rows_per_byte);
                let reference = adc16_sum_unbudgeted_reference(&code, &lut);
                let new = adc16_sum_budgeted(&code, &lut, f32::MAX).expect("no budget");
                assert_eq!(
                    new.to_bits(),
                    reference.to_bits(),
                    "code_len={code_len} rows/byte={rows_per_byte}: \
                     budgeted {new} != unbudgeted {reference}"
                );
                assert_eq!(
                    adc16_sum(&code, &lut).to_bits(),
                    reference.to_bits(),
                    "code_len={code_len} rows/byte={rows_per_byte}"
                );
                let legacy = adc16_sum_budgeted_legacy(&code, &lut, f32::MAX).expect("no budget");
                total += 1;
                if legacy.to_bits() != reference.to_bits() {
                    legacy_mismatch += 1;
                }
            }
        }
        // Documents WHY the old loop fails this property (not a tolerance).
        assert!(
            legacy_mismatch > 0,
            "legacy serial loop unexpectedly bit-identical on all {total} cases"
        );
    }

    #[test]
    fn a2_lut_rows_past_the_code_never_reach_the_sum() {
        // moon#1221 review red test. With 4 LUT rows per code byte (TQ4A2)
        // the block/tail chunking was cut from the WHOLE LUT instead of the
        // code's extent: padded 8 (A2 DIM 5-8, code_len 2) indexed an empty
        // row tail — an index-out-of-bounds panic on the shard thread — and
        // padded 32 (A2 DIM 17-32, code_len 8) zipped the code tail against
        // no rows, scoring every candidate 0.0. Rows past `2·code_len` must
        // never be read: poison them with NaN.
        for code_len in [0usize, 1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 32, 64, 256] {
            let mut rng = Rng(0x5851_F42D_4C95_7F2D ^ code_len as u64);
            let (code, mut lut) = make_case_rows(&mut rng, code_len, 4);
            let reference = adc16_sum_unbudgeted_reference(&code, &lut);
            for v in &mut lut[code_len * 2 * 16..] {
                *v = f32::NAN;
            }
            let new = adc16_sum_budgeted(&code, &lut, f32::MAX);
            assert_eq!(
                new.map(f32::to_bits),
                Some(reference.to_bits()),
                "code_len={code_len}: budgeted {new:?} != reference {reference}"
            );
            assert_eq!(adc16_sum(&code, &lut).to_bits(), reference.to_bits());
            assert_eq!(reference > 0.0, code_len > 0, "code_len={code_len}");
        }
    }

    #[test]
    fn empty_code_sums_to_zero_for_any_lut() {
        // TQ4A2 at padded 1/2 (DIM 1-2) has code_len = padded/4 = 0.
        for rows in [0usize, 2, 4, 16] {
            let lut = vec![1.0f32; rows * 16];
            assert_eq!(adc16_sum_budgeted(&[], &lut, f32::MAX), Some(0.0));
            assert_eq!(adc16_sum_budgeted(&[], &lut, -1.0), Some(0.0));
            assert_eq!(adc16_sum(&[], &lut), 0.0);
        }
    }

    /// A LUT shorter than the code is a caller bug (`hnsw_search_filtered`
    /// checks it once per search): a debug build asserts; a release build
    /// rejects the candidate instead of panicking on an index.
    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "ADC LUT shorter than the code")
    )]
    fn short_lut_rejects_the_candidate_instead_of_panicking() {
        let mut rng = Rng(3);
        for code_len in [1usize, 2, 17, 20] {
            let (code, lut) = make_case_rows(&mut rng, code_len, 2);
            let short = &lut[..lut.len() - 16];
            assert_eq!(adc16_sum_budgeted(&code, short, f32::MAX), None);
        }
    }

    #[test]
    fn budgeted_matches_legacy_within_reassociation_tolerance() {
        // Old vs new: same terms, different association order. The accepted
        // bound is the f32 reassociation bound the unbudgeted twin already
        // lives with: |Δ| <= n·eps·Σ|terms| (terms are ≥ 0 so Σ|t| = sum).
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        for (code_len, rows_per_byte) in shapes() {
            for _ in 0..64 {
                let (code, lut) = make_case_rows(&mut rng, code_len, rows_per_byte);
                let new = adc16_sum_budgeted(&code, &lut, f32::MAX).unwrap();
                let old = adc16_sum_budgeted_legacy(&code, &lut, f32::MAX).unwrap();
                let n = (code_len * 2) as f32;
                let tol = n * f32::EPSILON * old.abs().max(new.abs());
                assert!(
                    (new - old).abs() <= tol,
                    "code_len={code_len} rows/byte={rows_per_byte}: \
                     new {new} old {old} tol {tol}"
                );
            }
        }
    }

    #[test]
    fn early_exit_decisions_agree_with_legacy_except_at_the_boundary() {
        // Same check cadence (every 16 code bytes): a candidate is rejected
        // by both loops unless its partial sum lies within reassociation
        // error of the budget at some check.
        let mut rng = Rng(0x94D0_49BB_1331_11EB);
        let mut rejected = 0usize;
        for (code_len, rows_per_byte) in shapes() {
            for _ in 0..128 {
                let (code, lut) = make_case_rows(&mut rng, code_len, rows_per_byte);
                let full = adc16_sum(&code, &lut);
                // Budgets straddling the full sum and each partial boundary.
                let budget = full * (0.25 + rng.f32_unit());
                let new = adc16_sum_budgeted(&code, &lut, budget);
                let old = adc16_sum_budgeted_legacy(&code, &lut, budget);
                match (new, old) {
                    (Some(a), Some(b)) => {
                        let tol = (code_len * 2) as f32 * f32::EPSILON * a.max(b);
                        assert!((a - b).abs() <= tol);
                        // A completed sum is the full sum, bit for bit.
                        assert_eq!(a.to_bits(), full.to_bits());
                    }
                    (None, None) => rejected += 1,
                    (a, b) => {
                        let tol = (code_len * 2) as f32 * f32::EPSILON * full;
                        // Disagreement is only legal when some partial sum sits
                        // within rounding of the budget.
                        let mut near = false;
                        let blocks = code_len / CHECK_BYTES;
                        for blk in 1..=blocks {
                            let part = adc16_sum(&code[..blk * CHECK_BYTES], &lut);
                            if (part - budget).abs() <= tol {
                                near = true;
                            }
                        }
                        assert!(near, "code_len={code_len}: new {a:?} vs legacy {b:?}");
                    }
                }
                // Anything rejected by the kernel really is over budget:
                // partial sums of non-negative terms are monotone in f32.
                if new.is_none() {
                    assert!(full > budget, "rejected {full} <= budget {budget}");
                }
            }
        }
        assert!(rejected > 0, "fixture never exercised the early exit");
    }

    #[test]
    fn budget_nan_and_negative_are_handled_like_legacy() {
        let mut rng = Rng(7);
        let (code, lut) = make_case(&mut rng, 192);
        // Negative scaled budget (L2 candidate that cannot qualify): first
        // check rejects.
        assert_eq!(adc16_sum_budgeted(&code, &lut, -1.0), None);
        assert_eq!(adc16_sum_budgeted_legacy(&code, &lut, -1.0), None);
        // NaN budget: `sum > NaN` is false, so neither loop exits early.
        assert!(adc16_sum_budgeted(&code, &lut, f32::NAN).is_some());
        assert!(adc16_sum_budgeted_legacy(&code, &lut, f32::NAN).is_some());
    }
}
