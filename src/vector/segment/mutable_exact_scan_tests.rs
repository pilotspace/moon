//! BUILD_MODE EXACT: mutable-scan estimator and compaction-time QJL
//! (moon#1192).

use std::sync::Arc;

use crate::vector::distance;
use crate::vector::segment::compaction::compact;
use crate::vector::segment::holder::{MvccContext, SegmentHolder};
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::{TqCode, decode_tq_mse_scaled, encode_tq_mse_scaled};
use crate::vector::turbo_quant::inner_product::{prepare_query_prod, score_l2_prod};
use crate::vector::turbo_quant::qjl::{qjl_encode, qjl_encode_scalar_reference};
use crate::vector::turbo_quant::tq_adc::tq_l2_adc_scaled;
use crate::vector::types::{DistanceMetric, SearchTuning};

fn clustered(n: usize, dim: usize, seed: u64, spread: f32) -> Vec<Vec<f32>> {
    let mut s = seed;
    let mut next = move || {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..24)
        .map(|_| (0..dim).map(|_| next()).collect())
        .collect();
    (0..n)
        .map(|i| {
            centers[i % 24]
                .iter()
                .map(|&c| c + spread * next())
                .collect()
        })
        .collect()
}

fn normalize(v: &mut [f32]) {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

fn top_k(mut d: Vec<(f32, usize)>, k: usize) -> Vec<usize> {
    d.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    d.into_iter().take(k).map(|x| x.1).collect()
}

/// Estimator study: prod(r=0) [HEAD EXACT] vs TQ-ADC [LIGHT] vs exact.
#[test]
#[ignore = "analysis; run with --ignored --nocapture"]
fn exact_scan_estimator_study() {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let (n, k) = (3000usize, 10usize);
    for (label, dim, metric, unit_data, q_scale) in [
        (
            "cos unit 384",
            384usize,
            DistanceMetric::Cosine,
            true,
            1.0f32,
        ),
        ("l2 unit 384", 384, DistanceMetric::L2, true, 1.0),
        ("cos unit 768", 768, DistanceMetric::Cosine, true, 1.0),
        ("cos unit q*5 384", 384, DistanceMetric::Cosine, true, 5.0),
        ("l2 raw 384", 384, DistanceMetric::L2, false, 1.0),
    ] {
        let col = CollectionMetadata::with_build_mode(
            9,
            dim as u32,
            metric,
            QuantizationConfig::TurboQuant4,
            5,
            BuildMode::Exact,
        );
        let padded = col.padded_dimension as usize;
        let mut data = clustered(n, dim, 11, 0.6);
        if unit_data {
            data.iter_mut().for_each(|v| normalize(v));
        }
        let mut work = vec![0.0f32; padded];
        let codes: Vec<_> = data
            .iter()
            .map(|v| {
                encode_tq_mse_scaled(
                    v,
                    col.fwht_sign_flips.as_slice(),
                    col.codebook_boundaries_15(),
                    &mut work,
                )
            })
            .collect();
        let cb = col.codebook_16();
        let (mut r_prod, mut r_adc, mut agree) = (0usize, 0usize, 0usize);
        let queries = 60;
        for qi in 0..queries {
            let mut q: Vec<f32> = data[(qi * 53) % n]
                .iter()
                .enumerate()
                .map(|(j, &x)| x + 0.08 * (((qi * 7 + j * 3) % 11) as f32 / 11.0 - 0.5))
                .collect();
            if unit_data {
                normalize(&mut q);
            }
            q.iter_mut().for_each(|x| *x *= q_scale);
            let exact: Vec<(f32, usize)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let d = if metric == DistanceMetric::L2 {
                        v.iter()
                            .zip(&q)
                            .map(|(a, b)| (a - b) * (a - b))
                            .sum::<f32>()
                    } else {
                        let dot: f32 = v.iter().zip(&q).map(|(a, b)| a * b).sum();
                        let nv: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                        let nq: f32 = q.iter().map(|x| x * x).sum::<f32>().sqrt();
                        1.0 - dot / (nv * nq)
                    };
                    (d, i)
                })
                .collect();
            let st = prepare_query_prod(
                &q,
                &col.qjl_matrices,
                col.fwht_sign_flips.as_slice(),
                padded,
            );
            let qjl0 = vec![0u8; col.qjl_matrices.len() * dim.div_ceil(8)];
            let prod: Vec<(f32, usize)> = codes
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    (
                        score_l2_prod(&st, &c.codes, c.norm, &qjl0, 0.0, cb, dim, dim.div_ceil(8)),
                        i,
                    )
                })
                .collect();
            let qn: f32 = q.iter().map(|x| x * x).sum::<f32>().sqrt();
            let adc: Vec<(f32, usize)> = codes
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let v = tq_l2_adc_scaled(&st.q_rotated, &c.codes, c.norm, cb);
                    let d = if metric == DistanceMetric::L2 {
                        let diff = c.norm - qn;
                        if c.norm <= 0.0 {
                            diff * diff
                        } else {
                            diff * diff + (qn / c.norm) * v
                        }
                    } else {
                        v
                    };
                    (d, i)
                })
                .collect();
            let gt = top_k(exact, k);
            let tp = top_k(prod, k);
            let ta = top_k(adc, k);
            r_prod += tp.iter().filter(|x| gt.contains(x)).count();
            r_adc += ta.iter().filter(|x| gt.contains(x)).count();
            agree += ta.iter().filter(|x| tp.contains(x)).count();
        }
        let denom = (queries * k) as f32;
        println!(
            "{label}: R@{k} prod(r=0) {:.4}  adc {:.4}  overlap(prod,adc) {:.4}",
            r_prod as f32 / denom,
            r_adc as f32 / denom,
            agree as f32 / denom
        );
    }
}

fn col(
    dim: usize,
    metric: DistanceMetric,
    q: QuantizationConfig,
    mode: BuildMode,
) -> Arc<CollectionMetadata> {
    distance::init();
    Arc::new(CollectionMetadata::with_build_mode(
        21, dim as u32, metric, q, 1234, mode,
    ))
}

fn search_mvcc(holder: &SegmentHolder, q: &[f32], k: usize) -> Vec<(u32, u32)> {
    let committed = roaring::RoaringTreemap::new();
    let ctx = MvccContext {
        snapshot_lsn: 0,
        my_txn_id: 0,
        committed: &committed,
        dirty_set: &[],
        dimension: q.len() as u32,
        ef_defaulted: false,
        tuning: SearchTuning::default(),
    };
    let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 0);
    holder
        .search_mvcc(q, k, 64, &mut scratch, None, &ctx)
        .iter()
        .map(|r| (r.id.0, r.distance.to_bits()))
        .collect()
}

#[test]
fn exact_mutable_scan_is_bit_identical_to_light_scan() {
    // moon#1192 red test: HEAD scored EXACT's mutable segment with the
    // TurboQuant_prod estimator (after 8 dense d×d matvecs per query);
    // LIGHT used TQ-ADC + FastScan. Same seed ⇒ same rotation, codebook and
    // codes, so the two scans must now agree bit for bit — ids AND distances.
    for (dim, metric) in [
        (384usize, DistanceMetric::Cosine),
        (100, DistanceMetric::L2),
    ] {
        let data = clustered(600, dim, 3, 0.6);
        let exact = SegmentHolder::new(
            dim as u32,
            col(
                dim,
                metric,
                QuantizationConfig::TurboQuant4,
                BuildMode::Exact,
            ),
        );
        let light = SegmentHolder::new(
            dim as u32,
            col(
                dim,
                metric,
                QuantizationConfig::TurboQuant4,
                BuildMode::Light,
            ),
        );
        for (i, v) in data.iter().enumerate() {
            exact.load().mutable.append(i as u64, v, i as u64 + 1);
            light.load().mutable.append(i as u64, v, i as u64 + 1);
        }
        for qi in 0..12 {
            let q: Vec<f32> = data[qi * 41].iter().map(|x| x * 1.7 + 0.01).collect();
            assert_eq!(
                search_mvcc(&exact, &q, 10),
                search_mvcc(&light, &q, 10),
                "dim={dim} {metric:?} q={qi}"
            );
            let sync_e: Vec<u32> = exact
                .load()
                .mutable
                .brute_force_search(&q, None, 10)
                .iter()
                .map(|r| r.id.0)
                .collect();
            let sync_l: Vec<u32> = light
                .load()
                .mutable
                .brute_force_search(&q, None, 10)
                .iter()
                .map(|r| r.id.0)
                .collect();
            assert_eq!(sync_e, sync_l);
        }
    }
}

#[test]
fn prod_estimator_premise_qjl_term_vanishes_at_zero_residual_but_ranking_differs() {
    // The issue's premise, made precise: with residual_norm == 0 (every
    // mutable row on HEAD) the QJL signs are irrelevant to score_l2_prod —
    // but the remaining term is NOT the TQ-ADC distance:
    //   prod(r=0) = |q|² + |x|² − 2|x|⟨q̂,ĉ⟩   vs   ADC = |x|²(1 + |ĉ|² − 2⟨q̂,ĉ⟩)
    // so switching EXACT to TQ-ADC changes (improves) the ranking; it is
    // not rank-identical. See `exact_scan_estimator_study` for recall.
    let dim = 64;
    let c = col(
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        BuildMode::Exact,
    );
    let padded = c.padded_dimension as usize;
    let data = clustered(50, dim, 9, 0.5);
    let q = data[3].clone();
    let st = prepare_query_prod(&q, &c.qjl_matrices, c.fwht_sign_flips.as_slice(), padded);
    let single = dim.div_ceil(8);
    let zeros = vec![0u8; c.qjl_matrices.len() * single];
    let ones = vec![0xFFu8; c.qjl_matrices.len() * single];
    let mut work = vec![0.0f32; padded];
    for v in &data {
        let code = encode_tq_mse_scaled(
            v,
            c.fwht_sign_flips.as_slice(),
            c.codebook_boundaries_15(),
            &mut work,
        );
        let a = score_l2_prod(
            &st,
            &code.codes,
            code.norm,
            &zeros,
            0.0,
            c.codebook_16(),
            dim,
            single,
        );
        let b = score_l2_prod(
            &st,
            &code.codes,
            code.norm,
            &ones,
            0.0,
            c.codebook_16(),
            dim,
            single,
        );
        assert_eq!(a.to_bits(), b.to_bits(), "QJL term must vanish at r=0");
    }
}

#[test]
fn tq_adc_recall_is_not_worse_than_head_exact_estimator() {
    // Permanent, smaller slice of the estimator study: the switch must not
    // cost recall vs exact ground truth, and it fixes non-unit L2 (HEAD's
    // prod estimator drops the |q| factor of the cross term).
    distance::init();
    for (dim, metric, unit, min_gain) in [
        (128usize, DistanceMetric::Cosine, true, -0.02f32),
        (128, DistanceMetric::L2, false, 0.30),
    ] {
        let (r_prod, r_adc) = recall_pair(dim, metric, unit, 1200, 24, 10);
        assert!(
            r_adc - r_prod >= min_gain,
            "{metric:?} unit={unit}: adc {r_adc} vs prod {r_prod}"
        );
    }
}

fn recall_pair(
    dim: usize,
    metric: DistanceMetric,
    unit: bool,
    n: usize,
    queries: usize,
    k: usize,
) -> (f32, f32) {
    let c = col(
        dim,
        metric,
        QuantizationConfig::TurboQuant4,
        BuildMode::Exact,
    );
    let padded = c.padded_dimension as usize;
    let mut data = clustered(n, dim, 11, 0.6);
    if unit {
        data.iter_mut().for_each(|v| normalize(v));
    }
    let mut work = vec![0.0f32; padded];
    let codes: Vec<TqCode> = data
        .iter()
        .map(|v| {
            encode_tq_mse_scaled(
                v,
                c.fwht_sign_flips.as_slice(),
                c.codebook_boundaries_15(),
                &mut work,
            )
        })
        .collect();
    let cb = c.codebook_16();
    let single = dim.div_ceil(8);
    let qjl0 = vec![0u8; c.qjl_matrices.len() * single];
    let (mut rp, mut ra) = (0usize, 0usize);
    for qi in 0..queries {
        let mut q: Vec<f32> = data[(qi * 53) % n]
            .iter()
            .enumerate()
            .map(|(j, &x)| x + 0.08 * (((qi * 7 + j * 3) % 11) as f32 / 11.0 - 0.5))
            .collect();
        if unit {
            normalize(&mut q);
        }
        let exact: Vec<(f32, usize)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d = if metric == DistanceMetric::L2 {
                    v.iter()
                        .zip(&q)
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum::<f32>()
                } else {
                    let dot: f32 = v.iter().zip(&q).map(|(a, b)| a * b).sum();
                    1.0 - dot
                        / (v.iter().map(|x| x * x).sum::<f32>().sqrt()
                            * q.iter().map(|x| x * x).sum::<f32>().sqrt())
                };
                (d, i)
            })
            .collect();
        let st = prepare_query_prod(&q, &c.qjl_matrices, c.fwht_sign_flips.as_slice(), padded);
        let qn: f32 = q.iter().map(|x| x * x).sum::<f32>().sqrt();
        let prod = codes
            .iter()
            .enumerate()
            .map(|(i, cd)| {
                (
                    score_l2_prod(&st, &cd.codes, cd.norm, &qjl0, 0.0, cb, dim, single),
                    i,
                )
            })
            .collect();
        let adc = codes
            .iter()
            .enumerate()
            .map(|(i, cd)| {
                let v = tq_l2_adc_scaled(&st.q_rotated, &cd.codes, cd.norm, cb);
                let d = if metric == DistanceMetric::L2 {
                    let diff = cd.norm - qn;
                    if cd.norm <= 0.0 {
                        diff * diff
                    } else {
                        diff * diff + (qn / cd.norm) * v
                    }
                } else {
                    v
                };
                (d, i)
            })
            .collect();
        let gt = top_k(exact, k);
        rp += top_k(prod, k).iter().filter(|x| gt.contains(x)).count();
        ra += top_k(adc, k).iter().filter(|x| gt.contains(x)).count();
    }
    let den = (queries * k) as f32;
    (rp as f32 / den, ra as f32 / den)
}

#[test]
fn light_tq4a2_mvcc_scan_does_not_panic_and_matches_sync_scan() {
    // HEAD: the chunked MVCC scan had no TQ4A2 arm and reached
    // `query_state.unwrap()` — a LIGHT TQ4A2 index (no QJL state) panicked
    // on its first FT.SEARCH over a non-empty mutable segment.
    for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
        let dim = 32;
        let holder = SegmentHolder::new(
            dim as u32,
            col(
                dim,
                metric,
                QuantizationConfig::TurboQuant4A2,
                BuildMode::Light,
            ),
        );
        let data = clustered(120, dim, 5, 0.5);
        for (i, v) in data.iter().enumerate() {
            holder.load().mutable.append(i as u64, v, i as u64 + 1);
        }
        for qi in [0usize, 17, 64] {
            let q = &data[qi];
            let mvcc = search_mvcc(&holder, q, 5);
            let sync: Vec<(u32, u32)> = holder
                .load()
                .mutable
                .brute_force_search(q, None, 5)
                .iter()
                .map(|r| (r.id.0, r.distance.to_bits()))
                .collect();
            assert_eq!(mvcc, sync, "{metric:?}: chunked A2 scan != sync A2 scan");
            assert_eq!(mvcc[0].0, qi as u32, "self-query must rank itself first");
        }
    }
}

#[test]
fn freeze_no_longer_recomputes_qjl_on_the_shard_thread() {
    // Wall-time red test (CONVENTIONS "behavioral wall-time"): HEAD's
    // freeze_prefix ran 8 scalar d×d matvecs per vector for the whole
    // buffer (400 × 8 × 384² ≈ 472M MACs — seconds even in release, tens of
    // seconds in this debug build). Now it is an O(n·d) copy.
    let dim = 384;
    let c = col(
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        BuildMode::Exact,
    );
    let seg = MutableSegment::new(dim as u32, c);
    for (i, v) in clustered(400, dim, 2, 0.5).iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    let t = std::time::Instant::now();
    let frozen = seg.freeze_prefix(300);
    let el = t.elapsed();
    assert_eq!(frozen.entries.len(), 300);
    assert!(
        el < std::time::Duration::from_millis(1500),
        "freeze took {el:?}"
    );
}

#[test]
fn compaction_computes_qjl_for_live_entries_in_bfs_order() {
    // The worker-side QJL must be the QJL of the vector each BFS row holds.
    // HEAD also indexed the internal-id-ordered buffer by LIVE position, so
    // after a dead entry every row carried a neighbour's signs.
    let dim = 48;
    let c = col(
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        BuildMode::Exact,
    );
    let seg = MutableSegment::new(dim as u32, c.clone());
    let data = clustered(160, dim, 8, 0.5);
    for (i, v) in data.iter().enumerate() {
        seg.append(1000 + i as u64, v, i as u64 + 1);
    }
    for dead in [0u32, 5, 6, 77] {
        seg.mark_deleted(dead, 999);
    }
    let imm = compact(&seg.freeze(), &c, 7, None).expect("compact");
    let padded = c.padded_dimension as usize;
    let single = dim.div_ceil(8);
    let bpv = c.qjl_matrices.len() * single;
    let mut work = vec![0.0f32; padded];
    let mut flips = 0u32;
    for (bfs, h) in imm.mvcc_headers().iter().enumerate() {
        let v = &data[(h.key_hash - 1000) as usize];
        let code = encode_tq_mse_scaled(
            v,
            c.fwht_sign_flips.as_slice(),
            c.codebook_boundaries_15(),
            &mut work,
        );
        let dec = decode_tq_mse_scaled(
            &code,
            c.fwht_sign_flips.as_slice(),
            c.codebook_16(),
            dim,
            &mut work,
        );
        let r: Vec<f32> = v.iter().zip(&dec).map(|(a, b)| a - b).collect();
        let got = imm.qjl_bytes_for(bfs, bpv);
        assert_eq!(got.len(), bpv);
        for (p, m) in c.qjl_matrices.iter().enumerate() {
            let want = qjl_encode_scalar_reference(m, &r, dim);
            flips += got[p * single..(p + 1) * single]
                .iter()
                .zip(&want)
                .map(|(a, b)| (a ^ b).count_ones())
                .sum::<u32>();
        }
    }
    let rows = imm.mvcc_headers().len() as u32;
    assert_eq!(rows, 156);
    // SIMD reassociation may flip a near-zero projection; misalignment would
    // flip ~half of all bits (rows * 8 * dim / 2).
    assert!(flips <= rows, "{flips} QJL bit mismatches over {rows} rows");
}

#[test]
fn light_compaction_carries_no_dead_qjl_buffer() {
    let dim = 64;
    let c = col(
        dim,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        BuildMode::Light,
    );
    let seg = MutableSegment::new(dim as u32, c.clone());
    for (i, v) in clustered(100, dim, 4, 0.5).iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    let imm = compact(&seg.freeze(), &c, 7, None).expect("compact");
    assert_eq!(
        imm.qjl_bytes(),
        0,
        "LIGHT segments must not hold a zero QJL buffer"
    );
}

#[test]
fn simd_qjl_encode_matches_scalar_except_near_zero_projections() {
    distance::init();
    let dim = 200;
    let m = crate::vector::turbo_quant::qjl::generate_qjl_matrix(dim, 5);
    for seed in 0..8u64 {
        let v: Vec<f32> = clustered(1, dim, seed + 1, 0.9).remove(0);
        let a = qjl_encode(&m, &v, dim);
        let b = qjl_encode_scalar_reference(&m, &v, dim);
        for row in 0..dim {
            let bit = |x: &[u8]| (x[row / 8] >> (row % 8)) & 1;
            if bit(&a) != bit(&b) {
                let dot: f32 = m[row * dim..(row + 1) * dim]
                    .iter()
                    .zip(&v)
                    .map(|(x, y)| x * y)
                    .sum();
                let mag: f32 = m[row * dim..(row + 1) * dim]
                    .iter()
                    .zip(&v)
                    .map(|(x, y)| (x * y).abs())
                    .sum();
                assert!(dot.abs() <= mag * 1e-5, "row {row}: sign flip at dot {dot}");
            }
        }
    }
}
