//! WARM->COLD transition protocol for vector segments (design section 11.2).
//!
//! Converts a warm segment (mmap-backed HNSW with TQ codes) into a cold
//! segment (PQ codes in RAM + Vamana graph on NVMe). This dramatically
//! reduces memory usage for old segments while maintaining approximate
//! search capability via DiskANN beam search.
//!
//! Protocol:
//! 1. Decode TQ codes from warm segment into approximate f32 vectors
//! 2. Train ProductQuantizer on those vectors
//! 3. Encode all vectors into PQ codes
//! 4. Build VamanaGraph (warm-started from HNSW L0 if available)
//! 5. Write staging directory with vamana.mpf and pq_codes.bin
//! 6. Manifest commit 1: warm -> Compacting, DiskANN -> Building
//! 7. Recall verification (50 random queries, target >= 0.95)
//! 8. Manifest commit 2: DiskANN -> Active/Cold, warm -> Tombstone
//! 9. Rename staging -> final
//! 10. Return DiskAnnSegment

use std::io::Write as _;
use std::path::Path;

use crate::persistence::fsync::{fsync_directory, fsync_file};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::vector::diskann::page::write_vamana_mpf;
use crate::vector::diskann::pq::ProductQuantizer;
use crate::vector::diskann::segment::DiskAnnSegment;
use crate::vector::diskann::vamana::VamanaGraph;
use crate::vector::persistence::warm_search::WarmSearchSegment;

/// Decode TQ codes from a warm segment into approximate f32 vectors.
///
/// Uses the collection metadata's codebook to reconstruct approximate
/// floating-point vectors from the quantized TQ codes. The result is
/// a flat `Vec<f32>` of `n * dim` elements suitable for PQ training.
fn decode_warm_vectors(warm_seg: &WarmSearchSegment, dim: usize) -> Vec<f32> {
    let n = warm_seg.total_count() as usize;
    if n == 0 {
        return Vec::new();
    }

    let meta = warm_seg.collection_meta();
    let padded_dim = meta.padded_dimension as usize;
    let codebook = &meta.codebook;
    let bits_per_dim = meta.quantization.bits() as usize;
    let codes = warm_seg.codes_data();

    // Each vector occupies (padded_dim * bits_per_dim + 7) / 8 bytes in TQ encoding
    let bytes_per_vec = (padded_dim * bits_per_dim + 7) / 8;

    let mut vectors = Vec::with_capacity(n * dim);

    for i in 0..n {
        let code_start = i * bytes_per_vec;
        let code_end = code_start + bytes_per_vec;
        if code_end > codes.len() {
            // Truncated codes -- fill remaining vectors with zeros
            vectors.resize(n * dim, 0.0);
            break;
        }
        let code_slice = &codes[code_start..code_end];

        // Decode each dimension from TQ code using codebook centroids
        for d in 0..dim {
            if d < padded_dim {
                let val = decode_tq_dimension(code_slice, d, bits_per_dim, codebook);
                vectors.push(val);
            } else {
                vectors.push(0.0);
            }
        }
    }

    vectors
}

/// Decode a single dimension from TQ-encoded bytes using the codebook.
#[inline]
fn decode_tq_dimension(code: &[u8], dim_idx: usize, bits: usize, codebook: &[f32]) -> f32 {
    let bit_offset = dim_idx * bits;
    let byte_idx = bit_offset / 8;
    let bit_idx = bit_offset % 8;

    // Extract the quantization code for this dimension
    let mut val = 0u32;
    let mut bits_read = 0;
    let mut cur_byte = byte_idx;
    let mut cur_bit = bit_idx;

    while bits_read < bits {
        if cur_byte >= code.len() {
            break;
        }
        let available = 8 - cur_bit;
        let to_read = (bits - bits_read).min(available);
        let mask = (1u32 << to_read) - 1;
        let extracted = ((code[cur_byte] >> cur_bit) as u32) & mask;
        val |= extracted << bits_read;
        bits_read += to_read;
        cur_byte += 1;
        cur_bit = 0;
    }

    // Map code to codebook centroid value
    let code_idx = val as usize;
    if code_idx < codebook.len() {
        codebook[code_idx]
    } else {
        0.0
    }
}

/// Transition a warm segment to cold tier (PQ + Vamana DiskANN).
///
/// Follows the staging-directory atomic protocol:
/// 1. Write PQ codes + Vamana graph to staging dir
/// 2. Manifest transitions (warm -> Compacting, DiskANN -> Building -> Active)
/// 3. Recall verification
/// 4. Rename staging -> final
/// 5. Return DiskAnnSegment for registration in SegmentList.cold
pub fn transition_to_cold(
    shard_dir: &Path,
    warm_seg: &WarmSearchSegment,
    warm_file_id: u64,
    cold_file_id: u64,
    dim: usize,
    manifest: &mut ShardManifest,
) -> std::io::Result<DiskAnnSegment> {
    let n = warm_seg.total_count() as usize;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot transition empty warm segment to cold",
        ));
    }

    // Step 1: Decode TQ codes to approximate f32 vectors
    let vectors = decode_warm_vectors(warm_seg, dim);

    // Step 2: Train PQ codebook
    // m = dim / 8 subspaces (8 dims per subspace), 8 bits per code (256 centroids)
    let m = (dim / 8).max(1);
    // Ensure dim is divisible by m
    let m = if dim % m != 0 { dim } else { m };
    let pq = ProductQuantizer::train(&vectors, dim, m, 8);

    // Step 3: Encode all vectors into PQ codes
    let mut pq_codes = Vec::with_capacity(n * pq.m());
    for i in 0..n {
        let v = &vectors[i * dim..(i + 1) * dim];
        let codes = pq.encode(v);
        pq_codes.extend_from_slice(&codes);
    }

    // Step 4: Build Vamana graph (warm-started from HNSW layer-0)
    let r = 64u32.min(n.saturating_sub(1) as u32).max(1); // max degree
    let l = 128u32.min(n as u32).max(r); // search list size >= r
    let graph = VamanaGraph::build_from_hnsw(
        warm_seg.graph(),
        &vectors,
        dim,
        r,
        l,
    );

    // Step 5: Write to staging directory
    let vectors_dir = shard_dir.join("vectors");
    std::fs::create_dir_all(&vectors_dir)?;

    let staging = vectors_dir.join(format!(".segment-{cold_file_id}-diskann.staging"));
    let final_dir = vectors_dir.join(format!("segment-{cold_file_id}-diskann"));

    std::fs::create_dir_all(&staging)?;

    // Write vamana.mpf
    write_vamana_mpf(&staging.join("vamana.mpf"), &graph, &vectors, dim)?;

    // Write pq_codes.bin (raw PQ code bytes)
    {
        let pq_path = staging.join("pq_codes.bin");
        let mut f = std::fs::File::create(&pq_path)?;
        f.write_all(&pq_codes)?;
        f.flush()?;
    }

    // Fsync all files in staging
    for entry in std::fs::read_dir(&staging)? {
        let entry = entry?;
        fsync_file(&entry.path())?;
    }
    fsync_directory(&staging)?;

    // Step 6: Manifest commit 1 -- warm -> Compacting, DiskANN -> Building
    manifest.update_file(warm_file_id, |entry| {
        entry.status = FileStatus::Compacting;
    });

    let cold_entry = FileEntry {
        file_id: cold_file_id,
        file_type: PageType::VecGraph as u8,
        status: FileStatus::Building,
        tier: StorageTier::Cold,
        page_size_log2: 12, // 4KB pages for Vamana
        page_count: n as u32,
        byte_size: (n * 4096) as u64, // one 4KB page per node
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: u64::MAX,
    };
    manifest.add_file(cold_entry);
    manifest.commit()?;

    // Step 7: Recall verification (50 random queries from dataset)
    let recall = verify_recall(&graph, &vectors, dim, n);
    if recall < 0.95 {
        tracing::warn!(
            "Cold transition recall {:.2} < 0.95 target for segment {} ({} vectors, dim={})",
            recall, cold_file_id, n, dim,
        );
    } else {
        tracing::info!(
            "Cold transition recall {:.2} for segment {} ({} vectors)",
            recall, cold_file_id, n,
        );
    }

    // Step 8: Manifest commit 2 -- DiskANN -> Active/Cold, warm -> Tombstone
    manifest.update_file(cold_file_id, |entry| {
        entry.status = FileStatus::Active;
        entry.tier = StorageTier::Cold;
    });
    manifest.update_file(warm_file_id, |entry| {
        entry.status = FileStatus::Tombstone;
    });
    manifest.commit()?;

    // Step 9: Rename staging -> final
    std::fs::rename(&staging, &final_dir)?;
    fsync_directory(&vectors_dir)?;

    // Step 10: Create and return DiskAnnSegment
    let vamana_path = final_dir.join("vamana.mpf");
    let segment = DiskAnnSegment::new(
        pq_codes,
        pq,
        vamana_path,
        dim,
        n as u32,
        graph.entry_point(),
        graph.max_degree(),
        cold_file_id,
    );

    Ok(segment)
}

/// Verify recall of the Vamana graph against brute-force on exact vectors.
///
/// Runs up to 50 deterministic query vectors (sampled from the dataset),
/// computes recall@10 comparing Vamana greedy search against brute-force L2.
/// Returns recall as a float in [0.0, 1.0].
fn verify_recall(
    graph: &VamanaGraph,
    vectors: &[f32],
    dim: usize,
    n: usize,
) -> f64 {
    if n < 10 {
        return 1.0; // Not enough vectors for meaningful recall test
    }

    let k = 10usize.min(n);
    let num_queries = 50usize.min(n);
    let l = 128u32.min(n as u32);
    let mut total_recall = 0.0_f64;

    for q in 0..num_queries {
        // Deterministic query from dataset (stride by 2)
        let query_idx = (q * 2) % n;
        let query = &vectors[query_idx * dim..(query_idx + 1) * dim];

        // Vamana greedy search
        let vamana_results = graph.greedy_search(query, vectors, dim, l);
        let vamana_topk: std::collections::HashSet<u32> = vamana_results
            .iter()
            .take(k)
            .map(|&(id, _)| id)
            .collect();

        // Brute-force top-k
        let mut bf_dists: Vec<(f32, u32)> = (0..n as u32)
            .map(|i| {
                let v = &vectors[i as usize * dim..(i as usize + 1) * dim];
                let d: f32 = query.iter().zip(v.iter()).map(|(a, b)| (a - b) * (a - b)).sum();
                (d, i)
            })
            .collect();
        bf_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let bf_topk: std::collections::HashSet<u32> = bf_dists
            .iter()
            .take(k)
            .map(|&(_, id)| id)
            .collect();

        let hits = vamana_topk.intersection(&bf_topk).count();
        total_recall += hits as f64 / k as f64;
    }

    total_recall / num_queries as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::ShardManifest;
    use crate::vector::diskann::pq::ProductQuantizer;
    use crate::vector::diskann::vamana::VamanaGraph;

    /// Build a minimal set of test vectors for cold transition testing.
    fn make_test_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
        let mut vectors = Vec::with_capacity(n * dim);
        let mut s = seed as u32;
        for _ in 0..n * dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            vectors.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        vectors
    }

    #[test]
    fn test_cold_staging_and_rename() {
        // Test staging dir creation, file writes, and rename to final
        let n = 100;
        let dim = 32;
        let vectors = make_test_vectors(n, dim, 42);

        let m = dim / 8;
        let pq = ProductQuantizer::train(&vectors, dim, m, 8);

        let mut pq_codes = Vec::with_capacity(n * m);
        for i in 0..n {
            let codes = pq.encode(&vectors[i * dim..(i + 1) * dim]);
            pq_codes.extend_from_slice(&codes);
        }

        let r = 8u32;
        let l = 16u32;
        let graph = VamanaGraph::build(&vectors, dim, r, l);

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let vectors_dir = shard_dir.join("vectors");
        std::fs::create_dir_all(&vectors_dir).unwrap();

        let cold_file_id = 500u64;
        let staging = vectors_dir.join(format!(".segment-{cold_file_id}-diskann.staging"));
        let final_dir = vectors_dir.join(format!("segment-{cold_file_id}-diskann"));

        std::fs::create_dir_all(&staging).unwrap();
        write_vamana_mpf(&staging.join("vamana.mpf"), &graph, &vectors, dim).unwrap();
        {
            let mut f = std::fs::File::create(staging.join("pq_codes.bin")).unwrap();
            f.write_all(&pq_codes).unwrap();
        }

        std::fs::rename(&staging, &final_dir).unwrap();

        assert!(final_dir.join("vamana.mpf").exists());
        assert!(final_dir.join("pq_codes.bin").exists());
        assert!(!staging.exists(), "staging should not exist after rename");

        let pq_bytes = std::fs::read(final_dir.join("pq_codes.bin")).unwrap();
        assert_eq!(pq_bytes.len(), n * m);
    }

    #[test]
    fn test_verify_recall_high_quality() {
        let n = 100;
        let dim = 32;
        let vectors = make_test_vectors(n, dim, 100);
        let graph = VamanaGraph::build(&vectors, dim, 16, 32);
        let recall = verify_recall(&graph, &vectors, dim, n);

        // Vamana graph search on the exact vectors should have high recall
        assert!(
            recall >= 0.80,
            "recall {recall:.2} < 0.80 for 100 vectors dim=32",
        );
    }

    #[test]
    fn test_verify_recall_small_dataset() {
        // With fewer than 10 vectors, should return 1.0
        let n = 5;
        let dim = 8;
        let vectors = make_test_vectors(n, dim, 200);
        let graph = VamanaGraph::build(&vectors, dim, 4, 4);
        let recall = verify_recall(&graph, &vectors, dim, n);
        assert!((recall - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_tq_dimension_4bit() {
        // 4-bit TQ with codebook [0.0, 0.1, 0.2, ..., 1.5]
        let codebook: Vec<f32> = (0..16).map(|i| i as f32 * 0.1).collect();

        // Encode dim 0 = code 5 (0101), dim 1 = code 10 (1010)
        // Byte: lower nibble = dim0 = 5, upper nibble = dim1 = 10
        // => byte = 0b1010_0101 = 0xA5
        let code = [0xA5u8];

        let val0 = decode_tq_dimension(&code, 0, 4, &codebook);
        assert!(
            (val0 - 0.5).abs() < f32::EPSILON,
            "dim 0 should decode to codebook[5] = 0.5, got {val0}"
        );

        let val1 = decode_tq_dimension(&code, 1, 4, &codebook);
        assert!(
            (val1 - 1.0).abs() < f32::EPSILON,
            "dim 1 should decode to codebook[10] = 1.0, got {val1}"
        );
    }

    #[test]
    fn test_manifest_two_phase_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let warm_file_id = 100u64;
        let cold_file_id = 200u64;

        // Add initial warm entry
        let warm_entry = FileEntry {
            file_id: warm_file_id,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 1,
            byte_size: 1000,
            created_lsn: 0,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        };
        manifest.add_file(warm_entry);
        manifest.commit().unwrap();

        // Phase 1: warm -> Compacting, cold -> Building
        manifest.update_file(warm_file_id, |e| {
            e.status = FileStatus::Compacting;
        });
        let cold_entry = FileEntry {
            file_id: cold_file_id,
            file_type: PageType::VecGraph as u8,
            status: FileStatus::Building,
            tier: StorageTier::Cold,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409600,
            created_lsn: 0,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        };
        manifest.add_file(cold_entry);
        manifest.commit().unwrap();

        let warm = manifest.files().iter().find(|f| f.file_id == warm_file_id).unwrap();
        assert_eq!(warm.status, FileStatus::Compacting);
        let cold = manifest.files().iter().find(|f| f.file_id == cold_file_id).unwrap();
        assert_eq!(cold.status, FileStatus::Building);
        assert_eq!(cold.tier, StorageTier::Cold);

        // Phase 2: cold -> Active, warm -> Tombstone
        manifest.update_file(cold_file_id, |e| {
            e.status = FileStatus::Active;
        });
        manifest.update_file(warm_file_id, |e| {
            e.status = FileStatus::Tombstone;
        });
        manifest.commit().unwrap();

        let warm = manifest.files().iter().find(|f| f.file_id == warm_file_id).unwrap();
        assert_eq!(warm.status, FileStatus::Tombstone);
        let cold = manifest.files().iter().find(|f| f.file_id == cold_file_id).unwrap();
        assert_eq!(cold.status, FileStatus::Active);
        assert_eq!(cold.tier, StorageTier::Cold);
    }
}
