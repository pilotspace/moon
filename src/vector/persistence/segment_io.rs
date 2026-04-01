//! Immutable segment disk I/O: write and read segment directories.
//!
//! Each immutable segment is stored as a directory containing 6 files:
//! ```text
//! {persist_dir}/segment-{segment_id}/
//!   hnsw_graph.bin      -- HnswGraph::to_bytes() output
//!   tq_codes.bin        -- raw TQ code bytes
//!   sq_vectors.bin      -- raw SQ vector bytes (i8 as u8)
//!   f32_vectors.bin     -- raw f32 vector bytes (BFS-ordered, for HNSW search)
//!   mvcc_headers.bin    -- [count:u32 LE][MvccHeader; count] (20 bytes each)
//!   segment_meta.json   -- JSON metadata with checksum verification
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::persistence::fsync::{fsync_directory, fsync_file};
use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::segment::immutable::{ImmutableSegment, MvccHeader};
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

/// Error type for segment I/O operations.
#[derive(Debug)]
pub enum SegmentIoError {
    Io(std::io::Error),
    GraphDeserialize(String),
    MetadataChecksum { expected: u64, actual: u64 },
    InvalidMetadata(String),
}

impl std::fmt::Display for SegmentIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "segment I/O error: {e}"),
            Self::GraphDeserialize(msg) => write!(f, "graph deserialize: {msg}"),
            Self::MetadataChecksum { expected, actual } => {
                write!(
                    f,
                    "metadata checksum mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InvalidMetadata(msg) => write!(f, "invalid metadata: {msg}"),
        }
    }
}

impl From<std::io::Error> for SegmentIoError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// On-disk JSON metadata for an immutable segment.
#[derive(Serialize, Deserialize)]
struct SegmentMeta {
    version: u32,
    segment_id: u64,
    collection_id: u64,
    created_at_lsn: u64,
    dimension: u32,
    padded_dimension: u32,
    metric: String,
    quantization: String,
    live_count: u32,
    total_count: u32,
    metadata_checksum: u64,
    codebook_version: u8,
    codebook: Vec<f32>,
    codebook_boundaries: Vec<f32>,
    fwht_sign_flips: Vec<f32>,
    /// Build mode: "Light" or "Exact". Added in v1 — defaults to inferred if absent.
    #[serde(default)]
    build_mode: Option<String>,
}

fn segment_dir(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("segment-{segment_id}"))
}

fn metric_to_string(m: DistanceMetric) -> String {
    match m {
        DistanceMetric::L2 => "L2".to_owned(),
        DistanceMetric::Cosine => "Cosine".to_owned(),
        DistanceMetric::InnerProduct => "InnerProduct".to_owned(),
    }
}

fn string_to_metric(s: &str) -> Result<DistanceMetric, SegmentIoError> {
    match s {
        "L2" => Ok(DistanceMetric::L2),
        "Cosine" => Ok(DistanceMetric::Cosine),
        "InnerProduct" => Ok(DistanceMetric::InnerProduct),
        _ => Err(SegmentIoError::InvalidMetadata(format!(
            "unknown metric: {s}"
        ))),
    }
}

fn quant_to_string(q: QuantizationConfig) -> String {
    match q {
        QuantizationConfig::Sq8 => "Sq8".to_owned(),
        QuantizationConfig::TurboQuant1 => "TurboQuant1".to_owned(),
        QuantizationConfig::TurboQuant2 => "TurboQuant2".to_owned(),
        QuantizationConfig::TurboQuant3 => "TurboQuant3".to_owned(),
        QuantizationConfig::TurboQuant4 => "TurboQuant4".to_owned(),
        QuantizationConfig::TurboQuantProd4 => "TurboQuantProd4".to_owned(),
        QuantizationConfig::TurboQuant4A2 => "TurboQuant4A2".to_owned(),
    }
}

fn string_to_quant(s: &str) -> Result<QuantizationConfig, SegmentIoError> {
    match s {
        "Sq8" => Ok(QuantizationConfig::Sq8),
        "TurboQuant1" => Ok(QuantizationConfig::TurboQuant1),
        "TurboQuant2" => Ok(QuantizationConfig::TurboQuant2),
        "TurboQuant3" => Ok(QuantizationConfig::TurboQuant3),
        "TurboQuant4" => Ok(QuantizationConfig::TurboQuant4),
        "TurboQuantProd4" => Ok(QuantizationConfig::TurboQuantProd4),
        "TurboQuant4A2" => Ok(QuantizationConfig::TurboQuant4A2),
        _ => Err(SegmentIoError::InvalidMetadata(format!(
            "unknown quantization: {s}"
        ))),
    }
}

/// Write an immutable segment to disk.
///
/// Creates `{dir}/segment-{id}/` with 5 files.
pub fn write_immutable_segment(
    dir: &Path,
    segment_id: u64,
    segment: &ImmutableSegment,
    collection: &CollectionMetadata,
) -> Result<(), SegmentIoError> {
    let seg_dir = segment_dir(dir, segment_id);
    fs::create_dir_all(&seg_dir)?;

    // 1. hnsw_graph.bin
    let graph_bytes = segment.graph().to_bytes();
    let graph_path = seg_dir.join("hnsw_graph.bin");
    fs::write(&graph_path, &graph_bytes)?;
    fsync_file(&graph_path)?;

    // 2. tq_codes.bin
    let tq_path = seg_dir.join("tq_codes.bin");
    fs::write(&tq_path, segment.vectors_tq().as_slice())?;
    fsync_file(&tq_path)?;

    // 3. sq_vectors.bin — skipped (SQ8 no longer stored in ImmutableSegment).
    // 3b. f32_vectors.bin — skipped (f32 no longer stored; TQ-ADC used for search).

    // 4. mvcc_headers.bin: [version:u8][count:u32 LE][MvccHeader; count]
    // v2 format: 32 bytes/header (internal_id + global_id + key_hash + insert_lsn + delete_lsn)
    let mvcc = segment.mvcc_headers();
    let count = mvcc.len() as u32;
    let mut mvcc_buf = Vec::with_capacity(1 + 4 + mvcc.len() * 32);
    mvcc_buf.push(2u8); // format version
    mvcc_buf.extend_from_slice(&count.to_le_bytes());
    for h in mvcc {
        mvcc_buf.extend_from_slice(&h.internal_id.to_le_bytes());
        mvcc_buf.extend_from_slice(&h.global_id.to_le_bytes());
        mvcc_buf.extend_from_slice(&h.key_hash.to_le_bytes());
        mvcc_buf.extend_from_slice(&h.insert_lsn.to_le_bytes());
        mvcc_buf.extend_from_slice(&h.delete_lsn.to_le_bytes());
    }
    let mvcc_path = seg_dir.join("mvcc_headers.bin");
    fs::write(&mvcc_path, &mvcc_buf)?;
    fsync_file(&mvcc_path)?;

    // 5. segment_meta.json
    let meta = SegmentMeta {
        version: 1,
        segment_id,
        collection_id: collection.collection_id,
        created_at_lsn: collection.created_at_lsn,
        dimension: collection.dimension,
        padded_dimension: collection.padded_dimension,
        metric: metric_to_string(collection.metric),
        quantization: quant_to_string(collection.quantization),
        live_count: segment.live_count(),
        total_count: segment.total_count(),
        metadata_checksum: collection.metadata_checksum,
        codebook_version: collection.codebook_version,
        codebook: collection.codebook.clone(),
        codebook_boundaries: collection.codebook_boundaries.clone(),
        fwht_sign_flips: collection.fwht_sign_flips.as_slice().to_vec(),
        build_mode: Some(match collection.build_mode {
            crate::vector::turbo_quant::collection::BuildMode::Light => "Light".to_owned(),
            crate::vector::turbo_quant::collection::BuildMode::Exact => "Exact".to_owned(),
        }),
    };
    let json = serde_json::to_string_pretty(&meta)
        .map_err(|e| SegmentIoError::InvalidMetadata(e.to_string()))?;
    let meta_path = seg_dir.join("segment_meta.json");
    fs::write(&meta_path, json)?;
    fsync_file(&meta_path)?;

    // Fsync the segment directory to make all file entries durable
    fsync_directory(&seg_dir)?;

    Ok(())
}

/// Read an immutable segment from disk.
///
/// Reads from `{dir}/segment-{id}/` directory.
/// Verifies metadata_checksum against reconstructed CollectionMetadata.
pub fn read_immutable_segment(
    dir: &Path,
    segment_id: u64,
) -> Result<(ImmutableSegment, Arc<CollectionMetadata>), SegmentIoError> {
    let seg_dir = segment_dir(dir, segment_id);

    // 1. Read and parse metadata
    let meta_json = fs::read_to_string(seg_dir.join("segment_meta.json"))?;
    let meta: SegmentMeta = serde_json::from_str(&meta_json)
        .map_err(|e| SegmentIoError::InvalidMetadata(e.to_string()))?;

    // Reconstruct CollectionMetadata
    let metric = string_to_metric(&meta.metric)?;
    let quantization = string_to_quant(&meta.quantization)?;

    let mut sign_flips = AlignedBuffer::<f32>::new(meta.fwht_sign_flips.len());
    sign_flips
        .as_mut_slice()
        .copy_from_slice(&meta.fwht_sign_flips);

    // Variable-length codebook: validate size matches quantization variant.
    // SQ8 stores empty codebook (no quantization centroids needed).
    if quantization.is_turbo_quant() {
        let expected_centroids = quantization.n_centroids();
        let expected_boundaries = expected_centroids - 1;
        if meta.codebook.len() != expected_centroids {
            return Err(SegmentIoError::InvalidMetadata(format!(
                "codebook must have {} entries for {:?}, got {}",
                expected_centroids,
                quantization,
                meta.codebook.len()
            )));
        }
        if meta.codebook_boundaries.len() != expected_boundaries {
            return Err(SegmentIoError::InvalidMetadata(format!(
                "codebook_boundaries must have {} entries for {:?}, got {}",
                expected_boundaries,
                quantization,
                meta.codebook_boundaries.len()
            )));
        }
    }
    let codebook = meta.codebook.clone();
    let boundaries = meta.codebook_boundaries.clone();

    // Parse build mode from persisted metadata (defaults to Light for old segments).
    let build_mode = match meta.build_mode.as_deref() {
        Some("Exact") => crate::vector::turbo_quant::collection::BuildMode::Exact,
        Some("Light") | None => crate::vector::turbo_quant::collection::BuildMode::Light,
        Some(other) => {
            return Err(SegmentIoError::InvalidMetadata(format!(
                "unknown build_mode: {other}"
            )));
        }
    };

    // Reconstruct dense Gaussian QJL matrices from deterministic seeds.
    // Only generated in Exact mode — Light mode uses sub-centroid reranking instead.
    const QJL_NUM_PROJECTIONS: usize = 8;
    let (qjl_matrices, qjl_num_projections) = if build_mode
        == crate::vector::turbo_quant::collection::BuildMode::Exact
        && quantization.is_turbo_quant()
    {
        let matrices: Vec<Vec<f32>> = (0..QJL_NUM_PROJECTIONS)
            .map(|m| {
                crate::vector::turbo_quant::qjl::generate_qjl_matrix(
                    meta.dimension as usize,
                    meta.collection_id.wrapping_add(1 + m as u64),
                )
            })
            .collect();
        (matrices, QJL_NUM_PROJECTIONS)
    } else {
        (Vec::new(), 0)
    };

    let sub_centroid_table = if quantization.is_turbo_quant() {
        Some(
            crate::vector::turbo_quant::sub_centroid::SubCentroidTable::new(
                meta.padded_dimension,
                quantization.bits(),
            ),
        )
    } else {
        None
    };

    // Construct with a placeholder checksum, then recompute to match current formula.
    // The stored metadata_checksum validates the core fields (dimension, codebook, etc.)
    // were not corrupted; we recompute after reconstruction to cover any newly added fields.
    let collection = CollectionMetadata {
        collection_id: meta.collection_id,
        created_at_lsn: meta.created_at_lsn,
        dimension: meta.dimension,
        padded_dimension: meta.padded_dimension,
        metric,
        quantization,
        fwht_sign_flips: sign_flips,
        codebook_version: meta.codebook_version,
        codebook: codebook.clone(),
        codebook_boundaries: boundaries.clone(),
        metadata_checksum: meta.metadata_checksum,
        qjl_matrices,
        qjl_num_projections,
        build_mode,
        sub_centroid_table,
    };
    // Verify checksum: recompute from reconstructed collection and compare
    // against the stored value.
    if let Err(e) = collection.verify_checksum() {
        return Err(SegmentIoError::MetadataChecksum {
            expected: meta.metadata_checksum,
            actual: {
                match e {
                    crate::vector::turbo_quant::collection::CollectionMetadataError::ChecksumMismatch {
                        actual, ..
                    } => actual,
                }
            },
        });
    }

    let collection = Arc::new(collection);

    // 2. Read HNSW graph
    let graph_bytes = fs::read(seg_dir.join("hnsw_graph.bin"))?;
    let graph = HnswGraph::from_bytes(&graph_bytes)
        .map_err(|e| SegmentIoError::GraphDeserialize(e.to_owned()))?;

    // 3. Read TQ codes
    let tq_bytes = fs::read(seg_dir.join("tq_codes.bin"))?;
    let vectors_tq = AlignedBuffer::from_vec(tq_bytes);

    // 4. SQ and f32 vectors — no longer stored (TQ-ADC used for search).
    // Provide empty buffers for ImmutableSegment::new() which drops them.
    let _vectors_sq: AlignedBuffer<i8> = AlignedBuffer::new(0);
    let _vectors_f32: AlignedBuffer<f32> = AlignedBuffer::new(0);

    // 5. Read MVCC headers (version-aware: v1 = 20 bytes/header, v2 = 32 bytes/header)
    let mvcc_bytes = fs::read(seg_dir.join("mvcc_headers.bin"))?;
    if mvcc_bytes.len() < 4 {
        return Err(SegmentIoError::InvalidMetadata(
            "mvcc_headers.bin too short".to_owned(),
        ));
    }
    // Detect format version: v2 starts with version byte 2, v1 starts with count (u32 LE)
    let (mvcc_version, mvcc_count, mut pos) = if mvcc_bytes[0] == 2 && mvcc_bytes.len() >= 5 {
        let count = u32::from_le_bytes([mvcc_bytes[1], mvcc_bytes[2], mvcc_bytes[3], mvcc_bytes[4]])
            as usize;
        (2u8, count, 5usize)
    } else {
        let count = u32::from_le_bytes([mvcc_bytes[0], mvcc_bytes[1], mvcc_bytes[2], mvcc_bytes[3]])
            as usize;
        (1u8, count, 4usize)
    };
    let bytes_per_header: usize = if mvcc_version >= 2 { 32 } else { 20 };
    if mvcc_bytes.len() < pos + mvcc_count * bytes_per_header {
        return Err(SegmentIoError::InvalidMetadata(
            "mvcc_headers.bin truncated".to_owned(),
        ));
    }
    let mut mvcc = Vec::with_capacity(mvcc_count);
    for _ in 0..mvcc_count {
        let internal_id = u32::from_le_bytes([
            mvcc_bytes[pos],
            mvcc_bytes[pos + 1],
            mvcc_bytes[pos + 2],
            mvcc_bytes[pos + 3],
        ]);
        pos += 4;
        let (global_id, key_hash) = if mvcc_version >= 2 {
            let gid = u32::from_le_bytes([
                mvcc_bytes[pos],
                mvcc_bytes[pos + 1],
                mvcc_bytes[pos + 2],
                mvcc_bytes[pos + 3],
            ]);
            pos += 4;
            let kh = u64::from_le_bytes([
                mvcc_bytes[pos],
                mvcc_bytes[pos + 1],
                mvcc_bytes[pos + 2],
                mvcc_bytes[pos + 3],
                mvcc_bytes[pos + 4],
                mvcc_bytes[pos + 5],
                mvcc_bytes[pos + 6],
                mvcc_bytes[pos + 7],
            ]);
            pos += 8;
            (gid, kh)
        } else {
            (internal_id, 0u64) // v1 fallback: global_id = internal_id
        };
        let insert_lsn = u64::from_le_bytes([
            mvcc_bytes[pos],
            mvcc_bytes[pos + 1],
            mvcc_bytes[pos + 2],
            mvcc_bytes[pos + 3],
            mvcc_bytes[pos + 4],
            mvcc_bytes[pos + 5],
            mvcc_bytes[pos + 6],
            mvcc_bytes[pos + 7],
        ]);
        pos += 8;
        let delete_lsn = u64::from_le_bytes([
            mvcc_bytes[pos],
            mvcc_bytes[pos + 1],
            mvcc_bytes[pos + 2],
            mvcc_bytes[pos + 3],
            mvcc_bytes[pos + 4],
            mvcc_bytes[pos + 5],
            mvcc_bytes[pos + 6],
            mvcc_bytes[pos + 7],
        ]);
        pos += 8;
        mvcc.push(MvccHeader {
            internal_id,
            global_id,
            key_hash,
            insert_lsn,
            delete_lsn,
        });
    }

    // 6. Construct ImmutableSegment
    let dim = meta.dimension as usize;
    let qjl_bpv = (dim + 7) / 8;
    let sub_sign_bpv = (meta.padded_dimension as usize + 7) / 8;
    let segment = ImmutableSegment::new(
        graph,
        vectors_tq,
        Vec::new(), // QJL signs — not persisted yet
        Vec::new(), // residual norms — not persisted yet
        qjl_bpv,
        Vec::new(), // sub-centroid signs — not persisted yet
        sub_sign_bpv,
        mvcc,
        collection.clone(),
        meta.live_count,
        meta.total_count,
    );

    Ok((segment, collection))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::hnsw::build::HnswBuilder;
    use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled;
    use crate::vector::turbo_quant::fwht;

    fn lcg_f32(dim: usize, seed: u32) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s as f32) / (u32::MAX as f32) * 2.0 - 1.0);
        }
        v
    }

    fn normalize(v: &mut [f32]) -> f32 {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            let inv = 1.0 / norm;
            v.iter_mut().for_each(|x| *x *= inv);
        }
        norm
    }

    fn build_test_segment(n: usize, dim: usize) -> (ImmutableSegment, Arc<CollectionMetadata>) {
        distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            dim as u32,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let padded = collection.padded_dimension as usize;
        let signs = collection.fwht_sign_flips.as_slice();
        let bytes_per_code = padded / 2 + 4;

        let mut vectors = Vec::with_capacity(n);
        let mut codes = Vec::new();
        let mut sq_vectors: Vec<i8> = Vec::new();
        let mut work = vec![0.0f32; padded];

        for i in 0..n {
            let mut v = lcg_f32(dim, (i * 7 + 13) as u32);
            normalize(&mut v);
            let boundaries = collection.codebook_boundaries_15();
            let code = encode_tq_mse_scaled(&v, signs, boundaries, &mut work);
            for &val in &v {
                sq_vectors.push((val * 127.0).clamp(-128.0, 127.0) as i8);
            }
            codes.push(code);
            vectors.push(v);
        }

        let dist_table = distance::table();
        let codebook = collection.codebook_16();

        let mut tq_buffer_orig: Vec<u8> = Vec::with_capacity(n * bytes_per_code);
        for code in &codes {
            tq_buffer_orig.extend_from_slice(&code.codes);
            tq_buffer_orig.extend_from_slice(&code.norm.to_le_bytes());
        }

        let mut all_rotated: Vec<Vec<f32>> = Vec::with_capacity(n);
        let mut q_rot_buf = vec![0.0f32; padded];
        for i in 0..n {
            q_rot_buf[..dim].copy_from_slice(&vectors[i]);
            for v in q_rot_buf[dim..padded].iter_mut() {
                *v = 0.0;
            }
            fwht::fwht(&mut q_rot_buf[..padded], signs);
            all_rotated.push(q_rot_buf[..padded].to_vec());
        }

        let mut builder = HnswBuilder::new(16, 200, 12345);
        for _i in 0..n {
            builder.insert(|a: u32, b: u32| {
                let q_rot = &all_rotated[a as usize];
                let offset = b as usize * bytes_per_code;
                let code_slice = &tq_buffer_orig[offset..offset + bytes_per_code - 4];
                let norm_bytes =
                    &tq_buffer_orig[offset + bytes_per_code - 4..offset + bytes_per_code];
                let norm = f32::from_le_bytes([
                    norm_bytes[0],
                    norm_bytes[1],
                    norm_bytes[2],
                    norm_bytes[3],
                ]);
                (dist_table.tq_l2)(q_rot, code_slice, norm, codebook)
            });
        }

        let graph = builder.build(bytes_per_code as u32);

        let mut tq_buffer_bfs = vec![0u8; n * bytes_per_code];
        let qjl_bytes_per_vec = (dim + 7) / 8;
        let qjl_signs_bfs = vec![0u8; n * qjl_bytes_per_vec];
        let residual_norms_bfs = vec![0.0f32; n];
        for bfs_pos in 0..n {
            let orig_id = graph.to_original(bfs_pos as u32) as usize;
            let src = orig_id * bytes_per_code;
            let dst = bfs_pos * bytes_per_code;
            tq_buffer_bfs[dst..dst + bytes_per_code]
                .copy_from_slice(&tq_buffer_orig[src..src + bytes_per_code]);
            // QJL signs and residual norms: use zeros for test
        }

        let mvcc: Vec<MvccHeader> = (0..n as u32)
            .map(|i| MvccHeader {
                internal_id: i,
                global_id: i,
                key_hash: 0,
                insert_lsn: i as u64 + 1,
                delete_lsn: 0,
            })
            .collect();

        let sub_sign_bpv = (collection.padded_dimension as usize + 7) / 8;
        let segment = ImmutableSegment::new(
            graph,
            AlignedBuffer::from_vec(tq_buffer_bfs),
            qjl_signs_bfs,
            residual_norms_bfs,
            qjl_bytes_per_vec,
            Vec::new(), // sub-centroid signs — not needed for IO test
            sub_sign_bpv,
            mvcc,
            collection.clone(),
            n as u32,
            n as u32,
        );

        (segment, collection)
    }

    #[test]
    fn test_write_creates_4_files() {
        let (segment, collection) = build_test_segment(20, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 42, &segment, &collection).unwrap();

        let seg_dir = tmp.path().join("segment-42");
        assert!(seg_dir.join("hnsw_graph.bin").exists());
        assert!(seg_dir.join("tq_codes.bin").exists());
        // sq_vectors.bin and f32_vectors.bin no longer written (TQ-ADC used for search)
        assert!(seg_dir.join("mvcc_headers.bin").exists());
        assert!(seg_dir.join("segment_meta.json").exists());
    }

    #[test]
    fn test_roundtrip_preserves_counts() {
        let (segment, collection) = build_test_segment(30, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 1, &segment, &collection).unwrap();
        let (restored, _) = read_immutable_segment(tmp.path(), 1).unwrap();

        assert_eq!(restored.live_count(), segment.live_count());
        assert_eq!(restored.total_count(), segment.total_count());
    }

    #[test]
    fn test_roundtrip_search_works() {
        let (segment, collection) = build_test_segment(50, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 1, &segment, &collection).unwrap();
        let (restored, _restored_col) = read_immutable_segment(tmp.path(), 1).unwrap();

        let mut query = lcg_f32(64, 99999);
        normalize(&mut query);
        let padded = collection.padded_dimension;
        let mut scratch =
            crate::vector::hnsw::search::SearchScratch::new(restored.graph().num_nodes(), padded);
        let results = restored.search(&query, 5, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_segment_meta_valid_json() {
        let (segment, collection) = build_test_segment(10, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 7, &segment, &collection).unwrap();

        let json_str =
            std::fs::read_to_string(tmp.path().join("segment-7").join("segment_meta.json"))
                .unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(val["collection_id"], 1);
        assert_eq!(val["dimension"], 64);
        assert_eq!(val["live_count"], 10);
        assert_eq!(val["total_count"], 10);
        assert!(val["metadata_checksum"].as_u64().unwrap() > 0);
    }

    #[test]
    fn test_checksum_mismatch_on_read() {
        let (segment, collection) = build_test_segment(10, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 1, &segment, &collection).unwrap();

        // Corrupt metadata_checksum in JSON
        let meta_path = tmp.path().join("segment-1").join("segment_meta.json");
        let mut json_str = std::fs::read_to_string(&meta_path).unwrap();
        // Replace the checksum value
        json_str = json_str.replace(&format!("{}", collection.metadata_checksum), "12345");
        std::fs::write(&meta_path, &json_str).unwrap();

        match read_immutable_segment(tmp.path(), 1) {
            Err(SegmentIoError::MetadataChecksum { .. }) => {}
            Ok(_) => panic!("expected MetadataChecksum error, got Ok"),
            Err(e) => panic!("expected MetadataChecksum error, got {:?}", e),
        }
    }

    #[test]
    fn test_missing_graph_file_returns_error() {
        let (segment, collection) = build_test_segment(10, 64);
        let tmp = tempfile::tempdir().unwrap();

        write_immutable_segment(tmp.path(), 1, &segment, &collection).unwrap();

        // Delete the graph file
        std::fs::remove_file(tmp.path().join("segment-1").join("hnsw_graph.bin")).unwrap();

        match read_immutable_segment(tmp.path(), 1) {
            Err(SegmentIoError::Io(_)) => {}
            Ok(_) => panic!("expected Io error, got Ok"),
            Err(e) => panic!("expected Io error, got {:?}", e),
        }
    }
}
