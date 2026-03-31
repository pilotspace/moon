//! Benchmark vector insert throughput — measures the auto_index_hset path.

use std::time::Instant;

use moon::command::vector_search;
use moon::vector::distance;
use moon::vector::segment::mutable::MutableSegment;
use moon::vector::store::VectorStore;
use moon::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

/// Measure raw MutableSegment.append() throughput (no HSET parsing overhead)
#[test]
fn bench_raw_append_128d() {
    distance::init();
    let dim = 128;
    let n = 100_000;

    let seg = MutableSegment::new(dim as u32);

    // Pre-generate vectors
    let mut rng: u64 = 42;
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    let mut sq_vecs: Vec<Vec<i8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v: Vec<f32> = (0..dim)
            .map(|_| {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((rng >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in v.iter_mut() {
            *x /= norm;
        }

        let mut sq = vec![0i8; dim];
        vector_search::quantize_f32_to_sq(&v, &mut sq);

        vectors.push(v);
        sq_vecs.push(sq);
    }

    let start = Instant::now();
    for i in 0..n {
        let norm: f32 = vectors[i].iter().map(|x| x * x).sum::<f32>().sqrt();
        seg.append(i as u64, &vectors[i], &sq_vecs[i], norm, 0);
    }
    let elapsed = start.elapsed();

    let vps = n as f64 / elapsed.as_secs_f64();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    println!(
        "Raw append 128d: {n} vectors in {:.2}ms = {vps:.0} vec/s ({us_per:.2} µs/vec)",
        elapsed.as_millis()
    );
}

#[test]
fn bench_raw_append_768d() {
    distance::init();
    let dim = 768;
    let n = 10_000;

    let seg = MutableSegment::new(dim as u32);

    let mut rng: u64 = 42;
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    let mut sq_vecs: Vec<Vec<i8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v: Vec<f32> = (0..dim)
            .map(|_| {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((rng >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in v.iter_mut() {
            *x /= norm;
        }

        let mut sq = vec![0i8; dim];
        vector_search::quantize_f32_to_sq(&v, &mut sq);

        vectors.push(v);
        sq_vecs.push(sq);
    }

    let start = Instant::now();
    for i in 0..n {
        let norm: f32 = vectors[i].iter().map(|x| x * x).sum::<f32>().sqrt();
        seg.append(i as u64, &vectors[i], &sq_vecs[i], norm, 0);
    }
    let elapsed = start.elapsed();

    let vps = n as f64 / elapsed.as_secs_f64();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    println!(
        "Raw append 768d: {n} vectors in {:.2}ms = {vps:.0} vec/s ({us_per:.2} µs/vec)",
        elapsed.as_millis()
    );
}

/// Measure full insert pipeline: decode f32 + SQ quantize + append + payload index
#[test]
fn bench_full_insert_pipeline_128d() {
    distance::init();
    let dim = 128;
    let n = 50_000;

    // Create a VectorStore with an index
    let mut store = VectorStore::new();
    let meta = moon::vector::store::IndexMeta {
        name: bytes::Bytes::from_static(b"idx"),
        dimension: dim as u32,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        source_field: bytes::Bytes::from_static(b"vec"),
        key_prefixes: vec![bytes::Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
    };
    store.create_index(meta);

    // Pre-generate vector blobs (like HSET would receive)
    let mut rng: u64 = 42;
    let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v: Vec<f32> = (0..dim)
            .map(|_| {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((rng >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in v.iter_mut() {
            *x /= norm;
        }
        let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
        blobs.push(blob);
    }

    // Measure: decode + quantize + append (simulating auto_index_hset core path)
    let start = Instant::now();
    for i in 0..n {
        let blob = &blobs[i];
        // Decode f32
        let mut f32_vec = Vec::with_capacity(dim as usize);
        for chunk in blob.chunks_exact(4) {
            f32_vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        // SQ quantize
        let mut sq_vec = vec![0i8; dim as usize];
        vector_search::quantize_f32_to_sq(&f32_vec, &mut sq_vec);
        // Norm
        let norm: f32 = f32_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        // Key hash
        let key = format!("doc:{i}");
        let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        // Append
        let idx = store
            .get_index_mut(&bytes::Bytes::from_static(b"idx"))
            .unwrap();
        let snap = idx.segments.load();
        snap.mutable.append(key_hash, &f32_vec, &sq_vec, norm, 0);
    }
    let elapsed = start.elapsed();

    let vps = n as f64 / elapsed.as_secs_f64();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    println!(
        "Full pipeline 128d: {n} vectors in {:.2}ms = {vps:.0} vec/s ({us_per:.2} µs/vec)",
        elapsed.as_millis()
    );
}

#[test]
fn bench_full_insert_pipeline_768d() {
    distance::init();
    let dim = 768;
    let n = 10_000;

    let mut store = VectorStore::new();
    let meta = moon::vector::store::IndexMeta {
        name: bytes::Bytes::from_static(b"idx"),
        dimension: dim as u32,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        source_field: bytes::Bytes::from_static(b"vec"),
        key_prefixes: vec![bytes::Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
    };
    store.create_index(meta);

    let mut rng: u64 = 42;
    let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v: Vec<f32> = (0..dim)
            .map(|_| {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((rng >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in v.iter_mut() {
            *x /= norm;
        }
        let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
        blobs.push(blob);
    }

    let start = Instant::now();
    for i in 0..n {
        let blob = &blobs[i];
        let mut f32_vec = Vec::with_capacity(dim as usize);
        for chunk in blob.chunks_exact(4) {
            f32_vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        let mut sq_vec = vec![0i8; dim as usize];
        vector_search::quantize_f32_to_sq(&f32_vec, &mut sq_vec);
        let norm: f32 = f32_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        let key = format!("doc:{i}");
        let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        let idx = store
            .get_index_mut(&bytes::Bytes::from_static(b"idx"))
            .unwrap();
        let snap = idx.segments.load();
        snap.mutable.append(key_hash, &f32_vec, &sq_vec, norm, 0);
    }
    let elapsed = start.elapsed();

    let vps = n as f64 / elapsed.as_secs_f64();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    println!(
        "Full pipeline 768d: {n} vectors in {:.2}ms = {vps:.0} vec/s ({us_per:.2} µs/vec)",
        elapsed.as_millis()
    );
}
