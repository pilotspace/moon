//! ADD task `hotpath-lock-quickwins` — failing-first suite (runtime tests).
//!
//! RED tests (fail until the build lands):
//!   - qw7_vector_key_maps_pruned_on_delete — key-map pruning does not exist yet.
//!
//! PIN tests (green before AND after — the parity safety net the batch must
//! not break; Reject: "behavior_regression"):
//!   - qw5_backlog_append_golden — byte-identical backlog state machine.
//!   - qw2_try_wal_append_required_false_on_full — durability gate contract.
//!   - qw3_replication_offsets_exact — offset arithmetic under concurrency.
//!   - qw4_total_commands_exact — bespoke INFO counter exactness.
//!
//! New-API red tests (compile-red) live in `tests/quickwins_red_api.rs`.
//! Run this file alone with: cargo test --test quickwins_red

use bytes::Bytes;
use moon::replication::backlog::ReplicationBacklog;
use moon::replication::state::ReplicationState;

// ---------------------------------------------------------------------------
// QW7 — vector key maps shrink on delete (RED until pruning is implemented)
// ---------------------------------------------------------------------------

mod qw7 {
    use super::*;
    use moon::vector::distance;
    use moon::vector::store::{IndexMeta, MergeMode, VectorStore};
    use moon::vector::turbo_quant::collection::QuantizationConfig;
    use moon::vector::turbo_quant::encoder::padded_dimension;
    use moon::vector::types::DistanceMetric;

    fn make_idx(dim: u32) -> IndexMeta {
        IndexMeta {
            name: Bytes::from_static(b"idx"),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 8,
            hnsw_ef_construction: 50,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: vec![Bytes::from_static(b"doc:")],
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: moon::vector::turbo_quant::collection::BuildMode::Light,
            vector_fields: Vec::new(),
            schema_fields: Vec::new(),
            merge_mode: MergeMode::GraphUnion,
            keep_raw: false,
        }
    }

    fn det_vec(dim: usize, seed: u64) -> Vec<f32> {
        // Deterministic pseudo-vector; distribution quality is irrelevant here.
        (0..dim)
            .map(|i| {
                let x = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(i as u64);
                ((x >> 33) as f32) / (u32::MAX as f32)
            })
            .collect()
    }

    /// Deleting indexed vectors must shrink the per-index key-hash maps —
    /// they track LIVE keys, not historical inserts (review finding 6.3:
    /// today there is no `.remove()` call anywhere; the maps grow forever).
    #[test]
    fn qw7_vector_key_maps_pruned_on_delete() {
        distance::init();
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const TOTAL: usize = 100;
        const DELETED: usize = 60;

        for i in 0..TOTAL {
            let key = format!("doc:{i}");
            let hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
            store
                .insert_vector(b"idx", &det_vec(64, i as u64), hash, Bytes::from(key))
                .unwrap();
        }
        assert_eq!(
            store.get_index(b"idx").unwrap().key_hash_to_key.len(),
            TOTAL,
            "sanity: insert populates key_hash_to_key"
        );

        for i in 0..DELETED {
            let key = format!("doc:{i}");
            store.mark_deleted_for_key(key.as_bytes());
        }

        let idx = store.get_index(b"idx").unwrap();
        assert_eq!(
            idx.key_hash_to_key.len(),
            TOTAL - DELETED,
            "key_hash_to_key must track live keys only (RED until QW7 pruning lands)"
        );
        let deleted_hash = xxhash_rust::xxh64::xxh64(b"doc:0", 0);
        assert!(
            !idx.key_hash_to_key.contains_key(&deleted_hash),
            "deleted key's hash entry must be pruned"
        );
        assert!(
            !idx.key_hash_to_global_id.contains_key(&deleted_hash),
            "deleted key's global-id entry must be pruned"
        );
    }
}

// ---------------------------------------------------------------------------
// QW5 — backlog append golden model (PIN: byte-identical before/after)
// ---------------------------------------------------------------------------

/// Reference model: the current per-byte semantics, kept in the test so the
/// bulk-copy rewrite can be checked against it step by step.
struct ModelBacklog {
    buf: std::collections::VecDeque<u8>,
    capacity: usize,
    start_offset: u64,
    end_offset: u64,
}

impl ModelBacklog {
    fn new(capacity: usize) -> Self {
        ModelBacklog {
            buf: std::collections::VecDeque::with_capacity(capacity),
            capacity,
            start_offset: 0,
            end_offset: 0,
        }
    }
    fn append(&mut self, data: &[u8]) {
        for &b in data {
            if self.buf.len() == self.capacity {
                self.buf.pop_front();
                self.start_offset += 1;
            }
            self.buf.push_back(b);
        }
        self.end_offset += data.len() as u64;
    }
}

#[test]
fn qw5_backlog_append_golden() {
    const CAP: usize = 64;
    let mut real = ReplicationBacklog::new(CAP);
    let mut model = ModelBacklog::new(CAP);

    let sizes = [1usize, CAP / 2, CAP, CAP + 7, 3 * CAP, 0, 5];
    let mut byte: u8 = 0;
    for (step, &n) in sizes.iter().enumerate() {
        let chunk: Vec<u8> = (0..n)
            .map(|_| {
                byte = byte.wrapping_add(13);
                byte
            })
            .collect();
        real.append(&chunk);
        model.append(&chunk);

        assert_eq!(
            real.start_offset(),
            model.start_offset,
            "step {step}: start_offset diverged"
        );
        assert_eq!(
            real.end_offset(),
            model.end_offset,
            "step {step}: end_offset diverged"
        );
        let got = real
            .bytes_from(model.start_offset)
            .expect("start_offset must be readable");
        let want: Vec<u8> = model.buf.iter().copied().collect();
        assert_eq!(got, want, "step {step}: buffer contents diverged");
    }

    // Range probes across the live window.
    let (lo, hi) = (real.start_offset(), real.end_offset());
    for off in [lo, lo + 1, (lo + hi) / 2, hi] {
        let got = real.bytes_from(off);
        let want = if off >= model.start_offset && off <= model.end_offset {
            Some(
                model
                    .buf
                    .iter()
                    .copied()
                    .skip((off - model.start_offset) as usize)
                    .collect::<Vec<u8>>(),
            )
        } else {
            None
        };
        assert_eq!(got, want, "bytes_from({off}) diverged");
    }
    // Evicted and future offsets stay rejected.
    assert_eq!(real.bytes_from(lo.wrapping_sub(1)), None, "evicted offset");
    assert_eq!(real.bytes_from(hi + 1), None, "future offset");
}

// ---------------------------------------------------------------------------
// QW2 — WAL required-append gates mutation on a full channel (PIN)
// ---------------------------------------------------------------------------

#[test]
fn qw2_try_wal_append_required_false_on_full() {
    use moon::shard::shared_databases::ShardDatabases;
    use moon::storage::db::Database;

    let (dbs, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);

    // Persistence disabled -> no durability requirement -> true.
    assert!(
        dbs.try_wal_append_required(0, Bytes::from_static(b"x")),
        "no WAL configured: append must report success (no durability requirement)"
    );

    // Bounded(1) channel, never drained: first append fills it, second must
    // be refused — the caller-visible gate that prevents un-journaled mutation
    // (Reject: durability_contract_broken).
    let (tx, _rx) = moon::runtime::channel::mpsc_bounded::<Bytes>(1);
    dbs.set_wal_append_tx(0, tx);

    assert!(
        dbs.try_wal_append_required(0, Bytes::from_static(b"a")),
        "first append fits the bounded(1) channel"
    );
    assert!(
        !dbs.try_wal_append_required(0, Bytes::from_static(b"b")),
        "full WAL channel must report false so the caller skips the mutation"
    );

    // Fire-and-forget variant must not panic on a full channel.
    dbs.wal_append(0, Bytes::from_static(b"c"));
}

// ---------------------------------------------------------------------------
// QW3 — replication offset arithmetic exact under concurrency (PIN)
// ---------------------------------------------------------------------------

#[test]
fn qw3_replication_offsets_exact() {
    use std::sync::Arc;

    const SHARDS: usize = 4;
    const WRITES_PER_SHARD: u64 = 1_000;
    const DELTA: u64 = 17;

    let state = Arc::new(ReplicationState::new(
        SHARDS,
        "a".repeat(40),
        "b".repeat(40),
    ));

    let handles: Vec<_> = (0..SHARDS)
        .map(|shard| {
            let st = Arc::clone(&state);
            std::thread::spawn(move || {
                for _ in 0..WRITES_PER_SHARD {
                    st.increment_shard_offset(shard, DELTA);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    for shard in 0..SHARDS {
        assert_eq!(
            state.shard_offset(shard),
            WRITES_PER_SHARD * DELTA,
            "shard {shard} offset must equal exact byte total"
        );
    }
    assert_eq!(
        state.total_offset(),
        SHARDS as u64 * WRITES_PER_SHARD * DELTA,
        "master_repl_offset must equal the sum of all shard writes"
    );
}

// ---------------------------------------------------------------------------
// QW4 — bespoke INFO command counter exact under concurrency (PIN)
// ---------------------------------------------------------------------------

#[test]
fn qw4_total_commands_exact() {
    use moon::admin::metrics_setup;

    const THREADS: usize = 4;
    const PER_THREAD: u64 = 5_000;

    let before = metrics_setup::total_commands_processed();
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(|| {
                for i in 0..PER_THREAD {
                    if i % 2 == 0 {
                        metrics_setup::record_command_no_latency("set");
                    } else {
                        metrics_setup::record_command("get", 1);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let after = metrics_setup::total_commands_processed();

    assert_eq!(
        after - before,
        THREADS as u64 * PER_THREAD,
        "INFO total_commands_processed must stay exact (no lost or double counts)"
    );
}
