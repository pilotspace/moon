//! PERF-08 RED test: prove single-probe insert_or_update is faster
//! than the legacy get_mut + insert pattern, AND preserves Database::set
//! semantics bit-for-bit.

use bytes::Bytes;
use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use moon::storage::db::Database;
use moon::storage::entry::Entry;
use std::time::Instant;

const N: usize = 1_000_000;

/// Functional parity: 1000 SETs (mix of new & repeat keys) leave the DB in
/// the same state via `Database::set` as a hand-rolled `get_mut + insert`.
#[test]
fn test_database_set_semantic_parity() {
    let mut db_via_new = Database::new();
    let mut db_via_legacy = Database::new();

    for i in 0..1000 {
        let k_idx = i % 700; // 30% are repeat keys -> exercises both paths
        let key = Bytes::from(format!("p_{:04}", k_idx));
        let entry = Entry::new_string(Bytes::from(format!("v{}", i)));

        db_via_new.set(key.clone(), entry.clone());

        // Hand-rolled legacy path on the control DB
        if let Some(old) = db_via_legacy.data_mut().get_mut(key.as_ref()) {
            let mut e = entry.clone();
            e.set_version(old.version() + 1);
            *old = e;
        } else {
            db_via_legacy
                .data_mut()
                .insert(CompactKey::from(key), entry);
        }
    }

    assert_eq!(
        db_via_new.data().len(),
        db_via_legacy.data().len(),
        "len mismatch"
    );

    // Compare every key's version
    for i in 0..700 {
        let key = format!("p_{:04}", i);
        let v_new = db_via_new.data().get(key.as_bytes()).map(|e| e.version());
        let v_legacy = db_via_legacy
            .data()
            .get(key.as_bytes())
            .map(|e| e.version());
        assert_eq!(
            v_new, v_legacy,
            "Version mismatch at key {}: {:?} vs {:?}",
            key, v_new, v_legacy
        );
    }
}

/// Timing: DashTable::insert_or_update vs DashTable::get_mut + insert on 50%/50%.
///
/// Tests at the DashTable level for fair comparison (no entry_overhead, no
/// used_memory accounting — those are equal overhead in both paths).
///
/// Math: hit path is 1 probe in both old and new. Miss path: old = 2 probes
/// (get_mut miss + insert.find), new = 1 probe. With 50% misses:
/// expected ratio = (0.5*1 + 0.5*1) / (0.5*1 + 0.5*2) = 1.0/1.5 = 0.67.
/// Threshold 0.95 is deliberately loose — the authoritative gate is
/// perf annotate on Linux (Task 3). This test is the regression net.
#[test]
fn test_insert_or_update_at_least_faster_on_50_50() {
    // Pre-build keys so timing measures only the hash-table path.
    let pre: Vec<Bytes> = (0..N).map(|i| Bytes::from(format!("t_{:08}", i))).collect();

    // -- Control: get_mut + insert on DashTable --
    let mut dt_control: DashTable<CompactKey, u64> = DashTable::with_capacity(N);
    let t0 = Instant::now();
    for (i, k) in pre.iter().enumerate() {
        let lookup_key = if i % 2 == 0 || i < N / 2 {
            k.clone()
        } else {
            pre[i / 2].clone()
        };
        if let Some(old) = dt_control.get_mut(lookup_key.as_ref()) {
            *old = i as u64;
        } else {
            dt_control.insert(CompactKey::from(lookup_key), i as u64);
        }
    }
    let control_ns = t0.elapsed().as_nanos();

    // -- Test: insert_or_update on DashTable --
    let mut dt_test: DashTable<CompactKey, u64> = DashTable::with_capacity(N);
    let t1 = Instant::now();
    for (i, k) in pre.iter().enumerate() {
        let lookup_key = if i % 2 == 0 || i < N / 2 {
            k.clone()
        } else {
            pre[i / 2].clone()
        };
        dt_test.insert_or_update(CompactKey::from(lookup_key), |v| *v = i as u64, || i as u64);
    }
    let test_ns = t1.elapsed().as_nanos();

    let ratio = test_ns as f64 / control_ns as f64;
    eprintln!(
        "PERF-08 timing ratio: {:.3} (target <0.95), control={}ns test={}ns",
        ratio, control_ns, test_ns
    );
    // NOTE: this assertion may be noisy on macOS due to cache/scheduler variance.
    // The authoritative gate is perf annotate on Linux (Task 3).
    assert!(
        ratio < 0.95,
        "insert_or_update timing ratio {:.3} >= 0.95 (control {} ns vs test {} ns). \
         Single-probe gain not materializing.",
        ratio,
        control_ns,
        test_ns
    );
}
