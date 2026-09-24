//! Multi-shard FT.SEARCH KNN legs on the cooperative (yielding) path
//! (moon#1182).
//!
//! At `--shards 1` a dense-KNN FT.SEARCH captures an owned snapshot of the
//! index and `.await`s `SegmentHolder::search_mvcc_yielding`, which hands the
//! event loop back between chunks (the C5 task). At `--shards > 1` every shard
//! instead ran `search_local_filtered` synchronously: the coordinator's own
//! leg on its connection task, every remote leg inside `drain_spsc_shared` —
//! stopping that shard's tick, SPSC drain and local connections for the whole
//! search — so `ft_search_cooperative_yields_total` stayed 0 on every
//! multi-shard server.
//!
//! Both legs now go through the same capture + yielding search. The capture is
//! `capture_dense_knn_snapshot`, the one `ft_search_capture` makes at
//! `--shards 1`, with the scatter's own parameters (default field, no filter,
//! unpaginated: each shard answers its top `k` and the coordinator merges).
//! `None` means a shape the snapshot does not cover — unknown index or a
//! dimension mismatch — and the caller runs the synchronous
//! `search_local_filtered`, whose error frames those cases need.

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::text::store::TextStore;
use crate::vector::segment::holder::{SearchSnapshot, SegmentHolder, ft_search_yield_budget};
use crate::vector::store::VectorStore;

#[cfg(feature = "runtime-monoio")]
use crate::runtime::MonoioSpawner as Spawner;
#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
use crate::runtime::TokioSpawner as Spawner;

/// Capture the owned snapshot for this shard's KNN leg, or `None` for a shape
/// the synchronous path must answer. Runs inside the shard-slice borrow; after
/// it returns nothing borrows the store.
pub(crate) fn capture_knn(
    store: &mut VectorStore,
    text_store: &TextStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    as_of_lsn: u64,
    db_index: u8,
) -> Option<Box<SearchSnapshot>> {
    crate::command::vector_search::ft_search::dispatch::capture_dense_knn_snapshot(
        store,
        index_name,
        query_blob,
        k,
        None,
        None,
        as_of_lsn,
        db_index,
        Some(text_store),
    )
    .map(Box::new)
}

/// Run a captured KNN leg cooperatively and build its unpaginated reply —
/// the frame `search_local_filtered(.., None, 0, usize::MAX, None, ..)`
/// builds (C5's G-IDENTITY: the yielding search returns the synchronous
/// search's results).
///
/// Off-loop COLD→WARM reloads the capture submitted are awaited first
/// (parking only this task) and installed into the index (moon#1070), exactly
/// as the `--shards 1` handler does.
pub(crate) async fn run_knn(mut snapshot: Box<SearchSnapshot>) -> Frame {
    if snapshot.await_pending_reloads().await > 0 {
        crate::shard::slice::with_shard(|s| s.vector_store.install_completed_reloads());
    }
    let results =
        SegmentHolder::search_mvcc_yielding(&mut snapshot, ft_search_yield_budget()).await;
    crate::command::vector_search::build_search_response(
        &results,
        &snapshot.key_hash_to_key,
        0,
        usize::MAX,
    )
}

/// A remote shard's leg: run [`run_knn`] on a local task and send the reply
/// from there, so the SPSC drain that received the request returns at once.
pub(crate) fn spawn_knn_reply(
    snapshot: Box<SearchSnapshot>,
    reply_tx: channel::OneshotSender<Frame>,
) {
    <Spawner as crate::runtime::traits::RuntimeSpawn>::spawn_local(async move {
        let frame = run_knn(snapshot).await;
        let _ = reply_tx.send(frame);
    });
}
