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
//! `--shards 1`, with the scatter's own parameters (default field, the query's
//! prefilter, unpaginated: each shard answers its top `k` and the coordinator
//! merges). `None` means a shape the snapshot does not cover — unknown index,
//! a dimension mismatch, a filter this index refuses — and the leg runs the
//! synchronous `search_local_filtered_with_text`, whose error frames those
//! cases need ([`KnnLeg::Done`]).
//!
//! moon#1238: the prefilter (`@f:{v}=>[KNN …]`) used to stop at the
//! coordinator — every leg captured with no filter, so the scatter answered an
//! unfiltered KNN. It now rides to every leg and is evaluated by the same
//! capture `--shards 1` uses, and a leg that cannot evaluate it answers an
//! error the merge returns instead of skipping ([`merge_knn_legs`]).

use crate::protocol::Frame;
use crate::runtime::channel;
use crate::text::store::TextStore;
use crate::vector::filter::FilterExpr;
use crate::vector::segment::holder::{SearchSnapshot, SegmentHolder, ft_search_yield_budget};
use crate::vector::store::VectorStore;

#[cfg(feature = "runtime-monoio")]
use crate::runtime::MonoioSpawner as Spawner;
#[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
use crate::runtime::TokioSpawner as Spawner;

/// This shard's leg of a multi-shard KNN, decided inside the shard-slice
/// borrow; after [`plan_knn_leg`] returns nothing borrows the store.
pub(crate) enum KnnLeg {
    /// The owned snapshot for the cooperative search ([`run_knn`] /
    /// [`spawn_knn_reply`]).
    Yield(Box<SearchSnapshot>),
    /// A shape the snapshot does not cover, already answered by the
    /// synchronous search: its reply (in practice an error frame).
    Done(Frame),
}

/// Plan this shard's KNN leg: capture the snapshot, or answer a shape the
/// capture does not cover synchronously. `filter` is the query's prefilter,
/// evaluated on this shard's payload index (and BM25 plane, for a declared
/// TEXT field) exactly as `--shards 1` evaluates it.
pub(crate) fn plan_knn_leg(
    store: &mut VectorStore,
    text_store: &TextStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
    as_of_lsn: u64,
    db_index: u8,
) -> KnnLeg {
    let snapshot = crate::command::vector_search::ft_search::dispatch::capture_dense_knn_snapshot(
        store,
        index_name,
        query_blob,
        k,
        None,
        filter,
        as_of_lsn,
        db_index,
        Some(text_store),
    );
    match snapshot {
        Some(snapshot) => KnnLeg::Yield(Box::new(snapshot)),
        None => KnnLeg::Done(
            crate::command::vector_search::ft_search::search_local_filtered_with_text(
                store,
                index_name,
                query_blob,
                k,
                filter,
                0,
                usize::MAX,
                None,
                as_of_lsn,
                db_index,
                Some(text_store),
            ),
        ),
    }
}

/// Merge the legs' replies (local first, then remote in ascending shard
/// order) into the global top `k`.
///
/// With a prefilter, the first leg that answered an error is the reply
/// (moon#1238). `merge_search_results` skips errored legs, which would fold a
/// filter every leg refuses into a successful empty page, and one leg's
/// refusal into an answer silently missing that shard. Without a filter the
/// legacy fold is unchanged.
pub(crate) fn merge_knn_legs(legs: &[Frame], k: usize, filtered: bool) -> Frame {
    if filtered {
        if let Some(err) = legs.iter().find(|f| matches!(f, Frame::Error(_))) {
            return err.clone();
        }
    }
    crate::command::vector_search::merge_search_results(legs, k, 0, usize::MAX)
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::merge_knn_legs;
    use crate::command::vector_search::merge_search_results;
    use crate::protocol::Frame;

    fn leg(docs: &[(&'static str, &'static str)]) -> Frame {
        let mut items = vec![Frame::Integer(docs.len() as i64)];
        for (key, score) in docs {
            items.push(Frame::BulkString(Bytes::from_static(key.as_bytes())));
            items.push(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"__vec_score")),
                    Frame::BulkString(Bytes::from_static(score.as_bytes())),
                ]
                .into(),
            ));
        }
        Frame::Array(items.into())
    }

    /// moon#1238: with a prefilter, a leg that could not evaluate it is the
    /// reply — wherever it sits in merge order — instead of being skipped into
    /// an empty or partial page. Without one, the legacy fold is unchanged.
    #[test]
    fn a_refusing_leg_is_the_reply_of_a_filtered_query() {
        let ok = leg(&[("d:1", "0.5")]);
        let other = leg(&[("d:2", "0.25")]);
        let refused = Frame::Error(Bytes::from_static(b"ERR full-text KNN filter"));

        assert_eq!(
            merge_knn_legs(&[ok.clone(), refused.clone()], 5, true),
            refused
        );
        assert_eq!(
            merge_knn_legs(&[refused.clone(), ok.clone()], 5, true),
            refused
        );
        assert_eq!(
            merge_knn_legs(&[refused.clone(), refused.clone()], 5, true),
            refused
        );

        let plain = merge_search_results(&[ok.clone(), other.clone()], 5, 0, usize::MAX);
        assert_eq!(merge_knn_legs(&[ok.clone(), other.clone()], 5, true), plain);
        assert_eq!(merge_knn_legs(&[ok.clone(), other], 5, false), plain);
        assert_eq!(
            merge_knn_legs(&[ok.clone(), refused], 5, false),
            merge_search_results(&[ok], 5, 0, usize::MAX),
            "unfiltered: an errored leg is still skipped (unchanged)"
        );
    }
}
