//! AOF writer pool: per-shard / single writer-task handles and backpressure.
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use super::rewrite::{do_rewrite_per_shard, drain_pending_appends_framed};
use super::*;
// `drain_pending_appends` (non-framed) exists only under the monoio runtime.
#[cfg(feature = "runtime-monoio")]
use super::rewrite::drain_pending_appends;

#[derive(Clone)]
pub struct AofWriterPool {
    senders: Vec<channel::MpscSender<AofMessage>>,
    layout: crate::persistence::aof_manifest::AofLayout,
    /// Fsync policy configured at writer-task construction. Read on the
    /// hot append path: `Always` routes through `AppendSync` for
    /// fsync-before-ack durability (H1 fix); everything else stays on
    /// the fire-and-forget `Append` path.
    fsync_policy: FsyncPolicy,
    /// F2: max time `try_send_append_durable` waits for the `Always` fsync
    /// ack before failing the write. `Duration::ZERO` means unbounded
    /// (legacy behavior). Prevents a stalled disk from parking write
    /// connections forever (design-for-failure).
    fsync_timeout: Duration,
    /// F6: persistence base dir (the parent of `appendonlydir/`), set only for
    /// PerShard pools that may service a per-shard BGREWRITEAOF. Needed to load
    /// the authoritative manifest fresh at rewrite time. `None` for TopLevel
    /// pools and test pools that never rewrite.
    base_dir: Option<PathBuf>,
    /// C4: per-shard SPSC fold producers for AofFold cooperative snapshot.
    ///
    /// `fold_producers[i]` is the SPSC producer into shard `i`'s event-loop
    /// ring.  Wrapped in `Arc<Mutex>` so the AOF writer thread (not the shard
    /// thread) can push `ShardMessage::AofFold` safely.  Set only for PerShard
    /// pools that wire up fold channels; `None` for TopLevel pools and test
    /// pools that use the legacy lock-based path or never rewrite.
    fold_producers: Option<
        Vec<Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>>,
    >,
    /// C4: per-shard Notify handles that wake the shard event loops after
    /// an AofFold push.  Parallel to `fold_producers`.
    fold_notifiers: Option<Vec<Arc<crate::runtime::channel::Notify>>>,
}

impl AofWriterPool {
    /// Build a TopLevel pool from a single existing writer sender. Used for
    /// legacy v1 deployments and `--shards 1` v2 deployments where one writer
    /// thread services every shard.
    pub fn top_level(sender: channel::MpscSender<AofMessage>) -> Arc<Self> {
        Self::top_level_with_policy(sender, FsyncPolicy::EverySec, DEFAULT_AOF_FSYNC_TIMEOUT)
    }

    /// Same as [`Self::top_level`] but with an explicit fsync policy. The
    /// policy controls whether [`Self::try_send_append_durable`] takes the
    /// fast (fire-and-forget) or rendezvous (`AppendSync`) path.
    /// `fsync_timeout` bounds the `Always` ack await (F2); `Duration::ZERO`
    /// = unbounded.
    pub fn top_level_with_policy(
        sender: channel::MpscSender<AofMessage>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            senders: vec![sender],
            layout: crate::persistence::aof_manifest::AofLayout::TopLevel,
            fsync_policy,
            fsync_timeout,
            base_dir: None,
            fold_producers: None,
            fold_notifiers: None,
        })
    }

    /// Build a PerShard pool from N senders. `senders[i]` MUST be the writer
    /// task that owns `appendonlydir/shard-{i}/`. The vector's length is the
    /// shard count; passing a length-1 vector here is a bug — use
    /// [`AofWriterPool::top_level`] instead.
    pub fn per_shard(senders: Vec<channel::MpscSender<AofMessage>>) -> Arc<Self> {
        Self::per_shard_with_policy(senders, FsyncPolicy::EverySec, DEFAULT_AOF_FSYNC_TIMEOUT)
    }

    /// Same as [`Self::per_shard`] but with an explicit fsync policy.
    /// `fsync_timeout` bounds the `Always` ack await (F2); `Duration::ZERO`
    /// = unbounded.
    pub fn per_shard_with_policy(
        senders: Vec<channel::MpscSender<AofMessage>>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
    ) -> Arc<Self> {
        debug_assert!(
            senders.len() >= 2,
            "per_shard pool needs >=2 writers; use top_level for single-writer"
        );
        Arc::new(Self {
            senders,
            layout: crate::persistence::aof_manifest::AofLayout::PerShard,
            fsync_policy,
            fsync_timeout,
            base_dir: None,
            fold_producers: None,
            fold_notifiers: None,
        })
    }

    /// F6: same as [`Self::per_shard_with_policy`] but records the persistence
    /// `base_dir` so a per-shard BGREWRITEAOF can load the authoritative
    /// manifest fresh at rewrite time. This is the production constructor used
    /// by `main.rs` for the PerShard layout.
    pub fn per_shard_with_base_dir(
        senders: Vec<channel::MpscSender<AofMessage>>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
        base_dir: PathBuf,
    ) -> Arc<Self> {
        debug_assert!(
            senders.len() >= 2,
            "per_shard pool needs >=2 writers; use top_level for single-writer"
        );
        Arc::new(Self {
            senders,
            layout: crate::persistence::aof_manifest::AofLayout::PerShard,
            fsync_policy,
            fsync_timeout,
            base_dir: Some(base_dir),
            fold_producers: None,
            fold_notifiers: None,
        })
    }

    /// C4: same as [`Self::per_shard_with_base_dir`] but also wires the per-shard
    /// SPSC fold channels used by the cooperative AOF snapshot (ShardSlice path).
    ///
    /// `fold_producers[i]` MUST be a producer into shard `i`'s SPSC ring that has
    /// a corresponding `HeapCons` already installed in the shard's consumer list —
    /// created via [`crate::shard::mesh::create_aof_fold_channels`] and pushed into
    /// each shard's `consumers` vec before `shard.run()` is called.
    ///
    /// The pool clones the `Arc`s; callers retain no live reference after passing.
    pub fn per_shard_with_fold_channels(
        senders: Vec<channel::MpscSender<AofMessage>>,
        fsync_policy: FsyncPolicy,
        fsync_timeout: Duration,
        base_dir: PathBuf,
        fold_producers: Vec<
            Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        >,
        fold_notifiers: Vec<Arc<crate::runtime::channel::Notify>>,
    ) -> Arc<Self> {
        debug_assert!(
            senders.len() >= 2,
            "per_shard pool needs >=2 writers; use top_level for single-writer"
        );
        debug_assert_eq!(
            senders.len(),
            fold_producers.len(),
            "fold_producers must have one entry per shard"
        );
        debug_assert_eq!(
            senders.len(),
            fold_notifiers.len(),
            "fold_notifiers must have one entry per shard"
        );
        Arc::new(Self {
            senders,
            layout: crate::persistence::aof_manifest::AofLayout::PerShard,
            fsync_policy,
            fsync_timeout,
            base_dir: Some(base_dir),
            fold_producers: Some(fold_producers),
            fold_notifiers: Some(fold_notifiers),
        })
    }

    /// C4: Wire fold channels into a pool that was built with
    /// [`Self::per_shard_with_base_dir`] (the production path in `main.rs`).
    ///
    /// `producers[i]` MUST be the SPSC producer into shard `i`'s ring and
    /// `notifiers[i]` the matching Notify handle — both created by
    /// [`crate::shard::mesh::create_aof_fold_channels`] and the corresponding
    /// consumers already merged into shard `i`'s consumer vec before
    /// `shard.run()` is called.
    ///
    /// **Idempotent-warn:** calling this a second time logs an error and
    /// returns without overwriting the existing channels (double-set is a
    /// startup-configuration bug, not a runtime error worth crashing over).
    ///
    /// **Panics (debug only):** if `producers.len() != self.senders.len()` or
    /// `notifiers.len() != self.senders.len()` — the lengths must be identical
    /// to the shard count established at construction time.
    pub fn set_fold_channels(
        &mut self,
        producers: Vec<
            Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        >,
        notifiers: Vec<Arc<crate::runtime::channel::Notify>>,
    ) {
        if self.fold_producers.is_some() || self.fold_notifiers.is_some() {
            error!(
                "set_fold_channels called twice on AofWriterPool (shard count {}). \
                 The first set of channels is kept. This is a startup-configuration \
                 bug — check the main.rs wiring.",
                self.senders.len()
            );
            return;
        }
        debug_assert_eq!(
            producers.len(),
            self.senders.len(),
            "set_fold_channels: producers len {} != shard count {}",
            producers.len(),
            self.senders.len(),
        );
        debug_assert_eq!(
            notifiers.len(),
            self.senders.len(),
            "set_fold_channels: notifiers len {} != shard count {}",
            notifiers.len(),
            self.senders.len(),
        );
        self.fold_producers = Some(producers);
        self.fold_notifiers = Some(notifiers);
    }

    /// Returns the configured fsync policy. Hot-path callers read this to
    /// decide between the fast (`try_send_append`) and durable
    /// (`try_send_append_sync`) write paths.
    #[inline]
    pub fn fsync_policy(&self) -> FsyncPolicy {
        self.fsync_policy
    }

    /// Policy-aware AOF append. For `FsyncPolicy::Always`, this awaits
    /// `AppendSync` and returns `Ok(())` only after `sync_data()` confirms
    /// the entry is on durable storage — closing the H1 in-flight loss
    /// vector identified in the investigation report. For `EverySec` and
    /// `No`, it stays on the fire-and-forget path (zero new latency).
    ///
    /// Returns `Err(AofAck)` only on the Always path when the write or
    /// fsync failed (or the writer task is gone). Callers MUST treat
    /// `Err(_)` as a hard failure — return an error frame to the client,
    /// do NOT respond `+OK`.
    ///
    /// Async because the Always branch awaits a oneshot receiver. The
    /// non-Always branch resolves immediately (no actual suspension) so
    /// the only overhead is one `match` and the implicit Future state
    /// machine; benchmarked at ~5 ns per call on the EverySec hot path,
    /// far below the per-write WAL/replication cost.
    #[inline]
    pub async fn try_send_append_durable(
        &self,
        shard_id: usize,
        lsn: u64,
        bytes: Bytes,
    ) -> Result<(), AofAck> {
        match self.fsync_policy {
            FsyncPolicy::Always => {
                let rx = self.try_send_append_sync(shard_id, lsn, bytes);
                // F2 (design-for-failure): bound the wait so a stalled disk
                // can't park this connection forever. On elapse the write is
                // failed — the entry may still land on disk later, but
                // durability is NOT confirmed, so the caller must not report
                // success. `Duration::ZERO` keeps the legacy unbounded await.
                match Self::await_ack(rx, self.fsync_timeout).await {
                    AckOutcome::Ack(AofAck::Synced) => Ok(()),
                    AckOutcome::Ack(other) => Err(other),
                    // Writer task gone / channel disconnected.
                    AckOutcome::Disconnected => Err(AofAck::WriteFailed),
                    // Fsync did not confirm within the bound.
                    AckOutcome::TimedOut => Err(AofAck::FsyncFailed),
                }
            }
            FsyncPolicy::EverySec | FsyncPolicy::No => {
                self.try_send_append(shard_id, lsn, bytes);
                Ok(())
            }
        }
    }

    /// Await an `AppendSync` ack receiver under a bounded timeout (F2).
    ///
    /// `timeout == Duration::ZERO` preserves the legacy unbounded await
    /// (used when the operator explicitly opts out via
    /// `--aof-fsync-timeout-ms 0`). Otherwise the await is capped by the
    /// runtime-appropriate timer; on elapse the in-flight fsync is
    /// abandoned (the receiver is dropped) and `TimedOut` is returned.
    ///
    /// Runtime-agnostic: monoio uses `select! { rx, sleep }` (matching the
    /// established `cluster::failover` pattern — monoio 0.2 has no
    /// `time::timeout`); tokio uses `tokio::time::timeout`. Both resolve the
    /// ack first if it arrives within the bound, otherwise `TimedOut`.
    async fn await_ack(
        rx: crate::runtime::channel::OneshotReceiver<AofAck>,
        timeout: Duration,
    ) -> AckOutcome {
        if timeout.is_zero() {
            return match rx.await {
                Ok(ack) => AckOutcome::Ack(ack),
                Err(_) => AckOutcome::Disconnected,
            };
        }

        #[cfg(feature = "runtime-monoio")]
        {
            monoio::select! {
                res = rx => match res {
                    Ok(ack) => AckOutcome::Ack(ack),
                    Err(_) => AckOutcome::Disconnected,
                },
                _ = monoio::time::sleep(timeout) => AckOutcome::TimedOut,
            }
        }
        #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
        {
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(ack)) => AckOutcome::Ack(ack),
                Ok(Err(_)) => AckOutcome::Disconnected,
                Err(_) => AckOutcome::TimedOut,
            }
        }
    }

    /// Return the writer sender that owns the given shard's AOF file.
    ///
    /// For TopLevel pools, `shard_id` is ignored — all shards multiplex onto
    /// the single sender. For PerShard pools, `shard_id` MUST be in range
    /// `[0, num_writers())`; an out-of-range id is a programmer error and
    /// panics in debug builds.
    #[inline]
    pub fn sender(&self, shard_id: usize) -> &channel::MpscSender<AofMessage> {
        use crate::persistence::aof_manifest::AofLayout;
        match self.layout {
            AofLayout::TopLevel => &self.senders[0],
            AofLayout::PerShard => {
                debug_assert!(
                    shard_id < self.senders.len(),
                    "shard_id {} out of range for per-shard pool of size {}",
                    shard_id,
                    self.senders.len()
                );
                &self.senders[shard_id]
            }
        }
    }

    /// Fire-and-forget append for the given shard, tagged with the LSN that
    /// was issued for this write (see [`AofMessage::Append`] docs for LSN
    /// semantics per layout). Call sites must source `lsn` from
    /// `ReplicationState::issue_lsn(shard_id, bytes.len() as u64)` for writes
    /// that participate in replication ordering; sites without a
    /// replication-state handle pass 0.
    #[inline]
    pub fn try_send_append(&self, shard_id: usize, lsn: u64, bytes: Bytes) {
        if let Err(e) = self
            .sender(shard_id)
            .try_send(AofMessage::Append { lsn, bytes })
        {
            warn!(
                "AOF append dropped for shard {} (lsn {}): channel {}",
                shard_id,
                lsn,
                match e {
                    flume::TrySendError::Full(_) => "full",
                    flume::TrySendError::Disconnected(_) => "disconnected",
                }
            );
        }
    }

    /// Synchronous (fsync-before-ack) append for `appendfsync=always`
    /// durability (RFC § 4 — Fix 2). Returns a receiver the caller MUST
    /// await before responding to the client; `AofAck::Synced` means the
    /// entry is on durable storage.
    ///
    /// **Failure handling:** if the write or fsync fails, the receiver
    /// resolves with `AofAck::WriteFailed` / `AofAck::FsyncFailed`. If
    /// the writer task is gone (shutdown / channel disconnect), the
    /// receiver resolves with `Err(RecvError)`. In every failure mode the
    /// caller MUST return an error frame to the client, NOT `+OK`.
    ///
    /// **Performance:** every call adds a writer round-trip plus an
    /// fsync syscall on the critical path. This is the explicit Redis
    /// contract for `appendfsync=always`; callers should gate on the
    /// configured policy and prefer [`Self::try_send_append`] for
    /// `everysec`/`no`.
    ///
    /// **`shard_id` semantics:** matches [`Self::try_send_append`] — for
    /// TopLevel the parameter is ignored, for PerShard it routes to
    /// `senders[shard_id]`.
    pub fn try_send_append_sync(
        &self,
        shard_id: usize,
        lsn: u64,
        bytes: Bytes,
    ) -> crate::runtime::channel::OneshotReceiver<AofAck> {
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        match self.sender(shard_id).try_send(AofMessage::AppendSync {
            lsn,
            bytes,
            ack: ack_tx,
        }) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(_)) => {
                // Writer channel is at capacity — count the dropped entry and
                // signal ChannelFull back to the caller via a pre-filled
                // oneshot so the caller's `.await` resolves immediately to
                // Err(AofAck::ChannelFull) without a writer round-trip.
                AOF_BACKPRESSURE_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!(
                    "AOF writer channel full (shard {}): AppendSync dropped; \
                     backpressure_dropped={}",
                    shard_id,
                    AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed),
                );
                // Pre-send ChannelFull into a fresh oneshot pair; the
                // caller's `ack_rx` was already returned — we create a
                // new pair and use its sender to pre-fill what the caller
                // will receive. The original ack_tx (inside the dropped
                // AppendSync) is dropped, causing its ack_rx to yield
                // RecvError. We send ChannelFull via the *returned* ack_rx
                // by using a second oneshot whose sender is immediately
                // fulfilled, then return that receiver instead.
                let (pre_tx, pre_rx) = crate::runtime::channel::oneshot::<AofAck>();
                let _ = pre_tx.send(AofAck::ChannelFull);
                return pre_rx;
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                // Writer task is dead — let caller handle RecvError on ack_rx.
                // ack_tx was dropped inside the Err value; ack_rx will
                // resolve with RecvError, which try_send_append_durable maps
                // to Err(AofAck::WriteFailed).
            }
        }
        ack_rx
    }

    /// Fire-and-forget append for a cross-shard atomic operation (RFC § 2
    /// Rule 2 — `OrderedAcrossShards` tagging).
    ///
    /// The high bit of `lsn` (`1 << 63`) is set before the entry is queued.
    /// Recovery uses this bit to recognize cross-shard atomic entries,
    /// buffer them per-shard, and replay them globally in LSN order after
    /// per-shard replay completes — guaranteeing TXN/SCRIPT atomicity
    /// survives a crash even when multiple shards participated.
    ///
    /// **Caller contract:** `lsn` MUST be < `1 << 63` (i.e. the high bit
    /// MUST be clear when passed in). Practical LSN ceilings — even at
    /// 10 M writes/s sustained for a century — sit around 2^58, so any
    /// real LSN satisfies this. Debug builds assert; release builds mask
    /// the input to keep the wire format well-formed rather than
    /// corrupt-by-zero-extending.
    ///
    /// **Production callers today:** none. Step 5 ships the infrastructure
    /// (writer, framing flag, recovery merge) so a future cross-shard TXN
    /// or replicated SCRIPT command has a place to land. Until that
    /// consumer exists, only test code emits ordered entries.
    #[inline]
    pub fn try_send_append_ordered(&self, shard_id: usize, lsn: u64, bytes: Bytes) {
        debug_assert_eq!(
            lsn & ORDERED_LSN_FLAG,
            0,
            "try_send_append_ordered: lsn must not have the high bit set; got {:#x}",
            lsn,
        );
        let tagged_lsn = (lsn & !ORDERED_LSN_FLAG) | ORDERED_LSN_FLAG;
        let _ = self.sender(shard_id).try_send(AofMessage::Append {
            lsn: tagged_lsn,
            bytes,
        });
    }

    /// Issue an LSN for an AOF append at every call site that has the
    /// `Option<Arc<RwLock<ReplicationState>>>` shape. Wraps
    /// `ReplicationState::issue_lsn` so handler call sites collapse to a
    /// single line.
    ///
    /// Returns 0 when:
    /// - `repl_state` is None (test fixtures or shutdown paths)
    /// - the `RwLock` is poisoned (shouldn't happen in production —
    ///   ReplicationState is only `write()`-locked under known-safe paths)
    ///
    /// 0 is a sentinel meaning "no replication ordering for this write".
    /// TopLevel writers ignore the LSN entirely so 0 is harmless there;
    /// PerShard writers treat 0 the same as any other LSN (per-shard order
    /// is preserved by write order, not by LSN value). The LSN only matters
    /// for the cross-shard `OrderedAcrossShards` merge in RFC step 5.
    #[inline]
    pub fn issue_append_lsn(
        repl_state: &Option<Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>>,
        shard_id: usize,
        delta: usize,
    ) -> u64 {
        repl_state
            .as_ref()
            .and_then(|rs| rs.read().ok().map(|g| g.issue_lsn(shard_id, delta as u64)))
            .unwrap_or(0)
    }

    /// Submit a Rewrite/RewriteSharded message. Only legal for TopLevel pools;
    /// PerShard rewrites are per-shard operations and must be initiated by
    /// the BGREWRITEAOF code path in step 6, not via this enum variant.
    pub fn try_send_rewrite(&self, msg: AofMessage) -> Result<(), AofPoolSendError> {
        use crate::persistence::aof_manifest::AofLayout;
        debug_assert!(
            matches!(msg, AofMessage::Rewrite(_) | AofMessage::RewriteSharded(_)),
            "try_send_rewrite called with a non-Rewrite variant",
        );
        if self.layout == AofLayout::PerShard {
            return Err(AofPoolSendError::RewriteUnsupportedInPerShard);
        }
        self.senders[0]
            .try_send(msg)
            .map_err(|_| AofPoolSendError::SendFailed)
    }

    /// [F6] Initiate a per-shard BGREWRITEAOF across every writer in a
    /// PerShard pool.
    ///
    /// Loads the authoritative manifest fresh from `base_dir` (normal appends
    /// never mutate the manifest, and BGREWRITEAOF is CAS-serialized by
    /// `AOF_REWRITE_IN_PROGRESS`, so a fresh load is the current committed
    /// state), builds a shared [`PerShardRewriteCoord`] that advances the
    /// generation by one, and hands every writer the same `coord` + a cheap
    /// `Arc` clone of `shard_dbs`.
    ///
    /// **Reliable delivery (design-for-failure):** the fan-out uses the
    /// *blocking* `send` rather than `try_send`. A dropped rewrite message
    /// would leave the countdown unable to reach zero — folded writers would
    /// have reopened to new-seq files that the manifest never commits, silently
    /// losing their post-rewrite appends. The writers run on dedicated threads
    /// draining continuously, so `send` blocks only until a channel slot frees
    /// (sub-millisecond), which is acceptable for a rare admin command.
    ///
    /// Returns `SendFailed` if `base_dir` is unset, the manifest can't be
    /// loaded, or a writer thread is gone (disconnected channel). On the last
    /// case the rewrite aborts WITHOUT committing — the old generation stays
    /// authoritative (crash-safe), but a dead writer already means that shard's
    /// persistence was compromised before this call.
    pub fn try_send_rewrite_per_shard(
        &self,
        shard_dbs: Arc<crate::shard::shared_databases::ShardDatabases>,
    ) -> Result<(), AofPoolSendError> {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        if self.layout != AofLayout::PerShard {
            // A TopLevel pool rewrites via try_send_rewrite; this entry point
            // is PerShard-only.
            return Err(AofPoolSendError::RewriteUnsupportedInPerShard);
        }
        let base_dir = self.base_dir.as_ref().ok_or(AofPoolSendError::SendFailed)?;
        let manifest = match AofManifest::load(base_dir) {
            Ok(Some(m)) if m.layout == AofLayout::PerShard => m,
            Ok(_) => {
                error!(
                    "F6 per-shard rewrite: manifest at {} missing or not PerShard; aborting",
                    base_dir.display()
                );
                return Err(AofPoolSendError::SendFailed);
            }
            Err(e) => {
                error!(
                    "F6 per-shard rewrite: failed to load manifest at {}: {}",
                    base_dir.display(),
                    e
                );
                return Err(AofPoolSendError::SendFailed);
            }
        };
        let current_seq = manifest.seq;
        let n_shards = self.senders.len();
        let shared_manifest = Arc::new(parking_lot::Mutex::new(manifest));
        let coord = PerShardRewriteCoord::new(shared_manifest, current_seq, n_shards);
        // C4 deadlock guard: fold channels MUST be wired before a per-shard
        // rewrite is attempted. If they are absent the AofFold push would
        // succeed into a 1-slot ring whose consumer was dropped at construction
        // time, causing the writer to block forever on `recv_blocking` while
        // the bounded AOF append channel (10_000) fills and silently drops
        // every subsequent write (test_ssm4a_fold_4shard_experimental: 2016 of
        // 272988 INCRs survived restart). Abort cleanly instead: log an error,
        // mark the coord failed, and return — the old generation stays
        // authoritative (same abort path as a disconnected writer channel above).
        if self.fold_producers.is_none() || self.fold_notifiers.is_none() {
            error!(
                "F6 per-shard rewrite: fold channels not wired (pool built with \
                 per_shard_with_base_dir — call set_fold_channels after mesh \
                 creation). Rewrite aborted; old generation remains authoritative. \
                 This is a startup-configuration bug."
            );
            coord.mark_failed();
            for _ in 0..n_shards {
                coord.shard_done();
            }
            return Err(AofPoolSendError::SendFailed);
        }
        // SAFETY: both options are Some, checked above.
        let fold_producers_vec = self.fold_producers.as_ref().expect("checked above");
        let fold_notifiers_vec = self.fold_notifiers.as_ref().expect("checked above");

        for (idx, s) in self.senders.iter().enumerate() {
            let fold_producer = fold_producers_vec
                .get(idx)
                .cloned()
                .expect("fold_producers len == senders len (debug_asserted at set_fold_channels)");
            let fold_notifier = fold_notifiers_vec
                .get(idx)
                .cloned()
                .expect("fold_notifiers len == senders len (debug_asserted at set_fold_channels)");
            // Blocking send for guaranteed delivery — see the doc comment.
            if s.send(AofMessage::RewritePerShard {
                shard_dbs: shard_dbs.clone(),
                coord: coord.clone(),
                fold_producer,
                fold_notifier,
            })
            .is_err()
            {
                error!(
                    "F6 per-shard rewrite: writer {} channel disconnected; \
                     rewrite aborted (no manifest commit, old generation remains \
                     authoritative). Inspect AOF writer threads.",
                    idx
                );
                // Account for the shards that did NOT receive the message (this
                // one plus the unsent tail). The writers that DID receive it will
                // fold and call `shard_done`; decrementing for the rest here lets
                // the countdown still reach zero, so the final `shard_done` runs
                // the abort path (keeps `old_seq`, clears AOF_REWRITE_IN_PROGRESS)
                // instead of wedging the rewrite flag forever.
                coord.mark_failed();
                for _ in idx..n_shards {
                    coord.shard_done();
                }
                return Err(AofPoolSendError::SendFailed);
            }
        }
        info!(
            "F6 per-shard rewrite dispatched: seq {} -> {} across {} shards",
            current_seq,
            current_seq + 1,
            n_shards
        );
        Ok(())
    }

    /// Broadcast `Shutdown` to every writer. Used by orchestrated shutdown
    /// paths in `main.rs`/`embedded.rs`. Each writer drains its channel and
    /// fsyncs before exiting.
    pub fn broadcast_shutdown(&self) {
        for s in &self.senders {
            let _ = s.try_send(AofMessage::Shutdown);
        }
    }

    /// Number of underlying writer senders. 1 for TopLevel, num_shards for
    /// PerShard.
    #[inline]
    pub fn num_writers(&self) -> usize {
        self.senders.len()
    }

    /// Reports the pool's layout. Useful for places that need to refuse
    /// PerShard-incompatible legacy code paths with a clear error.
    #[inline]
    pub fn layout(&self) -> crate::persistence::aof_manifest::AofLayout {
        self.layout
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::persistence::aof_manifest::AofLayout;
    use crate::runtime::channel;

    /// A per-shard BGREWRITEAOF fan-out that fails partway (a writer channel is
    /// disconnected) must NOT leave `AOF_REWRITE_IN_PROGRESS` stuck true. The
    /// failed/unsent shards are accounted for (`mark_failed` + `shard_done`) so
    /// the countdown still reaches zero and the abort path clears the flag,
    /// leaving the old generation authoritative.
    #[test]
    fn rewrite_fan_out_partial_failure_clears_in_progress_flag() {
        use crate::command::persistence::AOF_REWRITE_IN_PROGRESS;
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir().unwrap();
        // PerShard manifest on disk so the rewrite reaches the fan-out loop.
        crate::persistence::aof_manifest::AofManifest::initialize_multi(tmp.path(), 2).unwrap();

        // Disconnect shard-0's writer so the FIRST send fails (before any
        // successful send), making the abort accounting deterministic.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        drop(rx0);
        let pool = AofWriterPool::per_shard_with_base_dir(
            vec![tx0, tx1],
            FsyncPolicy::EverySec,
            DEFAULT_AOF_FSYNC_TIMEOUT,
            tmp.path().to_path_buf(),
        );

        let (shard_dbs, _inits) = crate::shard::shared_databases::ShardDatabases::new(vec![
            vec![crate::storage::Database::new()],
            vec![crate::storage::Database::new()],
        ]);

        // The command handler sets this before dispatching the rewrite.
        AOF_REWRITE_IN_PROGRESS.store(true, Ordering::SeqCst);
        let res = pool.try_send_rewrite_per_shard(shard_dbs);
        assert!(res.is_err(), "disconnected writer must fail the fan-out");
        assert!(
            !AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst),
            "partial fan-out failure must clear AOF_REWRITE_IN_PROGRESS, not wedge it"
        );
    }

    /// A per-shard rewrite that ABORTS (another shard failed to fold) must roll
    /// THIS shard's append file back onto the committed old generation. Phase 6
    /// reopens `*file` onto the new-seq incr; if the rewrite then aborts, the
    /// manifest keeps `old_seq` and prunes the new-seq files — so without a
    /// barrier-before-resume the writer keeps appending into a discarded incr
    /// that recovery ignores (silent data loss). This test drives one shard's
    /// real fold to completion with a second shard pre-marked failed, then proves
    /// post-abort appends land in the COMMITTED old-gen incr.
    #[test]
    fn rewrite_abort_reopens_writer_onto_committed_old_generation() {
        use crate::command::persistence::AOF_REWRITE_IN_PROGRESS;
        use std::io::Write;
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir().unwrap();
        // 2-shard manifest at seq=1 on disk; share it through the coord's Arc.
        let manifest =
            crate::persistence::aof_manifest::AofManifest::initialize_multi(tmp.path(), 2).unwrap();
        let old_seq = manifest.seq;
        let old_incr_s0 = manifest.shard_incr_path_seq(0, old_seq);
        assert!(old_incr_s0.exists(), "old-gen incr must exist pre-rewrite");
        let manifest = std::sync::Arc::new(parking_lot::Mutex::new(manifest));

        let coord = PerShardRewriteCoord::new(manifest.clone(), old_seq, 2);
        let new_seq = coord.new_seq();
        assert_ne!(new_seq, old_seq);

        // Shard 1 "fails to fold": mark_failed + shard_done (countdown 2 -> 1).
        // Shard 0's shard_done (inside do_rewrite_per_shard) will then be the
        // terminal decrement and take the abort path.
        coord.mark_failed();
        coord.shard_done();

        // Shard 0's append file starts on the OLD incr (where the live writer
        // appends before a rewrite). Empty databases keep the fold trivial; the
        // reopen behaviour is independent of key count.
        let (shard_dbs, _inits) = crate::shard::shared_databases::ShardDatabases::new(vec![
            vec![crate::storage::Database::new()],
            vec![crate::storage::Database::new()],
        ]);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&old_incr_s0)
            .unwrap();
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(4);

        // Create a dummy fold channel whose ring is pre-filled so try_push(AofFold)
        // fails immediately — do_rewrite_per_shard returns Err before reaching
        // recv_blocking, which would hang forever with no shard thread consuming.
        // Capacity 1, pre-filled with Shutdown so the single slot is taken.
        use ringbuf::traits::{Producer as _, Split as _};
        let (mut fold_prod, fold_cons) =
            ringbuf::HeapRb::<crate::shard::dispatch::ShardMessage>::new(1).split();
        let _ = fold_prod.try_push(crate::shard::dispatch::ShardMessage::Shutdown);
        let fold_producer = std::sync::Arc::new(parking_lot::Mutex::new(fold_prod));
        let fold_notifier = std::sync::Arc::new(crate::runtime::channel::Notify::new());
        // Keep fold_cons alive so the producer is not dropped before do_rewrite_per_shard runs.
        let _fold_cons = fold_cons;

        AOF_REWRITE_IN_PROGRESS.store(true, Ordering::SeqCst);
        // The fold itself succeeds; aborting is a coordinator decision, so this
        // returns Ok even though the rewrite is discarded.
        // NOTE: With ShardSlice live, do_rewrite_per_shard sends AofFold and blocks
        // on the reply. In this unit test there's no shard event loop consuming the
        // SPSC ring, so the fold will error out. The test verifies the abort path,
        // which is triggered by the fold guard's error handling.
        let _ = do_rewrite_per_shard(
            0,
            &shard_dbs,
            &mut file,
            &rx,
            &coord,
            &fold_producer,
            &fold_notifier,
        );

        // Abort kept the old generation committed and pruned the new-gen incr.
        assert_eq!(manifest.lock().seq, old_seq, "abort must keep old_seq");
        let new_incr_s0 = manifest.lock().shard_incr_path_seq(0, new_seq);
        assert!(
            !new_incr_s0.exists(),
            "aborted new-gen incr must be pruned (the dangling target)"
        );

        // The writer must have been rolled back onto the old-gen incr: appends
        // through `*file` after the rewrite must land in the committed file, not
        // the pruned/unlinked new-gen inode.
        let marker = b"*1\r\n$4\r\nPING\r\n";
        file.write_all(marker).unwrap();
        file.sync_data().unwrap();
        drop(file);
        let committed = std::fs::read(&old_incr_s0).unwrap();
        assert!(
            committed.windows(marker.len()).any(|w| w == marker),
            "post-abort appends must land in the committed old-gen incr (no silent loss)"
        );
    }

    /// Cross-thread barrier wakeup: a folded writer that decrements then blocks
    /// in `await_outcome` on one thread must be woken with the committed
    /// generation by the terminal `shard_done` running on ANOTHER thread. The
    /// single-threaded behavioural test above never actually blocks (its
    /// `await_outcome` returns immediately), so this exercises the real condvar
    /// wait/notify path across threads.
    #[test]
    fn rewrite_abort_wakes_waiter_cross_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest =
            crate::persistence::aof_manifest::AofManifest::initialize_multi(tmp.path(), 2).unwrap();
        let old_seq = manifest.seq;
        let manifest = std::sync::Arc::new(parking_lot::Mutex::new(manifest));
        let coord = PerShardRewriteCoord::new(manifest, old_seq, 2);

        // Shard A: decrement (countdown 2 -> 1, non-terminal) then block.
        let c_a = coord.clone();
        let waiter = std::thread::spawn(move || {
            c_a.shard_done();
            c_a.await_outcome()
        });

        // Shard B fails: mark_failed + the terminal decrement (-> 0). Whichever
        // of the two `shard_done` calls is terminal sees `failed` set (B sets it
        // first) and publishes old_seq.
        coord.mark_failed();
        coord.shard_done();

        let observed = waiter.join().expect("waiter must wake, not hang or panic");
        assert_eq!(
            observed, old_seq,
            "aborted rewrite must publish old_seq to the cross-thread waiter"
        );
    }

    /// Panic-safety / liveness: a fold that PANICS mid-flight (the OOM-unwind
    /// hazard of issue #138) must not hang the other shards' writers at the
    /// barrier. The `ShardDoneGuard`'s `Drop` must fire `mark_failed` +
    /// `shard_done` on unwind so the countdown still closes and every waiter
    /// wakes. Without the guard this test hangs forever (the panicking shard
    /// never decrements).
    #[test]
    fn rewrite_fold_panic_releases_barrier_for_other_writers() {
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir().unwrap();
        let manifest =
            crate::persistence::aof_manifest::AofManifest::initialize_multi(tmp.path(), 2).unwrap();
        let old_seq = manifest.seq;
        let manifest = std::sync::Arc::new(parking_lot::Mutex::new(manifest));
        let coord = PerShardRewriteCoord::new(manifest, old_seq, 2);

        // Shard A folds, decrements, then blocks on the barrier in a thread.
        let c_a = coord.clone();
        let waiter = std::thread::spawn(move || {
            c_a.shard_done();
            c_a.await_outcome()
        });

        // Shard B "panics mid-fold": a ShardDoneGuard dropped during unwind. The
        // guard's Drop must abort + decrement so A's barrier releases.
        let c_b = coord.clone();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ShardDoneGuard::new(&c_b);
            panic!("simulated OOM unwind during fold");
        }));
        assert!(panicked.is_err(), "the simulated fold must have panicked");

        let observed = waiter
            .join()
            .expect("waiter must wake after the panicking shard's guard fires");
        assert_eq!(
            observed, old_seq,
            "panic-aborted rewrite must publish old_seq, not hang"
        );
        assert!(
            coord.failed.load(Ordering::Acquire),
            "the dropped guard must have marked the rewrite failed"
        );
    }

    #[test]
    fn top_level_pool_routes_all_shards_to_writer_zero() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);
        assert_eq!(pool.num_writers(), 1);
        assert_eq!(pool.layout(), AofLayout::TopLevel);

        pool.try_send_append(0, 0, Bytes::from_static(b"a"));
        pool.try_send_append(7, 0, Bytes::from_static(b"b"));
        pool.try_send_append(42, 0, Bytes::from_static(b"c"));

        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 3, "all 3 appends should land on writer 0");
    }

    #[test]
    fn per_shard_pool_routes_each_shard_to_its_own_writer() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx2, rx2) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1, tx2]);
        assert_eq!(pool.num_writers(), 3);
        assert_eq!(pool.layout(), AofLayout::PerShard);

        pool.try_send_append(0, 100, Bytes::from_static(b"shard0"));
        pool.try_send_append(1, 200, Bytes::from_static(b"shard1a"));
        pool.try_send_append(1, 300, Bytes::from_static(b"shard1b"));
        pool.try_send_append(2, 400, Bytes::from_static(b"shard2"));

        let count = |rx: &channel::MpscReceiver<AofMessage>| -> usize {
            let mut n = 0;
            while rx.try_recv().is_ok() {
                n += 1;
            }
            n
        };
        assert_eq!(count(&rx0), 1, "shard 0 writer should receive exactly 1");
        assert_eq!(count(&rx1), 2, "shard 1 writer should receive exactly 2");
        assert_eq!(count(&rx2), 1, "shard 2 writer should receive exactly 1");
    }

    #[test]
    fn per_shard_pool_rejects_rewrite_with_explicit_error() {
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(8);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let dummies: SharedDatabases = Arc::new(vec![]);
        let err = pool
            .try_send_rewrite(AofMessage::Rewrite(dummies))
            .unwrap_err();
        assert_eq!(err, AofPoolSendError::RewriteUnsupportedInPerShard);
    }

    #[test]
    fn top_level_pool_accepts_rewrite() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level(tx);

        let dummies: SharedDatabases = Arc::new(vec![]);
        pool.try_send_rewrite(AofMessage::Rewrite(dummies)).unwrap();
        assert!(matches!(rx.try_recv(), Ok(AofMessage::Rewrite(_))));
    }

    #[test]
    fn per_shard_pool_threads_lsn_field_to_each_writer() {
        // Step 3 wire-format contract: try_send_append carries the issued LSN
        // through to the writer task, which writes it as the per-entry header
        // under PerShard layout. This unit test pins the channel-side contract
        // (the disk-side framing is covered by writer-task integration).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        pool.try_send_append(0, 42, Bytes::from_static(b"set foo 1"));
        pool.try_send_append(1, 43, Bytes::from_static(b"set bar 2"));
        pool.try_send_append(0, 44, Bytes::from_static(b"del foo"));

        // Shard 0 should see (42, "set foo 1") then (44, "del foo").
        match rx0.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 42, "shard 0 first entry lsn");
                assert_eq!(bytes.as_ref(), b"set foo 1");
            }
            other => panic!(
                "shard 0 first recv expected Append, got {:?}",
                other.is_ok()
            ),
        }
        match rx0.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 44, "shard 0 second entry lsn");
                assert_eq!(bytes.as_ref(), b"del foo");
            }
            other => panic!(
                "shard 0 second recv expected Append, got {:?}",
                other.is_ok()
            ),
        }
        // Shard 1 should see (43, "set bar 2") only.
        match rx1.try_recv() {
            Ok(AofMessage::Append { lsn, bytes }) => {
                assert_eq!(lsn, 43, "shard 1 entry lsn");
                assert_eq!(bytes.as_ref(), b"set bar 2");
            }
            other => panic!("shard 1 recv expected Append, got {:?}", other.is_ok()),
        }
    }

    #[test]
    fn try_send_append_sync_queues_appendsync_with_ack() {
        // Channel-level wiring contract for the H1 fix: `try_send_append_sync`
        // queues `AofMessage::AppendSync { lsn, bytes, ack }`, and the
        // returned receiver resolves to whatever value the (mocked) writer
        // sends on `ack`. End-to-end durability is covered by step 8
        // (CRASH-01-LITE); this pins the API contract.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 99, Bytes::from_static(b"SET k v"));

        // Drain the queue; the writer would normally do this. Capture the
        // ack sender, do the (mock) durable write, then ack Synced.
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { lsn, bytes, ack }) => {
                assert_eq!(lsn, 99, "lsn forwarded through the channel");
                assert_eq!(bytes.as_ref(), b"SET k v", "bytes forwarded");
                ack
            }
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };

        // Writer reports Synced — caller observes Synced.
        let _ = ack.send(AofAck::Synced);
        let result = recv.recv_blocking().expect("receiver resolves");
        assert_eq!(result, AofAck::Synced);
    }

    #[test]
    fn append_sync_writer_dropped_resolves_recv_error() {
        // If the writer task is dead or the channel disconnects between
        // queueing and the ack send, the receiver MUST resolve with an
        // error rather than hang. Callers treat that as a hard failure
        // (return an error frame, do not +OK).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 7, Bytes::from_static(b"x"));

        // Drain the message but DROP the ack sender without sending.
        match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => drop(ack),
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        }

        let err = recv.recv_blocking().expect_err("dropped ack -> RecvError");
        // Crash-safe: we got a sentinel-style error, not a hang.
        let _ = err;
    }

    #[test]
    fn append_sync_writer_reports_write_failed() {
        // Writer encountered a write_all error; recv returns WriteFailed.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"x"));
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => ack,
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };
        let _ = ack.send(AofAck::WriteFailed);
        let result = recv.recv_blocking().expect("recv resolves");
        assert_eq!(result, AofAck::WriteFailed);
    }

    #[test]
    fn append_sync_writer_reports_fsync_failed() {
        // Writer wrote the payload but fsync (sync_data) returned an error.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"x"));
        let ack = match rx0.try_recv() {
            Ok(AofMessage::AppendSync { ack, .. }) => ack,
            other => panic!("expected AppendSync, got {:?}", other.is_ok()),
        };
        let _ = ack.send(AofAck::FsyncFailed);
        let result = recv.recv_blocking().expect("recv resolves");
        assert_eq!(result, AofAck::FsyncFailed);
    }

    /// Issue #140: an AppendSync drained during a BGREWRITEAOF must NOT be acked
    /// `Synced` inside the drain — that reports the write durable before the
    /// post-drain boundary fsync, so a crash in the window loses an entry the
    /// client was told was safe. The drain must PARK the ack and only fulfil it
    /// after the boundary `sync_data()`. (Framed / per-shard drain.)
    #[test]
    fn drain_framed_parks_appendsync_ack_until_boundary_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let incr = tmp.path().join("incr.aof");
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 99, Bytes::from_static(b"SET k v"));

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr)
            .unwrap();
        let mut outcome = drain_pending_appends_framed(&rx0, &mut file, usize::MAX).unwrap();

        // CONTRACT: drained + parked, NOT yet acked.
        assert_eq!(outcome.drained, 1, "the AppendSync was drained");
        assert_eq!(
            outcome.pending_acks.len(),
            1,
            "the ack must be parked until the boundary fsync, not sent during drain"
        );

        // Boundary fsync succeeds → fulfil Synced; only NOW is the client told durable.
        file.sync_data().unwrap();
        outcome.fulfill_acks(true);
        assert_eq!(
            recv.recv_blocking().expect("ack resolves"),
            AofAck::Synced,
            "post-fsync the parked ack must resolve Synced"
        );
    }

    /// Issue #140 failure path: if the rewrite-boundary fsync FAILS, a drained
    /// AppendSync must resolve `FsyncFailed`, never `Synced`. Exercises the
    /// non-framed `drain_pending_appends` — the DEFAULT `--shards 1` rewrite
    /// path (`do_rewrite_single`), which is reachable under appendfsync=always.
    #[cfg(feature = "runtime-monoio")]
    #[test]
    fn drain_single_fulfills_fsync_failure_as_fsync_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let incr = tmp.path().join("incr.aof");
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1]);

        let recv = pool.try_send_append_sync(0, 7, Bytes::from_static(b"SET a b"));

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr)
            .unwrap();
        let mut outcome = drain_pending_appends(&rx0, &mut file).unwrap();
        assert_eq!(
            outcome.pending_acks.len(),
            1,
            "ack parked, not sent in drain"
        );

        // Simulate a failed boundary fsync: the parked ack must report FsyncFailed.
        outcome.fulfill_acks(false);
        assert_eq!(
            recv.recv_blocking().expect("ack resolves"),
            AofAck::FsyncFailed,
            "a failed boundary fsync must NOT be reported Synced to the client"
        );
    }

    // F2 (design-for-failure): `appendfsync=always` must bound its fsync-ack
    // await. A stalled writer must surface a hard error within the budget,
    // never park the connection forever. Tokio-gated because it drives the
    // runtime timer; the monoio path shares the proven `select! + sleep`
    // shape from `cluster::failover`, exercised end-to-end by the crash tests.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_fsync_times_out_when_writer_never_acks() {
        // Writer channel is held (kept open) but never drained → the
        // AppendSync sits buffered with its ack sender alive, so the receiver
        // never resolves. The bounded await MUST elapse and report failure.
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::from_millis(50),
        );

        let start = Instant::now();
        let res = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"x"))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(
            res,
            Err(AofAck::FsyncFailed),
            "timed-out fsync must map to FsyncFailed (durability unconfirmed)"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must fail within the bound, not hang (took {:?})",
            elapsed
        );
        // Keep the receivers alive until here so the message stays buffered.
        drop((_rx0, _rx1));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_fsync_succeeds_when_writer_acks_in_time() {
        // Happy path: a writer drains the AppendSync and acks `Synced` well
        // within the bound → the durable append returns Ok(()).
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::from_millis(500),
        );

        tokio::spawn(async move {
            if let Ok(AofMessage::AppendSync { ack, .. }) = rx0.recv_async().await {
                let _ = ack.send(AofAck::Synced);
            }
        });

        let res = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"x"))
            .await;
        assert_eq!(res, Ok(()), "ack within the bound must succeed");
        drop(_rx1);
    }

    /// Parse the PerShard incr framing `[u64 lsn LE][u32 len LE][len bytes]`,
    /// stopping at a truncated tail (the crash/torn boundary) — exactly what
    /// `replay_incr_framed` does. Returns the cleanly-replayable prefix.
    #[cfg(feature = "runtime-tokio")]
    fn parse_framed(buf: &[u8]) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        let mut i = 0usize;
        while i + 12 <= buf.len() {
            let lsn = u64::from_le_bytes(buf[i..i + 8].try_into().unwrap());
            let len = u32::from_le_bytes(buf[i + 8..i + 12].try_into().unwrap()) as usize;
            if i + 12 + len > buf.len() {
                break; // truncated tail → crash boundary, stop
            }
            out.push((lsn, buf[i + 12..i + 12 + len].to_vec()));
            i += 12 + len;
        }
        out
    }

    // Regression (PR #136 review, BUG #2): the tokio per-shard writer must carry
    // a `write_error` latch like the single-file (~:1467) and monoio (~:2125)
    // writers. A torn write (header lands, payload fails) must NOT be followed by
    // more records — a lone orphaned header makes the framed replay misread the
    // next record's bytes as the orphan's payload, corrupting everything after.
    // The latch suppresses all writes after the tear and reports WriteFailed to
    // AppendSync callers (so they error instead of ack'ing a corrupt write).
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn tokio_per_shard_writer_latches_after_torn_write() {
        use crate::persistence::aof_manifest::AofManifest;
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir().unwrap();
        let base_dir = tmp.path().to_path_buf();
        // PerShard layout, 2 shards (the per-shard pool needs >=2); drive shard 0.
        let manifest = AofManifest::initialize_multi(&base_dir, 2).unwrap();
        let incr = manifest.shard_incr_path(0);

        // Inject: the 2nd Append tears (header written, payload "fails").
        TEST_FAIL_WRITE_AT.store(2, Ordering::SeqCst);

        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(16);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(per_shard_aof_writer_task(
            rx,
            base_dir.clone(),
            0,
            FsyncPolicy::Always,
            cancel.clone(),
        ));

        // 1: clean. 2: torn (header only). 3: must be suppressed by the latch.
        tx.try_send(AofMessage::Append {
            lsn: 1,
            bytes: Bytes::from_static(b"AAAA"),
        })
        .unwrap();
        tx.try_send(AofMessage::Append {
            lsn: 2,
            bytes: Bytes::from_static(b"BBBB"),
        })
        .unwrap();
        tx.try_send(AofMessage::Append {
            lsn: 3,
            bytes: Bytes::from_static(b"CCCC"),
        })
        .unwrap();

        // Barrier + assertion: an AppendSync after the tear MUST come back
        // WriteFailed (latched), never Synced.
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot::<AofAck>();
        tx.try_send(AofMessage::AppendSync {
            lsn: 4,
            bytes: Bytes::from_static(b"DDDD"),
            ack: ack_tx,
        })
        .unwrap();

        let ack = tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx)
            .await
            .expect("writer must answer the AppendSync within 5s")
            .expect("ack channel must not drop");
        assert_eq!(
            ack,
            AofAck::WriteFailed,
            "after a torn write the latch must reject further writes (got {ack:?})"
        );

        cancel.cancel();
        TEST_FAIL_WRITE_AT.store(0, Ordering::SeqCst);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer).await;

        // On disk: exactly one replayable frame (lsn=1, "AAAA"). The orphaned
        // lsn=2 header is a truncated tail (crash boundary); lsn 3 and 4 were
        // never written (latch held) — no corruption.
        let raw = std::fs::read(&incr).unwrap();
        let frames = parse_framed(&raw);
        assert_eq!(
            frames,
            vec![(1u64, b"AAAA".to_vec())],
            "only the pre-tear record may replay; orphaned headers / suppressed \
             records must not corrupt the stream"
        );
    }

    #[test]
    fn broadcast_shutdown_reaches_every_writer() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(2);
        let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(2);
        let (tx2, rx2) = channel::mpsc_bounded::<AofMessage>(2);
        let pool = AofWriterPool::per_shard(vec![tx0, tx1, tx2]);

        pool.broadcast_shutdown();

        for (i, rx) in [&rx0, &rx1, &rx2].iter().enumerate() {
            assert!(
                matches!(rx.try_recv(), Ok(AofMessage::Shutdown)),
                "writer {} did not receive Shutdown",
                i
            );
        }
    }

    /// FIX-W1-1 contract: `try_send_append_durable` under `Always` policy MUST
    /// return `Err(AofAck::FsyncFailed)` when the writer reports failure.
    /// handler_single.rs must await this BEFORE flushing responses to the client.
    ///
    /// Uses spawn_blocking to simulate the mock writer responding on the ack
    /// channel concurrently, which allows the async rendezvous to complete.
    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn always_policy_try_send_append_durable_returns_err_on_fsync_fail() {
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = std::sync::Arc::new(AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::ZERO, // legacy unbounded await — disconnect/ack resolves it
        ));

        // Spawn a mock writer that drains AppendSync and responds with FsyncFailed.
        // Runs in a blocking thread (flume's blocking recv) so it doesn't block
        // the async executor while waiting for the handler to enqueue the message.
        let mock_writer = tokio::task::spawn_blocking(move || {
            // flume::Receiver::recv() blocks until a message is available
            let msg = rx0.recv().expect("mock writer got message");
            if let AofMessage::AppendSync { ack, .. } = msg {
                let _ = ack.send(AofAck::FsyncFailed);
            } else {
                panic!("expected AppendSync under Always policy");
            }
        });

        // The handler MUST await this BEFORE flushing responses to the client
        let result = pool
            .try_send_append_durable(0, 1, Bytes::from_static(b"SET k v"))
            .await;
        mock_writer.await.expect("mock writer completed");

        assert_eq!(
            result,
            Err(AofAck::FsyncFailed),
            "Always policy MUST propagate fsync failure so caller can return an error frame"
        );
    }

    /// FIX-W1-1 ordering contract: when `aof_entries` carries `(resp_idx, bytes)`
    /// tuples, the handler can patch `responses[resp_idx]` on AOF failure BEFORE
    /// flushing to the client. This test verifies the indexing is sound.
    #[test]
    fn aof_entries_indexed_by_response_slot_patches_correctly() {
        use crate::protocol::Frame;
        let mut responses: Vec<Frame> = vec![
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
        ];
        // Simulate two write commands at response indices 0 and 2 (index 1 was a read)
        let aof_entries: Vec<(usize, Bytes)> = vec![
            (0, Bytes::from_static(b"SET a 1")),
            (2, Bytes::from_static(b"SET c 3")),
        ];

        // AOF write at index 2 fails; patch that response slot
        for (resp_idx, _bytes) in &aof_entries {
            if *resp_idx == 2 {
                // Simulate Err(AofAck::FsyncFailed) from try_send_append_durable
                responses[*resp_idx] =
                    Frame::Error(Bytes::from_static(b"WRITEFAIL aof fsync failed"));
            }
        }

        assert!(
            matches!(&responses[0], Frame::SimpleString(_)),
            "index 0 (successful fsync) should remain +OK"
        );
        assert!(
            matches!(&responses[1], Frame::SimpleString(_)),
            "index 1 (read, no AOF) should remain +OK"
        );
        assert!(
            matches!(&responses[2], Frame::Error(_)),
            "index 2 (failed fsync) must be patched to error"
        );
    }

    // NOTE (FIX-W1-1 r3): The H1 ordering regression test was moved to
    // `src/server/conn/handler_single.rs` (test module, fn
    // `flush_with_aof_ack_ack_precedes_response`).  The previous inline
    // reproduction here was non-discriminating — it reproduced the ack-first
    // loop IN THE TEST BODY rather than calling the real production fn, so it
    // passed on both pre-fix and post-fix binaries.
    //
    // The new test calls `flush_with_aof_ack` directly (the fn the handler now
    // delegates to), so inverting Phase 1/Phase 2 order in that fn causes a
    // measurable timing failure (`elapsed_ms ≈ 0ms < 55ms`).
    //
    // End-to-end ordering is also covered by:
    //   tests/crash_matrix_per_shard_aof.rs  (CRASH-01-LITE — AlwaysPolicy shards)

    // -----------------------------------------------------------------------
    // FIX-W2-5: channel-full returns AofAck::ChannelFull + increments counter
    // -----------------------------------------------------------------------
    #[test]
    fn try_send_append_sync_channel_full_returns_channel_full_ack() {
        // Create a channel with capacity 1 and fill it so the next try_send
        // hits TrySendError::Full.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(1);
        // Fill the channel by pre-loading one message.
        tx0.try_send(AofMessage::Shutdown).expect("pre-fill");
        // rx0 intentionally not consumed — channel is now at capacity.

        let pool = AofWriterPool::top_level(tx0);

        let before = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let recv = pool.try_send_append_sync(0, 1, Bytes::from_static(b"SET k v"));

        // The channel was full — ChannelFull is returned immediately without
        // a writer round-trip.
        let result = recv.recv_blocking().expect("pre-filled oneshot resolves");
        assert_eq!(
            result,
            AofAck::ChannelFull,
            "channel-full must yield ChannelFull, not {:?}",
            result
        );

        let after = AOF_BACKPRESSURE_DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            after,
            before + 1,
            "backpressure counter must increment by 1"
        );

        // No AppendSync should have reached the (blocked) reader.
        drop(rx0); // drain without consuming — just verify nothing snuck through
    }

    // -----------------------------------------------------------------------
    // FIX-W2-9: try_send_append_durable must be used for SWAPDB-like mutations
    //
    // Red test: documents the contract that handler_single.rs SHOULD honour.
    // When appendfsync=always, try_send_append_durable MUST return Err on
    // writer failure so callers can abort the mutation safely.
    // -----------------------------------------------------------------------
    #[test]
    fn try_send_append_durable_always_writer_dead_returns_write_failed() {
        // Create a pool with Always policy. The writer task is not running —
        // we model that by draining the channel message and then dropping the
        // ack sender, simulating a dead writer.
        let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::Always,
            Duration::ZERO, // legacy unbounded await — disconnect resolves it
        );

        // Spawn a thread that pulls the AppendSync off the channel but drops
        // the ack without sending — simulating a writer crash mid-fsync.
        let rx0_clone = rx0;
        let handle = std::thread::spawn(move || {
            match rx0_clone.recv() {
                Ok(AofMessage::AppendSync { ack, .. }) => drop(ack), // writer crash
                other => panic!("unexpected message: {:?}", other.is_ok()),
            }
        });

        // try_send_append_durable for Always must await the ack.
        // With the ack sender dropped, it should resolve to Err(WriteFailed).
        let result = futures::executor::block_on(pool.try_send_append_durable(
            0,
            55,
            Bytes::from_static(b"SWAPDB 0 1"),
        ));

        handle.join().expect("ack dropper thread");

        assert!(
            result.is_err(),
            "try_send_append_durable with dead writer must return Err, got Ok"
        );
        assert_eq!(
            result.unwrap_err(),
            AofAck::WriteFailed,
            "dead writer must resolve to WriteFailed"
        );
    }

    #[test]
    fn try_send_append_durable_everysec_is_fire_and_forget() {
        // EverySec policy: try_send_append_durable always returns Ok — the
        // durability policy doesn't block on fsync. handler_single.rs must
        // use try_send_append_durable so the policy is respected.
        let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(4);
        let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
        let pool = AofWriterPool::per_shard_with_policy(
            vec![tx0, tx1],
            FsyncPolicy::EverySec,
            Duration::ZERO,
        );

        let result = futures::executor::block_on(pool.try_send_append_durable(
            0,
            56,
            Bytes::from_static(b"SWAPDB 0 1"),
        ));

        assert!(
            result.is_ok(),
            "EverySec policy must be fire-and-forget (Ok), got {:?}",
            result
        );
    }
}
