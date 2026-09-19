pub mod client_cmd;
pub mod invalidation;
pub mod queue;

pub use queue::{InvalidationRx, InvalidationTx, invalidation_queue};

use crate::runtime::channel;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::protocol::Frame;

/// Number of clients with a registered invalidation channel, process-wide.
///
/// This is the write-path gate: every successful write checks
/// [`tracking_active`] (one relaxed load) before touching the shared
/// [`TrackingTable`] lock. With no tracking clients the KV hot path pays
/// a single atomic load and nothing else.
static ACTIVE_TRACKERS: AtomicUsize = AtomicUsize::new(0);

/// True when at least one connection has CLIENT TRACKING enabled.
#[inline]
pub fn tracking_active() -> bool {
    ACTIVE_TRACKERS.load(Ordering::Relaxed) > 0
}

/// The process-wide tracking table.
///
/// CLIENT TRACKING must be GLOBAL, not per-shard: a tracked read registers on
/// the reader connection's shard thread while the invalidating write can
/// execute on any other shard (or arrive over the SPSC mesh). Per-shard
/// tables silently dropped every cross-shard invalidation — the table is one
/// shared instance guarded by a mutex, gated off the hot path by
/// [`tracking_active`]. Invalidation senders are cross-thread-safe (flume),
/// so a write on shard A pushes directly into a connection's channel on
/// shard B; the connection's own event loop writes it to the socket.
pub fn global_table() -> std::sync::Arc<parking_lot::Mutex<TrackingTable>> {
    static GLOBAL: std::sync::OnceLock<std::sync::Arc<parking_lot::Mutex<TrackingTable>>> =
        std::sync::OnceLock::new();
    GLOBAL
        .get_or_init(|| std::sync::Arc::new(parking_lot::Mutex::new(TrackingTable::new())))
        .clone()
}

/// Per-client tracking configuration.
#[derive(Debug, Clone, Default)]
pub struct TrackingState {
    pub enabled: bool,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    pub redirect: Option<u64>,
    /// BCAST prefixes, sorted and de-duplicated (redis keeps them in a radix
    /// tree, which is the order `CLIENT TRACKINGINFO` reports).
    pub prefixes: Vec<Bytes>,
    pub invalidation_tx: Option<InvalidationTx>,
    /// `CLIENT CACHING yes|no` was given (redis `CLIENT_TRACKING_CACHING`):
    /// under OPTIN the next command's reads ARE tracked, under OPTOUT they are
    /// NOT. It covers the next command only — or the whole next transaction —
    /// see [`TrackingState::before_command`].
    pub caching: bool,
    /// The command before the one now executing was a `CLIENT` command.
    /// Redis clears the CACHING flag after every command except `CLIENT`
    /// (any subcommand) and except while a MULTI is open.
    pub prev_was_client: bool,
}

impl TrackingState {
    /// Whether the reads of the command now executing register their keys.
    ///
    /// Default mode tracks every read; BCAST tracks none (it is prefix
    /// driven); OPTIN tracks only after `CLIENT CACHING yes`; OPTOUT tracks
    /// unless `CLIENT CACHING no` preceded it (redis `trackingRememberKeys`).
    #[inline]
    pub fn tracks_reads(&self) -> bool {
        self.modes().tracks_reads()
    }

    /// The tracking identity of a script this connection runs now.
    #[inline]
    pub fn script_caller(&self, client_id: u64) -> ScriptCaller {
        self.modes().script_caller(client_id)
    }

    /// The flags that decide whether a read is tracked, copied out.
    #[inline]
    pub fn modes(&self) -> TrackingModes {
        TrackingModes {
            enabled: self.enabled,
            bcast: self.bcast,
            optin: self.optin,
            optout: self.optout,
            noloop: self.noloop,
            caching: self.caching,
        }
    }

    /// Per-command hook, called by every connection handler before it
    /// executes a top-level command.
    ///
    /// Redis clears `CLIENT_TRACKING_CACHING` in `resetClient` after each
    /// command unless that command was `CLIENT` or the client is inside
    /// MULTI. Running the same rule one step later — at the start of the NEXT
    /// command — needs only the previous command's CLIENT-ness, because
    /// `in_multi` has not changed in between.
    ///
    /// Cost when tracking is off: one branch on a connection-local bool.
    #[inline]
    pub fn before_command(&mut self, cmd: &[u8], in_multi: bool) {
        if self.enabled {
            self.caching_step(cmd, in_multi);
        }
    }

    #[inline(never)]
    fn caching_step(&mut self, cmd: &[u8], in_multi: bool) {
        if self.caching && !self.prev_was_client && !in_multi {
            self.caching = false;
        }
        self.prev_was_client = cmd.eq_ignore_ascii_case(b"CLIENT");
    }
}

/// The part of [`TrackingState`] that decides whether a read registers its
/// keys, as a `Copy` value.
///
/// A MULTI/EXEC body is bookkept after it ran (see
/// [`invalidation::after_transaction`]), and by then any `CLIENT TRACKING` or
/// `CLIENT CACHING` queued inside it has already changed the connection's
/// state. The body is therefore replayed from the modes captured when EXEC
/// began, applying each queued `CLIENT` command at its own position — the
/// order redis executes them in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackingModes {
    pub enabled: bool,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    pub caching: bool,
}

impl TrackingModes {
    /// Default mode tracks every read; BCAST tracks none (it is prefix
    /// driven); OPTIN tracks only after `CLIENT CACHING yes`; OPTOUT tracks
    /// unless `CLIENT CACHING no` preceded it (redis `trackingRememberKeys`).
    #[inline]
    pub fn tracks_reads(&self) -> bool {
        self.enabled
            && !self.bcast
            && !((self.optin && !self.caching) || (self.optout && self.caching))
    }

    /// The tracking identity a script run by `client_id` under these modes
    /// carries into its `redis.call`s.
    #[inline]
    pub fn script_caller(&self, client_id: u64) -> ScriptCaller {
        ScriptCaller {
            client_id,
            track_reads: self.tracks_reads(),
            noloop: self.noloop,
        }
    }
}

/// Who ran a script, as CLIENT TRACKING needs to know it (moon#1089).
///
/// Redis applies tracking inside `call()`, so every command a script runs is
/// covered: a write invalidates (`signalModifiedKey`), and a read is
/// remembered for the client that ran the script (`trackingRememberKeys`
/// with `server.current_client`), under that client's OPTIN/OPTOUT/CACHING
/// state as it stood when the script started — `CLIENT CACHING` covers the
/// whole `EVAL`. Moon's scripting bridge does the same through this value,
/// which rides with the script's ACL identity to whichever shard runs it.
///
/// `Default` is "no client": writes still invalidate (with no NOLOOP
/// exemption) and reads register nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScriptCaller {
    /// The calling connection; 0 when there is none (client ids start at 1).
    pub client_id: u64,
    /// Whether the caller's reads register their keys.
    pub track_reads: bool,
    /// The caller's NOLOOP flag, recorded with each key it tracks.
    pub noloop: bool,
}

impl ScriptCaller {
    /// Tracking bookkeeping for one command a script ran successfully.
    ///
    /// Callers gate on [`tracking_active`], so with nobody tracking a script
    /// pays one relaxed load per `redis.call` and never reaches here.
    #[cold]
    #[inline(never)]
    pub fn after_script_command(&self, cmd: &[u8], args: &[Frame]) {
        let table = global_table();
        invalidation::invalidate_after_write(&table, cmd, args, self.client_id);
        if self.track_reads {
            invalidation::track_script_read_keys(&table, cmd, args, self.client_id, self.noloop);
        }
    }
}

/// A connection's pub/sub delivery channel, registered so a REDIRECT source
/// can reach it.
///
/// The channel carries pre-serialised RESP, written verbatim by the
/// connection's subscriber loop, so the sender frames each message for the
/// protocol recorded here: RESP2 gets a pub/sub `message` on
/// `__redis__:invalidate`, RESP3 gets the `invalidate` push.
#[derive(Clone)]
pub struct PubSubInbox {
    pub tx: channel::MpscSender<Bytes>,
    pub resp3: bool,
    /// The connection the channel belongs to.
    pub owner: u64,
}

impl PubSubInbox {
    /// Queue invalidation bytes for the target. Never drops silently
    /// (moon#1088): a target whose channel is full is disconnected — redis's
    /// answer to a client past its output-buffer limit, which a caching
    /// client treats as "flush everything". A closed channel means the target
    /// is gone.
    ///
    /// The channel is the connection's pub/sub channel, whose 256 slots are
    /// PUBLISH's slow-subscriber policy. Growing it for invalidations would
    /// put a length check (a channel lock) on every published message,
    /// tracking or not, so it keeps its size, and [`DeliveryBatch`] spends one
    /// slot per COMMAND rather than per key instead. What remains is loud, not
    /// silent: more than 256 commands' worth of invalidations queued while the
    /// target does not read closes the target, where redis would still be
    /// buffering them.
    fn offer(&self, bytes: Bytes) {
        if let Err(flume::TrySendError::Full(_)) = self.tx.try_send(bytes) {
            self.overflow();
        }
    }

    #[cold]
    #[inline(never)]
    fn overflow(&self) {
        tracing::warn!(
            client_id = self.owner,
            "CLIENT TRACKING: a REDIRECT target's delivery queue is full; closing the \
             connection rather than dropping invalidations"
        );
        crate::client_registry::kill_clients(
            &crate::client_registry::KillFilter::Id(self.owner),
            None,
        );
    }
}

/// One recipient of a tracking message, resolved under the table lock by
/// `TrackingTable::route` and delivered by [`TrackingMessage::deliver`].
pub enum Delivery {
    /// The RESP3 push, on a tracking connection's own channel. The receiving
    /// connection drops it if it speaks RESP2, which cannot carry a push
    /// (redis sends such a connection nothing).
    Push(InvalidationTx),
    /// The pub/sub channel of a subscribed redirect target.
    PubSub(PubSubInbox),
    /// The redirect target no longer exists: tell the source, on its own
    /// channel, with `tracking-redir-broken <target>` (RESP3 only, like the
    /// invalidation push itself).
    RedirBroken { source: InvalidationTx, target: u64 },
}

/// A tracking client as `CLIENT LIST`/`CLIENT INFO` describe it: the `t`
/// flag is implied, `R` is `broken_redirect`, `B` is `bcast`, and `redir=` is
/// `redirect` (0 for none). Measured on redis-server 8.6.1: `flags=t redir=0`,
/// `flags=tB`, `flags=tRB redir=<gone id>`; tracking off is `flags=N redir=-1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTrackingView {
    pub redirect: u64,
    pub broken_redirect: bool,
    pub bcast: bool,
}

/// The pub/sub channel RESP2 redirect targets receive invalidations on.
pub const INVALIDATE_CHANNEL: &[u8] = b"__redis__:invalidate";

/// One invalidation, framed lazily for each kind of recipient.
///
/// `payload` is the second element of the message: the array of keys, or
/// Null for a flush.
pub struct TrackingMessage {
    payload: Frame,
    push: Option<Frame>,
    resp2_bytes: Option<Bytes>,
    resp3_bytes: Option<Bytes>,
}

impl TrackingMessage {
    /// An invalidation naming `keys`.
    pub fn keys(keys: &[Bytes]) -> Self {
        let named: Vec<Frame> = keys.iter().map(|k| Frame::BulkString(k.clone())).collect();
        Self::with_payload(Frame::Array(named.into()))
    }

    /// The flush invalidation (FLUSHALL/FLUSHDB): a Null payload means "drop
    /// everything you have cached".
    pub fn flush() -> Self {
        Self::with_payload(Frame::Null)
    }

    fn with_payload(payload: Frame) -> Self {
        Self {
            payload,
            push: None,
            resp2_bytes: None,
            resp3_bytes: None,
        }
    }

    /// `>2 invalidate <payload>`.
    fn push_frame(&mut self) -> &Frame {
        let payload = &self.payload;
        self.push.get_or_insert_with(|| {
            Frame::Push(crate::framevec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                payload.clone(),
            ])
        })
    }

    /// Hand the message to one recipient. Never blocks, and never drops
    /// silently (moon#1088): redis never loses an invalidation — it
    /// disconnects a client whose output buffer passes its limit, which a
    /// caching client treats as "flush everything" — and neither does moon
    /// (see [`queue`] and [`PubSubInbox::offer`]).
    pub fn deliver(&mut self, to: &Delivery) {
        match to {
            Delivery::Push(tx) => tx.send(self.push_frame().clone()),
            Delivery::PubSub(inbox) => inbox.offer(self.inbox_bytes(inbox.resp3)),
            Delivery::RedirBroken { source, target } => source.send(redir_broken_push(*target)),
        }
    }

    /// The message framed for a pub/sub inbox of the given protocol,
    /// serialised once per protocol however many inboxes receive it.
    fn inbox_bytes(&mut self, resp3: bool) -> Bytes {
        if resp3 {
            if let Some(b) = &self.resp3_bytes {
                return b.clone();
            }
            let mut buf = bytes::BytesMut::new();
            crate::protocol::serialize_resp3(self.push_frame(), &mut buf);
            self.resp3_bytes.insert(buf.freeze()).clone()
        } else {
            if let Some(b) = &self.resp2_bytes {
                return b.clone();
            }
            // `*3 message __redis__:invalidate <payload>` — the exact frame
            // redis-server 8.6.1 writes.
            let message = Frame::Array(crate::framevec![
                Frame::BulkString(Bytes::from_static(b"message")),
                Frame::BulkString(Bytes::from_static(INVALIDATE_CHANNEL)),
                self.payload.clone(),
            ]);
            let mut buf = bytes::BytesMut::new();
            crate::protocol::serialize(&message, &mut buf);
            self.resp2_bytes.insert(buf.freeze()).clone()
        }
    }
}

/// The deliveries of one command's invalidations, with everything bound for
/// the same pub/sub inbox coalesced into ONE channel item.
///
/// A REDIRECT target's inbox is the connection's pub/sub channel, whose slots
/// are shared with PUBLISH. One item per key would let a single wide write
/// (`MSET` of thousands of keys, or `DEL` of a big key list) fill it while the
/// target is not reading; one item per command keeps even a long pipeline of
/// such writes far inside it. The receiving loop writes each item verbatim,
/// so the wire is unchanged: the same messages, in the same order.
#[derive(Default)]
pub struct DeliveryBatch {
    inboxes: smallvec::SmallVec<[(PubSubInbox, bytes::BytesMut); 2]>,
}

impl DeliveryBatch {
    /// Deliver `msg` to `to`: at once for a tracking channel, or appended to
    /// the inbox's pending item.
    pub fn deliver(&mut self, msg: &mut TrackingMessage, to: &Delivery) {
        let Delivery::PubSub(inbox) = to else {
            msg.deliver(to);
            return;
        };
        let bytes = msg.inbox_bytes(inbox.resp3);
        match self
            .inboxes
            .iter_mut()
            .find(|(i, _)| i.owner == inbox.owner)
        {
            Some((_, buf)) => buf.extend_from_slice(&bytes),
            None => self
                .inboxes
                .push((inbox.clone(), bytes::BytesMut::from(bytes.as_ref()))),
        }
    }

    /// Send every coalesced inbox item.
    pub fn flush(self) {
        for (inbox, buf) in self.inboxes {
            inbox.offer(buf.freeze());
        }
    }
}

thread_local! {
    /// The batch a synchronous multi-command unit (a script, an EXEC body's
    /// bookkeeping) collects its inbox deliveries into, and how many nested
    /// units hold it open. See [`begin_delivery_batch`].
    static OPEN_BATCH: std::cell::RefCell<(u32, Option<DeliveryBatch>)> =
        const { std::cell::RefCell::new((0, None)) };
}

/// Open (or join) this thread's delivery batch, so every invalidation until
/// the matching [`end_delivery_batch`] reaches each REDIRECT inbox as ONE
/// channel item (moon#1088).
///
/// For units that run to completion WITHOUT yielding — a Lua script, the
/// bookkeeping of an EXEC body — whose target therefore cannot drain its
/// channel in between: a script with hundreds of writing `redis.call`s would
/// otherwise take hundreds of slots. Never hold one across an `.await`: other
/// connections' deliveries on this thread would wait for it.
pub fn begin_delivery_batch() {
    OPEN_BATCH.with_borrow_mut(|(depth, batch)| {
        *depth += 1;
        if batch.is_none() {
            *batch = Some(DeliveryBatch::default());
        }
    });
}

/// Close the unit opened by [`begin_delivery_batch`]; the outermost close
/// sends everything collected. A close with nothing open is a no-op.
pub fn end_delivery_batch() {
    let done = OPEN_BATCH.with_borrow_mut(|(depth, batch)| {
        if *depth == 0 {
            return None;
        }
        *depth -= 1;
        if *depth == 0 { batch.take() } else { None }
    });
    if let Some(batch) = done {
        batch.flush();
    }
}

/// Run `f` with this thread's open delivery batch, or, when none is open,
/// with a fresh one that is sent as soon as `f` returns.
pub(crate) fn with_delivery_batch<R>(f: impl FnOnce(&mut DeliveryBatch) -> R) -> R {
    let open = OPEN_BATCH.with_borrow_mut(|(_, batch)| batch.take());
    match open {
        Some(mut batch) => {
            let r = f(&mut batch);
            OPEN_BATCH.with_borrow_mut(|(_, slot)| *slot = Some(batch));
            r
        }
        None => {
            let mut batch = DeliveryBatch::default();
            let r = f(&mut batch);
            batch.flush();
            r
        }
    }
}

/// `>2 tracking-redir-broken :<target>` — what redis pushes to a RESP3 source
/// each time an invalidation cannot reach its vanished redirect target.
pub fn redir_broken_push(target: u64) -> Frame {
    Frame::Push(crate::framevec![
        Frame::BulkString(Bytes::from_static(b"tracking-redir-broken")),
        Frame::Integer(i64::try_from(target).unwrap_or(i64::MAX)),
    ])
}

/// Process-wide tracking table (see [`global_table`]).
///
/// Two modes:
/// 1. Normal (default): track_key records which clients have read a key.
///    On write, invalidate_key looks up clients and sends invalidation.
/// 2. BCAST: clients register prefixes. On ANY write, check if key matches
///    any registered prefix and invalidate matching clients.
///
/// Table is bounded: max_keys (default 1_000_000). When exceeded, evict oldest
/// entries with fake invalidation.
pub struct TrackingTable {
    /// Normal mode: key -> set of (client_id, noloop)
    key_clients: HashMap<Bytes, Vec<(u64, bool)>>,
    /// REVERSE index of `key_clients`: client_id -> the keys it currently tracks.
    ///
    /// Disconnect used to sweep every entry of `key_clients` looking for the
    /// departing client, holding the process-wide tracking mutex for the whole
    /// walk — so one client hanging up stalled every other shard's invalidation
    /// path, and the stall grew with the table (capped at `max_keys`, one
    /// million). This makes teardown proportional to what the client actually
    /// tracked. Kept exactly in step with `key_clients`: every insertion and
    /// every removal there has a matching update here.
    client_keys: HashMap<u64, HashSet<Bytes>>,
    /// BCAST mode: list of (client_id, prefix, noloop)
    bcast_clients: Vec<(u64, Bytes, bool)>,
    /// Client channels: client_id -> its invalidation queue.
    client_channels: HashMap<u64, InvalidationTx>,
    /// Redirect map: source_client_id -> target_client_id
    redirects: HashMap<u64, u64>,
    /// Pub/sub delivery channels of connections that have subscribed, keyed
    /// by client id — how a REDIRECT reaches a target that never enabled
    /// tracking itself (moon#1048). Registered once per connection, when its
    /// pub/sub channel is created, and removed when it disconnects.
    inboxes: HashMap<u64, PubSubInbox>,
    /// Sources whose redirect target was found gone (`broken_redirect` in
    /// `CLIENT TRACKINGINFO`). Cleared when the source re-enables or disables
    /// tracking.
    broken: HashSet<u64>,
    /// Whether a client id belongs to a live connection. The client registry
    /// in production; injectable so the routing rules are unit-testable.
    is_connected: fn(u64) -> bool,
    /// Maximum keys tracked (bounded table)
    max_keys: usize,
}

impl Default for TrackingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackingTable {
    pub fn new() -> Self {
        Self::with_max_keys(1_000_000)
    }

    /// Construct with an explicit key cap (tests; production uses `new`).
    pub fn with_max_keys(max_keys: usize) -> Self {
        Self {
            key_clients: HashMap::new(),
            client_keys: HashMap::new(),
            bcast_clients: Vec::new(),
            client_channels: HashMap::new(),
            redirects: HashMap::new(),
            inboxes: HashMap::new(),
            broken: HashSet::new(),
            is_connected: crate::client_registry::is_registered,
            max_keys,
        }
    }

    /// Replace the connection-liveness probe (tests).
    #[cfg(test)]
    pub(crate) fn with_liveness(mut self, is_connected: fn(u64) -> bool) -> Self {
        self.is_connected = is_connected;
        self
    }

    /// Register a client's invalidation channel.
    pub fn register_client(&mut self, client_id: u64, tx: impl Into<InvalidationTx>) {
        if self.client_channels.insert(client_id, tx.into()).is_none() {
            ACTIVE_TRACKERS.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set (or, with `None`, clear) where `source`'s invalidations go.
    /// Re-enabling tracking replaces the redirect wholesale, and a fresh
    /// redirect is not broken.
    pub fn set_redirect(&mut self, source: u64, target: Option<u64>) {
        match target {
            Some(t) => {
                self.redirects.insert(source, t);
            }
            None => {
                self.redirects.remove(&source);
            }
        }
        self.broken.remove(&source);
    }

    /// Whether `client_id` has tracking enabled (its channel is registered).
    pub fn is_tracking(&self, client_id: u64) -> bool {
        self.client_channels.contains_key(&client_id)
    }

    /// What `CLIENT LIST`/`CLIENT INFO` report about `client_id`'s tracking,
    /// or `None` when it has tracking off (moon#1078).
    pub fn client_view(&self, client_id: u64) -> Option<ClientTrackingView> {
        if !self.is_tracking(client_id) {
            return None;
        }
        Some(ClientTrackingView {
            redirect: self.redirects.get(&client_id).copied().unwrap_or(0),
            broken_redirect: self.broken.contains(&client_id),
            bcast: self.bcast_clients.iter().any(|(id, _, _)| *id == client_id),
        })
    }

    /// [`Self::client_view`] for every tracking client, for one `CLIENT
    /// LIST`. Proportional to the number of tracking clients, not of
    /// connections.
    pub fn client_views(&self) -> HashMap<u64, ClientTrackingView> {
        // One pass over the BCAST registrations, not one per client: this
        // runs under the tracking mutex every write path waits on.
        let bcast: HashSet<u64> = self.bcast_clients.iter().map(|(id, _, _)| *id).collect();
        self.client_channels
            .keys()
            .map(|&id| {
                let view = ClientTrackingView {
                    redirect: self.redirects.get(&id).copied().unwrap_or(0),
                    broken_redirect: self.broken.contains(&id),
                    bcast: bcast.contains(&id),
                };
                (id, view)
            })
            .collect()
    }

    /// Whether an invalidation for `source` has found its redirect target
    /// gone since tracking was (re-)enabled.
    pub fn is_redirect_broken(&self, source: u64) -> bool {
        self.broken.contains(&source)
    }

    /// Whether `client_id` names a connection that could be a redirect
    /// target right now.
    pub fn client_exists(&self, client_id: u64) -> bool {
        self.client_channels.contains_key(&client_id)
            || self.inboxes.contains_key(&client_id)
            || (self.is_connected)(client_id)
    }

    /// Register the pub/sub channel of a connection that has subscribed.
    pub fn register_inbox(&mut self, client_id: u64, inbox: PubSubInbox) {
        self.inboxes.insert(client_id, inbox);
    }

    /// Drop a disconnecting connection's pub/sub inbox.
    pub fn unregister_inbox(&mut self, client_id: u64) {
        self.inboxes.remove(&client_id);
    }

    /// Register a BCAST prefix for a client. Registering the same prefix
    /// twice is a no-op (re-enabling BCAST adds prefixes, it does not
    /// duplicate them); `noloop` is refreshed.
    pub fn register_prefix(&mut self, client_id: u64, prefix: Bytes, noloop: bool) {
        if let Some(entry) = self
            .bcast_clients
            .iter_mut()
            .find(|(id, p, _)| *id == client_id && *p == prefix)
        {
            entry.2 = noloop;
            return;
        }
        self.bcast_clients.push((client_id, prefix, noloop));
    }

    /// Re-enabling tracking replaces NOLOOP for every prefix the client
    /// already has.
    pub fn set_bcast_noloop(&mut self, client_id: u64, noloop: bool) {
        for entry in self.bcast_clients.iter_mut().filter(|e| e.0 == client_id) {
            entry.2 = noloop;
        }
    }

    /// Resolve where one tracker's message goes.
    ///
    /// Without a redirect, the tracker's own channel. With one, the target —
    /// by redis's rules (`sendTrackingMessage`):
    ///
    /// * a subscribed RESP2 target gets a pub/sub `message`;
    /// * a target with its own tracking channel gets the push (RESP3 writes
    ///   it, RESP2 drops it);
    /// * a subscribed RESP3 target gets the push through its pub/sub channel;
    /// * a target that exists but has none of those cannot receive anything —
    ///   redis drops the message too;
    /// * a target that no longer exists breaks the redirect: the source is
    ///   told, and `CLIENT TRACKINGINFO` reports `broken_redirect`.
    ///
    /// Known gap (moon#1078, left open on purpose): a RESP3 target that
    /// neither subscribed nor enabled tracking has no channel moon can reach,
    /// so it gets nothing where redis pushes to it. The two ways to close it
    /// were weighed and neither is taken here:
    ///
    /// * a channel for every RESP3 connection: a connection holding one waits
    ///   in a select that never parks. Measured (monoio on macOS,
    ///   `--shards 1`, `--conn-park-secs 2`, 2000 idle `HELLO 3` connections,
    ///   twice):
    ///   without a channel `parked_clients:2000`; with one (`CLIENT TRACKING
    ///   on`) `parked_clients:0`. Every RESP3 client would lose c1M parking.
    /// * install a channel lazily and wake the target: the target may be
    ///   parked in a cancelable read registered in its OWN shard's
    ///   thread-local idle registry, or task-exited behind a readiness
    ///   watcher, or in a tokio select — none of which another thread can
    ///   wake today except by `shutdown(2)` (`CLIENT KILL`). It needs a new
    ///   shard-mesh message and a wake arm in every park stage on both
    ///   runtimes; that is a change to the c1M park machinery, not to
    ///   tracking, and belongs in its own PR.
    fn route(&mut self, client_id: u64) -> Option<Delivery> {
        let Some(&target) = self.redirects.get(&client_id) else {
            return self
                .client_channels
                .get(&client_id)
                .cloned()
                .map(Delivery::Push);
        };
        let inbox = self.inboxes.get(&target);
        if let Some(inbox) = inbox.filter(|i| !i.resp3) {
            return Some(Delivery::PubSub(inbox.clone()));
        }
        if let Some(tx) = self.client_channels.get(&target) {
            return Some(Delivery::Push(tx.clone()));
        }
        if let Some(inbox) = inbox {
            return Some(Delivery::PubSub(inbox.clone()));
        }
        // Lock order: this probes the client registry (a striped RwLock) while
        // the tracking mutex is held. Nothing takes the tracking mutex while
        // holding a registry stripe — `client_registry::update` closures only
        // touch the entry — so the order cannot invert. Reached only for a
        // redirect whose target has neither an inbox nor a tracking channel.
        if (self.is_connected)(target) {
            return None;
        }
        self.broken.insert(client_id);
        self.client_channels
            .get(&client_id)
            .cloned()
            .map(|source| Delivery::RedirBroken { source, target })
    }

    /// Track that a client has read a key (normal mode).
    ///
    /// Enforces the `max_keys` bound (deep-review G1: the documented cap was
    /// dead code, so a long-lived tracking client reading many distinct
    /// never-written keys grew this table without limit). When tracking a NEW
    /// key would exceed the cap, an arbitrary existing entry is evicted and
    /// its `(key, recipients)` returned — the caller must deliver an
    /// invalidation for it so the evicted key's clients drop their cached copy
    /// (Redis's "fake invalidation" on tracking-table eviction). Returns
    /// `None` when no eviction occurred.
    pub fn track_key(
        &mut self,
        client_id: u64,
        key: &Bytes,
        noloop: bool,
    ) -> Option<(Bytes, Vec<Delivery>)> {
        if let Some(clients) = self.key_clients.get_mut(key) {
            if !clients.iter().any(|(id, _)| *id == client_id) {
                clients.push((client_id, noloop));
                self.client_keys
                    .entry(client_id)
                    .or_default()
                    .insert(key.clone());
            }
            return None;
        }

        let evicted = if self.key_clients.len() >= self.max_keys.max(1) {
            // Evict an arbitrary entry (HashMap has no age order; correctness
            // needs only that the evicted key's trackers are told to drop it).
            #[allow(clippy::unwrap_used)] // len >= 1 guaranteed by the branch
            let victim = self.key_clients.keys().next().unwrap().clone();
            let clients = self.key_clients.remove(&victim).unwrap_or_default();
            let mut recipients = Vec::new();
            for (cid, _noloop) in clients {
                Self::forget_client_key(&mut self.client_keys, cid, &victim);
                // No noloop skip: cap eviction is not a self-write — every
                // tracker of the victim key must drop its cached copy.
                if let Some(d) = self.route(cid) {
                    recipients.push(d);
                }
            }
            Some((victim, recipients))
        } else {
            None
        };

        self.key_clients
            .insert(key.clone(), vec![(client_id, noloop)]);
        self.client_keys
            .entry(client_id)
            .or_default()
            .insert(key.clone());
        evicted
    }

    /// Get the list of client IDs tracking a given key (for testing).
    pub fn tracked_clients(&self, key: &Bytes) -> Vec<u64> {
        self.key_clients
            .get(key)
            .map(|clients| clients.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default()
    }

    /// Invalidate a key: collect all clients that tracked this key (normal mode)
    /// and all BCAST clients whose prefixes match, resolved to their delivery
    /// routes. Removes the key from the tracking table after collection.
    pub fn invalidate_key(&mut self, key: &Bytes, writer_client_id: u64) -> Vec<Delivery> {
        let mut to_notify: Vec<Delivery> = Vec::new();

        // Normal mode: check key_clients
        if let Some(clients) = self.key_clients.remove(key) {
            for (cid, noloop) in clients {
                // The key is gone from the forward map, so it must go from the
                // reverse one too -- a stale entry would make the reverse index
                // grow without bound for a client that re-reads an
                // often-invalidated key, and teardown would walk the garbage.
                Self::forget_client_key(&mut self.client_keys, cid, key);
                // NOLOOP: skip if the writer is the same client
                if noloop && cid == writer_client_id {
                    continue;
                }
                if let Some(d) = self.route(cid) {
                    to_notify.push(d);
                }
            }
        }

        // BCAST mode: check prefix matches. Collected first, because routing
        // needs `&mut self` (it records broken redirects).
        let mut bcast_hits: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
        for (cid, prefix, noloop) in &self.bcast_clients {
            if key.starts_with(prefix.as_ref()) {
                if *noloop && *cid == writer_client_id {
                    continue;
                }
                bcast_hits.push(*cid);
            }
        }
        for cid in bcast_hits {
            if let Some(d) = self.route(cid) {
                to_notify.push(d);
            }
        }

        to_notify
    }

    /// Remove all tracking for a client (on disconnect or TRACKING OFF).
    ///
    /// Leaves the client's pub/sub inbox alone: that belongs to the
    /// connection's subscriptions, not its tracking, and another client may
    /// still redirect to it. [`TrackingTable::unregister_inbox`] drops it.
    pub fn untrack_all(&mut self, client_id: u64) {
        // Visit only the keys this client actually tracked. This used to
        // `retain` over the whole table -- O(tracked keys) per disconnect,
        // under the process-wide mutex, regardless of whether the departing
        // client had tracked anything at all.
        if let Some(keys) = self.client_keys.remove(&client_id) {
            for key in keys {
                let Some(clients) = self.key_clients.get_mut(&key) else {
                    continue;
                };
                clients.retain(|(id, _)| *id != client_id);
                if clients.is_empty() {
                    self.key_clients.remove(&key);
                }
            }
        }
        // Remove from bcast_clients
        self.bcast_clients.retain(|(id, _, _)| *id != client_id);
        // Remove channel and redirect
        if self.client_channels.remove(&client_id).is_some() {
            ACTIVE_TRACKERS.fetch_sub(1, Ordering::Relaxed);
        }
        self.redirects.remove(&client_id);
        self.broken.remove(&client_id);
    }

    /// Drop one (client, key) pair from the reverse index, retiring the
    /// client's entry when it has nothing left to track.
    ///
    /// Free function over the map so callers can hold a borrow of the forward
    /// map across the call.
    fn forget_client_key(
        client_keys: &mut HashMap<u64, HashSet<Bytes>>,
        client_id: u64,
        key: &Bytes,
    ) {
        if let Some(keys) = client_keys.get_mut(&client_id) {
            keys.remove(key);
            if keys.is_empty() {
                client_keys.remove(&client_id);
            }
        }
    }

    /// Cache-flush invalidation (FLUSHALL/FLUSHDB): every registered client
    /// must drop its whole local cache. Clears the per-key table and returns
    /// every tracking client's route, so the caller can deliver the flush
    /// invalidation (`invalidate` + Null payload, the Redis convention) — to
    /// the redirect target where there is one.
    pub fn invalidate_all(&mut self) -> Vec<Delivery> {
        self.key_clients.clear();
        self.client_keys.clear();
        let ids: Vec<u64> = self.client_channels.keys().copied().collect();
        ids.into_iter().filter_map(|id| self.route(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::channel;

    /// The forward and reverse indexes must describe exactly the same set of
    /// (key, client) pairs. Every test below asserts this after mutating the
    /// table -- a reverse index that drifts is worse than no reverse index,
    /// because teardown then silently leaves a departed client registered on a
    /// key and keeps pushing invalidations into a dead channel.
    fn assert_indexes_agree(table: &TrackingTable) {
        let mut forward: Vec<(u64, Bytes)> = Vec::new();
        for (key, clients) in &table.key_clients {
            assert!(!clients.is_empty(), "empty client list left on {key:?}");
            for (cid, _) in clients {
                forward.push((*cid, key.clone()));
            }
        }
        let mut reverse: Vec<(u64, Bytes)> = Vec::new();
        for (cid, keys) in &table.client_keys {
            assert!(!keys.is_empty(), "empty key set left for client {cid}");
            for key in keys {
                reverse.push((*cid, key.clone()));
            }
        }
        forward.sort();
        reverse.sort();
        assert_eq!(forward, reverse, "forward and reverse indexes disagree");
    }

    fn sender() -> channel::MpscSender<Frame> {
        let (tx, rx) = channel::mpsc_unbounded::<Frame>();
        std::mem::forget(rx); // keep the channel alive for the test's lifetime
        tx
    }

    #[test]
    fn disconnect_untracks_only_the_departing_client() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        table.register_client(2, sender());
        let shared = Bytes::from_static(b"shared");
        let solo = Bytes::from_static(b"solo");
        table.track_key(1, &shared, false);
        table.track_key(2, &shared, false);
        table.track_key(1, &solo, false);
        assert_indexes_agree(&table);

        table.untrack_all(1);

        assert_eq!(table.tracked_clients(&shared), vec![2]);
        assert!(
            !table.key_clients.contains_key(&solo),
            "a key with no trackers left must be dropped, not kept empty"
        );
        assert_indexes_agree(&table);
    }

    #[test]
    fn invalidating_a_key_clears_it_from_the_reverse_index() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        // Track, invalidate, re-track -- ten times. The reverse index must not
        // accumulate: it mirrors the forward map, which holds one entry.
        for _ in 0..10 {
            table.track_key(1, &k, false);
            table.invalidate_key(&k, 99);
        }
        assert!(table.client_keys.is_empty(), "reverse index accumulated");
        table.track_key(1, &k, false);
        assert_eq!(table.client_keys[&1].len(), 1);
        assert_indexes_agree(&table);

        // And teardown after an invalidation must not trip over the gap.
        table.invalidate_key(&k, 99);
        table.untrack_all(1);
        assert_indexes_agree(&table);
    }

    #[test]
    fn cap_eviction_clears_the_reverse_index() {
        let mut table = TrackingTable::with_max_keys(2);
        table.register_client(1, sender());
        table.track_key(1, &Bytes::from_static(b"a"), false);
        table.track_key(1, &Bytes::from_static(b"b"), false);
        // The third key evicts an arbitrary existing one.
        let evicted = table.track_key(1, &Bytes::from_static(b"c"), false);
        assert!(evicted.is_some(), "the cap must evict");
        assert_eq!(table.key_clients.len(), 2, "table must stay at the cap");
        assert_indexes_agree(&table);
    }

    #[test]
    fn flush_invalidation_clears_the_reverse_index() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        table.track_key(1, &Bytes::from_static(b"a"), false);
        table.invalidate_all();
        assert!(
            table.client_keys.is_empty(),
            "FLUSHALL left a stale reverse entry"
        );
        assert_indexes_agree(&table);
        // The client is still registered, so it can track again.
        table.track_key(1, &Bytes::from_static(b"b"), false);
        assert_indexes_agree(&table);
    }

    #[test]
    fn tracking_the_same_key_twice_records_one_reverse_entry() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        table.track_key(1, &k, false);
        table.track_key(1, &k, false);
        assert_eq!(table.tracked_clients(&k), vec![1]);
        assert_eq!(table.client_keys[&1].len(), 1);
        assert_indexes_agree(&table);
    }

    #[test]
    fn disconnecting_an_untracked_client_leaves_the_table_alone() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        table.track_key(1, &k, false);

        table.untrack_all(42); // never tracked, never registered

        assert_eq!(table.tracked_clients(&k), vec![1]);
        assert_indexes_agree(&table);
    }

    /// Cost of ONE client disconnecting, as the tracking table's key count grows.
    ///
    /// `#[ignore]`d: a measurement, not an assertion. Run it explicitly:
    /// `cargo test --release --lib bench_untrack_all_cost_vs_table_size -- --ignored --nocapture`
    ///
    /// The disconnecting client tracks NOTHING, so every microsecond spent is
    /// the sweep over other clients' keys. Flat µs/disconnect means the table
    /// size does not matter; growth means each disconnect is O(table).
    #[test]
    #[ignore = "measurement harness; run explicitly with --nocapture"]
    fn bench_untrack_all_cost_vs_table_size() {
        use std::time::Instant;
        println!("{:<10} {:<16} total", "keys", "µs/disconnect");
        for keys in [1000_usize, 2000, 4000, 8000, 16000] {
            let mut table = TrackingTable::new();
            let (tx, _rx) = channel::mpsc_unbounded::<Frame>();
            table.register_client(1, tx.clone());
            for k in 0..keys {
                table.track_key(1, &Bytes::from(format!("key:{k}")), false);
            }
            // 100 clients that tracked nothing at all connect and disconnect.
            const CHURN: usize = 100;
            for c in 0..CHURN {
                table.register_client(100 + c as u64, tx.clone());
            }
            let t = Instant::now();
            for c in 0..CHURN {
                table.untrack_all(100 + c as u64);
            }
            let el = t.elapsed();
            println!(
                "{:<10} {:<16.3} {:?}",
                keys,
                el.as_secs_f64() * 1e6 / CHURN as f64,
                el
            );
            assert_eq!(table.key_clients.len(), keys, "sweep dropped live entries");
        }
    }

    #[test]
    fn test_new_creates_empty_table() {
        let table = TrackingTable::new();
        assert!(table.key_clients.is_empty());
        assert!(table.bcast_clients.is_empty());
        assert!(table.client_channels.is_empty());
    }

    #[test]
    fn test_track_key_registers_client() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        assert_eq!(table.tracked_clients(&key), vec![1]);
    }

    #[test]
    fn test_track_key_idempotent() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        table.track_key(1, &key, false);
        assert_eq!(table.tracked_clients(&key), vec![1]);
    }

    #[test]
    fn test_track_key_multiple_clients() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        table.track_key(2, &key, false);
        let mut clients = table.tracked_clients(&key);
        clients.sort();
        assert_eq!(clients, vec![1, 2]);
    }

    /// G1 (deep review): the documented max_keys bound was dead code — a
    /// long-lived tracking client reading many distinct never-written keys
    /// grew key_clients without limit. The cap must evict an existing entry
    /// (with invalidation senders so the evicted key's clients drop their
    /// cached copy) instead of growing.
    #[test]
    fn test_track_key_enforces_max_keys_bound() {
        let mut table = TrackingTable::with_max_keys(2);
        let (tx, rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k1"), false)
                .is_none()
        );
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k2"), false)
                .is_none()
        );
        // Re-tracking an existing key never evicts.
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k2"), false)
                .is_none()
        );

        // Third distinct key: one existing entry must be evicted, with the
        // evicted key's client senders returned for invalidation.
        let evicted = table
            .track_key(1, &Bytes::from_static(b"k3"), false)
            .expect("cap reached: eviction expected");
        assert!(evicted.0 == Bytes::from_static(b"k1") || evicted.0 == Bytes::from_static(b"k2"));
        assert_eq!(evicted.1.len(), 1, "evicted key's tracker must be notified");
        assert_eq!(table.key_clients.len(), 2, "table must stay at the cap");
        assert_eq!(table.tracked_clients(&Bytes::from_static(b"k3")), vec![1]);
        drop(rx);
    }

    #[test]
    fn test_invalidate_key_returns_senders_and_removes() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);

        let senders = table.invalidate_key(&key, 99); // writer is different client
        assert_eq!(senders.len(), 1);
        // Key should be removed after invalidation
        assert!(table.tracked_clients(&key).is_empty());
    }

    #[test]
    fn test_untrack_all_removes_client() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key1 = Bytes::from_static(b"foo");
        let key2 = Bytes::from_static(b"bar");
        table.track_key(1, &key1, false);
        table.track_key(1, &key2, false);

        table.untrack_all(1);
        assert!(table.tracked_clients(&key1).is_empty());
        assert!(table.tracked_clients(&key2).is_empty());
        assert!(!table.client_channels.contains_key(&1));
    }

    #[test]
    fn test_bcast_prefix_match() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        table.register_prefix(1, Bytes::from_static(b"user:"), false);

        let key = Bytes::from_static(b"user:123");
        let senders = table.invalidate_key(&key, 99);
        assert_eq!(senders.len(), 1);
    }

    #[test]
    fn test_bcast_prefix_no_match() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        table.register_prefix(1, Bytes::from_static(b"user:"), false);

        let key = Bytes::from_static(b"other:key");
        let senders = table.invalidate_key(&key, 99);
        assert!(senders.is_empty());
    }

    #[test]
    fn test_noloop_skips_self_invalidation() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, true); // noloop = true

        // Writer is the same client (1), should skip
        let senders = table.invalidate_key(&key, 1);
        assert!(senders.is_empty());
    }

    #[test]
    fn test_redirect_sends_to_target() {
        let mut table = TrackingTable::new();
        let (tx1, _rx1) = channel::mpsc_bounded::<Frame>(16);
        let (tx2, rx2) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx1);
        table.register_client(2, tx2);
        table.set_redirect(1, Some(2)); // redirect client 1's invalidations to client 2

        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);

        let recipients = table.invalidate_key(&key, 99);
        assert_eq!(recipients.len(), 1);
        let mut msg = TrackingMessage::keys(std::slice::from_ref(&key));
        msg.deliver(&recipients[0]);
        let received = rx2.try_recv().unwrap();
        assert_eq!(
            received,
            invalidation::invalidation_push(std::slice::from_ref(&key))
        );
    }

    // ── moon#1048: REDIRECT routing ─────────────────────────────────────

    /// Ids >= 1000 are "connected" for the routing tests below; smaller ids
    /// behave as disconnected unless the table itself knows them.
    fn connected_above_1000(id: u64) -> bool {
        id >= 1000
    }

    fn inbox(resp3: bool) -> (PubSubInbox, channel::MpscReceiver<Bytes>) {
        let (tx, rx) = channel::mpsc_unbounded::<Bytes>();
        (
            PubSubInbox {
                tx,
                resp3,
                owner: 0,
            },
            rx,
        )
    }

    /// moon#1088: one command's invalidations for one REDIRECT inbox travel
    /// as ONE channel item, byte-identical to the per-key messages redis
    /// writes, so a wide write cannot fill the subscriber's channel.
    #[test]
    fn a_wide_write_reaches_an_inbox_as_one_item() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, _src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1071, src_tx);
        table.set_redirect(1071, Some(1072));
        let (ib, ib_rx) = inbox(false);
        table.register_inbox(1072, ib);
        let keys: Vec<Bytes> = (0..400).map(|i| Bytes::from(format!("w:{i}"))).collect();
        for k in &keys {
            table.track_key(1071, k, false);
        }
        let mut batch = DeliveryBatch::default();
        for k in &keys {
            let mut msg = TrackingMessage::keys(std::slice::from_ref(k));
            for to in &table.invalidate_key(k, 7) {
                batch.deliver(&mut msg, to);
            }
        }
        batch.flush();
        let item = ib_rx.try_recv().expect("one item");
        assert!(ib_rx.try_recv().is_err(), "exactly one item for the burst");
        let mut want = Vec::new();
        for k in &keys {
            want.extend_from_slice(
                format!(
                    "*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n${}\r\n{}\r\n",
                    k.len(),
                    String::from_utf8_lossy(k)
                )
                .as_bytes(),
            );
        }
        assert_eq!(item.as_ref(), want.as_slice());
        table.untrack_all(1071);
    }

    /// A script or EXEC body holds the thread's batch open across many
    /// commands: every invalidation for one inbox leaves as ONE item when the
    /// outermost unit closes, and nothing leaves before.
    #[test]
    fn an_open_batch_spans_many_commands() {
        let table =
            parking_lot::Mutex::new(TrackingTable::new().with_liveness(connected_above_1000));
        let (src_tx, _src_rx) = channel::mpsc_unbounded::<Frame>();
        let (ib, ib_rx) = inbox(true);
        {
            let mut t = table.lock();
            t.register_client(1081, src_tx);
            t.set_redirect(1081, Some(1082));
            t.register_inbox(1082, ib);
        }
        let keys: Vec<Bytes> = (0..300).map(|i| Bytes::from(format!("b:{i}"))).collect();
        for k in &keys {
            table.lock().track_key(1081, k, false);
        }
        begin_delivery_batch();
        begin_delivery_batch(); // nested unit (a script inside a transaction)
        for k in &keys {
            invalidation::invalidate_keys(&table, std::slice::from_ref(k), 7);
        }
        end_delivery_batch();
        assert!(ib_rx.try_recv().is_err(), "an inner close sends nothing");
        end_delivery_batch();
        let item = ib_rx.try_recv().expect("one item at the outermost close");
        assert!(
            ib_rx.try_recv().is_err(),
            "exactly one item for 300 commands"
        );
        assert_eq!(item.len(), pushes_len(&keys), "every key, byte for byte");
        end_delivery_batch(); // unbalanced close is a no-op
        table.lock().untrack_all(1081);
    }

    /// Serialised length of one RESP3 invalidation push per key.
    fn pushes_len(keys: &[Bytes]) -> usize {
        keys.iter()
            .map(|k| {
                let mut b = bytes::BytesMut::new();
                crate::protocol::serialize_resp3(
                    &invalidation::invalidation_push(std::slice::from_ref(k)),
                    &mut b,
                );
                b.len()
            })
            .sum()
    }

    /// moon#1088: an invalidation that finds the target's channel full is
    /// never dropped quietly — the target is disconnected, as redis closes a
    /// client past its output-buffer limit.
    #[test]
    fn a_full_inbox_disconnects_its_owner() {
        const OWNER: u64 = 9_108_801;
        let live = crate::client_registry::register(
            OWNER,
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            "default".into(),
            0,
            -1,
        );
        let (tx, rx) = channel::mpsc_bounded::<Bytes>(2);
        let ib = PubSubInbox {
            tx,
            resp3: false,
            owner: OWNER,
        };
        ib.offer(Bytes::from_static(b"1"));
        ib.offer(Bytes::from_static(b"2"));
        assert!(!live.is_killed(), "room left: nothing to do");
        ib.offer(Bytes::from_static(b"3"));
        assert!(live.is_killed(), "a full inbox must close its owner");
        assert_eq!(rx.len(), 2);
        crate::client_registry::deregister(OWNER);
    }

    fn deliver_all(recipients: &[Delivery], msg: &mut TrackingMessage) {
        for d in recipients {
            msg.deliver(d);
        }
    }

    /// The issue's case: the target never enabled tracking — it only
    /// SUBSCRIBEd. It must receive the RESP2 pub/sub form, byte for byte what
    /// redis-server 8.6.1 writes.
    #[test]
    fn redirect_to_a_resp2_subscriber_gets_a_pubsub_message() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1001, src_tx);
        table.set_redirect(1001, Some(1002));
        let (ib, ib_rx) = inbox(false);
        table.register_inbox(1002, ib);
        let key = Bytes::from_static(b"c:redir");
        table.track_key(1001, &key, false);

        let recipients = table.invalidate_key(&key, 7);
        deliver_all(&recipients, &mut TrackingMessage::keys(&[key]));
        assert_eq!(
            ib_rx.try_recv().unwrap(),
            Bytes::from_static(
                b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$7\r\nc:redir\r\n"
            )
        );
        assert!(src_rx.try_recv().is_err(), "the source gets nothing");
        assert!(!table.is_redirect_broken(1001));
        table.untrack_all(1001);
    }

    #[test]
    fn redirect_to_a_resp3_subscriber_gets_the_push_and_flush_is_null() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, _src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1011, src_tx);
        table.set_redirect(1011, Some(1012));
        let (ib, ib_rx) = inbox(true);
        table.register_inbox(1012, ib);
        let key = Bytes::from_static(b"k3");
        table.track_key(1011, &key, false);
        let recipients = table.invalidate_key(&key, 7);
        deliver_all(&recipients, &mut TrackingMessage::keys(&[key]));
        assert_eq!(
            ib_rx.try_recv().unwrap(),
            Bytes::from_static(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\nk3\r\n")
        );

        // FLUSHALL follows the redirect too.
        let recipients = table.invalidate_all();
        deliver_all(&recipients, &mut TrackingMessage::flush());
        assert_eq!(
            ib_rx.try_recv().unwrap(),
            Bytes::from_static(b">2\r\n$10\r\ninvalidate\r\n_\r\n")
        );
        table.untrack_all(1011);
    }

    #[test]
    fn resp2_flush_reaches_the_redirect_target_as_a_null_message() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1021, src_tx);
        table.set_redirect(1021, Some(1022));
        let (ib, ib_rx) = inbox(false);
        table.register_inbox(1022, ib);
        let recipients = table.invalidate_all();
        deliver_all(&recipients, &mut TrackingMessage::flush());
        assert_eq!(
            ib_rx.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n")
        );
        assert!(
            src_rx.try_recv().is_err(),
            "the flush went to the source instead of its target"
        );
        table.untrack_all(1021);
    }

    /// A target that exists but has neither subscribed nor enabled tracking
    /// cannot receive anything; the redirect is NOT broken (redis drops the
    /// message the same way).
    #[test]
    fn redirect_to_a_live_unreachable_target_drops_silently() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1031, src_tx);
        table.set_redirect(1031, Some(1032));
        let key = Bytes::from_static(b"k");
        table.track_key(1031, &key, false);
        assert!(table.invalidate_key(&key, 7).is_empty());
        assert!(src_rx.try_recv().is_err());
        assert!(!table.is_redirect_broken(1031));
        table.untrack_all(1031);
    }

    /// The target disconnected: the source is told `tracking-redir-broken`
    /// once per undeliverable message, and the flag sticks until tracking is
    /// re-enabled.
    #[test]
    fn redirect_to_a_vanished_target_is_broken() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1041, src_tx);
        table.set_redirect(1041, Some(41)); // 41 < 1000: not connected
        let key = Bytes::from_static(b"k");
        table.track_key(1041, &key, false);
        let recipients = table.invalidate_key(&key, 7);
        deliver_all(&recipients, &mut TrackingMessage::keys(&[key]));
        assert_eq!(src_rx.try_recv().unwrap(), redir_broken_push(41));
        assert!(table.is_redirect_broken(1041));

        // A registered inbox for the target means it is back / reachable.
        let (ib, _ib_rx) = inbox(false);
        table.register_inbox(41, ib);
        assert!(table.client_exists(41));
        // Re-enabling tracking clears the flag.
        table.set_redirect(1041, None);
        assert!(!table.is_redirect_broken(1041));
        table.untrack_all(1041);
        table.unregister_inbox(41);
        assert!(!table.client_exists(41));
    }

    /// Without a redirect, delivery goes to the tracker's own channel —
    /// even when the tracker has a pub/sub inbox (redis sends a RESP2
    /// connection's OWN invalidations nowhere, never as a pub/sub message).
    #[test]
    fn own_invalidations_never_use_the_pubsub_inbox() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (tx, rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1051, tx);
        let (ib, ib_rx) = inbox(false);
        table.register_inbox(1051, ib);
        let key = Bytes::from_static(b"own");
        table.track_key(1051, &key, false);
        let recipients = table.invalidate_key(&key, 7);
        deliver_all(
            &recipients,
            &mut TrackingMessage::keys(std::slice::from_ref(&key)),
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            invalidation::invalidation_push(&[key])
        );
        assert!(ib_rx.try_recv().is_err());
        table.untrack_all(1051);
        table.unregister_inbox(1051);
    }

    /// A BCAST source redirects like a default-mode one.
    #[test]
    fn bcast_redirect_reaches_the_target() {
        let mut table = TrackingTable::new().with_liveness(connected_above_1000);
        let (src_tx, _src_rx) = channel::mpsc_unbounded::<Frame>();
        table.register_client(1061, src_tx);
        table.set_redirect(1061, Some(1062));
        table.register_prefix(1061, Bytes::from_static(b"p:"), false);
        // Same prefix twice registers once.
        table.register_prefix(1061, Bytes::from_static(b"p:"), false);
        let (ib, ib_rx) = inbox(false);
        table.register_inbox(1062, ib);
        let recipients = table.invalidate_key(&Bytes::from_static(b"p:1"), 7);
        assert_eq!(recipients.len(), 1, "one prefix, one message");
        deliver_all(
            &recipients,
            &mut TrackingMessage::keys(&[Bytes::from_static(b"p:1")]),
        );
        assert!(ib_rx.try_recv().is_ok());
        table.untrack_all(1061);
    }

    // ── moon#1049: CACHING ──────────────────────────────────────────────

    fn state(optin: bool, optout: bool) -> TrackingState {
        TrackingState {
            enabled: true,
            optin,
            optout,
            ..TrackingState::default()
        }
    }

    #[test]
    fn optin_tracks_only_after_caching_yes_and_only_for_the_next_command() {
        let mut s = state(true, false);
        s.before_command(b"GET", false);
        assert!(!s.tracks_reads(), "OPTIN without CACHING yes");
        s.before_command(b"CLIENT", false);
        s.caching = true; // CLIENT CACHING yes
        s.before_command(b"GET", false);
        assert!(s.tracks_reads(), "the command right after CACHING yes");
        s.before_command(b"GET", false);
        assert!(!s.tracks_reads(), "the flag covers ONE command");
    }

    #[test]
    fn optout_skips_only_the_command_after_caching_no() {
        let mut s = state(false, true);
        s.before_command(b"GET", false);
        assert!(s.tracks_reads(), "OPTOUT tracks by default");
        s.before_command(b"client", false);
        s.caching = true; // CLIENT CACHING no
        s.before_command(b"GET", false);
        assert!(!s.tracks_reads());
        s.before_command(b"GET", false);
        assert!(s.tracks_reads());
    }

    /// Redis keeps the flag across any CLIENT command and across an open
    /// MULTI, and drops it after EXEC.
    #[test]
    fn caching_survives_client_commands_and_an_open_transaction() {
        let mut s = state(true, false);
        s.before_command(b"CLIENT", false);
        s.caching = true;
        s.before_command(b"CLIENT", false); // CLIENT ID
        s.before_command(b"MULTI", false);
        assert!(s.caching);
        s.before_command(b"GET", true); // queued
        s.before_command(b"GET", true); // queued
        s.before_command(b"EXEC", true);
        assert!(s.tracks_reads(), "EXEC runs the body with the flag set");
        s.before_command(b"GET", false);
        assert!(!s.tracks_reads(), "cleared after EXEC");
    }

    /// A CACHING queued inside MULTI takes effect when EXEC runs it, and is
    /// gone for the command after EXEC.
    #[test]
    fn caching_set_during_exec_does_not_leak_past_it() {
        let mut s = state(true, false);
        s.before_command(b"MULTI", false);
        s.before_command(b"CLIENT", true); // queued CACHING yes
        s.before_command(b"EXEC", true);
        s.caching = true; // EXEC ran the queued CACHING yes
        s.before_command(b"GET", false);
        assert!(!s.tracks_reads());
    }

    #[test]
    fn default_and_bcast_modes_ignore_the_flag() {
        let mut s = state(false, false);
        s.caching = true;
        assert!(s.tracks_reads(), "default mode tracks every read");
        s.bcast = true;
        assert!(!s.tracks_reads(), "BCAST never tracks reads");
        let off = TrackingState::default();
        assert!(!off.tracks_reads());
    }
}
