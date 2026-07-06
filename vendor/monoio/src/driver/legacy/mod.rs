//! Monoio Legacy Driver.

use std::{
    cell::UnsafeCell,
    io,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use super::{
    op::{CompletionMeta, Op, OpAble},
    ready::{self, Ready},
    scheduled_io::ScheduledIo,
    Driver, Inner, CURRENT,
};
use crate::utils::slab::Slab;

#[allow(missing_docs, unreachable_pub, dead_code, unused_imports)]
#[cfg(windows)]
pub(super) mod iocp;

#[cfg(feature = "sync")]
mod waker;
#[cfg(feature = "sync")]
pub(crate) use waker::UnparkHandle;

pub(crate) struct LegacyInner {
    pub(crate) io_dispatch: Slab<ScheduledIo>,
    #[cfg(unix)]
    events: mio::Events,
    #[cfg(unix)]
    poll: mio::Poll,
    #[cfg(windows)]
    events: iocp::Events,
    #[cfg(windows)]
    poll: iocp::Poller,

    #[cfg(feature = "sync")]
    shared_waker: std::sync::Arc<waker::EventWaker>,

    // Waker receiver
    #[cfg(feature = "sync")]
    waker_receiver: flume::Receiver<std::task::Waker>,
}

/// Driver with Poll-like syscall.
#[allow(unreachable_pub)]
pub struct LegacyDriver {
    inner: Rc<UnsafeCell<LegacyInner>>,

    // Used for drop
    #[cfg(feature = "sync")]
    thread_id: usize,
}

#[cfg(feature = "sync")]
const TOKEN_WAKEUP: mio::Token = mio::Token(1 << 31);

// moon patch: programmatic spin-budget override (host CLI flag path).
// u64::MAX = unset -> fall back to the MOON_EPOLL_SPIN_US env. Must be set by
// the host before runtime threads first park.
pub(crate) static SPIN_BUDGET_US: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(u64::MAX);

// moon patch: epoll spin budget (0 = disabled): programmatic override first,
// else MOON_EPOLL_SPIN_US env, read once. See inner_park for the poll-mode
// rationale.
fn moon_epoll_spin_budget_us() -> u64 {
    let ovr = SPIN_BUDGET_US.load(std::sync::atomic::Ordering::Relaxed);
    if ovr != u64::MAX {
        return ovr;
    }
    static SPIN_US: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SPIN_US.get_or_init(|| {
        std::env::var("MOON_EPOLL_SPIN_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

// moon patch: per-thread spin-park hooks (skip-notify handshake with the
// host's cross-shard mesh). `advertise(true)` is called at spin entry and
// `advertise(false)` at every spin exit; the host publishes this so remote
// producers can elide their cross-thread wake (flume send + waker relay +
// eventfd write) while this thread is polling anyway. `probe()` is called
// each spin iteration AND once after `advertise(false)` (the Dekker final
// check — a producer that skipped its wake concurrently with spin exit is
// caught either by its own flag re-read or by this probe): it returns true
// when host-level work (SPSC ringbuf items) is pending, after delivering a
// thread-local wake to the host task so the executor runs it on return.
// Not Send: registered and invoked on this driver's thread only.
struct MoonSpinHooks {
    advertise: Box<dyn Fn(bool)>,
    probe: Box<dyn Fn() -> bool>,
}

thread_local! {
    static MOON_SPIN_HOOKS: std::cell::RefCell<Option<MoonSpinHooks>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_spin_hooks(advertise: Box<dyn Fn(bool)>, probe: Box<dyn Fn() -> bool>) {
    MOON_SPIN_HOOKS.with(|h| *h.borrow_mut() = Some(MoonSpinHooks { advertise, probe }));
}

// Short borrows per call: the closures run host code (ringbuf checks, a
// local flume send) that must not observe a held RefCell borrow.
fn moon_spin_hooks_installed() -> bool {
    MOON_SPIN_HOOKS.with(|h| h.borrow().is_some())
}

fn moon_spin_advertise(spinning: bool) {
    MOON_SPIN_HOOKS.with(|h| {
        if let Some(hooks) = &*h.borrow() {
            (hooks.advertise)(spinning);
        }
    });
}

fn moon_spin_probe() -> bool {
    MOON_SPIN_HOOKS.with(|h| {
        h.borrow()
            .as_ref()
            .map_or(false, |hooks| (hooks.probe)())
    })
}

// moon patch: idle disengage for the spin-poll. The spin exists to delete
// wake latency UNDER TRAFFIC; without a gate an idle thread burns the full
// budget on EVERY park forever (~budget/park-interval CPU per shard thread,
// e.g. ~4%/core at 40µs vs 1ms timer parks — the "zombie CPU eater" when a
// stray server is orphaned). Once this thread has seen no readiness events
// and no host work for MOON_SPIN_IDLE_DISENGAGE_US (default 10ms; 0 = never
// disengage), parks fall back to plain blocking polls. The next real event
// still arrives via the normal wake path (a one-time ~µs penalty) and
// re-arms the spin. Timer-only wakeups (empty poll after timeout) do NOT
// count as events, so a quiescent server stays disengaged.
thread_local! {
    static MOON_SPIN_LAST_EVENT: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

fn moon_spin_idle_disengage_us() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MOON_SPIN_IDLE_DISENGAGE_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000)
    })
}

fn moon_spin_engaged() -> bool {
    let win = moon_spin_idle_disengage_us();
    if win == 0 {
        return true;
    }
    MOON_SPIN_LAST_EVENT.with(|c| match c.get() {
        // First park on this thread: start the window now (covers startup).
        None => {
            c.set(Some(std::time::Instant::now()));
            true
        }
        Some(t) => t.elapsed() < std::time::Duration::from_micros(win),
    })
}

fn moon_spin_note_event() {
    MOON_SPIN_LAST_EVENT.with(|c| c.set(Some(std::time::Instant::now())));
}

#[allow(dead_code)]
impl LegacyDriver {
    const DEFAULT_ENTRIES: u32 = 1024;

    pub(crate) fn new() -> io::Result<Self> {
        Self::new_with_entries(Self::DEFAULT_ENTRIES)
    }

    pub(crate) fn new_with_entries(entries: u32) -> io::Result<Self> {
        #[cfg(unix)]
        let poll = mio::Poll::new()?;
        #[cfg(windows)]
        let poll = iocp::Poller::new()?;

        #[cfg(all(unix, feature = "sync"))]
        let shared_waker = std::sync::Arc::new(waker::EventWaker::new(mio::Waker::new(
            poll.registry(),
            TOKEN_WAKEUP,
        )?));
        #[cfg(all(windows, feature = "sync"))]
        let shared_waker = std::sync::Arc::new(waker::EventWaker::new(iocp::Waker::new(
            &poll,
            TOKEN_WAKEUP,
        )?));
        #[cfg(feature = "sync")]
        let (waker_sender, waker_receiver) = flume::unbounded::<std::task::Waker>();
        #[cfg(feature = "sync")]
        let thread_id = crate::builder::BUILD_THREAD_ID.with(|id| *id);

        let inner = LegacyInner {
            io_dispatch: Slab::new(),
            #[cfg(unix)]
            events: mio::Events::with_capacity(entries as usize),
            #[cfg(unix)]
            poll,
            #[cfg(windows)]
            events: iocp::Events::with_capacity(entries as usize),
            #[cfg(windows)]
            poll,
            #[cfg(feature = "sync")]
            shared_waker,
            #[cfg(feature = "sync")]
            waker_receiver,
        };
        let driver = Self {
            inner: Rc::new(UnsafeCell::new(inner)),
            #[cfg(feature = "sync")]
            thread_id,
        };

        // Register unpark handle
        #[cfg(feature = "sync")]
        {
            let unpark = driver.unpark();
            super::thread::register_unpark_handle(thread_id, unpark.into());
            super::thread::register_waker_sender(thread_id, waker_sender);
        }

        Ok(driver)
    }

    fn inner_park(&self, mut timeout: Option<Duration>) -> io::Result<()> {
        let inner = unsafe { &mut *self.inner.get() };

        #[allow(unused_mut)]
        let mut need_wait = true;
        #[cfg(feature = "sync")]
        {
            // Process foreign wakers
            while let Ok(w) = inner.waker_receiver.try_recv() {
                w.wake();
                need_wait = false;
            }

            // Set status as not awake if we are going to sleep
            if need_wait {
                inner
                    .shared_waker
                    .awake
                    .store(false, std::sync::atomic::Ordering::Release);
            }

            // Process foreign wakers left
            while let Ok(w) = inner.waker_receiver.try_recv() {
                w.wake();
                need_wait = false;
            }
        }

        if !need_wait {
            timeout = Some(Duration::ZERO);
        }

        // here we borrow 2 mut self, but its safe.
        let events = unsafe { &mut (*self.inner.get()).events };

        // ---- moon patch: readiness spin-poll before blocking (MOON_EPOLL_SPIN_US) ----
        // Poll-mode for request/response workloads: instead of sleeping in
        // epoll_wait and paying the scheduler wake on every op, busy-loop
        // zero-timeout polls (~0.3µs each; readiness is published by softirq
        // with no wake machinery) for a bounded window, falling back to the
        // stock blocking poll on miss. Foreign wakes surface during the spin
        // too: the waker eventfd is registered in this same mio poll. Off
        // unless MOON_EPOLL_SPIN_US is set; zero behavior change otherwise.
        let mut polled = false;
        #[cfg(unix)]
        if need_wait {
            let spin_us = moon_epoll_spin_budget_us();
            // Idle disengage (see MOON_SPIN_LAST_EVENT above): only pay the
            // spin budget while events have arrived recently.
            if spin_us > 0 && moon_spin_engaged() {
                let budget = match timeout {
                    Some(d) => d.min(Duration::from_micros(spin_us)),
                    None => Duration::from_micros(spin_us),
                };
                let deadline = std::time::Instant::now() + budget;
                // Skip-notify handshake (see MoonSpinHooks above): advertise
                // the spin window so remote producers elide their wake; probe
                // host ringbufs each iteration in their stead.
                let hooks = moon_spin_hooks_installed();
                if hooks {
                    moon_spin_advertise(true);
                }
                let mut probe_hit = false;
                loop {
                    match inner.poll.poll(events, Some(Duration::ZERO)) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => {
                            if hooks {
                                moon_spin_advertise(false);
                            }
                            return Err(e);
                        }
                    }
                    if !events.is_empty() {
                        polled = true;
                        moon_spin_note_event();
                        break;
                    }
                    if hooks && moon_spin_probe() {
                        probe_hit = true;
                        break;
                    }
                    if std::time::Instant::now() >= deadline {
                        break;
                    }
                    for _ in 0..16 {
                        std::hint::spin_loop();
                    }
                }
                if hooks {
                    // Retract BEFORE the final probe (SeqCst handshake lives
                    // in the host closures): a producer that skipped its wake
                    // during our exit is guaranteed visible to this probe, or
                    // it re-reads the cleared flag and sends normally. Runs on
                    // ALL exits — an fd-event exit can also have raced a
                    // skipped notify.
                    moon_spin_advertise(false);
                    if !probe_hit && moon_spin_probe() {
                        probe_hit = true;
                    }
                }
                if probe_hit {
                    // Host work is pending and a local wake was delivered —
                    // return to the executor instead of blocking.
                    polled = true;
                    moon_spin_note_event();
                }
            }
        }
        // ---- end moon patch ----

        if !polled {
            match inner.poll.poll(events, timeout) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
            // moon patch: real fd/waker events (not bare timer expiry)
            // re-arm the idle-disengaged spin for the next park.
            #[cfg(unix)]
            if events.iter().next().is_some() {
                moon_spin_note_event();
            }
        }
        #[cfg(unix)]
        let iter = events.iter();
        #[cfg(windows)]
        let iter = events.events.iter();
        for event in iter {
            let token = event.token();

            #[cfg(feature = "sync")]
            if token != TOKEN_WAKEUP {
                inner.dispatch(token, Ready::from_mio(event));
            }

            #[cfg(not(feature = "sync"))]
            inner.dispatch(token, Ready::from_mio(event));
        }
        Ok(())
    }

    #[cfg(windows)]
    pub(crate) fn register(
        this: &Rc<UnsafeCell<LegacyInner>>,
        state: &mut iocp::SocketState,
        interest: mio::Interest,
    ) -> io::Result<usize> {
        let inner = unsafe { &mut *this.get() };
        let io = ScheduledIo::default();
        let token = inner.io_dispatch.insert(io);

        match inner.poll.register(state, mio::Token(token), interest) {
            Ok(_) => Ok(token),
            Err(e) => {
                inner.io_dispatch.remove(token);
                Err(e)
            }
        }
    }

    #[cfg(windows)]
    pub(crate) fn deregister(
        this: &Rc<UnsafeCell<LegacyInner>>,
        token: usize,
        state: &mut iocp::SocketState,
    ) -> io::Result<()> {
        let inner = unsafe { &mut *this.get() };

        // try to deregister fd first, on success we will remove it from slab.
        match inner.poll.deregister(state) {
            Ok(_) => {
                inner.io_dispatch.remove(token);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(unix)]
    pub(crate) fn register(
        this: &Rc<UnsafeCell<LegacyInner>>,
        source: &mut impl mio::event::Source,
        interest: mio::Interest,
    ) -> io::Result<usize> {
        let inner = unsafe { &mut *this.get() };
        let token = inner.io_dispatch.insert(ScheduledIo::new());

        let registry = inner.poll.registry();
        match registry.register(source, mio::Token(token), interest) {
            Ok(_) => Ok(token),
            Err(e) => {
                inner.io_dispatch.remove(token);
                Err(e)
            }
        }
    }

    #[cfg(unix)]
    pub(crate) fn deregister(
        this: &Rc<UnsafeCell<LegacyInner>>,
        token: usize,
        source: &mut impl mio::event::Source,
    ) -> io::Result<()> {
        let inner = unsafe { &mut *this.get() };

        // try to deregister fd first, on success we will remove it from slab.
        match inner.poll.registry().deregister(source) {
            Ok(_) => {
                inner.io_dispatch.remove(token);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl LegacyInner {
    fn dispatch(&mut self, token: mio::Token, ready: Ready) {
        let mut sio = match self.io_dispatch.get(token.0) {
            Some(io) => io,
            None => {
                return;
            }
        };
        let ref_mut = sio.as_mut();
        ref_mut.set_readiness(|curr| curr | ready);
        ref_mut.wake(ready);
    }

    pub(crate) fn poll_op<T: OpAble>(
        this: &Rc<UnsafeCell<Self>>,
        data: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<CompletionMeta> {
        let inner = unsafe { &mut *this.get() };
        let (direction, index) = match data.legacy_interest() {
            Some(x) => x,
            None => {
                // if there is no index provided, it means the action does not rely on fd
                // readiness. do syscall right now.
                return Poll::Ready(CompletionMeta {
                    result: OpAble::legacy_call(data),
                    flags: 0,
                });
            }
        };

        // wait io ready and do syscall
        let mut scheduled_io = inner.io_dispatch.get(index).expect("scheduled_io lost");
        let ref_mut = scheduled_io.as_mut();

        let readiness = ready!(ref_mut.poll_readiness(cx, direction));

        // check if canceled
        if readiness.is_canceled() {
            // clear CANCELED part only
            ref_mut.clear_readiness(readiness & Ready::CANCELED);
            return Poll::Ready(CompletionMeta {
                result: Err(io::Error::from_raw_os_error(125)),
                flags: 0,
            });
        }

        match OpAble::legacy_call(data) {
            Ok(n) => Poll::Ready(CompletionMeta {
                result: Ok(n),
                flags: 0,
            }),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                ref_mut.clear_readiness(direction.mask());
                ref_mut.set_waker(cx, direction);
                Poll::Pending
            }
            Err(e) => Poll::Ready(CompletionMeta {
                result: Err(e),
                flags: 0,
            }),
        }
    }

    pub(crate) fn cancel_op(
        this: &Rc<UnsafeCell<LegacyInner>>,
        index: usize,
        direction: ready::Direction,
    ) {
        let inner = unsafe { &mut *this.get() };
        let ready = match direction {
            ready::Direction::Read => Ready::READ_CANCELED,
            ready::Direction::Write => Ready::WRITE_CANCELED,
        };
        inner.dispatch(mio::Token(index), ready);
    }

    pub(crate) fn submit_with_data<T>(
        this: &Rc<UnsafeCell<LegacyInner>>,
        data: T,
    ) -> io::Result<Op<T>>
    where
        T: OpAble,
    {
        Ok(Op {
            driver: Inner::Legacy(this.clone()),
            // useless for legacy
            index: 0,
            data: Some(data),
        })
    }

    #[cfg(feature = "sync")]
    pub(crate) fn unpark(this: &Rc<UnsafeCell<LegacyInner>>) -> waker::UnparkHandle {
        let inner = unsafe { &*this.get() };
        let weak = std::sync::Arc::downgrade(&inner.shared_waker);
        waker::UnparkHandle(weak)
    }
}

impl Driver for LegacyDriver {
    fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        let inner = Inner::Legacy(self.inner.clone());
        CURRENT.set(&inner, f)
    }

    fn submit(&self) -> io::Result<()> {
        // wait with timeout = 0
        self.park_timeout(Duration::ZERO)
    }

    fn park(&self) -> io::Result<()> {
        self.inner_park(None)
    }

    fn park_timeout(&self, duration: Duration) -> io::Result<()> {
        self.inner_park(Some(duration))
    }

    #[cfg(feature = "sync")]
    type Unpark = waker::UnparkHandle;

    #[cfg(feature = "sync")]
    fn unpark(&self) -> Self::Unpark {
        LegacyInner::unpark(&self.inner)
    }
}

impl Drop for LegacyDriver {
    fn drop(&mut self) {
        // Deregister thread id
        #[cfg(feature = "sync")]
        {
            use crate::driver::thread::{unregister_unpark_handle, unregister_waker_sender};
            unregister_unpark_handle(self.thread_id);
            unregister_waker_sender(self.thread_id);
        }
    }
}
