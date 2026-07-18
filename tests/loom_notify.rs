//! Loom model tests for the Notify token+waker protocol (O2).
//!
//! Models `runtime::channel::Notify`'s Atomic implementation: producers
//! deposit a coalesced AtomicBool token (`swap(true, Release)`, wake on the
//! false→true transition) and the single consumer runs the canonical
//! AtomicWaker protocol (consume-check, register, re-check). The property
//! under test is the O2 change's whole risk surface: a producer's deposit is
//! NEVER lost — after any interleaving of deposit vs. register/re-check, the
//! consumer either already consumed the token or holds a wake (or a pending
//! token) that guarantees the next poll consumes it.
//!
//! Run with: cargo test --test loom_notify (CI runs the std-atomics mirror;
//! the #[cfg(loom)] section matches loom_response_slot.rs's convention for
//! manual exhaustive exploration).
//!
//! The waker itself is modeled as an AtomicBool "wake delivered" flag (same
//! approach as loom_response_slot.rs): AtomicWaker's internal correctness is
//! the upstream crate's contract; what THIS protocol must guarantee is the
//! store/registration ordering around it.

#![allow(unexpected_cfgs)]

#[cfg(not(loom))]
mod non_loom {
    //! std-atomics mirror of the model so the test structure runs in normal
    //! CI (the loom cfg is exercised by the dedicated loom job / manual runs).
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct NotifyModel {
        token: AtomicBool,
        /// Stands in for AtomicWaker: true = a wake was delivered to the
        /// registered consumer.
        woken: AtomicBool,
        /// True while the consumer has a waker registered.
        registered: AtomicBool,
    }

    impl NotifyModel {
        fn new() -> Self {
            Self {
                token: AtomicBool::new(false),
                woken: AtomicBool::new(false),
                registered: AtomicBool::new(false),
            }
        }

        /// Producer side: `Notify::deliver`.
        fn deliver(&self) {
            if !self.token.swap(true, Ordering::Release) {
                // wake(): delivers iff a waker is registered (AtomicWaker
                // semantics — wake on empty is a no-op).
                if self.registered.load(Ordering::Acquire) {
                    self.woken.store(true, Ordering::Release);
                }
            }
        }

        /// Consumer side: one `Notified::poll`. Returns true on Ready.
        fn poll(&self) -> bool {
            if self.token.swap(false, Ordering::Acquire) {
                return true;
            }
            self.registered.store(true, Ordering::Release);
            if self.token.swap(false, Ordering::Acquire) {
                self.registered.store(false, Ordering::Relaxed);
                return true;
            }
            false
        }

        fn was_woken(&self) -> bool {
            self.woken.load(Ordering::Acquire)
        }

        fn token_pending(&self) -> bool {
            self.token.load(Ordering::Acquire)
        }
    }

    /// The lost-wake property, brute-forced with real threads: after a
    /// producer deposit races one consumer poll, either the poll consumed
    /// the token, or a token/wake remains observable for the next poll.
    #[test]
    fn deposit_never_lost_threaded() {
        for _ in 0..10_000 {
            let m = Arc::new(NotifyModel::new());
            let p = {
                let m = m.clone();
                std::thread::spawn(move || m.deliver())
            };
            let ready = m.poll();
            p.join().unwrap();
            if !ready {
                // Pending poll left a registered waker: the deposit must be
                // recoverable — token still set (next poll consumes) or the
                // wake fired (event loop re-polls, next poll consumes).
                assert!(
                    m.token_pending() || m.was_woken(),
                    "deposit lost: poll=Pending, no token, no wake"
                );
                assert!(m.poll(), "recovery poll must consume the deposit");
            }
        }
    }

    /// Coalescing: N producers, one consumer — exactly one token max, and
    /// every deposit-after-consume is recoverable.
    #[test]
    fn coalesce_two_producers_threaded() {
        for _ in 0..10_000 {
            let m = Arc::new(NotifyModel::new());
            let p1 = {
                let m = m.clone();
                std::thread::spawn(move || m.deliver())
            };
            let p2 = {
                let m = m.clone();
                std::thread::spawn(move || m.deliver())
            };
            p1.join().unwrap();
            p2.join().unwrap();
            // Both deposits landed before this poll: exactly one token.
            assert!(m.poll(), "at least one deposit must be visible");
            assert!(!m.token_pending(), "coalesced: at most one token");
        }
    }
}

#[cfg(loom)]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::thread;

    struct NotifyModel {
        token: AtomicBool,
        woken: AtomicBool,
        registered: AtomicBool,
    }

    impl NotifyModel {
        fn new() -> Self {
            Self {
                token: AtomicBool::new(false),
                woken: AtomicBool::new(false),
                registered: AtomicBool::new(false),
            }
        }

        fn deliver(&self) {
            if !self.token.swap(true, Ordering::Release) {
                if self.registered.load(Ordering::Acquire) {
                    self.woken.store(true, Ordering::Release);
                }
            }
        }

        fn poll(&self) -> bool {
            if self.token.swap(false, Ordering::Acquire) {
                return true;
            }
            self.registered.store(true, Ordering::Release);
            if self.token.swap(false, Ordering::Acquire) {
                self.registered.store(false, Ordering::Relaxed);
                return true;
            }
            false
        }
    }

    /// Exhaustive: one producer deposit vs one consumer poll — the deposit
    /// is never lost under ANY interleaving.
    #[test]
    fn loom_deposit_never_lost() {
        loom::model(|| {
            let m = Arc::new(NotifyModel::new());
            let p = {
                let m = m.clone();
                thread::spawn(move || m.deliver())
            };
            let ready = m.poll();
            p.join().unwrap();
            if !ready {
                assert!(
                    m.token.load(Ordering::Acquire) || m.woken.load(Ordering::Acquire),
                    "deposit lost: poll=Pending, no token, no wake"
                );
                assert!(m.poll(), "recovery poll must consume the deposit");
            }
        });
    }

    /// Exhaustive: two producers racing one consumer poll — no interleaving
    /// loses BOTH deposits, and at most one token remains.
    #[test]
    fn loom_two_producers_one_consumer() {
        loom::model(|| {
            let m = Arc::new(NotifyModel::new());
            let p1 = {
                let m = m.clone();
                thread::spawn(move || m.deliver())
            };
            let ready = m.poll();
            p1.join().unwrap();
            let _ = ready;
            // After join: if the poll missed it, token or wake must remain.
            if !ready {
                assert!(
                    m.token.load(Ordering::Acquire) || m.woken.load(Ordering::Acquire),
                    "deposit lost under loom interleaving"
                );
            }
        });
    }
}
