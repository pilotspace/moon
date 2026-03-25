//! Runtime-agnostic CancellationToken replacing tokio_util::sync::CancellationToken.
//!
//! Uses AtomicBool + waker list for zero-runtime-dependency cancellation signaling.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

struct Inner {
    cancelled: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cancelled: AtomicBool::new(false),
                wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Signal cancellation and wake all waiters.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        let wakers = std::mem::take(&mut *self.inner.wakers.lock());
        for w in wakers {
            w.wake();
        }
    }

    /// Check if cancellation has been signaled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Returns a future that completes when cancellation is signaled.
    pub fn cancelled(&self) -> CancelledFuture {
        CancelledFuture {
            token: self.clone(),
        }
    }

    /// Create a child token. For simplicity, shares the same inner state
    /// (cancelling parent cancels child). This matches the codebase usage
    /// where child_token is used for shared shutdown signaling.
    pub fn child_token(&self) -> Self {
        self.clone()
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CancelledFuture {
    token: CancellationToken,
}

impl Future for CancelledFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        // Register waker before double-checking
        self.token.inner.wakers.lock().push(cx.waker().clone());
        // Double-check after registering waker to avoid lost wake-up
        if self.token.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_token_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_cancel_sets_flag() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_clone_shares_state() {
        let token = CancellationToken::new();
        let clone = token.clone();
        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn test_child_token_shares_state() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn test_default_not_cancelled() {
        let token = CancellationToken::default();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_multiple_cancel_calls_safe() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel(); // Should not panic
        assert!(token.is_cancelled());
    }
}
