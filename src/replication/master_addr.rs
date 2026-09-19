//! Resolve the master address named by `REPLICAOF host port` (moon#1034).
//!
//! Redis accepts any host at `REPLICAOF`, whether an IP literal or a DNS name,
//! and resolves it in the replica's connect loop, retrying with the link
//! reported `down` while resolution fails. moon's monoio replica task parsed
//! `"{host}:{port}"` as a `SocketAddr` with `.expect`, so `REPLICAOF localhost
//! 6379` panicked on the shard thread and the panic hook aborted the process.
//! The tokio task formatted the same string, which cannot express an IPv6
//! literal (`::1:6379`).
//!
//! [`MasterResolver`] is runtime-agnostic:
//! * An IP literal (`10.0.0.5`, `::1`, `[::1]`) resolves in place, with no
//!   thread and no DNS.
//! * A name is resolved with the system resolver (`getaddrinfo`, which
//!   BLOCKS) on a short-lived helper thread, never on the shard thread. The
//!   answer comes back over a `flume` channel.
//! * The wait is bounded by a caller-supplied deadline future (a runtime
//!   timer), so a hung resolver cannot park the task forever.
//! * At most one lookup is in flight per resolver. A lookup that outlives its
//!   deadline is kept and awaited again on the next attempt instead of
//!   spawning another thread, so a resolver that hangs costs one thread, not
//!   one per retry.

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::pin::pin;

use crate::runtime::race::{Arm, race2};

/// Result of one blocking resolution, sent back from the helper thread.
type Lookup = io::Result<Vec<SocketAddr>>;

/// Strip the brackets of an IPv6 literal written URL-style (`[::1]`).
///
/// `REPLICAOF [::1] 6379` is a natural way to type an IPv6 master, and the
/// system resolver rejects the bracketed form.
#[must_use]
pub fn normalize_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// The address of an IP-literal `host`, without touching the resolver.
#[must_use]
pub fn literal_addr(host: &str, port: u16) -> Option<SocketAddr> {
    normalize_host(host)
        .parse::<IpAddr>()
        .ok()
        .map(|ip| SocketAddr::new(ip, port))
}

/// Resolve `host:port` with the system resolver. BLOCKING: never call this
/// on a shard thread. [`MasterResolver`] runs it on a helper thread.
///
/// Returns every address in resolver order (a name such as `localhost`
/// commonly yields both `::1` and `127.0.0.1`), or an error. An answer with
/// no address is an error, so callers never hold an empty list.
pub fn resolve_blocking(host: &str, port: u16) -> Lookup {
    let host = normalize_host(host);
    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty master host",
        ));
    }
    if let Some(addr) = literal_addr(host, port) {
        return Ok(vec![addr]);
    }
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("master host {host:?} resolved to no address"),
        ));
    }
    Ok(addrs)
}

/// Off-thread, deadline-bounded resolver for one `REPLICAOF` target.
///
/// Owned by one replica task. Not `Clone`: the in-flight lookup belongs to
/// exactly one waiter.
pub struct MasterResolver {
    host: String,
    port: u16,
    /// The lookup still running from an earlier attempt whose deadline fired.
    pending: Option<flume::Receiver<Lookup>>,
    /// Helper threads spawned so far. Test observability for the
    /// one-in-flight bound; not used for control flow.
    lookups_started: usize,
}

impl MasterResolver {
    /// A resolver for `host:port` as the operator typed them.
    #[must_use]
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: normalize_host(host).to_owned(),
            port,
            pending: None,
            lookups_started: 0,
        }
    }

    /// Helper threads started so far.
    #[must_use]
    pub fn lookups_started(&self) -> usize {
        self.lookups_started
    }

    /// Resolve the target, giving up when `deadline` completes.
    ///
    /// Returns `ErrorKind::TimedOut` when the deadline wins. The lookup keeps
    /// running and the next call picks up its answer. Never panics. Every
    /// failure, from spawning the helper through to the resolver itself, is an
    /// `io::Error` for the caller's retry loop.
    pub async fn resolve<D: Future<Output = ()>>(&mut self, deadline: D) -> Lookup {
        if let Some(addr) = literal_addr(&self.host, self.port) {
            return Ok(vec![addr]);
        }
        let rx = match self.pending.take() {
            Some(rx) => rx,
            None => self.spawn_lookup()?,
        };
        let outcome = {
            let recv = pin!(rx.recv_async());
            let deadline = pin!(deadline);
            race2(recv, deadline).await
        };
        match outcome {
            Arm::First(Ok(lookup)) => lookup,
            Arm::First(Err(_disconnected)) => Err(io::Error::other(
                "master address resolver thread exited without an answer",
            )),
            Arm::Second(()) => {
                // Keep the running lookup: the next attempt awaits it rather
                // than stacking a second blocked thread behind it.
                self.pending = Some(rx);
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("resolving master host {:?} timed out", self.host),
                ))
            }
        }
    }

    fn spawn_lookup(&mut self) -> io::Result<flume::Receiver<Lookup>> {
        let (tx, rx) = flume::bounded(1);
        let host = self.host.clone();
        let port = self.port;
        std::thread::Builder::new()
            .name("moon-repl-resolve".into())
            .spawn(move || {
                // The receiver may be gone (task superseded); nothing to do.
                let _ = tx.send(resolve_blocking(&host, port));
            })?;
        self.lookups_started += 1;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    #[test]
    fn ip_literals_resolve_in_place_without_a_thread() {
        for (host, want) in [
            ("127.0.0.1", "127.0.0.1:6379"),
            ("::1", "[::1]:6379"),
            ("[::1]", "[::1]:6379"),
            ("10.1.2.3", "10.1.2.3:6379"),
        ] {
            let mut r = MasterResolver::new(host, 6379);
            let got = block_on(r.resolve(std::future::pending())).expect(host);
            assert_eq!(got, vec![want.parse::<SocketAddr>().expect(want)], "{host}");
            assert_eq!(r.lookups_started(), 0, "{host}: a literal needs no lookup");
        }
    }

    #[test]
    fn hostname_resolves_to_every_address() {
        // The moon#1034 reproducer: `SocketAddr::from_str("localhost:6379")`
        // fails, which is what the `.expect` turned into a process abort.
        assert!("localhost:6379".parse::<SocketAddr>().is_err());
        let mut r = MasterResolver::new("localhost", 6379);
        let got = block_on(r.resolve(std::future::pending())).expect("localhost resolves");
        assert!(!got.is_empty());
        assert!(
            got.iter().all(|a| a.ip().is_loopback() && a.port() == 6379),
            "{got:?}"
        );
        assert_eq!(r.lookups_started(), 1);
    }

    #[test]
    fn unresolvable_and_empty_hosts_are_errors_not_panics() {
        // RFC 6761: `.invalid` never resolves.
        let mut r = MasterResolver::new("moon-1034-no-such-master.invalid", 6379);
        assert!(block_on(r.resolve(std::future::pending())).is_err());
        let mut r = MasterResolver::new("", 6379);
        let e = block_on(r.resolve(std::future::pending())).expect_err("empty host");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(resolve_blocking("[]", 6379).is_err());
    }

    #[test]
    fn a_timed_out_lookup_is_reused_not_respawned() {
        let mut r = MasterResolver::new("localhost", 6379);
        // A deadline that has already passed: the race loses to it unless
        // the helper thread answered before the first poll.
        let first = block_on(r.resolve(std::future::ready(())));
        if let Err(e) = &first {
            assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        }
        // Whether or not the first call timed out, the second must not start
        // another thread while a lookup is pending, and must get the answer.
        let second = block_on(r.resolve(std::future::pending())).expect("answer arrives");
        assert!(!second.is_empty());
        assert_eq!(
            r.lookups_started(),
            if first.is_err() { 1 } else { 2 },
            "a pending lookup is awaited again, never stacked"
        );
    }

    /// A failed lookup is not cached: every retry asks the resolver again, so
    /// a DNS outage that later recovers is picked up by the reconnect loop.
    #[test]
    fn a_failed_lookup_is_retried_not_cached() {
        let mut r = MasterResolver::new("moon-1034-no-such-master.invalid", 6379);
        assert!(block_on(r.resolve(std::future::pending())).is_err());
        assert!(block_on(r.resolve(std::future::pending())).is_err());
        assert_eq!(r.lookups_started(), 2, "each attempt re-resolves");
    }

    #[test]
    fn normalize_host_only_strips_a_matched_bracket_pair() {
        assert_eq!(normalize_host("[::1]"), "::1");
        assert_eq!(normalize_host("[::1"), "[::1");
        assert_eq!(normalize_host("localhost"), "localhost");
        assert_eq!(literal_addr("[fe80::1]", 1), "[fe80::1]:1".parse().ok());
        assert_eq!(literal_addr("localhost", 1), None);
    }
}
