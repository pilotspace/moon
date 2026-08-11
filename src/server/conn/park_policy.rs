//! When may an idle connection be parked? (c10k D2)
//!
//! The park itself is monoio-only (`handler_monoio::idle_park`), but the
//! *policy* is pure logic with no runtime dependency, and it lives here
//! deliberately so it is testable under both feature sets. Every CI test job
//! builds `--no-default-features --features runtime-tokio`, so a test placed
//! inside the `#[cfg(feature = "runtime-monoio")]` handler tree never runs in
//! CI — a security predicate guarded by invisible tests is guarded by nothing.

/// Does the connection's buffer state permit a stage-2 park?
///
/// This used to be `read_buf.is_empty() && write_buf.is_empty()` inline in the
/// monoio handler, and the `read_buf` half was an unauthenticated DoS: one byte
/// — `*`, the first character of every RESP array — kept `read_buf` non-empty
/// permanently, which made the connection unparkable, which dropped it into the
/// handler's UNREGISTERED plain read. Nothing on that path carries a sweep
/// handle, so the connection became invisible to the idle sweep for as long as
/// the attacker left it open, holding its full stage-2 working set. Cost to the
/// attacker: one byte and one socket. At 1M connections, 10-15 GB that no
/// amount of idle time reclaims.
///
/// # Why there is no cap on the remainder
///
/// The obvious fix is "park only if the remainder is small" — and it is wrong.
/// Any threshold simply moves the attack: with a 512-byte cap, an attacker
/// sends 513 bytes of an incomplete frame instead of 1 and the connection is
/// invisible again, 513× more expensive and still free. A partial fix here
/// reads as a fix while leaving the vector open.
///
/// An unbounded remainder is safe because `read_buf` is *already* bounded, one
/// layer up: `client_query_buffer_limit` (and the smaller pre-auth ceiling) is
/// enforced after every read arm and ahead of both parse paths, precisely
/// because an incomplete frame is what makes `read_buf` grow. So the remainder
/// a park can carry is capped by an existing, configurable limit — adding a
/// second, arbitrary one would only reopen the hole between them.
///
/// Carrying the remainder is also strictly better than the alternative: it
/// moves those bytes from a ~10-15 KB live handler into the ~3.3 KB parked
/// state, where the ordinary sweep can reach them.
///
/// # Why `write_buf` is still strict
///
/// The two buffers are not symmetric. `read_buf` is unparsed *input*: it is
/// carried across the park in `MigratedConnectionState::read_buf_remainder`
/// and re-parsed on resume, so the partial frame continues exactly where it
/// left off. `write_buf` is a *reply the client is owed* and is carried
/// nowhere — parking with it non-empty would silently drop bytes the client is
/// waiting for, so it stays a strict emptiness check.
pub fn remainder_allows_park(read_buf_len: usize, write_buf_len: usize) -> bool {
    // `read_buf_len` is deliberately unused: see "Why there is no cap on the
    // remainder" above. It stays in the signature because the question "does
    // unparsed input block a park?" is exactly what this predicate answers,
    // and the answer being "no" is the fix.
    let _ = read_buf_len;
    write_buf_len == 0
}

#[cfg(test)]
mod park_policy_tests {
    use super::*;

    /// The D2 attack itself: one byte must not pin a connection awake.
    #[test]
    fn one_byte_partial_frame_still_parks() {
        assert!(
            remainder_allows_park(1, 0),
            "a 1-byte partial frame is the D2 attack: it MUST NOT pin the \
             connection out of the sweep's reach"
        );
    }

    /// The ordinary case must keep parking.
    #[test]
    fn empty_read_buf_still_parks() {
        assert!(remainder_allows_park(0, 0));
    }

    /// A cap on the remainder would only move the attack past the cap. This
    /// pins the decision: no size of unparsed input may block a park, because
    /// `client_query_buffer_limit` already bounds `read_buf` upstream.
    ///
    /// If someone later reintroduces a threshold here, this fails — which is
    /// the point.
    #[test]
    fn no_remainder_size_blocks_a_park() {
        for len in [512, 513, 8192, 64 * 1024, 512 * 1024, usize::MAX] {
            assert!(
                remainder_allows_park(len, 0),
                "a {len}-byte remainder must still park: any cap here is an \
                 escape hatch an attacker just sizes past"
            );
        }
    }

    /// A pending reply must NEVER park: unlike unparsed input, `write_buf` is
    /// not carried in `MigratedConnectionState`, so parking with it non-empty
    /// would silently drop bytes the client is owed.
    #[test]
    fn pending_reply_never_parks() {
        assert!(
            !remainder_allows_park(0, 1),
            "an unwritten reply must keep the connection awake — it is not \
             carried across the park"
        );
        assert!(!remainder_allows_park(1, 1));
        assert!(!remainder_allows_park(usize::MAX, 1));
    }
}
