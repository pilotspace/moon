//! The monoio AOF writers' bounded channel receive (moon#1266): park-free
//! while writes flow, parked once they stop. See [`poll_recv`].

use super::*;

/// Poll step while the writer is WARM (a message arrived within the last
/// [`AOF_WARM_POLL_SPAN`]): the longest an acked record can sit in the
/// channel before this writer picks it up and `write(2)`s it while writes
/// are flowing (moon#1266). It used to be `wait/16` — ~3 ms while writing,
/// 50 ms for the first write after an idle second — and a kill -9 inside
/// that window lost acknowledged writes under everysec.
///
/// 500 us, measured (4-vCPU Linux, --shards 1, everysec, SET): 100 us cost
/// ~10-15% rps at p1 c50 and +20-28% server CPU per op, 500 us ran at
/// parity with the old step, and a kill -9 1 ms after the last ack lost as
/// often with either (the residue is writer stalls, not the step).
const AOF_WARM_POLL_STEP: std::time::Duration = std::time::Duration::from_micros(500);

/// [`AOF_WARM_POLL_STEP`], or `MOON_AOF_WARM_POLL_US` (10..=50,000 µs) — a
/// diagnostic override for same-binary A/B runs of the step's trade-off: a
/// shorter step narrows the kill -9 window, a longer one costs fewer writer
/// wake-ups (`docs/internal/env-knobs.md`). Read once per process.
fn warm_poll_step() -> std::time::Duration {
    static STEP: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *STEP.get_or_init(|| {
        std::env::var("MOON_AOF_WARM_POLL_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|us| std::time::Duration::from_micros(us.clamp(10, 50_000)))
            .unwrap_or(AOF_WARM_POLL_STEP)
    })
}

/// How long the writer keeps polling at [`AOF_WARM_POLL_STEP`] after its
/// last message before it parks on the channel instead.
const AOF_WARM_POLL_SPAN: std::time::Duration = std::time::Duration::from_millis(5);

/// Bounded receive for the std-thread writer loops under
/// `FsyncPolicy::EverySec`/`No`: park-free while writes are flowing, parked
/// once they stop.
///
/// A parked `recv_timeout` registers this thread as a flume waiter, so the
/// producer `try_send` that finds it pays a futex WAKE **on the shard
/// thread**. Parking after every batch cost 149,718 futex calls (63% of the
/// shard thread's syscall time) in an 8 s p1 SET run under everysec, so
/// while WARM (`warm`: the previous receive returned a message) the writer
/// polls with `try_recv` + [`AOF_WARM_POLL_STEP`] sleeps for up to
/// [`AOF_WARM_POLL_SPAN`] — producer sends stay pure userspace atomics, and
/// a queued record waits at most one short step.
///
/// Once the span passes with nothing queued, or when the previous receive
/// already timed out (COLD), it parks for the rest of `wait`. The first
/// record after an idle period then costs its producer ONE futex wake and
/// is picked up at once (moon#1266: it used to wait up to a 50 ms poll step,
/// and a kill -9 in that step lost it though it had been acknowledged). An
/// idle writer wakes only when `wait` elapses — at most 1/s once escalated.
fn poll_recv(
    rx: &channel::MpscReceiver<AofMessage>,
    wait: std::time::Duration,
    warm: bool,
) -> Result<AofMessage, flume::RecvTimeoutError> {
    let start = std::time::Instant::now();
    let deadline = start + wait;
    let warm_until = if warm {
        start + AOF_WARM_POLL_SPAN.min(wait)
    } else {
        start
    };
    loop {
        match rx.try_recv() {
            Ok(m) => return Ok(m),
            Err(flume::TryRecvError::Disconnected) => {
                return Err(flume::RecvTimeoutError::Disconnected);
            }
            Err(flume::TryRecvError::Empty) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(flume::RecvTimeoutError::Timeout);
                }
                if now >= warm_until {
                    return rx.recv_timeout(deadline - now);
                }
                std::thread::sleep(warm_poll_step());
            }
        }
    }
}

/// Bounded receive for the std-thread writer loops: parked under `Always`
/// (ack latency is client-visible), warm-polled then parked otherwise (see
/// [`poll_recv`]).
///
/// WARM is earned, not granted by any message (R1 review, finding 5): the
/// next receive polls only if THIS message arrived within
/// [`warm_keep_max_wait`] (two poll steps, 1 ms by default) of the receive
/// starting. Every message used to re-arm the 5 ms polling span, so one
/// client writing every ~4 ms kept the writer polling forever — ~1,700
/// wake-ups/s per shard, 4× the CPU of the old cadence — to save a producer
/// futex wake that costs nothing at that rate. A writer fed faster than one
/// message per 1 ms stays warm, so its pickup latency is unchanged; a slower
/// one parks, and each record costs its producer one futex wake (a few µs,
/// at most ~1,000/s) and is picked up at once.
pub(super) fn recv_next(
    rx: &channel::MpscReceiver<AofMessage>,
    idle_wait: &mut IdleWait,
    park: bool,
) -> Result<AofMessage, flume::RecvTimeoutError> {
    if park {
        return rx.recv_timeout(idle_wait.current());
    }
    let started = std::time::Instant::now();
    let got = poll_recv(rx, idle_wait.current(), idle_wait.warm());
    if got.is_ok() {
        idle_wait.warm = started.elapsed() <= warm_keep_max_wait();
    }
    got
}

/// A message that arrived within this long of its receive starting keeps
/// the writer WARM (see [`recv_next`]): two poll steps.
fn warm_keep_max_wait() -> std::time::Duration {
    warm_poll_step() * 2
}

#[cfg(test)]
mod poll_recv_tests {
    use super::*;

    #[test]
    fn returns_queued_message_immediately() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        assert!(
            tx.try_send(AofMessage::Append {
                lsn: 7,
                db: 0,
                bytes: bytes::Bytes::from_static(b"x"),
                epoch: FoldEpoch::INITIAL,
                clock_ms: 0,
            })
            .is_ok()
        );
        let start = std::time::Instant::now();
        let Ok(got) = poll_recv(&rx, std::time::Duration::from_secs(1), true) else {
            panic!("expected message");
        };
        assert!(matches!(got, AofMessage::Append { lsn: 7, .. }));
        // No sleep step should have been taken for an already-queued message.
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
    }

    #[test]
    fn picks_up_message_sent_mid_wait() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert!(
                tx.try_send(AofMessage::Append {
                    lsn: 1,
                    db: 0,
                    bytes: bytes::Bytes::from_static(b"y"),
                    epoch: FoldEpoch::INITIAL,
                    clock_ms: 0,
                })
                .is_ok()
            );
        });
        let Ok(got) = poll_recv(&rx, std::time::Duration::from_secs(5), true) else {
            panic!("expected message");
        };
        assert!(matches!(got, AofMessage::Append { lsn: 1, .. }));
        assert!(sender.join().is_ok());
    }

    #[test]
    fn times_out_when_empty() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let start = std::time::Instant::now();
        // AofMessage has no Debug impl — match instead of unwrap_err.
        let Err(err) = poll_recv(&rx, std::time::Duration::from_millis(30), true) else {
            panic!("expected timeout");
        };
        assert!(matches!(err, flume::RecvTimeoutError::Timeout));
        assert!(start.elapsed() >= std::time::Duration::from_millis(30));
    }

    #[test]
    fn reports_disconnect() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        drop(tx);
        // AofMessage has no Debug impl — match instead of unwrap_err.
        let Err(err) = poll_recv(&rx, std::time::Duration::from_secs(1), false) else {
            panic!("expected disconnect");
        };
        assert!(matches!(err, flume::RecvTimeoutError::Disconnected));
    }

    fn append(lsn: u64) -> AofMessage {
        AofMessage::Append {
            lsn,
            db: 0,
            bytes: bytes::Bytes::from_static(b"z"),
            epoch: FoldEpoch::INITIAL,
            clock_ms: 0,
        }
    }

    /// Median pickup latency over 5 sends, each made `after` into a
    /// `wait`-long receive (the median, so one scheduling hiccup on a loaded
    /// host does not decide the verdict).
    fn median_pickup(
        wait: std::time::Duration,
        warm: bool,
        after: std::time::Duration,
    ) -> std::time::Duration {
        let mut lat = Vec::with_capacity(5);
        for round in 0..5u64 {
            let (tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
            let sender = std::thread::spawn(move || {
                std::thread::sleep(after);
                let sent = std::time::Instant::now();
                assert!(tx.try_send(append(round)).is_ok());
                (sent, tx)
            });
            let Ok(_) = poll_recv(&rx, wait, warm) else {
                panic!("expected message");
            };
            let got = std::time::Instant::now();
            let Ok((sent, _tx)) = sender.join() else {
                panic!("sender panicked");
            };
            lat.push(got.saturating_duration_since(sent));
        }
        lat.sort();
        lat[2]
    }

    /// moon#1266: the first record after an idle period is picked up at
    /// once — the COLD writer is parked on the channel, not sleeping through
    /// a 50 ms poll step (the old `wait/16` step at the escalated 1 s wait).
    #[test]
    fn a_cold_writer_picks_up_the_first_record_promptly() {
        // Old step: 50 ms, so a send 120 ms in waited ~30 ms. Bound 10 ms.
        let median = median_pickup(
            std::time::Duration::from_secs(1),
            false,
            std::time::Duration::from_millis(120),
        );
        assert!(
            median < std::time::Duration::from_millis(10),
            "a record sent to an idle writer waited {median:?} (median) to be picked up"
        );
    }

    /// moon#1266: while WARM the poll step is short (the old step was 3 ms
    /// at the 50 ms fast floor).
    #[test]
    fn a_warm_writer_polls_with_a_short_step() {
        // Old step: 3.125 ms, so a send 3.3 ms in waited ~2.9 ms. New step:
        // 500 us (+ timer slack). Bound 1.5 ms.
        let median = median_pickup(
            std::time::Duration::from_millis(50),
            true,
            std::time::Duration::from_micros(3300),
        );
        assert!(
            median < std::time::Duration::from_micros(1500),
            "a record sent to a warm writer waited {median:?} (median) to be picked up"
        );
    }

    /// R1 review, finding 5: a writer fed one record every ~4 ms parks
    /// between records instead of polling through each gap (every record
    /// used to re-arm the 5 ms span: ~8 poll wake-ups per record), and a
    /// writer fed faster than two poll steps stays warm.
    #[test]
    fn a_trickle_parks_between_records_and_a_stream_stays_warm() {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(64);
        let sender = std::thread::spawn(move || {
            for i in 0..40u64 {
                std::thread::sleep(std::time::Duration::from_millis(4));
                assert!(tx.try_send(append(i)).is_ok());
            }
            tx
        });
        let mut w = IdleWait::new();
        let mut warm_after = 0usize;
        for _ in 0..40 {
            let Ok(_) = recv_next(&rx, &mut w, false) else {
                panic!("expected a record");
            };
            w.on_message();
            warm_after += usize::from(w.warm());
        }
        let Ok(tx) = sender.join() else {
            panic!("sender panicked");
        };
        // A few may land within 1 ms on a loaded host; most must not.
        assert!(
            warm_after <= 10,
            "{warm_after}/40 records 4 ms apart left the writer warm-polling"
        );
        // A stream: every record already queued keeps it warm.
        for i in 0..8 {
            assert!(tx.try_send(append(100 + i)).is_ok());
        }
        for _ in 0..8 {
            let Ok(_) = recv_next(&rx, &mut w, false) else {
                panic!("expected a record");
            };
            w.on_message();
            assert!(w.warm(), "a queued record keeps the writer warm");
        }
    }

    /// A warm writer whose span passes with nothing queued parks for the
    /// rest of the wait and still times out on schedule.
    #[test]
    fn a_warm_writer_parks_after_its_span_and_times_out_on_schedule() {
        let (_tx, rx) = channel::mpsc_bounded::<AofMessage>(4);
        let start = std::time::Instant::now();
        let Err(err) = poll_recv(&rx, std::time::Duration::from_millis(40), true) else {
            panic!("expected timeout");
        };
        assert!(matches!(err, flume::RecvTimeoutError::Timeout));
        let took = start.elapsed();
        assert!(took >= std::time::Duration::from_millis(40), "{took:?}");
        assert!(took < std::time::Duration::from_millis(500), "{took:?}");
    }
}
