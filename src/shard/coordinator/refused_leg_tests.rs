//! The reply of a multi-shard write when a shard refused its leg unapplied
//! because its AOF writer was stalled (moon#769).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::shard::aof_admission::{AOF_BACKPRESSURE_PARTIAL_ERR, AOF_BACKPRESSURE_REFUSED_ERR};

fn refused() -> Frame {
    Frame::Error(Bytes::from_static(AOF_BACKPRESSURE_REFUSED_ERR))
}

#[test]
fn a_refused_leg_of_a_command_whose_other_parts_ran_is_a_partial_failure() {
    assert_eq!(
        super::refused_leg_error(refused(), true),
        Frame::Error(Bytes::from_static(AOF_BACKPRESSURE_PARTIAL_ERR)),
        "\"not executed\" is false once another leg or the local slice was applied"
    );
}

#[test]
fn a_refused_leg_of_a_command_nothing_else_ran_for_stays_not_executed() {
    assert_eq!(super::refused_leg_error(refused(), false), refused());
}

#[test]
fn other_leg_errors_pass_through() {
    let other = Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key"));
    assert_eq!(super::refused_leg_error(other.clone(), true), other);
}
