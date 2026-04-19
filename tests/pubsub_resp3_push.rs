//! G-2 regression test: RESP3 subscribers must receive pub/sub messages framed
//! as RESP3 Push (`>` prefix) rather than RESP2 Array (`*` prefix).
//!
//! v0.1.10 closes this gap so Lunaris SDK can use `ws.events(...)` over a
//! RESP3 connection and receive Push-framed event notifications directly.
//!
//! RESP2 subscribers must continue receiving Array-framed messages (backwards
//! compatibility with every existing Redis client).
#![cfg(feature = "runtime-tokio")]

use bytes::{Bytes, BytesMut};
use moon::protocol::{Frame, ParseConfig, parse};
use moon::pubsub::subscriber::Subscriber;
use moon::pubsub::{PubSubRegistry, next_subscriber_id};
use moon::runtime::channel;

fn parse_resp(data: &[u8]) -> Frame {
    let mut buf = BytesMut::from(data);
    parse(&mut buf, &ParseConfig::default())
        .expect("valid RESP")
        .expect("complete frame")
}

#[tokio::test]
async fn resp3_subscriber_receives_push_frame() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    let sub_id = next_subscriber_id();
    let sub = Subscriber::with_protocol(tx, sub_id, true);

    let channel_name = Bytes::from_static(b"events");
    registry.subscribe(channel_name.clone(), sub);

    let count = registry.publish(&channel_name, &Bytes::from_static(b"hello"));
    assert_eq!(count, 1, "one RESP3 subscriber should receive the message");

    let raw = rx.try_recv().expect("subscriber receives message");
    assert!(
        raw.starts_with(b">"),
        "RESP3 subscriber must receive Push frame (`>`), got: {:?}",
        std::str::from_utf8(&raw).unwrap_or("<non-utf8>")
    );

    let frame = parse_resp(&raw);
    match frame {
        Frame::Push(items) => {
            assert_eq!(items.len(), 3, "Push frame has 3 elements");
            // Validate envelope: ["message", channel, payload]
            match &items[0] {
                Frame::BulkString(b) | Frame::SimpleString(b) => {
                    assert_eq!(b.as_ref(), b"message")
                }
                other => panic!("expected 'message' string, got {:?}", other),
            }
        }
        other => panic!("expected Frame::Push, got {:?}", other),
    }
}

#[tokio::test]
async fn resp2_subscriber_still_receives_array_frame() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    let sub = Subscriber::with_protocol(tx, next_subscriber_id(), false);

    let channel_name = Bytes::from_static(b"events");
    registry.subscribe(channel_name.clone(), sub);

    registry.publish(&channel_name, &Bytes::from_static(b"hello"));

    let raw = rx.try_recv().expect("subscriber receives message");
    assert!(
        raw.starts_with(b"*"),
        "RESP2 subscriber must receive Array frame (`*`), got: {:?}",
        std::str::from_utf8(&raw).unwrap_or("<non-utf8>")
    );

    match parse_resp(&raw) {
        Frame::Array(_) => {}
        other => panic!("expected Frame::Array, got {:?}", other),
    }
}

#[tokio::test]
async fn mixed_resp2_resp3_subscribers_each_get_correct_frame() {
    let mut registry = PubSubRegistry::new();
    let (tx2, rx2) = channel::mpsc_bounded::<Bytes>(8);
    let (tx3, rx3) = channel::mpsc_bounded::<Bytes>(8);
    registry.subscribe(
        Bytes::from_static(b"events"),
        Subscriber::with_protocol(tx2, next_subscriber_id(), false),
    );
    registry.subscribe(
        Bytes::from_static(b"events"),
        Subscriber::with_protocol(tx3, next_subscriber_id(), true),
    );

    registry.publish(&Bytes::from_static(b"events"), &Bytes::from_static(b"hi"));

    let raw2 = rx2.try_recv().expect("resp2 sub receives");
    let raw3 = rx3.try_recv().expect("resp3 sub receives");
    assert!(raw2.starts_with(b"*"), "resp2 gets Array");
    assert!(raw3.starts_with(b">"), "resp3 gets Push");
}

#[tokio::test]
async fn resp3_pattern_subscriber_receives_pmessage_push_frame() {
    let mut registry = PubSubRegistry::new();
    let (tx, rx) = channel::mpsc_bounded::<Bytes>(8);
    registry.psubscribe(
        Bytes::from_static(b"news.*"),
        Subscriber::with_protocol(tx, next_subscriber_id(), true),
    );

    registry.publish(
        &Bytes::from_static(b"news.tech"),
        &Bytes::from_static(b"body"),
    );
    let raw = rx.try_recv().expect("pattern sub receives");
    assert!(
        raw.starts_with(b">"),
        "RESP3 pattern subscriber must receive Push frame, got: {:?}",
        std::str::from_utf8(&raw).unwrap_or("<non-utf8>")
    );

    match parse_resp(&raw) {
        Frame::Push(items) => {
            assert_eq!(items.len(), 4, "pmessage Push has 4 elements");
            match &items[0] {
                Frame::BulkString(b) | Frame::SimpleString(b) => {
                    assert_eq!(b.as_ref(), b"pmessage")
                }
                other => panic!("expected 'pmessage' string, got {:?}", other),
            }
        }
        other => panic!("expected Frame::Push for pmessage, got {:?}", other),
    }
}
