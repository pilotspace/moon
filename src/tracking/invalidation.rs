use bytes::Bytes;
use crate::protocol::Frame;

/// Construct an invalidation Push frame for the given keys.
/// Wire format: >2\r\n$10\r\ninvalidate\r\n*N\r\n$len\r\nkey\r\n...
pub fn invalidation_push(keys: &[Bytes]) -> Frame {
    let key_frames: Vec<Frame> = keys
        .iter()
        .map(|k| Frame::BulkString(k.clone()))
        .collect();
    Frame::Push(vec![
        Frame::BulkString(Bytes::from_static(b"invalidate")),
        Frame::Array(key_frames),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_push_single_key() {
        let key = Bytes::from_static(b"foo");
        let frame = invalidation_push(&[key.clone()]);
        assert_eq!(
            frame,
            Frame::Push(vec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"foo"))]),
            ])
        );
    }

    #[test]
    fn test_invalidation_push_multiple_keys() {
        let keys = vec![
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
        ];
        let frame = invalidation_push(&keys);
        assert_eq!(
            frame,
            Frame::Push(vec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(vec![
                    Frame::BulkString(Bytes::from_static(b"foo")),
                    Frame::BulkString(Bytes::from_static(b"bar")),
                ]),
            ])
        );
    }
}
