pub mod static_responses;
pub mod tokio_driver;

#[cfg(target_os = "linux")]
pub mod fd_table;

#[cfg(target_os = "linux")]
pub mod buf_ring;

#[cfg(target_os = "linux")]
pub mod uring_driver;

#[cfg(target_os = "linux")]
pub use uring_driver::{build_get_response_iovecs, IoEvent, UringConfig, UringDriver};

// Event type constants for io_uring user_data encoding.
pub const EVENT_ACCEPT: u8 = 1;
pub const EVENT_RECV: u8 = 2;
pub const EVENT_SEND: u8 = 3;
pub const EVENT_TIMEOUT: u8 = 4;
pub const EVENT_WAKEUP: u8 = 5;

/// Encode event type, connection ID, and auxiliary data into a 64-bit user_data value.
///
/// Layout: [event_type:8][conn_id:24][aux:32]
#[inline]
pub fn encode_user_data(event_type: u8, conn_id: u32, aux: u32) -> u64 {
    ((event_type as u64) << 56) | ((conn_id as u64 & 0xFF_FFFF) << 32) | (aux as u64)
}

/// Decode a 64-bit user_data value into (event_type, conn_id, aux).
#[inline]
pub fn decode_user_data(data: u64) -> (u8, u32, u32) {
    let event_type = (data >> 56) as u8;
    let conn_id = ((data >> 32) & 0xFF_FFFF) as u32;
    let aux = data as u32;
    (event_type, conn_id, aux)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let (et, cid, aux) = (EVENT_RECV, 0x123456, 0xDEADBEEF);
        // conn_id is masked to 24 bits
        let encoded = encode_user_data(et, cid, aux);
        let (dec_et, dec_cid, dec_aux) = decode_user_data(encoded);
        assert_eq!(dec_et, et);
        assert_eq!(dec_cid, cid & 0xFF_FFFF);
        assert_eq!(dec_aux, aux);
    }

    #[test]
    fn test_encode_decode_zero() {
        let encoded = encode_user_data(0, 0, 0);
        assert_eq!(encoded, 0);
        let (et, cid, aux) = decode_user_data(encoded);
        assert_eq!(et, 0);
        assert_eq!(cid, 0);
        assert_eq!(aux, 0);
    }

    #[test]
    fn test_encode_decode_max_conn_id() {
        // 24-bit max = 0xFFFFFF
        let encoded = encode_user_data(EVENT_ACCEPT, 0xFFFFFF, 42);
        let (et, cid, aux) = decode_user_data(encoded);
        assert_eq!(et, EVENT_ACCEPT);
        assert_eq!(cid, 0xFFFFFF);
        assert_eq!(aux, 42);
    }

    #[test]
    fn test_conn_id_truncated_to_24_bits() {
        // Pass 32-bit value, only lower 24 bits should survive
        let encoded = encode_user_data(EVENT_SEND, 0x1_FFFFFF, 0);
        let (_, cid, _) = decode_user_data(encoded);
        assert_eq!(cid, 0xFFFFFF); // upper bits truncated
    }

    #[test]
    fn test_event_constants() {
        assert_eq!(EVENT_ACCEPT, 1);
        assert_eq!(EVENT_RECV, 2);
        assert_eq!(EVENT_SEND, 3);
        assert_eq!(EVENT_TIMEOUT, 4);
        assert_eq!(EVENT_WAKEUP, 5);
    }

    #[test]
    fn test_event_constants_unique() {
        let constants = [EVENT_ACCEPT, EVENT_RECV, EVENT_SEND, EVENT_TIMEOUT, EVENT_WAKEUP];
        for i in 0..constants.len() {
            for j in (i + 1)..constants.len() {
                assert_ne!(
                    constants[i], constants[j],
                    "EVENT constants must be unique: index {} and {} both = {}",
                    i, j, constants[i]
                );
            }
        }
    }

    #[test]
    fn test_encode_decode_all_event_types() {
        // Verify roundtrip for each event type constant
        for &et in &[EVENT_ACCEPT, EVENT_RECV, EVENT_SEND, EVENT_TIMEOUT, EVENT_WAKEUP] {
            let encoded = encode_user_data(et, 100, 200);
            let (dec_et, dec_cid, dec_aux) = decode_user_data(encoded);
            assert_eq!(dec_et, et, "event type roundtrip failed for {}", et);
            assert_eq!(dec_cid, 100);
            assert_eq!(dec_aux, 200);
        }
    }

    #[test]
    fn test_encode_decode_max_aux() {
        let encoded = encode_user_data(EVENT_ACCEPT, 1, u32::MAX);
        let (et, cid, aux) = decode_user_data(encoded);
        assert_eq!(et, EVENT_ACCEPT);
        assert_eq!(cid, 1);
        assert_eq!(aux, u32::MAX);
    }

    #[test]
    fn test_encode_decode_max_event_type() {
        // Event type is u8, max = 255
        let encoded = encode_user_data(u8::MAX, 0, 0);
        let (et, cid, aux) = decode_user_data(encoded);
        assert_eq!(et, u8::MAX);
        assert_eq!(cid, 0);
        assert_eq!(aux, 0);
    }
}
