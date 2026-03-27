use bytes::Bytes;
use mlua::prelude::*;

use crate::protocol::{Frame, FrameVec};
/// Convert a Lua value to a Redis Frame (Lua -> RESP2 conversion).
///
/// Redis-compatible conversion table:
/// - nil -> Null
/// - false -> Null
/// - true -> Integer(1)
/// - integer -> Integer
/// - float -> Integer (truncated, Redis behavior)
/// - string -> BulkString
/// - table {ok=str} -> SimpleString
/// - table {err=str} -> Error
/// - table (array) -> Array (stops at first nil)
pub fn lua_value_to_frame(lua: &Lua, value: &LuaValue) -> mlua::Result<Frame> {
    match value {
        LuaValue::Nil => Ok(Frame::Null),
        LuaValue::Boolean(false) => Ok(Frame::Null),
        LuaValue::Boolean(true) => Ok(Frame::Integer(1)),
        LuaValue::Integer(n) => Ok(Frame::Integer(*n)),
        LuaValue::Number(f) => Ok(Frame::Integer(*f as i64)), // truncate -- Redis behavior
        LuaValue::String(s) => Ok(Frame::BulkString(Bytes::copy_from_slice(&s.as_bytes()))),
        LuaValue::Table(t) => {
            // Check for {ok = string} status reply
            if let Ok(LuaValue::String(s)) = t.get::<LuaValue>("ok") {
                return Ok(Frame::SimpleString(Bytes::copy_from_slice(&s.as_bytes())));
            }
            // Check for {err = string} error reply
            if let Ok(LuaValue::String(e)) = t.get::<LuaValue>("err") {
                return Ok(Frame::Error(Bytes::copy_from_slice(&e.as_bytes())));
            }
            // Array table: iterate integer keys 1, 2, 3... stopping at first nil
            let mut items = FrameVec::new();
            let mut i = 1i64;
            loop {
                let val: LuaValue = t.get(i)?;
                if val == LuaValue::Nil {
                    break;
                }
                items.push(lua_value_to_frame(lua, &val)?);
                i += 1;
            }
            Ok(Frame::Array(items))
        }
        _ => Err(mlua::Error::RuntimeError(format!(
            "ERR Lua type '{}' not supported as return value",
            value.type_name()
        ))),
    }
}

/// Convert a Redis Frame to a Lua value (RESP2 -> Lua conversion).
///
/// Redis-compatible conversion table:
/// - Integer -> integer
/// - BulkString -> string
/// - SimpleString -> table {ok=str}
/// - Error -> table {err=str}
/// - Null -> false
/// - Array -> table (1-indexed)
/// - Boolean -> Integer (1 or 0)
/// - Double -> Integer (truncated)
pub fn frame_to_lua_value(lua: &Lua, frame: &Frame) -> mlua::Result<LuaValue> {
    match frame {
        Frame::Integer(n) => Ok(LuaValue::Integer(*n)),
        Frame::BulkString(b) => Ok(LuaValue::String(lua.create_string(b.as_ref())?)),
        Frame::SimpleString(b) => {
            let t = lua.create_table()?;
            t.set("ok", lua.create_string(b.as_ref())?)?;
            Ok(LuaValue::Table(t))
        }
        Frame::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", lua.create_string(e.as_ref())?)?;
            Ok(LuaValue::Table(t))
        }
        Frame::Null => Ok(LuaValue::Boolean(false)),
        Frame::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.set(i as i64 + 1, frame_to_lua_value(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        Frame::Boolean(b) => Ok(LuaValue::Integer(if *b { 1 } else { 0 })),
        Frame::Double(f) => Ok(LuaValue::Integer(*f as i64)),
        Frame::PreSerialized(wire) => {
            // Extract payload from pre-serialized RESP bulk string: $<len>\r\n<data>\r\n
            if wire.len() >= 6 && wire[0] == b'$' {
                if let Some(crlf) = wire[1..].windows(2).position(|w| w == b"\r\n") {
                    let data_start = 1 + crlf + 2;
                    if wire.len() >= data_start + 2 {
                        let data = &wire[data_start..wire.len() - 2];
                        return Ok(LuaValue::String(lua.create_string(data)?));
                    }
                }
            }
            Ok(LuaValue::Boolean(false))
        }
        // Other RESP3 variants (Map, Set, etc.) -- convert to false
        _ => Ok(LuaValue::Boolean(false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;

    #[test]
    fn test_lua_nil_to_frame() {
        let lua = Lua::new();
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Nil).unwrap(),
            Frame::Null
        ));
    }

    #[test]
    fn test_lua_bool_to_frame() {
        let lua = Lua::new();
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Boolean(false)).unwrap(),
            Frame::Null
        ));
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Boolean(true)).unwrap(),
            Frame::Integer(1)
        ));
    }

    #[test]
    fn test_lua_integer_to_frame() {
        let lua = Lua::new();
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Integer(42)).unwrap(),
            Frame::Integer(42)
        ));
    }

    #[test]
    fn test_lua_float_truncation() {
        let lua = Lua::new();
        // Float truncation is Redis-compatible behavior
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Number(3.99)).unwrap(),
            Frame::Integer(3)
        ));
    }

    #[test]
    fn test_lua_string_to_frame() {
        let lua = Lua::new();
        let s = lua.create_string(b"hello").unwrap();
        let frame = lua_value_to_frame(&lua, &LuaValue::String(s)).unwrap();
        assert!(matches!(frame, Frame::BulkString(b) if b == &Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_lua_ok_table_to_frame() {
        let lua = Lua::new();
        let t = lua.create_table().unwrap();
        t.set("ok", "OK").unwrap();
        let frame = lua_value_to_frame(&lua, &LuaValue::Table(t)).unwrap();
        assert!(matches!(frame, Frame::SimpleString(b) if b == &Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_lua_err_table_to_frame() {
        let lua = Lua::new();
        let t = lua.create_table().unwrap();
        t.set("err", "ERR something").unwrap();
        let frame = lua_value_to_frame(&lua, &LuaValue::Table(t)).unwrap();
        assert!(matches!(frame, Frame::Error(b) if b == &Bytes::from_static(b"ERR something")));
    }

    #[test]
    fn test_lua_array_table_to_frame() {
        let lua = Lua::new();
        let t = lua.create_table().unwrap();
        t.set(1, 10i64).unwrap();
        t.set(2, 20i64).unwrap();
        t.set(3, 30i64).unwrap();
        let frame = lua_value_to_frame(&lua, &LuaValue::Table(t)).unwrap();
        match frame {
            Frame::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], Frame::Integer(10)));
                assert!(matches!(items[1], Frame::Integer(20)));
                assert!(matches!(items[2], Frame::Integer(30)));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_frame_null_to_lua() {
        let lua = Lua::new();
        let val = frame_to_lua_value(&lua, &Frame::Null).unwrap();
        assert!(matches!(val, LuaValue::Boolean(false)));
    }

    #[test]
    fn test_frame_integer_to_lua() {
        let lua = Lua::new();
        let val = frame_to_lua_value(&lua, &Frame::Integer(42)).unwrap();
        assert!(matches!(val, LuaValue::Integer(42)));
    }

    #[test]
    fn test_frame_bulk_string_to_lua() {
        let lua = Lua::new();
        let val =
            frame_to_lua_value(&lua, &Frame::BulkString(Bytes::from_static(b"hello"))).unwrap();
        match val {
            LuaValue::String(s) => assert_eq!(&*s.as_bytes(), b"hello"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_frame_simple_string_to_lua() {
        let lua = Lua::new();
        let val =
            frame_to_lua_value(&lua, &Frame::SimpleString(Bytes::from_static(b"OK"))).unwrap();
        match val {
            LuaValue::Table(t) => {
                let ok: LuaValue = t.get("ok").unwrap();
                match ok {
                    LuaValue::String(s) => assert_eq!(&*s.as_bytes(), b"OK"),
                    _ => panic!("Expected String"),
                }
            }
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_frame_error_to_lua() {
        let lua = Lua::new();
        let val = frame_to_lua_value(&lua, &Frame::Error(Bytes::from_static(b"ERR test"))).unwrap();
        match val {
            LuaValue::Table(t) => {
                let err: LuaValue = t.get("err").unwrap();
                match err {
                    LuaValue::String(s) => assert_eq!(&*s.as_bytes(), b"ERR test"),
                    _ => panic!("Expected String"),
                }
            }
            _ => panic!("Expected Table"),
        }
    }

    #[test]
    fn test_frame_boolean_to_lua() {
        let lua = Lua::new();
        let val = frame_to_lua_value(&lua, &Frame::Boolean(true)).unwrap();
        assert!(matches!(val, LuaValue::Integer(1)));
        let val = frame_to_lua_value(&lua, &Frame::Boolean(false)).unwrap();
        assert!(matches!(val, LuaValue::Integer(0)));
    }

    #[test]
    fn test_frame_double_to_lua() {
        let lua = Lua::new();
        let val = frame_to_lua_value(&lua, &Frame::Double(3.14)).unwrap();
        assert!(matches!(val, LuaValue::Integer(3)));
    }

    #[test]
    fn test_frame_array_to_lua() {
        let lua = Lua::new();
        let arr = Frame::Array(framevec![Frame::Integer(1), Frame::Integer(2)]);
        let val = frame_to_lua_value(&lua, &arr).unwrap();
        match val {
            LuaValue::Table(t) => {
                let v1: i64 = t.get(1i64).unwrap();
                let v2: i64 = t.get(2i64).unwrap();
                assert_eq!(v1, 1);
                assert_eq!(v2, 2);
            }
            _ => panic!("Expected Table"),
        }
    }
}
