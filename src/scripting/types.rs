use bytes::Bytes;
use mlua::prelude::*;

use crate::protocol::{Frame, FrameVec};
/// Convert one `redis.call`/`redis.pcall` ARGUMENT to a Frame.
///
/// Deliberately different from [`lua_value_to_frame`], which converts a
/// script's RETURN value. An argument is part of a command's argv, and a
/// wire client can only ever put a bulk string there — upstream Redis
/// stringifies Lua numbers for exactly this reason
/// (`luaArgsToRedisArgv`). Moon used to hand `Frame::Integer` straight
/// through, which meant `redis.call('SET', 'k', 5)` produced an argv no
/// command parser (and no AOF/replication replay) could read: the value
/// probes below all failed with `wrong number of arguments` before this.
///
/// moon#569 made it security-relevant too: `acl::keyspec` reads keys and
/// `numkeys` out of the argv as BYTES, so an integer frame in either slot
/// was un-enumerable and a legitimate `redis.call('LMPOP', 1, 'app:l',
/// 'LEFT')` failed CLOSED for every key-restricted user.
///
/// Float truncation matches what moon already did (`3.7` -> `3`); only the
/// frame TYPE changes.
/// The message real Redis returns when a `redis.call`/`redis.pcall` argument is
/// not a string or a number. Verified against Redis 8.6.1.
pub(crate) const LUA_ARG_TYPE_ERR: &str =
    "ERR Lua redis lib command arguments must be strings or integers";

// `_lua` is unused now that every accepted shape converts without allocating
// through the Lua state, but the parameter stays: it keeps this symmetric with
// `lua_value_to_frame`, and every call site passes it already.
pub fn lua_arg_to_frame(_lua: &Lua, value: &LuaValue) -> mlua::Result<Frame> {
    let n = match value {
        LuaValue::Integer(n) => *n,
        LuaValue::Number(f) => *f as i64,
        // Strings are already wire-shaped.
        LuaValue::String(s) => {
            return Ok(Frame::BulkString(Bytes::copy_from_slice(&s.as_bytes())));
        }
        // moon#823: everything else is REFUSED, as real Redis refuses it.
        //
        // This used to fall through to `lua_value_to_frame`, on the reasoning
        // that a nil/boolean/table argument is un-nameable in a key position
        // and the ACL walker therefore denies it. That reasoning is sound and
        // it covers the KEY position. It does not cover the VALUE position,
        // which is where the damage was: `Frame::Null`/`Frame::Integer` in an
        // argv makes `extract_bytes` return `None` halfway through a command's
        // mutation loop, and HSET/HMSET/LPUSH/RPUSH/LPUSHX/RPUSHX/ZREM/MSET all
        // return their arity error from INSIDE the memory-charge window with
        // part of the command already written. Propagation is gated on the
        // reply not being an error, so those writes were applied on the master
        // and never reached the AOF or a replica — silent data loss across
        // restart, and permanent `DEBUG DIGEST` divergence, driveable by any
        // client that can EVAL.
        //
        // The commands are hardened too (validate-before-mutate), but this is
        // the boundary that should never have let the shape through, and it is
        // the one place that fixes the whole class at once.
        _ => return Err(mlua::Error::RuntimeError(LUA_ARG_TYPE_ERR.to_string())),
    };
    let mut buf = itoa::Buffer::new();
    Ok(Frame::BulkString(Bytes::copy_from_slice(
        buf.format(n).as_bytes(),
    )))
}

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
        // Redis converts BOTH RESP2 nulls to Lua `false` — `redis.call('BLPOP',
        // k, 0)` that times out yields `false`, exactly like a `GET` miss. So
        // NullArray shares this arm rather than the catch-all below: the VALUE
        // would be the same either way, but stating it here means a future
        // Frame variant does not silently inherit "false" (moon#482).
        Frame::Null | Frame::NullArray => Ok(LuaValue::Boolean(false)),
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

    // ── moon#823: the argument boundary ────────────────────────────────
    //
    // `lua_arg_to_frame` is the ONLY place a non-wire-shaped frame can enter a
    // command's argv. Every command parser assumes argv is bulk strings,
    // because that is all a wire client can send; several of them discover
    // otherwise halfway through a mutation loop and return an error from
    // inside their memory-charge window, having already written part of the
    // command. Propagation is gated on the reply not being an error, so those
    // writes are applied on the master and never reach the AOF or a replica.
    //
    // Real Redis refuses the call outright rather than defending eight command
    // parsers, and so do we. Verified against Redis 8.6.1.

    #[test]
    fn a_boolean_argument_is_refused_not_converted() {
        let lua = Lua::new();
        for v in [LuaValue::Boolean(true), LuaValue::Boolean(false)] {
            let err = lua_arg_to_frame(&lua, &v)
                .expect_err("a boolean argument must be refused, not converted");
            assert!(
                err.to_string().contains(LUA_ARG_TYPE_ERR),
                "wrong message for {v:?}: {err}"
            );
        }
    }

    #[test]
    fn a_nil_argument_is_refused_not_converted() {
        let lua = Lua::new();
        let err = lua_arg_to_frame(&lua, &LuaValue::Nil)
            .expect_err("a nil argument must be refused, not converted");
        assert!(err.to_string().contains(LUA_ARG_TYPE_ERR), "{err}");
    }

    #[test]
    fn a_table_argument_is_refused_not_converted() {
        let lua = Lua::new();
        let t = lua.create_table().unwrap();
        let err = lua_arg_to_frame(&lua, &LuaValue::Table(t))
            .expect_err("a table argument must be refused, not converted");
        assert!(err.to_string().contains(LUA_ARG_TYPE_ERR), "{err}");
        // An {err=..} table is a RETURN shape, never an argument shape: it must
        // be refused too, not smuggled in as `Frame::Error`.
        let t = lua.create_table().unwrap();
        t.set("err", "boom").unwrap();
        let err = lua_arg_to_frame(&lua, &LuaValue::Table(t))
            .expect_err("an {err=..} table argument must be refused");
        assert!(err.to_string().contains(LUA_ARG_TYPE_ERR), "{err}");
    }

    #[test]
    fn strings_and_numbers_still_pass_through_as_bulk_strings() {
        let lua = Lua::new();
        // The moon#569 contract: numbers stringify, they do not stay integers.
        assert!(matches!(
            lua_arg_to_frame(&lua, &LuaValue::Integer(42)).unwrap(),
            Frame::BulkString(b) if b == &Bytes::from_static(b"42")
        ));
        assert!(matches!(
            lua_arg_to_frame(&lua, &LuaValue::Number(3.7)).unwrap(),
            Frame::BulkString(b) if b == &Bytes::from_static(b"3")
        ));
        let s = lua.create_string(b"hello").unwrap();
        assert!(matches!(
            lua_arg_to_frame(&lua, &LuaValue::String(s)).unwrap(),
            Frame::BulkString(b) if b == &Bytes::from_static(b"hello")
        ));
    }

    #[test]
    fn the_return_value_converter_is_unchanged_by_the_argument_rule() {
        // `lua_value_to_frame` converts a script's RETURN value, where nil,
        // booleans and tables are all legal and carry meaning. Tightening the
        // ARGUMENT path must not touch it.
        let lua = Lua::new();
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Nil).unwrap(),
            Frame::Null
        ));
        assert!(matches!(
            lua_value_to_frame(&lua, &LuaValue::Boolean(true)).unwrap(),
            Frame::Integer(1)
        ));
    }

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
        #[allow(clippy::approx_constant)]
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
