use mlua::VmState;
use mlua::prelude::*;
use std::time::{Duration, Instant};

/// Restrict the Lua VM to a safe sandbox.
///
/// Lua::new() already excludes io, package, debug, and require.
/// This function additionally:
/// - Restricts os.* to clock() only
/// - Nils out package (CVE-2022-0543 historical sandbox escape)
/// - Removes load, dofile, loadfile, collectgarbage
pub fn setup_sandbox(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();

    // Restrict os.* to clock() only
    let restricted_os = lua.create_table()?;
    if let Ok(os_table) = globals.get::<mlua::Table>("os") {
        if let Ok(clock_fn) = os_table.get::<mlua::Function>("clock") {
            restricted_os.set("clock", clock_fn)?;
        }
    }
    globals.set("os", restricted_os)?;

    // Ensure package is nil (historical sandbox escape: package.loadlib)
    globals.set("package", mlua::Nil)?;

    // Remove dangerous global functions
    globals.set("load", mlua::Nil)?;
    globals.set("dofile", mlua::Nil)?;
    globals.set("loadfile", mlua::Nil)?;
    globals.set("collectgarbage", mlua::Nil)?;

    Ok(())
}

/// Install a VM instruction hook that errors after a wall-clock timeout.
///
/// The hook fires every 100,000 VM instructions and checks elapsed time.
/// If the deadline is exceeded, a RuntimeError is raised which unwinds the Lua stack.
pub fn install_timeout_hook(lua: &Lua, timeout: Duration) -> mlua::Result<()> {
    let deadline = Instant::now() + timeout;
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(100_000),
        move |_lua, _debug| {
            if Instant::now() >= deadline {
                Err(mlua::Error::RuntimeError(
                    "ERR Lua script timeout".to_string(),
                ))
            } else {
                Ok(VmState::Continue)
            }
        },
    )
}

/// Remove the timeout hook. Must be called after EVERY script execution
/// (both success and error paths) to prevent stale deadlines from contaminating
/// subsequent scripts.
pub fn remove_timeout_hook(lua: &Lua) {
    lua.remove_hook();
}

/// Register the redis.* API table on the Lua VM.
///
/// Registers: redis.call, redis.pcall, redis.log, redis.error_reply,
/// redis.status_reply, redis.sha1hex, and LOG_* level constants.
pub fn register_redis_api(lua: &Lua) -> mlua::Result<()> {
    let redis_table = lua.create_table()?;

    // redis.call -- propagate errors as Lua errors
    redis_table.set(
        "call",
        crate::scripting::bridge::make_redis_call_fn(lua, true)?,
    )?;

    // redis.pcall -- catch errors as {err = string} table
    redis_table.set(
        "pcall",
        crate::scripting::bridge::make_redis_call_fn(lua, false)?,
    )?;

    // redis.log(level, message)
    redis_table.set(
        "log",
        lua.create_function(|_lua, (_level, msg): (LuaValue, String)| {
            tracing::info!(target: "lua_script", "{}", msg);
            Ok(())
        })?,
    )?;

    // redis.error_reply(msg) -- return {err = msg}
    redis_table.set(
        "error_reply",
        lua.create_function(|lua, msg: String| {
            let t = lua.create_table()?;
            t.set("err", msg)?;
            Ok(t)
        })?,
    )?;

    // redis.status_reply(msg) -- return {ok = msg}
    redis_table.set(
        "status_reply",
        lua.create_function(|lua, msg: String| {
            let t = lua.create_table()?;
            t.set("ok", msg)?;
            Ok(t)
        })?,
    )?;

    // redis.sha1hex(string)
    redis_table.set(
        "sha1hex",
        lua.create_function(|_lua, s: mlua::String| {
            Ok(sha1_smol::Sha1::from(s.as_bytes()).hexdigest())
        })?,
    )?;

    // Log level constants
    redis_table.set("LOG_DEBUG", 0i64)?;
    redis_table.set("LOG_VERBOSE", 1i64)?;
    redis_table.set("LOG_NOTICE", 2i64)?;
    redis_table.set("LOG_WARNING", 3i64)?;

    lua.globals().set("redis", redis_table)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sandbox_removes_dangerous_globals() {
        let lua = Lua::new();
        setup_sandbox(&lua).unwrap();

        let globals = lua.globals();
        assert!(globals.get::<LuaValue>("package").unwrap() == LuaValue::Nil);
        assert!(globals.get::<LuaValue>("load").unwrap() == LuaValue::Nil);
        assert!(globals.get::<LuaValue>("dofile").unwrap() == LuaValue::Nil);
        assert!(globals.get::<LuaValue>("loadfile").unwrap() == LuaValue::Nil);
        assert!(globals.get::<LuaValue>("collectgarbage").unwrap() == LuaValue::Nil);
    }

    #[test]
    fn test_sandbox_allows_os_clock() {
        let lua = Lua::new();
        setup_sandbox(&lua).unwrap();

        // os.clock() should work
        let result: f64 = lua.load("return os.clock()").eval().unwrap();
        assert!(result >= 0.0);
    }

    #[test]
    fn test_sandbox_blocks_os_other_fns() {
        let lua = Lua::new();
        setup_sandbox(&lua).unwrap();

        // os.time and os.date should not exist after sandbox
        let result: LuaValue = lua.load("return os.time").eval().unwrap();
        assert!(result == LuaValue::Nil);
    }

    #[test]
    fn test_sandbox_allows_string_math_table() {
        let lua = Lua::new();
        setup_sandbox(&lua).unwrap();

        // string, math, table should be available
        let _: String = lua.load("return string.upper('hello')").eval().unwrap();
        let _: f64 = lua.load("return math.floor(3.5)").eval().unwrap();
        let _: i64 = lua.load("local t = {1,2,3}; return #t").eval().unwrap();
    }

    #[test]
    fn test_timeout_hook() {
        let lua = Lua::new();
        // Very short timeout to trigger quickly
        install_timeout_hook(&lua, Duration::from_millis(1)).unwrap();

        // Infinite loop should be interrupted
        let result: mlua::Result<()> = lua.load("while true do end").exec();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("timeout"),
            "Expected timeout error, got: {}",
            err_msg
        );

        remove_timeout_hook(&lua);
    }
}
