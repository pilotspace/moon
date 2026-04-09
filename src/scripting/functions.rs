//! Function registry for Redis 7.0+ Functions API (FUNCTION LOAD / FCALL).
//!
//! **Phase 101 limitation:** RAM-only storage. Functions are not persisted
//! across restarts (no RDB/AOF serialization). Clients must re-LOAD after restart.
//!
//! Design: per-library Lua state for isolation. Each `FUNCTION LOAD` creates a
//! fresh sandboxed Lua VM, evaluates the library body (which calls
//! `redis.register_function`), and stores the resulting functions. FCALL looks up
//! a function by name across all loaded libraries.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use bytes::Bytes;
use mlua::prelude::*;

use crate::protocol::Frame;
use crate::storage::Database;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single registered function within a library.
#[derive(Clone)]
pub struct FunctionDef {
    /// Function name (unique across all libraries).
    pub name: Bytes,
    /// Optional description provided via table-form registration.
    pub description: Option<String>,
    /// Flags controlling execution semantics.
    pub flags: u8,
}

/// Function flag constants (mirrors Redis function flags).
pub mod func_flags {
    pub const NO_WRITES: u8 = 0b0001;
    pub const ALLOW_OOM: u8 = 0b0010;
    pub const ALLOW_STALE: u8 = 0b0100;
    pub const NO_CLUSTER: u8 = 0b1000;
}

/// A loaded library containing one or more functions.
pub struct Library {
    /// Library name (from shebang `name=<libname>`).
    pub name: Bytes,
    /// Engine identifier (always `"lua"` for now).
    pub engine: Bytes,
    /// Original source body (retained for FUNCTION LIST WITHCODE).
    pub source: Bytes,
    /// Registered functions keyed by function name.
    pub functions: HashMap<Bytes, FunctionDef>,
    /// Per-library Lua state (holds the compiled function closures).
    pub lua: Rc<Lua>,
}

/// Errors from `FunctionRegistry::load`.
#[derive(Debug)]
pub enum LoadError {
    /// Body does not start with `#!lua name=...` shebang.
    MissingShebang,
    /// Unsupported engine (only `lua` is supported).
    BadEngine(String),
    /// Library already loaded and REPLACE was not specified.
    AlreadyExists(Bytes),
    /// Lua compilation or evaluation error.
    LuaError(String),
    /// No functions were registered by the library body.
    NoFunctions,
}

impl LoadError {
    /// Convert to a Redis error Frame.
    pub fn into_frame(self) -> Frame {
        match self {
            LoadError::MissingShebang => {
                Frame::Error(Bytes::from_static(b"ERR Missing library metadata"))
            }
            LoadError::BadEngine(e) => {
                Frame::Error(Bytes::from(format!("ERR Engine '{e}' not found")))
            }
            LoadError::AlreadyExists(name) => Frame::Error(Bytes::from(format!(
                "ERR Library '{}' already exists",
                String::from_utf8_lossy(&name)
            ))),
            LoadError::LuaError(e) => Frame::Error(Bytes::from(format!("ERR {e}"))),
            LoadError::NoFunctions => {
                Frame::Error(Bytes::from_static(b"ERR No functions registered"))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Per-shard function registry. NOT behind a lock -- single-threaded shard access.
pub struct FunctionRegistry {
    /// Libraries keyed by library name.
    libraries: HashMap<Bytes, Library>,
    /// Reverse index: function_name -> library_name (for fast FCALL lookup).
    func_to_lib: HashMap<Bytes, Bytes>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        FunctionRegistry {
            libraries: HashMap::new(),
            func_to_lib: HashMap::new(),
        }
    }

    /// Load a library from its body text.
    ///
    /// The body must start with a shebang line: `#!lua name=<libname>`.
    /// The remaining body is evaluated in a sandboxed Lua VM. Calls to
    /// `redis.register_function(name, fn)` register functions.
    ///
    /// If `replace` is true, an existing library with the same name is replaced.
    pub fn load(&mut self, body: &[u8], replace: bool) -> Result<Bytes, LoadError> {
        let (lib_name, _rest) = parse_shebang(body)?;

        // Check for existing library
        if !replace && self.libraries.contains_key(&lib_name) {
            return Err(LoadError::AlreadyExists(lib_name));
        }

        // Create the library via Lua evaluation
        let library = self.create_library(lib_name.clone(), body)?;

        // Check for function name collisions with other libraries BEFORE removing old
        for func_name in library.functions.keys() {
            if let Some(other_lib) = self.func_to_lib.get(func_name) {
                if *other_lib != lib_name {
                    return Err(LoadError::LuaError(format!(
                        "Function '{}' already exists in library '{}'",
                        String::from_utf8_lossy(func_name),
                        String::from_utf8_lossy(other_lib),
                    )));
                }
            }
        }

        // Remove old library if replacing (safe now — collision check passed)
        if let Some(old) = self.libraries.remove(&lib_name) {
            for func_name in old.functions.keys() {
                self.func_to_lib.remove(func_name);
            }
        }

        // Register reverse index
        for func_name in library.functions.keys() {
            self.func_to_lib.insert(func_name.clone(), lib_name.clone());
        }

        self.libraries.insert(lib_name.clone(), library);
        Ok(lib_name)
    }

    /// Look up a function by name. Returns the library and function definition
    /// if found.
    pub fn lookup(&self, func_name: &[u8]) -> Option<(&Library, &FunctionDef)> {
        let lib_name = self.func_to_lib.get(func_name)?;
        let lib = self.libraries.get(lib_name)?;
        let func = lib.functions.get(func_name)?;
        Some((lib, func))
    }

    /// List all loaded libraries.
    pub fn list(&self) -> Vec<&Library> {
        self.libraries.values().collect()
    }

    /// Delete a library by name. Returns true if it existed.
    pub fn delete(&mut self, lib_name: &[u8]) -> bool {
        if let Some(lib) = self.libraries.remove(lib_name) {
            for func_name in lib.functions.keys() {
                self.func_to_lib.remove(func_name);
            }
            true
        } else {
            false
        }
    }

    /// Flush all libraries.
    pub fn flush(&mut self) {
        self.libraries.clear();
        self.func_to_lib.clear();
    }

    /// Execute a function by name with the given keys and args.
    pub fn call_function(
        &self,
        func_name: &[u8],
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        db: &mut Database,
        selected_db: usize,
        db_count: usize,
        read_only: bool,
    ) -> Frame {
        let (lib, _func_def) = match self.lookup(func_name) {
            Some(pair) => pair,
            None => {
                return Frame::Error(Bytes::from_static(b"ERR Function not found"));
            }
        };

        // Set up bridge
        crate::scripting::bridge::set_script_db(db, selected_db, db_count);
        if read_only {
            crate::scripting::bridge::set_script_read_only(true);
        }

        let timeout = std::time::Duration::from_secs(5);
        if crate::scripting::sandbox::install_timeout_hook(&lib.lua, timeout).is_err() {
            crate::scripting::bridge::clear_script_db();
            return Frame::Error(Bytes::from_static(
                b"ERR Failed to install script timeout hook",
            ));
        }

        let result = (|| -> mlua::Result<Frame> {
            // Set KEYS and ARGV globals
            let keys_table = lib.lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                keys_table.set(i as i64 + 1, lib.lua.create_string(key.as_ref())?)?;
            }
            lib.lua.globals().set("KEYS", keys_table)?;

            let argv_table = lib.lua.create_table()?;
            for (i, arg) in argv.iter().enumerate() {
                argv_table.set(i as i64 + 1, lib.lua.create_string(arg.as_ref())?)?;
            }
            lib.lua.globals().set("ARGV", argv_table)?;

            // Call the registered function
            let func_name_str = lib.lua.create_string(func_name)?;
            let func_tbl: mlua::Table =
                lib.lua.globals().get("__moon_functions")?;
            let registered: mlua::Function = func_tbl.get(func_name_str)?;
            let val: LuaValue = registered.call(())?;
            crate::scripting::types::lua_value_to_frame(&lib.lua, &val)
        })();

        // Always clean up
        crate::scripting::sandbox::remove_timeout_hook(&lib.lua);
        crate::scripting::bridge::clear_script_db();

        match result {
            Ok(frame) => frame,
            Err(mlua::Error::RuntimeError(msg))
                if msg.contains("ERR Lua script timeout") =>
            {
                Frame::Error(Bytes::from_static(b"BUSY Lua script timeout exceeded"))
            }
            Err(mlua::Error::RuntimeError(msg))
                if msg.contains("Write commands are not allowed") =>
            {
                Frame::Error(Bytes::from_static(
                    b"ERR Write commands are not allowed from read-only scripts",
                ))
            }
            Err(e) => Frame::Error(Bytes::from(format!("ERR Error running script: {e}"))),
        }
    }

    /// Internal: create a Library from body bytes by evaluating in a Lua sandbox.
    fn create_library(&self, lib_name: Bytes, body: &[u8]) -> Result<Library, LoadError> {
        let (_, rest) = parse_shebang(body)?;

        // Create a new sandboxed Lua VM
        let lua = Rc::new(mlua::Lua::new());
        crate::scripting::sandbox::setup_sandbox(&lua)
            .map_err(|e| LoadError::LuaError(e.to_string()))?;
        crate::scripting::sandbox::register_redis_api(&lua)
            .map_err(|e| LoadError::LuaError(e.to_string()))?;

        // Create a table to store registered functions
        let func_table = lua
            .create_table()
            .map_err(|e| LoadError::LuaError(e.to_string()))?;
        lua.globals()
            .set("__moon_functions", func_table)
            .map_err(|e| LoadError::LuaError(e.to_string()))?;

        // Create shared storage for function metadata
        let functions: Rc<RefCell<HashMap<Bytes, FunctionDef>>> =
            Rc::new(RefCell::new(HashMap::new()));

        // Create redis.register_function
        let funcs_clone = functions.clone();
        let register_fn = lua
            .create_function(move |lua, args: LuaMultiValue| {
                // Two forms:
                // 1. redis.register_function("name", function) -- positional
                // 2. redis.register_function({function_name="name", callback=fn, ...})

                if args.is_empty() {
                    return Err(mlua::Error::RuntimeError(
                        "ERR redis.register_function requires at least one argument"
                            .to_string(),
                    ));
                }

                let (name, callback, description, flags) = match &args[0] {
                    LuaValue::String(s) => {
                        // Positional form: (name, fn)
                        if args.len() < 2 {
                            return Err(mlua::Error::RuntimeError(
                                "ERR redis.register_function requires a function argument"
                                    .to_string(),
                            ));
                        }
                        let func = match &args[1] {
                            LuaValue::Function(f) => f.clone(),
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "ERR redis.register_function second argument \
                                     must be a function"
                                        .to_string(),
                                ));
                            }
                        };
                        let name_bytes = s.as_bytes();
                        (
                            Bytes::copy_from_slice(&name_bytes),
                            func,
                            None,
                            0u8,
                        )
                    }
                    LuaValue::Table(t) => {
                        // Table form
                        let name_s: mlua::String =
                            t.get("function_name").map_err(|_| {
                                mlua::Error::RuntimeError(
                                    "ERR redis.register_function: table must \
                                     have function_name"
                                        .to_string(),
                                )
                            })?;
                        let callback: mlua::Function =
                            t.get("callback").map_err(|_| {
                                mlua::Error::RuntimeError(
                                    "ERR redis.register_function: table must \
                                     have callback"
                                        .to_string(),
                                )
                            })?;
                        let desc: Option<String> = t.get("description").ok();
                        let mut flags: u8 = 0;
                        if let Ok(f) = t.get::<mlua::Table>("flags") {
                            for pair in f.sequence_values::<mlua::String>() {
                                if let Ok(flag_str) = pair {
                                    let flag_bytes = flag_str.as_bytes();
                                    if &*flag_bytes == b"no-writes" {
                                        flags |= func_flags::NO_WRITES;
                                    } else if &*flag_bytes == b"allow-oom" {
                                        flags |= func_flags::ALLOW_OOM;
                                    } else if &*flag_bytes == b"allow-stale" {
                                        flags |= func_flags::ALLOW_STALE;
                                    } else if &*flag_bytes == b"no-cluster" {
                                        flags |= func_flags::NO_CLUSTER;
                                    }
                                }
                            }
                        }
                        let nb = name_s.as_bytes();
                        (
                            Bytes::copy_from_slice(&nb),
                            callback,
                            desc,
                            flags,
                        )
                    }
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "ERR redis.register_function first argument \
                             must be a string or table"
                                .to_string(),
                        ));
                    }
                };

                // Store callback in __moon_functions table for FCALL
                let ftbl: mlua::Table = lua
                    .globals()
                    .get("__moon_functions")
                    .map_err(|_| {
                        mlua::Error::RuntimeError("internal error".to_string())
                    })?;
                ftbl.set(
                    lua.create_string(name.as_ref())?,
                    callback,
                )?;

                // Store metadata
                funcs_clone.borrow_mut().insert(
                    name.clone(),
                    FunctionDef {
                        name,
                        description,
                        flags,
                    },
                );

                Ok(())
            })
            .map_err(|e| LoadError::LuaError(e.to_string()))?;

        // Set redis.register_function
        let redis_table: mlua::Table = lua
            .globals()
            .get("redis")
            .map_err(|e| LoadError::LuaError(e.to_string()))?;
        redis_table
            .set("register_function", register_fn)
            .map_err(|e| LoadError::LuaError(e.to_string()))?;

        // Evaluate the library body (everything after shebang)
        lua.load(rest)
            .exec()
            .map_err(|e| LoadError::LuaError(e.to_string()))?;

        let functions = Rc::try_unwrap(functions)
            .map(|cell| cell.into_inner())
            .unwrap_or_else(|rc| rc.borrow().clone());

        if functions.is_empty() {
            return Err(LoadError::NoFunctions);
        }

        Ok(Library {
            name: lib_name,
            engine: Bytes::from_static(b"lua"),
            source: Bytes::copy_from_slice(body),
            functions,
            lua,
        })
    }
}

// ---------------------------------------------------------------------------
// Shebang parser
// ---------------------------------------------------------------------------

/// Parse the shebang line from a FUNCTION LOAD body.
///
/// Expected format: `#!<engine> name=<libname>\n<rest>`
/// Returns `(libname, rest_of_body)`.
pub fn parse_shebang(body: &[u8]) -> Result<(Bytes, &[u8]), LoadError> {
    // Find first newline
    let newline_pos = body
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(LoadError::MissingShebang)?;

    let first_line = &body[..newline_pos];
    let rest = &body[newline_pos + 1..];

    // Must start with #!
    if !first_line.starts_with(b"#!") {
        return Err(LoadError::MissingShebang);
    }

    let header = &first_line[2..];

    // Parse engine and key=value pairs
    let header_str =
        std::str::from_utf8(header).map_err(|_| LoadError::MissingShebang)?;

    let mut parts = header_str.split_whitespace();
    let engine = parts.next().ok_or(LoadError::MissingShebang)?;

    if engine != "lua" {
        return Err(LoadError::BadEngine(engine.to_string()));
    }

    // Find name=<libname>
    let mut lib_name: Option<&str> = None;
    for part in parts {
        if let Some(val) = part.strip_prefix("name=") {
            lib_name = Some(val);
        }
    }

    let name = lib_name.ok_or(LoadError::MissingShebang)?;
    if name.is_empty() {
        return Err(LoadError::MissingShebang);
    }

    Ok((Bytes::copy_from_slice(name.as_bytes()), rest))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shebang_basic() {
        let body = b"#!lua name=mylib\nreturn 1";
        let (name, rest) = parse_shebang(body).unwrap();
        assert_eq!(name, Bytes::from_static(b"mylib"));
        assert_eq!(rest, b"return 1");
    }

    #[test]
    fn test_parse_shebang_missing() {
        let body = b"return 1";
        assert!(parse_shebang(body).is_err());
    }

    #[test]
    fn test_parse_shebang_bad_engine() {
        let body = b"#!python name=mylib\nreturn 1";
        match parse_shebang(body) {
            Err(LoadError::BadEngine(e)) => assert_eq!(e, "python"),
            other => panic!("Expected BadEngine, got {:?}", other.err()),
        }
    }

    #[test]
    fn test_parse_shebang_no_name() {
        let body = b"#!lua\nreturn 1";
        assert!(matches!(
            parse_shebang(body),
            Err(LoadError::MissingShebang)
        ));
    }

    #[test]
    fn test_load_and_lookup() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        let name = reg.load(body, false).unwrap();
        assert_eq!(name, Bytes::from_static(b"mylib"));

        let (lib, func) = reg.lookup(b"hello").unwrap();
        assert_eq!(lib.name, Bytes::from_static(b"mylib"));
        assert_eq!(func.name, Bytes::from_static(b"hello"));
    }

    #[test]
    fn test_load_duplicate_without_replace() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        reg.load(body, false).unwrap();
        assert!(matches!(
            reg.load(body, false),
            Err(LoadError::AlreadyExists(_))
        ));
    }

    #[test]
    fn test_load_replace() {
        let mut reg = FunctionRegistry::new();
        let body1 = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        let body2 = b"#!lua name=mylib\nredis.register_function('hello', function() return 'replaced' end)";
        reg.load(body1, false).unwrap();
        reg.load(body2, true).unwrap();
        assert!(reg.lookup(b"hello").is_some());
    }

    #[test]
    fn test_delete() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        reg.load(body, false).unwrap();
        assert!(reg.delete(b"mylib"));
        assert!(reg.lookup(b"hello").is_none());
    }

    #[test]
    fn test_flush() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        reg.load(body, false).unwrap();
        reg.flush();
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_list() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        reg.load(body, false).unwrap();
        let libs = reg.list();
        assert_eq!(libs.len(), 1);
        assert_eq!(libs[0].name, Bytes::from_static(b"mylib"));
    }

    #[test]
    fn test_table_form_registration() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function{function_name='hello', callback=function() return 'world' end, description='test func'}";
        let name = reg.load(body, false).unwrap();
        assert_eq!(name, Bytes::from_static(b"mylib"));
        let (_lib, func) = reg.lookup(b"hello").unwrap();
        assert_eq!(func.description.as_deref(), Some("test func"));
    }

    #[test]
    fn test_call_function() {
        let mut reg = FunctionRegistry::new();
        let body = b"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";
        reg.load(body, false).unwrap();

        let mut db = Database::new();
        let result =
            reg.call_function(b"hello", vec![], vec![], &mut db, 0, 1, false);
        assert!(
            matches!(result, Frame::BulkString(ref b) if *b == Bytes::from_static(b"world"))
        );
    }

    #[test]
    fn test_call_function_not_found() {
        let reg = FunctionRegistry::new();
        let mut db = Database::new();
        let result =
            reg.call_function(b"nonexistent", vec![], vec![], &mut db, 0, 1, false);
        assert!(matches!(result, Frame::Error(_)));
    }
}
