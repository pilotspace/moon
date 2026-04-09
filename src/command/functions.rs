//! FUNCTION LOAD/LIST/DELETE/FLUSH + FCALL/FCALL_RO command handlers.
//!
//! **Phase 101 limitation:** RAM-only. FUNCTION DUMP/RESTORE/STATS return
//! `-ERR ... not supported in this release (Phase 101 limitation)`.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::scripting::functions::FunctionRegistry;
use crate::storage::Database;

/// Handle `FUNCTION <subcommand> [args...]`.
///
/// Supported: LOAD, LIST, DELETE, FLUSH.
/// Deferred: DUMP, RESTORE, STATS (return documented error).
pub fn handle_function(
    registry: &mut FunctionRegistry,
    args: &[Frame],
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'function' command",
        ));
    }

    let sub = match &args[0] {
        Frame::BulkString(b) => b,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'function' command",
            ));
        }
    };

    if sub.eq_ignore_ascii_case(b"LOAD") {
        handle_function_load(registry, &args[1..])
    } else if sub.eq_ignore_ascii_case(b"LIST") {
        handle_function_list(registry, &args[1..])
    } else if sub.eq_ignore_ascii_case(b"DELETE") {
        handle_function_delete(registry, &args[1..])
    } else if sub.eq_ignore_ascii_case(b"FLUSH") {
        registry.flush();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else if sub.eq_ignore_ascii_case(b"DUMP") {
        Frame::Error(Bytes::from_static(
            b"ERR FUNCTION DUMP not supported in this release (Phase 101 limitation)",
        ))
    } else if sub.eq_ignore_ascii_case(b"RESTORE") {
        Frame::Error(Bytes::from_static(
            b"ERR FUNCTION RESTORE not supported in this release (Phase 101 limitation)",
        ))
    } else if sub.eq_ignore_ascii_case(b"STATS") {
        Frame::Error(Bytes::from_static(
            b"ERR FUNCTION STATS not supported in this release (Phase 101 limitation)",
        ))
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
            String::from_utf8_lossy(sub)
        )))
    }
}

/// FUNCTION LOAD [REPLACE] <body>
fn handle_function_load(
    registry: &mut FunctionRegistry,
    args: &[Frame],
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'function|load' command",
        ));
    }

    let mut replace = false;
    let body: &Bytes;

    // Parse: FUNCTION LOAD [REPLACE] <body>
    if args.len() == 1 {
        // FUNCTION LOAD <body>
        body = match &args[0] {
            Frame::BulkString(b) => b,
            _ => {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        };
    } else if args.len() == 2 {
        // FUNCTION LOAD REPLACE <body>
        let flag = match &args[0] {
            Frame::BulkString(b) => b,
            _ => {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        };
        if !flag.eq_ignore_ascii_case(b"REPLACE") {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        replace = true;
        body = match &args[1] {
            Frame::BulkString(b) => b,
            _ => {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        };
    } else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'function|load' command",
        ));
    }

    match registry.load(body, replace) {
        Ok(lib_name) => Frame::BulkString(lib_name),
        Err(e) => e.into_frame(),
    }
}

/// FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
fn handle_function_list(
    registry: &FunctionRegistry,
    args: &[Frame],
) -> Frame {
    let mut _pattern: Option<&[u8]> = None;
    let mut with_code = false;

    let mut i = 0;
    while i < args.len() {
        match &args[i] {
            Frame::BulkString(b) if b.eq_ignore_ascii_case(b"LIBRARYNAME") => {
                if i + 1 < args.len() {
                    if let Frame::BulkString(p) = &args[i + 1] {
                        _pattern = Some(p.as_ref());
                    }
                    i += 2;
                } else {
                    return Frame::Error(Bytes::from_static(b"ERR syntax error"));
                }
            }
            Frame::BulkString(b) if b.eq_ignore_ascii_case(b"WITHCODE") => {
                with_code = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let libs = registry.list();
    let mut result = Vec::with_capacity(libs.len());

    for lib in libs {
        // Each library is a flat array of key-value pairs (Redis 7.0 format):
        // ["library_name", name, "engine", "LUA", "functions", [...]]
        let mut entry = Vec::with_capacity(if with_code { 8 } else { 6 });

        entry.push(Frame::BulkString(Bytes::from_static(b"library_name")));
        entry.push(Frame::BulkString(lib.name.clone()));

        entry.push(Frame::BulkString(Bytes::from_static(b"engine")));
        entry.push(Frame::BulkString(Bytes::from_static(b"LUA")));

        // Functions array
        let func_list: Vec<Frame> = lib
            .functions
            .values()
            .map(|f| {
                let mut fentry = Vec::with_capacity(4);
                fentry.push(Frame::BulkString(Bytes::from_static(b"name")));
                fentry.push(Frame::BulkString(f.name.clone()));
                if let Some(desc) = &f.description {
                    fentry.push(Frame::BulkString(Bytes::from_static(b"description")));
                    fentry.push(Frame::BulkString(Bytes::copy_from_slice(
                        desc.as_bytes(),
                    )));
                }
                Frame::Array(fentry.into())
            })
            .collect();

        entry.push(Frame::BulkString(Bytes::from_static(b"functions")));
        entry.push(Frame::Array(func_list.into()));

        if with_code {
            entry.push(Frame::BulkString(Bytes::from_static(b"library_code")));
            entry.push(Frame::BulkString(lib.source.clone()));
        }

        result.push(Frame::Array(entry.into()));
    }

    Frame::Array(result.into())
}

/// FUNCTION DELETE <libname>
fn handle_function_delete(
    registry: &mut FunctionRegistry,
    args: &[Frame],
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'function|delete' command",
        ));
    }

    let lib_name = match &args[0] {
        Frame::BulkString(b) => b,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    };

    if registry.delete(lib_name) {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR Library '{}' not found",
            String::from_utf8_lossy(lib_name)
        )))
    }
}

/// Handle FCALL: look up function by name, parse numkeys, dispatch.
pub fn handle_fcall(
    registry: &FunctionRegistry,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
) -> Frame {
    handle_fcall_inner(registry, args, db, shard_id, num_shards, selected_db, db_count, false)
}

/// Handle FCALL_RO: same as FCALL but sets read-only mode.
pub fn handle_fcall_ro(
    registry: &FunctionRegistry,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
) -> Frame {
    handle_fcall_inner(registry, args, db, shard_id, num_shards, selected_db, db_count, true)
}

/// Inner FCALL implementation shared by FCALL and FCALL_RO.
fn handle_fcall_inner(
    registry: &FunctionRegistry,
    args: &[Frame],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
    selected_db: usize,
    db_count: usize,
    read_only: bool,
) -> Frame {
    // FCALL funcname numkeys [key ...] [arg ...]
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'fcall' command",
        ));
    }

    let func_name = match &args[0] {
        Frame::BulkString(b) => b,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid function name"));
        }
    };

    let numkeys: usize = match &args[1] {
        Frame::BulkString(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        },
        Frame::Integer(n) => {
            if *n < 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
            *n as usize
        }
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };

    if args.len() < 2 + numkeys {
        return Frame::Error(Bytes::from_static(
            b"ERR Number of keys can't be greater than number of args",
        ));
    }

    let mut keys: Vec<Bytes> = Vec::with_capacity(numkeys);
    for f in &args[2..2 + numkeys] {
        match f {
            Frame::BulkString(b) => keys.push(b.clone()),
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"ERR Invalid argument type for key",
                ));
            }
        }
    }

    // Validate cross-shard keys
    if num_shards > 1 {
        if let Some(err) =
            crate::scripting::validate_keys_same_shard(&keys, shard_id, num_shards)
        {
            return err;
        }
    }

    let mut argv: Vec<Bytes> = Vec::with_capacity(args.len().saturating_sub(2 + numkeys));
    for f in &args[2 + numkeys..] {
        match f {
            Frame::BulkString(b) => argv.push(b.clone()),
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"ERR Invalid argument type for arg",
                ));
            }
        }
    }

    registry.call_function(func_name, keys, argv, db, selected_db, db_count, read_only)
}
