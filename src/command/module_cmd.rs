//! `MODULE` — the container Redis clients call on connect to feature-detect.
//!
//! moon has no module system, and is unlikely to grow one: the whole point of
//! the design is a single process with no dynamically loaded C. That makes
//! `MODULE LIST` trivially answerable and *worth answering* — a client that
//! probes it on connect reads `-ERR unknown command` as a broken server, while
//! `*0` is simply the truth (moon#636).
//!
//! Every reply below was captured from redis-server 8.6.1 on 2026-08-23. The
//! loader subcommands reuse redis's own refusal text verbatim rather than
//! inventing a moon-specific one: redis ships with `enable-module-command no`,
//! so that message is what a stock server answers, and it already says the
//! only true thing — this server will not load a module for you.

use bytes::Bytes;

use crate::protocol::Frame;

use crate::command::helpers::extract_bytes;

/// redis's refusal when `enable-module-command` is not set. moon has no such
/// option because it has no loader, so this is the permanent answer.
const NOT_ALLOWED: &[u8] = b"ERR MODULE command not allowed. If the enable-module-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.";

/// `MODULE <subcommand> [<arg> ...]`
pub fn module(args: &[Frame]) -> Frame {
    // `MODULE` bare is an arity error on the CONTAINER, not an unknown
    // subcommand — a distinction redis makes and clients read.
    let Some(sub) = args.first().and_then(extract_bytes) else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'module' command",
        ));
    };

    if sub.eq_ignore_ascii_case(b"LIST") {
        // Arity 2 exactly: `MODULE LIST foo` is an error on the SUBCOMMAND,
        // named `module|list`, not on the container.
        if args.len() != 1 {
            return Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'module|list' command",
            ));
        }
        // No modules, ever. An empty array, not an error and not a null.
        return Frame::Array(Vec::new().into());
    }

    if sub.eq_ignore_ascii_case(b"LOAD")
        || sub.eq_ignore_ascii_case(b"LOADEX")
        || sub.eq_ignore_ascii_case(b"UNLOAD")
    {
        return Frame::Error(Bytes::from_static(NOT_ALLOWED));
    }

    if sub.eq_ignore_ascii_case(b"HELP") {
        return Frame::Array(
            vec![
                Frame::SimpleString(Bytes::from_static(
                    b"MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                )),
                Frame::SimpleString(Bytes::from_static(b"LIST")),
                Frame::SimpleString(Bytes::from_static(
                    b"    Return a list of loaded modules. moon loads none, so this is always empty.",
                )),
                Frame::SimpleString(Bytes::from_static(b"LOAD <path> [<arg> ...]")),
                Frame::SimpleString(Bytes::from_static(b"LOADEX <path> [<arg> ...]")),
                Frame::SimpleString(Bytes::from_static(b"UNLOAD <name>")),
                Frame::SimpleString(Bytes::from_static(
                    b"    Not supported: moon has no module loader.",
                )),
                Frame::SimpleString(Bytes::from_static(b"HELP")),
                Frame::SimpleString(Bytes::from_static(b"    Print this help.")),
            ]
            .into(),
        );
    }

    crate::command::helpers::err_unknown_subcommand("MODULE", sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    #[test]
    fn module_list_is_an_empty_array_not_an_error() {
        // The whole point: a client feature-detecting on connect must get a
        // valid, empty answer rather than something it reads as a broken
        // server.
        match module(&[b(b"LIST")]) {
            Frame::Array(a) => assert!(a.is_empty(), "moon loads no modules"),
            other => panic!("MODULE LIST must be an empty array, got {other:?}"),
        }
        // Case-insensitive, like every other subcommand.
        assert!(matches!(module(&[b(b"list")]), Frame::Array(ref a) if a.is_empty()));
    }

    /// The control that stops this container from answering `MODULE LIST` to
    /// everything. redis distinguishes three failures here and so must moon:
    /// a bare container (container arity), a subcommand given extra args
    /// (SUBCOMMAND arity, named `module|list`), and an unknown subcommand.
    #[test]
    fn module_distinguishes_its_three_refusals() {
        assert_eq!(
            module(&[]),
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'module' command"
            ))
        );
        assert_eq!(
            module(&[b(b"LIST"), b(b"extra")]),
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'module|list' command"
            )),
            "an over-long LIST is an error on the subcommand, not the container"
        );
        assert_eq!(
            module(&[b(b"BOGUS")]),
            Frame::Error(Bytes::from(
                "ERR unknown subcommand 'BOGUS'. Try MODULE HELP.".to_string()
            ))
        );
        // The unknown-subcommand reply echoes the CLIENT's casing, as redis's
        // does — it is quoting what was sent, not naming a canonical form.
        assert_eq!(
            module(&[b(b"bOgUs")]),
            Frame::Error(Bytes::from(
                "ERR unknown subcommand 'bOgUs'. Try MODULE HELP.".to_string()
            ))
        );
    }

    #[test]
    fn the_loader_subcommands_are_refused_in_redis_words() {
        for sub in [b"LOAD".as_slice(), b"LOADEX", b"UNLOAD"] {
            let got = module(&[b(sub), b(b"/some/path.so")]);
            assert_eq!(
                got,
                Frame::Error(Bytes::from_static(NOT_ALLOWED)),
                "{} must be refused with redis's own text",
                String::from_utf8_lossy(sub)
            );
        }
    }

    #[test]
    fn module_help_is_an_array() {
        match module(&[b(b"HELP")]) {
            Frame::Array(a) => assert!(!a.is_empty()),
            other => panic!("MODULE HELP must be an array, got {other:?}"),
        }
    }
}
