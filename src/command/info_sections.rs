//! INFO section selection, de-duplication, and the single assembly point.
//!
//! Moon builds INFO in more than one place. `connection::info` writes every
//! section including a STUB `# Replication`, and each connection handler then
//! appends the REAL replication section from
//! `replication::handshake::build_info_replication`. That is why `INFO`
//! historically emitted `# Replication` twice, and it is why filtering cannot
//! live inside `connection::info` alone — a filter applied before the append
//! would leak the appended section on every request.
//!
//! [`finalize`] is that single point: it takes the raw payload, optionally
//! substitutes the real replication section for the stub, drops any section
//! header seen twice, and then keeps only the sections the client asked for.
//!
//! Semantics measured against redis-server 8.6.1:
//!
//! ```text
//! INFO                  -> every section EXCEPT Commandstats/Latencystats
//! INFO all | everything -> every section
//! INFO replication      -> only that one, case-insensitively
//! INFO server clients   -> both, in the SERVER's order, not the caller's
//! INFO nosuchsection    -> empty payload, NOT an error
//! ```

use crate::protocol::Frame;

/// Sections omitted from a bare `INFO` and included only on explicit request
/// or via `all`/`everything`. Redis treats these as opt-in because they grow
/// with the command table rather than being fixed-size.
const NON_DEFAULT: [&str; 2] = ["commandstats", "latencystats"];

/// What the caller asked for.
enum Want {
    /// Bare `INFO` — everything except [`NON_DEFAULT`].
    Default,
    /// `INFO all` / `INFO everything`.
    All,
    /// An explicit list, already lowercased. May be empty (unknown section
    /// only), which correctly yields an empty payload rather than an error.
    Named(Vec<String>),
}

impl Want {
    fn from_args(args: &[Frame]) -> Self {
        let mut named: Vec<String> = Vec::new();
        for a in args {
            let raw = match a {
                Frame::BulkString(b) => b.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => continue,
            };
            let name = String::from_utf8_lossy(raw).to_ascii_lowercase();
            if name == "all" || name == "everything" {
                return Want::All;
            }
            // A repeated section must not duplicate the section in the reply.
            if !named.contains(&name) {
                named.push(name);
            }
        }
        if named.is_empty() {
            Want::Default
        } else {
            Want::Named(named)
        }
    }

    fn accepts(&self, section_lower: &str) -> bool {
        match self {
            Want::All => true,
            Want::Default => !NON_DEFAULT.contains(&section_lower),
            Want::Named(list) => list.iter().any(|n| n == section_lower),
        }
    }
}

/// Split an assembled INFO payload into `(header_line, body_including_header)`
/// chunks. Anything before the first header (there should be nothing) is
/// dropped rather than silently attached to the first section.
fn split_sections(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut current: Option<(String, String)> = None;
    for line in text.split_inclusive("\r\n") {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(name) = trimmed.strip_prefix("# ") {
            if let Some(prev) = current.take() {
                out.push(prev);
            }
            current = Some((name.trim().to_string(), line.to_string()));
        } else if let Some((_, body)) = current.as_mut() {
            body.push_str(line);
        }
    }
    if let Some(prev) = current {
        out.push(prev);
    }
    out
}

/// Assemble the final INFO reply.
///
/// `raw` is the payload built by `connection::info`. `real_replication`, when
/// present, is the authoritative `# Replication` section from the replication
/// subsystem; it REPLACES the stub rather than being appended, which is what
/// removes the duplicate header.
///
/// De-duplication keeps the FIRST occurrence of a section. Callers that have a
/// better version of a section must pass it in, not append it.
pub fn finalize(raw: &str, real_replication: Option<&str>, args: &[Frame]) -> Frame {
    let want = Want::from_args(args);
    let mut chunks = split_sections(raw);

    if let Some(real) = real_replication {
        // Substitute in place so the section keeps its canonical position.
        let real_body = real.to_string();
        if let Some(slot) = chunks
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case("Replication"))
        {
            slot.1 = ensure_trailing_blank(&real_body);
        } else {
            chunks.push(("Replication".to_string(), ensure_trailing_blank(&real_body)));
        }
    }

    let mut seen: Vec<String> = Vec::new();
    let mut out = String::with_capacity(raw.len());
    for (name, body) in chunks {
        let lower = name.to_ascii_lowercase();
        if seen.contains(&lower) {
            continue; // a header emitted twice would break map-building parsers
        }
        seen.push(lower.clone());
        if want.accepts(&lower) {
            out.push_str(&body);
        }
    }
    Frame::BulkString(bytes::Bytes::from(out))
}

/// Sections are blank-line separated; a substituted body that lacks the
/// separator would run into the next header.
fn ensure_trailing_blank(body: &str) -> String {
    if body.ends_with("\r\n\r\n") {
        body.to_string()
    } else if body.ends_with("\r\n") {
        format!("{body}\r\n")
    } else {
        format!("{body}\r\n\r\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn arg(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_string()))
    }

    fn text(f: Frame) -> String {
        match f {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    const RAW: &str = "# Server\r\nredis_version:7.4.0\r\n\r\n\
                       # Clients\r\nconnected_clients:1\r\n\r\n\
                       # Replication\r\nrole:master\r\n\r\n\
                       # Commandstats\r\n\r\n";

    #[test]
    fn default_omits_commandstats() {
        let got = text(finalize(RAW, None, &[]));
        assert!(got.contains("# Server"));
        assert!(
            !got.contains("# Commandstats"),
            "a bare INFO must omit Commandstats; got {got:?}"
        );
    }

    #[test]
    fn all_includes_commandstats() {
        let got = text(finalize(RAW, None, &[arg("all")]));
        assert!(got.contains("# Commandstats"));
    }

    #[test]
    fn single_section_only() {
        let got = text(finalize(RAW, None, &[arg("replication")]));
        assert!(got.starts_with("# Replication"), "got {got:?}");
        assert!(!got.contains("# Server"), "got {got:?}");
    }

    #[test]
    fn section_match_is_case_insensitive() {
        let lower = text(finalize(RAW, None, &[arg("replication")]));
        let upper = text(finalize(RAW, None, &[arg("REPLICATION")]));
        assert_eq!(lower, upper);
    }

    #[test]
    fn unknown_section_is_empty_not_error() {
        let got = text(finalize(RAW, None, &[arg("nosuchsection")]));
        assert!(got.is_empty(), "got {got:?}");
    }

    #[test]
    fn repeated_section_emitted_once() {
        let got = text(finalize(RAW, None, &[arg("server"), arg("server")]));
        assert_eq!(got.matches("# Server").count(), 1, "got {got:?}");
    }

    #[test]
    fn multiple_sections_use_server_order() {
        // Caller asks clients-then-server; the reply must still be
        // server-then-clients, because that is the assembly order.
        let got = text(finalize(RAW, None, &[arg("clients"), arg("server")]));
        let s = got.find("# Server").expect("server present");
        let c = got.find("# Clients").expect("clients present");
        assert!(s < c, "server must precede clients; got {got:?}");
    }

    #[test]
    fn real_replication_replaces_stub_without_duplicating() {
        let real = "# Replication\r\nrole:slave\r\nmaster_link_status:up\r\n";
        let got = text(finalize(RAW, Some(real), &[]));
        assert_eq!(
            got.matches("# Replication").count(),
            1,
            "the real section must REPLACE the stub, not append; got {got:?}"
        );
        assert!(got.contains("master_link_status:up"), "got {got:?}");
        assert!(
            !got.contains("role:master"),
            "the stub's body must be gone; got {got:?}"
        );
    }

    #[test]
    fn substituted_section_keeps_blank_separator() {
        // Without the separator the next header would be glued to this body.
        let real = "# Replication\r\nrole:slave\r\n";
        let got = text(finalize(RAW, Some(real), &[arg("all")]));
        assert!(
            got.contains("role:slave\r\n\r\n# Commandstats"),
            "sections must stay blank-line separated; got {got:?}"
        );
    }
}
