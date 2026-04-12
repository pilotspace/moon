//! CORS origin allowlist (HARD-02, Phase 137).
//!
//! Admin port v0.1.5 emitted `Access-Control-Allow-Origin: *` unconditionally.
//! Once the port is exposed beyond loopback that is unsafe: any webpage in
//! the browser could call the admin REST API.
//!
//! This module implements a strict origin allowlist:
//! - Default allowlist covers the Vite dev server on localhost:5173 so the
//!   bundled console keeps working out of the box.
//! - Operators pass `--console-cors-origin <url>` one or more times to
//!   restrict the set.
//! - Wildcard `*` is permitted only when `--console-auth-required` is false,
//!   since `*` + credentials is explicitly forbidden by the CORS spec.
//!
//! The policy returns the matched origin (never `*` when the allowlist is
//! finite), which is what browsers require when `credentials: "include"` is
//! used on the fetch side.

/// CORS policy for the admin HTTP port.
pub struct CorsPolicy {
    origins: Vec<String>,
    wildcard: bool,
}

impl CorsPolicy {
    /// Build a new policy.
    ///
    /// - `configured` is the raw list from `--console-cors-origin`.
    /// - `auth_required` mirrors `--console-auth-required`. When true, a
    ///   wildcard entry is rejected at startup (see error string).
    ///
    /// If `configured` is empty, a sensible loopback default is installed
    /// (Vite dev server on ports 5173 on both `localhost` and `127.0.0.1`).
    pub fn new(configured: &[String], auth_required: bool) -> Result<Self, &'static str> {
        let wildcard = configured.iter().any(|o| o == "*");
        if wildcard && auth_required {
            return Err(
                "--console-cors-origin '*' is not permitted with --console-auth-required \
                 (wildcard + credentials is forbidden by the CORS spec)",
            );
        }
        let origins = if configured.is_empty() {
            vec![
                "http://localhost:5173".to_string(),
                "http://127.0.0.1:5173".to_string(),
            ]
        } else {
            configured.to_vec()
        };
        Ok(Self { origins, wildcard })
    }

    /// Return the header value to echo for a request's `Origin`, if any.
    ///
    /// - `None` means "do not emit `Access-Control-Allow-Origin`" — the
    ///   browser will block the cross-origin response.
    /// - When the policy is a wildcard (no-auth mode), returns `"*"`.
    /// - Otherwise returns the exact matched origin string so that
    ///   credentialed requests pass the browser's same-origin check.
    pub fn allowed_header(&self, request_origin: Option<&str>) -> Option<String> {
        if self.wildcard {
            return Some("*".to_string());
        }
        let origin = request_origin?;
        if self.origins.iter().any(|o| o == origin) {
            Some(origin.to_string())
        } else {
            None
        }
    }

    /// Whether this policy is a wildcard policy (only possible without auth).
    #[inline]
    pub fn is_wildcard(&self) -> bool {
        self.wildcard
    }

    /// Read-only view of the configured origins (for diagnostics / logging).
    pub fn origins(&self) -> &[String] {
        &self.origins
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_with_auth_rejected() {
        let r = CorsPolicy::new(&["*".to_string()], true);
        assert!(r.is_err());
    }

    #[test]
    fn wildcard_without_auth_ok() {
        let p = CorsPolicy::new(&["*".to_string()], false).unwrap();
        assert!(p.is_wildcard());
        assert_eq!(
            p.allowed_header(Some("https://any.example")),
            Some("*".to_string())
        );
        // Wildcard also matches when no Origin header is present (some tooling).
        assert_eq!(p.allowed_header(None), Some("*".to_string()));
    }

    #[test]
    fn default_allows_vite_dev() {
        let p = CorsPolicy::new(&[], false).unwrap();
        assert!(!p.is_wildcard());
        assert!(p.allowed_header(Some("http://localhost:5173")).is_some());
        assert!(p.allowed_header(Some("http://127.0.0.1:5173")).is_some());
    }

    #[test]
    fn finite_allowlist_echoes_exact_origin_not_wildcard() {
        let p = CorsPolicy::new(&["https://console.example".to_string()], true).unwrap();
        assert_eq!(
            p.allowed_header(Some("https://console.example")),
            Some("https://console.example".to_string())
        );
    }

    #[test]
    fn finite_allowlist_blocks_others() {
        let p = CorsPolicy::new(&["https://console.example".to_string()], true).unwrap();
        assert!(p.allowed_header(Some("https://evil.example")).is_none());
    }

    #[test]
    fn no_origin_no_header_on_finite_allowlist() {
        let p = CorsPolicy::new(&["https://x".to_string()], true).unwrap();
        assert!(p.allowed_header(None).is_none());
    }

    #[test]
    fn origins_view_returns_configured_list() {
        let p = CorsPolicy::new(
            &["https://a".to_string(), "https://b".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(p.origins(), &["https://a".to_string(), "https://b".to_string()]);
    }
}
