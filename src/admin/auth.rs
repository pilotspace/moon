//! Bearer-token auth for the admin/console HTTP port (HARD-01, Phase 137).
//!
//! Token format: `<user>.<base64url(HMAC-SHA256(secret, user))>`
//!
//! Properties:
//! - Stateless — no DB/session lookup required for verification.
//! - Per-user revocation is handled by rotating the secret (v1; OAuth2 deferred).
//! - Constant-time signature compare via `subtle::ConstantTimeEq` — never
//!   byte-wise `==`, which leaks timing.
//! - Empty secrets are rejected at construction so operator misconfig fails fast.
//!
//! This module is feature-gated behind `console` (see `src/admin/mod.rs`).

use base64::Engine;
use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Auth policy for the admin HTTP port.
///
/// Use [`AuthPolicy::enabled`] to enforce Bearer auth, or [`AuthPolicy::disabled`]
/// for the default "no auth" mode (backwards compatible with v0.1.5).
pub struct AuthPolicy {
    required: bool,
    secret: Vec<u8>,
}

impl AuthPolicy {
    /// Build an enforcing policy. Rejects empty secrets.
    ///
    /// The secret is used as the HMAC-SHA256 key for [`issue`](Self::issue)
    /// and [`verify_header`](Self::verify_header).
    pub fn enabled(secret: impl AsRef<[u8]>) -> Result<Self, &'static str> {
        if secret.as_ref().is_empty() {
            return Err("--console-auth-secret must not be empty");
        }
        Ok(Self {
            required: true,
            secret: secret.as_ref().to_vec(),
        })
    }

    /// Pass-through policy. [`verify_header`](Self::verify_header) always
    /// succeeds regardless of the supplied header.
    pub fn disabled() -> Self {
        Self {
            required: false,
            secret: Vec::new(),
        }
    }

    /// Returns whether this policy enforces auth on every non-exempt request.
    #[inline]
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Compute a bearer token for the given user id. Operators store this
    /// value and send it as `Authorization: Bearer <token>`.
    pub fn issue(&self, user_id: &str) -> String {
        // HMAC key length is unrestricted; only an empty secret would have been
        // rejected at construction, so this unwrap is post-invariant safe.
        #[allow(clippy::unwrap_used)] // invariant: secret verified non-empty in `enabled`
        let mut mac = HmacSha256::new_from_slice(&self.secret).unwrap();
        mac.update(user_id.as_bytes());
        let sig = mac.finalize().into_bytes();
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
        format!("{}.{}", user_id, b64)
    }

    /// Verify a full `Authorization` header value. Returns the embedded user id
    /// on success. When [`is_required`](Self::is_required) is false the header
    /// is ignored and `"anonymous"` is returned.
    pub fn verify_header(&self, header: Option<&str>) -> Result<String, &'static str> {
        if !self.required {
            return Ok("anonymous".to_string());
        }
        let hv = header.ok_or("missing Authorization header")?;
        let token = hv.strip_prefix("Bearer ").ok_or("expected Bearer scheme")?;
        let (user, b64) = token.split_once('.').ok_or("malformed token")?;
        if user.is_empty() {
            return Err("empty user in token");
        }
        let provided = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64)
            .map_err(|_| "token not base64url")?;

        #[allow(clippy::unwrap_used)] // invariant: secret non-empty
        let mut mac = HmacSha256::new_from_slice(&self.secret).unwrap();
        mac.update(user.as_bytes());
        let expected = mac.finalize().into_bytes();

        // Constant-time compare (subtle): prevents timing side-channels in
        // signature verification. Length mismatch is handled by short-circuit
        // to a fixed-time false result.
        if provided.len() != expected.len() {
            return Err("signature length mismatch");
        }
        if provided.ct_eq(expected.as_slice()).unwrap_u8() != 1 {
            return Err("signature mismatch");
        }
        Ok(user.to_string())
    }
}

impl Default for AuthPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_accepts_anything() {
        let p = AuthPolicy::disabled();
        assert!(p.verify_header(None).is_ok());
        assert!(p.verify_header(Some("garbage")).is_ok());
        assert!(!p.is_required());
    }

    #[test]
    fn enabled_requires_header() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        assert!(p.is_required());
        assert!(p.verify_header(None).is_err());
    }

    #[test]
    fn enabled_rejects_missing_bearer_prefix() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        assert!(p.verify_header(Some("Basic abc")).is_err());
    }

    #[test]
    fn enabled_rejects_malformed_token() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        assert!(p.verify_header(Some("Bearer no-dot-here")).is_err());
    }

    #[test]
    fn enabled_rejects_bad_base64() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        assert!(p.verify_header(Some("Bearer alice.!!!not-b64!!!")).is_err());
    }

    #[test]
    fn enabled_rejects_bad_signature() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        // Valid base64url but wrong signature.
        assert!(p.verify_header(Some("Bearer alice.QUJDREVGRw")).is_err());
    }

    #[test]
    fn enabled_accepts_issued_token() {
        let p = AuthPolicy::enabled(b"top-secret").unwrap();
        let token = p.issue("alice");
        let header = format!("Bearer {}", token);
        assert_eq!(p.verify_header(Some(&header)).unwrap(), "alice");
    }

    #[test]
    fn tokens_are_user_scoped() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        let alice = p.issue("alice");
        // Swapping the user in the prefix should invalidate the signature.
        let (_, sig) = alice.split_once('.').unwrap();
        let tampered = format!("Bearer mallory.{}", sig);
        assert!(p.verify_header(Some(&tampered)).is_err());
    }

    #[test]
    fn empty_secret_rejected() {
        assert!(AuthPolicy::enabled(b"").is_err());
    }

    #[test]
    fn empty_user_rejected() {
        let p = AuthPolicy::enabled(b"secret").unwrap();
        assert!(p.verify_header(Some("Bearer .abcdefgh")).is_err());
    }
}
