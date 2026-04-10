use std::io;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::info;

/// Map cipher suite name strings to rustls cipher suite constants.
/// Supports both TLS 1.3 and TLS 1.2 suites from aws-lc-rs provider.
fn resolve_cipher_suites(names: &str) -> io::Result<Vec<rustls::SupportedCipherSuite>> {
    use rustls::crypto::aws_lc_rs::cipher_suite;

    let mut suites = Vec::new();
    for name in names.split(',').map(|s| s.trim()) {
        let suite = match name {
            // TLS 1.3 suites
            "TLS_AES_256_GCM_SHA384" => cipher_suite::TLS13_AES_256_GCM_SHA384,
            "TLS_AES_128_GCM_SHA256" => cipher_suite::TLS13_AES_128_GCM_SHA256,
            "TLS_CHACHA20_POLY1305_SHA256" => cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
            // TLS 1.2 suites (if tls12 feature enabled)
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => {
                cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
            }
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => {
                cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
            }
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => {
                cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
            }
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => {
                cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
            }
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => {
                cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
            }
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => {
                cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
            }
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Unknown cipher suite: '{}'. Valid: TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256, TLS_CHACHA20_POLY1305_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, etc.",
                        unknown
                    ),
                ));
            }
        };
        suites.push(suite);
    }
    if suites.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No valid cipher suites specified",
        ));
    }
    Ok(suites)
}

/// Build a rustls ServerConfig from server configuration.
///
/// Loads certificate chain and private key from PEM files.
/// Optionally configures client certificate verification (mTLS).
/// Filters cipher suites if --tls-ciphersuites is specified.
pub fn build_tls_config(
    cert_path: &str,
    key_path: &str,
    ca_cert_path: Option<&str>,
    ciphersuites: Option<&str>,
) -> io::Result<Arc<rustls::ServerConfig>> {
    // Install aws-lc-rs as default crypto provider
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Load certificate chain
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("TLS cert file: {}", e)))?;
    let mut cert_reader = io::BufReader::new(cert_file);
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("TLS cert parse: {}", e))
            })?;

    // Load private key
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("TLS key file: {}", e)))?;
    let mut key_reader = io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS key parse: {}", e)))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "No private key found in TLS key file",
            )
        })?;

    // Explicit default cipher suite allowlist.
    //
    // When --tls-ciphersuites is not specified, Moon uses this frozen set instead
    // of accepting whatever rustls ships as defaults. This prevents a rustls
    // upgrade from silently enabling weaker suites.
    //
    // Allowlist (all AEAD-only, PFS-required):
    //   TLS 1.3: AES-256-GCM, AES-128-GCM, CHACHA20-POLY1305
    //   TLS 1.2: ECDHE-ECDSA + ECDHE-RSA variants of the above
    const DEFAULT_CIPHER_SUITES: &str = "\
        TLS_AES_256_GCM_SHA384,\
        TLS_AES_128_GCM_SHA256,\
        TLS_CHACHA20_POLY1305_SHA256,\
        TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,\
        TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,\
        TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,\
        TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,\
        TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,\
        TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256";

    // Build server config with explicit cipher suite allowlist
    let suite_names = ciphersuites.unwrap_or(DEFAULT_CIPHER_SUITES);
    let suites = resolve_cipher_suites(suite_names)?;
    let provider = rustls::crypto::CryptoProvider {
        cipher_suites: suites,
        ..rustls::crypto::aws_lc_rs::default_provider()
    };
    let config_builder = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TLS protocol versions: {}", e),
            )
        })?;

    let config = if let Some(ca_path) = ca_cert_path {
        // mTLS: require client certificates
        let ca_file = std::fs::File::open(ca_path).map_err(|e| {
            io::Error::new(io::ErrorKind::NotFound, format!("TLS CA cert file: {}", e))
        })?;
        let mut ca_reader = io::BufReader::new(ca_file);
        let ca_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("CA cert parse: {}", e))
                })?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store.add(cert).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("CA cert add: {}", e))
            })?;
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Client verifier: {}", e),
                )
            })?;

        config_builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {}", e)))?
    } else {
        // No mTLS: accept any client
        config_builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {}", e)))?
    };

    Ok(Arc::new(config))
}

/// Shared TLS configuration that supports atomic hot-reload via SIGHUP.
///
/// On SIGHUP, cert/key files are re-read from disk and the `ArcSwap` is
/// updated atomically. In-flight TLS handshakes continue with the old config;
/// new connections immediately pick up the reloaded config.
pub type SharedTlsConfig = Arc<ArcSwap<rustls::ServerConfig>>;

/// Wrap a freshly-built `Arc<ServerConfig>` in an `ArcSwap` for hot-reload.
pub fn make_shared(config: Arc<rustls::ServerConfig>) -> SharedTlsConfig {
    Arc::new(ArcSwap::from(config))
}

/// Reload TLS certificates from disk and swap into the shared config.
///
/// Returns `Ok(())` on success. On failure, the existing config is untouched
/// and an error is logged — the server continues serving with the old certs.
pub fn reload_tls_config(
    shared: &SharedTlsConfig,
    cert_path: &str,
    key_path: &str,
    ca_cert_path: Option<&str>,
    ciphersuites: Option<&str>,
) -> io::Result<()> {
    let new_config = build_tls_config(cert_path, key_path, ca_cert_path, ciphersuites)?;
    shared.store(new_config);
    info!("TLS certificates reloaded successfully");
    Ok(())
}

/// Spawn a background thread that listens for SIGHUP and reloads TLS certs.
///
/// Works on both tokio and monoio runtimes — uses a plain OS thread with
/// `sigwait()` to avoid async runtime dependencies.
#[cfg(target_os = "linux")]
pub fn spawn_sighup_reload_thread(
    shared: SharedTlsConfig,
    cert_path: String,
    key_path: String,
    ca_cert_path: Option<String>,
    ciphersuites: Option<String>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};

    static SIGHUP_REGISTERED: AtomicBool = AtomicBool::new(false);
    if SIGHUP_REGISTERED.swap(true, Ordering::SeqCst) {
        tracing::warn!("SIGHUP reload thread already registered, skipping duplicate");
        return;
    }

    std::thread::Builder::new()
        .name("tls-sighup".to_string())
        .spawn(move || {
            // Block SIGHUP in this thread and use sigwait to receive it
            // SAFETY: sigset_t is a plain C struct, sigemptyset/sigaddset/sigwait
            // are signal-safe POSIX functions. We block SIGHUP only in this thread.
            unsafe {
                let mut set: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut set);
                libc::sigaddset(&mut set, libc::SIGHUP);
                libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());

                loop {
                    let mut sig: libc::c_int = 0;
                    let ret = libc::sigwait(&set, &mut sig);
                    if ret != 0 {
                        tracing::error!("sigwait failed: {}", io::Error::from_raw_os_error(ret));
                        continue;
                    }
                    if sig == libc::SIGHUP {
                        info!("SIGHUP received — reloading TLS certificates");
                        if let Err(e) = reload_tls_config(
                            &shared,
                            &cert_path,
                            &key_path,
                            ca_cert_path.as_deref(),
                            ciphersuites.as_deref(),
                        ) {
                            tracing::error!("TLS reload failed, keeping old certificates: {}", e);
                        }
                    }
                }
            }
        })
        .expect("failed to spawn TLS SIGHUP reload thread");

    info!("TLS SIGHUP reload thread started — send SIGHUP to reload certificates");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_cipher_suites_tls13() {
        let suites =
            resolve_cipher_suites("TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256").unwrap();
        assert_eq!(suites.len(), 2);
    }

    #[test]
    fn test_resolve_cipher_suites_tls12() {
        let suites = resolve_cipher_suites("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384").unwrap();
        assert_eq!(suites.len(), 1);
    }

    #[test]
    fn test_resolve_cipher_suites_unknown() {
        let result = resolve_cipher_suites("TLS_INVALID_SUITE");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown cipher suite")
        );
    }

    #[test]
    fn test_resolve_cipher_suites_empty() {
        let result = resolve_cipher_suites("");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_tls_config_missing_cert() {
        let result = build_tls_config("/nonexistent/cert.pem", "/nonexistent/key.pem", None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("TLS cert file"));
    }

    #[test]
    fn test_build_tls_config_missing_key() {
        // Create a temp cert file but no key
        let dir = std::env::temp_dir().join("moon-tls-test");
        let _ = std::fs::create_dir_all(&dir);
        let cert_path = dir.join("test.crt");
        std::fs::write(&cert_path, "not a real cert").unwrap();

        let result = build_tls_config(
            cert_path.to_str().unwrap(),
            "/nonexistent/key.pem",
            None,
            None,
        );
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_tls_config_missing_ca() {
        let result = build_tls_config(
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
            Some("/nonexistent/ca.pem"),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_reload_tls_config_swaps_config() {
        let dir = std::env::temp_dir().join("moon-tls-reload-test");
        let _ = std::fs::create_dir_all(&dir);
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");

        // Generate self-signed cert+key using openssl CLI if available,
        // otherwise skip. This test targets Linux CI where openssl is present.
        let status = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-keyout",
                key_path.to_str().unwrap(),
                "-out",
                cert_path.to_str().unwrap(),
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=moon-reload-test-1",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        let ok = matches!(status, Ok(s) if s.success());
        if !ok {
            eprintln!("openssl not available, skipping TLS reload test");
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }

        // Build initial config
        let config = build_tls_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            None,
            None,
        )
        .expect("initial TLS config should build");

        let shared = make_shared(config);
        let ptr_before = Arc::as_ptr(&shared.load());

        // Overwrite cert+key with a new self-signed pair (different CN)
        let status2 = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-keyout",
                key_path.to_str().unwrap(),
                "-out",
                cert_path.to_str().unwrap(),
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=moon-reload-test-2",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        assert!(matches!(status2, Ok(s) if s.success()));

        // Reload — should swap the config
        reload_tls_config(
            &shared,
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            None,
            None,
        )
        .expect("reload should succeed");

        let ptr_after = Arc::as_ptr(&shared.load());
        assert_ne!(
            ptr_before, ptr_after,
            "ArcSwap should point to new config after reload"
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_reload_tls_config_failure_keeps_old() {
        let dir = std::env::temp_dir().join("moon-tls-reload-fail-test");
        let _ = std::fs::create_dir_all(&dir);
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");

        // Generate valid cert+key
        let status = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-keyout",
                key_path.to_str().unwrap(),
                "-out",
                cert_path.to_str().unwrap(),
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=moon-fail-test",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        if !matches!(status, Ok(s) if s.success()) {
            eprintln!("openssl not available, skipping TLS failure test");
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }

        let config = build_tls_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            None,
            None,
        )
        .expect("initial config should build");

        let shared = make_shared(config);
        let ptr_before = Arc::as_ptr(&shared.load());

        // Delete the cert file to make reload fail
        std::fs::remove_file(&cert_path).unwrap();

        let result = reload_tls_config(
            &shared,
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            None,
            None,
        );
        assert!(result.is_err(), "reload should fail with missing cert");

        // Config should be unchanged
        let ptr_after = Arc::as_ptr(&shared.load());
        assert_eq!(
            ptr_before, ptr_after,
            "failed reload must not change config"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
