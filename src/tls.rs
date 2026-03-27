use std::io;
use std::sync::Arc;

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
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Unknown cipher suite: '{}'. Valid: TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256, TLS_CHACHA20_POLY1305_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, etc.", unknown),
                ));
            }
        };
        suites.push(suite);
    }
    if suites.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "No valid cipher suites specified"));
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
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS cert parse: {}", e)))?;

    // Load private key
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("TLS key file: {}", e)))?;
    let mut key_reader = io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS key parse: {}", e)))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No private key found in TLS key file"))?;

    // Build server config -- with or without cipher suite filtering
    let config_builder = if let Some(suite_names) = ciphersuites {
        // Filter cipher suites: parse names, match to aws-lc-rs constants
        let suites = resolve_cipher_suites(suite_names)?;
        let provider = rustls::crypto::CryptoProvider {
            cipher_suites: suites,
            ..rustls::crypto::aws_lc_rs::default_provider()
        };
        rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS protocol versions: {}", e)))?
    } else {
        rustls::ServerConfig::builder()
    };

    let config = if let Some(ca_path) = ca_cert_path {
        // mTLS: require client certificates
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("TLS CA cert file: {}", e)))?;
        let mut ca_reader = io::BufReader::new(ca_file);
        let ca_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("CA cert parse: {}", e)))?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store.add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("CA cert add: {}", e)))?;
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Client verifier: {}", e)))?;

        config_builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key.into())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {}", e)))?
    } else {
        // No mTLS: accept any client
        config_builder
            .with_no_client_auth()
            .with_single_cert(certs, key.into())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {}", e)))?
    };

    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_cipher_suites_tls13() {
        let suites = resolve_cipher_suites("TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256").unwrap();
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
        assert!(result.unwrap_err().to_string().contains("Unknown cipher suite"));
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
        let dir = std::env::temp_dir().join("rust-redis-tls-test");
        let _ = std::fs::create_dir_all(&dir);
        let cert_path = dir.join("test.crt");
        std::fs::write(&cert_path, "not a real cert").unwrap();

        let result = build_tls_config(cert_path.to_str().unwrap(), "/nonexistent/key.pem", None, None);
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_tls_config_missing_ca() {
        let result = build_tls_config(
            "/nonexistent/cert.pem", "/nonexistent/key.pem",
            Some("/nonexistent/ca.pem"), None,
        );
        assert!(result.is_err());
    }
}
