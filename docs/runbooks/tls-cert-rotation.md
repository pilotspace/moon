# TLS Certificate Rotation

Rotate TLS certificates on a running Moon server without downtime.

## Prerequisites

- Moon running with `--tls-cert` and `--tls-key` flags
- New certificate and key files ready (PEM format)
- Certificate chain is valid (`openssl verify -CAfile ca.pem new-cert.pem`)

## Steps

### 1. Validate the new certificate

```bash
# Verify the certificate chain
openssl verify -CAfile ca.pem new-cert.pem

# Check the key matches the certificate
openssl x509 -noout -modulus -in new-cert.pem | md5sum
openssl rsa  -noout -modulus -in new-key.pem  | md5sum
# Both md5sums must match
```

### 2. Place new files on disk

Replace the certificate and key files at the paths Moon was started with.
Back up the old files first.

```bash
cp /etc/moon/tls/server.crt /etc/moon/tls/server.crt.bak
cp /etc/moon/tls/server.key /etc/moon/tls/server.key.bak
cp new-cert.pem /etc/moon/tls/server.crt
cp new-key.pem  /etc/moon/tls/server.key
chmod 600 /etc/moon/tls/server.key
```

### 3. Signal Moon to reload TLS config

```bash
kill -SIGHUP $(pidof moon)
```

Moon re-reads the certificate and key files on SIGHUP without dropping existing
connections. New connections will use the updated certificate.

### 4. Verify the new certificate is served

```bash
echo | openssl s_client -connect 127.0.0.1:6380 -servername moon 2>/dev/null \
  | openssl x509 -noout -dates -subject
```

Confirm the `notAfter` date and subject match the new certificate.

### 5. Test a client connection

```bash
redis-cli --tls --cert client.pem --key client-key.pem \
  --cacert ca.pem -h 127.0.0.1 -p 6380 PING
```

Expected: `PONG`

## Rollback

If the new certificate causes issues (handshake failures, wrong chain):

```bash
# Revert to backed-up files
cp /etc/moon/tls/server.crt.bak /etc/moon/tls/server.crt
cp /etc/moon/tls/server.key.bak /etc/moon/tls/server.key

# Reload again
kill -SIGHUP $(pidof moon)

# Verify old cert is served
echo | openssl s_client -connect 127.0.0.1:6380 -servername moon 2>/dev/null \
  | openssl x509 -noout -dates -subject
```

## Notes

- SIGHUP only reloads TLS certificates. It does not restart the server or drop data.
- If mTLS is enabled (`--tls-ca-cert`), the CA certificate file is also re-read on SIGHUP.
- Certificate files must be readable by the Moon process user.
