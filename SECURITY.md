# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability in Moon, please report it responsibly:

1. **Do NOT open a public GitHub issue.**
2. Email: security@pilotspace.io (or use [GitHub Security Advisories](https://github.com/pilotspace/moon/security/advisories/new))
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Impact assessment
   - Suggested fix (if any)

## Response Timeline

- **Acknowledgment:** within 48 hours
- **Triage + severity assessment:** within 7 days
- **Fix development:** within 30 days for Critical/High, 90 days for Medium/Low
- **Disclosure:** coordinated disclosure after fix is released, with 90-day maximum embargo

## Scope

In scope:
- Memory safety issues (buffer overflow, use-after-free, data races)
- RESP protocol parsing vulnerabilities (malformed input → crash/hang)
- ACL bypass (unauthorized command execution, key pattern escape)
- Lua sandbox escape (access to filesystem, network, OS functions)
- TLS configuration weaknesses (downgrade attacks, weak ciphers)
- Denial of service via resource exhaustion (unbounded allocation from client input)
- Replication protocol vulnerabilities (replica impersonation)

Out of scope:
- Performance issues (unless they constitute a DoS vector)
- Features working as documented
- Social engineering
- Physical security

## Security Measures

- **Fuzzing:** cargo-fuzz targets for RESP parser, WAL decoder, RDB loader, cluster bus, ACL rules (Phase 89)
- **Unsafe audit:** 156/156 unsafe blocks annotated with SAFETY comments, CI-enforced (Phase 90)
- **Supply chain:** `cargo audit` + `cargo deny` blocking in CI (Phase 98)
- **SBOM:** CycloneDX generated per release (Phase 98)
- **Signed releases:** cosign with provenance attestation (Phase 99)

## Credits

We gratefully acknowledge security researchers who report vulnerabilities responsibly. Contributors will be credited in the release notes and this file (with permission).
