# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| latest  | ✅                 |

## Reporting a Vulnerability

If you discover a security vulnerability in setu, please do **not** open a public issue. Instead, send a private report to the maintainer directly.

**Contact:** gaurav@example.com (replace with actual maintainer email)

You should receive a response within **48 hours**. If you do not, please follow up.

### What to include

- Description of the vulnerability and its impact
- Steps to reproduce (config, schema, WAL events if applicable)
- Suggested fix or mitigation (if any)

## Security Considerations for This Project

setu connects to PostgreSQL via logical replication and delivers events to external HTTP endpoints. Key security properties:

- **No persistence of secrets** — tokens and webhook URLs are loaded at startup from `channels-env.yml` (gitignored) and held in memory only. No database stores them.
- **TLS by default** — the `reqwest` client uses `rustls`; all outbound traffic is encrypted.
- **Binary column filtering** — binary/TOAST column values are explicitly discarded (`null`) and never transmitted to destinations.
- **Offset integrity** — LSNs are never released to PostgreSQL until the destination returns 2xx, preventing event loss on crash.
- **No inbound network** — the binary listens to no ports; it connects outbound only.
