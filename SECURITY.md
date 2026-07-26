# Security Policy

## Supported versions

Until Beni reaches 1.0, security fixes are released on the latest minor
version. Upgrade to the latest published version before reporting an issue.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
security advisory flow on the Beni repository instead:

https://github.com/ChiChuRita/beni/security/advisories/new

Include the affected version, runtime and adapter, a minimal reproduction, and
the impact you believe is possible. You should receive an acknowledgement
within 72 hours and a status update within seven days.

Beni sits on application data paths. Reports involving unsafe decoding,
cross-key access, lock ownership, transaction isolation, credential exposure,
or command injection are treated as high priority.
