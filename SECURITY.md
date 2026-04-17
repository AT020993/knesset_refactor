# Security Policy

## Supported versions

Active development happens on `main`. The `v1.0.0` tag pins the Parquet snapshot contract consumed by downstream repos (see [`CLAUDE.md`](CLAUDE.md) and [`src/data/snapshots/exporter.py`](src/data/snapshots/exporter.py)). Security fixes land on `main`; older tagged versions are not backported unless a downstream consumer still depends on them.

## Reporting a vulnerability

**Please do not open a public issue for vulnerabilities.**

Use GitHub's Private Vulnerability Reporting (PVR) channel:

**[Report a vulnerability](https://github.com/AT020993/knesset_refactor/security/advisories/new)**

This routes your report into a private security advisory visible only to you and the maintainers, with a built-in audit trail and a coordinated-disclosure workflow.

Please include, where possible:

- A description of the issue and the impact you believe it has.
- Steps to reproduce, a proof-of-concept, or affected commit(s).
- Any mitigations you are already aware of.

You can expect an acknowledgement within a few days. Fix turnaround depends on severity, but we will keep you updated inside the advisory thread until the issue is resolved or a coordinated disclosure timeline is agreed.

## Scope

In scope:

- Source code in this repository (`src/`, `scripts/`, CI workflows).
- Pinned dependencies declared in `requirements.txt`, `requirements-dev.txt`, `pyproject.toml`, and `uv.lock`.
- Deployment guidance in [`DEPLOYMENT_GUIDE.md`](DEPLOYMENT_GUIDE.md) (secrets handling, GCS credential flow).

Out of scope:

- The upstream [Knesset OData API](http://knesset.gov.il/Odata/ParliamentInfo.svc). Data fetched from it is public by nature; report API-side issues to the Knesset IT operators directly.
- User-deployed Streamlit Cloud instances (your credentials, your GCS bucket). This repo is a codebase, not a managed service.
- Denial-of-service against the upstream API via this client. The circuit breaker and rate limiter are there to be polite; please do not turn them into an attack surface.

## Sensitive data

This repo handles public parliamentary data, not PII. However, the CAP annotation system stores researcher credentials (hashed with bcrypt) and the GCS sync flow uses service-account keys. If you spot a regression that weakens credential hashing, leaks service-account keys, or bypasses authentication, please report via PVR immediately.
