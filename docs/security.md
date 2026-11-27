# Security notes

## Current safeguards

- Credentials are expected in ignored `.env` files; examples contain
  placeholders. Never commit secrets, local database exports, or generated
  datasets.
- Airflow requests use Basic authentication. DAG-to-backend execution uses
  `PIPELINE_INTERNAL_TOKEN`; API calls fail when that token is absent or
  invalid.
- CORS origins are configurable through `CORS_ORIGINS` and default to the
  local frontend origin.
- GitHub Actions use `contents: read` unless the explicit GHCR publishing job
  requires `packages: write`.
- Gitleaks, pip-audit, npm audit, and Dependabot are configured in CI.
- The portfolio Compose stack does not publish a PostgreSQL host port and
  binds frontend/API host ports to loopback by default.
- Database, Parquet, and local cloud-adapter state use persistent named
  volumes.

## Material limitations

- FastAPI does not currently enforce end-user authentication, authorization,
  or role-based access on dashboard/API operations. Some endpoints expose
  operational data and mutate quality/pipeline/cloud state.
- `/pipeline/execute` has an internal token, but a production reverse proxy
  must deny access from external clients. Restrict `/docs`, `/redoc`, and
  `/openapi.json`.
- The backend development Docker image runs with its base-image default
  privileges; container least privilege needs additional deployment work.
- Airflow local configuration uses `webserver --debug` due to the documented
  Gunicorn startup issue. It is not a production webserver configuration.
- HTTPS termination, secret management/rotation, database TLS, backup
  automation, monitoring, rate limits, and incident-response procedures are
  not implemented here.
- Azure and Fabric execution are not verified; Power BI Desktop, PBIX/PBIP,
  M/DAX, DirectQuery, and report rendering are not verified.

## Dependency vulnerability status

The initial local `npm audit --audit-level=high` found a high-severity
transitive PostCSS advisory through Next.js. The root npm override pins
PostCSS 8.5.23; after `npm ci`, `npm audit` reported zero vulnerabilities.
The frontend build and type check must validate this dependency override.

Python dependency auditing has not run locally. The `security.yml` workflow
runs `pip-audit` for backend and dbt requirements; review that result after a
GitHub Actions run. Zero npm findings does not imply Python dependencies have
no vulnerabilities.

## Operator checklist

1. Generate unique high-entropy database and internal pipeline secrets and
   keep them in a protected secrets manager.
2. Set exact HTTPS CORS origins and configure TLS and an identity-aware
   reverse proxy.
3. Deny external access to internal pipeline execution and administrative
   documentation routes.
4. Run dependency and secret scans; resolve findings with dependency owners.
5. Restrict database permissions and network paths; do not publish port 5432.
6. Encrypt, retain, and restore-test backups.
7. Review API access logs and define an incident-response process before
   exposing the demonstration publicly.

This repository is a portfolio/demo application, not a security-certified or
production-hardened service.
