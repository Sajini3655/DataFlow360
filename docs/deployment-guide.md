# Deployment guide

DataFlow360 has a local development Compose stack and a separate lightweight
portfolio-demonstration Compose file at [`../compose.deploy.yml`](../compose.deploy.yml).
No cloud deployment or production rollout has been performed.

## Deployment modes

| Mode | Purpose | Security/operations |
|---|---|---|
| Local development | `docker-compose.yml`, source mounts, Airflow and optional dbt/Spark profiles | Development credentials only; Airflow uses documented `webserver --debug` mode |
| Portfolio demonstration | `compose.deploy.yml` with frontend, API, PostgreSQL, and persistent named volumes | Reverse proxy/TLS and identity-aware access control must be supplied separately |
| Production | Requires security, operations, scaling and recovery work beyond this repository | Not production-ready or deployed |

## Portfolio demonstration setup

Requirements: Docker Engine/Compose, a host with persistent disk, a DNS name
and TLS reverse proxy managed separately.

1. Copy the example environment and generate unique values using a password
   manager. Keep the file private and out of version control:

   ```powershell
   Copy-Item .env.deploy.example .env.deploy
   ```

2. Set `POSTGRES_PASSWORD` to a URL-safe value because the existing backend
   constructs `DATABASE_URL` from it. Set `PIPELINE_INTERNAL_TOKEN` to a
   cryptographically random secret. Set `PUBLIC_API_URL` to the HTTPS API
   origin reachable by the user's browser and `CORS_ORIGINS` to the HTTPS
   frontend origin.
3. Keep `FRONTEND_BIND_ADDRESS` and `API_BIND_ADDRESS` at `127.0.0.1` and
   terminate TLS at a reverse proxy on the same host. Configure proxy routes
   so the frontend serves the application and API requests reach FastAPI.
   The Next.js browser code uses `NEXT_PUBLIC_API_URL` at build time.
4. Configure the reverse proxy to require authentication for the dashboard
   and API paths, block `/pipeline/execute` from external clients, and block
   or restrict `/docs`, `/redoc`, and `/openapi.json`. The API does not
   implement end-user authentication or role authorization.
5. Start and inspect health:

   ```powershell
   docker compose --env-file .env.deploy -f compose.deploy.yml config --quiet
   docker compose --env-file .env.deploy -f compose.deploy.yml up --build -d
   docker compose --env-file .env.deploy -f compose.deploy.yml ps
   ```

   Migrations run from checked-in SQL when the backend starts. Verify the API
   health from the host/proxy and check backend logs for migration failures.

The database has no published host port and is on a private Compose network.
PostgreSQL data, Parquet snapshots, and local cloud-adapter data use named
volumes. Back up these volumes and validate restores before a demonstration.
Normal `down` keeps volumes; avoid `down -v`.

## Environment and optional integrations

Required: `POSTGRES_USER`, URL-safe `POSTGRES_PASSWORD`, `POSTGRES_DB`,
`PIPELINE_INTERNAL_TOKEN`, `PUBLIC_API_URL`, and `CORS_ORIGINS`.
`AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD` may remain blank; pipeline triggering
then responds HTTP 503 rather than pretending a run was queued. The
demonstration stack deliberately omits Airflow, dbt, Spark, Azure, Fabric, and
Power BI services.

The frontend URL is compiled into the Next.js bundle. Changing
`PUBLIC_API_URL` requires rebuilding the frontend image. CORS should list only
the exact HTTPS browser origins. Never use `*` with credentials or treat CORS
as authentication.

## HTTPS, identity, and exposure

The Compose file binds frontend and API host ports to loopback by default.
Do not expose these HTTP ports directly to the internet. Place a maintained
TLS reverse proxy and authentication gateway in front, use valid
certificates, configure request-size/time limits, and deny direct access to
administrative/internal routes. `/pipeline/execute` uses an internal token
but does not substitute for external authorization on other mutation routes.
Rotate the internal token and database credentials if exposure is suspected.

## Backups and recovery

- Schedule PostgreSQL logical or physical backups using a separately secured
  process; this repository does not configure backup automation.
- Back up Parquet and local cloud volumes where they are needed. ETL Parquet
  files are current full snapshots and may be regenerated from the warehouse.
- Store backups off-host and encrypted, define retention, and regularly test a
  restore into an isolated environment.
- Preserve database volumes when upgrading; test migrations against a
  restorable backup before applying them to valuable data.
- For a failed portfolio deployment, inspect backend/PostgreSQL logs, health
  status, and database connectivity. Restore from a known-good backup only
  after identifying the required recovery point.

The stack is a reproducible starting point, not a production deployment
solution. Container least privilege, end-user authentication/authorization,
TLS, backup automation, monitoring/alerting, migration rollback, and
production capacity planning remain deployment-owner responsibilities.
