# Troubleshooting

## Compose cannot find required environment variables

Copy `.env.example` to `.env` for development or `.env.deploy.example` to
`.env.deploy` for the lightweight demo. Replace placeholders with unique
local values. Run `docker compose config --quiet` (or specify the deployment
file and `--env-file`) before starting containers.

## PostgreSQL is unhealthy or migrations fail

Check `docker compose ps` and `docker compose logs --tail 100 db backend`.
Verify database environment values and available disk. The backend applies
warehouse SQL/migrations on startup. Preserve the named data volume and take
a backup before manually applying schema changes. Do not use `down -v` to
troubleshoot.

## Airflow pipeline trigger is unavailable

The local dashboard requires the Airflow API service and credentials in
`.env`. The lightweight deployment Compose file excludes Airflow; without
`AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`, the API explicitly returns HTTP 503
for pipeline requests. Do not interpret that as a successful queued run.

The documented local Airflow webserver uses `webserver --debug` after the
standard Gunicorn startup exceeded its timeout. The underlying Gunicorn issue
remains unresolved. Avoid recreating containers repeatedly; inspect container
health and logs first. This debug server is not production hardened.

## Missing optional analytics

dbt marts appear in BI dataset listings only after they have been built in the
configured `analytics` schema. Spark endpoints require the optional Spark
profile. Azure/Fabric labels do not imply that a real cloud account is
connected or a notebook has run.

## Frontend shows an API/network error

Confirm the API URL used by the browser is reachable from the user's machine,
not only from inside a container. In portfolio deployment,
`PUBLIC_API_URL` is baked into the Next.js image; changing it requires a
frontend rebuild. Configure the backend `CORS_ORIGINS` with the exact
frontend HTTPS origin. CORS is not authentication.

## Frontend image does not start

The production Dockerfile uses Next.js standalone output. Rebuild after
changing public build-time environment values. Check container health and
logs, then probe the local frontend URL. Do not delete named volumes to fix
frontend issues.

## Power BI assets

Power BI Desktop and PBIX/PBIP execution remain unverified. Confirm PostgreSQL
permissions and connector availability in Desktop. The CSV export endpoint
can be used as an alternative source, but it is a point-in-time file.
