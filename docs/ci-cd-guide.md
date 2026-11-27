# CI/CD guide

GitHub Actions definitions live in `.github/workflows/`. They target pushes
and pull requests to `main` and `feature/**`, use repository read permissions
by default, and use ephemeral PostgreSQL service containers for tests.

## Workflows

| Workflow | Automatic checks | Manual / release behavior |
|---|---|---|
| `backend-ci.yml` | Python 3.12, compileall, warehouse migrations, unittest suite with isolated PostgreSQL, JUnit XML artifact | None |
| `frontend-ci.yml` | Node 22, `npm ci`, TypeScript, Next.js production build | None |
| `data-engineering-ci.yml` | 100-row synthetic fixture, ETL, data quality, warehouse upsert/idempotency tests, dbt parse/build against isolated PostgreSQL | No 50k benchmark |
| `docker-validation.yml` | Compose config, backend/frontend image builds, PostgreSQL/backend migration and API smoke | Uses disposable hosted-runner containers |
| `security.yml` | Gitleaks, pip-audit, npm audit | Also weekly scheduled/manual |
| `optional-integrations.yml` | Not run on every commit | Manual Airflow DAG import and small Spark equivalence tests; never the 50k benchmark |
| `release.yml` | Version-tag validation, tests, image build verification, release metadata artifact | GHCR publishing requires manual dispatch, a version-tag ref, and explicit `publish_images=true`; no deployment |

Dependabot checks Python dependencies for backend/dbt/Spark, npm, Dockerfiles,
and GitHub Actions weekly. Security audit workflows intentionally fail when
their configured threshold is exceeded; do not bypass or disable a finding
just to get a green check.

## Local preflight

```powershell
docker compose config --quiet
Set-Location frontend
npm ci
npm run typecheck
npm run build
Set-Location ..
docker compose exec backend python -m unittest discover -s tests -v
```

For CI-like isolated PostgreSQL tests, use a throwaway database/service and
set `DATABASE_URL`; never point these tests to development or production
data. The GitHub workflow creates a fresh Postgres service for each job.

## Verification boundaries

Workflow files have been authored locally, but are not proven until GitHub
executes them. External action versions, hosted runner images, current
vulnerability databases, registry permissions, and Docker downloads can affect
remote results. Inspect each run, test-report artifact, and security finding.
The release workflow verifies builds and metadata; it creates no GitHub
Release and does not publish images automatically.
