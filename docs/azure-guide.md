# Azure Data Lake Storage Gen2

Azure storage is optional. The application remains in local mode unless
`CLOUD_STORAGE_MODE=azure` is set. No account, storage account, file system, or
other paid resource is created automatically.

## Requirements and permissions

Use an Azure subscription and an existing StorageV2 account with hierarchical
namespace enabled. Create the file systems named by
`AZURE_STORAGE_BRONZE_CONTAINER`, `AZURE_STORAGE_SILVER_CONTAINER`, and
`AZURE_STORAGE_GOLD_CONTAINER` before testing. Assign the runtime Entra identity
`Storage Blob Data Contributor` on the storage account or specific file systems
for synchronization. `Storage Blob Data Reader` is sufficient for listing and
downloads; the backend's connection test also requires read access to each
configured file system.

For local development, sign in with `az login` and run the backend in an
environment where `DefaultAzureCredential` can use that identity. In a
container, the host Azure CLI token is not automatically shared with the
container. Prefer workload/managed identity in an Azure host; for local Docker
development, a short-lived service principal can be supplied through the
ignored `.env` file using `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and
`AZURE_CLIENT_SECRET`. These are environment credentials consumed by
`DefaultAzureCredential`; do not commit them. Never use storage account keys or
SAS tokens with this adapter.

## Configure

```powershell
Copy-Item .env.example .env
# Set the cloud values below in the ignored .env; keep credentials blank unless needed.
```

Set:

```dotenv
CLOUD_STORAGE_MODE=azure
AZURE_STORAGE_AUTH_MODE=entra
AZURE_STORAGE_ACCOUNT_URL=https://<account>.dfs.core.windows.net
AZURE_STORAGE_BRONZE_CONTAINER=dataflow-bronze
AZURE_STORAGE_SILVER_CONTAINER=dataflow-silver
AZURE_STORAGE_GOLD_CONTAINER=dataflow-gold
INSTALL_AZURE_SDK=true
```

For local Python development install the optional dependencies:

```powershell
python -m pip install -r backend/requirements-azure.txt
```

For Docker, set `INSTALL_AZURE_SDK=true` and build the backend image when you
choose to enable Azure mode. The default image and default runtime remain
Azure-independent. This guide does not ask you to restart or rebuild any
currently running service.

## Exercise real backend behavior

After you have independently configured the identity and file systems, call:

```powershell
curl.exe -X POST http://localhost:8000/cloud/storage/test
curl.exe http://localhost:8000/cloud/status
curl.exe -X POST http://localhost:8000/cloud/sync
curl.exe "http://localhost:8000/cloud/storage/files/bronze"
```

`/cloud/storage/test` requests file-system properties for all three configured
file systems. `/cloud/sync` uploads the ETL-produced
`bronze_sales.parquet`, `silver_sales.parquet`, and `gold_sales.parquet` files;
an unchanged content hash is skipped. Use the Cloud Integration page to see
real backend responses and persisted operation history. A local-mode result
explicitly states that it contacted no cloud service.

The storage adapter upload/retrieval operations are file-format agnostic, so
CSV, JSON, and Parquet files can be stored and retrieved by its callers. The
dashboard sync endpoint intentionally synchronizes only the three canonical
Parquet snapshots. See [Azure Data Factory](./azure-data-factory.md) for
deployable CSV, JSON, REST, and PostgreSQL ingestion templates.

## Official references

- [ADLS Gen2 connector and managed identity](https://learn.microsoft.com/azure/data-factory/connector-azure-data-lake-storage)
- [Azure Identity client library for Python](https://learn.microsoft.com/python/api/overview/azure/identity-readme)
- [ADLS Gen2 Python SDK](https://learn.microsoft.com/python/api/overview/azure/storage-file-datalake-readme)

## Current limitations

- Cloud connectivity and authorization have not been verified against a real
  subscription in this project session.
- The adapter requires existing file systems and never creates them.
- Local-to-cloud synchronization is initiated manually and covers the Parquet
  snapshots only; it is not scheduled.
- Fabric is not connected by this backend.
