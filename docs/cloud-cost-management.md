# Cloud cost management and cleanup

No Azure or Fabric resources were created for this implementation. The
following cost categories may apply when you choose to deploy the templates or
run the notebook; actual rates depend on region, SKU, capacity, data volume,
network path, and current Microsoft pricing.

## Potential cost drivers

| Service | Potential charge drivers | Cost control |
|---|---|---|
| ADLS Gen2 | Stored bytes, transactions, redundancy tier, reads/writes, retrieval, network egress | Set lifecycle rules; use small test files; monitor transactions and capacity |
| Azure Data Factory | Pipeline/activity orchestration, data movement, integration-runtime compute, managed virtual network, data flow (not used here) | Keep schedule stopped; use small bounded tests; stop dedicated/self-hosted compute when idle |
| Azure Key Vault | Secret operations and premium features | Limit secret polling and premium options |
| Azure Database for PostgreSQL | Compute, storage, backup, networking | Prefer an already-approved development database; stop/delete only with owner approval |
| Microsoft Fabric | Capacity runtime/units, trial conversion, storage and OneLake usage | Confirm workspace capacity and trial policy; pause capacity where permitted; avoid unattended schedules |
| Network | Private endpoints, gateways, outbound transfer and cross-region traffic | Keep data and compute co-located; use small test extracts |

Use the [Azure Pricing Calculator](https://azure.microsoft.com/pricing/calculator/)
and the Fabric capacity/licensing information available to your tenant before
deployment. This guide does not provide estimates because region, volume,
retention, and capacity were not supplied.

## Safe shutdown

1. Stop/disable the ADF trigger `TR_DataFlow360_HourlyPostgres` first.
2. Stop any running ADF pipeline or Fabric pipeline/notebook activity.
3. Stop or pause any dedicated integration runtime and Fabric capacity where
   your organization's policy permits.
4. Review storage lifecycle and retention settings; verify backups and data
   ownership before deleting any file system, Lakehouse, or database object.
5. Remove only resources created for the experiment, using the resource
   inventory and owner-approved Azure/Fabric procedures. Do not delete shared
   resource groups, existing PostgreSQL databases, or project database volumes.
6. Revoke temporary role assignments and delete expired development secrets
   after confirming no dependent service uses them.

Do not run broad `az group delete` commands as cleanup. The ADF deployment
script in this repository only updates resources inside an existing factory;
it does not provision infrastructure and is not an environment teardown tool.
