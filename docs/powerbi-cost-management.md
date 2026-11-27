# Power BI cost and cleanup notes

The deliverables in this repository require no paid cloud resource. Power BI
Desktop and PostgreSQL on the local machine can be used for manual assembly
and local refresh, subject to Microsoft's current licensing terms.

Power BI Service publishing, shared capacity, Premium/Fabric capacity,
on-premises data gateway hosting, and scheduled refresh can introduce
licensing or infrastructure costs. This phase does not create, configure, or
publish any of them. Check current Microsoft pricing and organizational
licensing before enabling a service.

To clean up, remove a local `.pbix` or PBIP folder that you created outside
this repository, clear saved Desktop credentials through Power BI's data
source settings, and remove any gateway or cloud resources only through the
owning account's supported controls. No cloud resources were provisioned by
this work.
