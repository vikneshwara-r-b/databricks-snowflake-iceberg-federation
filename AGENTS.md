# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (DABs) for deployment. Add project-specific instructions below.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill.**

It sets you up to work with this project reliably: CLI authentication, profile
selection, data discovery, and the bundle deployment workflow. Without it,
results are often slower and less accurate.

If this skill is not available (Databricks AI Tools are not installed), you can install them for your coding agent in seconds:

```bash
databricks aitools install
```

If the CLI is not installed, see: https://docs.databricks.com/dev-tools/cli/install

---

## Project Instructions

### What this project is

An end-to-end F1 race data lakehouse on Databricks (Lakeflow Declarative Pipelines,
Unity Catalog, one catalog per medallion zone), **plus** a federation layer that lets
Snowflake read the tables (every table except the quarantine table) directly as
Iceberg, via UniForm + the Iceberg REST Catalog API + credential vending (no data
copy). Full narrative/architecture: [`README.md`](README.md).
Full infra prerequisites/auth model/known-unknowns: [`terraform/README.md`](terraform/README.md).

### Two separate tools, two separate states — know which one owns what

- **This DAB bundle** (`databricks.yml`, `resources/**/*.yml`, `src/`) — the Lakeflow
  pipelines, the orchestrating job, and all transformation/notebook code. Deployed
  with `databricks bundle deploy -t <dev|prod>`.
- **`terraform/`** — a *separate* Terraform project/state that provisions the
  AWS/Databricks/Snowflake plumbing the bundle's Snowflake-federation piece depends
  on (storage credential, the three zone catalogs, service principal, Snowflake
  service user/role/warehouse/database/schemas, secrets). Applied independently with
  `terraform apply` from inside `terraform/`.
- Most cross-tool values are **copied manually, not wired automatically** — check
  whether changing one side needs the same update on the other; don't assume a
  Terraform output or a bundle variable updates itself. One exception:
  `databricks.yml`'s `job_run_as_application_id` variable resolves the
  Terraform-created service principal's `application_id` itself, at deploy time, via
  a bundle variable `lookup` on its `display_name` — **never hardcode a literal
  application_id there**; if the lookup ever needs replacing (e.g. the SP is
  renamed), update the `lookup.service_principal` name, not a static value.
  Another hand-copied pair: `databricks.yml`'s `prod` catalog variables (`raw_catalog`,
  `curated_catalog`, `analytics_catalog`) must equal Terraform's `raw_catalog_name`,
  `curated_catalog_name` and `catalog_name` (the gold catalog).

### Medallion layers → Lakeflow pipelines

| Zone | Catalog (`prod`) / schema | Pipeline | Source |
|---|---|---|---|
| raw | `f1_raw`.`raw` | `f1_load_to_prepared` | `src/transformations/prepared/*.py` |
| curated | `f1_curated`.`curated` | `f1_curated_transformation` | `src/transformations/curated/*.py` |
| analytics (gold) | `f1_analytics`.`analytics` | `f1_analytics_transformation` | `src/transformations/modelled/*.sql` |

**Each zone has its own Unity Catalog catalog** (bundle variables `raw_catalog`/
`curated_catalog`/`analytics_catalog`; Terraform `raw_catalog_name`/
`curated_catalog_name`/`catalog_name`). `catalog_name` stays the *gold* catalog
(`f1_analytics`) — it was the project's only catalog originally, and keeping it there
means the `uc-${var.catalog_name}-*` storage credential/location/IAM role names never
changed. Cross-zone reads use the *source* zone's catalog: the curated pipeline gets
`raw_catalog_name` and the analytics pipeline gets `curated_catalog_name` as pipeline
configuration. `dev` points all three variables at the built-in `workspace` catalog
(dev can't create catalogs), so zone separation there is by schema only. All three
catalogs share one S3 bucket, storage credential and external location, each under its
own `s3://<bucket>/<catalog>` prefix.

The gold layer is deliberately **materialized views** (`CREATE OR REFRESH
MATERIALIZED VIEW` in SQL), not plain tables — that's an explicit project
requirement, not an oversight. Every table in every layer sets UniForm properties
plus `pipelines.externalMetadata.enabled = 'true'` (and `delta.enableRowTracking`)
so Unity Catalog serves external Iceberg metadata for the table's *queryable* name
— without that property, a materialized view's Iceberg metadata attaches only to its
hidden internal backing table, which is what broke Snowflake access before this was
in place. Don't "fix" an Iceberg-exposure issue by converting an MV to a plain table;
fix the table properties instead.

### Gold is queried through a Snowflake catalog-linked database

The analytics/gold catalog is **not** synced table by table. A Snowflake
**catalog-linked database** (`F1_ANALYTICS_LINKED`) is linked to it via the
`uc_f1_analytics_iceberg` catalog integration and discovers/refreshes every table in the
`analytics` schema on its own (`SYNC_INTERVAL_SECONDS`, 60). It is created **once, by a
human running SQL as `SYSADMIN`** (SQL in the root `README.md`'s deploy steps) — not by the
notebook and not by Terraform. The notebook
`src/utility/create_snowflake_catalog_integration.ipynb` (one integration per zone catalog —
a REST integration's `CATALOG_NAME` is a single catalog) runs as `UC_ICEBERG_READER`, which
deliberately holds **no** account-level `CREATE DATABASE`; Terraform can't grant it anyway
(`SECURITYADMIN` lacks it `WITH GRANT OPTION`), and `SYSADMIN` already has it. Don't "fix" a
missing database by adding a `CREATE DATABASE` grant to `UC_ICEBERG_READER` or
`SECURITYADMIN` — that's the exact failure (`003102 (42501) Insufficient privileges`) this
design replaced. The notebook's last cell grants `SYSADMIN` `USAGE` on the analytics
integration, which the `CREATE DATABASE ... LINKED_CATALOG` needs.
- **Keep `ALLOWED_WRITE_OPERATIONS = NONE`.** The default is `ALL`, and with it a
  `DROP ICEBERG TABLE` in Snowflake also deletes the table and its data in Databricks.
- `ALLOWED_NAMESPACES = ('analytics')` keeps the catalog's `information_schema` (not
  Iceberg tables; errors during sync) out of discovery. No external volume — the
  integration uses vended credentials.
- There is deliberately no `analytics_snowflake_share` task and no `analytics` key in
  `snowflake_sync_config.json`. Don't re-add one to "fix" a missing table; check the
  linked database's discovery instead.
- The database is owned by `SYSADMIN`; the sync service role can't see it, so other consumers
  need grants (e.g. database roles).
- Unverified until a live run — verify before assuming any of these:
  - That `SYSADMIN` can create a linked database over an integration owned by
    `UC_ICEBERG_READER`. The explicit `USAGE` grant is there to make it work; if it still
    fails, create the database as `ACCOUNTADMIN`, or fall back to a one-time
    `ACCOUNTADMIN` grant of `CREATE DATABASE` to `SECURITYADMIN` `WITH GRANT OPTION`.
  - That the linked database discovers Lakeflow materialized views.
  - What it does after a full refresh: per Snowflake's docs a UUID mismatch drops the
    table from the linked database (the remote is untouched); whether the next poll
    re-adds it is untested.

### Snowflake sync of raw and curated is config-driven, per-layer, not hardcoded or end-of-job

`src/utility/sync_to_snowflake.ipynb` runs as **two separate job
tasks** — `{raw,curated}_snowflake_share` — each right after
that layer's own pipeline finishes (in parallel with the *next* layer's transform,
not blocking it), not once at the end of the job. Each invocation passes a different
`layer` widget value and that layer's own `catalog_name` (so it uses that catalog's
`uc_<catalog>_iceberg` integration) and creates/refreshes a Snowflake `ICEBERG TABLE`
for every entry under that key in `src/utility/snowflake_sync_config.json` (keys under
`layers`: `raw`/`curated`) — the single source of truth for which Unity Catalog
tables are synced and where in Snowflake they land. `race_raw_quarantined` has no
entry under `raw` on purpose — quarantined/rejected records aren't meant to be
externally readable. To add/remove a synced table or layer, edit that JSON file (and
the matching job task if adding a whole new layer); don't add a new job parameter or
widget for an individual table.

The job passes each sync task a `pipeline_id` widget directly from
`resources.pipelines.*.id` (no runtime job/task introspection to rediscover it). The
notebook uses that to make **one `event_log(pipeline_id)` call per layer**
(`GROUP BY flow_name`) — not one `event_log()` call per table — covering every
table in that layer's pipeline in a single query, native to streaming tables and
materialized views, not a custom state table. An already-existing table only gets
refreshed if that layer's latest pipeline update actually changed it. **The two
object types need different signals, verified against the real event log, not
assumed:**
- **Streaming tables**: sum of `num_output_rows`/`num_upserted_rows`/
  `num_deleted_rows` across the latest update's `flow_progress` events — these are
  incremental, so a positive sum means real new data.
- **Materialized views**: `num_output_rows` is the *full* recomputed row count on
  every refresh, not incremental — always positive, so it's useless as a change
  signal for MVs (an earlier version of this notebook used it for both object types,
  which made MV refreshes look "changed" every single run regardless of reality).
  Use the `planning_information` event's chosen maintenance technique instead
  (`technique_information[].is_chosen`): `MAINTENANCE_TYPE_NO_OP` means nothing
  changed; anything else means it did.

`event_log()` is owner-only; this works because the sync notebook connects as the
same service principal that owns these pipeline-created tables (`run_as`). A final
status other than `COMPLETED` (or no flow events at all, including when
`pipeline_id` is empty) is treated as "assume changed" — refresh rather than
silently skip on an ambiguous signal. A newly-created table is always refreshed
once regardless. If this logic is ever revisited, don't reintroduce a single
row-count-based check for both object types — that's the exact bug this version
fixes.

The notebook **drops and recreates a Snowflake table** on one specific condition, so
don't mistake it for stray cleanup code. (It decides whether to *create* a table from the
table's name alone, so a stale table pinned to a removed catalog namespace — for example
left over from the old single-catalog layout — is neither recreated nor refreshed
successfully; drop such tables by hand once. This is read from the code, not an observed
failure.) A full refresh of a Lakeflow table rebuilds
its underlying storage and changes the table's Iceberg UUID; Snowflake's existing
`ICEBERG TABLE` object is still pinned to the old UUID, and `ALTER ICEBERG TABLE ...
REFRESH` then fails with `The UUID of Iceberg table '<name>' does not match the table
UUID in metadata file '<path>'`. The refresh call is wrapped to catch exactly that
message, `DROP ICEBERG TABLE` and recreate it through the same
`create_iceberg_table()` helper used for first-time creation; every other
`ProgrammingError` is re-raised unchanged. Confirmed live, back when there were three
sync tasks (before gold moved to the catalog-linked database): all three
`*_snowflake_share` tasks failed this way after a full-refresh run and all three
succeeded on the repaired run with this handling in place. The gold zone no longer goes
through this code path. Keep the match narrow — a
broad `except` here would silently drop and recreate Snowflake tables on unrelated
errors, which also discards any grants issued directly on those tables.

### Full refresh is a deploy-time bundle variable, not a job parameter

`full_refresh_all` (`databricks.yml`) feeds `pipeline_task.full_refresh` on all three
pipeline tasks via `${var.full_refresh_all}`. Default `"false"`. To force one:
`databricks bundle deploy -t prod --var="full_refresh_all=true"`, run the job, then
redeploy to return to incremental. Two deploys per use.

**Don't "improve" this into a job parameter.** Both runtime routes were tried against
the live API and both fail:
- `full_refresh: "{{job.parameters.full_refresh_all}}"` deploys without error but the
  field comes back **empty** from `databricks jobs get` — it's a boolean field, and the
  Jobs service does not substitute `{{...}}` into non-string fields. A silent failure,
  not a loud one.
- The legacy per-task `pipeline_params` override on `run-now`/`repair-run` is rejected
  outright on this job: `Cannot use legacy parameters (notebook_params, jar_params,
  python_params, spark_submit_params, sql_params) because the job has job parameters
  configured.` This job declares job-level `parameters:`, so that route is closed.

`${var.x}` works because the DAB CLI substitutes it into the YAML *before* type
checking, so the field receives a real boolean — verified by inspecting the resolved
value's Python type via `databricks bundle validate --output json`. Also note that a
full refresh breaks the raw/curated Snowflake tables until the sync notebook's
UUID-mismatch recovery runs; see the sync section above. For the gold zone's
catalog-linked database the outcome is untested (see that section).

### Liquid Clustering: declared everywhere, whether it actually runs is unverified

Every Lakeflow-managed table (`@dp.table`/`dp.create_streaming_table` in
`src/transformations/prepared/` and `curated/`, `CREATE OR REFRESH MATERIALIZED
VIEW ... CLUSTER BY AUTO` in `src/transformations/modelled/*.sql`) declares
`cluster_by_auto=True`/`CLUSTER BY AUTO` — this exists because Snowflake reads
these tables as externally-managed Iceberg tables and can't run `OPTIMIZE`/
clustering on them itself; all file-layout maintenance has to happen on the
Databricks side.

**Predictive Optimization (PO) is off at every level above the tables, but on for
the tables themselves.** Checked live against the workspace with `databricks
catalogs|schemas|tables get`, not assumed:
- Metastore `metastore_aws_us_east_1`, catalog `f1_analytics` and the `raw`/
  `curated`/`analytics` schemas: effective value **DISABLE** (catalog and schemas
  inherit it from the metastore). Enabling PO at the catalog was explicitly rejected
  earlier — not this project's call to revisit without asking again.
  (That was checked when everything lived in `f1_analytics`; `f1_raw` and `f1_curated`
  came later and should inherit **DISABLE** from the same metastore, but haven't been
  checked yet — confirm with `databricks catalogs get` before relying on it.)
- All 14 pipeline-created tables (11 streaming tables, 3 materialized views):
  `enable_predictive_optimization = ENABLE`, set **on the table itself**, not
  inherited. Nothing in this repo's code or Terraform sets it, so it appears to be
  applied by Lakeflow when it creates the table. The Databricks Predictive
  Optimization docs list streaming tables and materialized views as *not*
  maintained, while a Databricks employee on the community forum says PO maintenance
  runs on pipeline-managed tables regardless of catalog/schema settings and can't be
  disabled for them — the two sources conflict, so don't rely on either.

So **don't assume clustering is happening, and don't assume it isn't.** To find out,
check a table's history after a pipeline run (`SELECT operation, count(*) FROM
(DESCRIBE HISTORY f1_analytics.<schema>.<table>) GROUP BY operation` — look for
`OPTIMIZE`), which needs `SELECT` on the table — the pipelines' service principal owns
these tables, so a human gets that only through the catalog-level `ALL_PRIVILEGES` grant
Terraform gives the identity it authenticates as. Until that's been looked at, treat
`cluster_by_auto` as declared intent.

Separately: any PO/`OPTIMIZE`/clustering pass runs *outside* the Lakeflow pipeline's
own update/flow framework, so it won't show up in `event_log()`'s `flow_progress`
events — `sync_to_snowflake.ipynb`'s change-detection logic (above) won't trigger an
extra Snowflake refresh just because a background compaction pass happened between
pipeline runs. Not a correctness issue (the next real data-driven pipeline update
refreshes Snowflake as usual), just a reason Snowflake might lag behind an improved
file layout by one pipeline run.

`src/utility/create_snowflake_catalog_integration.ipynb` is a **separate, one-time,
manually-run** notebook (not a job task) that bootstraps the Snowflake catalog
integrations (one per zone catalog) — it's deliberately not re-run automatically. Because
it isn't a job task it doesn't get the sync job's `snowflake_sync_env` environment, so on
serverless compute `snowflake-connector-python` must be installed by hand (the notebook's
Environment panel, or `%pip install` in a cell at the top) or the first code cell fails
with `ModuleNotFoundError`.

### Identity notes

- `svc-f1-analytics-automation` (Databricks service principal, `terraform/
  databricks_access.tf`) is **reused** for two purposes: the identity Snowflake's
  catalog integration authenticates as (OAuth M2M), and the `run_as` identity for
  the `f1_ingest_and_transform_workflow` job (`databricks.yml`, `prod` target).
  It's one identity on purpose (a deliberate simplification for this project's
  scale) — don't assume a second, dedicated job-runner SP exists or should be
  created; check with whoever owns this decision before splitting it back out.
- `terraform apply` succeeding on this SP's `display_name` does **not** mean the
  change is visible yet — verified against the provider's own source
  (`scim/resource_service_principal.go`): Update is one `PUT`, no read-back, no
  retry/consistency-wait logic at all. Combined with this workspace being
  identity-federated (where account-level sync timing for an *update*, as opposed to
  creation, isn't documented), a success message only confirms the workspace-level
  `PUT` was accepted — not that the account-level record anything else reads back
  reflects it yet. Don't trust it alone if the change needs to be visible
  immediately; confirm with `databricks service-principals get`. Separately: **never
  set `api = "account"`** on `databricks_service_principal` in
  this project — it fails with `Not Found` for this workspace's SP, and Terraform
  persists that `api` value into state before the call errors, which makes every
  later refresh query the wrong (account) API and wrongly conclude the SP was
  deleted — a real incident during this project's work, fixed via `terraform state
  rm` + `terraform import` (both state-only, no real infrastructure touched). See
  `terraform/README.md`'s `databricks_access.tf` section for the full writeup.
- Deploying `prod` (`databricks bundle deploy -t prod`) requires the deploying
  user/SP to hold the **"Service principal: User"** role on
  `svc-f1-analytics-automation` — a separate grant from workspace admin or from
  having created the SP ("Service principal: Manager" doesn't include "User"). `403
  PERMISSION_DENIED: Cannot bind the service principal provided in 'run_as' field
  ...` means this is missing. `terraform apply` now grants it automatically
  (`databricks_access_control_rule_set.snowflake_reader_user`,
  `terraform/databricks_access.tf`) to whichever identity `var.databricks_profile`
  authenticates as — confirmed working live (via `terraform import` first, since
  this resource type is authoritative and would otherwise silently revoke the
  SP-creator's existing Manager role). A raw CLI probe of the same API returned `Not
  Found`, but Terraform's provider reaches it fine — don't assume CLI and Terraform
  have the same reach here. See `terraform/README.md` Prerequisites item 4 for the
  manual UI fallback.
- `run_as` changes who *executes* the job/pipelines but **not** who can read the
  workspace files (notebooks) `databricks bundle deploy` writes — those stay owned
  by whoever ran that command. Missing this surfaces as `Unable to access the
  notebook "..." ... the identity used to run this job, svc-f1-analytics-automation,
  lacks the required permissions` at job run time, not at deploy time. Fixed
  in `databricks.yml`'s `prod` target via a top-level `permissions:` block granting
  the SP `CAN_MANAGE` (cascades to resources *and* deployed workspace files/
  directories, per Databricks' bundle-permissions precedence docs), plus the deploying
  user explicitly via `${workspace.current_user.userName}` (resolved at deploy time, not
  a hardcoded name or a group; a `databricks bundle validate` recommendation — otherwise
  `CAN_MANAGE` only holds while the same identity keeps deploying). If a
  similar "lacks the required permissions" error shows up for a *different* notebook
  path later, check this block covers it before assuming `run_as` itself is broken.
- **Workspace ACLs are not data privileges.** The bundle's `permissions` block (above)
  controls who can manage the job, pipelines and notebooks; it gives **no** access to the
  tables. The pipelines run as the service principal (`run_as`), so it owns every table
  they create, and the human who owns the catalogs and deploys the bundle has no privilege
  on them. Terraform therefore grants whichever identity it authenticates as
  `ALL_PRIVILEGES` on all three catalogs (`databricks_grants.catalog`, a second `grant`
  block next to the service principal's; inherited by current and future objects). That
  resource is **authoritative** — never fix access with a manual `GRANT` or UI change on
  these catalogs, it is reverted by the next `terraform apply`. The principal is looked up
  at plan time (`data.databricks_current_user.me`), so no name is hardcoded, but the
  grant follows whoever runs Terraform. Write privilege names in the underscore form the
  provider stores (`USE_CATALOG`); the spaced form works but makes Terraform plan a
  revoke-and-re-add of the whole grant whenever a sibling block changes.
- **Snowflake privileges are deliberately narrow, with one accepted trade-off.**
  `UC_ICEBERG_READER` (the sync service role) has account-level `CREATE INTEGRATION`, owns
  the catalog integrations and the raw/curated `ICEBERG TABLE`s, and has **no**
  `CREATE DATABASE`. `SECURITYADMIN` holds `CREATE INTEGRATION WITH GRANT OPTION` (one-time
  manual grant) — and does not hold `CREATE DATABASE`, which is why a Terraform grant of it
  failed. The trade-off: the runtime role owning the integrations is more than it needs
  (only `USAGE` is required to reference one); the tighter design — admin-owned
  integrations created once by a human `ACCOUNTADMIN`, no `create_integration` grant, no
  grant-option prerequisite, optionally a dedicated custom Terraform role — is a deferred
  follow-up, documented in `terraform/README.md` ("Least privilege in Snowflake"). Don't
  widen these grants to work around an error without re-reading that section.
- **Terraform-managed Snowflake objects can be dropped by hand.** If `UC_ICEBERG_SYNC` (or
  a schema in it) disappears, `terraform plan` shows "has been deleted" plus `Marking the
  resource as removed` warnings from the schemas and grants beneath it; those are expected,
  and `terraform apply` recreates them. The linked database and the catalog integrations
  are not Terraform-managed, so they're not recreated that way.
- The Unity Catalog metastore's `external_access_enabled` setting (required for any
  credential vending to work at all) is a **shared, account-level resource** this
  project's Terraform deliberately does not manage — it's a one-time manual CLI step
  (`databricks metastores update <id> --external-access-enabled=true`), documented
  in `terraform/README.md`. Don't propose importing the metastore into this
  project's Terraform state without re-reading that rationale first.

### Security practices to keep following

- Never dump the full contents of `terraform.tfvars`, `.tfstate`, or
  `.tfstate.backup` (they hold real private keys/account identifiers) — extract
  specific fields with targeted `grep`/`sed` instead.
- Before suggesting or making a commit, check staged diffs for private keys, AWS
  access keys, account IDs, or other real credentials.
- Don't hardcode a person's name or email in Terraform or the bundle: the deploying
  identity is resolved at plan/deploy time (`data.databricks_current_user.me`,
  `${workspace.current_user.userName}`).
- Never commit unless explicitly asked.
