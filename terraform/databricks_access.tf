# The identity Snowflake's catalog integration authenticates as (OAuth M2M), plus the
# Unity Catalog grants needed for credential vending to an external engine. NOTE (flagged
# in the plan, verify before relying on it): the metastore containing this catalog must
# also be explicitly enabled for "external data access" (Public Preview) — that toggle is
# likely account-admin scoped and is NOT something this workspace-profile-only project
# attempts to manage; treat it as a one-time manual prerequisite performed by whoever has
# account-admin rights.
#
# This same service principal is also reused (not a separate one) as the run_as
# identity for the f1_ingest_and_transform_workflow job — see databricks.yml's prod
# target — so its entitlements/grants below cover both purposes: what Snowflake's
# OAuth M2M client needs, and what the job itself needs to execute its notebook and
# pipeline tasks. Reusing one identity means its blast radius covers both trust
# boundaries (an external system's credential-vending identity and an internal job's
# execution identity are no longer separate) — a deliberate simplification for this
# project's scale, not a default to carry into a larger one without reconsidering it.

resource "databricks_service_principal" "snowflake_reader" {
  display_name = "svc-f1-analytics-automation"

  # Service principals get zero entitlements by default — without one of
  # databricks_sql_access/workspace_access/workspace_consume, every workspace API call
  # is rejected with a 403 "disabled for users without the ... entitlements" error,
  # regardless of any catalog/schema grants. workspace_access (broader than the
  # workspace_consume this SP started with) is needed now that it also runs job/
  # pipeline tasks, not just Unity Catalog's Iceberg REST endpoint's own internal API
  # calls — workspace_consume alone doesn't cover executing notebooks or triggering
  # pipeline updates.
  workspace_access = true
}

resource "databricks_service_principal_secret" "snowflake_reader" {
  service_principal_id = databricks_service_principal.snowflake_reader.id
}

# Granted catalog-wide rather than scoped to just the analytics/gold schema: Unity
# Catalog privileges granted at the catalog level cascade to every current and future
# schema/table inside it, so this single grant covers analytics, raw, and curated
# without having to enumerate schemas. It also sidesteps an ordering problem entirely
# — the catalog is created by this same Terraform config, unlike the schemas
# (DAB-owned), so there's no need to wait for `databricks bundle deploy` to run first
# or gate this behind a second apply.
#
# MODIFY/CREATE TABLE/READ VOLUME/WRITE VOLUME were added when this SP took on the
# job-runner role too: the pipelines it now runs as create/refresh tables across all
# three schemas, and the extract_api notebook task writes JSON dumps into the raw UC
# volume. CREATE MATERIALIZED VIEW is a separate privilege from CREATE TABLE — Unity
# Catalog's pipeline-permission model requires it specifically for the gold layer's
# `CREATE OR REFRESH MATERIALIZED VIEW` tables; CREATE TABLE alone covers raw/
# curated's streaming tables but left the SP with "PERMISSION_DENIED: User does not
# have CREATE MATERIALIZED VIEW on Schema 'f1_analytics.analytics'" the first time it
# ran the analytics pipeline as run_as.
#
# NOTE (flagged — verify before relying on it): confirm EXTERNAL USE SCHEMA is actually
# a valid catalog-level grant target for the databricks_grants resource/API, not just a
# schema-level one, against current Databricks docs.
#
# One grant set per zone catalog (raw/curated/analytics — local.zone_catalogs): the job
# writes to all three, and Snowflake's catalog integrations (one per catalog, since a
# REST integration's CATALOG_NAME is a single catalog) read all three. The gold
# catalog's grant is what the catalog-linked database authenticates against.
#
# The second grant gives whichever identity this Terraform authenticates as
# (var.databricks_profile — the same pattern as the rule set at the bottom of this file)
# full access to everything in all three catalogs. It's needed because the pipelines run
# as the service principal (databricks.yml's prod run_as), so every table they create is
# OWNED by it and the human who owns the catalog and deploys the bundle has no privilege
# on those tables at all. The bundle's `permissions` block does NOT cover this: that's a
# workspace ACL on the job/pipelines/notebook files, not a Unity Catalog data privilege.
# Granted at catalog level so it is inherited by every current and future schema, table,
# materialized view and volume the pipelines create. This resource is authoritative, so
# this has to live here — a manual GRANT would be reverted by the next apply.
#
# ALL_PRIVILEGES is deliberately broad (read AND write, incl. the raw layer); narrow it
# to e.g. USE CATALOG/USE SCHEMA/SELECT if write access isn't wanted.
resource "databricks_grants" "catalog" {
  for_each = local.zone_catalogs
  catalog  = each.value

  grant {
    principal = databricks_service_principal.snowflake_reader.application_id
    # Underscore form (what the provider stores), not "USE CATALOG" with a space: the
    # two are equivalent to Databricks, but a spaced name makes Terraform show this
    # whole grant as removed-and-re-added whenever any other grant block on this
    # resource changes.
    privileges = [
      "USE_CATALOG", "USE_SCHEMA", "SELECT", "EXTERNAL_USE_SCHEMA",
      "MODIFY", "CREATE_TABLE", "CREATE_MATERIALIZED_VIEW",
      "READ_VOLUME", "WRITE_VOLUME",
    ]
  }

  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

# The grant that already exists on the gold catalog moves to its for_each address
# instead of being destroyed and recreated.
moved {
  from = databricks_grants.catalog
  to   = databricks_grants.catalog["analytics"]
}

resource "databricks_grants" "external_location" {
  external_location = databricks_external_location.uc_external.name

  grant {
    principal  = databricks_service_principal.snowflake_reader.application_id
    privileges = ["EXTERNAL USE LOCATION"]
  }
}

# The sync_to_snowflake notebook task (run by this same SP, as the job's
# run_as identity) reads every Snowflake credential via dbutils.secrets.get() against
# this scope — without CAN_READ here, that task fails the moment it starts, regardless
# of the Unity Catalog grants above.
resource "databricks_secret_acl" "snowflake_reader_secrets" {
  scope      = databricks_secret_scope.snowflake_iceberg_sync.id
  principal  = databricks_service_principal.snowflake_reader.application_id
  permission = "READ"
}

output "job_runner_application_id" {
  description = "Service principal application_id for the f1_ingest_and_transform_workflow job's run_as — the same SP Snowflake's catalog integration authenticates as, reused rather than creating a second identity. Not needed for databricks.yml itself (that resolves it at deploy time via a bundle variable `lookup` on this SP's display_name, not this output) — kept as a convenience for manual inspection/debugging."
  value       = databricks_service_principal.snowflake_reader.application_id
}

# Whoever deploys the bundle (`databricks bundle deploy -t prod`) needs the
# "Service principal: User" role on this SP to set it as a job/pipeline's run_as —
# a separate grant from being a workspace admin or from having created the SP
# ("Service principal: Manager" does not include "User"). Granted here to whichever
# identity this Terraform provider itself authenticates as (var.databricks_profile)
# — the same person who typically also deploys the bundle in a project this size.
#
# This resource (`databricks_access_control_rule_set`) is AUTHORITATIVE: applying it
# REPLACES every grant_rule already on this rule set with exactly what's listed here
# — it doesn't add to what's there. Confirmed live via `terraform import` before
# writing this config: the rule set already had `roles/servicePrincipal.manager` for
# this same user (auto-granted to whoever creates an SP) — manager is listed below
# alongside user for that reason; dropping it here would have silently revoked it on
# apply. If this is ever extended to grant a different/additional principal, check
# the current live grants first (`terraform state show
# databricks_access_control_rule_set.snowflake_reader_user`, or `terraform import`
# before the first apply) rather than assuming grant_rules starts empty.
#
# Also confirmed live: a raw CLI probe (`databricks account access-control
# get-rule-set`/`get-assignable-roles-for-resource`) against this exact resource name
# returned `Not Found` from this workspace-scoped profile — but the Terraform
# provider (same profile) reached it successfully via its workspace-level routing.
# So this is a case where the Terraform provider can do something the bare CLI
# can't; don't conclude "Not Found" from the CLI means Terraform can't either.
data "databricks_current_user" "me" {}

resource "databricks_access_control_rule_set" "snowflake_reader_user" {
  name = "accounts/${var.databricks_account_id}/servicePrincipals/${databricks_service_principal.snowflake_reader.application_id}/ruleSets/default"

  grant_rules {
    principals = [data.databricks_current_user.me.acl_principal_id]
    role       = "roles/servicePrincipal.manager"
  }

  grant_rules {
    principals = [data.databricks_current_user.me.acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }
}
