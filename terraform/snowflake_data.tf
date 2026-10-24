# The warehouse/database/schema the service user reads and writes Iceberg table data
# in — created here under the SYSADMIN role (provider alias in providers.tf), matching
# Snowflake's standard convention that SYSADMIN owns compute/storage objects while
# SECURITYADMIN/ACCOUNTADMIN own users/roles/grants. Ownership is set at creation time
# by which role's session creates the object — no separate "owner" argument exists on
# these resource types, which is why the aliased provider (not a grant-ownership
# transfer after the fact) is the mechanism here.

resource "snowflake_warehouse" "service_user" {
  provider = snowflake.sysadmin

  name                = var.snowflake_warehouse
  warehouse_size      = var.snowflake_warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Warehouse for the Databricks UniForm/Iceberg sync service user — created by Terraform"
}

resource "snowflake_database" "uc_sync" {
  provider = snowflake.sysadmin

  name    = var.snowflake_database
  comment = "Holds the Iceberg tables mirroring Unity Catalog's raw, curated and analytics tables — created by Terraform"
}

# One schema per entry in var.snowflake_sync_schemas (ANALYTICS/RAW/CURATED by
# default) — a single for_each, now that ANALYTICS/RAW/CURATED all live under one
# variable. This used to be two separate resources (one singular, one for_each) only
# because ANALYTICS was already live under the singular address
# (snowflake_schema.uc_sync) when RAW/CURATED were added, and refolding it into a
# for_each at the time would have made Terraform plan to destroy and recreate the
# already-live schema just to rename its state address. Unified now via `terraform
# state mv` (moving the existing ANALYTICS/RAW/CURATED resources to their new
# for_each addresses before this config was applied) rather than a destroy/recreate.
resource "snowflake_schema" "sync" {
  provider = snowflake.sysadmin
  for_each = toset(var.snowflake_sync_schemas)

  database = snowflake_database.uc_sync.name
  name     = each.value
  comment  = "Schema the sync_to_snowflake notebook creates/refreshes Iceberg tables in"
}
