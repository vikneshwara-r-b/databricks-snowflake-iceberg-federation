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
  comment = "Holds the Iceberg tables mirroring Unity Catalog's raw and curated tables — created by Terraform"
}

# One schema per entry in var.snowflake_sync_schemas (RAW/CURATED by default). The gold
# zone is deliberately not listed: it is exposed through a catalog-linked database
# (created by a human SYSADMIN, see README.md), so ANALYTICS was removed from this list
# — applying that destroyed the old ANALYTICS schema and the three ICEBERG TABLE
# objects in it, Snowflake-side objects only, not any Databricks data.
#
# Everything under this database is Terraform-managed: if the database or a schema is
# dropped outside Terraform, the next `terraform plan` reports it as "has been deleted"
# (with "Marking the resource as removed" warnings from the dependent grants) and plans
# to recreate it — the warnings are expected, not errors; `terraform apply` fixes it.
resource "snowflake_schema" "sync" {
  provider = snowflake.sysadmin
  for_each = toset(var.snowflake_sync_schemas)

  database = snowflake_database.uc_sync.name
  name     = each.value
  comment  = "Schema the sync_to_snowflake notebook creates/refreshes Iceberg tables in"
}
