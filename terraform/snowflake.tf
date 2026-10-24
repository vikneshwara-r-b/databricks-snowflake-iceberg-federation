# Terraform owns the full lifecycle of the service user the sync notebook connects as:
# generates its RSA key pair, creates the Snowflake user with the public half, and
# writes the private half into Secrets Manager + the Databricks secret scope
# (secrets.tf / databricks_secrets.tf). Nothing here is manual anymore — the admin
# identity authenticating this provider (providers.tf, a human ACCOUNTADMIN user's own
# key-pair auth) is privileged enough to run CREATE USER directly.
#
# Trade-off worth knowing: the generated private key lands in Terraform state (every
# resource attribute is stored in plaintext there, `sensitive` only hides it from CLI
# output) — same category of exposure already accepted for the admin credentials this
# project writes into Secrets Manager via tfvars. Treat terraform.tfstate with the same
# care as a secrets file: encrypt the backend, restrict who can read it.

resource "tls_private_key" "service_user" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

# Snowflake wants the raw base64 body only, no PEM header/footer or newlines.
locals {
  service_user_rsa_public_key = replace(
    replace(tls_private_key.service_user.public_key_pem, "/-----(BEGIN|END) PUBLIC KEY-----/", ""),
    "/\\s+/", ""
  )
}

# snowflake_service_user (as opposed to the general-purpose snowflake_user resource)
# is always TYPE = SERVICE — no password auth is possible for it at all, key pair is
# the only way in — so there's no separate argument to set for that.
resource "snowflake_service_user" "service_user" {
  name              = var.snowflake_service_user_name
  default_warehouse = snowflake_warehouse.service_user.name
  rsa_public_key    = local.service_user_rsa_public_key
  comment           = "Key-pair auth service user for the Databricks UniForm/Iceberg sync notebook (sync_to_snowflake.ipynb) — created and rotated by Terraform"
}

# NOTE (flagged in the plan — verify before relying on it): exact privileges required for
# Iceberg REST catalog integrations are a Public Preview feature; confirm against current
# Snowflake docs. CREATE INTEGRATION is account-level; the rest are schema/warehouse-level.

resource "snowflake_account_role" "uc_iceberg_reader" {
  name    = var.snowflake_role_name
  comment = "Grants needed to sync Unity Catalog Iceberg tables via the sync_to_snowflake notebook"
}

resource "snowflake_grant_privileges_to_account_role" "create_integration" {
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["CREATE INTEGRATION"]
  on_account        = true
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.service_user.name
  }
}

# One set of grants per schema in snowflake_schema.sync (ANALYTICS/RAW/CURATED by
# default) — a single for_each, now that all of them live under one variable
# (var.snowflake_sync_schemas). Previously two sets of near-identical resources (one
# singular, one for_each) for the same reason snowflake_schema itself used to be
# split — see snowflake_data.tf's comment on snowflake_schema.sync. Unified via
# `terraform state mv`, not a destroy/recreate.
resource "snowflake_grant_privileges_to_account_role" "schema_privileges" {
  for_each          = snowflake_schema.sync
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["USAGE", "CREATE ICEBERG TABLE"]
  on_schema {
    schema_name = "\"${snowflake_database.uc_sync.name}\".\"${each.value.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.uc_sync.name
  }
}

# Read + write on the actual table data, current and future, so the service user can
# both query and modify rows in any table (Iceberg or otherwise) created in this
# schema — not just create the Iceberg table objects themselves (schema_privileges,
# above, only covers CREATE ICEBERG TABLE / USAGE).
#
# NOTE: tables synced from Unity Catalog via the Iceberg REST catalog integration are
# normally a read-mostly mirror of Databricks-owned data (writes there would need to
# go through Databricks, not Snowflake, to actually propagate back) — these grants
# give the role the privileges regardless, per what was asked, but INSERT/UPDATE/
# DELETE/TRUNCATE against a vended-credential Iceberg table may be rejected by
# Snowflake/Databricks even with the grant in place. Confirm against current docs if
# write-back is actually needed, as opposed to read-only sync.
resource "snowflake_grant_privileges_to_account_role" "table_data_rw" {
  for_each          = snowflake_schema.sync
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.uc_sync.name}\".\"${each.value.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_table_data_rw" {
  for_each          = snowflake_schema.sync
  account_role_name = snowflake_account_role.uc_iceberg_reader.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.uc_sync.name}\".\"${each.value.name}\""
    }
  }
}

resource "snowflake_grant_account_role" "to_service_user" {
  role_name = snowflake_account_role.uc_iceberg_reader.name
  user_name = snowflake_service_user.service_user.name
}

# UC_ICEBERG_READER was only ever granted to the service user above, so no human —
# not even ACCOUNTADMIN — could SELECT from the synced Iceberg tables
# ("Insufficient privileges to operate on iceberg_table ..."). Snowflake's built-in
# roles don't auto-inherit custom roles; ACCOUNTADMIN only picks up a custom role's
# privileges if that role is linked into the SYSADMIN hierarchy (ACCOUNTADMIN has
# SYSADMIN granted to it by default). This is the standard Snowflake pattern for
# keeping admins able to see/query objects owned by automation roles.
resource "snowflake_grant_account_role" "to_sysadmin" {
  role_name        = snowflake_account_role.uc_iceberg_reader.name
  parent_role_name = "SYSADMIN"
}
