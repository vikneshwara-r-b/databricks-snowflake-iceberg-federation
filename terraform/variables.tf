# --- Profiles ---

variable "aws_profile" {
  description = "Named AWS CLI profile (~/.aws/credentials or ~/.aws/config) Terraform applies as"
  type        = string
}

variable "databricks_profile" {
  description = "Named Databricks CLI profile (~/.databrickscfg, e.g. set up via `databricks auth login --profile <name>`) Terraform applies as"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account id (not workspace id) — `databricks auth describe --profile <profile>` prints it. Needed only for databricks_access_control_rule_set's resource name (databricks_access.tf), which encodes it even when routed through the workspace API."
  type        = string
}

variable "aws_region" {
  description = "AWS region the storage bucket, IAM role, and secrets are created in"
  type        = string
}

variable "s3_bucket_name" {
  description = "Prefix for the S3 bucket backing the Unity Catalog catalog's storage. The actual bucket name appends a -YYYYMMDD suffix captured once at creation (aws_storage.tf) for global uniqueness — don't include a suffix here yourself."
  type        = string
}

# --- Databricks workspace (existing — this project never creates a workspace) ---

# One Unity Catalog catalog per medallion zone. catalog_name is the gold/analytics zone's
# catalog (kept under this name so every uc-${var.catalog_name}-* AWS/UC resource name
# stays put); raw and curated get their own, all sharing the same S3 bucket, storage
# credential and external location (databricks_catalog.tf).
variable "catalog_name" {
  description = "Name of the gold/analytics zone's Unity Catalog catalog. Must match the `analytics_catalog` variable used for the `prod` target in databricks.yml. Also the naming prefix for the storage credential, external location and IAM role."
  type        = string
  default     = "f1_analytics"
}

variable "raw_catalog_name" {
  description = "Name of the raw zone's Unity Catalog catalog to create. Must match the `raw_catalog` variable used for the `prod` target in databricks.yml"
  type        = string
  default     = "f1_raw"
}

variable "curated_catalog_name" {
  description = "Name of the curated zone's Unity Catalog catalog to create. Must match the `curated_catalog` variable used for the `prod` target in databricks.yml"
  type        = string
  default     = "f1_curated"
}

# Which Unity Catalog tables get synced to Snowflake, and where, now lives in
# src/utility/snowflake_sync_config.json — a single table list duplicated here would
# just drift from that file. This variable only covers the Terraform-managed side:
# which Snowflake *schemas* exist to receive them (snowflake_data.tf/snowflake.tf).

# --- Secrets Manager secret name Terraform creates (this project writes the values —
# see the sensitive variables below). One secret holds everything Snowflake-related:
# the admin identity's credentials and the generated service user's credentials,
# namespaced under separate top-level keys (secrets.tf). ---

variable "snowflake_secret_name" {
  description = "Secrets Manager secret name to create, holding every Snowflake credential this project uses"
  type        = string
  default     = "federation-poc/snowflake"
}

# --- Snowflake admin identity — Terraform's own provider auth. Supplied via tfvars
# (not committed); this project writes these same values into Secrets Manager
# (secrets.tf) for durability/rotation visibility, but authenticates its own snowflake
# provider directly off the variables, not by reading that secret back. ---

variable "snowflake_organization_name" {
  type      = string
  sensitive = true
}

variable "snowflake_account_name" {
  type      = string
  sensitive = true
}

variable "snowflake_admin_user" {
  type      = string
  sensitive = true
}

variable "snowflake_admin_role" {
  type    = string
  default = "SECURITYADMIN"
}

variable "snowflake_admin_private_key_pem" {
  description = "Private key (PEM) for the Snowflake admin identity this project's snowflake provider authenticates as"
  type        = string
  sensitive   = true
}

# --- Snowflake service user — fully Terraform-managed (see snowflake.tf): the key
# pair is generated and the user created here, not supplied via tfvars. Only its name
# and the account identifier (needed by the sync notebook's connector) are variables. ---

variable "snowflake_account_identifier" {
  description = "Snowflake account identifier used by the sync notebook's connector (e.g. ORGNAME-ACCOUNTNAME)"
  type        = string
}

variable "snowflake_service_user_name" {
  description = "Name for the Snowflake service user Terraform creates for the sync notebook to connect as"
  type        = string
  default     = "SVC_DATABRICKS_UC_READER"
}

variable "snowflake_role_name" {
  description = "Name of the Snowflake role Terraform creates/manages and grants to the service user"
  type        = string
  default     = "UC_ICEBERG_READER"
}

# --- Warehouse/database/schema — Terraform creates all three (snowflake_data.tf,
# under the SYSADMIN role), so these are names to assign, not pre-existing objects. ---

variable "snowflake_warehouse" {
  description = "Name for the warehouse Terraform creates for Iceberg sync operations"
  type        = string
}

variable "snowflake_warehouse_size" {
  description = "Warehouse size for var.snowflake_warehouse"
  type        = string
  default     = "XSMALL"
}

variable "snowflake_database" {
  description = "Name for the database Terraform creates to hold the Iceberg tables"
  type        = string
}

variable "snowflake_sync_schemas" {
  description = "Snowflake schemas (inside snowflake_database) Terraform creates to mirror this project's Unity Catalog schemas — one per Databricks schema any table in snowflake_sync_config.json targets. The gold/analytics zone is deliberately absent: it is exposed through the catalog-linked database (created by a human SYSADMIN, see README.md), not per-table sync. The first element is treated as the default/initial schema for the sync notebook's Snowflake connection (databricks_secrets.tf) — purely cosmetic session context, since every statement the notebook runs fully qualifies database.schema.table itself."
  type        = list(string)
  default     = ["RAW", "CURATED"]
}
