# Bridges every credential the sync_to_snowflake notebook needs into a
# Databricks secret scope, read via dbutils.secrets.get(). Nothing here is ever typed
# into notebook code or passed as a plain job parameter. Sourced directly from the same
# variables/resources secrets.tf writes into Secrets Manager — not read back from
# there, since these resources already have the values and a round trip would just add
# a dependency on the secretsmanager write finishing first for no benefit.

resource "databricks_secret_scope" "snowflake_iceberg_sync" {
  name = "snowflake-iceberg-sync"
}

resource "databricks_secret" "snowflake_account" {
  key          = "snowflake-account"
  string_value = var.snowflake_account_identifier
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_service_user" {
  key          = "snowflake-service-user"
  string_value = snowflake_service_user.service_user.name
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_service_user_private_key" {
  key          = "snowflake-service-user-private-key"
  string_value = tls_private_key.service_user.private_key_pem
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_role" {
  key          = "snowflake-role"
  string_value = snowflake_account_role.uc_iceberg_reader.name
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_warehouse" {
  key          = "snowflake-warehouse"
  string_value = var.snowflake_warehouse
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_database" {
  key          = "snowflake-database"
  string_value = var.snowflake_database
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "snowflake_schema" {
  key = "snowflake-schema"
  # The sync notebook's initial Snowflake connection schema only — purely cosmetic
  # session context, since every statement it runs fully qualifies
  # database.schema.table itself. The first entry in snowflake_sync_schemas (default:
  # ANALYTICS) is as good a default as any.
  string_value = var.snowflake_sync_schemas[0]
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "databricks_sp_client_id" {
  key          = "databricks-sp-client-id"
  string_value = databricks_service_principal.snowflake_reader.application_id
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}

resource "databricks_secret" "databricks_sp_client_secret" {
  key          = "databricks-sp-client-secret"
  string_value = databricks_service_principal_secret.snowflake_reader.secret
  scope        = databricks_secret_scope.snowflake_iceberg_sync.id
}
