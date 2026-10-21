output "catalog_name" {
  value = databricks_catalog.f1_analytics.name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.uc_storage.id
}

output "storage_credential_name" {
  value = databricks_storage_credential.uc_external.name
}

output "external_location_name" {
  value = databricks_external_location.uc_external.name
}

output "service_principal_application_id" {
  value = databricks_service_principal.snowflake_reader.application_id
}

output "secret_scope_name" {
  value = databricks_secret_scope.snowflake_iceberg_sync.name
}

output "snowflake_role_name" {
  value = snowflake_account_role.uc_iceberg_reader.name
}

output "snowflake_service_user_name" {
  value = snowflake_service_user.service_user.name
}

output "snowflake_secret_arn" {
  value = aws_secretsmanager_secret.snowflake.arn
}
