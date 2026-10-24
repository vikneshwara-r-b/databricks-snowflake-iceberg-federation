# Writes every Snowflake credential into a SINGLE Secrets Manager secret, as flat,
# top-level key-value pairs — not nested objects — so it renders correctly in Secrets
# Manager's console "Key/value" view. Keys are prefixed (admin_*/service_user_*) since
# both credential sets independently have a private_key field. This is a write, not a
# read: nothing in this file is read back to configure a provider in this same apply
# (that would be the actual circular case) — providers.tf's snowflake block uses the
# variables directly. This secret exists purely for durability/rotation visibility and
# so the Databricks secret scope (databricks_secrets.tf) has a consistent source of
# truth alongside the variables.

resource "aws_secretsmanager_secret" "snowflake" {
  name        = var.snowflake_secret_name
  description = "All Snowflake credentials for this project: the identity Terraform's own snowflake provider authenticates as, and the Terraform-generated service user the sync_to_snowflake notebook connects as"
  kms_key_id  = aws_kms_key.secrets.arn

  # Some AWS orgs enforce a Service Control Policy that explicitly denies
  # secretsmanager:UntagResource (even to admins) — if this account has one, any diff
  # that would remove/change a tag Terraform doesn't know about fails the whole apply.
  # This resource declares no tags, so there's nothing for Terraform to reconcile.
  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_secretsmanager_secret_version" "snowflake" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  secret_string = jsonencode({
    admin_organization_name         = var.snowflake_organization_name
    admin_account_name              = var.snowflake_account_name
    admin_user                      = var.snowflake_admin_user
    admin_role                      = var.snowflake_admin_role
    admin_private_key               = var.snowflake_admin_private_key_pem
    service_user_account_identifier = var.snowflake_account_identifier
    service_user_username           = snowflake_service_user.service_user.name
    service_user_private_key        = tls_private_key.service_user.private_key_pem
  })
}
