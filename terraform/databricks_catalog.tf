# Storage credential -> external location -> managed catalog, all backed by the S3
# bucket in aws_storage.tf. This replaces src/utility/utiltiy_notebook.ipynb's manual
# `CREATE CATALOG IF NOT EXISTS` for the `prod` target — Terraform now owns the
# catalog's lifecycle (and its storage), not an ad hoc notebook run. The `dev` target
# is untouched: it keeps using the workspace's built-in `workspace` catalog.

resource "databricks_storage_credential" "uc_external" {
  name = "uc-${var.catalog_name}-credential"

  aws_iam_role {
    role_arn = aws_iam_role.uc_storage.arn
    # external_id is deliberately NOT set here — it's response-only (confirmed against
    # the Create Storage Credential REST API docs); Databricks generates it and we
    # read it back (aws_storage.tf's null_resource.fix_trust_policy) to patch the
    # role's trust policy afterward. Setting it here would just drift against
    # whatever Databricks actually assigns.
  }

  # Deliberately does NOT depend on time_sleep.wait_for_iam_propagation — that would
  # cycle, since the sleep now waits on null_resource.fix_trust_policy, which itself
  # needs this resource to exist first (to read its real external_id). Databricks
  # doesn't validate S3 access at storage-credential creation time anyway, only at
  # external-location creation — confirmed by this exact resource having succeeded
  # cleanly in every failed apply so far, with the error always on the next resource.
}

resource "databricks_external_location" "uc_external" {
  name            = "uc-${var.catalog_name}-location"
  url             = "s3://${aws_s3_bucket.uc_storage.id}/"
  credential_name = databricks_storage_credential.uc_external.id

  depends_on = [time_sleep.wait_for_iam_propagation]
}

resource "databricks_catalog" "f1_analytics" {
  name         = var.catalog_name
  comment      = "Managed by Terraform — backs the prod target's catalog, with external S3 storage. Replaces the manual CREATE CATALOG step in utiltiy_notebook.ipynb."
  storage_root = "s3://${aws_s3_bucket.uc_storage.id}/${var.catalog_name}"

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [databricks_external_location.uc_external]
}
