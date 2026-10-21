# S3 bucket + IAM role backing the Unity Catalog catalog's managed storage.

# Captures "now" once, at first creation, and freezes it in state from then on —
# unlike timestamp(), which re-evaluates on every plan and would otherwise make the
# bucket name (and so the bucket itself) look like it needs replacing on every apply.
resource "time_static" "bucket_suffix" {}

locals {
  bucket_name = "${var.s3_bucket_name}-${formatdate("YYYYMMDD", time_static.bucket_suffix.rfc3339)}"
}

resource "aws_s3_bucket" "uc_storage" {
  bucket = local.bucket_name

  # This account auto-injects tags (e.g. Owner, c7n-created-by) outside Terraform —
  # confirmed by a live plan showing Terraform wanting to remove them, the same org-
  # level tag-governance pattern that denied secretsmanager:UntagResource earlier
  # (secrets.tf). Ignoring tags here avoids hitting the equivalent deny for S3.
  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_s3_bucket_versioning" "uc_storage" {
  bucket = aws_s3_bucket.uc_storage.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "uc_storage" {
  bucket = aws_s3_bucket.uc_storage.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "uc_storage" {
  bucket                  = aws_s3_bucket.uc_storage.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_caller_identity" "current" {}

# Unity Catalog storage credential roles need their EXTERNAL ID to be whatever
# Databricks actually generates for the specific storage credential (databricks_
# catalog.tf) that will assume this role — it's a response-only field (confirmed
# against the Create Storage Credential REST API docs), never something the client
# gets to pick. That value doesn't exist until the storage credential itself has been
# created, which itself needs this role's ARN to exist first — a genuine bootstrap
# cycle. Databricks' own documented fix for this (see "Create a storage credential for
# connecting to AWS S3") is: create the role with a placeholder trust policy, create
# the credential, copy its real external_id, then edit the role's trust policy to
# match. We automate that edit with null_resource.fix_trust_policy below instead of
# requiring a manual step or a second `terraform apply` — it can run as a one-directional
# dependency (reads the credential's output, shells out to the AWS CLI to PATCH the
# role) without the role resource itself ever referencing the credential, which is
# what would otherwise make this a real Terraform dependency cycle.
resource "aws_iam_role" "uc_storage" {
  name = "uc-${var.catalog_name}-storage-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "Bootstrap"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })

  # null_resource.fix_trust_policy patches this role's REAL trust policy directly via
  # the AWS CLI, outside Terraform's management of this attribute. Without
  # ignore_changes here, the next plan's refresh would see that live value no longer
  # matches the placeholder above and revert it right back — silently undoing the fix
  # on every subsequent apply. This is what hands ongoing ownership of
  # assume_role_policy to the null_resource once bootstrapped.
  lifecycle {
    ignore_changes = [assume_role_policy]
  }
}

data "databricks_aws_unity_catalog_policy" "uc_storage" {
  aws_account_id = data.aws_caller_identity.current.account_id
  bucket_name    = aws_s3_bucket.uc_storage.id
  role_name      = "uc-${var.catalog_name}-storage-role"
}

resource "aws_iam_policy" "uc_storage" {
  name   = "uc-${var.catalog_name}-storage-policy"
  policy = data.databricks_aws_unity_catalog_policy.uc_storage.json
}

resource "aws_iam_role_policy_attachment" "uc_storage" {
  role       = aws_iam_role.uc_storage.name
  policy_arn = aws_iam_policy.uc_storage.arn
}

# The REAL trust policy — built once the storage credential (databricks_catalog.tf)
# exists and has handed back its actual external_id. This data source makes no live
# API call itself; it just templates JSON from its arguments, same as before.
data "databricks_aws_unity_catalog_assume_role_policy" "uc_storage_final" {
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = aws_iam_role.uc_storage.name
  external_id    = databricks_storage_credential.uc_external.aws_iam_role[0].external_id
}

resource "local_file" "uc_storage_trust_policy" {
  content  = data.databricks_aws_unity_catalog_assume_role_policy.uc_storage_final.json
  filename = "${path.module}/.generated/${aws_iam_role.uc_storage.name}-trust-policy.json"
}

resource "null_resource" "fix_trust_policy" {
  triggers = {
    external_id = databricks_storage_credential.uc_external.aws_iam_role[0].external_id
  }

  provisioner "local-exec" {
    command = "aws iam update-assume-role-policy --role-name ${aws_iam_role.uc_storage.name} --policy-document file://${local_file.uc_storage_trust_policy.filename} --profile ${var.aws_profile} --region ${var.aws_region}"
  }

  depends_on = [local_file.uc_storage_trust_policy]
}

# IAM is eventually consistent — both the initial permission-policy attachment and
# the trust-policy patch above need a moment before Databricks' live S3 access check
# (at external location creation) will see them. This pause is what the Databricks
# provider's own docs recommend for exactly this case.
resource "time_sleep" "wait_for_iam_propagation" {
  depends_on      = [aws_iam_role_policy_attachment.uc_storage, null_resource.fix_trust_policy]
  create_duration = "30s"
}
