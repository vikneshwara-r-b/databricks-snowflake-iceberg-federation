# Customer-managed KMS key for the two Secrets Manager secrets in secrets.tf. Without
# this, Secrets Manager falls back to the AWS-managed aws/secretsmanager key, which a
# security scan (CC-130) flags because a customer-managed key gives you control over
# the key policy, rotation, and the ability to revoke access independently of the
# secret itself.

resource "aws_kms_key" "secrets" {
  description             = "Customer-managed key for federation-poc Secrets Manager secrets"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/federation-poc-secrets"
  target_key_id = aws_kms_key.secrets.key_id
}
