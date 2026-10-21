terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.11"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

# Scoped to a named profile, not whatever's ambient-default, so an apply can't land in
# the wrong AWS account.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# Scoped to a named Databricks CLI profile (~/.databrickscfg) — the same mechanism this
# project's README already uses (`databricks auth login --profile <name>`). No
# Databricks token lives in Secrets Manager or anywhere in this Terraform config.
provider "databricks" {
  profile = var.databricks_profile
}

# Authenticates directly off the admin variables below — NOT by reading back the
# Secrets Manager secret those same variables get written into (secrets.tf). That
# read-back is what would create a same-apply provider/resource cycle; using the plain
# variable here avoids it entirely, since a provider block can depend on input
# variables freely, just not on another resource's computed attributes.
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_admin_private_key_pem
  role              = var.snowflake_admin_role
}

# Same identity/key as above, different active role. Snowflake's standard convention
# is that SYSADMIN (not ACCOUNTADMIN/SECURITYADMIN) owns warehouses/databases/schemas,
# so the data-plane objects in snowflake_data.tf are created under this alias. This
# assumes the admin user can activate SYSADMIN — true by default for an ACCOUNTADMIN
# user, since SYSADMIN sits in ACCOUNTADMIN's role hierarchy; if your admin role setup
# differs, grant SYSADMIN to var.snowflake_admin_user directly.
provider "snowflake" {
  alias             = "sysadmin"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_admin_private_key_pem
  role              = "SYSADMIN"
}
