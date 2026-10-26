# Plans the module against a mocked AWS provider and checks the bucket's
# name, protections and lifecycle without an AWS account.

mock_provider "aws" {
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::changeport-test-tiered-storage"
    }
  }
}

variables {
  name       = "changeport-test"
  versioning = false
}

run "bucket" {
  command = plan

  assert {
    condition     = aws_s3_bucket.tiered_storage.bucket == "changeport-test-tiered-storage"
    error_message = "The bucket must be named as eksTieredStorage in internal/provisioning/spec.go expects."
  }
  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.tiered_storage.block_public_acls,
      aws_s3_bucket_public_access_block.tiered_storage.block_public_policy,
      aws_s3_bucket_public_access_block.tiered_storage.ignore_public_acls,
      aws_s3_bucket_public_access_block.tiered_storage.restrict_public_buckets,
    ])
    error_message = "All public access must be blocked."
  }
  assert {
    condition     = one(one(aws_s3_bucket_server_side_encryption_configuration.tiered_storage.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"
    error_message = "Objects must be encrypted at rest."
  }
}

# The policy names the bucket's ARN, known once the mocked bucket exists.
run "tls_only" {
  command = apply

  assert {
    condition = anytrue([for statement in jsondecode(aws_s3_bucket_policy.tiered_storage.policy).Statement :
      statement.Effect == "Deny" && try(statement.Condition.Bool["aws:SecureTransport"], "") == "false"
    ])
    error_message = "Requests without TLS must be denied."
  }
  assert {
    condition = anytrue([for statement in jsondecode(aws_s3_bucket_policy.tiered_storage.policy).Statement :
      statement.Effect == "Deny" && try(statement.Condition.NumericLessThan["s3:TlsVersion"], "") == "1.2"
    ])
    error_message = "TLS older than 1.2 must be denied."
  }
}

run "dev_lifecycle" {
  command = plan

  assert {
    condition     = one(aws_s3_bucket_versioning.tiered_storage.versioning_configuration).status == "Disabled"
    error_message = "Without versioning the bucket is unversioned."
  }
  assert {
    condition     = [for rule in aws_s3_bucket_lifecycle_configuration.tiered_storage.rule : rule.id] == ["abort-incomplete-multipart-uploads"]
    error_message = "Without versioning, only incomplete uploads expire."
  }
  assert {
    condition     = one(aws_s3_bucket_lifecycle_configuration.tiered_storage.rule[0].abort_incomplete_multipart_upload).days_after_initiation == 1
    error_message = "Incomplete multipart uploads are aborted after a day."
  }
}

run "prod_lifecycle" {
  command = plan

  variables {
    versioning = true
  }

  assert {
    condition     = one(aws_s3_bucket_versioning.tiered_storage.versioning_configuration).status == "Enabled"
    error_message = "With versioning the bucket keeps old versions."
  }
  assert {
    condition     = one([for rule in aws_s3_bucket_lifecycle_configuration.tiered_storage.rule : one(rule.noncurrent_version_expiration).noncurrent_days if rule.id == "expire-old-versions"]) == 7
    error_message = "Old versions expire after a week."
  }
}
