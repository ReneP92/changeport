# The bucket of one environment for Kafka tiered storage, which holds topic
# history for replay. Every customer's brokers write under a key prefix of
# their own, their namespace (customer-<id>/), and may reach no other
# (terraform/modules/iam). Kafka deletes what retention expires.

resource "aws_s3_bucket" "tiered_storage" {
  bucket = "${var.name}-tiered-storage"
}

resource "aws_s3_bucket_public_access_block" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Object ACLs are off: the bucket owns everything written to it.
resource "aws_s3_bucket_ownership_controls" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning keeps what a broker deletes or overwrites for a while, so a
# mistake can be undone; old versions expire.
resource "aws_s3_bucket_versioning" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id

  versioning_configuration {
    status = var.versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id

  # The brokers upload segments in parts; a broker that dies mid-upload
  # leaves parts behind that are billed but never read.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  dynamic "rule" {
    for_each = var.versioning ? [1] : []

    content {
      id     = "expire-old-versions"
      status = "Enabled"
      filter {}

      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_version_expiration_days
      }
      # Removes the delete markers left once a key's old versions expired.
      expiration {
        expired_object_delete_marker = true
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.tiered_storage]
}

# Requests without TLS, or with TLS older than 1.2, are refused.
resource "aws_s3_bucket_policy" "tiered_storage" {
  bucket = aws_s3_bucket.tiered_storage.id
  policy = jsonencode(local.bucket_policy)

  depends_on = [aws_s3_bucket_public_access_block.tiered_storage]
}

locals {
  bucket_and_objects = [aws_s3_bucket.tiered_storage.arn, "${aws_s3_bucket.tiered_storage.arn}/*"]

  bucket_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = local.bucket_and_objects
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      {
        Sid       = "DenyOldTLS"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = local.bucket_and_objects
        Condition = { NumericLessThan = { "s3:TlsVersion" = "1.2" } }
      },
    ]
  }
}
