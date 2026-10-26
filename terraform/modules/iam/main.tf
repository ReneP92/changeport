# The role every customer's Kafka brokers assume through EKS Pod Identity for
# tiered storage. The provisioning service associates each customer's broker
# service account with it (internal/provisioning/podidentity.go), with session
# tags on, so each session carries the pod's namespace as the
# kubernetes-namespace tag. The policy matches the tag against the key prefix:
# a customer's brokers may only reach customer-<id>/, their own namespace.
# The customers' node role and the DNS roles are in terraform/modules/eks.

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "kafka_broker" {
  name        = "${var.name}-kafka-broker"
  description = "Kafka brokers' access to their own prefix of the tiered storage bucket"

  assume_role_policy = jsonencode(local.kafka_broker_trust)
}

resource "aws_iam_role_policy" "kafka_broker" {
  name   = "tiered-storage"
  role   = aws_iam_role.kafka_broker.id
  policy = jsonencode(local.kafka_broker_policy)
}

locals {
  # Only EKS Pod Identity of this account's cluster may hand out sessions.
  kafka_broker_trust = {
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
      Condition = {
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        ArnEquals    = { "aws:SourceArn" = var.cluster_arn }
      }
    }]
  }

  # $${...} is an IAM policy variable, resolved per session: the namespace of
  # the pod the session is for. Sessions of other namespaces than customers'
  # get nothing.
  own_prefix         = "$${aws:PrincipalTag/kubernetes-namespace}/"
  customer_namespace = { "aws:PrincipalTag/kubernetes-namespace" = "customer-*" }

  kafka_broker_policy = {
    Version = "2012-10-17"
    Statement = [
      {
        # What the Aiven remote storage manager does with segments: multipart
        # uploads, ranged reads, and deletes once retention expires.
        Sid    = "OwnPrefixObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource  = "${var.tiered_storage_bucket_arn}/${local.own_prefix}*"
        Condition = { StringLike = local.customer_namespace }
      },
      {
        Sid      = "OwnPrefixListing"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = var.tiered_storage_bucket_arn
        Condition = {
          StringLike = merge(local.customer_namespace, { "s3:prefix" = "${local.own_prefix}*" })
        }
      },
    ]
  }
}
