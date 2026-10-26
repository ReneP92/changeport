# Plans the module against a mocked AWS provider and checks the brokers' role:
# who may assume it, and that each customer's brokers reach only their own
# key prefix.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }
}

variables {
  name                      = "changeport-test"
  cluster_arn               = "arn:aws:eks:us-east-1:123456789012:cluster/changeport-test"
  tiered_storage_bucket_arn = "arn:aws:s3:::changeport-test-tiered-storage"
}

run "role" {
  command = plan

  assert {
    condition     = aws_iam_role.kafka_broker.name == "changeport-test-kafka-broker"
    error_message = "The role must be named as eksTieredStorage in internal/provisioning/spec.go expects."
  }
  assert {
    condition = jsondecode(aws_iam_role.kafka_broker.assume_role_policy).Statement == [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
      Condition = {
        StringEquals = { "aws:SourceAccount" = "123456789012" }
        ArnEquals    = { "aws:SourceArn" = "arn:aws:eks:us-east-1:123456789012:cluster/changeport-test" }
      }
    }]
    error_message = "Only the environment's cluster's Pod Identity may assume the role, with session tags."
  }
}

run "own_prefix_only" {
  command = plan

  assert {
    condition = [for statement in jsondecode(aws_iam_role_policy.kafka_broker.policy).Statement : statement.Resource] == [
      "arn:aws:s3:::changeport-test-tiered-storage/$${aws:PrincipalTag/kubernetes-namespace}/*",
      "arn:aws:s3:::changeport-test-tiered-storage",
    ]
    error_message = "Objects are reachable under the session's namespace prefix only, and the bucket only for listing."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.kafka_broker.policy).Statement[1].Condition.StringLike["s3:prefix"] == "$${aws:PrincipalTag/kubernetes-namespace}/*"
    error_message = "Listing is limited to the session's namespace prefix."
  }
  assert {
    condition = alltrue([for statement in jsondecode(aws_iam_role_policy.kafka_broker.policy).Statement :
      statement.Effect == "Allow" && statement.Condition.StringLike["aws:PrincipalTag/kubernetes-namespace"] == "customer-*"
    ])
    error_message = "Sessions of namespaces other than customers' must get nothing."
  }
  assert {
    condition = alltrue([for statement in jsondecode(aws_iam_role_policy.kafka_broker.policy).Statement :
      alltrue([for action in flatten([statement.Action]) : !strcontains(action, "*")])
    ])
    error_message = "The policy names its actions; no wildcards."
  }
}
