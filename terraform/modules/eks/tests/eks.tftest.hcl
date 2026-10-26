# Plans the module against mocked providers, so the access entries, the
# customer node role and security group tag the per-customer NodeClasses
# depend on, and the DNS roles are checked without an AWS account.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_AdministratorAccess_0123456789abcdef/operator"
    }
  }
  mock_data "aws_iam_session_context" {
    defaults = {
      issuer_arn = "arn:aws:iam::123456789012:role/AWSReservedSSO_AdministratorAccess_0123456789abcdef"
    }
  }
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/mocked"
    }
  }
  mock_resource "aws_iam_policy" {
    defaults = {
      arn = "arn:aws:iam::123456789012:policy/mocked"
    }
  }
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

mock_provider "tls" {}

mock_provider "time" {}

override_data {
  target = data.aws_iam_roles.administrators["AdministratorAccess"]
  values = {
    names = ["AWSReservedSSO_AdministratorAccess_0123456789abcdef"]
  }
}

variables {
  name                          = "changeport-test"
  kubernetes_version            = "1.37"
  vpc_id                        = "vpc-0123456789abcdef0"
  subnet_ids                    = ["subnet-a", "subnet-b", "subnet-c"]
  api_allowed_cidrs             = ["198.51.100.0/24"]
  administrator_permission_sets = ["AdministratorAccess"]
  deletion_protection           = false
}

run "access_entries" {
  command = plan

  assert {
    condition     = module.eks.access_entries["AWSReservedSSO_AdministratorAccess_0123456789abcdef"].principal_arn == "arn:aws:iam::123456789012:role/AWSReservedSSO_AdministratorAccess_0123456789abcdef"
    error_message = "Each administrator permission set's role gets an access entry, by its ARN without the SSO path."
  }
  assert {
    condition     = module.eks.access_entries["customer-nodes"].type == "EC2"
    error_message = "The customer node role needs an EC2 access entry."
  }
  assert {
    condition     = length(module.eks.access_entries) == 2
    error_message = "Only the administrators and the customer nodes have access entries."
  }
}

run "customer_nodes" {
  command = plan

  assert {
    condition     = aws_iam_role.customer_node.name == "changeport-test-customer-node"
    error_message = "The customer node role must be named as eksDedicatedNodes in internal/provisioning/spec.go expects."
  }
  assert {
    condition     = toset(keys(aws_iam_role_policy_attachment.customer_node)) == toset(["AmazonEKSWorkerNodeMinimalPolicy", "AmazonEC2ContainerRegistryPullOnly"])
    error_message = "The customer node role needs the policies of the built-in node pools' role."
  }
  assert {
    condition     = aws_ec2_tag.customer_nodes_security_group["changeport.io/customer-nodes"].value == "true"
    error_message = "The cluster security group must carry the tag the customers' NodeClasses select."
  }
}

run "no_dns_roles_without_zones" {
  command = plan

  assert {
    condition     = module.external_dns_pod_identity.iam_role_arn == null && module.cert_manager_pod_identity.iam_role_arn == null
    error_message = "Without kafka.* zones there is nothing for external-dns and cert-manager to change."
  }
}

run "dns_roles_limited_to_the_zones" {
  command = plan

  variables {
    kafka_dns_zone_ids = ["Z0123456789ABCDEFGHIJ"]
  }

  assert {
    condition     = local.kafka_dns_zone_arns == ["arn:aws:route53:::hostedzone/Z0123456789ABCDEFGHIJ"]
    error_message = "The DNS roles are limited to the kafka.* zones."
  }
  assert {
    condition     = length(module.external_dns_pod_identity.associations) == 1 && length(module.cert_manager_pod_identity.associations) == 1
    error_message = "external-dns and cert-manager each get a Pod Identity association."
  }
}

run "open_api_is_refused" {
  command = plan

  variables {
    api_allowed_cidrs = ["0.0.0.0/0"]
  }

  expect_failures = [var.api_allowed_cidrs]
}

run "two_zones_are_refused" {
  command = plan

  variables {
    subnet_ids = ["subnet-a", "subnet-b"]
  }

  expect_failures = [var.subnet_ids]
}
