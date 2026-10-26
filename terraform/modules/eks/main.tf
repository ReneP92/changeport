# The one EKS cluster of an environment, in EKS Auto Mode: AWS runs node
# provisioning, OS patching, EBS volumes and load balancers. The platform
# components run on the built-in system and general-purpose node pools; each
# customer's brokers run on a NodePool and NodeClass of their own, which the
# Go provisioning service creates (eksDedicatedNodes in
# internal/provisioning/spec.go) with the node role and the security group tag
# made here. Customer namespaces and Strimzi resources are not managed by
# Terraform either.

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

locals {
  account   = data.aws_caller_identity.current.account_id
  partition = data.aws_partition.current.partition

  # The roles IAM Identity Center creates in this account for each
  # administrator permission set. Access entries take a role's ARN without
  # its /aws-reserved/sso.amazonaws.com/ path.
  administrator_roles = sort(flatten([for roles in data.aws_iam_roles.administrators : tolist(roles.names)]))

  # Read by the per-customer NodeClasses: subnetSelectorTerms in
  # terraform/modules/vpc, securityGroupSelectorTerms here.
  customer_nodes_tag = { "changeport.io/customer-nodes" = "true" }
}

data "aws_iam_roles" "administrators" {
  for_each = toset(var.administrator_permission_sets)

  name_regex  = "^AWSReservedSSO_${each.key}_[0-9a-f]+$"
  path_prefix = "/aws-reserved/sso.amazonaws.com/"
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.29.0"

  name               = var.name
  kubernetes_version = var.kubernetes_version
  # Standard support only: a version past it is upgraded rather than billed
  # for extended support.
  upgrade_policy = {
    support_type = "STANDARD"
  }
  deletion_protection = var.deletion_protection

  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids

  # The API is reachable from the allowlisted addresses, and from the nodes
  # inside the VPC.
  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.api_allowed_cidrs
  endpoint_private_access      = true

  compute_config = {
    enabled    = true
    node_pools = ["system", "general-purpose"]
  }
  # Lets the cluster role tag what Auto Mode launches with the NodeClass's
  # tags, such as each customer's changeport.io/tenant.
  enable_auto_mode_custom_tags = true

  # Access entries only, no aws-auth ConfigMap.
  authentication_mode                      = "API"
  enable_cluster_creator_admin_permissions = false
  access_entries = merge(
    { for role in local.administrator_roles : role => {
      principal_arn = "arn:${local.partition}:iam::${local.account}:role/${role}"
      policy_associations = {
        admin = {
          policy_arn   = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    } },
    {
      customer-nodes = {
        principal_arn = aws_iam_role.customer_node.arn
        type          = "EC2"
        policy_associations = {
          auto-node = {
            policy_arn   = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSAutoNodePolicy"
            access_scope = { type = "cluster" }
          }
        }
      }
    },
  )
}

# The cluster security group, which EKS creates, is the one the customer
# nodes join.
resource "aws_ec2_tag" "customer_nodes_security_group" {
  for_each = local.customer_nodes_tag

  resource_id = module.eks.cluster_primary_security_group_id
  key         = each.key
  value       = each.value
}

# The role of every customer's nodes, named as eksDedicatedNodes expects. It
# has the policies of the built-in node pools' role.
resource "aws_iam_role" "customer_node" {
  name        = "${var.name}-customer-node"
  description = "Nodes of the per-customer EKS Auto Mode NodeClasses"

  assume_role_policy = data.aws_iam_policy_document.customer_node_assume_role.json
}

data "aws_iam_policy_document" "customer_node_assume_role" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "customer_node" {
  for_each = toset(["AmazonEKSWorkerNodeMinimalPolicy", "AmazonEC2ContainerRegistryPullOnly"])

  role       = aws_iam_role.customer_node.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

# external-dns publishes the customers' Kafka names and cert-manager answers
# Let's Encrypt's DNS challenges for them, in the kafka.* hosted zones only.
# The zones come with the domain (#8); until then neither role exists.
locals {
  kafka_dns_zone_arns = [for id in var.kafka_dns_zone_ids : "arn:${local.partition}:route53:::hostedzone/${id}"]
}

module "external_dns_pod_identity" {
  source  = "terraform-aws-modules/eks-pod-identity/aws"
  version = "2.10.0"

  create = length(var.kafka_dns_zone_ids) > 0

  name                          = "${var.name}-external-dns"
  attach_external_dns_policy    = true
  external_dns_hosted_zone_arns = local.kafka_dns_zone_arns

  associations = {
    this = {
      cluster_name    = module.eks.cluster_name
      namespace       = "external-dns"
      service_account = "external-dns"
    }
  }
}

module "cert_manager_pod_identity" {
  source  = "terraform-aws-modules/eks-pod-identity/aws"
  version = "2.10.0"

  create = length(var.kafka_dns_zone_ids) > 0

  name                          = "${var.name}-cert-manager"
  attach_cert_manager_policy    = true
  cert_manager_hosted_zone_arns = local.kafka_dns_zone_arns

  associations = {
    this = {
      cluster_name    = module.eks.cluster_name
      namespace       = "cert-manager"
      service_account = "cert-manager"
    }
  }
}
