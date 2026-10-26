# Base infrastructure of the prod environment, in its own AWS account.
# Nothing here has been applied yet; the account and the state backend are
# chosen in https://github.com/ReneP92/changeport/issues/10.

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "changeport"
      Environment = "prod"
    }
  }
}

locals {
  name = "changeport-prod"
}

module "vpc" {
  source = "../../modules/vpc"

  name               = local.name
  cidr_block         = var.vpc_cidr_block
  availability_zones = var.availability_zones

  # A NAT gateway per zone, so the loss of one zone leaves the others
  # their way to the internet.
  nat_gateway_per_zone = true
}

module "eks" {
  source = "../../modules/eks"

  name               = local.name
  kubernetes_version = var.kubernetes_version
}

module "s3" {
  source = "../../modules/s3"

  name = local.name
}

module "iam" {
  source = "../../modules/iam"

  name = local.name
}
