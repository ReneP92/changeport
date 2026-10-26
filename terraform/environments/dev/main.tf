# Base infrastructure of the dev environment, in its own AWS account.
# Nothing here has been applied yet; the account and the state backend are
# chosen in https://github.com/ReneP92/changeport/issues/10.

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "changeport"
      Environment = "dev"
    }
  }
}

locals {
  name = "changeport-dev"
}

module "vpc" {
  source = "../../modules/vpc"

  name               = local.name
  cidr_block         = var.vpc_cidr_block
  availability_zones = var.availability_zones

  # One NAT gateway for the whole VPC: dev can live with a zone outage
  # cutting it off from the internet.
  nat_gateway_per_zone = false
}

module "eks" {
  source = "../../modules/eks"

  name               = local.name
  kubernetes_version = var.kubernetes_version
  vpc_id             = module.vpc.vpc_id
  subnet_ids         = module.vpc.private_subnet_ids

  api_allowed_cidrs             = var.api_allowed_cidrs
  administrator_permission_sets = var.administrator_permission_sets
  kafka_dns_zone_ids            = var.kafka_dns_zone_ids

  # Dev can be torn down and rebuilt.
  deletion_protection = false
}

module "s3" {
  source = "../../modules/s3"

  name = local.name
}

module "iam" {
  source = "../../modules/iam"

  name = local.name
}
