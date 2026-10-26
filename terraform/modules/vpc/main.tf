# Network for one environment: a VPC spanning three availability zones with
# public and private subnets, so brokers can survive the loss of a zone.
#
# - Private subnets hold the EKS nodes and pods. EKS Auto Mode gives every
#   pod an address in them, so they take three quarters of the VPC.
# - Public subnets hold the NAT gateways and the internet-facing Network
#   Load Balancers of the customers' Kafka listeners, from the last quarter.
# - Tiered storage traffic to S3 goes through a gateway endpoint rather than
#   the NAT gateways, which would charge for every byte.

locals {
  # With a /16: three /18 private subnets, and three /24 public subnets in
  # the fourth /18.
  private_subnets = [for i in range(3) : cidrsubnet(var.cidr_block, 2, i)]
  public_subnets  = [for i in range(3) : cidrsubnet(cidrsubnet(var.cidr_block, 2, 3), 6, i)]
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.7.3"

  name = var.name
  cidr = var.cidr_block
  azs  = var.availability_zones

  private_subnets = local.private_subnets
  public_subnets  = local.public_subnets

  enable_nat_gateway     = true
  single_nat_gateway     = !var.nat_gateway_per_zone
  one_nat_gateway_per_az = var.nat_gateway_per_zone

  enable_dns_hostnames = true
  enable_dns_support   = true

  # EKS Auto Mode places internet-facing load balancers in subnets tagged
  # kubernetes.io/role/elb and internal ones in kubernetes.io/role/internal-elb.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  # The per-customer NodeClasses select the private subnets by
  # changeport.io/customer-nodes (eksDedicatedNodes in
  # internal/provisioning/spec.go).
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
    "changeport.io/customer-nodes"    = "true"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = {
    Name = "${var.name}-s3"
  }
}

data "aws_region" "current" {}
