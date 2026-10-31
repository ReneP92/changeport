output "vpc_id" {
  description = "ID of the VPC."
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets, one per availability zone, for the EKS nodes."
  value       = module.vpc.private_subnets
}

output "public_subnet_ids" {
  description = "IDs of the public subnets, one per availability zone, for load balancers."
  value       = module.vpc.public_subnets
}

output "nat_public_ips" {
  description = "The public addresses of the NAT gateways: where the platform's connections to the internet come from, which customers' databases allowlist for Postgres CDC sources."
  value       = module.vpc.nat_public_ips
}
