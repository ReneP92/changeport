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
