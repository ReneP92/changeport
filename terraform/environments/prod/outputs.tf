output "cluster_name" {
  description = "Name of the EKS cluster: aws eks update-kubeconfig --name <this>."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "Endpoint of the Kubernetes API."
  value       = module.eks.cluster_endpoint
}

output "customer_node_role_name" {
  description = "IAM role of the customers' nodes, which eksDedicatedNodes in internal/provisioning/spec.go names."
  value       = module.eks.customer_node_role_name
}

output "vpc_id" {
  description = "ID of the environment's VPC."
  value       = module.vpc.vpc_id
}
