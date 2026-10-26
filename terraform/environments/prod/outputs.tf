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

output "tiered_storage_bucket" {
  description = "The tiered storage bucket, the Bucket of eksTieredStorage in internal/provisioning/spec.go."
  value       = module.s3.tiered_storage_bucket
}

output "kafka_broker_role_name" {
  description = "The brokers' role, the RoleName of eksTieredStorage in internal/provisioning/spec.go."
  value       = module.iam.kafka_broker_role_name
}
