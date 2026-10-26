output "cluster_name" {
  description = "Name of the EKS cluster, the ClusterName of the platform's Pod Identity in internal/provisioning."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "Endpoint of the Kubernetes API, for aws eks update-kubeconfig."
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64-encoded certificate authority of the Kubernetes API."
  value       = module.eks.cluster_certificate_authority_data
}

output "customer_node_role_name" {
  description = "IAM role of the customers' nodes, the NodeRole of eksDedicatedNodes in internal/provisioning/spec.go."
  value       = aws_iam_role.customer_node.name
}

output "cluster_security_group_id" {
  description = "The cluster security group, tagged changeport.io/customer-nodes=true for the customers' NodeClasses."
  value       = module.eks.cluster_primary_security_group_id
}
