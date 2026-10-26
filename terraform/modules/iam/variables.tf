variable "name" {
  description = "Name prefix of the roles, for example changeport-dev, which makes changeport-dev-kafka-broker as eksTieredStorage in internal/provisioning/spec.go expects."
  type        = string
}

variable "cluster_arn" {
  description = "ARN of the environment's EKS cluster, the only one whose Pod Identity may hand out broker sessions."
  type        = string
}

variable "tiered_storage_bucket_arn" {
  description = "ARN of the tiered storage bucket (terraform/modules/s3)."
  type        = string
}
