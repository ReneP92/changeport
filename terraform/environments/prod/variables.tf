variable "region" {
  description = "AWS region of the environment."
  type        = string
  default     = "us-east-1"
}

variable "availability_zones" {
  description = "The three availability zones used in the region, by zone ID. use1-az3 is left out because EKS does not run control planes there."
  type        = list(string)
  default     = ["use1-az1", "use1-az2", "use1-az4"]
}

variable "vpc_cidr_block" {
  description = "IPv4 CIDR block of the environment's VPC. Dev and prod do not overlap, so they can be peered or connected later."
  type        = string
  default     = "10.20.0.0/16"
}

variable "kubernetes_version" {
  description = "Kubernetes minor version of the EKS cluster."
  type        = string
  default     = "1.37"
}

variable "api_allowed_cidrs" {
  description = "IPv4 CIDR blocks allowed to reach the EKS cluster's public API endpoint."
  type        = list(string)
}

variable "administrator_permission_sets" {
  description = "IAM Identity Center permission sets whose roles administer the EKS cluster."
  type        = list(string)
  default     = ["AdministratorAccess"]
}

variable "kafka_dns_zone_ids" {
  description = "Route 53 hosted zone IDs of the environment's kafka.* zones, once the domain is registered (#8)."
  type        = list(string)
  default     = []
}
