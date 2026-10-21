variable "region" {
  description = "AWS region of the environment."
  type        = string
}

variable "availability_zones" {
  description = "The three availability zones used in the region."
  type        = list(string)
}

variable "vpc_cidr_block" {
  description = "IPv4 CIDR block of the environment's VPC."
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes minor version of the EKS cluster."
  type        = string
  default     = "1.37"
}
