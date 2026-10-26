variable "name" {
  description = "Name of the EKS cluster, for example changeport-dev."
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes minor version of the control plane, kept in step with KIND_NODE_IMAGE in hack/lib.sh."
  type        = string
}

variable "vpc_id" {
  description = "ID of the environment's VPC (terraform/modules/vpc)."
  type        = string
}

variable "subnet_ids" {
  description = "The private subnets, one per availability zone, for the control plane's network interfaces and the built-in node pools."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) == 3
    error_message = "The cluster spans exactly three availability zones."
  }
}

variable "api_allowed_cidrs" {
  description = "IPv4 CIDR blocks allowed to reach the public Kubernetes API endpoint, such as the operators' and CI's addresses."
  type        = list(string)

  validation {
    condition     = length(var.api_allowed_cidrs) > 0 && alltrue([for cidr in var.api_allowed_cidrs : can(cidrhost(cidr, 0)) && cidr != "0.0.0.0/0"])
    error_message = "List at least one IPv4 CIDR block, and not 0.0.0.0/0: the API must not be open to the internet."
  }
}

variable "administrator_permission_sets" {
  description = "IAM Identity Center permission sets whose roles in this account administer the cluster (AmazonEKSClusterAdminPolicy)."
  type        = list(string)
}

variable "kafka_dns_zone_ids" {
  description = "Route 53 hosted zone IDs of the kafka.* zones the customers' Kafka names live in, which external-dns and cert-manager may change. Empty until the domain is registered (#8)."
  type        = list(string)
  default     = []
}

variable "deletion_protection" {
  description = "Whether EKS refuses to delete the cluster until this is turned off."
  type        = bool
}
