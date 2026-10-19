variable "name" {
  description = "Name of the EKS cluster, for example changeport-dev."
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes minor version of the control plane, kept in step with deploy/kind/cluster.yaml."
  type        = string
}
