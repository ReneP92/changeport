# The one EKS cluster of an environment, with Karpenter for the per-customer
# NodePools. It takes the VPC and private subnets from the vpc module once that
# module creates them. Customer namespaces, NodePools and Strimzi resources are
# created by the Go provisioning service, not by Terraform. Resources are added
# in Phase 1.
