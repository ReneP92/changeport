# terraform

Base infrastructure per environment: VPC, EKS, S3 and IAM. Each environment in
`environments/` lives in its own AWS account and composes the modules in
`modules/`. Nothing has been applied yet: that waits for the accounts and the
state backend (#10).

Per-customer resources are not managed here; the Go provisioning service
creates them through the Kubernetes API.

```bash
just tf-fmt tf-validate tf-test
```

The recipes download the Terraform version that `hack/lib.sh` pins into
`bin/`, checksum-verified, and CI runs the same recipes. `tf-test` runs each
module's `tests/` with `terraform test` against a mocked AWS provider, so it
needs no AWS account. The lock files pin the AWS provider for Linux and macOS
on amd64 and arm64; after changing a provider version, run
`terraform providers lock -platform=linux_amd64 -platform=linux_arm64
-platform=darwin_amd64 -platform=darwin_arm64` in each environment and module
with tests.

## Network (`modules/vpc`)

Each environment has one VPC in us-east-1 across three availability zones,
named by zone ID (`use1-az1`, `use1-az2`, `use1-az4`) so they are the same
zones in every account. `use1-az3` is left out because EKS does not run
control planes there.

| | dev | prod |
| --- | --- | --- |
| CIDR block | `10.10.0.0/16` | `10.20.0.0/16` |
| NAT gateways | one, shared | one per zone |

- **Private subnets**, a /18 per zone, hold the EKS nodes and pods: EKS Auto
  Mode gives every pod a VPC address, so they take three quarters of the VPC.
  They carry `kubernetes.io/role/internal-elb` for internal load balancers and
  `changeport.io/customer-nodes=true`, which the per-customer NodeClasses
  select (`eksDedicatedNodes` in `internal/provisioning/spec.go`).
- **Public subnets**, a /24 per zone in the last quarter, hold the NAT
  gateways and the internet-facing Network Load Balancers of the customers'
  Kafka listeners. They carry `kubernetes.io/role/elb`.
- **An S3 gateway endpoint** on the private route tables carries tiered
  storage traffic, which would otherwise pay NAT gateway charges per byte.

Prod has a NAT gateway per zone, so a zone outage cannot cut the other zones
off from the internet; dev shares one to save about $65 a month.

## Cluster (`modules/eks`)

Each environment has one EKS cluster in Auto Mode, on Kubernetes 1.37 with
standard support only, in the private subnets. AWS runs node provisioning, OS
patching, EBS volumes and load balancers.

- **Nodes:** the platform components run on Auto Mode's built-in `system` and
  `general-purpose` node pools. Each customer's brokers run on a NodePool and
  NodeClass of their own, which the provisioning service creates
  (`eksDedicatedNodes` in `internal/provisioning/spec.go`). Those NodeClasses
  use the role `changeport-<env>-customer-node` made here, which has an EC2
  access entry with `AmazonEKSAutoNodePolicy`, and select the cluster security
  group by its `changeport.io/customer-nodes=true` tag. The cluster role may
  tag what it launches, for each customer's `changeport.io/tenant`.
- **Access:** access entries only (`API` mode). The roles IAM Identity Center
  creates for the `administrator_permission_sets` (`AdministratorAccess` by
  default) are cluster administrators. The public API endpoint only answers
  the `api_allowed_cidrs`, which each apply must be given; `0.0.0.0/0` is
  refused.
- **DNS:** once the domain is registered (#8), `kafka_dns_zone_ids` gives
  external-dns and cert-manager Pod Identity roles that may change only the
  `kafka.*` zones. Without zones, neither role exists.
- **Deletion protection** is on in prod and off in dev, which can be rebuilt.

After an apply, sign in through single sign-on and point kubectl at the
cluster:

```bash
aws sso login
aws eks update-kubeconfig --region us-east-1 --name "$(terraform -chdir=terraform/environments/dev output -raw cluster_name)"
kubectl get nodes
```

## Tiered storage (`modules/s3`, `modules/iam`)

Each environment has one bucket for Kafka tiered storage,
`changeport-<env>-tiered-storage`, and one role the brokers assume through
EKS Pod Identity, `changeport-<env>-kafka-broker`, the names
`eksTieredStorage` in `internal/provisioning/spec.go` expects.

- **The bucket** blocks all public access, has object ACLs off, encrypts at
  rest (SSE-S3) and refuses requests without TLS or with TLS older than 1.2.
  Incomplete multipart uploads are aborted after a day. Prod keeps old
  versions of deleted or overwritten objects for a week; dev is unversioned.
- **The role** trusts only the Pod Identity of the environment's own cluster.
  Its policy reaches objects under `${aws:PrincipalTag/kubernetes-namespace}/`
  only: the provisioner associates each customer's broker service account
  with the role, with session tags on, so every session carries the pod's
  namespace, which is also the customer's key prefix (`customer-<id>/`).
  Sessions of namespaces not named `customer-*` get nothing.

After an apply, check with the IAM policy simulator that two customers cannot
reach each other's prefix:

```bash
just tiered-storage-isolation-test dev
```
