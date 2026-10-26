# terraform

Base infrastructure per environment: VPC, EKS, S3 and IAM. Each environment in
`environments/` lives in its own AWS account and composes the modules in
`modules/`. Nothing has been applied yet: that waits for the accounts and the
state backend (#10). The EKS, S3 and IAM modules are still empty (#16, #17).

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
