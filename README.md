# changeport
The place where CDC is done correctly

Changeport starts as a managed Kafka service on AWS with a dedicated cluster
per customer; see [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/provisioner` | Stub of the provisioning service: creates and removes customer Kafka clusters |
| `internal/provisioning` | The per-customer template and the code that applies it through the Kubernetes API |
| `web` | React frontend (placeholder) |
| `terraform` | Base infrastructure per environment: VPC, EKS, S3, IAM (empty modules, nothing applied) |
| `deploy` | Kubernetes manifests: the kind cluster and the Strimzi operator |
| `hack` | Local development scripts behind the Makefile |
| `tools` | Go module pinning the tools (kind, golangci-lint); `make tools` builds them into `bin/` |

## Local development

Every customer gets a Kafka cluster in its own namespace, generated from one
template by the provisioning service. Locally this runs on
[kind](https://kind.sigs.k8s.io/) with the
[Strimzi](https://strimzi.io/) operator, mirroring the EKS layout.

Prerequisites: Docker (give it at least 6 GB of memory), Go 1.26 and make.
The scripts download pinned, checksum-verified copies of kubectl and the
Strimzi install file into `bin/` and `deploy/strimzi/upstream/`, and build
kind from `tools/go.mod`.

```bash
make dev-up
```

This creates the `changeport` kind cluster, installs Strimzi 1.3.0 watching
all namespaces, provisions the `demo` customer (namespace `customer-demo`, a
single-node KRaft cluster on Kafka 4.3.1) with the provisioning service, and
runs a smoke test that produces a message and consumes it back. It is safe to
run again.

Provision more customers from the same template, or run the end-to-end check,
which provisions a second customer, smoke tests both clusters and removes the
second one again:

```bash
make provision CUSTOMER=acme
```

```bash
make e2e
```

Other targets:

```bash
make smoke-test CUSTOMER=acme
```

```bash
make deprovision CUSTOMER=acme
```

```bash
make dev-down
```

The scripts only ever use the `kind-changeport` kubectl context. To look
around yourself, use the pinned kubectl:

```bash
bin/kubectl-v1.36.5 --context kind-changeport get kafka -A
```

To see what the template produces for a customer without a cluster:

```bash
go run ./cmd/provisioner render -customer acme -profile production
```

## Checks

```bash
make lint test
```

CI runs the same Go lint and tests, plus `make tf-fmt tf-validate` and
`make shellcheck`. Run `make help` for every target.
