# changeport
The place where CDC is done correctly

Changeport starts as a managed Kafka service on AWS with a dedicated cluster
per customer; see [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md).

## Architecture

```text
                                Customer team
                              (browser, HTTPS)
                                      |
                                      v
+-- Control plane: Go services ---------------------------------------------+
|                                                                           |
|  +-------------------+    +---------v---------+    +-------------------+  |
|  | Provisioning      |    | API and React UI  |<---| Metrics, alerting |  |
|  | cmd/provisioner   |    | topology, lag,    |    | VictoriaMetrics,  |  |
|  |                   |    | diagnoses, usage  |    | Alertmanager      |  |
|  +-------------------+    +-------------------+    +-------------------+  |
|                           login: Clerk or Auth0                           |
+------------|-------------------------------------------------^------------+
             | Kubernetes API:                                 |
             | namespace, node pool,        broker and client  |
             v Kafka (one template)     metrics, tenant label  |
+-- Data plane: one EKS cluster per environment, 3 AZs --------|------------+
|                                                                           |
|  strimzi namespace: Strimzi cluster operator, watching all namespaces     |
|                                                                           |
|  +-- customer-acme --------------+     +-- customer-globex ------------+  |
|  | Kafka 4.3, KRaft, RF 3        |     | Kafka 4.3, KRaft, RF 3        |  |
|  | brokers across 3 AZs          |     | brokers across 3 AZs          |  |
|  | dedicated nodes (Karpenter)   |     | dedicated nodes (Karpenter)   |  |
|  +-------------------------------+     +-------------------------------+  |
|                  ^                                     |                  |
+------------------|-------------------------------------|------------------+
                   |                                     v
        Customer applications                    S3 tiered storage
      over TLS to their own cluster             (history for replay)
```

Every customer's Kafka cluster runs in its own namespace on dedicated nodes,
inside one EKS cluster per environment. Customer applications connect
straight to their own cluster; the control plane is never in the data path.
Terraform builds the base infrastructure (VPC, EKS, S3, IAM), and the
provisioning service creates everything per customer through the Kubernetes
API from one template.

Built so far: the provisioning service stub and the Strimzi layout, which run
locally on kind (see [Local development](#local-development)). The rest is
planned; shared clusters behind a topic-rewriting proxy follow in Phase 6,
then Kafka Connect and CDC.

## Roadmap

Each phase is a GitHub milestone; see the plan for the scope of each and the
[milestones overview](https://github.com/ReneP92/changeport/milestones?direction=asc&sort=due_date)
for progress.

| Milestone | Due |
| --- | --- |
| [Phase 0: Design partner and foundations](https://github.com/ReneP92/changeport/milestone/1) | Nov 6, 2026 |
| [Phase 1: Single-tenant data plane](https://github.com/ReneP92/changeport/milestone/2) | Dec 25, 2026 |
| [Phase 2: Observability](https://github.com/ReneP92/changeport/milestone/3) | Mar 5, 2027 |
| [Phase 3: Guidance layer](https://github.com/ReneP92/changeport/milestone/4) | Apr 16, 2027 |
| [Phase 4: Replay, hardening and design-partner launch](https://github.com/ReneP92/changeport/milestone/5) | Jun 11, 2027 |
| [Phase 5: Grow to 50 customers](https://github.com/ReneP92/changeport/milestone/6) | Oct 15, 2027 |
| [Phase 6: Multi-tenancy and self-serve](https://github.com/ReneP92/changeport/milestone/7) | Dec 24, 2027 |
| [Phase 7: Managed Connect and Debezium](https://github.com/ReneP92/changeport/milestone/8) | Mar 3, 2028 |
| [Phase 8: Migration wizard](https://github.com/ReneP92/changeport/milestone/9) | Apr 14, 2028 |
| [Phase 9: CDC product](https://github.com/ReneP92/changeport/milestone/10) | Jul 14, 2028 |

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

To see what the template produces for a customer without a cluster, pick a
size profile (`dev` or `production`) and a platform (`kind`, `eks-dev` or
`eks-prod`); kind has one zone, so the three-zone production profile only
renders for EKS:

```bash
go run ./cmd/provisioner render -customer acme -profile production -platform eks-prod
```

## Checks

```bash
make lint test
```

CI runs the same Go lint and tests, plus `make tf-fmt tf-validate` and
`make shellcheck`. Run `make help` for every target.
