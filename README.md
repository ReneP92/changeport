<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/changeport-logo-dark.svg">
    <img alt="ChangePort" src="docs/brand/changeport-logo.svg" height="40">
  </picture>
</h1>

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
|                               login: Clerk                                |
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

Built so far: the provisioning service stub, the platform components and
customer clusters with public TLS listeners and SCRAM users, running locally
on kind (see [Local development](#local-development)), and the Terraform
layout for EKS, not applied yet. The rest is planned; shared clusters behind
a topic-rewriting proxy follow in Phase 6, then Kafka Connect and CDC.

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
| `cmd/console`, `internal/console` | The console: the API of `api/openapi.yaml` behind Clerk sign-in, serving the web app built into its image |
| `cmd/provisioner` | Creates and removes customer Kafka clusters: a CLI, and the controller that reconciles `Customer` resources |
| `api/openapi.yaml` | The console's API; `make generate` writes its Go server and TypeScript client from it |
| `api/v1alpha1` | The `Customer` resource (`customers.changeport.io`); `make generate` writes its CRD into `deploy/base/provisioner` |
| `internal/controller` | The `Customer` controller, which provisions, reports status and deprovisions through a finalizer |
| `internal/clusters` | The console's Kafka admin clients, one per customer cluster, connected on demand with certificates that follow Strimzi's renewals |
| `internal/metrics` | The console's named VictoriaMetrics queries, which always select the customer's tenant |
| `cmd/agent` | The Kafka agent next to every customer's cluster: turns on tiered storage for every topic that can use it |
| `internal/provisioning` | The per-customer template and the code that applies it through the Kubernetes API |
| `web` | The console's React app, built and tested with Bun (see [web/README.md](web/README.md)) |
| `terraform` | Base infrastructure per environment: VPC, EKS, S3, IAM (empty modules, nothing applied) |
| `deploy` | Kubernetes manifests: the kind cluster, and the platform components (Strimzi, cert-manager) with one overlay per environment |
| `images` | Container images we build: Strimzi's Kafka image plus the tiered storage plugin, the Kafka agent, the provisioner and the console |
| `hack` | Local development scripts behind the Makefile |
| `tools` | Go module pinning the tools (kind, golangci-lint); `make tools` builds them into `bin/` |

## Local development

Every customer gets a Kafka cluster in its own namespace, generated from one
template by the provisioning service. Locally this runs on
[kind](https://kind.sigs.k8s.io/) with the
[Strimzi](https://strimzi.io/) operator, mirroring the EKS layout.

Prerequisites: Docker (give it at least 8 GB of memory; the failover test
peaks at about 5 GB), Go 1.26 and make.
The scripts download a pinned, checksum-verified kubectl into `bin/` and the
upstream install files pinned in `deploy/upstream-files.txt` (Strimzi,
cert-manager) into `deploy/base/*/upstream/`, and build kind from
`tools/go.mod`.

```bash
make dev-up
```

This builds the Kafka broker image (`images/kafka`, Strimzi's image plus the
Aiven tiered storage plugin; the first build takes about 10 minutes) and
creates the `changeport` kind cluster on Kubernetes 1.37: a control plane and
three workers, one per zone like the three availability zones on EKS, with
[cloud-provider-kind](https://github.com/kubernetes-sigs/cloud-provider-kind)
for load balancers. It then installs the platform components from
`deploy/overlays/kind` (Strimzi 1.3.0 watching all namespaces, cert-manager
with a local CA, Strimzi Drain Cleaner, SeaweedFS as the S3 store for tiered
storage, and VictoriaMetrics for metrics), provisions the `demo` customer
(namespace `customer-demo`, a single-node KRaft cluster on Kafka 4.3.1) with
the provisioning service, and runs the smoke test. It is safe to run again. If
the cluster was created from an older `deploy/kind/cluster.yaml`, it says so;
recreate it with `make dev-down dev-up`.

Customers connect the way they will on EKS: to public names such as
`bootstrap.demo.kafka.changeport.test:9094`, over TLS with SCRAM, as their
`admin` user. On kind the names resolve to cloud-provider-kind's load
balancers on the `kind` Docker network, and the certificates come from a
local CA. The smoke test connects from a client container outside the
cluster: it creates a topic, produces a message and consumes it back, checks
that no credentials, a wrong password, plaintext and an address the
certificate does not name are refused, and checks the internal mTLS listener
the platform's agent uses.

To work with a customer's cluster yourself, open a shell with the Kafka tools
and the customer's `client.properties`:

```bash
make kafka-client CUSTOMER=demo
```

`go run ./cmd/provisioner credentials -context kind-changeport -customer demo`
prints the same `client.properties`.

Provision more customers from the same template, or run the end-to-end check,
which provisions a second customer, smoke tests both clusters and removes the
second one again, with the metrics test in between, then runs the tiered
storage and failover tests:

```bash
make provision CUSTOMER=acme
```

```bash
make e2e
```

The metrics test checks that VictoriaMetrics scrapes every customer's brokers
and that every series carries its tenant and the other labels in
[docs/metrics-labels.md](docs/metrics-labels.md):

```bash
make metrics-test
```

Kafka has no broker-wide default for tiered storage, so the Kafka agent
(`cmd/agent`) next to every customer's cluster turns it on for each new topic
that can use it, leaving compacted topics and a customer's explicit
`remote.storage.enable=false` alone. The agent test checks that:

```bash
make agent-test
```

The lag test produces messages at a known moment and checks the consumer lag
in seconds the agent reports, for a classic and a KIP-848 consumer group:

```bash
make lag-test
```

The console reaches each customer's cluster as the Kafka user
`changeport-console` on the internal mTLS listener, and its service account
may read only the four Secrets it needs in each customer namespace. The
console access test checks both from the `changeport` namespace: the console
user may create, describe and delete topics and list consumer groups, but
producing and consuming are refused:

```bash
make console-access-test
```

The tiered storage test offloads a topic with small segments to the local
object store (SeaweedFS, standing in for S3), checks that the brokers delete
their local copies, and reads every message back from offset 0:

```bash
make tiered-storage-test
```

The failover test provisions a three-zone customer with the `dev-ha` profile
(three small brokers, replication factor 3, as durable as production) and
runs an idempotent producer and a consumer throughout two disruptions: one
zone's worker freezes for a minute, and a broker is evicted, which Strimzi
Drain Cleaner turns into a rolling restart by the Strimzi operator. Every
message must arrive exactly once, and the cluster must recover with all
partitions in sync:

```bash
make failover-test
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
bin/kubectl-v1.37.1 --context kind-changeport get kafka -A
```

To see what the template produces for a customer without a cluster, pick a
size profile (`dev`, `dev-ha` or `production`) and a platform (`kind`,
`eks-dev` or `eks-prod`). On EKS, each customer also gets a NodePool and
NodeClass of their own, so their brokers run on dedicated nodes:

```bash
go run ./cmd/provisioner render -customer acme -profile production -platform eks-prod
```

Customers are Kubernetes resources: a cluster-scoped `Customer`, named by
the customer ID, with a display name, a profile, an optional Clerk
organization and deletion protection. The provisioning controller
(`provisioner controller`, in the `changeport` namespace) turns each one into
its cluster. It applies the template in order without blocking, reports how
far the cluster is in the conditions `CertificateIssued`, `KafkaReady`,
`UsersReady`, `AgentAvailable` and `Ready`, and on deletion removes the
cluster before the `Customer` goes, unless deletion protection is on.
`make provision` and `make deprovision` create and delete Customers and wait
for the controller; `render` and `credentials` work without it.

```bash
kubectl get customers
```

After changing the provisioner, rebuild it and roll it out on kind:

```bash
make provisioner-reload
```

### Console

`make dev-up` also runs the console at <http://localhost:8088>. Customers
sign in with Clerk, where each Clerk organization is a customer and members
of the "Changeport staff" organization are operators. Give it the keys of
your Clerk development instance in a `.env` file, which git ignores:

```bash
cp .env.sample .env  # then fill it in
make console-config
```

Without them the console fails closed: it says it is not configured and
refuses every API call, which is how CI runs it. The console only accepts
session tokens of that Clerk instance, issued to its own origins.

To work on the web app with hot reload, run the Vite dev server, which sends
`/api` to the console on kind, and open <http://localhost:5173>. After
changing the console's Go code, rebuild it and roll it out:

```bash
make web-dev
```

```bash
make console-reload
```

The console test checks it as a browser reaches it: the web app and its
Content Security Policy, an API that refuses requests without a valid
session, and in Chromium either the "not configured" page or Clerk's sign-in
form:

```bash
make console-test
```

The signed-in tests use the Clerk instance of `.env`. A setup creates this
run's throwaway `+clerk_test` users, two customer organizations with a
customer each, and a staff member; the tests sign in through the form and
with Clerk's testing helpers, and check in the browser and the API that a
customer sees only their own cluster and never the operator console. A
teardown removes the run's users, organizations and customers, and the next
run removes what a crashed run left behind after an hour:

```bash
make ui-e2e
```

## Checks

```bash
make lint test
make web-check web-e2e
make check-text
```

The controller's tests run against a real API server with the Changeport,
Strimzi and cert-manager CRDs (envtest), which `make test` downloads into
`bin/`. After changing `api/`, run `make generate`; CI fails on stale
generated code.

CI runs the same Go lint and tests, the web app's checks and Playwright
tests, `make tf-fmt tf-validate`, `make shellcheck`, and `make check-text`,
which fails on any em dash in the repository, and on every pull request the
whole end-to-end suite on a fresh kind cluster: `make dev-up e2e
platform-check`, where `platform-check` also server-side dry-runs the EKS
overlays. When that job fails, it uploads
the output of `hack/diagnostics.sh` (pods, events, resources and logs) as the
`e2e-diagnostics` artifact. A second kind job runs `make ui-e2e` against the
Clerk development instance, with the secret `CLERK_SECRET_KEY` and the
variables `CLERK_PUBLISHABLE_KEY` and `CHANGEPORT_STAFF_ORG_ID`; where the
secret is not available, such as in pull requests from forks, it skips with
a notice. CI also builds the Kafka, agent, provisioner and console images
for amd64 and arm64 and runs `make kafka-image-test`, which starts a broker
from the Kafka image with tiered storage configured. Run
`make help` for every target.
