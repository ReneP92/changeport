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
|  | dedicated nodes (Auto Mode)   |     | dedicated nodes (Auto Mode)   |  |
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

More on the data plane:

- [docs/architecture/data-plane.md](docs/architecture/data-plane.md): how the customers' clusters run, as built.
- [docs/connecting.md](docs/connecting.md): the customers' guide to connecting, with examples in Java, Python, Go and Node.js.
- [docs/runbook.md](docs/runbook.md): the operators' procedures, from provisioning to upgrades.

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
| `api/openapi.yaml` | The console's API; `just generate` writes its Go server and TypeScript client from it |
| `api/v1alpha1` | The `Customer` resource (`customers.changeport.io`); `just generate` writes its CRD into `deploy/base/provisioner` |
| `internal/controller` | The `Customer` controller, which provisions, reports status and deprovisions through a finalizer |
| `internal/clusters` | The console's Kafka admin clients, one per customer cluster, connected on demand with certificates that follow Strimzi's renewals |
| `internal/metrics` | The console's named VictoriaMetrics queries, which always select the customer's tenant |
| `cmd/agent` | The Kafka agent next to every customer's cluster: turns on tiered storage for every topic that can use it, keeps the client metrics subscription in place and measures consumer lag in seconds |
| `internal/provisioning` | The per-customer template and the code that applies it through the Kubernetes API |
| `web` | The console's React app, built and tested with Bun (see [web/README.md](web/README.md)) |
| `terraform` | Base infrastructure per environment: VPC, EKS, S3, IAM, and ECR in prod (modules written; nothing applied yet; see [terraform/README.md](terraform/README.md)) |
| `deploy` | Kubernetes manifests: the kind cluster, and the platform components (Strimzi, cert-manager) with one overlay per environment |
| `images` | Container images we build: Strimzi's Kafka image plus the tiered storage plugin and client telemetry, the Kafka agent, the provisioner and the console. `.github/workflows/publish-images.yml` publishes them to ECR from main, and `cmd/eksimages` sets the digests the EKS platforms run |
| `hack` | Local development scripts behind the justfile |
| `tools` | Go module pinning the tools (kind, golangci-lint); `just tools` builds them into `bin/` |

## Local development

Every customer gets a Kafka cluster in its own namespace, generated from one
template by the provisioning service. Locally this runs on
[kind](https://kind.sigs.k8s.io/) with the
[Strimzi](https://strimzi.io/) operator, mirroring the EKS layout.

Prerequisites: Docker (give it at least 12 GB of memory: with 8 GB the full
end-to-end suite runs out, as the e2e customers' brokers come and go next to
the demo's), Go 1.26 and [just](https://just.systems/) (`brew install just`),
which runs every task here; `just` alone lists them.
The scripts download a pinned, checksum-verified kubectl into `bin/` and the
upstream install files pinned in `deploy/upstream-files.txt` (Strimzi,
cert-manager) into `deploy/base/*/upstream/`, and build kind from
`tools/go.mod`.

```bash
just dev-up
```

This builds the Kafka broker image (`images/kafka`, Strimzi's image plus the
Aiven tiered storage plugin and Strimzi's metrics reporter with client
telemetry; the first build takes about 10 minutes) and
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
recreate it with `just dev-down dev-up`.

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
just kafka-client demo
```

`go run ./cmd/provisioner credentials -context kind-changeport -customer demo`
prints the same `client.properties`.

Provision more customers from the same template, or run the end-to-end
check. Its core suite provisions a second customer, protected from deletion,
smoke tests both clusters, runs the metrics, deletion protection, agent, lag,
client telemetry, console, tiered storage, host access and connection guide
tests, and removes the second customer again; its failover suite freezes a zone and drains a
broker of a three-zone customer. CI runs the two on separate clusters at once;
locally `just e2e` runs both, one after the other:

```bash
just provision acme
```

```bash
just e2e
```

To run tests quickly while working on one part, run one suite, or one test
against the running cluster, such as the lag test; most tests have a
recipe of their own (`just` lists them). For the web app, `just web-dev`
serves it with hot reload against the console on kind:

```bash
just e2e core
```

```bash
just lag-test
```

The metrics test checks that VictoriaMetrics scrapes every customer's brokers
and that every series carries its tenant and the other labels in
[docs/metrics-labels.md](docs/metrics-labels.md):

```bash
just metrics-test
```

Kafka has no broker-wide default for tiered storage, so the Kafka agent
(`cmd/agent`) next to every customer's cluster turns it on for each new topic
that can use it, leaving compacted topics and a customer's explicit
`remote.storage.enable=false` alone. The agent test checks that:

```bash
just agent-test
```

The lag test produces messages at a known moment and checks the consumer lag
in seconds the agent reports, for a classic and a KIP-848 consumer group:

```bash
just lag-test
```

Clients push their own metrics to the brokers (KIP-714), and the agent keeps
the subscription that says which ones in place. The client telemetry test
runs a Java producer as the demo customer's admin and checks that its counts
reach VictoriaMetrics with the tenant, its Kafka user and client ID
(docs/metrics-labels.md):

```bash
just client-telemetry-test
```

The console reaches each customer's cluster as the Kafka user
`changeport-console` on the internal mTLS listener, and its service account
may read only the four Secrets it needs in each customer namespace. The
console access test checks both from the `changeport` namespace: the console
user may create, describe and delete topics and list consumer groups, but
producing and consuming are refused:

```bash
just console-access-test
```

The tiered storage test offloads a topic with small segments to the local
object store (SeaweedFS, standing in for S3), checks that the brokers delete
their local copies, and reads every message back from offset 0:

```bash
just tiered-storage-test
```

The failover test provisions a three-zone customer with the `dev-ha` profile
(three small brokers, replication factor 3, as durable as production) and
runs an idempotent producer and a consumer throughout two disruptions: one
zone's worker freezes for a minute, and a broker is evicted, which Strimzi
Drain Cleaner turns into a rolling restart by the Strimzi operator. Every
message must arrive exactly once, and the cluster must recover with all
partitions in sync:

```bash
just failover-test
```

Other targets:

```bash
just smoke-test acme
```

```bash
just deprovision acme
```

```bash
just dev-down
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
`just provision` and `just deprovision` create and delete Customers and wait
for the controller; `render` and `credentials` work without it.

```bash
kubectl get customers
```

Deletion protection keeps a customer's data from going by mistake. The API
server refuses deleting a protected `Customer` or its namespace, and
`provisioner provision` protects new production customers unless told
otherwise (`-deletion-protection=false`). Turning protection off takes the
customer ID typed again. Deprovisioned customers' volumes and tiered storage
are kept for 30 days before the purge; [docs/runbook.md](docs/runbook.md)
has the procedures:

```bash
go run ./cmd/provisioner unprotect -customer acme -confirm acme
```

Docker Desktop lists the local setup's containers without grouping them, as
kind and cloud-provider-kind create theirs without a Compose project. This
lists them by role: the cluster's nodes, the customers' load balancers (one
per broker and one per bootstrap address), and the helpers:

```bash
just ps
```

After changing the provisioner, rebuild it and roll it out on kind:

```bash
just provisioner-reload
```

### Console

`just dev-up` also runs the console at <http://localhost:8088>. Customers
sign in with Clerk, where each Clerk organization is a customer and members
of the "Changeport staff" organization are operators. Give it the keys of
your Clerk development instance in a `.env` file, which git ignores:

```bash
cp .env.sample .env  # then fill it in
just console-config
```

In the Clerk dashboard, turn off **Allow user-created organizations**
(Organizations, Settings), in every instance: organizations are customers,
which only operators create, in the operator console. Otherwise Clerk offers
every user to create one in the organization switcher. The signed-in tests
check it.

With the keys there, `just dev-up` also sets up your own test user,
`<you>+clerk_test@example.com`, which Clerk's development instances verify
with the code 424242. It is an admin of the staff organization, so it lands
on the operator console, and of an organization of its own, "Demo (<you>)",
linked to the demo customer, so the organization switcher takes it to the
demo's overview, topics and consumer groups too. It creates only what is
missing; `<you>` is `CHANGEPORT_DEV_USER` in `.env`, or else the start of
your git email address. The signed-in tests' cleanup only removes their own
`e2e...` users and organizations, so yours stay. To set it up again without
the rest of dev-up:

```bash
just dev-user
```

Without the keys the console fails closed: it says it is not configured and
refuses every API call, which is how CI runs it. The console only accepts
session tokens of that Clerk instance, issued to its own origins.

To work on the web app with hot reload, run the Vite dev server, which sends
`/api` to the console on kind, and open <http://localhost:5173>. After
changing the console's Go code, rebuild it and roll it out:

```bash
just web-dev
```

```bash
just console-reload
```

A customer whose cluster is ready sees its overview: how to connect, with a
`client.properties` download for the organization's admins (it holds the
admin user's password), the brokers' health and the partitions' from the
metrics store, the cluster's throughput over the last hour to week, and the
topics with their retention, tiering, size and throughput, and how far
behind each consumer group is, in plain language ("3 minutes behind"), with
its lag over the last hour as the agent measures it. The console reads
the topics as the `changeport-console` Kafka user, which cannot read or write
messages.

The organization's admins also create and delete topics there, within the
guardrails of `internal/topics`, which the console serves to the web app at
`/api/customer/topic-policy`: a name that leaves the platform's prefixes
alone, partitions up to the profile's cap, a retention preset, and old
messages deleted (tiered from the start) or compacted. Deleting a topic takes
its name typed and names the consumer groups still reading it. The console
logs every change it makes for a user as an `audit=true` line with the Clerk
user and organization:

```bash
kubectl -n changeport logs deployment/changeport-console | grep audit=true
```

Operators, in the staff organization, see every customer at
<http://localhost:8088/operator> with how far its cluster is. To provision
one, they name it and choose a profile and whether deletion protection is
on; the console creates a Clerk organization of that name, invites its first
admin if asked, and creates a Customer with a new opaque ID such as
`c-7f3k2q`, whose page then follows the cluster getting ready. Deprovisioning
takes the customer ID typed and is refused while deletion protection is on;
the Clerk organization is kept, no longer linked to a cluster. The console
may create and delete Customers but not change them, so it cannot turn
deletion protection off.

Applications on your machine connect to a customer's cluster with the
`client.properties` the console hands out, unchanged, as they would to
Changeport in the cloud. `just dev-up` starts a host gateway: a container on
the kind network with Kafka's port 9094 published on `127.0.0.1`, which
reads the name each client asks for in its TLS handshake and forwards the
connection, still encrypted end to end, to that broker's load balancer. It
also answers DNS for `*.kafka.changeport.test` with `127.0.0.1`. Point your
machine's DNS at it once (on macOS a resolver for `changeport.test`, which
covers every customer to come; on Linux the current customers' names in
`/etc/hosts`); it asks for your password:

```bash
just host-dns
```

The host access test produces and consumes as an application on your
machine would:

```bash
just host-access-test demo
```

Port 9094 of `127.0.0.1` must be free for the gateway, so stop any local
Kafka first.

The Kafka command-line tools also run from the kind network, in a container,
with a customer's `client.properties`:

```bash
hack/kafka-with.sh acme acme-client.properties kafka-topics.sh --list \
  --bootstrap-server bootstrap.acme.kafka.changeport.test:9094 --command-config /config/client.properties
```

The console test checks it as a browser reaches it: the web app and its
Content Security Policy, an API that refuses requests without a valid
session, and in Chromium either the "not configured" page or Clerk's sign-in
form:

```bash
just console-test
```

The signed-in tests use the Clerk instance of `.env`. A setup creates this
run's throwaway `+clerk_test` users, two customer organizations with a
customer each, and a staff member; the tests sign in through the form and
with Clerk's testing helpers, and check in the browser and the API that a
customer sees only their own cluster and never the operator console. On the
overview, an admin downloads `client.properties` and produces with it from
the kind network, and a member sees how to connect but not the password. An
admin creates a topic keeping an hour of messages, checks with the Kafka
tools that it is tiered, reads it with a consumer group, and deletes it
after the warning about that group; a member cannot change topics. A
consumer group left 100 messages behind shows as minutes behind, with its
lag over the last hour, next to one that is caught up. The staff member
provisions a customer with an invitation, waits for it to get ready, and
deprovisions it, and finds a protected customer refused. A
teardown removes the run's users, organizations and customers, and the next
run removes what a crashed run left behind after an hour:

```bash
just ui-e2e
```

The same setup also runs a tour of every screen on a desktop and a phone,
which provisions Acme Corp through the operator console, fills its charts
with a few minutes of traffic, and deprovisions it again. It writes the
screenshots that [docs/console.md](docs/console.md) shows:

```bash
just ui-screenshots
```

## Checks

```bash
just lint test
just web-check web-e2e
just check-text
```

The controller's tests run against a real API server with the Changeport,
Strimzi and cert-manager CRDs (envtest), which `just test` downloads into
`bin/`. After changing `api/`, run `just generate`; CI fails on stale
generated code.

CI runs the same Go lint and tests, the web app's checks and Playwright
tests, `just tf-fmt tf-validate tf-test`, `just shellcheck`, and `just check-text`,
which fails on any em dash in the repository, and on every pull request the
whole end-to-end suite on a fresh kind cluster: `just dev-up e2e
platform-check`, where `platform-check` also server-side dry-runs the EKS
overlays. When that job fails, it uploads
the output of `hack/diagnostics.sh` (pods, events, resources and logs) as the
`e2e-diagnostics` artifact. A second kind job runs `just ui-e2e` against the
Clerk development instance, with the secret `CLERK_SECRET_KEY` and the
variables `CLERK_PUBLISHABLE_KEY` and `CHANGEPORT_STAFF_ORG_ID`; where the
secret is not available, such as in pull requests from forks, it skips with
a notice. CI also builds the Kafka, agent, provisioner and console images
for amd64 and arm64 and runs `just kafka-image-test`, which starts a broker
from the Kafka image with tiered storage configured. Run
`just` for every recipe.
