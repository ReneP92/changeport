# Data plane

How the customers' Kafka clusters run, as built. The decisions behind it are
in [PROJECT_PLAN.md](../PROJECT_PLAN.md); operating it is in
[runbook.md](../runbook.md); connecting to it is in
[connecting.md](../connecting.md).

## One environment

Each environment (dev, prod) is one AWS account with one EKS cluster in Auto
Mode, in us-east-1 across three availability zones (`use1-az1`, `use1-az2`,
`use1-az4`), built by Terraform ([terraform/README.md](../../terraform/README.md)):

- a VPC with a private subnet per zone for nodes and pods, a public subnet
  per zone for NAT gateways and load balancers, and an S3 gateway endpoint;
- the EKS cluster, whose built-in node pools run the platform components;
- the tiered storage bucket `changeport-<env>-tiered-storage` and the
  brokers' role `changeport-<env>-kafka-broker`;
- in prod, the ECR registry of the platform's own images.

The platform components come from `deploy/overlays/eks-<env>`:

- the Strimzi cluster operator, watching every namespace;
- cert-manager;
- Strimzi Drain Cleaner;
- VictoriaMetrics;
- the provisioner, whose controller turns each `Customer` resource into a
  Kafka cluster; and
- the console.

On a laptop, kind stands in for all of it: three workers in three zones,
SeaweedFS for S3 and a local CA for Let's Encrypt (`just dev-up`).

## One customer

Every customer has a Kafka cluster of its own, in the namespace
`customer-<id>`, on nodes of its own. Everything in it comes from one
template, `internal/provisioning/templates/customer-cluster.yaml.tmpl`, which
the provisioner renders for the customer's profile and the environment's
platform (`internal/provisioning/spec.go`) and applies with server-side
apply. It re-applies the template on every change and every ten minutes, so
template changes reach every customer.

| Resource | Purpose |
| --- | --- |
| `Namespace` `customer-<id>` | Holds everything else; labelled with the tenant, and with deletion protection when the customer has it |
| `Certificate` `external-listener` | The public listener's certificate for `*.<id>.<Kafka domain>`, from cert-manager |
| `KafkaNodePool` `dual-role` | The brokers, each also a KRaft controller, with a persistent volume each |
| `Kafka` `<id>` | The cluster: listeners, tiered storage, metrics, and Strimzi's topic and user operators |
| `KafkaUser` `admin` | The customer's user (SCRAM-SHA-512), with full access to their topics, consumer groups and transactions |
| `KafkaUser` `changeport-agent`, `changeport-console` | The platform's users (mTLS), which cannot read or write messages |
| `Deployment` `changeport-agent` | The Kafka agent: turns on tiered storage for new topics, sets up client telemetry, measures consumer lag in seconds |
| `Role` `changeport-console` | Lets the console read the few Secrets it needs, and the pods |
| `NodeClass`, `NodePool` `customer-<id>` (EKS) | The customer's dedicated nodes: arm64 (m7g, m8g), on demand, tainted for the customer's pods only |

### Profiles

| Profile | Brokers | Zones | Copies (in sync) | Broker resources | Disk |
| --- | --- | --- | --- | --- | --- |
| `dev` | 1 | 1 | 1 (1) | 0.25 CPU, 1 GiB | 5 GiB |
| `dev-ha` | 3 | 3 | 3 (2) | 0.25 CPU, 1 GiB | 5 GiB |
| `production` | 3 | 3 | 3 (2) | 1.5 CPU, 5 GiB | 100 GiB |

With three zones, the brokers spread one per zone and one per node. Each
broker's rack is its zone ID, so every partition has a copy in each zone.
Consumers that set `client.rack` read from the copy in their own zone.

## Listeners

| Listener | Port | Who | Authentication |
| --- | --- | --- | --- |
| `external` | 9094 | Customers' applications, over the internet | TLS with SCRAM-SHA-512 |
| `tls` | 9093 | The platform: the agent and the console, inside the EKS cluster | Mutual TLS |

The external listener has a Network Load Balancer for the bootstrap address
and one per broker:
- **Names:** `bootstrap.<id>.<Kafka domain>` and `b<n>.<id>.<Kafka domain>`,
  published in Route 53 by external-dns.
- **Certificate:** from Let's Encrypt, so clients need no CA configuration.
- **Load balancers:** internet-facing, sending traffic straight to the broker
  pods. Their idle timeout outlasts the brokers' own, so brokers close idle
  connections rather than the load balancers.

Topics are only created on purpose (`auto.create.topics.enable=false`), and
Kafka's ACLs authorize every request.

## Storage

- **On the brokers:** a persistent volume per broker, gp3 on EKS, kept when
  the customer is deprovisioned, so data survives a mistake for 30 days.
  Each topic keeps a day there.
- **Tiered storage:** finished log segments go to S3 through the Aiven
  remote storage manager in the Changeport Kafka image (`images/kafka`).
  They land under the customer's prefix `customer-<id>/` in the
  environment's bucket, and the agent turns tiered storage on for every new
  topic that can use it.
- **Bucket access:** the brokers authenticate with EKS Pod Identity. The
  broker role's policy limits each customer's brokers to their own prefix,
  by the namespace their session is tagged with.

## Metrics

Each broker's Strimzi metrics reporter serves Prometheus metrics on port
9404: an allow-listed set of the broker's own, and the metrics its clients
push (KIP-714). VictoriaMetrics scrapes them with the customer's `tenant`
label, in separate jobs for the broker's and the clients' series, and scrapes
the agent ([metrics-labels.md](../metrics-labels.md)).

## Images

The platform builds four images: the Kafka broker image (Strimzi's, plus the
tiered storage plugin and the KIP-714 metrics reporter), the agent, the
provisioner and the console. `.github/workflows/publish-images.yml`
publishes them to ECR in prod for amd64 and arm64, and the EKS platforms run
them by digest from `internal/provisioning/eks-images.json`, which changes
only through reviewed pull requests.

## Not built yet

- **External-dns and the Let's Encrypt ClusterIssuers** arrive with the domain
  and the EKS environments (#8, #28).
- **NetworkPolicies:** none yet. Customers' clusters are isolated by
  namespaces, dedicated nodes, their own credentials and Kafka's ACLs.
