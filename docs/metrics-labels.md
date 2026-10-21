# Metrics labels

Every metric Changeport stores carries the labels below, from the first
customer on, so that observability, alerting and usage metering can slice by
customer, and so that shared clusters in Phase 6 add tenants without
relabeling anything. The store is VictoriaMetrics single-node
(`deploy/base/victoria-metrics`), which scrapes the brokers itself.

## Labels

| Label | Meaning | Source |
| --- | --- | --- |
| `tenant` | The customer ID, such as `acme` or `c-7f3k2q` | Pod label `changeport.io/tenant`, which the provisioner puts on the Kafka resource and Strimzi copies to the pods |
| `cluster` | The Kafka cluster (the Strimzi `Kafka` resource) | Pod label `strimzi.io/cluster` |
| `env` | The environment: `kind`, `dev` or `prod` | The overlay sets it on the metrics store (`CHANGEPORT_ENV`) |
| `node_id` | The Kafka node ID of the broker or controller | The number at the end of the pod name |
| `zone` | The availability zone of the broker | Pod label `topology.kubernetes.io/zone` |
| `instance` | The pod, by name rather than address, so that restarts keep it | The pod name |
| `topic` | The topic, on per-topic metrics | The Strimzi Metrics Reporter |
| `partition` | The partition, only on per-partition metrics (log size) | The Strimzi Metrics Reporter |
| `listener`, `request`, `networkProcessor` | The broker listener, API request type and network thread | The Strimzi Metrics Reporter |

On dedicated clusters `cluster` and `tenant` hold the same ID. They are
separate labels because a shared cluster in Phase 6 serves many tenants:
there `cluster` names the shared cluster, and the tenant of each series comes
from its topic prefix or principal.

Labels that arrive in Phase 2 with KIP-714 client telemetry and the Kafka
agent, and that must carry `tenant`, `cluster` and `env` as well:

| Label | Meaning |
| --- | --- |
| `principal` | The Kafka user a client connects as |
| `client_id` | The `client.id` an application sets |
| `client_instance_id` | The KIP-714 ID of one client instance |
| `client_software_name`, `client_software_version` | The client library and version, from KIP-714 |
| `consumer_group` | The consumer group, on lag and group metrics |

No label may take unbounded values: never message keys, offsets, client IP
addresses or timestamps.

## Relabel rules

`deploy/base/victoria-metrics/scrape.yaml` scrapes port 9404 of every pod
that Strimzi labels as a Kafka node and that carries `changeport.io/tenant`.
A pod without that label is not scraped at all, so no Kafka series can exist
without a tenant; `hack/metrics-test.sh` checks this in every end-to-end run.
The rules then:

- copy `changeport.io/tenant` into `tenant`, `strimzi.io/cluster` into
  `cluster` and `topology.kubernetes.io/zone` into `zone`;
- take `node_id` from the end of the pod name, and set `instance` to the pod
  name;
- add `env` to every series;
- drop the JVM and process metrics the reporter always exports, except heap
  memory, garbage collection, threads, CPU and open files;
- keep request latency only for produce and fetch requests.

## Allow list

The Strimzi Metrics Reporter on each broker only exports what the allow list
in the provisioning template
(`internal/provisioning/templates/customer-cluster.yaml.tmpl`) names. Its
patterns match the metric name before Prometheus appends `_total` to
counters. They cover:

- **Health:** under-replicated, under-min-ISR and offline partitions, ISR
  shrinks and expands, leader and partition counts, the active controller,
  fenced brokers, offline log directories and the broker state.
- **Usage per topic:** bytes in and out, messages in, produce and fetch
  requests and failures, and tiered storage copies, fetches, lag and size,
  plus the log size of each partition on the broker.
- **Load:** request rates by API, produce and fetch latency, network and
  request handler idle time, connections per listener and consumer group
  counts.
- **JVM and process:** heap memory, garbage collection, threads, CPU and
  open files.

Without an allow list an idle single-broker cluster exports about 8,500
series, most of them latency histograms for every request type; with it,
about 240, of which 100 are the log sizes of the internal topics.

## Cardinality

Measured on kind with Kafka 4.3.1, a broker sends:

- about **135** series that do not depend on topics;
- **8** series per topic with traffic on that broker;
- **1** series per partition replica it holds.

The internal topics alone add 100 partitions (`__consumer_offsets` and
`__remote_log_metadata`, 50 each).

The design partner's cluster, with 3 brokers and an assumed 20 topics of 6
partitions at replication factor 3, holds (20 × 6 + 100) × 3 = 660 partition
replicas, 220 per broker. Each broker then sends about 135 + 220 + 22 × 8 ≈
530 series, and the cluster about **1,600**.

Fifty such clusters come to about **80,000** active series. At the 30-second
scrape interval that is about 2,700 samples per second, well below the one
million samples per second VictoriaMetrics recommends single-node for, and
about 80 MB of memory for the active series. Customers ten times larger
would still fit comfortably; the first limit to watch is the number of
partitions, since every partition replica is a series.

`hack/metrics-test.sh` fails when a broker in the end-to-end test sends more
than 1,000 series, which catches an allow list that lets too much through.
