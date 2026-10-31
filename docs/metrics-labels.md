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

Client telemetry (KIP-714, below) adds the labels of the client a series
comes from, next to `tenant`, `cluster` and `env`:

| Label | Meaning | Source |
| --- | --- | --- |
| `principal` | The Kafka user a client connects as, such as `User:admin` | The broker, as the Kafka agent configures the reporter |
| `client_id` | The `client.id` an application sets | The broker, as the Kafka agent configures the reporter |
| `client_instance_id` | The KIP-714 ID of one client instance, new on every start | The reporter, always |
| `topic`, `partition` | The topic and partition, on a client's per-topic metrics | The client's own attributes |
| `consumer_group` | The consumer group, on the agent's lag metrics | The Kafka agent |

The client library and version (`client_software_name` and
`client_software_version`) are part of KIP-714 but not among the labels the
reporter can add.

No label may take unbounded values: never message keys, offsets, client IP
addresses or timestamps.

The Kafka agent next to each cluster exports its own metrics
(`changeport_agent_*`) with `tenant`, `cluster` and `instance`, and each
consumer group's lag with `consumer_group` and `topic` added:

| Metric | Meaning |
| --- | --- |
| `changeport_consumergroup_lag_seconds` | Age of the oldest message the group has not consumed yet, the highest over the topic's partitions |
| `changeport_consumergroup_lag_messages` | Messages the group has not consumed yet, summed over the topic's partitions |
| `changeport_consumergroup_lag_capped` | 1 when the lag is older than 23 hours, so `lag_seconds` is a lower bound |

The agent measures lag every 30 seconds without reading any message. It
reads each group's committed offsets and the partitions' end offsets, looks
up the offsets at fixed times in Kafka's time index (15 seconds to 23 hours
ago), and interpolates the age of the oldest unconsumed message between the
two times around it. The estimate is exact under steady traffic and off by
at most the gap between those two times otherwise: 15 seconds below a
minute, half an hour around an hour and a half. It depends on producers'
clocks, since it uses the time the producer gave each message. Classic and
KIP-848 consumer groups are both covered; groups that keep their offsets
outside Kafka are not. When a lookup fails, for example while a topic
switches to tiered storage, the group keeps its last value instead of
reporting a wrong one.

## Postgres CDC sources

A customer's Postgres sources (docs/cdc.md) have series from two places,
each with `tenant`, `cluster`, `env` and `source`, the source's name. The
alerting rules in `deploy/base/victoria-metrics/alerts` watch them.

The source controller, in the provisioner, reads each source's replication
slot in the customer's database every minute and exports what it found; the
`changeport-provisioner` job keeps the series' own `tenant` and `source`
labels, and copies `tenant` into `cluster`:

| Metric | Meaning |
| --- | --- |
| `changeport_source_retained_wal_bytes` | WAL the slot keeps in the database, which grows while the source is paused, failed or behind |
| `changeport_source_slot_safe_wal_bytes` | How much more WAL the slot may keep before the database invalidates it; absent without `max_slot_wal_keep_size` |
| `changeport_source_slot_lost` | 1 once the database has invalidated the slot (`wal_status` lost) |
| `changeport_source_slot_checked_timestamp_seconds` | When the controller last read the slot; it stops moving while the database cannot be reached |
| `changeport_source_phase` | 1 for the source's phase (`Pending`, `Snapshotting`, `Streaming`, `Stopped`, `Failed` or `Deleting`), 0 for the others |

The Kafka Connect worker of a customer with sources exports Debezium's and
Connect's metrics through its JMX exporter on port 9404, which the template
configures; the `kafka-connect` job scrapes it. Only these, besides the
JVM's own:

| Metric | Meaning |
| --- | --- |
| `debezium_streaming_millisecondsbehindsource` | How long ago the last change the source read happened in the database |
| `debezium_streaming_millisecondssincelastevent` | How long ago the source read its last change |
| `debezium_streaming_connected` | 1 while the source is connected to the database's replication stream |
| `debezium_streaming_totalnumberofeventsseen` | Changes the source has read since its connector started |
| `debezium_streaming_numberofcommittedtransactions` | Transactions the source has read since its connector started |
| `debezium_streaming_queueremainingcapacity`, `debezium_streaming_queuetotalcapacity` | How full the queue between reading and writing is |
| `kafka_connect_connector_status`, `kafka_connect_task_status` | 1 for the connector's and its task's state, in `status`: running, paused, stopped, failed or unassigned |
| `kafka_connect_source_record_poll_total`, `kafka_connect_source_record_write_total` | Records the task has read from Debezium and written to Kafka |
| `kafka_connect_task_record_errors_total`, `kafka_connect_task_record_failures_total` | Records the task could not handle |

## Client telemetry

Kafka clients push their own metrics to a broker (KIP-714): Java clients
from Kafka 3.7 on do by default, librdkafka-based clients from 2.5 on. The
brokers run Strimzi's metrics reporter built with KIP-714 support
(`images/kafka`), which exposes what clients push as `clients_*` series on
the same port 9404, such as
`clients_org_apache_kafka_producer_record_send_total`. Customers do not
change their code or configuration.

The reporter labels each client's series with its client ID and Kafka user
(`prometheus.metrics.reporter.client.telemetry.labels`). Strimzi forbids
that option in the Kafka resource, so the Kafka agent sets it as a
cluster-wide dynamic broker config, which the reporter applies without a
restart; client addresses, which it could add too, are left out as
unbounded.

Clients push only what a client metrics subscription asks for. The Kafka
agent keeps the platform's, named `changeport`, in place: about 30 metrics,
by full name, in `internal/agent/telemetry.go`, pushed every minute
(every 5 seconds on kind, for the tests). They are KIP-714's standard
metrics, which every client library reports, and the Java clients' counts of
records, errors and bytes: per producer 13 series and 3 per topic it writes
to, per consumer 15 series and 2 per topic it reads. Java clients have about
120 metrics; the per-broker ones (`node.*`) are left out, as there are as
many of them as brokers and their `node_id` attribute clashes with the
broker's own label.

A client pushes to one broker at a time, so its series carry that broker's
`node_id`, `zone` and `instance`. When it moves to another broker, both
expose its series for up to a minute, the time a broker keeps a client's
last push. Queries over clients therefore take the largest value per client
instance before adding up, for example records sent per application:

```promql
sum by (client_id) (max by (client_id, client_instance_id) (
  rate(clients_org_apache_kafka_producer_record_send_total{tenant="acme"}[5m])))
```

Java producers of Kafka 4.3 skip the final push KIP-714 asks for when a
client closes, so what a producer does in its last push interval before it
closes is not reported, and one that lives shorter than an interval
reports nothing.

The reporter's allow list does not apply to client series; the subscription
bounds how many series each client sends, and the scrape job below bounds how
many clients count.

## Console queries

The console never sends PromQL from the browser. It asks for named queries
(`internal/metrics`): bytes in and out and messages in per topic, each
topic's size on the brokers and in tiered storage, each consumer group's
lag in seconds and messages and whether it is capped, and the cluster's health: under-replicated,
under-min-ISR and offline partitions, active controllers, and active and
fenced brokers, summed over the brokers or, for the controller's counts,
taken from the active controller. The server writes the PromQL and always
selects the signed-in customer's `tenant`, with any topic or consumer group
quoted as a label value. The end-to-end test runs every named query against
the local VictoriaMetrics.

## Relabel rules

`deploy/base/victoria-metrics/scrape.yaml` scrapes port 9404 of every pod
that Strimzi labels as a Kafka node and that carries `changeport.io/tenant`,
in two jobs: `kafka` takes the broker's own series and `kafka-clients` the
`clients_*` series of client telemetry.
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

Consumer lag adds 3 series per consumer group and topic it reads, capped at
2,000 pairs per cluster: 20 groups reading 3 topics each add 180 series.

Fifty such clusters come to about **80,000** active series. At the 30-second
scrape interval that is about 2,700 samples per second, well below the one
million samples per second VictoriaMetrics recommends single-node for, and
about 80 MB of memory for the active series. Customers ten times larger
would still fit comfortably; the first limit to watch is the number of
partitions, since every partition replica is a series.

`hack/metrics-test.sh` fails when a broker in the end-to-end test sends more
than 1,000 series, which catches an allow list that lets too much through.

Client telemetry depends on how many clients a customer runs and how often
they restart, since every start brings a new `client_instance_id`. The
design partner's assumed 30 client instances come to about 750 series per
cluster. The `kafka-clients` scrape job takes at most **2,000** unique series
per broker in 24 hours (`series_limit`); past that, VictoriaMetrics drops new
client series, the broker's own series are unaffected, and
`scrape_series_limit_samples_dropped` counts what was dropped. Alert on
`scrape_series_current / scrape_series_limit > 0.9` before customers reach
it. At the limit, 50 three-broker clusters would add 300,000 series; at the
expected numbers, about 40,000.
