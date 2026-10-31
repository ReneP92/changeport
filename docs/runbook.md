# Runbook

Procedures for operators of the Changeport platform. Every command runs
against the platform's Kubernetes cluster with the pinned kubectl and the
provisioner CLI (`go run ./cmd/provisioner`), as an operator with cluster
access. On EKS, sign in with `aws sso login` and use the context
`changeport-<env>` (`aws eks update-kubeconfig --name changeport-<env>
--alias changeport-<env>`); pass `-context changeport-prod` to the provisioner
CLI, or make it the current context.

Customer `c-7f3k2q` stands for any customer. Its Kafka cluster is named after
it and runs in the namespace `customer-c-7f3k2q`, with brokers
`c-7f3k2q-dual-role-<n>`.

- [Where to look](#where-to-look)
- [Provisioning a customer](#provisioning-a-customer)
- [Deletion protection](#deletion-protection)
- [Deprovisioning a customer](#deprovisioning-a-customer)
- [Purging a deprovisioned customer's data](#purging-a-deprovisioned-customers-data)
- [A broker is down](#a-broker-is-down)
- [An availability zone is down](#an-availability-zone-is-down)
- [Replacing a node](#replacing-a-node)
- [Certificates](#certificates)
- [Rotating a customer's password](#rotating-a-customers-password)
- [Upgrading Strimzi](#upgrading-strimzi)
- [Upgrading Kafka](#upgrading-kafka)
- [A Postgres source fails](#a-postgres-source-fails)
- [A source's database is unreachable](#a-sources-database-is-unreachable)
- [A replication slot left behind](#a-replication-slot-left-behind)
- [A lost replication slot](#a-lost-replication-slot)

## Where to look

- **The operator console** (`/operator`) lists every customer with a dot per
  condition of its `Customer`: `CertificateIssued`, `KafkaReady`,
  `UsersReady`, `AgentAvailable` and `Ready`. A customer's page shows why a
  condition is not met.
- **The `Customer` resource** says the same from the command line:

  ```bash
  kubectl get customers
  kubectl get customer c-7f3k2q -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}'
  ```

- **The customer's cluster:** `kubectl -n customer-c-7f3k2q get kafka,kafkanodepool,kafkauser,pods,pvc`,
  and the brokers' logs with `kubectl -n customer-c-7f3k2q logs c-7f3k2q-dual-role-0`.
- **Metrics** in VictoriaMetrics, by the customer's `tenant` label
  ([metrics-labels.md](metrics-labels.md)): the console's health queries
  count under-replicated, under-min-ISR and offline partitions, active
  controllers, and active and fenced brokers. A healthy production cluster
  has no under-replicated or offline partitions, one active controller and
  three active brokers.
- **The provisioner's logs:** `kubectl -n changeport logs deploy/changeport-provisioner`.
- **A customer's Postgres sources** ([cdc.md](cdc.md)): each `PostgresSource`
  says in its status what it does or waits for, how far each table's copy
  is, and its slot's health, as the console's source page shows them:

  ```bash
  kubectl -n customer-c-7f3k2q get postgressources
  kubectl -n customer-c-7f3k2q get postgressource shop -o jsonpath='{.status.phase}: {.status.conditions[?(@.type=="Ready")].message}{"\n"}'
  kubectl -n customer-c-7f3k2q get postgressource shop -o jsonpath='{.status.slot}{"\n"}{.status.tables}{"\n"}'
  ```

  Its connector is the `KafkaConnector` `<source>-<id>`, and the Kafka Connect
  worker logs as `kubectl -n customer-c-7f3k2q logs changeport-connect-connect-0`.
  Their metrics (`changeport_source_*`, `debezium_streaming_*`,
  `kafka_connect_*`, by `tenant` and `source`) and the alerts on them, in
  `deploy/base/victoria-metrics/alerts/postgres-sources.yaml`, link to the
  sections below.

## Provisioning a customer

Operators provision customers in the operator console, which also creates the
customer's Clerk organization and invites its first admin, or with the CLI:

```bash
provisioner provision -customer c-7f3k2q -profile production -display-name "Acme" -wait
```

Both create a `Customer` resource; the provisioning controller renders the
one template (`internal/provisioning/templates/customer-cluster.yaml.tmpl`)
for it and applies it. Profiles, which cannot change later:

| Profile | Brokers and zones | Replication | Broker | Disk per broker |
| --- | --- | --- | --- | --- |
| `dev` | 1 in 1 zone | 1 copy | 0.25 CPU, 1 GiB | 5 GiB |
| `dev-ha` | 3 in 3 zones | 3 copies, 2 in sync | 0.25 CPU, 1 GiB | 5 GiB |
| `production` | 3 in 3 zones | 3 copies, 2 in sync | 1.5 CPU, 5 GiB | 100 GiB |

New production customers are protected from deletion. `-wait` returns once
the customer is `Ready`; on EKS the customer's nodes start first. While it
waits, the conditions say what is pending: the listener certificate
(`CertificatePending`), the Kafka cluster, its users or the Kafka agent. A
certificate that stays pending usually means the DNS challenge: cert-manager
proves the names to Let's Encrypt with a record in the `kafka.*` hosted zone
(see [Certificates](#certificates)).

When the customer is `Ready`, its admins download `client.properties` in the
console (Overview, Connection) and connect as [connecting.md](connecting.md)
describes. Operators can print the same file:

```bash
provisioner credentials -customer c-7f3k2q -platform eks-prod
```

## Deletion protection

A customer with deletion protection (`spec.deletionProtection` on its
Customer) cannot be deleted by anyone: the API server's admission policies
(`deploy/base/provisioner/deletion-protection.yaml`) refuse deleting the
Customer and deleting its namespace, which carries the
`changeport.io/deletion-protection` label, and the provisioner CLI and the
console refuse deprovisioning it. New production customers are protected by
default, from the console and from `provisioner provision`.

Protection is turned on and off with the provisioner CLI only; the console
can create and delete customers but not change them. Turning it off takes
the customer ID typed again:

```bash
provisioner protect -customer c-7f3k2q
provisioner unprotect -customer c-7f3k2q -confirm c-7f3k2q
```

Turning protection off takes the label off the customer's namespace once
the provisioning controller has reconciled the Customer, within seconds.

## Deprovisioning a customer

Operators deprovision a customer in the operator console, typing its ID, or
with the CLI:

```bash
provisioner deprovision -customer c-7f3k2q -wait
```

The provisioning controller then deletes the customer's namespace, with its
Kafka cluster, and on EKS its dedicated nodes. The customer's Clerk
organization is kept, no longer linked to a cluster. The customer's data is
not deleted at once:

- **Volumes.** The production storage class retains the brokers' EBS volumes
  when their claims go. Before deleting the namespace, the controller labels
  each volume of the customer's claims with `changeport.io/tenant=<id>` and
  annotates it with `changeport.io/deprovisioned-at`, the time of the
  deprovisioning. Other environments delete volumes with the namespace.
- **Tiered storage.** Topic history in S3, under the customer's prefix
  `customer-<id>/` in the platform's tiered storage bucket, stays.

A customer's Postgres sources go first: the controller deletes them, and
deletes the namespace only once each has dropped its replication slot in the
customer's database. Until then the `Customer`'s status names the source it
waits for; one whose database cannot be reached waits for its slot to be
dropped by hand ([A replication slot left behind](#a-replication-slot-left-behind)).

The data is kept for 30 days, so that a customer deprovisioned by mistake can
be restored, and then purged. Do not reuse a deprovisioned customer's ID
before the purge: a new cluster of the same ID would share its prefix.

## Purging a deprovisioned customer's data

After 30 days, purge the customer's data. Check first that the customer is
gone and has been for 30 days:

```bash
kubectl get customer c-7f3k2q   # NotFound
kubectl get pv -l changeport.io/tenant=c-7f3k2q \
  -o custom-columns='NAME:.metadata.name,DEPROVISIONED:.metadata.annotations.changeport\.io/deprovisioned-at,VOLUME:.spec.csi.volumeHandle,PHASE:.status.phase'
```

On EKS, delete each retained volume in AWS, then its PersistentVolume, and
the customer's tiered storage prefix (the bucket is
`changeport-<environment>-tiered-storage`):

```bash
aws ec2 delete-volume --volume-id <VOLUME>
kubectl delete pv <NAME>
aws s3 rm s3://changeport-prod-tiered-storage/customer-c-7f3k2q/ --recursive
```

The prod bucket is versioned: what `aws s3 rm` deletes stays as old versions
for 7 days, and then the bucket's lifecycle rule removes it for good.

On kind, the volumes went with the namespace, and the object store is
SeaweedFS; the deletion protection test (`just deletion-protection-test`)
runs this purge:

```bash
kubectl -n object-store exec deploy/seaweedfs -- \
  sh -c "echo 'fs.rm -r /buckets/changeport-tiered-storage/customer-c-7f3k2q' | weed shell"
```

## A broker is down

**Signs:** `KafkaReady` turns false; the cluster's under-replicated partitions
rise; a broker pod is not ready or restarts.

On a production or `dev-ha` cluster, one broker down does not stop the
customer: every partition has three copies, two of which must be in sync, so
the other two brokers take over its partitions' leadership and keep
accepting writes. A `dev` cluster has one broker and is down until it is
back.

1. Find the broker and why it is down:

   ```bash
   kubectl -n customer-c-7f3k2q get pods -o wide
   kubectl -n customer-c-7f3k2q describe pod c-7f3k2q-dual-role-1
   kubectl -n customer-c-7f3k2q logs c-7f3k2q-dual-role-1 --previous
   ```

2. Act on the cause:
   - **Pending:** its node is gone or full. A broker's volume lives in one
     zone, so the pod can only run there; EKS Auto Mode starts a node for
     it in that zone. Wait for the node, and if none comes, see
     [Replacing a node](#replacing-a-node).
   - **Crash loop, disk full:** the broker keeps a day of each topic locally
     and the rest in tiered storage. Check that tiered storage keeps up (the
     `remotecopylag` metrics, and the brokers' logs for S3 errors) before
     anything else. Do not delete data from the volume.
   - **Crash loop, other errors:** read the log; Strimzi restarts the pod.
3. Never delete a broker's PersistentVolumeClaim: it holds the broker's data
   and its KRaft metadata.
4. Once the broker is back, check that it caught up: no under-replicated
   partitions, and `KafkaReady` true again.

## An availability zone is down

**Signs:** the brokers, nodes and load balancer targets of one zone stop
answering; several customers' clusters lose one broker each at once.

Production and `dev-ha` clusters are built to ride it out: one broker per
zone, every partition with a copy in each zone (Kafka's rack awareness), and
two copies in sync needed for writes. With one zone gone, the other two
brokers lead every partition, producers with Kafka's default retries resend
what was in flight without losing or duplicating messages, and consumers
carry on. The failover test (`hack/failover-test.sh`) checks exactly this on
kind, by freezing one zone's node under load for a minute.

1. Confirm it is the zone, not one node: the AWS Health Dashboard, and
   `kubectl get nodes -L topology.k8s.aws/zone-id`.
2. Check the clusters keep writing: no offline partitions and no
   under-min-ISR partitions on any customer. Under-replicated partitions are
   expected until the zone is back.
3. Leave the brokers of the lost zone where they are. Their volumes cannot
   move to another zone, and the spread constraint keeps a third broker from
   starting in one of the remaining zones; they come back with the zone.
4. Customers whose clients set a small `retries`, against
   [connecting.md](connecting.md), see failed sends; point them to the
   guide.
5. When the zone is back, the brokers rejoin and catch up. Check that every
   cluster is `Ready` with no under-replicated partitions.

`dev` clusters run in one zone and are down for as long as it is.

## Replacing a node

Each customer's brokers run on nodes of their own, from the customer's EKS
Auto Mode NodePool `customer-c-7f3k2q`, one broker per node. Auto Mode
replaces those nodes itself:

- when they reach the end of their life (`expireAfter`, 21 days);
- when they drift from the NodeClass, only between 06:00 and 10:00 UTC; and
- one node per customer at a time.

Replacing a node evicts its broker. Strimzi Drain Cleaner refuses that
eviction and asks the Strimzi operator to restart the broker instead, which
it does only when every partition keeps enough copies in sync without it, so
a replacement never stops writes. The failover
test checks this path too. A node waits up to 24 hours for its broker to
move (`terminationGracePeriod`).

To replace a node by hand, for example one that misbehaves, delete its
NodeClaim and let Auto Mode do the rest:

```bash
kubectl get nodeclaims -l karpenter.sh/nodepool=customer-c-7f3k2q
kubectl delete nodeclaim <name>
```

Then watch the broker restart on the new node and the cluster return to
`Ready`. Do not delete the broker's pod or volume yourself, and replace one
node at a time.

## Certificates

Three kinds of certificate are involved; all renew themselves.

- **The customers' listener certificate** (`external-listener` in the
  customer's namespace, Secret `external-listener-tls`): issued by
  cert-manager from Let's Encrypt in prod (`letsencrypt-staging` in dev,
  the local CA on kind) for `*.c-7f3k2q.<Kafka domain>`. cert-manager renews
  it when a third of its lifetime is left, and the Strimzi operator rolls the
  brokers onto the new one, one at a time. Clients trust Let's Encrypt, so
  nothing changes for them. If renewal fails, the `Certificate` says why:

  ```bash
  kubectl -n customer-c-7f3k2q describe certificate external-listener
  kubectl -n customer-c-7f3k2q get certificaterequests,orders,challenges
  ```

  The usual cause is the DNS challenge: cert-manager's Pod Identity role
  may only change the `kafka.*` hosted zones.
- **Strimzi's cluster and clients CAs**, inside each customer's cluster, for
  the brokers' internal listener and the platform's mTLS users
  (`changeport-agent`, `changeport-console`): Strimzi's defaults, renewed
  automatically with rolling restarts before they expire. Customers never
  see them. The console re-reads its user certificate and the cluster CA
  every five minutes, and the agent re-reads its certificate on every
  connection; after a cluster CA renewal, the agent fails its health check
  within five minutes and Kubernetes restarts it with the new CA.
- **The local CA on kind**, valid for ten years.

The `letsencrypt` and `letsencrypt-staging` ClusterIssuers, and external-dns,
come with the domain and the EKS environments (#8, #28).

## Rotating a customer's password

Each customer connects as its Kafka user `admin`, with a password the Strimzi
User Operator generated and keeps in the Secret `admin` of the customer's
namespace. Rotate it when the customer asks or the password may have leaked.

The new password takes effect at once and the old one stops working at once,
so clients that still use it fail to connect until they have the new one.
Agree a time with the customer first.

1. Delete the Secret; the User Operator generates a new password within
   seconds:

   ```bash
   kubectl -n customer-c-7f3k2q delete secret admin
   kubectl -n customer-c-7f3k2q get secret admin
   ```

2. The customer downloads `client.properties` again in the console, or you
   send them `provisioner credentials -customer c-7f3k2q -platform eks-prod`
   through a secure channel, and they roll it out to their applications.

There is no overlap between the old and the new password. For rotations
without downtime, customers would need a second user to switch to, which
the platform does not offer yet.

The platform's own users (`changeport-agent`, `changeport-console`) use
certificates that Strimzi renews; they need no rotation.

## Upgrading Strimzi

The Strimzi version is pinned in four places, which change together:

- `deploy/upstream-files.txt`: the operator's manifest, by checksum;
- `hack/lib.sh` (`KIND_PRELOAD_IMAGES`): its image, for kind;
- `images/kafka/Dockerfile` (`STRIMZI_KAFKA_IMAGE`): the Kafka image of the
  new Strimzi release, which the Changeport Kafka image builds on, by
  digest; and
- `internal/provisioning` if the new release changes the supported Kafka
  versions (see [Upgrading Kafka](#upgrading-kafka)).

1. Read the release's upgrade notes, change the pins, and run the whole
   suite on kind: `just dev-up e2e`. It must pass as it did before.
2. Merge. The image workflow publishes the new Kafka image and proposes it
   for EKS in a pull request; merge that too.
3. Apply to dev, then to prod:

   ```bash
   just platform-apply dev
   CONFIRM=prod just platform-apply prod
   ```

   The new operator rolls every customer's brokers onto its images, one
   broker at a time per cluster. Watch each cluster return to `Ready`
   before moving on to prod.

## Upgrading Kafka

`internal/provisioning` pins the Kafka version (`KafkaVersion`) and the KRaft
metadata version (`MetadataVersion`), and the Strimzi Kafka image in
`images/kafka/Dockerfile` must be the one for that version. The provisioning
controller re-applies the template to every customer within ten minutes of
a change, and Strimzi rolls each cluster's brokers one at a time.

The metadata version can never go back down, so upgrade in two steps:

1. **The Kafka version alone.** Change `KafkaVersion` and the Kafka image,
   keep `MetadataVersion`, run `just dev-up e2e`, merge, and merge the image
   pull request. Once the new provisioner runs on EKS, every cluster rolls
   onto the new version; wait until all are `Ready`. Until the next step,
   this can be undone by changing the version back.
2. **The metadata version**, once the new version has run for a while:
   change `MetadataVersion`, test, merge and deploy the same way. From now
   on the clusters cannot go back to the earlier version.

## A Postgres source fails

Alerts: `PostgresSourceFailed`, `PostgresSourceTaskFailed`,
`PostgresSourceBehind`, `PostgresSourceRetainedWALHigh`,
`PostgresSourceSlotNearLimit`, `PostgresSourcePausedLong`.

A source that does not stream keeps its replication slot, and the slot keeps
every change since in the customer's database: its WAL grows on the
database's disk until the source runs again, or, with
`max_slot_wal_keep_size`, until the database invalidates the slot. So a
failed or long-paused source is urgent for the customer even when nobody
reads its topics.

1. **Read what the source says.** Its status explains the common failures
   in plain language, as the console's source page shows it ([Where to
   look](#where-to-look)). The usual causes, and what fixes them:

   | The status says | Fix |
   | --- | --- |
   | The password is wrong, or the role does not exist | The customer gives the source a new password in the console (New password), which it uses once it logs in |
   | The database refused the connection, or does not answer | [A source's database is unreachable](#a-sources-database-is-unreachable) |
   | The publication does not exist, or does not publish a table | The customer runs the SQL the console shows for the source's tables again |
   | The certificate is not trusted, or is for another name | The customer's database changed certificates or its name. An operator sets the new CA or host in the source's `spec.connection` (`kubectl -n customer-c-7f3k2q edit postgressource shop`); the controller hands the CA to the connector, which uses it when it connects again |
   | The database lost the replication slot | [A lost replication slot](#a-lost-replication-slot) |

   Otherwise the connector's own error is in the `KafkaConnector`'s status
   and the Kafka Connect worker's log.

2. **Let it restart, or restart it.** Strimzi restarts a failed connector
   by itself, more and more slowly. Once the cause is fixed, restart it at
   once with:

   ```bash
   kubectl -n customer-c-7f3k2q annotate kafkaconnector shop-x1y2z3 strimzi.io/restart=true
   ```

3. **A source that runs but falls behind** (`PostgresSourceBehind`): the
   database writes changes faster than the source reads them, or the Kafka
   Connect worker is short of CPU or memory. Check the worker's pod
   (`kubectl -n customer-c-7f3k2q top pod changeport-connect-connect-0`) and
   the brokers' health; `debezium_streaming_queueremainingcapacity` near 0
   means Kafka is the bottleneck.

4. **WAL close to the limit** (`PostgresSourceSlotNearLimit`): the database
   invalidates the slot once it keeps `max_slot_wal_keep_size` of WAL, after
   which the source needs a new slot and copies its tables again. Fix the
   cause within the hour, or ask the customer to raise the limit for now.
   Without a limit (`PostgresSourceRetainedWALHigh`), the WAL fills the
   database's disk instead; the customer should set one.

## A source's database is unreachable

Alert: `PostgresSourceDatabaseUnreachable`.

The source controller reads each source's slot every minute; when it cannot
reach the database, it keeps the last reading, and nobody sees the WAL grow.
The connector usually fails too.

1. Find what the controller and the connector say:

   ```bash
   kubectl -n changeport logs deploy/changeport-provisioner | grep 'source=shop'
   kubectl -n customer-c-7f3k2q get postgressource shop -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}{"\n"}'
   ```

2. The usual causes, all on the customer's side: the database is down, its
   firewall or security group no longer lets Changeport's addresses in (the
   console lists them; they are the NAT gateways' `nat_public_ips`), its
   `pg_hba.conf` no longer lets the role in over TLS, or its host name
   changed. Ask the customer to check them in that order.
3. Meanwhile the customer can watch their slot themselves:

   ```sql
   SELECT slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
   FROM pg_replication_slots WHERE slot_name LIKE 'changeport\_%';
   ```

## A replication slot left behind

Alert: `PostgresSourceSlotNotDropped`.

Changeport drops a source's slot when the source is deleted, as the source's
role. A slot left behind keeps WAL in the customer's database for ever, so
it must go.

- **A deleted source waits for its slot** (`Deleting` for 30 minutes): the
  database cannot be reached, and the source's status, like its page in the
  console, shows the SQL that drops the slot. Once the customer has run it,
  they confirm with "I dropped the slot" in the console, and the source goes;
  an operator can confirm for them, once they have said so:

  ```bash
  kubectl -n customer-c-7f3k2q annotate postgressource shop changeport.io/slot-dropped=true
  ```

  A deprovisioned customer's namespace waits for its sources the same way.
- **A slot without a source,** such as one a customer confirmed without
  dropping: Changeport no longer knows of it. Slots and roles are named
  `changeport_<source>_<id>`, as the console shows each source's role. Ask
  the customer to list them with the query in [A source's database is
  unreachable](#a-sources-database-is-unreachable), and to drop each
  inactive one that belongs to no source in the console:

  ```sql
  SELECT pg_drop_replication_slot('changeport_shop_x1y2z3');
  ```

## A lost replication slot

Alerts: `PostgresSourceSlotLost`, then `PostgresSourceFailed`.

The database invalidated the source's slot, as it held back more WAL than
`max_slot_wal_keep_size` allows: the source was paused, failed or behind for
too long. Debezium cannot read the changes since, so the source needs a new
slot and a new copy of its tables. Tell the customer what that means: every
row arrives again, and rows deleted while the slot was lost were never
deleted from the topics, so consumers that keep a copy of the tables keep
those rows. Find and fix why the source fell behind first ([A Postgres
source fails](#a-postgres-source-fails)).

`hack/cdc-recovery-test.sh` (in the `cdc` suite) loses a slot on kind and
recovers it with these steps:

1. Pause the source, in the console or with:

   ```bash
   kubectl -n customer-c-7f3k2q patch postgressource shop --type merge -p '{"spec":{"state":"Stopped"}}'
   kubectl -n customer-c-7f3k2q get postgressource shop -o jsonpath='{.status.phase}{"\n"}'   # Stopped
   ```

2. The customer drops the lost slot:

   ```sql
   SELECT pg_drop_replication_slot('changeport_shop_x1y2z3');
   ```

3. Reset the stopped connector's offsets, so that it starts afresh from a new
   slot. Strimzi takes the annotation off once it is done:

   ```bash
   kubectl -n customer-c-7f3k2q annotate kafkaconnector shop-x1y2z3 strimzi.io/connector-offsets=reset
   kubectl -n customer-c-7f3k2q get kafkaconnector shop-x1y2z3 -o jsonpath='{.metadata.annotations}{"\n"}'
   ```

4. Ask for a new copy of every table; each new value of the annotation asks
   once:

   ```bash
   kubectl -n customer-c-7f3k2q annotate postgressource shop changeport.io/resnapshot="$(date -u +%FT%TZ)" --overwrite
   ```

5. Resume the source, in the console or by patching its state back to
   `Running`. The connector creates a new slot and streams from now on, and
   the source copies its tables one at a time, as its page shows.

