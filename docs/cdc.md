# Postgres CDC source

A customer connects their Postgres database in the console and picks tables.
Each table then streams into a topic of its own over logical replication (the
write-ahead log), starting with a snapshot of the rows already there. This is
the source half of change data capture. Delivering the tables on to a
warehouse is the CDC product of Phase 9.

It is tracked in the
[Postgres CDC source milestone](https://github.com/ReneP92/changeport/milestone/11),
pulled forward from Phase 7. This document is the design; the decisions behind
it are in [PROJECT_PLAN.md](PROJECT_PLAN.md).

## What the customer does

1. **Connect:** enters the database's host, port, database name and TLS
   settings. The console checks that it can reach the database and that the
   certificate is valid.
2. **Run our first SQL:** creates a replication user for Changeport with a
   password the console generated. The console shows the exact SQL for the
   customer's provider (RDS and Aurora, Cloud SQL, Azure, Supabase, Neon, or
   plain Postgres), with the steps outside SQL before it, and lists the
   outbound IP addresses to allowlist. On Neon, the role is created in Neon's
   console instead, and the customer enters the password Neon shows.
3. **Pick tables:** chooses the tables to sync. Tables without a primary key
   or a unique index as replica identity are listed but cannot be picked, with
   the reason.
4. **Run our second SQL:** grants read access to those tables and creates a
   publication for them.
5. **Verify and start:** the console checks everything below, then starts
   the source. Each table gets the topic `<source>.<schema>.<table>`. The
   existing rows arrive first, then every insert, update and delete as it
   happens.

Messages are Avro. Their schemas live in the customer's schema registry at
`https://registry.<customer>.<domain>`, which takes the customer's Kafka
credentials, so the `client.properties` from the console covers both.

## Requirements on the customer's database

- PostgreSQL 14 or newer.
- `wal_level = logical`.
- Reachable from the internet on its port from Changeport's outbound IP
  addresses, over TLS with a certificate the client can verify
  (`verify-full`, or `verify-ca` with the customer's CA).
- A free replication slot and WAL sender (`max_replication_slots`,
  `max_wal_senders`).
- `max_slot_wal_keep_size` set, strongly recommended. It caps how much WAL a
  slot can hold back if the source stops reading, so a stuck source can never
  fill the database's disk.

## How it runs

Everything a customer's sources need is rendered from the one template
(`internal/provisioning/templates/customer-cluster.yaml.tmpl`) into the
customer's namespace. It exists only while the customer has a source.

### Sources

A namespaced `PostgresSource` resource in `customer-<id>` describes each
source (`api/v1alpha1/postgressource_types.go`). Its name is the topic prefix,
following the same rules as `internal/cdc`: at most 40 lowercase letters,
digits and dashes, not starting with a reserved prefix.

- **Spec:**
  - `id`: the 6 characters that make the source's role, publication and slot
    names unique. The console chooses it, and it cannot change;
  - `provider`;
  - `connection`:
    - `host`, `port` (5432 by default), and `database`, which cannot
      change;
    - `passwordSecret`, the Secret the console wrote the password into.
      Naming another one gives the role a new password;
    - `tls`: `verify-full` (the default) or `verify-ca`, which needs `ca`;
  - `tables`, from 1 to 200, each once;
  - `retentionMs`: one of the topic presets from 1 hour to 30 days, 7 days by
    default;
  - `state`: `Running` (the default) or `Stopped`.
- **Status:**
  - `phase`: Pending, Snapshotting, Streaming, Stopped, Failed or Deleting;
  - `passwordSecret`, the Secret whose password the source uses;
  - `slot`: whether it is active, the WAL it retains, its `wal_status` and
    `safe_wal_size`;
  - `tables`: each table's topic and snapshot state, as rows copied of
    estimated;
  - conditions.

A test keeps the resource's limits equal to `internal/cdc`'s and the topic
presets, and envtest tests check its validation.

The provisioning controller renders the runtime and a `KafkaConnector` per
source. A second controller, for sources, does everything else: credentials,
snapshots, slot monitoring, status, and dropping the slot when a source is
deleted. The `Customer` gets a `CDCReady` condition, which `Ready` does not
include, so `Ready` keeps meaning that clients can connect.

### Kafka Connect and Debezium

- **The worker:** one Kafka Connect worker per customer, `changeport-connect`,
  run by Strimzi.
  - It is placed on the customer's dedicated nodes, but never on a broker's
    node.
  - It connects as the mTLS user `changeport-connect`. That user may create
    and write each source's topics and use Connect's own topics
    (`changeport.connect.*`), but never read the customer's topics.
- **The image:** `images/connect`, Strimzi's Kafka image with the Debezium
  Postgres connector and Apicurio's Avro converter, pinned and checksummed.
- **Replication:** with `pgoutput`.
  - The publication is the one the customer created.
  - The slot is the connector's own.
  - The role, the publication and the slot share one name,
    `changeport_<source>_<id>`, with the source's dashes as underscores. The
    ID is 6 random characters chosen when the source is created, so two
    sources never share a slot, even with the same name.
- **Snapshots:** read-only incremental snapshots.
  - Tables are copied in chunks while changes keep streaming.
  - Snapshots take no locks and write nothing to the customer's database.
  - They resume where they stopped, and do not hold a transaction open long
    enough to hold back vacuum.
  - Snapshot requests and progress travel through Kafka topics.
- **Heartbeat:** every minute, a transactional logical message with no table
  (`pg_logical_emit_message(true, 'changeport-heartbeat', '')`). It lets the
  slot advance when the published tables are idle while the rest of the
  database is busy.
  - Every role may call the function, so the replication role needs no grant
    for it.
  - Debezium drops the messages (`message.prefix.exclude.list`), so they never
    become events.
- **Topics:**
  - one partition per table, keyed by primary key;
  - replicated and tiered like every topic;
  - the source's retention;
  - a deleted row becomes a tombstone.

### Schema registry

- **Registry:** Apicurio Registry 3, one per customer, keeping its schemas in
  the customer's Kafka in three topics, `changeport.registry.journal`,
  `.snapshots` and `.events`. It connects over mTLS as `changeport-registry`.
- **Message format:** the converter writes Confluent's wire format, so
  standard Avro deserializers read the topics.
- **Access:** customers reach the registry through `registry-gateway`, a small
  Go service of ours.
  - TLS uses the cluster's existing certificate, and the gateway has its own
    load balancer.
  - It checks the customer's Kafka credentials and limits failed attempts.
  - It refuses deleting a source's schemas or changing their compatibility.

### The database password

- **The console writes it once:** as an immutable Secret of its own type. An
  admission policy allows the console only that, so it can never read,
  change or delete a Secret.
- **The controller keeps it:** it moves the password into a Secret only the
  controller and the Connect worker use, then deletes the console's copy.
- **Connect reads it through a config provider:** connector configurations
  hold only a placeholder, never the password.
- **Rotation:** the console generates a new password and its `ALTER ROLE`
  statement. Once the customer has run it and the console has tested it, the
  new password replaces the old one.

## Keeping the customer's database safe

A replication slot keeps every WAL record its reader has not confirmed. A
source that stops reading makes the database keep WAL until its disk fills.
So:

- **Monitoring:** every minute, the source controller reads the slot from
  `pg_replication_slots`: whether it is active, the WAL it retains, its
  `wal_status` and `safe_wal_size`. It shows them in the console and exports
  `changeport_source_retained_wal_bytes{tenant,source}` for alerts.
- **A setup warning:** the checks warn when `max_slot_wal_keep_size` is
  unlimited.
- **Pausing:** pausing a source keeps its slot and offsets, and the console
  shows the retained WAL growing meanwhile.
- **Deleting:** deleting a source drops its slot as soon as the connector has
  let go of it. If the database cannot be reached, the console shows the SQL
  to drop it, and the customer confirms once it is done. Slots are never
  dropped any other way.
- **A lost slot:** when the database invalidates a slot because it held back
  too much WAL, the source fails, saying so plainly. Recovery is a new slot
  and a new snapshot.

## Providers

The setup differs by provider. The console's instructions
(`internal/cdc/sql.go`) cover these:

- **Plain PostgreSQL:** `wal_level = logical` in `postgresql.conf`, and a
  `hostssl` line in `pg_hba.conf` per Changeport address.
- **Amazon RDS and Aurora:**
  - `rds.logical_replication = 1` in the parameter group;
  - the role gets `rds_replication` rather than `REPLICATION`;
  - RDS certificates come from Amazon's own CA, so the customer adds the
    region's certificate bundle.
- **Google Cloud SQL:**
  - the `cloudsql.logical_decoding` flag;
  - an instance whose certificate does not name its host needs `verify-ca`
    with the instance's server CA.
- **Azure Database for PostgreSQL:** the `wal_level` server parameter. On
  PostgreSQL 16 and newer, the admin needs `REPLICATION` itself before it can
  give it to a new role.
- **Supabase:**
  - logical replication is on by default;
  - the direct connection is IPv6 only without the IPv4 add-on, and
    Changeport connects over IPv4;
  - the pooler cannot replicate.
- **Neon:**
  - logical replication is turned on in the project's settings;
  - roles created with SQL cannot replicate, so the role is created in Neon's
    console. That makes it a member of `neon_superuser`, more than Changeport
    needs;
  - the pooler cannot replicate;
  - Neon removes slots inactive for about 40 hours, so a source stopped for
    longer needs a new snapshot.

## The console's checks

The console runs these checks before a source starts and again on request
(`internal/cdc/checks.go`). It runs them itself, from the same outbound IP
addresses as the Connect worker. Each result says what was found and, when
it failed, what to do. The checks:

- reachability, TLS and the certificate, and authentication;
- the server version, `wal_level`, the replication privilege (a superuser
  passes with a warning), and a free slot and WAL sender;
- a replication connection (`IDENTIFY_SYSTEM`), which catches a missing
  `pg_hba.conf` entry;
- the chosen tables: each exists, has a primary key or a unique replica
  identity, and is readable by the role;
- the publication: inserts, updates and deletes, partitioned tables published
  as themselves, and every chosen table in it;
- `max_slot_wal_keep_size`, which only warns.

They only connect to public addresses. The console resolves the host once,
refuses it if any of its addresses is not public, and connects only to the
addresses it resolved, while checking the certificate against the host's
name. A name cannot resolve to a public address for the check and to an
internal one for the connection.

## Guardrails

- **Names:** a source reserves its prefix `<source>.`, and nobody may create
  other topics under it. The topic names for the source's tables are computed
  and checked before it starts: valid characters, at most 200 characters, no
  collisions.
- **Capacity:** a source and every table added later count against the
  cluster's replica budget.
- **Ownership:** a source's topics are marked as its own in the console, and
  cannot be deleted while the source keeps them.
- **Hidden topics:** the platform's own topics and consumer groups, those
  starting with `changeport`, are hidden from the customer.

## What the spike proved

Before anything was built on them, a spike on kind (#101) ran each untried part
of this design with the Connect image, Apicurio 3.3.3 and Postgres 17:

- **Read-only incremental snapshots work.** A role with only `REPLICATION`
  and `SELECT` copied a 5,000-row table through a signal on the Kafka signal
  topic, with no table for signals and nothing written to the database.
  Inserts, updates and deletes then streamed as they happened.
- **The heartbeat must be transactional.** With a non-transactional
  logical message, Debezium 3.7 fails during a read-only incremental snapshot:
  `PostgresReadOnlyIncrementalSnapshotContext.isTransactionVisible` throws on
  the message's missing transaction ID.
  - `pg_logical_emit_message(true, ...)` works.
  - The function is executable by every role on PostgreSQL 14 and 17.
  - Without `message.prefix.exclude.list`, the messages become events in a
    `<source>.message` topic.
- **Send each snapshot signal once.** A signal sent while the connector's
  task was failing ran once the task recovered, so it copied the table a
  second time. The source controller sends a table's signal once and follows
  its progress in the notification topic.
- **Apicurio stores its schemas in Kafka over mTLS,** with the PKCS12 key and
  truststores of Strimzi's Secrets. It needs a third topic, its events
  (`apicurio.events.kafka.topic`), named under the same prefix.
- **Standard Confluent clients read the messages.**
  - `confluent-kafka` 2.16's `AvroDeserializer` decoded keys, values and
    tombstones through Apicurio's Confluent-compatible API
    (`/apis/ccompat/v7`).
  - The converter's defaults (`headers.enabled=false`, 4-byte IDs) match it.
  - Apicurio registers nested record schemas as subjects of their own, which
    the topic's schema references.
- **Kafka's `DirectoryConfigProvider` works under Strimzi 1.3.0 and Kafka
  4.3.1,** for the database password and for the signal consumer's keystore
  passwords. No allowlist change was needed.
- **Strimzi's v1 `KafkaConnect` takes the group and its three topics as spec
  fields** (`groupId`, `configStorageTopic` and the like), not as worker
  configuration.
- **NetworkPolicies are enforced on kind** (kindnet): a deny-all ingress
  policy blocked a connection that worked without it.
  - On EKS Auto Mode, enforcement has to be turned on (VPC CNI's network
    policy agent).
  - That is checked when the dev environment comes up (#28, #29).

## Testing

- **`internal/cdc`'s tests** run against Postgres 13, 14 and 18 in Docker.
  - They run the SQL the console generates, as a customer would, and check
    the database after each step: creating the role, choosing, adding and
    removing tables, a new password, the connector's slot and cleaning up.
  - They also cover a certificate for another name, another CA, a wrong
    password, `wal_level = replica`, and a server that is too old.
- **On kind:** a Postgres 17 in the namespace `source-postgres`
  (`deploy/base/source-postgres`) stands in for a customer's database.
  - It runs with `wal_level = logical` and `max_slot_wal_keep_size`, and takes
    clients over TLS only, with a certificate from the local CA for
    `postgres.source-postgres.svc`.
  - Its database `app` has seed tables: `customers` and `orders` with primary
    keys, `notes` without one, and `events`, partitioned by year.
  - `hack/postgres-test.sh` (`just postgres-test`, in the core suite) checks
    the seed, opens a replication connection with `verify-full`, and checks
    that a client without TLS, or verifying the certificate against the
    service's address, is refused.
- **`hack/cdc-test.sh`,** in its own CI suite, creates a source as the console
  does and takes it through its whole life:
  1. snapshot;
  2. inserts, updates and deletes, decoded through the registry gateway;
  3. adding a table;
  4. pausing and resuming;
  5. deleting it, which drops its slot.
- **A Playwright spec** runs the console's wizard against the same database.

## Steps

Each step is an issue in the milestone and a pull request with its tests:

1. [#101](https://github.com/ReneP92/changeport/issues/101): the Kafka Connect image, and a spike of the parts no one has run yet
2. [#102](https://github.com/ReneP92/changeport/issues/102): Postgres with logical replication on kind
3. [#103](https://github.com/ReneP92/changeport/issues/103): naming, setup SQL and checks (`internal/cdc`)
4. [#104](https://github.com/ReneP92/changeport/issues/104): the `PostgresSource` resource
5. [#105](https://github.com/ReneP92/changeport/issues/105): the CDC runtime from the template
6. [#106](https://github.com/ReneP92/changeport/issues/106): streaming changes into topics
7. [#107](https://github.com/ReneP92/changeport/issues/107): snapshots, and adding and removing tables
8. [#108](https://github.com/ReneP92/changeport/issues/108): the registry gateway
9. [#109](https://github.com/ReneP92/changeport/issues/109): the console's API
10. [#110](https://github.com/ReneP92/changeport/issues/110): the console's pages
11. [#111](https://github.com/ReneP92/changeport/issues/111): monitoring and the runbook
