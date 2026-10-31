# The console

Every screen of the first console, as `just ui-screenshots` captures it on
kind: an operator provisions Acme Corp and Globex through the operator
console, Acme's admin Ada creates topics and adds a Postgres source,
producers and consumer groups run for a few minutes, and Acme is
deprovisioned again. Each screen also has a
phone version, under the same name ending in `-phone.png`. To capture them
again after changing the console, run `just dev-up` with `.env` filled in,
then:

```bash
just ui-screenshots
```

## Customers

Signing in, through Clerk.

![Sign-in](screenshots/01-sign-in-desktop.png)

A new customer's cluster getting ready, part by part.

![A customer's cluster getting ready](screenshots/02-customer-starting-desktop.png)

The overview of a ready cluster: how to connect, with `client.properties`
for admins; the brokers' health; throughput; the topics with their
retention, tiering, size and traffic; and how far behind each consumer group
is, in plain language, with its lag over the last hour.

![The overview](screenshots/03-overview-desktop.png)

Creating a topic within the guardrails, which the dialog applies as Ada
types.

![Creating a topic](screenshots/04-create-topic-desktop.png)

![The guardrails](screenshots/05-create-topic-guardrails-desktop.png)

Deleting a topic names the consumer groups still reading it and takes the
topic's name typed.

![Deleting a topic](screenshots/06-delete-topic-desktop.png)

What customers see outside their overview: the operator console is closed
to them, an unknown page, and an organization without a cluster.

![Operators only](screenshots/07-operators-only-desktop.png)

![No such page](screenshots/08-not-found-desktop.png)

![No cluster](screenshots/09-no-cluster-desktop.png)

## Postgres sources

A customer's Postgres sources, under Sources in the header (see
[cdc.md](cdc.md)). Ada adds one through the wizard, against the Postgres on
kind that stands in for a customer's database, and runs its SQL as the
database's owner would. Members of the organization see everything, and
change nothing.

![No sources yet](screenshots/20-sources-empty-desktop.png)

The wizard's six steps. First, where the database is, with the addresses to
allow in and how to check its certificate; Changeport checks that it can
reach it.

![Connecting](screenshots/21-source-connect-desktop.png)

Then the role Changeport logs in as, with the password the console chose,
shown only here.

![The role](screenshots/22-source-role-desktop.png)

The tables, each with its topic. Those without a primary key Changeport can
use are listed, disabled, with the reason.

![Choosing tables](screenshots/23-source-tables-desktop.png)

After the SQL that grants and publishes the tables, every check, each with
what it found or what to do. The last step starts the source.

![Verifying](screenshots/24-source-verify-desktop.png)

The source copies each table's existing rows, then streams every change.

![A source starting](screenshots/25-source-starting-desktop.png)

![A source streaming](screenshots/26-source-desktop.png)

Adding tables: Changeport's source controller lists the database's tables,
as the console cannot log in to it, and the new tables' rows are copied once
their SQL has run.

![Adding tables](screenshots/27-source-add-tables-desktop.png)

Deleting a source takes its name typed, and shows the SQL that removes
Changeport's role and publication once it is gone. Its topics stay.

![Deleting a source](screenshots/28-source-delete-desktop.png)

![The sources](screenshots/29-sources-desktop.png)

## Operators

Provisioning a customer: its name, which also names its Clerk organization,
its profile, deletion protection and an optional invitation for its first
admin.

![Provisioning a customer](screenshots/10-operator-provision-desktop.png)

The new customer's page follows its cluster as it gets ready.

![A customer starting](screenshots/11-operator-customer-starting-desktop.png)

Every customer, with a dot per part of its cluster.

![Every customer](screenshots/12-operator-customers-desktop.png)

![A customer ready](screenshots/13-operator-customer-ready-desktop.png)

A protected customer cannot be deprovisioned.

![A protected customer](screenshots/14-operator-customer-protected-desktop.png)

Deprovisioning takes the customer ID typed; the Clerk organization is kept.

![Deprovisioning](screenshots/15-operator-deprovision-desktop.png)

![A customer being deleted](screenshots/16-operator-customer-deleting-desktop.png)

## Still to design

The first console is built and refined on the real UI. Still to design are
the guided first ten minutes, from signing up to a live sample producer and
consumer, and the live topology map of producers, topics and consumer
groups; see [Open questions and next steps](PROJECT_PLAN.md#open-questions-and-next-steps).
