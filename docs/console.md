# The console

Every screen of the first console, as `make ui-screenshots` captures it on
kind: an operator provisions Acme Corp and Globex through the operator
console, Acme's admin Ada creates topics, producers and consumer groups run
for a few minutes, and Acme is deprovisioned again. Each screen also has a
phone version, under the same name ending in `-phone.png`. To capture them
again after changing the console, run `make dev-up` with `.env` filled in,
then:

```bash
make ui-screenshots
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
