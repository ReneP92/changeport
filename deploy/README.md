# deploy

Kubernetes manifests that are not generated per customer.

- `kind/`: the local development cluster.
- `strimzi/`: the Strimzi cluster operator, installed once per Kubernetes
  cluster and watching every namespace.

Per-customer resources (namespace, Kafka, node pool) are not here: the
provisioning service renders them from
`internal/provisioning/templates/customer-cluster.yaml.tmpl`.
