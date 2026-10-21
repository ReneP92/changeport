# deploy

Kubernetes manifests that are not generated per customer.

- `kind/`: the local development cluster, a control plane and three workers
  in three zones.
- `base/`: the platform components, installed once per Kubernetes cluster:
  - `strimzi/`: the Strimzi cluster operator, watching every namespace.
  - `cert-manager/`: issues the customer endpoint certificates.
  - `drain-cleaner/`: Strimzi Drain Cleaner, which turns the eviction of a
    Kafka pod, for example when a node is replaced, into a rolling restart by
    the Strimzi operator.
  - `eks-storage/`: the encrypted gp3 StorageClass for EKS Auto Mode.
  - `object-store/`: SeaweedFS, standing in for S3 on kind, where brokers
    offload topic history.
  - `victoria-metrics/`: VictoriaMetrics single-node, which scrapes every
    customer's brokers and labels each series with its tenant
    (docs/metrics-labels.md).
- `overlays/`: one per environment (`kind`, `eks-dev`, `eks-prod`), choosing
  the components and the environment's settings. Resources that need an
  operator's CRDs or webhooks, such as cert-manager issuers and Drain Cleaner,
  live in the overlay's `config/` directory and are applied once the
  operators are ready.
- `upstream-files.txt`: the upstream install files the bases build on, pinned
  by URL and SHA-256 and downloaded into the git-ignored `base/*/upstream/`.

Apply an environment's components, or validate every overlay:

```bash
make platform-apply ENV=kind
```

```bash
make platform-apply ENV=prod CONFIRM=prod
```

```bash
make platform-check
```

`ENV=dev` and `ENV=prod` use the kubectl contexts `changeport-dev` and
`changeport-prod` (`aws eks update-kubeconfig --name changeport-<env>
--alias changeport-<env>`) and refuse to run against anything that is not an
EKS cluster.

Per-customer resources (namespace, Kafka, node pool) are not here: the
provisioning service renders them from
`internal/provisioning/templates/customer-cluster.yaml.tmpl`.
