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
  - `provisioner/`: the provisioning controller in the `changeport`
    namespace, with its RBAC, and the `Customer` CRD
    (`customers.changeport.io`), which `just generate` writes from
    `api/v1alpha1`. On EKS it stays at zero replicas until its image is in
    ECR (#18). On kind, `hack/platform-apply.sh` applies the kind overlay
    through the git-ignored `overlays/kind-local`, which points the
    controller at the locally built images.
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
just platform-apply kind
```

```bash
CONFIRM=prod just platform-apply prod
```

```bash
just platform-check
```

`just platform-apply dev` and `prod` use the kubectl contexts `changeport-dev`
and `changeport-prod` (`aws eks update-kubeconfig --name changeport-<env>
--alias changeport-<env>`) and refuse to run against anything that is not an
EKS cluster.

Per-customer resources (namespace, Kafka, node pool) are not here: the
provisioner renders them from
`internal/provisioning/templates/customer-cluster.yaml.tmpl`, for each
`Customer` resource.
