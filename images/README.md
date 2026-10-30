# images

Container images we build.

- `kafka/`: the Changeport Kafka broker image. It is Strimzi's Kafka image,
  pinned by digest, plus the Aiven remote storage manager for tiered storage
  on S3, built from a pinned commit into `/opt/kafka/plugins/tiered-storage/`.
  The Dockerfile explains the pins.

```bash
just kafka-image
```

```bash
just kafka-image-test
```

- `agent/`: the Kafka agent (`cmd/agent`), which runs next to every
  customer's cluster and turns on tiered storage for every topic that can use
  it. It is a static Go binary on a distroless, non-root base, built from the
  repository root.

```bash
just agent-image
```

- `registry-gateway/`: the registry gateway (`cmd/registry-gateway`), through
  which a customer's clients reach their schema registry from the internet,
  with their Kafka credentials (docs/cdc.md). Built like the agent.

```bash
just registry-gateway-image
```

`just kafka-image` builds `changeport/kafka:local` for this machine, and
`just dev-up` builds it and loads it into kind. `just kafka-image-test` starts
a single broker from it with tiered storage configured and creates a tiered
topic. CI builds both images for amd64 and arm64 on every PR; pushing them to ECR
for the EKS platforms follows with the ECR repositories (#18).
