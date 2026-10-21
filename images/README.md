# images

Container images we build.

- `kafka/`: the Changeport Kafka broker image. It is Strimzi's Kafka image,
  pinned by digest, plus the Aiven remote storage manager for tiered storage
  on S3, built from a pinned commit into `/opt/kafka/plugins/tiered-storage/`.
  The Dockerfile explains the pins.

```bash
make kafka-image
```

```bash
make kafka-image-test
```

`make kafka-image` builds `changeport/kafka:local` for this machine, and
`make dev-up` builds it and loads it into kind. `make kafka-image-test` starts
a single broker from it with tiered storage configured and creates a tiered
topic. CI builds the image for amd64 and arm64 on every PR; pushing it to ECR
for the EKS platforms follows with the ECR repositories.
