#!/usr/bin/env bash
# Builds the Kafka image, creates the local kind cluster, installs the platform
# components (Strimzi, cert-manager) and provisions the "demo" customer's Kafka cluster with the
# provisioning service, then runs the smoke test against it. Safe to run again.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEMO_CUSTOMER=${DEMO_CUSTOMER:-demo}

require_tools
ensure_kind
ensure_kubectl

build_kafka_image
ensure_kind_cluster
"$ROOT/hack/platform-apply.sh" kind

log "Provisioning customer $DEMO_CUSTOMER"
provisioner provision -customer "$DEMO_CUSTOMER" -wait -timeout 10m

"$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"

log "Ready. Kafka for customer $DEMO_CUSTOMER: $DEMO_CUSTOMER-kafka-bootstrap.customer-$DEMO_CUSTOMER.svc:9092 (kubectl context $KUBE_CONTEXT)"
