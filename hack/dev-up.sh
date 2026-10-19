#!/usr/bin/env bash
# Creates the local kind cluster, installs the Strimzi operator and provisions
# the "demo" customer's Kafka cluster with the provisioning service, then runs
# the smoke test against it. Safe to run again.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEMO_CUSTOMER=${DEMO_CUSTOMER:-demo}

require_tools
ensure_kind
ensure_kubectl

if cluster_exists; then
  log "kind cluster $CLUSTER_NAME already exists"
else
  log "Creating kind cluster $CLUSTER_NAME"
  kind create cluster --name "$CLUSTER_NAME" --config deploy/kind/cluster.yaml --wait 3m
fi

upstream=deploy/strimzi/upstream/strimzi-cluster-operator.yaml
if [[ ! -f "$upstream" || $(sha256_of "$upstream") != "$STRIMZI_SHA256" ]]; then
  log "Downloading Strimzi $STRIMZI_VERSION"
  mkdir -p "$(dirname "$upstream")"
  curl -fsSL -o "$upstream.tmp" \
    "https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml"
  if [[ $(sha256_of "$upstream.tmp") != "$STRIMZI_SHA256" ]]; then
    rm -f "$upstream.tmp"
    die "Strimzi install file checksum mismatch"
  fi
  mv "$upstream.tmp" "$upstream"
fi

log "Installing the Strimzi $STRIMZI_VERSION cluster operator"
# Server-side apply: some Strimzi CRDs exceed the client-side apply annotation limit.
kubectl apply --server-side --force-conflicts -k deploy/strimzi >/dev/null
kubectl -n strimzi rollout status deployment/strimzi-cluster-operator --timeout=5m

log "Provisioning customer $DEMO_CUSTOMER"
provisioner provision -customer "$DEMO_CUSTOMER" -wait -timeout 10m

"$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"

log "Ready. Kafka for customer $DEMO_CUSTOMER: $DEMO_CUSTOMER-kafka-bootstrap.customer-$DEMO_CUSTOMER.svc:9092 (kubectl context $KUBE_CONTEXT)"
