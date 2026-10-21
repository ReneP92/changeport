#!/usr/bin/env bash
# Deletes the local kind cluster and everything in it, and stops
# cloud-provider-kind.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ensure_kind
if cluster_exists; then
  log "Deleting kind cluster $CLUSTER_NAME"
  kind delete cluster --name "$CLUSTER_NAME"
else
  log "kind cluster $CLUSTER_NAME does not exist"
fi
if docker inspect "$CLOUD_PROVIDER_KIND_CONTAINER" >/dev/null 2>&1; then
  log "Stopping cloud-provider-kind"
  docker rm -f "$CLOUD_PROVIDER_KIND_CONTAINER" >/dev/null
fi
# cloud-provider-kind leaves the load balancer containers of a deleted cluster
# behind.
load_balancers=$(docker ps -aq --filter "label=io.x-k8s.cloud-provider-kind.cluster=$CLUSTER_NAME")
if [[ -n "$load_balancers" ]]; then
  log "Removing the load balancer containers of $CLUSTER_NAME"
  # shellcheck disable=SC2086 # one container ID per word
  docker rm -f $load_balancers >/dev/null
fi
