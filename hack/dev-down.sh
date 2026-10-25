#!/usr/bin/env bash
# Deletes the local kind cluster and everything in it, and stops the helpers
# hack/lib.sh starts next to it: cloud-provider-kind and the host gateway,
# whose copy of the cluster's credentials goes too. The host's resolver entry
# from just host-dns stays, as it is a one-time step that serves the next
# cluster as well; without a gateway, lookups of changeport.test just fail.
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
if docker inspect "$HOST_GATEWAY_CONTAINER" >/dev/null 2>&1; then
  log "Stopping the host gateway"
  docker rm -f "$HOST_GATEWAY_CONTAINER" >/dev/null
fi
rm -f "$BIN_DIR/host-gateway.kubeconfig"
# cloud-provider-kind leaves the load balancer containers of a deleted cluster
# behind.
load_balancers=$(docker ps -aq --filter "label=io.x-k8s.cloud-provider-kind.cluster=$CLUSTER_NAME")
if [[ -n "$load_balancers" ]]; then
  log "Removing the load balancer containers of $CLUSTER_NAME"
  # shellcheck disable=SC2086 # one container ID per word
  docker rm -f $load_balancers >/dev/null
fi
