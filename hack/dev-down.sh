#!/usr/bin/env bash
# Deletes the local kind cluster and everything in it.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ensure_kind
if cluster_exists; then
  log "Deleting kind cluster $CLUSTER_NAME"
  kind delete cluster --name "$CLUSTER_NAME"
else
  log "kind cluster $CLUSTER_NAME does not exist"
fi
