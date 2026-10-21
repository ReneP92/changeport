#!/usr/bin/env bash
# Validates every platform overlay against the real Kubernetes API: applies
# the kind overlay to the local kind cluster (creating it if needed), then
# server-side dry-runs the EKS overlays there, which checks them against the
# pinned Kubernetes version and the installed CRDs. Used by CI.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require_tools
ensure_kind
ensure_kubectl
ensure_kind_cluster
"$ROOT/hack/platform-apply.sh" kind

for overlay in deploy/overlays/eks-*/; do
  for dir in "$overlay" "${overlay}config"; do
    [[ -d $dir ]] || continue
    log "Dry-running $dir"
    kubectl apply --server-side --dry-run=server --force-conflicts --field-manager=changeport-platform -k "$dir" >/dev/null
  done
done
log "Every platform overlay is valid"
