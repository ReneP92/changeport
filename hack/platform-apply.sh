#!/usr/bin/env bash
# Applies the platform components of one environment: the operators in
# deploy/overlays/<overlay>, then, once they are ready, the resources in
# config/ that need their CRDs and webhooks. Safe to run again.
# Usage: hack/platform-apply.sh kind|dev|prod (prod also needs CONFIRM=prod)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

env=${1:?usage: hack/platform-apply.sh kind|dev|prod}
case "$env" in
  kind) overlay=kind ;;
  dev | prod)
    overlay=eks-$env
    # Created by: aws eks update-kubeconfig --name changeport-<env> --alias changeport-<env>
    KUBE_CONTEXT=changeport-$env
    ;;
  *) die "unknown environment $env; use kind, dev or prod" ;;
esac
[[ $env != prod || ${CONFIRM:-} == prod ]] || die "applying to prod needs CONFIRM=prod"

ensure_kubectl
fetch_upstream_files

server=$("$KUBECTL_BIN" config view --minify --context "$KUBE_CONTEXT" -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null) ||
  die "kubectl context $KUBE_CONTEXT does not exist"
if [[ $env == kind && $server == *.eks.amazonaws.com* ]] || [[ $env != kind && $server != *.eks.amazonaws.com* ]]; then
  die "kubectl context $KUBE_CONTEXT points at $server, which is not a $env cluster"
fi

apply() { kubectl apply --server-side --force-conflicts --field-manager=changeport-platform -k "$1" >/dev/null; }

log "Applying the $overlay platform components to $KUBE_CONTEXT"
apply "deploy/overlays/$overlay"
while read -r namespace name; do
  kubectl -n "$namespace" rollout status "deployment/$name" --timeout=5m
done < <(kubectl get deployments -A -l changeport.io/platform -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}')

config="deploy/overlays/$overlay/config"
if [[ -d $config ]]; then
  log "Applying the $overlay platform configuration"
  # A webhook can refuse requests for a few seconds after its deployment is
  # available, until its serving certificate has been injected.
  for attempt in $(seq 12); do
    apply "$config" && break
    [[ $attempt -lt 12 ]] || die "applying $config failed"
    sleep 5
  done
  if [[ -n $(kubectl get clusterissuers -o name) ]]; then
    kubectl wait clusterissuers --all --for=condition=Ready --timeout=2m >/dev/null
  fi
fi
log "Platform components of $env are ready"
