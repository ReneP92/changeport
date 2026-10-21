#!/usr/bin/env bash
# Proves the per-customer approach end to end on kind: the provisioning service
# creates a second customer's namespace and Kafka cluster from the same template
# as the demo cluster, both clusters pass the smoke test side by side, and the
# second one is deprovisioned again. Set KEEP=1 to keep it. Run make dev-up first.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-acme}
DEMO_CUSTOMER=${DEMO_CUSTOMER:-demo}

ensure_kind
ensure_kubectl
cluster_exists || die "kind cluster $CLUSTER_NAME does not exist; run make dev-up first"

log "Provisioning customer $customer"
provisioner provision -customer "$customer" -wait -timeout 10m

"$ROOT/hack/smoke-test.sh" "$customer"
"$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"

log "Kafka clusters by tenant"
kubectl get kafka -A -L changeport.io/tenant
for c in "$customer" "$DEMO_CUSTOMER"; do
  warnings=$(kubectl -n "customer-$c" get kafka "$c" \
    -o jsonpath='{range .status.conditions[?(@.type=="Warning")]}{.reason}: {.message}{"\n"}{end}')
  [[ -z "$warnings" ]] || die "Kafka cluster of customer $c reports warnings:"$'\n'"$warnings"
done

log "Checking that every image was preloaded"
missing=$(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{range .spec.initContainers[*]}{" "}{.image}{end}{range .spec.containers[*]}{" "}{.image}{end}{"\n"}{end}' |
  awk '$1 != "kube-system" && $1 != "local-path-storage" { for (i = 2; i <= NF; i++) print $i }' | sort -u |
  grep -vxF -f <(printf '%s\n' "${KIND_PRELOAD_IMAGES[@]}") || true)
[[ -z "$missing" ]] || die "pods run images that are not in KIND_PRELOAD_IMAGES (hack/lib.sh):"$'\n'"$missing"

if [[ ${KEEP:-0} != 1 ]]; then
  log "Deprovisioning customer $customer"
  provisioner deprovision -customer "$customer" -wait -timeout 5m
fi
log "End-to-end test passed"
