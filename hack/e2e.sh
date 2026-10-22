#!/usr/bin/env bash
# Proves the per-customer approach end to end on kind: the provisioning service
# creates a second customer's namespace and Kafka cluster from the same
# template as the demo cluster, both clusters pass the smoke test side by side,
# the metrics test checks that both customers' brokers are scraped with their
# tenant, and the second customer is deprovisioned again. Then the agent test
# checks that the Kafka agent tiers new topics, the lag test checks its
# consumer lag in seconds, the tiered storage test offloads a topic of the demo
# customer to the object store and reads it back, and the failover test freezes
# a zone and drains a broker of a three-zone customer under load. Set KEEP=1 to
# keep the customers. Run make dev-up first.
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

"$ROOT/hack/metrics-test.sh"

log "Checking that every image was preloaded"
missing=$(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{range .spec.initContainers[*]}{" "}{.image}{end}{range .spec.containers[*]}{" "}{.image}{end}{"\n"}{end}' |
  awk '$1 != "kube-system" && $1 != "local-path-storage" { for (i = 2; i <= NF; i++) print $i }' | sort -u |
  grep -vxF -f <(printf '%s\n' "${KIND_PRELOAD_IMAGES[@]}" "$(content_tag "$KAFKA_IMAGE")" "$(content_tag "$AGENT_IMAGE")") || true)
[[ -z "$missing" ]] || die "pods run images that are not built or preloaded by hack/lib.sh:"$'\n'"$missing"

if [[ ${KEEP:-0} != 1 ]]; then
  log "Deprovisioning customer $customer"
  provisioner deprovision -customer "$customer" -wait -timeout 5m
fi

"$ROOT/hack/agent-test.sh" "$DEMO_CUSTOMER"
"$ROOT/hack/lag-test.sh" "$DEMO_CUSTOMER"
"$ROOT/hack/tiered-storage-test.sh" "$DEMO_CUSTOMER"
"$ROOT/hack/failover-test.sh"
log "End-to-end test passed"
