#!/usr/bin/env bash
# Proves the per-customer approach end to end on kind, in two suites that
# need separate clusters, as the failover test freezes a zone. The core suite:
# the provisioning service creates a second customer's namespace and Kafka
# cluster from the same template as the demo cluster, protected from
# deletion, while the demo cluster is smoke tested; both clusters pass the
# smoke test side by side, and the metrics test checks that both customers'
# brokers are scraped with their tenant. The deletion protection test checks
# that the second customer cannot be deleted until protection is turned off,
# deprovisions it, and checks that its tiered storage outlives it until the
# purge. Then the agent test checks that the Kafka agent tiers new topics, the
# lag test checks its consumer lag in seconds, the console access test checks
# what the console may read and do on a customer's cluster, the console test
# opens the console as a browser does, the tiered storage test offloads a
# topic of the demo customer to the object store and reads it back, and the
# host access test produces and consumes as an application on this machine.
# The failover suite freezes a zone and drains a broker of a three-zone
# customer under load. SUITE=core or SUITE=failover runs one (default: both,
# one after the other). Set KEEP=1 to keep the customers. Run make dev-up
# first.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-acme}
DEMO_CUSTOMER=${DEMO_CUSTOMER:-demo}

ensure_kind
ensure_kubectl
cluster_exists || die "kind cluster $CLUSTER_NAME does not exist; run make dev-up first"
# A full disk shows up as obscure failures deep in a test, such as the
# object store refusing tiered storage uploads.
check_docker_disk

SUITE=${SUITE:-all}
case $SUITE in
core | failover | all) ;;
*) die "SUITE must be core, failover or all, not $SUITE" ;;
esac

core_suite() {
  # The second customer's cluster gets ready while the demo is tested.
  log "Provisioning customer $customer"
  provisioner provision -customer "$customer" -deletion-protection
  "$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"
  provisioner provision -customer "$customer" -deletion-protection -wait -timeout 10m
  "$ROOT/hack/smoke-test.sh" "$customer"

  log "Kafka clusters by tenant"
  kubectl get kafka -A -L changeport.io/tenant
  local c warnings missing
  for c in "$customer" "$DEMO_CUSTOMER"; do
    warnings=$(kubectl -n "customer-$c" get kafka "$c" \
      -o jsonpath='{range .status.conditions[?(@.type=="Warning")]}{.reason}: {.message}{"\n"}{end}')
    [[ -z "$warnings" ]] || die "Kafka cluster of customer $c reports warnings:"$'\n'"$warnings"
  done

  "$ROOT/hack/metrics-test.sh"

  log "Checking that every image was preloaded"
  missing=$(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{range .spec.initContainers[*]}{" "}{.image}{end}{range .spec.containers[*]}{" "}{.image}{end}{"\n"}{end}' |
    awk '$1 != "kube-system" && $1 != "local-path-storage" { for (i = 2; i <= NF; i++) print $i }' | sort -u |
    grep -vxF -f <(printf '%s\n' "${KIND_PRELOAD_IMAGES[@]}" "$(content_tag "$KAFKA_IMAGE")" "$(content_tag "$AGENT_IMAGE")" "$(content_tag "$PROVISIONER_IMAGE")" "$(content_tag "$CONSOLE_IMAGE")") || true)
  [[ -z "$missing" ]] || die "pods run images that are not built or preloaded by hack/lib.sh:"$'\n'"$missing"

  if [[ ${KEEP:-0} != 1 ]]; then
    "$ROOT/hack/deletion-protection-test.sh" "$customer"
  fi

  "$ROOT/hack/agent-test.sh" "$DEMO_CUSTOMER"
  "$ROOT/hack/lag-test.sh" "$DEMO_CUSTOMER"
  "$ROOT/hack/console-access-test.sh" "$DEMO_CUSTOMER"
  "$ROOT/hack/console-test.sh"
  "$ROOT/hack/tiered-storage-test.sh" "$DEMO_CUSTOMER"
  "$ROOT/hack/host-access-test.sh" "$DEMO_CUSTOMER"
}

[[ $SUITE == failover ]] || core_suite
[[ $SUITE == core ]] || "$ROOT/hack/failover-test.sh"
log "End-to-end test passed ($SUITE)"
