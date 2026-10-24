#!/usr/bin/env bash
# Tests deletion protection on kind with a customer provisioned protected:
# the API server refuses deleting its Customer and its namespace, and the
# provisioner refuses deprovisioning it; turning protection off takes the
# customer ID typed again and takes the namespace's label off. Deprovisioned
# then, the customer's tiered storage stays in the object store until the
# purge of docs/runbook.md, which this test runs on kind.
# Usage: hack/deletion-protection-test.sh <customer-id>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:?usage: hack/deletion-protection-test.sh <customer-id>}
namespace="customer-$customer"
topic="kept-$(date +%s)-$RANDOM"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
log "Deletion protection test for customer $customer"
[[ $(kubectl get customer "$customer" -o jsonpath='{.spec.deletionProtection}') == true ]] ||
  die "customer $customer has no deletion protection; provision it with -deletion-protection"
[[ $(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.changeport\.io/deletion-protection}') == true ]] ||
  die "namespace $namespace has no deletion protection label"

# Prints a command's output, and fails unless the command failed saying why.
refused() {
  local want=$1 output
  shift
  if output=$("$@" 2>&1); then
    die "$* was not refused"
  fi
  [[ $output == *"$want"* ]] || die "$* failed, but not saying \"$want\":"$'\n'"$output"
  log "Refused as expected: $*"
}
refused "has deletion protection" kubectl delete customer "$customer" --wait=false
refused "belongs to a customer with deletion protection" kubectl delete namespace "$namespace" --wait=false
refused "has deletion protection" provisioner deprovision -customer "$customer"
refused "the customer ID typed again" provisioner unprotect -customer "$customer"
refused "the customer ID typed again" provisioner unprotect -customer "$customer" -confirm "$customer-x"

# Data in tiered storage, which must outlive the cluster: a topic with small
# segments whose finished segments the brokers offload within a minute.
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
kafka() { docker run "${KAFKA_CLIENT[@]}" -i --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }
kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --create \
  --topic "$topic" --partitions 1 --config remote.storage.enable=true --config segment.bytes=1048576 >/dev/null
seq -f '%08g' 1 3000 | awk '{ printf "%s-%01000d\n", $0, 0 }' |
  kafka kafka-console-producer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --topic "$topic"
prefix="/buckets/$(kubectl -n "$namespace" get kafka "$customer" \
  -o jsonpath='{.spec.kafka.tieredStorage.remoteStorageManager.config.storage\.s3\.bucket\.name}')/$namespace"
for _ in $(seq 60); do
  object_store_ls "$prefix/" | grep -q "^$topic-" && break
  sleep 5
done
object_store_ls "$prefix/" | grep -q "^$topic-" || die "no segment of $topic was offloaded within 5 minutes"
log "Topic $topic has segments in the object store under $prefix"

log "Turning deletion protection off and deprovisioning"
provisioner unprotect -customer "$customer" -confirm "$customer"
for _ in $(seq 30); do
  [[ -z $(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.changeport\.io/deletion-protection}') ]] && break
  sleep 2
done
[[ -z $(kubectl get namespace "$namespace" -o jsonpath='{.metadata.labels.changeport\.io/deletion-protection}') ]] ||
  die "namespace $namespace kept its deletion protection label after protection was turned off"
provisioner deprovision -customer "$customer" -wait -timeout 5m

object_store_ls "$prefix/" | grep -q "^$topic-" || die "deprovisioning removed the tiered storage of $customer under $prefix"
log "The tiered storage of $customer outlived its cluster under $prefix"

# The purge of docs/runbook.md, after 30 days; on kind the object store is
# SeaweedFS, and the volumes went with the namespace.
kubectl -n object-store exec deploy/seaweedfs -- sh -c "echo 'fs.rm -r $prefix' | weed shell >/dev/null 2>&1"
[[ -z $(object_store_ls "$prefix/") ]] || die "the purge left data under $prefix"
log "Deletion protection test passed for customer $customer"
