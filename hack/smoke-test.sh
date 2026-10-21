#!/usr/bin/env bash
# Produces one message to a customer's Kafka cluster and consumes it back.
# Usage: hack/smoke-test.sh <customer-id>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:?usage: hack/smoke-test.sh <customer-id>}
namespace="customer-$customer"
bootstrap="$customer-kafka-bootstrap:9092"
run_id="$(date +%s)-$RANDOM"
topic="smoke-$run_id"
message="changeport smoke test $run_id"

ensure_kubectl

# Use the broker image so the client tools match the Kafka version; dev-up
# preloads it into every node.
image=$(kubectl -n "$namespace" get pods -l "strimzi.io/cluster=$customer,strimzi.io/broker-role=true" \
  -o jsonpath='{.items[0].spec.containers[0].image}')
[[ -n "$image" ]] || die "no Kafka broker pods for customer $customer in namespace $namespace"

cleanup() {
  kubectl -n "$namespace" delete pod "$topic-producer" "$topic-consumer" --ignore-not-found --wait=false >/dev/null
  kubectl -n "$namespace" delete kafkatopic "$topic" --ignore-not-found --timeout=2m >/dev/null
}
trap cleanup EXIT

# Runs a shell command in a one-off pod, waits for it to finish and prints its
# logs. Fails if the pod fails or does not finish in time.
run_pod() {
  local name=$1 command=$2 phase=""
  kubectl -n "$namespace" run "$name" --image="$image" --restart=Never --quiet --command -- sh -c "$command" >/dev/null
  for _ in $(seq 120); do
    phase=$(kubectl -n "$namespace" get pod "$name" -o jsonpath='{.status.phase}')
    [[ $phase == Succeeded || $phase == Failed ]] && break
    sleep 1
  done
  if [[ $phase != Succeeded && $phase != Failed ]]; then
    kubectl -n "$namespace" get events --field-selector "involvedObject.name=$name" >&2
    die "pod $name did not finish within 2 minutes (phase '${phase:-unknown}')"
  fi
  kubectl -n "$namespace" logs "$name"
  [[ $phase == Succeeded ]] || die "pod $name ended in phase '$phase'"
}

log "Smoke test for customer $customer: creating topic $topic"
kubectl -n "$namespace" apply -f - >/dev/null <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: $topic
  labels:
    strimzi.io/cluster: $customer
spec:
  partitions: 1
YAML
kubectl -n "$namespace" wait kafkatopic "$topic" --for=condition=Ready --timeout=2m >/dev/null

log "Producing a message"
run_pod "$topic-producer" \
  "echo '$message' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $bootstrap --topic $topic"

log "Consuming it back"
consumed=$(run_pod "$topic-consumer" \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $bootstrap --topic $topic --from-beginning --max-messages 1 --timeout-ms 60000")

if ! grep -qxF "$message" <<<"$consumed"; then
  printf '%s\n' "$consumed" >&2
  die "smoke test failed: did not consume the produced message"
fi
log "Smoke test passed: produced and consumed '$message' on $bootstrap"
