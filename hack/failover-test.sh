#!/usr/bin/env bash
# Tests that a three-zone customer cluster keeps its clients working through
# the disruptions production clusters see, each under load: an idempotent
# acks=all producer and a consumer run throughout, and every message must
# arrive exactly once.
# - Zone outage: the worker of one broker's zone freezes (docker pause) for a
#   minute; clients carry on with the other two zones, and the cluster
#   recovers with all partitions in sync.
# - Node drain: a broker's eviction is denied by Strimzi Drain Cleaner, which
#   marks the pod for the Strimzi operator to restart instead.
# Usage: hack/failover-test.sh [customer-id] (default ha). The customer gets
# the dev-ha profile and is deprovisioned afterwards; KEEP=1 keeps it.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-ha}
namespace="customer-$customer"
work=$(mktemp -d)
paused=""
cleanup() {
  [[ -z "$paused" ]] || docker unpause "$paused" >/dev/null 2>&1 || true
  docker rm -f "$customer-producer" "$customer-consumer" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

ensure_kubectl
log "Provisioning customer $customer with the dev-ha profile"
provisioner provision -customer "$customer" -profile dev-ha -wait -timeout 15m
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
# The clients write their results here; they run as the image's own user.
mkdir -m 777 "$work/out"
KAFKA_CLIENT+=(-v "$work/out:/out")

kafka() { docker run "${KAFKA_CLIENT[@]}" --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }
consumed() { grep -c . "$work/out/$1-consumed" 2>/dev/null || true; }

# Creates topic and starts the load: a producer sending 1, 2, 3, ... until
# the file stop-<topic> appears, and a consumer reading them back. The
# console producer only retries 3 times by default; the producer here retries
# until delivery.timeout.ms like any Kafka client with default settings.
start_load() {
  local topic=$1
  kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
    --create --topic "$topic" --partitions 3 >/dev/null
  # shellcheck disable=SC2016 # expanded by the container's shell
  docker run -d --name "$customer-producer" "${KAFKA_CLIENT[@]}" -e TOPIC="$topic" -e BOOTSTRAP="$bootstrap" \
    --entrypoint bash "$KAFKA_IMAGE" -c '
      { i=0
        while [ ! -e "/out/stop-$TOPIC" ]; do i=$((i + 1)); echo "$i"; sleep 0.02; done
        echo "$i" >"/out/$TOPIC-produced"
      } | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
        --command-config /config/client.properties --producer-property acks=all \
        --producer-property enable.idempotence=true --producer-property retries=2147483647 \
        2>"/out/$TOPIC-producer.log"
      echo $? >"/out/$TOPIC-producer-exit"' >/dev/null
  # shellcheck disable=SC2016 # expanded by the container's shell
  docker run -d --name "$customer-consumer" "${KAFKA_CLIENT[@]}" -e TOPIC="$topic" -e BOOTSTRAP="$bootstrap" \
    --entrypoint bash "$KAFKA_IMAGE" -c '
      /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
        --command-config /config/client.properties --group "$TOPIC-check" --from-beginning \
        >"/out/$TOPIC-consumed" 2>"/out/$TOPIC-consumer.log"' >/dev/null
  for _ in $(seq 60); do
    [[ $(consumed "$topic") -gt 100 ]] && return
    sleep 1
  done
  die "the load on $topic did not start"
}

# Stops the producer, waits until the consumer has read everything produced,
# and checks that every message from 1 to the last arrived exactly once.
stop_load() {
  local topic=$1 produced exit_code
  touch "$work/out/stop-$topic"
  for _ in $(seq 120); do
    [[ -s "$work/out/$topic-producer-exit" ]] && break
    sleep 1
  done
  exit_code=$(cat "$work/out/$topic-producer-exit" 2>/dev/null) || die "the producer on $topic did not finish"
  produced=$(cat "$work/out/$topic-produced")
  if [[ $exit_code != 0 ]] || grep -E "ERROR|Error when sending" "$work/out/$topic-producer.log" >/dev/null; then
    cat "$work/out/$topic-producer.log" >&2
    die "the producer on $topic failed"
  fi
  for _ in $(seq 120); do
    [[ $(consumed "$topic") -ge $produced ]] && break
    sleep 1
  done
  # The producer has exited and removed itself by now.
  docker rm -f "$customer-producer" "$customer-consumer" >/dev/null 2>&1 || true
  local lines unique
  lines=$(consumed "$topic")
  unique=$(sort -un "$work/out/$topic-consumed" | grep -c .)
  if [[ $lines != "$produced" || $unique != "$produced" ]] ||
    [[ $(sort -n "$work/out/$topic-consumed" | head -1) != 1 || $(sort -n "$work/out/$topic-consumed" | tail -1) != "$produced" ]]; then
    tail -20 "$work/out/$topic-consumer.log" >&2
    die "on $topic, $produced messages were produced, but $lines were consumed ($unique distinct)"
  fi
  log "All $produced messages on $topic arrived exactly once"
}

# Waits until the Kafka cluster is ready and every partition is in sync.
wait_recovered() {
  kubectl -n "$namespace" wait kafka "$customer" --for=condition=Ready --timeout=10m >/dev/null
  kubectl -n "$namespace" wait pods -l "strimzi.io/cluster=$customer,strimzi.io/broker-role=true" --for=condition=Ready --timeout=10m >/dev/null
  for _ in $(seq 60); do
    [[ -z $(kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
      --describe --under-replicated-partitions 2>/dev/null) ]] && return
    sleep 5
  done
  die "partitions of customer $customer are still under-replicated"
}

broker_node() { kubectl -n "$namespace" get pod "$1" -o jsonpath='{.spec.nodeName}'; }

log "Zone outage: freezing the zone of broker $customer-dual-role-1 under load"
start_load zone-outage
node=$(broker_node "$customer-dual-role-1")
zone=$(kubectl get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
docker pause "$node" >/dev/null
paused=$node
before=$(consumed zone-outage)
sleep 60
during=$(consumed zone-outage)
[[ $during -gt $((before + 1000)) ]] ||
  die "clients stalled while $zone was down: $((during - before)) messages consumed in 60 seconds"
log "Clients kept working while $zone was down: $((during - before)) messages consumed in 60 seconds"
docker unpause "$node" >/dev/null
paused=""
stop_load zone-outage
wait_recovered
log "The cluster recovered from the outage of $zone"

log "Node drain: evicting broker $customer-dual-role-2 under load"
start_load node-drain
pod="$customer-dual-role-2"
uid=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.uid}')
if kubectl create --raw "/api/v1/namespaces/$namespace/pods/$pod/eviction" -f - >/dev/null 2>"$work/eviction.log" <<JSON; then
{"apiVersion": "policy/v1", "kind": "Eviction", "metadata": {"name": "$pod", "namespace": "$namespace"}}
JSON
  die "the eviction of $pod was allowed; Drain Cleaner should deny it"
fi
[[ $(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.annotations.strimzi\.io/manual-rolling-update}') == true ]] ||
  die "Drain Cleaner denied the eviction of $pod but did not mark it for a rolling restart: $(cat "$work/eviction.log")"
log "Drain Cleaner denied the eviction and marked $pod for the Strimzi operator"
for _ in $(seq 100); do
  current=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  [[ -n "$current" && "$current" != "$uid" ]] && break
  sleep 3
done
[[ -n "$current" && "$current" != "$uid" ]] || die "the Strimzi operator did not restart $pod"
wait_recovered
stop_load node-drain
log "The Strimzi operator restarted $pod without losing a message"

if [[ ${KEEP:-0} != 1 ]]; then
  log "Deprovisioning customer $customer"
  provisioner deprovision -customer "$customer" -wait -timeout 5m
fi
log "Failover test passed"
