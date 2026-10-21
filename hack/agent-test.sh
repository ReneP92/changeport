#!/usr/bin/env bash
# Tests the Kafka agent on a customer's cluster on kind: of three topics
# created through the Admin API, as a customer would, the agent turns on
# tiered storage for the plain one within a few reconcile intervals, leaves
# the compacted one alone (Kafka cannot tier it) and keeps the customer's
# explicit remote.storage.enable=false; its metrics reach the metrics store
# with the tenant.
# Usage: hack/agent-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
run_id="$(date +%s)-$RANDOM"
plain="agent-plain-$run_id"
compacted="agent-compacted-$run_id"
opted_out="agent-opted-out-$run_id"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
kafka() { docker run "${KAFKA_CLIENT[@]}" --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }
topics() { kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties "$@"; }
# Prints the topic's remote.storage.enable if it is set on the topic itself.
remote_storage() {
  kafka kafka-configs.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
    --describe --entity-type topics --entity-name "$1" 2>/dev/null |
    sed -nE 's/^ *remote\.storage\.enable=([a-z]+) .*/\1/p'
}

log "Agent test for customer $customer: creating a plain, a compacted and an opted-out topic"
topics --create --topic "$plain" --partitions 1 >/dev/null
topics --create --topic "$compacted" --partitions 1 --config cleanup.policy=compact >/dev/null
topics --create --topic "$opted_out" --partitions 1 --config remote.storage.enable=false >/dev/null

for _ in $(seq 24); do
  [[ $(remote_storage "$plain") == true ]] && break
  sleep 5
done
[[ $(remote_storage "$plain") == true ]] || die "the agent did not turn on tiered storage for $plain within 2 minutes"
log "The agent turned on tiered storage for $plain"
[[ -z $(remote_storage "$compacted") ]] || die "the agent changed the compacted topic $compacted"
[[ $(remote_storage "$opted_out") == false ]] || die "the agent overrode the customer's remote.storage.enable=false on $opted_out"
log "The agent left the compacted topic and the customer's opt-out alone"

for _ in $(seq 24); do
  enabled=$(metrics_query "sum(changeport_agent_tiering_enabled_total{tenant=\"$customer\"})")
  [[ ${enabled%.*} -ge 1 ]] && break
  sleep 5
done
[[ ${enabled%.*} -ge 1 ]] || die "the metrics store has no tiering by the agent of customer $customer"
[[ -z $(metrics_query 'count(changeport_agent_topics{tenant=""})') ]] || die "agent metrics without a tenant"

topics --delete --topic "$plain,$compacted,$opted_out" >/dev/null 2>&1 ||
  for topic in "$plain" "$compacted" "$opted_out"; do topics --delete --topic "$topic" >/dev/null; done
log "Agent test passed: topics tiered so far by the agent of customer $customer: $enabled"
