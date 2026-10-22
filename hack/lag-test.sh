#!/usr/bin/env bash
# Tests consumer lag in seconds on a customer's cluster on kind. 100 messages
# are produced at a known moment, so the oldest unconsumed message of a
# group is exactly as old as the time since. A classic consumer group is set
# to the start of the topic, and a KIP-848 consumer group consumes half of
# it. The lag the agent reports through the metrics store must lie between
# the true lag, less the agent and scrape intervals it may be behind by, and
# the first anchor at or above the true lag, where the time index estimate
# lands when all messages share one moment.
# Usage: hack/lag-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
run_id="$(date +%s)-$RANDOM"
topic="lag-$run_id"
classic="lag-classic-$run_id"
kip848="lag-kip848-$run_id"
# The anchors of internal/agent.Anchors up to an hour, in seconds.
anchors=(15 30 45 60 90 120 180 300 450 600 900 1200 1800 2700 3600)
# The agent measures and VictoriaMetrics scrapes every 30 seconds, plus slack.
staleness=65
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
kafka() { docker run "${KAFKA_CLIENT[@]}" -i --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }

log "Lag test for customer $customer: producing 100 messages to $topic"
kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --create --topic "$topic" --partitions 1 >/dev/null
seq 1 100 | kafka kafka-console-producer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --topic "$topic"
produced=$(date +%s)

log "A classic group at the start of the topic, and a KIP-848 group halfway"
kafka kafka-consumer-groups.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --group "$classic" --topic "$topic" --reset-offsets --to-earliest --execute >/dev/null
kafka kafka-console-consumer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --topic "$topic" --group "$kip848" --consumer-property group.protocol=consumer \
  --from-beginning --max-messages 50 --timeout-ms 60000 >/dev/null 2>&1

# Prints the reported lag of group in seconds and in messages, or nothing.
reported() {
  local seconds messages
  seconds=$(metrics_query "changeport_consumergroup_lag_seconds{tenant=\"$customer\", consumer_group=\"$1\", topic=\"$topic\"}")
  messages=$(metrics_query "changeport_consumergroup_lag_messages{tenant=\"$customer\", consumer_group=\"$1\", topic=\"$topic\"}")
  [[ -n "$seconds" && -n "$messages" ]] && echo "$seconds $messages"
}

check() {
  local group=$1 want_messages=$2 seconds messages true_age lower upper anchor
  for _ in $(seq 36); do
    # Nothing reported yet is not an error: poll again.
    read -r seconds messages < <(reported "$group") || true
    [[ ${messages:-} == "$want_messages" ]] && break
    sleep 5
  done
  [[ ${messages:-} == "$want_messages" ]] || die "group $group: reported ${messages:-no} messages of lag, want $want_messages"
  true_age=$(($(date +%s) - produced))
  lower=$((true_age - staleness))
  upper=0
  for anchor in "${anchors[@]}"; do
    if [[ $anchor -ge $true_age ]]; then
      upper=$anchor
      break
    fi
  done
  [[ $upper -gt 0 ]] || die "the test ran longer than its anchors cover"
  seconds=${seconds%.*}
  if [[ $seconds -lt $lower || $seconds -gt $upper ]]; then
    die "group $group: reported $seconds seconds of lag, want between $lower and $upper (true lag $true_age seconds)"
  fi
  log "Group $group: $messages messages and $seconds seconds behind (true lag $true_age seconds, allowed $lower to $upper)"
}

check "$classic" 100
check "$kip848" 50
# The console's lag queries see the groups while they exist.
check_named_queries "$customer" lag_seconds,lag_messages

kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --delete --topic "$topic"
for group in "$classic" "$kip848"; do
  kafka kafka-consumer-groups.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
    --delete --group "$group" >/dev/null
done
log "Lag test passed for customer $customer"
