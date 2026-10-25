#!/usr/bin/env bash
# Tests client telemetry (KIP-714) on a customer's cluster on kind: a Java
# producer outside the cluster, which pushes its metrics to the brokers as
# every Java client does by default, sends 200 records as the customer's
# admin user and stays open for a few of the 5-second push intervals on kind. Its counts must reach the metrics store with the tenant, the
# Kafka user and the client ID, exactly as many as it sent, and only the
# metrics the agent's subscription asks for.
# Usage: hack/client-telemetry-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
run_id="$(date +%s)-$RANDOM"
topic="telemetry-$run_id"
client_id="telemetry-$run_id"
records=200
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
user=$(sed -nE 's/^sasl\.jaas\.config=.* username="([^"]+)".*/\1/p' "$work/client.properties")
[[ -n "$user" ]] || die "no user name in customer $customer's client.properties"
kafka() { docker run "${KAFKA_CLIENT[@]}" -i --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }

log "Client telemetry test for customer $customer: producing $records records as $client_id"
kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --create --topic "$topic" --partitions 1 >/dev/null
# Java producers skip the push KIP-714 asks for when a client closes, so the
# producer stays open until its regular pushes have counted every record.
{
  seq 1 "$records"
  sleep 20
} | kafka kafka-console-producer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --topic "$topic" --command-property "client.id=$client_id"

# The brokers keep a client's last push for a minute after it closes, and
# VictoriaMetrics scrapes every 30 seconds, so the queries look at the last
# five minutes rather than only the latest samples.
selector="tenant=\"$customer\", client_id=\"$client_id\""
recent() { metrics_query "$1(last_over_time($2[5m]))"; }
sent="clients_org_apache_kafka_producer_record_send_total{$selector}"
for _ in $(seq 24); do
  value=$(recent max "$sent")
  [[ ${value%.*} == "$records" ]] && break
  sleep 5
done
[[ ${value%.*} == "$records" ]] || die "the metrics store has ${value:-no} records sent by $client_id, want $records"
log "The producer's $records records reached the metrics store"

for label in cluster env node_id instance client_instance_id; do
  missing=$(recent count "{__name__=~\"clients_.*\", $selector, $label=\"\"}")
  [[ -z "$missing" ]] || die "$missing series of $client_id have no $label label"
done
others=$(recent count "{__name__=~\"clients_.*\", $selector, principal!=\"User:$user\"}")
[[ -z "$others" ]] || die "$others series of $client_id do not name its Kafka user $user"
others=$(recent count "{__name__=~\"clients_.*\", $selector, job!=\"kafka-clients\"}")
[[ -z "$others" ]] || die "$others series of $client_id do not come from the kafka-clients scrape job"
by_topic=$(recent max "clients_org_apache_kafka_producer_topic_record_send_total{$selector, topic=\"$topic\"}")
[[ ${by_topic%.*} == "$records" ]] || die "the metrics store has ${by_topic:-no} records sent to $topic, want $records"
# Java clients have many more metrics than the subscription asks for.
unsubscribed=$(recent count "{__name__=~\"clients_.*_rate\", $selector}")
[[ -z "$unsubscribed" ]] || die "$unsubscribed series of $client_id are not in the agent's subscription"
log "Client telemetry test passed: $client_id's metrics carry tenant, cluster, its user and client ID"
