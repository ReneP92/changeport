#!/usr/bin/env bash
# Tests a Postgres CDC source end to end on kind (docs/cdc.md), against the
# Postgres that stands in for a customer's database (deploy/base/source-postgres):
# it runs the setup SQL the console generates as the customer would, writes
# the password Secret and the PostgresSource as the console does, and waits
# for the source to stream. Inserts, updates and deletes then reach the
# tables' topics, deletes with a tombstone; a stopped source keeps its slot
# and catches up once it runs again; and deleting the source drops its slot
# and its credentials.
# Usage: hack/cdc-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
namespace=customer-$customer
source_name="shop-$RANDOM"
id=$(random_string a-z0-9 6)
password=$(random_string A-Za-z0-9 32)
slot="changeport_${source_name//-/_}_$id"
tables=public.customers,public.orders
marker="cdc-$RANDOM"
work=$(mktemp -d)

ensure_kubectl
# Runs SQL on the stand-in database as its owner, as the customer runs ours.
pg() { kubectl -n source-postgres exec -i deploy/postgres -- psql -U postgres -d app -v ON_ERROR_STOP=1 -qtA "$@"; }
cdcsql() { go run ./cmd/cdcsql -source "$source_name" -id "$id" "$@"; }
source_field() { kubectl -n "$namespace" get postgressource "$source_name" -o jsonpath="{$1}" 2>/dev/null; }
# Waits until the source reaches a phase.
wait_phase() {
  local phase
  for _ in $(seq 90); do
    phase=$(source_field .status.phase)
    [[ $phase == "$1" ]] && return
    sleep 4
  done
  die "source $source_name is in phase \"$phase\", not $1, after 6 minutes: $(source_field '.status.conditions[?(@.type=="Ready")].message')"
}
slot_active() { pg -c "SELECT active FROM pg_replication_slots WHERE slot_name = '$slot'"; }

kafka() { docker run "${KAFKA_CLIENT[@]}" --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" --bootstrap-server "$bootstrap" "${@:2}"; }
cleanup() {
  kubectl -n "$namespace" delete postgressource "$source_name" --ignore-not-found --timeout=3m >/dev/null 2>&1 || true
  cdcsql -step cleanup -tables "$tables" | pg >/dev/null 2>&1 || true
  local topic
  for topic in "$source_name.public.customers" "$source_name.public.orders" "changeport.cdc.heartbeat.$source_name"; do
    kafka kafka-topics.sh --command-config /config/client.properties --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
  done
  rm -rf "$work"
}
trap cleanup EXIT
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")

log "CDC test for customer $customer: setting up source $source_name in the stand-in database"
cdcsql -step role -password "$password" | pg
cdcsql -step tables -tables "$tables" | pg

log "Creating source $source_name as the console does"
kubectl apply -f - >/dev/null <<SECRET
apiVersion: v1
kind: Secret
metadata:
  name: postgres-password-$source_name-1
  namespace: $namespace
type: changeport.io/postgres-password
immutable: true
stringData:
  password: $password
SECRET
ca=$(kubectl -n source-postgres get secret postgres-tls -o jsonpath='{.data.ca\.crt}' | base64 -d | sed 's/^/        /')
kubectl apply -f - >/dev/null <<SOURCE
apiVersion: changeport.io/v1alpha1
kind: PostgresSource
metadata:
  name: $source_name
  namespace: $namespace
spec:
  id: $id
  provider: generic
  connection:
    host: postgres.source-postgres.svc
    database: app
    passwordSecret: postgres-password-$source_name-1
    tls:
      mode: verify-full
      ca: |
$ca
  tables:
    - {schema: public, name: customers}
    - {schema: public, name: orders}
  retentionMs: 3600000
SOURCE
wait_phase Streaming
if kubectl -n "$namespace" get secret "postgres-password-$source_name-1" >/dev/null 2>&1; then
  die "the console's password Secret is left after the handover"
fi
[[ $(slot_active) == t ]] || die "the source streams, but its slot $slot is not active"

# Prints the topic's messages as key|value, a null value for a tombstone,
# with the bytes of Avro as they are. It reads the one partition directly,
# which leaves no consumer group behind.
messages() {
  kafka kafka-console-consumer.sh --command-config /config/client.properties --topic "$1" --partition 0 --offset earliest \
    --timeout-ms 15000 --formatter-property print.key=true --formatter-property key.separator='|' 2>/dev/null |
    LC_ALL=C tr -d '\000' || true
}
# Prints how many messages the topic has.
message_count() {
  kafka kafka-get-offsets.sh --command-config /config/client.properties --topic "$1" 2>/dev/null | cut -d: -f3
}
# Waits until the topic has a message containing text. Avro's bytes are no
# text, so grep reads them in the C locale.
wait_message() {
  local seen
  for _ in $(seq 20); do
    seen=$(messages "$1")
    LC_ALL=C grep -qF -- "$2" <<<"$seen" && return
    sleep 3
  done
  die "topic $1 has no message with \"$2\":"$'\n'"$(LC_ALL=C tr -cd '[:print:]\n' <<<"$seen")"
}

log "Inserts, updates and deletes reach the tables' topics"
pg -c "INSERT INTO customers (id, name, email) VALUES (900001, '$marker-inserted', '$marker@example.com')"
pg -c "UPDATE customers SET name = '$marker-updated' WHERE id = 900001"
pg -c "INSERT INTO orders (id, customer_id, amount) VALUES (900001, 900001, 42.50)"
pg -c "DELETE FROM orders WHERE id = 900001"
pg -c "DELETE FROM customers WHERE id = 900001"
customers_topic="$source_name.public.customers"
wait_message "$customers_topic" "$marker-updated"
seen=$(messages "$customers_topic")
LC_ALL=C grep -qF "$marker-inserted" <<<"$seen" || die "the insert is missing from $customers_topic"
# The delete's event, then its tombstone: a key with a null value.
LC_ALL=C grep -q '|null$' <<<"$seen" || die "$customers_topic has no tombstone for the deleted row"
count=$(message_count "$customers_topic")
[[ $count == 4 ]] || die "$customers_topic has $count messages, want the insert, the update, the delete and its tombstone"
count=$(message_count "$source_name.public.orders")
[[ $count == 3 ]] || die "$source_name.public.orders has $count messages, want the insert, the delete and its tombstone"
partitions=$(kafka kafka-topics.sh --command-config /config/client.properties --describe --topic "$customers_topic" 2>/dev/null | sed -n 1p)
if ! grep -q "PartitionCount: 1" <<<"$partitions" || ! grep -q "retention.ms=3600000" <<<"$partitions"; then
  die "$customers_topic is not one partition with the source's retention: $partitions"
fi

log "The slot's health is in the source's status"
# The controller reads the slot every minute once it exists.
for _ in $(seq 45); do
  [[ $(source_field .status.slot.active) == true ]] && break
  sleep 4
done
[[ $(source_field .status.slot.name) == "$slot" && $(source_field .status.slot.active) == true ]] ||
  die "the source's slot status is \"$(source_field .status.slot)\", not its active slot $slot"

log "A stopped source keeps its slot, and catches up once it runs again"
kubectl -n "$namespace" patch postgressource "$source_name" --type merge -p '{"spec":{"state":"Stopped"}}' >/dev/null
wait_phase Stopped
for _ in $(seq 30); do
  [[ $(slot_active) == f ]] && break
  sleep 2
done
[[ $(slot_active) == f ]] || die "the stopped source's slot $slot is still active, or gone: \"$(slot_active)\""
pg -c "INSERT INTO customers (id, name, email) VALUES (900002, '$marker-while-stopped', '$marker-2@example.com')"
kubectl -n "$namespace" patch postgressource "$source_name" --type merge -p '{"spec":{"state":"Running"}}' >/dev/null
wait_phase Streaming
wait_message "$customers_topic" "$marker-while-stopped"
pg -c "DELETE FROM customers WHERE id = 900002"

log "Deleting the source drops its slot and its credentials"
kubectl -n "$namespace" delete postgressource "$source_name" --timeout=3m >/dev/null
[[ -z $(slot_active) ]] || die "the slot $slot is left after the source was deleted"
if kubectl -n "$namespace" get secret changeport-connect-sources -o jsonpath='{.data}' | grep -q "\"$source_name\""; then
  die "source $source_name's password is left in the Connect worker's Secret"
fi
log "CDC test passed: source $source_name streamed inserts, updates and deletes, paused and resumed, and dropped its slot"
