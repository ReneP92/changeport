#!/usr/bin/env bash
# Tests a Postgres CDC source end to end on kind (docs/cdc.md), against the
# Postgres that stands in for a customer's database (deploy/base/source-postgres):
# it runs the setup SQL the console generates as the customer would, writes
# the password Secret and the PostgresSource as the console does, and waits
# for the source to copy its tables' existing rows and stream. Inserts,
# updates and deletes then reach the tables' topics, deletes with a
# tombstone, and a Confluent client decodes them through the registry
# gateway, which refuses changes to the source's schemas and wrong
# credentials; a stopped source keeps its slot and catches up once it runs
# again; an added table's rows arrive, and only its; a removed table stops
# streaming and keeps its topic; and deleting the source drops its slot and
# its credentials.
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
# A partitioned table of the test's own, keyed by a date as partitioned
# tables usually are, which the test adds to the source.
invoices="invoices_$id"
# Every table the source has at some point, for cleaning up.
all_tables="public.customers,public.orders,public.$invoices"
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

# Calls the schema registry's Confluent-compatible API from the Connect worker.
registry() {
  kubectl -n "$namespace" exec changeport-connect-connect-0 -- curl -sf "http://changeport-registry:8080/apis/ccompat/v7$1" "${@:2}" 2>/dev/null
}
kafka() { docker run "${KAFKA_CLIENT[@]}" --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" --bootstrap-server "$bootstrap" "${@:2}"; }
cleanup() {
  kubectl -n "$namespace" delete postgressource "$source_name" --ignore-not-found --timeout=3m >/dev/null 2>&1 || true
  cdcsql -step cleanup -tables "$all_tables" | pg >/dev/null 2>&1 || true
  pg -c "DROP TABLE IF EXISTS $invoices" >/dev/null 2>&1 || true
  local topic
  for topic in "$source_name.public.customers" "$source_name.public.orders" "$source_name.public.$invoices" "changeport.cdc.heartbeat.$source_name"; do
    kafka kafka-topics.sh --command-config /config/client.properties --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
  done
  # The schemas of the source's topics, which the registry keeps. Only the
  # Connect worker reaches the registry.
  local subject
  for subject in $(registry /subjects | tr -d '[]"' | tr ',' '\n' | grep -E "(^|[.])(${source_name}|${source_name//-/_})[.-]"); do
    registry "/subjects/$subject" -X DELETE >/dev/null || true
    registry "/subjects/$subject?permanent=true" -X DELETE >/dev/null || true
  done
  rm -rf "$work"
}
trap cleanup EXIT
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")

log "CDC test for customer $customer: setting up source $source_name in the stand-in database"
pg -c "CREATE TABLE $invoices (id bigint, issued_on date, total numeric(10, 2), PRIMARY KEY (id, issued_on)) PARTITION BY RANGE (issued_on);
  CREATE TABLE ${invoices}_2026 PARTITION OF $invoices FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
  CREATE TABLE ${invoices}_2027 PARTITION OF $invoices FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
  INSERT INTO $invoices SELECT g, date '2026-06-01' + (g % 400), g FROM generate_series(1, 2500) g"
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
log "The tables' existing rows arrived first"
customers_rows=$(pg -c "SELECT count(*) FROM customers")
orders_rows=$(pg -c "SELECT count(*) FROM orders")
if [[ $(source_field '.status.tables[?(@.name=="customers")].rowsCopied') != "$customers_rows" ||
  $(source_field '.status.tables[?(@.name=="orders")].rowsCopied') != "$orders_rows" ||
  $(source_field '.status.tables[*].snapshot') != "Done Done" ]]; then
  # The snapshot's notifications, with Avro's bytes as spaces.
  kafka kafka-console-consumer.sh --command-config /config/client.properties --topic "changeport.cdc.notifications.$source_name" \
    --partition 0 --offset earliest --timeout-ms 10000 2>/dev/null | LC_ALL=C tr -c '[:print:]\n' ' ' >&2 || true
  kubectl -n "$namespace" logs changeport-connect-connect-0 2>&1 | grep -F "$source_name" |
    grep -iE "incremental|chunk|snapshot|signal|task|restart" | grep -v "Context is null" | tail -60 >&2 || true
  die "the snapshot's status is $(source_field .status.tables), want $customers_rows customers and $orders_rows orders copied"
fi
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
# A read-only snapshot reads a chunk again while transactions commit during
# it, so a row may arrive twice: at least once.
customers_snapshot=$(message_count "$source_name.public.customers")
orders_snapshot=$(message_count "$source_name.public.orders")
[[ $customers_snapshot -ge $customers_rows && $orders_snapshot -ge $orders_rows ]] ||
  die "the topics hold $customers_snapshot customers and $orders_snapshot orders, want each table's rows at least once"
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
[[ $count == $((customers_snapshot + 4)) ]] || die "$customers_topic has $count messages, want the snapshot's, then the insert, the update, the delete and its tombstone"
count=$(message_count "$source_name.public.orders")
[[ $count == $((orders_snapshot + 3)) ]] || die "$source_name.public.orders has $count messages, want the snapshot's, then the insert, the delete and its tombstone"
partitions=$(kafka kafka-topics.sh --command-config /config/client.properties --describe --topic "$customers_topic" 2>/dev/null | sed -n 1p)
if ! grep -q "PartitionCount: 1" <<<"$partitions" || ! grep -q "retention.ms=3600000" <<<"$partitions"; then
  die "$customers_topic is not one partition with the source's retention: $partitions"
fi

log "A client decodes the messages through the registry gateway, with the customer's credentials"
registry_host="registry.$customer.kafka.changeport.test"
for _ in $(seq 60); do
  gateway_address=$(kubectl -n "$namespace" get service changeport-registry-gateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
  [[ -n $gateway_address ]] && break
  sleep 2
done
[[ -n $gateway_address ]] || die "the registry gateway's load balancer has no address"
# The local CA, from the truststore lines of client.properties.
sed -n '/BEGIN CERTIFICATE/,/END CERTIFICATE/p' "$work/client.properties" |
  sed -E 's/^ssl\.truststore\.certificates=//; s/^ +//; s/ \\$//' >"$work/ca.crt"
cat >"$work/decode.py" <<'PYTHON'
# Reads a topic as a customer's consumer would, decoding its Avro through the
# registry gateway, then checks what the gateway refuses.
import json, os, sys, urllib.error, urllib.request, base64, ssl
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

registry_url, user, password, topic = os.environ["REGISTRY"], os.environ["KAFKA_USERNAME"], os.environ["KAFKA_PASSWORD"], os.environ["TOPIC"]
registry = SchemaRegistryClient({"url": registry_url, "basic.auth.user.info": f"{user}:{password}", "ssl.ca.location": "/config/ca.crt"})
deserialize = AvroDeserializer(registry)
consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "security.protocol": "SASL_SSL", "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": user, "sasl.password": password, "ssl.ca.location": "/config/ca.crt",
    "group.id": "cdc-test-unused", "enable.auto.commit": False,
})
low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=30)
consumer.assign([TopicPartition(topic, 0, low)])
read = 0
while read < high - low:
    message = consumer.poll(30)
    if message is None:
        sys.exit(f"read {read} of {high - low} messages")
    if message.error():
        sys.exit(str(message.error()))
    read += 1
    key = deserialize(message.key(), SerializationContext(topic, MessageField.KEY))
    value = deserialize(message.value(), SerializationContext(topic, MessageField.VALUE)) if message.value() else None
    print(json.dumps({"key": key, "value": value}, default=str))
consumer.close()

def call(method, path, credentials):
    request = urllib.request.Request(registry_url + path, method=method)
    request.add_header("Authorization", "Basic " + base64.b64encode(credentials.encode()).decode())
    try:
        with urllib.request.urlopen(request, context=ssl.create_default_context(cafile="/config/ca.crt")) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code

subject = topic + "-value"
checks = [
    ("deleting a source's subject", call("DELETE", f"/subjects/{subject}", f"{user}:{password}"), 403),
    ("changing its compatibility", call("PUT", f"/config/{subject}", f"{user}:{password}"), 403),
    ("reading it", call("GET", f"/subjects/{subject}/versions/latest", f"{user}:{password}"), 200),
    ("a wrong password", call("GET", "/subjects", f"{user}:wrong"), 401),
]
# Ten failures lock the client out for a while, even with the right password.
for _ in range(9):
    call("GET", "/subjects", f"{user}:wrong")
checks.append(("the right password after ten failures", call("GET", "/subjects", f"{user}:{password}"), 429))
for name, got, want in checks:
    if got != want:
        sys.exit(f"{name}: the gateway answered {got}, want {want}")
print("GATEWAY CHECKS PASSED")
PYTHON
username=$(sed -nE 's/.*username="([^"]+)".*/\1/p' "$work/client.properties")
kafka_password=$(sed -nE 's/.*password="([^"]+)".*/\1/p' "$work/client.properties")
decoded=$(docker run "${KAFKA_CLIENT[@]}" --add-host "$registry_host:$gateway_address" \
  -e "REGISTRY=https://$registry_host" -e "KAFKA_BOOTSTRAP=$bootstrap" -e "KAFKA_USERNAME=$username" -e "KAFKA_PASSWORD=$kafka_password" \
  -e "TOPIC=$customers_topic" "$PYTHON_IMAGE" sh -c \
  "pip install -q --root-user-action=ignore --disable-pip-version-check 'confluent-kafka[avro]==$CONFLUENT_KAFKA_PYTHON' && python /config/decode.py" 2>&1) ||
  die "decoding through the registry gateway failed:"$'\n'"$decoded"
grep -q "GATEWAY CHECKS PASSED" <<<"$decoded" || die "the registry gateway's checks did not finish:"$'\n'"$decoded"
if ! grep -q "\"name\": \"$marker-updated\"" <<<"$decoded" || ! grep -q "\"value\": null" <<<"$decoded"; then
  die "the decoded messages lack the update or the tombstone:"$'\n'"$decoded"
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

# Waits until a table's snapshot is in a state.
wait_snapshot() {
  local state
  for _ in $(seq 60); do
    state=$(source_field ".status.tables[?(@.name==\"$1\")].snapshot")
    [[ $state == "$2" ]] && return
    sleep 4
  done
  die "the snapshot of table $1 is \"$state\", not $2: $(source_field .status.conditions)"
}

log "An added table's existing rows arrive, and only its"
wait_message "$customers_topic" "$marker-while-stopped"
customers_before=$(message_count "$customers_topic")
cdcsql -step add -tables "public.$invoices" | pg
kubectl -n "$namespace" patch postgressource "$source_name" --type json \
  -p "[{\"op\":\"add\",\"path\":\"/spec/tables/-\",\"value\":{\"schema\":\"public\",\"name\":\"$invoices\"}}]" >/dev/null
wait_snapshot "$invoices" Done
invoices_rows=$(pg -c "SELECT count(*) FROM $invoices")
[[ $(source_field ".status.tables[?(@.name==\"$invoices\")].rowsCopied") == "$invoices_rows" &&
  $(message_count "$source_name.public.$invoices") -ge $invoices_rows ]] ||
  die "$source_name.public.$invoices has $(message_count "$source_name.public.$invoices") messages, want the partitioned table's $invoices_rows rows"
[[ $(message_count "$customers_topic") == "$customers_before" ]] || die "adding a table copied customers again"
wait_phase Streaming

log "A removed table stops streaming, and keeps its topic"
orders_before=$(message_count "$source_name.public.orders")
cdcsql -step remove -tables public.orders -remaining "public.customers,public.$invoices" | pg
kubectl -n "$namespace" patch postgressource "$source_name" --type merge \
  -p "{\"spec\":{\"tables\":[{\"schema\":\"public\",\"name\":\"customers\"},{\"schema\":\"public\",\"name\":\"$invoices\"}]}}" >/dev/null
for _ in $(seq 30); do
  [[ $(source_field '.status.tables[*].name') == "customers $invoices" ]] && break
  sleep 2
done
[[ $(source_field '.status.tables[*].name') == "customers $invoices" ]] || die "the source's tables are $(source_field '.status.tables[*].name') after removing orders"
# Kafka Connect restarts the connector without the table.
connector_field() { kubectl -n "$namespace" get kafkaconnector "$source_name-$id" -o jsonpath="{$1}"; }
for _ in $(seq 60); do
  if [[ $(connector_field '.spec.config.table\.include\.list') != *orders* &&
    $(connector_field .status.observedGeneration) == $(connector_field .metadata.generation) &&
    $(connector_field '.status.connectorStatus.tasks[0].state') == RUNNING ]]; then
    break
  fi
  sleep 2
done
# Changes to both tables in one transaction: once the customer's arrives,
# the order's would have too.
pg -c "BEGIN; INSERT INTO customers (id, name, email) VALUES (900003, '$marker-after-removal', '$marker-3@example.com');
  INSERT INTO orders (id, customer_id, amount) VALUES (900003, 900003, 1); COMMIT"
wait_message "$customers_topic" "$marker-after-removal"
[[ $(message_count "$source_name.public.orders") == "$orders_before" ]] || die "a removed table's change reached its topic"
pg -c "DELETE FROM orders WHERE id = 900003; DELETE FROM customers WHERE id = 900003"

log "Deleting the source drops its slot and its credentials"
kubectl -n "$namespace" delete postgressource "$source_name" --timeout=3m >/dev/null
[[ -z $(slot_active) ]] || die "the slot $slot is left after the source was deleted"
if kubectl -n "$namespace" get secret changeport-connect-sources -o jsonpath='{.data}' | grep -q "\"$source_name\""; then
  die "source $source_name's password is left in the Connect worker's Secret"
fi
log "CDC test passed: source $source_name copied its tables and streamed their changes, paused and resumed, added and removed a table, and dropped its slot"
