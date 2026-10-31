#!/usr/bin/env bash
# Tests the runbook's recovery of a Postgres CDC source whose replication
# slot the database lost (docs/runbook.md), end to end on kind, against the
# Postgres that stands in for a customer's database: a paused source holds
# back more WAL than max_slot_wal_keep_size allows, the database invalidates
# its slot, and once resumed the source fails and its alerts fire. Recovered
# as the runbook says (paused, the lost slot dropped, the connector's
# offsets reset, its tables copied again, resumed), it streams again from a
# new slot, and its alerts are quiet.
# Usage: hack/cdc-recovery-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
namespace=customer-$customer
source_name="lost-$RANDOM"
id=$(random_string a-z0-9 6)
password=$(random_string A-Za-z0-9 32)
slot="changeport_${source_name//-/_}_$id"
connector="$source_name-$id"
tables=public.customers
marker="recovery-$RANDOM"
work=$(mktemp -d)

ensure_kubectl
# Runs SQL on the stand-in database as its owner, as the customer runs ours.
pg() { kubectl -n source-postgres exec -i deploy/postgres -- psql -U postgres -d app -v ON_ERROR_STOP=1 -qtA "$@"; }
cdcsql() { go run ./cmd/cdcsql -source "$source_name" -id "$id" "$@"; }
source_field() { kubectl -n "$namespace" get postgressource "$source_name" -o jsonpath="{$1}" 2>/dev/null; }
set_state() { kubectl -n "$namespace" patch postgressource "$source_name" --type merge -p "{\"spec\":{\"state\":\"$1\"}}" >/dev/null; }
# Waits up to 6 minutes until the source reaches a phase.
wait_phase() {
  local phase
  for _ in $(seq 90); do
    phase=$(source_field .status.phase)
    [[ $phase == "$1" ]] && return
    sleep 4
  done
  kubectl -n "$namespace" get kafkaconnector "$connector" -o jsonpath='{.status}' >&2 || true
  echo >&2
  kubectl -n "$namespace" logs changeport-connect-connect-0 --since=10m 2>&1 | grep -F "$source_name" | grep -E "ERROR|WARN|Exception" | tail -20 >&2 || true
  die "source $source_name is in phase \"$phase\", not $1: $(source_field '.status.conditions[?(@.type=="Ready")].message')"
}
slot_column() { pg -c "SELECT $1 FROM pg_replication_slots WHERE slot_name = '$slot'"; }
kafka() { docker run "${KAFKA_CLIENT[@]}" --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" --bootstrap-server "$bootstrap" "${@:2}"; }
cleanup() {
  # The stand-in database keeps a 1 GB limit, as the setup guide recommends.
  pg -c "ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'" -c "SELECT pg_reload_conf()" >/dev/null 2>&1 || true
  pg -c "DROP TABLE IF EXISTS wal_filler_$id" >/dev/null 2>&1 || true
  kubectl -n "$namespace" delete postgressource "$source_name" --ignore-not-found --timeout=3m >/dev/null 2>&1 || true
  cdcsql -step cleanup -tables "$tables" | pg >/dev/null 2>&1 || true
  pg -c "SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name = '$slot' AND NOT active" >/dev/null 2>&1 || true
  kafka kafka-topics.sh --command-config /config/client.properties --delete --if-exists --topic "$source_name.public.customers" >/dev/null 2>&1 || true
  kafka kafka-topics.sh --command-config /config/client.properties --delete --if-exists --topic "changeport.cdc.heartbeat.$source_name" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")

log "Recovery test for customer $customer: source $source_name streams"
cdcsql -step role -password "$password" | pg
cdcsql -step tables -tables "$tables" | pg
create_source_as_console "$namespace" "$source_name" "$id" "$password" "$tables" 3600000
wait_phase Streaming
check_source_alerts "$customer" "$source_name" ""

log "The paused source holds back more WAL than the database allows, which invalidates its slot"
set_state Stopped
wait_phase Stopped
for _ in $(seq 60); do
  [[ $(slot_column active) == f ]] && break
  sleep 2
done
[[ $(slot_column active) == f ]] || die "the paused source's slot $slot is still active"
pg -c "ALTER SYSTEM SET max_slot_wal_keep_size = '16MB'" -c "SELECT pg_reload_conf()" >/dev/null
# 80 MB of WAL, then a checkpoint, which removes the WAL past the limit.
pg -c "CREATE TABLE wal_filler_$id AS SELECT g, repeat('x', 1000) AS pad FROM generate_series(1, 80000) g" \
  -c "DROP TABLE wal_filler_$id" -c "CHECKPOINT" >/dev/null
[[ $(slot_column wal_status) == lost ]] || die "the slot $slot is $(slot_column wal_status), not lost"
pg -c "ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'" -c "SELECT pg_reload_conf()" >/dev/null

log "Resumed, the source fails, says why, and its alerts fire"
set_state Running
wait_phase Failed
message=$(source_field '.status.conditions[?(@.type=="Ready")].message')
grep -q "lost the replication slot $slot" <<<"$message" || die "the failed source says: $message"
check_source_alerts "$customer" "$source_name" PostgresSourceSlotLost,PostgresSourceFailed

log "Recovering as the runbook says"
# 1. Pause the source, which stops its connector.
set_state Stopped
wait_phase Stopped
# 2. The customer drops the lost slot, with the SQL the console shows.
pg -c "SELECT pg_drop_replication_slot('$slot')" >/dev/null
# 3. Strimzi resets the stopped connector's offsets, so that it starts
# afresh from a new slot.
kubectl -n "$namespace" annotate kafkaconnector "$connector" strimzi.io/connector-offsets=reset >/dev/null
for _ in $(seq 60); do
  [[ -z $(kubectl -n "$namespace" get kafkaconnector "$connector" -o jsonpath='{.metadata.annotations.strimzi\.io/connector-offsets}') ]] && break
  sleep 2
done
[[ -z $(kubectl -n "$namespace" get kafkaconnector "$connector" -o jsonpath='{.metadata.annotations.strimzi\.io/connector-offsets}') ]] ||
  die "Strimzi did not reset the offsets of connector $connector"
# 4. Ask for the tables' rows again, then 5. resume.
kubectl -n "$namespace" annotate postgressource "$source_name" changeport.io/resnapshot="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --overwrite >/dev/null
set_state Running
wait_phase Streaming
rows=$(pg -c "SELECT count(*) FROM customers")
[[ $(source_field '.status.tables[0].snapshot') == Done && $(source_field '.status.tables[0].rowsCopied') == "$rows" ]] ||
  die "the table was not copied again: $(source_field .status.tables)"
[[ $(slot_column active) == t && $(slot_column wal_status) == reserved ]] ||
  die "the new slot $slot is active $(slot_column active), $(slot_column wal_status)"

log "Changes stream again from the new slot, and the alerts are quiet"
pg -c "UPDATE customers SET name = '$marker' WHERE id = 1"
seen=""
for _ in $(seq 20); do
  seen=$(kafka kafka-console-consumer.sh --command-config /config/client.properties --topic "$source_name.public.customers" \
    --partition 0 --offset earliest --timeout-ms 10000 2>/dev/null | LC_ALL=C tr -d '\000' || true)
  LC_ALL=C grep -qF "$marker" <<<"$seen" && break
  sleep 3
done
LC_ALL=C grep -qF "$marker" <<<"$seen" || die "the change after the recovery did not reach $source_name.public.customers"
pg -c "UPDATE customers SET name = 'Customer 1' WHERE id = 1"
check_source_alerts "$customer" "$source_name" ""
log "Recovery test passed: source $source_name lost its slot, failed and alerted, and streams again from a new slot"
