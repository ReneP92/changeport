#!/usr/bin/env bash
# Tests tiered storage on a customer's Kafka cluster on kind: on a topic with
# small segments and one second of local retention, finished segments are
# offloaded to the local object store under the customer's prefix, the
# brokers delete their local copies, and consuming from offset 0 still returns
# every message, now read back from the object store.
# Usage: hack/tiered-storage-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
topic="tiered-$(date +%s)-$RANDOM"
messages=4000
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")
kafka() { docker run "${KAFKA_CLIENT[@]}" -i --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }
offset() {
  kafka kafka-get-offsets.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
    --topic "$topic" --time "$1" 2>/dev/null | cut -d: -f3
}
# Each message is its 8-digit number padded to about 1 KiB, so 4000 of them
# fill about four 1 MiB segments.
payload() { seq -f '%08g' 1 "$messages" | awk '{ printf "%s-%01000d\n", $0, 0 }'; }

log "Tiered storage test for customer $customer: producing $messages messages to $topic"
kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --create \
  --topic "$topic" --partitions 1 --config remote.storage.enable=true --config segment.bytes=1048576 \
  --config local.retention.ms=1000 >/dev/null
payload | kafka kafka-console-producer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --topic "$topic"

# The remote log manager copies finished segments every 30 seconds.
log "Waiting for finished segments to be offloaded"
for _ in $(seq 60); do
  tiered=$(offset latest-tiered)
  [[ -n "$tiered" ]] && break
  sleep 5
done
[[ -n "$tiered" ]] || die "no segment of $topic was offloaded within 5 minutes"

prefix="/buckets/$(kubectl -n "customer-$customer" get kafka "$customer" \
  -o jsonpath='{.spec.kafka.tieredStorage.remoteStorageManager.config.storage\.s3\.bucket\.name}')/customer-$customer"
weed_ls() { kubectl -n object-store exec deploy/seaweedfs -- sh -c "echo 'fs.ls $1' | weed shell 2>/dev/null"; }
topic_dir=$(weed_ls "$prefix/" | grep -E "^$topic-" || true)
[[ -n "$topic_dir" ]] || die "the object store has nothing for $topic under $prefix"
segments=$(weed_ls "$prefix/$topic_dir/0/" | grep -c '\.log$' || true)
[[ $segments -ge 2 ]] || die "the object store holds $segments segments of $topic under $prefix/$topic_dir, want at least 2"
log "$segments segments, up to offset $tiered, are in the object store under $prefix"

# Brokers delete offloaded segments on their retention check, every 5 minutes.
log "Waiting for the brokers to delete their local copies"
for _ in $(seq 80); do
  local_start=$(offset earliest-local)
  [[ -n "$local_start" && $local_start -gt 0 ]] && break
  sleep 5
done
[[ -n "$local_start" && $local_start -gt 0 ]] || die "the brokers still keep $topic from offset 0 locally"
log "The brokers keep $topic from offset $local_start locally; earlier messages are only in the object store"

log "Consuming from offset 0"
kafka kafka-console-consumer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --topic "$topic" --from-beginning --max-messages "$messages" --timeout-ms 120000 >"$work/consumed" 2>"$work/consumer.log" || true
if ! cmp -s <(payload) "$work/consumed"; then
  tail -20 "$work/consumer.log" >&2
  die "consumed $(grep -c . "$work/consumed") of $messages messages, or not the ones produced"
fi
kafka kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --delete --topic "$topic"
log "Tiered storage test passed: all $messages messages read back, $local_start of them from the object store"
