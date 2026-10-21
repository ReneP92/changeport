#!/usr/bin/env bash
# Checks that the Kafka image runs a broker with tiered storage: a single-node
# KRaft broker in Docker loads and configures the remote storage manager for
# S3 and accepts a topic with remote.storage.enable=true. This catches plugin
# loading problems such as Aiven issue #776 without any S3 store, since nothing
# is uploaded before a segment rolls; offloading is tested on kind.
# Usage: hack/kafka-image-test.sh [image] (default: the kind platform's image)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

image=${1:-$KAFKA_IMAGE}
name="changeport-kafka-image-test-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# The rsm.config. and rlmm.config. prefixes are what Strimzi's tieredStorage
# settings turn into.
server_properties='process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=/tmp/kafka-logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered-storage/*
remote.log.metadata.manager.listener.name=PLAINTEXT
rlmm.config.remote.log.metadata.topic.replication.factor=1
rsm.config.chunk.size=4194304
rsm.config.storage.backend.class=io.aiven.kafka.tieredstorage.storage.s3.S3Storage
rsm.config.storage.s3.bucket.name=changeport-image-test
rsm.config.storage.s3.region=us-east-1
rsm.config.storage.s3.endpoint.url=http://localhost:9000
rsm.config.storage.s3.path.style.access.enabled=true
rsm.config.storage.aws.access.key.id=test
rsm.config.storage.aws.secret.access.key=test'

log "Starting a broker from $image with tiered storage configured"
# LOG_DIR: the scripts' default, /opt/kafka/logs, is not writable by the
# broker user; under Strimzi the operator sets up logging instead.
docker run -d --name "$name" -e SERVER_PROPERTIES="$server_properties" -e LOG_DIR=/tmp/kafka-app-logs --entrypoint bash "$image" -c '
  printf "%s\n" "$SERVER_PROPERTIES" >/tmp/server.properties &&
  /opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /tmp/server.properties >/dev/null &&
  exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties' >/dev/null

# grep reads all of the logs rather than stopping at the first match (-q),
# which would fail the pipeline under pipefail.
started() { docker logs "$name" 2>&1 | grep "Kafka Server started" >/dev/null; }
for _ in $(seq 90); do
  started && break
  [[ $(docker inspect -f '{{.State.Running}}' "$name") == true ]] || break
  sleep 1
done
if ! started; then
  docker logs "$name" 2>&1 | tail -40 >&2
  die "the broker did not start"
fi

log "Creating a topic with tiered storage"
docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tiered \
  --config remote.storage.enable=true --config local.retention.ms=60000 >/dev/null
docker exec "$name" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics \
  --entity-name tiered | grep "remote.storage.enable=true" >/dev/null || die "the topic does not have tiered storage enabled"

errors=$(docker logs "$name" 2>&1 | grep -E "ERROR|Exception" || true)
[[ -z "$errors" ]] || die "the broker logged errors:"$'\n'"$errors"
log "Kafka image test passed: $image runs a broker with the tiered storage plugin"
