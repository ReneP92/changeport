#!/usr/bin/env bash
# Checks that the Kafka image runs a broker with tiered storage and client
# telemetry: a single-node KRaft broker in Docker loads and configures the
# remote storage manager for S3 and accepts a topic with
# remote.storage.enable=true, and its metrics reporter exposes the metrics a
# producer pushes (KIP-714). This catches plugin loading problems such as
# Aiven issue #776 without any S3 store, since nothing is uploaded before a
# segment rolls; offloading is tested on kind.
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
rsm.config.storage.aws.secret.access.key=test
metric.reporters=io.strimzi.kafka.metrics.prometheus.ServerKafkaMetricsReporter
kafka.metrics.reporters=io.strimzi.kafka.metrics.prometheus.ServerYammerMetricsReporter
prometheus.metrics.reporter.listener=http://:9404
prometheus.metrics.reporter.client.telemetry.labels=client_id,principal'

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

log "Producing with client telemetry"
docker exec "$name" /opt/kafka/bin/kafka-client-metrics.sh --bootstrap-server localhost:9092 --alter --name image-test \
  --metrics org.apache.kafka.producer.record.send.total --interval 1000 >/dev/null
# Java producers skip the push KIP-714 asks for when a client closes, so the
# producer stays open for a few push intervals after sending.
docker exec "$name" bash -c '{ seq 1 100; sleep 4; } | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic tiered --command-property client.id=image-test' 
sent=$(docker exec "$name" curl -sf localhost:9404/metrics |
  sed -nE 's/^clients_org_apache_kafka_producer_record_send_total\{.*client_id="image-test".*principal="User:ANONYMOUS".*\} ([0-9.E]+)$/\1/p')
[[ $sent == 100.0 ]] || die "the metrics reporter exposes ${sent:-no} records sent by the producer, want 100.0"

errors=$(docker logs "$name" 2>&1 | grep -E "ERROR|Exception" || true)
[[ -z "$errors" ]] || die "the broker logged errors:"$'\n'"$errors"
log "Kafka image test passed: $image runs a broker with the tiered storage plugin and client telemetry"
