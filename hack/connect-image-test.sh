#!/usr/bin/env bash
# Checks that the Kafka Connect image (images/connect) loads its plugins:
# Kafka's own plugin tool, run from the image, must find Debezium's Postgres
# connector and Apicurio's Avro converter in /opt/kafka/plugins, loadable and
# at the versions the image's labels name.
# Usage: hack/connect-image-test.sh [image] (default changeport/connect:local)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

image=${1:-$CONNECT_IMAGE}
debezium=$(docker image inspect -f '{{index .Config.Labels "io.changeport.debezium.version"}}' "$image")
[[ -n $debezium ]] || die "$image has no Debezium version label; is it the Connect image?"

log "Listing the plugins of $image"
plugins=$(docker run --rm --entrypoint /opt/kafka/bin/connect-plugin-path.sh "$image" \
  list --plugin-path /opt/kafka/plugins 2>/dev/null)

# Prints the plugin tool's line for a class: class, two aliases, version, type,
# loadable, and whether a service manifest declares it.
plugin() { awk -F'\t' -v class="$1" '$1 == class' <<<"$plugins"; }
expect() {
  local class=$1 type=$2 version=$3 line
  line=$(plugin "$class")
  [[ -n $line ]] || die "$image has no $class"
  IFS=$'\t' read -r _ _ _ found_version found_type loadable _ _ <<<"$line"
  [[ $found_type == "$type" && $loadable == true ]] || die "$class is not a loadable $type: $line"
  [[ -z $version || $found_version == "$version" ]] || die "$class is version $found_version, want $version"
}
expect io.debezium.connector.postgresql.PostgresConnector source "$debezium"
expect io.debezium.transforms.ExtractNewRecordState transformation "$debezium"
expect io.apicurio.registry.utils.converter.AvroConverter converter ""
log "Connect image test passed: $image has Debezium's Postgres connector $debezium and Apicurio's Avro converter"
