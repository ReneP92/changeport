#!/usr/bin/env bash
# Opens a shell in a Kafka client container connected to a customer's public
# listener on kind, as the customer's admin user: the public names resolve to
# the load balancers, and /config/client.properties holds the credentials.
# Usage: hack/kafka-client.sh <customer-id>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:?usage: hack/kafka-client.sh <customer-id>}
ensure_kubectl
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
prepare_kafka_client "$customer" "$work"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$work/client.properties")

cat >&2 <<TEXT
Connected to customer $customer's Kafka cluster at $bootstrap as user admin.
Try:
  kafka-topics.sh --bootstrap-server \$BOOTSTRAP --command-config /config/client.properties --list
TEXT
# shellcheck disable=SC2016 # $PATH expands inside the container
docker run "${KAFKA_CLIENT[@]}" -it -e BOOTSTRAP="$bootstrap" -w /tmp --entrypoint bash "$KAFKA_IMAGE" \
  -c 'export PATH="$PATH:/opt/kafka/bin" && exec bash -i'
