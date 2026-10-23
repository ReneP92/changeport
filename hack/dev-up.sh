#!/usr/bin/env bash
# Builds the Kafka, agent, provisioner and console images, creates the local
# kind cluster with cloud-provider-kind for load balancers, installs the
# platform components (Strimzi, cert-manager, the provisioning controller,
# the console) and gives the console its Clerk keys from .env, creates the
# "demo" Customer, which the controller provisions, and runs the smoke test
# against it. Safe to run again.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEMO_CUSTOMER=${DEMO_CUSTOMER:-demo}

require_tools
ensure_kind
ensure_kubectl

build_kafka_image
build_agent_image
build_provisioner_image
build_console_image
check_docker_disk
ensure_kind_cluster
ensure_cloud_provider_kind
"$ROOT/hack/platform-apply.sh" kind
"$ROOT/hack/console-config.sh"

log "Provisioning customer $DEMO_CUSTOMER"
provisioner provision -customer "$DEMO_CUSTOMER" -wait -timeout 10m

"$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"

log "Ready. Connect to customer $DEMO_CUSTOMER's Kafka cluster with: make kafka-client CUSTOMER=$DEMO_CUSTOMER"
log "The console is at http://localhost:$CONSOLE_PORT"
