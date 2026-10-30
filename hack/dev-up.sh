#!/usr/bin/env bash
# Builds the Kafka, agent, Connect, provisioner and console images, creates the local
# kind cluster with cloud-provider-kind for load balancers, installs the
# platform components (Strimzi, cert-manager, the provisioning controller,
# the console) and gives the console its Clerk keys from .env, creates the
# "demo" Customer, which the controller provisions, and runs the smoke test
# against it, and with .env sets up your test users for the console
# (hack/dev-user.sh). DEMO_CUSTOMER= (empty) leaves the demo out, for runs that
# bring customers of their own. Safe to run again.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEMO_CUSTOMER=${DEMO_CUSTOMER-demo}

require_tools
ensure_kind
ensure_kubectl

build_kafka_image
build_agent_image
build_connect_image
build_provisioner_image
build_console_image
check_docker_disk
ensure_kind_cluster
ensure_cloud_provider_kind
ensure_host_gateway
"$ROOT/hack/platform-apply.sh" kind
"$ROOT/hack/console-config.sh"

if [[ -n $DEMO_CUSTOMER ]]; then
  log "Provisioning customer $DEMO_CUSTOMER"
  provisioner provision -customer "$DEMO_CUSTOMER" -wait -timeout 10m
  "$ROOT/hack/smoke-test.sh" "$DEMO_CUSTOMER"
  log "Ready. Connect to customer $DEMO_CUSTOMER's Kafka cluster with: just kafka-client $DEMO_CUSTOMER"
fi
DEMO_CUSTOMER=$DEMO_CUSTOMER "$ROOT/hack/dev-user.sh"
log "The console is at http://localhost:$CONSOLE_PORT"
