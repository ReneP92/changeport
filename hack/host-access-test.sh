#!/usr/bin/env bash
# Tests that an application on this machine reaches a customer's Kafka
# cluster on kind with the customer's client.properties, as the console hands
# it out: through the host's DNS and the host gateway, a Go Kafka client
# produces to and consumes from every broker. On Linux, as in CI, it writes
# the customers' names into /etc/hosts first (just host-dns); on macOS that is
# a one-time step the test checks.
# Usage: hack/host-access-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ensure_kubectl
[[ $(docker inspect -f '{{.State.Running}}' "$HOST_GATEWAY_CONTAINER" 2>/dev/null) == true ]] ||
  die "the host gateway is not running; run just dev-up"
case $(uname -s) in
Linux) "$ROOT/hack/host-dns.sh" ;;
Darwin)
  [[ -f /etc/resolver/changeport.test ]] ||
    die "macOS does not ask the host gateway for changeport.test yet; run just host-dns once"
  ;;
esac

log "Host access test for customer $customer"
provisioner credentials -customer "$customer" >"$work/client.properties"
CHANGEPORT_CLIENT_PROPERTIES="$work/client.properties" \
  go test -count=1 -v -run TestProduceAndConsumeFromTheHost ./internal/hostgateway >"$work/test.log" 2>&1 ||
  { cat "$work/test.log" >&2; die "a client on this machine could not use customer $customer's cluster"; }
grep -o 'produced and consumed.*' "$work/test.log" >&2
log "Host access test passed for customer $customer"
