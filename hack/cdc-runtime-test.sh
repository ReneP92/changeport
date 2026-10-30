#!/usr/bin/env bash
# Tests the runtime of a customer's Postgres CDC sources on kind
# (docs/cdc.md): a customer's first PostgresSource brings the Kafka Connect
# worker and the schema registry, which become ready while the customer stays
# Ready; the worker reaches the brokers, the registry and the stand-in
# database, but not the platform's other services or the metadata address;
# only the worker reaches the registry. Removing the last source removes
# them again.
# Usage: hack/cdc-runtime-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
namespace=customer-$customer
source_name="runtime-$RANDOM"

ensure_kubectl
# Prints the customer's condition, as "<status> <message>".
condition() {
  kubectl get customer "$customer" -o jsonpath="{.status.conditions[?(@.type==\"$1\")].status} {.status.conditions[?(@.type==\"$1\")].message}"
}
# Waits until the customer's condition starts with the given text.
wait_condition() {
  local state
  for _ in $(seq 120); do
    state=$(condition "$1")
    [[ $state == "$2"* ]] && return
    sleep 5
  done
  die "customer $customer's $1 is \"$state\", not \"$2\" after 10 minutes"
}
# Prints whether pod (as namespace/pod) reaches host:port over TCP.
reaches() {
  kubectl -n "${1%%/*}" exec "${1#*/}" -- bash -c "timeout 5 bash -c '</dev/tcp/$2/$3' 2>/dev/null && echo reachable || echo blocked"
}
expect() {
  local got
  got=$(reaches "$1" "$2" "$3")
  [[ $got == "$4" ]] || die "$1 to $2:$3 is $got, want $4"
}

if kubectl -n "$namespace" get postgressources -o name | grep -q .; then
  die "customer $customer has Postgres sources already; this test needs a customer without"
fi
wait_condition CDCReady "True No Postgres CDC sources"

log "CDC runtime test for customer $customer: adding source $source_name"
trap 'kubectl -n "$namespace" delete postgressource "$source_name" --ignore-not-found --wait=false >/dev/null' EXIT
kubectl apply -f - >/dev/null <<SOURCE
apiVersion: changeport.io/v1alpha1
kind: PostgresSource
metadata:
  name: $source_name
  namespace: $namespace
spec:
  id: $(random_string a-z0-9 6)
  provider: generic
  connection: {host: postgres.source-postgres.svc, database: app, passwordSecret: postgres-password-$source_name}
  tables: [{schema: public, name: orders}]
SOURCE
wait_condition CDCReady "True Kafka Connect and the schema registry run for 1 source"
[[ $(condition Ready) == True* ]] || die "customer $customer is not Ready with its CDC runtime: $(condition Ready)"

log "The Connect worker reaches the brokers, the registry and the stand-in database, and nothing else"
connect=$namespace/changeport-connect-connect-0
expect "$connect" "$customer-kafka-bootstrap" 9093 reachable
expect "$connect" changeport-registry 8080 reachable
expect "$connect" postgres.source-postgres.svc 5432 reachable
expect "$connect" victoria-metrics.metrics.svc 8428 blocked
expect "$connect" changeport-console.changeport.svc 80 blocked
expect "$connect" 169.254.169.254 80 blocked
# The brokers reach the same service, which shows that the policy blocks it.
expect "$namespace/$customer-dual-role-0" victoria-metrics.metrics.svc 8428 reachable
log "Only the Connect worker reaches the registry"
expect source-postgres/"$(kubectl -n source-postgres get pod -l app.kubernetes.io/name=postgres -o name | sed -n 1p | cut -d/ -f2)" \
  "changeport-registry.$namespace.svc" 8080 blocked

log "Removing source $source_name removes the CDC runtime"
kubectl -n "$namespace" delete postgressource "$source_name" >/dev/null
wait_condition CDCReady "True No Postgres CDC sources"
for _ in $(seq 60); do
  left=$(kubectl -n "$namespace" get kafkaconnect,kafkauser,deployment,service,networkpolicy -l changeport.io/part=cdc -o name 2>/dev/null)
  [[ -z $left ]] && break
  sleep 2
done
[[ -z $left ]] || die "the CDC runtime is left after its last source went:"$'\n'"$left"
log "CDC runtime test passed: customer $customer's first source brought Kafka Connect and the registry, and the last took them"
