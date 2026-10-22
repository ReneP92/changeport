#!/usr/bin/env bash
# Tests the console's access to a customer's cluster on kind, the way the
# console will use it: from the changeport namespace, as the changeport-console
# service account and Kafka user. The service account may read exactly the
# Secrets the console needs in the customer's namespace. Over the internal
# mTLS listener, the console user may create, describe and delete topics and
# list consumer groups, but neither produce nor consume, with or without a
# consumer group.
# Usage: hack/console-access-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
namespace="customer-$customer"
console="system:serviceaccount:changeport:changeport-console"
run_id="$(date +%s)-$RANDOM"
name="console-access-$run_id"
topic="console-access-$run_id"
work=$(mktemp -d)
cleanup() {
  kubectl -n changeport delete pod,secret -l "changeport.io/console-access-test=$run_id" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

ensure_kubectl

log "Console service account's access to the Secrets of customer $customer"
can_i() { kubectl auth can-i --as "$console" "$@" 2>/dev/null || true; }
for secret in changeport-console "$customer-cluster-ca-cert" admin external-listener-tls; do
  [[ $(can_i -n "$namespace" get "secret/$secret") == yes ]] || die "the console cannot read secret $namespace/$secret"
done
for denied in "-n $namespace get secret/tiered-storage-credentials" "-n $namespace get secret/changeport-agent" \
  "-n $namespace list secrets" "-n $namespace get pods" "-n kube-system get secret/admin"; do
  # shellcheck disable=SC2086 # the words are separate arguments
  [[ $(can_i $denied) == no ]] || die "the console may $denied"
done

log "Console Kafka user on the internal listener, from the changeport namespace"
# The pod runs where the console will, so it gets the certificates through a
# copy of the Secrets there.
kubectl -n "$namespace" get secret changeport-console -o jsonpath='{.data.user\.crt}' | base64 -d >"$work/user.crt"
kubectl -n "$namespace" get secret changeport-console -o jsonpath='{.data.user\.key}' | base64 -d >"$work/user.key"
kubectl -n "$namespace" get secret "$customer-cluster-ca-cert" -o jsonpath='{.data.ca\.crt}' | base64 -d >"$work/ca.crt"
bootstrap="$customer-kafka-bootstrap.$namespace.svc:9093"
# The pod's script: each step prints its output, then "step <name> <exit code>".
cat >"$work/run.sh" <<SCRIPT
set -u
bin=/opt/kafka/bin
cat /certs/user.key /certs/user.crt >/tmp/keystore.pem
cat >/tmp/console.properties <<PROPERTIES
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.crt
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/keystore.pem
request.timeout.ms=5000
default.api.timeout.ms=15000
max.block.ms=15000
PROPERTIES
step() { name=\$1; shift; "\$@" 2>&1; echo "step \$name \$?"; }
step create \$bin/kafka-topics.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --create --topic $topic --partitions 1
step describe \$bin/kafka-topics.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --describe --topic $topic
step groups \$bin/kafka-consumer-groups.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --list
echo message | step produce \$bin/kafka-console-producer.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --topic $topic \\
  --producer-property enable.idempotence=false --producer-property acks=1
step consume \$bin/kafka-console-consumer.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --topic $topic --group $topic --from-beginning --max-messages 1 --timeout-ms 15000
# Without a group, reading meets the topic's Read ACL alone.
step read \$bin/kafka-console-consumer.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --topic $topic --partition 0 --offset earliest --max-messages 1 --timeout-ms 15000
step delete \$bin/kafka-topics.sh --bootstrap-server $bootstrap --command-config /tmp/console.properties --delete --topic $topic
SCRIPT
kubectl -n changeport create secret generic "$name" --from-file="$work/user.crt" --from-file="$work/user.key" \
  --from-file="$work/ca.crt" --from-file="$work/run.sh" --dry-run=client -o yaml |
  kubectl label --local -f - "changeport.io/console-access-test=$run_id" -o yaml |
  kubectl apply -f - >/dev/null

kubectl -n changeport apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels:
    changeport.io/console-access-test: "$run_id"
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
    - name: client
      image: $(content_tag "$KAFKA_IMAGE")
      command: [sh, /certs/run.sh]
      env:
        - name: LOG_DIR
          value: /tmp/kafka-logs
      volumeMounts:
        - { name: certs, mountPath: /certs, readOnly: true }
  volumes:
    - name: certs
      secret: { secretName: $name }
YAML
phase=""
for _ in $(seq 180); do
  phase=$(kubectl -n changeport get pod "$name" -o jsonpath='{.status.phase}')
  [[ $phase == Succeeded || $phase == Failed ]] && break
  sleep 1
done
[[ $phase == Succeeded ]] || die "pod changeport/$name did not finish (phase '${phase:-unknown}')"
kubectl -n changeport logs "$name" >"$work/output"

# Fails unless the pod's step exited with code 0 (want "ok"), or reported
# an error matching pattern (want "refused").
expect() {
  local step=$1 want=$2 pattern=${3:-} output code
  # The step's output: the lines since the previous step's marker.
  output=$(awk -v step="$step" '/^step / { if ($2 == step) { printf "%s", buf; exit } buf = ""; next } { buf = buf $0 "\n" }' "$work/output")
  code=$(awk -v step="$step" '$1 == "step" && $2 == step { print $3 }' "$work/output")
  [[ -n $code ]] || die "step $step did not run:"$'\n'"$(cat "$work/output")"
  if [[ $want == ok && $code != 0 ]]; then
    die "the console user's $step failed with exit code $code:"$'\n'"$output"
  fi
  if [[ $want == refused ]]; then
    grep -Eq "$pattern" <<<"$output" || die "the console user's $step was not refused:"$'\n'"$output"
    log "Refused as expected: $step ($(grep -Eo "$pattern" <<<"$output" | head -1))"
  fi
}
# The producer runs without idempotence, so that the topic's Write ACL
# refuses it rather than the cluster's IdempotentWrite.
denied="TopicAuthorizationException|GroupAuthorizationException"
expect create ok
expect describe ok
expect groups ok
expect produce refused "$denied"
expect consume refused "$denied"
expect read refused "TopicAuthorizationException"
expect delete ok
log "The console user created, described and deleted $topic and listed groups; producing and consuming were refused"
log "Console access test passed for customer $customer"
