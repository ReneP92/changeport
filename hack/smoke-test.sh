#!/usr/bin/env bash
# Tests a customer's Kafka cluster the way its users reach it. On the public
# listener, a client outside the cluster connects to the public names over TLS
# with hostname verification and SCRAM, creates a topic, produces a message and
# consumes it back; no credentials, a wrong password, plaintext and an address
# the certificate does not name must all be refused. On the internal listener,
# the platform's agent user connects with mTLS, and a client without a
# certificate is refused.
# Usage: hack/smoke-test.sh <customer-id>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:?usage: hack/smoke-test.sh <customer-id>}
namespace="customer-$customer"
run_id="$(date +%s)-$RANDOM"
topic="smoke-$run_id"
message="changeport smoke test $run_id"

ensure_kubectl
work=$(mktemp -d)
cleanup() {
  kubectl -n "$namespace" delete pod -l "changeport.io/smoke-test=$run_id" --ignore-not-found --wait=false >/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

prepare_kafka_client "$customer" "$work"
read -r bootstrap_host bootstrap_ip < <(public_addresses "$customer" | awk '$1 ~ /^bootstrap\./')
bootstrap="$bootstrap_host:9094"
# Negative cases give up after 10 seconds instead of the default minute.
printf 'request.timeout.ms=5000\ndefault.api.timeout.ms=10000\n' >"$work/timeouts.properties"
grep -v '^sasl\.' "$work/client.properties" | sed 's/^security.protocol=SASL_SSL$/security.protocol=SSL/' |
  cat - "$work/timeouts.properties" >"$work/no-credentials.properties"
sed -E 's/password="[^"]*"/password="wrong-password"/' "$work/client.properties" |
  cat - "$work/timeouts.properties" >"$work/wrong-password.properties"
printf 'security.protocol=PLAINTEXT\n' | cat - "$work/timeouts.properties" >"$work/plaintext.properties"
cat "$work/client.properties" "$work/timeouts.properties" >"$work/client-timeouts.properties"

# Runs a Kafka command-line tool in a client container outside the cluster.
client() { docker run "${KAFKA_CLIENT[@]}" -i --entrypoint "/opt/kafka/bin/$1" "$KAFKA_IMAGE" "${@:2}"; }

# Runs a client command that must fail, and checks its output for the reason.
must_fail() {
  local what=$1 reason=$2 output
  shift 2
  if output=$("$@" 2>&1); then
    printf '%s\n' "$output" >&2
    die "$what was accepted"
  fi
  grep -E "$reason" <<<"$output" >/dev/null || {
    printf '%s\n' "$output" >&2
    die "$what failed, but not with /$reason/"
  }
  log "Refused as expected: $what"
}

log "Smoke test for customer $customer on $bootstrap: creating topic $topic"
client kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --create --topic "$topic" --partitions 1 >/dev/null

log "Producing a message"
echo "$message" | client kafka-console-producer.sh --bootstrap-server "$bootstrap" \
  --command-config /config/client.properties --topic "$topic"

log "Consuming it back"
consumed=$(client kafka-console-consumer.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties \
  --topic "$topic" --from-beginning --max-messages 1 --timeout-ms 60000 2>"$work/consumer.log")
if ! grep -xF "$message" <<<"$consumed" >/dev/null; then
  cat "$work/consumer.log" >&2
  die "smoke test failed: did not consume the produced message"
fi

list_topics() { client kafka-topics.sh --bootstrap-server "$1" --command-config "/config/$2" --list; }
must_fail "a client without credentials" "TimeoutException|Timed out|disconnected" list_topics "$bootstrap" no-credentials.properties
must_fail "a wrong password" "Authentication failed" list_topics "$bootstrap" wrong-password.properties
must_fail "a plaintext client" "TimeoutException|Timed out|disconnected" list_topics "$bootstrap" plaintext.properties
must_fail "a connection to $bootstrap_ip, which the certificate does not name" "SSLHandshakeException|No subject alternative" \
  list_topics "$bootstrap_ip:9094" client-timeouts.properties

# Runs a shell command in a one-off pod with the agent user's certificate and
# the cluster CA mounted, waits for it to finish and prints its logs.
run_pod() {
  local name="$topic-$1" command=$2 phase=""
  kubectl -n "$namespace" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels:
    changeport.io/smoke-test: "$run_id"
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: $cluster_kafka_image
      command: [sh, -c, '$command']
      env:
        - name: LOG_DIR
          value: /tmp/kafka-logs
      volumeMounts:
        - { name: agent, mountPath: /agent, readOnly: true }
        - { name: cluster-ca, mountPath: /cluster-ca, readOnly: true }
  volumes:
    - name: agent
      secret: { secretName: changeport-agent }
    - name: cluster-ca
      secret: { secretName: $customer-cluster-ca-cert }
YAML
  for _ in $(seq 120); do
    phase=$(kubectl -n "$namespace" get pod "$name" -o jsonpath='{.status.phase}')
    [[ $phase == Succeeded || $phase == Failed ]] && break
    sleep 1
  done
  if [[ $phase != Succeeded && $phase != Failed ]]; then
    kubectl -n "$namespace" get events --field-selector "involvedObject.name=$name" >&2
    die "pod $name did not finish within 2 minutes (phase '${phase:-unknown}')"
  fi
  kubectl -n "$namespace" logs "$name"
  [[ $phase == Succeeded ]]
}

internal="$customer-kafka-bootstrap:9093"
# Pods in the cluster run the content-tagged image the nodes have loaded.
cluster_kafka_image=$(content_tag "$KAFKA_IMAGE")
tls_config='security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=/cluster-ca/ca.crt\nrequest.timeout.ms=5000\ndefault.api.timeout.ms=10000\n'
log "Listing topics as the agent user on the internal mTLS listener $internal"
agent_topics=$(run_pod agent "cat /agent/user.key /agent/user.crt >/tmp/keystore.pem &&
  printf \"${tls_config}ssl.keystore.type=PEM\nssl.keystore.location=/tmp/keystore.pem\n\" >/tmp/agent.properties &&
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $internal --command-config /tmp/agent.properties --list") ||
  die "the agent user could not list topics on the internal listener:"$'\n'"$agent_topics"
grep -xF "$topic" <<<"$agent_topics" >/dev/null || die "the agent user does not see topic $topic:"$'\n'"$agent_topics"
must_fail "an internal client without a certificate" "SSLHandshakeException|TimeoutException|Timed out|disconnected" run_pod no-certificate \
  "printf \"$tls_config\" >/tmp/client.properties &&
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $internal --command-config /tmp/client.properties --list"

client kafka-topics.sh --bootstrap-server "$bootstrap" --command-config /config/client.properties --delete --topic "$topic"
log "Smoke test passed for customer $customer"
