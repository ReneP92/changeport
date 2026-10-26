#!/usr/bin/env bash
# Tests the examples of the customer connection guide (docs/connecting.md)
# against a customer's cluster on kind: each example, taken from the guide as
# it is written, produces a message and consumes it back as a consumer group,
# from outside the cluster through the load balancers, with the customer's
# credentials. The guide's examples trust the public certificate authorities
# that issue production certificates; on kind, the only change made to them
# is trusting the local CA.
# Usage: hack/connecting-guide-test.sh [customer-id] (default demo)
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

customer=${1:-demo}
guide="$ROOT/docs/connecting.md"
run_id="$(date +%s)-$RANDOM"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The images and client versions the examples run with.
JAVA_IMAGE=eclipse-temurin:21-jdk@sha256:23cc5734ff46e776f51ce66eb9ee08444fd47caabec0a95b7eb6e94be2d3696d
PYTHON_IMAGE=python:3.13-slim@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962
GO_IMAGE=golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678
NODE_IMAGE=node:24-slim@sha256:55fb5f5a527ea2c5c4a95f963e48ef90aa38c7b346054fdc8ee9957a2b55d205
CONFLUENT_KAFKA_PYTHON=2.16.0
FRANZ_GO=v1.22.2
CONFLUENT_KAFKA_JAVASCRIPT=1.11.0

# Prints the first code block of the given language after a heading.
# Usage: example <heading> <language>
example() {
  awk -v heading="$1" -v fence="\`\`\`$2" '
    $0 == heading { found = 1; next }
    found && $0 == fence { inside = 1; next }
    inside && $0 == "```" { exit }
    inside { print }
  ' "$guide"
}

ensure_kubectl
prepare_kafka_client "$customer" "$work"
properties="$work/client.properties"
bootstrap=$(sed -n 's/^bootstrap.servers=//p' "$properties")
username=$(sed -nE 's/.*username="([^"]+)".*/\1/p' "$properties")
password=$(sed -nE 's/.*password="([^"]+)".*/\1/p' "$properties")
# The local CA, from the truststore lines of client.properties.
sed -n '/BEGIN CERTIFICATE/,/END CERTIFICATE/p' "$properties" |
  sed -E 's/^ssl\.truststore\.certificates=//; s/^ +//; s/ \\$//' >"$work/ca.crt"
grep -q "BEGIN CERTIFICATE" "$work/ca.crt" || die "no CA certificate in customer $customer's client.properties"

# Runs an example in its image: the container works on a copy of the
# example, so that what it installs and builds, owned by the container's
# user, stays out of this machine's directories.
# Usage: run <language> <image> <command>
run() {
  local language=$1 topic="guide-$1-$run_id" output
  docker run "${KAFKA_CLIENT[@]}" --entrypoint /opt/kafka/bin/kafka-topics.sh "$KAFKA_IMAGE" \
    --bootstrap-server "$bootstrap" --command-config /config/client.properties --create --topic "$topic" >/dev/null
  log "Running the guide's $language example"
  if ! output=$(docker run "${KAFKA_CLIENT[@]}" -e "KAFKA_BOOTSTRAP=$bootstrap" -e "KAFKA_USERNAME=$username" \
    -e "KAFKA_PASSWORD=$password" -e KAFKA_CLIENT_RACK=zone-a -e "TOPIC=$topic" -v "$work/$language:/example:ro" "$2" \
    sh -c "cp -R /example /app && cd /app && $3" 2>&1); then
    die "the guide's $language example failed:"$'\n'"$output"
  fi
  grep -q "consumed: hello from" <<<"$output" || die "the guide's $language example consumed nothing:"$'\n'"$output"
}

mkdir -p "$work/java" "$work/python" "$work/go" "$work/node"

# Java takes client.properties as it is, with its truststore lines on kind.
example "### Java" java >"$work/java/Example.java"
cp "$properties" "$work/java/client.properties"
container=$(docker create "$KAFKA_IMAGE")
docker cp -q "$container:/opt/kafka/libs/." "$work/java/libs/"
docker rm "$container" >/dev/null
run java "$JAVA_IMAGE" 'javac -cp "libs/*" Example.java && java -cp "libs/*:." Example'

example "### Python" python | sed 's|^topic = |config["ssl.ca.location"] = "/config/ca.crt"\ntopic = |' >"$work/python/example.py"
run python "$PYTHON_IMAGE" "pip install -q --root-user-action=ignore --disable-pip-version-check confluent-kafka==$CONFLUENT_KAFKA_PYTHON && python example.py"

example "### Go" go | sed 's|kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})|kgo.DialTLSConfig(\&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: caPool()})|' >"$work/go/main.go"
cat >>"$work/go/main.go" <<'GO'

func caPool() *x509.CertPool {
	pem, err := os.ReadFile("/config/ca.crt")
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return pool
}
GO
sed -i.bak 's|^	"crypto/tls"$|	"crypto/tls"\n	"crypto/x509"|' "$work/go/main.go"
run go "$GO_IMAGE" "go mod init example >/dev/null 2>&1 && go get github.com/twmb/franz-go@$FRANZ_GO >/dev/null 2>&1 && go mod tidy >/dev/null 2>&1 && go run ."

example "### Node.js" js | sed 's|^  "sasl.password": process.env.KAFKA_PASSWORD,|&\n  "ssl.ca.location": "/config/ca.crt",|' >"$work/node/example.js"
run node "$NODE_IMAGE" "npm install --silent --no-audit --no-fund @confluentinc/kafka-javascript@$CONFLUENT_KAFKA_JAVASCRIPT >/dev/null && node example.js"

# The examples' cleanup is the topics'.
for language in java python go node; do
  docker run "${KAFKA_CLIENT[@]}" --entrypoint /opt/kafka/bin/kafka-topics.sh "$KAFKA_IMAGE" \
    --bootstrap-server "$bootstrap" --command-config /config/client.properties --delete --topic "guide-$language-$run_id" >/dev/null
done
log "Connection guide test passed: the Java, Python, Go and Node.js examples produce and consume on customer $customer's cluster"
