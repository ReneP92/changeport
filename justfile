# Common development tasks. Run "just" for the list.

set shell := ["bash", "-euo", "pipefail", "-c"]

tools := "sigs.k8s.io/kind github.com/golangci/golangci-lint/v2/cmd/golangci-lint sigs.k8s.io/controller-tools/cmd/controller-gen github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
kube_context := "kind-" + env("CLUSTER_NAME", "changeport")

# Show this list
[group('Help')]
default:
    @just --list --unsorted

# Build all Go binaries into bin/
[group('Go')]
build:
    go build -o bin/ ./cmd/...

# Run the Go tests, the controller's against envtest's API server
[group('Go')]
test:
    hack/go-test.sh

# Build the pinned tools from tools/go.mod into bin/
[group('Go')]
tools:
    go -C tools build -o ../bin/ {{ tools }}

# Run golangci-lint
[group('Go')]
lint: tools
    bin/golangci-lint run ./...

# Format Go code
[group('Go')]
fmt: tools
    bin/golangci-lint fmt ./...

# Tidy the Go and tools modules
[group('Go')]
tidy:
    go mod tidy
    go -C tools mod tidy

# Generate code from api/: the CRDs and deep copies, and the console API's Go server and TypeScript client
[group('Go')]
generate: tools
    bin/controller-gen object paths=./api/... crd output:crd:dir=deploy/base/provisioner
    bin/oapi-codegen -config internal/console/api/oapi-codegen.yaml api/openapi.yaml
    hack/bun.sh install --frozen-lockfile
    hack/bun.sh run generate

# Regenerate the provisioning template golden files
[group('Go')]
golden:
    go test ./internal/provisioning -run Golden -update

# Install the web app's dependencies exactly as bun.lock pins them
[group('Web (web/, run by the pinned Bun in bin/)')]
web-install:
    hack/bun.sh install --frozen-lockfile

# Run the web app with hot reload on http://localhost:5173
[group('Web (web/, run by the pinned Bun in bin/)')]
web-dev: web-install
    hack/bun.sh run dev

# Lint, type-check, format-check, unit test and build the web app
[group('Web (web/, run by the pinned Bun in bin/)')]
web-check: web-install
    hack/bun.sh run lint
    hack/bun.sh run typecheck
    hack/bun.sh run format:check
    hack/bun.sh run test
    hack/bun.sh run build

# Run the web app's Playwright tests in Chromium against a production build
[group('Web (web/, run by the pinned Bun in bin/)')]
web-e2e: web-install
    hack/bun.sh x playwright install {{ if env("CI", "") != "" { "--with-deps" } else { "" } }} chromium
    hack/bun.sh run e2e

# Fail on em dashes anywhere in the repository
[group('Web (web/, run by the pinned Bun in bin/)')]
check-text:
    hack/check-text.sh

# Build the Kafka broker image (images/kafka) for this machine as changeport/kafka:local
[group('Images')]
kafka-image:
    @source hack/lib.sh && build_kafka_image

# Build the Kafka Connect image (images/connect) for this machine as changeport/connect:local
[group('Images')]
connect-image:
    @source hack/lib.sh && build_connect_image

# Build the Kafka agent image (images/agent) for this machine as changeport/agent:local
[group('Images')]
agent-image:
    @source hack/lib.sh && build_agent_image

# Build the provisioner image (images/provisioner) for this machine as changeport/provisioner:local
[group('Images')]
provisioner-image:
    @source hack/lib.sh && build_provisioner_image

# Build the console image (images/console, with the web app) as changeport/console:local
[group('Images')]
console-image:
    @source hack/lib.sh && build_console_image

# Rebuild the console and roll it out on kind (http://localhost:8088)
[group('Images')]
console-reload:
    @source hack/lib.sh && build_console_image && ensure_kind && ensure_kubectl && ensure_kind_cluster && hack/platform-apply.sh kind && hack/console-config.sh

# Set up your Clerk test users: an operator, and the demo customer's admin (dev-up does it too)
[group('Local development (kind + Strimzi)')]
dev-user:
    hack/dev-user.sh

# Give the console on kind its Clerk keys from .env (see .env.sample)
[group('Images')]
console-config:
    hack/console-config.sh

# Check the console on kind as a browser reaches it, with Playwright
[group('Images')]
console-test:
    hack/console-test.sh

# Run the console's Playwright tests on kind, signed in as Clerk test users (needs .env)
[group('Images')]
ui-e2e:
    hack/ui-e2e.sh

# Capture every screen of the console on kind into docs/screenshots (needs .env)
[group('Images')]
ui-screenshots:
    hack/ui-screenshots.sh

# Rebuild the provisioner and roll it out on kind
[group('Images')]
provisioner-reload:
    @source hack/lib.sh && build_provisioner_image && ensure_kind && ensure_kubectl && ensure_kind_cluster && ensure_host_gateway && hack/platform-apply.sh kind

# Start a broker from the Kafka image with tiered storage and client telemetry (default changeport/kafka:local)
[group('Images')]
kafka-image-test image="":
    hack/kafka-image-test.sh {{ image }}

# Check that the Kafka Connect image loads its plugins (default changeport/connect:local)
[group('Images')]
connect-image-test image="":
    hack/connect-image-test.sh {{ image }}

# Create the kind cluster, install the platform components, provision the demo customer and smoke test it
[group('Local development (kind + Strimzi)')]
dev-up:
    hack/dev-up.sh

# Delete the kind cluster
[group('Local development (kind + Strimzi)')]
dev-down:
    hack/dev-down.sh

# List the local setup's containers: kind nodes, customers' load balancers and helpers
[group('Local development (kind + Strimzi)')]
ps:
    hack/ps.sh

# Produce and consume a message on a customer's cluster
[group('Local development (kind + Strimzi)')]
smoke-test customer="demo":
    hack/smoke-test.sh {{ customer }}

# Open a shell with Kafka tools connected to a customer's public listener
[group('Local development (kind + Strimzi)')]
kafka-client customer="demo":
    hack/kafka-client.sh {{ customer }}

# Check that every customer's brokers are scraped into VictoriaMetrics with their tenant
[group('Local development (kind + Strimzi)')]
metrics-test:
    hack/metrics-test.sh

# Check that a customer's Kafka agent turns on tiered storage for new topics
[group('Local development (kind + Strimzi)')]
agent-test customer="demo":
    hack/agent-test.sh {{ customer }}

# Check the consumer lag in seconds that a customer's Kafka agent reports
[group('Local development (kind + Strimzi)')]
lag-test customer="demo":
    hack/lag-test.sh {{ customer }}

# Check that a customer's first Postgres source brings Kafka Connect and the schema registry, and the last takes them
[group('Local development (kind + Strimzi)')]
cdc-runtime-test customer="demo":
    hack/cdc-runtime-test.sh {{customer}}

# Check the Postgres on kind that stands in for a customer's database: seed, logical replication, TLS only
[group('Local development (kind + Strimzi)')]
postgres-test:
    hack/postgres-test.sh

# Run the examples of the customer connection guide (docs/connecting.md) against a customer's cluster
[group('Local development (kind + Strimzi)')]
connecting-guide-test customer="demo":
    hack/connecting-guide-test.sh {{ customer }}

# Check that a producer's own metrics (KIP-714) reach the metrics store with its tenant
[group('Local development (kind + Strimzi)')]
client-telemetry-test customer="demo":
    hack/client-telemetry-test.sh {{ customer }}

# Check that the console may manage a customer's topics but not read or write messages
[group('Local development (kind + Strimzi)')]
console-access-test customer="demo":
    hack/console-access-test.sh {{ customer }}

# Offload a topic of a customer to the object store and read it back from offset 0
[group('Local development (kind + Strimzi)')]
tiered-storage-test customer="demo":
    hack/tiered-storage-test.sh {{ customer }}

# Point this machine's DNS for the customers' Kafka names at the host gateway (asks for sudo)
[group('Local development (kind + Strimzi)')]
host-dns:
    hack/host-dns.sh

# Produce to and consume from a customer's cluster as an application on this machine
[group('Local development (kind + Strimzi)')]
host-access-test customer="demo":
    hack/host-access-test.sh {{ customer }}

# Check that a protected customer cannot be deleted, then deprovision it and purge its data
[group('Local development (kind + Strimzi)')]
deletion-protection-test customer:
    hack/deletion-protection-test.sh {{ customer }}

# Freeze a zone and drain a broker of a three-zone customer under load; no message may be lost
[group('Local development (kind + Strimzi)')]
failover-test:
    hack/failover-test.sh

# Provision a customer's Kafka cluster on kind, with a profile (dev, dev-ha or production)
[group('Local development (kind + Strimzi)')]
provision customer="acme" profile="dev":
    go run ./cmd/provisioner provision -context {{ kube_context }} -customer {{ customer }} -profile {{ profile }} -wait

# Delete a customer's Kafka cluster from kind
[group('Local development (kind + Strimzi)')]
deprovision customer="acme":
    go run ./cmd/provisioner deprovision -context {{ kube_context }} -customer {{ customer }} -wait

# Run the end-to-end suites on kind: core, failover or all; KEEP=1 keeps the customers
[group('Local development (kind + Strimzi)')]
e2e suite="all" customer="acme":
    SUITE={{ suite }} hack/e2e.sh {{ customer }}

# Apply the platform components to an environment: kind, dev, or prod with CONFIRM=prod
[group('Platform components')]
platform-apply env:
    hack/platform-apply.sh {{ env }}

# Check with the IAM policy simulator that an AWS environment keeps customers' tiered storage apart (needs AWS credentials)
[group('Platform components')]
tiered-storage-isolation-test env:
    hack/tiered-storage-isolation-test.sh {{ env }}

# Apply the kind overlay to kind and server-side dry-run the EKS overlays there
[group('Platform components')]
platform-check:
    hack/platform-check.sh

# Lint the shell scripts (needs shellcheck)
[group('Platform components')]
shellcheck:
    shellcheck hack/*.sh

# Check Terraform formatting
[group('Terraform')]
tf-fmt:
    @source hack/lib.sh && terraform fmt -check -recursive terraform

# Validate every Terraform environment without a backend
[group('Terraform')]
tf-validate:
    #!/usr/bin/env bash
    set -euo pipefail
    source hack/lib.sh
    for env in terraform/environments/*/; do
      log "Validating $env"
      terraform -chdir="$env" init -backend=false -input=false >/dev/null
      terraform -chdir="$env" validate
    done

# Run the Terraform modules' tests, which plan against a mocked AWS provider
[group('Terraform')]
tf-test:
    #!/usr/bin/env bash
    set -euo pipefail
    source hack/lib.sh
    for tests in terraform/modules/*/tests/; do
      module=${tests%/tests/}
      log "Testing $module"
      terraform -chdir="$module" init -backend=false -input=false >/dev/null
      terraform -chdir="$module" test
    done

# Remove build output and downloaded tools
[group('Housekeeping')]
clean:
    @# setup-envtest leaves its binaries read-only.
    @if [ -d bin/envtest ]; then chmod -R u+w bin/envtest; fi
    rm -rf bin deploy/base/*/upstream web/dist web/node_modules
