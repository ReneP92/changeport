# Common development tasks. Run "make help" for the list.

TOOLS := sigs.k8s.io/kind github.com/golangci/golangci-lint/v2/cmd/golangci-lint sigs.k8s.io/controller-tools/cmd/controller-gen github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
PROFILE ?= dev
KUBE_CONTEXT := kind-$(or $(CLUSTER_NAME),changeport)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^##@/ {printf "\n%s\n", substr($$0, 5)} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Go

.PHONY: build
build: ## Build all Go binaries into bin/
	go build -o bin/ ./cmd/...

.PHONY: test
test: ## Run the Go tests, the controller's against envtest's API server
	hack/go-test.sh

.PHONY: tools
tools: ## Build the pinned tools from tools/go.mod into bin/
	go -C tools build -o ../bin/ $(TOOLS)

.PHONY: lint
lint: tools ## Run golangci-lint
	bin/golangci-lint run ./...

.PHONY: fmt
fmt: tools ## Format Go code
	bin/golangci-lint fmt ./...

.PHONY: tidy
tidy: ## Tidy the Go and tools modules
	go mod tidy
	go -C tools mod tidy

.PHONY: generate
generate: tools ## Generate code from api/: the CRDs and deep copies, and the console API's Go server and TypeScript client
	bin/controller-gen object paths=./api/... crd output:crd:dir=deploy/base/provisioner
	bin/oapi-codegen -config internal/console/api/oapi-codegen.yaml api/openapi.yaml
	hack/bun.sh install --frozen-lockfile
	hack/bun.sh run generate

.PHONY: golden
golden: ## Regenerate the provisioning template golden files
	go test ./internal/provisioning -run Golden -update

##@ Web (web/, run by the pinned Bun in bin/)

.PHONY: web-install
web-install: ## Install the web app's dependencies exactly as bun.lock pins them
	hack/bun.sh install --frozen-lockfile

.PHONY: web-dev
web-dev: web-install ## Run the web app with hot reload on http://localhost:5173
	hack/bun.sh run dev

.PHONY: web-check
web-check: web-install ## Lint, type-check, format-check, unit test and build the web app
	hack/bun.sh run lint
	hack/bun.sh run typecheck
	hack/bun.sh run format:check
	hack/bun.sh run test
	hack/bun.sh run build

.PHONY: web-e2e
web-e2e: web-install ## Run the web app's Playwright tests in Chromium against a production build
	hack/bun.sh x playwright install $(if $(CI),--with-deps) chromium
	hack/bun.sh run e2e

.PHONY: check-text
check-text: ## Fail on em dashes anywhere in the repository
	hack/check-text.sh

##@ Images

.PHONY: kafka-image
kafka-image: ## Build the Kafka broker image (images/kafka) for this machine as changeport/kafka:local
	@bash -c 'source hack/lib.sh && build_kafka_image'

.PHONY: agent-image
agent-image: ## Build the Kafka agent image (images/agent) for this machine as changeport/agent:local
	@bash -c 'source hack/lib.sh && build_agent_image'

.PHONY: provisioner-image
provisioner-image: ## Build the provisioner image (images/provisioner) for this machine as changeport/provisioner:local
	@bash -c 'source hack/lib.sh && build_provisioner_image'

.PHONY: console-image
console-image: ## Build the console image (images/console, with the web app) as changeport/console:local
	@bash -c 'source hack/lib.sh && build_console_image'

.PHONY: console-reload
console-reload: ## Rebuild the console and roll it out on kind (http://localhost:8088)
	@bash -c 'source hack/lib.sh && build_console_image && ensure_kind && ensure_kubectl && ensure_kind_cluster && hack/platform-apply.sh kind && hack/console-config.sh'

.PHONY: console-config
console-config: ## Give the console on kind its Clerk keys from .env (see .env.sample)
	hack/console-config.sh

.PHONY: console-test
console-test: ## Check the console on kind as a browser reaches it, with Playwright
	hack/console-test.sh

.PHONY: provisioner-reload
provisioner-reload: ## Rebuild the provisioner and roll it out on kind
	@bash -c 'source hack/lib.sh && build_provisioner_image && ensure_kind && ensure_kubectl && ensure_kind_cluster && hack/platform-apply.sh kind'

.PHONY: kafka-image-test
kafka-image-test: ## Start a broker from the Kafka image with tiered storage configured (IMAGE=changeport/kafka:local)
	hack/kafka-image-test.sh $(IMAGE)

##@ Local development (kind + Strimzi)

.PHONY: dev-up
dev-up: ## Create the kind cluster, install the platform components, provision the demo customer and smoke test it
	hack/dev-up.sh

.PHONY: dev-down
dev-down: ## Delete the kind cluster
	hack/dev-down.sh

.PHONY: smoke-test
smoke-test: CUSTOMER ?= demo
smoke-test: ## Produce and consume a message on CUSTOMER's cluster (default demo)
	hack/smoke-test.sh $(CUSTOMER)

provision deprovision e2e: CUSTOMER ?= acme

.PHONY: kafka-client
kafka-client: CUSTOMER ?= demo
kafka-client: ## Open a shell with Kafka tools connected to CUSTOMER's public listener (default demo)
	hack/kafka-client.sh $(CUSTOMER)

.PHONY: metrics-test
metrics-test: ## Check that every customer's brokers are scraped into VictoriaMetrics with their tenant
	hack/metrics-test.sh

.PHONY: agent-test
agent-test: CUSTOMER ?= demo
agent-test: ## Check that CUSTOMER's Kafka agent (default demo) turns on tiered storage for new topics
	hack/agent-test.sh $(CUSTOMER)

.PHONY: lag-test
lag-test: CUSTOMER ?= demo
lag-test: ## Check the consumer lag in seconds that CUSTOMER's Kafka agent (default demo) reports
	hack/lag-test.sh $(CUSTOMER)

.PHONY: console-access-test
console-access-test: CUSTOMER ?= demo
console-access-test: ## Check that the console may manage CUSTOMER's topics (default demo) but not read or write messages
	hack/console-access-test.sh $(CUSTOMER)

.PHONY: tiered-storage-test
tiered-storage-test: CUSTOMER ?= demo
tiered-storage-test: ## Offload a topic of CUSTOMER (default demo) to the object store and read it back from offset 0
	hack/tiered-storage-test.sh $(CUSTOMER)

.PHONY: failover-test
failover-test: ## Freeze a zone and drain a broker of a three-zone customer under load; no message may be lost
	hack/failover-test.sh

.PHONY: provision
provision: ## Provision CUSTOMER's Kafka cluster on kind (default acme, PROFILE=dev)
	go run ./cmd/provisioner provision -context $(KUBE_CONTEXT) -customer $(CUSTOMER) -profile $(PROFILE) -wait

.PHONY: deprovision
deprovision: ## Delete CUSTOMER's Kafka cluster from kind
	go run ./cmd/provisioner deprovision -context $(KUBE_CONTEXT) -customer $(CUSTOMER) -wait

.PHONY: e2e
e2e: ## Provision a second customer, smoke test both clusters, then deprovision it (KEEP=1 keeps it)
	hack/e2e.sh $(CUSTOMER)

##@ Platform components

.PHONY: platform-apply
platform-apply: ## Apply the platform components to ENV (kind, dev, or prod with CONFIRM=prod)
	@test -n "$(ENV)" || { echo "usage: make platform-apply ENV=kind|dev|prod" >&2; exit 1; }
	CONFIRM=$(CONFIRM) hack/platform-apply.sh $(ENV)

.PHONY: platform-check
platform-check: ## Apply the kind overlay to kind and server-side dry-run the EKS overlays there
	hack/platform-check.sh

.PHONY: shellcheck
shellcheck: ## Lint the shell scripts (needs shellcheck)
	shellcheck hack/*.sh

##@ Terraform

.PHONY: tf-fmt
tf-fmt: ## Check Terraform formatting
	terraform fmt -check -recursive terraform

.PHONY: tf-validate
tf-validate: ## Validate every Terraform environment without a backend
	@for env in terraform/environments/*/; do \
		echo "==> $$env"; \
		terraform -chdir=$$env init -backend=false -input=false >/dev/null && terraform -chdir=$$env validate || exit 1; \
	done

.PHONY: clean
clean: ## Remove build output and downloaded tools
	@# setup-envtest leaves its binaries read-only.
	@if [ -d bin/envtest ]; then chmod -R u+w bin/envtest; fi
	rm -rf bin deploy/base/*/upstream web/dist web/node_modules
