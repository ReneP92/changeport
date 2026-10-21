# Common development tasks. Run "make help" for the list.

TOOLS := sigs.k8s.io/kind github.com/golangci/golangci-lint/v2/cmd/golangci-lint
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
test: ## Run the Go unit tests
	go test -race ./...

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

.PHONY: golden
golden: ## Regenerate the provisioning template golden files
	go test ./internal/provisioning -run Golden -update

##@ Images

.PHONY: kafka-image
kafka-image: ## Build the Kafka broker image (images/kafka) for this machine as changeport/kafka:local
	@bash -c 'source hack/lib.sh && build_kafka_image'

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
	rm -rf bin deploy/base/*/upstream
