# shellcheck shell=bash
# Shared settings and helpers for the local development scripts. Source it.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

# Pinned versions. The platform manifests' upstream files are pinned in
# deploy/upstream-files.txt. The kind node image follows the Kubernetes minor
# version of EKS (terraform/environments/*/variables.tf); images are listed
# at https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0
KIND_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
KUBECTL_VERSION=v1.37.1
# The kube-apiserver and etcd the controller tests run against (envtest),
# matching the kind node image. setup-envtest verifies their checksums.
ENVTEST_K8S_VERSION=1.37.0
# Bun runs all TypeScript and React work in web/. The version must match
# packageManager in web/package.json, which CI installs from; ensure_bun
# checks that they agree.
BUN_VERSION=1.4.3

# The Changeport Kafka image from images/kafka, as named by
# internal/provisioning.KindPlatform. dev-up builds it and loads it into kind.
KAFKA_IMAGE=changeport/kafka:local
# The Kafka agent's image from images/agent, as named by
# internal/provisioning.KindPlatform. dev-up builds it and loads it into kind.
AGENT_IMAGE=changeport/agent:local
# The provisioner's image from images/provisioner, as named by
# deploy/base/provisioner. dev-up builds it and loads it into kind.
PROVISIONER_IMAGE=changeport/provisioner:local
# The console's image from images/console, as named by deploy/base/console.
# dev-up builds it and loads it into kind, where it is served on the host at
# http://localhost:$CONSOLE_PORT (deploy/kind/cluster.yaml).
CONSOLE_IMAGE=changeport/console:local
CONSOLE_PORT=8088

# cloud-provider-kind gives LoadBalancer Services on kind an address: a
# container per Service on the kind Docker network. It runs as a container on
# the host's Docker, watching the kind clusters.
CLOUD_PROVIDER_KIND_IMAGE=registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0@sha256:1b97632e5dfec8f83c7e970cf18cedb9e6b707e272011d712cabf6ab59d323c9
CLOUD_PROVIDER_KIND_CONTAINER=changeport-cloud-provider-kind

# The host gateway (cmd/hostgateway) lets Kafka clients on this machine reach
# the customers' clusters: it runs on the kind network with Kafka's port and
# its DNS published on the loopback address. HOST_DNS_PORT is what
# make host-dns points the host's resolver at for changeport.test.
HOST_GATEWAY_CONTAINER=changeport-host-gateway
HOST_KAFKA_PORT=9094
HOST_DNS_PORT=15353

# Every other image the platform components and customer clusters run on kind,
# matching the versions in deploy/upstream-files.txt and
# internal/provisioning.KafkaVersion. They are pulled once into the host's
# Docker, which keeps them across cluster rebuilds, and loaded into every kind
# node; pulling 400 MB Kafka images on each node can outlast Strimzi's
# timeouts. make e2e fails if a pod runs an image missing here.
KIND_PRELOAD_IMAGES=(
  quay.io/strimzi/operator:1.3.0
  quay.io/strimzi/drain-cleaner:1.8.0
  chrislusf/seaweedfs:4.48
  victoriametrics/victoria-metrics:v1.154.1
  quay.io/jetstack/cert-manager-cainjector:v1.21.2
  quay.io/jetstack/cert-manager-controller:v1.21.2
  quay.io/jetstack/cert-manager-webhook:v1.21.2
)

CLUSTER_NAME=${CLUSTER_NAME:-changeport}
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
BIN_DIR="$ROOT/bin"
KUBECTL_BIN="$BIN_DIR/kubectl-$KUBECTL_VERSION"
BUN_BIN="$BIN_DIR/bun-$BUN_VERSION"

log() { printf '\033[1m==> %s\033[0m\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# Runs a command that reaches a container registry up to three times, as
# registries fail now and then (such as quay.io answering 503 for a layer).
retry_registry() {
  local attempt
  for attempt in 1 2 3; do
    "$@" && return 0
    if [[ $attempt -lt 3 ]]; then
      log "It failed; trying again in $((attempt * 20))s"
      sleep $((attempt * 20))
    fi
  done
  return 1
}

# kubectl always targets KUBE_CONTEXT, never the current context. It is the
# local kind cluster unless a script sets it explicitly (hack/platform-apply.sh).
kubectl() { "$KUBECTL_BIN" --context "$KUBE_CONTEXT" "$@"; }
kind() { "$BIN_DIR/kind" "$@"; }
# Runs the provisioning CLI against KUBE_CONTEXT.
provisioner() { go run ./cmd/provisioner "$1" -context "$KUBE_CONTEXT" "${@:2}"; }

# Prints the content-addressed name of a locally built image, such as
# changeport/agent:7a88857b2635 for changeport/agent:local: a rebuild with
# new content gets a new name, so the pods running it roll.
content_tag() {
  local id
  id=$(docker image inspect -f '{{.Id}}' "$1")
  id=${id#sha256:}
  echo "${1%:*}:${id:0:12}"
}

# Tags image with its content tag and removes the content tags of its earlier
# builds, so that rebuilds do not fill Docker's disk. Usage: tag_content <image>
tag_content() {
  local image=$1 current old
  current=$(content_tag "$image")
  docker tag "$image" "$current"
  while read -r old; do
    [[ $old == "$current" || $old == "$image" ]] || docker rmi "$old" >/dev/null 2>&1 || true
  done < <(docker images --format '{{.Repository}}:{{.Tag}}' "${image%:*}")
}

sha256_of() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

require_tools() {
  command -v go >/dev/null || die "Go is required: https://go.dev/dl/"
  command -v docker >/dev/null || die "Docker is required: https://docs.docker.com/get-docker/"
  docker info >/dev/null 2>&1 || die "Docker is not running"
}

# Builds kind at the version pinned in tools/go.mod.
ensure_kind() { go -C "$ROOT/tools" build -o "$BIN_DIR/" sigs.k8s.io/kind; }

# Downloads envtest's kube-apiserver and etcd into bin/envtest, unless they
# are there, and prints their directory for KUBEBUILDER_ASSETS.
envtest_assets() {
  go -C "$ROOT/tools" build -o "$BIN_DIR/" sigs.k8s.io/controller-runtime/tools/setup-envtest
  "$BIN_DIR/setup-envtest" use "$ENVTEST_K8S_VERSION" --bin-dir "$BIN_DIR/envtest" -p path
}

# Downloads url to dest unless dest already has the SHA-256 want. The file only
# appears at dest once its checksum matches.
download_verified() {
  local url=$1 want=$2 dest=$3 tmp
  [[ -f "$dest" && $(sha256_of "$dest") == "$want" ]] && return
  log "Downloading $url"
  mkdir -p "$(dirname "$dest")"
  tmp=$(mktemp "$dest.XXXXXX")
  if ! curl -fsSL -o "$tmp" "$url"; then
    rm -f "$tmp"
    die "download of $url failed"
  fi
  if [[ $(sha256_of "$tmp") != "$want" ]]; then
    rm -f "$tmp"
    die "checksum mismatch for $url"
  fi
  mv "$tmp" "$dest"
}

ensure_kubectl() {
  local os arch want
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac
  case "$os/$arch" in
    darwin/arm64) want=fd65982c97ddad3106754b69ffa196d0e543aa591930ae52aed1adfb92f8c77f ;;
    darwin/amd64) want=6851381c486ff6edd691623e3d65c87cb9a5b02887ff8fbbb38d8a031b748387 ;;
    linux/amd64) want=65691ff77eb6fa44c908b77a1082c9f092c3b9733b5cefabec0d1104890e21a8 ;;
    linux/arm64) want=ff749f4b78d9c4f1ec87307df9b50119ed819e2094aa9810cb9acffc3286c8c7 ;;
    *) die "unsupported platform $os/$arch" ;;
  esac
  download_verified "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$os/$arch/kubectl" "$want" "$KUBECTL_BIN"
  chmod +x "$KUBECTL_BIN"
}

# Downloads the pinned Bun into bin/, checksum-verified, unless it is there.
ensure_bun() {
  local pinned os arch want archive
  pinned=$(sed -n 's/^ *"packageManager": "bun@\(.*\)",*$/\1/p' web/package.json)
  [[ "$pinned" == "$BUN_VERSION" ]] ||
    die "web/package.json pins Bun ${pinned:-nowhere} but hack/lib.sh pins $BUN_VERSION; change both, with the checksums"
  [[ -x "$BUN_BIN" ]] && return
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in x86_64) arch=x64 ;; arm64) arch=aarch64 ;; esac
  case "$os-$arch" in
    darwin-aarch64) want=80afd4c069b46a6fa8f3ec520bebc750a7843e4a8d5b9583d29ad436c00a3403 ;;
    darwin-x64) want=62660e6438eb872278877acca77e1152d5d031324df368d4a81b428211489d2d ;;
    linux-x64) want=3e730528d89775f03f87f05c06787c4df4099d0a37f3f252e7b829cb23816e18 ;;
    linux-aarch64) want=efa9813da5ed72423bf847f916e8d2c47c0d776add972354026a75e10da9aa21 ;;
    *) die "unsupported platform $os-$arch" ;;
  esac
  archive="$BIN_DIR/bun-$BUN_VERSION-$os-$arch.zip"
  download_verified "https://github.com/oven-sh/bun/releases/download/bun-v$BUN_VERSION/bun-$os-$arch.zip" "$want" "$archive"
  unzip -o -q -j "$archive" "bun-$os-$arch/bun" -d "$BIN_DIR/bun-unpack"
  mv "$BIN_DIR/bun-unpack/bun" "$BUN_BIN"
  rm -rf "$BIN_DIR/bun-unpack" "$archive"
  chmod +x "$BUN_BIN"
}

# Downloads every upstream file pinned in deploy/upstream-files.txt.
fetch_upstream_files() {
  local sha path url
  while read -r sha path url; do
    [[ -z "$sha" || "$sha" == \#* ]] && continue
    download_verified "$url" "$sha" "$ROOT/$path"
  done <"$ROOT/deploy/upstream-files.txt"
}

# Fails when Docker's disk is nearly full, and warns when it is getting there:
# kind, its images and the local object store all live on it, and a full disk
# shows up as obscure failures deep inside a test.
check_docker_disk() {
  local free_kb
  free_kb=$(docker run --rm --entrypoint df "$KAFKA_IMAGE" -Pk / | awk 'NR == 2 { print $4 }')
  if [[ $free_kb -lt $((2 * 1024 * 1024)) ]]; then
    die "Docker's disk has only $((free_kb / 1024)) MiB free; free space (docker system df) or enlarge the disk in Docker Desktop"
  elif [[ $free_kb -lt $((5 * 1024 * 1024)) ]]; then
    log "Warning: Docker's disk has only $((free_kb / 1024)) MiB free"
  fi
}

build_kafka_image() {
  log "Building $KAFKA_IMAGE"
  retry_registry docker buildx build --load --quiet -t "$KAFKA_IMAGE" images/kafka >/dev/null
  tag_content "$KAFKA_IMAGE"
}

build_agent_image() {
  log "Building $AGENT_IMAGE"
  retry_registry docker buildx build --load --quiet -t "$AGENT_IMAGE" -f images/agent/Dockerfile . >/dev/null
  tag_content "$AGENT_IMAGE"
}

build_provisioner_image() {
  log "Building $PROVISIONER_IMAGE"
  retry_registry docker buildx build --load --quiet -t "$PROVISIONER_IMAGE" -f images/provisioner/Dockerfile . >/dev/null
  tag_content "$PROVISIONER_IMAGE"
}

build_console_image() {
  log "Building $CONSOLE_IMAGE"
  retry_registry docker buildx build --load --quiet -t "$CONSOLE_IMAGE" -f images/console/Dockerfile . >/dev/null
  tag_content "$CONSOLE_IMAGE"
}

# The git-ignored overlay kind_local_overlay writes.
KIND_LOCAL_OVERLAY="$ROOT/deploy/overlays/kind-local"

# Writes KIND_LOCAL_OVERLAY: the kind overlay with the locally built images.
# The provisioner and the console run the content tags of their images, and
# the provisioner gives customers those of the Kafka and agent images, so
# that every pod rolls when its image is rebuilt.
kind_local_overlay() {
  local image kafka agent provisioner console
  for image in "$KAFKA_IMAGE" "$AGENT_IMAGE" "$PROVISIONER_IMAGE" "$CONSOLE_IMAGE"; do
    docker image inspect "$image" >/dev/null 2>&1 || die "$image has not been built; run make dev-up"
  done
  kafka=$(content_tag "$KAFKA_IMAGE")
  agent=$(content_tag "$AGENT_IMAGE")
  provisioner=$(content_tag "$PROVISIONER_IMAGE")
  console=$(content_tag "$CONSOLE_IMAGE")
  mkdir -p "$KIND_LOCAL_OVERLAY"
  cat >"$KIND_LOCAL_OVERLAY/kustomization.yaml" <<KUSTOMIZATION
# Written by hack/lib.sh (kind_local_overlay); do not edit.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../kind
images:
  - name: ${PROVISIONER_IMAGE%:*}
    newTag: "${provisioner##*:}"
  - name: ${CONSOLE_IMAGE%:*}
    newTag: "${console##*:}"
patches:
  - target:
      kind: Deployment
      name: changeport-provisioner
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: -kafka-image=$kafka
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: -agent-image=$agent
KUSTOMIZATION
}

cluster_exists() { kind get clusters 2>/dev/null | grep -x "$CLUSTER_NAME" >/dev/null; }

# Creates the kind cluster from deploy/kind/cluster.yaml, or checks that the
# existing one still matches it: three zone-labelled workers on the pinned
# Kubernetes version. Then preloads KIND_PRELOAD_IMAGES into its workers, and
# the content tags of KAFKA_IMAGE, AGENT_IMAGE, PROVISIONER_IMAGE and
# CONSOLE_IMAGE once they have been built.
ensure_kind_cluster() {
  if cluster_exists; then
    check_kind_cluster
  else
    log "Creating kind cluster $CLUSTER_NAME"
    kind create cluster --name "$CLUSTER_NAME" --config deploy/kind/cluster.yaml --image "$KIND_NODE_IMAGE" --wait 3m
  fi
  local image
  for image in "${KIND_PRELOAD_IMAGES[@]}"; do
    docker image inspect "$image" >/dev/null 2>&1 && continue
    log "Pulling $image"
    retry_registry docker pull --quiet "$image" >/dev/null
  done
  # Only the workers run these pods; the control plane is tainted, and every
  # node keeps its own copy of each image.
  local workers
  workers=$(kind get nodes --name "$CLUSTER_NAME" | grep -v control-plane | paste -sd , -)
  log "Loading images into the kind workers"
  kind load docker-image --name "$CLUSTER_NAME" --nodes "$workers" "${KIND_PRELOAD_IMAGES[@]}" >/dev/null 2>&1
  local image
  for image in "$KAFKA_IMAGE" "$AGENT_IMAGE" "$PROVISIONER_IMAGE" "$CONSOLE_IMAGE"; do
    if docker image inspect "$image" >/dev/null 2>&1; then
      kind load docker-image --name "$CLUSTER_NAME" --nodes "$workers" "$(content_tag "$image")" >/dev/null 2>&1
      prune_node_images "$image" "$workers"
    fi
  done
}

# Removes the content tags of image's earlier builds from the kind workers,
# where kind load leaves them, unless a container still runs them.
# Usage: prune_node_images <image> <comma-separated workers>
prune_node_images() {
  local image=$1 workers=$2 current node old
  current=docker.io/$(content_tag "$image")
  for node in ${workers//,/ }; do
    while read -r old; do
      [[ $old == "$current" || $old == "docker.io/$image" ]] && continue
      docker exec "$node" crictl rmi "$old" >/dev/null 2>&1 || true
    done < <(docker exec "$node" crictl images | awk -v repo="docker.io/${image%:*}" '$1 == repo { print $1 ":" $2 }')
  done
}

# Starts cloud-provider-kind unless it is already running. The Gateway API
# controller it also offers is not used.
ensure_cloud_provider_kind() {
  [[ $(docker inspect -f '{{.State.Running}}' "$CLOUD_PROVIDER_KIND_CONTAINER" 2>/dev/null) == true ]] && return
  docker rm -f "$CLOUD_PROVIDER_KIND_CONTAINER" >/dev/null 2>&1 || true
  log "Starting cloud-provider-kind"
  docker run -d --name "$CLOUD_PROVIDER_KIND_CONTAINER" --restart unless-stopped --network kind \
    -v /var/run/docker.sock:/var/run/docker.sock "$CLOUD_PROVIDER_KIND_IMAGE" --gateway-channel disabled >/dev/null
}

# Starts the host gateway from the provisioner image, anew, so that it runs
# the latest build with the current cluster's credentials. It reads the load
# balancers' Services as the kind cluster's admin, on the kind network.
ensure_host_gateway() {
  local kubeconfig="$BIN_DIR/host-gateway.kubeconfig"
  kind get kubeconfig --internal --name "$CLUSTER_NAME" >"$kubeconfig"
  chmod 600 "$kubeconfig"
  docker rm -f "$HOST_GATEWAY_CONTAINER" >/dev/null 2>&1 || true
  log "Starting the host gateway on 127.0.0.1:$HOST_KAFKA_PORT"
  docker run -d --name "$HOST_GATEWAY_CONTAINER" --restart unless-stopped --network kind \
    --user "$(id -u):$(id -g)" -v "$kubeconfig:/kubeconfig:ro" -e KUBECONFIG=/kubeconfig \
    -p "127.0.0.1:$HOST_KAFKA_PORT:9094" -p "127.0.0.1:$HOST_DNS_PORT:15353/udp" \
    --entrypoint /hostgateway "$(content_tag "$PROVISIONER_IMAGE")" >/dev/null ||
    die "could not start the host gateway; is port $HOST_KAFKA_PORT of 127.0.0.1 taken, by a local Kafka perhaps?"
}

# Prints "<public name> <address>" for each load balancer of a customer's
# public listener, from the external-dns hostname annotations that publish the
# names on EKS.
public_addresses() {
  kubectl -n "customer-$1" get services -l "strimzi.io/cluster=$1" -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.annotations.external-dns\.alpha\.kubernetes\.io/hostname}{" "}{.status.loadBalancer.ingress[0].ip}{"\n"}{end}'
}

# Prepares a Kafka client container for a customer's public listener on kind,
# as a client outside the cluster would connect: it writes the admin user's
# client.properties into dir (mounted at /config) and sets KAFKA_CLIENT, the
# docker run arguments that resolve the public names to the load balancers on
# the kind network. Usage: prepare_kafka_client <customer> <dir>
prepare_kafka_client() {
  provisioner credentials -customer "$1" >"$2/client.properties"
  kafka_client_container "$1" "$2"
}

# Sets KAFKA_CLIENT, the docker run arguments of a Kafka client container on
# the kind network that mounts dir at /config and resolves the customer's
# public names to their load balancers.
# Usage: kafka_client_container <customer> <dir>
kafka_client_container() {
  local customer=$1 dir=$2 name address hosts=0
  KAFKA_CLIENT=(--rm --network kind -e LOG_DIR=/tmp/kafka-logs -v "$dir:/config:ro")
  while read -r name address; do
    [[ -n "$name" && -n "$address" ]] || die "a load balancer of customer $customer has no public name or address yet"
    KAFKA_CLIENT+=(--add-host "$name:$address")
    hosts=$((hosts + 1))
  done < <(public_addresses "$customer")
  [[ $hosts -gt 0 ]] || die "customer $customer has no load balancers"
}

# Lists a directory of the local object store, such as
# /buckets/changeport-tiered-storage/customer-demo/, one entry per line.
# Usage: object_store_ls <path>
object_store_ls() { kubectl -n object-store exec deploy/seaweedfs -- sh -c "echo 'fs.ls $1' | weed shell 2>/dev/null"; }

# Prints the HTTP status the console at localhost answers GET /api/config
# with (200 configured, 503 not), once it answers at all: right after a
# restart, the old pod may drop connections for a moment.
# Usage: console_config_status
console_config_status() {
  local code
  for _ in $(seq 30); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://localhost:$CONSOLE_PORT/api/config" || true)
    if [[ $code != 000 ]]; then
      echo "$code"
      return
    fi
    sleep 2
  done
  die "the console at http://localhost:$CONSOLE_PORT does not answer"
}

# Prints the value of a PromQL query against the metrics store that returns at
# most one number, or nothing when it returns no series. It goes through the
# Kubernetes API server's service proxy, so it needs no port-forward.
metrics_query() {
  local encoded
  encoded=$(printf '%s' "$1" | od -An -tx1 -v | tr -d ' \n' | sed 's/../%&/g')
  kubectl get --raw "/api/v1/namespaces/metrics/services/victoria-metrics:http/proxy/api/v1/query?query=$encoded" |
    sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p'
}

# Runs the console's named metrics queries (internal/metrics) against the
# local VictoriaMetrics for tenant, through a port-forward. Each query must
# succeed, and those in expected (comma-separated names) must return series.
check_named_queries() {
  local tenant=$1 expected=$2 output port pid
  output=$(mktemp)
  kubectl -n metrics port-forward svc/victoria-metrics :8428 >"$output" 2>&1 &
  pid=$!
  for _ in $(seq 50); do
    port=$(sed -nE 's/^Forwarding from 127\.0\.0\.1:([0-9]+) .*/\1/p' "$output")
    [[ -n $port ]] && break
    sleep 0.2
  done
  if [[ -z $port ]]; then
    kill "$pid" 2>/dev/null || true
    die "could not port-forward to VictoriaMetrics:"$'\n'"$(cat "$output")"
  fi
  # Rates need two scrapes, 30 seconds apart, after the traffic they measure,
  # so a customer that just served its first messages may take a minute.
  local attempt
  for attempt in $(seq 10); do
    CHANGEPORT_METRICS_URL="http://127.0.0.1:$port" CHANGEPORT_METRICS_TENANT="$tenant" \
      CHANGEPORT_METRICS_EXPECT="$expected" go test -count=1 -run TestQueriesAgainstVictoriaMetrics ./internal/metrics >"$output" 2>&1 && break
    if [[ $attempt -eq 10 ]]; then
      kill "$pid" 2>/dev/null || true
      die "the console's metrics queries failed for tenant $tenant:"$'\n'"$(cat "$output")"
    fi
    sleep 10
  done
  kill "$pid" 2>/dev/null || true
  rm -f "$output"
  log "The console's metrics queries work for tenant $tenant, with series for $expected"
}

# Prints a variable of the git-ignored .env at the repository root, without
# running .env as a script, or nothing.
dotenv_value() {
  [[ -f "$ROOT/.env" ]] || return 0
  sed -n "s/^$1=//p" "$ROOT/.env" | tail -1 | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/"
}

check_kind_cluster() {
  log "kind cluster $CLUSTER_NAME already exists"
  local want=${KIND_NODE_IMAGE#kindest/node:} zones versions
  want=${want%@*}
  zones=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' | sort -u | grep -c . || true)
  versions=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.nodeInfo.kubeletVersion}{"\n"}{end}' | sort -u | paste -sd ' ' -)
  if [[ $zones -ne 3 || "$versions" != "$want" ]]; then
    die "kind cluster $CLUSTER_NAME has nodes in $zones zones on Kubernetes $versions; deploy/kind/cluster.yaml wants 3 zones on $want. Recreate it: make dev-down dev-up"
  fi
  if [[ $(docker port "$CLUSTER_NAME-control-plane" 30088/tcp 2>/dev/null) != "127.0.0.1:$CONSOLE_PORT" ]]; then
    die "kind cluster $CLUSTER_NAME does not publish the console on localhost:$CONSOLE_PORT, which deploy/kind/cluster.yaml asks for. Recreate it: make dev-down dev-up"
  fi
}
