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

# cloud-provider-kind gives LoadBalancer Services on kind an address: a
# container per Service on the kind Docker network. It runs as a container on
# the host's Docker, watching the kind clusters.
CLOUD_PROVIDER_KIND_IMAGE=registry.k8s.io/cloud-provider-kind/cloud-controller-manager:v0.12.0@sha256:1b97632e5dfec8f83c7e970cf18cedb9e6b707e272011d712cabf6ab59d323c9
CLOUD_PROVIDER_KIND_CONTAINER=changeport-cloud-provider-kind

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

# kubectl always targets KUBE_CONTEXT, never the current context. It is the
# local kind cluster unless a script sets it explicitly (hack/platform-apply.sh).
kubectl() { "$KUBECTL_BIN" --context "$KUBE_CONTEXT" "$@"; }
kind() { "$BIN_DIR/kind" "$@"; }
# Runs the provisioning CLI against KUBE_CONTEXT. provision also gets the
# content-tagged local images, so that customers run the latest builds.
provisioner() {
  local images=()
  if [[ $1 == provision ]]; then
    for image in KAFKA_IMAGE AGENT_IMAGE; do
      if docker image inspect "${!image}" >/dev/null 2>&1; then
        images+=("-$(echo "${image%_IMAGE}" | tr '[:upper:]' '[:lower:]')-image" "$(content_tag "${!image}")")
      fi
    done
  fi
  go run ./cmd/provisioner "$1" -context "$KUBE_CONTEXT" "${images[@]}" "${@:2}"
}

# Prints the content-addressed name of a locally built image, such as
# changeport/agent:7a88857b2635 for changeport/agent:local: a rebuild with
# new content gets a new name, so the pods running it roll.
content_tag() {
  local id
  id=$(docker image inspect -f '{{.Id}}' "$1")
  id=${id#sha256:}
  echo "${1%:*}:${id:0:12}"
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
  docker buildx build --load --quiet -t "$KAFKA_IMAGE" images/kafka >/dev/null
  docker tag "$KAFKA_IMAGE" "$(content_tag "$KAFKA_IMAGE")"
}

build_agent_image() {
  log "Building $AGENT_IMAGE"
  docker buildx build --load --quiet -t "$AGENT_IMAGE" -f images/agent/Dockerfile . >/dev/null
  docker tag "$AGENT_IMAGE" "$(content_tag "$AGENT_IMAGE")"
}

cluster_exists() { kind get clusters 2>/dev/null | grep -x "$CLUSTER_NAME" >/dev/null; }

# Creates the kind cluster from deploy/kind/cluster.yaml, or checks that the
# existing one still matches it: three zone-labelled workers on the pinned
# Kubernetes version. Then preloads KIND_PRELOAD_IMAGES into its workers, and
# KAFKA_IMAGE and AGENT_IMAGE once they have been built.
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
    docker pull --quiet "$image" >/dev/null
  done
  # Only the workers run these pods; the control plane is tainted, and every
  # node keeps its own copy of each image.
  local workers
  workers=$(kind get nodes --name "$CLUSTER_NAME" | grep -v control-plane | paste -sd , -)
  log "Loading images into the kind workers"
  kind load docker-image --name "$CLUSTER_NAME" --nodes "$workers" "${KIND_PRELOAD_IMAGES[@]}" >/dev/null 2>&1
  local image
  for image in "$KAFKA_IMAGE" "$AGENT_IMAGE"; do
    if docker image inspect "$image" >/dev/null 2>&1; then
      kind load docker-image --name "$CLUSTER_NAME" --nodes "$workers" "$(content_tag "$image")" >/dev/null 2>&1
    fi
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
  local customer=$1 dir=$2 name address hosts=0
  provisioner credentials -customer "$customer" >"$dir/client.properties"
  KAFKA_CLIENT=(--rm --network kind -e LOG_DIR=/tmp/kafka-logs -v "$dir:/config:ro")
  while read -r name address; do
    [[ -n "$name" && -n "$address" ]] || die "a load balancer of customer $customer has no public name or address yet"
    KAFKA_CLIENT+=(--add-host "$name:$address")
    hosts=$((hosts + 1))
  done < <(public_addresses "$customer")
  [[ $hosts -gt 0 ]] || die "customer $customer has no load balancers"
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

check_kind_cluster() {
  log "kind cluster $CLUSTER_NAME already exists"
  local want=${KIND_NODE_IMAGE#kindest/node:} zones versions
  want=${want%@*}
  zones=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' | sort -u | grep -c . || true)
  versions=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.nodeInfo.kubeletVersion}{"\n"}{end}' | sort -u | paste -sd ' ' -)
  if [[ $zones -ne 3 || "$versions" != "$want" ]]; then
    die "kind cluster $CLUSTER_NAME has nodes in $zones zones on Kubernetes $versions; deploy/kind/cluster.yaml wants 3 zones on $want. Recreate it: make dev-down dev-up"
  fi
}
