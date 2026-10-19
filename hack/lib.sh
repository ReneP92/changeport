# shellcheck shell=bash
# Shared settings and helpers for the local development scripts. Source it.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

# Pinned versions. Strimzi 1.3.0 is the newest release and supports Kafka 4.3.1,
# the newest Kafka 4.x (see internal/provisioning.KafkaVersion).
# shellcheck disable=SC2034 # used by the scripts that source this file
STRIMZI_VERSION=1.3.0
# shellcheck disable=SC2034
STRIMZI_SHA256=c58da61a66c20169d1c9c42736a9355b183ee17ac7779ab7875beaedbf972232
KUBECTL_VERSION=v1.36.5

CLUSTER_NAME=${CLUSTER_NAME:-changeport}
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
BIN_DIR="$ROOT/bin"
KUBECTL_BIN="$BIN_DIR/kubectl-$KUBECTL_VERSION"

log() { printf '\033[1m==> %s\033[0m\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# kubectl always targets the local kind cluster, never the current context.
kubectl() { "$KUBECTL_BIN" --context "$KUBE_CONTEXT" "$@"; }
kind() { "$BIN_DIR/kind" "$@"; }
provisioner() { go run ./cmd/provisioner "$1" -context "$KUBE_CONTEXT" "${@:2}"; }

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

ensure_kubectl() {
  [[ -x "$KUBECTL_BIN" ]] && return
  local os arch want
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac
  case "$os/$arch" in
    darwin/arm64) want=c5850a4a6b9469b26cce47c1ab456285f58e308283dcdf72b4ddda3db49b0ec0 ;;
    darwin/amd64) want=03338828c3ba74c37bf5964573fbfbab132eb5bb15da2f821c25a4043d6eb3f0 ;;
    linux/amd64) want=33bc88a24c3b09cf55bfd59e6ca977881b61fc3f25ab65bc80c65b430248648e ;;
    linux/arm64) want=88fc1aca8fd0c1b44fe379f67c39bd6858bdfc50241dc55775f84d0f5b31da3a ;;
    *) die "unsupported platform $os/$arch" ;;
  esac
  log "Downloading kubectl $KUBECTL_VERSION"
  mkdir -p "$BIN_DIR"
  local tmp
  tmp=$(mktemp "$BIN_DIR/kubectl.XXXXXX")
  curl -fsSL -o "$tmp" "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$os/$arch/kubectl"
  if [[ $(sha256_of "$tmp") != "$want" ]]; then
    rm -f "$tmp"
    die "kubectl checksum mismatch"
  fi
  chmod +x "$tmp"
  mv "$tmp" "$KUBECTL_BIN"
}

cluster_exists() { kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; }
