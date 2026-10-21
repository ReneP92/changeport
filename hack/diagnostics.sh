#!/usr/bin/env bash
# Collects what is needed to understand a failed run on kind into a directory:
# Docker containers and disk, the cluster's nodes, pods and events, the
# Strimzi and cert-manager resources, and the logs of every platform component
# and customer pod, including the previous run of restarted containers. CI
# uploads it when the end-to-end job fails.
# Usage: hack/diagnostics.sh <directory>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# Collect as much as possible: one failing command must not stop the rest.
set +e

out=${1:?usage: hack/diagnostics.sh <directory>}
mkdir -p "$out/logs"
ensure_kubectl

docker ps -a >"$out/docker-ps.txt" 2>&1
docker system df >"$out/docker-df.txt" 2>&1
df -h >"$out/df.txt" 2>&1
if ! cluster_exists; then
  echo "kind cluster $CLUSTER_NAME does not exist" >"$out/cluster.txt"
  exit 0
fi

kubectl get nodes -o wide --show-labels >"$out/nodes.txt" 2>&1
kubectl get pods -A -o wide >"$out/pods.txt" 2>&1
kubectl get events -A --sort-by=.lastTimestamp >"$out/events.txt" 2>&1
kubectl get kafka,kafkanodepool,kafkauser,kafkatopic,certificate,clusterissuer -A -o yaml >"$out/resources.yaml" 2>&1
kubectl get services,persistentvolumeclaims -A -o wide >"$out/services-and-volumes.txt" 2>&1

while read -r namespace pod; do
  for container in $(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.spec.containers[*].name}'); do
    kubectl -n "$namespace" logs "$pod" -c "$container" >"$out/logs/$namespace.$pod.$container.log" 2>&1
    kubectl -n "$namespace" logs "$pod" -c "$container" --previous >"$out/logs/$namespace.$pod.$container.previous.log" 2>/dev/null ||
      rm -f "$out/logs/$namespace.$pod.$container.previous.log"
  done
done < <(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' | grep -v '^kube-system ')

log "Diagnostics collected in $out"
