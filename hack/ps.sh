#!/usr/bin/env bash
# Lists every container of the local setup, grouped: the kind cluster's
# nodes, the customers' load balancers that cloud-provider-kind runs (one per
# broker and one per bootstrap address), and the helpers hack/lib.sh starts.
# Docker Desktop cannot group them, as kind and cloud-provider-kind create
# their containers without a Compose project label; their own labels say
# what each one is.
# Usage: hack/ps.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Prints rows of tab-separated columns as a table, indented.
table() { column -t -s $'\t' | sed 's/^/  /'; }

# Prints a container's address on the kind network.
address() { docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "$1"; }

printf 'Kind cluster %s: nodes\n' "$CLUSTER_NAME"
nodes=$(docker ps -a --filter "label=io.x-k8s.kind.cluster=$CLUSTER_NAME" \
  --format '{{.Names}}\t{{.Label "io.x-k8s.kind.role"}}\t{{.Status}}' | sort)
if [[ -z $nodes ]]; then
  echo "  none; make dev-up creates the cluster"
else
  { printf 'NODE\tROLE\tSTATUS\n' && printf '%s\n' "$nodes"; } | table
fi

printf '\nLoad balancers, by customer\n'
balancers=$(docker ps -a --filter "label=io.x-k8s.cloud-provider-kind.cluster=$CLUSTER_NAME" \
  --format '{{.Names}}\t{{.Label "io.x-k8s.cloud-provider-kind.loadbalancer.name"}}\t{{.Status}}')
if [[ -z $balancers ]]; then
  echo "  none"
else
  {
    printf 'CUSTOMER\tSERVICE\tCONTAINER\tADDRESS\tSTATUS\n'
    # The load balancer's name is <cluster>/<namespace>/<service>.
    while IFS=$'\t' read -r name lb status; do
      IFS=/ read -r _ namespace service <<<"$lb"
      printf '%s\t%s\t%s\t%s\t%s\n' "${namespace#customer-}" "$service" "$name" "$(address "$name")" "$status"
    done <<<"$balancers" | sort
  } | table
fi

printf '\nHelpers\n'
{
  printf 'CONTAINER\tPORTS\tSTATUS\n'
  for name in "$CLOUD_PROVIDER_KIND_CONTAINER" "$HOST_GATEWAY_CONTAINER"; do
    docker ps -a --filter "name=^${name}\$" --format '{{.Names}}\t{{if .Ports}}{{.Ports}}{{else}}-{{end}}\t{{.Status}}'
  done
} | table
