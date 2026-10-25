#!/usr/bin/env bash
# Points this machine's DNS for the customers' public names on kind
# (*.kafka.changeport.test) at the host gateway, which forwards Kafka clients
# to their clusters. On macOS it adds a resolver for changeport.test that asks
# the gateway, once for every customer to come. On Linux it writes the current
# customers' names into /etc/hosts; run it again after provisioning more.
# Both need sudo, which asks for your password.
# Usage: hack/host-dns.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

domain=changeport.test

# Fails with the commands to run by hand when sudo cannot ask for a password,
# as from an editor's or agent's shell without a terminal.
need_sudo() {
  sudo -n true 2>/dev/null && return
  [[ -t 0 ]] && return
  die "sudo needs your password, but there is no terminal to ask in; run this in a terminal:"$'\n'"  $1"
}

case $(uname -s) in
Darwin)
  resolver=/etc/resolver/$domain
  want=$'# Written by changeport hack/host-dns.sh: the host gateway on kind\nnameserver 127.0.0.1\nport '"$HOST_DNS_PORT"
  if [[ -f $resolver && $(cat "$resolver") == "$want" ]]; then
    log "$resolver already sends $domain to the host gateway"
    exit 0
  fi
  log "Writing $resolver, so that macOS asks the host gateway for $domain"
  need_sudo "just host-dns   # or: sudo mkdir -p /etc/resolver && printf 'nameserver 127.0.0.1\\nport $HOST_DNS_PORT\\n' | sudo tee $resolver"
  sudo mkdir -p /etc/resolver
  printf '%s\n' "$want" | sudo tee "$resolver" >/dev/null
  ;;
Linux)
  ensure_kubectl
  begin="# BEGIN changeport host gateway"
  end="# END changeport host gateway"
  names=$(kubectl get services -A -l strimzi.io/cluster \
    -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.annotations.external-dns\.alpha\.kubernetes\.io/hostname}{"\n"}{end}' |
    grep -E "\.kafka\.$domain$" | sort -u)
  [[ -n $names ]] || die "no customer on kind has public names yet"
  block=$(printf '%s\n' "$begin" && while read -r name; do printf '127.0.0.1 %s\n' "$name"; done <<<"$names" && printf '%s\n' "$end")
  log "Writing the customers' $(wc -l <<<"$names" | tr -d ' ') public names into /etc/hosts"
  need_sudo "just host-dns"
  {
    sed "/^$begin\$/,/^$end\$/d" /etc/hosts
    printf '%s\n' "$block"
  } >"${TMPDIR:-/tmp}/hosts.changeport"
  sudo cp "${TMPDIR:-/tmp}/hosts.changeport" /etc/hosts
  rm -f "${TMPDIR:-/tmp}/hosts.changeport"
  ;;
*)
  die "just host-dns supports macOS and Linux"
  ;;
esac
log "Kafka clients on this machine reach the customers' clusters on kind with their client.properties"
