#!/usr/bin/env bash
# Gives the console on kind its Clerk configuration: writes the Secret
# changeport/changeport-console-clerk from the git-ignored .env at the
# repository root (variables in .env.sample) and restarts the console when it
# changed. Without .env the console stays unconfigured and refuses every API
# call, which is how CI runs it.
# Usage: hack/console-config.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

env_file="$ROOT/.env"
secret=changeport-console-clerk
ensure_kubectl

if [[ ! -f $env_file ]]; then
  log "No .env, so the console stays unconfigured; see .env.sample"
  kubectl -n changeport delete secret "$secret" --ignore-not-found >/dev/null
  exit 0
fi

publishable_key=$(dotenv_value CLERK_PUBLISHABLE_KEY)
# The name Clerk's React quickstart uses.
[[ -n $publishable_key ]] || publishable_key=$(dotenv_value VITE_CLERK_PUBLISHABLE_KEY)
secret_key=$(dotenv_value CLERK_SECRET_KEY)
staff_org=$(dotenv_value CHANGEPORT_STAFF_ORG_ID)

[[ $publishable_key == pk_* ]] || die ".env has no Clerk publishable key (CLERK_PUBLISHABLE_KEY=pk_...)"
[[ $secret_key == sk_* ]] || die ".env has no Clerk secret key (CLERK_SECRET_KEY=sk_...)"
[[ $staff_org == org_* ]] || die ".env has no staff organization ID (CHANGEPORT_STAFF_ORG_ID=org_...)"

# The keys go through files, not command lines, which other users can see.
work=$(umask 077 && mktemp -d)
trap 'rm -rf "$work"' EXIT
printf '%s' "$publishable_key" >"$work/CLERK_PUBLISHABLE_KEY"
printf '%s' "$secret_key" >"$work/CLERK_SECRET_KEY"
printf '%s' "$staff_org" >"$work/CHANGEPORT_STAFF_ORG_ID"
# Server-side apply keeps no copy of the data in an annotation.
kubectl -n changeport create secret generic "$secret" --dry-run=client -o yaml \
  --from-file="$work/CLERK_PUBLISHABLE_KEY" --from-file="$work/CLERK_SECRET_KEY" --from-file="$work/CHANGEPORT_STAFF_ORG_ID" |
  kubectl apply --server-side --field-manager=changeport-console-config -f - >/dev/null

# The console reads its configuration when it starts, so a change restarts
# it; the annotation holds a hash of the configuration, not the keys.
cat "$work/CLERK_PUBLISHABLE_KEY" "$work/CLERK_SECRET_KEY" "$work/CHANGEPORT_STAFF_ORG_ID" >"$work/all"
hash=$(sha256_of "$work/all")
current=$(kubectl -n changeport get deployment changeport-console -o jsonpath='{.spec.template.metadata.annotations.changeport\.io/clerk-config}')
if [[ $hash != "$current" ]]; then
  log "Restarting the console with its Clerk configuration"
  kubectl -n changeport patch deployment changeport-console --type merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"changeport.io/clerk-config\":\"$hash\"}}}}}" >/dev/null
  kubectl -n changeport rollout status deployment/changeport-console --timeout=2m >/dev/null
elif [[ $(console_config_status) == 503 ]]; then
  # The console started while the Secret was missing.
  log "Restarting the console, which started without its Clerk configuration"
  kubectl -n changeport rollout restart deployment/changeport-console >/dev/null
  kubectl -n changeport rollout status deployment/changeport-console --timeout=2m >/dev/null
fi
[[ $(console_config_status) == 200 ]] || die "the console at http://localhost:$CONSOLE_PORT did not take its Clerk configuration"
log "The console at http://localhost:$CONSOLE_PORT signs in with Clerk"
