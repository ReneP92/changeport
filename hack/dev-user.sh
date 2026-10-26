#!/usr/bin/env bash
# Makes sure you can sign in to the console on kind without manual steps, as
# either side of it, with Clerk test addresses that Clerk development
# instances verify with the code 424242 (cmd/devuser):
# - <developer>+clerk_test@example.com, an admin of the staff organization,
#   who sees the operator console; and
# - <developer>-demo+clerk_test@example.com, an admin of an organization of
#   your own, "Demo (<developer>)", which it links to the demo customer, who
#   sees exactly what a customer's admin sees.
# It creates what is missing and changes nothing that is already right. The
# developer's name is CHANGEPORT_DEV_USER from .env, or else the start of
# your git email address, up to a dot or plus sign; CHANGEPORT_DEV_USER=none
# turns this off. It does nothing without .env or in CI, whose signed-in tests
# bring users of their own.
# Usage: hack/dev-user.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEMO_CUSTOMER=${DEMO_CUSTOMER-demo}

# Prints the developer's name from an email address: its local part up to the
# first dot or plus sign, lowercased, without characters names cannot have.
# Usage: developer_from_email <email>
developer_from_email() {
  local name=${1%%@*}
  name=${name%%[.+]*}
  name=$(tr '[:upper:]' '[:lower:]' <<<"$name" | tr -cd 'a-z0-9_-')
  echo "$name"
}

if [[ ! -f "$ROOT/.env" ]]; then
  log "No .env, so there is no dev user to set up"
  exit 0
fi
if [[ -n ${CI:-} ]]; then
  exit 0
fi
developer=$(dotenv_value CHANGEPORT_DEV_USER)
if [[ $developer == none ]]; then
  exit 0
fi
[[ -n $developer ]] || developer=$(developer_from_email "$(git -C "$ROOT" config user.email || true)")
[[ -n $developer ]] || developer=$(developer_from_email "${USER:-}")
[[ -n $developer ]] || die "set CHANGEPORT_DEV_USER in .env to your name, such as rene"

demo_organization=""
[[ -z $DEMO_CUSTOMER ]] || demo_organization="Demo ($developer)"
log "Setting up your Clerk test users"
output=$(CLERK_SECRET_KEY=$(dotenv_value CLERK_SECRET_KEY) CHANGEPORT_STAFF_ORG_ID=$(dotenv_value CHANGEPORT_STAFF_ORG_ID) \
  go run ./cmd/devuser -developer "$developer" -demo-org "$demo_organization")
grep -v '^demo_organization=' <<<"$output" | sed 's/^/  /' >&2

organization=$(sed -n 's/^demo_organization=//p' <<<"$output")
if [[ -n $organization ]]; then
  ensure_kubectl
  if [[ -n $(kubectl get customer "$DEMO_CUSTOMER" --ignore-not-found -o name) ]]; then
    kubectl patch customer "$DEMO_CUSTOMER" --type merge \
      -p "{\"spec\":{\"clerkOrganizationID\":\"$organization\"}}" >/dev/null
  fi
fi
log "Sign in at http://localhost:$CONSOLE_PORT with the code 424242 as $developer+clerk_test@example.com for the operator console"
if [[ -n $demo_organization ]]; then
  log "or as $developer-demo+clerk_test@example.com for the demo customer's admin"
fi
